#![deny(clippy::all)]
#![forbid(unsafe_code)]

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use pathweave_core::{
    Connection, PathweaveError, PeerAddress, PeerAnnouncement, Result, Transport, TransportCost,
    TransportKind,
};
use quinn::{
    crypto::rustls::QuicClientConfig,
    rustls::{
        self,
        client::danger::ServerCertVerifier,
        pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
    },
    ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig,
};
use rcgen::CertifiedKey;
use tokio::sync::Mutex;

const SERVICE_TYPE: &str = "_pathweave._udp.local.";

// --------------------------------------------------------------------------
// mDNS state
// --------------------------------------------------------------------------

struct MdnsState {
    daemon: ServiceDaemon,
    service_fullname: String,
    // Taken by discover() on first call; None afterwards.
    announce_rx: Option<tokio::sync::mpsc::UnboundedReceiver<PeerAnnouncement>>,
    // Taken by departures() on first call; None afterwards.
    departure_rx: Option<tokio::sync::mpsc::UnboundedReceiver<PeerAddress>>,
    bridge: tokio::task::JoinHandle<()>,
}

// --------------------------------------------------------------------------
// Transport
// --------------------------------------------------------------------------

pub struct QuicTransport {
    listen_addr: SocketAddr,
    endpoint: Mutex<Option<Endpoint>>,
    mdns: std::sync::Mutex<Option<MdnsState>>,
    instance_name: String,
    // Detected at start() time; read lock-free by cost(). Encoding: 0=Free, 1=Metered, 2=Unknown.
    cost: AtomicU8,
    // Set to Some(addr) in start(), cleared to None in stop(). Accessed by the sync
    // local_addresses() method, so a std::sync::Mutex is used instead of tokio's.
    started_addr: std::sync::Mutex<Option<SocketAddr>>,
}

impl QuicTransport {
    pub fn new(listen_addr: SocketAddr) -> Self {
        let mut bytes = [0u8; 8];
        getrandom::getrandom(&mut bytes).expect("system entropy unavailable");
        let instance_name = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        Self {
            listen_addr,
            endpoint: Mutex::new(None),
            mdns: std::sync::Mutex::new(None),
            instance_name,
            cost: AtomicU8::new(2), // Unknown until start() detects the interface type
            started_addr: std::sync::Mutex::new(None),
        }
    }

    /// Returns the OS-assigned local address after `start()`, or `None` if not started.
    pub async fn local_addr(&self) -> Option<SocketAddr> {
        self.endpoint
            .lock()
            .await
            .as_ref()
            .and_then(|e| e.local_addr().ok())
    }
}

/// Finds the local IPv4 address the OS would route through when reaching an
/// external host. Opens a UDP socket but sends no packets — just queries the
/// routing table. Falls back to loopback if the query fails.
fn local_ipv4() -> Ipv4Addr {
    std::net::UdpSocket::bind("0.0.0.0:0")
        .ok()
        .and_then(|s| {
            s.connect("8.8.8.8:80").ok()?;
            match s.local_addr().ok()?.ip() {
                IpAddr::V4(ip) => Some(ip),
                _ => None,
            }
        })
        .unwrap_or(Ipv4Addr::LOCALHOST)
}

// --------------------------------------------------------------------------
// Network cost detection (ADR 015)
// --------------------------------------------------------------------------

// Linux: parse /proc/net/route to find the default-route interface, then
// classify by interface name prefix. No new dependencies required.
#[cfg(target_os = "linux")]
fn detect_network_cost() -> TransportCost {
    let content = match std::fs::read_to_string("/proc/net/route") {
        Ok(c) => c,
        Err(_) => return TransportCost::Unknown,
    };
    let iface = content.lines().skip(1).find_map(|line| {
        let mut fields = line.split_whitespace();
        let iface = fields.next()?;
        let dest = fields.next()?;
        if dest == "00000000" {
            Some(iface.to_string())
        } else {
            None
        }
    });
    match iface.as_deref() {
        Some(n) if n.starts_with("wlan") || n.starts_with("wlp") || n.starts_with("wlx") => {
            TransportCost::Free
        }
        Some(n)
            if n.starts_with("eth")
                || n.starts_with("enp")
                || n.starts_with("eno")
                || n.starts_with("ens")
                || n.starts_with("enx") =>
        {
            TransportCost::Free
        }
        Some(n) if n.starts_with("wwan") || n.starts_with("rmnet") || n.starts_with("ppp") => {
            TransportCost::Metered
        }
        _ => TransportCost::Unknown,
    }
}

// Windows: ask the OS directly via WinRT NetworkInformation. Unrestricted maps
// to Free; Fixed and Variable are metered plans with data caps.
#[cfg(target_os = "windows")]
fn detect_network_cost() -> TransportCost {
    use windows::Networking::Connectivity::{NetworkCostType, NetworkInformation};
    let result = (|| -> windows::core::Result<TransportCost> {
        let profile = NetworkInformation::GetInternetConnectionProfile()?;
        let cost_type = profile.GetConnectionCost()?.NetworkCostType()?;
        if cost_type == NetworkCostType::Unrestricted {
            Ok(TransportCost::Free)
        } else if cost_type == NetworkCostType::Unknown {
            Ok(TransportCost::Unknown)
        } else {
            Ok(TransportCost::Metered)
        }
    })();
    result.unwrap_or(TransportCost::Unknown)
}

// macOS: match active interface BSD names (from if_addrs) against the
// SystemConfiguration interface list to get the interface type. Free wins
// over Metered if multiple active interfaces are found.
#[cfg(target_os = "macos")]
fn detect_network_cost() -> TransportCost {
    use std::collections::HashSet;
    use system_configuration::network_configuration::{get_interfaces, SCNetworkInterfaceType};

    let active: HashSet<String> = if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .filter(|i| !i.is_loopback())
        .map(|i| i.name)
        .collect();

    let mut found_free = false;
    let mut found_metered = false;

    for iface in get_interfaces().iter() {
        let Some(bsd) = iface.bsd_name() else {
            continue;
        };
        if !active.contains(bsd.to_string().as_str()) {
            continue;
        }
        match iface.interface_type() {
            Some(SCNetworkInterfaceType::IEEE80211) | Some(SCNetworkInterfaceType::Ethernet) => {
                found_free = true;
            }
            Some(SCNetworkInterfaceType::WWAN) => {
                found_metered = true;
            }
            _ => {}
        }
    }

    if found_free {
        TransportCost::Free
    } else if found_metered {
        TransportCost::Metered
    } else {
        TransportCost::Unknown
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
fn detect_network_cost() -> TransportCost {
    TransportCost::Unknown
}

fn make_server_config() -> Result<ServerConfig> {
    let CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["pathweave".into()])
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
    let cert_der = cert.der().clone();
    let priv_key = PrivatePkcs8KeyDer::from(key_pair.serialize_der());
    ServerConfig::with_single_cert(vec![cert_der], priv_key.into())
        .map_err(|e| PathweaveError::Transport(e.to_string()))
}

fn make_client_config() -> Result<ClientConfig> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    let quic_client_config =
        QuicClientConfig::try_from(crypto).map_err(|e| PathweaveError::Transport(e.to_string()))?;
    Ok(ClientConfig::new(Arc::new(quic_client_config)))
}

// --------------------------------------------------------------------------
// TLS certificate verifier
//
// QUIC requires TLS 1.3; we skip TLS-level certificate verification because
// Noise_XX already authenticates both peers. The TLS layer is just the
// protocol requirement (RFC 9000). See ADR 004 and ADR 008.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

// --------------------------------------------------------------------------
// Transport impl
// --------------------------------------------------------------------------

#[async_trait]
impl Transport for QuicTransport {
    async fn start(&self, _identity: &pathweave_core::NodeIdentity) -> Result<()> {
        let server_config = make_server_config()?;
        let endpoint =
            Endpoint::server(server_config, self.listen_addr).map_err(PathweaveError::Io)?;
        let local_addr = endpoint.local_addr().map_err(PathweaveError::Io)?;
        *self.endpoint.lock().await = Some(endpoint);
        *self.started_addr.lock().unwrap() = Some(local_addr);

        let daemon = ServiceDaemon::new().map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let host_name = format!("{}.local.", self.instance_name);
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            &self.instance_name,
            &host_name,
            IpAddr::V4(local_ipv4()),
            local_addr.port(),
            None,
        )
        .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let service_fullname = info.get_fullname().to_string();

        daemon
            .register(info)
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let browse_rx = daemon
            .browse(SERVICE_TYPE)
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        // Bridge: drains flume ServiceEvents into two tokio mpsc channels.
        // announce_tx carries PeerAnnouncement for discover(); dep_tx carries
        // PeerAddress for departures(). The bridge tracks fullname -> Vec<SocketAddr>
        // internally because ServiceRemoved only provides the fullname, not the
        // address, so we must look up the address from the prior ServiceResolved.
        let (announce_tx, announce_rx) = tokio::sync::mpsc::unbounded_channel::<PeerAnnouncement>();
        let (dep_tx, dep_rx) = tokio::sync::mpsc::unbounded_channel::<PeerAddress>();
        let bridge = tokio::spawn(async move {
            let mut resolved: std::collections::HashMap<String, Vec<SocketAddr>> =
                std::collections::HashMap::new();
            while let Ok(event) = browse_rx.recv_async().await {
                match event {
                    ServiceEvent::ServiceResolved(info) => {
                        let port = info.get_port();
                        let fullname = info.get_fullname().to_string();
                        let mut addrs = Vec::new();
                        for ip in info.get_addresses_v4() {
                            let addr = SocketAddr::new(IpAddr::V4(*ip), port);
                            addrs.push(addr);
                            if announce_tx
                                .send(PeerAnnouncement {
                                    address: PeerAddress::Quic(addr),
                                    short_id: None,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        resolved.insert(fullname, addrs);
                    }
                    ServiceEvent::ServiceRemoved(_, fullname) => {
                        if let Some(addrs) = resolved.remove(&fullname) {
                            for addr in addrs {
                                if dep_tx.send(PeerAddress::Quic(addr)).is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        *self.mdns.lock().unwrap() = Some(MdnsState {
            daemon,
            service_fullname,
            announce_rx: Some(announce_rx),
            departure_rx: Some(dep_rx),
            bridge,
        });

        let detected = detect_network_cost();
        self.cost.store(
            match detected {
                TransportCost::Free => 0,
                TransportCost::Metered => 1,
                TransportCost::Unknown => 2,
            },
            Ordering::Relaxed,
        );
        tracing::debug!("QUIC transport cost detected: {:?}", detected);

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        *self.started_addr.lock().unwrap() = None;
        if let Some(ep) = self.endpoint.lock().await.take() {
            ep.close(0u32.into(), b"shutdown");
        }
        if let Some(state) = self.mdns.lock().unwrap().take() {
            let _ = state.daemon.unregister(&state.service_fullname);
            let _ = state.daemon.stop_browse(SERVICE_TYPE);
            state.bridge.abort();
            let _ = state.daemon.shutdown();
        }
        Ok(())
    }

    fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
        let rx = self
            .mdns
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|state| state.announce_rx.take());

        match rx {
            Some(rx) => Box::pin(futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|ann| (ann, rx))
            })),
            None => Box::pin(futures::stream::empty()),
        }
    }

    async fn connect(&self, peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
        let addr = match &peer.address {
            PeerAddress::Quic(addr) => *addr,
            _ => return Err(PathweaveError::Transport("expected QUIC address".into())),
        };

        let client_config = make_client_config()?;
        let mut endpoint =
            Endpoint::client("0.0.0.0:0".parse().unwrap()).map_err(PathweaveError::Io)?;
        endpoint.set_default_client_config(client_config);

        let connection = endpoint
            .connect(addr, "pathweave")
            .map_err(|e| PathweaveError::Transport(e.to_string()))?
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        Ok(Box::new(QuicConnection { send, recv }))
    }

    async fn accept(&self) -> Result<Box<dyn Connection>> {
        let endpoint = {
            let guard = self.endpoint.lock().await;
            guard
                .as_ref()
                .ok_or_else(|| PathweaveError::Transport("transport not started".into()))?
                .clone()
        };

        let incoming = endpoint
            .accept()
            .await
            .ok_or_else(|| PathweaveError::Transport("endpoint closed".into()))?;

        let connection = incoming
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        Ok(Box::new(QuicConnection { send, recv }))
    }

    fn mtu_hint(&self) -> usize {
        1200
    }

    fn cost(&self) -> TransportCost {
        match self.cost.load(Ordering::Relaxed) {
            0 => TransportCost::Free,
            1 => TransportCost::Metered,
            _ => TransportCost::Unknown,
        }
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Quic
    }

    fn name(&self) -> &'static str {
        "quic"
    }

    fn local_addresses(&self) -> Vec<PeerAddress> {
        self.started_addr
            .lock()
            .unwrap()
            .map(|addr| vec![PeerAddress::Quic(addr)])
            .unwrap_or_default()
    }

    fn departures(&self) -> BoxStream<'static, PeerAddress> {
        let rx = self
            .mdns
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|state| state.departure_rx.take());

        match rx {
            Some(rx) => Box::pin(futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|addr| (addr, rx))
            })),
            None => Box::pin(futures::stream::empty()),
        }
    }
}

// --------------------------------------------------------------------------
// Connection
// --------------------------------------------------------------------------

pub struct QuicConnection {
    send: SendStream,
    recv: RecvStream,
}

// Quinn resets a SendStream on drop instead of sending FIN, so the receiver
// gets a stream-reset error before it can read buffered data. Finish here to
// ensure every code path sends FIN and the peer can drain the stream cleanly.
impl Drop for QuicConnection {
    fn drop(&mut self) {
        let _ = self.send.finish();
    }
}

// QUIC is a byte stream, not a message stream. We prefix every frame with a
// 4-byte big-endian length so recv_bytes can reassemble complete messages.
#[async_trait]
impl Connection for QuicConnection {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let len = (bytes.len() as u32).to_be_bytes();
        self.send
            .write_all(&len)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        self.send
            .write_all(bytes)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))
    }

    async fn recv_bytes(&mut self) -> Result<Bytes> {
        let mut len_buf = [0u8; 4];
        self.recv
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.recv
            .read_exact(&mut buf)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    async fn close(&mut self) -> Result<()> {
        self.send
            .finish()
            .map_err(|e| PathweaveError::Transport(e.to_string()))
    }

    fn mtu(&self) -> usize {
        1200
    }
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pathweave_core::{BundleLayer, NodeIdentity, PeerAddress, PeerAnnouncement, Session};
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    };

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[tokio::test]
    async fn full_stack_roundtrip() {
        let server = Arc::new(QuicTransport::new(loopback(0)));
        server.start(&NodeIdentity::generate()).await.unwrap();
        let server_addr = server.local_addr().await.unwrap();

        let server_identity = NodeIdentity::generate();
        let client_identity = NodeIdentity::generate();

        let server_clone = Arc::clone(&server);
        let server_id = server_identity.clone();
        let server_task = tokio::spawn(async move {
            let conn = server_clone.accept().await.unwrap();
            let bundled = Box::new(BundleLayer::new(conn));
            let mut session = Session::respond(&server_id, bundled).await.unwrap();
            session.recv().await.unwrap()
        });

        let peer = PeerAnnouncement {
            address: PeerAddress::Quic(server_addr),
            short_id: None,
        };
        let client = QuicTransport::new(loopback(0));
        let conn = client.connect(&peer).await.unwrap();
        let bundled = Box::new(BundleLayer::new(conn));
        let mut session = Session::initiate(&client_identity, bundled, None)
            .await
            .unwrap();
        session.send(b"hello from quic").await.unwrap();

        let received = server_task.await.unwrap();
        assert_eq!(received, Bytes::from_static(b"hello from quic"));
    }

    #[tokio::test]
    async fn accept_before_start_returns_error() {
        let transport = QuicTransport::new(loopback(0));
        let result = transport.accept().await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }

    #[tokio::test]
    async fn wrong_address_type_returns_error() {
        let transport = QuicTransport::new(loopback(0));
        let peer = PeerAnnouncement {
            address: PeerAddress::Ble("AA:BB:CC:DD:EE:FF".into()),
            short_id: None,
        };
        let result = transport.connect(&peer).await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }
}
