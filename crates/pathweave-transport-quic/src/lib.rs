use std::{net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
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

// --------------------------------------------------------------------------
// Transport
// --------------------------------------------------------------------------

pub struct QuicTransport {
    listen_addr: SocketAddr,
    endpoint: Mutex<Option<Endpoint>>,
}

impl QuicTransport {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            endpoint: Mutex::new(None),
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
    async fn start(&self) -> Result<()> {
        let server_config = make_server_config()?;
        let endpoint =
            Endpoint::server(server_config, self.listen_addr).map_err(PathweaveError::Io)?;
        *self.endpoint.lock().await = Some(endpoint);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(ep) = self.endpoint.lock().await.take() {
            ep.close(0u32.into(), b"shutdown");
        }
        Ok(())
    }

    fn discover(&self) -> BoxStream<'_, PeerAnnouncement> {
        Box::pin(futures::stream::empty())
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
        TransportCost::Metered
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Quic
    }

    fn name(&self) -> &'static str {
        "quic"
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
        server.start().await.unwrap();
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
        let mut session = Session::initiate(&client_identity, bundled).await.unwrap();
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
