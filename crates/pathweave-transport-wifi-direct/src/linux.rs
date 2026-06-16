//! Linux WiFi Direct backend via wpa_supplicant D-Bus.
//!
//! Protocol:
//!   `fi.w1.wpa_supplicant1` — main service
//!   `fi.w1.wpa_supplicant1.Interface.P2PDevice` — per-interface P2P extension
//!
//! ADR 022 documents the GO election rule and address lifecycle.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use pathweave_core::{
    NodeIdentity, PathweaveError, PeerAddress, PeerAnnouncement, Result,
};
use tokio::net::TcpStream;
use zbus::zvariant::{OwnedObjectPath, OwnedValue, Value};
use zbus::{Connection as ZbusConn, Proxy};

use super::{Inner, P2P_SERVICE_NAME, WIFI_DIRECT_PORT};
use crate::WifiDirectConnection;

// --------------------------------------------------------------------------
// D-Bus interface and service constants
// --------------------------------------------------------------------------

const WPA_SERVICE: &str = "fi.w1.wpa_supplicant1";
const WPA_IFACE: &str = "fi.w1.wpa_supplicant1.Interface";
const WPA_P2P_IFACE: &str = "fi.w1.wpa_supplicant1.Interface.P2PDevice";
const WPA_PATH: &str = "/fi/w1/wpa_supplicant1";

// --------------------------------------------------------------------------
// State held while the transport is running
// --------------------------------------------------------------------------

pub(crate) struct LinuxState {
    _conn: ZbusConn,
    iface_path: OwnedObjectPath,
    conn_tx: tokio::sync::mpsc::UnboundedSender<WifiDirectConnection>,
    local_short_id: [u8; 8],
}

// --------------------------------------------------------------------------
// start / stop
// --------------------------------------------------------------------------

pub(crate) async fn start(inner: &Arc<Inner>, identity: &NodeIdentity) -> Result<()> {
    let local_short_id: [u8; 8] = identity.peer_id().as_bytes()[..8]
        .try_into()
        .expect("PeerId is always 32 bytes");

    let conn = ZbusConn::system()
        .await
        .map_err(|e| PathweaveError::Transport(format!("D-Bus connection failed: {e}")))?;

    // Get the first P2P-capable wireless interface from wpa_supplicant.
    let wpa_proxy = Proxy::new(&conn, WPA_SERVICE, WPA_PATH, "fi.w1.wpa_supplicant1")
        .await
        .map_err(|e| PathweaveError::Transport(format!("wpa_supplicant proxy failed: {e}")))?;

    let iface_paths: Vec<OwnedObjectPath> = wpa_proxy
        .get_property("Interfaces")
        .await
        .map_err(|e| PathweaveError::Transport(format!("could not read Interfaces: {e}")))?;

    let iface_path = iface_paths
        .into_iter()
        .next()
        .ok_or_else(|| PathweaveError::Transport("no wpa_supplicant interfaces found".into()))?;

    // Register Pathweave P2P service so remote peers can filter by name.
    let p2p_proxy = Proxy::new(&conn, WPA_SERVICE, iface_path.as_str(), WPA_P2P_IFACE)
        .await
        .map_err(|e| PathweaveError::Transport(format!("P2PDevice proxy failed: {e}")))?;

    let mut svc_args: HashMap<&str, OwnedValue> = HashMap::new();
    svc_args.insert(
        "service_type",
        OwnedValue::try_from(Value::from("bonjour")).unwrap(),
    );
    svc_args.insert("version", OwnedValue::try_from(Value::from(1u32)).unwrap());
    svc_args.insert(
        "service",
        OwnedValue::try_from(Value::from(P2P_SERVICE_NAME)).unwrap(),
    );

    p2p_proxy
        .call_method("ServiceAdd", &(svc_args,))
        .await
        .map_err(|e| PathweaveError::Transport(format!("ServiceAdd failed: {e}")))?;

    // Start P2P discovery.
    let find_args: HashMap<&str, OwnedValue> = HashMap::new();
    p2p_proxy
        .call_method("Find", &(0i32, find_args))
        .await
        .map_err(|e| PathweaveError::Transport(format!("P2PFind failed: {e}")))?;

    // Spawn the accept listener. Incoming P2P group formations where we are GO
    // result in a TCP accept on the P2P interface address.
    let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel();

    let accept_conn = conn.clone();
    let accept_iface = iface_path.clone();
    let accept_tx = conn_tx.clone();
    let short_id = local_short_id;
    tokio::spawn(async move {
        accept_loop(accept_conn, accept_iface, accept_tx, short_id).await;
    });

    let state = LinuxState {
        _conn: conn,
        iface_path,
        conn_tx,
        local_short_id,
    };

    *inner.state.lock().await = Some(state);
    *inner.conn_rx.lock().await = Some(conn_rx);
    Ok(())
}

pub(crate) async fn stop(inner: &Arc<Inner>) -> Result<()> {
    // Dropping LinuxState drops the D-Bus connection, which closes all proxies
    // and stops any pending signal subscriptions. conn_tx dropping causes
    // accept_loop's channel send to fail, exiting the loop.
    *inner.state.lock().await = None;
    *inner.conn_rx.lock().await = None;
    Ok(())
}

// --------------------------------------------------------------------------
// discover
// --------------------------------------------------------------------------

pub(crate) fn discover(inner: Arc<Inner>) -> BoxStream<'static, PeerAnnouncement> {
    let (tx, rx) = futures::channel::mpsc::unbounded();

    tokio::spawn(async move {
        let guard = inner.state.lock().await;
        let state = match guard.as_ref() {
            Some(s) => s,
            None => return,
        };

        let conn = state._conn.clone();
        let iface_path = state.iface_path.clone();
        drop(guard);

        let p2p_proxy =
            match Proxy::new(&conn, WPA_SERVICE, iface_path.as_str(), WPA_P2P_IFACE).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("WiFi Direct discover: P2PDevice proxy failed: {e}");
                    return;
                }
            };

        // Subscribe to P2PPeerFound signals.
        let mut stream = match p2p_proxy.receive_signal("P2PPeerFound").await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("WiFi Direct discover: P2PPeerFound signal failed: {e}");
                return;
            }
        };

        while let Some(msg) = stream.next().await {
            // P2PPeerFound body: (object_path: str, properties: dict<str, variant>)
            let body: zbus::Result<(String, HashMap<String, OwnedValue>)> =
                msg.body().deserialize();
            match body {
                Ok((peer_path, props)) => {
                    // Extract short_id from service data if present.
                    let device_name = props.get("DeviceName").and_then(|v| match &**v {
                        Value::Str(s) => Some(s.to_string()),
                        _ => None,
                    });
                    let short_id =
                        device_name.as_deref().and_then(parse_short_id_from_service_name);

                    // The peer P2P device address is the last component of the object path.
                    let mac = peer_path.rsplit('/').next().unwrap_or("").to_string();
                    if mac.is_empty() {
                        continue;
                    }
                    tracing::debug!("WiFi Direct: discovered peer {mac}");
                    let ann = PeerAnnouncement {
                        address: PeerAddress::WifiDirect(mac),
                        short_id,
                    };
                    if tx.unbounded_send(ann).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("WiFi Direct: P2PPeerFound parse error: {e}");
                }
            }
        }
    });

    Box::pin(rx)
}

// --------------------------------------------------------------------------
// connect (initiator side)
// --------------------------------------------------------------------------

pub(crate) async fn connect(
    inner: &Arc<Inner>,
    peer: &PeerAnnouncement,
) -> Result<Box<dyn pathweave_core::Connection>> {
    let device_id = match &peer.address {
        PeerAddress::WifiDirect(id) => id.clone(),
        _ => {
            return Err(PathweaveError::Transport(
                "WiFi Direct connect: wrong address type".into(),
            ))
        }
    };

    let guard = inner.state.lock().await;
    let state = match guard.as_ref() {
        Some(s) => s,
        None => {
            return Err(PathweaveError::Transport(
                "WiFi Direct transport not started".into(),
            ))
        }
    };

    let conn = state._conn.clone();
    let iface_path = state.iface_path.clone();
    let local_short_id = state.local_short_id;
    drop(guard);

    // Determine GO role from short_id comparison (ADR 022).
    let peer_short_id = peer.short_id;
    let we_are_go = match peer_short_id {
        Some(their_id) => local_short_id > their_id,
        None => false, // fall back to client if peer short_id unknown
    };

    let go_intent: u32 = if we_are_go { 15 } else { 0 };

    let p2p_proxy = Proxy::new(&conn, WPA_SERVICE, iface_path.as_str(), WPA_P2P_IFACE)
        .await
        .map_err(|e| PathweaveError::Transport(format!("P2PDevice proxy failed: {e}")))?;

    // Subscribe to GroupStarted before calling P2PConnect so we don't miss it.
    let iface_proxy = Proxy::new(&conn, WPA_SERVICE, iface_path.as_str(), WPA_IFACE)
        .await
        .map_err(|e| PathweaveError::Transport(format!("Interface proxy failed: {e}")))?;

    let mut group_stream = iface_proxy
        .receive_signal("P2PGroupStarted")
        .await
        .map_err(|e| PathweaveError::Transport(format!("P2PGroupStarted signal failed: {e}")))?;

    let mut connect_args: HashMap<&str, OwnedValue> = HashMap::new();
    connect_args.insert(
        "go_intent",
        OwnedValue::try_from(Value::from(go_intent)).unwrap(),
    );
    connect_args.insert(
        "wps_method",
        OwnedValue::try_from(Value::from("pbc")).unwrap(),
    );

    p2p_proxy
        .call_method("Connect", &(device_id.as_str(), connect_args))
        .await
        .map_err(|e| PathweaveError::Transport(format!("P2PConnect failed: {e}")))?;

    // Wait for GroupStarted with a 30-second timeout.
    let group_props = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(msg) = group_stream.next().await {
            let body: zbus::Result<(HashMap<String, OwnedValue>,)> = msg.body().deserialize();
            if let Ok((props,)) = body {
                return Some(props);
            }
        }
        None
    })
    .await
    .map_err(|_| PathweaveError::Transport("WiFi Direct: P2P group formation timed out".into()))?
    .ok_or_else(|| PathweaveError::Transport("WiFi Direct: GroupStarted stream closed".into()))?;

    let peer_ip = extract_go_ip(&group_props, we_are_go)?;

    let addr: SocketAddr = format!("{peer_ip}:{WIFI_DIRECT_PORT}")
        .parse()
        .map_err(|e| {
            PathweaveError::Transport(format!("WiFi Direct: invalid peer IP '{peer_ip}': {e}"))
        })?;

    tracing::debug!("WiFi Direct: connecting TCP to {addr}");
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| PathweaveError::Transport(format!("WiFi Direct TCP connect failed: {e}")))?;

    Ok(Box::new(WifiDirectConnection::new(stream)))
}

// --------------------------------------------------------------------------
// accept (responder side — called by PathweaveNode's accept loop)
// --------------------------------------------------------------------------

pub(crate) async fn accept(inner: &Arc<Inner>) -> Result<Box<dyn pathweave_core::Connection>> {
    let conn = inner.conn_rx.lock().await.as_mut().and_then(|rx| {
        // Non-blocking: return None if no connection is ready yet.
        rx.try_recv().ok()
    });

    if let Some(c) = conn {
        return Ok(Box::new(c));
    }

    // Block until a connection arrives.
    let mut guard = inner.conn_rx.lock().await;
    match guard.as_mut() {
        Some(rx) => rx
            .recv()
            .await
            .map(|c| Box::new(c) as Box<dyn pathweave_core::Connection>)
            .ok_or_else(|| PathweaveError::Transport("WiFi Direct: accept channel closed".into())),
        None => Err(PathweaveError::Transport(
            "WiFi Direct transport not started".into(),
        )),
    }
}

// --------------------------------------------------------------------------
// accept_loop: listens for P2PGroupStarted events where we are GO and
// accepts incoming TCP connections from the client on the P2P interface.
// --------------------------------------------------------------------------

async fn accept_loop(
    conn: ZbusConn,
    iface_path: OwnedObjectPath,
    conn_tx: tokio::sync::mpsc::UnboundedSender<WifiDirectConnection>,
    local_short_id: [u8; 8],
) {
    let iface_proxy = match Proxy::new(&conn, WPA_SERVICE, iface_path.as_str(), WPA_IFACE).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("WiFi Direct accept_loop: Interface proxy failed: {e}");
            return;
        }
    };

    let mut group_stream = match iface_proxy.receive_signal("P2PGroupStarted").await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("WiFi Direct accept_loop: P2PGroupStarted signal failed: {e}");
            return;
        }
    };

    while let Some(msg) = group_stream.next().await {
        let body: zbus::Result<(HashMap<String, OwnedValue>,)> = msg.body().deserialize();
        let props = match body {
            Ok((p,)) => p,
            Err(_) => continue,
        };

        // Only act on groups where we are GO.
        let role: String = props
            .get("role")
            .and_then(|v| match &**v {
                Value::Str(s) => Some(s.to_string()),
                _ => None,
            })
            .unwrap_or_default();
        if role != "GO" {
            continue;
        }

        // The GO's address on the P2P interface comes from the "go_dev_addr" property
        // or can be derived from the P2P interface address.
        let p2p_iface_addr = match props.get("interface_address").and_then(|v| match &**v {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        }) {
            Some(addr) => addr,
            None => continue,
        };

        let bind_addr: SocketAddr = match format!("{p2p_iface_addr}:{WIFI_DIRECT_PORT}").parse() {
            Ok(a) => a,
            Err(_) => continue,
        };

        let tx = conn_tx.clone();
        tokio::spawn(async move {
            tracing::debug!("WiFi Direct: GO listening on {bind_addr}");
            match tokio::net::TcpListener::bind(bind_addr).await {
                Ok(listener) => match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        tracing::debug!("WiFi Direct: accepted TCP from {peer_addr}");
                        let _ = tx.send(WifiDirectConnection::new(stream));
                    }
                    Err(e) => tracing::warn!("WiFi Direct: TCP accept failed: {e}"),
                },
                Err(e) => tracing::warn!("WiFi Direct: TCP bind failed: {e}"),
            }
        });

        let _ = local_short_id; // used for role determination upstream
    }
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// Extracts the peer's IP address from the GroupStarted properties.
/// When we are GO, the peer is the client; when we are client, the peer is GO.
fn extract_go_ip(props: &HashMap<String, OwnedValue>, we_are_go: bool) -> Result<String> {
    // wpa_supplicant GroupStarted provides "ip_addr_go" (the GO's IP) and
    // "ip_addr" (the local interface IP). If we are client, the peer is GO.
    // If we are GO, the client IP is available via "ip_addr_go" only after
    // DHCP completes; we bind and wait for the incoming TCP connection instead.
    let key = if we_are_go { "ip_addr" } else { "ip_addr_go" };
    props
        .get(key)
        .and_then(|v| match &**v {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .ok_or_else(|| {
            PathweaveError::Transport(format!(
                "WiFi Direct: '{key}' missing from GroupStarted properties"
            ))
        })
}

/// Attempts to parse an 8-byte short_id encoded as hex from the service
/// device name field. Returns None if the name does not contain a valid
/// hex-encoded short_id suffix (format: "<P2P_SERVICE_NAME>:<hex16>").
fn parse_short_id_from_service_name(name: &str) -> Option<[u8; 8]> {
    let prefix = format!("{}:", P2P_SERVICE_NAME);
    let hex = name.strip_prefix(prefix.as_str())?;
    if hex.len() != 16 {
        return None;
    }
    let bytes = (0..8)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<std::result::Result<Vec<u8>, _>>()
        .ok()?;
    bytes.try_into().ok()
}
