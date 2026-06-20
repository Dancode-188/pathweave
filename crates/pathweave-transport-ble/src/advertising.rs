//! BLE advertising-mode bearer (ADR 018, Option A). Carries data inside BLE advertisement
//! packets instead of a GATT connection. The dispatcher and virtual-connection logic here
//! are platform-independent; only `AdvertisingBearer` differs per OS. See the ADR 018
//! addendum (notes/decisions/018-broadcast-transport-design.md) for why this requires
//! BLE 5.0 extended advertising and why macOS is permanently unsupported.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use pathweave_core::{
    Connection, NodeIdentity, PathweaveError, PeerAddress, PeerAnnouncement, Result, Transport,
    TransportCost, TransportKind,
};
use tokio::sync::{broadcast, mpsc, Mutex};

/// `[dest_short_id: 8 bytes][source_short_id: 8 bytes]`, per ADR 018's Decision section.
pub(crate) const HEADER_LEN: usize = 16;

/// Conservative payload budget for a single BLE 5.0 extended advertising PDU once
/// Manufacturer Specific Data AD overhead (4 bytes), the magic byte below, and the
/// 16-byte header above are subtracted from a common ~200-byte practical single-PDU
/// budget. Adapter-dependent and unverified without hardware; see the ADR 018 addendum.
const MAX_PAYLOAD_LEN: usize = 150;

const DEDUP_TTL: Duration = Duration::from_secs(2);

/// Bluetooth SIG company ID reserved for testing and unregistered use, not a real
/// registered company identifier. See the ADR 018 addendum for why this is the honest
/// choice for this project's current stage, and why Manufacturer Specific Data is the
/// wire encoding both platform bearers use (Windows forbids apps from publishing
/// Service Data AD structures at all).
pub(crate) const MANUFACTURER_COMPANY_ID: u16 = 0xFFFF;

/// One-byte prefix inside the manufacturer data payload, before the ADR 018 header.
/// Company ID 0xFFFF is the universal "testing" value; other unrelated devices may
/// also be using it. This magic byte lets a bearer cheaply reject manufacturer-data
/// advertisements that are not ours before attempting to parse them as a header.
pub(crate) const ADV_BEARER_MAGIC: u8 = 0xC0;

fn encode_header(dest: [u8; 8], source: [u8; 8]) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[..8].copy_from_slice(&dest);
    header[8..].copy_from_slice(&source);
    header
}

fn decode_header(bytes: &[u8]) -> Option<([u8; 8], [u8; 8], &[u8])> {
    if bytes.len() < HEADER_LEN {
        return None;
    }
    let mut dest = [0u8; 8];
    let mut source = [0u8; 8];
    dest.copy_from_slice(&bytes[0..8]);
    source.copy_from_slice(&bytes[8..16]);
    Some((dest, source, &bytes[HEADER_LEN..]))
}

fn short_id_to_hex(id: [u8; 8]) -> String {
    id.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_to_short_id(hex: &str) -> Option<[u8; 8]> {
    if hex.len() != 16 {
        return None;
    }
    let mut out = [0u8; 8];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn hash_payload(payload: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    payload.hash(&mut hasher);
    hasher.finish()
}

// --------------------------------------------------------------------------
// AdvertisingBearer: the only platform-specific surface. Everything else in
// this module is written once against this trait. See ADR 018 addendum.
// --------------------------------------------------------------------------

#[async_trait]
pub(crate) trait AdvertisingBearer: Send + Sync {
    /// Broadcasts `packet` (header + payload) into the air.
    async fn advertise(&self, packet: Vec<u8>) -> Result<()>;
    /// Stream of raw packets (header + payload) observed on the medium.
    fn scan(&self) -> BoxStream<'static, Vec<u8>>;
}

// --------------------------------------------------------------------------
// Unsupported-platform fallback. macOS is permanently out of scope: see the
// ADR 018 addendum for why CBPeripheralManager cannot carry this transport.
// --------------------------------------------------------------------------

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
struct UnsupportedBearer;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
#[async_trait]
impl AdvertisingBearer for UnsupportedBearer {
    async fn advertise(&self, _packet: Vec<u8>) -> Result<()> {
        Err(PathweaveError::Transport(
            "BLE advertising-mode bearer is not implemented on this platform".into(),
        ))
    }

    fn scan(&self) -> BoxStream<'static, Vec<u8>> {
        Box::pin(stream::empty())
    }
}

// --------------------------------------------------------------------------
// Virtual connection: a peer-scoped view over the shared broadcast medium.
// --------------------------------------------------------------------------

struct BroadcastConnection {
    bearer: Arc<dyn AdvertisingBearer>,
    local_short_id: [u8; 8],
    peer_short_id: [u8; 8],
    inbound_rx: Mutex<mpsc::UnboundedReceiver<Bytes>>,
}

#[async_trait]
impl Connection for BroadcastConnection {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let header = encode_header(self.peer_short_id, self.local_short_id);
        let mut packet = Vec::with_capacity(HEADER_LEN + bytes.len());
        packet.extend_from_slice(&header);
        packet.extend_from_slice(bytes);
        self.bearer.advertise(packet).await
    }

    async fn recv_bytes(&mut self) -> Result<Bytes> {
        self.inbound_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| PathweaveError::Transport("ble-advertising connection closed".into()))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn mtu(&self) -> usize {
        MAX_PAYLOAD_LEN
    }
}

// --------------------------------------------------------------------------
// Dispatcher: reads the bearer's scan stream, parses headers, routes packets.
// --------------------------------------------------------------------------

type ConnectionMap = Arc<StdMutex<HashMap<[u8; 8], mpsc::UnboundedSender<Bytes>>>>;

async fn dispatch_loop(
    bearer: Arc<dyn AdvertisingBearer>,
    local_short_id: [u8; 8],
    connections: ConnectionMap,
    incoming_tx: mpsc::UnboundedSender<Box<dyn Connection>>,
    announce_tx: broadcast::Sender<PeerAnnouncement>,
) {
    let mut recent: HashMap<([u8; 8], u64), Instant> = HashMap::new();
    let mut scan = bearer.scan();

    while let Some(packet) = scan.next().await {
        let Some((dest, source, payload)) = decode_header(&packet) else {
            continue;
        };
        if source == local_short_id {
            // Self-originated; guards against a medium that echoes our own broadcasts.
            continue;
        }

        let now = Instant::now();
        recent.retain(|_, seen_at| now.duration_since(*seen_at) < DEDUP_TTL);
        let key = (source, hash_payload(payload));
        if recent.contains_key(&key) {
            continue;
        }
        recent.insert(key, now);

        // Anyone we observe a valid-header packet from is provably in range, regardless
        // of whether the packet is addressed to us. The header is plaintext metadata
        // already (ADR 018's security note), so this reveals nothing a passive observer
        // couldn't already see.
        let _ = announce_tx.send(PeerAnnouncement {
            address: PeerAddress::BleAdvertising(short_id_to_hex(source)),
            short_id: Some(source),
        });

        if dest != local_short_id {
            continue;
        }

        let existing_tx = connections
            .lock()
            .expect("mutex not poisoned")
            .get(&source)
            .cloned();
        if let Some(tx) = existing_tx {
            let _ = tx.send(Bytes::copy_from_slice(payload));
        } else {
            let (tx, rx) = mpsc::unbounded_channel();
            let _ = tx.send(Bytes::copy_from_slice(payload));
            connections
                .lock()
                .expect("mutex not poisoned")
                .insert(source, tx);
            let conn: Box<dyn Connection> = Box::new(BroadcastConnection {
                bearer: Arc::clone(&bearer),
                local_short_id,
                peer_short_id: source,
                inbound_rx: Mutex::new(rx),
            });
            if incoming_tx.send(conn).is_err() {
                break;
            }
        }
    }
}

// --------------------------------------------------------------------------
// BleAdvertisingTransport
// --------------------------------------------------------------------------

pub struct BleAdvertisingTransport {
    bearer: Arc<dyn AdvertisingBearer>,
    local_short_id: StdMutex<Option<[u8; 8]>>,
    connections: ConnectionMap,
    incoming_tx: mpsc::UnboundedSender<Box<dyn Connection>>,
    incoming_rx: Mutex<mpsc::UnboundedReceiver<Box<dyn Connection>>>,
    announce_tx: broadcast::Sender<PeerAnnouncement>,
    dispatcher_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl BleAdvertisingTransport {
    pub fn new() -> Self {
        #[cfg(target_os = "linux")]
        let bearer: Arc<dyn AdvertisingBearer> =
            Arc::new(super::linux_adv::LinuxAdvertisingBearer::new());
        #[cfg(target_os = "windows")]
        let bearer: Arc<dyn AdvertisingBearer> =
            Arc::new(super::windows_adv::WindowsAdvertisingBearer::new());
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        let bearer: Arc<dyn AdvertisingBearer> = Arc::new(UnsupportedBearer);

        Self::with_bearer(bearer)
    }

    pub(crate) fn with_bearer(bearer: Arc<dyn AdvertisingBearer>) -> Self {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (announce_tx, _) = broadcast::channel(64);
        Self {
            bearer,
            local_short_id: StdMutex::new(None),
            connections: Arc::new(StdMutex::new(HashMap::new())),
            incoming_tx,
            incoming_rx: Mutex::new(incoming_rx),
            announce_tx,
            dispatcher_handle: Mutex::new(None),
        }
    }
}

impl Default for BleAdvertisingTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for BleAdvertisingTransport {
    async fn start(&self, identity: &NodeIdentity) -> Result<()> {
        let short_id: [u8; 8] = identity.peer_id().as_bytes()[..8]
            .try_into()
            .expect("PeerId is always 32 bytes");
        *self.local_short_id.lock().expect("mutex not poisoned") = Some(short_id);

        let bearer = Arc::clone(&self.bearer);
        let connections = Arc::clone(&self.connections);
        let incoming_tx = self.incoming_tx.clone();
        let announce_tx = self.announce_tx.clone();

        let handle = tokio::spawn(dispatch_loop(
            bearer,
            short_id,
            connections,
            incoming_tx,
            announce_tx,
        ));
        *self.dispatcher_handle.lock().await = Some(handle);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(handle) = self.dispatcher_handle.lock().await.take() {
            handle.abort();
        }
        *self.local_short_id.lock().expect("mutex not poisoned") = None;
        self.connections.lock().expect("mutex not poisoned").clear();
        Ok(())
    }

    fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
        let rx = self.announce_tx.subscribe();
        Box::pin(stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(ann) => return Some((ann, rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        }))
    }

    async fn connect(&self, peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
        let hex_id = match &peer.address {
            PeerAddress::BleAdvertising(s) => s.clone(),
            _ => {
                return Err(PathweaveError::Transport(
                    "expected BLE advertising address".into(),
                ))
            }
        };
        let peer_short_id = hex_to_short_id(&hex_id).ok_or_else(|| {
            PathweaveError::Transport("malformed BLE advertising short_id".into())
        })?;
        let local_short_id = self
            .local_short_id
            .lock()
            .expect("mutex not poisoned")
            .ok_or_else(|| PathweaveError::Transport("transport not started".into()))?;

        let (tx, rx) = mpsc::unbounded_channel();
        self.connections
            .lock()
            .expect("mutex not poisoned")
            .insert(peer_short_id, tx);

        Ok(Box::new(BroadcastConnection {
            bearer: Arc::clone(&self.bearer),
            local_short_id,
            peer_short_id,
            inbound_rx: Mutex::new(rx),
        }))
    }

    async fn accept(&self) -> Result<Box<dyn Connection>> {
        self.incoming_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| PathweaveError::Transport("ble-advertising transport stopped".into()))
    }

    fn mtu_hint(&self) -> usize {
        MAX_PAYLOAD_LEN
    }

    fn cost(&self) -> TransportCost {
        TransportCost::Free
    }

    fn kind(&self) -> TransportKind {
        TransportKind::BleAdvertising
    }

    fn name(&self) -> &'static str {
        "ble-advertising"
    }

    fn local_addresses(&self) -> Vec<PeerAddress> {
        match *self.local_short_id.lock().expect("mutex not poisoned") {
            Some(id) => vec![PeerAddress::BleAdvertising(short_id_to_hex(id))],
            None => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockMedium {
        tx: broadcast::Sender<Vec<u8>>,
    }

    impl MockMedium {
        fn new() -> Self {
            let (tx, _) = broadcast::channel(64);
            Self { tx }
        }

        fn bearer(&self) -> Arc<MockBearer> {
            Arc::new(MockBearer {
                tx: self.tx.clone(),
            })
        }
    }

    struct MockBearer {
        tx: broadcast::Sender<Vec<u8>>,
    }

    #[async_trait]
    impl AdvertisingBearer for MockBearer {
        async fn advertise(&self, packet: Vec<u8>) -> Result<()> {
            let _ = self.tx.send(packet);
            Ok(())
        }

        fn scan(&self) -> BoxStream<'static, Vec<u8>> {
            let rx = self.tx.subscribe();
            Box::pin(stream::unfold(rx, |mut rx| async move {
                loop {
                    match rx.recv().await {
                        Ok(packet) => return Some((packet, rx)),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }))
        }
    }

    #[test]
    fn header_roundtrips() {
        let dest = [1u8; 8];
        let source = [2u8; 8];
        let header = encode_header(dest, source);
        let bytes = [&header[..], b"hello"].concat();
        let (d, s, payload) = decode_header(&bytes).unwrap();
        assert_eq!(d, dest);
        assert_eq!(s, source);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn short_id_hex_roundtrips() {
        let id = [0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04];
        let hex = short_id_to_hex(id);
        assert_eq!(hex_to_short_id(&hex), Some(id));
    }

    #[tokio::test]
    async fn two_transports_exchange_payload_via_mock_medium() {
        let medium = MockMedium::new();

        let a_id = NodeIdentity::generate();
        let b_id = NodeIdentity::generate();

        let a = BleAdvertisingTransport::with_bearer(medium.bearer());
        let b = BleAdvertisingTransport::with_bearer(medium.bearer());

        a.start(&a_id).await.unwrap();
        b.start(&b_id).await.unwrap();

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let b_short_id: [u8; 8] = b_id.peer_id().as_bytes()[..8].try_into().unwrap();
        let peer = PeerAnnouncement {
            address: PeerAddress::BleAdvertising(short_id_to_hex(b_short_id)),
            short_id: Some(b_short_id),
        };

        let mut conn_a = a.connect(&peer).await.unwrap();
        conn_a.send_bytes(b"hello from a").await.unwrap();

        let mut conn_b = b.accept().await.unwrap();
        let received = conn_b.recv_bytes().await.unwrap();
        assert_eq!(received, Bytes::from_static(b"hello from a"));
    }

    #[tokio::test]
    async fn packet_addressed_to_someone_else_is_dropped() {
        let medium = MockMedium::new();

        let a_id = NodeIdentity::generate();
        let b_id = NodeIdentity::generate();
        let someone_else_short_id = [0xffu8; 8];

        let a = BleAdvertisingTransport::with_bearer(medium.bearer());
        let b = BleAdvertisingTransport::with_bearer(medium.bearer());

        a.start(&a_id).await.unwrap();
        b.start(&b_id).await.unwrap();

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let peer = PeerAnnouncement {
            address: PeerAddress::BleAdvertising(short_id_to_hex(someone_else_short_id)),
            short_id: Some(someone_else_short_id),
        };

        let mut conn_a = a.connect(&peer).await.unwrap();
        conn_a.send_bytes(b"not for b").await.unwrap();

        // b must never see an incoming connection for a packet not addressed to it.
        let result = tokio::time::timeout(Duration::from_millis(200), b.accept()).await;
        assert!(
            result.is_err(),
            "b must not observe a packet addressed to someone else"
        );
    }

    #[tokio::test]
    async fn discover_emits_announcement_for_observed_peer() {
        let medium = MockMedium::new();

        let a_id = NodeIdentity::generate();
        let b_id = NodeIdentity::generate();

        let a = BleAdvertisingTransport::with_bearer(medium.bearer());
        let b = BleAdvertisingTransport::with_bearer(medium.bearer());

        a.start(&a_id).await.unwrap();
        b.start(&b_id).await.unwrap();

        let mut b_discover = b.discover();

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let b_short_id: [u8; 8] = b_id.peer_id().as_bytes()[..8].try_into().unwrap();
        let peer = PeerAnnouncement {
            address: PeerAddress::BleAdvertising(short_id_to_hex(b_short_id)),
            short_id: Some(b_short_id),
        };
        let mut conn_a = a.connect(&peer).await.unwrap();
        conn_a.send_bytes(b"hi").await.unwrap();

        let a_short_id: [u8; 8] = a_id.peer_id().as_bytes()[..8].try_into().unwrap();
        let announcement = tokio::time::timeout(Duration::from_secs(2), b_discover.next())
            .await
            .expect("timed out waiting for discover() announcement")
            .expect("discover stream ended");
        assert_eq!(announcement.short_id, Some(a_short_id));
    }
}
