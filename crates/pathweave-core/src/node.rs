use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::stream::BoxStream;

use tokio::sync::watch;

use crate::{
    BundleLayer, Connection, MessageHandler, NodeConfig, NodeIdentity, PathweaveError, PeerAddress,
    PeerAnnouncement, PeerId, Result, Router, Session, Transport, TransportEvent,
};

const DEDUP_TTL: Duration = Duration::from_secs(60);

/// Tracks recently seen (PeerId, message_id) pairs to suppress duplicate deliveries.
///
/// A retry that arrives after a lost ACK carries the same message ID as the original.
/// The cache detects this and suppresses the second on_message() call while still
/// sending the ACK so the sender does not time out. Entries expire after DEDUP_TTL.
struct DeduplicationCache {
    seen: HashMap<(PeerId, u64), Instant>,
    ttl: Duration,
}

impl DeduplicationCache {
    fn new() -> Self {
        Self {
            seen: HashMap::new(),
            ttl: DEDUP_TTL,
        }
    }

    /// Returns true if (peer_id, message_id) was seen within the TTL window.
    ///
    /// If not seen before, records the pair and returns false. Expired entries
    /// are evicted lazily on each call.
    fn check_and_insert(&mut self, peer_id: &PeerId, message_id: u64) -> bool {
        let now = Instant::now();
        let ttl = self.ttl;
        self.seen.retain(|_, inserted_at| {
            now.checked_duration_since(*inserted_at)
                .map(|age| age < ttl)
                .unwrap_or(true)
        });
        let key = (peer_id.clone(), message_id);
        if self.seen.contains_key(&key) {
            return true;
        }
        self.seen.insert(key, now);
        false
    }

    #[cfg(test)]
    fn with_ttl(ttl: Duration) -> Self {
        Self {
            seen: HashMap::new(),
            ttl,
        }
    }
}

/// The top-level entry point for the Pathweave library.
///
/// PathweaveNode wires together the Router, Session layer, and registered transports.
/// Callers create a node, register transports, inject any known peer addresses, and
/// then use the four documented API surfaces (send, on_message, events, new).
///
/// The peer table maps PeerId -> PeerAnnouncement. The discover task (one per
/// transport) populates it automatically via mDNS. add_peer() and connect() also
/// insert into the table for manually-supplied addresses.
pub struct PathweaveNode {
    router: Router,
    identity: NodeIdentity,
    peers: Arc<Mutex<HashMap<PeerId, PeerAnnouncement>>>,
    // Dedup-only: tracks addresses currently in-flight or already connected.
    // Not authoritative for routing — peers is. peer_stream only upserts to
    // peers; it never removes. This invariant is what makes the concurrent
    // add_peer() edge case benign: even if known_addrs loses an entry, the
    // peers entry remains intact and send() continues to work.
    known_addrs: Arc<Mutex<HashSet<SocketAddr>>>,
    handler: Arc<Mutex<Option<Box<dyn MessageHandler>>>>,
    dedup: Arc<Mutex<DeduplicationCache>>,
}

impl PathweaveNode {
    /// Creates a new node. `config` is accepted but unused until transport
    /// implementations are complete; callers should pass `NodeConfig::default()` for now.
    pub async fn new(_config: NodeConfig, identity: NodeIdentity) -> Result<Self> {
        Ok(Self {
            router: Router::new(),
            identity,
            peers: Arc::new(Mutex::new(HashMap::new())),
            known_addrs: Arc::new(Mutex::new(HashSet::new())),
            handler: Arc::new(Mutex::new(None)),
            dedup: Arc::new(Mutex::new(DeduplicationCache::new())),
        })
    }

    /// Registers a transport, starts its background availability monitor, and
    /// spawns an accept loop that delivers incoming connections to the message
    /// handler and a discover loop that populates the peer table via mDNS.
    ///
    /// Not part of the UniFFI boundary; Rust callers use this during setup.
    /// Must be called before any send() calls that depend on this transport.
    pub fn register_transport(&mut self, transport: Box<dyn Transport>) {
        let arc: Arc<dyn Transport> = Arc::from(transport);
        let identity = self.identity.clone();
        let handler = Arc::clone(&self.handler);
        let dedup = Arc::clone(&self.dedup);
        let started = self.router.register_transport(Arc::clone(&arc));

        tokio::spawn(accept_loop(
            Arc::clone(&arc),
            identity.clone(),
            handler,
            dedup,
            started.clone(),
        ));

        tokio::spawn(crate::router::peer_stream(
            arc,
            identity,
            started,
            Arc::clone(&self.peers),
            Arc::clone(&self.known_addrs),
            self.identity.peer_id().clone(),
        ));
    }

    /// Dials `announcement`, completes the Noise_XX handshake as the initiator, stores
    /// the resulting PeerId -> announcement mapping in the peer table, and returns the PeerId.
    ///
    /// Tries transports in cost order (Free first). The handshake session is closed
    /// immediately after the PeerId is learned; subsequent send() calls re-dial.
    ///
    /// Returns NoTransportAvailable if no registered transport is available or all fail.
    pub async fn connect(&mut self, announcement: PeerAnnouncement) -> Result<PeerId> {
        let peer_id = self.router.connect(&announcement, &self.identity).await?;
        if let PeerAddress::Quic(addr) = &announcement.address {
            self.known_addrs.lock().unwrap().insert(*addr);
        }
        self.peers
            .lock()
            .unwrap()
            .insert(peer_id.clone(), announcement);
        Ok(peer_id)
    }

    /// Records a known PeerId -> PeerAnnouncement mapping in the peer table.
    ///
    /// Not part of the UniFFI boundary. Used by pw-chat to inject a QUIC peer
    /// address resolved from the command line, and by tests to set up known peers.
    pub fn add_peer(&mut self, peer_id: PeerId, announcement: PeerAnnouncement) {
        if let PeerAddress::Quic(addr) = &announcement.address {
            self.known_addrs.lock().unwrap().insert(*addr);
        }
        self.peers.lock().unwrap().insert(peer_id, announcement);
    }

    /// Sends `payload` to the peer identified by `peer_id`.
    ///
    /// Looks up the peer's transport address in the peer table, then delegates
    /// to the Router, which selects the lowest-cost available transport and
    /// opens a Session-encrypted connection for each message.
    ///
    /// Returns NoTransportAvailable if the peer is not in the peer table or if
    /// no registered transport can reach the peer.
    pub async fn send(&self, peer_id: PeerId, payload: Vec<u8>) -> Result<()> {
        let announcement = self
            .peers
            .lock()
            .unwrap()
            .get(&peer_id)
            .cloned()
            .ok_or(PathweaveError::NoTransportAvailable)?;
        self.router
            .send(&announcement, &self.identity, payload)
            .await
    }

    /// Registers a handler that will be called for each incoming message.
    ///
    /// The accept loop is spawned per transport in register_transport(). Transports
    /// that do not support incoming connections (e.g. BLE central mode) return an
    /// error from accept(), which the loop handles with a backoff.
    pub fn on_message(&self, handler: Box<dyn MessageHandler>) {
        *self.handler.lock().unwrap() = Some(handler);
    }

    /// Returns a stream of transport lifecycle events from the Router.
    pub fn events(&self) -> BoxStream<'_, TransportEvent> {
        self.router.events()
    }
}

/// Loops calling transport.accept(). Waits for the transport's start() to complete
/// before the first accept() call, eliminating the startup race. Each accepted
/// connection is handed to handle_incoming in a spawned task. On error, backs off
/// for 5 seconds to avoid busy-looping for transports that don't support inbound.
async fn accept_loop(
    transport: Arc<dyn Transport>,
    identity: NodeIdentity,
    handler: Arc<Mutex<Option<Box<dyn MessageHandler>>>>,
    dedup: Arc<Mutex<DeduplicationCache>>,
    mut started: watch::Receiver<bool>,
) {
    let _ = started.wait_for(|v| *v).await;
    loop {
        match transport.accept().await {
            Ok(conn) => {
                let identity = identity.clone();
                let handler = Arc::clone(&handler);
                let dedup = Arc::clone(&dedup);
                tokio::spawn(handle_incoming(conn, identity, handler, dedup));
            }
            Err(e) => {
                tracing::debug!(transport = transport.name(), "accept error: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}

/// Completes the Noise_XX handshake as the responder, then loops receiving
/// messages and delivering them to the handler until the connection closes.
///
/// Each payload begins with an 8-byte big-endian message ID. The ID is checked
/// against the deduplication cache: if the (peer_id, message_id) pair was seen
/// recently, on_message() is skipped. The ACK is sent regardless so the sender
/// does not time out (see ADR 009 and ADR 011).
async fn handle_incoming(
    conn: Box<dyn Connection>,
    identity: NodeIdentity,
    handler: Arc<Mutex<Option<Box<dyn MessageHandler>>>>,
    dedup: Arc<Mutex<DeduplicationCache>>,
) {
    let bundled: Box<dyn Connection> = Box::new(BundleLayer::new(conn));
    let mut session = match Session::respond(&identity, bundled).await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("incoming handshake failed: {}", e);
            return;
        }
    };
    let peer_id = session.peer_id().clone();
    while let Ok(payload) = session.recv().await {
        if payload.len() < 8 {
            tracing::debug!(peer = %peer_id, "received payload shorter than 8 bytes; skipping");
            let _ = session.send(b"").await;
            continue;
        }
        let msg_id = u64::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);
        let data = payload[8..].to_vec();

        let is_dup = dedup.lock().unwrap().check_and_insert(&peer_id, msg_id);
        if !is_dup {
            let guard = handler.lock().unwrap();
            if let Some(h) = guard.as_ref() {
                h.on_message(peer_id.clone(), data);
            }
        } else {
            tracing::debug!(peer = %peer_id, "suppressed duplicate message");
        }
        // ACK so the sender knows the data was delivered before it tears
        // down the QUIC connection (see try_send in router.rs and ADR 009).
        let _ = session.send(b"").await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Connection, NodeIdentity, PathweaveError, PeerAddress, Session, TransportCost,
        TransportKind,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::{self, BoxStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

    use crate::BundleLayer;

    // --- in-memory connection -------------------------------------------------

    struct TestConn {
        tx: UnboundedSender<Bytes>,
        rx: UnboundedReceiver<Bytes>,
    }

    fn conn_pair() -> (TestConn, TestConn) {
        let (tx1, rx1) = unbounded_channel();
        let (tx2, rx2) = unbounded_channel();
        (TestConn { tx: tx1, rx: rx2 }, TestConn { tx: tx2, rx: rx1 })
    }

    #[async_trait]
    impl Connection for TestConn {
        async fn send_bytes(&mut self, bytes: &[u8]) -> crate::Result<()> {
            self.tx
                .send(Bytes::copy_from_slice(bytes))
                .map_err(|_| PathweaveError::Transport("closed".into()))
        }

        async fn recv_bytes(&mut self) -> crate::Result<Bytes> {
            self.rx
                .recv()
                .await
                .ok_or_else(|| PathweaveError::Transport("closed".into()))
        }

        async fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }

        fn mtu(&self) -> usize {
            65535
        }
    }

    // --- mock transport for outbound-only tests --------------------------------

    struct MockTransport {
        cost: TransportCost,
        kind: TransportKind,
        responder_tx: UnboundedSender<TestConn>,
        connect_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl crate::Transport for MockTransport {
        async fn start(&self) -> Result<()> {
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            Ok(())
        }

        fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
            Box::pin(stream::empty())
        }

        async fn connect(&self, _peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
            self.connect_count.fetch_add(1, Ordering::Relaxed);
            let (a, b) = conn_pair();
            self.responder_tx.send(b).ok();
            Ok(Box::new(a))
        }

        async fn accept(&self) -> Result<Box<dyn Connection>> {
            // No inbound side; back off so the accept loop doesn't spin.
            futures::future::pending::<()>().await;
            unreachable!()
        }

        fn mtu_hint(&self) -> usize {
            65535
        }

        fn cost(&self) -> TransportCost {
            self.cost
        }

        fn kind(&self) -> TransportKind {
            self.kind
        }

        fn name(&self) -> &'static str {
            "mock"
        }
    }

    fn make_transport(
        cost: TransportCost,
        kind: TransportKind,
    ) -> (MockTransport, UnboundedReceiver<TestConn>, Arc<AtomicUsize>) {
        let (tx, rx) = unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        (
            MockTransport {
                cost,
                kind,
                responder_tx: tx,
                connect_count: Arc::clone(&count),
            },
            rx,
            count,
        )
    }

    // --- mock transport for accept-side tests ---------------------------------

    struct AcceptMockTransport {
        conn_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Box<dyn Connection>>>,
    }

    #[async_trait]
    impl crate::Transport for AcceptMockTransport {
        async fn start(&self) -> Result<()> {
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            Ok(())
        }

        fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
            Box::pin(stream::empty())
        }

        async fn connect(&self, _peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
            Err(PathweaveError::Transport("not used".into()))
        }

        async fn accept(&self) -> Result<Box<dyn Connection>> {
            self.conn_rx
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| PathweaveError::Transport("no more connections".into()))
        }

        fn mtu_hint(&self) -> usize {
            65535
        }

        fn cost(&self) -> TransportCost {
            TransportCost::Free
        }

        fn kind(&self) -> TransportKind {
            TransportKind::Ble
        }

        fn name(&self) -> &'static str {
            "accept-mock"
        }
    }

    // -------------------------------------------------------------------------

    fn dummy_peer() -> PeerAnnouncement {
        PeerAnnouncement {
            address: PeerAddress::Ble("aa:bb:cc:dd:ee:ff".into()),
            short_id: None,
        }
    }

    async fn run_responder(conn: TestConn, identity: NodeIdentity) {
        let bundled = Box::new(BundleLayer::new(Box::new(conn)));
        let mut session = Session::respond(&identity, bundled).await.unwrap();
        let _ = session.recv().await;
        let _ = session.send(b"").await;
    }

    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn send_to_unknown_peer_returns_no_transport_available() {
        let node = PathweaveNode::new(NodeConfig::default(), NodeIdentity::generate())
            .await
            .unwrap();
        let unknown_peer = NodeIdentity::generate().peer_id().clone();
        let result = node.send(unknown_peer, b"hello".to_vec()).await;
        assert!(matches!(result, Err(PathweaveError::NoTransportAvailable)));
    }

    #[tokio::test]
    async fn send_to_known_peer_routes_through_transport() {
        let (mock, mut mock_rx, connect_count) =
            make_transport(TransportCost::Free, TransportKind::Ble);

        let sender_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();
        let peer_id = responder_id.peer_id().clone();

        let mut node = PathweaveNode::new(NodeConfig::default(), sender_id)
            .await
            .unwrap();
        node.register_transport(Box::new(mock));
        node.add_peer(peer_id.clone(), dummy_peer());

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        tokio::spawn(async move {
            let conn = mock_rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        node.send(peer_id, b"hello node".to_vec()).await.unwrap();
        assert_eq!(connect_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn on_message_stores_handler() {
        struct RecordingHandler {
            called: Arc<std::sync::atomic::AtomicBool>,
        }
        impl MessageHandler for RecordingHandler {
            fn on_message(&self, _peer_id: PeerId, _payload: Vec<u8>) {
                self.called.store(true, Ordering::Relaxed);
            }
        }

        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let node = PathweaveNode::new(NodeConfig::default(), NodeIdentity::generate())
            .await
            .unwrap();
        node.on_message(Box::new(RecordingHandler {
            called: Arc::clone(&called),
        }));

        // Handler is stored; call it directly to verify it's wired.
        node.handler
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .on_message(NodeIdentity::generate().peer_id().clone(), vec![]);
        assert!(called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn events_returns_router_stream() {
        let node = PathweaveNode::new(NodeConfig::default(), NodeIdentity::generate())
            .await
            .unwrap();
        // events() must not panic and must return a stream.
        let _stream = node.events();
    }

    #[tokio::test]
    async fn incoming_message_delivered_to_handler() {
        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel::<Box<dyn Connection>>();
        let transport = AcceptMockTransport {
            conn_rx: tokio::sync::Mutex::new(conn_rx),
        };

        let receiver_id = NodeIdentity::generate();
        let sender_id = NodeIdentity::generate();

        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        struct RecordingHandler {
            done_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<Vec<u8>>>>>,
        }
        impl MessageHandler for RecordingHandler {
            fn on_message(&self, _peer_id: PeerId, payload: Vec<u8>) {
                if let Some(tx) = self.done_tx.lock().unwrap().take() {
                    let _ = tx.send(payload);
                }
            }
        }

        let mut node = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node.register_transport(Box::new(transport));
        node.on_message(Box::new(RecordingHandler {
            done_tx: Arc::clone(&done_tx),
        }));

        // Yield so the accept loop starts and blocks on accept().
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Inject an inbound connection and run the initiator concurrently.
        let (client_conn, server_conn) = conn_pair();
        conn_tx
            .send(Box::new(server_conn))
            .expect("transport still alive");

        tokio::spawn(async move {
            let bundled: Box<dyn Connection> = Box::new(BundleLayer::new(Box::new(client_conn)));
            let mut session = Session::initiate(&sender_id, bundled).await.unwrap();
            // Prepend the 8-byte message ID as handle_incoming now expects (ADR 011).
            let msg_id: u64 = 0x0102030405060708;
            let mut framed = Vec::with_capacity(8 + b"hello from peer".len());
            framed.extend_from_slice(&msg_id.to_be_bytes());
            framed.extend_from_slice(b"hello from peer");
            session.send(&framed).await.unwrap();
        });

        let payload = tokio::time::timeout(tokio::time::Duration::from_secs(5), done_rx)
            .await
            .expect("timed out waiting for message")
            .unwrap();

        assert_eq!(payload, b"hello from peer");
    }

    #[tokio::test]
    async fn connect_learns_and_stores_peer_id() {
        let (mock, mut mock_rx, _) = make_transport(TransportCost::Free, TransportKind::Ble);

        let initiator_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();
        let expected_peer_id = responder_id.peer_id().clone();

        let mut node = PathweaveNode::new(NodeConfig::default(), initiator_id)
            .await
            .unwrap();
        node.register_transport(Box::new(mock));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        tokio::spawn(async move {
            let conn = mock_rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        let peer_id = node.connect(dummy_peer()).await.unwrap();
        assert_eq!(peer_id, expected_peer_id);
        // Verify the mapping was stored so send() can now route to this peer.
        assert!(node.peers.lock().unwrap().contains_key(&peer_id));
    }

    #[tokio::test]
    async fn connect_no_transport_returns_error() {
        let mut node = PathweaveNode::new(NodeConfig::default(), NodeIdentity::generate())
            .await
            .unwrap();
        let result = node
            .connect(PeerAnnouncement {
                address: PeerAddress::Quic("127.0.0.1:1234".parse().unwrap()),
                short_id: None,
            })
            .await;
        assert!(matches!(result, Err(PathweaveError::NoTransportAvailable)));
    }

    // --- DeduplicationCache unit tests ---------------------------------------

    #[test]
    fn dedup_cache_first_insert_not_duplicate() {
        let mut cache = DeduplicationCache::new();
        let peer = NodeIdentity::generate().peer_id().clone();
        assert!(!cache.check_and_insert(&peer, 42));
    }

    #[test]
    fn dedup_cache_second_insert_same_key_is_duplicate() {
        let mut cache = DeduplicationCache::new();
        let peer = NodeIdentity::generate().peer_id().clone();
        cache.check_and_insert(&peer, 42);
        assert!(cache.check_and_insert(&peer, 42));
    }

    #[test]
    fn dedup_cache_different_message_id_not_duplicate() {
        let mut cache = DeduplicationCache::new();
        let peer = NodeIdentity::generate().peer_id().clone();
        cache.check_and_insert(&peer, 1);
        assert!(!cache.check_and_insert(&peer, 2));
    }

    #[test]
    fn dedup_cache_different_peer_same_id_not_duplicate() {
        let mut cache = DeduplicationCache::new();
        let peer_a = NodeIdentity::generate().peer_id().clone();
        let peer_b = NodeIdentity::generate().peer_id().clone();
        cache.check_and_insert(&peer_a, 42);
        assert!(!cache.check_and_insert(&peer_b, 42));
    }

    #[test]
    fn dedup_cache_entry_redeliverable_after_ttl_expires() {
        let mut cache = DeduplicationCache::with_ttl(Duration::from_millis(10));
        let peer = NodeIdentity::generate().peer_id().clone();
        cache.check_and_insert(&peer, 99);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!cache.check_and_insert(&peer, 99));
    }

    // --- duplicate suppression integration test ------------------------------

    #[tokio::test]
    async fn duplicate_message_delivered_only_once() {
        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel::<Box<dyn Connection>>();
        let transport = AcceptMockTransport {
            conn_rx: tokio::sync::Mutex::new(conn_rx),
        };

        let receiver_id = NodeIdentity::generate();
        let sender_id = NodeIdentity::generate();

        let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        struct CountingHandler {
            count: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl MessageHandler for CountingHandler {
            fn on_message(&self, _peer_id: PeerId, _payload: Vec<u8>) {
                self.count.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut node = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node.register_transport(Box::new(transport));
        node.on_message(Box::new(CountingHandler {
            count: Arc::clone(&call_count),
        }));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let msg_id: u64 = 0xDEADBEEFCAFEBABE;
        let mut framed = Vec::with_capacity(8 + b"hello".len());
        framed.extend_from_slice(&msg_id.to_be_bytes());
        framed.extend_from_slice(b"hello");

        // Send two connections from the same sender with the same message ID.
        for _ in 0..2 {
            let (client_conn, server_conn) = conn_pair();
            conn_tx
                .send(Box::new(server_conn))
                .expect("transport still alive");
            let sender_id = sender_id.clone();
            let framed = framed.clone();
            tokio::spawn(async move {
                let bundled: Box<dyn Connection> =
                    Box::new(BundleLayer::new(Box::new(client_conn)));
                let mut session = Session::initiate(&sender_id, bundled).await.unwrap();
                session.send(&framed).await.unwrap();
                // Wait for the ACK so handle_incoming has time to process.
                let _ =
                    tokio::time::timeout(tokio::time::Duration::from_secs(2), session.recv()).await;
            });
        }

        // Give both connections time to be fully processed.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(
            call_count.load(Ordering::Relaxed),
            1,
            "on_message must be called exactly once for a duplicated message"
        );
    }
}
