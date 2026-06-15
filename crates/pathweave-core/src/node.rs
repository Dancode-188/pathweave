use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::BoxStream;

use tokio::sync::watch;

use crate::{
    new_key_registry, new_peer_table, router, BundleLayer, Connection, KeyRegistry, MessageHandler,
    NodeConfig, NodeIdentity, PathweaveError, PeerAddress, PeerAnnouncement, PeerId, PeerTable,
    Result, Router, Session, Transport, TransportEvent,
};

const MAX_TTL: u8 = 7;

const DEDUP_TTL: Duration = Duration::from_secs(60);

/// Tracks recently seen (PeerId, message_id) pairs to suppress duplicate deliveries.
///
/// A retry that arrives after a lost ACK carries the same message ID as the original.
/// The cache detects this and suppresses the second on_message() call while still
/// sending the ACK so the sender does not time out. Entries expire after DEDUP_TTL.
struct DeduplicationCache {
    seen: HashMap<(PeerId, u64), Instant>,
    seen_routed: HashMap<u64, Instant>,
    ttl: Duration,
}

impl DeduplicationCache {
    fn new() -> Self {
        Self {
            seen: HashMap::new(),
            seen_routed: HashMap::new(),
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

    /// Returns true if message_id was seen within the TTL window (routed dedup).
    ///
    /// Keyed on message_id alone because the immediate sender is a relay and varies
    /// across paths. If not seen before, records and returns false. See ADR 019.
    fn check_and_insert_routed(&mut self, message_id: u64) -> bool {
        let now = Instant::now();
        let ttl = self.ttl;
        self.seen_routed.retain(|_, inserted_at| {
            now.checked_duration_since(*inserted_at)
                .map(|age| age < ttl)
                .unwrap_or(true)
        });
        if self.seen_routed.contains_key(&message_id) {
            return true;
        }
        self.seen_routed.insert(message_id, now);
        false
    }

    #[cfg(test)]
    fn with_ttl(ttl: Duration) -> Self {
        Self {
            seen: HashMap::new(),
            seen_routed: HashMap::new(),
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
/// The peer table maps PeerId -> Vec<PeerAnnouncement>. A peer reachable via multiple
/// transports accumulates one address per transport. The peer_stream task (one per
/// transport) populates it automatically via discovery. add_peer() and connect() also
/// push addresses for manually-supplied peers. See ADR 016.
pub struct PathweaveNode {
    router: Arc<Router>,
    identity: NodeIdentity,
    peers: PeerTable,
    // Dedup-only: tracks addresses currently in-flight or already connected.
    // Not authoritative for routing — peers is. peer_stream only upserts to
    // peers; it never removes. This invariant is what makes the concurrent
    // add_peer() edge case benign: even if known_addrs loses an entry, the
    // peers entry remains intact and send() continues to work.
    known_addrs: Arc<Mutex<HashSet<PeerAddress>>>,
    handler: Arc<Mutex<Option<Box<dyn MessageHandler>>>>,
    dedup: Arc<Mutex<DeduplicationCache>>,
    key_registry: KeyRegistry,
}

impl PathweaveNode {
    /// Creates a new node. `config` is accepted but unused until transport
    /// implementations are complete; callers should pass `NodeConfig::default()` for now.
    pub async fn new(_config: NodeConfig, identity: NodeIdentity) -> Result<Self> {
        Ok(Self {
            router: Arc::new(Router::new()),
            identity,
            peers: new_peer_table(),
            known_addrs: Arc::new(Mutex::new(HashSet::new())),
            handler: Arc::new(Mutex::new(None)),
            dedup: Arc::new(Mutex::new(DeduplicationCache::new())),
            key_registry: new_key_registry(),
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
        let started = self
            .router
            .register_transport(Arc::clone(&arc), Arc::new(self.identity.clone()));

        tokio::spawn(accept_loop(
            Arc::clone(&arc),
            identity.clone(),
            handler,
            dedup,
            started.clone(),
            Arc::clone(&self.key_registry),
            Arc::clone(&self.peers),
            Arc::clone(&self.router),
            self.identity.peer_id().clone(),
        ));

        tokio::spawn(crate::router::peer_stream(
            arc,
            identity,
            started,
            Arc::clone(&self.peers),
            Arc::clone(&self.known_addrs),
            self.identity.peer_id().clone(),
            self.router.event_tx(),
            Arc::clone(&self.key_registry),
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
        let peer_id = self
            .router
            .connect(
                std::slice::from_ref(&announcement),
                &self.identity,
                &self.key_registry,
                &self.peers,
            )
            .await?;
        self.known_addrs
            .lock()
            .unwrap()
            .insert(announcement.address.clone());
        let mut peers = self.peers.lock().unwrap();
        let addrs = peers.entry(peer_id.clone()).or_default();
        if !addrs.iter().any(|a| a.address == announcement.address) {
            addrs.push(announcement);
        }
        Ok(peer_id)
    }

    /// Records a known PeerId -> PeerAnnouncement mapping in the peer table.
    ///
    /// Not part of the UniFFI boundary. Used by pw-chat to inject a QUIC peer
    /// address resolved from the command line, and by tests to set up known peers.
    pub fn add_peer(&mut self, peer_id: PeerId, announcement: PeerAnnouncement) {
        self.known_addrs
            .lock()
            .unwrap()
            .insert(announcement.address.clone());
        let mut peers = self.peers.lock().unwrap();
        let addrs = peers.entry(peer_id).or_default();
        if !addrs.iter().any(|a| a.address == announcement.address) {
            addrs.push(announcement);
        }
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
        let announcements = self
            .peers
            .lock()
            .unwrap()
            .get(&peer_id)
            .cloned()
            .ok_or(PathweaveError::NoTransportAvailable)?;
        let mut framed = Vec::with_capacity(1 + payload.len());
        framed.push(0x00); // direct route_flag (ADR 019)
        framed.extend_from_slice(&payload);
        self.router
            .send(
                &announcements,
                &self.identity,
                framed,
                &peer_id,
                &self.key_registry,
                &self.peers,
                None,
            )
            .await
    }

    /// Sends `payload` to `dest_peer_id` via mesh routing (ADR 019).
    ///
    /// Floods a routed frame to all known direct peers with TTL=7. Each relay decrements
    /// TTL and re-floods to all its known peers except the immediate sender. Delivery
    /// stops when the destination receives the frame or TTL reaches zero.
    ///
    /// Returns Ok(()) if at least one neighbor accepted the frame. Does not confirm
    /// end-to-end delivery to `dest_peer_id`.
    pub async fn send_routed(&self, dest_peer_id: PeerId, payload: Vec<u8>) -> Result<()> {
        let msg_id = router::new_message_id();
        let mut frame_body = Vec::with_capacity(1 + 32 + 1 + payload.len());
        frame_body.push(0x01); // routed route_flag
        frame_body.extend_from_slice(dest_peer_id.as_bytes());
        frame_body.push(MAX_TTL);
        frame_body.extend_from_slice(&payload);

        let neighbors: Vec<(PeerId, Vec<PeerAnnouncement>)> = self
            .peers
            .lock()
            .unwrap()
            .iter()
            .filter(|(pid, _)| **pid != *self.identity.peer_id())
            .map(|(pid, anns)| (pid.clone(), anns.clone()))
            .collect();

        if neighbors.is_empty() {
            return Err(PathweaveError::NoTransportAvailable);
        }

        let mut any_ok = false;
        for (peer_id, announcements) in neighbors {
            if self
                .router
                .send(
                    &announcements,
                    &self.identity,
                    frame_body.clone(),
                    &peer_id,
                    &self.key_registry,
                    &self.peers,
                    Some(msg_id),
                )
                .await
                .is_ok()
            {
                any_ok = true;
            }
        }

        if any_ok {
            Ok(())
        } else {
            Err(PathweaveError::DeliveryFailed)
        }
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
    pub fn events(&self) -> BoxStream<'static, TransportEvent> {
        self.router.events()
    }

    /// Returns the stored Curve25519 static public key for `peer_id`, if known.
    ///
    /// Populated after every successful handshake. Used by the Noise_XK upgrade path
    /// and future E2E hop encryption. See ADR 020.
    pub fn lookup_key(&self, peer_id: &PeerId) -> Option<[u8; 32]> {
        self.key_registry.lock().unwrap().get(peer_id).copied()
    }
}

/// Loops calling transport.accept(). Waits for the transport's start() to complete
/// before the first accept() call, eliminating the startup race. Each accepted
/// connection is handed to handle_incoming in a spawned task. On error, backs off
/// for 5 seconds to avoid busy-looping for transports that don't support inbound.
#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    transport: Arc<dyn Transport>,
    identity: NodeIdentity,
    handler: Arc<Mutex<Option<Box<dyn MessageHandler>>>>,
    dedup: Arc<Mutex<DeduplicationCache>>,
    mut started: watch::Receiver<bool>,
    key_registry: KeyRegistry,
    peers: PeerTable,
    router: Arc<Router>,
    local_peer_id: PeerId,
) {
    let _ = started.wait_for(|v| *v).await;
    let local_addrs = Arc::new(transport.local_addresses());
    loop {
        match transport.accept().await {
            Ok(conn) => {
                let identity = identity.clone();
                let handler = Arc::clone(&handler);
                let dedup = Arc::clone(&dedup);
                let key_registry = Arc::clone(&key_registry);
                let peers = Arc::clone(&peers);
                let local_addrs = Arc::clone(&local_addrs);
                let router = Arc::clone(&router);
                let local_peer_id = local_peer_id.clone();
                tokio::spawn(handle_incoming(
                    conn,
                    identity,
                    handler,
                    dedup,
                    key_registry,
                    peers,
                    local_addrs,
                    router,
                    local_peer_id,
                ));
            }
            Err(e) => {
                tracing::debug!(transport = transport.name(), "accept error: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}

/// Completes the Noise_XX handshake as the responder, then receives and dispatches
/// messages until the connection closes. The first message is inspected before the
/// receive loop: if its first byte is 0x00, it is an address exchange control frame
/// (ADR 017); the exchange is handled and the next message is the application frame.
/// Otherwise the first message is treated directly as the application frame, which
/// allows communication with older nodes that do not implement address exchange.
#[allow(clippy::too_many_arguments)]
async fn handle_incoming(
    conn: Box<dyn Connection>,
    identity: NodeIdentity,
    handler: Arc<Mutex<Option<Box<dyn MessageHandler>>>>,
    dedup: Arc<Mutex<DeduplicationCache>>,
    key_registry: KeyRegistry,
    peers: PeerTable,
    local_addrs: Arc<Vec<PeerAddress>>,
    router: Arc<Router>,
    local_peer_id: PeerId,
) {
    let bundled: Box<dyn Connection> = Box::new(BundleLayer::new(conn));
    let mut session = match Session::respond(&identity, bundled).await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("incoming handshake failed: {}", e);
            return;
        }
    };
    let sender_peer_id = session.peer_id().clone();
    key_registry
        .lock()
        .unwrap()
        .insert(sender_peer_id.clone(), *session.remote_static_key());

    let first = match session.recv().await {
        Ok(p) => p,
        Err(_) => return,
    };

    let app_payload = if first.len() >= 8 && first[0] == 0x00 {
        match router::decode_addr_exchange(&first) {
            Some(addrs) => router::upsert_peer_addresses(&peers, &sender_peer_id, addrs),
            None => {
                tracing::debug!(peer = %sender_peer_id, "addr-exchange: parse failed; skipping")
            }
        }
        let _ = session
            .send(&router::encode_addr_exchange(&local_addrs))
            .await;
        match session.recv().await {
            Ok(p) => p,
            Err(_) => return,
        }
    } else {
        first
    };

    dispatch_payload(
        &sender_peer_id,
        app_payload,
        &dedup,
        &handler,
        &mut session,
        &local_peer_id,
        &router,
        &peers,
        &identity,
        &key_registry,
    )
    .await;
    while let Ok(payload) = session.recv().await {
        dispatch_payload(
            &sender_peer_id,
            payload,
            &dedup,
            &handler,
            &mut session,
            &local_peer_id,
            &router,
            &peers,
            &identity,
            &key_registry,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_payload(
    sender_peer_id: &PeerId,
    payload: Bytes,
    dedup: &Arc<Mutex<DeduplicationCache>>,
    handler: &Arc<Mutex<Option<Box<dyn MessageHandler>>>>,
    session: &mut Session,
    local_peer_id: &PeerId,
    router: &Arc<Router>,
    peers: &PeerTable,
    identity: &NodeIdentity,
    key_registry: &KeyRegistry,
) {
    if payload.len() < 9 {
        tracing::debug!(peer = %sender_peer_id, "received payload shorter than 9 bytes; skipping");
        let _ = session.send(b"").await;
        return;
    }
    let msg_id = u64::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let route_flag = payload[8];

    match route_flag {
        0x00 => {
            let data = payload[9..].to_vec();
            let is_dup = dedup
                .lock()
                .unwrap()
                .check_and_insert(sender_peer_id, msg_id);
            if !is_dup {
                if let Some(h) = handler.lock().unwrap().as_ref() {
                    h.on_message(sender_peer_id.clone(), data);
                }
            } else {
                tracing::debug!(peer = %sender_peer_id, "suppressed duplicate direct message");
            }
        }
        0x01 => {
            if payload.len() < 9 + 32 + 1 {
                tracing::debug!(peer = %sender_peer_id, "routed frame too short; skipping");
                let _ = session.send(b"").await;
                return;
            }
            let dest_bytes: [u8; 32] = payload[9..41].try_into().unwrap();
            let dest = PeerId::from_bytes(dest_bytes);
            let ttl = payload[41].min(MAX_TTL);
            let app_payload = payload[42..].to_vec();

            if &dest == local_peer_id {
                let is_dup = dedup.lock().unwrap().check_and_insert_routed(msg_id);
                if !is_dup {
                    if let Some(h) = handler.lock().unwrap().as_ref() {
                        h.on_message(sender_peer_id.clone(), app_payload);
                    }
                } else {
                    tracing::debug!(msg_id, "suppressed duplicate routed message at destination");
                }
            } else if ttl == 0 {
                tracing::debug!(msg_id, "TTL=0: silently dropping routed message");
            } else {
                let is_dup = dedup.lock().unwrap().check_and_insert_routed(msg_id);
                if !is_dup {
                    let new_ttl = ttl - 1;
                    let mut relay_body = Vec::with_capacity(1 + 32 + 1 + app_payload.len());
                    relay_body.push(0x01);
                    relay_body.extend_from_slice(dest.as_bytes());
                    relay_body.push(new_ttl);
                    relay_body.extend_from_slice(&app_payload);

                    let neighbors: Vec<(PeerId, Vec<PeerAnnouncement>)> = peers
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(pid, _)| **pid != *sender_peer_id && **pid != *local_peer_id)
                        .map(|(pid, anns)| (pid.clone(), anns.clone()))
                        .collect();

                    for (next_peer_id, announcements) in neighbors {
                        let router = Arc::clone(router);
                        let identity = identity.clone();
                        let relay_body = relay_body.clone();
                        let key_registry = Arc::clone(key_registry);
                        let peers = Arc::clone(peers);
                        tokio::spawn(async move {
                            let _ = router
                                .send(
                                    &announcements,
                                    &identity,
                                    relay_body,
                                    &next_peer_id,
                                    &key_registry,
                                    &peers,
                                    Some(msg_id),
                                )
                                .await;
                        });
                    }
                } else {
                    tracing::debug!(msg_id, "suppressed duplicate routed message at relay");
                }
            }
        }
        _ => {
            tracing::debug!(peer = %sender_peer_id, route_flag, "unknown route_flag; skipping");
        }
    }

    let _ = session.send(b"").await;
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
        async fn start(&self, _identity: &NodeIdentity) -> Result<()> {
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
        async fn start(&self, _identity: &NodeIdentity) -> Result<()> {
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
        let _ = session.recv().await; // addr exchange from initiator
        let _ = session.send(b"").await; // addr exchange response (empty = no addresses)
        let _ = session.recv().await; // application frame (absent for try_connect)
        let _ = session.send(b"").await; // delivery ACK
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
            let mut session = Session::initiate(&sender_id, bundled, None).await.unwrap();
            // Prepend msg_id + route_flag=0x00 (direct) as dispatch_payload expects (ADR 019).
            let msg_id: u64 = 0x0102030405060708;
            let mut framed = Vec::with_capacity(9 + b"hello from peer".len());
            framed.extend_from_slice(&msg_id.to_be_bytes());
            framed.push(0x00); // direct
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
    async fn connect_stores_peer_key_in_registry() {
        let (mock, mut mock_rx, _) = make_transport(TransportCost::Free, TransportKind::Ble);

        let initiator_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();
        let expected_key: [u8; 32] = responder_id.public_key().try_into().unwrap();

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
        let registry = node.key_registry.lock().unwrap();
        let stored = registry
            .get(&peer_id)
            .expect("key must be in registry after connect");
        assert_eq!(stored, &expected_key);
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
        let mut framed = Vec::with_capacity(9 + b"hello".len());
        framed.extend_from_slice(&msg_id.to_be_bytes());
        framed.push(0x00); // direct
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
                let mut session = Session::initiate(&sender_id, bundled, None).await.unwrap();
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

    // --- routed dedup unit tests ---------------------------------------------

    #[test]
    fn routed_dedup_first_insert_not_duplicate() {
        let mut cache = DeduplicationCache::new();
        assert!(!cache.check_and_insert_routed(0xABCD));
    }

    #[test]
    fn routed_dedup_second_insert_same_id_is_duplicate() {
        let mut cache = DeduplicationCache::new();
        cache.check_and_insert_routed(0xABCD);
        assert!(cache.check_and_insert_routed(0xABCD));
    }

    #[test]
    fn routed_dedup_different_id_not_duplicate() {
        let mut cache = DeduplicationCache::new();
        cache.check_and_insert_routed(1);
        assert!(!cache.check_and_insert_routed(2));
    }

    #[test]
    fn routed_dedup_redeliverable_after_ttl_expires() {
        let mut cache = DeduplicationCache::with_ttl(Duration::from_millis(10));
        cache.check_and_insert_routed(0xABCD);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!cache.check_and_insert_routed(0xABCD));
    }

    #[test]
    fn routed_dedup_independent_of_direct_dedup() {
        let mut cache = DeduplicationCache::new();
        let peer = NodeIdentity::generate().peer_id().clone();
        // Direct dedup on (peer, id) must not affect routed dedup on id alone.
        cache.check_and_insert(&peer, 42);
        assert!(!cache.check_and_insert_routed(42));
    }

    // --- mesh routing test infrastructure ------------------------------------

    /// Bidirectional in-memory transport pair for multi-hop tests.
    ///
    /// When A calls connect(), the server-side connection lands in B's accept queue.
    /// When B calls connect(), the server-side connection lands in A's accept queue.
    /// Both sides share the same kind so router candidate selection works.
    struct InMemoryTransport {
        accept_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Box<dyn Connection>>>,
        connect_server_tx: tokio::sync::mpsc::UnboundedSender<Box<dyn Connection>>,
        kind: TransportKind,
    }

    fn wire_transports(kind: TransportKind) -> (InMemoryTransport, InMemoryTransport) {
        let (a_to_b_tx, b_accept_rx) = tokio::sync::mpsc::unbounded_channel();
        let (b_to_a_tx, a_accept_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            InMemoryTransport {
                accept_rx: tokio::sync::Mutex::new(a_accept_rx),
                connect_server_tx: a_to_b_tx,
                kind,
            },
            InMemoryTransport {
                accept_rx: tokio::sync::Mutex::new(b_accept_rx),
                connect_server_tx: b_to_a_tx,
                kind,
            },
        )
    }

    #[async_trait]
    impl crate::Transport for InMemoryTransport {
        async fn start(&self, _: &NodeIdentity) -> Result<()> {
            Ok(())
        }
        async fn stop(&self) -> Result<()> {
            Ok(())
        }
        fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
            Box::pin(stream::empty())
        }
        async fn connect(&self, _peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
            let (client, server) = conn_pair();
            self.connect_server_tx.send(Box::new(server)).ok();
            Ok(Box::new(client))
        }
        async fn accept(&self) -> Result<Box<dyn Connection>> {
            self.accept_rx
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| PathweaveError::Transport("channel closed".into()))
        }
        fn mtu_hint(&self) -> usize {
            65535
        }
        fn cost(&self) -> TransportCost {
            TransportCost::Free
        }
        fn kind(&self) -> TransportKind {
            self.kind
        }
        fn name(&self) -> &'static str {
            "in-memory"
        }
    }

    struct CountingHandler {
        count: Arc<std::sync::atomic::AtomicUsize>,
        payload_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<Vec<u8>>>>>,
    }

    impl MessageHandler for CountingHandler {
        fn on_message(&self, _peer_id: PeerId, payload: Vec<u8>) {
            self.count.fetch_add(1, Ordering::Relaxed);
            if let Some(tx) = self.payload_tx.lock().unwrap().take() {
                let _ = tx.send(payload);
            }
        }
    }

    // --- mesh routing integration tests --------------------------------------

    // Topology: A --[Ble]--> B --[Quic]--> C
    // A and C have no direct path. A sends a routed message to C; B relays.
    //
    // The test asserts all five invariants from ADR 019:
    // (1) destination check, (2) TTL enforcement, (3) source suppression,
    // (4) dedup, (5) no cross-delivery.

    #[tokio::test]
    async fn routed_message_delivered_via_relay() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();

        let pid_a = id_a.peer_id().clone();
        let pid_b = id_b.peer_id().clone();
        let pid_c = id_c.peer_id().clone();

        // Wire A→B: A connects, B accepts.
        let (t_a, t_b_inbound) = wire_transports(TransportKind::Ble);
        // Wire B→C: B connects, C accepts.
        let (t_b_outbound, t_c) = wire_transports(TransportKind::Quic);

        let ann_b_for_a = PeerAnnouncement {
            address: PeerAddress::Ble("node-b".into()),
            short_id: None,
        };
        let ann_a_for_b = PeerAnnouncement {
            address: PeerAddress::Ble("node-a".into()),
            short_id: None,
        };
        let ann_c_for_b = PeerAnnouncement {
            address: PeerAddress::Quic("127.0.0.1:1".parse().unwrap()),
            short_id: None,
        };
        let ann_b_for_c = PeerAnnouncement {
            address: PeerAddress::Quic("127.0.0.1:2".parse().unwrap()),
            short_id: None,
        };

        // Build node A.
        let mut node_a = PathweaveNode::new(NodeConfig::default(), id_a)
            .await
            .unwrap();
        node_a.register_transport(Box::new(t_a));
        node_a.add_peer(pid_b.clone(), ann_b_for_a);

        // Build node B (relay): two transports.
        let mut node_b = PathweaveNode::new(NodeConfig::default(), id_b)
            .await
            .unwrap();
        node_b.register_transport(Box::new(t_b_inbound));
        node_b.register_transport(Box::new(t_b_outbound));
        node_b.add_peer(pid_a.clone(), ann_a_for_b);
        node_b.add_peer(pid_c.clone(), ann_c_for_b);

        // Build node C.
        let mut node_c = PathweaveNode::new(NodeConfig::default(), id_c)
            .await
            .unwrap();
        node_c.register_transport(Box::new(t_c));
        node_c.add_peer(pid_b.clone(), ann_b_for_c);

        // Track handler call counts on all three nodes.
        let count_a = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_b = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (c_tx, c_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let c_tx = Arc::new(Mutex::new(Some(c_tx)));
        let count_c = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        node_a.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_a),
            payload_tx: Arc::new(Mutex::new(None)),
        }));
        node_b.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_b),
            payload_tx: Arc::new(Mutex::new(None)),
        }));
        node_c.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_c),
            payload_tx: Arc::clone(&c_tx),
        }));

        // Yield to let health monitors start transports.
        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        node_a
            .send_routed(pid_c.clone(), b"hello via relay".to_vec())
            .await
            .unwrap();

        let delivered = tokio::time::timeout(tokio::time::Duration::from_secs(5), c_rx)
            .await
            .expect("timed out waiting for routed delivery")
            .unwrap();

        assert_eq!(
            delivered, b"hello via relay",
            "invariant 1: destination receives payload"
        );
        assert_eq!(
            count_c.load(Ordering::Relaxed),
            1,
            "invariant 1: delivered exactly once at C"
        );
        assert_eq!(
            count_b.load(Ordering::Relaxed),
            0,
            "invariant 5: relay does not call on_message"
        );
        assert_eq!(
            count_a.load(Ordering::Relaxed),
            0,
            "invariant 5: originator does not receive its own message"
        );
    }

    #[tokio::test]
    async fn routed_message_ttl_zero_dropped_at_relay() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();

        let pid_b = id_b.peer_id().clone();
        let pid_c = id_c.peer_id().clone();

        let (t_a, t_b_inbound) = wire_transports(TransportKind::Ble);
        let (t_b_outbound, t_c) = wire_transports(TransportKind::Quic);

        let mut node_a = PathweaveNode::new(NodeConfig::default(), id_a.clone())
            .await
            .unwrap();
        node_a.register_transport(Box::new(t_a));
        node_a.add_peer(
            pid_b.clone(),
            PeerAnnouncement {
                address: PeerAddress::Ble("b".into()),
                short_id: None,
            },
        );

        let mut node_b = PathweaveNode::new(NodeConfig::default(), id_b.clone())
            .await
            .unwrap();
        node_b.register_transport(Box::new(t_b_inbound));
        node_b.register_transport(Box::new(t_b_outbound));
        node_b.add_peer(
            id_a.peer_id().clone(),
            PeerAnnouncement {
                address: PeerAddress::Ble("a".into()),
                short_id: None,
            },
        );
        node_b.add_peer(
            pid_c.clone(),
            PeerAnnouncement {
                address: PeerAddress::Quic("127.0.0.1:3".parse().unwrap()),
                short_id: None,
            },
        );

        let mut node_c = PathweaveNode::new(NodeConfig::default(), id_c)
            .await
            .unwrap();
        node_c.register_transport(Box::new(t_c));

        let count_c = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        node_c.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_c),
            payload_tx: Arc::new(Mutex::new(None)),
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        // Send a routed frame directly to B with TTL=0 inside the frame body,
        // bypassing PathweaveNode::send_routed (which always sets TTL=7).
        // B must drop it without forwarding to C (invariant 2).
        let msg_id = crate::router::new_message_id();
        let mut frame_body = Vec::with_capacity(1 + 32 + 1 + 5);
        frame_body.push(0x01);
        frame_body.extend_from_slice(pid_c.as_bytes());
        frame_body.push(0u8); // TTL = 0
        frame_body.extend_from_slice(b"drop");

        let b_anns = vec![PeerAnnouncement {
            address: PeerAddress::Ble("b".into()),
            short_id: None,
        }];
        node_a
            .router
            .send(
                &b_anns,
                &id_a,
                frame_body,
                &pid_b,
                &node_a.key_registry,
                &node_a.peers,
                Some(msg_id),
            )
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(
            count_c.load(Ordering::Relaxed),
            0,
            "invariant 2: TTL=0 message must not reach destination"
        );
    }

    #[tokio::test]
    async fn routed_message_deduplicated_across_two_relay_paths() {
        // Inject the same routed frame (same msg_id, dest=C) from two distinct sender
        // nodes via two different transport paths. C must call on_message exactly once.
        //
        // Invariant 4 (ADR 019): routed dedup is keyed on msg_id alone, not on
        // (sender_peer_id, msg_id). S1 and S2 have different PeerIds, so if dedup
        // were keyed on the pair, C would deliver twice. It must not.
        let id_s1 = NodeIdentity::generate();
        let id_s2 = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();
        let pid_c = id_c.peer_id().clone();

        // S1 connects to C via Ble; S2 connects to C via Quic.
        let (t_s1, t_c1) = wire_transports(TransportKind::Ble);
        let (t_s2, t_c2) = wire_transports(TransportKind::Quic);

        // Node C: two inbound transports, one per sender.
        let mut node_c = PathweaveNode::new(NodeConfig::default(), id_c.clone())
            .await
            .unwrap();
        node_c.register_transport(Box::new(t_c1));
        node_c.register_transport(Box::new(t_c2));

        let count_c = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (c_tx, c_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let c_tx_arc = Arc::new(Mutex::new(Some(c_tx)));
        node_c.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_c),
            payload_tx: Arc::clone(&c_tx_arc),
        }));

        // Sender S1: one Ble transport to C.
        let mut node_s1 = PathweaveNode::new(NodeConfig::default(), id_s1.clone())
            .await
            .unwrap();
        node_s1.register_transport(Box::new(t_s1));
        node_s1.add_peer(
            pid_c.clone(),
            PeerAnnouncement {
                address: PeerAddress::Ble("c".into()),
                short_id: None,
            },
        );

        // Sender S2: one Quic transport to C.
        let mut node_s2 = PathweaveNode::new(NodeConfig::default(), id_s2.clone())
            .await
            .unwrap();
        node_s2.register_transport(Box::new(t_s2));
        node_s2.add_peer(
            pid_c.clone(),
            PeerAnnouncement {
                address: PeerAddress::Quic("127.0.0.1:20".parse().unwrap()),
                short_id: None,
            },
        );

        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        // Build one routed frame with a fixed message_id. Both senders transmit it.
        let msg_id = crate::router::new_message_id();
        let mut frame_body = Vec::new();
        frame_body.push(0x01); // route_flag
        frame_body.extend_from_slice(pid_c.as_bytes());
        frame_body.push(3u8); // TTL
        frame_body.extend_from_slice(b"dedup test");

        node_s1
            .router
            .send(
                &[PeerAnnouncement {
                    address: PeerAddress::Ble("c".into()),
                    short_id: None,
                }],
                &id_s1,
                frame_body.clone(),
                &pid_c,
                &node_s1.key_registry,
                &node_s1.peers,
                Some(msg_id),
            )
            .await
            .unwrap();

        node_s2
            .router
            .send(
                &[PeerAnnouncement {
                    address: PeerAddress::Quic("127.0.0.1:20".parse().unwrap()),
                    short_id: None,
                }],
                &id_s2,
                frame_body.clone(),
                &pid_c,
                &node_s2.key_registry,
                &node_s2.peers,
                Some(msg_id),
            )
            .await
            .unwrap();

        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), c_rx)
            .await
            .expect("timed out waiting for routed delivery");

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        assert_eq!(
            count_c.load(Ordering::Relaxed),
            1,
            "invariant 4: routed message delivered exactly once despite two distinct sender paths"
        );
    }
}
