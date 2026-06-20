use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::BoxStream;

use tokio::sync::{broadcast, watch};

use crate::{
    new_key_registry_with_bound, new_peer_table, router, BundleLayer, Connection, KeyRegistry,
    MessageHandler, NodeConfig, NodeIdentity, PathweaveError, PeerAddress, PeerAnnouncement,
    PeerId, PeerTable, Result, Router, Session, Transport, TransportEvent,
};

pub(crate) const MAX_TTL: u8 = 7;

const DEDUP_TTL: Duration = Duration::from_secs(60);

const STORE_TTL_DEFAULT: Duration = Duration::from_secs(86_400); // 24 hours

/// (msg_id, app_payload, queued_at). msg_id is assigned at enqueue and reused on every
/// drain attempt so the receiver's DeduplicationCache can suppress retried deliveries.
/// See ADR 021.
type PendingStore = Arc<Mutex<HashMap<PeerId, VecDeque<(u64, Vec<u8>, Instant)>>>>;

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
    store_ttl: Duration,
    max_queue_depth: Option<usize>,
    pending_direct: PendingStore,
    pending_routed: PendingStore,
}

impl PathweaveNode {
    pub async fn new(config: NodeConfig, identity: NodeIdentity) -> Result<Self> {
        let store_ttl = config.store_ttl.unwrap_or(STORE_TTL_DEFAULT);
        let max_queue_depth = config.max_queue_depth;
        let router = Arc::new(Router::new());
        let peers = new_peer_table();
        let key_registry = new_key_registry_with_bound(config.max_key_registry_size);
        let pending_direct: PendingStore = Arc::new(Mutex::new(HashMap::new()));
        let pending_routed: PendingStore = Arc::new(Mutex::new(HashMap::new()));

        // Drain queued payloads whenever a PeerConnected event arrives from discovery.
        // connect() and add_peer() trigger drains directly; this task covers the
        // peer_stream (automatic discovery) path. Exits when the broadcast channel closes.
        {
            let mut event_rx = router.event_tx().subscribe();
            let pd = Arc::clone(&pending_direct);
            let pr = Arc::clone(&pending_routed);
            let p = Arc::clone(&peers);
            let id = identity.clone();
            let kr = Arc::clone(&key_registry);
            let r = Arc::clone(&router);
            let etx = router.event_tx();
            tokio::spawn(async move {
                loop {
                    match event_rx.recv().await {
                        Ok(TransportEvent::PeerConnected(peer_id)) => {
                            drain_direct_for_peer(&peer_id, &pd, &p, &id, &kr, &r, store_ttl, &etx)
                                .await;
                            drain_routed_all(&pr, &p, &id, &kr, &r, store_ttl, &etx).await;
                        }
                        Ok(TransportEvent::KeyLearned {
                            peer_id,
                            public_key,
                        }) => {
                            router::flood_key_announcement(
                                &r,
                                &id,
                                &kr,
                                &p,
                                id.peer_id(),
                                &peer_id,
                                &public_key,
                            )
                            .await;
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
            });
        }

        Ok(Self {
            router,
            identity,
            peers,
            known_addrs: Arc::new(Mutex::new(HashSet::new())),
            handler: Arc::new(Mutex::new(None)),
            dedup: Arc::new(Mutex::new(DeduplicationCache::new())),
            key_registry,
            store_ttl,
            max_queue_depth,
            pending_direct,
            pending_routed,
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
            .expect("mutex not poisoned")
            .insert(announcement.address.clone());
        {
            let mut peers = self.peers.lock().expect("mutex not poisoned");
            let addrs = peers.entry(peer_id.clone()).or_default();
            if !addrs.iter().any(|a| a.address == announcement.address) {
                addrs.push(announcement);
            }
        }

        let event_tx = self.router.event_tx();
        drain_direct_for_peer(
            &peer_id,
            &self.pending_direct,
            &self.peers,
            &self.identity,
            &self.key_registry,
            &self.router,
            self.store_ttl,
            &event_tx,
        )
        .await;
        drain_routed_all(
            &self.pending_routed,
            &self.peers,
            &self.identity,
            &self.key_registry,
            &self.router,
            self.store_ttl,
            &event_tx,
        )
        .await;

        Ok(peer_id)
    }

    /// Records a known PeerId -> PeerAnnouncement mapping in the peer table.
    ///
    /// Not part of the UniFFI boundary. Used by pw-chat to inject a QUIC peer
    /// address resolved from the command line, and by tests to set up known peers.
    pub fn add_peer(&mut self, peer_id: PeerId, announcement: PeerAnnouncement) {
        self.known_addrs
            .lock()
            .expect("mutex not poisoned")
            .insert(announcement.address.clone());
        {
            let mut peers = self.peers.lock().expect("mutex not poisoned");
            let addrs = peers.entry(peer_id.clone()).or_default();
            if !addrs.iter().any(|a| a.address == announcement.address) {
                addrs.push(announcement);
            }
        }

        let pd = Arc::clone(&self.pending_direct);
        let pr = Arc::clone(&self.pending_routed);
        let peers = Arc::clone(&self.peers);
        let identity = self.identity.clone();
        let key_registry = Arc::clone(&self.key_registry);
        let router = Arc::clone(&self.router);
        let store_ttl = self.store_ttl;
        let event_tx = self.router.event_tx();
        tokio::spawn(async move {
            drain_direct_for_peer(
                &peer_id,
                &pd,
                &peers,
                &identity,
                &key_registry,
                &router,
                store_ttl,
                &event_tx,
            )
            .await;
            drain_routed_all(
                &pr,
                &peers,
                &identity,
                &key_registry,
                &router,
                store_ttl,
                &event_tx,
            )
            .await;
        });
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
            .expect("mutex not poisoned")
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
            .expect("mutex not poisoned")
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

    /// Sends `payload` to `dest_peer_id` via mesh routing, sealed end-to-end with
    /// Noise_K so only `dest_peer_id` can read it (ADR 023).
    ///
    /// Relays along the path see `dest_peer_id` and `ttl` (needed to route) but never
    /// the plaintext. Requires `dest_peer_id`'s static key already in the key registry
    /// (from a direct handshake or gossip); returns `PathweaveError::KeyUnknown` if it
    /// is not there yet rather than silently falling back to an unsealed send.
    pub async fn send_routed_sealed(&self, dest_peer_id: PeerId, payload: Vec<u8>) -> Result<()> {
        let dest_public_key = self
            .key_registry
            .lock()
            .expect("mutex not poisoned")
            .get(&dest_peer_id)
            .ok_or_else(|| PathweaveError::KeyUnknown(dest_peer_id.clone()))?;

        let sealed = crate::session::seal(&self.identity, &dest_public_key, &payload)?;
        let mut envelope = Vec::with_capacity(32 + sealed.len());
        envelope.extend_from_slice(self.identity.peer_id().as_bytes());
        envelope.extend_from_slice(&sealed);

        let msg_id = router::new_message_id();
        let mut frame_body = Vec::with_capacity(1 + 32 + 1 + envelope.len());
        frame_body.push(0x03); // E2E-sealed routed route_flag (ADR 023)
        frame_body.extend_from_slice(dest_peer_id.as_bytes());
        frame_body.push(MAX_TTL);
        frame_body.extend_from_slice(&envelope);

        let neighbors: Vec<(PeerId, Vec<PeerAnnouncement>)> = self
            .peers
            .lock()
            .expect("mutex not poisoned")
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

    /// Accepts `payload` for deferred delivery to `peer_id` and returns immediately.
    ///
    /// If the peer is already reachable, delivery is attempted right away using the same
    /// path as `send()`. On success the payload is gone. On failure, or if the peer is
    /// not yet in the peer table, the payload is queued. It drains when the peer next
    /// appears via discovery, `connect()`, or `add_peer()`.
    ///
    /// Entries that remain undelivered past `NodeConfig::store_ttl` are expired with a
    /// `TransportEvent::StoreFailed` event. See ADR 021.
    pub async fn store_forward(&self, peer_id: PeerId, payload: Vec<u8>) {
        let msg_id = router::new_message_id();
        let queued_at = Instant::now();

        let announcements = self
            .peers
            .lock()
            .expect("mutex not poisoned")
            .get(&peer_id)
            .cloned();
        if let Some(anns) = announcements {
            if !anns.is_empty() {
                let mut framed = Vec::with_capacity(1 + payload.len());
                framed.push(0x00);
                framed.extend_from_slice(&payload);
                if self
                    .router
                    .send(
                        &anns,
                        &self.identity,
                        framed,
                        &peer_id,
                        &self.key_registry,
                        &self.peers,
                        Some(msg_id),
                    )
                    .await
                    .is_ok()
                {
                    return;
                }
            }
        }

        let mut queue = self.pending_direct.lock().expect("mutex not poisoned");
        let deque = queue.entry(peer_id.clone()).or_default();
        if let Some(max) = self.max_queue_depth {
            if deque.len() >= max {
                let _ = self
                    .router
                    .event_tx()
                    .send(TransportEvent::StoreFailed { peer_id });
                return;
            }
        }
        deque.push_back((msg_id, payload, queued_at));
    }

    /// Accepts `payload` for deferred mesh delivery to `dest_peer_id` and returns immediately.
    ///
    /// If any neighbor is reachable, the payload is flooded immediately (same as `send_routed`).
    /// If no neighbors are reachable, the payload is queued and flooded when any neighbor next
    /// appears. Delivery is confirmed at the neighbor level only; no end-to-end confirmation.
    ///
    /// Entries expire the same way as `store_forward`. See ADR 021.
    pub async fn store_forward_routed(&self, dest_peer_id: PeerId, payload: Vec<u8>) {
        let msg_id = router::new_message_id();
        let queued_at = Instant::now();

        let neighbors: Vec<(PeerId, Vec<PeerAnnouncement>)> = self
            .peers
            .lock()
            .expect("mutex not poisoned")
            .iter()
            .filter(|(pid, _)| **pid != *self.identity.peer_id())
            .map(|(pid, anns)| (pid.clone(), anns.clone()))
            .collect();

        if !neighbors.is_empty() {
            let mut frame_body = Vec::with_capacity(1 + 32 + 1 + payload.len());
            frame_body.push(0x01);
            frame_body.extend_from_slice(dest_peer_id.as_bytes());
            frame_body.push(MAX_TTL);
            frame_body.extend_from_slice(&payload);

            let mut any_ok = false;
            for (neighbor_id, anns) in &neighbors {
                if self
                    .router
                    .send(
                        anns,
                        &self.identity,
                        frame_body.clone(),
                        neighbor_id,
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
                return;
            }
        }

        let mut queue = self.pending_routed.lock().expect("mutex not poisoned");
        let deque = queue.entry(dest_peer_id.clone()).or_default();
        if let Some(max) = self.max_queue_depth {
            if deque.len() >= max {
                let _ = self.router.event_tx().send(TransportEvent::StoreFailed {
                    peer_id: dest_peer_id,
                });
                return;
            }
        }
        deque.push_back((msg_id, payload, queued_at));
    }

    /// Registers a handler that will be called for each incoming message.
    ///
    /// The accept loop is spawned per transport in register_transport(). Transports
    /// that do not support incoming connections (e.g. BLE central mode) return an
    /// error from accept(), which the loop handles with a backoff.
    pub fn on_message(&self, handler: Box<dyn MessageHandler>) {
        *self.handler.lock().expect("mutex not poisoned") = Some(handler);
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
        self.key_registry
            .lock()
            .expect("mutex not poisoned")
            .get(peer_id)
    }
}

/// Drains the direct pending queue for `peer_id`.
///
/// Removes all entries atomically under the lock, sends each live entry, and
/// re-prepends failures to preserve FIFO order. Expired entries fire StoreFailed.
/// A concurrent drain finds the queue empty and is a no-op. See ADR 021.
#[allow(clippy::too_many_arguments)]
async fn drain_direct_for_peer(
    peer_id: &PeerId,
    pending_direct: &PendingStore,
    peers: &PeerTable,
    identity: &NodeIdentity,
    key_registry: &KeyRegistry,
    router: &Arc<Router>,
    store_ttl: Duration,
    event_tx: &broadcast::Sender<TransportEvent>,
) {
    let entries: Vec<(u64, Vec<u8>, Instant)> = {
        let mut store = pending_direct.lock().expect("mutex not poisoned");
        match store.remove(peer_id) {
            Some(q) => q.into_iter().collect(),
            None => return,
        }
    };

    let now = Instant::now();
    let announcements = peers
        .lock()
        .expect("mutex not poisoned")
        .get(peer_id)
        .cloned()
        .unwrap_or_default();

    let mut failures: VecDeque<(u64, Vec<u8>, Instant)> = VecDeque::new();
    for (msg_id, app_payload, queued_at) in entries {
        if now.saturating_duration_since(queued_at) > store_ttl {
            let _ = event_tx.send(TransportEvent::StoreFailed {
                peer_id: peer_id.clone(),
            });
            continue;
        }
        if announcements.is_empty() {
            failures.push_back((msg_id, app_payload, queued_at));
            continue;
        }
        let mut framed = Vec::with_capacity(1 + app_payload.len());
        framed.push(0x00);
        framed.extend_from_slice(&app_payload);
        match router
            .send(
                &announcements,
                identity,
                framed,
                peer_id,
                key_registry,
                peers,
                Some(msg_id),
            )
            .await
        {
            Ok(()) => {}
            Err(_) => failures.push_back((msg_id, app_payload, queued_at)),
        }
    }

    if !failures.is_empty() {
        let mut store = pending_direct.lock().expect("mutex not poisoned");
        let queue = store.entry(peer_id.clone()).or_default();
        for entry in failures.into_iter().rev() {
            queue.push_front(entry);
        }
    }
}

/// Drains the routed pending queue for every destination.
///
/// Any available neighbor is used as the next hop; routed delivery is confirmed at the
/// neighbor level only. Failed entries are re-prepended to preserve FIFO order.
/// Expired entries fire StoreFailed. See ADR 021.
#[allow(clippy::too_many_arguments)]
async fn drain_routed_all(
    pending_routed: &PendingStore,
    peers: &PeerTable,
    identity: &NodeIdentity,
    key_registry: &KeyRegistry,
    router: &Arc<Router>,
    store_ttl: Duration,
    event_tx: &broadcast::Sender<TransportEvent>,
) {
    let dest_ids: Vec<PeerId> = pending_routed
        .lock()
        .expect("mutex not poisoned")
        .keys()
        .cloned()
        .collect();

    for dest_peer_id in dest_ids {
        let entries: Vec<(u64, Vec<u8>, Instant)> = {
            let mut store = pending_routed.lock().expect("mutex not poisoned");
            match store.remove(&dest_peer_id) {
                Some(q) => q.into_iter().collect(),
                None => continue,
            }
        };

        let now = Instant::now();
        let neighbors: Vec<(PeerId, Vec<PeerAnnouncement>)> = peers
            .lock()
            .expect("mutex not poisoned")
            .iter()
            .filter(|(pid, _)| **pid != *identity.peer_id())
            .map(|(pid, anns)| (pid.clone(), anns.clone()))
            .collect();

        let mut failures: VecDeque<(u64, Vec<u8>, Instant)> = VecDeque::new();
        for (msg_id, app_payload, queued_at) in entries {
            if now.saturating_duration_since(queued_at) > store_ttl {
                let _ = event_tx.send(TransportEvent::StoreFailed {
                    peer_id: dest_peer_id.clone(),
                });
                continue;
            }
            if neighbors.is_empty() {
                failures.push_back((msg_id, app_payload, queued_at));
                continue;
            }
            let mut frame_body = Vec::with_capacity(1 + 32 + 1 + app_payload.len());
            frame_body.push(0x01);
            frame_body.extend_from_slice(dest_peer_id.as_bytes());
            frame_body.push(MAX_TTL);
            frame_body.extend_from_slice(&app_payload);

            let mut any_ok = false;
            for (neighbor_id, anns) in &neighbors {
                if router
                    .send(
                        anns,
                        identity,
                        frame_body.clone(),
                        neighbor_id,
                        key_registry,
                        peers,
                        Some(msg_id),
                    )
                    .await
                    .is_ok()
                {
                    any_ok = true;
                }
            }
            if !any_ok {
                failures.push_back((msg_id, app_payload, queued_at));
            }
        }

        if !failures.is_empty() {
            let mut store = pending_routed.lock().expect("mutex not poisoned");
            let queue = store.entry(dest_peer_id.clone()).or_default();
            for entry in failures.into_iter().rev() {
                queue.push_front(entry);
            }
        }
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
    loop {
        match transport.accept().await {
            Ok(conn) => {
                let identity = identity.clone();
                let handler = Arc::clone(&handler);
                let dedup = Arc::clone(&dedup);
                let key_registry = Arc::clone(&key_registry);
                let peers = Arc::clone(&peers);
                let router = Arc::clone(&router);
                let local_peer_id = local_peer_id.clone();
                tokio::spawn(handle_incoming(
                    conn,
                    identity,
                    handler,
                    dedup,
                    key_registry,
                    peers,
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
    let is_new_key = key_registry
        .lock()
        .expect("mutex not poisoned")
        .insert_direct(sender_peer_id.clone(), *session.remote_static_key());
    if is_new_key {
        let _ = router.event_tx().send(TransportEvent::KeyLearned {
            peer_id: sender_peer_id.clone(),
            public_key: *session.remote_static_key(),
        });
    }

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
            .send(&router::encode_addr_exchange(&router.local_addresses()))
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

/// Spawns a background send of `relay_body` (already framed with its own route_flag
/// byte) to every known neighbor except `sender_peer_id` (so a relayed frame never
/// bounces straight back to whoever just sent it) and `local_peer_id`. Shared by both
/// routed-message (0x01) and key-announcement (0x02) relay in `dispatch_payload`, which
/// differ only in how `relay_body` is framed before reaching this point.
#[allow(clippy::too_many_arguments)]
fn spawn_relay_to_neighbors(
    relay_body: Vec<u8>,
    msg_id: u64,
    sender_peer_id: &PeerId,
    local_peer_id: &PeerId,
    peers: &PeerTable,
    router: &Arc<Router>,
    identity: &NodeIdentity,
    key_registry: &KeyRegistry,
) {
    let neighbors: Vec<(PeerId, Vec<PeerAnnouncement>)> = peers
        .lock()
        .expect("mutex not poisoned")
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
                .expect("mutex not poisoned")
                .check_and_insert(sender_peer_id, msg_id);
            if !is_dup {
                if let Some(h) = handler.lock().expect("mutex not poisoned").as_ref() {
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
            let dest_bytes: [u8; 32] = payload[9..41]
                .try_into()
                .expect("length checked above: payload[9..41] is exactly 32 bytes");
            let dest = PeerId::from_bytes(dest_bytes);
            let ttl = payload[41].min(MAX_TTL);
            let app_payload = payload[42..].to_vec();

            if &dest == local_peer_id {
                let is_dup = dedup
                    .lock()
                    .expect("mutex not poisoned")
                    .check_and_insert_routed(msg_id);
                if !is_dup {
                    if let Some(h) = handler.lock().expect("mutex not poisoned").as_ref() {
                        h.on_message(sender_peer_id.clone(), app_payload);
                    }
                } else {
                    tracing::debug!(msg_id, "suppressed duplicate routed message at destination");
                }
            } else if ttl == 0 {
                tracing::debug!(msg_id, "TTL=0: silently dropping routed message");
            } else {
                let is_dup = dedup
                    .lock()
                    .expect("mutex not poisoned")
                    .check_and_insert_routed(msg_id);
                if !is_dup {
                    let new_ttl = ttl - 1;
                    let mut relay_body = Vec::with_capacity(1 + 32 + 1 + app_payload.len());
                    relay_body.push(0x01);
                    relay_body.extend_from_slice(dest.as_bytes());
                    relay_body.push(new_ttl);
                    relay_body.extend_from_slice(&app_payload);

                    spawn_relay_to_neighbors(
                        relay_body,
                        msg_id,
                        sender_peer_id,
                        local_peer_id,
                        peers,
                        router,
                        identity,
                        key_registry,
                    );
                } else {
                    tracing::debug!(msg_id, "suppressed duplicate routed message at relay");
                }
            }
        }
        0x02 => match router::decode_key_announcement(&payload[9..]) {
            Some((announced_peer_id, public_key, ttl)) => {
                if !router::validate_key_announcement(&announced_peer_id, &public_key) {
                    tracing::debug!(
                        peer = %sender_peer_id,
                        "key announcement failed hash validation; dropping"
                    );
                } else {
                    let is_dup = dedup
                        .lock()
                        .expect("mutex not poisoned")
                        .check_and_insert_routed(msg_id);
                    if is_dup {
                        tracing::debug!(msg_id, "suppressed duplicate key announcement");
                    } else {
                        key_registry
                            .lock()
                            .expect("mutex not poisoned")
                            .insert_gossip(announced_peer_id.clone(), public_key);

                        let ttl = ttl.min(MAX_TTL);
                        if ttl > 0 {
                            let new_ttl = ttl - 1;
                            let relay_body = router::encode_key_announcement(
                                &announced_peer_id,
                                &public_key,
                                new_ttl,
                            );

                            spawn_relay_to_neighbors(
                                relay_body,
                                msg_id,
                                sender_peer_id,
                                local_peer_id,
                                peers,
                                router,
                                identity,
                                key_registry,
                            );
                        }
                    }
                }
            }
            None => {
                tracing::debug!(peer = %sender_peer_id, "key announcement: parse failed; skipping");
            }
        },
        0x03 => {
            // [dest_peer_id: 32][ttl: 1][envelope: sender_peer_id(32) + noise_k_message] (ADR 023)
            if payload.len() < 9 + 32 + 1 + 32 {
                tracing::debug!(peer = %sender_peer_id, "sealed routed frame too short; skipping");
                let _ = session.send(b"").await;
                return;
            }
            let dest_bytes: [u8; 32] = payload[9..41]
                .try_into()
                .expect("length checked above: payload[9..41] is exactly 32 bytes");
            let dest = PeerId::from_bytes(dest_bytes);
            let ttl = payload[41].min(MAX_TTL);
            let envelope = &payload[42..];
            let sealed_sender_bytes: [u8; 32] = envelope[..32]
                .try_into()
                .expect("length checked above: envelope[..32] is exactly 32 bytes");
            let sealed_sender = PeerId::from_bytes(sealed_sender_bytes);
            let noise_k_message = &envelope[32..];

            if &dest == local_peer_id {
                let is_dup = dedup
                    .lock()
                    .expect("mutex not poisoned")
                    .check_and_insert_routed(msg_id);
                if is_dup {
                    tracing::debug!(
                        msg_id,
                        "suppressed duplicate sealed routed message at destination"
                    );
                } else {
                    let sender_key = key_registry
                        .lock()
                        .expect("mutex not poisoned")
                        .get(&sealed_sender);
                    match sender_key {
                        Some(sender_key) => match crate::session::unseal(
                            identity,
                            &sealed_sender,
                            &sender_key,
                            noise_k_message,
                        ) {
                            Ok(plaintext) => {
                                if let Some(h) =
                                    handler.lock().expect("mutex not poisoned").as_ref()
                                {
                                    h.on_message(sealed_sender, plaintext);
                                }
                            }
                            Err(e) => {
                                tracing::debug!(peer = %sealed_sender, error = %e, "unseal failed; dropping");
                            }
                        },
                        None => {
                            tracing::debug!(peer = %sealed_sender, "sealed routed message: sender key unknown; dropping");
                        }
                    }
                }
            } else if ttl == 0 {
                tracing::debug!(msg_id, "TTL=0: silently dropping sealed routed message");
            } else {
                let is_dup = dedup
                    .lock()
                    .expect("mutex not poisoned")
                    .check_and_insert_routed(msg_id);
                if !is_dup {
                    let new_ttl = ttl - 1;
                    let mut relay_body = Vec::with_capacity(1 + 32 + 1 + envelope.len());
                    relay_body.push(0x03);
                    relay_body.extend_from_slice(dest.as_bytes());
                    relay_body.push(new_ttl);
                    relay_body.extend_from_slice(envelope);

                    spawn_relay_to_neighbors(
                        relay_body,
                        msg_id,
                        sender_peer_id,
                        local_peer_id,
                        peers,
                        router,
                        identity,
                        key_registry,
                    );
                } else {
                    tracing::debug!(
                        msg_id,
                        "suppressed duplicate sealed routed message at relay"
                    );
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

    // --- mock transport whose advertised address can change after accept_loop
    // has already started, to simulate a transport restart -----------------

    struct RestartableMockTransport {
        conn_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Box<dyn Connection>>>,
        addrs: Arc<std::sync::Mutex<Vec<PeerAddress>>>,
    }

    #[async_trait]
    impl crate::Transport for RestartableMockTransport {
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
            TransportKind::Quic
        }

        fn name(&self) -> &'static str {
            "restartable-mock"
        }

        fn local_addresses(&self) -> Vec<PeerAddress> {
            self.addrs.lock().unwrap().clone()
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
        assert_eq!(stored, expected_key);
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

    type IdentifiedSender = tokio::sync::oneshot::Sender<(PeerId, Vec<u8>)>;

    // Captures both the reported sender PeerId and the payload, unlike
    // CountingHandler which discards the sender. Used to assert E2E-sealed
    // delivery reports the cryptographically authenticated sender (ADR 023),
    // not just the immediate transport-level relay.
    struct IdentifyingHandler {
        count: Arc<std::sync::atomic::AtomicUsize>,
        tx: Arc<Mutex<Option<IdentifiedSender>>>,
    }

    impl MessageHandler for IdentifyingHandler {
        fn on_message(&self, peer_id: PeerId, payload: Vec<u8>) {
            self.count.fetch_add(1, Ordering::Relaxed);
            if let Some(tx) = self.tx.lock().unwrap().take() {
                let _ = tx.send((peer_id, payload));
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
    async fn key_announcement_propagates_to_indirect_peer() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();

        let pid_a = id_a.peer_id().clone();
        let key_a: [u8; 32] = id_a.public_key().try_into().unwrap();

        // B is the active connector on both legs, so connection order is deterministic:
        // B connects to C first, then to A. When B learns A's key (new), C is already
        // a known neighbor, so B floods A's key to C. See ADR 020.
        let (t_b_for_bc, t_c) = wire_transports(TransportKind::Quic);
        let (t_b_for_ab, t_a) = wire_transports(TransportKind::Ble);

        let mut node_a = PathweaveNode::new(NodeConfig::default(), id_a)
            .await
            .unwrap();
        node_a.register_transport(Box::new(t_a));

        let mut node_b = PathweaveNode::new(NodeConfig::default(), id_b)
            .await
            .unwrap();
        node_b.register_transport(Box::new(t_b_for_bc));
        node_b.register_transport(Box::new(t_b_for_ab));

        let mut node_c = PathweaveNode::new(NodeConfig::default(), id_c)
            .await
            .unwrap();
        node_c.register_transport(Box::new(t_c));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        node_b
            .connect(PeerAnnouncement {
                address: PeerAddress::Quic("127.0.0.1:1".parse().unwrap()),
                short_id: None,
            })
            .await
            .unwrap();

        node_b
            .connect(PeerAnnouncement {
                address: PeerAddress::Ble("a".into()),
                short_id: None,
            })
            .await
            .unwrap();

        // The flood is fired as a TransportEvent and handled by a background task
        // (not awaited synchronously by connect()), so poll rather than assert
        // immediately.
        let stored = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if let Some(key) = node_c.key_registry.lock().unwrap().get(&pid_a) {
                    return key;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for C to learn A's key via gossip");
        assert_eq!(
            stored, key_a,
            "C must learn A's key via gossip without ever connecting to A directly"
        );
    }

    #[tokio::test]
    async fn forged_key_announcement_is_rejected() {
        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel::<Box<dyn Connection>>();
        let transport = AcceptMockTransport {
            conn_rx: tokio::sync::Mutex::new(conn_rx),
        };

        let receiver_id = NodeIdentity::generate();
        let sender_id = NodeIdentity::generate();

        let victim_identity = NodeIdentity::generate();
        let victim_peer_id = victim_identity.peer_id().clone();
        let attacker_identity = NodeIdentity::generate();
        let attacker_public_key: [u8; 32] = attacker_identity.public_key().try_into().unwrap();

        let mut node = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node.register_transport(Box::new(transport));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let (client_conn, server_conn) = conn_pair();
        conn_tx
            .send(Box::new(server_conn))
            .expect("transport still alive");

        let victim_peer_id_for_frame = victim_peer_id.clone();
        tokio::spawn(async move {
            let bundled: Box<dyn Connection> = Box::new(BundleLayer::new(Box::new(client_conn)));
            let mut session = Session::initiate(&sender_id, bundled, None).await.unwrap();
            let msg_id: u64 = 0xDEAD_BEEF_0001_0002;
            let mut framed = Vec::with_capacity(9 + 32 + 32 + 1);
            framed.extend_from_slice(&msg_id.to_be_bytes());
            framed.push(0x02); // key announcement
            framed.extend_from_slice(victim_peer_id_for_frame.as_bytes());
            framed.extend_from_slice(&attacker_public_key); // mismatched: forged
            framed.push(7);
            session.send(&framed).await.unwrap();
            // Wait for the ACK so dispatch_payload has time to process before this exits.
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), session.recv()).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let stored = node.key_registry.lock().unwrap().get(&victim_peer_id);
        assert!(
            stored.is_none(),
            "forged key announcement must not be stored in the registry"
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

    // --- E2E hop encryption tests (ADR 023) -----------------------------------

    // Topology: A --[Ble]--> B --[Quic]--> C, same as routed_message_delivered_via_relay,
    // but A seals the payload to C's key. B relays the opaque envelope without ever
    // calling on_message; only C, holding the matching private key, can open it.
    #[tokio::test]
    async fn sealed_routed_message_relay_cannot_read_payload() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();

        let pid_a = id_a.peer_id().clone();
        let pid_b = id_b.peer_id().clone();
        let pid_c = id_c.peer_id().clone();
        let key_a: [u8; 32] = id_a.public_key().try_into().unwrap();
        let key_c: [u8; 32] = id_c.public_key().try_into().unwrap();

        let (t_a, t_b_inbound) = wire_transports(TransportKind::Ble);
        let (t_b_outbound, t_c) = wire_transports(TransportKind::Quic);

        let mut node_a = PathweaveNode::new(NodeConfig::default(), id_a)
            .await
            .unwrap();
        node_a.register_transport(Box::new(t_a));
        node_a.add_peer(pid_b.clone(), ble_ann("node-b"));
        // A must already have C's key to seal to it (from a prior handshake or gossip;
        // ADR 023 never falls back to sending unsealed).
        node_a
            .key_registry
            .lock()
            .unwrap()
            .insert_direct(pid_c.clone(), key_c);

        let mut node_b = PathweaveNode::new(NodeConfig::default(), id_b)
            .await
            .unwrap();
        node_b.register_transport(Box::new(t_b_inbound));
        node_b.register_transport(Box::new(t_b_outbound));
        node_b.add_peer(pid_a.clone(), ble_ann("node-a"));
        node_b.add_peer(
            pid_c.clone(),
            PeerAnnouncement {
                address: PeerAddress::Quic("127.0.0.1:30".parse().unwrap()),
                short_id: None,
            },
        );

        let mut node_c = PathweaveNode::new(NodeConfig::default(), id_c)
            .await
            .unwrap();
        node_c.register_transport(Box::new(t_c));
        node_c.add_peer(
            pid_b.clone(),
            PeerAnnouncement {
                address: PeerAddress::Quic("127.0.0.1:31".parse().unwrap()),
                short_id: None,
            },
        );
        // C must already have A's key to verify and open the seal.
        node_c
            .key_registry
            .lock()
            .unwrap()
            .insert_direct(pid_a.clone(), key_a);

        let count_a = Arc::new(AtomicUsize::new(0));
        let count_b = Arc::new(AtomicUsize::new(0));
        let count_c = Arc::new(AtomicUsize::new(0));
        let (c_tx, c_rx) = tokio::sync::oneshot::channel::<(PeerId, Vec<u8>)>();
        let c_tx = Arc::new(Mutex::new(Some(c_tx)));

        node_a.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_a),
            payload_tx: Arc::new(Mutex::new(None)),
        }));
        node_b.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_b),
            payload_tx: Arc::new(Mutex::new(None)),
        }));
        node_c.on_message(Box::new(IdentifyingHandler {
            count: Arc::clone(&count_c),
            tx: c_tx,
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        node_a
            .send_routed_sealed(pid_c.clone(), b"e2e sealed payload".to_vec())
            .await
            .unwrap();

        let (reported_sender, delivered) =
            tokio::time::timeout(tokio::time::Duration::from_secs(5), c_rx)
                .await
                .expect("timed out waiting for sealed delivery")
                .unwrap();

        assert_eq!(delivered, b"e2e sealed payload");
        assert_eq!(
            reported_sender, pid_a,
            "destination must see the cryptographically authenticated sender, not the relay"
        );
        assert_eq!(
            count_c.load(Ordering::Relaxed),
            1,
            "delivered exactly once at C"
        );
        assert_eq!(
            count_b.load(Ordering::Relaxed),
            0,
            "relay never decrypts or delivers a sealed message"
        );
        assert_eq!(count_a.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn send_routed_sealed_returns_key_unknown_without_destination_key() {
        let id_a = NodeIdentity::generate();
        let node_a = PathweaveNode::new(NodeConfig::default(), id_a)
            .await
            .unwrap();

        let unknown_dest = NodeIdentity::generate().peer_id().clone();
        let result = node_a
            .send_routed_sealed(unknown_dest.clone(), b"payload".to_vec())
            .await;

        match result {
            Err(PathweaveError::KeyUnknown(p)) => assert_eq!(p, unknown_dest),
            other => panic!("expected KeyUnknown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replayed_sealed_message_is_dropped_at_dedup() {
        // Build one route_flag 0x03 frame by hand with a fixed message_id and send it
        // twice over the same path. dispatch_payload's 0x03 branch checks
        // check_and_insert_routed before calling unseal (ADR 023), so the second
        // delivery must be suppressed without needing to involve unseal at all.
        let id_s = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();
        let pid_c = id_c.peer_id().clone();
        let key_c: [u8; 32] = id_c.public_key().try_into().unwrap();

        let (t_s, t_c) = wire_transports(TransportKind::Ble);

        let mut node_s = PathweaveNode::new(NodeConfig::default(), id_s.clone())
            .await
            .unwrap();
        node_s.register_transport(Box::new(t_s));

        let mut node_c = PathweaveNode::new(NodeConfig::default(), id_c)
            .await
            .unwrap();
        node_c.register_transport(Box::new(t_c));

        let count_c = Arc::new(AtomicUsize::new(0));
        let (c_tx, c_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let c_tx_arc = Arc::new(Mutex::new(Some(c_tx)));
        node_c.on_message(Box::new(CountingHandler {
            count: Arc::clone(&count_c),
            payload_tx: Arc::clone(&c_tx_arc),
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        let sealed = crate::session::seal(&id_s, &key_c, b"replay me").unwrap();
        let mut envelope = Vec::with_capacity(32 + sealed.len());
        envelope.extend_from_slice(id_s.peer_id().as_bytes());
        envelope.extend_from_slice(&sealed);

        let msg_id = crate::router::new_message_id();
        let mut frame_body = Vec::new();
        frame_body.push(0x03);
        frame_body.extend_from_slice(pid_c.as_bytes());
        frame_body.push(MAX_TTL);
        frame_body.extend_from_slice(&envelope);

        for _ in 0..2 {
            node_s
                .router
                .send(
                    &[ble_ann("c")],
                    &id_s,
                    frame_body.clone(),
                    &pid_c,
                    &node_s.key_registry,
                    &node_s.peers,
                    Some(msg_id),
                )
                .await
                .unwrap();
        }

        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), c_rx)
            .await
            .expect("timed out waiting for sealed delivery");

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        assert_eq!(
            count_c.load(Ordering::Relaxed),
            1,
            "replayed sealed frame must not be delivered twice"
        );
    }

    // --- store_forward tests -------------------------------------------------

    fn ble_ann(addr: &str) -> PeerAnnouncement {
        PeerAnnouncement {
            address: PeerAddress::Ble(addr.into()),
            short_id: None,
        }
    }

    #[tokio::test]
    async fn store_forward_delivers_immediately_when_peer_known() {
        let (t_sender, t_receiver) = wire_transports(TransportKind::Ble);

        let sender_id = NodeIdentity::generate();
        let receiver_id = NodeIdentity::generate();
        let pid_receiver = receiver_id.peer_id().clone();

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

        let mut node_sender = PathweaveNode::new(NodeConfig::default(), sender_id)
            .await
            .unwrap();
        node_sender.register_transport(Box::new(t_sender));
        node_sender.add_peer(pid_receiver.clone(), ble_ann("receiver"));

        let mut node_receiver = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node_receiver.register_transport(Box::new(t_receiver));
        node_receiver.on_message(Box::new(RecordingHandler {
            done_tx: Arc::clone(&done_tx),
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        // Peer is already in the table, so store_forward delivers immediately.
        node_sender
            .store_forward(pid_receiver, b"immediate".to_vec())
            .await;

        let payload = tokio::time::timeout(tokio::time::Duration::from_secs(5), done_rx)
            .await
            .expect("timed out")
            .unwrap();
        assert_eq!(payload, b"immediate");
    }

    #[tokio::test]
    async fn store_forward_queues_and_delivers_on_add_peer() {
        let (t_sender, t_receiver) = wire_transports(TransportKind::Ble);

        let sender_id = NodeIdentity::generate();
        let receiver_id = NodeIdentity::generate();
        let pid_receiver = receiver_id.peer_id().clone();

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

        let mut node_sender = PathweaveNode::new(NodeConfig::default(), sender_id)
            .await
            .unwrap();
        node_sender.register_transport(Box::new(t_sender));

        let mut node_receiver = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node_receiver.register_transport(Box::new(t_receiver));
        node_receiver.on_message(Box::new(RecordingHandler {
            done_tx: Arc::clone(&done_tx),
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        // Peer not yet in the table: queued.
        node_sender
            .store_forward(pid_receiver.clone(), b"queued delivery".to_vec())
            .await;

        // Verify payload sits in the queue.
        assert!(node_sender
            .pending_direct
            .lock()
            .unwrap()
            .contains_key(&pid_receiver));

        // add_peer triggers the drain.
        node_sender.add_peer(pid_receiver.clone(), ble_ann("receiver"));

        let payload = tokio::time::timeout(tokio::time::Duration::from_secs(5), done_rx)
            .await
            .expect("timed out waiting for store_forward delivery")
            .unwrap();
        assert_eq!(payload, b"queued delivery");
    }

    #[tokio::test]
    async fn store_forward_queues_and_delivers_on_connect() {
        let (t_sender, t_receiver) = wire_transports(TransportKind::Ble);

        let sender_id = NodeIdentity::generate();
        let receiver_id = NodeIdentity::generate();
        let pid_receiver = receiver_id.peer_id().clone();

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

        let mut node_sender = PathweaveNode::new(NodeConfig::default(), sender_id)
            .await
            .unwrap();
        node_sender.register_transport(Box::new(t_sender));

        let mut node_receiver = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node_receiver.register_transport(Box::new(t_receiver));
        node_receiver.on_message(Box::new(RecordingHandler {
            done_tx: Arc::clone(&done_tx),
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        // Peer not yet in the table: queued.
        node_sender
            .store_forward(pid_receiver.clone(), b"connect delivery".to_vec())
            .await;

        // connect() completes the handshake and triggers the drain.
        node_sender.connect(ble_ann("receiver")).await.unwrap();

        let payload = tokio::time::timeout(tokio::time::Duration::from_secs(5), done_rx)
            .await
            .expect("timed out waiting for store_forward delivery after connect")
            .unwrap();
        assert_eq!(payload, b"connect delivery");
    }

    #[tokio::test]
    async fn store_forward_fifo_order_preserved() {
        let (t_sender, t_receiver) = wire_transports(TransportKind::Ble);

        let sender_id = NodeIdentity::generate();
        let receiver_id = NodeIdentity::generate();
        let pid_receiver = receiver_id.peer_id().clone();

        let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));
        let received_clone = Arc::clone(&received);

        struct OrderHandler {
            received: Arc<Mutex<Vec<Vec<u8>>>>,
            done_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
        }
        impl MessageHandler for OrderHandler {
            fn on_message(&self, _peer_id: PeerId, payload: Vec<u8>) {
                let mut r = self.received.lock().unwrap();
                r.push(payload);
                if r.len() == 3 {
                    if let Some(tx) = self.done_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            }
        }

        let mut node_sender = PathweaveNode::new(NodeConfig::default(), sender_id)
            .await
            .unwrap();
        node_sender.register_transport(Box::new(t_sender));

        let mut node_receiver = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node_receiver.register_transport(Box::new(t_receiver));
        node_receiver.on_message(Box::new(OrderHandler {
            received: Arc::clone(&received_clone),
            done_tx: Arc::clone(&done_tx),
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        node_sender
            .store_forward(pid_receiver.clone(), b"first".to_vec())
            .await;
        node_sender
            .store_forward(pid_receiver.clone(), b"second".to_vec())
            .await;
        node_sender
            .store_forward(pid_receiver.clone(), b"third".to_vec())
            .await;

        node_sender.add_peer(pid_receiver, ble_ann("receiver"));

        tokio::time::timeout(tokio::time::Duration::from_secs(5), done_rx)
            .await
            .expect("timed out waiting for three deliveries")
            .unwrap();

        let r = received.lock().unwrap();
        assert_eq!(
            *r,
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
        );
    }

    #[tokio::test]
    async fn store_forward_fires_store_failed_on_expiry() {
        let sender_id = NodeIdentity::generate();
        let unknown_peer = NodeIdentity::generate().peer_id().clone();

        let config = NodeConfig {
            store_ttl: Some(Duration::from_millis(10)),
            ..NodeConfig::default()
        };
        let mut node = PathweaveNode::new(config, sender_id).await.unwrap();

        // Subscribe before queuing so we don't miss the event.
        let mut event_rx = node.router.event_tx().subscribe();

        node.store_forward(unknown_peer.clone(), b"will expire".to_vec())
            .await;

        // Wait past the TTL.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // add_peer triggers the drain; the expired check fires StoreFailed before
        // attempting delivery, even though an announcement is now in the table.
        node.add_peer(unknown_peer.clone(), ble_ann("unknown"));

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            loop {
                match event_rx.recv().await {
                    Ok(ev @ TransportEvent::StoreFailed { .. }) => return ev,
                    Ok(_) => continue,
                    Err(_) => panic!("broadcast channel closed"),
                }
            }
        })
        .await
        .expect("timed out waiting for StoreFailed");

        assert!(
            matches!(event, TransportEvent::StoreFailed { peer_id } if peer_id == unknown_peer)
        );
    }

    #[tokio::test]
    async fn store_forward_queue_depth_bound_drops_overflow_and_delivers_kept_in_fifo_order() {
        let (t_sender, t_receiver) = wire_transports(TransportKind::Ble);

        let sender_id = NodeIdentity::generate();
        let receiver_id = NodeIdentity::generate();
        let pid_receiver = receiver_id.peer_id().clone();

        let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));
        let received_clone = Arc::clone(&received);

        struct OrderHandler {
            received: Arc<Mutex<Vec<Vec<u8>>>>,
            done_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
            expected: usize,
        }
        impl MessageHandler for OrderHandler {
            fn on_message(&self, _peer_id: PeerId, payload: Vec<u8>) {
                let mut v = self.received.lock().unwrap();
                v.push(payload);
                if v.len() == self.expected {
                    if let Some(tx) = self.done_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            }
        }

        let config = NodeConfig {
            max_queue_depth: Some(2),
            ..NodeConfig::default()
        };
        let mut node_sender = PathweaveNode::new(config, sender_id).await.unwrap();
        node_sender.register_transport(Box::new(t_sender));

        let mut node_receiver = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node_receiver.register_transport(Box::new(t_receiver));
        node_receiver.on_message(Box::new(OrderHandler {
            received: Arc::clone(&received_clone),
            done_tx: Arc::clone(&done_tx),
            expected: 2,
        }));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        // Subscribe before enqueuing so the StoreFailed event is not missed.
        let mut event_rx = node_sender.router.event_tx().subscribe();

        // Enqueue three payloads. The third must be dropped because max_queue_depth = 2.
        node_sender
            .store_forward(pid_receiver.clone(), b"first".to_vec())
            .await;
        node_sender
            .store_forward(pid_receiver.clone(), b"second".to_vec())
            .await;
        node_sender
            .store_forward(pid_receiver.clone(), b"third".to_vec())
            .await;

        // Queue must contain exactly two entries.
        assert_eq!(
            node_sender
                .pending_direct
                .lock()
                .unwrap()
                .get(&pid_receiver)
                .map(|q| q.len())
                .unwrap_or(0),
            2,
            "queue must be capped at max_queue_depth"
        );

        // StoreFailed must have been fired for the dropped third payload.
        let failed_event = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            loop {
                match event_rx.recv().await {
                    Ok(ev @ TransportEvent::StoreFailed { .. }) => return ev,
                    Ok(_) => continue,
                    Err(_) => panic!("broadcast channel closed"),
                }
            }
        })
        .await
        .expect("timed out waiting for StoreFailed on overflow");

        assert!(
            matches!(failed_event, TransportEvent::StoreFailed { peer_id } if peer_id == pid_receiver)
        );

        // Adding the peer triggers the drain; exactly two messages must arrive in order.
        node_sender.add_peer(pid_receiver, ble_ann("receiver"));

        tokio::time::timeout(tokio::time::Duration::from_secs(5), done_rx)
            .await
            .expect("timed out waiting for queued messages to be delivered")
            .unwrap();

        let msgs = received.lock().unwrap().clone();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], b"first");
        assert_eq!(msgs[1], b"second");
    }

    #[tokio::test]
    async fn store_forward_routed_delivers_immediately_when_neighbor_known() {
        let (t_a, t_b) = wire_transports(TransportKind::Ble);

        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();
        let pid_b = id_b.peer_id().clone();
        let pid_c = id_c.peer_id().clone();

        let mut node_a = PathweaveNode::new(NodeConfig::default(), id_a)
            .await
            .unwrap();
        node_a.register_transport(Box::new(t_a));
        node_a.add_peer(pid_b.clone(), ble_ann("b"));

        // B accepts the relay frame; no handler needed for this assertion.
        let mut node_b = PathweaveNode::new(NodeConfig::default(), id_b)
            .await
            .unwrap();
        node_b.register_transport(Box::new(t_b));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        // B is a neighbor. store_forward_routed should flood immediately and leave
        // nothing in the queue.
        node_a
            .store_forward_routed(pid_c.clone(), b"routed immediate".to_vec())
            .await;

        assert!(node_a.pending_routed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn store_forward_routed_queues_and_delivers_on_peer_added() {
        let (t_a, t_b) = wire_transports(TransportKind::Ble);

        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let id_c = NodeIdentity::generate();
        let pid_b = id_b.peer_id().clone();
        let pid_c = id_c.peer_id().clone();

        // B just needs to accept the frame; no handler needed for this test.
        let mut node_a = PathweaveNode::new(NodeConfig::default(), id_a)
            .await
            .unwrap();
        node_a.register_transport(Box::new(t_a));

        let mut node_b = PathweaveNode::new(NodeConfig::default(), id_b)
            .await
            .unwrap();
        node_b.register_transport(Box::new(t_b));

        for _ in 0..6 {
            tokio::task::yield_now().await;
        }

        // No neighbors yet: queued.
        node_a
            .store_forward_routed(pid_c.clone(), b"routed queued".to_vec())
            .await;

        assert!(node_a.pending_routed.lock().unwrap().contains_key(&pid_c));

        // Adding B as a neighbor triggers the drain.
        node_a.add_peer(pid_b, ble_ann("b"));

        // Give the spawned drain task time to run.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Queue must be empty after the drain.
        assert!(node_a.pending_routed.lock().unwrap().is_empty());
    }

    // --- address exchange reflects live transport addresses (issue #93) ---------

    #[tokio::test]
    async fn address_exchange_reflects_post_restart_transport_addresses() {
        let initial_addr = PeerAddress::Quic("127.0.0.1:4000".parse().unwrap());
        let post_restart_addr = PeerAddress::Quic("127.0.0.1:5000".parse().unwrap());

        let addrs = Arc::new(std::sync::Mutex::new(vec![initial_addr.clone()]));
        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel::<Box<dyn Connection>>();

        let transport = RestartableMockTransport {
            conn_rx: tokio::sync::Mutex::new(conn_rx),
            addrs: Arc::clone(&addrs),
        };

        let sender_id = NodeIdentity::generate();
        let receiver_id = NodeIdentity::generate();

        let mut node = PathweaveNode::new(NodeConfig::default(), receiver_id)
            .await
            .unwrap();
        node.register_transport(Box::new(transport));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // First connection: transport advertises the initial address.
        let (c1, s1) = conn_pair();
        conn_tx.send(Box::new(s1)).unwrap();
        let sid1 = sender_id.clone();
        let addrs1 = tokio::spawn(async move {
            let bundled: Box<dyn Connection> = Box::new(BundleLayer::new(Box::new(c1)));
            let mut session = Session::initiate(&sid1, bundled, None).await.unwrap();
            session
                .send(&router::encode_addr_exchange(&[]))
                .await
                .unwrap();
            let frame = session.recv().await.unwrap();
            router::decode_addr_exchange(&frame).unwrap_or_default()
        });
        let addrs1 = tokio::time::timeout(Duration::from_secs(5), addrs1)
            .await
            .expect("first connection timed out")
            .unwrap();
        assert!(
            addrs1.contains(&initial_addr),
            "first addr-exchange must advertise the initial address"
        );

        // Simulate a transport restart: OS assigns a new port.
        *addrs.lock().unwrap() = vec![post_restart_addr.clone()];

        // Second connection: must see the live address, not the pre-restart snapshot.
        let (c2, s2) = conn_pair();
        conn_tx.send(Box::new(s2)).unwrap();
        let addrs2 = tokio::spawn(async move {
            let bundled: Box<dyn Connection> = Box::new(BundleLayer::new(Box::new(c2)));
            let mut session = Session::initiate(&sender_id, bundled, None).await.unwrap();
            session
                .send(&router::encode_addr_exchange(&[]))
                .await
                .unwrap();
            let frame = session.recv().await.unwrap();
            router::decode_addr_exchange(&frame).unwrap_or_default()
        });
        let addrs2 = tokio::time::timeout(Duration::from_secs(5), addrs2)
            .await
            .expect("second connection timed out")
            .unwrap();
        assert!(
            !addrs2.contains(&initial_addr),
            "second addr-exchange must not return the stale pre-restart address"
        );
        assert!(
            addrs2.contains(&post_restart_addr),
            "second addr-exchange must reflect the post-restart address"
        );
    }
}
