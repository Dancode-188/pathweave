use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use crate::{
    BundleLayer, NodeIdentity, PathweaveError, PeerAddress, PeerAnnouncement, PeerId, Result,
    Session, Transport, TransportCost, TransportEvent, TransportKind,
};

const MAX_SEND_ATTEMPTS: usize = 3;
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const NETWORK_POLL_INTERVAL: Duration = Duration::from_secs(3);

struct TransportEntry {
    transport: Arc<dyn Transport>,
    available: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

/// Routes outgoing messages across registered transports using static priority fallback.
///
/// Free transports (BLE) are preferred over Metered (QUIC). A background monitoring
/// task per transport tracks availability so send() always reflects current state.
/// Connections are lazy: opened on send(), closed after.
pub struct Router {
    transports: Vec<TransportEntry>,
    event_tx: broadcast::Sender<TransportEvent>,
}

impl Router {
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(64);
        Self {
            transports: Vec::new(),
            event_tx,
        }
    }

    /// Registers a transport and starts its availability monitoring task.
    ///
    /// Returns a watch receiver that fires once `start()` succeeds. Callers
    /// that need to wait until the transport is ready (e.g. the accept loop or
    /// the discover loop) should `wait_for(|v| *v).await` on a clone of the
    /// returned receiver. Unlike `Notify`, a watch receiver that arrives late
    /// still sees `true` because the value is retained.
    pub fn register_transport(
        &mut self,
        transport: Arc<dyn Transport>,
        identity: Arc<NodeIdentity>,
    ) -> watch::Receiver<bool> {
        let available = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = watch::channel(false);

        let kind = transport.kind();
        let t = Arc::clone(&transport);
        let a = Arc::clone(&available);
        let task = tokio::spawn(health_monitor(
            t,
            started_tx,
            a,
            identity,
            self.event_tx.clone(),
            kind,
        ));

        self.transports.push(TransportEntry {
            transport,
            available,
            task,
        });

        started_rx
    }

    /// Sends `payload` to `peer`, retrying up to MAX_SEND_ATTEMPTS times.
    ///
    /// A random 8-byte message ID is generated once per call and reused across all
    /// retry attempts. The receiver's deduplication cache suppresses duplicate delivery
    /// when the same ID arrives more than once (ADR 011).
    ///
    /// Each attempt builds a candidate list of (transport, announcement) pairs where
    /// the transport kind matches the announcement address kind, sorted by transport
    /// cost (Free first). All pairs are tried before the attempt is counted as failed.
    ///
    /// Returns NoTransportAvailable if no transport is currently available or no
    /// registered transport can handle any of the given addresses.
    /// Returns DeliveryFailed if all attempts are exhausted without a delivery ACK.
    pub async fn send(
        &self,
        peers: &[PeerAnnouncement],
        identity: &NodeIdentity,
        payload: Vec<u8>,
    ) -> Result<()> {
        let any_available = self
            .transports
            .iter()
            .any(|t| t.available.load(Ordering::Acquire));
        if !any_available {
            return Err(PathweaveError::NoTransportAvailable);
        }

        let message_id = new_message_id();

        for attempt in 0..MAX_SEND_ATTEMPTS {
            let mut candidates: Vec<(&TransportEntry, &PeerAnnouncement)> = self
                .transports
                .iter()
                .filter(|t| t.available.load(Ordering::Acquire))
                .flat_map(|entry| {
                    peers.iter().filter_map(move |ann| {
                        if ann.address.kind() == entry.transport.kind() {
                            Some((entry, ann))
                        } else {
                            None
                        }
                    })
                })
                .collect();

            if candidates.is_empty() {
                return Err(PathweaveError::NoTransportAvailable);
            }

            candidates.sort_by_key(|(entry, _)| match entry.transport.cost() {
                TransportCost::Free => 0u8,
                TransportCost::Metered => 1,
                TransportCost::Unknown => 2,
            });

            for (entry, ann) in &candidates {
                tracing::debug!(
                    attempt,
                    transport = entry.transport.name(),
                    addr = ?ann.address,
                    "try_send attempt"
                );
                match try_send(
                    entry.transport.as_ref(),
                    ann,
                    identity,
                    &payload,
                    message_id,
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(e) => tracing::debug!(
                        attempt,
                        transport = entry.transport.name(),
                        addr = ?ann.address,
                        error = %e,
                        "try_send failed"
                    ),
                }
            }

            if attempt + 1 < MAX_SEND_ATTEMPTS {
                tokio::time::sleep(RETRY_BACKOFF).await;
            }
        }

        Err(PathweaveError::DeliveryFailed)
    }

    /// Dials each address in `peers` in transport-cost order, completes the
    /// Noise_XX handshake as the initiator, and returns the remote PeerId on
    /// the first success. Tries (transport, announcement) pairs where the
    /// transport kind matches the announcement address kind. The session is
    /// closed after the handshake; the caller is responsible for storing the
    /// PeerId -> announcements mapping.
    pub async fn connect(
        &self,
        peers: &[PeerAnnouncement],
        identity: &NodeIdentity,
    ) -> Result<PeerId> {
        let mut candidates: Vec<(&TransportEntry, &PeerAnnouncement)> = self
            .transports
            .iter()
            .filter(|t| t.available.load(Ordering::Acquire))
            .flat_map(|entry| {
                peers.iter().filter_map(move |ann| {
                    if ann.address.kind() == entry.transport.kind() {
                        Some((entry, ann))
                    } else {
                        None
                    }
                })
            })
            .collect();

        candidates.sort_by_key(|(entry, _)| match entry.transport.cost() {
            TransportCost::Free => 0u8,
            TransportCost::Metered => 1,
            TransportCost::Unknown => 2,
        });

        for (entry, ann) in candidates {
            if let Ok(peer_id) = try_connect(entry.transport.as_ref(), ann, identity).await {
                return Ok(peer_id);
            }
        }

        Err(PathweaveError::NoTransportAvailable)
    }

    /// Returns a clone of the event sender so callers can fire events into the same channel.
    pub(crate) fn event_tx(&self) -> broadcast::Sender<TransportEvent> {
        self.event_tx.clone()
    }

    /// Returns a stream of transport lifecycle events.
    pub fn events(&self) -> BoxStream<'static, TransportEvent> {
        let rx = self.event_tx.subscribe();
        Box::pin(stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => return Some((event, rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        }))
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        for entry in &self.transports {
            entry.task.abort();
        }
    }
}

/// Manages the full lifecycle of a single transport: initial start, periodic
/// interface polling, and restart on address change.
///
/// Operates in one of two modes. Normal mode: polls the non-loopback IPv4
/// address set every NETWORK_POLL_INTERVAL; restarts the transport when the
/// set changes. Recovery mode: entered when start() fails; retries start()
/// unconditionally on every tick without comparing address sets. This handles
/// the case where the interface stays down: empty == empty would never trigger
/// a restart in normal mode, so an unconditional retry path is required.
///
/// Sets available and sends on the started watch channel to signal consumers
/// (accept_loop, peer_stream) on each transition.
pub(crate) async fn health_monitor(
    transport: Arc<dyn Transport>,
    started: watch::Sender<bool>,
    available: Arc<AtomicBool>,
    identity: Arc<NodeIdentity>,
    event_tx: broadcast::Sender<TransportEvent>,
    kind: TransportKind,
) {
    let mut prev_addrs = current_ipv4_addrs();
    let mut in_recovery = if transport.start(&identity).await.is_ok() {
        available.store(true, Ordering::Release);
        let _ = started.send(true);
        let _ = event_tx.send(TransportEvent::TransportChanged {
            from: None,
            to: kind,
        });
        false
    } else {
        tracing::warn!(
            transport = transport.name(),
            "initial start failed; entering recovery mode"
        );
        true
    };

    loop {
        tokio::time::sleep(NETWORK_POLL_INTERVAL).await;
        let curr_addrs = current_ipv4_addrs();

        if !in_recovery && curr_addrs == prev_addrs {
            continue;
        }

        available.store(false, Ordering::Release);
        let _ = started.send(false);
        let _ = transport.stop().await;

        if transport.start(&identity).await.is_ok() {
            available.store(true, Ordering::Release);
            let _ = started.send(true);
            let _ = event_tx.send(TransportEvent::TransportChanged {
                from: None,
                to: kind,
            });
            prev_addrs = curr_addrs;
            in_recovery = false;
            tracing::info!(
                transport = transport.name(),
                "transport restarted after address change"
            );
        } else {
            tracing::debug!(
                transport = transport.name(),
                "transport restart failed; will retry"
            );
            in_recovery = true;
        }
    }
}

fn current_ipv4_addrs() -> HashSet<Ipv4Addr> {
    if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .filter(|i| !i.is_loopback())
        .filter_map(|i| match i.addr {
            if_addrs::IfAddr::V4(v4) => Some(v4.ip),
            _ => None,
        })
        .collect()
}

/// Dials the transport, completes the Noise_XX handshake, and returns the remote PeerId.
/// The session is closed explicitly so transports that require an active teardown
/// (e.g. BLE peripheral.disconnect()) release their resources before the next connect.
async fn try_connect(
    transport: &dyn Transport,
    peer: &PeerAnnouncement,
    identity: &NodeIdentity,
) -> Result<PeerId> {
    let raw = transport.connect(peer).await?;
    let bundled = Box::new(BundleLayer::new(raw));
    let mut session = Session::initiate(identity, bundled).await?;
    let peer_id = session.peer_id().clone();
    let _ = session.close().await;
    Ok(peer_id)
}

/// Generates a cryptographically random 64-bit message ID from OS entropy.
///
/// Panics if the system entropy source is unavailable — the same condition
/// that would have already caused NodeIdentity::generate() to panic.
fn new_message_id() -> u64 {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).expect("system entropy unavailable");
    u64::from_be_bytes(bytes)
}

/// Opens a connection through the given transport, wraps it in BundleLayer and
/// Session, prepends the 8-byte message ID for receiver-side deduplication,
/// sends the framed payload, waits for the receiver's delivery ACK, then closes.
async fn try_send(
    transport: &dyn Transport,
    peer: &PeerAnnouncement,
    identity: &NodeIdentity,
    payload: &[u8],
    message_id: u64,
) -> Result<()> {
    let raw = transport.connect(peer).await.map_err(|e| {
        tracing::debug!(addr = ?peer.address, error = %e, "try_send: connect failed");
        e
    })?;
    let bundled = Box::new(BundleLayer::new(raw));
    let mut session = Session::initiate(identity, bundled).await.map_err(|e| {
        tracing::debug!(addr = ?peer.address, error = %e, "try_send: handshake failed");
        e
    })?;

    let mut framed = Vec::with_capacity(8 + payload.len());
    framed.extend_from_slice(&message_id.to_be_bytes());
    framed.extend_from_slice(payload);

    session.send(&framed).await.map_err(|e| {
        tracing::debug!(addr = ?peer.address, error = %e, "try_send: send failed");
        e
    })?;
    // Quinn's write_all() buffers internally; CONNECTION_CLOSE fires when the last
    // connection handle drops, which happens before the buffer is flushed. Waiting
    // for the receiver's ACK keeps the connection alive until the data is delivered.
    match tokio::time::timeout(Duration::from_secs(5), session.recv()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            tracing::debug!(addr = ?peer.address, error = %e, "try_send: ACK recv failed");
            Err(e)
        }
        Err(_) => Err(PathweaveError::Transport("delivery ACK timed out".into())),
    }
}

/// Drives peer discovery for a single transport across restarts.
///
/// Waits for the transport to start, then drains its discover() stream. For
/// each announced address that is not already in `known_addrs`, performs a
/// Noise_XX handshake to learn the remote PeerId. On success, upserts the
/// PeerId -> PeerAnnouncement mapping into the shared peer table. On failure,
/// removes the address from `known_addrs` so the next re-announcement retries.
/// Skips self-announcements by comparing the remote PeerId with `local_peer_id`.
///
/// When the discover stream ends (transport stopped by health_monitor), loops
/// back and waits for the next started -> stopped -> started cycle. The
/// wait_for(false) step prevents a spin loop on transports whose discover()
/// returns an empty stream immediately.
pub(crate) async fn peer_stream(
    transport: Arc<dyn Transport>,
    identity: NodeIdentity,
    mut started: watch::Receiver<bool>,
    peers: Arc<Mutex<HashMap<PeerId, Vec<PeerAnnouncement>>>>,
    known_addrs: Arc<Mutex<HashSet<PeerAddress>>>,
    local_peer_id: PeerId,
    event_tx: broadcast::Sender<TransportEvent>,
) {
    loop {
        let _ = started.wait_for(|v| *v).await;
        let mut discover = transport.discover();
        while let Some(announcement) = discover.next().await {
            let addr = announcement.address.clone();

            // Skip re-announcements from already-connected addresses (O(1) check).
            if !known_addrs.lock().unwrap().insert(addr.clone()) {
                continue;
            }

            match try_connect(transport.as_ref(), &announcement, &identity).await {
                Ok(peer_id) if peer_id == local_peer_id => {
                    // Self-discovery: keep addr in known_addrs so we don't retry.
                    tracing::debug!(addr = %addr, "discovered self; skipping");
                }
                Ok(peer_id) => {
                    tracing::debug!(addr = %addr, peer = %peer_id, "peer connected");
                    peers
                        .lock()
                        .unwrap()
                        .entry(peer_id.clone())
                        .or_default()
                        .push(announcement);
                    let _ = event_tx.send(TransportEvent::PeerConnected(peer_id));
                }
                Err(e) => {
                    tracing::debug!(addr = %addr, "handshake failed: {}", e);
                    // Remove so we retry on the next re-announcement.
                    known_addrs.lock().unwrap().remove(&addr);
                }
            }
        }
        // Stream ended (transport stopped). Wait for the stopped signal before
        // looping so we don't spin if discover() returns an empty stream.
        let _ = started.wait_for(|v| !v).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Connection, NodeIdentity, PeerAddress, Session, TransportKind};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

    // --- in-memory connection --------------------------------------------------

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

    // --- mock transport -------------------------------------------------------

    struct MockTransport {
        cost: TransportCost,
        kind: TransportKind,
        fail_connect: bool,
        remaining_failures: Arc<AtomicUsize>,
        responder_tx: UnboundedSender<TestConn>,
        connect_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Transport for MockTransport {
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
            if self.fail_connect {
                return Err(PathweaveError::Transport("mock: connect failed".into()));
            }
            // Transient failures: decrement remaining_failures; fail while non-zero.
            if self
                .remaining_failures
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    if n > 0 {
                        Some(n - 1)
                    } else {
                        None
                    }
                })
                .is_ok()
            {
                return Err(PathweaveError::Transport("mock: transient failure".into()));
            }
            let (a, b) = conn_pair();
            self.responder_tx.send(b).ok();
            Ok(Box::new(a))
        }

        async fn accept(&self) -> Result<Box<dyn Connection>> {
            Err(PathweaveError::Transport(
                "mock: accept not used in tests".into(),
            ))
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
            match self.kind {
                TransportKind::Ble => "mock-ble",
                TransportKind::Quic => "mock-quic",
            }
        }
    }

    fn make_transport(
        cost: TransportCost,
        kind: TransportKind,
        fail_connect: bool,
    ) -> (
        Arc<MockTransport>,
        UnboundedReceiver<TestConn>,
        Arc<AtomicUsize>,
    ) {
        let (tx, rx) = unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(MockTransport {
                cost,
                kind,
                fail_connect,
                remaining_failures: Arc::new(AtomicUsize::new(0)),
                responder_tx: tx,
                connect_count: Arc::clone(&count),
            }),
            rx,
            count,
        )
    }

    fn make_transport_with_failures(
        cost: TransportCost,
        kind: TransportKind,
        initial_failures: usize,
    ) -> (
        Arc<MockTransport>,
        UnboundedReceiver<TestConn>,
        Arc<AtomicUsize>,
    ) {
        let (tx, rx) = unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(MockTransport {
                cost,
                kind,
                fail_connect: false,
                remaining_failures: Arc::new(AtomicUsize::new(initial_failures)),
                responder_tx: tx,
                connect_count: Arc::clone(&count),
            }),
            rx,
            count,
        )
    }

    fn dummy_peer() -> PeerAnnouncement {
        PeerAnnouncement {
            address: PeerAddress::Ble("aa:bb:cc:dd:ee:ff".into()),
            short_id: None,
        }
    }

    // Peer reachable via both BLE and QUIC: used to test cross-transport fallback.
    fn dual_peer() -> Vec<PeerAnnouncement> {
        vec![
            PeerAnnouncement {
                address: PeerAddress::Ble("aa:bb:cc:dd:ee:ff".into()),
                short_id: None,
            },
            PeerAnnouncement {
                address: PeerAddress::Quic("127.0.0.1:9001".parse().unwrap()),
                short_id: None,
            },
        ]
    }

    /// Runs a responder: completes the Noise_XX handshake, receives one message, and
    /// sends an empty ACK so try_send() can return before dropping the connection.
    async fn run_responder(conn: TestConn, identity: NodeIdentity) {
        let bundled = Box::new(BundleLayer::new(Box::new(conn)));
        let mut session = Session::respond(&identity, bundled).await.unwrap();
        let _ = session.recv().await;
        let _ = session.send(b"").await;
    }

    #[tokio::test]
    async fn send_with_dual_address_peer_prefers_free_transport_and_skips_metered() {
        // When the peer has both addresses and BLE succeeds, QUIC must not be attempted.
        let (ble, mut ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, false);
        let (quic, _quic_rx, quic_count) =
            make_transport(TransportCost::Metered, TransportKind::Quic, false);

        let identity = Arc::new(NodeIdentity::generate());
        let mut router = Router::new();
        router.register_transport(ble, Arc::clone(&identity));
        router.register_transport(quic, Arc::clone(&identity));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();

        tokio::spawn(async move {
            let conn = ble_rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        router
            .send(&dual_peer(), &sender_id, b"hello".to_vec())
            .await
            .unwrap();

        assert_eq!(ble_count.load(Ordering::Relaxed), 1, "BLE should be used");
        assert_eq!(
            quic_count.load(Ordering::Relaxed),
            0,
            "QUIC must not be attempted when BLE succeeds"
        );
    }

    #[tokio::test]
    async fn send_prefers_free_transport() {
        let (ble, mut ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, false);
        let (quic, _quic_rx, quic_count) =
            make_transport(TransportCost::Metered, TransportKind::Quic, false);

        let identity = Arc::new(NodeIdentity::generate());
        let mut router = Router::new();
        router.register_transport(ble, Arc::clone(&identity));
        router.register_transport(quic, Arc::clone(&identity));

        // Yield so monitoring tasks run start() and set available = true.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();

        tokio::spawn(async move {
            let conn = ble_rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        router
            .send(&[dummy_peer()], &sender_id, b"hello".to_vec())
            .await
            .unwrap();

        assert_eq!(ble_count.load(Ordering::Relaxed), 1, "BLE should be used");
        assert_eq!(
            quic_count.load(Ordering::Relaxed),
            0,
            "QUIC should not be used"
        );
    }

    #[tokio::test]
    async fn send_falls_back_on_connect_failure() {
        // Peer has both a BLE address and a QUIC address. BLE transport fails;
        // router should fall back to the QUIC address on the same attempt.
        let (ble, _ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, true); // fail
        let (quic, mut quic_rx, quic_count) =
            make_transport(TransportCost::Metered, TransportKind::Quic, false);

        let identity = Arc::new(NodeIdentity::generate());
        let mut router = Router::new();
        router.register_transport(ble, Arc::clone(&identity));
        router.register_transport(quic, Arc::clone(&identity));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();

        tokio::spawn(async move {
            let conn = quic_rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        router
            .send(&dual_peer(), &sender_id, b"fallback".to_vec())
            .await
            .unwrap();

        assert_eq!(
            ble_count.load(Ordering::Relaxed),
            1,
            "BLE should be attempted"
        );
        assert_eq!(
            quic_count.load(Ordering::Relaxed),
            1,
            "QUIC should be used after BLE fails"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn send_returns_delivery_failed_when_all_transports_fail() {
        // Peer has both addresses; both transports fail. Each attempt tries
        // BLE first (Free) then QUIC (Metered) before backing off.
        let (ble, _ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, true);
        let (quic, _quic_rx, quic_count) =
            make_transport(TransportCost::Metered, TransportKind::Quic, true);

        let identity = Arc::new(NodeIdentity::generate());
        let mut router = Router::new();
        router.register_transport(ble, Arc::clone(&identity));
        router.register_transport(quic, Arc::clone(&identity));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let result = router
            .send(&dual_peer(), &sender_id, b"ignored".to_vec())
            .await;

        assert!(
            matches!(result, Err(PathweaveError::DeliveryFailed)),
            "expected DeliveryFailed, got: {result:?}"
        );
        assert_eq!(
            ble_count.load(Ordering::Relaxed),
            MAX_SEND_ATTEMPTS,
            "BLE should be tried once per attempt"
        );
        assert_eq!(
            quic_count.load(Ordering::Relaxed),
            MAX_SEND_ATTEMPTS,
            "QUIC should be tried once per attempt"
        );
    }

    #[tokio::test]
    async fn send_returns_no_transport_when_none_registered() {
        let router = Router::new();
        let sender_id = NodeIdentity::generate();
        let result = router
            .send(&[dummy_peer()], &sender_id, b"ignored".to_vec())
            .await;
        assert!(matches!(result, Err(PathweaveError::NoTransportAvailable)));
    }

    #[tokio::test(start_paused = true)]
    async fn send_succeeds_on_retry_after_transient_failure() {
        // Transport fails the first connect, then succeeds.
        let (transport, mut rx, count) =
            make_transport_with_failures(TransportCost::Free, TransportKind::Ble, 1);

        let identity = Arc::new(NodeIdentity::generate());
        let mut router = Router::new();
        router.register_transport(transport, Arc::clone(&identity));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();

        tokio::spawn(async move {
            let conn = rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        router
            .send(&[dummy_peer()], &sender_id, b"hello".to_vec())
            .await
            .unwrap();

        // First attempt failed (transient), second succeeded: 2 total connects.
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn send_returns_no_transport_when_none_available_after_precheck() {
        // Transports are registered but their availability flags never flip to true
        // because we skip the yield_now() calls. Pre-check should catch this fast.
        let (ble, _ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, false);

        let identity = Arc::new(NodeIdentity::generate());
        let mut router = Router::new();
        router.register_transport(ble, Arc::clone(&identity));

        // No yield: monitoring task has not run, available = false.
        let sender_id = NodeIdentity::generate();
        let result = router
            .send(&[dummy_peer()], &sender_id, b"ignored".to_vec())
            .await;

        assert!(
            matches!(result, Err(PathweaveError::NoTransportAvailable)),
            "expected NoTransportAvailable, got: {result:?}"
        );
        assert_eq!(
            ble_count.load(Ordering::Relaxed),
            0,
            "no connect attempts should be made"
        );
    }

    #[tokio::test]
    async fn connect_returns_peer_id_on_success() {
        let (transport, mut rx, _) = make_transport(TransportCost::Free, TransportKind::Ble, false);

        let identity = Arc::new(NodeIdentity::generate());
        let mut router = Router::new();
        router.register_transport(transport, Arc::clone(&identity));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let initiator_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();
        let expected_peer_id = responder_id.peer_id().clone();

        tokio::spawn(async move {
            let conn = rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        let peer_id = router
            .connect(&[dummy_peer()], &initiator_id)
            .await
            .unwrap();
        assert_eq!(peer_id, expected_peer_id);
    }

    #[tokio::test]
    async fn connect_returns_no_transport_when_none_registered() {
        let router = Router::new();
        let identity = NodeIdentity::generate();
        let result = router.connect(&[dummy_peer()], &identity).await;
        assert!(matches!(result, Err(PathweaveError::NoTransportAvailable)));
    }

    // --- health_monitor tests -------------------------------------------------

    struct StartFailNTimes {
        remaining_failures: Arc<AtomicUsize>,
        stop_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Transport for StartFailNTimes {
        async fn start(&self, _identity: &NodeIdentity) -> Result<()> {
            let n =
                self.remaining_failures
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    });
            if n.is_ok() {
                Err(PathweaveError::Transport("start: injected failure".into()))
            } else {
                Ok(())
            }
        }
        async fn stop(&self) -> Result<()> {
            self.stop_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
            Box::pin(stream::empty())
        }
        async fn connect(&self, _: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
            Err(PathweaveError::Transport("not used".into()))
        }
        async fn accept(&self) -> Result<Box<dyn Connection>> {
            futures::future::pending::<()>().await;
            unreachable!()
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
            "start-fail-n-times"
        }
    }

    #[tokio::test(start_paused = true)]
    async fn health_monitor_initial_success_sets_available() {
        let (t, _, _) = make_transport(TransportCost::Free, TransportKind::Ble, false);
        let available = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = watch::channel(false);

        let (event_tx, _) = broadcast::channel(8);
        tokio::spawn(health_monitor(
            t,
            started_tx,
            Arc::clone(&available),
            Arc::new(NodeIdentity::generate()),
            event_tx,
            TransportKind::Ble,
        ));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(available.load(Ordering::Acquire));
        assert!(*started_rx.borrow());
    }

    #[tokio::test(start_paused = true)]
    async fn health_monitor_enters_recovery_and_retries_on_next_tick() {
        let failures = Arc::new(AtomicUsize::new(1));
        let stop_count = Arc::new(AtomicUsize::new(0));
        let transport = Arc::new(StartFailNTimes {
            remaining_failures: Arc::clone(&failures),
            stop_count: Arc::clone(&stop_count),
        });
        let available = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = watch::channel(false);

        let (event_tx, _) = broadcast::channel(8);
        tokio::spawn(health_monitor(
            transport,
            started_tx,
            Arc::clone(&available),
            Arc::new(NodeIdentity::generate()),
            event_tx,
            TransportKind::Ble,
        ));

        // Yield so the initial start() runs and fails.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            !available.load(Ordering::Acquire),
            "should be unavailable after failed start"
        );
        assert!(!*started_rx.borrow());

        // Advance one poll interval: recovery mode retries start() unconditionally.
        tokio::time::advance(NETWORK_POLL_INTERVAL + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            available.load(Ordering::Acquire),
            "should be available after recovery retry"
        );
        assert!(*started_rx.borrow());
    }

    #[tokio::test(start_paused = true)]
    async fn health_monitor_recovery_does_not_require_address_change() {
        // Transport fails twice, then succeeds. Verifies that recovery mode retries
        // on every tick without needing the address set to change between ticks.
        let failures = Arc::new(AtomicUsize::new(2));
        let stop_count = Arc::new(AtomicUsize::new(0));
        let transport = Arc::new(StartFailNTimes {
            remaining_failures: Arc::clone(&failures),
            stop_count: Arc::clone(&stop_count),
        });
        let available = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = watch::channel(false);

        let (event_tx, _) = broadcast::channel(8);
        tokio::spawn(health_monitor(
            transport,
            started_tx,
            Arc::clone(&available),
            Arc::new(NodeIdentity::generate()),
            event_tx,
            TransportKind::Ble,
        ));

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(!available.load(Ordering::Acquire));

        // First tick: recovery retry fails again (second failure).
        tokio::time::advance(NETWORK_POLL_INTERVAL + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            !available.load(Ordering::Acquire),
            "still in recovery after second failure"
        );

        // Second tick: recovery retry succeeds (no third failure).
        tokio::time::advance(NETWORK_POLL_INTERVAL + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            available.load(Ordering::Acquire),
            "recovered on second retry"
        );
        assert!(*started_rx.borrow());
    }

    // --- peer_stream tests ----------------------------------------------------

    struct ControllableDiscoverTransport {
        // Each pop_front dequeues the next discover() stream in call order.
        streams: std::sync::Mutex<std::collections::VecDeque<BoxStream<'static, PeerAnnouncement>>>,
        discover_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Transport for ControllableDiscoverTransport {
        async fn start(&self, _identity: &NodeIdentity) -> Result<()> {
            Ok(())
        }
        async fn stop(&self) -> Result<()> {
            Ok(())
        }
        fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
            self.discover_calls.fetch_add(1, Ordering::Relaxed);
            self.streams
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Box::pin(stream::empty()))
        }
        async fn connect(&self, _: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
            Err(PathweaveError::Transport("not used".into()))
        }
        async fn accept(&self) -> Result<Box<dyn Connection>> {
            futures::future::pending::<()>().await;
            unreachable!()
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
            "controllable-discover"
        }
    }

    #[tokio::test]
    async fn peer_stream_calls_discover_again_after_restart() {
        // Two streams: both empty. Verify discover() is called twice after a
        // stopped -> started cycle.
        let discover_calls = Arc::new(AtomicUsize::new(0));
        let transport = Arc::new(ControllableDiscoverTransport {
            streams: std::sync::Mutex::new(std::collections::VecDeque::from([
                Box::pin(stream::empty()) as BoxStream<'static, PeerAnnouncement>,
                Box::pin(stream::empty()) as BoxStream<'static, PeerAnnouncement>,
            ])),
            discover_calls: Arc::clone(&discover_calls),
        });
        let (started_tx, started_rx) = watch::channel(false);
        let peers = Arc::new(Mutex::new(HashMap::new()));
        let known_addrs = Arc::new(Mutex::new(HashSet::new()));
        let local_peer_id = NodeIdentity::generate().peer_id().clone();

        let (event_tx, _) = broadcast::channel(8);
        tokio::spawn(peer_stream(
            transport,
            NodeIdentity::generate(),
            started_rx,
            Arc::clone(&peers),
            Arc::clone(&known_addrs),
            local_peer_id,
            event_tx,
        ));

        // Start the transport: peer_stream calls discover() (first call, empty stream ends).
        let _ = started_tx.send(true);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // peer_stream is now in wait_for(false). Send false to let it proceed.
        let _ = started_tx.send(false);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Send true again: peer_stream calls discover() a second time.
        let _ = started_tx.send(true);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            discover_calls.load(Ordering::Relaxed),
            2,
            "discover() must be called again after a restart cycle"
        );
    }
}
