use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, BoxStream};
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;

use crate::{
    BundleLayer, NodeIdentity, PathweaveError, PeerAnnouncement, PeerId, Result, Session,
    Transport, TransportCost, TransportEvent,
};

const MAX_SEND_ATTEMPTS: usize = 3;
const RETRY_BACKOFF: Duration = Duration::from_secs(1);

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
    /// Returns a `Notify` that fires once after `start()` succeeds. Callers that
    /// need to wait until the transport is ready (e.g. the accept loop) should
    /// await `notified()` on the returned handle before proceeding.
    pub fn register_transport(&mut self, transport: Arc<dyn Transport>) -> Arc<Notify> {
        let available = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Notify::new());

        let t = Arc::clone(&transport);
        let a = Arc::clone(&available);
        let s = Arc::clone(&started);
        let task = tokio::spawn(monitor(t, a, s));

        self.transports.push(TransportEntry {
            transport,
            available,
            task,
        });

        started
    }

    /// Sends `payload` to `peer`, retrying up to MAX_SEND_ATTEMPTS times.
    ///
    /// A random 8-byte message ID is generated once per call and reused across all
    /// retry attempts. The receiver's deduplication cache suppresses duplicate delivery
    /// when the same ID arrives more than once (ADR 011).
    ///
    /// Returns NoTransportAvailable immediately if no transport is currently available.
    /// Returns DeliveryFailed if all attempts across all available transports are
    /// exhausted without receiving a delivery ACK.
    pub async fn send(
        &self,
        peer: &PeerAnnouncement,
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
            let mut candidates: Vec<&TransportEntry> = self
                .transports
                .iter()
                .filter(|t| t.available.load(Ordering::Acquire))
                .collect();

            candidates.sort_by_key(|t| match t.transport.cost() {
                TransportCost::Free => 0u8,
                TransportCost::Metered => 1,
                TransportCost::Unknown => 2,
            });

            for entry in candidates {
                if try_send(
                    entry.transport.as_ref(),
                    peer,
                    identity,
                    &payload,
                    message_id,
                )
                .await
                .is_ok()
                {
                    return Ok(());
                }
            }

            if attempt + 1 < MAX_SEND_ATTEMPTS {
                tokio::time::sleep(RETRY_BACKOFF).await;
            }
        }

        Err(PathweaveError::DeliveryFailed)
    }

    /// Dials `peer`, completes the Noise_XX handshake as the initiator, and returns
    /// the remote PeerId. Tries transports in cost order (Free first). The session is
    /// closed after the handshake; the caller is responsible for storing the mapping.
    pub async fn connect(
        &self,
        peer: &PeerAnnouncement,
        identity: &NodeIdentity,
    ) -> Result<PeerId> {
        let mut candidates: Vec<&TransportEntry> = self
            .transports
            .iter()
            .filter(|t| t.available.load(Ordering::Acquire))
            .collect();

        candidates.sort_by_key(|t| match t.transport.cost() {
            TransportCost::Free => 0u8,
            TransportCost::Metered => 1,
            TransportCost::Unknown => 2,
        });

        for entry in candidates {
            if let Ok(peer_id) = try_connect(entry.transport.as_ref(), peer, identity).await {
                return Ok(peer_id);
            }
        }

        Err(PathweaveError::NoTransportAvailable)
    }

    /// Returns a stream of transport lifecycle events.
    pub fn events(&self) -> BoxStream<'_, TransportEvent> {
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

/// Monitors a single transport. Marks it available after start() succeeds, signals
/// `started` so the accept loop can begin, then parks until the router is dropped.
async fn monitor(transport: Arc<dyn Transport>, available: Arc<AtomicBool>, started: Arc<Notify>) {
    if transport.start().await.is_ok() {
        available.store(true, Ordering::Release);
        started.notify_one();
        std::future::pending::<()>().await;
    }
}

/// Dials the transport, completes the Noise_XX handshake, and returns the remote PeerId.
/// The session is dropped after the handshake, closing the connection.
async fn try_connect(
    transport: &dyn Transport,
    peer: &PeerAnnouncement,
    identity: &NodeIdentity,
) -> Result<PeerId> {
    let raw = transport.connect(peer).await?;
    let bundled = Box::new(BundleLayer::new(raw));
    let session = Session::initiate(identity, bundled).await?;
    Ok(session.peer_id().clone())
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
    let raw = transport.connect(peer).await?;
    let bundled = Box::new(BundleLayer::new(raw));
    let mut session = Session::initiate(identity, bundled).await?;

    let mut framed = Vec::with_capacity(8 + payload.len());
    framed.extend_from_slice(&message_id.to_be_bytes());
    framed.extend_from_slice(payload);

    session.send(&framed).await?;
    // Quinn's write_all() buffers internally; CONNECTION_CLOSE fires when the last
    // connection handle drops, which happens before the buffer is flushed. Waiting
    // for the receiver's ACK keeps the connection alive until the data is delivered.
    match tokio::time::timeout(Duration::from_secs(5), session.recv()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(PathweaveError::Transport("delivery ACK timed out".into())),
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
        async fn start(&self) -> Result<()> {
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            Ok(())
        }

        fn discover(&self) -> BoxStream<'_, PeerAnnouncement> {
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

    /// Runs a responder: completes the Noise_XX handshake, receives one message, and
    /// sends an empty ACK so try_send() can return before dropping the connection.
    async fn run_responder(conn: TestConn, identity: NodeIdentity) {
        let bundled = Box::new(BundleLayer::new(Box::new(conn)));
        let mut session = Session::respond(&identity, bundled).await.unwrap();
        let _ = session.recv().await;
        let _ = session.send(b"").await;
    }

    #[tokio::test]
    async fn send_prefers_free_transport() {
        let (ble, mut ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, false);
        let (quic, _quic_rx, quic_count) =
            make_transport(TransportCost::Metered, TransportKind::Quic, false);

        let mut router = Router::new();
        router.register_transport(ble);
        router.register_transport(quic);

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
            .send(&dummy_peer(), &sender_id, b"hello".to_vec())
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
        let (ble, _ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, true); // fail
        let (quic, mut quic_rx, quic_count) =
            make_transport(TransportCost::Metered, TransportKind::Quic, false);

        let mut router = Router::new();
        router.register_transport(ble);
        router.register_transport(quic);

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();

        tokio::spawn(async move {
            let conn = quic_rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        router
            .send(&dummy_peer(), &sender_id, b"fallback".to_vec())
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
        let (ble, _ble_rx, ble_count) =
            make_transport(TransportCost::Free, TransportKind::Ble, true);
        let (quic, _quic_rx, quic_count) =
            make_transport(TransportCost::Metered, TransportKind::Quic, true);

        let mut router = Router::new();
        router.register_transport(ble);
        router.register_transport(quic);

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let result = router
            .send(&dummy_peer(), &sender_id, b"ignored".to_vec())
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
            .send(&dummy_peer(), &sender_id, b"ignored".to_vec())
            .await;
        assert!(matches!(result, Err(PathweaveError::NoTransportAvailable)));
    }

    #[tokio::test(start_paused = true)]
    async fn send_succeeds_on_retry_after_transient_failure() {
        // Transport fails the first connect, then succeeds.
        let (transport, mut rx, count) =
            make_transport_with_failures(TransportCost::Free, TransportKind::Ble, 1);

        let mut router = Router::new();
        router.register_transport(transport);

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sender_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();

        tokio::spawn(async move {
            let conn = rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        router
            .send(&dummy_peer(), &sender_id, b"hello".to_vec())
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

        let mut router = Router::new();
        router.register_transport(ble);

        // No yield: monitoring task has not run, available = false.
        let sender_id = NodeIdentity::generate();
        let result = router
            .send(&dummy_peer(), &sender_id, b"ignored".to_vec())
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

        let mut router = Router::new();
        router.register_transport(transport);

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let initiator_id = NodeIdentity::generate();
        let responder_id = NodeIdentity::generate();
        let expected_peer_id = responder_id.peer_id().clone();

        tokio::spawn(async move {
            let conn = rx.recv().await.unwrap();
            run_responder(conn, responder_id).await;
        });

        let peer_id = router.connect(&dummy_peer(), &initiator_id).await.unwrap();
        assert_eq!(peer_id, expected_peer_id);
    }

    #[tokio::test]
    async fn connect_returns_no_transport_when_none_registered() {
        let router = Router::new();
        let identity = NodeIdentity::generate();
        let result = router.connect(&dummy_peer(), &identity).await;
        assert!(matches!(result, Err(PathweaveError::NoTransportAvailable)));
    }
}
