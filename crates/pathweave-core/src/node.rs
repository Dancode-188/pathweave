use std::collections::HashMap;
use std::sync::Mutex;

use futures::stream::BoxStream;

use crate::{
    MessageHandler, NodeConfig, NodeIdentity, PathweaveError, PeerAnnouncement, PeerId, Result,
    Router, Transport, TransportEvent,
};

/// The top-level entry point for the Pathweave library.
///
/// PathweaveNode wires together the Router, Session layer, and registered transports.
/// Callers create a node, register transports, inject any known peer addresses, and
/// then use the four documented API surfaces (send, on_message, events, new).
///
/// The peer table maps PeerId -> PeerAnnouncement. send() looks up the announcement
/// here; if the peer is not present, NoTransportAvailable is returned immediately.
/// Use add_peer() to inject a known address (e.g. a QUIC address from the command
/// line) or wait for a future connect() implementation that dials and learns the
/// remote PeerId via the Noise_XX handshake.
pub struct PathweaveNode {
    router: Router,
    identity: NodeIdentity,
    peers: HashMap<PeerId, PeerAnnouncement>,
    handler: Mutex<Option<Box<dyn MessageHandler>>>,
}

impl PathweaveNode {
    /// Creates a new node. `config` is accepted but unused until transport
    /// implementations are complete; callers should pass `NodeConfig::default()` for now.
    pub async fn new(_config: NodeConfig, identity: NodeIdentity) -> Result<Self> {
        Ok(Self {
            router: Router::new(),
            identity,
            peers: HashMap::new(),
            handler: Mutex::new(None),
        })
    }

    /// Registers a transport and starts its background availability monitor.
    ///
    /// Not part of the UniFFI boundary; Rust callers use this during setup.
    /// Must be called before any send() calls that depend on this transport.
    pub fn register_transport(&mut self, transport: Box<dyn Transport>) {
        self.router.register_transport(transport);
    }

    /// Records a known PeerId -> PeerAnnouncement mapping in the peer table.
    ///
    /// Not part of the UniFFI boundary. Used by pw-chat to inject a QUIC peer
    /// address resolved from the command line, and by tests to set up known peers.
    pub fn add_peer(&mut self, peer_id: PeerId, announcement: PeerAnnouncement) {
        self.peers.insert(peer_id, announcement);
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
            .get(&peer_id)
            .ok_or(PathweaveError::NoTransportAvailable)?;
        self.router
            .send(announcement, &self.identity, payload)
            .await
    }

    /// Registers a handler that will be called for each incoming message.
    ///
    /// The accept loop that delivers messages to this handler is scaffolded
    /// here but not yet wired: transport crates are stubs in v0.1.0. The
    /// handler is stored and will be used once accept loops are implemented.
    pub fn on_message(&self, handler: Box<dyn MessageHandler>) {
        *self.handler.lock().unwrap() = Some(handler);
    }

    /// Returns a stream of transport lifecycle events from the Router.
    pub fn events(&self) -> BoxStream<'_, TransportEvent> {
        self.router.events()
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

    // --- mock transport -------------------------------------------------------

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

        fn discover(&self) -> BoxStream<'_, PeerAnnouncement> {
            Box::pin(stream::empty())
        }

        async fn connect(&self, _peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
            self.connect_count.fetch_add(1, Ordering::Relaxed);
            let (a, b) = conn_pair();
            self.responder_tx.send(b).ok();
            Ok(Box::new(a))
        }

        async fn accept(&self) -> Result<Box<dyn Connection>> {
            Err(PathweaveError::Transport("not used in tests".into()))
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
}
