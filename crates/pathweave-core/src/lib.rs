use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

pub type Result<T> = std::result::Result<T, PathweaveError>;

// -- error ---------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum PathweaveError {
    #[error("no transport available to reach peer")]
    NoTransportAvailable,
    #[error("transport error: {0}")]
    Transport(String),
    #[error("session error: {0}")]
    Session(String),
    #[error("invalid key material")]
    InvalidKey,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// -- identity ------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerId([u8; 32]);

impl PeerId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_base58(&self) -> String {
        bs58::encode(&self.0).into_string()
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_base58())
    }
}

#[allow(dead_code)]
pub struct NodeIdentity {
    pub(crate) private_key: Vec<u8>,
    pub(crate) public_key: Vec<u8>,
    peer_id: PeerId,
}

impl NodeIdentity {
    pub fn generate() -> Self {
        todo!(
            "generate Noise_XX_25519_ChaChaPoly_BLAKE2s keypair via snow, derive PeerId via blake3"
        )
    }

    pub fn from_bytes(_private_key: &[u8]) -> Result<Self> {
        todo!("restore NodeIdentity from persisted private key bytes")
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn public_key(&self) -> &[u8] {
        &self.public_key
    }
}

// -- transport abstractions ----------------------------------------------

#[derive(Debug, Clone)]
pub struct PeerAnnouncement {
    pub address: PeerAddress,
    pub short_id: Option<[u8; 8]>,
}

#[derive(Debug, Clone)]
pub enum PeerAddress {
    Quic(std::net::SocketAddr),
    Ble(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportCost {
    Free,
    Metered,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Quic,
    Ble,
}

#[derive(Debug, Clone)]
pub enum TransportEvent {
    TransportChanged {
        from: Option<TransportKind>,
        to: TransportKind,
    },
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
}

pub trait MessageHandler: Send + Sync {
    fn on_message(&self, peer_id: PeerId, payload: Vec<u8>);
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    fn discover(&self) -> BoxStream<'_, PeerAnnouncement>;
    async fn connect(&self, peer: &PeerAnnouncement) -> Result<Box<dyn Connection>>;
    async fn accept(&self) -> Result<Box<dyn Connection>>;
    fn mtu_hint(&self) -> usize;
    fn cost(&self) -> TransportCost;
    fn name(&self) -> &'static str;
}

#[async_trait]
pub trait Connection: Send + Sync {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()>;
    async fn recv_bytes(&mut self) -> Result<Bytes>;
    async fn close(&mut self) -> Result<()>;
    fn mtu(&self) -> usize;
}

// -- node ----------------------------------------------------------------

#[derive(Default)]
pub struct NodeConfig {
    pub listen_port: Option<u16>,
}

pub struct PathweaveNode;

impl PathweaveNode {
    pub async fn new(_config: NodeConfig, _identity: NodeIdentity) -> Result<Self> {
        todo!("initialize router, session layer, and transport registry")
    }

    pub async fn send(&self, _peer_id: PeerId, _payload: Vec<u8>) -> Result<()> {
        todo!("route payload through best available transport via session layer")
    }

    pub fn on_message(&self, _handler: Box<dyn MessageHandler>) {
        todo!("register message handler for incoming bundles")
    }

    pub fn events(&self) -> BoxStream<'_, TransportEvent> {
        todo!("subscribe to transport events from the router")
    }
}
