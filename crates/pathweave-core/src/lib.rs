use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

pub mod bundle;
pub use bundle::BundleLayer;

pub mod session;
pub use session::Session;

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
    #[error("bundle error: {0}")]
    Bundle(String),
    #[error("invalid key material")]
    InvalidKey,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// -- identity ------------------------------------------------------------

pub(crate) const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

pub(crate) fn peer_id_from_public_key(public_key: &[u8]) -> PeerId {
    PeerId(*blake3::hash(public_key).as_bytes())
}

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

pub struct NodeIdentity {
    pub(crate) private_key: Vec<u8>,
    pub(crate) public_key: Vec<u8>,
    peer_id: PeerId,
}

impl NodeIdentity {
    /// Generates a fresh Noise_XX_25519_ChaChaPoly_BLAKE2s static keypair.
    /// Panics if the system entropy source is unavailable (unrecoverable).
    pub fn generate() -> Self {
        let params: snow::params::NoiseParams = NOISE_PARAMS
            .parse()
            .expect("hardcoded noise params are valid");
        let keypair = snow::Builder::new(params)
            .generate_keypair()
            .expect("keypair generation failed: system entropy unavailable");
        let peer_id = peer_id_from_public_key(&keypair.public);
        Self {
            private_key: keypair.private,
            public_key: keypair.public,
            peer_id,
        }
    }

    /// Restores a NodeIdentity from a previously persisted 32-byte private key.
    pub fn from_bytes(private_key: &[u8]) -> Result<Self> {
        let key_array: [u8; 32] = private_key
            .try_into()
            .map_err(|_| PathweaveError::InvalidKey)?;
        let secret = x25519_dalek::StaticSecret::from(key_array);
        let public_key = x25519_dalek::PublicKey::from(&secret).as_bytes().to_vec();
        let peer_id = peer_id_from_public_key(&public_key);
        Ok(Self {
            private_key: private_key.to_vec(),
            public_key,
            peer_id,
        })
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn public_key(&self) -> &[u8] {
        &self.public_key
    }

    /// Returns the raw private key bytes. The caller is responsible for persisting these.
    pub fn private_key_bytes(&self) -> &[u8] {
        &self.private_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_valid_peer_id() {
        let identity = NodeIdentity::generate();
        let peer_id_str = identity.peer_id().to_string();
        // blake3 produces 32 bytes; base58 of 32 bytes is 43-44 characters
        assert!(
            peer_id_str.len() == 43 || peer_id_str.len() == 44,
            "unexpected peer id length: {}",
            peer_id_str.len()
        );
    }

    #[test]
    fn from_bytes_roundtrips() {
        let original = NodeIdentity::generate();
        let restored = NodeIdentity::from_bytes(original.private_key_bytes()).unwrap();
        assert_eq!(original.peer_id(), restored.peer_id());
        assert_eq!(original.public_key(), restored.public_key());
    }

    #[test]
    fn peer_id_is_deterministic() {
        let identity = NodeIdentity::generate();
        let a = NodeIdentity::from_bytes(identity.private_key_bytes()).unwrap();
        let b = NodeIdentity::from_bytes(identity.private_key_bytes()).unwrap();
        assert_eq!(a.peer_id(), b.peer_id());
    }

    #[test]
    fn from_bytes_rejects_wrong_length() {
        assert!(matches!(
            NodeIdentity::from_bytes(&[0u8; 31]),
            Err(PathweaveError::InvalidKey)
        ));
        assert!(matches!(
            NodeIdentity::from_bytes(&[0u8; 33]),
            Err(PathweaveError::InvalidKey)
        ));
    }

    #[test]
    fn distinct_identities_have_distinct_peer_ids() {
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        assert_ne!(a.peer_id(), b.peer_id());
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
