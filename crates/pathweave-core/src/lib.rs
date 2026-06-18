#![deny(clippy::all)]
#![forbid(unsafe_code)]

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub mod bundle;
pub use bundle::BundleLayer;

pub mod node;
pub use node::PathweaveNode;

pub mod router;
pub use router::Router;

pub mod session;
pub use session::Session;

pub type Result<T> = std::result::Result<T, PathweaveError>;

// -- error ---------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum PathweaveError {
    #[error("no transport available to reach peer")]
    NoTransportAvailable,
    #[error("delivery failed: all retry attempts exhausted without confirmation")]
    DeliveryFailed,
    #[error("transport error: {0}")]
    Transport(String),
    #[error("session error: {0}")]
    Session(String),
    #[error("bundle error: {0}")]
    Bundle(String),
    #[error("invalid key material")]
    InvalidKey,
    #[error("no known key for destination {0}")]
    KeyUnknown(PeerId),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// -- identity ------------------------------------------------------------

pub(crate) const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
pub(crate) const NOISE_XK_PARAMS: &str = "Noise_XK_25519_ChaChaPoly_BLAKE2s";
pub(crate) const NOISE_K_PARAMS: &str = "Noise_K_25519_ChaChaPoly_BLAKE2s";

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

#[derive(Clone)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PeerAddress {
    Quic(std::net::SocketAddr),
    Ble(String),
    WifiDirect(String),
    /// Hex-encoded 8-byte short_id of the remote peer. Broadcast transports have no
    /// connection-layer device identifier; short_id is the only addressing information
    /// available, since it is what every packet header carries. See ADR 018.
    BleAdvertising(String),
}

impl PeerAddress {
    pub fn kind(&self) -> TransportKind {
        match self {
            PeerAddress::Quic(_) => TransportKind::Quic,
            PeerAddress::Ble(_) => TransportKind::Ble,
            PeerAddress::WifiDirect(_) => TransportKind::WifiDirect,
            PeerAddress::BleAdvertising(_) => TransportKind::BleAdvertising,
        }
    }
}

impl std::fmt::Display for PeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerAddress::Quic(addr) => write!(f, "{}", addr),
            PeerAddress::Ble(id) => write!(f, "{}", id),
            PeerAddress::WifiDirect(id) => write!(f, "{}", id),
            PeerAddress::BleAdvertising(id) => write!(f, "{}", id),
        }
    }
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
    WifiDirect,
    BleAdvertising,
}

#[derive(Debug, Clone)]
pub enum TransportEvent {
    TransportChanged {
        from: Option<TransportKind>,
        to: TransportKind,
    },
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    /// Fired after each successful send(), naming the transport that carried the message.
    /// pw-chat uses this to show which transport actually delivered the last send, not
    /// which transport started most recently.
    MessageDelivered {
        peer_id: PeerId,
        transport: TransportKind,
    },
    /// Fired once per store_forward or store_forward_routed entry that expires before
    /// the destination is reached. See ADR 021.
    StoreFailed {
        peer_id: PeerId,
    },
    /// Fired whenever a key is learned for the first time (not already in the registry),
    /// whether via a direct Noise handshake or a received gossip announcement. A
    /// background task floods this onward per ADR 020; firing this event rather than
    /// flooding inline keeps the handshake/send path that triggered it non-blocking.
    KeyLearned {
        peer_id: PeerId,
        public_key: [u8; 32],
    },
}

pub trait MessageHandler: Send + Sync {
    fn on_message(&self, peer_id: PeerId, payload: Vec<u8>);
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn start(&self, identity: &NodeIdentity) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    fn discover(&self) -> BoxStream<'static, PeerAnnouncement>;
    async fn connect(&self, peer: &PeerAnnouncement) -> Result<Box<dyn Connection>>;
    async fn accept(&self) -> Result<Box<dyn Connection>>;
    fn mtu_hint(&self) -> usize;
    fn cost(&self) -> TransportCost;
    fn kind(&self) -> TransportKind;
    fn name(&self) -> &'static str;
    fn local_addresses(&self) -> Vec<PeerAddress> {
        vec![]
    }

    /// Returns a stream of addresses that have departed the discovery layer.
    ///
    /// Fires when a previously resolved peer stops advertising (e.g. mDNS record
    /// expires). The default returns an empty stream for transports that have no
    /// departure signal.
    fn departures(&self) -> BoxStream<'static, PeerAddress> {
        Box::pin(stream::empty())
    }
}

#[async_trait]
pub trait Connection: Send + Sync {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()>;
    async fn recv_bytes(&mut self) -> Result<Bytes>;
    async fn close(&mut self) -> Result<()>;
    fn mtu(&self) -> usize;
}

// -- key registry --------------------------------------------------------

/// Maps PeerId to Curve25519 static public key for the Noise_XK upgrade path and E2E
/// hop encryption. Populated after every handshake and, since #100, by gossip
/// announcements. `gossip_order` is the sole record of which entries were learned via
/// gossip rather than a direct handshake (oldest first): a peer_id's presence there,
/// not a separate field on the entry, is what makes it eligible for eviction when the
/// registry is bounded (#101). A direct handshake removes a peer_id from this tracking
/// even if it was originally learned via gossip, since the handshake confirms it
/// deserves the same protection direct entries always have. See ADR 020.
pub struct KeyRegistryState {
    entries: HashMap<PeerId, [u8; 32]>,
    gossip_order: VecDeque<PeerId>,
    max_size: Option<usize>,
}

impl KeyRegistryState {
    fn new(max_size: Option<usize>) -> Self {
        Self {
            entries: HashMap::new(),
            gossip_order: VecDeque::new(),
            max_size,
        }
    }

    pub(crate) fn get(&self, peer_id: &PeerId) -> Option<[u8; 32]> {
        self.entries.get(peer_id).copied()
    }

    pub(crate) fn remove(&mut self, peer_id: &PeerId) -> Option<[u8; 32]> {
        self.gossip_order.retain(|p| p != peer_id);
        self.entries.remove(peer_id)
    }

    /// Inserts a key learned via a direct Noise handshake. Never evicted by the size
    /// bound. Promotes the entry out of gossip-eviction tracking if it was previously
    /// gossip-learned. Returns true if this peer_id had no entry before this call, so
    /// callers can decide whether to fire TransportEvent::KeyLearned.
    pub(crate) fn insert_direct(&mut self, peer_id: PeerId, public_key: [u8; 32]) -> bool {
        self.gossip_order.retain(|p| p != &peer_id);
        self.entries.insert(peer_id, public_key).is_none()
    }

    /// Inserts a key learned via a gossip announcement (#100). If at capacity, evicts
    /// the oldest gossip-learned entry first; if every entry is directly handshaked,
    /// drops the new key instead of evicting one. See #101.
    pub(crate) fn insert_gossip(&mut self, peer_id: PeerId, public_key: [u8; 32]) {
        if let Some(existing) = self.entries.get_mut(&peer_id) {
            // Already known (direct handshake or earlier gossip): idempotent refresh,
            // no capacity check needed since this does not grow the registry.
            *existing = public_key;
            return;
        }

        if let Some(max) = self.max_size {
            if self.entries.len() >= max {
                match self.gossip_order.pop_front() {
                    Some(oldest) => {
                        self.entries.remove(&oldest);
                    }
                    None => return, // every slot is directly handshaked; drop the new key
                }
            }
        }

        self.entries.insert(peer_id.clone(), public_key);
        self.gossip_order.push_back(peer_id);
    }
}

pub(crate) type KeyRegistry = Arc<Mutex<KeyRegistryState>>;

#[cfg(test)]
pub(crate) fn new_key_registry() -> KeyRegistry {
    new_key_registry_with_bound(None)
}

pub(crate) fn new_key_registry_with_bound(max_size: Option<usize>) -> KeyRegistry {
    Arc::new(Mutex::new(KeyRegistryState::new(max_size)))
}

#[cfg(test)]
mod key_registry_tests {
    use super::KeyRegistryState;
    use crate::{NodeIdentity, PeerId};

    fn dummy_key(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn dummy_peer_id() -> PeerId {
        NodeIdentity::generate().peer_id().clone()
    }

    #[test]
    fn gossip_entries_evict_oldest_first_without_bound_overflow() {
        let mut registry = KeyRegistryState::new(Some(2));
        let p1 = dummy_peer_id();
        let p2 = dummy_peer_id();
        let p3 = dummy_peer_id();

        registry.insert_gossip(p1.clone(), dummy_key(1));
        registry.insert_gossip(p2.clone(), dummy_key(2));
        // At capacity (2). p1 is the oldest gossip entry and must be evicted to make
        // room for p3.
        registry.insert_gossip(p3.clone(), dummy_key(3));

        assert_eq!(
            registry.get(&p1),
            None,
            "oldest gossip entry must be evicted"
        );
        assert_eq!(registry.get(&p2), Some(dummy_key(2)));
        assert_eq!(registry.get(&p3), Some(dummy_key(3)));
    }

    #[test]
    fn direct_handshake_entry_is_never_evicted_by_gossip() {
        let mut registry = KeyRegistryState::new(Some(1));
        let direct_peer = dummy_peer_id();
        let gossip_peer = dummy_peer_id();

        registry.insert_direct(direct_peer.clone(), dummy_key(1));
        // At capacity (1), and the only entry is direct, not gossip-learned: the new
        // gossip key must be dropped, not evict the direct entry.
        registry.insert_gossip(gossip_peer.clone(), dummy_key(2));

        assert_eq!(registry.get(&direct_peer), Some(dummy_key(1)));
        assert_eq!(
            registry.get(&gossip_peer),
            None,
            "gossip-learned key must be dropped when only directly-handshaked entries exist to evict"
        );
    }

    #[test]
    fn direct_handshake_promotes_a_previously_gossip_learned_entry() {
        let mut registry = KeyRegistryState::new(Some(1));
        let peer = dummy_peer_id();
        let other = dummy_peer_id();

        registry.insert_gossip(peer.clone(), dummy_key(1));
        // A real handshake with the same peer now confirms the key directly; it must
        // no longer be eligible for gossip eviction.
        registry.insert_direct(peer.clone(), dummy_key(1));

        // At capacity (1) with the only entry now direct: a new gossip key must be
        // dropped rather than evicting the promoted entry.
        registry.insert_gossip(other.clone(), dummy_key(2));

        assert_eq!(registry.get(&peer), Some(dummy_key(1)));
        assert_eq!(registry.get(&other), None);
    }

    #[test]
    fn unbounded_registry_never_evicts() {
        let mut registry = KeyRegistryState::new(None);
        let peers: Vec<PeerId> = (0..50u8)
            .map(|i| {
                let peer_id = dummy_peer_id();
                registry.insert_gossip(peer_id.clone(), dummy_key(i));
                peer_id
            })
            .collect();

        for (i, peer_id) in peers.iter().enumerate() {
            assert_eq!(
                registry.get(peer_id),
                Some(dummy_key(i as u8)),
                "no entry should be evicted when max_size is None"
            );
        }
    }
}

// -- peer table ----------------------------------------------------------

/// Shared peer table: PeerId -> known addresses for routing. See ADR 016 and ADR 017.
pub(crate) type PeerTable = Arc<Mutex<HashMap<PeerId, Vec<PeerAnnouncement>>>>;

pub(crate) fn new_peer_table() -> PeerTable {
    Arc::new(Mutex::new(HashMap::new()))
}

// -- node ----------------------------------------------------------------

#[derive(Default)]
pub struct NodeConfig {
    pub listen_port: Option<u16>,
    /// How long to hold a payload in the store-and-forward queue before expiring it.
    /// `None` uses the 24-hour default. See ADR 021.
    pub store_ttl: Option<Duration>,
    /// Maximum number of payloads held per destination in the store-and-forward queue.
    /// When a destination's queue is full, the incoming payload is dropped and
    /// `TransportEvent::StoreFailed` is fired immediately. `None` means no bound.
    pub max_queue_depth: Option<usize>,
    /// Maximum number of entries in the key registry. When at capacity, the oldest
    /// gossip-learned entry (#100) is evicted first; if every entry was learned via a
    /// direct handshake, a new gossip-learned key is dropped instead of evicting one.
    /// `None` means no bound. See #101.
    pub max_key_registry_size: Option<usize>,
}
