#![deny(clippy::all)]
#![forbid(unsafe_code)]

//! WiFi Direct (IEEE 802.11 P2P) transport for Pathweave. Linux and Windows only.
//! See ADR 022 for the full design rationale, GO election rule, and address lifecycle.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use pathweave_core::{
    Connection, NodeIdentity, PeerAddress, PeerAnnouncement, Result, Transport, TransportCost,
    TransportKind,
};
use tokio::sync::Mutex;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "windows")]
mod windows;

// --------------------------------------------------------------------------
// Service name embedded in P2P service records. Both platforms advertise and
// filter by this name so that cross-platform discovery works (ADR 022).
// --------------------------------------------------------------------------

pub(crate) const P2P_SERVICE_NAME: &str = "pathweave-p2p";

// The TCP port both sides listen on / connect to once the P2P IP link is up.
pub(crate) const WIFI_DIRECT_PORT: u16 = 47808;

// --------------------------------------------------------------------------
// Platform-independent TCP connection (ADR 022)
//
// After P2P group formation both ends have a routable IP on the P2P interface.
// The connection is a TCP stream with a 4-byte big-endian length prefix,
// matching the framing used by QuicConnection (ADR 004).
// --------------------------------------------------------------------------

pub(crate) struct WifiDirectConnection {
    stream: Mutex<tokio::net::TcpStream>,
}

impl WifiDirectConnection {
    pub(crate) fn new(stream: tokio::net::TcpStream) -> Self {
        Self {
            stream: Mutex::new(stream),
        }
    }
}

#[async_trait]
impl Connection for WifiDirectConnection {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut guard = self.stream.lock().await;
        let len = (bytes.len() as u32).to_be_bytes();
        guard
            .write_all(&len)
            .await
            .map_err(|e| pathweave_core::PathweaveError::Transport(e.to_string()))?;
        guard
            .write_all(bytes)
            .await
            .map_err(|e| pathweave_core::PathweaveError::Transport(e.to_string()))
    }

    async fn recv_bytes(&mut self) -> Result<Bytes> {
        use tokio::io::AsyncReadExt;
        let mut guard = self.stream.lock().await;
        let mut len_buf = [0u8; 4];
        guard
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| pathweave_core::PathweaveError::Transport(e.to_string()))?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        guard
            .read_exact(&mut buf)
            .await
            .map_err(|e| pathweave_core::PathweaveError::Transport(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    async fn close(&mut self) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut guard = self.stream.lock().await;
        guard
            .shutdown()
            .await
            .map_err(|e| pathweave_core::PathweaveError::Transport(e.to_string()))
    }

    fn mtu(&self) -> usize {
        // TCP has no inherent message size limit. Return the practical maximum
        // that fits within a single WiFi Direct frame without IP fragmentation.
        1400
    }
}

// --------------------------------------------------------------------------
// WifiDirectTransport
// --------------------------------------------------------------------------

/// WiFi Direct transport. Supported on Linux and Windows only.
///
/// Registers the node as a WiFi Direct peer using the platform's P2P APIs,
/// advertises the `pathweave-p2p` service name, and implements the `Transport`
/// trait over TCP connections established after P2P group formation.
///
/// See ADR 022 for the Group Owner election rule, address lifecycle, and
/// rationale for the design choices made here.
pub struct WifiDirectTransport {
    inner: Arc<Inner>,
}

struct Inner {
    #[cfg(target_os = "linux")]
    state: Mutex<Option<linux::LinuxState>>,
    #[cfg(target_os = "linux")]
    conn_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<WifiDirectConnection>>>,

    #[cfg(target_os = "windows")]
    state: Mutex<Option<windows::WindowsState>>,
    #[cfg(target_os = "windows")]
    conn_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<WifiDirectConnection>>>,
}

impl WifiDirectTransport {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                #[cfg(any(target_os = "linux", target_os = "windows"))]
                state: Mutex::new(None),
                #[cfg(any(target_os = "linux", target_os = "windows"))]
                conn_rx: Mutex::new(None),
            }),
        }
    }
}

impl Default for WifiDirectTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for WifiDirectTransport {
    async fn start(&self, identity: &NodeIdentity) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            linux::start(&self.inner, identity).await
        }
        #[cfg(target_os = "windows")]
        {
            windows::start(&self.inner, identity).await
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            let _ = identity;
            Err(pathweave_core::PathweaveError::Transport(
                "WiFi Direct is not supported on this platform. \
                 Linux (wpa_supplicant) and Windows (WinRT) only."
                    .into(),
            ))
        }
    }

    async fn stop(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            linux::stop(&self.inner).await
        }
        #[cfg(target_os = "windows")]
        {
            windows::stop(&self.inner).await
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            Ok(())
        }
    }

    fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
        #[cfg(target_os = "linux")]
        {
            linux::discover(Arc::clone(&self.inner))
        }
        #[cfg(target_os = "windows")]
        {
            windows::discover(Arc::clone(&self.inner))
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            Box::pin(futures::stream::empty())
        }
    }

    async fn connect(&self, peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
        #[cfg(target_os = "linux")]
        {
            linux::connect(&self.inner, peer).await
        }
        #[cfg(target_os = "windows")]
        {
            windows::connect(&self.inner, peer).await
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            let _ = peer;
            Err(pathweave_core::PathweaveError::Transport(
                "WiFi Direct is not supported on this platform.".into(),
            ))
        }
    }

    async fn accept(&self) -> Result<Box<dyn Connection>> {
        #[cfg(target_os = "linux")]
        {
            linux::accept(&self.inner).await
        }
        #[cfg(target_os = "windows")]
        {
            windows::accept(&self.inner).await
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            futures::future::pending::<()>().await;
            unreachable!()
        }
    }

    fn mtu_hint(&self) -> usize {
        1400
    }

    fn cost(&self) -> TransportCost {
        TransportCost::Free
    }

    fn kind(&self) -> TransportKind {
        TransportKind::WifiDirect
    }

    fn name(&self) -> &'static str {
        "wifi-direct"
    }

    fn local_addresses(&self) -> Vec<PeerAddress> {
        // WiFi Direct addresses are ephemeral and P2P-interface-specific.
        // The transport does not advertise a persistent local address; peers
        // discover this node via the P2P service record, not via address exchange.
        vec![]
    }
}
