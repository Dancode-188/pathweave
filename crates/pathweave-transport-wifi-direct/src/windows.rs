//! Windows WiFi Direct backend via WinRT (Windows.Devices.WiFiDirect).
//!
//! WinRT COM objects are not Send. All WinRT calls are either confined to a
//! dedicated std::thread (for the publisher/listener lifecycle) or wrapped in
//! block_in_place (for one-shot calls in connect). No WinRT object is held
//! across an .await boundary. See also: BLE transport Windows backend for the
//! same pattern.
//!
//! ADR 022 documents the GO election rule and address lifecycle.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::stream::BoxStream;
use pathweave_core::{NodeIdentity, PathweaveError, PeerAddress, PeerAnnouncement, Result};
use tokio::net::TcpStream;
use windows::Devices::Enumeration::DeviceInformation;
use windows::Devices::WiFiDirect::WiFiDirectAdvertisementPublisher;
use windows::Devices::WiFiDirect::{WiFiDirectConnectionListener, WiFiDirectDevice};

use super::{Inner, WIFI_DIRECT_PORT};
use crate::WifiDirectConnection;

// --------------------------------------------------------------------------
// State held while the transport is running.
// All fields are Send; WinRT objects live on the background thread.
// --------------------------------------------------------------------------

pub(crate) struct WindowsState {
    /// Dropping this signals the background thread to stop the publisher and listener.
    _stop_tx: tokio::sync::oneshot::Sender<()>,
    pub(crate) local_short_id: [u8; 8],
}

// --------------------------------------------------------------------------
// start / stop
// --------------------------------------------------------------------------

pub(crate) async fn start(inner: &Arc<Inner>, identity: &NodeIdentity) -> Result<()> {
    let local_short_id: [u8; 8] = identity.peer_id().as_bytes()[..8]
        .try_into()
        .expect("PeerId is always 32 bytes");

    let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel::<WifiDirectConnection>();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

    // Capture the tokio handle before spawning so the background thread can
    // call handle.spawn() from within WinRT callback threads (which have no
    // tokio runtime). tokio::spawn() directly would panic outside of a runtime.
    let rt_handle = tokio::runtime::Handle::current();

    // Spawn a std::thread to own all WinRT objects for the lifetime of the transport.
    let thread_conn_tx = conn_tx.clone();
    std::thread::spawn(move || {
        publisher_thread(thread_conn_tx, stop_rx, rt_handle);
    });

    *inner.state.lock().await = Some(WindowsState {
        _stop_tx: stop_tx,
        local_short_id,
    });
    *inner.conn_rx.lock().await = Some(conn_rx);
    Ok(())
}

pub(crate) async fn stop(inner: &Arc<Inner>) -> Result<()> {
    // Dropping WindowsState drops _stop_tx, which signals the background thread
    // to call publisher.Stop() and exit. conn_tx dropping causes accept()'s
    // recv() to return None.
    *inner.state.lock().await = None;
    *inner.conn_rx.lock().await = None;
    Ok(())
}

/// Owns the WinRT publisher and listener for the lifetime of the transport.
/// Runs on a dedicated std::thread because WinRT COM objects are not Send.
fn publisher_thread(
    conn_tx: tokio::sync::mpsc::UnboundedSender<WifiDirectConnection>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
    rt_handle: tokio::runtime::Handle,
) {
    let publisher = match WiFiDirectAdvertisementPublisher::new() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("WiFi Direct: publisher creation failed: {e}");
            return;
        }
    };

    let listener = match WiFiDirectConnectionListener::new() {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!("WiFi Direct: listener creation failed: {e}");
            return;
        }
    };

    let tx = conn_tx.clone();
    let handler_result =
        listener.ConnectionRequested(&windows::Foundation::TypedEventHandler::new(
            move |_,
                  args: &Option<
                windows::Devices::WiFiDirect::WiFiDirectConnectionRequestedEventArgs,
            >| {
                if let Some(args) = args {
                    if let Ok(req) = args.GetConnectionRequest() {
                        if let Ok(device_info) = req.DeviceInformation() {
                            let tx2 = tx.clone();
                            let handle2 = rt_handle.clone();
                            // Spawn a tokio task via the captured handle; direct
                            // tokio::spawn would panic outside a runtime context.
                            handle2.spawn(async move {
                                if let Err(e) = handle_incoming(device_info, tx2).await {
                                    tracing::warn!("WiFi Direct: incoming connection error: {e}");
                                }
                            });
                        }
                    }
                }
                Ok(())
            },
        ));

    if let Err(e) = handler_result {
        tracing::warn!("WiFi Direct: ConnectionRequested handler failed: {e}");
        return;
    }

    if let Err(e) = publisher.Start() {
        tracing::warn!("WiFi Direct: publisher Start failed: {e}");
        return;
    }

    tracing::debug!("WiFi Direct: publisher started");

    // Block until stop is signalled (stop_tx dropped from WindowsState::drop).
    let _ = stop_rx.blocking_recv();

    let _ = publisher.Stop();
    tracing::debug!("WiFi Direct: publisher stopped");
}

// --------------------------------------------------------------------------
// discover
// --------------------------------------------------------------------------

pub(crate) fn discover(inner: Arc<Inner>) -> BoxStream<'static, PeerAnnouncement> {
    let (tx, rx) = futures::channel::mpsc::unbounded();

    tokio::spawn(async move {
        // Read local_short_id to filter out our own device.
        let local_short_id = inner.state.lock().await.as_ref().map(|s| s.local_short_id);

        // All device enumeration is synchronous WinRT; run in block_in_place.
        let announcements: Vec<PeerAnnouncement> =
            tokio::task::block_in_place(|| enumerate_peers(local_short_id));

        for ann in announcements {
            if tx.unbounded_send(ann).is_err() {
                return;
            }
        }
    });

    Box::pin(rx)
}

fn enumerate_peers(local_short_id: Option<[u8; 8]>) -> Vec<PeerAnnouncement> {
    let selector = match WiFiDirectDevice::GetDeviceSelector() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("WiFi Direct: GetDeviceSelector failed: {e}");
            return vec![];
        }
    };

    let devices = match DeviceInformation::FindAllAsyncAqsFilter(&selector).and_then(|op| op.get())
    {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!("WiFi Direct: device enumeration failed: {e}");
            return vec![];
        }
    };

    let mut out = Vec::new();
    let count = devices.Size().unwrap_or(0);
    for i in 0..count {
        let device = match devices.GetAt(i) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let id: windows::core::HSTRING = match device.Id() {
            Ok(id) => id,
            Err(_) => continue,
        };
        let id_str = id.to_string();

        let short_id: Option<[u8; 8]> = device
            .Name()
            .ok()
            .and_then(|n| parse_short_id_from_name(&n.to_string()));

        // Skip our own device.
        if let (Some(local), Some(peer)) = (local_short_id, short_id) {
            if local == peer {
                continue;
            }
        }

        tracing::debug!("WiFi Direct: discovered {id_str}");
        out.push(PeerAnnouncement {
            address: PeerAddress::WifiDirect(id_str),
            short_id,
        });
    }
    out
}

// --------------------------------------------------------------------------
// connect (initiator / client side)
// --------------------------------------------------------------------------

pub(crate) async fn connect(
    inner: &Arc<Inner>,
    peer: &PeerAnnouncement,
) -> Result<Box<dyn pathweave_core::Connection>> {
    let device_id = match &peer.address {
        PeerAddress::WifiDirect(id) => id.clone(),
        _ => {
            return Err(PathweaveError::Transport(
                "WiFi Direct connect: wrong address type".into(),
            ))
        }
    };

    let local_short_id = inner
        .state
        .lock()
        .await
        .as_ref()
        .map(|s| s.local_short_id)
        .ok_or_else(|| PathweaveError::Transport("WiFi Direct transport not started".into()))?;

    // Determine role (ADR 022): higher short_id becomes GO.
    let we_are_go = match peer.short_id {
        Some(their_id) => local_short_id > their_id,
        None => false,
    };

    if we_are_go {
        // We are GO. The peer will call FromIdAsync() against us. Our accept()
        // call will deliver the connection when the peer's TCP arrives.
        return Err(PathweaveError::Transport(
            "WiFi Direct: this node is GO — the remote side will initiate; \
             the router should call accept() to receive the inbound connection"
                .into(),
        ));
    }

    // We are client. Call FromIdAsync() and extract the remote IP synchronously,
    // then do the TCP connect asynchronously. No WinRT object crosses the await.
    let id_hstring = windows::core::HSTRING::from(device_id.as_str());
    let remote_ip: String = tokio::task::block_in_place(|| {
        let wifi_device = WiFiDirectDevice::FromIdAsync(&id_hstring)
            .and_then(|op| op.get())
            .map_err(|e| {
                PathweaveError::Transport(format!("WiFiDirectDevice::FromIdAsync failed: {e}"))
            })?;

        let endpoints = wifi_device.GetConnectionEndpointPairs().map_err(|e| {
            PathweaveError::Transport(format!("GetConnectionEndpointPairs failed: {e}"))
        })?;

        let ep = endpoints
            .GetAt(0)
            .map_err(|e| PathweaveError::Transport(format!("no endpoint pairs: {e}")))?;

        let hostname = ep
            .RemoteHostName()
            .map_err(|e| PathweaveError::Transport(format!("RemoteHostName failed: {e}")))?;

        hostname
            .DisplayName()
            .map(|h| h.to_string())
            .map_err(|e| PathweaveError::Transport(format!("DisplayName failed: {e}")))
    })?;

    let addr: SocketAddr = format!("{remote_ip}:{WIFI_DIRECT_PORT}")
        .parse()
        .map_err(|e| {
            PathweaveError::Transport(format!("WiFi Direct: invalid remote IP '{remote_ip}': {e}"))
        })?;

    tracing::debug!("WiFi Direct (Windows): connecting TCP to {addr}");
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| PathweaveError::Transport(format!("WiFi Direct TCP connect failed: {e}")))?;

    Ok(Box::new(WifiDirectConnection::new(stream)))
}

// --------------------------------------------------------------------------
// accept
// --------------------------------------------------------------------------

pub(crate) async fn accept(inner: &Arc<Inner>) -> Result<Box<dyn pathweave_core::Connection>> {
    let mut guard = inner.conn_rx.lock().await;
    match guard.as_mut() {
        Some(rx) => rx
            .recv()
            .await
            .map(|c| Box::new(c) as Box<dyn pathweave_core::Connection>)
            .ok_or_else(|| PathweaveError::Transport("WiFi Direct: accept channel closed".into())),
        None => Err(PathweaveError::Transport(
            "WiFi Direct transport not started".into(),
        )),
    }
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// Handles an incoming connection from the publisher_thread event handler.
/// Binds a TCP listener on the local P2P interface address and waits for the
/// client to connect.
async fn handle_incoming(
    device_info: DeviceInformation,
    conn_tx: tokio::sync::mpsc::UnboundedSender<WifiDirectConnection>,
) -> Result<()> {
    let local_ip: String = tokio::task::block_in_place(|| {
        let id = device_info
            .Id()
            .map_err(|e| PathweaveError::Transport(format!("device Id failed: {e}")))?;

        let wifi_device = WiFiDirectDevice::FromIdAsync(&id)
            .and_then(|op| op.get())
            .map_err(|e| {
                PathweaveError::Transport(format!("FromIdAsync (incoming) failed: {e}"))
            })?;

        let endpoints = wifi_device
            .GetConnectionEndpointPairs()
            .map_err(|e| PathweaveError::Transport(format!("GetConnectionEndpointPairs: {e}")))?;

        let ep = endpoints
            .GetAt(0)
            .map_err(|e| PathweaveError::Transport(format!("no endpoint pairs: {e}")))?;

        let hostname = ep
            .LocalHostName()
            .map_err(|e| PathweaveError::Transport(format!("LocalHostName failed: {e}")))?;

        hostname
            .DisplayName()
            .map(|h| h.to_string())
            .map_err(|e| PathweaveError::Transport(format!("DisplayName failed: {e}")))
    })?;

    let bind_addr: SocketAddr = format!("{local_ip}:{WIFI_DIRECT_PORT}")
        .parse()
        .map_err(|e| {
            PathweaveError::Transport(format!("WiFi Direct: invalid local IP '{local_ip}': {e}"))
        })?;

    tracing::debug!("WiFi Direct (Windows): GO listening on {bind_addr}");
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| PathweaveError::Transport(format!("TCP bind failed: {e}")))?;

    let (stream, peer_addr) = listener
        .accept()
        .await
        .map_err(|e| PathweaveError::Transport(format!("TCP accept failed: {e}")))?;

    tracing::debug!("WiFi Direct (Windows): accepted TCP from {peer_addr}");
    let _ = conn_tx.send(WifiDirectConnection::new(stream));
    Ok(())
}

/// Attempts to extract a short_id from a WinRT device name.
/// Expected format: "<P2P_SERVICE_NAME>:<hex16>".
fn parse_short_id_from_name(name: &str) -> Option<[u8; 8]> {
    let prefix = format!("{}:", super::P2P_SERVICE_NAME);
    let hex = name.strip_prefix(prefix.as_str())?;
    if hex.len() != 16 {
        return None;
    }
    let bytes = (0..8)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<std::result::Result<Vec<u8>, _>>()
        .ok()?;
    bytes.try_into().ok()
}
