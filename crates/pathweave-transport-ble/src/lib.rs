use std::sync::Arc;

use async_trait::async_trait;
use btleplug::{
    api::{
        Central, CentralEvent, Characteristic, Manager as _, Peripheral as _, ScanFilter, WriteType,
    },
    platform::{Adapter, Manager, Peripheral},
};
use bytes::Bytes;
use futures::{channel::mpsc, stream::BoxStream, StreamExt};
use pathweave_core::{
    Connection, NodeIdentity, PathweaveError, PeerAddress, PeerAnnouncement, Result, Transport,
    TransportCost, TransportKind,
};
use tokio::sync::Mutex;
use uuid::{uuid, Uuid};

// --------------------------------------------------------------------------
// Protocol constants (defined in ARCHITECTURE.md)
// --------------------------------------------------------------------------

const PATHWEAVE_SERVICE_UUID: Uuid = uuid!("82dfc0ba-e2b5-4e65-ad11-c7238ca545c9");
const WRITE_CHAR_UUID: Uuid = uuid!("3439992c-8453-4ca3-9688-639ef5f6f5dc");
const NOTIFY_CHAR_UUID: Uuid = uuid!("6de63378-8bc3-4e87-8892-0a9a80efff64");

// Service data format: [0x01] ++ [short_id: 8 bytes]
const ADVERTISEMENT_VERSION: u8 = 0x01;

// --------------------------------------------------------------------------
// Linux-only peripheral types (ADR 014)
// --------------------------------------------------------------------------

#[cfg(target_os = "linux")]
struct BlePeripheralState {
    _adv_handle: bluer::adv::AdvertisementHandle,
    _app_handle: bluer::gatt::local::ApplicationHandle,
    conn_rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<BlePeripheralConnection>>>,
}

#[cfg(target_os = "linux")]
struct BlePeripheralConnection {
    write_rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<Bytes>>,
    reply_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

#[cfg(target_os = "linux")]
#[async_trait]
impl Connection for BlePeripheralConnection {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.reply_tx
            .send(Bytes::copy_from_slice(bytes))
            .map_err(|_| PathweaveError::Transport("peripheral reply channel closed".into()))
    }

    async fn recv_bytes(&mut self) -> Result<Bytes> {
        self.write_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| PathweaveError::Transport("peripheral write channel closed".into()))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn mtu(&self) -> usize {
        512
    }
}

// peripheral_loop: sole owner of CharacteristicWriter, bridges bluer events to connections.
//
// Creates a BlePeripheralConnection on Subscribe (Notify event) and forwards each
// Write event's bytes to the connection's internal channel. Multiple writes from the
// same subscriber map to multiple channel sends, allowing Session::respond() to call
// recv_bytes() twice for the Noise_XX handshake (messages 1 and 3).
//
// Rejects a second subscriber while a connection is active (v0.2.0 limitation).
// Clears active_write_tx when the connection's receiver is dropped, allowing the
// next subscriber to open a fresh connection.
#[cfg(target_os = "linux")]
async fn peripheral_loop(
    mut write_ctrl: bluer::gatt::local::CharacteristicControl,
    mut notify_ctrl: bluer::gatt::local::CharacteristicControl,
    conn_tx: tokio::sync::mpsc::UnboundedSender<BlePeripheralConnection>,
) {
    use bluer::gatt::local::CharacteristicControlEvent;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let mut writer: Option<bluer::gatt::local::CharacteristicWriter> = None;
    let mut active_write_tx: Option<tokio::sync::mpsc::UnboundedSender<Bytes>> = None;

    loop {
        tokio::select! {
            evt = notify_ctrl.next() => match evt {
                Some(CharacteristicControlEvent::Notify(w)) => {
                    if active_write_tx.as_ref().is_some_and(|tx| !tx.is_closed()) {
                        tracing::debug!("BLE peripheral: second subscriber while connection active; ignored");
                    } else {
                        writer = Some(w);
                        let (write_tx, write_rx) = tokio::sync::mpsc::unbounded_channel();
                        active_write_tx = Some(write_tx);
                        let _ = conn_tx.send(BlePeripheralConnection {
                            write_rx: Mutex::new(write_rx),
                            reply_tx: reply_tx.clone(),
                        });
                    }
                }
                _ => return,
            },
            evt = write_ctrl.next() => match evt {
                Some(CharacteristicControlEvent::Write(req)) => {
                    match &active_write_tx {
                        Some(tx) => {
                            if let Ok(mut reader) = req.accept() {
                                let mut buf = Vec::new();
                                if reader.read_to_end(&mut buf).await.is_ok() {
                                    if tx.send(Bytes::from(buf)).is_err() {
                                        active_write_tx = None;
                                    }
                                }
                            }
                        }
                        None => {
                            let _ = req.accept();
                            tracing::debug!("BLE peripheral: write with no active connection; frame discarded");
                        }
                    }
                }
                _ => return,
            },
            data = reply_rx.recv(), if writer.is_some() => {
                if let (Some(bytes), Some(w)) = (data, writer.as_mut()) {
                    if w.write_all(&bytes).await.is_err() {
                        writer = None;
                        active_write_tx = None;
                    }
                }
            },
        }
    }
}

// --------------------------------------------------------------------------
// Transport
// --------------------------------------------------------------------------

pub struct BleTransport {
    adapter: Arc<Mutex<Option<Adapter>>>,
    #[cfg(target_os = "linux")]
    peripheral: Arc<Mutex<Option<BlePeripheralState>>>,
}

impl BleTransport {
    pub fn new() -> Self {
        Self {
            adapter: Arc::new(Mutex::new(None)),
            #[cfg(target_os = "linux")]
            peripheral: Arc::new(Mutex::new(None)),
        }
    }
}

impl Default for BleTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for BleTransport {
    async fn start(&self, identity: &NodeIdentity) -> Result<()> {
        // Central mode: acquire btleplug adapter for scanning.
        let manager = Manager::new()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let adapters = manager
            .adapters()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let adapter = adapters
            .into_iter()
            .next()
            .ok_or_else(|| PathweaveError::Transport("no Bluetooth adapter found".into()))?;
        *self.adapter.lock().await = Some(adapter);

        // Peripheral mode: register GATT application and begin advertising (Linux only).
        #[cfg(target_os = "linux")]
        self.start_peripheral(identity).await?;

        // Suppress unused-variable warning on non-Linux.
        #[cfg(not(target_os = "linux"))]
        let _ = identity;

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut guard = self.adapter.lock().await;
        if let Some(adapter) = guard.as_ref() {
            let _ = adapter.stop_scan().await;
        }
        *guard = None;

        #[cfg(target_os = "linux")]
        {
            // Dropping BlePeripheralState drops _adv_handle and _app_handle,
            // which unregisters the advertisement and GATT application via bluer's
            // RAII guards. This also closes peripheral_loop's control streams,
            // causing it to exit and drop conn_tx, which unblocks any waiting
            // accept() call (recv() returns None).
            *self.peripheral.lock().await = None;
        }

        Ok(())
    }

    /// Returns a stream of nearby Pathweave peers found via BLE scanning.
    ///
    /// Starts a background task that filters `ServiceDataAdvertisement` events
    /// for the Pathweave service UUID and decodes the short_id from service data.
    /// The stream ends when the sender is dropped (i.e., after `stop()` clears the
    /// adapter and the event stream closes).
    fn discover(&self) -> BoxStream<'static, PeerAnnouncement> {
        let adapter_arc = Arc::clone(&self.adapter);
        let (tx, rx) = mpsc::unbounded::<PeerAnnouncement>();

        tokio::spawn(async move {
            let adapter = {
                let guard = adapter_arc.lock().await;
                match guard.as_ref() {
                    Some(a) => a.clone(),
                    None => return,
                }
            };

            let filter = ScanFilter {
                services: vec![PATHWEAVE_SERVICE_UUID],
            };
            if let Err(e) = adapter.start_scan(filter).await {
                tracing::warn!("BLE scan failed to start: {}", e);
                return;
            }

            let events = match adapter.events().await {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("BLE events stream unavailable: {}", e);
                    return;
                }
            };
            futures::pin_mut!(events);

            while let Some(event) = events.next().await {
                if let CentralEvent::ServiceDataAdvertisement { id, service_data } = event {
                    let data = match service_data.get(&PATHWEAVE_SERVICE_UUID) {
                        Some(d) => d.clone(),
                        None => continue,
                    };
                    // Validate advertisement format: [0x01] ++ [short_id: 8 bytes]
                    if data.len() < 9 || data[0] != ADVERTISEMENT_VERSION {
                        continue;
                    }
                    let short_id: [u8; 8] = data[1..9].try_into().unwrap();
                    let announcement = PeerAnnouncement {
                        address: PeerAddress::Ble(id.to_string()),
                        short_id: Some(short_id),
                    };
                    if tx.unbounded_send(announcement).is_err() {
                        break; // receiver dropped
                    }
                }
            }
        });

        Box::pin(rx)
    }

    /// Connects to a previously discovered BLE peer.
    ///
    /// The peer must have been seen via `discover()` before this is called because
    /// `btleplug` looks up the peripheral from the adapter's internal scan cache.
    async fn connect(&self, peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
        let id_str = match &peer.address {
            PeerAddress::Ble(s) => s.clone(),
            _ => return Err(PathweaveError::Transport("expected BLE address".into())),
        };

        let adapter = {
            let guard = self.adapter.lock().await;
            guard
                .as_ref()
                .ok_or_else(|| PathweaveError::Transport("transport not started".into()))?
                .clone()
        };

        // Find the peripheral in the scan cache by matching its platform ID string.
        // The format is platform-specific (BlueZ device path on Linux, UUID on macOS,
        // BDAddr on Windows), but we always store whatever id().to_string() returns in
        // discover(), so the comparison is consistent within a session.
        let peripherals = adapter
            .peripherals()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let peripheral = peripherals
            .into_iter()
            .find(|p| p.id().to_string() == id_str)
            .ok_or_else(|| {
                PathweaveError::Transport(format!(
                    "BLE peer {} not in scan cache; discover() must complete first",
                    id_str
                ))
            })?;

        peripheral
            .connect()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        peripheral
            .discover_services()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let chars = peripheral.characteristics();

        let write_char = chars
            .iter()
            .find(|c| c.uuid == WRITE_CHAR_UUID)
            .ok_or_else(|| PathweaveError::Transport("write characteristic not found".into()))?
            .clone();

        let notify_char = chars
            .iter()
            .find(|c| c.uuid == NOTIFY_CHAR_UUID)
            .ok_or_else(|| PathweaveError::Transport("notify characteristic not found".into()))?
            .clone();

        peripheral
            .subscribe(&notify_char)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let notifications = peripheral
            .notifications()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        // btleplug's notification stream is Send but not Sync. Bridge it to a
        // tokio channel via a background task; the receiver goes into a Mutex to
        // satisfy Connection: Sync.
        let (notify_tx, notify_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        tokio::spawn(async move {
            futures::pin_mut!(notifications);
            while let Some(notif) = notifications.next().await {
                if notify_tx.send(Bytes::from(notif.value)).is_err() {
                    break;
                }
            }
        });

        Ok(Box::new(BleConnection {
            peripheral,
            write_char,
            rx: tokio::sync::Mutex::new(notify_rx),
        }))
    }

    #[cfg(target_os = "linux")]
    async fn accept(&self) -> Result<Box<dyn Connection>> {
        // Clone the Arc to conn_rx inside the lock, then release the outer lock before
        // awaiting recv(). This prevents a deadlock with stop(): if accept() held the
        // outer lock across recv(), stop() could not acquire it to drop BlePeripheralState
        // and unblock the recv() (see ADR 014 rationale).
        let rx = {
            let guard = self.peripheral.lock().await;
            let state = guard
                .as_ref()
                .ok_or_else(|| PathweaveError::Transport("transport not started".into()))?;
            Arc::clone(&state.conn_rx)
        };
        rx.lock()
            .await
            .recv()
            .await
            .map(|c| Box::new(c) as Box<dyn Connection>)
            .ok_or_else(|| PathweaveError::Transport("peripheral loop ended".into()))
    }

    #[cfg(not(target_os = "linux"))]
    async fn accept(&self) -> Result<Box<dyn Connection>> {
        Err(PathweaveError::Transport(
            "BLE peripheral mode requires Linux with BlueZ".into(),
        ))
    }

    fn mtu_hint(&self) -> usize {
        512
    }

    fn cost(&self) -> TransportCost {
        TransportCost::Free
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Ble
    }

    fn name(&self) -> &'static str {
        "ble"
    }
}

#[cfg(target_os = "linux")]
impl BleTransport {
    async fn start_peripheral(&self, identity: &NodeIdentity) -> Result<()> {
        use bluer::gatt::local::{
            characteristic_control, Application, Characteristic, CharacteristicNotify,
            CharacteristicNotifyMethod, CharacteristicWrite, CharacteristicWriteMethod, Service,
        };
        use std::collections::BTreeMap;

        let short_id: [u8; 8] = identity.peer_id().as_bytes()[..8].try_into().unwrap();

        let session = bluer::Session::new()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let adapter = session
            .default_adapter()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let (write_ctrl, write_handle) = characteristic_control();
        let (notify_ctrl, notify_handle) = characteristic_control();

        let app = Application {
            services: vec![Service {
                uuid: PATHWEAVE_SERVICE_UUID,
                primary: true,
                characteristics: vec![
                    Characteristic {
                        uuid: WRITE_CHAR_UUID,
                        write: Some(CharacteristicWrite {
                            write_without_response: true,
                            method: CharacteristicWriteMethod::Io,
                            ..Default::default()
                        }),
                        control_handle: write_handle,
                        ..Default::default()
                    },
                    Characteristic {
                        uuid: NOTIFY_CHAR_UUID,
                        notify: Some(CharacteristicNotify {
                            notify: true,
                            method: CharacteristicNotifyMethod::Io,
                            ..Default::default()
                        }),
                        control_handle: notify_handle,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let app_handle = adapter
            .serve_gatt_application(app)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let mut svc_data = BTreeMap::new();
        let mut payload = Vec::with_capacity(9);
        payload.push(ADVERTISEMENT_VERSION);
        payload.extend_from_slice(&short_id);
        svc_data.insert(PATHWEAVE_SERVICE_UUID, payload);

        let adv = bluer::adv::Advertisement {
            advertisement_type: bluer::adv::Type::Peripheral,
            service_data: svc_data,
            ..Default::default()
        };

        let adv_handle = adapter
            .advertise(adv)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(peripheral_loop(write_ctrl, notify_ctrl, conn_tx));

        *self.peripheral.lock().await = Some(BlePeripheralState {
            _adv_handle: adv_handle,
            _app_handle: app_handle,
            conn_rx: Arc::new(Mutex::new(conn_rx)),
        });

        Ok(())
    }
}

// --------------------------------------------------------------------------
// Central-mode connection (btleplug)
// --------------------------------------------------------------------------

pub struct BleConnection {
    peripheral: Peripheral,
    write_char: Characteristic,
    // GATT is message-oriented: each notification is a complete frame.
    // No length-prefix framing is needed (unlike QUIC's byte stream).
    // Wrapped in Mutex to satisfy Connection: Sync; recv_bytes holds &mut self
    // so there is never actual contention on the lock.
    rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Bytes>>,
}

#[async_trait]
impl Connection for BleConnection {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.peripheral
            .write(&self.write_char, bytes, WriteType::WithoutResponse)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))
    }

    async fn recv_bytes(&mut self) -> Result<Bytes> {
        match self.rx.lock().await.recv().await {
            Some(bytes) => Ok(bytes),
            None => Err(PathweaveError::Transport(
                "BLE notification stream ended".into(),
            )),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.peripheral
            .disconnect()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))
    }

    fn mtu(&self) -> usize {
        512
    }
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pathweave_core::{PeerAddress, PeerAnnouncement};

    #[tokio::test]
    async fn wrong_address_type_returns_error() {
        let transport = BleTransport::new();
        // start() not called: adapter is None
        let peer = PeerAnnouncement {
            address: PeerAddress::Quic("127.0.0.1:1234".parse().unwrap()),
            short_id: None,
        };
        let result = transport.connect(&peer).await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }

    #[tokio::test]
    async fn connect_before_start_returns_error() {
        let transport = BleTransport::new();
        let peer = PeerAnnouncement {
            address: PeerAddress::Ble("some-peripheral-id".into()),
            short_id: None,
        };
        let result = transport.connect(&peer).await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn accept_returns_not_implemented_on_non_linux() {
        let transport = BleTransport::new();
        let result = transport.accept().await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn accept_before_start_returns_error() {
        let transport = BleTransport::new();
        let result = transport.accept().await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }
}
