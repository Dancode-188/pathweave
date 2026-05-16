use std::sync::Arc;

use async_trait::async_trait;
use btleplug::{
    api::{
        Central, CentralEvent, CharPropFlags, Characteristic, Manager as _, Peripheral as _,
        ScanFilter, WriteType,
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
// Shared peripheral connection type (Linux and Windows, ADR 014)
// --------------------------------------------------------------------------

// BlePeripheralConnection is platform-independent: the peripheral task on each
// platform wires its platform-specific events into these channels and hands the
// connection to accept(). Session::respond() then drives the Noise_XX handshake
// over recv_bytes/send_bytes without knowing what's underneath.
#[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
struct BlePeripheralConnection {
    write_rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<Bytes>>,
    reply_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

#[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
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

// --------------------------------------------------------------------------
// Linux-only peripheral types (ADR 014)
// --------------------------------------------------------------------------

#[cfg(target_os = "linux")]
struct BlePeripheralState {
    _adv_handle: bluer::adv::AdvertisementHandle,
    _app_handle: bluer::gatt::local::ApplicationHandle,
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
    let mut writer = None;
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
                                if reader.read_to_end(&mut buf).await.is_ok()
                                    && tx.send(Bytes::from(buf)).is_err()
                                {
                                    active_write_tx = None;
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
// Windows-only peripheral types (ADR 014, Windows section)
// --------------------------------------------------------------------------

#[cfg(target_os = "windows")]
fn uuid_to_guid(uuid: &Uuid) -> windows::core::GUID {
    let b = uuid.as_bytes();
    windows::core::GUID::from_values(
        u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
        u16::from_be_bytes([b[4], b[5]]),
        u16::from_be_bytes([b[6], b[7]]),
        [b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]],
    )
}

#[cfg(target_os = "windows")]
fn bytes_to_ibuffer(bytes: &[u8]) -> windows::core::Result<windows::Storage::Streams::IBuffer> {
    let writer = windows::Storage::Streams::DataWriter::new()?;
    writer.WriteBytes(bytes)?;
    writer.DetachBuffer()
}

// BlePeripheralState on Windows holds only the stop signal. The background task
// (windows_peripheral_task) owns the GattServiceProvider and calls StopAdvertising()
// when the stop signal fires.
//
// StopAdvertising() stops BLE advertisement packets but does NOT close the
// WriteRequested or SubscribedClientsChanged event registrations. Dropping this
// struct (which drops _stop_signal) is therefore the only mechanism that exits
// windows_peripheral_task and drops conn_tx, unblocking any waiting accept() call.
// Calling StopAdvertising() directly in stop() without this signal would leave
// peripheral_task running and accept() blocked indefinitely.
#[cfg(target_os = "windows")]
struct BlePeripheralState {
    _stop_signal: tokio::sync::oneshot::Sender<()>,
}

// windows_peripheral_task: Windows equivalent of peripheral_loop.
//
// WinRT GATT server events arrive as TypedEventHandler callbacks on Windows thread
// pool threads, not as an async stream. start_peripheral() registers the handlers
// and bridges them to tokio channels; this task drives the connection lifecycle from
// the tokio side.
//
// Rejects a second subscriber while a connection is active (v0.2.0 limitation).
// Calls StopAdvertising() and exits when the stop signal fires.
#[cfg(target_os = "windows")]
async fn windows_peripheral_task(
    service_provider: windows::Devices::Bluetooth::GenericAttributeProfile::GattServiceProvider,
    notify_char: windows::Devices::Bluetooth::GenericAttributeProfile::GattLocalCharacteristic,
    conn_tx: tokio::sync::mpsc::UnboundedSender<BlePeripheralConnection>,
    write_forward: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<Bytes>>>>,
    mut subscribe_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    'outer: loop {
        tokio::select! {
            _ = &mut stop_rx => break 'outer,
            msg = subscribe_rx.recv() => {
                if msg.is_none() { break 'outer; }
            }
        }

        let (write_tx, write_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

        if let Ok(mut guard) = write_forward.lock() {
            *guard = Some(write_tx);
        }

        let _ = conn_tx.send(BlePeripheralConnection {
            write_rx: Mutex::new(write_rx),
            reply_tx,
        });

        loop {
            tokio::select! {
                _ = &mut stop_rx => break 'outer,
                data = reply_rx.recv() => {
                    match data {
                        Some(bytes) => {
                            if let Ok(buffer) = bytes_to_ibuffer(&bytes) {
                                if let Ok(op) = notify_char.NotifyValueAsync(&buffer) {
                                    let _ = tokio::task::block_in_place(|| op.get());
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = subscribe_rx.recv() => {
                    tracing::debug!("BLE peripheral (Windows): second subscriber while connection active; ignored");
                }
            }
        }

        if let Ok(mut guard) = write_forward.lock() {
            *guard = None;
        }
    }

    if let Ok(mut guard) = write_forward.lock() {
        *guard = None;
    }
    let _ = service_provider.StopAdvertising();
}

// --------------------------------------------------------------------------
// macOS-only peripheral types (ADR 014, macOS section)
// --------------------------------------------------------------------------

// macOS mirrors Windows in lifecycle: _stop_signal drop exits macos_peripheral_task
// and drops conn_tx, releasing any blocked accept() call. stopAdvertising() alone
// does not unblock the channel receive.
#[cfg(target_os = "macos")]
struct BlePeripheralState {
    _stop_signal: tokio::sync::oneshot::Sender<()>,
}

// PeripheralDelegateBridge holds the channels that macos_peripheral_task reads from.
// The delegate fires CoreBluetooth callbacks on its dispatch queue thread and sends
// into these channels without any tokio runtime involvement.
#[cfg(target_os = "macos")]
struct PeripheralDelegateBridge {
    subscribe_tx: tokio::sync::mpsc::UnboundedSender<()>,
    write_forward: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<Bytes>>>>,
}

// MacosPeripheralDelegate: Objective-C class that implements CBPeripheralManagerDelegate.
//
// The write characteristic is declared with CBCharacteristicPropertyWrite (write with
// response), not WriteWithoutResponse. CoreBluetooth's peripheralManager:didReceiveWriteRequests:
// is only called for write-with-response; write commands are silently dropped on macOS.
// This means the central must use WriteType::WithResponse when connecting to a macOS
// peripheral, which BleConnection::send_bytes detects from the characteristic properties.
#[cfg(target_os = "macos")]
mod macos_delegate {
    use super::*;
    use objc2::rc::Retained;
    use objc2::{define_class, msg_send, AllocAnyThread, DefinedClass};
    use objc2_core_bluetooth::{
        CBATTError, CBCentral, CBCharacteristic, CBPeripheralManager, CBPeripheralManagerDelegate,
    };
    use objc2_foundation::{NSObject, NSObjectProtocol};

    pub struct Ivars {
        pub bridge: Arc<super::PeripheralDelegateBridge>,
    }

    define_class!(
        // No thread_kind attribute: the default allows use from any thread.
        // MainThreadOnly would require a main thread token at allocation time,
        // which is unavailable on a tokio worker thread or dispatch queue thread.
        #[unsafe(super(NSObject))]
        #[name = "PathweaveMacosPeripheralDelegate"]
        #[ivars = Ivars]
        pub struct MacosPeripheralDelegate;

        unsafe impl NSObjectProtocol for MacosPeripheralDelegate {}

        unsafe impl CBPeripheralManagerDelegate for MacosPeripheralDelegate {
            #[unsafe(method(peripheralManagerDidUpdateState:))]
            fn did_update_state(&self, manager: &CBPeripheralManager) {
                tracing::debug!("CBPeripheralManager state changed: {:?}", unsafe {
                    manager.state()
                });
            }

            #[unsafe(method(peripheralManager:central:didSubscribeToCharacteristic:))]
            fn did_subscribe(
                &self,
                _manager: &CBPeripheralManager,
                _central: &CBCentral,
                _characteristic: &CBCharacteristic,
            ) {
                let _ = self.ivars().bridge.subscribe_tx.send(());
            }

            // Called only for CBCharacteristicPropertyWrite (write with response).
            // Must call respondToRequest:withResult: for every request.
            // WriteWithoutResponse commands are NOT delivered here.
            #[unsafe(method(peripheralManager:didReceiveWriteRequests:))]
            fn did_receive_writes(
                &self,
                manager: &CBPeripheralManager,
                requests: &objc2_foundation::NSArray<objc2_core_bluetooth::CBATTRequest>,
            ) {
                for req in unsafe { requests.iter() } {
                    if let Some(data) = unsafe { req.value() } {
                        let bytes = Bytes::copy_from_slice(unsafe { data.as_bytes_unchecked() });
                        if let Ok(guard) = self.ivars().bridge.write_forward.lock() {
                            if let Some(tx) = guard.as_ref() {
                                let _ = tx.send(bytes);
                            } else {
                                tracing::debug!(
                                    "BLE peripheral (macOS): write with no active connection; frame discarded"
                                );
                            }
                        }
                    }
                }
                // Respond to the first request; CoreBluetooth applies it to the batch.
                if let Some(first) = unsafe { requests.firstObject_unchecked() } {
                    unsafe { manager.respondToRequest_withResult(first, CBATTError::Success) };
                }
            }
        }
    );

    impl MacosPeripheralDelegate {
        pub fn new(bridge: Arc<super::PeripheralDelegateBridge>) -> Retained<Self> {
            // set_ivars writes the Rust ivars into the allocated object memory before
            // calling NSObject's init, required by objc2's DefinedClass contract.
            let this = Self::alloc();
            let this = this.set_ivars(Ivars { bridge });
            unsafe { msg_send![super(this), init] }
        }
    }
}

// SendPtr<T>: raw pointer wrapper that is Send. CBPeripheralManager and
// CBMutableCharacteristic are not Send on their own; wrapping them here lets
// macos_peripheral_task's future be Send so tokio::spawn accepts it.
#[cfg(target_os = "macos")]
struct SendPtr<T>(*mut T);
#[cfg(target_os = "macos")]
unsafe impl<T> Send for SendPtr<T> {}

// macos_peripheral_task: macOS equivalent of peripheral_loop / windows_peripheral_task.
//
// CBPeripheralManager and CBMutableCharacteristic are passed as SendPtr<T> because
// those ObjC types are not Send. start_peripheral leaks the Retained<> objects before
// spawning this task; this task reconstructs them from raw pointers (via
// Retained::from_raw) on exit, releasing the ObjC objects. The delegate is not leaked:
// the manager holds its own ObjC retain, so the Rust Retained<delegate> is allowed to
// drop at the end of start_peripheral, reducing the count from 2 to 1. When the manager
// is released here, the count goes from 1 to 0 and the delegate is freed.
#[cfg(target_os = "macos")]
async fn macos_peripheral_task(
    manager: SendPtr<objc2_core_bluetooth::CBPeripheralManager>,
    notify_char: SendPtr<objc2_core_bluetooth::CBMutableCharacteristic>,
    conn_tx: tokio::sync::mpsc::UnboundedSender<BlePeripheralConnection>,
    write_forward: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<Bytes>>>>,
    mut subscribe_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    'outer: loop {
        tokio::select! {
            _ = &mut stop_rx => break 'outer,
            msg = subscribe_rx.recv() => {
                if msg.is_none() { break 'outer; }
            }
        }

        let (write_tx, write_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

        if let Ok(mut guard) = write_forward.lock() {
            *guard = Some(write_tx);
        }
        let _ = conn_tx.send(BlePeripheralConnection {
            write_rx: Mutex::new(write_rx),
            reply_tx,
        });

        loop {
            tokio::select! {
                _ = &mut stop_rx => break 'outer,
                data = reply_rx.recv() => {
                    match data {
                        Some(bytes) => {
                            let mgr = manager.0;
                            let chr = notify_char.0;
                            tokio::task::block_in_place(|| {
                                let data = unsafe {
                                    objc2_foundation::NSData::with_bytes(bytes.as_ref())
                                };
                                unsafe {
                                    (*mgr).updateValue_forCharacteristic_onSubscribedCentrals(
                                        &data,
                                        &*chr,
                                        None,
                                    );
                                }
                            });
                        }
                        None => break,
                    }
                }
                _ = subscribe_rx.recv() => {
                    tracing::debug!(
                        "BLE peripheral (macOS): second subscriber while connection active; ignored"
                    );
                }
            }
        }

        if let Ok(mut guard) = write_forward.lock() {
            *guard = None;
        }
    }

    if let Ok(mut guard) = write_forward.lock() {
        *guard = None;
    }

    // Release the leaked ObjC objects. Retained::from_raw takes ownership of the one
    // retain we forgot earlier; dropping it decrements the retain count. The manager
    // cascades: its dealloc releases the service, which releases characteristics and
    // delegate. notify_char_ptr is released here (its Rust-leaked retain); the service
    // continues to hold its own retain until the manager releases it.
    tokio::task::block_in_place(|| unsafe {
        (*manager.0).stopAdvertising();
        let _ = objc2::rc::Retained::from_raw(notify_char.0);
        let _ = objc2::rc::Retained::from_raw(manager.0);
    });
}

// --------------------------------------------------------------------------
// Transport
// --------------------------------------------------------------------------

pub struct BleTransport {
    adapter: Arc<Mutex<Option<Adapter>>>,
    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
    peripheral: Arc<Mutex<Option<BlePeripheralState>>>,
    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
    conn_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<BlePeripheralConnection>>>,
}

impl BleTransport {
    pub fn new() -> Self {
        Self {
            adapter: Arc::new(Mutex::new(None)),
            #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
            peripheral: Arc::new(Mutex::new(None)),
            #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
            conn_rx: Mutex::new(None),
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

        #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
        self.start_peripheral(identity).await?;

        #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
        let _ = identity;

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut guard = self.adapter.lock().await;
        if let Some(adapter) = guard.as_ref() {
            let _ = adapter.stop_scan().await;
        }
        *guard = None;

        #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
        {
            // On Linux: dropping BlePeripheralState drops the bluer RAII handles,
            // which closes the CharacteristicControl streams and causes peripheral_loop
            // to exit, dropping conn_tx.
            //
            // On Windows/macOS: dropping BlePeripheralState drops _stop_signal, which
            // is the only mechanism that exits the peripheral task and drops conn_tx.
            // StopAdvertising() alone does not close GATT event streams on either platform.
            //
            // In all cases, conn_tx dropping causes accept()'s recv() to return None,
            // releasing the conn_rx guard so we can acquire it here to clear it.
            *self.peripheral.lock().await = None;
            *self.conn_rx.lock().await = None;
        }

        Ok(())
    }

    /// Returns a stream of nearby Pathweave peers found via BLE scanning.
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
                match event {
                    CentralEvent::ServiceDataAdvertisement { id, service_data } => {
                        let data = match service_data.get(&PATHWEAVE_SERVICE_UUID) {
                            Some(d) => d.clone(),
                            None => continue,
                        };
                        if data.len() < 9 || data[0] != ADVERTISEMENT_VERSION {
                            continue;
                        }
                        let short_id: [u8; 8] = data[1..9].try_into().unwrap();
                        let announcement = PeerAnnouncement {
                            address: PeerAddress::Ble(id.to_string()),
                            short_id: Some(short_id),
                        };
                        if tx.unbounded_send(announcement).is_err() {
                            break;
                        }
                    }
                    // macOS (and iOS) peripherals cannot include service data in
                    // advertisements; CoreBluetooth silently drops the key. They
                    // advertise only their service UUID, which btleplug surfaces as
                    // ServicesAdvertisement. short_id is None; Noise_XX provides
                    // full identity during the handshake.
                    //
                    // No deduplication against ServiceDataAdvertisement: Linux and
                    // Windows peripherals always produce ServiceDataAdvertisement
                    // (they set service data explicitly) and macOS/iOS always produce
                    // ServicesAdvertisement (they cannot set service data). Both events
                    // firing for the same device in a Pathweave scan is not a realistic
                    // scenario.
                    CentralEvent::ServicesAdvertisement { id, services }
                        if services.contains(&PATHWEAVE_SERVICE_UUID) =>
                    {
                        let announcement = PeerAnnouncement {
                            address: PeerAddress::Ble(id.to_string()),
                            short_id: None,
                        };
                        if tx.unbounded_send(announcement).is_err() {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        });

        Box::pin(rx)
    }

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

        // Dropping a BleConnection without calling close() leaves the btleplug
        // Peripheral's internal BLEDevice alive. When connect() creates a new
        // BLEDevice for the same address while the old one is still open,
        // Windows returns RO_E_CLOSED on the new handle. Disconnecting first
        // drops the existing BLEDevice cleanly before the new one is created.
        let _ = peripheral.disconnect().await;

        // Give the Windows BLE stack time to fully tear down the previous
        // session before issuing GetGattServicesWithCacheModeAsync on the
        // new one. Without this settling window the WinRT call hangs.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        tokio::time::timeout(std::time::Duration::from_secs(10), peripheral.connect())
            .await
            .map_err(|_| PathweaveError::Transport("BLE connect timed out".into()))?
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

    // Borrow conn_rx from self (lifetime 'life0: 'async_trait). Holding the guard
    // across recv() is safe: stop() drops BlePeripheralState first (closing conn_tx
    // via the peripheral task exiting), causing recv() to return None and accept()
    // to release the guard. stop() then acquires the lock to clear it.
    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
    async fn accept(&self) -> Result<Box<dyn Connection>> {
        let mut guard = self.conn_rx.lock().await;
        match guard.as_mut() {
            Some(rx) => rx
                .recv()
                .await
                .map(|c| Box::new(c) as Box<dyn Connection>)
                .ok_or_else(|| PathweaveError::Transport("peripheral loop ended".into())),
            None => Err(PathweaveError::Transport("transport not started".into())),
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    async fn accept(&self) -> Result<Box<dyn Connection>> {
        Err(PathweaveError::Transport(
            "BLE peripheral mode not supported on this platform".into(),
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

// --------------------------------------------------------------------------
// Linux peripheral: start_peripheral (ADR 014)
// --------------------------------------------------------------------------

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

        // services populates the "Complete List of 128-bit Service Class UUIDs" AD type
        // (0x07). BlueZ's SetDiscoveryFilter UUID check reads that list. service_data
        // alone produces only AD type 0x21, which BlueZ does not reliably extract into
        // the device's UUID list on all versions, so the scan filter silently drops the
        // advertisement and discover() never fires.
        let adv = bluer::adv::Advertisement {
            advertisement_type: bluer::adv::Type::Peripheral,
            service_uuids: [PATHWEAVE_SERVICE_UUID].into_iter().collect(),
            service_data: svc_data,
            ..Default::default()
        };

        let adv_handle = adapter
            .advertise(adv)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(peripheral_loop(write_ctrl, notify_ctrl, conn_tx));

        *self.conn_rx.lock().await = Some(conn_rx);
        *self.peripheral.lock().await = Some(BlePeripheralState {
            _adv_handle: adv_handle,
            _app_handle: app_handle,
        });

        Ok(())
    }
}

// --------------------------------------------------------------------------
// Windows peripheral: start_peripheral (ADR 014, Windows section)
// --------------------------------------------------------------------------

#[cfg(target_os = "windows")]
impl BleTransport {
    async fn start_peripheral(&self, identity: &NodeIdentity) -> Result<()> {
        use windows::Devices::Bluetooth::GenericAttributeProfile::{
            GattCharacteristicProperties, GattLocalCharacteristicParameters, GattServiceProvider,
            GattServiceProviderAdvertisingParameters,
        };
        use windows::Foundation::TypedEventHandler;
        use windows::Storage::Streams::DataReader;

        let short_id: [u8; 8] = identity.peer_id().as_bytes()[..8].try_into().unwrap();

        let write_forward: Arc<
            std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<Bytes>>>,
        > = Arc::new(std::sync::Mutex::new(None));
        let (subscribe_tx, subscribe_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel::<BlePeripheralConnection>();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        // IAsyncOperation<T> does not implement Future in windows crate 0.52+.
        // Use block_in_place (safe on multi-threaded tokio) with .get() for init calls.
        let service_result = tokio::task::block_in_place(|| {
            GattServiceProvider::CreateAsync(uuid_to_guid(&PATHWEAVE_SERVICE_UUID))?.get()
        })
        .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let service_provider = service_result
            .ServiceProvider()
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let service = service_provider
            .Service()
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let write_params = GattLocalCharacteristicParameters::new()
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        write_params
            .SetCharacteristicProperties(GattCharacteristicProperties::WriteWithoutResponse)
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let write_char = tokio::task::block_in_place(|| {
            service
                .CreateCharacteristicAsync(uuid_to_guid(&WRITE_CHAR_UUID), &write_params)?
                .get()
        })
        .map_err(|e| PathweaveError::Transport(e.to_string()))?
        .Characteristic()
        .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        let notify_params = GattLocalCharacteristicParameters::new()
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        notify_params
            .SetCharacteristicProperties(GattCharacteristicProperties::Notify)
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let notify_char = tokio::task::block_in_place(|| {
            service
                .CreateCharacteristicAsync(uuid_to_guid(&NOTIFY_CHAR_UUID), &notify_params)?
                .get()
        })
        .map_err(|e| PathweaveError::Transport(e.to_string()))?
        .Characteristic()
        .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        // Write handler runs on a WinRT thread pool thread, not inside the tokio
        // runtime. IAsyncOperation::get() blocks synchronously; that is safe here
        // because this thread is outside the tokio executor. GetRequestAsync() resolves
        // immediately since the WinRT stack already holds the completed request.
        // Respond() is not called: the Microsoft GATT server docs show the conditional
        // pattern (Respond only for WriteWithResponse). Our characteristic is
        // WriteWithoutResponse only, so Respond() is never appropriate here.
        let write_forward_clone = Arc::clone(&write_forward);
        write_char
            .WriteRequested(&TypedEventHandler::new(
                move |_,
                      args: &Option<
                    windows::Devices::Bluetooth::GenericAttributeProfile::GattWriteRequestedEventArgs,
                >| {
                    let Some(args) = args else { return Ok(()); };
                    let Ok(request) = args.GetRequestAsync().and_then(|op| op.get()) else {
                        return Ok(());
                    };
                    let reader = DataReader::FromBuffer(&request.Value()?)?;
                    let len = reader.UnconsumedBufferLength()? as usize;
                    let mut buf = vec![0u8; len];
                    reader.ReadBytes(&mut buf)?;
                    if let Ok(guard) = write_forward_clone.lock() {
                        if let Some(tx) = guard.as_ref() {
                            let _ = tx.send(Bytes::from(buf));
                        } else {
                            tracing::debug!("BLE peripheral (Windows): write with no active connection; frame discarded");
                        }
                    }
                    Ok(())
                },
            ))
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        // Subscribe handler fires on subscribe and unsubscribe.
        //
        // Size > 0: signal peripheral_task that a client is ready.
        //
        // Size == 0: the client disconnected. Drop write_tx by clearing
        // write_forward so that BlePeripheralConnection.recv_bytes() returns
        // an error, which unwinds handle_incoming, which drops reply_tx,
        // which breaks the peripheral_task inner loop. Without this, the
        // inner loop stays stuck and the next subscribe signal is silently
        // consumed and ignored, making message delivery impossible.
        let write_forward_for_unsub = Arc::clone(&write_forward);
        notify_char
            .SubscribedClientsChanged(&TypedEventHandler::new(
                move |char: &Option<
                    windows::Devices::Bluetooth::GenericAttributeProfile::GattLocalCharacteristic,
                >,
                      _| {
                    let Some(c) = char else {
                        return Ok(());
                    };
                    if c.SubscribedClients()?.Size()? > 0 {
                        let _ = subscribe_tx.send(());
                    } else if let Ok(mut guard) = write_forward_for_unsub.lock() {
                        *guard = None;
                    }
                    Ok(())
                },
            ))
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;

        // Scoped block: IBuffer and GattServiceProviderAdvertisingParameters are not
        // Send, so they must be dropped before the first .await below.
        {
            // Service data: [0x01] ++ [short_id: 8 bytes]. SetServiceData requires
            // Windows 10 version 1903+ (Build 18362, IGattServiceProviderAdvertisingParameters2).
            let mut svc_data = vec![ADVERTISEMENT_VERSION];
            svc_data.extend_from_slice(&short_id);
            let buffer = bytes_to_ibuffer(&svc_data)
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;

            let adv_params = GattServiceProviderAdvertisingParameters::new()
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;
            adv_params
                .SetIsConnectable(true)
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;
            adv_params
                .SetIsDiscoverable(true)
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;
            adv_params
                .SetServiceData(&buffer)
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;

            service_provider
                .StartAdvertisingWithParameters(&adv_params)
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        }

        tokio::spawn(windows_peripheral_task(
            service_provider,
            notify_char,
            conn_tx,
            write_forward,
            subscribe_rx,
            stop_rx,
        ));

        *self.conn_rx.lock().await = Some(conn_rx);
        *self.peripheral.lock().await = Some(BlePeripheralState {
            _stop_signal: stop_tx,
        });

        Ok(())
    }
}

// --------------------------------------------------------------------------
// macOS peripheral: start_peripheral (ADR 014, macOS section)
// --------------------------------------------------------------------------

#[cfg(target_os = "macos")]
impl BleTransport {
    async fn start_peripheral(&self, identity: &NodeIdentity) -> Result<()> {
        use macos_delegate::MacosPeripheralDelegate;
        use objc2::{AllocAnyThread, ClassType};
        use objc2_core_bluetooth::{
            CBAttributePermissions, CBCharacteristic, CBCharacteristicProperties,
            CBMutableCharacteristic, CBMutableService, CBPeripheralManager, CBUUID,
        };
        use objc2_foundation::{NSMutableArray, NSString};

        // short_id is not included in the macOS advertisement payload; CoreBluetooth
        // silently ignores CBAdvertisementDataServiceDataKey. See ADR 014 macOS section.
        let _ = identity;

        let write_forward: Arc<
            std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<Bytes>>>,
        > = Arc::new(std::sync::Mutex::new(None));
        let (subscribe_tx, subscribe_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (conn_tx, conn_rx) = tokio::sync::mpsc::unbounded_channel::<BlePeripheralConnection>();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let bridge = Arc::new(PeripheralDelegateBridge {
            subscribe_tx,
            write_forward: Arc::clone(&write_forward),
        });

        // All CoreBluetooth objects are not Send. We set everything up synchronously
        // inside block_in_place, then extract raw pointers for the async task.
        //
        // Memory contract:
        //   manager and notify_char are forgot (Retained leaked); the task releases
        //   them on exit via Retained::from_raw.
        //
        //   delegate is NOT forgot: we let the Rust Retained drop at the end of this
        //   block_in_place closure, taking the count from 2 to 1 (the manager holds
        //   its own ObjC retain). When the manager is released in the task, the count
        //   drops to 0 and the delegate is freed.
        let (manager_ptr, notify_char_ptr) = tokio::task::block_in_place(|| {
            let delegate = MacosPeripheralDelegate::new(Arc::clone(&bridge));

            // Use a background serial queue so callbacks are not tied to the main
            // run loop, which is not guaranteed to spin in a CLI/daemon process.
            let queue = dispatch2::DispatchQueue::new(
                "com.pathweave.ble.peripheral",
                dispatch2::DispatchQueueAttr::SERIAL,
            );

            let manager = unsafe {
                CBPeripheralManager::initWithDelegate_queue(
                    CBPeripheralManager::alloc(),
                    Some(objc2::runtime::ProtocolObject::from_ref(&*delegate)),
                    Some(&queue),
                )
            };

            let svc_uuid = unsafe {
                CBUUID::UUIDWithString(&NSString::from_str(
                    &PATHWEAVE_SERVICE_UUID.hyphenated().to_string(),
                ))
            };
            let write_uuid = unsafe {
                CBUUID::UUIDWithString(&NSString::from_str(
                    &WRITE_CHAR_UUID.hyphenated().to_string(),
                ))
            };
            let notify_uuid = unsafe {
                CBUUID::UUIDWithString(&NSString::from_str(
                    &NOTIFY_CHAR_UUID.hyphenated().to_string(),
                ))
            };

            // Write characteristic: CBCharacteristicPropertyWrite (not WriteWithoutResponse).
            // CoreBluetooth only delivers data to peripheralManager:didReceiveWriteRequests:
            // for write-with-response. Write commands (without response) are silently
            // dropped on macOS. The central detects the property and uses WithResponse.
            let write_char = unsafe {
                CBMutableCharacteristic::initWithType_properties_value_permissions(
                    CBMutableCharacteristic::alloc(),
                    &write_uuid,
                    CBCharacteristicProperties::Write,
                    None,
                    CBAttributePermissions::Writeable,
                )
            };
            let notify_char = unsafe {
                CBMutableCharacteristic::initWithType_properties_value_permissions(
                    CBMutableCharacteristic::alloc(),
                    &notify_uuid,
                    CBCharacteristicProperties::Notify,
                    None,
                    CBAttributePermissions::Readable,
                )
            };

            // Use CBCharacteristic as the array element type so setCharacteristics
            // accepts it without coercion (CBMutableCharacteristic: Deref<Target = CBCharacteristic>).
            let chars = NSMutableArray::<CBCharacteristic>::new();
            unsafe { chars.addObject(&write_char) };
            unsafe { chars.addObject(&*notify_char) };

            let service = unsafe {
                CBMutableService::initWithType_primary(CBMutableService::alloc(), &svc_uuid, true)
            };
            unsafe { service.setCharacteristics(Some(&chars)) };
            unsafe { manager.addService(&service) };

            // macOS only supports CBAdvertisementDataLocalNameKey and
            // CBAdvertisementDataServiceUUIDsKey. Service data is silently ignored.
            // See ADR 014 macOS section.
            //
            // We build the advertisement dictionary via msg_send! to avoid generic
            // type parameter gymnastics (NSDictionary<K,V> coercions are not yet
            // ergonomic for mixed-type dictionaries in objc2 0.6).
            let uuid_arr = NSMutableArray::<CBUUID>::new();
            unsafe { uuid_arr.addObject(&*svc_uuid) };
            let adv_key = NSString::from_str("kCBAdvDataServiceUUIDs");
            let adv: Option<
                objc2::rc::Retained<
                    objc2_foundation::NSDictionary<NSString, objc2::runtime::AnyObject>,
                >,
            > = unsafe {
                objc2::msg_send_id![
                    objc2_foundation::NSDictionary::<NSString, objc2::runtime::AnyObject>::class(),
                    dictionaryWithObject: &*uuid_arr,
                    forKey: &*adv_key
                ]
            };
            unsafe { manager.startAdvertising(adv.as_deref()) };

            let mgr_ptr = objc2::rc::Retained::as_ptr(&manager) as *mut CBPeripheralManager;
            let notify_ptr =
                objc2::rc::Retained::as_ptr(&notify_char) as *mut CBMutableCharacteristic;

            std::mem::forget(manager);
            std::mem::forget(notify_char);
            // delegate drops here; manager holds its own ObjC retain on the delegate

            (mgr_ptr, notify_ptr)
        });

        tokio::spawn(macos_peripheral_task(
            SendPtr(manager_ptr),
            SendPtr(notify_char_ptr),
            conn_tx,
            write_forward,
            subscribe_rx,
            stop_rx,
        ));

        *self.conn_rx.lock().await = Some(conn_rx);
        *self.peripheral.lock().await = Some(BlePeripheralState {
            _stop_signal: stop_tx,
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
    // No length-prefix framing needed (unlike QUIC's byte stream).
    // Wrapped in Mutex to satisfy Connection: Sync.
    rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Bytes>>,
}

#[async_trait]
impl Connection for BleConnection {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        // Linux/Windows peripherals expose WriteWithoutResponse; macOS peripherals
        // expose only Write (with response) because CoreBluetooth does not deliver
        // ATT write commands to peripheralManager:didReceiveWriteRequests:.
        let write_type = if self
            .write_char
            .properties
            .contains(CharPropFlags::WRITE_WITHOUT_RESPONSE)
        {
            WriteType::WithoutResponse
        } else {
            WriteType::WithResponse
        };
        self.peripheral
            .write(&self.write_char, bytes, write_type)
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

    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    #[tokio::test]
    async fn accept_returns_not_supported_on_other_platforms() {
        let transport = BleTransport::new();
        let result = transport.accept().await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }

    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
    #[tokio::test]
    async fn accept_before_start_returns_error() {
        let transport = BleTransport::new();
        let result = transport.accept().await;
        assert!(matches!(result, Err(PathweaveError::Transport(_))));
    }
}
