//! Windows `AdvertisingBearer` using `BluetoothLEAdvertisementPublisher`/`Watcher`.
//! Requires BLE 5.0 extended advertising; see the ADR 018 addendum for why legacy
//! advertising cannot carry this transport's header. See issue #98.

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use pathweave_core::{PathweaveError, Result};
use windows::Devices::Bluetooth::Advertisement::{
    BluetoothLEAdvertisementPublisher, BluetoothLEAdvertisementReceivedEventArgs,
    BluetoothLEAdvertisementWatcher, BluetoothLEManufacturerData,
};
use windows::Foundation::TypedEventHandler;
use windows::Storage::Streams::DataReader;

use crate::advertising::{AdvertisingBearer, ADV_BEARER_MAGIC, MANUFACTURER_COMPANY_ID};
use crate::bytes_to_ibuffer;

/// Held duration for a single advertisement before withdrawal. The controller repeats
/// the same advertisement at its configured interval; this just needs to be long enough
/// for a nearby scanner to observe at least one repetition.
const ADVERTISE_HOLD: Duration = Duration::from_millis(500);

pub(crate) struct WindowsAdvertisingBearer;

impl WindowsAdvertisingBearer {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AdvertisingBearer for WindowsAdvertisingBearer {
    async fn advertise(&self, packet: Vec<u8>) -> Result<()> {
        let mut data = Vec::with_capacity(1 + packet.len());
        data.push(ADV_BEARER_MAGIC);
        data.extend_from_slice(&packet);

        // Every WinRT object touched below wraps a raw COM pointer (NonNull<c_void>)
        // that windows-rs does not mark Send, regardless of the underlying WinRT type's
        // "Agile" marshaling behavior. spawn_blocking's closure only captures `data`
        // (a plain Vec<u8>, which is Send); the publisher, buffer, and advertisement
        // objects are created and consumed entirely inside this closure on its own
        // thread, so they never need to cross an .await point in this async fn.
        tokio::task::spawn_blocking(move || -> Result<()> {
            // Publisher's own Advertisement property starts as an empty advertisement;
            // populate that object directly rather than constructing a separate one,
            // since BluetoothLEAdvertisementPublisher's argument-taking constructor
            // would require resolving which of its two WinRT constructors windows-rs
            // binds to which name.
            let publisher = BluetoothLEAdvertisementPublisher::new()
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;

            let buffer =
                bytes_to_ibuffer(&data).map_err(|e| PathweaveError::Transport(e.to_string()))?;
            let manufacturer_data =
                BluetoothLEManufacturerData::Create(MANUFACTURER_COMPANY_ID, &buffer)
                    .map_err(|e| PathweaveError::Transport(e.to_string()))?;
            let advertisement = publisher
                .Advertisement()
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;
            advertisement
                .ManufacturerData()
                .map_err(|e| PathweaveError::Transport(e.to_string()))?
                .Append(&manufacturer_data)
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;

            // Legacy advertising cannot carry the ADR 018 header at all; see the ADR
            // 018 addendum. Extended advertising is the baseline, not an enhancement.
            publisher
                .SetUseExtendedAdvertisement(true)
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;

            publisher
                .Start()
                .map_err(|e| PathweaveError::Transport(e.to_string()))?;
            std::thread::sleep(ADVERTISE_HOLD);
            let _ = publisher.Stop();
            Ok(())
        })
        .await
        .map_err(|e| PathweaveError::Transport(format!("blocking task panicked: {e}")))?
    }

    fn scan(&self) -> BoxStream<'static, Vec<u8>> {
        let (tx, rx) = futures::channel::mpsc::unbounded::<Vec<u8>>();

        // BluetoothLEAdvertisementWatcher delivers events via its own WinRT dispatch
        // mechanism; a dedicated OS thread (rather than a tokio task) avoids any
        // assumption about tokio's worker threads being suitable for driving it, and
        // gives this loop a place to park for the watcher's lifetime.
        std::thread::spawn(move || {
            let watcher = match BluetoothLEAdvertisementWatcher::new() {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!("BLE advertising-mode scan: watcher init failed: {}", e);
                    return;
                }
            };
            if let Err(e) = watcher.SetAllowExtendedAdvertisements(true) {
                tracing::warn!(
                    "BLE advertising-mode scan: extended advertisements unavailable: {}",
                    e
                );
                return;
            }

            let handler_tx = tx.clone();
            let registered = watcher.Received(&TypedEventHandler::new(
                move |_, args: &Option<BluetoothLEAdvertisementReceivedEventArgs>| {
                    let Some(args) = args else {
                        return Ok(());
                    };
                    let Ok(advertisement) = args.Advertisement() else {
                        return Ok(());
                    };
                    let Ok(entries) =
                        advertisement.GetManufacturerDataByCompanyId(MANUFACTURER_COMPANY_ID)
                    else {
                        return Ok(());
                    };
                    let Ok(size) = entries.Size() else {
                        return Ok(());
                    };
                    if size == 0 {
                        return Ok(());
                    }
                    let Ok(entry) = entries.GetAt(0) else {
                        return Ok(());
                    };
                    let Ok(buffer) = entry.Data() else {
                        return Ok(());
                    };
                    let Ok(reader) = DataReader::FromBuffer(&buffer) else {
                        return Ok(());
                    };
                    let Ok(len) = reader.UnconsumedBufferLength() else {
                        return Ok(());
                    };
                    let mut buf = vec![0u8; len as usize];
                    if reader.ReadBytes(&mut buf).is_err() {
                        return Ok(());
                    }
                    if buf.first() != Some(&ADV_BEARER_MAGIC) {
                        return Ok(());
                    }
                    let _ = handler_tx.unbounded_send(buf[1..].to_vec());
                    Ok(())
                },
            ));
            if registered.is_err() {
                tracing::warn!("BLE advertising-mode scan: failed to register Received handler");
                return;
            }

            if watcher.Start().is_err() {
                tracing::warn!("BLE advertising-mode scan: watcher failed to start");
                return;
            }

            // Dropping `tx` (when the dispatcher consuming this stream exits, e.g. on
            // BleAdvertisingTransport::stop()) is the only signal this loop has to exit;
            // poll for that rather than blocking forever.
            loop {
                std::thread::sleep(Duration::from_millis(500));
                if tx.is_closed() {
                    let _ = watcher.Stop();
                    break;
                }
            }
        });

        Box::pin(rx)
    }
}
