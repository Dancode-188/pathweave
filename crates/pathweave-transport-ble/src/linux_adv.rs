//! Linux `AdvertisingBearer` using `bluer`. Requires BLE 5.0 extended advertising; see
//! the ADR 018 addendum for why legacy advertising cannot carry this transport's header.
//!
//! Compile-checked by CI's ubuntu-latest runner. Not exercised against real hardware in
//! this codebase yet; see issue #98's "Out of scope" (matches the precedent already set
//! by #72 for the GATT-based BLE transport).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bluer::adv::{Advertisement, SecondaryChannel, Type as AdvertisementType};
use futures::stream::{BoxStream, StreamExt};
use pathweave_core::{PathweaveError, Result};
use tokio::sync::Mutex;

use crate::advertising::{AdvertisingBearer, ADV_BEARER_MAGIC, MANUFACTURER_COMPANY_ID};

/// Held duration for a single advertisement before withdrawal. The controller repeats
/// the same advertisement at its configured interval; this just needs to be long enough
/// for a nearby scanner to observe at least one repetition.
const ADVERTISE_HOLD: Duration = Duration::from_millis(500);

pub(crate) struct LinuxAdvertisingBearer {
    adapter: Arc<Mutex<Option<bluer::Adapter>>>,
}

impl LinuxAdvertisingBearer {
    pub(crate) fn new() -> Self {
        Self {
            adapter: Arc::new(Mutex::new(None)),
        }
    }

    async fn ensure_adapter(
        adapter: &Arc<Mutex<Option<bluer::Adapter>>>,
    ) -> Result<bluer::Adapter> {
        let mut guard = adapter.lock().await;
        if let Some(a) = guard.as_ref() {
            return Ok(a.clone());
        }
        let session = bluer::Session::new()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        let new_adapter = session
            .default_adapter()
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        *guard = Some(new_adapter.clone());
        Ok(new_adapter)
    }
}

#[async_trait]
impl AdvertisingBearer for LinuxAdvertisingBearer {
    async fn advertise(&self, packet: Vec<u8>) -> Result<()> {
        let adapter = Self::ensure_adapter(&self.adapter).await?;

        let mut data = Vec::with_capacity(1 + packet.len());
        data.push(ADV_BEARER_MAGIC);
        data.extend_from_slice(&packet);

        let mut manufacturer_data = BTreeMap::new();
        manufacturer_data.insert(MANUFACTURER_COMPANY_ID, data);

        let adv = Advertisement {
            advertisement_type: AdvertisementType::Broadcast,
            manufacturer_data,
            // Legacy advertising cannot carry the ADR 018 header at all; see the ADR
            // 018 addendum. Extended advertising is the baseline, not an enhancement.
            secondary_channel: Some(SecondaryChannel::OneM),
            ..Default::default()
        };

        let handle = adapter
            .advertise(adv)
            .await
            .map_err(|e| PathweaveError::Transport(e.to_string()))?;
        tokio::time::sleep(ADVERTISE_HOLD).await;
        drop(handle);
        Ok(())
    }

    fn scan(&self) -> BoxStream<'static, Vec<u8>> {
        let adapter_arc = Arc::clone(&self.adapter);
        let (tx, rx) = futures::channel::mpsc::unbounded::<Vec<u8>>();

        tokio::spawn(async move {
            let adapter = match Self::ensure_adapter(&adapter_arc).await {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!("BLE advertising-mode scan: adapter init failed: {}", e);
                    return;
                }
            };

            let mut events = match adapter.discover_devices().await {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("BLE advertising-mode scan: discover_devices failed: {}", e);
                    return;
                }
            };

            // AdapterEvent::PropertyChanged carries an adapter-level property, not a
            // device address, so it cannot tell us which device changed. A device that
            // re-advertises with a new payload (the normal case for this transport,
            // since every advertise() call carries a different packet) surfaces that
            // through a separate per-device events() stream, not this adapter-level one.
            // Each newly discovered device gets its own forwarding task below.
            while let Some(event) = events.next().await {
                let bluer::AdapterEvent::DeviceAdded(addr) = event else {
                    continue;
                };
                let device = match adapter.device(addr) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    if check_and_forward(&device, &tx).await.is_err() {
                        return;
                    }
                    let Ok(mut device_events) = device.events().await else {
                        return;
                    };
                    // Any property change is treated as "re-check manufacturer data,"
                    // rather than pattern-matching which property changed: the dispatcher's
                    // own dedup (by source + payload hash, see advertising.rs) collapses
                    // any duplicate checks this produces, and this is robust to property
                    // change payload shapes we have not exhaustively verified.
                    while device_events.next().await.is_some() {
                        if check_and_forward(&device, &tx).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        Box::pin(rx)
    }
}

/// Reads `device`'s current manufacturer data; forwards the packet (magic byte
/// stripped) if it matches this transport's company ID and magic byte. Returns `Err`
/// only when the forwarding channel itself has closed, signaling the caller to stop.
async fn check_and_forward(
    device: &bluer::Device,
    tx: &futures::channel::mpsc::UnboundedSender<Vec<u8>>,
) -> std::result::Result<(), ()> {
    let Ok(Some(manufacturer_data)) = device.manufacturer_data().await else {
        return Ok(());
    };
    let Some(data) = manufacturer_data.get(&MANUFACTURER_COMPANY_ID) else {
        return Ok(());
    };
    if data.first() != Some(&ADV_BEARER_MAGIC) {
        return Ok(());
    }
    tx.unbounded_send(data[1..].to_vec()).map_err(|_| ())
}
