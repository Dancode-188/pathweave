# ADR 014: Linux BLE peripheral mode via bluer v0.17

**Status:** Accepted

## Context

Phase 3 of v0.2.0 implements BLE peripheral mode: advertising this node's identity
and accepting GATT connections from scanning centrals. ADR 006 settled the split:
central mode uses `btleplug` on all platforms; peripheral mode uses `bluer` v0.17 on
Linux. ADR 010 settled that `Transport::start()` takes `&NodeIdentity`.

Three design questions remained open:

1. Which bluer data-path API to use for the GATT server
2. How `accept()` maps onto bluer's event-driven model
3. How `bluer` and `btleplug` coexist inside the same crate

This ADR settles all three.

## bluer v0.17 GATT server data API

bluer exposes two data-path variants for GATT server characteristics:

| Variant | Field values | Execution context |
|---|---|---|
| Callback | `CharacteristicWriteMethod::Fn` / `CharacteristicNotifyMethod::Fn` | Closures called on a BlueZ D-Bus thread, outside Tokio |
| Async IO | `CharacteristicWriteMethod::Io` / `CharacteristicNotifyMethod::Io` | Async stream, inside the Tokio runtime |

The `Fn` variants call closures on a BlueZ D-Bus event thread. Getting data into
async tasks from those closures requires `Arc<Mutex<...>>` bridges with no benefit:
the closures cannot `await` and data must leave the Tokio executor regardless.

The `Io` variants are driven by `characteristic_control()`, a factory that returns a
`(CharacteristicControl, CharacteristicControlHandle)` pair:

- `CharacteristicControlHandle` goes into the `Characteristic` struct and is
  registered with bluer when `adapter.serve_gatt_application(app).await` is called.
- `CharacteristicControl` is a `Stream<Item = CharacteristicControlEvent>` that
  drives the application event loop inside the Tokio runtime.

Events on the write characteristic:

- `CharacteristicControlEvent::Write(req)`: a central wrote a frame. `req.accept()?`
  returns a `CharacteristicReader` (implements `AsyncRead`) containing the frame bytes.
  The reader reaches EOF after one write operation's data is consumed.

Events on the notify characteristic:

- `CharacteristicControlEvent::Notify(writer)`: a central subscribed. The yielded
  `CharacteristicWriter` (implements `AsyncWrite`) remains valid until the central
  disconnects or `_app_handle` is dropped.

## Decision

### GATT characteristic configuration

Both characteristics use the `Io` variant:

```rust
CharacteristicWrite {
    write_without_response: true,
    method: CharacteristicWriteMethod::Io,
    ..Default::default()
}

CharacteristicNotify {
    notify: true,
    method: CharacteristicNotifyMethod::Io,
    ..Default::default()
}
```

`write_without_response: true` matches ADR 002: the central writes without a GATT
acknowledgement. Delivery guarantees are provided by the at-least-once retry loop
(ADR 011) at the application layer.

### bluer/btleplug coexistence

`BleTransport` gains one Linux-only field:

```rust
#[cfg(target_os = "linux")]
peripheral: Arc<tokio::sync::Mutex<Option<BlePeripheralState>>>,
```

All bluer types are gated under `#[cfg(target_os = "linux")]`. The btleplug
central-mode field (`adapter: Arc<Mutex<Option<Adapter>>>`) is unchanged and
platform-independent. On macOS and Windows, `bluer` is absent from the dependency
tree and `accept()` returns the existing not-implemented error.

`pathweave-transport-ble/Cargo.toml` adds a
`[target.'cfg(target_os = "linux")'.dependencies]` section with
`bluer = { workspace = true }`.

### Handle lifetime and peripheral state

```rust
#[cfg(target_os = "linux")]
struct BlePeripheralState {
    _adv_handle: bluer::adv::AdvertisementHandle,
    _app_handle: bluer::gatt::local::ApplicationHandle,
    conn_rx: Arc<tokio::sync::Mutex<
        tokio::sync::mpsc::UnboundedReceiver<BlePeripheralConnection>
    >>,
}
```

`AdvertisementHandle` and `ApplicationHandle` are RAII guards: bluer unregisters
the advertisement and the GATT application when they are dropped. The `_` prefix
makes the intent explicit.

`conn_rx` is wrapped in `Arc<tokio::sync::Mutex<...>>` so `accept()` can clone the
Arc, release the outer `self.peripheral` lock, and then await `recv()` without
holding the outer lock. This prevents a deadlock when `stop()` needs to acquire
the same outer lock while `accept()` is blocked. See the `accept()` section below.

Dropping `BlePeripheralState` in `stop()` drops the two handles, which closes the
characteristic control streams in `peripheral_loop`, which causes it to return and
drop `conn_tx`. The `conn_rx.recv()` call in `accept()` then returns `None` and
`accept()` returns an error, unblocking `accept_loop`.

### Advertisement format

```rust
bluer::adv::Advertisement {
    advertisement_type: bluer::adv::Type::Peripheral,
    service_data: BTreeMap::from([(
        PATHWEAVE_SERVICE_UUID,
        [&[ADVERTISEMENT_VERSION], short_id.as_slice()].concat(),
    )]),
    ..Default::default()
}
```

`short_id` is the first 8 bytes of `identity.peer_id()`. This matches ADR 002.
BlueZ constructs the `ServiceData` AD structure from this map. btleplug's
`CentralEvent::ServiceDataAdvertisement` on the central side reports the same UUID
key and byte payload, so the advertisement format is consistent end to end.

The service UUID does not appear in `service_uuids` as a separate field. ADR 002's
byte analysis showed that adding a `Complete List of 128-bit UUIDs` AD structure
would exceed the 31-byte advertisement limit. BlueZ extracts the UUID from
`service_data` when filtering advertisements; btleplug's scan filter works
correctly with this layout.

### Transport::start() on Linux

`BleTransport::start(&identity)` on Linux executes the following sequence:

1. Create a `bluer::Session` and acquire the default adapter.
2. Derive `short_id`: first 8 bytes of `identity.peer_id()`.
3. Call `characteristic_control()` twice, once for each characteristic. Retain the
   `CharacteristicControl` streams; pass the `CharacteristicControlHandle` values
   into their respective `Characteristic` structs.
4. Call `adapter.serve_gatt_application(app).await`; retain `ApplicationHandle`.
5. Call `adapter.advertise(adv).await`; retain `AdvertisementHandle`.
6. Create an unbounded channel `(conn_tx, conn_rx)`.
7. Spawn `peripheral_loop(write_ctrl, notify_ctrl, conn_tx)`.
8. Store `BlePeripheralState { _adv_handle, _app_handle, conn_rx: Arc::new(Mutex::new(conn_rx)) }`.

On non-Linux platforms, `start()` uses `_identity` and performs the existing
btleplug adapter initialization unchanged.

### peripheral_loop

`peripheral_loop` runs as a background task and is the sole owner of
`CharacteristicWriter`. It drives a `tokio::select!` loop over the write control
stream, the notify control stream, and a reply channel from connections:

```rust
async fn peripheral_loop(
    mut write_ctrl: CharacteristicControl,
    mut notify_ctrl: CharacteristicControl,
    conn_tx: UnboundedSender<BlePeripheralConnection>,
) {
    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let mut writer: Option<CharacteristicWriter> = None;
    let mut active_write_tx: Option<UnboundedSender<Bytes>> = None;

    loop {
        tokio::select! {
            evt = notify_ctrl.next() => match evt {
                Some(CharacteristicControlEvent::Notify(w)) => {
                    if active_write_tx.is_some() {
                        // v0.2.0 supports one central at a time.
                        tracing::debug!("BLE peripheral: second subscriber while connection active; ignored");
                    } else {
                        writer = Some(w);
                        let (write_tx, write_rx) = tokio::sync::mpsc::unbounded_channel();
                        active_write_tx = Some(write_tx);
                        let _ = conn_tx.send(BlePeripheralConnection {
                            write_rx: tokio::sync::Mutex::new(write_rx),
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
                                if tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf).await.is_ok() {
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
                    if tokio::io::AsyncWriteExt::write_all(w, &bytes).await.is_err() {
                        writer = None;
                        active_write_tx = None;
                    }
                }
            },
        }
    }
}
```

On a `Notify` event, `peripheral_loop` creates the connection immediately and sends
it to `accept()`. The connection carries an internal channel (`write_rx`) that
receives frames forwarded from subsequent `Write` events. Multiple `Write` events
from the same subscriber map to multiple channel receives, which is what
`Session::respond()` requires: two separate `recv_bytes()` calls for Noise_XX
messages 1 and 3.

On a `Write` event, `peripheral_loop` reads all bytes from `CharacteristicReader`
via `read_to_end` (one write operation = one frame = EOF) and forwards the frame to
the active connection's channel. If `active_write_tx.send()` returns an error, the
connection was dropped by the router; `active_write_tx` is cleared so the next
`Notify` event can create a fresh connection.

`CharacteristicWriter` is owned by `peripheral_loop` so it persists across multiple
write events from the same subscriber. Reply bytes flow from connections through
`reply_rx` and are written to the subscriber via `write_all`. If `write_all` fails
(central disconnected), both `writer` and `active_write_tx` are cleared.

### accept()

```rust
#[cfg(target_os = "linux")]
async fn accept(&self) -> Result<Box<dyn Connection>> {
    let rx = {
        let guard = self.peripheral.lock().await;
        let state = guard.as_ref()
            .ok_or_else(|| PathweaveError::Transport("transport not started".into()))?;
        Arc::clone(&state.conn_rx)
    }; // outer Mutex released before awaiting
    rx.lock().await.recv().await
        .map(|c| Box::new(c) as Box<dyn Connection>)
        .ok_or_else(|| PathweaveError::Transport("peripheral loop ended".into()))
}
```

The outer `self.peripheral` lock is acquired only to clone the `Arc<Mutex<...>>`
for `conn_rx`. It is released before `recv()` blocks. This allows `stop()` to
acquire `self.peripheral`, drop `BlePeripheralState`, and unblock the waiting
`recv()` without deadlock.

### BlePeripheralConnection

```rust
#[cfg(target_os = "linux")]
struct BlePeripheralConnection {
    write_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Bytes>>,
    reply_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
}
```

`write_rx` is wrapped in `tokio::sync::Mutex` to satisfy `Connection: Sync`.
`recv_bytes` takes `&mut self`, so there is never actual contention on this lock.

`recv_bytes()` receives the next frame from the channel. Each call blocks until
`peripheral_loop` forwards a write frame. Multiple sequential `recv_bytes()` calls
on the same connection map to multiple sequential write events from the central,
which is the correct model for `Session::respond()`.

`send_bytes()` sends via `reply_tx`. The actual write to `CharacteristicWriter`
happens in `peripheral_loop`. `close()` is a no-op: dropping the connection
drops `write_rx` and the cloned `reply_tx`. `peripheral_loop` detects the dropped
receiver when `active_write_tx.send()` fails and clears the active connection state.

`mtu()` returns 512, consistent with `BleConnection` and ADR 002's GATT MTU.

### NodeIdentity propagation chain

Implementing ADR 010 requires changes across several layers in addition to the BLE
crate itself:

- `Transport` trait (`pathweave-core/src/lib.rs`): `start(&self) -> Result<()>`
  becomes `start(&self, identity: &NodeIdentity) -> Result<()>`.
- `QuicTransport::start()`: adds `_identity` parameter; no other changes.
- All mock `Transport` implementations in `pathweave-core` tests: add `_identity`.
- `health_monitor()` in `router.rs`: gains `identity: Arc<NodeIdentity>`; calls
  `transport.start(&identity).await` in place of `transport.start().await`.
- `Router::register_transport()`: gains `identity: Arc<NodeIdentity>`; passes it to
  `health_monitor`.
- `PathweaveNode::add_transport()` in `node.rs`: acquires identity from `self` and
  passes `Arc::clone(&self.identity)` to `register_transport()`.

## Rationale

**Why accept on Subscribe, not on Write?**

`Session::respond()` performs the Noise_XX three-message handshake, which requires
two sequential `recv_bytes()` calls on the same connection: read message 1, send
message 2, read message 3. If the connection is created on a Write event with a
single `CharacteristicReader`, the first `recv_bytes()` reads the frame and the
reader reaches EOF. The second `recv_bytes()` returns zero bytes; the Noise layer
cannot parse that as message 3 and fails. Meanwhile the second Write event (message
3) creates a new connection in `conn_rx`, which `accept_loop` picks up and tries to
parse as message 1, also failing.

Creating the connection on Subscribe and forwarding each Write event to the channel
gives `Session::respond()` what it needs: multiple sequential `recv_bytes()` calls
that block on the channel until data arrives.

**Why own CharacteristicWriter in peripheral_loop?**

`CharacteristicWriter` does not implement `Clone`. A single notify subscription
persists across multiple write events from the same central: the central subscribes
once per GATT connection and then writes multiple times. Moving the writer into the
connection (or into the first write's connection) leaves subsequent writes with no
way to reply. Owning the writer in `peripheral_loop` and routing replies through a
channel gives each connection an independent reply path without ownership conflicts.

**Why reject a second subscriber rather than replacing the writer?**

If a second central subscribes while a connection is active and the writer is
replaced silently, the ACK for the first central's message is sent to the second
central. The first central never receives its ACK, its at-least-once retry fires,
and it writes again to the peripheral, which creates a new connection that is also
routed to the wrong writer. Silent replacement produces confused state; an explicit
rejection with a debug log is predictable and honest about the v0.2.0 limitation.
The at-least-once retry loop on the first central handles the missed ACK.

**Why release the outer lock before recv() in accept()?**

`stop()` calls `transport.stop().await`, which must acquire `self.peripheral` to
drop `BlePeripheralState`. If `accept()` holds that lock across the blocking
`recv()` call, `stop()` waits for the lock. But the unblock signal (`conn_tx` drop
that causes `recv()` to return `None`) only arrives after `BlePeripheralState` is
dropped, which requires `stop()` to acquire the lock first: a deadlock. Cloning the
Arc before releasing the lock breaks the cycle: `stop()` can acquire the outer lock,
drop the state and its handles, which closes `peripheral_loop` and drops `conn_tx`,
which causes the already-waiting `recv()` to return `None`.

## Implications

- `bluer` is added as a `[target.'cfg(target_os = "linux")'.dependencies]` entry
  in `pathweave-transport-ble/Cargo.toml`.
- `BleTransport` gains `peripheral` field, `BlePeripheralState`, and
  `BlePeripheralConnection` under `#[cfg(target_os = "linux")]`.
- `Transport::start()` signature changes to `start(&self, identity: &NodeIdentity)`.
- `QuicTransport`, all mock transports in tests: add `_identity`.
- `health_monitor()`, `Router::register_transport()`, `PathweaveNode::add_transport()`:
  propagate `Arc<NodeIdentity>` as described above.
- `accept()` on non-Linux platforms returns the not-implemented error unchanged.
- Multiple simultaneous centrals are not supported in v0.2.0. A second subscriber
  while a connection is active is dropped with a debug log.
- macOS and Windows peripheral mode remain deferred per ADR 006.
