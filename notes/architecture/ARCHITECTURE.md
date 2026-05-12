# Pathweave Architecture

## What this document is

The contract. Everything we build is built against what's written here. When something
is unclear during implementation, we look here first. If the answer isn't here, we update
this before writing the code.

## The problem in one sentence

Apps that need to work when the internet goes down have to rebuild multi-transport fallback
from scratch every time. Pathweave solves that once.

## The system in one picture

```
Your application
      |
      v
PathweaveNode          // the public API: send(), on_message(), events()
      |
      v
Router                 // picks the best available transport based on cost and availability
      |
      v
Session layer          // Noise_XX handshake, encryption, decryption. Lives in pathweave-core.
      |
      v
Bundle layer           // BPv7 encode/decode, fragmentation, reassembly. Lives in pathweave-core.
      |
      v
Transport trait        // the abstraction boundary. dumb byte pipes with metadata.
      |
   +--+--+
   |     |
  QUIC  BLE            // separate crates. no knowledge of crypto or peer identity.
```

Transports are dumb. They move bytes. Everything else (crypto, routing, bundling)
lives in `pathweave-core`. Adding a new transport (WiFi Direct, SMS, USSD) means
implementing one trait. It doesn't touch anything else.

---

## Crates

| Crate | What it does |
|---|---|
| `pathweave-core` | Transport trait, PathweaveNode, Router, Session layer, Bundle layer |
| `pathweave-transport-quic` | QUIC implementation of Transport |
| `pathweave-transport-ble` | BLE implementation of Transport. btleplug for central mode (scanning, GATT); bluer on Linux, WinRT on Windows, CoreBluetooth on macOS for peripheral mode (advertising). Native platform APIs for mobile. |
| `pathweave-uniffi` | FFI bindings layer. Exposes PathweaveNode to Swift and Kotlin. |
| `pw-chat` (example) | Terminal chat demo. The v0.1.0 launch artifact. |

Build order: core, then quic, then ble, then uniffi, then pw-chat. Each one builds on
the previous.

---

## The public API

The public API has seven surfaces: four exposed through UniFFI to Swift and Kotlin,
three Rust-only methods that are not part of the FFI boundary.

**UniFFI-facing (four):**

```rust
// 1. create a node
let node = PathweaveNode::new(config, identity).await?;

// 2. send a message
node.send(peer_id, payload).await?;

// 3. receive incoming messages
node.on_message(handler); // handler implements MessageHandler (see below)

// 4. subscribe to transport events
let mut events = node.events(); // Stream<Item = TransportEvent>
```

**Rust-only (three, not in the UDL):**

```rust
// register a transport before first use
node.register_transport(Box::new(quic));

// dial a peer, complete the Noise_XX handshake, store the PeerId -> address mapping
// the session closes immediately after the handshake; send() re-dials on each call
node.connect(announcement).await?;

// inject a known peer address (e.g. QUIC --peer <address> from the command line)
node.add_peer(peer_id, announcement);
```

`connect()` tries transports in cost order (Free first). It returns the PeerId learned
from the Noise_XX handshake. Subsequent `send()` calls look up the stored address and
re-dial lazily.

**MessageHandler** is a callback interface, not a closure. This is a UniFFI requirement --
closures don't cross FFI boundaries cleanly. In Rust, you implement the trait. In Swift,
you implement the protocol. In Kotlin, you implement the interface.

```rust
pub trait MessageHandler: Send + Sync {
    fn on_message(&self, peer_id: PeerId, payload: Vec<u8>);
}
```

The callback is synchronous at the boundary. It receives bytes and returns. Any async
work on the caller's side is the caller's responsibility.

**TransportEvent** covers the events any real application will consume:

```rust
pub enum TransportEvent {
    TransportChanged {
        from: Option<TransportKind>,  // None on first connection
        to: TransportKind,
    },
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
}

pub enum TransportKind {
    Quic,
    Ble,
}
```

`events()` is what powers the "switched to BLE" line in pw-chat. These enums must be
defined before the UDL is written because they cross the UniFFI boundary.

---

## NodeIdentity

```rust
let identity = NodeIdentity::generate();          // fresh random keypair -- caller must persist
let identity = NodeIdentity::from_bytes(bytes)?;  // restored from wherever the caller keeps it
let node     = PathweaveNode::new(config, identity).await?;
```

Pathweave never touches key persistence. The caller decides where keys live: iOS Secure
Enclave, Android Keystore, a server secrets manager, or a file in tests. Keeping it
outside the library means we're not making that decision for people on every platform.

It also makes testing clean. Pass a known keypair, get a predictable PeerId.

One thing worth calling out for server deployments: a server must persist its identity.
If it generates a new keypair on restart it gets a new PeerId, and every client that
cached the old one can no longer reach it.

---

## PeerId

```
PeerId = base58( blake3( noise_static_public_key ) )
```

Stored internally as `[u8; 32]`. The base58 encoding (about 44 characters) is for display
and the UniFFI boundary only.

Why Blake3 over SHA-256: faster, and it's what modern security-focused Rust projects
reach for now. Well-understood, no patent concerns, actively maintained.

Why base58 over hex: 44 characters instead of 64, and no ambiguous characters (no 0, O,
I, l). Safe to read aloud, type by hand, or put in a QR code.

---

## The Transport trait

```rust
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
}

#[async_trait]
pub trait Connection: Send + Sync {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()>;
    async fn recv_bytes(&mut self) -> Result<Bytes>;
    async fn close(&mut self) -> Result<()>;
    fn mtu(&self) -> usize;
}
```

`start()` receives the node's identity so transports that advertise (BLE peripheral) can
derive the `short_id` from `identity.peer_id()` without knowing anything about PeerId
derivation themselves. See ADR 010.

`discover()` returns `BoxStream<'static, ...>` not `BoxStream<'_, ...>`. The stream must
outlive the `&self` borrow because the Router drives it from a spawned background task
after the `register_transport()` call returns. The stream owns all the state it needs
(the mDNS channel receiver, the btleplug adapter clone) so no lifetime tie to `self` is
required.

`PeerAnnouncement` carries a transport-level address: a BLE MAC address or a QUIC socket
address. It is not a PeerId. The Noise handshake happens above this in the Session layer.
Transports have no knowledge of peer identity.

`mtu_hint()` on Transport is a conservative pre-connection estimate, useful for routing
decisions. `mtu()` on Connection is the actual negotiated MTU after the connection is
established, which is what the Bundle layer uses for fragmentation.

Two Rust things worth noting here because they'll bite if forgotten:

`discover()` returns `BoxStream` not `impl Stream`. The trait needs to be object-safe
for `Box<dyn Transport>`, and `impl Trait` in trait methods doesn't work with dynamic
dispatch.

`close()` takes `&mut self` not `self`. You can't move out of a `Box<dyn Connection>`.

---

## TransportCost

```rust
pub enum TransportCost {
    Free,      // BLE -- no data usage, no monetary cost
    Metered,   // QUIC -- conservative default for all QUIC connections (see note below)
    Unknown,
}
```

`Metered` covers two situations that are not the same: QUIC over WiFi (flat monthly fee,
functionally free) and QUIC over mobile data (per-megabyte, genuinely metered). Detecting
which one you're on requires platform-specific OS APIs. Currently, all QUIC connections
report `Metered` as the conservative default. The v0.2.0 cost intelligence work will
replace this with WiFi vs. mobile data detection. Battery level, signal quality, and
payload-size routing are deferred beyond v0.2.0.

---

## The routing layer

Static priority fallback. That's what it is and what we call it.

1. If BLE is available and the peer is reachable over BLE, use BLE.
2. Otherwise, use QUIC.

The routing gets real cost intelligence later in v0.2.0. For now, free transport wins.

**How the switch works: health monitoring, lazy connections**

The router runs a `health_monitor` background task for each registered transport. On
startup, the task calls `start(&identity)` and marks the transport available if it
succeeds. It then polls the non-loopback IPv4 address set every 3 seconds
(`NETWORK_POLL_INTERVAL`). When the address set changes (a network interface came up or
went away), it calls `stop()` and restarts the transport with `start()`, updating the
`AtomicBool` availability flag accordingly.

If `start()` fails, the monitor enters recovery mode: it retries `start()` unconditionally
on every tick instead of comparing address sets. This handles the case where an interface
stays down: the address set would never change, so a pure change-detection loop would
never retry. See ADR 013.

Connections are lazy. The router doesn't maintain active connections to every peer on
every transport simultaneously. It maintains transport availability state and opens a
connection on the best available transport when `send()` is called.

**Retry behavior**

`send()` retries up to `MAX_SEND_ATTEMPTS` (3) times across available transports, with
a 1-second back-off (`RETRY_BACKOFF`) between attempts. Each attempt dials the peer,
completes the Noise_XX handshake, sends the framed payload, and waits for the delivery
ACK. If all attempts fail, `send()` returns `PathweaveError::DeliveryFailed`. See the
delivery guarantees section.

---

## Incoming message delivery (the accept loop)

`register_transport()` on `PathweaveNode` does three things: registers the transport
with the Router (which starts its `health_monitor` task), spawns an `accept_loop` task
that delivers incoming connections to the message handler, and spawns a `peer_stream`
task that drives `discover()` and populates the peer table for outbound routing. All
three tasks run for the lifetime of the node.

The accept loop:

```
wait for health_monitor to signal start() success   // prevents startup race
loop {
    conn = transport.accept().await
    if error: log, sleep 5 s, retry    // handles transports that don't support inbound
    spawn handle_incoming(conn)
}
```

`handle_incoming` runs per-connection:

```
BundleLayer::new(conn)               // wraps in bundle framing
Session::respond(identity, bundled)  // Noise_XX handshake as responder
peer_id = session.peer_id()          // identity revealed after handshake
loop:
    payload = session.recv()         // payload = [message_id: u64 big-endian] ++ [app bytes]
    if payload.len() < 8:
        session.send(b"")            // ACK malformed frame so sender doesn't hang
        continue
    message_id = payload[0..8]       // extract 8-byte ID prepended by the sender
    if dedup_cache.check_and_insert(peer_id, message_id):
        session.send(b"")            // ACK even on duplicate -- stops the sender retrying
        continue
    handler.on_message(peer_id, payload[8..])   // deliver app bytes (ID stripped)
    session.send(b"")                // empty ACK to the sender (see note below)
```

**Why the ACK is required:** Quinn's `write_all()` writes to an internal send buffer; actual transmission is async. If the sender drops the session immediately after `send()`, Quinn fires `CONNECTION_CLOSE` before flushing the buffer and the data is silently lost. The receiver's empty ACK keeps the sender's connection alive long enough for the data to be transmitted. `send()` returning `Ok(())` is only meaningful when this round-trip completes. See ADR 009 and issue #34.

**Why the message ID exists:** `send()` retries up to 3 times on failure. If the first
attempt delivers the payload and the ACK is lost in transit, the second attempt carries
the same message ID. The `DeduplicationCache` at the receiver suppresses the duplicate
call to `on_message()` but still sends the ACK so the sender stops retrying. Cache
entries are keyed on `(PeerId, message_id)` and expire after `DEDUP_TTL` (60 seconds).
Given `MAX_SEND_ATTEMPTS = 3` and `RETRY_BACKOFF = 1s`, the entire retry window is well
under 60 seconds, so a duplicate will always hit a live cache entry. See ADR 011.

The handler is stored as `Arc<Mutex<Option<Box<dyn MessageHandler>>>>`. The Arc is
cloned into each accept loop task so a single handler registration covers all
transports. Locking is brief: the `on_message` call is synchronous and the lock is
released before the next recv().await.

Transports that don't support inbound connections (BLE central-only mode) return
an error from `accept()`. The accept loop catches this, logs it at debug level, and
backs off for 5 seconds before retrying. This means registering a central-only BLE
transport does not produce a tight busy loop.

---

## The session layer

Handles the Noise handshake and all encryption and decryption. Lives entirely in
`pathweave-core`. Never in the transport crates. Crypto in one place.

Handshake pattern: `Noise_XX_25519_ChaChaPoly_BLAKE2s`

Why ChaChaPoly over AES-GCM: mobile ARM processors don't have hardware AES acceleration,
so AES-GCM runs in software and it's slower. ChaChaPoly was built for exactly this
situation. WireGuard made the same call for the same reason.

Why BLAKE2s over BLAKE2b: BLAKE2s is optimized for 32-bit processors. The cheap Android
phones that are the primary target for this library run 32-bit or constrained 64-bit
environments where this matters.

Why Noise_XX and not Noise_XK: BLE discovers strangers. You don't know their static key
before you connect. Noise_XK requires knowing the responder's key before initiating, which
means you can't use it for unknown nearby peers. Noise_XX lets both sides reveal keys
during the handshake, so you can connect to anyone you discover.

Noise_XK is the v0.3.0 upgrade, once we have a contact and key registry. The pattern
string changes; the crate doesn't.

After the handshake, the peer's static public key is known. PeerId is derived here.

Crate: `snow` v0.10, RustCrypto backends. The underlying primitives (x25519-dalek,
chacha20poly1305) are independently verified. The ring backend uses hand-optimized
assembly that's harder to reason about and has had maintenance issues. We use RustCrypto.

---

## The bundle layer

BPv7 (RFC 9171) via the `bp7` crate. Handles message framing, bundle IDs, fragmentation
for transports with small MTUs, and reassembly on the receiving end.

The bundle layer asks the Connection for its actual negotiated `mtu()`, then fragments
accordingly. The transport never sees a message larger than its MTU. The application
never sees a fragmented message.

---

## QUIC transport -- discovery

`QuicTransport::discover()` returns a live stream of peers found via mDNS
(`_pathweave._udp.local.`). `start()` registers the node as an mDNS service on the
local network and begins browsing for other Pathweave services. Each resolved service
maps to a `PeerAnnouncement` with a `PeerAddress::Quic` socket address. See ADR 012.

In pw-chat, the `--peer <address>` argument constructs a `PeerAnnouncement` directly
from the socket address and bypasses discovery entirely. This is intentional: mDNS
works on local networks where multicast is available; direct addressing works everywhere.

## QUIC TLS

QUIC requires TLS 1.3; it's in the spec (RFC 9000) and there's no way around it. We
satisfy the requirement with a fresh ephemeral self-signed certificate generated at
startup and thrown away when the session ends.

All real security comes from Noise_XX. The TLS layer is just the protocol requirement.
We keep the cert completely separate from the Noise keypair so the two layers stay
independent. SECURITY.md documents this clearly because it's an important thing for
anyone integrating Pathweave to understand.

Crates: `quinn` v0.11, `rcgen` for certificate generation.

## QUIC stream framing

QUIC is a byte stream, not a message stream. The Bundle layer sends complete BPv7
bundles and expects to receive complete bundles. To reassemble messages correctly,
`QuicConnection` prefixes every write with a 4-byte big-endian length, then reads the
same 4-byte prefix on the receiving end before reading exactly that many payload bytes.

Any future transport that wraps a byte-stream protocol (TCP, WebSocket in binary mode,
etc.) should use the same framing convention for consistency.

---

## BLE peer discovery

Two phases: advertise first, connect second.

**Phase 1 -- advertisement**

```
Pathweave service UUID:       82dfc0ba-e2b5-4e65-ad11-c7238ca545c9
Service data:                 [version: u8 = 0x01] ++ [short_id: [u8; 8]]
```

`short_id` is the first 8 bytes of the node's PeerId. It is included in the
advertisement so that future contact-aware filtering (v0.3.0, once there is a key
registry) can skip the Noise handshake for unknown peers. Currently the `peer_stream`
task connects to every node carrying the Pathweave service UUID regardless of short_id;
the full PeerId is only known after the Noise_XX handshake completes. The payload
fits in the classic BLE advertisement limit of about 31 bytes, which gives us the
widest hardware compatibility.

**Phase 2 -- GATT connection**

```
Write characteristic:     3439992c-8453-4ca3-9688-639ef5f6f5dc
                          Write Without Response mode.
                          No GATT-level ack needed -- Pathweave handles its own reliability.

Notify characteristic:    6de63378-8bc3-4e87-8892-0a9a80efff64
                          Notify mode.
```

After the GATT connection: Noise_XX handshake over these two characteristics, full
static public key revealed, full PeerId derived, communication begins.

These UUIDs are permanent. They are part of the protocol. Changing them after any
deployment breaks every integration silently.

**Implementation split: central and peripheral mode**

BLE has two roles: central (scanning, initiating GATT connections) and peripheral
(advertising, accepting GATT connections). No single Rust crate covers both roles
well across all platforms, so we split them.

Central mode: `btleplug` v0.11, all platforms. Handles scanning and GATT connections.

Peripheral mode: platform-specific.

- Linux: `bluer` v0.17, shipped in v0.2.0. A well-maintained async Rust crate that
  wraps the full BlueZ D-Bus API including advertising. See ADR 014.
- Android and iOS: the platform's native BLE APIs called through the UniFFI layer.
  CoreBluetooth on iOS, BluetoothManager on Android. The `pathweave-transport-ble`
  Rust crate is not used for peripheral mode on mobile; the native layer handles it.
- macOS: `objc2-core-bluetooth` v0.3.2 via `objc2` bindings, shipped in v0.2.0.
  CoreBluetooth's `CBPeripheralManager` with `CBCharacteristicPropertyWrite` (not
  WriteWithoutResponse; the platform does not deliver write commands to the delegate).
  Advertisement includes service UUID only; service data is silently ignored by macOS.
  See ADR 014.
- Windows: `windows` crate v0.58, shipped in v0.2.0. WinRT GATT Server API
  (`Windows.Devices.Bluetooth.GenericAttributeProfile`). See ADR 014.

**BLE peripheral and NodeIdentity**

`Transport::start(&identity)` passes the node's identity to `BleTransport` so
`start_peripheral()` can derive `short_id` (first 8 bytes of `identity.peer_id()`)
for the advertisement payload. The transport never stores the identity beyond `start()`;
it uses only the derived short_id. See ADR 010.

---

## Delivery guarantees

At-least-once confirmed delivery. `send()` retries up to `MAX_SEND_ATTEMPTS` (3) times
on transient failures, with a 1-second back-off between attempts. `send()` returns
`Ok(())` when the receiver has acknowledged delivery via the session ACK round-trip.

Each attempt carries the same 8-byte message ID (generated from OS entropy before the
first attempt). The receiver's `DeduplicationCache` suppresses duplicate delivery to
`on_message()` if a retry arrives after a successful first delivery where the ACK was
lost. The ACK is sent regardless of whether the message is a duplicate, so the sender
stops retrying. See ADR 011.

No guarantee of ordering or exactly-once semantics across sessions. If all `MAX_SEND_ATTEMPTS`
fail, `send()` returns `PathweaveError::DeliveryFailed`. When no transport can reach the
peer at all, `send()` returns `PathweaveError::NoTransportAvailable` immediately. No
silent queuing.

---

## Servers

A server running Pathweave is just another node. It has a PeerId, runs Noise_XX sessions,
and communicates through the same API as any other peer. `node.send(server_peer_id, payload)`
works the same whether the peer is a phone or a machine in a data centre.

"Peer-to-peer" describes the addressing model, not a serverless architecture. The README
makes this clear upfront. Developers with existing servers need to know Pathweave works
with them, not against them.

---

## pw-chat

The launch demo. Two people run it. They're talking over QUIC. Internet goes down. The
app switches to BLE automatically. The conversation continues.

```
pw-chat --peer 192.168.1.42:9001   // connect to a known QUIC address
pw-chat                             // listen mode -- accepts BLE and QUIC connections
```

QUIC peer discovery is address-based for direct connections, or automatic via mDNS on
local networks. BLE discovery is automatic: the transport scans for the Pathweave service
UUID and connects without configuration.

When neither transport can reach the peer: "message failed: no transport available."
No queuing, no silent retry.

BLE in pw-chat works when at least one peer can advertise. Supported configurations:
two Linux machines, a Linux or Windows machine and a macOS machine, or any combination
with a phone running the native SDK.

---

## What this version doesn't do

Being clear about this matters as much as being clear about what it does.

- No WiFi Direct, SMS, or USSD transports
- No MLS group key exchange (Noise_XX is 1:1 only)
- No multi-hop BLE routing (single-hop only)
- No contact or key registry (Noise_XK upgrade deferred to v0.3.0)
- No OS network event integration for health monitoring (polling via if-addrs; rtnetlink,
  NWPathMonitor, and WinRT network events deferred to v0.3.0). See ADR 013.
- No WiFi vs. mobile data detection for QUIC cost reporting (v0.2.0 cost intelligence
  work not yet complete; all QUIC connections currently report Metered)
- macOS BLE peripheral mode compiled from Windows; needs hardware verification on macOS
- No security audit completed

v1.0.0 means the API is stable and the library is ready for production use. We're
pursuing a security audit for that milestone and it's a real goal. Whatever the audit
status is when v1.0.0 ships, SECURITY.md documents it honestly.

v0.x.0 means the API is still evolving and no stability guarantees are made.
