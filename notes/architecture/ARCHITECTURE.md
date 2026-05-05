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

Transports are dumb. They move bytes. Everything else -- crypto, routing, bundling --
lives in `pathweave-core`. Adding a new transport (WiFi Direct, SMS, USSD) means
implementing one trait. It doesn't touch anything else.

---

## Crates

| Crate | What it does |
|---|---|
| `pathweave-core` | Transport trait, PathweaveNode, Router, Session layer, Bundle layer |
| `pathweave-transport-quic` | QUIC implementation of Transport |
| `pathweave-transport-ble` | BLE implementation of Transport. btleplug for central mode (scanning, GATT); bluer for peripheral mode (advertising) on Linux; native platform APIs on mobile. |
| `pathweave-uniffi` | FFI bindings layer. Exposes PathweaveNode to Swift and Kotlin. |
| `pw-chat` (example) | Terminal chat demo. The v0.1.0 launch artifact. |

Build order: core, then quic, then ble, then uniffi, then pw-chat. Each one builds on
the previous.

---

## The public API

The public API has six surfaces: four exposed through UniFFI to Swift and Kotlin,
two Rust-only setup methods that are not part of the FFI boundary.

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

**Rust-only setup (two, not in the UDL):**

```rust
// register a transport before first use
node.register_transport(Box::new(quic));

// inject a known peer address (e.g. QUIC --peer <address> from the command line)
// the PeerId -> PeerAnnouncement mapping is populated here so send() can route to it
node.add_peer(peer_id, announcement);
```

`connect(announcement: PeerAnnouncement) -> Result<PeerId>` dials the peer, completes
the Noise_XX handshake as the initiator, stores `(PeerId, PeerAnnouncement)` in the
peer table, and returns the PeerId. The session is closed immediately after the handshake;
`send()` re-dials on each call (lazy connections). Tries transports in cost order.

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
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    fn discover(&self) -> BoxStream<'_, PeerAnnouncement>;
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

Simple for v0.1.0. Gets richer in v0.2.0 when we add real cost intelligence: battery
level, signal quality, payload size, WiFi vs mobile data.

`Metered` covers two situations that are not the same: QUIC over WiFi (flat monthly fee,
functionally free) and QUIC over mobile data (per-megabyte, genuinely metered). Detecting
which one you're on requires platform-specific OS APIs. v0.1.0 reports `Metered` for all
QUIC connections as the conservative default. The v0.2.0 cost intelligence work starts
here.

---

## The routing layer (v0.1.0)

Static priority fallback. That's what it is and what we call it.

1. If BLE is available and the peer is reachable over BLE, use BLE.
2. Otherwise, use QUIC.

The routing gets real cost intelligence in v0.2.0. For now, free transport wins.

**How the switch works: proactive health monitoring, lazy connections**

The router runs a background task for each registered transport that monitors availability
continuously. For QUIC, this means tracking connection health through keepalives and OS
network events. For BLE, this means continuous scanning. Transport state is always current.

When a transport's state changes (QUIC drops, BLE comes into range), the router reacts
immediately -- not after a send() timeout. This is what makes the pw-chat demo work
cleanly: when WiFi goes down, BLE is already in the router's known-reachable list and
the switch is instant.

Connections are lazy. The router doesn't maintain active connections to every peer on
every transport simultaneously. It maintains transport state (available/unavailable) and
opens a connection on the best available transport when send() is called.

There is a narrow window between when a transport actually drops and when the monitoring
task detects it. A send() call in that window will attempt the now-dead transport, get a
transport error, and retry on the next best available transport. At most one message
experiences this delay during a failover. This is acceptable behavior.

---

## Incoming message delivery (the accept loop)

`register_transport()` on `PathweaveNode` does two things: hands the transport to the
Router for outbound use, and spawns an accept loop task that runs for the lifetime of
the node.

The accept loop:

```
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
    payload = session.recv()
    handler.on_message(peer_id, payload)  // delivered to the registered handler
    session.send(b"")                // empty ACK to the sender (see note below)
```

**Why the ACK is required:** Quinn's `write_all()` writes to an internal send buffer; actual transmission is async. If the sender drops the session immediately after `send()`, Quinn fires `CONNECTION_CLOSE` before flushing the buffer and the data is silently lost. The receiver's empty ACK keeps the sender's connection alive long enough for the data to be transmitted. `send()` returning `Ok(())` is only meaningful when this round-trip completes. See ADR 009 and issue #34.

The handler is stored as `Arc<Mutex<Option<Box<dyn MessageHandler>>>>`. The Arc is
cloned into each accept loop task so a single handler registration covers all
transports. Locking is brief: the `on_message` call is synchronous and the lock is
released before the next recv().await.

Transports that don't support inbound connections (BLE central mode in v0.1.0) return
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

Noise_XK is the v0.2.0 upgrade, once we have a contact and key registry. The pattern
string changes; the crate doesn't.

After the handshake, the peer's static public key is known. PeerId is derived here.

Crate: `snow` v0.10, RustCrypto backends. The underlying primitives (x25519-dalek,
chacha20poly1305) are independently verified. The ring backend uses hand-optimized
assembly that's harder to reason about and has had maintenance issues. We use RustCrypto.

---

## The bundle layer

BPv7 (RFC 9171) via the `bp7` crate. Handles message framing, bundle IDs (which lay
the groundwork for at-least-once delivery later), fragmentation for transports with small
MTUs, and reassembly on the receiving end.

The bundle layer asks the Connection for its actual negotiated `mtu()`, then fragments
accordingly. The transport never sees a message larger than its MTU. The application
never sees a fragmented message.

---

## QUIC transport -- discovery

QUIC has no automatic peer discovery. `discover()` on the QUIC transport returns an
empty stream. This is intentional. In pw-chat, the `--peer <address>` argument
constructs a `PeerAnnouncement` directly from the socket address and calls `connect()`
without going through discovery at all. When someone reads this in the code later, it's
not a bug -- it's how QUIC works in Pathweave. mDNS-based automatic discovery is a
v0.2.0 addition.

## QUIC TLS

QUIC requires TLS 1.3 -- it's in the spec (RFC 9000) and there's no way around it. We
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

`short_id` is the first 8 bytes of the node's PeerId. A scanning node sees: this is a
Pathweave peer, here are the first 8 bytes of their identity. Enough to decide whether
to connect, without any handshake overhead. The payload fits in the classic BLE
advertisement limit of about 31 bytes, which gives us the widest hardware compatibility.

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

- Linux: `bluer`, a well-maintained async Rust crate that wraps the full BlueZ API
  including advertising.
- Android and iOS: the platform's native BLE APIs called through the UniFFI layer.
  CoreBluetooth on iOS, BluetoothManager on Android. The `pathweave-transport-ble`
  Rust crate is not used for peripheral mode on mobile; the native layer handles it.
- macOS: deferred to v0.2.0. CoreBluetooth is available on macOS (same framework as
  iOS); the native binding work is not prioritized for v0.1.0 given the primary
  deployment targets are phones.
- Windows: deferred to v0.2.0.

**macOS and Windows peripheral mode**

macOS has CoreBluetooth, the same framework used for iOS peripheral mode. The path
exists. But the native binding work for macOS desktop is separate from the iOS UniFFI
path, and the primary deployment targets for v0.1.0 are phones. macOS peripheral mode
is deferred to v0.2.0.

Windows has OS-level BLE GATT Server support (WinRT, since Windows 10 Creators
Update) but no maintained Rust crate wraps it reliably. Implementing it requires
writing a WinRT wrapper using the `windows` crate, which involves COM apartment
threading rules that interact with async Rust runtimes in ways that produce subtle,
hardware-dependent bugs. The risk and timeline cost are not justified for v0.1.0
given that the primary deployment targets are phones.

BLE peripheral development for v0.1.0 requires a Linux machine or a phone via the
native bindings path. A macOS or Windows machine cannot act as the advertising peer
in v0.1.0.

---

## Delivery guarantees (v0.1.0)

Single-attempt confirmed delivery. `send()` returns `Ok(())` when the receiver has
acknowledged delivery via the session ACK round-trip. No guarantee of ordering or
exactly-once semantics. If the connection fails before the ACK arrives, `send()` returns
an error -- there is no automatic retry. BPv7 bundle IDs are there to support full
at-least-once delivery with retry later, but v0.1.0 doesn't implement it.

This is stronger than "bytes handed to the transport" but weaker than at-least-once.
The README and docs say this plainly. A developer who knows the constraint can design
around it.

When no transport can reach the peer, `send()` returns
`PathweaveError::NoTransportAvailable` immediately. No silent queuing.

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

The v0.1.0 demo. Two people run it. They're talking over QUIC. Internet goes down. The
app switches to BLE automatically. The conversation continues.

```
pw-chat --peer 192.168.1.42:8080   // connect to a known QUIC address
pw-chat                             // listen mode -- accepts BLE and QUIC connections
```

QUIC peer discovery is address-based. You know the address. BLE discovery is automatic.
The transport scans and connects without any configuration. mDNS for automatic QUIC
discovery is a v0.2.0 addition.

When neither transport can reach the peer: "message failed: no transport available."
No queuing, no silent retry. Known v0.1.0 limitation, documented as one.

BLE in pw-chat works when at least one peer can advertise. Supported configurations
for the BLE fallback demo: two Linux machines, or a Linux machine and a phone running
the native SDK. macOS and Windows machines cannot act as the advertising peer in v0.1.0.

---

## What v0.1.0 doesn't do

Being clear about this matters as much as being clear about what it does.

- No WiFi Direct, SMS, or USSD transports
- No MLS group key exchange (Noise_XX is 1:1 only)
- No at-least-once delivery
- No automatic QUIC peer discovery (mDNS)
- No multi-hop BLE routing (single-hop only)
- No contact or key registry (Noise_XK upgrade deferred to v0.2.0)
- No BLE peripheral mode (advertising) on macOS or Windows (deferred to v0.2.0)
- No security audit completed

v1.0.0 means the API is stable and the library is ready for production use. We're
pursuing a security audit for that milestone and it's a real goal. Whatever the audit
status is when v1.0.0 ships, SECURITY.md documents it honestly.

v0.x.0 means the API is still evolving and no stability guarantees are made.
