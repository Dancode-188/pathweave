# ADR 018: Broadcast transport design — destination short_id in every packet

**Status:** Accepted

## Context

The `Transport` trait models a connection-oriented channel: `connect()` opens a connection
to one peer, `accept()` yields an incoming connection from one peer. This maps directly
to QUIC (one QUIC stream per peer) and BLE GATT (one GATT connection per peripheral).

Two planned transports do not fit this model:

**BLE advertising-mode bearer.** BLE advertisement packets are small broadcast frames sent
by a peripheral to every device within radio range, not to a specific recipient. There is
no connection setup and no addressing at the BLE layer.

**Acoustic transport.** ggwave encodes data into audible tones and broadcasts them into
the air. Any device within earshot receives the signal. Like BLE advertisements, acoustic
frames have no link-layer addressing.

Both transports are useful precisely because they do not require a prior connection: they
work when all other transports are offline. But their broadcast nature means every node
in range receives every frame, regardless of who it is for.

Two design options exist:

**Option A — destination identifier in every packet header.** Each broadcast frame
carries an 8-byte destination short_id in its header. Nodes discard frames not addressed
to their own short_id. The Transport trait is unchanged: broadcast transports return
virtual connections that filter the shared medium by short_id pair.

**Option B — shared BroadcastTransport abstraction.** Extract the common broadcast
pattern into a new trait or base type. The router learns about broadcast semantics and
drives them differently from connection-oriented transports.

## Decision

Use Option A. Every broadcast packet begins with a fixed 16-byte header:

```
[dest_short_id:   8 bytes]   first 8 bytes of the destination PeerId
[source_short_id: 8 bytes]   first 8 bytes of the sender's PeerId
[payload:         N bytes]   Noise-encrypted content
```

`short_id` is already used for BLE advertisement filtering and is defined as the first
8 bytes of the raw PeerId bytes (blake3 of the static public key). It is not a new
concept.

A receiving node reads the `dest_short_id` and silently drops the frame if it does not
match the node's own short_id. If it matches, the frame is handed to the virtual
connection for the `source_short_id` peer (or used to open a new incoming connection if
no such virtual connection exists).

Broadcast transports implement the existing `Transport` trait:

- `connect(peer)` returns a `BroadcastConnection` that writes packets with the peer's
  short_id in `dest_short_id` and the local node's short_id in `source_short_id`, and
  reads only packets where `source_short_id` matches the peer and `dest_short_id`
  matches the local node.

- `accept()` waits for a packet addressed to the local node from any source, creates a
  `BroadcastConnection` scoped to that source short_id, and returns it. Each broadcast
  transport implementation maintains an internal dispatcher that reads the shared medium
  and routes packets to the correct virtual connection or queues them for the next
  `accept()` call.

Each broadcast transport defines its own `PeerAddress` variant and `TransportKind`
variant. The transport's `discover()` stream emits `PeerAnnouncement` entries with
`short_id` populated from the packet header as peers are observed.

Option B is deferred. Once both BLE advertising-mode and acoustic are implemented and
their shared patterns are observed in real code, a common abstraction can be extracted.
Premature abstraction before two concrete implementations exist would likely produce the
wrong interface.

## Rationale

**Why Option A over Option B?**

Option B requires designing a new abstraction before we have two working implementations
to learn from. The existing `Transport` trait already works: connection-oriented semantics
over a broadcast medium are a well-understood pattern (UDP unicast over Ethernet multicast
works the same way). Adding a new trait now would delay implementation and risk designing
the wrong interface.

Option A lets us build BLE advertising-mode and acoustic as independent crates using the
existing `Transport` contract. If both implementations reveal a common pattern worth
abstracting, that abstraction emerges from working code rather than speculation.

**Why 8 bytes for short_id?**

8 bytes gives a collision probability of 1 in 2^64 between any two peers in range. For
a broadcast medium where all nodes in range hear all packets, collisions cause a node to
process frames intended for another node, which Noise decryption will reject. 8 bytes is
sufficient: in practice, Pathweave nodes within acoustic or BLE advertising range number
in the tens, not the billions.

**Why include source_short_id in the header?**

A virtual connection is scoped to a (source, destination) pair. Without source_short_id,
`accept()` cannot distinguish packets from peer A and peer B both arriving simultaneously.
Including source_short_id allows the internal dispatcher to route correctly without
inspecting the encrypted Noise payload.

**Why not use the full 32-byte PeerId?**

Acoustic throughput in audible mode is 8–16 bytes per second. A 32-byte destination
field alone would take 2–4 seconds to transmit. For acoustic specifically, a 16-byte
header is already a significant fraction of a typical message. The 8-byte short_id is a
pragmatic tradeoff between collision resistance and bandwidth.

Acoustic transport implementations may reduce the header further (for example, 4 bytes
per field for an 8-byte total header) at the cost of higher collision probability. This
is a transport-level decision documented in the acoustic transport implementation, not
overridden here.

**Security note: the header is visible in plaintext.**

`dest_short_id` and `source_short_id` are not encrypted. A passive observer in range
sees who is talking to whom. `short_id` is pseudonymous: it is derived from the static
public key via blake3, which links it to the node's persistent identity but does not
reveal the key itself.

For the close-range scenarios where broadcast transports operate (two devices in the
same room), the threat model is an eavesdropper also in the same room. Metadata
protection (hiding the communication graph) is deferred to the Sphinx anonymous routing
work planned for a later release.

## Implications

- `Transport` trait is unchanged. Broadcast transports implement it with virtual
  connection types backed by an internal packet dispatcher.
- Each broadcast transport (BLE advertising-mode, acoustic) adds a new `PeerAddress`
  variant and a new `TransportKind` variant. The `PeerAddress::kind()` match and any
  other exhaustive matches over `TransportKind` must be updated when each transport is
  added.
- `short_id` in `PeerAnnouncement` becomes load-bearing for broadcast transports. It is
  already `Option<[u8; 8]>` and populated by the BLE transport. Broadcast transports
  require it to be `Some`.
- The internal packet dispatcher in each broadcast transport is responsible for
  deduplication (the shared medium may deliver the same frame multiple times in rapid
  succession, particularly for acoustic where ggwave repeats transmissions for
  reliability).
- The Noise handshake runs over the virtual connection as-is. Broadcast transports do
  not require any Noise layer changes.

## Addendum: legacy BLE advertising cannot carry the 16-byte header (2026-06-17)

Legacy BLE advertising's AD payload budget is 31 bytes total. A custom 128-bit vendor
service UUID (the kind used everywhere else in this codebase, including
`PATHWEAVE_SERVICE_UUID`) costs 18 bytes of Service Data AD structure overhead by
itself (1 length byte + 1 AD type byte + 16 UUID bytes), plus another 3 bytes for the
mandatory Flags structure. That leaves 10 bytes for header and payload combined, less
than this ADR's 16-byte header alone. There is no header size, including the reduced
sizes this ADR already permits for bandwidth-constrained transports, that leaves room
for a Noise-encrypted payload once a minimum ChaChaPoly authentication tag (16 bytes)
is accounted for. This held on both platforms checked: BlueZ's 31-byte legacy AD limit
on Linux and Windows' identical 31-byte limit for `BluetoothLEAdvertisementPublisher`
are the same constraint, since both are bound by the same BLE 4.x link-layer spec, not
an OS-specific choice.

BLE advertising-mode bearer therefore requires BLE 5.0 extended advertising as its
baseline, not legacy advertising: `secondary_channel` on `bluer::adv::Advertisement`
(Linux) or `UseExtendedAdvertisement` on `BluetoothLEAdvertisementPublisher` (Windows).
Extended advertising's data length budget is adapter-dependent but well above legacy's
31 bytes on any BLE 5.0 controller. This does not change the header format or any other
part of this ADR's decision; it changes which BLE advertising mode each platform
implementation must use to fit that header at all.

macOS is out of scope for this transport, not pending verification like the existing
GATT-based BLE transport's macOS gap. `CBPeripheralManager` does not expose custom
service data to advertisements under any advertising mode; this is an Apple API
restriction already documented in `pathweave-transport-ble/src/lib.rs` for the GATT
peripheral's advertisement (`CBAdvertisementDataServiceDataKey` is silently ignored).
Extended advertising does not change what CoreBluetooth exposes to applications. Same
permanent status as WiFi Direct on macOS.
