# ADR 022: WiFi Direct transport design

**Status:** Accepted

## Context

The QUIC transport requires an existing IP network: both nodes must share an AP, a VPN,
or some other routable path. The BLE GATT transport operates without infrastructure but
is limited to roughly 30–50m and low throughput. Two Pathweave nodes within a few hundred
meters with no shared AP and no BLE proximity cannot reach each other via either
transport.

WiFi Direct (IEEE 802.11 P2P) creates a direct device-to-device IP link without an
access point. One device becomes a Group Owner (GO) and broadcasts a softAP; the other
connects as a client. Once the link is established, both ends communicate over a normal
IP interface at WiFi speeds across roughly 200m line of sight and 50m indoors. The
infrastructure cost is zero.

Two design choices must be made before implementation:

**How deep should the Transport implementation go?** WiFi Direct could be modelled as
either a full transport (handles discovery, P2P link establishment, and data framing) or
a connectivity-only layer (handles link setup, then hands off IP addresses to the QUIC
transport for data).

**Which data protocol runs over the P2P IP link?** Once both ends have routable addresses
on the P2P interface, any IP protocol can be used: raw TCP, raw UDP, or QUIC.

## Decision

### Full Transport implementation (Option A)

`WifiDirectTransport` implements the `Transport` trait end-to-end: discovery, P2P
negotiation, and data framing. The router sees it as an independent transport, identical
in structure to `BleTransport` and `QuicTransport`.

The alternative (Option B) is a connectivity-only layer that establishes the P2P link
and then reports the resulting IP addresses to the QUIC transport. Option B avoids
duplicating framing logic but creates a dependency between two transport crates and
requires the router to understand that a QUIC connection on a particular interface was
set up by WiFi Direct. Cost attribution and `TransportKind` reporting become ambiguous.
Option A keeps each transport self-contained and preserves the existing router contract.

### TCP as the data layer

Once the P2P IP interface exists, the `Connection` impl is backed by a TCP stream. Byte
framing uses a 4-byte big-endian length prefix, matching `QuicConnection`. The Noise
handshake runs over the TCP stream unchanged: `Session::initiate()` and
`Session::respond()` call `send_bytes` and `recv_bytes` with no knowledge of what is
underneath.

QUIC could also run over the P2P interface, but it would create a compile-time dependency
between `pathweave-transport-wifi-direct` and `pathweave-transport-quic` and duplicate
the connection establishment work that Noise already handles. TCP is sufficient: it is
reliable, in-order, and universally available on the P2P interface on both Linux and
Windows.

### `PeerAddress::WifiDirect(String)` — two-phase address lifecycle

`discover()` emits `PeerAnnouncement` entries before any P2P connection exists. At that
point the only identifier available is a platform-specific device identifier: a MAC
address string on Linux (from wpa_supplicant), a WinRT device ID on Windows. These are
structurally identical to `PeerAddress::Ble(String)`, which holds a BLE peripheral
address before connection.

`PeerAddress::WifiDirect(String)` holds this pre-connection identifier. The SocketAddr
of the established P2P interface is an internal transport detail, resolved inside
`connect()` and never surfaced as a `PeerAddress`. The router and peer table remain
agnostic to how the transport establishes the link.

### GO election by `short_id` comparison

When two Pathweave nodes connect via WiFi Direct, one must become the Group Owner and one
the client. Leaving this to the platform without explicit intent values risks a deadlock:
if both sides signal neutral intent (intent=7 in wpa_supplicant), the platform may choose
inconsistently or stall.

The rule: the side with the lexicographically greater `short_id` (first 8 bytes of the
PeerId, present in `PeerAnnouncement` at discovery time) becomes GO. Both sides compute
this from the same `short_id` they already have, independently and without coordination.

On Linux, intent values map the rule directly: the GO side passes `go_intent=15` to
`P2PConnect`; the client side passes `go_intent=0`. On Windows, the GO side runs
`WiFiDirectAdvertisementPublisher` and `WiFiDirectConnectionListener`; the client side
calls `WiFiDirectDevice::FromIdAsync()`. The same `short_id` comparison determines which
role each side takes before touching the platform API.

If `short_id` is absent from the `PeerAnnouncement` (not populated at discovery time),
the transport falls back to treating this node as client. The remote will attempt GO; if
it also has no `short_id` it will also fall back to client, and one side will fail to
connect. This is a degenerate case that does not arise when both nodes run this
implementation, because `discover()` always populates `short_id` from the P2P service
record.

### Wire format for address exchange

Address type byte `0x03` is assigned to `PeerAddress::WifiDirect` in the address
exchange frame (ADR 017). The encoding is identical to `PeerAddress::Ble` (`0x02`):
one byte type, one byte length, length bytes of UTF-8 device identifier. The same
decoder handles both.

## Rationale

**Why not Option B (QUIC over P2P)?**

Option B blurs the transport boundary. The router selects among registered transports
by cost and kind. If a WiFi Direct connectivity layer surfaces a QUIC connection on the
P2P interface, that connection appears as `TransportKind::Quic` with `TransportCost::?`
at a QUIC port that nobody dialled directly. `MessageDelivered` events would report the
wrong kind. Cost ordering would not account for the underlying medium correctly. Option A
keeps all of this clean.

**Why TCP and not QUIC within Option A?**

QUIC within a `WifiDirectTransport` would require importing `pathweave-transport-quic`
internals (the TLS certificate generation, the Quinn configuration) or duplicating them.
TCP gives the same reliable byte stream without the dependency. The Noise layer above TCP
provides authentication and encryption; QUIC's TLS would be redundant.

**Why `short_id` and not full PeerId for GO election?**

The full 32-byte PeerId is not available at P2P discovery time on either platform. The
wpa_supplicant `P2PPeerFound` signal includes the peer's P2P device address (a MAC) and
service data; the full PeerId is not transmitted until after the Noise handshake. The
`short_id` (first 8 bytes) is embedded in the P2P service record and is available before
connection. The collision probability (two peers sharing the same 8-byte prefix) is 1 in
2^64, which is negligible for any realistic mesh size.

## Implications

- `PeerAddress::WifiDirect(String)` and `TransportKind::WifiDirect` are new variants.
  All exhaustive matches over these enums in `pathweave-core`, `pathweave-transport-wifi-direct`,
  and `pw-chat` must be updated.
- `encode_addr_exchange` and `decode_addr_exchange` in `router.rs` gain address type
  byte `0x03` for WiFi Direct, encoded identically to BLE (`0x02`).
- A new crate `pathweave-transport-wifi-direct` is added to the workspace. It has no
  compile-time dependency on `pathweave-transport-quic`.
- Platform dependencies are target-gated: `zbus` on Linux, `windows` crate
  (`Devices_WiFiDirect` and `Networking_Sockets` features) on Windows. Other platforms
  produce a compile error at the `WifiDirectTransport::new()` call site with a clear
  message.
- The P2P service name embedded in the wpa_supplicant service record and the WinRT
  advertisement must be the same on both platforms so that cross-platform discovery
  works. The agreed name is `pathweave-p2p`.
- macOS does not support third-party WiFi Direct access. No stub or placeholder is
  provided for macOS in this crate.
