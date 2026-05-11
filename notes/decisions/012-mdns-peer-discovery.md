# ADR 012: mDNS peer discovery via mdns-sd with background connect

**Status:** Accepted

## Context

v0.1.0 requires callers to supply peer addresses manually (e.g. a `--peer` flag or
`add_peer()` call). For the pw-chat demo to work without any configuration, QUIC nodes
on the same LAN need to find each other automatically.

mDNS (RFC 6762) with DNS-SD (RFC 6763) is the standard mechanism for local service
discovery. It uses IP multicast to announce and browse services on the local link.
Because multicast does not route across network boundaries, mDNS is inherently
local-only: a Pathweave node can only discover and be discovered by other nodes on the
same network segment.

## Crate choice: mdns-sd

Two viable Rust crates exist for mDNS:

| Crate | Approach | System deps | Notes |
|---|---|---|---|
| `mdns-sd` | Pure Rust UDP multicast | None | Cross-platform: Linux, macOS, Windows |
| `zeroconf` | Wraps Bonjour / avahi | avahi-daemon on Linux, Bonjour on macOS/Windows | Requires a running system daemon |

`mdns-sd` is the right choice for Pathweave:

- No system daemon dependency. A node on a freshly provisioned Linux machine does not
  need avahi installed. On Windows, Bonjour is not present by default.
- Pure Rust. No FFI boundary. Easier to audit, consistent behaviour across platforms.
- Active maintenance and a straightforward browse/register API.

## Service type and TXT record

Service type: `_pathweave._udp.local.`

The `_udp` suffix is correct: DNS-SD convention labels the service type with the
underlying transport protocol of the application service being announced. QUIC runs
over UDP.

TXT record: empty (no fields).

The TXT record deliberately contains no identity information. Specifically, the PeerId
is not included. See the Privacy section below for the rationale.

The mDNS instance name is a random hex string generated at node startup, distinct from
the PeerId. A passive mDNS observer sees a service at an address but learns nothing
about the Pathweave identity behind it.

## Discovery and peer table population

When `discover()` emits a `PeerAnnouncement`, the node does not yet know the remote
peer's PeerId. A `peer_stream()` free function in `router.rs` drives this process:

1. Calls `transport.discover()` to get a stream of `PeerAnnouncement`s.
2. For each announcement, checks whether the announced address is already recorded in
   the known addresses set (`known_addrs: Arc<Mutex<HashSet<SocketAddr>>>`). If it is,
   the address is already being handled and the announcement is skipped.
3. If the address is new, calls `try_connect()` to initiate a Noise_XX handshake. The
   handshake authenticates both sides and reveals the remote static public key, from
   which the PeerId is derived.
4. On success, upserts `(PeerId, PeerAnnouncement)` into the shared peer table. If the
   PeerId was already known at a different address (peer roamed), the entry is updated
   to the new address.
5. On handshake failure, logs and continues. The peer may not be a Pathweave node, or
   may be temporarily unreachable.
6. Skips the announcement if the discovered PeerId matches the local node's own
   identity, to avoid adding a self-entry to the peer table.

`peer_stream()` is a `pub(crate)` async function in `router.rs`, consistent with the
module's existing pattern of private free functions (`try_connect`, `try_send`,
`monitor`). It takes owned values so it can be spawned as a `'static` task. It accepts
the shared peer table and known addresses set directly and writes to them internally,
rather than returning a stream for the caller to drain:

```rust
pub(crate) async fn peer_stream(
    transport: Arc<dyn Transport>,
    identity: NodeIdentity,
    started: watch::Receiver<bool>,
    peers: Arc<Mutex<HashMap<PeerId, PeerAnnouncement>>>,
    known_addrs: Arc<Mutex<HashSet<SocketAddr>>>,
    local_peer_id: PeerId,
)
```

`try_connect()` remains private to `router.rs`. No cross-module visibility is needed.

`node.rs` spawns `peer_stream()` per transport in `register_transport()`, alongside the
existing `accept_loop`. Both tasks receive a clone of the same `watch::Receiver<bool>`
so they both wait for `started` to be `true` before doing work. Unlike `Arc<Notify>`,
a watch receiver retains its last value, so a task that subscribes after the monitor
fires still sees `true` without a race.

### Deduplication: address-based, not PeerId-based

Deduplication in the discover loop is keyed on announced address (IP:port), not PeerId,
for the following reason:

mDNS re-announces services periodically. A peer can also roam between addresses: DHCP
lease renewal, switching between WiFi networks, interface changes. In all these cases,
the discover loop receives a new announcement. If dedup were PeerId-based, the loop
would skip the re-announcement for a known peer even when the address has changed,
leaving a stale address in the peer table indefinitely. Subsequent sends would fail on
connection refused with no recovery path.

Address-based dedup handles both cases correctly:
- Re-announcement with same address: address already in known addresses set, skip.
- Peer roamed to new address: new address, do the handshake, upsert with updated
  address.

**Edge case:** If a node restarts with a new identity at the same IP:port (violating
ADR 005's requirement that the static keypair be persisted across restarts), address-
based dedup will keep the stale PeerId until the address disappears from mDNS and
reappears. This is an operational error rather than a protocol gap. The assumption
address-based dedup makes is: at any given IP:port, the Pathweave identity is stable
for the lifetime of a process.

## Privacy rationale

Including the PeerId in the mDNS TXT record would be a privacy regression:

- The PeerId is a stable, globally unique identifier derived from the node's static
  public key. It does not change across sessions or network changes.
- mDNS announcements are sent as multicast to the entire local network segment. Every
  device on that segment receives them, including devices not running Pathweave.
- A passive observer (a neighbour on a public WiFi, a network administrator) could
  record which PeerId is at which IP address and build a persistent identity graph
  across sessions and locations.

By keeping the TXT record empty, a passive mDNS observer learns only that a Pathweave
node exists at a given address. The PeerId is exchanged only inside the Noise_XX
handshake, which is encrypted. A passive observer cannot derive the PeerId from the
handshake transcript.

This is consistent with the project's security-first principle. The trade-off is one
background Noise_XX handshake per newly discovered address. The privacy gain is that
stable identity is not broadcast in plaintext.

**Remaining limitation:** A passive observer can still observe that a node at a given
IP is running Pathweave (from the `_pathweave._udp.local.` service type). Protocol
obfuscation and traffic analysis resistance are not in scope for v0.2.0.

## Implications

- `peers` in `PathweaveNode` changes from `HashMap<PeerId, PeerAnnouncement>` to
  `Arc<Mutex<HashMap<PeerId, PeerAnnouncement>>>` so the peer_stream task can write to
  it from a spawned context.
- A second lookup structure — `Arc<Mutex<HashSet<SocketAddr>>>` — tracks known
  addresses so the discover loop can skip re-announcements in O(1) without scanning
  the peer table values.
- `try_connect()` in `router.rs` remains private. `peer_stream()` is added as a new
  `pub(crate)` free function in the same module.
- mDNS is currently implemented only for the QUIC transport. BLE peer discovery uses
  the existing GATT scanning mechanism and is out of scope for this ADR.
