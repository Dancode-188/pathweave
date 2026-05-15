# ADR 016: Multi-address peer routing with kind-matched candidates

**Status:** Accepted

## Context

The peer table was `HashMap<PeerId, PeerAnnouncement>`: one address per peer. A peer
reachable via both QUIC (mDNS-discovered) and BLE (BLE-discovered) would only have the
first-discovered address stored. `send()` looked up a single announcement, and the router
tried transports in cost order against that one address. If the stored address was
unreachable and the peer had a second address on a different transport, delivery failed
instead of falling back.

The "BLE fallback" story was also incomplete in a subtler way: the old candidate-building
loop passed a single `PeerAnnouncement` to every available transport, including transports
that cannot handle that address type (a QUIC transport receiving a BLE address). In
production, `QuicTransport::connect()` would reject any non-QUIC address immediately.
This was harmless but incorrect: it made `send()` appear to try more options than it
actually had.

Two constraints:

1. The transport-address pairing must be strict: a transport should only be asked to
   dial an address of the kind it handles. Mismatched pairs are not a recoverable
   failure; they are a caller error.

2. A peer discovered via multiple transports (BLE scan and mDNS at the same time) must
   accumulate all known addresses without overwriting the ones already stored.

## Decision

Change the peer table to `HashMap<PeerId, Vec<PeerAnnouncement>>`. Every call site that
previously overwrote the entry now pushes a new announcement onto the Vec, with an
address-equality dedup check to prevent unbounded growth on repeated discovery cycles.

Change `Router::send()` and `Router::connect()` to accept `&[PeerAnnouncement]`. The
candidate list is built by pairing each available transport with each announcement where
`announcement.address.kind() == transport.kind()`. Candidates are sorted by transport
cost (Free first). All pairs are tried before an attempt is counted as failed. On
`send()`, all `MAX_SEND_ATTEMPTS` retry attempts use this same per-attempt candidate
list, rebuilding it each time to reflect any availability changes between retries.

Add `PeerAddress::kind() -> TransportKind` to make the kind comparison explicit and
exhaustive. A new `PeerAddress` variant must assign a kind here or the match will
not compile, preventing silent omission from the routing path.

If the candidate list is empty for a given attempt (no registered transport can handle
any of the peer's known addresses), return `NoTransportAvailable` immediately rather
than exhausting retries.

## Rationale

**Why `Vec<PeerAnnouncement>` and not `HashMap<PeerAddress, PeerAnnouncement>`?**

Iteration order matters for routing: candidates must be sortable by transport cost.
A `Vec` supports stable sort and preserves insertion order as a tiebreaker for equal-cost
addresses. A `HashMap` would require collecting into a `Vec` for sorting anyway, and its
dedup semantics would need the same address-equality check. The `Vec` is simpler and the
access pattern (full iteration on every send, no random lookup by address) matches it
well.

**Why kind-matching instead of trying all (transport, announcement) pairs?**

A QUIC transport calling `connect()` with a BLE address is not a recoverable situation:
it is a logic error. Transports are not interchangeable address handlers; each transport
only knows how to connect to addresses of its own kind. Filtering mismatched pairs at
candidate-build time gives an accurate candidate count, makes the retry logic reflect
actual options, and avoids misleading error logs from transports rejecting addresses they
never should have received.

**Why dedup by address rather than by the full `PeerAnnouncement`?**

`short_id` in `PeerAnnouncement` is metadata derived from the remote PeerId and may
differ across announcements for the same physical address if the remote re-derives it.
Two announcements for the same address with different `short_id` values are not two
distinct reachability options; they are the same address with stale metadata. Deduping
on address alone keeps the routing path simple: `send()` already re-derives the remote
PeerId via Noise_XX on every connection, so `short_id` in the stored announcement is
never load-bearing for routing.

## Implications

- `PathweaveNode::peers` field type changes from `HashMap<PeerId, PeerAnnouncement>` to
  `HashMap<PeerId, Vec<PeerAnnouncement>>`.
- `peer_stream` uses `entry().or_default().push()` with no Vec-level dedup guard because
  `known_addrs` (a `HashSet<PeerAddress>` shared across all peer_stream tasks) prevents
  the same address from reaching the handshake or the Vec push a second time.
- `connect()` and `add_peer()` check the Vec directly with an address-equality guard
  before pushing, since those call sites have no upstream known_addrs gating.
- `Router::send()` and `Router::connect()` signatures change; all call sites updated.
- The `dual_peer()` test helper is added to the router test module to exercise the
  cross-transport fallback path.
- Removing stale addresses from the peer Vec when a transport goes down is deferred to
  v0.3.0, consistent with the broader session lifecycle work planned for that milestone.
