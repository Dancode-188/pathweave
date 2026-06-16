# ADR 021: Store-and-forward for direct and mesh delivery

**Status:** Accepted

## Context

`send()` and `send_routed()` are fail-fast. If the destination is not reachable when
you call them, you get an error back immediately. That is the right behaviour for a chat
message you are typing right now. It is the wrong behaviour when a node needs to hand
off a payload before it knows whether the destination is reachable.

Pathweave targets disconnected-intermittent networks: devices that are offline more than
they are online, that come into range briefly, that forward messages on behalf of people
who will never share a direct link. BPv7 was designed for exactly this kind of network.
The bundle layer is already there. What was missing was the hold-and-deliver logic above
it.

`store_forward(peer_id, payload)` and `store_forward_routed(dest_peer_id, payload)` fill
this gap. Both accept the payload immediately and return. Delivery happens when the
conditions are right: a direct path for the first, any neighbor for the second. Payloads
that never find a path expire after `store_ttl` with a `StoreFailed` event.

## Decision

### Two separate queues

We keep a direct queue and a routed queue on `PathweaveNode`. Both are
`Arc<Mutex<HashMap<PeerId, VecDeque<(u64, Vec<u8>, Instant)>>>>`. Each entry is
`(msg_id, app_payload, queued_at)`. The msg_id is generated at enqueue time and reused
on every drain attempt.

Separating the queues makes the drain logic easier to reason about. The drain triggers
are different, the send functions are different, and the delivery confirmation semantics
are different. A unified queue with a type flag would save one field but it would make
the drain paths harder to follow.

### Message ID generated at enqueue time, not at drain time

This is the most important invariant. The same msg_id is used for every attempt to
deliver a given queued payload, including the first immediate attempt if the peer is
already reachable.

The reason is the same as ADR 011: if a send delivers the payload but the ACK is lost,
the drain retries with the same msg_id. The receiver's `DeduplicationCache` sees a key
it already knows and suppresses the second delivery. If we generated a fresh msg_id on
each drain attempt, a lost-ACK retry would look like a new message and the receiver
would deliver it twice.

### ACK-based removal with re-queue on failure

Entries are removed from the queue only after `router.send()` returns `Ok(())`. That
return value means the ACK round-trip completed: the receiver confirmed delivery. We do
not remove optimistically and re-add on failure because that approach has a race: two
concurrent drains triggered by near-simultaneous events (say `PeerConnected` and
`connect()` returning at the same moment) would both see the same entries. With drain-
then-re-queue, the first drain empties the queue atomically under the lock. The second
drain arrives and finds nothing.

Failed entries are collected and prepended back to the front of the queue after all
sends in the batch complete, before any new entries that arrived during the drain. FIFO
order is preserved.

Re-queueing does not create an infinite retry loop because the drain is event-driven.
It runs when a peer appears, not on a ticker. If every send in a drain batch fails, the
entries sit in the queue until the next `PeerConnected` event, `connect()`, or
`add_peer()` call.

### Drain triggers

**Direct queue:** drains for the specific `peer_id` when:
- `PeerConnected(peer_id)` fires on the background event-watcher task
- `PathweaveNode::connect()` succeeds and returns that peer_id
- `PathweaveNode::add_peer()` is called with that peer_id (spawns a task)

The direct drain looks up the peer's announcements from the peer table. `peer_stream`
inserts the peer into the table before firing `PeerConnected`, so the announcements are
guaranteed to be there when the event-watcher task reacts.

**Routed queue:** drains for ALL pending destinations when any of the same three
triggers fire. Any new neighbor is a potential relay. We do not wait for the specific
destination to appear; we flood to every available neighbor and let the mesh carry it.

### Delivery confirmation semantics differ

For direct delivery: `router.send()` returning `Ok(())` means the ACK completed and the
destination received the payload. This is the same guarantee as `send()`.

For routed delivery: `router.send()` returns `Ok(())` when one neighbor accepted the
frame for relay. That is the same guarantee as `send_routed()`. We do not know whether
the destination ever received it. The entry is removed from the routed queue when one
neighbor accepts. This is documented behaviour, not a bug.

### TTL in NodeConfig, not per call

`NodeConfig::store_ttl: Option<Duration>` with `None` defaulting to 24 hours. This
keeps `store_forward` and `store_forward_routed` signatures simple: no TTL parameter.
The 24-hour default fits most real scenarios (offline contacts, intermittent gateways,
disaster relief relays). Applications that need shorter or longer windows set
`store_ttl` on the config they pass to `PathweaveNode::new()`.

We deliberately do not mirror `BUNDLE_LIFETIME` (1 hour) from `bundle.rs`. That
constant governs how long a BPv7 bundle lives on a single connection. How long we are
willing to carry a message for an absent peer is a different question.

### StoreFailed event

`TransportEvent::StoreFailed { peer_id: PeerId }` fires once per expired entry, for
both queues. The existing `Some(_)` catch-all in pw-chat absorbs it without changes.
Expiry is checked lazily when the drain runs: before attempting any sends, we split the
queue into expired entries (fire `StoreFailed` for each) and live entries (attempt
delivery).

## Rationale for alternatives rejected

**Single unified queue with a routing-mode flag.** Avoids one field on
`PathweaveNode`, but the drain code needs to branch on the flag anyway. Two queues with
two clear drain functions are easier to test and audit independently.

**Per-call TTL parameter.** Adds an argument to `store_forward` and
`store_forward_routed`. The common case does not need it. Callers who need a custom
window set `NodeConfig::store_ttl`. A per-call override can be added later if demand
appears.

**Drain the routed queue only when the specific destination appears.** This would give
stronger delivery semantics but defeats the purpose of mesh routing. The whole point is
that we do not need a direct path to the destination.

**Fire `StoreFailed` on a background ticker rather than lazily at drain time.** A ticker
would fire expiry events closer to the actual expiry moment. The trade-off is a
permanent background task that holds Arcs indefinitely even when the queues are empty.
Lazy eviction at drain time is simpler and the precision difference does not matter in
practice.

## Implications

**No queue size bound.** Memory grows linearly with enqueue rate if the destination
never appears. Callers are responsible for rate-limiting. A bound can be added later.

**No persistence across restarts.** Both queues are in-memory. A process restart drops
all pending payloads. Persistence via BPv7 storage would require a separate design.

**Routed expiry fires on drain, not on schedule.** If no neighbors ever appear, the
drain never runs, and `StoreFailed` never fires even when entries are long past their
TTL. This is acceptable: if there are no neighbors, there is nobody to tell either.

**Duplicate `StoreFailed` possible in pathological cases.** If two drain attempts run
concurrently for the same peer (near-simultaneous triggers), both might check expiry and
both might fire `StoreFailed` for the same entry if the entry was taken by neither (not
possible with the atomic-drain design, but worth noting as a non-issue because of it).
