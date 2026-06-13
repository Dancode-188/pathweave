# ADR 019: Mesh routing via TTL-limited flooding

**Status:** Accepted

## Context

The current routing layer is direct-only. `Router::send()` consults the peer table and
tries every known address for a given peer. If the destination is not directly reachable,
delivery fails. There is no mechanism to route through an intermediate node.

Two nodes that cannot reach each other directly — because they are on different network
segments, or because one transport is down and the other is not mutually visible — cannot
exchange messages even if a third node can bridge them.

## Decision

Add a routing header to the message wire format. Every forwarded message carries a
destination PeerId and a TTL. Intermediate nodes that receive a message not addressed to
themselves decrement the TTL and forward the message to all directly known peers except
the immediate sender. When a message addressed to the local node arrives, it is delivered
to `on_message` and not forwarded further. When TTL reaches zero, the message is dropped.

This is TTL-limited flooding. No routing tables are maintained. No topology knowledge is
required beyond the local peer table.

### Wire format

The existing application message format is unchanged for direct messages:

```
[message_id: 8 bytes, high bit set per ADR 017]
[payload:    N bytes]
```

A routed message adds a routing header immediately after the message ID:

```
[message_id:   8 bytes]
[route_flag:   1 byte]    0x00 = direct (no routing header follows)
                          0x01 = routed (routing header follows)
[dest_peer_id: 32 bytes]  raw bytes of the destination PeerId
[ttl:          1 byte]    remaining hops; maximum value at origin is 7
[payload:      N bytes]   application data
```

Direct messages (route_flag `0x00`) are wire-compatible with the current format after
the first byte: the existing `handle_incoming` receives `[message_id][payload]` and the
first payload byte being `0x00` now means "this is direct." Nodes that do not implement
routing treat `0x00` as normal payload data, which is correct — direct messages are
delivered unchanged.

Routed messages (route_flag `0x01`) require the receiving node to inspect the routing
header before acting.

### Routing invariants

A conforming node must satisfy all of the following:

1. **Destination check.** On receiving a routed message, a node checks whether
   `dest_peer_id` matches its own PeerId. If yes, it delivers the payload to
   `on_message` and stops. It does not forward a message addressed to itself.

2. **TTL enforcement.** A node must not forward a message with TTL = 0. Decrement TTL
   before forwarding. A node that receives TTL = 0 drops the message silently.

3. **Source suppression.** When forwarding, a node must not send the message back to the
   peer it just received it from. All other directly known peers are candidates.

4. **Deduplication.** A node that receives a message with a message_id it has already
   seen within the deduplication TTL window must drop it immediately, before forwarding
   or delivering. This prevents loops even when multiple paths exist between nodes.
   For routed messages, dedup is keyed on message_id alone, not on (sender_peer_id,
   message_id), because the immediate sender is a relay and varies across paths.

5. **No cross-delivery.** A node must not call `on_message` for a routed message whose
   `dest_peer_id` does not match the local node. Payloads addressed to other peers are
   not delivered locally, regardless of content.

### Cross-transport bridge model

A node registered with both a QUIC transport and a BLE transport acts as a bridge
implicitly. When it forwards a routed message, it calls `Router::send()` for the
destination peer using whichever transports and addresses are available. The router's
existing cost-ordered candidate selection (ADR 016) handles the rest. There is no
explicit bridge configuration. A dual-transport node is a bridge by construction.

The concrete scenario: node A (QUIC only) sends to node C (BLE only) via node B
(QUIC + BLE). A sends to B via QUIC with dest=C, TTL=3. B receives it, sees dest≠B,
finds C in its peer table (known via BLE scan), calls Router::send(C, payload) via BLE,
TTL=2. C receives it, sees dest=C, delivers to on_message.

A does not need to know C's addresses. A only needs C's PeerId and at least one peer
that can reach C. For v0.3.0 with flooding, A sends to all known peers and lets the
network find C.

### Maximum TTL

The maximum TTL at origin is 7. Intermediate nodes clamp any received TTL to 7 before
decrementing, so a malformed or malicious message cannot force unbounded forwarding.
TTL=7 supports meshes up to seven hops deep, which is sufficient for the expected
deployment scale.

In the worst case (fully connected mesh of N nodes, all forwarding), the number of
message copies is bounded by `(N-1)^TTL`. For small meshes (N ≤ 10, TTL = 7) this is a
large number. In practice, deduplication at each node cuts this to at most one copy per
node per message. The actual upper bound on copies delivered to `on_message` is N-1
(one per node that receives the message), regardless of how many relay paths exist.

## Rationale

**Why TTL-limited flooding instead of Spray-and-Wait?**

Spray-and-Wait requires either a pre-seeded contact history (to choose good relay nodes)
or a two-phase protocol (spray phase broadcasts L copies; wait phase delivers directly).
Both require state beyond the local peer table. For a first mesh implementation, this
complexity is unnecessary.

TTL-limited flooding with per-message deduplication is correct by construction for small
meshes: every reachable node receives the message exactly once, regardless of topology.
No routing tables, no contact history, no coordination. The cost is bandwidth: each node
forwards to all known peers. For the expected mesh size in v0.3.0 (fewer than twenty
nodes), this is acceptable. Spray-and-Wait or gradient routing can replace flooding in
a later release once the mesh is exercised in real deployments.

**Why route_flag byte instead of a separate message type?**

A separate control message type (as in ADR 017) would require routing to be an entirely
new framing layer. The route_flag byte is a one-byte extension to the existing payload
format. Direct messages (flag = 0x00) have zero overhead and are wire-compatible with
pre-routing nodes, which read the flag byte as the first payload byte and handle it as
data. The actual payload content of a direct message does not begin with `0x01`, so
there is no ambiguity in practice. New transport implementations and upgrades are
backwards-compatible at the wire level without a versioning mechanism.

**Why 32-byte dest_peer_id instead of 8-byte short_id?**

Short_id is sufficient for broadcast transports (ADR 018) where nodes in range are few.
For routed messages relayed across a mesh, short_id collisions would cause incorrect
delivery: node X receives and delivers a message intended for node Y because they share
the same 8-byte prefix. Full 32-byte PeerId eliminates this. The 32-byte overhead per
message is acceptable on QUIC and BLE GATT; acoustic transport can compress by
segmenting long messages across multiple acoustic frames.

**Why dedup on message_id alone for routed messages?**

In the existing direct model, dedup is keyed on `(sender_peer_id, message_id)` because
the same application might send different messages with the same ID from different peers.
In a multi-hop mesh, the immediate sender is a relay node, not the original source. The
same message arrives via multiple relay paths with different sender_peer_ids but the same
message_id. Keying on message_id alone correctly deduplicates across all paths. The
collision risk (two different messages from two different original sources sharing the
same message_id within the TTL window) is 1 in 2^63 (ADR 017's high-bit mask), which is
negligible.

## Implications

- `handle_incoming()` in `node.rs` gains a route_flag check before the dedup and
  on_message call. It must have access to `Router::send()` to forward. A reference to
  the `Router` (or `PathweaveNode`) is added to the `handle_incoming` call chain.
- `accept_loop()` passes a router reference to `handle_incoming`.
- `PathweaveNode::send()` is extended with a routed variant that sets route_flag = 0x01
  and a caller-supplied dest_peer_id and TTL. The existing `send()` retains route_flag =
  0x00 behavior for direct delivery.
- The deduplication cache in `node.rs` is extended to support two keying modes: the
  existing `(sender_peer_id, message_id)` for direct messages, and `message_id` alone
  for routed messages.
- The peer table remains `HashMap<PeerId, Vec<PeerAnnouncement>>`. No routing table is
  added. Flooding uses the peer table as-is.
- Store-and-forward (BPv7 bundle hold-and-deliver) is a separate subsystem. It operates
  at the application level and does not interact with the routing header defined here.
- This ADR does not define how a sending node learns that a destination is reachable via
  a relay. For v0.3.0, the application supplies the destination PeerId; the router floods
  to all known peers. Explicit route discovery is deferred.
