# ADR 010: Transport::start() takes &NodeIdentity

**Status:** Accepted — implementation deferred to v0.2.0 (required by issue #20, BLE peripheral mode)

## Context

BLE peripheral mode needs the local `NodeIdentity` at `start()` time to derive the
`short_id` (first 8 bytes of PeerId) for the advertisement service data. The current
`Transport::start()` signature takes no parameters:

```rust
async fn start(&self) -> Result<()>;
```

Three options were considered: pass identity to `start()`, pass it at construction
time on BLE's concrete type, or add an optional `set_identity()` method with a no-op
default.

## Decision

Change `Transport::start()` to accept a `&NodeIdentity` parameter:

```rust
async fn start(&self, identity: &NodeIdentity) -> Result<()>;
```

Transports that do not need identity (QUIC, future TCP) ignore the parameter with
`_identity`. BLE peripheral mode uses it momentarily to derive and store the `short_id`
before advertising begins.

The monitor task in `router.rs` will need identity passed to it. This requires a
call-site change: `router.register_transport()` currently takes only `Arc<dyn Transport>`
and has no identity. Identity lives in `PathweaveNode`, not `Router`. The implementation
will need to either thread identity through `register_transport()` or restructure how
`node.rs` coordinates the monitor and accept tasks. That design decision is deferred to
the v0.2.0 implementation of issue #20.

## Rationale

**Semantic fit.** `start()` is when a transport comes alive. QUIC advertises nothing
about itself on startup. BLE literally broadcasts who it is on startup. The trait already
has this semantic split baked in; `start()` just did not have the parameter to express
it yet.

**Compiler enforcement.** An optional `set_identity()` with a no-op default means a
transport author can forget to call it and the compiler stays quiet. With Option 1, if
you handle the parameter wrong the compiler complains. Explicitness is the feature.

**Ownership stays with the caller (ADR 005 alignment).** BLE needs identity momentarily
to derive `short_id`, not permanently. `&NodeIdentity` keeps the borrow short and leaves
ownership with the node, consistent with the principle that Pathweave never holds or
persists keys.

**Forward compatibility.** QUIC will need identity context when mDNS-based peer
discovery lands in v0.2.0. Having the parameter already in `start()` means that work
does not require another trait change.

## What changes when this is implemented

- `Transport::start()` signature: one new `&NodeIdentity` parameter
- `QuicTransport::start()`: add `_identity` parameter, no other changes
- `BleTransport::start()`: add `identity` parameter, derive `short_id`, store for advertising
- Monitor task in `router.rs`: pass `&identity` through to `transport.start(identity).await`
- All mock `Transport` implementations in tests: add `_identity` parameter

These changes land in v0.2.0 as part of issue #20.
