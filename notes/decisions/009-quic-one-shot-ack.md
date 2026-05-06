# ADR 009: One-shot QUIC sends require a receiver ACK before dropping the session

**Status:** Accepted

## Context

`try_send()` in `router.rs` opens a QUIC connection, completes the Noise_XX handshake,
sends the payload, and then returns. Returning drops the session, which drops the last
Quinn `Connection` handle.

Quinn sends a `CONNECTION_CLOSE` frame when the last connection handle is dropped.
Quinn's `write_all()` writes to an internal send buffer; actual wire transmission is
handled asynchronously by Quinn's IO task. `CONNECTION_CLOSE` does not flush the buffer
before firing. The result: the payload sits in the buffer, the connection closes, and
the data is silently discarded. The sender gets `Ok(())`. The receiver gets
"connection lost."

This was confirmed by diagnostic tracing on 2026-05-05 (see issue #34). The
Noise handshake completed correctly; only the payload was lost.

An earlier attempt to fix this by explicitly calling `finish()` on `SendStream` in a
`Drop` impl was wrong because Quinn 0.11.9's own `SendStream::Drop` already calls
`finish()`. That fix was a no-op. The real issue was the connection handle drop, not
the stream FIN.

## Decision

Implement a round-trip delivery ACK over the Noise session:

1. `handle_incoming()` sends an empty message (`b""`) back to the sender after calling
   `handler.on_message()` for each received payload.

2. `try_send()` waits up to 5 seconds for that ACK via `session.recv()` before
   returning. This keeps the QUIC connection alive long enough for Quinn's IO task to
   flush the send buffer and for the receiver to confirm delivery.

## Rationale

**Why an ACK and not some other approach?**

Quinn does not provide a synchronous flush-and-close primitive. `SendStream::finish()`
sends a FIN but does not wait for the peer to receive the data. The only reliable signal
that data was received is a message from the receiver. An empty ACK over the existing
Noise session is the simplest implementation and reuses the authentication we already have.

**Why 5 seconds?**

It's a conservative timeout for a local network or nearby peer. Long enough to survive
transient jitter. Short enough that a dead peer doesn't block the caller indefinitely.
This will be revisited if we see timeout-related issues in real deployments.

**Why not keep connections open between sends?**

Lazy connections (one per send) are the current model. Persistent connections per peer
would reduce the per-message handshake cost but require connection lifecycle management
across the router, the node, and transport-level keepalives. That's a bigger change with
more surface area. The ACK approach fixes the correctness issue within the existing model.
Persistent connections are a v0.2.0 consideration.

**Semantic change:** `send()` returning `Ok(())` now means "receiver confirmed delivery
on this attempt." Previously it meant "bytes handed to Quinn's internal buffer." This is
strictly stronger and what callers reasonably expect.

## Implications for future transport implementations

Any transport that wraps a byte-stream protocol with asynchronous flushing (TCP,
WebSocket) has the same risk: writing bytes to a socket does not mean they were
received. New transports should document their flushing behavior and confirm that the
session ACK round-trip is sufficient to guarantee delivery for their protocol.

BLE is not affected by the QUIC buffering issue, but the ACK is not optional on BLE
either. GATT write-without-response has no link-layer delivery guarantee: the
characteristic write is fire-and-forget at the BLE protocol level. The Noise session
ACK is the only confirmation that the payload reached the handler. The ACK is arguably
more load-bearing on BLE than on QUIC, where at least Quinn confirms the bytes reached
the peer's buffer.

**Double-delivery edge case:** if `handler.on_message()` succeeds but the ACK is lost
(connection drops between delivery and ACK), `try_send()` times out after 5 seconds.
The payload was delivered but the sender cannot know this. A retry at the application
layer would send the message again. This is acceptable in v0.1.0 and is the exact
problem that v0.2.0's at-least-once delivery work (BPv7 bundle IDs + retry) is designed
to solve. Do not treat a 5-second timeout as equivalent to "delivery failed."

**Per-message latency:** each message currently costs a full QUIC handshake, a Noise_XX
handshake, the payload send, and the ACK round-trip. On a local network this is
acceptable for a chat demo. At scale or under latency constraints it becomes a problem,
which is the primary motivation for persistent connections in v0.2.0.
