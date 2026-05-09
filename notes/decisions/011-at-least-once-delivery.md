# ADR 011: At-least-once delivery via message ID framing and retry loop

**Status:** Accepted

## Context

v0.1.0 provides single-attempt confirmed delivery. `send()` returns `Ok(())` when the
receiver ACKs via the session round-trip (ADR 009), or an error if the ACK times out.
There is no retry. If the ACK is lost after delivery (connection drops between delivery
and ACK), the sender cannot distinguish "delivery failed" from "delivery succeeded, ACK
lost." Callers that retry at the application layer will double-deliver.

The goal for v0.2.0 is at-least-once delivery: `send()` retries up to N times before
returning an error, and the receiver deduplicates retried messages.

## Why BPv7 bundle IDs are not usable for deduplication

The natural deduplication key for a bundle protocol is the BPv7 bundle ID, which RFC 9171
defines as `(source_node_id, creation_timestamp)`. In the current implementation:

- Source node ID is always `pathweave/local` for every bundle.
- The creation timestamp is `(DTN_TIME_EPOCH, seq)` where `seq` is a monotonically
  increasing counter on each `BundleLayer` instance.

`BundleLayer` is created fresh for every connection, and every `try_send()` call opens
a new connection. So `seq` resets to 0 for every message sent. Every message has bundle
ID `(pathweave/local, (0, 0))`. Using this as a dedup key would cause every second
message from the same sender to be treated as a duplicate.

Fixing the timestamp to use actual wall-clock time would help for messages sent more
than one second apart, but retries (which happen within the 5-second ACK timeout) often
arrive within the same second — and the next new message sent by the same sender after
the TTL expires would get seq=0 again and collide anyway. The lazy-connection model
makes BPv7 bundle IDs unreliable as dedup keys without a deeper refactor of the bundle
layer.

## Decision

Add an 8-byte random message ID, generated once per `router.send()` call and prepended
to the encrypted payload. The receiver extracts this ID and uses `(peer_id, message_id)`
as the deduplication key.

### Message ID framing

In `router.send()`, generate a cryptographically random `u64` using `getrandom`. Pass
this ID to `try_send()`. In `try_send()`, prepend the ID as a big-endian `u64` (8 bytes)
before calling `session.send()`. The ID is inside the Noise_XX-encrypted stream, so it
is not visible to passive observers and cannot be forged.

When the retry loop (see below) retries a failed send, it calls `try_send()` with the
same message ID. The receiver sees the same `(peer_id, message_id)` tuple and suppresses
the second delivery.

### Deduplication cache

The receiver maintains a `DeduplicationCache` on `PathweaveNode`, shared across all
accept loops via `Arc<Mutex<...>>`. The cache maps `(PeerId, u64)` to the insertion
`Instant`.

On receiving a payload in `handle_incoming()`:

1. Extract the 8-byte message ID from the first 8 bytes of the decrypted payload.
2. Call `cache.check_and_insert(peer_id, message_id)`:
   - Evict entries older than the TTL (lazy eviction on access).
   - If the key is present: return `true` (duplicate).
   - If the key is absent: insert it with the current time, return `false` (new).
3. If duplicate: skip `on_message()`. Always send the ACK regardless, so the sender
   does not time out.
4. If new: call `on_message(data)` with the payload bytes after the 8-byte prefix. Then
   send the ACK.

Default TTL: 60 seconds. This gives a large margin over the 5-second ACK timeout.

### Retry loop

`router.send()` generates the message ID once, then calls `try_send()` in a loop:

- On ACK timeout: retry on the same transport (if still available), then fall back to
  the next available transport.
- Max attempts: 3 (configurable).
- Backoff: 1 second between attempts.
- If all attempts exhausted: return `PathweaveError::DeliveryFailed`.

The same message ID is reused across all attempts of the same `router.send()` call.

### Wire format change

Payload as seen by the session layer is now:

```
[message_id: u64 big-endian, 8 bytes][application payload: variable]
```

This is a breaking change to the wire format. v0.1.0 and v0.2.0 nodes are not
wire-compatible for application payloads. This is acceptable: v0.x.0 provides no
stability guarantees.

`pw-chat` does not need updating for the framing itself since it communicates through
`node.send()`, which picks up the new framing automatically. Tests that construct
sessions directly (bypassing `try_send()`) must be updated to include the 8-byte prefix.

## Rationale

**Why a random ID and not a counter?**

A monotonic counter resets on node restart. If the node restarts within the cache TTL
and sends a new message, it gets counter=0 again. If the receiver still has counter=0
from before the restart in its cache, the new message is incorrectly deduplicated.
A random u64 has negligible collision probability (approximately 1 in 2^64) and is
unaffected by restarts.

**Why getrandom and not rand?**

`getrandom` is the minimal primitive: it calls the OS entropy source directly and has
no dependencies beyond platform-specific OS bindings. The `rand` crate is larger and
provides PRNG facilities we do not need. For a security-focused library, using the
smallest, most audited primitive is the right call.

**Why prepend the ID inside the encryption and not in the bundle header?**

Putting the ID in a BPv7 extension block would expose it in plaintext at the bundle
layer, allowing a passive observer to correlate retried messages. Inside the encryption,
the ID is not visible. The receiver can still extract it after decryption.

**Why 60 seconds TTL?**

The ACK timeout is 5 seconds and the max retry window is 3 attempts × (5 second timeout
+ 1 second backoff) ≈ 18 seconds. A 60-second TTL provides more than 3× margin. Memory
cost is bounded: at typical chat message rates, the cache holds at most a few dozen
entries.

## Implications

**Double-delivery edge case reduced, not eliminated:** If `on_message()` is called
successfully but the ACK is lost and all retries are also lost, `send()` returns
`DeliveryFailed`. The message was delivered but the sender cannot know this. This is
the irreducible at-least-once edge case and is documented behaviour.

**Timeout semantics change:** In v0.1.0, a 5-second timeout on the first attempt meant
the sender gave up. In v0.2.0, `DeliveryFailed` means the sender gave up after N
attempts, each of which waited up to 5 seconds. Callers that interpret error return
values need to be aware that the elapsed time can be up to N × 5 seconds + backoff.

**Implementation order:** The deduplication cache (#42) must be merged before the retry
loop (#43). A retry loop without deduplication would double-deliver on ACK loss.
