# ADR 023: E2E hop encryption via Noise_K

**Status:** Accepted

## Context

A routed message (ADR 019, `route_flag 0x01`) travels hop by hop. Each link between two
nodes is encrypted with Noise (XX or XK), but every relay along the path decrypts its
Noise session with its immediate neighbor and sees the plaintext payload before
re-encrypting for the next hop. A relay forwarding a message is fully able to read it.

E2E hop encryption means sealing the payload to the destination's actual identity key,
so a relay can still read what it needs to route (`dest_peer_id`, `ttl`) but cannot read
the content. Only the destination, holding the matching private key, can open it. This
requires the sender to already have the destination's public key before sending, which
the key registry and gossip (ADR 020, #100, #101) now provide, including for
destinations the sender has never directly connected to.

## Decision

### Sealing mechanism: Noise_K, not a custom construction

The first draft of this ADR proposed a hand-rolled construction: an X25519
Diffie-Hellman between the sender's static key and the destination's static key, used
directly as a ChaCha20-Poly1305 key. That was wrong before any code was written against
it: a raw DH output is not safe to use directly as a symmetric key without a key
derivation step first (Noise itself never does this; every `MixKey` operation in the
protocol runs DH output through HKDF before deriving a cipher key). Rolling that by hand
risks getting the derivation subtly wrong, and would have required a new direct
dependency on `chacha20poly1305` for a construction with no formal analysis behind it.

The Noise Protocol Framework already defines this exact case. Section 7.4 specifies
**one-way handshake patterns** (`N`, `X`, `K`) for "a one-way stream of data from a
sender to a recipient... to encrypt files, database records, or other non-interactive
data streams." `Noise_K` fits: both parties' static keys are known in advance, it is a
single message with no response possible, and that single message carries the
application payload directly, no separate handshake-then-transport phase.

`snow` (already a direct dependency, used for XX and XK) supports one-way patterns
natively:

```rust
pattern_enum! {
    HandshakePattern {
        // 7.4. One-way handshake patterns
        N, X, K,
        ...
```

with a dedicated `is_oneway()` method (confirmed against `snow`'s actual source, not
assumed from the spec alone). `Noise_K_25519_ChaChaPoly_BLAKE2s` parses through the
exact same pattern-string mechanism already proven for `Noise_XX_25519_ChaChaPoly_BLAKE2s`
and `Noise_XK_25519_ChaChaPoly_BLAKE2s`. No new dependency.

Noise_K's token sequence:

```
K:
  -> s
  <- s
  ...
  -> e, es, ss
```

The pre-message tokens (`-> s`, `<- s`) mean both parties' static keys must be supplied
to `snow::Builder` before the single message is written or read; neither key is
transmitted as part of the Noise_K message itself. The sender already has the
destination's key from the registry. The destination needs to know which key to supply
as the sender's, which the envelope format below provides.

### seal/unseal: stateless functions, not an extension of Session

`Session` models an ongoing, multi-message, bidirectional connection (`initiate`/
`respond` followed by repeated `send`/`recv`). Noise_K is the opposite: exactly one
message, never followed by anything else over that handshake state. Forcing it through
`Session`'s shape would mean adding states to an abstraction built for a fundamentally
different lifecycle. Instead, two new stateless functions, alongside `Session` in
`session.rs` but not part of it:

```rust
pub(crate) fn seal(
    identity: &NodeIdentity,
    dest_public_key: &[u8; 32],
    payload: &[u8],
) -> Result<Vec<u8>>;

pub(crate) fn unseal(
    identity: &NodeIdentity,
    sender_peer_id: &PeerId,
    sender_public_key: &[u8; 32],
    sealed: &[u8],
) -> Result<Vec<u8>>;
```

`seal` builds a fresh `snow::Builder` with `Noise_K_25519_ChaChaPoly_BLAKE2s`, supplies
`identity`'s private key and `dest_public_key` as the pre-shared remote key, calls
`build_initiator()`, and calls `write_message(payload, &mut buf)` exactly once. The
resulting bytes are the sealed message; the `HandshakeState` is discarded immediately
after, there is no transport phase to convert into. `unseal` does the same as responder,
supplying `sender_public_key` as the pre-shared remote key (the caller looks this up
from the key registry using `sender_peer_id`, see the envelope format below), and calls
`read_message` exactly once.

### Envelope format

```
[sender_peer_id:    32 bytes]   so the recipient knows which registry key to supply
[noise_k_message:    N bytes]   output of seal(): ephemeral pubkey + ciphertext + tag
```

`sender_peer_id` travels in the clear inside the envelope (still inside the outer Noise
hop-to-hop encryption, so relays decrypting their own link still see it, but it is not
visible to a passive observer outside any hop). This is necessary, not optional: Noise_K
never transmits the sender's static key as part of the message, by design (it is a
pre-message token, assumed already known), so without `sender_peer_id` the destination
has no way to know which key to configure before attempting `read_message`.

### Wire format: a new route_flag, not a redefinition of an existing one

A new `route_flag` value, alongside the existing `0x00` (direct), `0x01` (routed,
unsealed), and `0x02` (key announcement, ADR 020):

```
route_flag 0x03: E2E-sealed routed message
[dest_peer_id: 32 bytes]   same as 0x01, relays read this to forward
[ttl:           1 byte]    same as 0x01, clamped to MAX_TTL (7) on receipt
[envelope:      N bytes]   sender_peer_id + noise_k_message, see above
```

`0x01` keeps its current meaning (unsealed routed messages continue to work exactly as
they do today). `0x03` is the sealed variant, sharing the same TTL clamp, dedup, and
relay-spawn logic already built for `0x01` and `0x02` (`spawn_relay_to_neighbors` in
`node.rs`, added in the gossip work): a relay forwarding a `0x03` frame never inspects
or modifies the `envelope` field, exactly as it already does not have a way to read into
an opaque payload it cannot decrypt.

### Replay: covered by existing dedup, not solved here

The Noise spec notes one-way patterns have no built-in replay protection: there is no
`ee` (ephemeral-ephemeral) component, so a captured message can be replayed and will
decrypt successfully every time, since decryption only depends on the fixed sender and
destination static keys plus the ephemeral key embedded in the captured message itself.
This does not need new protection in Pathweave: every routed frame, `0x01`, `0x02`, or
`0x03`, already carries a `message_id` deduplicated at every hop (ADR 019,
`check_and_insert_routed`), before any attempt to interpret the payload. A `0x03` frame
replayed within the same 60-second dedup TTL used everywhere else in this codebase
(ADR 011, SECURITY.md's Replay suppression section) is dropped at the dedup check,
never reaching `unseal`. A frame replayed after that window expires is no longer
recognized as a duplicate and will successfully unseal and deliver again, the same
time-bounded property `0x01` and `0x02` already have. This is not a new gap introduced
here; closing it for good would mean adding a freshness mechanism Noise_K does not
provide on its own, which is out of scope for this ADR.

### Missing key: fail loudly, never silently downgrade

`PathweaveNode::send_routed_sealed` (or equivalent) returns a new error,
`PathweaveError::KeyUnknown(PeerId)`, when the destination's key is not in the registry.
It never falls back to sending unsealed. A security feature that can silently downgrade
is worse than not having it: a caller who believes a message was E2E-protected, when it
silently was not, is a worse failure mode than an explicit error the caller must handle
(retry later once gossip propagates the key, surface the failure, or choose to send
unsealed via the existing `0x01` path explicitly if that is genuinely acceptable for
that call site).

## Rationale

**Why Noise_K over a custom construction, restated plainly.** Noise_K is formally
analyzed, already implemented by a crate this project already trusts for its entire
existing security model, and requires no new dependency. The alternative would have
required this project to get a KDF step right by hand, for a construction with none of
that history behind it. There is no advantage to rolling a custom scheme here that
offsets the risk.

**Why not Noise_X or Noise_N?** `Noise_N` provides no sender authentication (the
recipient's key is known, but the sender's identity is never verified): anyone could
seal a message claiming to be from anyone, since there is no static-static DH binding
the message to a specific sender key at all. `Noise_X` transmits the sender's static key
as part of the message (encrypted, but the recipient does not need to already know it),
which is the right choice when the recipient has no prior way to learn the sender's
identity. Pathweave's destinations can always look up a claimed sender's key from their
own registry (populated by gossip), so `Noise_K`'s pre-shared assumption already holds in
practice, and `Noise_K` is simpler: no key transmission step, no need to additionally
verify that a transmitted key matches what the registry already has.

**The KCI caveat, stated honestly.** Noise_K's sender authentication is vulnerable to
key-compromise impersonation: if an attacker obtains the *destination's* private key,
they can forge a message appearing to come from any sender to that destination, without
needing that sender's actual private key, because the `ss` (static-static) DH only
requires one party's private key and the other's public key to compute. This is narrow:
it requires the destination's key specifically to be compromised, and does not affect
confidentiality or authentication between other, uncompromised parties. It is the same
class of property already accepted for Noise_XK in this codebase (ADR 020 documents
XK's own limitations honestly rather than overselling it); E2E hop encryption inherits
a comparable, named, and disclosed tradeoff rather than an undisclosed one.

**Why a new route_flag instead of changing what 0x01 means.** Changing `0x01`'s meaning
would mean every existing routed message becomes implicitly sealed, which is not always
possible (the sender may not have the destination's key yet) and is a behavior change
to an already-shipped wire format. A new value costs one byte of design space and
nothing else.

## Implications

- `session.rs` gains `seal`/`unseal` as free functions, not methods on `Session`.
- `PathweaveError` gains `KeyUnknown(PeerId)`.
- `node.rs`'s `dispatch_payload` gains a `route_flag 0x03` branch: decode the envelope,
  look up `sender_peer_id`'s key in the registry (if absent, drop silently, the same as
  an unverifiable `0x02` announcement; the sender's key should already be in the
  registry via gossip if any honest path exists), call `unseal`, deliver to `on_message`
  if addressed locally, otherwise relay via `spawn_relay_to_neighbors` exactly as `0x01`
  and `0x02` already do.
- `PathweaveNode` gains a public sealed-send method. The exact name and whether it
  replaces or sits alongside `send_routed` is an implementation detail for the tracking
  issue, not fixed by this ADR.
- Noise_K's pre-shared key requirement means a destination can only verify a sender's
  claimed identity if that sender's key is already in the destination's registry. For a
  sender whose key has not yet propagated via gossip to wherever the destination is,
  `unseal` fails and the frame is dropped; this is a delivery gap inherent to gossip's
  own propagation time, not a defect introduced by this ADR.
