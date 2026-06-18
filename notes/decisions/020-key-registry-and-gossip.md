# ADR 020: Key registry, key gossip, and Noise_XK upgrade

**Status:** Accepted

## Context

`PeerId` is `base58(blake3(public_key))`. The blake3 hash is one-way: the 32-byte
Curve25519 static public key cannot be recovered from the PeerId alone.

During a Noise_XX handshake, `session.get_remote_static()` returns the remote peer's
32-byte static public key. The current code in `session.rs` uses these bytes to derive
the PeerId and then drops them. Two planned features require those bytes to be retained:

**Noise_XK (known-peer handshake).** In Noise_XK, the initiator supplies the
responder's static public key before the handshake begins, as a pre-message the
initiator already has out-of-band. The responder's static key is therefore never
transmitted on the wire at all: there is nothing to intercept, encrypted or otherwise.
In Noise_XX, by contrast, the responder's static key is sent in message 2, encrypted
with a key derived from the ephemeral-ephemeral DH alone. That protects it from a
passive eavesdropper, but not from an active, anonymous initiator: anyone who can
connect and run the handshake, even a complete stranger with no prior relationship, can
decrypt and learn the responder's identity simply by being the one who initiates
(Noise spec identity-hiding property 1). XK's real advantage over XX is not passive-
observer protection, XX already has that for this key; it is that XK refuses to reveal
the responder's identity to anyone who does not already hold the pre-shared key,
closing the door XX leaves open to any anonymous prober. ADR 001 deferred XK to this
release on the condition that a key registry exists.

**E2E hop encryption.** To seal a message to a destination node that may be several
hops away, the sender encrypts the inner payload to the destination's public key. Only
the destination can decrypt it. Intermediate relay nodes forward the outer routed message
(ADR 019) without being able to read the payload. This requires the sender to have the
destination's public key before the message is sent.

Both features share the same prerequisite: a mapping from PeerId to the 32-byte
Curve25519 static public key, populated from direct handshakes and propagated to nodes
that have never directly connected.

## Decision

### Key registry

Add `key_registry: Arc<Mutex<HashMap<PeerId, [u8; 32]>>>` to `PathweaveNode`. After
every successful Noise_XX handshake (on both the initiator and responder sides), store
the remote peer's static public key:

```rust
// in session.rs, after get_remote_static() is called to derive the PeerId:
let raw: [u8; 32] = remote_static
    .try_into()
    .expect("Noise static key is always 32 bytes");
// caller stores: key_registry.lock().unwrap().insert(peer_id.clone(), raw);
```

The registry is append-only by design: a key is never overwritten with a different
value for the same PeerId, only re-inserted idempotently or evicted outright. The one
exception is eviction on XK handshake failure (see "Noise_XK upgrade" below), which
removes a stale entry rather than replacing it with an unverified one; the next
handshake falls back to XX and repopulates the registry from scratch.

### Key gossip via flooded announcement (amended 2026-06-17)

**Status: implementation deferred; tracked in issue #100.** The key registry and the
Noise_XK upgrade below have shipped. This subsection has not: it was corrected to a
different mechanism (see below) before any code was written against the original
version, and that corrected mechanism is not yet implemented.

The original version of this section specified piggybacking the sender's own public
key onto the address exchange (a new control type 2, address list plus a 32-byte key
field). That mechanism does not actually achieve what this section claims: address
exchange only ever carries a transport's own `local_addresses()`, never another peer's
information, so adding the sender's own key to it is redundant with what the Noise
handshake already provides for that same direct peer. It does not get a destination's
key to a sender who has never directly connected to that destination, which is exactly
the case E2E hop encryption needs. This was found before any code was written against
it and is corrected here rather than left as a known-wrong addendum, since nothing
downstream depends on the original text.

Key gossip instead reuses the TTL-limited flooding mechanism ADR 019 already built for
application messages, the same pattern Reticulum uses for its destination announces.
A new `route_flag` value carries a key announcement:

```
route_flag 0x02: key announcement
[peer_id:    32 bytes]   the identity this announcement is about
[public_key: 32 bytes]   that identity's Curve25519 static public key
[ttl:         1 byte]    clamped to MAX_TTL (7) on receipt, same as 0x01 routed frames
```

A node generates a fresh announcement (new message ID, fresh TTL) only at the moment
it directly learns a new key via a Noise handshake; it does not periodically
re-announce keys it already knows, and does not announce its own key before any
connection exists. This is the minimal mechanism that solves the stated problem:
genuine multi-hop propagation, without adding a second, independent "broadcast my
existence on a timer" feature nobody asked for.

**Validation is mandatory, not an implementation detail.** `PeerId` is
`base58(blake3(public_key))` by construction (see "PeerId" in ARCHITECTURE.md), which
makes every `(peer_id, public_key)` pair self-certifying: a receiver can verify
`blake3(claimed_public_key) == claimed_peer_id` before storing or re-flooding an
announcement. Since blake3 is preimage-resistant, no attacker can construct a different
key that hashes to an existing victim's PeerId, so this check alone prevents the
substitution attack a flooded gossip mechanism would otherwise be vulnerable to: no
signature scheme (the kind Reticulum's announce needs on top of its own hash binding)
is required. Every hop must perform this check independently before storing or
forwarding; never trust that an upstream relay already validated. A malicious relay
that tries to inject a forged pair gets it dropped at the very next honest hop.

**Hop radius.** Announcements are clamped to `MAX_TTL` (7), the same cap application
messages already use, not a larger radius. Propagating a key further than an
E2E-encrypted message could ever travel to reach that destination would only widen
exposure for no corresponding benefit.

**What this does not solve.** Key gossip makes a peer's full public key discoverable
to anyone within the announcement's hop radius, not just direct contacts, a widening
of disclosure beyond what direct handshakes alone would produce. This is judged
acceptable because the *pseudonymous identifier* (PeerId, the hash) is already
disclosed at this same radius today via routed message `dest_peer_id` headers (ADR
019) and BLE `short_id` (ADR 018); gossip adds "and here is the actual key, not just
its hash" to information that already traveled that far, and a public key is not
independently exploitable without the corresponding private key. Sybil identities
(an attacker generating many free `NodeIdentity` keypairs and announcing all of them)
are a pre-existing property of the identity model this ADR does not change or worsen;
no sybil-resistance mechanism exists at this layer in Reticulum or libp2p either. Two
related concerns are deliberately deferred rather than solved here: per-source
announcement rate-limiting (the existing application-message flood has the same gap),
and a growth bound on the key registry itself now that it can grow from gossip, not
just from peers directly met, tracked as a separate follow-up issue.

No further broadcast mechanism beyond this flooded announcement is needed; the
existing TTL-flood reuse provides sufficient propagation for expected mesh sizes.

### Noise_XK upgrade

Add a 1-byte protocol version prefix before the first Noise handshake message:

```
0x00 = Noise_XX_25519_ChaChaPoly_BLAKE2s   (unknown peer, no stored key)
0x01 = Noise_XK_25519_ChaChaPoly_BLAKE2s   (known peer, initiator has stored key)
```

The prefix is sent before the Noise handshake begins, unencrypted. The responder reads
it and builds a responder for the matching pattern.

**When to use XK:** `Session::initiate()` checks the key registry for the destination
PeerId. If a key is found, it sends prefix `0x01` and calls
`snow::Builder::new(...).local_private_key(...).remote_public_key(stored_key).build_initiator()` with the XK
pattern. If no key is found, it sends prefix `0x00` and uses XX.

**When to use XX:** always on first contact. Also as a fallback if the stored key is
stale (XK handshake fails because the remote's key changed, which violates ADR 005 but
must be handled gracefully). On XK failure, the initiator retries with XX and updates
the registry with the new key.

**Pattern string change:** `snow::Builder::new("Noise_XK_25519_ChaChaPoly_BLAKE2s")`.
No other changes to the `snow` crate usage. The `Session` struct is unchanged;
`Session::initiate()` and `Session::respond()` gain the protocol version logic.

XX and XK coexist permanently. XX is always used for first contact. XK is used once the
key is known. XK's improvement over XX is specifically that the responder's static key
is never transmitted at all, closing the anonymous-prober gap XX leaves open (see the
Noise_XK paragraph in Context above). The initiator's static key is still transmitted,
in message 3, with the same forward-secret protection XX's message 3 already has:
decryptable only by an already-authenticated party, not by a passive eavesdropper or an
anonymous prober. It is not visible in the first XK message, which carries only the
initiator's ephemeral key and no static-identity material. Full mutual anonymity
requires Sphinx routing (planned for a later release).

## Rationale

**Why append-only and no expiry?**

A peer's static key is tied to its identity keypair, which ADR 005 requires to persist
across restarts. A key in the registry should never become wrong; it can only become
stale if the peer violates ADR 005 (regenerates its keypair). Expiry would require
re-running XX handshakes with known peers unnecessarily. The registry is a cache of
immutable facts, not session state.

**Why a flooded announcement instead of piggybacking on address exchange?**

The original version of this ADR proposed piggybacking the sender's own key onto the
address exchange, reasoning that every connection already does one and a dedicated
flood would need new message types and flood control. That reasoning held for cost
but missed correctness: address exchange only ever carries a transport's own
`local_addresses()`, so piggybacking a sender's own key onto it can never deliver a
*third party's* key to someone who has never connected to that third party, which is
the actual requirement. A flooded announcement is the mechanism that genuinely
achieves multi-hop propagation, and it costs less new code than it first appears to:
the flood control, dedup, and TTL machinery already exist for application messages
(ADR 019); a key announcement is one more `route_flag` value reusing all of it.

**Why no signature on the announcement, unlike Reticulum's announce?**

Reticulum's announce needs an Ed25519 signature because its destination hash and
routing metadata are not, on their own, sufficient to prevent forgery. Pathweave's
PeerId is `base58(blake3(public_key))`: the identity and the key are bound by
construction, not by a separate signature. A receiver re-derives `blake3(public_key)`
and compares it to the claimed `peer_id`; any mismatch means the pair was forged or
corrupted, and is rejected. This is cheaper than a signature scheme and exists because
of a property Pathweave's identity model already has, not something added for gossip.

**Why a protocol version byte instead of attempting XK and falling back?**

The Noise handshake pattern is not self-describing at the wire level. A node receiving
an XK message 1 cannot tell from the bytes alone whether it is an XX or XK message
without attempting both and seeing which decrypts correctly. Attempting both requires
storing intermediate state and adds latency. A single prefix byte costs one byte and
makes the pattern explicit, consistent with the control message approach in ADR 017.

**Why store the key in the caller rather than inside Session?**

`Session` does not hold a reference to `PathweaveNode` or the key registry. Adding one
would create a dependency cycle. The session handshake produces the key; the caller
(initiation path in router.rs, incoming path in node.rs) stores it. This keeps `Session`
a pure cryptographic layer with no knowledge of the routing or storage layer above it.

## Implications

- `PathweaveNode` gains `key_registry: Arc<Mutex<HashMap<PeerId, [u8; 32]>>>`.
- `Session::initiate()` gains a `key_registry` parameter (or the caller checks the
  registry before calling initiate). The protocol version byte is written before the
  first Noise handshake bytes.
- `Session::respond()` reads one byte before starting the handshake to select the
  correct Noise pattern.
- `try_connect()` and `try_send()` in `router.rs` write to the key registry after a
  successful handshake. Both pass a reference to the shared registry.
- `handle_incoming()` in `node.rs` writes to the key registry after a successful
  `Session::respond()`. It is a free function taking `key_registry`, `peers`, and
  `router` as separate explicit parameters (added in ADR 019 to support forwarding),
  not a method accessed through a `PathweaveNode` reference.
- `route_flag` (ADR 019) gains value `0x02` for key announcements, alongside the
  existing `0x00` (direct) and `0x01` (routed). The address exchange (ADR 017) is
  unaffected; key propagation is a routing-layer concern, not a connection-time one.
- ADR 001's deferred Noise_XK work is resolved by this ADR. The implementation
  conditions stated there (key registry must exist) are satisfied.
- E2E hop encryption uses the key registry to look up the destination's public key
  before sealing the inner payload. That feature is implemented separately and depends
  on this ADR's registry being in place.
