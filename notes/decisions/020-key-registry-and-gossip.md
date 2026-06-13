# ADR 020: Key registry, key gossip, and Noise_XK upgrade

**Status:** Accepted

## Context

`PeerId` is `base58(blake3(public_key))`. The blake3 hash is one-way: the 32-byte
Curve25519 static public key cannot be recovered from the PeerId alone.

During a Noise_XX handshake, `session.get_remote_static()` returns the remote peer's
32-byte static public key. The current code in `session.rs` uses these bytes to derive
the PeerId and then drops them. Two planned features require those bytes to be retained:

**Noise_XK (known-peer handshake).** In Noise_XK, the initiator supplies the
responder's static public key before the handshake begins. The responder's first message
is encrypted with the initiator's ephemeral key and the responder's known static key,
which means a passive observer cannot link the connection to the responder's identity.
In Noise_XX, the static keys are exchanged in the clear during the handshake, so a
passive observer sees both identities. XK is strictly better for privacy with known
peers, and ADR 001 deferred it to this release on the condition that a key registry
exists.

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

The registry is append-only in normal operation. Keys do not expire. A peer's key is
deterministic from its static keypair (ADR 005 requires the keypair to persist across
restarts), so overwriting an existing entry with the same key is idempotent.

### Key gossip via address exchange

Extend the address exchange protocol (ADR 017, control message type 1) with a new
control message type:

```
type 2 — address + key exchange
[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]   control ID
[count: u8]                                          number of addresses
[addresses: N bytes]                                 same format as type 1
[public_key: 32 bytes]                               sender's static public key
```

Nodes that receive a type 2 exchange store the sender's public key in the registry
before processing the addresses. Nodes that do not yet implement type 2 ignore it
(unknown control types are silently dropped per ADR 017).

This provides one-hop key propagation: when A connects to B, A learns B's key directly
from the Noise handshake and also receives B's addresses. B likewise learns A's key.
For multi-hop scenarios, keys reach nodes that have never directly connected through the
natural flow of address exchanges across the mesh as connections form.

A separate gossip broadcast mechanism (flooding key announcements to all known peers) is
not implemented in v0.3.0. The address exchange on each connection provides sufficient
propagation for expected mesh sizes.

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
`snow::Builder::with_remote_public_key(stored_key).build_initiator()` with the XK
pattern. If no key is found, it sends prefix `0x00` and uses XX.

**When to use XX:** always on first contact. Also as a fallback if the stored key is
stale (XK handshake fails because the remote's key changed, which violates ADR 005 but
must be handled gracefully). On XK failure, the initiator retries with XX and updates
the registry with the new key.

**Pattern string change:** `snow::Builder::new("Noise_XK_25519_ChaChaPoly_BLAKE2s")`.
No other changes to the `snow` crate usage. The `Session` struct is unchanged;
`Session::initiate()` and `Session::respond()` gain the protocol version logic.

XX and XK coexist permanently. XX is always used for first contact. XK is used once the
key is known. The privacy improvement is asymmetric: XK hides the responder's identity
from a passive observer during the handshake; the initiator's identity is still visible
in the first XK message. Full mutual anonymity requires Sphinx routing (planned for a
later release).

## Rationale

**Why append-only and no expiry?**

A peer's static key is tied to its identity keypair, which ADR 005 requires to persist
across restarts. A key in the registry should never become wrong; it can only become
stale if the peer violates ADR 005 (regenerates its keypair). Expiry would require
re-running XX handshakes with known peers unnecessarily. The registry is a cache of
immutable facts, not session state.

**Why type 2 address exchange instead of a dedicated gossip message?**

Every connection already does an address exchange. Piggybacking the public key on that
exchange adds 32 bytes to a message that already travels and costs no additional
round-trip. A dedicated gossip flood would require new message types, scheduling logic,
and flood control. The address exchange is sufficient for the expected mesh size.

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
  `Session::respond()`. It already receives a reference to `PathweaveNode` (added in
  ADR 019 to support forwarding); the key registry is accessed through that reference.
- The address exchange (ADR 017) moves from control type 1 to type 2 when the sender
  has a public key to share (always, once key storage is implemented). Type 1 remains
  valid for nodes running older versions.
- ADR 001's deferred Noise_XK work is resolved by this ADR. The implementation
  conditions stated there (key registry must exist) are satisfied.
- E2E hop encryption uses the key registry to look up the destination's public key
  before sealing the inner payload. That feature is implemented separately and depends
  on this ADR's registry being in place.
