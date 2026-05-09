# ADR 001: Use Noise_XX for v0.1.0, defer Noise_XK to v0.3.0

**Status:** Accepted

## Context

Pathweave needs a Noise handshake pattern that works for BLE peer discovery, where two
devices encounter each other without prior knowledge of each other's static keys. The
two candidates were Noise_XX and Noise_XK.

Noise_XK requires the initiator to know the responder's static public key before the
handshake begins. This is fine for connecting to a known peer (a server, a contact from
an address book), but it cannot work when discovering a stranger over BLE. You do not
have their key yet.

Noise_XX lets both sides reveal their static public keys during the handshake. Either
side can initiate without prior knowledge. After the handshake completes, both sides
know each other's static public key and can derive a PeerId.

## Decision

Use `Noise_XX_25519_ChaChaPoly_BLAKE2s` for all sessions in v0.1.0.

## Consequences

Both parties' static public keys are visible to a passive observer who can intercept
the handshake. This is a known limitation of Noise_XX and is documented in SECURITY.md
under "What v0.1.0 does not provide."

Noise_XK, which hides the initiator's identity, becomes the v0.3.0 upgrade once we have
a contact and key registry. The pattern string changes; the snow crate and everything
else stays the same.
