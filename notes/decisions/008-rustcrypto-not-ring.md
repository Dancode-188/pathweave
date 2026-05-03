# ADR 008: Use RustCrypto as the active Noise resolver; ring is present in the binary but does not handle Noise crypto

**Status:** Accepted

## Context

The `snow` crate (v0.10) supports two cryptographic backends: RustCrypto and ring.
The backend is selected at build time via Cargo features. Snow's default features
activate the RustCrypto backend.

Ring IS present in the compiled binary. This comes from two sources:

- **quinn** (the QUIC transport crate) depends on `rustls`, which depends on `ring`
  for its TLS 1.3 implementation. This is unavoidable while QUIC is a transport.
- **snow's `std` feature** includes `"ring/std"` in its feature list. In Cargo,
  activating `dep/feature` for an optional dependency activates that dependency.
  Since `snow = "0.10"` in the workspace uses default features (which include `std`),
  ring is pulled in from snow as well, even though ring is not snow's active resolver.

This can be verified directly:

```
grep -A 20 'name = "snow"' Cargo.lock
```

Ring appears in snow's `[[package]]` dependencies list. It also appears under snow
in the full dependency tree:

```
cargo tree -p pathweave-core | grep -A 25 "snow v0"
```

A security auditor or new contributor looking at Cargo.lock will see ring in the
dependency graph and may reasonably ask: is this the library doing the Noise crypto?
The answer is no, and this document records why.

## Decision

Use RustCrypto as the active cryptographic backend for the Noise_XX session layer.
Snow's `ring-resolver` feature is not enabled. Ring's presence in the binary is
collateral from two sources (QUIC/TLS and snow's `std` feature) and does not affect
which code handles Noise operations.

## Rationale

**ARM performance**: ChaCha20-Poly1305 was designed for software implementation on
processors without hardware AES acceleration. Mobile ARM chips (the primary deployment
target) fall into this category. WireGuard made the same call for the same reason.
ChaChaPoly in RustCrypto runs fast on ARM; AES-GCM in software does not.

**BLAKE2s for 32-bit targets**: BLAKE2s is optimized for 32-bit word sizes. Cheap
Android phones running 32-bit or constrained 64-bit processors benefit meaningfully.
BLAKE2b is faster on 64-bit desktops but slower on the devices this library targets.

**Ring maintenance concerns**: Ring uses hand-optimized assembly that is hard to
audit and has had periods of slow maintenance and breaking API changes. RustCrypto
primitives are pure Rust (easier to audit), actively maintained, and independently
verified. Using them reduces audit surface at the cost of being slightly slower on
x86-64 desktop, which is not a target we optimize for.

**Separation of concerns**: Keeping the Noise crypto entirely within RustCrypto means
that ring security advisories are only relevant to the QUIC transport (via rustls),
not to the session layer.

## What ring is actually used for in this codebase

Ring's code runs in two places:

1. Via `quinn -> rustls -> ring`: TLS 1.3 for the QUIC transport. This satisfies the
   QUIC protocol requirement (RFC 9000). As documented in ADR 004, all real security
   comes from the Noise_XX handshake; the TLS layer is just the protocol requirement.

2. Via snow's `std` feature activating `ring/std`: ring is compiled as a dependency
   of snow even though it is not snow's active resolver. This is collateral from
   snow's feature structure. Snow declares ring as `optional = true` but its `std`
   feature (which we activate via default features) includes `"ring/std"`, which in
   Cargo's feature resolution activates the optional ring dependency.

In both cases, ring does not handle any Noise handshake messages, key derivation,
or message encryption. All of that goes through snow's default-resolver-crypto path:
chacha20poly1305, blake2, curve25519-dalek, and sha2.

## Can ring be removed from the binary?

Not entirely. Quinn (the QUIC transport) requires ring via rustls. Even if snow were
changed to `default-features = false, features = ["default-resolver-crypto"]` to
avoid snow's `std` activating ring, ring would still be present from quinn. Removing
ring entirely would require replacing quinn with a QUIC implementation that does not
depend on ring, which is a significant undertaking and deferred to v0.2.0 or later.

The `default-features = false` change on snow has not been made because: (a) it would
not remove ring from the binary, so the security picture is unchanged; and (b) it
risks breaking std entropy and other std-dependent features in snow's crypto
primitives without clear benefit. It is not ruled out, but is not the current priority.

## How to verify the active resolver

Snow's active resolver is determined by which backend feature is enabled. In the
workspace Cargo.toml, snow is listed without the `ring-resolver` feature:

```toml
snow = "0.10"
```

Default features include `default-resolver` and `default-resolver-crypto`, not
`ring-resolver`. `Builder::new(params).build_initiator()` in session.rs uses snow's
default resolver, which dispatches to RustCrypto primitives.

To confirm what is compiled under snow:

```
cargo tree -p pathweave-core | grep -A 25 "snow v0"
```

You will see `chacha20poly1305`, `blake2`, `curve25519-dalek`, and `sha2` in snow's
subtree (the RustCrypto path), alongside `ring` (the collateral dep). Ring appears
as a top-level dep of snow, not as part of the resolver dispatch chain for Noise
operations.
