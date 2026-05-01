# ADR 005: Key storage stays outside the library

**Status:** Accepted

## Context

Pathweave needs a static Noise keypair per node to establish peer identity. The question
was whether the library should manage key persistence (reading and writing keys to disk,
a keychain, or a secrets manager) or whether it should accept a keypair from the caller
and do nothing else with storage.

Managing storage inside the library would require making platform-specific decisions:
iOS Secure Enclave, Android Keystore, a file on Linux servers, an in-memory key for
tests. Getting any of those wrong has security consequences. Getting all of them right
for every platform is a significant ongoing maintenance surface.

## Decision

The caller owns key storage. Pathweave accepts a `NodeIdentity` at construction time
and never touches persistence.

```rust
let identity = NodeIdentity::generate();          // caller must persist the private key
let identity = NodeIdentity::from_bytes(bytes)?;  // caller restores from wherever it lives
let node     = PathweaveNode::new(config, identity).await?;
```

## Consequences

The library stays platform-neutral. Each deployment uses whatever storage mechanism
is appropriate: Secure Enclave on iOS, Keystore on Android, a secrets manager on
servers, a fixed keypair in tests.

Testing is clean. Pass a known keypair, get a deterministic PeerId. No filesystem
state to clean up.

One operational consequence worth noting: a server must persist its identity. If it
generates a fresh keypair on restart it gets a new PeerId, and any client that cached
the old one can no longer reach it by PeerId. This is documented in
`notes/architecture/ARCHITECTURE.md`.
