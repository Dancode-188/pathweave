# ADR 007: Use BPv7 for bundle framing, fragmentation, and reassembly

**Status:** Accepted

## Context

The library needs a message framing layer that sits between the Session (crypto) layer
and the raw transport connection. This layer has three jobs:

1. Frame messages so the receiver knows where each one starts and ends.
2. Fragment messages that exceed the transport's MTU (BLE is ~512 bytes).
3. Reassemble fragments on the receiving end before handing bytes to the Session layer.

Options considered:

**Custom length-prefix framing** -- a 4-byte big-endian length header followed by the
payload. Simple, zero dependencies, no overhead beyond the header. Does not give you
anything else for free.

**CBOR-only** -- encode messages as CBOR byte strings. Self-delimiting, widely
implemented. Still needs a fragmentation scheme on top for small MTUs.

**BPv7 (Bundle Protocol version 7, RFC 9171)** -- a full delay-tolerant networking
bundle format encoded as CBOR. Carries a primary block with a creation timestamp,
sequence number, and optional fragmentation fields (fragmentation_offset,
total_data_length). The `bp7` crate (v0.10) implements the spec.

## Decision

Use BPv7 via the `bp7` crate.

## Rationale

The bundle ID (creation timestamp + sequence number) is the groundwork for
at-least-once delivery in v0.2.0. A custom framing format would not carry this
information, so adding at-least-once delivery later would require a protocol change.
BPv7 gives us the infrastructure now at the cost of a slightly larger header.

BPv7's fragmentation model (BUNDLE_IS_FRAGMENT flag, fragmentation_offset,
total_data_length in the primary block) maps cleanly onto the problem. Fragments of
the same bundle share a creation timestamp, so reassembly only needs to accumulate
by sequence number and offset without a separate bundle-ID field.

BPv7 is the IETF standard for delay-tolerant networking. The library is aimed at
exactly the conditions DTN was designed for: unreliable links, high latency, small
MTUs. Using the standard format means Pathweave bundles are at least structurally
compatible with other DTN implementations, even if full interop is not a v0.1.0 goal.

## Implementation notes

CRC is set to CRC_NO on all bundles. CRC in BPv7 is optional and redundant here:
the Noise_XX session layer provides authenticated encryption (ChaCha20-Poly1305 with
a 16-byte authentication tag) which already detects any bit corruption or tampering.
Computing a CRC on top would add overhead with no security or reliability benefit.

The `bp7` crate lists `cdylib` in its crate-type, which forces full symbol resolution
during `cargo test`. On Windows MSVC, this surfaced an unresolved symbol
(SystemFunction036 from advapi32.dll) in nanorand 0.7.0, a transitive dependency.
The fix is in `.cargo/config.toml`: a workspace-level linker flag that adds advapi32
as a default library for the MSVC target. This is documented in that file.

## Alternatives not chosen

**Custom length-prefix framing**: Ruled out because it provides no path to at-least-once
delivery without a protocol version bump. The simplicity is not worth the dead end.

**CBOR-only**: Same problem as custom framing -- no bundle ID, fragmentation would
be ad-hoc.
