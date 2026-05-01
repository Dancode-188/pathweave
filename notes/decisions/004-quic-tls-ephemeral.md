# ADR 004: Satisfy QUIC's TLS requirement with an ephemeral self-signed certificate

**Status:** Accepted

## Context

QUIC requires TLS 1.3 (RFC 9000). There is no way to use QUIC without it. Pathweave
uses Noise_XX for all authentication and encryption, so the TLS layer serves only as
a protocol compliance requirement, not as a security boundary.

The options were: use a long-lived certificate tied to node identity, or generate a
throwaway certificate per session.

Tying the TLS certificate to node identity would couple two independent security layers
and create confusion about which one is authoritative. It would also mean managing
certificate lifetimes and renewal, which adds operational complexity for no security
benefit.

## Decision

Generate a fresh ephemeral self-signed certificate at startup using `rcgen`. One
certificate is generated per node process startup, not per individual QUIC connection.
Discard it when the process exits. The certificate has no relationship to the Noise
keypair or the node's PeerId.

All authentication and encryption come from Noise_XX. The TLS certificate exists solely
to satisfy the QUIC protocol requirement.

## Consequences

The two security layers stay independent and easy to reason about. The QUIC connection
is encrypted by TLS, and then Noise_XX runs on top of that, providing the actual peer
authentication and forward secrecy that Pathweave guarantees.

SECURITY.md documents this clearly because developers integrating Pathweave need to
understand that peer authentication comes from Noise_XX, not from TLS certificate
validation.
