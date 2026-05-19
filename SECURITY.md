# Security

## Reporting a vulnerability

Please do not open a public GitHub issue for security vulnerabilities.

Use GitHub's private vulnerability reporting instead. Go to the Security tab of this
repository and click "Report a vulnerability." We'll acknowledge your report within
48 hours and keep you updated as we work on a fix. Credit in the changelog is standard
unless you'd prefer to stay anonymous.

---

## Security properties (v0.2.0)

What Pathweave actually provides in this version:

**Mutual authentication.** Every connection goes through a Noise_XX handshake
(`Noise_XX_25519_ChaChaPoly_BLAKE2s`). Both sides prove they hold their static private
key. You know who you're talking to.

**Forward secrecy.** Noise_XX generates fresh ephemeral keys for every session. If a
static key is compromised later, past sessions stay protected.

**Encrypted transport.** All messages are encrypted with ChaCha20-Poly1305 after the
handshake. Nobody reading the wire sees the content.

**Peer identity binding.** A PeerId is derived from the static public key
(`base58(blake3(public_key))`). You can't impersonate a peer without their private key.

**Replay suppression.** Every message carries an 8-byte ID. The receiver maintains a
deduplication cache keyed on `(PeerId, message_id)` with a 60-second TTL. A replayed
message with the same ID from the same peer is ACKed but not delivered to the handler a
second time, preventing duplicate delivery from the at-least-once retry mechanism.

---

## What v0.2.0 does not provide

Being clear about this matters.

**No initiator identity hiding.** We use Noise_XX, where both sides reveal their static
keys during the handshake. A passive observer who can intercept the handshake learns
both parties' public keys. Noise_XK, which hides the initiator's identity from passive
observers, is a future milestone.

**No group key exchange.** Noise_XX handles 1:1 sessions. There is no MLS or any other
group key mechanism in v0.2.0.

**No formal security audit.** Pathweave has not been reviewed by a third-party security
firm. We're working toward that. See `SECURITY_AUDIT_STATUS` below.

---

## Crypto bill of materials

Every cryptographic dependency, what it does, and what we know about its review status.

| Crate | Role | Audit status |
|---|---|---|
| `snow` v0.10 | Noise_XX handshake implementation | No formal audit. Minimal logic on top of audited primitives below. |
| `x25519-dalek` | Curve25519 key exchange (used by snow) | Audited by Quarkslab (2019, part of broader dalek libraries audit commissioned by Tari Labs); no critical findings |
| `chacha20poly1305` | AEAD encryption (used by snow) | Audited by NCC Group, no significant findings |
| `blake2` | BLAKE2s hash (used by snow) | Pure Rust, RustCrypto project, no formal audit |
| `blake3` | PeerId derivation | Maintained by the Blake3 spec authors, no formal audit |
| `bp7` | BPv7 bundle framing, fragmentation, reassembly; parses untrusted remote input | No formal audit |
| `rustls` | TLS 1.3 (used by quinn for QUIC) | Audited by Cure53 (2020, funded by CNCF); sponsored by ISRG Prossimo |
| `quinn` | QUIC transport | No formal audit |
| `rcgen` | Ephemeral self-signed TLS cert generation | No formal audit |

**Note on QUIC TLS:** Pathweave generates a throwaway TLS certificate per session to
satisfy QUIC's protocol requirement. This certificate is not used for authentication.
All authentication and encryption are handled by Noise_XX.

---

## SECURITY_AUDIT_STATUS

No third-party security audit has been completed as of this writing. This file will be
updated when that changes.

We're pursuing funding for an audit through NLnet, OTF, or similar security-focused
grant programs. If you're an organization that funds security audits of open source
infrastructure, we'd like to hear from you.
