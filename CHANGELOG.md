# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-07

### Added

- `pathweave-core`: `PathweaveNode` public API: `register_transport()` and `add_peer()` for setup, `send()`, `on_message()`, `connect()`, and `events()` for runtime use
- `pathweave-core`: `NodeIdentity` and `PeerId` derivation (`base58(blake3(noise_static_public_key))`)
- `pathweave-core`: Noise_XX_25519_ChaChaPoly_BLAKE2s session layer with mutual authentication and per-session forward secrecy
- `pathweave-core`: BPv7 bundle layer (RFC 9171) for framing, fragmentation, and reassembly
- `pathweave-core`: Cost-aware router with static priority fallback (Free before Metered); lazy connections opened on `send()` and closed after delivery
- `pathweave-transport-quic`: QUIC transport via `quinn` with length-prefixed framing and ephemeral self-signed TLS certificates (authentication handled by Noise_XX, not TLS)
- `pathweave-transport-ble`: BLE central mode (scanning, GATT connect, message exchange) via `btleplug`; peripheral mode (advertising) deferred to v0.2.0
- `pw-chat`: bidirectional terminal chat demo; two machines exchange messages over QUIC with mutual authentication and encryption
- UniFFI bindings scaffolding for Swift and Kotlin (bindings generation deferred to v0.2.0)

### Fixed

- QUIC CONNECTION_CLOSE race: `quinn`'s `write_all()` writes to an internal buffer; the connection was being torn down before the buffer flushed. The sender now waits for a session-level delivery ACK from the receiver before closing, which keeps the connection alive until data is transmitted (#34, #35)
- Accept loop startup race: the accept loop could call `accept()` before `Transport::start()` completed. Fixed with `tokio::sync::Notify`: the monitor task fires a one-shot signal after `start()` succeeds and the accept loop waits on it before entering its loop (#36, #37)

### Security

- All connections authenticated with Noise_XX mutual handshake; both parties prove possession of their static private key before any payload is exchanged
- All payloads encrypted with ChaCha20-Poly1305; no plaintext on the wire after the handshake
- Forward secrecy on by default: fresh ephemeral keys per session
- SECURITY.md documents the full crypto bill of materials, explicit security properties, and known limitations of v0.1.0
