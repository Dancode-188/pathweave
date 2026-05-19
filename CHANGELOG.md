# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-05-19

### Added

- `pathweave-transport-ble`: BLE peripheral mode on Linux via `bluer` (GATT server, LE advertising with service UUID and 8-byte `short_id` in service data)
- `pathweave-transport-ble`: BLE peripheral mode on Windows via WinRT `GattServiceProvider`
- `pathweave-transport-ble`: BLE peripheral mode on macOS via `objc2-core-bluetooth` (`CBPeripheralManager`); CoreBluetooth strips service data from advertisements, so macOS peripherals advertise the service UUID only and identity is established via Noise_XX handshake
- `pathweave-transport-quic`: mDNS peer discovery; both sides announce via mDNS and connect automatically when they see each other on the same network, no `--peer` flag required
- `pathweave-transport-quic`: WiFi vs mobile data detection for `TransportCost`; QUIC is `Free` on WiFi and `Metered` on mobile data, improving routing decisions when both QUIC and BLE are registered
- `pathweave-core`: at-least-once delivery; every `send()` call attaches an 8-byte message ID, the receiver deduplicates by `(PeerId, message_id)` and sends an ACK, and the sender retries on failure
- `pathweave-core`: `Transport::start()` now takes `&NodeIdentity`; transports that need to embed peer identity in their advertisements (BLE peripheral) receive it at start time rather than at construction
- `pathweave-core`: `TransportEvent` broadcasting; `PathweaveNode::events()` emits `PeerConnected`, `PeerDisconnected`, and `TransportChanged` events so callers can react to transport lifecycle changes
- `pathweave-core`: multi-address peer routing; a peer reachable via multiple transports accumulates one address per transport and `send()` selects the lowest-cost available path
- `pathweave-core`: health monitor replaces the previous `monitor()` call; polls transport availability continuously and triggers re-routing when a transport recovers or fails
- `pw-chat`: ratatui split-pane TUI with status bar (local ID, peer ID, active transport), message pane, and input box; replaces the previous stdin/stdout REPL
- `pw-chat`: auto-discovery mode; omit `--peer` and both sides find each other via mDNS automatically
- `pw-chat`: logs write to `/tmp/pathweave.log` when `RUST_LOG` is set, so BLE debugging is possible while the TUI's alternate screen is active

### Fixed

- Linux BLE advertisement: `bluer::adv::Advertisement` was constructed with wrong field name (`services` instead of `service_uuids`) and wrong type (`Vec<Uuid>` instead of `BTreeSet<Uuid>`); both were compile errors only on Linux (#72, #74)
- Linux BLE discovery: `discover()` only handled `CentralEvent::ServiceDataAdvertisement`; on Linux `btleplug` fires `CentralEvent::DeviceDiscovered` for the initial device appearance; the first discovery event was silently dropped (#72, #74)
- Windows BLE send failure: WinRT `GattServiceProvider` lifetime and peripheral task sequencing bugs caused send operations to fail after the first connection (#69)
- BLE recovery log noise: spurious tracing output during BLE transport recovery no longer appears in the TUI

### Changed

- `Transport::start(&self)` is now `Transport::start(&self, identity: &NodeIdentity)`; any external `Transport` implementation must update its signature

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
