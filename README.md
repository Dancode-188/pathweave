# Pathweave

[![CI](https://github.com/Dancode-188/pathweave/actions/workflows/ci.yml/badge.svg)](https://github.com/Dancode-188/pathweave/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

The internet fails exactly when communication matters most: disaster zones, government shutdowns, contested elections, conflict areas. Apps that need to keep working in those moments have to build multi-transport fallback from scratch every time. QUIC when there's connectivity, BLE when there isn't, SMS or USSD when neither is available. Every team solving this problem re-invents the same plumbing. Pathweave solves it once.

It's a Rust library that abstracts multiple physical transports behind a single API. You call `send()`. Pathweave picks the best available path, handles the Noise_XX handshake, encrypts the payload, and falls back automatically if the preferred transport fails. Your app doesn't change when the network does.

The guarantee is connectivity continuity, not performance continuity. A 200-byte field report feels instant over both QUIC and BLE. A 5MB file does not. BLE is a fallback for survival, not for throughput. For the use cases Pathweave is designed for (coordination messages, field reports, status updates, chat) that distinction rarely matters.

**v0.2.0 status:** QUIC transport (with mDNS auto-discovery), BLE peripheral and central mode on all three platforms, at-least-once delivery, and cost-aware routing are complete. The `pw-chat` demo connects two machines over QUIC or BLE, finds peers automatically via mDNS, and falls back between transports as conditions change. Linux and macOS BLE are awaiting hardware verification (#72, #73); Windows BLE is tested.

**Platform support:** Linux, macOS, and Windows. Linux BLE requires BlueZ. macOS BLE requires Bluetooth permission granted in System Settings.

---

## The demo

**Auto-discovery (recommended):** both sides find each other via mDNS. No configuration needed.

```
# Machine A
cargo run -p pw-chat

# Machine B
cargo run -p pw-chat
```

**Manual mode:** specify the peer address directly.

```
# Machine A
cargo run -p pw-chat -- --peer <machine-B-IP>:9001

# Machine B
cargo run -p pw-chat -- --peer <machine-A-IP>:9001
```

Both modes give you encrypted chat over QUIC with mutual authentication. Neither side sees the other's traffic in plaintext. Neither side can be impersonated.

Both machines already have BLE registered when running `pw-chat`. If both have Bluetooth hardware and are within range, pull the network cable: the conversation keeps going over Bluetooth. The active transport is shown in the status bar. That handoff is what the library is built for.

---

## Using the library

Requires Rust 1.75 or later. Clone the repository first:

```
git clone https://github.com/Dancode-188/pathweave && cd pathweave
```

```toml
[dependencies]
pathweave-core = { path = "crates/pathweave-core" }
pathweave-transport-quic = { path = "crates/pathweave-transport-quic" }
```

```rust
use pathweave_core::{MessageHandler, NodeConfig, NodeIdentity, PeerAnnouncement,
                     PeerAddress, PeerId, PathweaveNode};
use pathweave_transport_quic::QuicTransport;

// Your identity. Generate once, persist somewhere safe. See SECURITY.md.
let identity = NodeIdentity::generate();

let mut node = PathweaveNode::new(NodeConfig::default(), identity).await?;

// Register transports. The router picks the lowest-cost available one.
node.register_transport(Box::new(QuicTransport::new("0.0.0.0:9001".parse()?)));

// Handle incoming messages.
node.on_message(Box::new(MyHandler));

// Dial a peer and learn their PeerId via Noise_XX handshake.
let peer_id = node.connect(PeerAnnouncement {
    address: PeerAddress::Quic("192.168.1.42:9001".parse()?),
    short_id: None,
}).await?;

// Send. Returns Ok(()) when the receiver has confirmed delivery.
node.send(peer_id, b"hello".to_vec()).await?;
```

---

## How it works

```
Your application
      |
  PathweaveNode      send() / on_message() / events()
      |
  Router             picks best available transport by cost
      |
  Session layer      Noise_XX_25519_ChaChaPoly_BLAKE2s, mutual auth, encryption
      |
  Bundle layer       BPv7 framing, fragmentation, reassembly
      |
  Transport trait    dumb byte pipes: QUIC, BLE, more coming
```

Transports know nothing about crypto or peer identity. The Session layer handles all of that. Adding a new transport means implementing one trait. It doesn't touch the crypto, routing, or framing.

**You choose the transports. Pathweave chooses the path.** `register_transport()` is entirely in your hands. Register only `QuicTransport` and you get QUIC only. Register both QUIC and BLE and you get automatic fallback. Pathweave never forces a transport on you; it only selects between the ones you've opted into, the same way TCP/IP selects a routing path without asking the application.

**Cost-aware routing.** BLE is `Free`. QUIC is `Free` on WiFi and `Metered` on mobile data (detected via platform APIs). The router tries Free transports first and falls back to Metered only if needed.

**Lazy connections.** The router doesn't hold open connections to every peer. It opens a connection on the best available transport when `send()` is called and closes it after delivery is confirmed.

**Peer identity.** A PeerId is `base58(blake3(noise_static_public_key))`, derived from the Noise keypair, not assigned by a central authority. You can't impersonate a peer without their private key.

---

## What v0.2.0 includes

- QUIC transport: full send and receive, Noise_XX mutual auth, length-prefixed framing
- QUIC mDNS auto-discovery: both sides announce on the local network and connect automatically
- QUIC cost intelligence: Free on WiFi, Metered on mobile data
- BLE peripheral mode: advertising on Linux (BlueZ), macOS (CoreBluetooth), and Windows (WinRT)
- BLE central mode: scanning, peer discovery, GATT connect and message exchange
- At-least-once delivery: message ID framing, deduplication cache, retry on failure
- Cost-aware routing with WiFi/mobile awareness and multi-address peer support
- Health monitoring: continuous transport availability polling and automatic re-routing
- `pw-chat`: split-pane ratatui TUI with mDNS auto-discovery and transport status bar
- UniFFI bindings layer: the scaffolding for Swift and Kotlin bindings

---

## What v0.2.0 does not include

- **Linux and macOS BLE hardware verification.** The Linux and macOS BLE implementations are correct based on source analysis but have not been confirmed working on physical hardware. Issues #72 and #73 track this. Windows BLE is tested.
- **WiFi Direct, SMS, USSD.** Designed for, not yet implemented. USSD (works on any GSM phone, no data required) is the long-term differentiator for Sub-Saharan Africa.
- **Noise_XK upgrade.** v0.2.0 uses Noise_XX, which reveals both parties' public keys during the handshake. Noise_XK hides the initiator's identity from passive observers and requires a contact registry.
- **Security audit.** Not yet. We're pursuing NLnet/OTF funding for this. See SECURITY.md.

---

## Security

All messages are encrypted with ChaCha20-Poly1305 and authenticated with a Noise_XX handshake before any payload is sent. Both sides prove they hold their static private key. Forward secrecy is on by default: fresh ephemeral keys per session.

The full picture (what we protect, what we don't, the crypto bill of materials, audit status) is in [SECURITY.md](SECURITY.md).

Pathweave never touches key persistence. You decide where the `NodeIdentity` lives. That decision belongs with the app, not the library.

---

## Why this and not X

**libp2p** is the right shape but the wrong constraints: large binary footprint and no transport fallback chain for BLE, SMS, or USSD. Pathweave is designed for the places libp2p can't go.

**Briar/Bramble** has the best protocol design in this space. It's Android-only and Bramble was never released as a standalone library.

**Bridgefy** has been broken twice by researchers: encryption applied after compression, session tokens leaked. The market it served is real. The implementation isn't trustworthy.

**Iroh** is architecturally clean and solves a different problem: QUIC-based content-addressed data sync. It's not designed for transport-agnostic fallback to BLE, SMS, or USSD.

In July 2025 Jack Dorsey shipped Bitchat, mesh chat over Bluetooth, 360K downloads in its first three months, security vulnerabilities published by researchers shortly after launch. The failure wasn't BLE. BLE worked. The failure was the cryptography. Pathweave runs the same Bluetooth transport with Noise_XX (the same cryptographic framework WireGuard uses) plus automatic fallback to QUIC when the internet is available. The demand is real. The good implementation is what Pathweave is building toward.

---

## Contributing

Issues, PRs, and questions are welcome. The design decisions are documented in `notes/decisions/`, worth reading before a significant contribution to understand what was already considered and why.

If you're building election monitoring tools, humanitarian infrastructure, or offline-first apps and want to talk about what Pathweave needs to do for your use case: open an issue.

---

## License

MIT OR Apache-2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
