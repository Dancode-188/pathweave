use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use pathweave_core::{
    MessageHandler, NodeConfig, NodeIdentity, PeerAddress, PeerAnnouncement, PeerId, TransportEvent,
};
use pathweave_transport_ble::BleTransport;
use pathweave_transport_quic::QuicTransport;
use tokio::io::AsyncBufReadExt;

const DEFAULT_LISTEN_PORT: u16 = 9001;

#[derive(Parser)]
#[command(
    name = "pw-chat",
    about = "Pathweave terminal chat demo",
    long_about = "Bidirectional encrypted chat over QUIC with automatic BLE fallback.\n\n\
        Manual mode: specify --peer on both sides.\n\
        \n  Machine A:  pw-chat --peer 192.168.1.2:9001\
        \n  Machine B:  pw-chat --peer 192.168.1.1:9001\n\n\
        Auto-discovery mode: omit --peer. Both sides announce via mDNS and connect\n\
        automatically when they see each other on the same network."
)]
struct Args {
    /// QUIC peer address (host:port). Omit to use mDNS auto-discovery.
    #[arg(long)]
    peer: Option<SocketAddr>,

    /// Local QUIC listen port. Must match what the other side connects to.
    #[arg(long, default_value_t = DEFAULT_LISTEN_PORT)]
    port: u16,
}

struct PrintHandler {
    label: Arc<str>,
}

impl PrintHandler {
    fn new(label: &str) -> Self {
        Self {
            label: Arc::from(label),
        }
    }
}

impl MessageHandler for PrintHandler {
    fn on_message(&self, _peer_id: PeerId, payload: Vec<u8>) {
        let text = String::from_utf8_lossy(&payload);
        println!("{}: {}", self.label, text);
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let identity = NodeIdentity::generate();
    println!("my peer id: {}", identity.peer_id());

    let listen_addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse().unwrap();

    let mut node = pathweave_core::PathweaveNode::new(NodeConfig::default(), identity)
        .await
        .expect("failed to create node");

    // Subscribe before registering transports: broadcast only delivers events sent
    // after subscription. Registering first risks missing TransportChanged if a
    // health_monitor task fires on another thread before events() is called.
    let mut events = node.events();

    let quic = QuicTransport::new(listen_addr);
    node.register_transport(Box::new(quic));
    node.register_transport(Box::new(BleTransport::new()));
    loop {
        match events.next().await {
            Some(TransportEvent::TransportChanged { .. }) => break,
            None => {
                eprintln!("error: event stream closed before any transport started");
                std::process::exit(1);
            }
            _ => {}
        }
    }

    if let Some(peer_addr) = args.peer {
        let announcement = PeerAnnouncement {
            address: PeerAddress::Quic(peer_addr),
            short_id: None,
        };

        print!("connecting to {}...", peer_addr);
        let peer_id = match node.connect(announcement).await {
            Ok(id) => id,
            Err(e) => {
                eprintln!("error: could not connect to {}: {}", peer_addr, e);
                eprintln!(
                    "hint: check that the other side is running and port {} is not blocked by a firewall",
                    args.port
                );
                std::process::exit(1);
            }
        };
        let short_id = &peer_id.to_base58()[..8];
        println!(" connected. peer: {}", short_id);

        node.on_message(Box::new(PrintHandler::new(short_id)));
        run_send_loop(&node, peer_id).await;
    } else {
        println!("listening on {}. waiting for peer via mDNS...", listen_addr);

        let peer_id = loop {
            match events.next().await {
                Some(TransportEvent::PeerConnected(peer_id)) => break peer_id,
                None => {
                    eprintln!("error: event stream closed before a peer was discovered");
                    std::process::exit(1);
                }
                _ => {}
            }
        };

        let short_id = &peer_id.to_base58()[..8];
        println!("peer discovered: {}. starting chat.", short_id);

        node.on_message(Box::new(PrintHandler::new(short_id)));
        run_send_loop(&node, peer_id).await;
    }
}

async fn run_send_loop(node: &pathweave_core::PathweaveNode, peer_id: PeerId) {
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await.expect("stdin error") {
        if line.is_empty() {
            continue;
        }
        if let Err(e) = node.send(peer_id.clone(), line.into_bytes()).await {
            eprintln!("message failed: {}", e);
        }
    }
}
