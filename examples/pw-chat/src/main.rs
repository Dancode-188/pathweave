use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use pathweave_core::{
    MessageHandler, NodeConfig, NodeIdentity, PeerAddress, PeerAnnouncement, PeerId,
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
        Run with --peer on both sides for full bidirectional chat:\n\
        \n  Machine A:  pw-chat --peer 192.168.1.2:9001\
        \n  Machine B:  pw-chat --peer 192.168.1.1:9001\n\n\
        Omit --peer to listen and receive only."
)]
struct Args {
    /// QUIC peer address (host:port). Specify on both sides for bidirectional chat.
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

    let mut node = pathweave_core::PathweaveNode::new(NodeConfig::default(), identity)
        .await
        .expect("failed to create node");

    if let Some(peer_addr) = args.peer {
        // Bidirectional mode: bind to a known port so the peer can connect back.
        let listen_addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse().unwrap();
        let quic = QuicTransport::new(listen_addr);
        node.register_transport(Box::new(quic));
        node.register_transport(Box::new(BleTransport::new()));

        // Yield so the monitor tasks run start() and mark transports available.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let announcement = PeerAnnouncement {
            address: PeerAddress::Quic(peer_addr),
            short_id: None,
        };

        print!("connecting to {}...", peer_addr);
        let peer_id = node.connect(announcement).await.expect("failed to connect");
        let short_id = &peer_id.to_base58()[..8];
        println!(" connected. peer: {}", short_id);

        node.on_message(Box::new(PrintHandler::new(short_id)));

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
    } else {
        // Listener mode: bind to the specified port and wait for connections.
        let listen_addr: SocketAddr = format!("0.0.0.0:{}", args.port)
            .parse()
            .expect("invalid listen address");
        let quic = QuicTransport::new(listen_addr);
        node.register_transport(Box::new(quic));
        node.register_transport(Box::new(BleTransport::new()));

        println!("listening on {}", listen_addr);
        node.on_message(Box::new(PrintHandler::new("peer")));

        // Block forever; the accept loop delivers messages to the handler.
        std::future::pending::<()>().await;
    }
}
