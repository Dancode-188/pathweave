use clap::Parser;

#[derive(Parser)]
#[command(name = "pw-chat", about = "Pathweave terminal chat demo")]
struct Args {
    #[arg(long, help = "QUIC peer address (host:port). Omit to listen.")]
    peer: Option<std::net::SocketAddr>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let _args = Args::parse();
    todo!("wire up PathweaveNode with QUIC and BLE transports")
}
