use std::io;
use std::net::SocketAddr;

use clap::Parser;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use pathweave_core::{
    MessageHandler, NodeConfig, NodeIdentity, PeerAddress, PeerAnnouncement, PeerId,
    TransportEvent, TransportKind,
};
use pathweave_transport_ble::BleTransport;
use pathweave_transport_quic::QuicTransport;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    text::Line,
    widgets::{Block, Borders, Paragraph},
    Frame, Terminal,
};
use tokio::sync::mpsc;

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

struct TuiHandler {
    tx: mpsc::UnboundedSender<(PeerId, Vec<u8>)>,
}

impl MessageHandler for TuiHandler {
    fn on_message(&self, peer_id: PeerId, payload: Vec<u8>) {
        let _ = self.tx.send((peer_id, payload));
    }
}

struct App {
    messages: Vec<String>,
    input: String,
    peer_id: Option<PeerId>,
    peer_short_id: Option<String>,
    transport_name: Option<&'static str>,
    local_short_id: String,
}

impl App {
    fn new(local_short_id: String, peer_id: Option<PeerId>) -> Self {
        let peer_short_id = peer_id.as_ref().map(|id| id.to_base58()[..8].to_owned());
        Self {
            messages: Vec::new(),
            input: String::new(),
            peer_id,
            peer_short_id,
            transport_name: None,
            local_short_id,
        }
    }
}

fn transport_kind_name(kind: TransportKind) -> &'static str {
    match kind {
        TransportKind::Quic => "QUIC",
        TransportKind::Ble => "BLE",
        TransportKind::WifiDirect => "WiFiDirect",
        TransportKind::BleAdvertising => "BLE-Advertising",
    }
}

fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let peer_label = app.peer_short_id.as_deref().unwrap_or("waiting...");
    let transport_label = app.transport_name.unwrap_or("none");
    let status = Paragraph::new(format!(
        " me: {}   peer: {}   transport: {}",
        app.local_short_id, peer_label, transport_label
    ))
    .style(Style::default().bg(Color::Blue).fg(Color::White));
    frame.render_widget(status, chunks[0]);

    let inner_height = chunks[1].height.saturating_sub(2) as usize;
    let start = app.messages.len().saturating_sub(inner_height);
    let lines: Vec<Line> = app.messages[start..]
        .iter()
        .map(|m| Line::from(m.as_str()))
        .collect();
    let messages = Paragraph::new(lines).block(Block::default().borders(Borders::ALL));
    frame.render_widget(messages, chunks[1]);

    let input_text = format!("> {}", app.input);
    let input_widget = Paragraph::new(input_text.as_str())
        .block(Block::default().borders(Borders::ALL).title("input"));
    frame.render_widget(input_widget, chunks[2]);

    // Cursor: inside left border (x+1), past the "> " prompt (x+2), at end of input text.
    let cursor_x = chunks[2].x + 1 + 2 + app.input.len() as u16;
    let max_x = chunks[2].x + chunks[2].width.saturating_sub(2);
    frame.set_cursor_position((cursor_x.min(max_x), chunks[2].y + 1));
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    node: &pathweave_core::PathweaveNode,
    mut node_events: futures::stream::BoxStream<'static, TransportEvent>,
    mut msg_rx: mpsc::UnboundedReceiver<(PeerId, Vec<u8>)>,
    mut app: App,
) -> io::Result<()> {
    let mut key_events = EventStream::new();

    loop {
        terminal.draw(|f| draw(f, &app))?;

        tokio::select! {
            maybe_key = key_events.next() => {
                let Some(Ok(Event::Key(key))) = maybe_key else { continue };
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return Ok(());
                    }
                    KeyCode::Esc => {
                        return Ok(());
                    }
                    KeyCode::Enter if !app.input.is_empty() => {
                        if let Some(peer_id) = app.peer_id.clone() {
                            let text = std::mem::take(&mut app.input);
                            app.messages.push(format!("me: {}", text));
                            // Redraw before awaiting send so the message appears immediately.
                            terminal.draw(|f| draw(f, &app))?;
                            if let Err(e) = node.send(peer_id, text.into_bytes()).await {
                                app.messages.push(format!("[error] {}", e));
                            }
                        }
                    }
                    KeyCode::Backspace => {
                        app.input.pop();
                    }
                    KeyCode::Char(c) => {
                        app.input.push(c);
                    }
                    _ => {}
                }
            }
            maybe_event = node_events.next() => {
                match maybe_event {
                    Some(TransportEvent::MessageDelivered { transport, .. }) => {
                        app.transport_name = Some(transport_kind_name(transport));
                    }
                    Some(TransportEvent::PeerConnected(peer_id)) => {
                        let short_id = peer_id.to_base58()[..8].to_owned();
                        app.messages.push(format!("[connected] peer: {}", short_id));
                        app.peer_short_id = Some(short_id);
                        app.peer_id = Some(peer_id);
                    }
                    Some(TransportEvent::PeerDisconnected(peer_id)) => {
                        let short_id = peer_id.to_base58()[..8].to_owned();
                        app.messages.push(format!("[disconnected] peer: {}", short_id));
                    }
                    Some(_) | None => {}
                }
            }
            maybe_msg = msg_rx.recv() => {
                if let Some((peer_id, payload)) = maybe_msg {
                    let short_id = peer_id.to_base58()[..8].to_owned();
                    let text = String::from_utf8_lossy(&payload);
                    app.messages.push(format!("{}: {}", short_id, text));
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // Default to off so nothing bleeds into the TUI's alternate screen.
    // When RUST_LOG is set, write to /tmp/pathweave.log instead of stderr
    // so the logs land somewhere readable regardless of terminal state.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off"));
    if std::env::var_os("RUST_LOG").is_some() {
        let log_file = std::fs::File::create("/tmp/pathweave.log")
            .expect("failed to create /tmp/pathweave.log");
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::sync::Mutex::new(log_file))
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::stderr)
            .init();
    }

    let args = Args::parse();

    let identity = NodeIdentity::generate();
    let local_short_id = identity.peer_id().to_base58()[..8].to_owned();

    let listen_addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse().unwrap();

    let mut node = pathweave_core::PathweaveNode::new(NodeConfig::default(), identity)
        .await
        .expect("failed to create node");

    // Subscribe before registering transports to avoid missing the first TransportChanged.
    let events = node.events();

    node.register_transport(Box::new(QuicTransport::new(listen_addr)));
    node.register_transport(Box::new(BleTransport::new()));

    let (msg_tx, msg_rx) = mpsc::unbounded_channel();
    node.on_message(Box::new(TuiHandler { tx: msg_tx }));

    let initial_peer_id = if let Some(peer_addr) = args.peer {
        let announcement = PeerAnnouncement {
            address: PeerAddress::Quic(peer_addr),
            short_id: None,
        };
        match node.connect(announcement).await {
            Ok(id) => Some(id),
            Err(e) => {
                eprintln!("error: could not connect to {}: {}", peer_addr, e);
                eprintln!(
                    "hint: check that the other side is running and port {} is not blocked by a firewall",
                    args.port
                );
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let app = App::new(local_short_id, initial_peer_id);

    // Restore the terminal before printing any panic message, otherwise the
    // shell is left in raw mode with the alternate screen active.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    enable_raw_mode().expect("failed to enable raw mode");
    execute!(io::stdout(), EnterAlternateScreen).expect("failed to enter alternate screen");
    let mut terminal =
        Terminal::new(CrosstermBackend::new(io::stdout())).expect("failed to create terminal");

    let result = run_app(&mut terminal, &node, events, msg_rx, app).await;

    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);

    if let Err(e) = result {
        eprintln!("terminal error: {}", e);
        std::process::exit(1);
    }
}
