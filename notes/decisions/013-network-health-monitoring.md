# ADR 013: Network health monitoring via interface polling and transport restart

**Status:** Accepted

## Context

v0.2.0 adds mDNS peer discovery (ADR 012). mDNS registrations include the node's current
local IPv4 address, obtained at `start()` time via a UDP routing-table query. If the
network interface changes after startup (DHCP lease renewal, a WiFi switch, or an
interface bounce), two things go wrong simultaneously:

1. The mDNS record still advertises the old address. Peers that resolve it find an
   unreachable endpoint.
2. The QUIC endpoint, bound to `0.0.0.0:PORT`, will re-bind correctly on the next
   incoming connection attempt (because `0.0.0.0` accepts all interfaces), but new
   outgoing connections from this node use the wrong source address as the mDNS hint
   until mDNS is re-registered.

The primary failure mode is that discovered peers can no longer reach this node. The
fix is to restart the transport on interface change: `stop()` tears down the mDNS
registration and QUIC endpoint, `start()` re-queries the routing table and re-registers
with the current address.

## What to monitor

The minimum signal needed is: have the non-loopback IPv4 addresses on this machine
changed since the last check?

Full OS event integration would deliver this with minimal latency:

| Platform | API |
|---|---|
| Linux | `rtnetlink` NEWADDR/DELADDR events via the `netlink-sys` or `rtnetlink` crates |
| macOS / iOS | `NWPathMonitor` (Network.framework) |
| Windows | `WinRT NetworkInformation.NetworkStatusChanged` |

Each API is platform-specific. Wrapping all three for a single feature adds significant
complexity and conditional compilation surface. None of them is available via a
single cross-platform Rust crate at the level of stability we require.

The `if-addrs` crate (already present as a transitive dependency via `mdns-sd`) provides
a synchronous `get_if_addrs()` call that returns the current interface address list.
Polling this on a fixed interval is a workable approximation. The lag is at most one
poll interval. For a LAN mesh application, a few seconds of stale advertisement is
acceptable: peers will fail to connect, retry, and succeed after the re-registration
propagates.

OS event integration is deferred to v0.3.0.

## Decision

### Polling

A background task, `health_monitor()` in `router.rs`, polls `get_if_addrs()` every
`NETWORK_POLL_INTERVAL` (3 seconds). On each tick it computes the set of non-loopback
IPv4 addresses currently assigned to the machine and compares it against the set from
the previous tick. If the sets differ, it triggers a transport restart.

`NETWORK_POLL_INTERVAL` is a named constant in `router.rs`:

```rust
const NETWORK_POLL_INTERVAL: Duration = Duration::from_secs(3);
```

The function signature:

```rust
pub(crate) async fn health_monitor(
    transport: Arc<dyn Transport>,
    started: watch::Sender<bool>,
    available: Arc<AtomicBool>,
)
```

`node.rs` spawns `health_monitor()` in `register_transport()` alongside the existing
`accept_loop` and `peer_stream` tasks. It receives the `watch::Sender<bool>` that was
created for the same transport.

### Transport restart

`health_monitor()` operates in one of two modes.

**Normal mode:** On each tick, compute the current set of non-loopback IPv4 addresses.
If it equals the set from the previous tick, do nothing. If the sets differ, execute the
restart sequence below. On a successful restart, update the previous address set to the
current one and remain in normal mode.

**Recovery mode:** Entered when `start()` fails. On each tick, unconditionally retry
`start()` without comparing address sets. The previous address set is not updated while
in recovery mode. On a successful `start()`, update the previous address set to the
current one and return to normal mode.

Recovery mode exists because the change-detection path fails if `start()` fails and the
interface stays down: both the current and previous address sets would be empty, the
comparison would show no change, and the transport would stay stopped indefinitely. An
unconditional retry path is required.

The restart sequence (executed from normal mode on address-set change, and on every tick
in recovery mode):

1. Sends `false` on the `started` watch channel and sets `available` to `false`. This
   causes `peer_stream` to re-enter `started.wait_for(|v| *v)` and prevents `send()`
   from routing new messages to this transport.
2. Calls `transport.stop().await`. This unregisters the mDNS service, aborts the bridge
   task, shuts down the mDNS daemon, and closes the QUIC endpoint, releasing the port.
3. Calls `transport.start().await`. This re-queries the routing table for the current
   local IPv4 address, creates a fresh `MdnsState` (including a new bridge channel and
   a new `announce_rx`), re-registers the service with the new address, spawns the bridge
   task, and re-binds the QUIC endpoint to `0.0.0.0:PORT`. Creating a fresh `MdnsState`
   each time is what allows `transport.discover()` to return a valid stream on subsequent
   calls: the first call takes `announce_rx` out of the state via `Option::take()`,
   leaving `None`; `start()` repopulates it.
4. On success: sets `available` to `true` and sends `true` on the `started` watch
   channel. `peer_stream` wakes, calls `transport.discover()`, and resumes discovery.
   `send()` can route to this transport again.
5. On failure: logs the error and enters (or remains in) recovery mode.

Re-binding to `0.0.0.0:PORT` after an interface change does not conflict with the
previous endpoint because `stop()` closes the endpoint and releases the port before
`start()` re-binds.

v0.2.0 uses lazy connections: `try_send()` opens a new QUIC connection per send and
closes it after the ACK round-trip (ADR 009). There are no persistent connections to
lose during a restart. A message in-flight during a restart will time out and the
at-least-once retry loop (ADR 011) will retry it on the restarted transport.

### peer_stream resilience

`peer_stream` is a long-running task that must survive transport restarts. The current
implementation calls `transport.discover()` once and drains the resulting stream. After
a restart, the old bridge task is aborted and the old stream yields `None`. The while
loop exits and the task dies silently.

`peer_stream` is changed to loop across restarts:

```rust
loop {
    let _ = started.wait_for(|v| *v).await;
    let mut discover = transport.discover();
    while let Some(announcement) = discover.next().await {
        // same body as before
    }
    // stream ended: transport was stopped. loop back and wait for restart.
}
```

The watch channel carries the restart signal without any additional mechanism:
`health_monitor()` sends `false` before `stop()` and `true` after a successful `start()`.
`peer_stream` observes the transition and re-calls `discover()` at the right moment.

`known_addrs` is not cleared on restart. An address that was in-flight before the restart
remains in the set. This is conservative: the peer at that address may now be
unreachable (their interface also changed), but the next mDNS re-announcement from that
peer will arrive at a new address (or the same address if only this node's interface
changed), and the discover loop will handle it correctly. Clearing `known_addrs` on
restart would cause redundant handshake attempts to all previously known peers; keeping
it avoids that churn.

## Rationale

**Why not clear known_addrs on restart?**

If this node's interface changes but a peer's interface did not, the peer is still at
the same address. Its mDNS re-announcement arrives at the same address that is already
in `known_addrs`. Clearing the set would trigger a redundant handshake to every
previously known peer on every restart. Keeping the set means we only handshake peers
at genuinely new addresses.

**Why polling and not OS events for v0.2.0?**

Three platform-specific event integrations with conditional compilation, separate
dependency trees, and distinct testing requirements. Polling with `if-addrs` is one
function call on all platforms using a dependency already in the graph. The latency
trade-off (up to 3 seconds) is acceptable for a LAN mesh where peers are persistent
and re-registration propagates quickly.

**Why 3 seconds?**

Fast enough that a roaming node re-announces within one handshake timeout (5 seconds,
ADR 009). Slow enough that a brief interface flap (a WiFi reconnect that resolves in
under a second) does not trigger an unnecessary restart. One syscall every 3 seconds
per node is negligible overhead.

**Why does health_monitor own the watch::Sender exclusively?**

`monitor()` currently takes the `watch::Sender<bool>` to send the initial `true` after
`start()` succeeds. `health_monitor()` also needs the sender to toggle between `false`
and `true` on restart. Sharing the sender between the two functions would require an
`Arc<watch::Sender<bool>>`, adding complexity for no benefit.

The clean resolution: absorb the initial `start()` call into `health_monitor()`. The
task is responsible for the full transport lifecycle (initial start, detection, and
restart) and owns the sender exclusively. `monitor()` as a standalone function is
removed. `register_transport()` spawns `health_monitor()` first (which calls `start()`
and sends the initial `true`), then spawns `accept_loop` and `peer_stream` with cloned
receivers. This eliminates the split responsibility and the sharing problem at once.

## Implications

- `monitor()` in `router.rs` is removed. `health_monitor()` absorbs its responsibility.
- `peer_stream()` gains an outer `loop` so it survives transport restarts.
- `register_transport()` in `node.rs` spawns three tasks per transport:
  `health_monitor`, `accept_loop`, and `peer_stream`.
- `health_monitor()` receives `available: Arc<AtomicBool>` from `TransportEntry` and
  updates it alongside the watch sender. The `send()` path continues to read `available`
  without change.
- `known_addrs` is not cleared on restart (see rationale above).
- `if-addrs` is added as a direct dependency of `pathweave-core` even though it is
  already transitively present, to make the dependency explicit and prevent it from
  disappearing silently if `mdns-sd` ever changes its own dependency tree.
- OS event integration (rtnetlink / NWPathMonitor / WinRT) is deferred to v0.3.0.
