# ADR 015: Per-platform network cost detection for QuicTransport

**Status:** Accepted

## Context

`QuicTransport::cost()` returns `TransportCost::Metered` unconditionally. That is a
correct conservative default, but it is wrong in the common case: a laptop on WiFi
should report `Free` so the router treats it at the same priority as BLE. The gap is
documented in ARCHITECTURE.md and designated for closure in v0.2.0.

Two constraints shape the design:

1. `Transport::cost()` is a synchronous `fn(&self)`. It cannot perform async or
   blocking I/O on every call; the router calls it on every routing decision.

2. There is no cross-platform Rust crate that answers "is the default route over WiFi
   or cellular?" with acceptable stability and a small dependency footprint. Each OS
   exposes a different API surface (see table below).

The `health_monitor` task in the router already calls `stop()` and `start()` whenever
the non-loopback IPv4 address set changes (ADR 013). `start()` is therefore called on
every meaningful network topology change, making it the right place to refresh the
detected cost. Caching the result in the struct and reading it from `cost()` is both
correct and zero-overhead at read time.

## Decision

Add a `cost: std::sync::atomic::AtomicU8` field to `QuicTransport`. Values are:
`0 = Free`, `1 = Metered`, `2 = Unknown`. `start()` calls the platform-specific
`detect_network_cost()` function and stores the result with `Relaxed` ordering.
`cost()` loads with `Relaxed` ordering and decodes.

`Unknown` is the default and the fallback on detection failure. It is more honest than
hard-coding `Metered`, and the router already handles it: `Unknown` transports are
used last, but they are still used when nothing else is available.

### Per-platform approach

| Platform | API | New dependency |
|---|---|---|
| Linux | `/proc/net/route` default route interface + name prefix classification | none |
| Windows | `NetworkInformation::GetInternetConnectionProfile()` → `GetConnectionCost()` → `NetworkCostType()` | `windows = "0.58"` with `Networking_Connectivity` feature |
| macOS | `if_addrs` active interface names cross-referenced with `system-configuration`'s `get_interfaces()` by BSD name; `SCNetworkInterfaceType` determines cost | `if-addrs` (workspace) + `system-configuration = "0.7"` |
| other | returns `Unknown` | none |

**Linux detail:** Parse `/proc/net/route`. Find the line where the Destination field is
`00000000` (the default route). Extract the interface name. Classify:
- `wlan*`, `wlp*`, `wlx*` → `Free` (WiFi)
- `eth*`, `enp*`, `eno*`, `ens*`, `enx*` → `Free` (Ethernet, including Ethernet-over-USB)
- `wwan*`, `rmnet*`, `ppp*` → `Metered` (cellular)
- unrecognized or parse failure → `Unknown`

The `en*` prefixes cover both the legacy scheme (`eth0`) and all systemd predictable
names: slot-based (`enp*`), on-board (`eno*`), hotplug slot (`ens*`), and MAC-based
(`enx*`, common on Raspberry Pi and USB Ethernet adapters).

`/proc/net/route` is always present on Linux kernels that support networking. Parsing it
requires only `std::fs` and string splitting. No new dependency.

**Windows detail:** `NetworkInformation::GetInternetConnectionProfile()` returns the
profile for the connection most likely used for internet traffic. Its
`GetConnectionCost()` returns a `ConnectionCost` whose `NetworkCostType()` exposes the
OS-level cost classification. `Unrestricted` maps to `Free`; `Fixed` and `Variable`
map to `Metered`; `Unknown` (the WinRT variant) maps to `TransportCost::Unknown`. Any
error returns `Unknown`. `GetInternetConnectionProfile()` is a static synchronous
method that returns cached OS state immediately; it is not an `IAsyncOperation` and
does not require `block_in_place`.

**macOS detail:** `system_configuration::network_configuration::get_interfaces()` returns
all configured network interfaces with their BSD names and interface types. Active
interfaces (those currently assigned an IP) are identified by cross-referencing with
`if_addrs::get_if_addrs()`. For each active interface, `SCNetworkInterface::interface_type()`
returns a `SCNetworkInterfaceType` variant: `IEEE80211` and `Ethernet` map to `Free`;
`WWAN` maps to `Metered`. If multiple active interfaces are found, `Free` takes priority
over `Metered`. Unrecognized or absent types fall through to `Unknown`.

This approach finds the "best" active interface rather than strictly the default route's
interface. In practice these are the same: a machine with active WiFi uses WiFi as the
default route. The edge case (WiFi up but default route via cellular) is rare on desktop
macOS and acceptable for v0.2.0.

## Rationale

**Why not re-query on every `cost()` call?**

`cost()` is called synchronously on the hot routing path. A syscall or framework call on
every invocation would add latency that compounds across multiple transports. Caching at
`start()` gives correct behavior at zero marginal cost per routing decision, and
`health_monitor` ensures the cache is refreshed on every real topology change.

**Why `AtomicU8` and not `Mutex<TransportCost>`?**

`AtomicU8` with `Relaxed` ordering gives lock-free reads. There is no correctness
requirement for sequential consistency here: a slightly stale cost value on the routing
path (in the window between `start()` writing and `cost()` reading) is indistinguishable
in effect from the existing always-`Metered` behavior. `Mutex` would serialize all
routing decisions on the same lock and is unnecessary.

**Why `Unknown` as fallback and not `Metered`?**

`Metered` carries a false precision: it claims we know the connection is metered when we
do not. `Unknown` is honest. The router handles it correctly: `Unknown` transports are
tried after `Free` and `Metered`, but they are not excluded. In a two-transport
deployment (BLE + QUIC), an `Unknown` QUIC transport still provides connectivity when
BLE is unavailable.

**Why interface name heuristics on Linux instead of netlink?**

`rtnetlink` events give the correct default route interface with zero ambiguity, but
they require an async stream and a new crate dependency. The procfs approach requires
only `std::fs::read_to_string` and is correct on any Linux machine with a network
stack. The heuristic covers all systemd predictable name prefixes (`enp*`, `eno*`,
`ens*`, `enx*`), the legacy scheme (`eth*`, `wlan*`), and cellular modem names
(`wwan*`, `rmnet*`, `ppp*`).

**Why not a single cross-platform crate?**

The available candidates (`network-interface`, `default-net`, `netdev`) either lack
interface-type information (only IP addresses), have unstable APIs, or pull in async
runtimes as dependencies. Using OS APIs directly keeps the dependency graph minimal and
the behavior predictable.

## Implications

- `QuicTransport` gains a `cost: AtomicU8` field, initialized to `2` (Unknown).
- `QuicTransport::start()` calls `detect_network_cost()` and stores the result.
- `QuicTransport::cost()` reads and decodes the stored value.
- `pathweave-transport-quic/Cargo.toml` gains:
  - Windows-only: `windows = { version = "0.58", features = ["Networking_Connectivity"] }`
  - macOS-only: `if-addrs = { workspace = true }` and `system-configuration = "0.7"`
- ARCHITECTURE.md TransportCost section updated to remove the "gap" note.
- Battery level, signal quality, and payload-size routing remain deferred beyond v0.2.0.
- Real-time cost change notifications (OS events instead of polling) remain deferred
  to v0.3.0, consistent with the health monitoring decision in ADR 013.
