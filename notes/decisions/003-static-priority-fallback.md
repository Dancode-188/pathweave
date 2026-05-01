# ADR 003: Static priority fallback routing for v0.1.0

**Status:** Accepted

## Context

The router needs a strategy for choosing between BLE and QUIC when both are available.
The options ranged from simple static priority to dynamic cost-aware selection based on
battery level, signal quality, payload size, and network type.

Dynamic cost intelligence is the right long-term answer but requires platform-specific
APIs to determine network type (WiFi vs. mobile data), signal strength, and battery
state. That work belongs in v0.2.0 once the core transport machinery is proven.

## Decision

v0.1.0 uses static priority fallback: BLE first, QUIC as fallback. `TransportCost::Free`
beats `TransportCost::Metered`. The router maintains transport availability state
continuously via background monitoring tasks but makes no dynamic cost calculations.

The router does not hold open connections to every peer on every transport
simultaneously. It tracks which transports are available and opens a connection on the
best available transport when `send()` is called (lazy connections, proactive monitoring).

## Consequences

The pw-chat demo works as intended: when internet connectivity drops, BLE is already in
the router's known-reachable set and the switch is immediate rather than waiting for a
send timeout.

There is a narrow window between when a transport drops and when the monitoring task
detects it. A `send()` call in that window will attempt the dead transport, get a
transport error, and retry on the next best option. At most one message is delayed
during a failover.

`TransportCost::Metered` for QUIC is a conservative default that conflates QUIC over
WiFi (flat monthly fee) with QUIC over mobile data (per-megabyte). Detecting the
difference requires platform-specific OS APIs. That detection is the v0.2.0 cost
intelligence starting point.
