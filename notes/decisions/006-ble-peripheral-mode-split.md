# ADR 006: Split BLE central and peripheral mode across crates

**Status:** Accepted

## Context

BLE has two distinct roles: central (scanning for peers, initiating GATT connections)
and peripheral (advertising presence, accepting GATT connections). A working BLE
fallback in pw-chat requires both roles simultaneously -- one peer must advertise
while the other scans and connects.

No single Rust crate provides reliable support for both roles across all platforms.
The options were: pick one crate and accept its gaps, or split the implementation
by role and by platform.

The two most-evaluated central-mode crates:

- `btleplug` v0.11: cross-platform (Linux, macOS, Windows, Android, iOS), actively
  maintained, good async support. Central mode only. Peripheral mode exists in
  theory but is not implemented on any platform.
- `bluer` v0.17: Linux only. Wraps the full BlueZ D-Bus API including advertising
  (peripheral mode). Async-native, well-maintained by the bluez-dbus-io team.

One alternative for peripheral mode was evaluated: `ble-peripheral`. Active at the
time of evaluation but minimal adoption and no established security track record.
Not appropriate for a library with Pathweave's threat model. Ruled out.

## Decision

Split by role and platform:

- Central mode (scanning, GATT connections): `btleplug` v0.11 on all platforms.
- Peripheral mode (advertising):
  - Linux: `bluer` v0.17, wrapping BlueZ.
  - Android and iOS: native platform APIs (CoreBluetooth, BluetoothManager) called
    through the UniFFI layer. The Rust crate does not handle peripheral mode on
    mobile; the native layer does.
  - macOS: deferred to v0.2.0. CoreBluetooth is available on macOS (same framework
    as iOS); the native binding work is not prioritized for v0.1.0 given that the
    primary deployment targets are phones.
  - Windows: deferred to v0.2.0 (see below).

## Why macOS and Windows peripheral mode are deferred

macOS has CoreBluetooth, the same framework used for iOS peripheral mode. The path
exists. But implementing the native binding layer for macOS desktop is separate work
from the iOS UniFFI path, and the primary deployment targets for v0.1.0 are phones.
A macOS machine running v0.1.0 can scan and connect as central but cannot advertise
as peripheral.

Windows has WinRT BLE GATT Server APIs (available since Windows 10 Creators Update)
but no maintained Rust crate wraps them reliably. A correct wrapper requires the
`windows` crate and careful COM apartment threading, which interacts with async Rust
runtimes in ways that produce subtle, hardware-dependent bugs. The risk and timeline
cost are not justified for v0.1.0 given that the primary deployment targets are phones.

A Windows machine running v0.1.0 can scan and connect as central but cannot advertise
as peripheral. It can still participate in the pw-chat demo as long as the other peer
is a Linux machine or a phone.

## Consequences

`pathweave-transport-ble` uses `btleplug` for all central operations and `bluer` on
Linux for peripheral operations, with a `cfg(target_os = "linux")` conditional at the
crate level.

BLE peripheral development for v0.1.0 requires a Linux machine or a phone via the
native bindings path.

The `bluer` dependency is Linux-only. On macOS and Windows it is excluded at compile
time. CI must include a Linux runner for BLE peripheral tests.

The v0.2.0 macOS work uses CoreBluetooth native bindings, following the same approach
used for iOS via the UniFFI layer. The v0.2.0 Windows work starts with the `windows`
crate WinRT wrapper for GATT Server.
