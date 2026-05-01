# ADR 002: Permanent BLE service and characteristic UUIDs

**Status:** Accepted

## Context

The BLE transport needs a service UUID for advertisement and two characteristic UUIDs
for data transfer. These values are visible to any scanning device and are how Pathweave
nodes recognize each other in the wild.

## Decision

The following UUIDs are fixed for the lifetime of the protocol:

```
Service UUID:             82dfc0ba-e2b5-4e65-ad11-c7238ca545c9
Write characteristic:     3439992c-8453-4ca3-9688-639ef5f6f5dc  (Write Without Response)
Notify characteristic:    6de63378-8bc3-4e87-8892-0a9a80efff64  (Notify)
```

Advertisement payload: `[version: u8 = 0x01] ++ [short_id: [u8; 8]]`, where `short_id`
is the first 8 bytes of the node's PeerId. This fits within the classic 31-byte BLE
advertisement limit, giving the widest possible hardware compatibility.

Byte breakdown of the advertisement structure:
- Flags AD structure: 3 bytes (mandatory)
- Service Data AD structure: 1 byte length + 1 byte type + 16 bytes UUID + 1 byte
  version + 8 bytes short_id = 27 bytes
- Total: 30 bytes, one byte to spare

The service UUID is embedded in the Service Data structure rather than in a separate
"Complete List of 128-bit Service UUIDs" entry. Scanning devices can filter by service
UUID from the service data field, so this works correctly for peer discovery.

## Consequences

The `short_id` field is broadcast to any BLE scanner in range, not only Pathweave
nodes. Any device performing a BLE scan will see the first 8 bytes of the node's
PeerId. This is a deliberate tradeoff: partial identity disclosure in exchange for
fast peer filtering without the cost of a full GATT connection and Noise handshake.
Applications that require identity privacy before the handshake should not use BLE
discovery, or should treat the short_id as inherently public information.

These UUIDs cannot change after any deployment. A scanning device uses the service UUID
to identify Pathweave traffic. A characteristic UUID change breaks every integration
silently: the GATT connection succeeds but no data flows. Treat these as protocol
constants on the same level as port numbers.

If a future protocol version requires different characteristics, the service UUID's
version byte (`0x01`) provides the upgrade path without changing the service UUID itself.
