# ADR-20 — OPC UA on igs-opcua

- **Status:** Accepted
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §6.7, §24 Q6; [GW] §3.2, §17.6

## Context

The owner chose `igs-opcua` for the OPC UA client and server. The library does not exist yet.

## Decision

OPC UA adapters are built on `igs-opcua`, to be developed. They are not on the critical path of P0 to M3. If the library is not ready by phase P7, the OPC UA adapters move to P9 without changing M4.

## Consequences

- The OPC UA client and server appear in the architecture diagrams marked "to develop".
- No OPC UA code is written before the library is available.

## Open points

Delivery date of `igs-opcua` (SPECIFICATIONS.md §24 Q6).
