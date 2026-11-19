# ADR-12 — Versioning of the management contract and capabilities

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §11.2, §11.9; [GW] ADR-12, FUN-SCA-009

## Context

Several generations of gateway coexist, and the aggregation layer must accept cells one to two versions older.

## Decision

Semantic versioning for the management contract, with N-2 compatibility. Capabilities are versioned individually (`south.iec104` version `1.0`, and so on). The capability document is canonical JSON, and its hash binds the planner's signed plan (`runtime_capabilities_sha256`).

## Consequences

- A major change of the contract requires a new target profile version or a documented transfer procedure.
- The capability catalogue is generated from the release, not written by hand.

## Open points

None.
