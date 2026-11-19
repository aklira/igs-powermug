# ADR-11 — Local action catalogue

- **Status:** Accepted
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §11.4; [GW] §12.8, ADR-11

## Context

The gateway may act on its own only through a closed list of actions. The IBTNM remediation catalogue adds two actions used in the MVP (`purge_tmp`, `failover_cell`).

## Decision

The catalogue is the one of GW §12.8, extended with `purge_tmp`, `failover_cell` and `quarantine_processor`, which is added by this ADR. Any action outside the catalogue is refused in the revision and at request time (ACC-024).

## Consequences

- The extension is approved; the list is versioned as `local_actions.v1`.
- `quarantine_processor` only stops a processor and sends its outputs invalid. It never changes a command path.

## Open points

The full catalogue of phase P9 (ACC-024 applies to any addition).
