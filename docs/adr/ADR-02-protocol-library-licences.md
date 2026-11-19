# ADR-02 — Licences of protocol libraries

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §6.7, §21.1; [GW] ADR-02, NFR-MNT-004

## Context

Copyleft protocol libraries (lib60870-C under GPL-3.0 or a commercial licence) would change how adapters can be distributed.

## Decision

Only permissive licences are linked into Powermug: Apache-2.0, MIT, BSD-2-Clause and BSD-3-Clause, the same list as the production target profile `igs-gateway@1` in the instantiation catalogue. lib60870 is used only as an independent peer on the test bench, never linked. Each dependency is checked by `cargo-deny` in CI.

## Consequences

- INST-013 holds for every production implementation.
- A copyleft library needs a new ADR before it can be linked.

## Open points

The licence of `librdkafka` and `lib60870-C` in the catalogue is declared from memory and marked `license_verified: false` in gridloop. It must be verified before the Kafka adapter is qualified (P4).
