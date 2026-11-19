# ADR-09 — Aggregation bus

- **Status:** Open (decision in phase P8, with the correlated-peak bench)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §12, §24 Q4; [GW] §15.6, ADR-09

## Context

[GW] §15.6 makes NATS JetStream the default aggregation bus of the concentrator and hybrid topologies. Powermug already uses Kafka at P0 (ADR-18) for the MVP, and Kafka is also a candidate for the aggregation layer.

## Decision

Not decided. The choice between Kafka and NATS JetStream stays open until phase P8. Both can be flux destinations, and the aggregation layer consumes cells only as flux destinations (FUN-SCA-006), so the choice does not change the cell.

## Consequences

- No aggregation code is written before P8.
- The P8 decision uses the correlated-peak bench (ACC-021) and the operation cost of each bus.

## Open points

Decision in P8. JetStream must never be used as the guarantee of command uniqueness (GW §15.6).
