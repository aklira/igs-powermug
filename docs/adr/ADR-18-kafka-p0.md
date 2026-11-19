# ADR-18 — Kafka in P0

- **Status:** Accepted
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §6.7, §21.2; [GW] §3.2

## Context

The MVP migrates S3 and `CELL_DC1`, whose SCADA data also goes to a Kafka bus with a historian. Moving Kafka to P0 was confirmed by the owner.

## Decision

Kafka is a P0 destination, through `rdkafka`, which wraps `librdkafka` (C). It is built as a separate executable (`kafka-producer`) so that the C code is confined by the process boundary (PM5).

## Consequences

- `librdkafka` is never linked into the core.
- The licence of `librdkafka` must be verified before qualification (ADR-02).

## Open points

Authentication model (mTLS or SASL) through secret references (GW §20; instantiation Q6).
