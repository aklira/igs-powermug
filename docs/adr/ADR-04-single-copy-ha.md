# ADR-04 — Single-copy continuation after fencing

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §14, §21.1; [GW] §13.5, ADR-04, §20.1 item 8

## Context

When the passive node is fenced and confirmed out of service, the active node has no replica of the critical state. Either it continues alone, accepting commands in single copy, or it stops command acceptance.

## Decision

The default is continuation in single copy: the active node keeps accepting commands, raises `HA_SINGLE_COPY` and audits entry and exit. A site may configure a strict stop instead. Without fencing proof, no promotion happens.

## Consequences

- The residual risk is a double failure during the rebuild window of the passive node. It is accepted by default.
- Each site decides the policy in its revision.

## Open points

Customer confirmation of single-copy continuation, or the choice of a strict stop, is still required (GW §20.1 item 8).
