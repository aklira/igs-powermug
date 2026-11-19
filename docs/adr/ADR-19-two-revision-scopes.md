# ADR-19 — Cell and partition revision scopes

- **Status:** Accepted
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §10.2, §10.3; [AST] §6.2, §12.6

## Context

A concentrator serves many partitions through shared North modules, such as one IEC 104 server for eleven substations. The IBTNM engine plans per site. A single revision per site cannot describe a shared module.

## Decision

Two scopes: a `cell` revision (shared adapters, shared destinations, partition registry, classes, quotas, time, HA) and a `partition` revision (sessions, points, groups, chains, routes, processors, command ACLs and guards). The active generation is the pair of active revisions. A partition revision is validated against the cell revision it refers to.

## Consequences

- The `target-revision` schema moves to 1.2.0 with a `scope` member (decided; GW §23 and IMPLEMENTATION §6.2).
- Activation of one partition does not touch the data path of another.

## Open points

The igs-iec104 server serves one station per listener, so one listener cannot serve eleven common addresses today. This is recorded in the [igs-iec104 gap report](../reports/igs-iec104-gaps.md), gap G-1, and must be decided before phase P5.
