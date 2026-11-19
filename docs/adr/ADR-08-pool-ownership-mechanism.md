# ADR-08 — Partition ownership in a central pool

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §14.7; [GW] §13.7, ADR-08

## Context

In a concentrator, cells move between hosts of a pool. Ownership must never be granted without proof that the previous owner cannot emit commands.

## Decision

Up to about ten nodes: Pacemaker and Corosync with fencing. Beyond that, or for a heterogeneous pool: a lease in a consensus registry plus watchdog self-fencing. The HA term is the lease epoch.

## Consequences

- The pool mechanism is not needed before phase P8.
- The lease prototype is written in P8 only if the pool exceeds ten nodes.

## Open points

Fencing hardware for the target pool (phase P8).
