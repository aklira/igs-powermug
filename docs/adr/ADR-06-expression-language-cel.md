# ADR-06 — Expression language: CEL

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §8.3; [GW] §10.8, ADR-06

## Context

Telemetry calculations and interlock guards need a typed, side-effect-free expression language with guaranteed termination.

## Decision

Use Common Expression Language (CEL). The engine is behind an `ExprEngine` trait; the Rust implementation is ADR-21. The core and the CLI use the same implementation, so validation and evaluation cannot diverge.

## Consequences

- Cost is bounded by CEL's static estimate; expressions without a finite estimate are refused.
- Domain functions are a closed list per core release (`expr.cel.v1`).

## Open points

Conformance of the Rust engine (ADR-21).
