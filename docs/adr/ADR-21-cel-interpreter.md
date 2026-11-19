# ADR-21 — CEL engine: cel-interpreter

- **Status:** Proposed. Written in the specification, not yet confirmed by the owner.
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §8.3, §19; [GW] ADR-06

## Context

ADR-06 chooses CEL. A Rust implementation with static cost estimation is needed, and the Python side of the validator must not diverge.

## Decision

Use the `cel-interpreter` crate, behind the `ExprEngine` trait, so that another engine can replace it. The core and the CLI share this single implementation (PM3), which avoids the Rust/Python parity problem for CEL.

## Consequences

- Conformance to the CEL specification must be tested at the start of phase P5.
- If the crate fails the conformance tests, the trait allows a replacement without a change to the revision format.

## Open points

Conformance results and maturity of the crate. Not confirmed yet.
