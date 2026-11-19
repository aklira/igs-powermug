# ADR-15 — Rust revision tooling

- **Status:** Proposed. Written in the specification, not yet confirmed by the owner.
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §10.7, §19; [GW] §12.5, §15.1

## Context

[GW] §12.5 plans a Python `config-cli`. A Python validator would diverge from the Rust core on canonicalization, CEL types and costs (PM3).

## Decision

The revision tooling is the Rust `powermug` CLI, sharing the `powermug-revision` crate with the core. Commands: `validate`, `hash`, `diff`, `preview`, `build`, `sign`, and `catalog export`. The CLI is static, deterministic and offline.

## Consequences

- The gridloop `powermug@1` driver calls the CLI and does not reimplement validation.
- The Python `config-cli` of GW is not written for Powermug.

## Open points

Whether the Python configuration tool is still useful for the isolated-site workflow of GW §12.5 (to decide in P5).
