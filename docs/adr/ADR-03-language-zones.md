# ADR-03 — Language per zone

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §19, §21.1; [GW] §15.1, ADR-03

## Context

[GW] §15.1 splits the code into zones by criticality: the runtime in Rust, tooling and tests in Python, the interface in HTML, CSS and JavaScript.

## Decision

- Rust: core, command plane, process image, journal, adapters, sandbox, revision tooling (see ADR-15), management API.
- Python: the IBTNM engine and the `powermug@1` driver in gridloop, test benches and simulators. Nothing Python runs on a production cell (§15.1).
- HTML, CSS and JavaScript: the local web UI, which reads the public API only.

## Consequences

- The revision validator is Rust, so the Python driver calls the CLI rather than reimplementing validation (PM3).
- Engineers need Rust for the runtime. The phase plan starts with contracts and a walking skeleton to limit the learning risk.

## Open points

None.
