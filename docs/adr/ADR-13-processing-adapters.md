# ADR-13 — Processing adapters (N3)

- **Status:** Accepted
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §8.1, §8.5, §8.6; [GW] §3.3, §10.8, §16

## Context

Some processing does not fit the closed transform catalogue or CEL: stateful or heavy computation such as a state estimator or event detection. [GW] excludes third-party plugins, natively or in WebAssembly, from the core process (NFR-MNT-002).

## Decision

A processing adapter is a first-party, signed, out-of-process module, run with the adapter sandbox and supervision. It has two kinds of output points: derived points (declared inputs, quality no better than the worst input) and own points (no inputs, quality set by the processor, treated as a virtual South source). Its outputs re-enter through ingestion. It has no command service, and a command aimed at its points is refused (`not_commandable`). Every point it writes is declared in the revision; undeclared writes are refused. Processing adapters are listed in the module catalogue with a qualification status.

## Consequences

- This relaxes the exclusion in [GW] §3.3, §10.8 and §16 for first-party signed modules only. A note is due in GW (SPECIFICATIONS.md §23).
- Each N3 hop costs one process round trip and one journal write, a few milliseconds. Chains are bounded to a depth of 4.
- A processor can be disabled (`enabled: false`) or run in shadow mode without a core release.

## Open points

Qualification criteria of a processor kind (PM-ACC-008, 009). The state estimator is a prototype used to qualify N3, not a product feature.
