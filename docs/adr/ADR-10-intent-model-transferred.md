# ADR-10 — Intent model and reference formats

- **Status:** Transferred
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §1.4; [GW] ADR-10

## Context

The intent model, the translation and the planning belong to the IBTNM engine, not to the gateway.

## Decision

Powermug has no intent model. Its only input is a signed revision, from any source (PM1, FUN-MGT-002). The decision is recorded in the IBTNM specification.

## Consequences

Powermug code must never reference an intent, an objective or a plan identifier. Traceability is carried by the `parent` and `upstream` fields, which Powermug stores and echoes without reading.

## Open points

None for Powermug.
