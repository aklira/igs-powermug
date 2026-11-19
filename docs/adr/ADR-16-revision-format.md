# ADR-16 — Revision format and identity

- **Status:** Accepted
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §10.1, §10.6; [GW] FUN-CFG-001, FUN-CFG-003

## Context

Revisions must be easy to author, deterministic in identity, and verifiable by gridloop's Python signing service.

## Decision

Authors write strict YAML 1.2 (core schema) or JSON. Identity is the SHA-256 of the RFC 8785 canonical form, excluding `id`, written `revision:sha256:<hex>`. Signatures are Ed25519 over the canonical manifest, in the `ibtnm.signed-plan` envelope.

## Consequences

- Cross-language canonicalization is tested against gridloop's `rfc8785` on a corpus (PM-ACC-002), from P1.
- YAML anchors, aliases, explicit tags and duplicate keys are refused.

## Open points

None.
