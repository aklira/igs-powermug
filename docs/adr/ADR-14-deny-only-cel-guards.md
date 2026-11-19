# ADR-14 — Deny-only CEL guards on commands

- **Status:** Proposed. Written in the specification, not yet confirmed by the owner.
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §9.5, §9.6; [GW] FUN-CMD-012, FUN-PIV-008

## Context

Local interlocking is allowed as a complement to field protections (FUN-CMD-012), but FUN-PIV-008 forbids arbitrary scripts on the command path.

## Decision

A command may carry guards: CEL predicates over the process image that evaluate to `bool`. A false guard, or a referenced point that is not good under `on_unknown: refuse`, refuses the command with `REJECTED_POLICY` and reason `guard:<id>`. A guard can never create, modify or delay a command.

## Consequences

- The command path gains one CEL evaluation, bounded in cost.
- Guards never replace field protections (GW §2.2).

## Open points

Owner confirmation. The author included this decision in the specification without an explicit validation.
