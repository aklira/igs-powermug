# ADR-01 — Development from scratch, no FledgePower code

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §1.4, §21.1; [GW] ADR-01

## Context

The project brief requires a new development with no module reused from FledgePower or Fledge. The v0.5 specification leaves the study of FledgePower's protocol logic open.

## Decision

Powermug is a new runtime with new adapters. No FledgePower or Fledge code, plugin ABI, configuration format or storage layout is reused. Protocol logic comes from the IGS stacks (`igs-iec104` and others).

## Consequences

- A reviewer of any Powermug change can rely on the rule: an identifier or format that comes from FledgePower is a defect, unless it is a published interoperability requirement such as an IEC or Modbus field.
- FledgePower stays an IBTNM target (`ibtnm-fledgepower@1`) on the MVP bench until the takeover of §11.8.

## Open points

None.
