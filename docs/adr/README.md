# Architecture decision records

One file per decision. The numbers are those of [SPECIFICATIONS.md](../../specifications/SPECIFICATIONS.md) §21: ADR-01 to ADR-12 are the decisions of the gateway specification v0.5 and their Powermug position; ADR-13 to ADR-21 are Powermug's own.

Status values:

- **Accepted**: decided and in force.
- **Accepted (default)**: the default position of the v0.5 specification, adopted by Powermug. It can be revisited with a new ADR.
- **Proposed**: written in the specification and not yet confirmed by the owner.
- **Open**: deliberately deferred to a named phase.
- **Transferred**: belongs to another specification.

## Template

```markdown
# ADR-NN — Title

- **Status:**
- **Date:**
- **Specification:** SPECIFICATIONS.md §…; [GW] ADR-NN if any

## Context

## Decision

## Consequences

## Open points
```

## Index

| ADR | Title | Status |
|---|---|---|
| [ADR-01](ADR-01-no-fledgepower-reuse.md) | Development from scratch, no FledgePower code | Accepted (default) |
| [ADR-02](ADR-02-protocol-library-licences.md) | Licences of protocol libraries | Accepted (default) |
| [ADR-03](ADR-03-language-zones.md) | Language per zone | Accepted (default) |
| [ADR-04](ADR-04-single-copy-ha.md) | Single-copy continuation after fencing | Accepted (default) |
| [ADR-05](ADR-05-command-issuer-identity.md) | Identity of command issuers | Accepted (default) |
| [ADR-06](ADR-06-expression-language-cel.md) | Expression language: CEL | Accepted (default) |
| [ADR-07](ADR-07-production-distribution.md) | Production distribution and OTA | Accepted (default) |
| [ADR-08](ADR-08-pool-ownership-mechanism.md) | Partition ownership in a central pool | Accepted (default) |
| [ADR-09](ADR-09-aggregation-bus.md) | Aggregation bus | Open (phase P8) |
| [ADR-10](ADR-10-intent-model-transferred.md) | Intent model | Transferred |
| [ADR-11](ADR-11-local-action-catalogue.md) | Local action catalogue | Accepted |
| [ADR-12](ADR-12-contract-versioning.md) | Versioning of the management contract | Accepted (default) |
| [ADR-13](ADR-13-processing-adapters.md) | Processing adapters (N3) | Accepted |
| [ADR-14](ADR-14-deny-only-cel-guards.md) | Deny-only CEL guards on commands | Proposed |
| [ADR-15](ADR-15-rust-revision-tooling.md) | Rust revision tooling | Proposed |
| [ADR-16](ADR-16-revision-format.md) | Revision format and identity | Accepted |
| [ADR-17](ADR-17-adapter-isolation-per-cell.md) | Adapter isolation inside one container per cell | Accepted |
| [ADR-18](ADR-18-kafka-p0.md) | Kafka in P0 | Accepted |
| [ADR-19](ADR-19-two-revision-scopes.md) | Cell and partition revision scopes | Accepted |
| [ADR-20](ADR-20-opcua-igs-opcua.md) | OPC UA on igs-opcua | Accepted |
| [ADR-21](ADR-21-cel-interpreter.md) | CEL engine: cel-interpreter | Proposed |
