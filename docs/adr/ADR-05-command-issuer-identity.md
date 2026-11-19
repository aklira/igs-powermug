# ADR-05 — Identity of command issuers

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §9.3; [GW] §11.6, ADR-05, §20.1 item 11

## Context

Authorization cannot be finer than the identity the North protocol carries. IEC 104 without TLS identifies only the peer address and the common address.

## Decision

Adopt the table of [GW] §11.6. Every command records the identity it actually had and its level. Powermug does not claim to identify a human operator when the protocol does not allow it. ACLs are written at the finest level available and document that level.

## Consequences

- IEC 104 without TLS is restricted to isolated networks. The revision must say so.
- Operator traceability for such links stays with the upstream SCADA.

## Open points

The operator identity model of each North protocol (GW §20.1 item 11) remains to be confirmed with the customer.
