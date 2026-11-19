# ADR-07 — Production distribution and OTA

- **Status:** Accepted (default)
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §18.4; [GW] §15.8, ADR-07

## Context

The operating system choice for production is still open. A single production distribution is required by the target model.

## Decision

Until the first pilot site, use Debian minimal with signed packages and a documented rollback. At P9, move to one minimal production distribution built with Yocto or an equivalent, with RAUC or SWUpdate A/B updates.

## Consequences

- Development and bench run on Ubuntu (this host) and Debian-compatible images; the container image is the portable unit.
- The OTA choice is made in P9 with the hardware references.

## Open points

Choice of RAUC or SWUpdate, and of the production distribution, at P9.
