# ADR-17 — Adapter isolation inside one container per cell

- **Status:** Accepted
- **Date:** 2026-11-19
- **Specification:** SPECIFICATIONS.md §4.2, §6.4, §18.2; [GW] FUN-ACQ-001, FUN-ACQ-002, ACC-011

## Context

A crash of any adapter, including one in a C library, must never bring the core down. The owner chose isolation of adapters over the core (requirement given with the brief). A cell also has to stay simple to deploy.

## Decision

Each adapter instance is a separate process, spawned by the core through a minimal spawner and run under its own uid, namespaces, Landlock, seccomp and cgroup. All of them run inside one OCI container per cell, described by one Quadlet unit. The variant with one container per adapter remains possible but is not adopted.

## Consequences

- The cell needs cgroup v2 delegation, which the Quadlet unit requests with `Delegate=yes`.
- The spawner keeps `CAP_SETUID` and `CAP_SETGID` only.
- Development on this host works: the `systemd --user` scope provides a delegated cgroup, and rootless Podman is available (see [kernel report](../reports/kernel-features.md)).

## Open points

Uid allocation. SPECIFICATIONS.md §6.4 says "cell's uid range (default 20000–20999)". On this host, the only subordinate range is `100000:65536`. The specification must say which range applies in development and which in production. This is a follow-up on SPECIFICATIONS.md.
