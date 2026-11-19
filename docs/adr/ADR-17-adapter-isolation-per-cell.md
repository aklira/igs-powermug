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

Uid allocation is settled by SPECIFICATIONS.md §6.4 and PM-ADP-007: adapter uids are allocated inside the cell's user namespace, from 10000–10999 (H), and mapped to host subordinate ids. Development (rootless) uses `/etc/subuid`, `100000:65536` on this host. Production (rootful) declares the mapping in the Quadlet unit, with disjoint ranges per cell. The fixed host range `20000–20999` is not used.
