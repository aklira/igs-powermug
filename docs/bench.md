# Bench

- **Status:** Phase P0, partly done. The qualification hardware is **not chosen** yet.
- **Date:** 2026-11-19

The bench has two roles: the development host, used from P1 to P3, and the qualification hardware, used for performance numbers, HIL tests and the release campaigns. Numbers obtained on the development host are not valid for qualification ([SPECIFICATIONS.md](../specifications/SPECIFICATIONS.md) §13.1, [GW] §6.1).

## 1. Development host (in use)

This machine, described in [kernel-features.md](reports/kernel-features.md):

- Ubuntu 26.04 LTS under WSL2, kernel `6.18.33.2-microsoft-standard-WSL2`;
- 16 CPUs, 15.6 GiB RAM, ext4 with 712 GiB free;
- rootless Podman with cgroups v2 and `crun`;
- `systemd --user` with delegated scopes;
- Rust toolchain from rustup (`cargo`, `rustc`);
- `chronyc` (NTP only).

Usable for: contracts, revision crate, sandbox, supervisor, IEC 104 integration on loopback, fuzzing, unit and property tests.

Not usable for: performance budgets (NFR-PERF), real PTP (NFR-TIME), watchdog and HA, nftables (no root), any hardware profile test.

## 2. Qualification hardware (to decide)

[GW] §14 requires a qualified combination of hardware, firmware, OS, network and configuration (principle 10). Two references are needed, as in [SPECIFICATIONS.md](../specifications/SPECIFICATIONS.md) §12 and §20.1 items 5 and 8.

**Proposal, for the owner to confirm:**

| Reference | Role | Minimum | Why |
|---|---|---|---|
| x86_64 industrial PC | Concentrator and standalone profiles (§18.3) | 8 cores, 16 GiB RAM, two NICs, hardware PTP capable NIC, TPM 2.0, watchdog, NVMe with power-loss protection | Journal fsync behaviour, PTP, watchdog |
| ARM64 board | Edge profile (§18.3) | 4 cores, 4 GiB RAM, two NICs, watchdog, eMMC or NVMe | ARM64 portability (NFR-PORT-001) |

The two references are needed by P7 (ARM64 and PTP). The x86_64 reference is needed by P2 for the first p99 measurements.

**Decision needed:** the models of the two references, and the purchase lead time.

## 3. Network segments

Following [MVP] "Architecture de déploiement applicative" and [SPECIFICATIONS.md](../specifications/SPECIFICATIONS.md) §18.3:

| Segment | Carries | Used in |
|---|---|---|
| Management | Target API (mTLS), OpenMetrics | P2 onwards |
| Administration of targets | Adapter control, cell revisions | P2 onwards |
| Process | IEC 104 (2404), Kafka (9092) | P2 onwards |
| Time | NTP or PTP | P2 onwards |

On the development host, these are loopback addresses or separate Podman networks.

## 4. The IBTNM MVP bench (gridloop)

The MVP bench is in `~/Dev/gridloop/bench` (`compose.yaml`, `bench_runner.py`, `configure-fledge.sh`, and others). It runs FledgePower today.

- **Not checked in P0.** Whether the bench starts on this host, and whether it is reachable from CI, is not verified.
- **To do in P0:** start the bench without Powermug, record what runs and what does not, and report. The bench is owned by the gridloop side.

## 5. Tools to install

| Tool | Phase | Purpose |
|---|---|---|
| `buf` | P1 | Protobuf lint and breaking-change checks (PM-PIV-002) |
| `cargo-deny` | P1 | Licence, advisory and source checks (ADR-02) |
| `cargo-fuzz` | P1 | Fuzz targets on loader, decoder and API (NFR-SEC-006) |
| `criterion` (crate) | P2 | Microbenchmarks (PM-PERF-001) |
| `ptp4l`, `phc2sys` | P7 | PTP on hardware |
| `nft` with root | P1 optional | Only if the Landlock fallback needs testing |

## 6. Exit state of the bench in P0

| Item | State |
|---|---|
| Development host described and feature-checked | Done |
| Qualification hardware chosen | **Pending decision** |
| Bench reachable from CI | **Not done** |
