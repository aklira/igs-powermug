# Progress

Implementation status of Powermug, against [specifications/IMPLEMENTATION.md](specifications/IMPLEMENTATION.md). Update this file whenever a task or phase changes state.

- **Last updated:** 2026-11-19
- **Current phase:** P0 · Framing, in progress
- **Milestones reached:** none (M1 to M5 are not reached)

Legend: `[x]` done · `[~]` in progress · `[ ]` not started · `[!]` blocked, with the reason.

## Phases

### P0 · Framing (1 week)

- [x] ADR-13 to ADR-21 recorded, and positions on ADR-01 to ADR-12 (`docs/adr/`)
- [!] Development hardware chosen. Blocked: the owner must choose the models of the two qualification references (`docs/bench.md` §2)
- [x] Kernel features confirmed: cgroup v2 delegation, Landlock ABI, seccomp (`docs/reports/kernel-features.md`)
- [x] Features needed from `igs-iec104` inventoried, gaps opened. Inventory in `docs/reports/igs-iec104-gaps.md`. Issues opened on `aklira/igs-iec104`: #2 (G-1), #3 (G-2), #4 (G-3), #5 (G-4). G-5 concerns the bench, not the library, and is not opened there
- [~] Bench set up: gridloop MVP environment, simulated RTUs, demo SCADA, Kafka, reachable from CI. Started on the development host: 17 containers up, Fledge healthy, SCADA connected to Fledge. S3 data path not active: S3 is deployed by the IBTNM engine, not by `compose up`. Not yet checked from CI
- [ ] Gridloop changes agreed and ordered with the IBTNM side (list in `IMPLEMENTATION.md` §6.2)

Exit: ADRs merged (done), bench reachable (not done), kernel report written (done). **P0 not exited.**

### P1 · Foundations (4 weeks)

- [ ] Cargo workspace, CI with all quality gates, x86_64 and aarch64 builds
- [ ] `powermug-pivot`: `powermug.pivot.v1` Protobuf, compact `PointEvent`, quality model
- [ ] `powermug-contract`: adapter contract, handshake, credits, bounded messages, `SO_PEERCRED` check
- [ ] `powermug-revision`: strict YAML/JSON loader, RFC 8785, content ids, signed-plan verification, validation stage 1
- [ ] Cross-language RFC 8785 corpus against gridloop (PM-ACC-002)
- [ ] Signed-plan fixtures from gridloop's signing service (PM-ACC-003)
- [ ] `powermug` CLI: `revision hash`, `revision validate` (stage 1)
- [ ] `powermug-sandbox`: spawner, namespaces, uid allocation, Landlock, seccomp, cgroup v2
- [ ] Supervisor: lifecycle, restart budget, backoff, quarantine
- [ ] `powermug-adapter-sdk`, fake core, fake adapter with four fault modes

Exit: PM-ACC-002 and PM-ACC-003 pass; the four fault modes are confined.

### P2 · IEC 104 telemetry loop (5 weeks) · milestone M1

- [ ] `iec104-client` and `iec104-server` adapters on `igs-iec104`
- [ ] Partition lanes, ingestion, telemetry writer with group commit, process image
- [ ] Telemetry log, snapshots, image rebuild at start
- [ ] Validation stages 2 to 4 for the P2 subset; prepare, activate, rollback
- [ ] Target API v1 (`/capabilities`, `/state`, `/revisions/*`, `/indicators`), mTLS
- [ ] Containerfile, Quadlet template, bench profile
- [ ] gridloop: `powermug.py` driver for S3, `powermug@1` in `capabilities.json`
- [ ] `powermug-bench` and the first p99 gate in CI

Exit: ACC-011, ACC-013, ACC-014, ACC-018 pass; MVP S1, S15, S16 pass with Powermug.

### P3 · Command plane (4 weeks)

- [ ] FSM with transition table, critical store, dedicated writer, recovery at start
- [ ] Authorization per issuer, point, operation, value and mode; maintenance mode
- [ ] IEC 104 single, double, set-point commands, direct and select-before-operate
- [ ] Dispatch with partition epochs
- [ ] Hash-chained audit records

Exit: ACC-002, ACC-003, ACC-008 pass in reduced campaigns; MVP S17, S18, S19 pass.

### P4 · Store-and-forward, Kafka, MQTT 5, indicators (4 weeks) · milestone M2

- [ ] `kafka-producer` adapter on `rdkafka`, as a separate executable (ADR-18)
- [ ] `mqtt5-client` adapter (crate to confirm under NFR-MNT-003)
- [ ] Delivery: live ring, cursors, replay, explicit start point, image buffers
- [ ] Classes, quotas, saturation thresholds, separate volumes
- [ ] Indicators `indicators.v1`, OpenMetrics with bounded series
- [ ] Local actions and `/actions/execute`
- [ ] MVP bench running Powermug in place of FledgePower

Exit: ACC-001, ACC-007, ACC-015, ACC-023, PM-ACC-010, PM-ACC-011 pass.

### P5 · Processing N1 and N2, cell scope, catalogue integration (4 weeks) · milestone M3

- [ ] `powermug-process`: N1 catalogue, chain compiler, ingress and route chains, engineering parameters, overrides
- [ ] Cycle supervision (`cycle_check`) with timing wheel
- [ ] CEL engine on `cel-interpreter` (ADR-21), domain functions, static cost bound
- [ ] Graph checks and validation codes `PMV-*`
- [ ] CLI: `revision diff`, `preview`, `build`, `sign`; `catalog export`
- [ ] Cell-scope revisions (ADR-19)
- [!] Cell scope with eleven stations on one listener. Blocked until `igs-iec104` issue #2 (multi-station listener) is implemented; the decision (option a) is taken
- [ ] gridloop: catalogue rename `igs-gateway@1` → `powermug@1`, `target-revision` schema 1.2.0

Exit: ACC-017, ACC-019, ACC-024, PM-ACC-001, 005, 006, 007, 012, 013 pass.

### P6 · Processing adapters, guards, ownership transfer, shadow mode (4 weeks)

- [ ] N3 processor role, `stream` and `snapshot` modes, derived and own points
- [ ] Reference state estimator prototype
- [ ] Deny-only CEL guards on commands (ADR-14, not yet validated)
- [ ] Cession and takeover with signed proofs and epochs
- [ ] Shadow mode
- [ ] `quarantine_processor` local action
- [ ] Migration of S3 from FledgePower to Powermug on the bench

Exit: ACC-025, ACC-026, PM-ACC-008, PM-ACC-009 pass.

### P7 · P1 protocols, ARM64, fidelity, PKI (6 weeks) · milestone M4

- [ ] `iec61850-mms-client` adapter
- [ ] `modbus` adapter (crate to confirm)
- [!] `opcua-client` and `opcua-server`. Blocked: `igs-opcua` does not exist yet (ADR-20); moves to P9 if not ready
- [ ] Fidelity matrices and round-trip corpus per adapter kind
- [ ] ARM64 test suite and multi-arch images
- [ ] EST enrolment and trust-store rotation
- [ ] 50-station bench

Exit: ACC-010, ACC-022, PM-ACC-004 pass on every delivered adapter kind.

### P8 · High availability and concentrator scale (6 weeks) · milestone M5

- [ ] OCF resource agent `powermug-cell`, role API, promotion conditions
- [ ] DRBD for the critical volume, single-copy continuation
- [ ] Concentrator profile, several cells per host
- [ ] Aggregation bus decided (ADR-09)
- [ ] 300-station bench, correlated peak scenario

Exit: ACC-004, ACC-005, ACC-016, ACC-020, ACC-021 pass.

### P9 · Industrialisation (6 weeks, then continuous)

- [ ] Signed releases, CycloneDX SBOM, provenance, CVE policy
- [ ] Continuous fuzzing, HIL benches, 30-day endurance
- [ ] Yocto image with RAUC or SWUpdate A/B (ADR-07)
- [ ] OpenTelemetry traces
- [ ] TASE.2, and OPC UA if moved from P7
- [ ] IEC 62443, IEC 62351 and ANSSI matrix

Exit: ACC-009, ACC-012 pass; first major-release campaign complete.

## Decisions waiting for the owner

| # | Decision | Blocks | Where |
|---|---|---|---|
| 1 | ~~G-1: option a chosen~~ Done. Implementation is in `igs-iec104` issue #2 | P5 | `aklira/igs-iec104#2` |
| 2 | Qualification hardware, two references | P2 measurements, P7 | `docs/bench.md` §2 |
| 3 | ~~Open gaps in `igs-iec104`~~ Done: issues #2 to #5 | P2 to P8 | `aklira/igs-iec104` |
| 4 | ~~Uid range~~ Done: §6.4 and PM-ADP-007 now use 10000–10999 inside the cell's user namespace. Kernel report and ADR-17 updated | P1 | `specifications/SPECIFICATIONS.md` §6.4 |
| 5 | Validate ADR-14, ADR-15, ADR-21 (currently Proposed) | P5, P6 | `docs/adr/` |

## Journal

- **2026-11-19:** Specifications written and committed (`862d034`). Phase P0 started: ADRs, kernel report, `igs-iec104` inventory, bench description and P0 status committed (`93d5aed`). `.claude/` ignored (`09fb7cc`), pushed to origin.
- **2026-11-19:** Gridloop bench started on the development host; S3 data path not active until the IBTNM engine deploys S3. G-1 option a chosen; issues #2 to #5 opened on `aklira/igs-iec104`.
- **2026-11-19:** Findings from P0 to carry into the specification: the uid range in §6.4 does not match the development host; `igs-iec104` serves one station per listener, which conflicts with the `CELL_DC1` model.
