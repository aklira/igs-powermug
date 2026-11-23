# Phase P2 — Exit grid (milestone M1)

- **Date:** 2026-11-20
- **Plan:** [IMPLEMENTATION.md](../../specifications/IMPLEMENTATION.md) §4, P2
- **Criteria:** [SPECIFICATION_PASSERELLE_TELECONDUITE_v0.5.md](../../../gridloop/specifications/SPECIFICATION_PASSERELLE_TELECONDUITE_v0.5.md) (the `ACC-*` criteria, in gridloop); [SPECIFICATIONS.md](../../specifications/SPECIFICATIONS.md) §20 (the `PM-ACC-*` criteria, in this repository)
- **Status:** Not exited. Each criterion below is marked by what is verified here, and what is not.

The phase exit names four `ACC-*` criteria and three MVP scenarios. The MVP scenarios need the gridloop MVP bench (IBTNM engine, SCADA, simulated RTUs, Kafka), which is not in this repository and is not up yet (decision 2 and the bench in [bench.md](../bench.md) §4).

## Exit criteria

| Criterion | Requirement (summary) | Evidence in the repository | Verified here | Missing |
|---|---|---|---|---|
| ACC-011 | A crash or an OOM kill of an adapter does not bring down the core or another adapter | `crates/powermug/tests/fault_isolation.rs` kills faulty adapters (abort, panic, OOM, seccomp, forbidden read) next to healthy ones, each in its own cgroup and sandbox; `crates/powermug/tests/adapter_launch.rs` SIGKILLs a real IEC 104 adapter and checks the core and the lane survive | Yes, the fault test runs under `systemd-run --user --scope -p Delegate=yes` (skipped otherwise) | SIGSEGV is not injected. Degradation under 2 s (PM-ACC-004) is not measured. The bench run with real equipment is not done |
| ACC-013 | Loss of a South session degrades every point attached to it in under 2 s, on every destination | Lane: one `CommunicationState` and one invalid quality per point (PM-TEL-007, lane tests). End to end: `station_loss_reaches_the_lane_as_a_lost_session`; `a_station_that_stops_degrades_its_points_in_under_two_seconds` (`adapter_launch.rs`) measures the delay with a real adapter and station | Partly: a clean stop of the station degrades the points in 6.7 ms on the development host (under 2 s). A silent network loss is not measured: it depends on TCP keepalive and the IEC 104 timers | "Every destination": only the North subscriber stream exists; there are no flux or image destinations yet. A silent network loss is not measured |
| ACC-014 | After a SCADA reconnection, a general interrogation returns the current state of every point, with degraded quality for points not yet resynchronised | `an_interrogation_after_a_restart_answers_every_point_as_not_yet_resynchronised` (`crates/powermug-core/tests/general_interrogation.rs`): after a restart from a snapshot both points are answered invalid; after a fresh value for one point, that point is good and the other stays invalid. `controlling_station_interrogates_the_core_image_and_gets_changes` (`crates/powermug-adapter-iec104/tests/serve.rs`) checks the GI over IEC 104 | Yes at the core level, with the answer built from the image. Over IEC 104 the GI answer is tested with values, not with a degraded point | A test over the socket: restart a cell with a snapshot, connect a controlling station, check the quality of each point in its GI answer |
| ACC-018 | The same revision applied twice causes no change and no interruption | `generation.rs` tests: `prepare` is idempotent per revision id; activation is idempotent; `plans.rs` (management API): an already active revision answers ALREADY_ACTIVE | Yes, at the store and the API level | "No interruption" is not measured on a running cell |
| MVP S1 | Commissioning, monitoring part, with Powermug as target | Not in this repository | No | The MVP bench |
| MVP S15 | Stopping the engine leaves acquisition, commands and the active configuration intact | The cell does not depend on the engine (ENG-017); not exercised | No | The MVP bench |
| MVP S16 | SCADA connection failover | Adapter failover between stations of a group: `station.rs` (failover test). SCADA failover is not exercised | Partly | The MVP bench, with the SCADA |

## Other P2 measurements

| Item | Status |
|---|---|
| NFR-PERF-002 and NFR-PERF-004, first measurements | The bench (`powermug-bench`) runs on the development host only: about 18,400 events/s, p99 6 ms for 100-event batches. Not a qualification result (decision 2) |
| PM-ACC-013, eleven partitions behind one redundancy group | The partition model and the adapter support one group per instance; eleven partitions in one cell are not run |
| Packaging in the image | Verified: the cell starts with real certificates, the management API answers over mTLS and refuses a connection without a client certificate. South instances inside the container are refused: the container's cgroup filesystem is read-only (see PROGRESS.md) |

## Blockers, by owner

| Blocker | Owner | Needed for |
|---|---|---|
| Qualification hardware and the MVP bench | Owner (decision 2) | ACC-013 timing, S1, S15, S16, the performance measurements |
| Decision 9: uid of South instances (`CAP_CHOWN` of the spawner) | Owner | PM-ADP-007, and the production profile |
| Cgroup configuration of the Quadlet unit | Owner, with the deployment profile (§18.3) | South instances inside the container |
| `target.revision.rest` in the capability list | Gridloop | The capability catalogue |

## Conclusion

Verified here: ACC-011 (with the delegated-scope run), ACC-018, the core part of ACC-014, and ACC-013 for a clean station stop (6.7 ms on the development host). Not verified here: a silent network loss, the destinations of ACC-013, the GI over the socket with a degraded point, and all three MVP scenarios. Closing P2 needs the bench and two owner decisions; more code in P2 does not close it.
