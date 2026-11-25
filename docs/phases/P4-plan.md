# Phase P4 — Plan (store-and-forward, Kafka, MQTT 5, indicators)

- **Status:** In progress. Slice 1 (live ring and replay) is done; the other slices wait for the decisions in section 4. P3 is open until the bench.
- **Date:** 2026-11-25
- **Plan:** [IMPLEMENTATION.md](../../specifications/IMPLEMENTATION.md) §4 (P4) and the P4 deliverables
- **Criteria:** `ACC-001`, `ACC-007`, `ACC-015`, `ACC-023` (gridloop), `PM-ACC-010`, `PM-ACC-011` (this repository), and the MVP scenarios S1, S6, S7, S14 to S19 (`PM-ACC-011`)
- **Milestone:** M2, the MVP target swap: the MVP scenarios that involve the target pass with Powermug in place of FledgePower

## 1. Scope

From the checklist of P4 in `PROGRESS.md`:

1. `kafka-producer` adapter on `rdkafka`, as a separate executable (ADR-18); delivery acknowledgements mapped to cursors; mTLS or SASL from secret references.
2. `mqtt5-client` adapter; the crate is confirmed under NFR-MNT-003 at the start of the phase.
3. Delivery: a live ring, cursors, replay from the journal (marked and rate-capped), explicit start point (PM-TEL-004), image-destination event buffers.
4. Classes and saturation: quotas per class, four thresholds, shedding of class C, separate volumes and a quota check at start (PM-TEL-006).
5. Indicators: every `indicators.v1` indicator that Powermug can measure, OpenMetrics with bounded series.
6. Local actions: `restart_adapter`, `shed_class_c`, `general_interrogation`, `switch_redundant_connection`, `alert`, `purge_tmp`, and the target API extension `/actions/execute`.
7. The MVP bench running Powermug in place of FledgePower.

Out of P4: the North IEC 104 entry for commands (S18, gap G-7), the processing adapters (P5 and P6), and the qualification hardware (decision 2).

## 2. What already exists

| Area | In the repository | Gap |
|---|---|---|
| Cursors and replay | `powermug-store/src/cursors.rs`: declared cursors, acknowledgements, replay with a limit, a start policy, retention per class with an audit, dropped counters | The adapters that acknowledge (no destination adapter yet); the live ring; the rate cap of the catch-up (FUN-SAF-005) |
| Event classes | `powermug-store/src/log.rs`: `A`, `B`, `C`, `Critical`; class C not journaled | Quotas per class, the four thresholds, shedding, separate volumes |
| Indicators | `powermug-core/src/indicators.rs`: two indicators (`telemetry_last_seen_seconds`, `iec104_connection_up`), the others are not estimated | The rest of the catalogue needs sources (cell health, host statistics, destinations, commands); OpenMetrics |
| Adapters contract | `powermug-contract`: the batch sink, the acknowledgements (`AckBatch`), the North subscriber stream | The destination adapters (Kafka, MQTT 5) as separate executables, each supervised like a South instance |
| Management API | `powermug-mgmt`: `GET /state`, `GET /indicators`, revisions, commands | `/actions/execute` and `/actions/rollback`, `/revisions/remove` (target API v1.1) |

## 3. Slices, in order

Each slice is self-contained, verified by tests, recorded in `PROGRESS.md`, and stops for the commit instruction.

1. **Live ring and replay (no external dependency).** A bounded in-memory ring of the last committed events per destination, with cursors over the journal when a consumer is behind; the replay is marked `replay=true` and rate-capped (FUN-SAF-003, FUN-SAF-005). Tests: a consumer behind the ring replays from the journal without losing an acknowledged `DURABLE` event (ACC-001 in miniature), and the replay rate is bounded.
2. **Classes, quotas and thresholds.** Quotas per class and the four thresholds (FUN-SAF-008): saturation leads to a deterministic shedding of class C, never to unbounded memory. Tests: a telemetry flood at 120 % of nominal keeps the command path within its budget (PM-ACC-010 in miniature); a full quota does not produce an out-of-memory or a loss of the critical journal (ACC-007 in miniature).
3. **Separate volumes and quota check at start (PM-TEL-006).** The critical store stays on its own volume; the cell refuses to start when a telemetry volume is not separate or over quota.
4. **Indicators.** The `indicators.v1` indicators Powermug can measure, exposed as OpenMetrics with bounded series (the series do not depend on the point count, PM-IND-001). Tests: the number of series is constant for 10 and 10,000 points; each indicator carries its measurement time (ACC-023 in miniature).
5. **Local actions and `/actions/execute`.** The six actions, each audited with its trigger; the revision selects the enabled actions and their budgets. The action never changes ACLs, command policy, partition ownership, or configuration (§6.2). Tests: an action outside the catalogue is refused; an audited action carries its trigger.
6. **Kafka adapter.** A separate executable on `rdkafka`; delivery acknowledgements mapped to cursors; mTLS or SASL from secret references. Tests: a Kafka broker in the bench environment (the containers of the development host run Kafka), a long North disconnection with no more than the bounded buffer delivered (ACC-015), duplicates identifiable through `event_id` (FUN-SAF-003).
7. **MQTT 5 adapter.** A separate executable on the crate confirmed under NFR-MNT-003. Tests: as for Kafka, on an MQTT broker.
8. **MVP bench.** Powermug in place of FledgePower on the MVP environment: 11 simulated RTUs, the demo SCADA with its redundancy group, Kafka and the historian. The reduced campaigns: 1,000 random power cuts (ACC-001), a full telemetry quota (ACC-007), a long North disconnection (ACC-015), the scenarios S1, S6, S7, S14 to S19.

## 4. Decisions needed from the owner

| # | Decision | Recommendation | Blocks |
|---|---|---|---|
| D1 | Accept `rdkafka` (librdkafka, C, BSD-2-Clause, native build) under NFR-MNT-003 and NFR-MNT-004: owner, licence, CVE policy, and the packaging of the native library in the image | Accept, with the CVE watch and the native build in the image | Slice 6 |
| D2 | The MQTT 5 crate | To confirm: `rumqttc` (Apache-2.0 or MIT) after a licence and CVE check, as NFR-MNT-003 requires | Slice 7 |
| D3 | The target API extension (`/actions/execute`, `/actions/rollback`, `/revisions/remove`, v1.1) is a change of gridloop's schema | Order it with the gridloop side, as the P0 item "Gridloop changes agreed and ordered" asks; no change to the other repository without approval | Slice 5 |
| D4 | Quotas and the four thresholds (values of the H parameters) | Proposed defaults, to be confirmed on the bench | Slice 2 |
| D5 | The MVP bench: its environment (the development host runs the containers; S3 is not active there) and the qualification hardware (decision 2) | The bench environment first; the hardware later for the performance numbers | Slice 8, and the exit |

## 5. Exit

P4 exits when ACC-001, ACC-007, ACC-015, ACC-023, PM-ACC-010 and PM-ACC-011 pass. The exit needs the MVP bench, so it cannot be closed on the development host alone. The open items of P3 (bench, G-6, G-7, site rules) stay open and are carried over.

## 6. Risks

- The exit depends on the bench (decision 2 and D5): the slices can be verified here, the exit cannot.
- The native dependency of Kafka (D1) affects the image and the licence review.
- The gridloop schema (D3) can delay slice 5 whatever Powermug does.
- The first slice is the only one that needs no decision: it is the proposed start once this plan is accepted.
