# Phase P3 — Exit grid (command plane)

- **Date:** 2026-11-25
- **Plan:** [IMPLEMENTATION.md](../../specifications/IMPLEMENTATION.md) §4, P3
- **Criteria:** [SPECIFICATION_PASSERELLE_TELECONDUITE_v0.5.md](../../../gridloop/specifications/SPECIFICATION_PASSERELLE_TELECONDUITE_v0.5.md) (`ACC-*`, in gridloop); the MVP scenarios are in `Plan d'implémentation du MVP IBTNM — chaîne en six étapes.md` (gridloop); [SPECIFICATIONS.md](../../specifications/SPECIFICATIONS.md) §9 (command plane)
- **Status:** **Not exited.** P3 stays open until the bench (owner's decision, 2026-11-25). Each criterion is marked by what is verified here on the development host, and what remains.

The phase exit names three `ACC-*` criteria, three MVP scenarios, and the command latency budget. The bench is not up (decision 2, [bench.md](../bench.md)). What is verified here runs on loopback, on one host, with a real `igs-iec104` station in a separate process where the test needs one.

## Exit criteria

| Criterion | Requirement (summary) | Evidence in the repository | Verified here | Missing |
|---|---|---|---|---|
| ACC-002 | 10 000 submissions of one `command_id` cause at most one `EXECUTE_SENT` transition | Idempotent submission in the journal (a known id returns its state and writes nothing, PM-CMD-003). Dispatcher: `a_known_command_id_is_answered_with_its_state_and_not_dispatched_again`, and the reduced campaign `ten_thousand_submissions_of_one_command_are_dispatched_once` (`powermug-cell/tests/commands.rs`) | Yes, at the dispatcher, reduced to 10 000 submissions of one id on loopback | The campaign on the bench through the management API, with its measurements |
| ACC-003 | Any cut between emission and confirmation leads to `IN_DOUBT`, with no automatic replay | Process level: a child exits at each journal write (`powermug-command/tests/fault.rs`); a child exits after the dispatch is sent (`powermug-cell/tests/crash.rs`); a child exits during a critical change (`crash.rs`); a real adapter is killed while the station holds the execution, and the command is in doubt (`powermug/tests/adapter_crash.rs`). Stream cut: `a_command_in_flight_when_the_stream_ends_is_in_doubt_and_not_replayed` | Yes, with process stops and a SIGKILL of the adapter on the development host | A network cut (TCP) between emission and confirmation with real equipment. The reduced campaign of the criterion (number of cuts) |
| ACC-008 | A failure at each step of an activation leaves the old or the new revision complete, never a mix | The activation is one transaction of the critical store (PM-CFG-005): `powermug-store/tests/generation.rs`. Activation is idempotent (`plans.rs`). A critical change blocks the commands, and is resumed on success or abort (`critical_change.rs`) | Partly: the store switch is atomic; the cell reads the active revision at open, so a crash after the switch restarts on the new one | A failure injected at each step of the sequence (prepare, switch, rules, revision, unblock). Not done |
| MVP S17 | TC and TVC from the demo SCADA to S3: activation and end-of-execution confirmations within 2 s; the effect visible in the TS and TM of S3 | Single, double, set point (float, normalized, scaled) and regulating-step commands, select before operate, answered by a real station: `powermug-adapter-iec104/tests/command.rs`, `setpoint.rs`, `step.rs`, `run_commands.rs`. End to end: `powermug/tests/command_path.rs` sends a selection and an execution over the management API (mutual TLS) to a station, through the cell and its adapter; the termination is in the journal within two seconds | Yes for the command path, on this host, in a delegated cgroup scope (`systemd-run --user --scope -p Delegate=yes -- <binary>`; skipped elsewhere). Not checked: the effect in TS and TM | The effect in the TS and TM of S3 (the lane is not read by the test). The bench. **A direct execution is refused by the library** (gap G-6): S17 passes by select before operate, or after the library is fixed |
| MVP S18 | A second IEC 104 master, absent from the model, tries a TC: refused by the cell, traced in its journal | Unauthorized issuer refused and journaled (`a_command_from_an_identity_without_a_grant_is_refused_and_never_dispatched`; `powermug-command/tests/authorization.rs`). The identity is the certificate's common name on the REST entry (`powermug-mgmt/tests/commands_api.rs`) | Partly: the refusal and the trace, through the REST entry | The IEC 104 North entry, from which a second master would send the command: not implemented. Only the REST identity is covered |
| MVP S19 | A cut of the active connection between the activation confirmation and the end of execution: the command is `IN_DOUBT`, never replayed | A confirmed execution not terminated is `IN_DOUBT` when the stream is cut, and at restart (`an_execution_confirmed_but_not_terminated_is_in_doubt_when_the_stream_is_cut`, `an_execution_confirmed_but_not_terminated_is_in_doubt_at_restart`); the adapter killed in flight (`adapter_crash.rs`) | Yes, for the cut of the stream and a restart, on the development host | A real cut of the active connection with equipment. The order R3 to check the state of the equipment is not in the repository |
| NFR-PERF-003 | Receiving then persisting a command: p99 at most 20 ms | The writer, on this host: p99 0.68 ms for a submission and a transition (500 pairs, `powermug-command/tests/writer.rs`) | Measured on the development host only; not a qualification result (decision 2) | The qualification hardware (decision 2) |

Since the grid was written: the S17 command path runs end to end (`command_path.rs`). A single node is the confirmed owner of its partitions at opening (owner's decision, 2026-11-25), so the commands are accepted from the start of the cell. Before, no production path took ownership.

## Blockers, by owner

| Blocker | Owner | Needed for |
|---|---|---|
| Qualification hardware and the MVP bench | Owner (decision 2) | ACC-002, ACC-003, ACC-008, S17 to S19, NFR-PERF-003 |
| Direct execution refused by `igs-iec104` (gap G-6) | Owner of `igs-iec104` (report drafted in `docs/reports/igs-iec104-report-direct-execute.md`, not sent) | S17 with direct commands |
| The IEC 104 North entry for commands | Powermug (P4 or later) | S18 with a second IEC 104 master |
| Site command rules (ACL of the real site) | Owner, with the partition revisions | Commands enabled on the bench |
| Location of the separate domain for the audit anchor | Operator | NFR-SEC-008 in production |

## Conclusion

P3 is not exited. The command plane is in place end to end on the development host: commands are authorized, journaled, dispatched through the adapter to a real station, and recovered after each kind of stop, with the in-doubt rule applied where the outcome is unknown. What remains is the bench (decision 2) and the three items above that need the owner or another phase. P3 stays open until the bench runs.
