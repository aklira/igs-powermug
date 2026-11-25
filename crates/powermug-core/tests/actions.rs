//! Local actions (SPECIFICATIONS 11.4, PM-ACT-001 to PM-ACT-003): the closed catalogue refuses
//! unknown names, each action is bounded by its budget, an `action_id` runs once, and every
//! request is audited with its trigger.

use std::collections::BTreeMap;

use powermug_core::actions::{
    Action, ActionHost, AuditSink, Budget, Executor, Outcome, Record, Request,
};

/// Keeps the records it is given; refuses all of them when `fail` is set.
#[derive(Default)]
struct Sink {
    records: Vec<(String, Record)>,
    fail: bool,
}

impl AuditSink for Sink {
    fn record(&mut self, kind: &str, record: &Record) -> Result<(), String> {
        if self.fail {
            return Err("the chain is not writable".into());
        }
        self.records.push((kind.to_owned(), record.clone()));
        Ok(())
    }
}

/// Records what it was asked to run.
#[derive(Default)]
struct Host {
    runs: Vec<(Action, String)>,
    fail: bool,
}

impl ActionHost for Host {
    fn run(&mut self, action: Action, target: &str) -> Result<(), String> {
        self.runs.push((action, target.to_owned()));
        if self.fail {
            Err("the adapter is not reachable".into())
        } else {
            Ok(())
        }
    }
}

fn executor(max: usize) -> Executor {
    let mut budgets = BTreeMap::new();
    budgets.insert(
        Action::RestartAdapter,
        Budget {
            max,
            window_ns: 60 * 1_000_000_000,
        },
    );
    budgets.insert(
        Action::GeneralInterrogation,
        Budget {
            max: 10,
            window_ns: 60 * 1_000_000_000,
        },
    );
    budgets.insert(
        Action::Alert,
        Budget {
            max: 100,
            window_ns: 60 * 1_000_000_000,
        },
    );
    Executor::new(budgets)
}

fn request(id: &str, action: &str) -> Request {
    Request {
        action_id: id.into(),
        action: action.into(),
        target: "s3-ca1".into(),
        trigger: "iec104_connection_up".into(),
    }
}

#[test]
fn a_name_outside_the_catalogue_is_refused_without_running_anything() {
    let mut host = Host::default();
    let mut sink = Sink::default();
    let mut executor = executor(3);
    let outcome = executor.execute(&mut host, &mut sink, &request("a1", "change_acl"), 1);
    assert!(matches!(outcome, Outcome::Refused(_)));
    assert_eq!(host.runs.len(), 0);
    assert_eq!(executor.audit().len(), 1, "the refusal is audited");
}

#[test]
fn the_catalogue_names_are_the_specified_ones_and_nothing_touches_the_control_plane() {
    for name in [
        "restart_adapter",
        "shed_class_c",
        "general_interrogation",
        "switch_redundant_connection",
        "alert",
        "purge_tmp",
        "failover_cell",
        "quarantine_processor",
    ] {
        let action = Action::from_name(name).expect("in the catalogue");
        assert_eq!(action.name(), name);
    }
    for forbidden in [
        "change_acl",
        "set_mode",
        "takeover",
        "resolve_in_doubt",
        "activate",
    ] {
        assert!(
            Action::from_name(forbidden).is_none(),
            "{forbidden} is not an action"
        );
    }
}

#[test]
fn an_action_not_run_by_this_cell_yet_is_refused() {
    let mut host = Host::default();
    let mut sink = Sink::default();
    let mut executor = executor(3);
    let outcome = executor.execute(&mut host, &mut sink, &request("a1", "purge_tmp"), 1);
    assert!(matches!(outcome, Outcome::Refused(_)));
    assert_eq!(host.runs.len(), 0);
}

#[test]
fn an_action_id_runs_once_however_often_it_is_sent() {
    let mut host = Host::default();
    let mut sink = Sink::default();
    let mut executor = executor(3);
    assert_eq!(
        executor.execute(&mut host, &mut sink, &request("a1", "alert"), 1),
        Outcome::Done
    );
    assert_eq!(
        executor.execute(&mut host, &mut sink, &request("a1", "alert"), 2),
        Outcome::Duplicate
    );
    assert_eq!(host.runs.len(), 1);
}

#[test]
fn an_action_stops_at_its_budget_and_runs_again_after_the_window() {
    let mut host = Host::default();
    let mut sink = Sink::default();
    let mut executor = executor(2);
    let minute = 60 * 1_000_000_000_i64;
    assert_eq!(
        executor.execute(&mut host, &mut sink, &request("r1", "restart_adapter"), 0),
        Outcome::Done
    );
    assert_eq!(
        executor.execute(&mut host, &mut sink, &request("r2", "restart_adapter"), 1),
        Outcome::Done
    );
    assert!(matches!(
        executor.execute(&mut host, &mut sink, &request("r3", "restart_adapter"), 2),
        Outcome::Refused(_)
    ));
    assert_eq!(host.runs.len(), 2, "the third is not run");
    assert_eq!(
        executor.execute(
            &mut host,
            &mut sink,
            &request("r4", "restart_adapter"),
            minute + 5
        ),
        Outcome::Done,
        "the window has passed"
    );
}

#[test]
fn a_failed_run_is_refused_with_its_reason_and_audited_with_its_trigger() {
    let mut host = Host {
        fail: true,
        ..Host::default()
    };
    let mut sink = Sink::default();
    let mut executor = executor(3);
    let outcome = executor.execute(
        &mut host,
        &mut sink,
        &request("g1", "general_interrogation"),
        7,
    );
    assert_eq!(
        outcome,
        Outcome::Refused("the adapter is not reachable".into())
    );
    let record = &executor.audit()[0];
    assert_eq!(record.action, "general_interrogation");
    assert_eq!(record.trigger, "iec104_connection_up");
    assert_eq!(record.at_ns, 7);
}

#[test]
fn an_action_without_a_budget_is_not_run() {
    let mut host = Host::default();
    let mut sink = Sink::default();
    let mut executor = executor(3);
    let outcome = executor.execute(&mut host, &mut sink, &request("s1", "shed_class_c"), 1);
    assert!(matches!(outcome, Outcome::Refused(_)));
    assert_eq!(host.runs.len(), 0, "no bound, no run (PM8)");
}

#[test]
fn a_request_that_is_not_audited_runs_nothing_and_can_be_sent_again() {
    let mut host = Host::default();
    let mut failing = Sink {
        fail: true,
        ..Sink::default()
    };
    let mut executor = executor(3);
    let outcome = executor.execute(&mut host, &mut failing, &request("n1", "alert"), 1);
    assert!(matches!(outcome, Outcome::Refused(_)));
    assert_eq!(host.runs.len(), 0, "nothing runs without its record");

    let mut sink = Sink::default();
    assert_eq!(
        executor.execute(&mut host, &mut sink, &request("n1", "alert"), 2),
        Outcome::Done,
        "the same request runs once it can be recorded"
    );
}

#[test]
fn every_request_is_recorded_as_received_then_with_its_outcome() {
    let mut host = Host::default();
    let mut sink = Sink::default();
    let mut executor = executor(3);
    executor.execute(&mut host, &mut sink, &request("e1", "alert"), 5);
    let kinds: Vec<&str> = sink.records.iter().map(|(kind, _)| kind.as_str()).collect();
    assert_eq!(kinds, ["action.requested", "action.finished"]);
    assert_eq!(sink.records[0].1.outcome, None);
    assert_eq!(sink.records[1].1.outcome, Some(Outcome::Done));
}
