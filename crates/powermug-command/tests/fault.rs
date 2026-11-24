//! Fault injection at process level (SPECIFICATIONS 9.7, PM-CMD-002): a child process writes the
//! journal of one command and exits at a chosen point, as a crash would; the parent reopens the
//! journal and applies the rules of a restart. The exit skips every destructor, so the only
//! durability left is what the journal committed (synchronous FULL).

use std::path::Path;
use std::process::Command;

use powermug_command::{Action, CommandJournal, Granularity, NewCommand, RecoveryContext, State};

const T0: i64 = 1_000;
const FAR: i64 = 10_000_000_000;
const CHILD_TEST: &str = "crash_child";
const CRASH_AT: &str = "POWERMUG_FAULT_CRASH_AT";
const JOURNAL_DIR: &str = "POWERMUG_FAULT_DIR";
const CRASH_EXIT: i32 = 17;

/// The points of injection, in the order the child reaches them.
/// 0: before the submission; 1: before `VALIDATED`; 2: before `DURABLY_ACCEPTED`;
/// 3: before `READY`; 4: before the intent of the execution; 5: after the intent, before its
/// result (the frame may have left); 6: after the result.
const POINTS: usize = 7;

fn command() -> NewCommand {
    NewCommand {
        command_id: "c1".into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::Client,
        point: "S3/TC_101".into(),
        operation: "direct_execute".into(),
        value: "true".into(),
        expires_ns: FAR,
        expected_revision: "r1".into(),
        owner_epoch: 1,
        ha_term: 1,
    }
}

fn context() -> RecoveryContext {
    RecoveryContext {
        now_ns: T0 + 10,
        active_revision: "r1".into(),
        owner_epoch: 1,
        ha_active: true,
    }
}

/// The child: runs the steps, and exits at `crash_at` before the step of that index.
#[test]
fn crash_child() {
    let (Ok(dir), Ok(point)) = (std::env::var(JOURNAL_DIR), std::env::var(CRASH_AT)) else {
        return;
    };
    let crash_at: usize = point.parse().expect("a point index");
    let mut step = 0;
    let reach = |step: &mut usize| {
        if *step == crash_at {
            std::process::exit(CRASH_EXIT);
        }
        *step += 1;
    };

    let mut journal = CommandJournal::open(Path::new(&dir).join("critical.db")).expect("opens");
    reach(&mut step);
    journal.submit(&command(), T0).expect("submits");
    reach(&mut step);
    journal
        .move_to("c1", State::Validated, T0, "")
        .expect("validated");
    reach(&mut step);
    journal
        .move_to("c1", State::DurablyAccepted, T0, "")
        .expect("accepted");
    reach(&mut step);
    journal.move_to("c1", State::Ready, T0, "").expect("ready");
    reach(&mut step);
    journal
        .record_intent("c1", State::ExecuteSent, T0)
        .expect("intent");
    reach(&mut step);
    journal
        .record_result("c1", State::ExecuteSent, T0, "sent")
        .expect("result");
    reach(&mut step);
}

/// Runs the child to `crash_at` in `dir`, and returns whether it exited at the injection point.
fn crash_at(dir: &Path, crash_at: usize) -> bool {
    let status = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", CHILD_TEST, "--test-threads=1", "--nocapture"])
        .env(JOURNAL_DIR, dir)
        .env(CRASH_AT, crash_at.to_string())
        .status()
        .expect("runs the child");
    status.code() == Some(CRASH_EXIT)
}

/// After a crash at each point, the restart decides what the journal says it must.
#[test]
fn a_crash_at_each_point_is_recovered_by_the_rules_of_a_restart() {
    let expected: [Option<(State, Action)>; POINTS] = [
        // Nothing was written.
        None,
        // Received, or validated: interrupted before durable acceptance, so failed.
        Some((State::Failed, Action::Nothing)),
        Some((State::Failed, Action::Nothing)),
        // Durably accepted, or ready, and never sent: emitted now (the gates pass).
        Some((State::DurablyAccepted, Action::Emit)),
        Some((State::Ready, Action::Emit)),
        // An intent without its result: the frame may have left, so in doubt.
        Some((State::InDoubt, Action::Nothing)),
        // The result was written: the execution was sent without confirmation.
        Some((State::InDoubt, Action::Nothing)),
    ];
    assert_eq!(expected.len(), POINTS);

    for (point, expectation) in expected.iter().enumerate() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert!(
            crash_at(dir.path(), point),
            "the child exits at point {point}"
        );

        let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("reopens");
        let recovered = journal.recover(&context()).expect("recovers");
        match expectation {
            None => {
                assert!(recovered.is_empty(), "point {point}: nothing to recover");
                assert_eq!(journal.state("c1").expect("reads"), None);
            }
            Some((state, action)) => {
                assert_eq!(
                    journal.state("c1").expect("reads"),
                    Some(*state),
                    "point {point}"
                );
                if let [outcome] = recovered.as_slice() {
                    assert_eq!(outcome.action, *action, "point {point}");
                } else {
                    panic!("point {point}: expected one recovery, got {recovered:?}");
                }
            }
        }

        // A command that may have left is in doubt, and no later restart emits it again
        // (PM-CMD-002). A command never sent stays emittable: nothing was emitted in between.
        let again = journal.recover(&context()).expect("recovers again");
        if matches!(expectation, Some((State::InDoubt, _))) {
            for outcome in again {
                assert_ne!(outcome.action, Action::Emit, "point {point}: no replay");
            }
        }
        assert!(
            journal.verify_audit().is_ok(),
            "point {point}: the chain verifies"
        );
    }
}
