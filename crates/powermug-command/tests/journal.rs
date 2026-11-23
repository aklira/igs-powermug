//! The critical journal of commands (SPECIFICATIONS 9.2, 9.7): idempotent submission, checked
//! transitions, intent before result, and the rules of a restart ([GW] 11.5).

use powermug_command::journal::Phase;
use powermug_command::{
    Action, CommandError, CommandJournal, NewCommand, RecoveryContext, State, Submission,
};

const T0: i64 = 1_000;
const FAR: i64 = 10_000_000_000;

fn command(id: &str, expires: i64) -> NewCommand {
    NewCommand {
        command_id: id.into(),
        identity: "scada-demo".into(),
        point: "S3/TC_101".into(),
        operation: "direct_execute".into(),
        value: "true".into(),
        expires_ns: expires,
        expected_revision: "r1".into(),
        owner_epoch: 3,
        ha_term: 1,
    }
}

fn context(now: i64) -> RecoveryContext {
    RecoveryContext {
        now_ns: now,
        active_revision: "r1".into(),
        owner_epoch: 3,
        ha_active: true,
    }
}

fn journal(dir: &tempfile::TempDir) -> CommandJournal {
    CommandJournal::open(dir.path().join("critical.db")).expect("opens")
}

/// A command accepted and ready to be emitted.
fn ready(journal: &mut CommandJournal, id: &str, expires: i64) {
    journal.submit(&command(id, expires), T0).expect("submits");
    journal
        .move_to(id, State::Validated, T0, "")
        .expect("validated");
    journal
        .move_to(id, State::DurablyAccepted, T0, "")
        .expect("accepted");
    journal.move_to(id, State::Ready, T0, "").expect("ready");
}

#[test]
fn a_known_command_id_returns_its_state_and_writes_nothing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    assert_eq!(
        journal.submit(&command("c1", FAR), T0).expect("submits"),
        Submission::Created
    );
    journal
        .move_to("c1", State::Validated, T0, "")
        .expect("validated");

    let before = journal.history("c1").expect("history").len();
    assert_eq!(
        journal.submit(&command("c1", FAR), T0 + 5).expect("again"),
        Submission::Existing(State::Validated)
    );
    assert_eq!(journal.history("c1").expect("history").len(), before);
}

#[test]
fn a_transition_outside_the_table_is_refused_and_nothing_changes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    journal.submit(&command("c1", FAR), T0).expect("submits");

    assert!(matches!(
        journal.move_to("c1", State::VerifiedSuccess, T0, ""),
        Err(CommandError::IllegalTransition { .. })
    ));
    assert_eq!(journal.state("c1").expect("reads"), Some(State::Received));
    assert!(matches!(
        journal.move_to("missing", State::Validated, T0, ""),
        Err(CommandError::UnknownCommand(_))
    ));
}

#[test]
fn an_intent_is_written_before_the_result_and_closed_by_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    ready(&mut journal, "c1", FAR);

    journal
        .record_intent("c1", State::ExecuteSent, T0 + 1)
        .expect("intent");
    // The state does not change until the result: the command is still ready.
    assert_eq!(journal.state("c1").expect("reads"), Some(State::Ready));
    assert!(matches!(
        journal.move_to("c1", State::Expired, T0, ""),
        Err(CommandError::IntentOpen(_))
    ));

    journal
        .record_result("c1", State::ExecuteSent, T0 + 2, "sent")
        .expect("result");
    assert_eq!(
        journal.state("c1").expect("reads"),
        Some(State::ExecuteSent)
    );

    let history = journal.history("c1").expect("history");
    let intent = history
        .iter()
        .find(|t| t.phase == Phase::Intent)
        .expect("intent row");
    assert!(intent.closed);
    assert_eq!(intent.to, State::ExecuteSent);
}

#[test]
fn a_result_without_a_matching_intent_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    ready(&mut journal, "c1", FAR);
    assert!(matches!(
        journal.record_result("c1", State::ExecuteSent, T0, ""),
        Err(CommandError::NoOpenIntent(_, _))
    ));
}

#[test]
fn an_execution_sent_without_confirmation_is_in_doubt_and_never_emitted_again() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    ready(&mut journal, "c1", FAR);
    journal
        .record_intent("c1", State::ExecuteSent, T0)
        .expect("intent");
    journal
        .record_result("c1", State::ExecuteSent, T0, "sent")
        .expect("result");

    let outcome = journal.recover(&context(T0 + 10)).expect("recovers");
    assert_eq!(outcome.len(), 1);
    assert_eq!(outcome[0].to, State::InDoubt);
    assert_eq!(outcome[0].action, Action::Nothing);

    // A second restart changes nothing: IN_DOUBT is not replayed.
    let again = journal.recover(&context(T0 + 20)).expect("recovers again");
    assert!(again
        .iter()
        .all(|r| r.action == Action::Nothing && r.to == State::InDoubt));
    assert_eq!(journal.state("c1").expect("reads"), Some(State::InDoubt));
}

#[test]
fn an_intent_still_open_at_restart_makes_the_command_in_doubt() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    ready(&mut journal, "c1", FAR);
    // The process stopped after the intent, before the call's result was written.
    journal
        .record_intent("c1", State::ExecuteSent, T0)
        .expect("intent");

    let outcome = journal.recover(&context(T0 + 10)).expect("recovers");
    assert_eq!(outcome[0].to, State::InDoubt);
    assert_eq!(outcome[0].action, Action::Nothing);
}

#[test]
fn a_ready_command_is_emitted_only_when_its_gates_pass() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    ready(&mut journal, "ok", FAR);
    ready(&mut journal, "late", T0 + 5);
    ready(&mut journal, "moved", FAR);
    ready(&mut journal, "stale-epoch", FAR);

    let mut context = context(T0 + 100);
    let outcome = journal.recover(&context).expect("recovers");
    let action = |id: &str| {
        outcome
            .iter()
            .find(|r| r.command_id == id)
            .expect("listed")
            .action
    };
    assert_eq!(action("ok"), Action::Emit);
    assert_eq!(journal.state("late").expect("reads"), Some(State::Expired));
    assert_eq!(journal.state("ok").expect("reads"), Some(State::Ready));

    // The node is not the active HA node: the command is held, not failed.
    context.ha_active = false;
    let held = journal.recover(&context).expect("recovers");
    assert!(held
        .iter()
        .any(|r| r.command_id == "ok" && r.action == Action::Hold));
    assert_eq!(journal.state("ok").expect("reads"), Some(State::Ready));
}

#[test]
fn a_revision_or_epoch_change_fails_the_command_at_restart() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    ready(&mut journal, "revision", FAR);
    ready(&mut journal, "epoch", FAR);
    let mut moved = context(T0 + 1);
    moved.active_revision = "r2".into();
    journal.recover(&moved).expect("recovers");
    assert_eq!(
        journal.state("revision").expect("reads"),
        Some(State::Failed)
    );

    let mut takeover = context(T0 + 1);
    takeover.owner_epoch = 4;
    journal.recover(&takeover).expect("recovers");
    assert_eq!(journal.state("epoch").expect("reads"), Some(State::Failed));
}

#[test]
fn a_command_interrupted_before_acceptance_fails_and_a_terminal_one_is_left_alone() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    journal
        .submit(&command("received", FAR), T0)
        .expect("submits");
    ready(&mut journal, "done", FAR);
    journal
        .record_intent("done", State::ExecuteSent, T0)
        .expect("intent");
    journal
        .record_result("done", State::ExecuteSent, T0, "")
        .expect("result");
    journal
        .record_intent("done", State::ExecuteConfirmed, T0)
        .expect("intent");
    journal
        .record_result("done", State::ExecuteConfirmed, T0, "")
        .expect("result");
    journal
        .move_to("done", State::TerminationConfirmed, T0, "")
        .expect("termination");
    journal
        .move_to("done", State::VerifiedSuccess, T0, "")
        .expect("verified");

    journal.recover(&context(T0 + 1)).expect("recovers");
    assert_eq!(
        journal.state("received").expect("reads"),
        Some(State::Failed)
    );
    assert_eq!(
        journal.state("done").expect("reads"),
        Some(State::VerifiedSuccess)
    );
    assert!(journal
        .recover(&context(T0 + 2))
        .expect("recovers")
        .iter()
        .all(|r| r.command_id != "done"));
}

#[test]
fn the_journal_survives_a_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut journal = journal(&dir);
        ready(&mut journal, "c1", FAR);
    }
    let journal = journal(&dir);
    assert_eq!(journal.state("c1").expect("reads"), Some(State::Ready));
}
