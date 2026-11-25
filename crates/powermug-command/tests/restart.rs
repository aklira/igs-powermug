//! The restart rules by partition (SPECIFICATIONS 9.7, GW 11.5): a partition's commands are
//! decided with that partition's context, and the commands of the other partitions are untouched.

use powermug_command::{
    Action, CommandJournal, CommandWriter, Granularity, NewCommand, RecoveryContext, State,
};

const T0: i64 = 1_000;
const FAR: i64 = 10_000_000_000;

fn command(id: &str, point: &str) -> NewCommand {
    NewCommand {
        command_id: id.into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::Client,
        point: point.into(),
        operation: "direct_execute".into(),
        value: "true".into(),
        expires_ns: FAR,
        expected_revision: "r1".into(),
        owner_epoch: 1,
        ha_term: 1,
    }
}

fn ready(journal: &mut CommandJournal, id: &str, point: &str) {
    journal.submit(&command(id, point), T0).expect("submits");
    journal
        .move_to(id, State::Validated, T0, "")
        .expect("validated");
    journal
        .move_to(id, State::DurablyAccepted, T0, "")
        .expect("accepted");
    journal.move_to(id, State::Ready, T0, "").expect("ready");
}

fn context(owner_epoch: u64) -> RecoveryContext {
    RecoveryContext {
        now_ns: T0 + 10,
        active_revision: "r1".into(),
        owner_epoch,
        ha_active: true,
    }
}

#[test]
fn each_partition_is_decided_with_its_own_context_and_the_others_are_untouched() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("opens");
    ready(&mut journal, "s3", "S3/TC_101");
    ready(&mut journal, "s4", "S4/TC_201");

    // S3 kept its owner epoch 1: its command may be emitted. S4 has no owner: its command fails.
    let s3 = journal
        .recover_partition("S3", &context(1))
        .expect("recovers S3");
    assert_eq!(s3.len(), 1);
    assert_eq!(s3[0].command_id, "s3");
    assert_eq!(s3[0].action, Action::Emit);
    assert_eq!(journal.state("s3").expect("reads"), Some(State::Ready));

    assert_eq!(journal.state("s4").expect("reads"), Some(State::Ready));
    let s4 = journal
        .recover_partition("S4", &context(0))
        .expect("recovers S4");
    assert_eq!(s4.len(), 1);
    assert_eq!(s4[0].to, State::Failed);
    assert_eq!(journal.state("s4").expect("reads"), Some(State::Failed));
}

#[test]
fn a_partition_is_a_whole_site_and_not_a_prefix_of_another() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("opens");
    ready(&mut journal, "s30", "S30/TC_301");

    let outcome = journal
        .recover_partition("S3", &context(0))
        .expect("recovers S3");
    assert!(outcome.is_empty(), "S30 is not in S3");
    assert_eq!(journal.state("s30").expect("reads"), Some(State::Ready));
}

#[test]
fn the_writer_recovers_a_partition_as_the_journal_does() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    client
        .submit(command("s3", "S3/TC_101"), T0)
        .expect("submits");
    client
        .move_to("s3", State::Validated, T0, "")
        .expect("validated");
    client
        .move_to("s3", State::DurablyAccepted, T0, "")
        .expect("accepted");
    client.move_to("s3", State::Ready, T0, "").expect("ready");

    let outcome = client
        .recover_partition("S3", context(0))
        .expect("recovers");
    assert_eq!(outcome.len(), 1);
    assert_eq!(client.state("s3").expect("reads"), Some(State::Failed));
    drop(client);
    writer.join();
}

#[test]
fn an_execution_confirmed_but_not_terminated_is_in_doubt_at_restart() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("opens");
    ready(&mut journal, "c1", "S3/TC_101");
    journal
        .record_intent("c1", State::ExecuteSent, T0)
        .expect("intent");
    journal
        .record_result("c1", State::ExecuteSent, T0, "sent")
        .expect("result");
    journal
        .move_to("c1", State::ExecuteConfirmed, T0, "")
        .expect("confirmed");

    let recovered = journal
        .recover_partition("S3", &context(0))
        .expect("recovers");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].to, State::InDoubt);
    assert_eq!(recovered[0].action, Action::Nothing);
    assert_eq!(journal.state("c1").expect("reads"), Some(State::InDoubt));
}

#[test]
fn the_commands_in_doubt_are_counted_per_site_by_prefix() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("opens");
    ready(&mut journal, "s3", "S3/TC_101");
    ready(&mut journal, "s30", "S30/TC_301");
    ready(&mut journal, "s4", "S4/TC_201");
    for id in ["s3", "s30"] {
        journal
            .move_to(id, State::InDoubt, T0, "lost")
            .expect("in doubt");
    }
    assert_eq!(journal.count_in_doubt("S3").expect("counts"), 1);
    assert_eq!(journal.count_in_doubt("S30").expect("counts"), 1);
    assert_eq!(journal.count_in_doubt("S4").expect("counts"), 0);
}
