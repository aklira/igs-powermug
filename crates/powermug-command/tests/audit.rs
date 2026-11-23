//! The hash-chained audit records (SPECIFICATIONS 9.2, NFR-SEC-008): every command write is
//! recorded, the chain verifies, and an altered record is found at its position.

use powermug_command::audit::GENESIS;
use powermug_command::Granularity;
use powermug_command::{CommandError, CommandJournal, NewCommand, RecoveryContext, State};

const T0: i64 = 1_000;
const FAR: i64 = 10_000_000_000;

fn journal_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("critical.db")
}

fn command(id: &str) -> NewCommand {
    NewCommand {
        command_id: id.into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::User,
        point: "S3/TC_101".into(),
        operation: "direct_execute".into(),
        value: "true".into(),
        expires_ns: FAR,
        expected_revision: "r1".into(),
        owner_epoch: 1,
        ha_term: 1,
    }
}

#[test]
fn an_empty_chain_verifies_and_its_head_is_the_genesis() {
    let dir = tempfile::tempdir().expect("temp dir");
    let journal = CommandJournal::open(journal_path(&dir)).expect("opens");
    assert_eq!(journal.verify_audit(), Ok(0));
    assert_eq!(journal.audit_head(), Ok(GENESIS));
}

#[test]
fn every_write_is_recorded_and_the_chain_verifies() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(journal_path(&dir)).expect("opens");
    journal.submit(&command("c1"), T0).expect("submits");
    journal
        .move_to("c1", State::Validated, T0, "")
        .expect("validated");
    journal
        .move_to("c1", State::DurablyAccepted, T0, "")
        .expect("accepted");
    journal.move_to("c1", State::Ready, T0, "").expect("ready");
    journal
        .record_intent("c1", State::ExecuteSent, T0)
        .expect("intent");
    journal
        .record_result("c1", State::ExecuteSent, T0, "sent")
        .expect("result");

    assert_eq!(journal.verify_audit(), Ok(6));
    assert_ne!(journal.audit_head().expect("head"), GENESIS);
}

#[test]
fn a_restart_that_expires_a_command_writes_the_decision_to_the_chain() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(journal_path(&dir)).expect("opens");
    let mut short = command("c1");
    short.expires_ns = T0 + 5;
    journal.submit(&short, T0).expect("submits");
    journal
        .move_to("c1", State::Validated, T0, "")
        .expect("validated");
    journal
        .move_to("c1", State::DurablyAccepted, T0, "")
        .expect("accepted");
    journal.move_to("c1", State::Ready, T0, "").expect("ready");
    let before = journal.verify_audit().expect("verifies");

    let outcome = journal
        .recover(&RecoveryContext {
            now_ns: T0 + 100,
            active_revision: "r1".into(),
            owner_epoch: 1,
            ha_active: true,
        })
        .expect("recovers");
    assert_eq!(outcome[0].to, State::Expired);
    assert_eq!(journal.verify_audit(), Ok(before + 1));
}

#[test]
fn an_altered_record_is_found_at_its_position() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(journal_path(&dir)).expect("opens");
    journal.submit(&command("c1"), T0).expect("submits");
    journal
        .move_to("c1", State::Validated, T0, "")
        .expect("validated");
    journal
        .move_to("c1", State::DurablyAccepted, T0, "")
        .expect("accepted");
    assert_eq!(journal.verify_audit(), Ok(3));

    // Someone with access to the file rewrites record 2.
    let raw = rusqlite::Connection::open(journal_path(&dir)).expect("opens raw");
    raw.execute("UPDATE audit SET payload = 'forged' WHERE seq = 2", [])
        .expect("tampers");

    assert_eq!(journal.verify_audit(), Err(CommandError::AuditBroken(2)));
}

#[test]
fn removing_the_last_record_is_visible_only_against_an_anchored_head() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(journal_path(&dir)).expect("opens");
    journal.submit(&command("c1"), T0).expect("submits");
    journal
        .move_to("c1", State::Validated, T0, "")
        .expect("validated");
    let anchored = journal.audit_head().expect("head");

    let raw = rusqlite::Connection::open(journal_path(&dir)).expect("opens raw");
    raw.execute("DELETE FROM audit WHERE seq = 2", [])
        .expect("removes");

    // The chain itself still verifies: the head shows the removal.
    assert_eq!(journal.verify_audit(), Ok(1));
    assert_ne!(journal.audit_head().expect("head"), anchored);
}
