//! The operating mode and its audit (SPECIFICATIONS 9.3, FUN-OPS-006).

use powermug_command::audit::GENESIS;
use powermug_command::{CommandJournal, Mode};

fn journal(dir: &tempfile::TempDir) -> CommandJournal {
    CommandJournal::open(dir.path().join("critical.db")).expect("opens")
}

#[test]
fn a_new_cell_runs_in_normal_mode_and_a_change_is_audited() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    assert_eq!(journal.mode(), Ok(Mode::Normal));
    assert_eq!(journal.verify_audit(), Ok(0));

    assert_eq!(
        journal.set_mode(Mode::Maintenance, "ops-jdoe", 10),
        Ok(true)
    );
    assert_eq!(journal.mode(), Ok(Mode::Maintenance));
    assert_eq!(journal.verify_audit(), Ok(1));
    assert_ne!(journal.audit_head(), Ok(GENESIS));
}

#[test]
fn setting_the_same_mode_again_changes_and_audits_nothing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = journal(&dir);
    journal
        .set_mode(Mode::Shadow, "ops-jdoe", 10)
        .expect("sets");
    let head = journal.audit_head();
    assert_eq!(journal.set_mode(Mode::Shadow, "ops-other", 20), Ok(false));
    assert_eq!(journal.verify_audit(), Ok(1));
    assert_eq!(journal.audit_head(), head);
}

#[test]
fn the_mode_survives_a_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut journal = journal(&dir);
        journal
            .set_mode(Mode::Maintenance, "ops-jdoe", 10)
            .expect("sets");
    }
    assert_eq!(journal(&dir).mode(), Ok(Mode::Maintenance));
}

#[test]
fn mode_codes_round_trip() {
    for mode in [Mode::Normal, Mode::Maintenance, Mode::Shadow] {
        assert_eq!(Mode::from_code(mode.code()), Some(mode));
    }
    assert_eq!(Mode::from_code("debug"), None);
}
