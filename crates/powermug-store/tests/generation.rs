//! The generation pointer of the critical store (SPECIFICATIONS §10.5).

use powermug_store::{CriticalStore, GenerationError, Outcome};

fn open(dir: &tempfile::TempDir) -> CriticalStore {
    CriticalStore::open(dir.path().join("critical.db")).expect("opens")
}

#[test]
fn prepare_is_idempotent_and_refuses_the_id_for_another_scope() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut store = open(&dir);

    assert_eq!(
        store.prepare("r1", "S3", None, b"{}").expect("prepares"),
        Outcome::Changed
    );
    assert_eq!(
        store.prepare("r1", "S3", None, b"{}").expect("again"),
        Outcome::Unchanged
    );
    let err = store
        .prepare("r1", "S4", None, b"{}")
        .expect_err("other scope");
    assert_eq!(err, GenerationError::ConflictingRevision("r1".into()));
}

#[test]
fn activation_switches_the_pointer_and_is_idempotent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut store = open(&dir);
    store.prepare("r1", "S3", None, b"1").expect("prepares");

    assert_eq!(
        store.activate("r1", "S3").expect("activates"),
        Outcome::Changed
    );
    assert_eq!(
        store.activate("r1", "S3").expect("again"),
        Outcome::Unchanged
    );
    assert_eq!(store.active("S3").expect("reads"), Some("r1".into()));
}

#[test]
fn a_revision_prepared_against_an_old_parent_is_refused() {
    // PM-CFG-003: the parent must still be the active revision (409).
    let dir = tempfile::tempdir().expect("temp dir");
    let mut store = open(&dir);
    store.prepare("r1", "S3", None, b"1").expect("prepares");
    store.activate("r1", "S3").expect("activates r1");
    store
        .prepare("r2", "S3", Some("r1"), b"2")
        .expect("prepares r2");
    store
        .prepare("r3", "S3", Some("r1"), b"3")
        .expect("prepares r3");
    store.activate("r2", "S3").expect("activates r2");

    let err = store.activate("r3", "S3").expect_err("stale parent");

    assert_eq!(
        err,
        GenerationError::ParentMismatch {
            expected: Some("r2".into()),
            found: Some("r1".into()),
        }
    );
    assert_eq!(store.active("S3").expect("reads"), Some("r2".into()));
}

#[test]
fn an_unprepared_revision_cannot_be_activated() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut store = open(&dir);
    let err = store.activate("ghost", "S3").expect_err("unknown");
    assert_eq!(err, GenerationError::UnknownRevision("ghost".into()));
}

#[test]
fn rollback_restores_the_previous_revision_and_is_idempotent_per_revision() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut store = open(&dir);
    store.prepare("r1", "S3", None, b"1").expect("prepares");
    store.activate("r1", "S3").expect("activates r1");
    store
        .prepare("r2", "S3", Some("r1"), b"2")
        .expect("prepares r2");
    store.activate("r2", "S3").expect("activates r2");

    assert_eq!(
        store.rollback("S3", "r2").expect("rolls back"),
        Outcome::Changed
    );
    assert_eq!(store.active("S3").expect("reads"), Some("r1".into()));
    // Rolling back r2 again, now that it is not active: nothing is done.
    assert_eq!(
        store.rollback("S3", "r2").expect("again"),
        Outcome::Unchanged
    );
    assert_eq!(store.active("S3").expect("reads"), Some("r1".into()));
}

#[test]
fn rollback_without_an_earlier_revision_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut store = open(&dir);
    store.prepare("r1", "S3", None, b"1").expect("prepares");
    store.activate("r1", "S3").expect("activates");

    let err = store.rollback("S3", "r1").expect_err("nothing before r1");
    assert_eq!(err, GenerationError::NothingToRollBack("S3".into()));
}

#[test]
fn scopes_are_independent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut store = open(&dir);
    store.prepare("s3-r1", "S3", None, b"1").expect("prepares");
    store.prepare("s4-r1", "S4", None, b"1").expect("prepares");
    store.activate("s3-r1", "S3").expect("activates S3");

    assert_eq!(store.active("S4").expect("reads"), None);
    store.activate("s4-r1", "S4").expect("activates S4");
    assert_eq!(store.active("S3").expect("reads"), Some("s3-r1".into()));
}

#[test]
fn the_active_pointer_survives_a_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut store = open(&dir);
        store.prepare("r1", "S3", None, b"1").expect("prepares");
        store.activate("r1", "S3").expect("activates");
    }
    let store = open(&dir);
    assert_eq!(store.active("S3").expect("reads"), Some("r1".into()));
}
