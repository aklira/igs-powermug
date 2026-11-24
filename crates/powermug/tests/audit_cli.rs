//! The audit chain outside the cell (NFR-SEC-008): `powermug audit export` writes the chain of
//! a critical store to a new file and prints its anchored head; `powermug audit verify` checks
//! that file on its own, against the head when one is given.

use std::path::Path;
use std::process::{Command, Output};

use powermug_command::{CommandJournal, Granularity, NewCommand, State};

const BIN: &str = env!("CARGO_BIN_EXE_powermug");
const T0: i64 = 1_000;
const FAR: i64 = 10_000_000_000;

/// A critical store with one command, validated: two records in its chain.
fn store(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("critical.db");
    let mut journal = CommandJournal::open(&path).expect("opens");
    journal
        .submit(
            &NewCommand {
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
            },
            T0,
        )
        .expect("submits");
    journal
        .move_to("c1", State::Validated, T0, "")
        .expect("validated");
    path
}

fn audit(args: &[&str]) -> Output {
    Command::new(BIN)
        .arg("audit")
        .args(args)
        .output()
        .expect("runs the CLI")
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The head printed by an export: the `head:` line.
fn head_of(output: &Output) -> String {
    text(output)
        .lines()
        .find_map(|line| line.strip_prefix("head: "))
        .expect("prints the head")
        .to_owned()
}

#[test]
fn an_export_verifies_on_its_own_against_the_head_it_prints() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = store(dir.path());
    let out = dir.path().join("export.jsonl");

    let exported = audit(&[
        "export",
        "--critical-db",
        db.to_str().expect("path"),
        "--out",
        out.to_str().expect("path"),
    ]);
    assert!(exported.status.success(), "{}", text(&exported));
    assert!(text(&exported).contains("records: 2"));
    let head = head_of(&exported);

    let verified = audit(&[
        "verify",
        "--file",
        out.to_str().expect("path"),
        "--anchor",
        &head,
    ]);
    assert!(verified.status.success(), "{}", text(&verified));
    assert!(text(&verified).contains("verified: 2 records"));
}

#[test]
fn an_export_is_never_overwritten() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = store(dir.path());
    let out = dir.path().join("export.jsonl");
    let args = [
        "export",
        "--critical-db",
        db.to_str().expect("path"),
        "--out",
        out.to_str().expect("path"),
    ];
    assert!(audit(&args).status.success());
    assert!(
        !audit(&args).status.success(),
        "the second export is refused"
    );
}

#[test]
fn an_altered_export_fails_verification() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = store(dir.path());
    let out = dir.path().join("export.jsonl");
    assert!(audit(&[
        "export",
        "--critical-db",
        db.to_str().expect("path"),
        "--out",
        out.to_str().expect("path"),
    ])
    .status
    .success());

    let original = std::fs::read_to_string(&out).expect("reads");
    let altered = original.replacen("\"subject\":\"c1\"", "\"subject\":\"c9\"", 1);
    assert_ne!(altered, original, "the export names its command");
    std::fs::write(&out, altered).expect("writes");

    let verified = audit(&["verify", "--file", out.to_str().expect("path")]);
    assert!(!verified.status.success());
    assert!(
        text(&verified).is_empty(),
        "nothing is reported as verified"
    );
}

#[test]
fn a_chain_that_does_not_end_at_the_anchored_head_fails() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = store(dir.path());
    let out = dir.path().join("export.jsonl");
    assert!(audit(&[
        "export",
        "--critical-db",
        db.to_str().expect("path"),
        "--out",
        out.to_str().expect("path"),
    ])
    .status
    .success());

    let wrong = "0".repeat(64);
    let verified = audit(&[
        "verify",
        "--file",
        out.to_str().expect("path"),
        "--anchor",
        &wrong,
    ]);
    assert!(!verified.status.success());
}

#[test]
fn a_missing_store_is_not_created_by_an_export() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("absent.db");
    let out = dir.path().join("export.jsonl");
    let exported = audit(&[
        "export",
        "--critical-db",
        db.to_str().expect("path"),
        "--out",
        out.to_str().expect("path"),
    ]);
    assert!(!exported.status.success());
    assert!(!db.exists());
    assert!(!out.exists());
}
