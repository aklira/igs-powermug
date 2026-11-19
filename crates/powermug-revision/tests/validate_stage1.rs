//! Stage 1 of the validation (§10.4) on revisions produced by gridloop.

use std::fs;
use std::path::PathBuf;

use powermug_revision::{parse_strict_json, validate_stage1};
use serde_json::Value;

fn fixture_revision() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/signed-plan/envelope.json");
    let text = fs::read_to_string(path).expect("envelope present");
    let envelope = parse_strict_json(&text).expect("strict JSON");
    envelope["payload"]["artifacts"][0].clone()
}

#[test]
fn a_gridloop_revision_has_no_stage1_findings() {
    assert_eq!(validate_stage1(&fixture_revision()), Vec::new());
}

#[test]
fn an_enum_outside_its_domain_is_refused_with_its_pointer() {
    let mut revision = fixture_revision();
    revision["mappings"][0]["type"] = Value::from("XX");
    let findings = validate_stage1(&revision);
    // The change also breaks the content identifier, so PMV-ENV-002 is expected too.
    let schema = findings
        .iter()
        .find(|f| f.code == "PMV-ENV-001")
        .expect("schema finding");
    assert_eq!(schema.pointer, "/mappings/0/type");
    assert!(findings.iter().any(|f| f.code == "PMV-ENV-002"));
}

#[test]
fn an_unknown_member_is_refused() {
    let mut revision = fixture_revision();
    revision["undeclared"] = Value::from(true);
    let mut revision_without_id = revision.clone();
    // Keep the id consistent with the new content, so only the schema reports.
    let id = powermug_revision::canonical::content_id("revision", &revision_without_id).unwrap();
    revision_without_id["id"] = Value::from(id.clone());
    revision["id"] = Value::from(id);
    let findings = validate_stage1(&revision);
    assert!(
        findings.iter().any(|f| f.code == "PMV-ENV-001"),
        "{findings:?}"
    );
    assert!(!findings.iter().any(|f| f.code == "PMV-ENV-002"));
}

#[test]
fn a_wrong_identifier_is_refused() {
    let mut revision = fixture_revision();
    revision["site"] = Value::from("S9");
    let findings = validate_stage1(&revision);
    assert!(findings
        .iter()
        .any(|f| f.code == "PMV-ENV-002" && f.pointer == "/id"));
}

#[test]
fn findings_are_deterministic() {
    let mut revision = fixture_revision();
    revision["mappings"][0]["type"] = Value::from("XX");
    revision["mappings"][0]["data_class"] = Value::from("Z");
    assert_eq!(validate_stage1(&revision), validate_stage1(&revision));
}
