//! The command rules of a partition revision (SPECIFICATIONS 10.3, 10.4 stage 2) and the change
//! class of a revision against the one it replaces (FUN-CFG-007).

use powermug_revision::change::{change_class, ChangeClass};
use powermug_revision::partition::{parse, validate_stage2, PartitionRevision};
use serde_json::{json, Value};

fn points() -> Value {
    json!([
        {"id": "S3/TM_001", "session": "s3-ca3", "address": 101, "class": "A"},
        {"id": "S3/TC_101", "session": "s3-ca3", "address": 5, "class": "Critical"}
    ])
}

fn rule() -> Value {
    json!({
        "point": "S3/TC_101",
        "issuer": "scada-demo",
        "operations": ["direct_execute", "select"],
        "values": "bool",
        "min_level": "link"
    })
}

fn document(site: &str, port: u16, points: Value, commands: Value) -> Value {
    let mut document = json!({
        "apiVersion": "powermug/v1",
        "kind": "PartitionRevision",
        "metadata": {"cell": "CELL_DC1", "site": site, "id": "partitionrevision:sha256:x"},
        "spec": {
            "requires": {"contract": "1.0", "capabilities": ["south.iec104"]},
            "sessions": [{
                "id": "s3-ca3",
                "adapter": "iec104-client",
                "common_address": 3,
                "endpoint": {"host": "10.3.0.3", "port": port},
                "transport_security": {"mode": "none", "justification": "isolated process network"}
            }]
        }
    });
    document["spec"]["points"] = points;
    document["spec"]["commands"] = commands;
    document
}

fn revision(document: &Value) -> PartitionRevision {
    parse(document).expect("parses")
}

fn codes(document: &Value) -> Vec<&'static str> {
    validate_stage2(document)
        .iter()
        .map(|finding| finding.code)
        .collect()
}

#[test]
fn valid_rules_on_a_point_of_the_document_pass_stage_two() {
    let document = document("S3", 2404, points(), json!([rule()]));
    assert_eq!(validate_stage2(&document), Vec::new());
}

#[test]
fn a_rule_on_a_point_the_document_does_not_have_is_refused() {
    let mut unknown = rule();
    unknown["point"] = json!("S3/TC_999");
    let document = document("S3", 2404, points(), json!([unknown]));
    assert_eq!(codes(&document), vec!["PMV-ACL-002"]);
}

#[test]
fn a_rule_with_an_unknown_operation_value_rule_or_level_is_refused() {
    let mut operation = rule();
    operation["operations"] = json!(["open"]);
    let mut level = rule();
    level["min_level"] = json!("root");
    let mut empty_issuer = rule();
    empty_issuer["issuer"] = json!(" ");
    let document = document(
        "S3",
        2404,
        points(),
        json!([operation, level, empty_issuer]),
    );
    assert_eq!(codes(&document), vec!["PMV-ACL-003"; 3]);
}

#[test]
fn a_document_without_commands_has_no_commandable_point() {
    let document = document("S3", 2404, points(), json!([]));
    assert_eq!(validate_stage2(&document), Vec::new());
}

#[test]
fn an_identical_revision_is_hot() {
    let before = revision(&document("S3", 2404, points(), json!([rule()])));
    assert_eq!(change_class(&before, &before), ChangeClass::Hot);
}

#[test]
fn a_change_on_a_point_that_is_not_commandable_is_hot() {
    let before = revision(&document("S3", 2404, points(), json!([rule()])));
    let mut moved = points();
    moved[0]["address"] = json!(102);
    let after = revision(&document("S3", 2404, moved, json!([rule()])));
    assert_eq!(change_class(&before, &after), ChangeClass::Hot);
}

#[test]
fn an_endpoint_change_restarts_the_session() {
    let before = revision(&document("S3", 2404, points(), json!([rule()])));
    let after = revision(&document("S3", 2405, points(), json!([rule()])));
    assert_eq!(change_class(&before, &after), ChangeClass::SessionRestart);
}

#[test]
fn a_change_of_a_commandable_point_is_command_critical() {
    let before = revision(&document("S3", 2404, points(), json!([rule()])));
    let mut moved = points();
    moved[1]["address"] = json!(6);
    let after = revision(&document("S3", 2404, moved, json!([rule()])));
    assert_eq!(change_class(&before, &after), ChangeClass::CommandCritical);

    let mut removed = points();
    removed.as_array_mut().expect("array").pop();
    let without = revision(&document("S3", 2404, removed, json!([])));
    assert_eq!(
        change_class(&before, &without),
        ChangeClass::CommandCritical
    );
}

#[test]
fn a_change_of_a_rule_is_command_critical() {
    let before = revision(&document("S3", 2404, points(), json!([rule()])));
    let mut widened = rule();
    widened["values"] = json!("any");
    let after = revision(&document("S3", 2404, points(), json!([widened])));
    assert_eq!(change_class(&before, &after), ChangeClass::CommandCritical);

    let added = revision(&document("S3", 2404, points(), json!([rule(), rule()])));
    assert_eq!(change_class(&before, &added), ChangeClass::CommandCritical);
}

#[test]
fn a_change_of_the_partition_of_the_session_is_command_critical() {
    let before = revision(&document("S3", 2404, points(), json!([rule()])));
    let after = revision(&document("S4", 2404, points(), json!([rule()])));
    assert_eq!(change_class(&before, &after), ChangeClass::CommandCritical);
}

#[test]
fn the_classes_are_ordered_by_cost_and_named_as_in_the_specification() {
    assert!(ChangeClass::Hot < ChangeClass::SessionRestart);
    assert!(ChangeClass::SessionRestart < ChangeClass::CommandCritical);
    assert_eq!(ChangeClass::CommandCritical.code(), "COMMAND_CRITICAL");
    assert_eq!(ChangeClass::SessionRestart.code(), "SESSION_RESTART");
    assert_eq!(ChangeClass::Hot.code(), "HOT");
}

fn declaring(mut document: Value, class: &str) -> Value {
    document["spec"]["change_class"] = json!(class);
    document
}

#[test]
fn a_declared_class_names_a_known_class_or_is_refused() {
    let known = declaring(
        document("S3", 2404, points(), json!([rule()])),
        "COMMAND_CRITICAL",
    );
    assert_eq!(validate_stage2(&known), Vec::new());
    let unknown = declaring(document("S3", 2404, points(), json!([rule()])), "FAST");
    assert_eq!(codes(&unknown), vec!["PMV-SEM-001"]);
}

#[test]
fn a_declared_class_below_the_computed_one_is_refused() {
    use powermug_revision::change::declared_class_findings;

    let before = revision(&document("S3", 2404, points(), json!([rule()])));
    let mut widened = rule();
    widened["values"] = json!("any");
    let critical = document("S3", 2404, points(), json!([widened]));

    let too_low = revision(&declaring(critical.clone(), "HOT"));
    let findings = declared_class_findings(&too_low, Some(&before));
    assert_eq!(
        findings
            .iter()
            .map(|finding| finding.code)
            .collect::<Vec<_>>(),
        vec!["PMV-CEL-007"]
    );

    let declared = revision(&declaring(critical.clone(), "COMMAND_CRITICAL"));
    assert_eq!(
        declared_class_findings(&declared, Some(&before)),
        Vec::new()
    );
    let higher = revision(&declaring(critical, "SYSTEM_REBOOT"));
    assert_eq!(declared_class_findings(&higher, Some(&before)), Vec::new());
}

#[test]
fn without_an_active_revision_the_computed_class_is_hot() {
    use powermug_revision::change::declared_class_findings;

    let first = revision(&declaring(
        document("S3", 2404, points(), json!([rule()])),
        "HOT",
    ));
    assert_eq!(declared_class_findings(&first, None), Vec::new());
    let undeclared = revision(&document("S3", 2404, points(), json!([rule()])));
    assert_eq!(declared_class_findings(&undeclared, None), Vec::new());
}

#[test]
fn the_class_names_round_trip_and_order_by_cost() {
    use powermug_revision::change::ChangeClass;

    for class in [
        ChangeClass::Hot,
        ChangeClass::SessionRestart,
        ChangeClass::CommandCritical,
        ChangeClass::SystemReboot,
    ] {
        assert_eq!(ChangeClass::from_code(class.code()), Some(class));
    }
    assert!(ChangeClass::CommandCritical < ChangeClass::SystemReboot);
    assert_eq!(ChangeClass::from_code("FAST"), None);
}
