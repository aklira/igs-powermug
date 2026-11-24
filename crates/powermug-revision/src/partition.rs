//! Partition revision: the subset of §10.3 that P2 needs (sessions and points), and
//! stage 2 of the validation (§10.4) on it.
//!
//! The document is `apiVersion: powermug/v1`, `kind: PartitionRevision`, with
//! `metadata.cell`, `metadata.site` and `spec.sessions` and `spec.points`. Groups,
//! chains, routes, processors and commands are not in this subset: a document that
//! carries them is refused, because the checks for them do not exist yet.
//!
//! Point fields beyond the identifier, the session, the object address and the class
//! are not in §10.3 and are defined here by default, to be confirmed.

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use crate::canonical::content_id;
use crate::error::SignatureError;
use crate::validate::Finding;

/// The JSON Schema of the partition revision, generated from the types below.
const PARTITION_SCHEMA: &str =
    include_str!("../../../schemas/powermug-partition-revision-1.0.schema.json");

/// The kind of the content identifier of a partition revision.
const PARTITION_KIND: &str = "partitionrevision";

/// A partition revision of the P2 subset.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PartitionRevision {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub cell: String,
    pub site: String,
    /// Content identifier: `partitionrevision:sha256:<hex>` of the document without
    /// this member (the canonical JSON of RFC 8785).
    pub id: String,
    /// Id of the revision this one was prepared against. Absent for the first one.
    /// Not in §10.3: a default of this subset, to confirm.
    #[serde(default)]
    pub parent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub requires: Requires,
    pub sessions: Vec<Session>,
    pub points: Vec<Point>,
    /// The access rules of the commandable points (SPECIFICATIONS 10.3, FUN-CMD-003). Absent
    /// means no point is commandable.
    #[serde(default)]
    pub commands: Vec<CommandRule>,
}

/// One grant on a commandable point: an issuer may run these operations on it, with values in
/// the given rule, when its identity is at least as fine as `min_level`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommandRule {
    pub point: String,
    pub issuer: String,
    /// `direct_execute` and `select`.
    pub operations: Vec<String>,
    /// `any` or `bool`.
    pub values: String,
    /// The finest identity level the grant can be checked against: `link`, `upstream`, `client`,
    /// `application` or `user`.
    pub min_level: String,
}

/// The operations a rule can name.
pub const COMMAND_OPERATIONS: [&str; 2] = ["direct_execute", "select"];
/// The value rules a rule can name.
pub const COMMAND_VALUES: [&str; 2] = ["any", "bool"];
/// The identity levels a rule can name, from the coarsest to the finest.
pub const IDENTITY_LEVELS: [&str; 5] = ["link", "upstream", "client", "application", "user"];

/// What the revision needs from the target: the contract version and the capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Requires {
    pub contract: String,
    pub capabilities: Vec<String>,
}

/// A South session. Only the IEC 104 client adapter is in the P2 subset.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub id: String,
    pub adapter: String,
    pub common_address: u16,
    /// Where the station listens. Carried in the signed revision (§10.3), so that the
    /// endpoint is audited like the rest of the configuration.
    pub endpoint: Endpoint,
    pub transport_security: TransportSecurity,
}

/// The host and port of a station.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    /// An IP address or a name.
    pub host: String,
    pub port: u16,
}

/// The transport security of a session. `none` needs a justification (§10.3).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransportSecurity {
    /// `none` or `tls`.
    pub mode: String,
    #[serde(default)]
    pub justification: Option<String>,
}

/// A point of the partition.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Point {
    /// Prefixed by the site, for example `S3/TM_001` (FUN-SCA-010).
    pub id: String,
    pub session: String,
    /// The information object address of the point in its session.
    pub address: u32,
    /// `A`, `B`, `C` or `Critical` (§7.5).
    pub class: String,
}

const CLASSES: [&str; 4] = ["A", "B", "C", "Critical"];

/// Parses the document into the P2 subset.
///
/// # Errors
///
/// Returns a finding with code `PMV-SEM-000` when the document does not match the
/// subset.
pub fn parse(document: &Value) -> Result<PartitionRevision, Finding> {
    serde_json::from_value(document.clone()).map_err(|err| Finding {
        code: "PMV-SEM-000",
        pointer: String::new(),
        message: format!("the document is outside the P2 partition subset: {err}"),
    })
}

/// Stage 3 of the validation (§10.4) on the P2 subset: the revision requires only
/// capabilities the profile declares (`PMV-CAP-001`), its contract version is the
/// profile's major version (`PMV-CAP-003`), and its sessions and points stay within
/// the profile's limits (`PMV-CAP-002`, FUN-SCA-003).
#[must_use]
pub fn validate_stage3(document: &Value, profile: &crate::profile::Profile) -> Vec<Finding> {
    let revision = match parse(document) {
        Ok(revision) => revision,
        Err(finding) => return vec![finding],
    };
    let mut findings = Vec::new();
    let mut push = |code: &'static str, pointer: &str, message: String| {
        findings.push(Finding {
            code,
            pointer: pointer.to_owned(),
            message,
        });
    };
    for (index, capability) in revision.spec.requires.capabilities.iter().enumerate() {
        if !profile.capabilities.contains_key(capability) {
            push(
                "PMV-CAP-001",
                &format!("/spec/requires/capabilities/{index}"),
                format!(
                    "capability {capability} is not declared by {}",
                    profile.name
                ),
            );
        }
    }
    let major = |version: &str| version.split('.').next().unwrap_or_default().to_owned();
    if major(&revision.spec.requires.contract) != major(&profile.contract_version) {
        push(
            "PMV-CAP-003",
            "/spec/requires/contract",
            format!(
                "contract {} is not the major version of {} ({})",
                revision.spec.requires.contract, profile.name, profile.contract_version
            ),
        );
    }
    let limits = &profile.limits;
    let sessions = u64::try_from(revision.spec.sessions.len()).unwrap_or(u64::MAX);
    if sessions > limits.max_sessions {
        push(
            "PMV-CAP-002",
            "/spec/sessions",
            format!(
                "{sessions} sessions exceed the limit of {}",
                limits.max_sessions
            ),
        );
    }
    let points = u64::try_from(revision.spec.points.len()).unwrap_or(u64::MAX);
    if points > limits.max_points {
        push(
            "PMV-CAP-002",
            "/spec/points",
            format!("{points} points exceed the limit of {}", limits.max_points),
        );
    }
    findings.sort();
    findings
}

/// The JSON Schema of a partition revision, as generated from the types.
#[must_use]
pub fn partition_schema() -> String {
    let schema = schemars::schema_for!(PartitionRevision);
    let mut text = serde_json::to_string_pretty(&schema).unwrap_or_default();
    text.push('\n');
    text
}

/// The content identifier of a partition revision: its canonical JSON without
/// `metadata.id`.
///
/// # Errors
///
/// Returns [`SignatureError::Canonical`] when the document cannot be canonicalized.
pub fn partition_content_id(document: &Value) -> Result<String, SignatureError> {
    let mut content = document.clone();
    if let Some(metadata) = content.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.remove("id");
    }
    content_id(PARTITION_KIND, &content)
}

/// Stage 1 of the validation (§10.4) for a partition revision: the document matches
/// the schema generated from the types, and its `metadata.id` is its content identifier.
#[must_use]
pub fn validate_stage1_partition(document: &Value) -> Vec<Finding> {
    let mut findings = Vec::new();
    match serde_json::from_str::<Value>(PARTITION_SCHEMA)
        .map_err(|err| err.to_string())
        .and_then(|schema| jsonschema::validator_for(&schema).map_err(|err| err.to_string()))
    {
        Ok(validator) => {
            for error in validator.iter_errors(document) {
                findings.push(Finding {
                    code: "PMV-ENV-001",
                    pointer: error.instance_path().to_string(),
                    message: error.to_string(),
                });
            }
        }
        Err(message) => findings.push(Finding {
            code: "PMV-ENV-000",
            pointer: String::new(),
            message,
        }),
    }
    let declared = document
        .get("metadata")
        .and_then(|metadata| metadata.get("id"))
        .and_then(Value::as_str);
    match partition_content_id(document) {
        Ok(expected) if declared == Some(expected.as_str()) => {}
        Ok(expected) => findings.push(Finding {
            code: "PMV-ENV-002",
            pointer: "/metadata/id".to_owned(),
            message: format!("the id is not the content identifier {expected}"),
        }),
        Err(err) => findings.push(Finding {
            code: "PMV-ENV-002",
            pointer: "/metadata/id".to_owned(),
            message: err.to_string(),
        }),
    }
    findings.sort();
    findings
}

/// Stage 4 of the validation (§10.4) on the P2 subset, the part that needs the
/// live partition: the revision was prepared against the revision that is active
/// now (PM-CFG-003, optimistic concurrency). `active` is the active revision of the
/// partition's scope in the critical store, if any. A mismatch is the 409 of §10.2.
///
/// The other stage 4 checks (signature, trust, storage margin, the adapters'
/// `ValidateConfig` and `PrepareConfig`, the passive node) are not in this subset.
#[must_use]
pub fn validate_stage4(document: &Value, active: Option<&str>) -> Vec<Finding> {
    let revision = match parse(document) {
        Ok(revision) => revision,
        Err(finding) => return vec![finding],
    };
    let parent = revision.metadata.parent.as_deref();
    if parent == active {
        return Vec::new();
    }
    vec![Finding {
        code: "PMV-CEL-001",
        pointer: "/metadata/parent".to_owned(),
        message: format!(
            "the revision was prepared against {parent:?}, but {active:?} is active (409)"
        ),
    }]
}

/// Stage 2 of the validation on the P2 subset: unique identifiers and addresses,
/// existing sessions, known classes, valid common addresses, point identifiers
/// prefixed by the site. Findings are sorted, so that the output is deterministic.
#[must_use]
pub fn validate_stage2(document: &Value) -> Vec<Finding> {
    let revision = match parse(document) {
        Ok(revision) => revision,
        Err(finding) => return vec![finding],
    };
    let mut findings = Vec::new();
    if revision.api_version != "powermug/v1" || revision.kind != "PartitionRevision" {
        push_finding(
            &mut findings,
            "PMV-SEM-000",
            String::new(),
            "apiVersion must be powermug/v1 and kind PartitionRevision".to_owned(),
        );
    }
    let session_ids = check_sessions(&revision, &mut findings);
    check_points(&revision, &session_ids, &mut findings);
    check_commands(&revision, &mut findings);
    findings.sort();
    findings
}

/// Checks the command rules: each names a point of the document, and holds valid operations,
/// value rule and identity level (`PMV-ACL-002` for an unknown point, `PMV-ACL-003` otherwise).
fn check_commands(revision: &PartitionRevision, findings: &mut Vec<Finding>) {
    let point_ids: std::collections::HashSet<&str> = revision
        .spec
        .points
        .iter()
        .map(|point| point.id.as_str())
        .collect();
    for (index, rule) in revision.spec.commands.iter().enumerate() {
        let pointer = format!("/spec/commands/{index}");
        if !point_ids.contains(rule.point.as_str()) {
            push_finding(
                findings,
                "PMV-ACL-002",
                pointer.clone(),
                format!(
                    "the command rule names {}, which is not a point of the document",
                    rule.point
                ),
            );
        }
        let refused = |reason: String| {
            (
                "PMV-ACL-003",
                pointer.clone(),
                format!("the command rule on {} is not valid: {reason}", rule.point),
            )
        };
        let problem = if rule.issuer.trim().is_empty() {
            Some(refused("the issuer is empty".to_owned()))
        } else if rule.operations.is_empty() {
            Some(refused("it grants no operation".to_owned()))
        } else if let Some(operation) = rule
            .operations
            .iter()
            .find(|operation| !COMMAND_OPERATIONS.contains(&operation.as_str()))
        {
            Some(refused(format!("{operation} is not an operation")))
        } else if !COMMAND_VALUES.contains(&rule.values.as_str()) {
            Some(refused(format!("{} is not a value rule", rule.values)))
        } else if !IDENTITY_LEVELS.contains(&rule.min_level.as_str()) {
            Some(refused(format!(
                "{} is not an identity level",
                rule.min_level
            )))
        } else {
            None
        };
        if let Some((code, pointer, message)) = problem {
            push_finding(findings, code, pointer, message);
        }
    }
}

fn push_finding(findings: &mut Vec<Finding>, code: &'static str, pointer: String, message: String) {
    findings.push(Finding {
        code,
        pointer,
        message,
    });
}

/// Checks the sessions and their endpoints; returns the identifiers of the sessions.
fn check_sessions(
    revision: &PartitionRevision,
    findings: &mut Vec<Finding>,
) -> std::collections::HashSet<String> {
    let mut session_ids = std::collections::HashSet::new();
    let mut endpoints = std::collections::HashSet::new();
    for (index, session) in revision.spec.sessions.iter().enumerate() {
        let pointer = format!("/spec/sessions/{index}");
        if !session_ids.insert(session.id.clone()) {
            push_finding(
                findings,
                "PMV-SEM-001",
                pointer.clone(),
                format!("session id {} is used twice", session.id),
            );
        }
        if session.common_address == 0 {
            push_finding(
                findings,
                "PMV-SEM-007",
                pointer.clone(),
                format!(
                    "session {} has common address 0, which is not used",
                    session.id
                ),
            );
        }
        if session.endpoint.host.trim().is_empty() {
            push_finding(
                findings,
                "PMV-SEM-008",
                pointer.clone(),
                format!("session {} has no host", session.id),
            );
        }
        if session.endpoint.port == 0 {
            push_finding(
                findings,
                "PMV-SEM-009",
                pointer.clone(),
                format!("session {} has port 0", session.id),
            );
        }
        if !endpoints.insert((session.endpoint.host.clone(), session.endpoint.port)) {
            push_finding(
                findings,
                "PMV-SEM-010",
                pointer.clone(),
                format!(
                    "endpoint {}:{} is used by two sessions",
                    session.endpoint.host, session.endpoint.port
                ),
            );
        }
        check_transport_security(session, &pointer, findings);
    }
    session_ids
}

fn check_transport_security(session: &Session, pointer: &str, findings: &mut Vec<Finding>) {
    match session.transport_security.mode.as_str() {
        "tls" => {}
        "none" => {
            let justified = session
                .transport_security
                .justification
                .as_deref()
                .is_some_and(|text| !text.trim().is_empty());
            if !justified {
                push_finding(
                    findings,
                    "PMV-SEM-012",
                    pointer.to_owned(),
                    format!(
                        "session {} has no transport security and no justification",
                        session.id
                    ),
                );
            }
        }
        other => push_finding(
            findings,
            "PMV-SEM-011",
            pointer.to_owned(),
            format!("transport security {other} is not none or tls"),
        ),
    }
}

/// Checks the points: unique identifiers and addresses, existing sessions, classes,
/// and the site prefix.
fn check_points(
    revision: &PartitionRevision,
    session_ids: &std::collections::HashSet<String>,
    findings: &mut Vec<Finding>,
) {
    let mut point_ids = std::collections::HashSet::new();
    let mut addresses = std::collections::HashSet::new();
    let prefix = format!("{}/", revision.metadata.site);
    for (index, point) in revision.spec.points.iter().enumerate() {
        let pointer = format!("/spec/points/{index}");
        if !point_ids.insert(point.id.clone()) {
            push_finding(
                findings,
                "PMV-SEM-002",
                pointer.clone(),
                format!("point id {} is used twice", point.id),
            );
        }
        if !session_ids.contains(&point.session) {
            push_finding(
                findings,
                "PMV-SEM-003",
                pointer.clone(),
                format!(
                    "point {} refers to unknown session {}",
                    point.id, point.session
                ),
            );
        }
        if !addresses.insert((point.session.clone(), point.address)) {
            push_finding(
                findings,
                "PMV-SEM-004",
                pointer.clone(),
                format!(
                    "address {} is used twice in session {}",
                    point.address, point.session
                ),
            );
        }
        if !CLASSES.contains(&point.class.as_str()) {
            push_finding(
                findings,
                "PMV-SEM-005",
                pointer.clone(),
                format!("class {} is not A, B, C or Critical", point.class),
            );
        }
        if !point.id.starts_with(&prefix) {
            push_finding(
                findings,
                "PMV-SEM-006",
                pointer,
                format!(
                    "point {} is not prefixed by the site {}",
                    point.id, revision.metadata.site
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A session of the test document: a station on its own endpoint, without TLS.
    fn session(id: &str, common_address: u16) -> Value {
        json!({
            "id": id,
            "adapter": "iec104-client",
            "common_address": common_address,
            "endpoint": { "host": format!("10.3.0.{common_address}"), "port": 2404 },
            "transport_security": { "mode": "none", "justification": "isolated process network" }
        })
    }

    /// The document with its content identifier set, as a producer writes it.
    fn sealed(mut document: Value) -> Value {
        let id = partition_content_id(&document).expect("canonical");
        document["metadata"]["id"] = json!(id);
        document
    }

    #[test]
    fn the_checked_in_schema_is_the_one_generated_from_the_types() {
        assert_eq!(
            PARTITION_SCHEMA,
            partition_schema(),
            "schemas/powermug-partition-revision-1.0.schema.json is out of date: run `cargo test -p powermug-revision -- --ignored regenerate_schema`"
        );
    }

    #[test]
    #[ignore = "writes the checked-in schema; run it after changing the partition types"]
    fn regenerate_schema() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/powermug-partition-revision-1.0.schema.json"
        );
        std::fs::write(path, partition_schema()).expect("writes the schema");
    }

    #[test]
    fn a_sealed_document_passes_stage_one() {
        assert_eq!(validate_stage1_partition(&sealed(document())), Vec::new());
    }

    #[test]
    fn a_wrong_content_identifier_is_found() {
        let findings = validate_stage1_partition(&document());
        assert_eq!(codes_of(&findings), vec!["PMV-ENV-002"]);
    }

    #[test]
    fn a_field_the_schema_does_not_know_is_found() {
        let mut doc = document();
        doc["spec"]["unexpected"] = json!(1);
        let findings = validate_stage1_partition(&sealed(doc));
        assert!(codes_of(&findings).contains(&"PMV-ENV-001"), "{findings:?}");
    }

    fn document() -> Value {
        json!({
            "apiVersion": "powermug/v1",
            "kind": "PartitionRevision",
            "metadata": { "cell": "CELL_DC1", "site": "S3", "id": "partitionrevision:sha256:placeholder" },
            "spec": {
                "requires": { "contract": "1.0", "capabilities": ["south.iec104"] },
                "sessions": [ session("s3-rtu1", 3) ],
                "points": [
                    { "id": "S3/TM_001", "session": "s3-rtu1", "address": 1, "class": "A" },
                    { "id": "S3/TM_002", "session": "s3-rtu1", "address": 2, "class": "B" }
                ]
            }
        })
    }

    fn codes(findings: &[Finding]) -> Vec<&'static str> {
        findings.iter().map(|f| f.code).collect()
    }

    #[test]
    fn a_consistent_document_has_no_findings() {
        assert_eq!(validate_stage2(&document()), Vec::new());
    }

    #[test]
    fn duplicate_session_and_point_ids_are_found() {
        let mut doc = document();
        doc["spec"]["sessions"]
            .as_array_mut()
            .expect("array")
            .push(session("s3-rtu1", 4));
        doc["spec"]["points"]
            .as_array_mut()
            .expect("array")
            .push(json!({"id": "S3/TM_001", "session": "s3-rtu1", "address": 9, "class": "A"}));
        assert_eq!(
            codes(&validate_stage2(&doc)),
            vec!["PMV-SEM-001", "PMV-SEM-002"]
        );
    }

    #[test]
    fn a_point_on_an_unknown_session_is_found() {
        let mut doc = document();
        doc["spec"]["points"][0]["session"] = json!("nope");
        assert!(codes(&validate_stage2(&doc)).contains(&"PMV-SEM-003"));
    }

    #[test]
    fn an_address_used_twice_in_a_session_is_found() {
        let mut doc = document();
        doc["spec"]["points"][1]["address"] = json!(1);
        assert_eq!(codes(&validate_stage2(&doc)), vec!["PMV-SEM-004"]);
    }

    #[test]
    fn an_unknown_class_and_a_foreign_prefix_are_found() {
        let mut doc = document();
        doc["spec"]["points"][0]["class"] = json!("Z");
        doc["spec"]["points"][1]["id"] = json!("S4/TM_002");
        assert_eq!(
            codes(&validate_stage2(&doc)),
            vec!["PMV-SEM-005", "PMV-SEM-006"]
        );
    }

    #[test]
    fn endpoint_checks_are_found() {
        let mut doc = document();
        doc["spec"]["sessions"][0]["endpoint"] = json!({ "host": " ", "port": 0 });
        assert_eq!(
            codes(&validate_stage2(&doc)),
            vec!["PMV-SEM-008", "PMV-SEM-009"]
        );
    }

    #[test]
    fn two_sessions_on_one_endpoint_are_found() {
        let mut doc = document();
        doc["spec"]["sessions"]
            .as_array_mut()
            .expect("array")
            .push(json!({
                "id": "s3-rtu2",
                "adapter": "iec104-client",
                "common_address": 4,
                "endpoint": { "host": "10.3.0.3", "port": 2404 },
                "transport_security": { "mode": "tls" }
            }));
        assert_eq!(codes(&validate_stage2(&doc)), vec!["PMV-SEM-010"]);
    }

    #[test]
    fn transport_security_needs_a_known_mode_and_a_justification_without_tls() {
        let mut doc = document();
        doc["spec"]["sessions"][0]["transport_security"] = json!({ "mode": "plain" });
        assert_eq!(codes(&validate_stage2(&doc)), vec!["PMV-SEM-011"]);

        let mut unjustified = document();
        unjustified["spec"]["sessions"][0]["transport_security"] = json!({ "mode": "none" });
        assert_eq!(codes(&validate_stage2(&unjustified)), vec!["PMV-SEM-012"]);
    }

    #[test]
    fn common_address_zero_is_found() {
        let mut doc = document();
        doc["spec"]["sessions"][0]["common_address"] = json!(0);
        assert_eq!(codes(&validate_stage2(&doc)), vec!["PMV-SEM-007"]);
    }

    #[test]
    fn a_revision_prepared_against_the_active_one_passes_stage_four() {
        let mut doc = document();
        doc["metadata"]["parent"] = json!("r1");
        assert_eq!(validate_stage4(&doc, Some("r1")), Vec::new());
    }

    #[test]
    fn the_first_revision_passes_stage_four_when_nothing_is_active() {
        assert_eq!(validate_stage4(&document(), None), Vec::new());
    }

    #[test]
    fn a_revision_prepared_against_an_old_parent_is_refused() {
        let mut doc = document();
        doc["metadata"]["parent"] = json!("r1");
        let findings = validate_stage4(&doc, Some("r2"));
        assert_eq!(codes_of(&findings), vec!["PMV-CEL-001"]);
    }

    fn small_profile() -> crate::profile::Profile {
        let mut profile = crate::profile::powermug_v1();
        profile.limits.max_points = 1;
        profile
    }

    #[test]
    fn a_consistent_document_passes_stage_three() {
        assert_eq!(
            validate_stage3(&document(), &crate::profile::powermug_v1()),
            Vec::new()
        );
    }

    #[test]
    fn an_undeclared_capability_is_found() {
        let mut doc = document();
        doc["spec"]["requires"]["capabilities"] = json!(["south.modbus_tcp"]);
        let findings = validate_stage3(&doc, &crate::profile::powermug_v1());
        assert_eq!(codes_of(&findings), vec!["PMV-CAP-001"]);
    }

    #[test]
    fn a_contract_of_another_major_version_is_found() {
        let mut doc = document();
        doc["spec"]["requires"]["contract"] = json!("2.0");
        let findings = validate_stage3(&doc, &crate::profile::powermug_v1());
        assert_eq!(codes_of(&findings), vec!["PMV-CAP-003"]);
    }

    #[test]
    fn points_above_the_profile_limit_are_found() {
        let findings = validate_stage3(&document(), &small_profile());
        assert_eq!(codes_of(&findings), vec!["PMV-CAP-002"]);
    }

    fn codes_of(findings: &[Finding]) -> Vec<&'static str> {
        findings.iter().map(|f| f.code).collect()
    }

    #[test]
    fn a_field_outside_the_subset_is_refused_as_a_whole() {
        let mut doc = document();
        doc["spec"]["routes"] = json!([]);
        assert_eq!(codes(&validate_stage2(&doc)), vec!["PMV-SEM-000"]);
    }
}
