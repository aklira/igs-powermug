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

use serde::Deserialize;
use serde_json::Value;

use crate::validate::Finding;

/// A partition revision of the P2 subset.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionRevision {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub cell: String,
    pub site: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub requires: Requires,
    pub sessions: Vec<Session>,
    pub points: Vec<Point>,
}

/// What the revision needs from the target: the contract version and the capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requires {
    pub contract: String,
    pub capabilities: Vec<String>,
}

/// A South session. Only the IEC 104 client adapter is in the P2 subset.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub id: String,
    pub adapter: String,
    pub common_address: u16,
}

/// A point of the partition.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    let mut push = |code: &'static str, pointer: String, message: String| {
        findings.push(Finding {
            code,
            pointer,
            message,
        });
    };

    if revision.api_version != "powermug/v1" || revision.kind != "PartitionRevision" {
        push(
            "PMV-SEM-000",
            String::new(),
            "apiVersion must be powermug/v1 and kind PartitionRevision".to_owned(),
        );
    }

    let mut session_ids = std::collections::HashSet::new();
    for (index, session) in revision.spec.sessions.iter().enumerate() {
        let pointer = format!("/spec/sessions/{index}");
        if !session_ids.insert(session.id.clone()) {
            push(
                "PMV-SEM-001",
                pointer.clone(),
                format!("session id {} is used twice", session.id),
            );
        }
        if session.common_address == 0 {
            push(
                "PMV-SEM-007",
                pointer,
                format!(
                    "session {} has common address 0, which is not used",
                    session.id
                ),
            );
        }
    }

    let mut point_ids = std::collections::HashSet::new();
    let mut addresses = std::collections::HashSet::new();
    let prefix = format!("{}/", revision.metadata.site);
    for (index, point) in revision.spec.points.iter().enumerate() {
        let pointer = format!("/spec/points/{index}");
        if !point_ids.insert(point.id.clone()) {
            push(
                "PMV-SEM-002",
                pointer.clone(),
                format!("point id {} is used twice", point.id),
            );
        }
        if !session_ids.contains(&point.session) {
            push(
                "PMV-SEM-003",
                pointer.clone(),
                format!(
                    "point {} refers to unknown session {}",
                    point.id, point.session
                ),
            );
        }
        if !addresses.insert((point.session.clone(), point.address)) {
            push(
                "PMV-SEM-004",
                pointer.clone(),
                format!(
                    "address {} is used twice in session {}",
                    point.address, point.session
                ),
            );
        }
        if !CLASSES.contains(&point.class.as_str()) {
            push(
                "PMV-SEM-005",
                pointer.clone(),
                format!("class {} is not A, B, C or Critical", point.class),
            );
        }
        if !point.id.starts_with(&prefix) {
            push(
                "PMV-SEM-006",
                pointer,
                format!(
                    "point {} is not prefixed by the site {}",
                    point.id, revision.metadata.site
                ),
            );
        }
    }

    findings.sort();
    findings
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn document() -> Value {
        json!({
            "apiVersion": "powermug/v1",
            "kind": "PartitionRevision",
            "metadata": { "cell": "CELL_DC1", "site": "S3" },
            "spec": {
                "requires": { "contract": "1.0", "capabilities": ["south.iec104"] },
                "sessions": [ { "id": "s3-rtu1", "adapter": "iec104-client", "common_address": 3 } ],
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
            .push(json!({"id": "s3-rtu1", "adapter": "iec104-client", "common_address": 4}));
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
    fn common_address_zero_is_found() {
        let mut doc = document();
        doc["spec"]["sessions"][0]["common_address"] = json!(0);
        assert_eq!(codes(&validate_stage2(&doc)), vec!["PMV-SEM-007"]);
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
