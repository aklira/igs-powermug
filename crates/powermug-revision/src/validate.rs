//! Stage 1 of the validation of §10.4: envelope, schema and content identifier of a revision.
//!
//! Stages 2 to 4 need the revision's semantics, the target capabilities and a live cell.
//! They are added with the revision types of P2 and P5. Stage 1 is what `powermug revision
//! validate` runs offline, and what the core runs before it accepts a document.

use std::sync::OnceLock;

use serde::Serialize;
use serde_json::Value;

use crate::canonical::verify_content_id;
use crate::error::SignatureError;

/// The envelope schema of `ibtnm.target-revision` 1.1.0, copied from gridloop
/// (`schemas/gridloop/PROVENANCE.md`).
const TARGET_REVISION_SCHEMA: &str =
    include_str!("../../../schemas/gridloop/target-revision-1.1.0.schema.json");

/// A finding of the validation. Findings are sorted by code, then by pointer, so that two
/// runs on the same input give the same bytes (ACC-AST-002 style determinism).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Finding {
    /// The validation code, for example `PMV-ENV-001`.
    pub code: &'static str,
    /// JSON pointer of the element in the document, `""` for the whole document.
    pub pointer: String,
    /// What is wrong, in plain words.
    pub message: String,
}

/// Validates the envelope and the content identifier of a revision document.
///
/// # Panics
///
/// Panics only if the embedded schema is not valid JSON, which the build would have shown.
#[must_use]
pub fn validate_stage1(document: &Value) -> Vec<Finding> {
    let mut findings = Vec::new();
    match schema_validator() {
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
    match verify_content_id("revision", document) {
        Ok(()) => {}
        Err(SignatureError::ContentId { .. }) => findings.push(Finding {
            code: "PMV-ENV-002",
            pointer: "/id".to_owned(),
            message: "the id is not the content identifier of the document".to_owned(),
        }),
        Err(other) => findings.push(Finding {
            code: "PMV-ENV-002",
            pointer: "/id".to_owned(),
            message: other.to_string(),
        }),
    }
    findings.sort();
    findings
}

fn schema_validator() -> Result<&'static jsonschema::Validator, String> {
    static VALIDATOR: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();
    VALIDATOR
        .get_or_init(|| {
            let schema: Value = serde_json::from_str(TARGET_REVISION_SCHEMA)
                .map_err(|e| format!("embedded schema is not JSON: {e}"))?;
            jsonschema::validator_for(&schema).map_err(|e| format!("embedded schema: {e}"))
        })
        .as_ref()
        .map_err(Clone::clone)
}
