//! Verification of the IBTNM signed-plan envelope (`ibtnm.signed-plan` 1.0.0, §10.6).
//!
//! The checks run in the order of gridloop's `verify_signed_plan`, so that the same
//! refusal is reported for the same defect on both sides.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde_json::Value;

use crate::canonical::{canonical_bytes, content_id, sha256_hex, verify_content_id};
use crate::error::SignatureError;

/// A plan whose envelope and content have been verified.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedPlan {
    payload: Value,
}

impl VerifiedPlan {
    /// The verified plan document (the envelope payload).
    #[must_use]
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// The single revision of the plan with identifier `revision_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SignatureError::RevisionNotFound`] unless exactly one artifact has that id.
    pub fn revision(&self, revision_id: &str) -> Result<&Value, SignatureError> {
        let mut matches = self
            .payload
            .get("artifacts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|a| a.get("id").and_then(Value::as_str) == Some(revision_id));
        match (matches.next(), matches.next()) {
            (Some(revision), None) => Ok(revision),
            _ => Err(SignatureError::RevisionNotFound(revision_id.to_owned())),
        }
    }
}

/// The key id of a public key: `ed25519:sha256:<sha256 of the raw key>`.
#[must_use]
pub fn key_id(key: &VerifyingKey) -> String {
    format!("ed25519:sha256:{}", sha256_hex(key.as_bytes()))
}

/// Verifies a signed-plan envelope against the trusted public key.
///
/// # Errors
///
/// Returns the first [`SignatureError`] found, in gridloop's order of checks.
pub fn verify_signed_plan(
    envelope: &Value,
    trusted_key: &VerifyingKey,
) -> Result<VerifiedPlan, SignatureError> {
    if envelope.get("artifact_type").and_then(Value::as_str) != Some("ibtnm.signed-plan") {
        return Err(SignatureError::Envelope);
    }
    // The envelope is a closed object, and only `manifest`, `payload` and `signature` are
    // covered by the signature. `artifact_type` and `schema_version` are therefore checked
    // here, and any other member is refused (gridloop's check leaves `schema_version` open).
    let members = envelope.as_object().ok_or(SignatureError::Envelope)?;
    let allowed = [
        "artifact_type",
        "manifest",
        "payload",
        "schema_version",
        "signature",
    ];
    if members.len() != allowed.len()
        || members.keys().any(|k| !allowed.contains(&k.as_str()))
        || envelope.get("schema_version").and_then(Value::as_str) != Some("1.0.0")
    {
        return Err(SignatureError::Envelope);
    }
    let manifest = envelope.get("manifest").ok_or(SignatureError::Envelope)?;
    let payload = envelope.get("payload").ok_or(SignatureError::Envelope)?;
    let signature_text = envelope
        .get("signature")
        .and_then(Value::as_str)
        .ok_or(SignatureError::Envelope)?;
    if !manifest.is_object() || !payload.is_object() {
        return Err(SignatureError::Envelope);
    }

    if manifest.get("algorithm").and_then(Value::as_str) != Some("Ed25519")
        || manifest.get("key_id").and_then(Value::as_str) != Some(key_id(trusted_key).as_str())
    {
        return Err(SignatureError::KeyMismatch);
    }

    let signature_bytes = STANDARD
        .decode(signature_text)
        .map_err(|_| SignatureError::Signature)?;
    let signature =
        Signature::from_slice(&signature_bytes).map_err(|_| SignatureError::Signature)?;
    trusted_key
        .verify(&canonical_bytes(manifest)?, &signature)
        .map_err(|_| SignatureError::Signature)?;

    if manifest.get("plan_id") != payload.get("id") {
        return Err(SignatureError::PlanReference);
    }
    let plan_digest = sha256_hex(&canonical_bytes(payload)?);
    if manifest.get("plan_sha256").and_then(Value::as_str) != Some(plan_digest.as_str()) {
        return Err(SignatureError::PlanDigest);
    }

    let manifest_runtime = manifest.get("runtime_capabilities_sha256");
    let plan_runtime = payload
        .get("sources")
        .and_then(|s| s.get("runtime_capabilities_sha256"));
    if manifest_runtime != plan_runtime {
        return Err(SignatureError::RuntimeCapabilities);
    }

    verify_content_id("plan", payload)?;

    let expected_artifacts = expected_artifact_list(payload)?;
    if manifest.get("artifacts") != Some(&expected_artifacts) {
        return Err(SignatureError::ArtifactList);
    }

    for artifact in payload
        .get("artifacts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        verify_content_id("revision", artifact)?;
    }

    Ok(VerifiedPlan {
        payload: payload.clone(),
    })
}

/// The `{id, sha256}` list of the plan's artifacts, sorted by id, as the manifest must carry it.
fn expected_artifact_list(payload: &Value) -> Result<Value, SignatureError> {
    let mut artifacts: Vec<&Value> = payload
        .get("artifacts")
        .and_then(Value::as_array)
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    artifacts.sort_by(|a, b| {
        let ia = a.get("id").and_then(Value::as_str).unwrap_or("");
        let ib = b.get("id").and_then(Value::as_str).unwrap_or("");
        ia.cmp(ib)
    });
    let mut list = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let id = artifact.get("id").cloned().unwrap_or(Value::Null);
        let digest = sha256_hex(&canonical_bytes(artifact)?);
        list.push(serde_json::json!({ "id": id, "sha256": digest }));
    }
    Ok(Value::Array(list))
}

/// The content identifier of a plan, for callers that need it without verifying the envelope.
///
/// # Errors
///
/// Returns [`SignatureError::Canonical`] if the plan cannot be canonicalized.
pub fn plan_content_id(payload: &Value) -> Result<String, SignatureError> {
    content_id("plan", payload)
}
