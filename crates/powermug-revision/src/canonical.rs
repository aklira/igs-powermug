//! RFC 8785 canonical JSON and content identifiers (ADR-16).
//!
//! An identifier is `<kind>:sha256:<hex>`, where the digest covers the canonical form of
//! the document with its `id` member removed. This is gridloop's `artifact_id`.

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::SignatureError;

/// The canonical (RFC 8785) UTF-8 bytes of a JSON value.
///
/// # Errors
///
/// Returns [`SignatureError::Canonical`] if the value cannot be canonicalized.
pub fn canonical_bytes(value: &Value) -> Result<Vec<u8>, SignatureError> {
    serde_json_canonicalizer::to_vec(value).map_err(|e| SignatureError::Canonical(e.to_string()))
}

/// The lowercase hexadecimal SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    digest.iter().fold(String::with_capacity(64), |mut out, b| {
        // Writing to a String cannot fail.
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// The content identifier `<kind>:sha256:<hex>` of a document, excluding its `id` member.
///
/// # Errors
///
/// Returns [`SignatureError::Canonical`] if the document cannot be canonicalized.
pub fn content_id(kind: &str, document: &Value) -> Result<String, SignatureError> {
    let mut content = document.clone();
    if let Value::Object(map) = &mut content {
        map.remove("id");
    }
    let bytes = canonical_bytes(&content)?;
    Ok(format!("{kind}:sha256:{}", sha256_hex(&bytes)))
}

/// Checks that the `id` member of `document` equals its content identifier for `kind`.
///
/// # Errors
///
/// Returns [`SignatureError::ContentId`] when the identifier does not match.
pub fn verify_content_id(kind: &str, document: &Value) -> Result<(), SignatureError> {
    let expected = content_id(kind, document)?;
    match document.get("id").and_then(Value::as_str) {
        Some(id) if id == expected => Ok(()),
        _ => Err(SignatureError::ContentId {
            kind: kind.to_owned(),
        }),
    }
}
