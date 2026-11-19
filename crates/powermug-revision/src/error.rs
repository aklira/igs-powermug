//! Errors of loading and verification.

use thiserror::Error;

/// A document refused by the strict loader (PM4, §10.1).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LoadError {
    /// The text is not valid JSON.
    #[error("invalid JSON: {0}")]
    Syntax(String),
    /// The same key appears twice in one object.
    #[error("duplicate key `{key}` at {path}")]
    DuplicateKey {
        /// The repeated key.
        key: String,
        /// JSON pointer of the object that repeats it.
        path: String,
    },
    /// An integer lies outside the I-JSON safe range (±2^53 − 1), so its value is ambiguous.
    #[error("integer {value} at {path} is outside the I-JSON safe range")]
    UnsafeInteger {
        /// The integer as written.
        value: String,
        /// JSON pointer of the value.
        path: String,
    },
    /// A number is not finite.
    #[error("non-finite number at {path}")]
    NonFinite {
        /// JSON pointer of the value.
        path: String,
    },
}

/// A signed plan refused by verification (§10.6). The messages follow gridloop's wording.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SignatureError {
    /// The envelope is not an `ibtnm.signed-plan` document or lacks a member.
    #[error("signed plan envelope absent or incomplete")]
    Envelope,
    /// The public key is not a valid Ed25519 key.
    #[error("public key is not an Ed25519 key")]
    PublicKey,
    /// The algorithm or key id does not match the trusted key.
    #[error("unexpected key or algorithm")]
    KeyMismatch,
    /// The Ed25519 signature does not verify over the canonical manifest.
    #[error("invalid Ed25519 signature")]
    Signature,
    /// The manifest does not reference the plan it carries.
    #[error("manifest does not reference the plan it carries")]
    PlanReference,
    /// The plan digest in the manifest does not match the payload.
    #[error("plan digest does not match the manifest")]
    PlanDigest,
    /// The runtime capabilities digest differs between manifest and plan.
    #[error("runtime capabilities digest does not match the plan")]
    RuntimeCapabilities,
    /// A content identifier does not match the content.
    #[error("invalid content identifier for {kind}")]
    ContentId {
        /// The artifact kind whose identifier is wrong.
        kind: String,
    },
    /// The artifact list in the manifest does not match the plan.
    #[error("artifact list does not match the plan")]
    ArtifactList,
    /// The requested revision is not in the plan, or appears more than once.
    #[error("revision {0} is not in the plan exactly once")]
    RevisionNotFound(String),
    /// A value is not valid canonical JSON input.
    #[error("canonicalization failed: {0}")]
    Canonical(String),
}
