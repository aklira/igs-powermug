//! PM-ACC-003: a plan signed by gridloop's signing service is accepted, and any alteration
//! of the envelope is refused.

use std::fs;
use std::path::PathBuf;

use ed25519_dalek::pkcs8::DecodePublicKey;
use ed25519_dalek::VerifyingKey;
use powermug_revision::{parse_strict_json, verify_signed_plan, SignatureError};
use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/signed-plan")
        .join(name)
}

fn trusted_key() -> VerifyingKey {
    let pem = fs::read_to_string(fixture("public.pem")).expect("public key present");
    VerifyingKey::from_public_key_pem(&pem).expect("Ed25519 PEM key")
}

fn envelope_text() -> String {
    fs::read_to_string(fixture("envelope.json")).expect("envelope present")
}

fn revision_ids() -> Value {
    let text = fs::read_to_string(fixture("revisions.json")).expect("ids present");
    parse_strict_json(&text).expect("ids are strict JSON")
}

#[test]
fn gridloop_signed_plan_is_accepted_and_yields_each_revision() {
    let envelope = parse_strict_json(&envelope_text()).expect("strict JSON");
    let plan = verify_signed_plan(&envelope, &trusted_key()).expect("signed plan accepted");
    let ids = revision_ids();
    for site in ["S3", "S4"] {
        let id = ids[site].as_str().expect("id string");
        let revision = plan.revision(id).expect("revision present once");
        assert_eq!(revision["site"], site);
    }
}

#[test]
fn an_unknown_revision_is_refused() {
    let envelope = parse_strict_json(&envelope_text()).expect("strict JSON");
    let plan = verify_signed_plan(&envelope, &trusted_key()).expect("signed plan accepted");
    let missing = "revision:sha256:".to_owned() + &"0".repeat(64);
    assert!(matches!(
        plan.revision(&missing),
        Err(SignatureError::RevisionNotFound(_))
    ));
}

#[test]
fn every_single_byte_alteration_of_the_envelope_is_refused() {
    let key = trusted_key();
    let original = envelope_text();
    let bytes = original.as_bytes();
    let mut tested = 0usize;
    for index in 0..bytes.len() {
        let original_byte = bytes[index];
        // Only alphanumeric bytes are changed: the result stays readable, and any change
        // reaches the canonical manifest, payload or signature.
        if !original_byte.is_ascii_alphanumeric() {
            continue;
        }
        let replacement = if original_byte == b'a' { b'b' } else { b'a' };
        let mut altered = bytes.to_vec();
        altered[index] = replacement;
        let text = String::from_utf8(altered).expect("ASCII stays ASCII");
        let accepted = parse_strict_json(&text)
            .ok()
            .and_then(|envelope| verify_signed_plan(&envelope, &key).ok())
            .is_some();
        assert!(
            !accepted,
            "byte {index} altered from {original_byte} to {replacement} was accepted"
        );
        tested += 1;
    }
    assert!(
        tested > 1000,
        "the envelope has enough alterable bytes to be meaningful, got {tested}"
    );
}

#[test]
fn a_different_trusted_key_is_refused() {
    let envelope = parse_strict_json(&envelope_text()).expect("strict JSON");
    let other = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]).verifying_key();
    assert_eq!(
        verify_signed_plan(&envelope, &other),
        Err(SignatureError::KeyMismatch)
    );
}
