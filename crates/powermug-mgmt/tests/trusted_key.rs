//! The trusted key of signed plans, read from its PEM file.

use ed25519_dalek::pkcs8::{spki::der::pem::LineEnding, EncodePublicKey};
use ed25519_dalek::SigningKey;
use powermug_mgmt::{load_trusted_key, KeyError};

#[test]
fn a_public_key_pem_file_is_loaded_as_the_trusted_key() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("trusted.pub.pem");
    let key = SigningKey::from_bytes(&[7u8; 32]).verifying_key();
    std::fs::write(
        &path,
        key.to_public_key_pem(LineEnding::LF).expect("encodes"),
    )
    .expect("writes");

    assert_eq!(load_trusted_key(&path).expect("loads"), key);
}

#[test]
fn a_file_that_is_not_a_public_key_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("not-a-key.pem");
    std::fs::write(&path, "hello").expect("writes");

    assert!(matches!(load_trusted_key(&path), Err(KeyError::Pem(_))));
    assert!(matches!(
        load_trusted_key(&dir.path().join("missing.pem")),
        Err(KeyError::Read(_))
    ));
}
