//! PM4 on documents: the strict loader refuses what a standard parser would hide.

use powermug_revision::{parse_strict_json, LoadError};

#[test]
fn a_duplicate_key_is_refused_with_its_path() {
    let err = parse_strict_json(r#"{"a":{"x":1,"x":2}}"#).expect_err("duplicate refused");
    assert_eq!(
        err,
        LoadError::DuplicateKey {
            key: "x".into(),
            path: "/a".into()
        }
    );
}

#[test]
fn an_integer_beyond_the_safe_range_is_refused() {
    let err = parse_strict_json(r#"{"n":9007199254740993}"#).expect_err("unsafe integer refused");
    assert!(matches!(err, LoadError::UnsafeInteger { ref path, .. } if path == "/n"));
}

#[test]
fn the_safe_limit_is_accepted() {
    assert!(parse_strict_json(r#"{"n":9007199254740991,"m":-9007199254740991}"#).is_ok());
}

#[test]
fn trailing_content_is_refused() {
    assert!(matches!(
        parse_strict_json(r#"{"a":1} {"b":2}"#),
        Err(LoadError::Syntax(_))
    ));
}

#[test]
fn an_ordinary_document_loads_unchanged() {
    let value = parse_strict_json(r#"{"b":[1,2.5,"é",null,true],"a":{}}"#).expect("loads");
    assert_eq!(value["b"][1], 2.5);
    assert_eq!(value["a"], serde_json::json!({}));
}
