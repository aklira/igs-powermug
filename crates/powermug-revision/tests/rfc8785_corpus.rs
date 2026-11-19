//! PM-ACC-002: the RFC 8785 output of this crate equals gridloop's `rfc8785` output
//! on the committed corpus of 10,000 documents (tests/fixtures/rfc8785/corpus.jsonl).

use std::fs;
use std::path::PathBuf;

use powermug_revision::canonical::canonical_bytes;
use powermug_revision::parse_strict_json;
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    input: String,
    canonical: String,
}

fn corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/rfc8785/corpus.jsonl")
}

#[test]
fn canonical_form_matches_gridloop_on_10000_documents() {
    let text = fs::read_to_string(corpus_path()).expect("corpus present");
    let mut count = 0usize;
    let mut mismatches = Vec::new();
    for (line_no, line) in text.lines().enumerate() {
        let case: Case = serde_json::from_str(line).expect("corpus line is valid JSON");
        let document = parse_strict_json(&case.input).unwrap_or_else(|e| {
            panic!(
                "line {}: strict loader refused a corpus document: {e}",
                line_no + 1
            )
        });
        let ours = String::from_utf8(canonical_bytes(&document).expect("canonicalizes"))
            .expect("canonical form is UTF-8");
        if ours != case.canonical {
            mismatches.push((
                line_no + 1,
                case.input.clone(),
                ours,
                case.canonical.clone(),
            ));
        }
        count += 1;
    }
    assert_eq!(count, 10_000, "the corpus holds 10,000 documents");
    if let Some((line, input, ours, theirs)) = mismatches.first() {
        panic!(
            "{} mismatches; first at line {line}:\n input:  {input}\n ours:   {ours}\n gridloop: {theirs}",
            mismatches.len()
        );
    }
}
