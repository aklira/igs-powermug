//! The hash-chained audit records of the critical store (SPECIFICATIONS 9.2, NFR-SEC-008).
//!
//! Each record carries the SHA-256 of the previous record, so that changing or removing a
//! record breaks the chain from that point on. The chain does not show that its last records
//! were removed: the head must be anchored outside the store (see [`head`]), and the records
//! exported to a separate domain.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::journal::CommandError;

pub(crate) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS audit (
    seq         INTEGER PRIMARY KEY,
    at_ns       INTEGER NOT NULL,
    kind        TEXT    NOT NULL,
    subject     TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    prev_hash   BLOB    NOT NULL CHECK (length(prev_hash) = 32),
    hash        BLOB    NOT NULL CHECK (length(hash) = 32)
);
";

/// The hash before the first record.
pub const GENESIS: [u8; 32] = [0; 32];

/// A hash as 64 lowercase hex characters: the form an anchor or an export carries.
#[must_use]
pub fn to_hex(hash: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    hash.iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// Reads a hash written by [`to_hex`]. Returns `None` for any other text.
#[must_use]
pub fn from_hex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut hash = [0_u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        let digits = std::str::from_utf8(pair).ok()?;
        hash[index] = u8::from_str_radix(digits, 16).ok()?;
    }
    Some(hash)
}

/// The chain does not verify: the record at `seq` is altered, or its predecessor is.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the audit chain breaks at record {seq}")]
pub struct ChainBroken {
    pub seq: i64,
}

fn digest(
    prev: &[u8; 32],
    seq: i64,
    at_ns: i64,
    kind: &str,
    subject: &str,
    payload: &str,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(prev);
    hasher.update(seq.to_be_bytes());
    hasher.update(at_ns.to_be_bytes());
    for text in [kind, subject, payload] {
        hasher.update((text.len() as u64).to_be_bytes());
        hasher.update(text.as_bytes());
    }
    hasher.finalize().into()
}

fn hash_of(row: &[u8]) -> Result<[u8; 32], CommandError> {
    <[u8; 32]>::try_from(row)
        .map_err(|_| CommandError::Database("audit hash is not 32 bytes".into()))
}

/// Appends a record to the chain, inside the caller's transaction.
pub(crate) fn append(
    tx: &Transaction<'_>,
    kind: &str,
    subject: &str,
    payload: &str,
    at_ns: i64,
) -> Result<(), CommandError> {
    let last: Option<(i64, Vec<u8>)> = tx
        .query_row(
            "SELECT seq, hash FROM audit ORDER BY seq DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (seq, prev) = match last {
        Some((seq, hash)) => (seq + 1, hash_of(&hash)?),
        None => (1, GENESIS),
    };
    let hash = digest(&prev, seq, at_ns, kind, subject, payload);
    tx.execute(
        "INSERT INTO audit (seq, at_ns, kind, subject, payload, prev_hash, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            seq,
            at_ns,
            kind,
            subject,
            payload,
            prev.as_slice(),
            hash.as_slice()
        ],
    )?;
    Ok(())
}

/// Checks the whole chain: every record's hash, and that each record names its predecessor.
/// Returns the number of records.
///
/// # Errors
///
/// Returns [`ChainBroken`] at the first record that does not verify.
pub(crate) fn verify(conn: &Connection) -> Result<u64, VerifyError> {
    let mut statement = conn
        .prepare(
            "SELECT seq, at_ns, kind, subject, payload, prev_hash, hash FROM audit ORDER BY seq",
        )
        .map_err(VerifyError::Database)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Vec<u8>>(6)?,
            ))
        })
        .map_err(VerifyError::Database)?;
    let mut expected = GENESIS;
    let mut count = 0;
    for row in rows {
        let (seq, at_ns, kind, subject, payload, prev, hash) =
            row.map_err(VerifyError::Database)?;
        let broken = || VerifyError::Broken(ChainBroken { seq });
        let prev = hash_of(&prev).map_err(|_| broken())?;
        let hash = hash_of(&hash).map_err(|_| broken())?;
        if prev != expected || digest(&prev, seq, at_ns, &kind, &subject, &payload) != hash {
            return Err(broken());
        }
        expected = hash;
        count += 1;
    }
    Ok(count)
}

/// The hash of the last record, or [`GENESIS`] for an empty chain. Anchoring it outside the
/// store shows that records were not removed from the end.
pub(crate) fn head(conn: &Connection) -> Result<[u8; 32], CommandError> {
    let last: Option<Vec<u8>> = conn
        .query_row(
            "SELECT hash FROM audit ORDER BY seq DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    last.map_or(Ok(GENESIS), |hash| hash_of(&hash))
}

/// One record of the chain, as exported to a separate domain (NFR-SEC-008). The export carries
/// the hashes, so that a verifier needs nothing but the export and an anchored head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedRecord {
    pub seq: i64,
    pub at_ns: i64,
    pub kind: String,
    pub subject: String,
    pub payload: String,
    pub prev_hash: [u8; 32],
    pub hash: [u8; 32],
}

/// Reads the whole chain, in order, for its export.
pub(crate) fn export(conn: &Connection) -> Result<Vec<ExportedRecord>, CommandError> {
    let mut statement = conn.prepare(
        "SELECT seq, at_ns, kind, subject, payload, prev_hash, hash FROM audit ORDER BY seq",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Vec<u8>>(5)?,
            row.get::<_, Vec<u8>>(6)?,
        ))
    })?;
    let mut records = Vec::new();
    for row in rows {
        let (seq, at_ns, kind, subject, payload, prev, hash) = row?;
        records.push(ExportedRecord {
            seq,
            at_ns,
            kind,
            subject,
            payload,
            prev_hash: hash_of(&prev)?,
            hash: hash_of(&hash)?,
        });
    }
    Ok(records)
}

/// An exported chain does not verify on its own.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExportVerifyError {
    /// The record at `seq` is altered, out of place, or does not follow its predecessor.
    #[error("the exported chain breaks at record {0}")]
    Broken(i64),
    /// The chain is consistent, but it does not end at the anchored head: records were
    /// removed from its end, or the anchor is not the head of this chain.
    #[error("the exported chain does not end at the anchored head")]
    AnchorMismatch,
}

/// Verifies an exported chain without the store: every record's hash and its link to its
/// predecessor, from the genesis, and, when `anchor` is given, that the chain ends at it.
/// Returns the number of records.
///
/// # Errors
///
/// Returns [`ExportVerifyError::Broken`] at the first record that does not verify, and
/// [`ExportVerifyError::AnchorMismatch`] when the chain does not end at `anchor`.
pub fn verify_export(
    records: &[ExportedRecord],
    anchor: Option<[u8; 32]>,
) -> Result<u64, ExportVerifyError> {
    let mut expected = GENESIS;
    for (index, record) in records.iter().enumerate() {
        let position = i64::try_from(index + 1).unwrap_or(i64::MAX);
        if record.seq != position
            || record.prev_hash != expected
            || digest(
                &record.prev_hash,
                record.seq,
                record.at_ns,
                &record.kind,
                &record.subject,
                &record.payload,
            ) != record.hash
        {
            return Err(ExportVerifyError::Broken(record.seq));
        }
        expected = record.hash;
    }
    if anchor.is_some_and(|anchored| anchored != expected) {
        return Err(ExportVerifyError::AnchorMismatch);
    }
    Ok(u64::try_from(records.len()).unwrap_or(u64::MAX))
}

/// What the verification of a chain can report.
#[derive(Debug, Error)]
pub(crate) enum VerifyError {
    #[error(transparent)]
    Broken(#[from] ChainBroken),
    #[error("database error: {0}")]
    Database(rusqlite::Error),
}
