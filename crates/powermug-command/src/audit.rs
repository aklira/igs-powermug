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

/// What the verification of a chain can report.
#[derive(Debug, Error)]
pub(crate) enum VerifyError {
    #[error(transparent)]
    Broken(#[from] ChainBroken),
    #[error("database error: {0}")]
    Database(rusqlite::Error),
}
