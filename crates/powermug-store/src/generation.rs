//! The critical store's generation pointer (SPECIFICATIONS §10.5, PM-CFG-002 to
//! PM-CFG-006).
//!
//! A revision is prepared under its id, then activated: the activation switches
//! the active pointer of its scope in one transaction, after checking that the
//! revision was prepared against the active one (optimistic concurrency,
//! PM-CFG-003). Preparing and rolling back are idempotent per revision id. The
//! store lives in its own file, apart from the telemetry log (PM-TEL-006).

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use thiserror::Error;

/// The critical store could not do what was asked.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GenerationError {
    #[error("revision {0} is already prepared for another scope")]
    ConflictingRevision(String),
    #[error("revision {0} is not prepared")]
    UnknownRevision(String),
    #[error("revision {revision} is for scope {scope}, not {requested}")]
    WrongScope {
        revision: String,
        scope: String,
        requested: String,
    },
    #[error("revision was prepared against {found:?}, but {expected:?} is active (409)")]
    ParentMismatch {
        expected: Option<String>,
        found: Option<String>,
    },
    #[error("nothing to roll back to in scope {0}")]
    NothingToRollBack(String),
    #[error("the critical store failed: {0}")]
    Database(String),
}

impl From<rusqlite::Error> for GenerationError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Database(err.to_string())
    }
}

/// The active and previous revision of one scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeState {
    pub scope: String,
    pub active: Option<String>,
    pub previous: Option<String>,
}

/// The status of one revision: its scope, its lifecycle state and its parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionStatus {
    pub id: String,
    pub scope: String,
    /// `PREPARED`, `ACTIVE`, `SUPERSEDED` or `ROLLED_BACK`.
    pub state: String,
    pub parent: Option<String>,
}

/// What a call changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Changed,
    /// The revision was already in the requested state: nothing was done.
    Unchanged,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS revisions (
    id      TEXT PRIMARY KEY,
    scope   TEXT NOT NULL,
    parent  TEXT,
    body    BLOB NOT NULL,
    state   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS generation (
    scope    TEXT PRIMARY KEY,
    active   TEXT,
    previous TEXT
);
";

/// The critical store: revisions and the generation pointer of each scope.
#[derive(Debug)]
pub struct CriticalStore {
    conn: Connection,
}

impl CriticalStore {
    /// Opens or creates the store at `path`, in WAL mode with `synchronous=FULL`.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Database`] when the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, GenerationError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Stores a revision as prepared. Preparing the same revision again for the
    /// same scope changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::ConflictingRevision`] when the id exists for
    /// another scope.
    pub fn prepare(
        &mut self,
        id: &str,
        scope: &str,
        parent: Option<&str>,
        body: &[u8],
    ) -> Result<Outcome, GenerationError> {
        if let Some(existing) = self.revision_scope(id)? {
            return if existing == scope {
                Ok(Outcome::Unchanged)
            } else {
                Err(GenerationError::ConflictingRevision(id.to_owned()))
            };
        }
        self.conn.execute(
            "INSERT INTO revisions (id, scope, parent, body, state) VALUES (?1, ?2, ?3, ?4, 'PREPARED')",
            params![id, scope, parent, body],
        )?;
        Ok(Outcome::Changed)
    }

    /// Makes a prepared revision the active one of its scope, in one transaction.
    /// Activating the active revision again changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::UnknownRevision`] when the revision is not
    /// prepared, [`GenerationError::WrongScope`] for another scope, and
    /// [`GenerationError::ParentMismatch`] when the revision was prepared against
    /// a revision that is no longer the active one.
    pub fn activate(&mut self, id: &str, scope: &str) -> Result<Outcome, GenerationError> {
        let tx = self.conn.transaction()?;
        let (revision_scope, parent) = revision_row(&tx, id)?;
        if revision_scope != scope {
            return Err(GenerationError::WrongScope {
                revision: id.to_owned(),
                scope: revision_scope,
                requested: scope.to_owned(),
            });
        }
        let active = active_of(&tx, scope)?;
        if active.as_deref() == Some(id) {
            return Ok(Outcome::Unchanged);
        }
        if parent != active {
            return Err(GenerationError::ParentMismatch {
                expected: active,
                found: parent,
            });
        }
        tx.execute(
            "INSERT INTO generation (scope, active, previous) VALUES (?1, ?2, ?3)
             ON CONFLICT(scope) DO UPDATE SET previous = generation.active, active = excluded.active",
            params![scope, id, active],
        )?;
        if let Some(old) = &active {
            tx.execute(
                "UPDATE revisions SET state = 'SUPERSEDED' WHERE id = ?1",
                params![old],
            )?;
        }
        tx.execute(
            "UPDATE revisions SET state = 'ACTIVE' WHERE id = ?1",
            params![id],
        )?;
        tx.commit()?;
        Ok(Outcome::Changed)
    }

    /// Rolls back `from`, the active revision of `scope`, to the one before it.
    /// If `from` is not active any more, nothing is done.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::NothingToRollBack`] when there is no earlier
    /// revision to return to.
    pub fn rollback(&mut self, scope: &str, from: &str) -> Result<Outcome, GenerationError> {
        let tx = self.conn.transaction()?;
        let active = active_of(&tx, scope)?;
        if active.as_deref() != Some(from) {
            return Ok(Outcome::Unchanged);
        }
        let previous: Option<String> = tx
            .query_row(
                "SELECT previous FROM generation WHERE scope = ?1",
                params![scope],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let Some(previous) = previous else {
            return Err(GenerationError::NothingToRollBack(scope.to_owned()));
        };
        tx.execute(
            "UPDATE generation SET active = ?2, previous = NULL WHERE scope = ?1",
            params![scope, previous],
        )?;
        tx.execute(
            "UPDATE revisions SET state = 'ROLLED_BACK' WHERE id = ?1",
            params![from],
        )?;
        tx.execute(
            "UPDATE revisions SET state = 'ACTIVE' WHERE id = ?1",
            params![previous],
        )?;
        tx.commit()?;
        Ok(Outcome::Changed)
    }

    /// The active revision of `scope`, if any.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Database`] when the read fails.
    pub fn active(&self, scope: &str) -> Result<Option<String>, GenerationError> {
        Ok(self
            .conn
            .query_row(
                "SELECT active FROM generation WHERE scope = ?1",
                params![scope],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    /// The active and the previous revision of every scope that has one.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Database`] when the read fails.
    pub fn scopes(&self) -> Result<Vec<ScopeState>, GenerationError> {
        let mut statement = self
            .conn
            .prepare("SELECT scope, active, previous FROM generation ORDER BY scope")?;
        let rows = statement.query_map([], |row| {
            Ok(ScopeState {
                scope: row.get(0)?,
                active: row.get(1)?,
                previous: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The stored body of a prepared revision: the canonical bytes of its document.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Database`] when the read fails.
    pub fn body(&self, id: &str) -> Result<Option<Vec<u8>>, GenerationError> {
        Ok(self
            .conn
            .query_row(
                "SELECT body FROM revisions WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?)
    }

    /// The scope, lifecycle state and parent of a revision, if it is known.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Database`] when the read fails.
    pub fn revision(&self, id: &str) -> Result<Option<RevisionStatus>, GenerationError> {
        Ok(self
            .conn
            .query_row(
                "SELECT scope, state, parent FROM revisions WHERE id = ?1",
                params![id],
                |row| {
                    Ok(RevisionStatus {
                        id: id.to_owned(),
                        scope: row.get(0)?,
                        state: row.get(1)?,
                        parent: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    fn revision_scope(&self, id: &str) -> Result<Option<String>, GenerationError> {
        Ok(self
            .conn
            .query_row(
                "SELECT scope FROM revisions WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?)
    }
}

fn revision_row(
    tx: &Transaction<'_>,
    id: &str,
) -> Result<(String, Option<String>), GenerationError> {
    tx.query_row(
        "SELECT scope, parent FROM revisions WHERE id = ?1",
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()?
    .ok_or_else(|| GenerationError::UnknownRevision(id.to_owned()))
}

fn active_of(tx: &Transaction<'_>, scope: &str) -> Result<Option<String>, GenerationError> {
    Ok(tx
        .query_row(
            "SELECT active FROM generation WHERE scope = ?1",
            params![scope],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten())
}
