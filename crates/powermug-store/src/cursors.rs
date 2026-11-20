//! Destination cursors and retention of the telemetry log (SPECIFICATIONS §7.2, §7.4;
//! PM-TEL-004; FUN-SAF-007).
//!
//! A cursor records the last sequence number a destination acknowledged. A flux destination
//! replays from its cursor when it falls behind. Retention deletes events by quota and by
//! age, per class. Class A is deleted only once every flux cursor has passed it; while no
//! flux cursor exists, no class A event is deleted by retention. Critical events are never
//! subject to retention. Deleted events are counted per class.

use rusqlite::{params, OptionalExtension};

use crate::log::{EventClass, StoreError, StoredEvent, TelemetryLog};

pub(crate) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS cursors (
    destination TEXT    PRIMARY KEY,
    flux        INTEGER NOT NULL CHECK (flux IN (0, 1)),
    last_seq    INTEGER NOT NULL,
    updated_ns  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS retention (
    class       INTEGER PRIMARY KEY,
    max_events  INTEGER,
    max_age_ns  INTEGER,
    dropped     INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS retention_audit (
    id          INTEGER PRIMARY KEY,
    at_ns       INTEGER NOT NULL,
    class       INTEGER NOT NULL,
    deleted     INTEGER NOT NULL,
    max_events  INTEGER,
    max_age_ns  INTEGER,
    floor_seq   INTEGER
);
CREATE INDEX IF NOT EXISTS events_class_seq ON events (class, seq);
";

/// Where a new destination starts (PM-TEL-004).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// The events journaled from now on.
    Now,
    /// The oldest event still retained.
    OldestRetained,
    /// The first event received at or after this time, in nanoseconds since the epoch.
    Timestamp(i64),
}

/// The state of one destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub destination: String,
    /// A flux destination takes part in the protection of class A.
    pub flux: bool,
    /// The last sequence number acknowledged. Zero before any event.
    pub last_seq: i64,
    pub updated_ns: i64,
}

/// The events a destination has not acknowledged, read from its cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replay {
    pub events: Vec<StoredEvent>,
    /// When retention deleted events the destination had not acknowledged, the oldest
    /// sequence number still retained: the destination is told of the gap before it.
    pub skipped_before: Option<i64>,
}

/// The retention policy of one class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// The newest events of the class to keep, at most.
    pub max_events: Option<u64>,
    /// The age beyond which an event of the class is deleted, in nanoseconds.
    pub max_age_ns: Option<i64>,
}

/// One deletion recorded by retention, with the policy in effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionAudit {
    pub id: i64,
    pub at_ns: i64,
    pub class: EventClass,
    pub deleted: u64,
    pub max_events: Option<u64>,
    pub max_age_ns: Option<i64>,
    /// The flux cursors' floor that bounded the deletion: class A only.
    pub floor_seq: Option<i64>,
}

/// What one enforcement deleted from one class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Enforced {
    pub class: EventClass,
    pub deleted: u64,
    /// The oldest sequence number of the class still retained, if any.
    pub oldest_seq: Option<i64>,
}

fn class_code(class: EventClass) -> i64 {
    match class {
        EventClass::A => 1,
        EventClass::B => 2,
        EventClass::C => 3,
        EventClass::Critical => 4,
    }
}

impl TelemetryLog {
    /// Declares a destination. An existing destination is returned as it is, when its kind
    /// matches.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::CursorConflict`] when the destination exists with another kind,
    /// and [`StoreError::Database`] on a failure.
    pub fn declare_cursor(
        &mut self,
        destination: &str,
        flux: bool,
        start: Start,
        now_ns: i64,
    ) -> Result<Cursor, StoreError> {
        if let Some(existing) = self.cursor(destination)? {
            if existing.flux != flux {
                return Err(StoreError::CursorConflict(destination.to_owned()));
            }
            return Ok(existing);
        }
        let last = self.last_seq()?.unwrap_or(0);
        let last_seq = match start {
            Start::Now => last,
            Start::OldestRetained => self.oldest_seq()?.map_or(last, |oldest| oldest - 1),
            Start::Timestamp(ts) => {
                let first_at_or_after: Option<i64> = self
                    .conn
                    .query_row(
                        "SELECT seq FROM events WHERE ts_receive >= ?1 ORDER BY seq LIMIT 1",
                        params![ts],
                        |row| row.get(0),
                    )
                    .optional()?;
                first_at_or_after.map_or(last, |seq| seq - 1)
            }
        };
        self.conn.execute(
            "INSERT INTO cursors (destination, flux, last_seq, updated_ns)
             VALUES (?1, ?2, ?3, ?4)",
            params![destination, i64::from(flux), last_seq, now_ns],
        )?;
        Ok(Cursor {
            destination: destination.to_owned(),
            flux,
            last_seq,
            updated_ns: now_ns,
        })
    }

    /// The state of a destination, if it is declared.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] on a read failure.
    pub fn cursor(&self, destination: &str) -> Result<Option<Cursor>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT flux, last_seq, updated_ns FROM cursors WHERE destination = ?1",
                params![destination],
                |row| {
                    Ok(Cursor {
                        destination: destination.to_owned(),
                        flux: row.get::<_, i64>(0)? == 1,
                        last_seq: row.get(1)?,
                        updated_ns: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    /// Records that `destination` has acknowledged every event up to `seq`. The cursor
    /// never moves backwards.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::UnknownCursor`] for an undeclared destination, and
    /// [`StoreError::AheadOfLog`] for a sequence number not yet journaled.
    pub fn acknowledge(
        &mut self,
        destination: &str,
        seq: i64,
        now_ns: i64,
    ) -> Result<Cursor, StoreError> {
        let current = self
            .cursor(destination)?
            .ok_or_else(|| StoreError::UnknownCursor(destination.to_owned()))?;
        let last = self.last_seq()?.unwrap_or(0);
        if seq > last {
            return Err(StoreError::AheadOfLog { seq, last });
        }
        let new_last = current.last_seq.max(seq);
        self.conn.execute(
            "UPDATE cursors SET last_seq = ?2, updated_ns = ?3 WHERE destination = ?1",
            params![destination, new_last, now_ns],
        )?;
        Ok(Cursor {
            last_seq: new_last,
            updated_ns: now_ns,
            ..current
        })
    }

    /// Removes a destination. Its cursor no longer protects any event.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] on a failure.
    pub fn remove_cursor(&mut self, destination: &str) -> Result<bool, StoreError> {
        let removed = self.conn.execute(
            "DELETE FROM cursors WHERE destination = ?1",
            params![destination],
        )?;
        Ok(removed > 0)
    }

    /// Reads up to `limit` events after the destination's cursor. The cursor is not moved:
    /// the destination acknowledges what it has applied.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::UnknownCursor`] for an undeclared destination.
    pub fn replay(&self, destination: &str, limit: u32) -> Result<Replay, StoreError> {
        let cursor = self
            .cursor(destination)?
            .ok_or_else(|| StoreError::UnknownCursor(destination.to_owned()))?;
        let from = cursor.last_seq + 1;
        let skipped_before = self.oldest_seq()?.filter(|oldest| *oldest > from);
        Ok(Replay {
            events: self.read_from(from, limit)?,
            skipped_before,
        })
    }

    /// Sets the retention policy of a class.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ProtectedClass`] for class C (not journaled) and for the
    /// critical class, which retention never deletes.
    pub fn set_retention(&mut self, class: EventClass, policy: Policy) -> Result<(), StoreError> {
        match class {
            EventClass::A | EventClass::B => {}
            EventClass::C => return Err(StoreError::ProtectedClass("class C is not journaled")),
            EventClass::Critical => {
                return Err(StoreError::ProtectedClass(
                    "critical events are never deleted by retention",
                ))
            }
        }
        let max_events = policy
            .max_events
            .map(|n| i64::try_from(n).unwrap_or(i64::MAX));
        self.conn.execute(
            "INSERT INTO retention (class, max_events, max_age_ns) VALUES (?1, ?2, ?3)
             ON CONFLICT(class) DO UPDATE SET max_events = excluded.max_events,
                                              max_age_ns = excluded.max_age_ns",
            params![class_code(class), max_events, policy.max_age_ns],
        )?;
        Ok(())
    }

    /// Applies the retention policies at `now_ns`, in one transaction. Returns what each
    /// class lost. A class with no policy is left alone.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] on a failure; nothing is deleted then.
    pub fn enforce_retention(&mut self, now_ns: i64) -> Result<Vec<Enforced>, StoreError> {
        let tx = self.conn.transaction()?;
        let mut outcome = Vec::new();
        for class in [EventClass::A, EventClass::B] {
            let policy: Option<(Option<i64>, Option<i64>)> = tx
                .query_row(
                    "SELECT max_events, max_age_ns FROM retention WHERE class = ?1",
                    params![class_code(class)],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((max_events, max_age_ns)) = policy else {
                continue;
            };
            // Class A is protected by the flux cursors: the least acknowledged one bounds
            // what may be deleted. With no flux cursor, nothing of class A is deleted.
            let floor: i64 = match class {
                EventClass::A => {
                    let least: Option<i64> = tx
                        .query_row(
                            "SELECT MIN(last_seq) FROM cursors WHERE flux = 1",
                            [],
                            |row| row.get(0),
                        )
                        .optional()?
                        .flatten();
                    let Some(least) = least else {
                        outcome.push(Enforced {
                            class,
                            deleted: 0,
                            oldest_seq: oldest_in(&tx, class)?,
                        });
                        continue;
                    };
                    least
                }
                _ => i64::MAX,
            };
            let cut = max_age_ns.map(|age| now_ns.saturating_sub(age));
            let deleted = tx.execute(
                "DELETE FROM events
                 WHERE class = ?1 AND seq <= ?2 AND (
                     (?3 IS NOT NULL AND ts_receive < ?3)
                  OR (?4 IS NOT NULL AND seq NOT IN (
                        SELECT seq FROM events WHERE class = ?1 ORDER BY seq DESC LIMIT ?4))
                 )",
                params![class_code(class), floor, cut, max_events],
            )?;
            let deleted_count = u64::try_from(deleted).unwrap_or(0);
            tx.execute(
                "UPDATE retention SET dropped = dropped + ?2 WHERE class = ?1",
                params![
                    class_code(class),
                    i64::try_from(deleted_count).unwrap_or(i64::MAX)
                ],
            )?;
            if deleted_count > 0 {
                // FUN-SAF-007: the deletion is recorded with the policy that caused it.
                let floor_seq = (floor != i64::MAX).then_some(floor);
                tx.execute(
                    "INSERT INTO retention_audit
                        (at_ns, class, deleted, max_events, max_age_ns, floor_seq)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        now_ns,
                        class_code(class),
                        i64::try_from(deleted_count).unwrap_or(i64::MAX),
                        max_events,
                        max_age_ns,
                        floor_seq
                    ],
                )?;
            }
            outcome.push(Enforced {
                class,
                deleted: deleted_count,
                oldest_seq: oldest_in(&tx, class)?,
            });
        }
        tx.commit()?;
        Ok(outcome)
    }

    /// The deletions recorded by retention, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] on a read failure.
    pub fn retention_audit(&self) -> Result<Vec<RetentionAudit>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT id, at_ns, class, deleted, max_events, max_age_ns, floor_seq
             FROM retention_audit ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?;
        let mut audit = Vec::new();
        for row in rows {
            let (id, at_ns, class, deleted, max_events, max_age_ns, floor_seq) = row?;
            let class = EventClass::from_code(class).ok_or(StoreError::UnknownClass(class))?;
            audit.push(RetentionAudit {
                id,
                at_ns,
                class,
                deleted: u64::try_from(deleted).unwrap_or(0),
                max_events: max_events.map(|n| u64::try_from(n).unwrap_or(0)),
                max_age_ns,
                floor_seq,
            });
        }
        Ok(audit)
    }

    /// Events deleted by retention, per class, since the log was created.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] on a read failure.
    pub fn dropped(&self, class: EventClass) -> Result<u64, StoreError> {
        let dropped: Option<i64> = self
            .conn
            .query_row(
                "SELECT dropped FROM retention WHERE class = ?1",
                params![class_code(class)],
                |row| row.get(0),
            )
            .optional()?;
        Ok(dropped.map_or(0, |n| u64::try_from(n).unwrap_or(0)))
    }

    fn oldest_seq(&self) -> Result<Option<i64>, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT MIN(seq) FROM events", [], |row| row.get(0))?)
    }
}

fn oldest_in(tx: &rusqlite::Transaction<'_>, class: EventClass) -> Result<Option<i64>, StoreError> {
    Ok(tx.query_row(
        "SELECT MIN(seq) FROM events WHERE class = ?1",
        params![class_code(class)],
        |row| row.get(0),
    )?)
}
