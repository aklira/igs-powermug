//! Partition lane: the ingestion path of a set of points (SPECIFICATIONS §7.1).
//!
//! For each batch, in this order:
//! 1. check every point against the table; a refused batch has no effect;
//! 2. collect the stale events of the points whose deadline has passed (§7.8);
//! 3. append the stale events and the batch in one transaction;
//! 4. update the image, stale events first, then the batch;
//! 5. re-arm the supervision of the points of the batch.
//!
//! If step 3 fails, the expiries of step 2 are undone, so that no point is
//! reported stale without its event in the log.
//!
//! A session lost (§7.6) is journaled as one `CommunicationState` and one
//! quality event per point with a value; the points are then suspended, so no
//! stale event is written for them until a fresh value arrives.

use std::collections::{HashMap, HashSet};

use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{Detail, EventId, PointEvent, PointIndex, Quality, Validity};
use powermug_store::{Committed, EventClass, NewEvent, StoreError, TelemetryLog};
use prost::Message;
use thiserror::Error;
use uuid::Uuid;

use crate::clock::Times;
use crate::cycle::{CycleSupervisor, Expired, Supervision};
use crate::image::{OutOfRange as ImageOutOfRange, ProcessImage};

/// A South session of a partition, as numbered by the revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u32);

/// Static description of a point, interned at activation (PM-PIV-001).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointSpec {
    pub site_id: String,
    /// Canonical identity, prefixed by `site_id`, for example `S3/TM_001`.
    pub point_id: String,
    pub class: EventClass,
    /// The session the point is attached to, if any.
    pub session: Option<SessionId>,
    /// Cycle supervision of the point, if any (§7.8).
    pub supervision: Option<Supervision>,
}

/// The points of a lane, from one partition revision. The index of a point is
/// its position in the table, and its `point_id` is unique.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointTable {
    revision_index: u32,
    specs: Vec<PointSpec>,
    by_id: HashMap<String, PointIndex>,
}

/// Two points of one table share an identifier.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("point id {0} appears twice in the table")]
pub struct DuplicatePointId(pub String);

impl PointTable {
    /// Builds the table of partition revision `revision_index`.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicatePointId`] when two points share an identifier.
    ///
    /// # Panics
    ///
    /// Panics when the table holds 2^32 points or more, which no partition does.
    pub fn new(revision_index: u32, specs: Vec<PointSpec>) -> Result<Self, DuplicatePointId> {
        let mut by_id = HashMap::with_capacity(specs.len());
        for (position, spec) in specs.iter().enumerate() {
            let index = PointIndex(
                u32::try_from(position).expect("a point table holds fewer than 2^32 points"),
            );
            if by_id.insert(spec.point_id.clone(), index).is_some() {
                return Err(DuplicatePointId(spec.point_id.clone()));
            }
        }
        Ok(Self {
            revision_index,
            specs,
            by_id,
        })
    }

    /// The partition revision that mapped these points.
    #[must_use]
    pub fn revision_index(&self) -> u32 {
        self.revision_index
    }

    /// The index of the point with this identifier.
    #[must_use]
    pub fn index_of(&self, point_id: &str) -> Option<PointIndex> {
        self.by_id.get(point_id).copied()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    #[must_use]
    pub fn get(&self, point: PointIndex) -> Option<&PointSpec> {
        self.specs.get(point.0 as usize)
    }

    /// Every point with its index, in table order.
    ///
    /// # Panics
    ///
    /// Panics when the table holds 2^32 points or more, as [`PointTable::new`] does.
    pub fn iter(&self) -> impl Iterator<Item = (PointIndex, &PointSpec)> {
        self.specs.iter().enumerate().map(|(position, spec)| {
            let index = PointIndex(
                u32::try_from(position).expect("a point table holds fewer than 2^32 points"),
            );
            (index, spec)
        })
    }
}

/// Errors of a lane.
#[derive(Debug, Error)]
pub enum LaneError {
    #[error("point index {0} is not in the point table; the batch was refused")]
    UnknownPoint(u32),
    #[error("no point of the table is attached to session {0}")]
    UnknownSession(u32),
    #[error("point index {0} was reported stale but has no value in the image")]
    StaleWithoutValue(u32),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Image(#[from] ImageOutOfRange),
    #[error(transparent)]
    Supervision(#[from] crate::cycle::OutOfRange),
}

/// Why a point event was produced. It sets the cause and the provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Acquired,
    Stale,
    CommunicationLoss,
}

/// One item of a journal transaction.
enum Journaled {
    Point {
        event: PointEvent,
        origin: Origin,
    },
    Session {
        site_id: String,
        message: pb::CommunicationState,
        event_id: EventId,
        wall_ns: i64,
    },
}

/// A journal row, encoded and ready for the store.
struct Encoded {
    partition: String,
    point: Option<u32>,
    class: EventClass,
    event_id: [u8; 16],
    revision: u32,
    ts_receive_ns: i64,
    payload: Vec<u8>,
}

/// The ingestion path of one lane: its points, its image, its journal, the
/// cycle supervision of its points and the sessions currently lost.
#[derive(Debug)]
pub struct Lane {
    table: PointTable,
    image: ProcessImage,
    log: TelemetryLog,
    supervisor: CycleSupervisor,
    lost: HashSet<SessionId>,
}

impl Lane {
    /// A lane over `table` and `log`, whose monotonic clock reads `now_ns`.
    #[must_use]
    pub fn new(table: PointTable, log: TelemetryLog, now_ns: u64) -> Self {
        let image = ProcessImage::with_points(table.len());
        let mut supervisor = CycleSupervisor::new(table.len(), now_ns);
        for (index, spec) in table.iter() {
            if let Some(params) = spec.supervision {
                // The index comes from the table itself, so it is in range.
                let _ = supervisor.supervise(index, params);
            }
        }
        Self {
            table,
            image,
            log,
            supervisor,
            lost: HashSet::new(),
        }
    }

    #[must_use]
    pub fn table(&self) -> &PointTable {
        &self.table
    }

    #[must_use]
    pub fn image(&self) -> &ProcessImage {
        &self.image
    }

    #[must_use]
    pub fn log(&self) -> &TelemetryLog {
        &self.log
    }

    /// The cycle state of `point`, for tests and diagnostics.
    #[must_use]
    pub fn supervisor_state(&self, point: PointIndex) -> Option<crate::cycle::CycleState> {
        self.supervisor.state(point)
    }

    /// Whether `session` is currently lost.
    #[must_use]
    pub fn is_lost(&self, session: SessionId) -> bool {
        self.lost.contains(&session)
    }

    /// Ingests a batch at `times`. Returns the commit of the journaled events,
    /// or `None` when there is nothing to journal (only class C events).
    ///
    /// # Errors
    ///
    /// Returns [`LaneError::UnknownPoint`] before anything is changed when a
    /// point is not in the table. Returns [`LaneError::Store`] when the journal
    /// refuses the batch, for example on a replayed event id; the image and the
    /// supervision are then unchanged.
    pub fn ingest(
        &mut self,
        events: &[PointEvent],
        times: Times,
    ) -> Result<Option<Committed>, LaneError> {
        for event in events {
            if self.table.get(event.point).is_none() {
                return Err(LaneError::UnknownPoint(event.point.0));
            }
        }

        let expired = self.supervisor.expire(times.monotonic_ns);
        let stale = match self.stale_events(&expired, times.wall_ns) {
            Ok(stale) => stale,
            Err(err) => {
                self.supervisor.revert(&expired);
                return Err(err);
            }
        };

        let entries: Vec<Journaled> = stale
            .into_iter()
            .map(|event| Journaled::Point {
                event,
                origin: Origin::Stale,
            })
            .chain(events.iter().map(|event| Journaled::Point {
                event: *event,
                origin: Origin::Acquired,
            }))
            .collect();
        let committed = match self.append(&entries) {
            Ok(committed) => committed,
            Err(err) => {
                self.supervisor.revert(&expired);
                return Err(err);
            }
        };

        for entry in &entries {
            if let Journaled::Point { event, origin } = entry {
                self.image.apply(event)?;
                if *origin == Origin::Acquired {
                    self.supervisor.accepted(event.point, times.monotonic_ns)?;
                }
            }
        }
        Ok(committed)
    }

    /// Records that `session` is lost. Every point of the session with a value
    /// gets a quality event (invalid), and the points are suspended. A session
    /// already lost is left as it is.
    ///
    /// # Errors
    ///
    /// Returns [`LaneError::UnknownSession`] when no point is attached to the
    /// session, and [`LaneError::Store`] when the journal refuses the events; the
    /// image and the supervision are then unchanged.
    pub fn session_lost(
        &mut self,
        session: SessionId,
        reason: &str,
        times: Times,
    ) -> Result<Option<Committed>, LaneError> {
        if self.lost.contains(&session) {
            return Ok(None);
        }
        let site_id = self.site_of(session)?;
        let attached: Vec<PointIndex> = self
            .table
            .iter()
            .filter(|(_, spec)| spec.session == Some(session))
            .map(|(index, _)| index)
            .collect();

        let mut entries = vec![Journaled::Session {
            site_id: site_id.clone(),
            message: pb::CommunicationState {
                site_id,
                session_id: session.0.to_string(),
                connected: false,
                reason: reason.to_owned(),
            },
            event_id: new_event_id(),
            wall_ns: times.wall_ns,
        }];
        for index in &attached {
            if let Some(slot) = self.image.get(*index) {
                entries.push(Journaled::Point {
                    event: PointEvent {
                        point: *index,
                        value: slot.value,
                        quality: Quality::new(Validity::Invalid),
                        source_time_ns: slot.source_time_ns,
                        receive_time_ns: slot.receive_time_ns,
                        process_time_ns: times.wall_ns,
                        event_id: new_event_id(),
                        revision_index: self.table.revision_index(),
                    },
                    origin: Origin::CommunicationLoss,
                });
            }
        }

        let committed = self.append(&entries)?;
        for entry in &entries {
            if let Journaled::Point { event, .. } = entry {
                self.image.apply(event)?;
            }
        }
        for index in attached {
            self.supervisor.suspend(index)?;
        }
        self.lost.insert(session);
        Ok(committed)
    }

    /// Records that `session` is back. Its points stay degraded until a fresh
    /// value arrives. A session that is not lost is left as it is.
    ///
    /// # Errors
    ///
    /// Returns [`LaneError::UnknownSession`] or [`LaneError::Store`] as for
    /// [`Lane::session_lost`].
    pub fn session_restored(
        &mut self,
        session: SessionId,
        reason: &str,
        times: Times,
    ) -> Result<Option<Committed>, LaneError> {
        if !self.lost.contains(&session) {
            return Ok(None);
        }
        let site_id = self.site_of(session)?;
        let entries = vec![Journaled::Session {
            site_id: site_id.clone(),
            message: pb::CommunicationState {
                site_id,
                session_id: session.0.to_string(),
                connected: true,
                reason: reason.to_owned(),
            },
            event_id: new_event_id(),
            wall_ns: times.wall_ns,
        }];
        let committed = self.append(&entries)?;
        self.lost.remove(&session);
        Ok(committed)
    }

    /// The site of the points attached to `session`.
    fn site_of(&self, session: SessionId) -> Result<String, LaneError> {
        self.table
            .iter()
            .find(|(_, spec)| spec.session == Some(session))
            .map(|(_, spec)| spec.site_id.clone())
            .ok_or(LaneError::UnknownSession(session.0))
    }

    /// One stale event per expired point, from the value the image holds.
    fn stale_events(
        &self,
        expired: &[Expired],
        detected_wall_ns: i64,
    ) -> Result<Vec<PointEvent>, LaneError> {
        expired
            .iter()
            .map(|item| {
                let slot = self
                    .image
                    .get(item.point)
                    .ok_or(LaneError::StaleWithoutValue(item.point.0))?;
                Ok(PointEvent {
                    point: item.point,
                    value: slot.value,
                    quality: Quality::new(Validity::Questionable).with_detail(Detail::OldData),
                    source_time_ns: slot.source_time_ns,
                    receive_time_ns: slot.receive_time_ns,
                    process_time_ns: detected_wall_ns,
                    event_id: new_event_id(),
                    revision_index: self.table.revision_index(),
                })
            })
            .collect()
    }

    /// Journals the items that are not class C, in one transaction.
    fn append(&mut self, entries: &[Journaled]) -> Result<Option<Committed>, LaneError> {
        let mut encoded = Vec::with_capacity(entries.len());
        for entry in entries {
            match entry {
                Journaled::Point { event, origin } => {
                    let spec = self
                        .table
                        .get(event.point)
                        .ok_or(LaneError::UnknownPoint(event.point.0))?;
                    if spec.class == EventClass::C {
                        continue;
                    }
                    let payload = envelope(
                        event,
                        &spec.site_id,
                        &spec.point_id,
                        *origin,
                        self.table.revision_index(),
                    )
                    .encode_to_vec();
                    encoded.push(Encoded {
                        partition: spec.site_id.clone(),
                        point: Some(event.point.0),
                        class: spec.class,
                        event_id: *event.event_id.as_bytes(),
                        revision: event.revision_index,
                        ts_receive_ns: event.receive_time_ns,
                        payload,
                    });
                }
                Journaled::Session {
                    site_id,
                    message,
                    event_id,
                    wall_ns,
                } => {
                    encoded.push(Encoded {
                        partition: site_id.clone(),
                        point: None,
                        class: EventClass::A,
                        event_id: *event_id.as_bytes(),
                        revision: self.table.revision_index(),
                        ts_receive_ns: *wall_ns,
                        payload: message.encode_to_vec(),
                    });
                }
            }
        }

        let records: Vec<NewEvent<'_>> = encoded
            .iter()
            .map(|row| NewEvent {
                partition: row.partition.as_str(),
                point: row.point,
                class: row.class,
                event_id: row.event_id,
                revision: row.revision,
                ts_receive_ns: row.ts_receive_ns,
                payload: row.payload.as_slice(),
            })
            .collect();
        Ok(self.log.append(&records)?)
    }
}

/// A new `UUIDv7`. The core assigns event ids (§5.1).
fn new_event_id() -> EventId {
    EventId::from_bytes(*Uuid::now_v7().as_bytes())
        .expect("uuid crate produces UUIDv7 with the RFC 9562 variant")
}

/// Encodes one point event as the journal record: a pivot `Envelope`.
///
/// Stale events carry their provenance and cause; communication-loss events
/// carry their cause. The other fields the lane cannot know yet (unit, config
/// revision, source epoch, origin gateway, replay) keep their empty values.
fn envelope(
    event: &PointEvent,
    site_id: &str,
    point_id: &str,
    origin: Origin,
    revision: u32,
) -> pb::Envelope {
    let mut envelope = pb::Envelope {
        event_id: event.event_id.as_bytes().to_vec(),
        site_id: site_id.to_owned(),
        point_id: point_id.to_owned(),
        value: Some(event.value.into()),
        quality: Some(event.quality.into()),
        source_time_ns: event.source_time_ns,
        receive_time_ns: event.receive_time_ns,
        process_time_ns: event.process_time_ns,
        mapping_revision: event.revision_index.to_string(),
        ..Default::default()
    };
    match origin {
        Origin::Acquired => {}
        Origin::Stale => {
            envelope.provenance = vec![format!("cycle_check@{revision}")];
            envelope.cause = Some(pb::Cause {
                normalized: "supervision".to_owned(),
                native: String::new(),
            });
        }
        Origin::CommunicationLoss => {
            envelope.cause = Some(pb::Cause {
                normalized: "communication_loss".to_owned(),
                native: String::new(),
            });
        }
    }
    envelope
}
