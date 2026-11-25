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
use powermug_store::{Committed, EventClass, Journal, NewEvent, StoreError};
use prost::Message;
use thiserror::Error;
use uuid::Uuid;

use crate::clock::Times;
use crate::cycle::{CycleSupervisor, Expired, Supervision};
use crate::decode::point_event;
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

/// A point table that cannot be built.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TableError {
    #[error("point id {0} appears twice in the table")]
    DuplicatePointId(String),
    #[error("a lane holds one partition: point {point_id} is on site {site} instead of {first}")]
    MixedSites {
        first: String,
        site: String,
        point_id: String,
    },
}

impl PointTable {
    /// Builds the table of partition revision `revision_index`.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::DuplicatePointId`] when two points share an
    /// identifier, and [`TableError::MixedSites`] when the points are not all on
    /// the same site: a lane is one partition (§7.1).
    ///
    /// # Panics
    ///
    /// Panics when the table holds 2^32 points or more, which no partition does.
    pub fn new(revision_index: u32, specs: Vec<PointSpec>) -> Result<Self, TableError> {
        if let Some(first) = specs.first() {
            for spec in &specs {
                if spec.site_id != first.site_id {
                    return Err(TableError::MixedSites {
                        first: first.site_id.clone(),
                        site: spec.site_id.clone(),
                        point_id: spec.point_id.clone(),
                    });
                }
            }
        }
        let mut by_id = HashMap::with_capacity(specs.len());
        for (position, spec) in specs.iter().enumerate() {
            let index = PointIndex(
                u32::try_from(position).expect("a point table holds fewer than 2^32 points"),
            );
            if by_id.insert(spec.point_id.clone(), index).is_some() {
                return Err(TableError::DuplicatePointId(spec.point_id.clone()));
            }
        }
        Ok(Self {
            revision_index,
            specs,
            by_id,
        })
    }

    /// The site of the partition: the site of its points, or empty for an empty table.
    #[must_use]
    pub fn site_id(&self) -> &str {
        self.specs.first().map_or("", |spec| spec.site_id.as_str())
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
    #[error("the snapshot cannot be restored: {0}")]
    Snapshot(String),
    #[error("journal record {seq} cannot be replayed: {reason}")]
    Replay { seq: i64, reason: String },
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
    log: Box<dyn Journal>,
    supervisor: CycleSupervisor,
    lost: HashSet<SessionId>,
    /// Sessions whose state an adapter has reported at least once.
    reported: HashSet<SessionId>,
    last_snapshot_ns: u64,
    published: Vec<pb::Envelope>,
    live: crate::ring::LiveRing,
    last_accepted_wall_ns: Option<i64>,
    /// Changes of the connection state of each session, lost or restored, since the start.
    connection_events: HashMap<SessionId, u32>,
}

/// How often a lane takes a snapshot of its image (§7.2, H).
pub const SNAPSHOT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

impl Lane {
    /// A lane over `table` and `log`, whose monotonic clock reads `now_ns`.
    #[must_use]
    pub fn new(table: PointTable, log: impl Journal + 'static, now_ns: u64) -> Self {
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
            log: Box::new(log),
            supervisor,
            lost: HashSet::new(),
            reported: HashSet::new(),
            last_snapshot_ns: now_ns,
            published: Vec::new(),
            live: crate::ring::LiveRing::new(crate::ring::DEFAULT_CAPACITY),
            last_accepted_wall_ns: None,
            connection_events: HashMap::new(),
        }
    }

    /// The live ring of the lane: the last journaled envelopes (see [`crate::ring`]).
    #[must_use]
    pub fn live(&self) -> &crate::ring::LiveRing {
        &self.live
    }

    /// Sets the capacity of the live ring. The ring starts empty: call it before the lane runs.
    pub fn set_live_capacity(&mut self, capacity: usize) {
        self.live = crate::ring::LiveRing::new(capacity);
    }

    /// Takes a snapshot of the image at the current journal sequence. Returns
    /// the sequence, or `None` when the journal is empty.
    ///
    /// # Errors
    ///
    /// Returns [`LaneError::Store`] when the snapshot cannot be written.
    pub fn snapshot(&mut self) -> Result<Option<i64>, LaneError> {
        let Some(seq) = self.log.last_seq()? else {
            return Ok(None);
        };
        let payload = crate::snapshot::encode(&self.image);
        self.log.put_snapshot(self.table.site_id(), seq, &payload)?;
        Ok(Some(seq))
    }

    /// Takes a snapshot when [`SNAPSHOT_INTERVAL`] has passed since the last one.
    ///
    /// # Errors
    ///
    /// As [`Lane::snapshot`].
    pub fn snapshot_if_due(&mut self, times: Times) -> Result<bool, LaneError> {
        let interval = u64::try_from(SNAPSHOT_INTERVAL.as_nanos()).unwrap_or(u64::MAX);
        if times.monotonic_ns.saturating_sub(self.last_snapshot_ns) < interval {
            return Ok(false);
        }
        self.snapshot()?;
        self.last_snapshot_ns = times.monotonic_ns;
        Ok(true)
    }

    /// Opens a lane over `log`, rebuilding the image from the newest snapshot
    /// and the journal written after it.
    ///
    /// Every replayed point is then degraded and suspended: nothing is valid
    /// until a South resynchronization gives a fresh value (PM-TEL-003,
    /// FUN-IMG-004). Class C points are not journaled, so they have no value
    /// until then.
    ///
    /// # Errors
    ///
    /// Returns [`LaneError::Replay`] for a journal record that does not decode
    /// against the table, and [`LaneError::Store`] when the journal cannot be
    /// read.
    pub fn open(
        table: PointTable,
        log: impl Journal + 'static,
        now_ns: u64,
    ) -> Result<Self, LaneError> {
        let mut lane = Self::new(table, log, now_ns);
        let mut next = 1;
        if let Some(snapshot) = lane.log.latest_snapshot(lane.table.site_id())? {
            crate::snapshot::restore(&snapshot.payload, &mut lane.image)
                .map_err(LaneError::Snapshot)?;
            next = snapshot.seq + 1;
        }
        lane.replay(next)?;
        lane.image.degrade_all();
        let suspended: Vec<PointIndex> = lane
            .table
            .iter()
            .filter(|(index, _)| lane.image.get(*index).is_some())
            .map(|(index, _)| index)
            .collect();
        for index in suspended {
            lane.supervisor.suspend(index)?;
        }
        Ok(lane)
    }

    /// Applies every journaled point event, in sequence order.
    fn replay(&mut self, mut next: i64) -> Result<(), LaneError> {
        const PAGE: u32 = 1_000;
        loop {
            let rows = self.log.read_from(next, PAGE)?;
            for row in &rows {
                // Session events concern no point and do not change the image.
                let Some(_) = row.point else {
                    continue;
                };
                let event = self.replay_event(row.seq, &row.payload)?;
                self.image.apply(&event)?;
            }
            let Some(last) = rows.last() else {
                return Ok(());
            };
            if rows.len() < PAGE as usize {
                return Ok(());
            }
            next = last.seq + 1;
        }
    }

    /// Decodes one journaled point event, keeping the revision it was mapped with.
    fn replay_event(&self, seq: i64, payload: &[u8]) -> Result<PointEvent, LaneError> {
        let replay = |reason: String| LaneError::Replay { seq, reason };
        let envelope = pb::Envelope::decode(payload).map_err(|err| replay(err.to_string()))?;
        let revision = envelope.mapping_revision.parse::<u32>().map_err(|_| {
            replay(format!(
                "mapping revision {} is not a number",
                envelope.mapping_revision
            ))
        })?;
        let mut event = point_event(&envelope, &self.table, envelope.process_time_ns)
            .map_err(|err| replay(err.to_string()))?;
        event.revision_index = revision;
        Ok(event)
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
    pub fn log(&self) -> &dyn Journal {
        self.log.as_ref()
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
        self.publish(&entries, committed.as_ref());

        for entry in &entries {
            if let Journaled::Point { event, origin } = entry {
                self.image.apply(event)?;
                if *origin == Origin::Acquired {
                    self.supervisor.accepted(event.point, times.monotonic_ns)?;
                }
            }
        }
        if !events.is_empty() {
            self.last_accepted_wall_ns = Some(times.wall_ns);
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
        self.publish(&entries, committed.as_ref());
        for entry in &entries {
            if let Journaled::Point { event, .. } = entry {
                self.image.apply(event)?;
            }
        }
        for index in attached {
            self.supervisor.suspend(index)?;
        }
        self.lost.insert(session);
        self.reported.insert(session);
        let changes = self.connection_events.entry(session).or_insert(0);
        *changes = changes.saturating_add(1);
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
            self.reported.insert(session);
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
        self.reported.insert(session);
        let changes = self.connection_events.entry(session).or_insert(0);
        *changes = changes.saturating_add(1);
        Ok(committed)
    }

    /// The wall-clock time of the last batch the lane accepted, if any.
    #[must_use]
    pub fn last_accepted_wall_ns(&self) -> Option<i64> {
        self.last_accepted_wall_ns
    }

    /// The number of changes of the connection state of `session` since the start.
    #[must_use]
    pub fn connection_events(&self, session: SessionId) -> u32 {
        self.connection_events.get(&session).copied().unwrap_or(0)
    }

    /// The sessions of the table, each with whether it is up (not lost).
    #[must_use]
    pub fn session_states(&self) -> Vec<(SessionId, Option<bool>)> {
        let mut sessions: Vec<SessionId> = self
            .table
            .iter()
            .filter_map(|(_, spec)| spec.session)
            .collect();
        sessions.sort_by_key(|session| session.0);
        sessions.dedup();
        sessions
            .into_iter()
            .map(|session| {
                let state = self
                    .reported
                    .contains(&session)
                    .then(|| !self.lost.contains(&session));
                (session, state)
            })
            .collect()
    }

    /// Takes the envelopes of the point events committed since the last call, in
    /// commit order. The core publishes them to the North adapters.
    pub fn take_published(&mut self) -> Vec<pb::Envelope> {
        std::mem::take(&mut self.published)
    }

    /// The current state of the points of `session` that hold a value, as
    /// envelopes. A general interrogation is answered from this.
    #[must_use]
    pub fn session_state(&self, session: SessionId) -> Vec<pb::Envelope> {
        self.table
            .iter()
            .filter(|(_, spec)| spec.session == Some(session))
            .filter_map(|(index, spec)| {
                let slot = self.image.get(index)?;
                Some(pb::Envelope {
                    site_id: spec.site_id.clone(),
                    point_id: spec.point_id.clone(),
                    value: Some(slot.value.into()),
                    quality: Some(slot.quality.into()),
                    source_time_ns: slot.source_time_ns,
                    receive_time_ns: slot.receive_time_ns,
                    mapping_revision: slot.revision_index.to_string(),
                    ..Default::default()
                })
            })
            .collect()
    }

    /// Keeps the envelopes of the point items of a commit for publication, and the journaled ones
    /// in the live ring. The journaled items take the sequence numbers of the commit in order;
    /// class C items are not journaled and take none.
    fn publish(&mut self, entries: &[Journaled], committed: Option<&Committed>) {
        let mut next = committed.map(|commit| commit.first_seq);
        for entry in entries {
            let Journaled::Point { event, origin } = entry else {
                continue;
            };
            let Some(spec) = self.table.get(event.point) else {
                continue;
            };
            let envelope = envelope(
                event,
                &spec.site_id,
                &spec.point_id,
                *origin,
                self.table.revision_index(),
            );
            if spec.class != EventClass::C {
                if let Some(seq) = next {
                    self.live.push(seq, envelope.clone());
                    next = Some(seq + 1);
                }
            }
            self.published.push(envelope);
        }
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
