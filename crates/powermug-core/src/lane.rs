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

use std::collections::HashMap;

use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{Detail, EventId, PointEvent, PointIndex, Quality, Validity};
use powermug_store::{Committed, EventClass, NewEvent, StoreError, TelemetryLog};
use prost::Message;
use thiserror::Error;
use uuid::Uuid;

use crate::clock::Times;
use crate::cycle::{CycleSupervisor, Expired, Supervision};
use crate::image::{OutOfRange as ImageOutOfRange, ProcessImage};

/// Static description of a point, interned at activation (PM-PIV-001).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointSpec {
    pub site_id: String,
    /// Canonical identity, prefixed by `site_id`, for example `S3/TM_001`.
    pub point_id: String,
    pub class: EventClass,
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
    #[error("point index {0} was reported stale but has no value in the image")]
    StaleWithoutValue(u32),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Image(#[from] ImageOutOfRange),
    #[error(transparent)]
    Supervision(#[from] crate::cycle::OutOfRange),
}

/// The ingestion path of one lane: its points, its image, its journal and the
/// cycle supervision of its points.
#[derive(Debug)]
pub struct Lane {
    table: PointTable,
    image: ProcessImage,
    log: TelemetryLog,
    supervisor: CycleSupervisor,
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

        let ordered: Vec<(PointEvent, bool)> = stale
            .iter()
            .map(|event| (*event, true))
            .chain(events.iter().map(|event| (*event, false)))
            .collect();
        let committed = match self.append(&ordered) {
            Ok(committed) => committed,
            Err(err) => {
                self.supervisor.revert(&expired);
                return Err(err);
            }
        };

        for (event, is_stale) in &ordered {
            self.image.apply(event)?;
            if !is_stale {
                self.supervisor.accepted(event.point, times.monotonic_ns)?;
            }
        }
        Ok(committed)
    }

    /// The cycle state of `point`, for tests and diagnostics.
    #[must_use]
    pub fn supervisor_state(&self, point: PointIndex) -> Option<crate::cycle::CycleState> {
        self.supervisor.state(point)
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

    /// Journals the events that are not class C, in one transaction.
    fn append(&mut self, ordered: &[(PointEvent, bool)]) -> Result<Option<Committed>, LaneError> {
        let mut payloads: Vec<Option<(Vec<u8>, &str, EventClass)>> =
            Vec::with_capacity(ordered.len());
        for (event, is_stale) in ordered {
            let spec = self
                .table
                .get(event.point)
                .ok_or(LaneError::UnknownPoint(event.point.0))?;
            let payload = (spec.class != EventClass::C).then(|| {
                envelope(
                    event,
                    &spec.site_id,
                    &spec.point_id,
                    *is_stale,
                    self.table.revision_index(),
                )
                .encode_to_vec()
            });
            payloads.push(payload.map(|bytes| (bytes, spec.site_id.as_str(), spec.class)));
        }

        let records: Vec<NewEvent<'_>> = ordered
            .iter()
            .zip(&payloads)
            .filter_map(|((event, _), payload)| {
                payload.as_ref().map(|(bytes, site, class)| NewEvent {
                    partition: site,
                    point: event.point.0,
                    class: *class,
                    event_id: *event.event_id.as_bytes(),
                    revision: event.revision_index,
                    ts_receive_ns: event.receive_time_ns,
                    payload: bytes,
                })
            })
            .collect();

        Ok(self.log.append(&records)?)
    }
}

/// A new `UUIDv7` for a stale event. The core assigns event ids (§5.1).
fn new_event_id() -> EventId {
    EventId::from_bytes(*Uuid::now_v7().as_bytes())
        .expect("uuid crate produces UUIDv7 with the RFC 9562 variant")
}

/// Encodes one event as the journal record: a pivot `Envelope`.
///
/// A stale event carries its provenance and its cause. The other fields the
/// lane cannot know yet (unit, config revision, source epoch, origin gateway,
/// replay) keep their empty values.
fn envelope(
    event: &PointEvent,
    site_id: &str,
    point_id: &str,
    is_stale: bool,
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
    if is_stale {
        envelope.provenance = vec![format!("cycle_check@{revision}")];
        envelope.cause = Some(pb::Cause {
            normalized: "supervision".to_owned(),
            native: String::new(),
        });
    }
    envelope
}
