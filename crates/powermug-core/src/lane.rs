//! Partition lane: the ingestion path of a set of points (SPECIFICATIONS §7.1).
//!
//! For each batch, in this order: check every point against the table, encode
//! the journaled events, append them in one transaction, and only then update
//! the image. A refused batch touches neither the journal nor the image.

use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{PointEvent, PointIndex};
use powermug_store::{Committed, EventClass, NewEvent, StoreError, TelemetryLog};
use prost::Message;
use thiserror::Error;

use crate::image::{OutOfRange, ProcessImage};

/// Static description of a point, interned at activation (PM-PIV-001).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointSpec {
    pub site_id: String,
    /// Canonical identity, prefixed by `site_id`, for example `S3/TM_001`.
    pub point_id: String,
    pub class: EventClass,
}

/// The points of a lane. The index of a point is its position in the table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PointTable {
    specs: Vec<PointSpec>,
}

impl PointTable {
    #[must_use]
    pub fn new(specs: Vec<PointSpec>) -> Self {
        Self { specs }
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
}

/// Errors of a lane.
#[derive(Debug, Error)]
pub enum LaneError {
    #[error("point index {0} is not in the point table; the batch was refused")]
    UnknownPoint(u32),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Image(#[from] OutOfRange),
}

/// The ingestion path of one lane: its points, its image and its journal.
#[derive(Debug)]
pub struct Lane {
    table: PointTable,
    image: ProcessImage,
    log: TelemetryLog,
}

impl Lane {
    #[must_use]
    pub fn new(table: PointTable, log: TelemetryLog) -> Self {
        let image = ProcessImage::with_points(table.len());
        Self { table, image, log }
    }

    #[must_use]
    pub fn image(&self) -> &ProcessImage {
        &self.image
    }

    #[must_use]
    pub fn log(&self) -> &TelemetryLog {
        &self.log
    }

    /// Ingests a batch. Returns the commit of the journaled part, or `None`
    /// when every event of the batch is class C.
    ///
    /// # Errors
    ///
    /// Returns [`LaneError::UnknownPoint`] before anything is written when a
    /// point is not in the table. Returns [`LaneError::Store`] when the journal
    /// refuses the batch, for example on a replayed event id; the image is then
    /// unchanged.
    pub fn ingest(&mut self, events: &[PointEvent]) -> Result<Option<Committed>, LaneError> {
        let mut specs = Vec::with_capacity(events.len());
        for event in events {
            let spec = self
                .table
                .get(event.point)
                .ok_or(LaneError::UnknownPoint(event.point.0))?;
            specs.push(spec);
        }

        // Encode the journaled events first, so that the borrows of the
        // records below hold only shared data.
        let payloads: Vec<Option<Vec<u8>>> = events
            .iter()
            .zip(&specs)
            .map(|(event, spec)| {
                (spec.class != EventClass::C)
                    .then(|| envelope(event, &spec.site_id, &spec.point_id).encode_to_vec())
            })
            .collect();

        let records: Vec<NewEvent<'_>> = events
            .iter()
            .zip(&specs)
            .zip(&payloads)
            .filter_map(|((event, spec), payload)| {
                payload.as_deref().map(|payload| NewEvent {
                    partition: spec.site_id.as_str(),
                    point: event.point.0,
                    class: spec.class,
                    event_id: *event.event_id.as_bytes(),
                    revision: event.revision_index,
                    ts_receive_ns: event.receive_time_ns,
                    payload,
                })
            })
            .collect();

        let committed = self.log.append(&records)?;

        // The image follows the journal (PM-TEL-001). Class C is not journaled
        // and updates the image directly, in the order of the batch.
        for event in events {
            self.image.apply(event)?;
        }
        Ok(committed)
    }
}

/// Encodes one event as the journal record: a pivot `Envelope`.
///
/// Fields the lane cannot know yet (unit, config revision, provenance, source
/// epoch, origin gateway, replay) keep their empty values.
fn envelope(event: &PointEvent, site_id: &str, point_id: &str) -> pb::Envelope {
    pb::Envelope {
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
    }
}
