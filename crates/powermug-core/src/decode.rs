//! From the wire to the hot path: an `Envelope` of an adapter batch becomes a
//! [`PointEvent`] of the lane (SPECIFICATIONS §5.1).
//!
//! Decoding is strict: an unknown point, a missing or malformed field, or a
//! text value is refused. Nothing is defaulted.

use powermug_contract::proto::v1::EventBatch;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{ConvertError, EventId, EventIdError, PointEvent, Quality, Value};
use thiserror::Error;

use crate::lane::PointTable;

/// An event of an adapter batch cannot enter the lane.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    #[error("point {0} is not in the point table")]
    UnknownPoint(String),
    #[error("event id has {0} bytes, expected 16")]
    EventIdLength(usize),
    #[error("event id is not a UUIDv7: {0}")]
    EventId(EventIdError),
    #[error("event has no value")]
    MissingValue,
    #[error("event has no quality")]
    MissingQuality,
    #[error("value: {0}")]
    Value(ConvertError),
    #[error("quality: {0}")]
    Quality(ConvertError),
}

/// Decodes one event. `process_time_ns` is the core's time of conversion.
///
/// # Errors
///
/// Returns the first [`DecodeError`] found in the event.
pub fn point_event(
    envelope: &pb::Envelope,
    table: &PointTable,
    process_time_ns: i64,
) -> Result<PointEvent, DecodeError> {
    let point = table
        .index_of(&envelope.point_id)
        .ok_or_else(|| DecodeError::UnknownPoint(envelope.point_id.clone()))?;

    let bytes: [u8; 16] = envelope
        .event_id
        .as_slice()
        .try_into()
        .map_err(|_| DecodeError::EventIdLength(envelope.event_id.len()))?;
    let event_id = EventId::from_bytes(bytes).map_err(DecodeError::EventId)?;

    let value = envelope
        .value
        .clone()
        .ok_or(DecodeError::MissingValue)
        .and_then(|value| Value::try_from(value).map_err(DecodeError::Value))?;

    let quality = envelope
        .quality
        .clone()
        .ok_or(DecodeError::MissingQuality)
        .and_then(|quality| Quality::try_from(quality).map_err(DecodeError::Quality))?;

    Ok(PointEvent {
        point,
        value,
        quality,
        source_time_ns: envelope.source_time_ns,
        receive_time_ns: envelope.receive_time_ns,
        process_time_ns,
        event_id,
        revision_index: table.revision_index(),
    })
}

/// Decodes every event of a batch, or refuses the batch.
///
/// # Errors
///
/// Returns the first [`DecodeError`] of the batch; no event of it is returned.
pub fn batch_events(
    batch: &EventBatch,
    table: &PointTable,
    process_time_ns: i64,
) -> Result<Vec<PointEvent>, DecodeError> {
    batch
        .events
        .iter()
        .map(|envelope| point_event(envelope, table, process_time_ns))
        .collect()
}
