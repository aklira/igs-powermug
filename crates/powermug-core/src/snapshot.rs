//! Encoding of the image for a snapshot (SPECIFICATIONS §7.2).
//!
//! The snapshot is written every 60 s and restored at start, so that the
//! recovery reads only the journal written after it.

use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{PointIndex, Quality, Value};
use prost::Message;

use crate::image::{ProcessImage, Slot};

#[derive(Clone, PartialEq, Message)]
struct SlotRecord {
    #[prost(uint32, tag = "1")]
    point: u32,
    #[prost(message, optional, tag = "2")]
    value: Option<pb::Value>,
    #[prost(message, optional, tag = "3")]
    quality: Option<pb::Quality>,
    #[prost(int64, tag = "4")]
    source_time_ns: i64,
    #[prost(int64, tag = "5")]
    receive_time_ns: i64,
    #[prost(uint32, tag = "6")]
    revision_index: u32,
}

#[derive(Clone, PartialEq, Message)]
struct ImageRecord {
    #[prost(message, repeated, tag = "1")]
    slots: Vec<SlotRecord>,
}

/// Encodes every value of the image.
#[must_use]
pub fn encode(image: &ProcessImage) -> Vec<u8> {
    let record = ImageRecord {
        slots: image
            .iter()
            .map(|(point, slot)| SlotRecord {
                point: point.0,
                value: Some(slot.value.into()),
                quality: Some(slot.quality.into()),
                source_time_ns: slot.source_time_ns,
                receive_time_ns: slot.receive_time_ns,
                revision_index: slot.revision_index,
            })
            .collect(),
    };
    record.encode_to_vec()
}

/// Restores the values of an encoded image into `image`.
///
/// # Errors
///
/// Returns a description of the first record that does not decode.
pub fn restore(payload: &[u8], image: &mut ProcessImage) -> Result<(), String> {
    let record = ImageRecord::decode(payload).map_err(|err| err.to_string())?;
    for slot in record.slots {
        let value = slot
            .value
            .ok_or_else(|| format!("point {} has no value", slot.point))
            .and_then(|value| Value::try_from(value).map_err(|err| err.to_string()))?;
        let quality = slot
            .quality
            .ok_or_else(|| format!("point {} has no quality", slot.point))
            .and_then(|quality| Quality::try_from(quality).map_err(|err| err.to_string()))?;
        image
            .restore(
                PointIndex(slot.point),
                Slot {
                    value,
                    quality,
                    source_time_ns: slot.source_time_ns,
                    receive_time_ns: slot.receive_time_ns,
                    revision_index: slot.revision_index,
                },
            )
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}
