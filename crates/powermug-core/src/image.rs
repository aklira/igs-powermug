//! Process image: the last value of every point, one slot per `PointIndex`
//! (SPECIFICATIONS §7.3).
//!
//! The image is written only by a [`crate::lane::Lane`], after the batch that
//! carries the value is committed (PM-TEL-001). Nothing else can change it.

use powermug_pivot::{PointEvent, PointIndex, Quality, Value};
use thiserror::Error;

/// Last known state of one point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Slot {
    pub value: Value,
    pub quality: Quality,
    pub source_time_ns: i64,
    pub receive_time_ns: i64,
    pub revision_index: u32,
}

/// A point index outside the image.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("point index {0} is outside the image")]
pub struct OutOfRange(pub u32);

/// The image of one lane.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessImage {
    slots: Vec<Option<Slot>>,
}

impl ProcessImage {
    /// An empty image with one slot per point.
    #[must_use]
    pub fn with_points(count: usize) -> Self {
        Self {
            slots: vec![None; count],
        }
    }

    /// The slot of `point`, if the point has a value.
    #[must_use]
    pub fn get(&self, point: PointIndex) -> Option<&Slot> {
        self.slots.get(point.0 as usize).and_then(Option::as_ref)
    }

    /// Replaces the slot of the event's point. Only the lane calls this.
    pub(crate) fn apply(&mut self, event: &PointEvent) -> Result<(), OutOfRange> {
        let slot = self
            .slots
            .get_mut(event.point.0 as usize)
            .ok_or(OutOfRange(event.point.0))?;
        *slot = Some(Slot {
            value: event.value,
            quality: event.quality,
            source_time_ns: event.source_time_ns,
            receive_time_ns: event.receive_time_ns,
            revision_index: event.revision_index,
        });
        Ok(())
    }
}
