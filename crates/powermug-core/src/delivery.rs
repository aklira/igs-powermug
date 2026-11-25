//! The delivery of journaled events to a flux destination (SPECIFICATIONS 10.3,
//! FUN-SAF-002 to FUN-SAF-005). The events after a sequence number are served from the live ring of
//! the lane when it still holds them. Otherwise they are read from the journal, marked as a replay
//! and capped per call. The destination's cursor moves only on acknowledgement, so a batch not
//! acknowledged is delivered again: at least once.

use powermug_pivot::proto::v1 as pb;
use powermug_store::StoreError;
use prost::Message;
use thiserror::Error;

use crate::lane::Lane;

/// The envelopes of one call, each with its journal sequence number, and where they come from.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    pub items: Vec<(i64, pb::Envelope)>,
    /// Whether the envelopes come from the journal, behind the live ring (`replay=true`).
    pub replay: bool,
    /// When retention deleted events the destination had not acknowledged, the oldest sequence
    /// number still retained: the destination is told of the gap before the envelopes.
    pub skipped_before: Option<i64>,
}

/// A feed could not be read.
#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("a journaled event does not decode: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// The events after the journal sequence `after`, at most `limit` of them.
///
/// # Errors
///
/// Returns [`DeliveryError::Store`] when the journal cannot be read, and
/// [`DeliveryError::Decode`] for a journaled event that is not an envelope.
pub fn after(lane: &Lane, after: i64, limit: u32) -> Result<Batch, DeliveryError> {
    if lane.live().covers(after) {
        return Ok(Batch {
            items: lane.live().after(after, limit as usize),
            replay: false,
            skipped_before: None,
        });
    }
    let stored = lane.log().read_from(after + 1, limit)?;
    let skipped_before = stored
        .first()
        .filter(|event| event.seq > after + 1)
        .map(|event| event.seq);
    let items = stored
        .iter()
        .map(|event| Ok((event.seq, pb::Envelope::decode(event.payload.as_slice())?)))
        .collect::<Result<Vec<_>, DeliveryError>>()?;
    Ok(Batch {
        items,
        replay: true,
        skipped_before,
    })
}
