//! The delivery of journaled events to a destination of type flux (SPECIFICATIONS 10.3,
//! FUN-SAF-002 to FUN-SAF-005). A destination caught up with the lane is served from the live ring
//! of the lane. A destination behind it is served from the journal, from its cursor, marked as a
//! replay and capped at `limit` events per call. The cursor moves only when the destination
//! acknowledges: a batch not acknowledged is delivered again, so the delivery is at least once.

use powermug_pivot::proto::v1 as pb;
use powermug_store::{StoreError, TelemetryLog};
use prost::Message;
use thiserror::Error;

use crate::lane::Lane;

/// The envelopes of one call, and where they come from.
#[derive(Debug, Clone, PartialEq)]
pub struct Feed {
    pub envelopes: Vec<pb::Envelope>,
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

/// The next envelopes for `destination`, after its cursor, at most `limit` of them.
///
/// # Errors
///
/// Returns [`DeliveryError::Store`] for an undeclared destination or a failed read, and
/// [`DeliveryError::Decode`] for a journaled event that is not an envelope.
pub fn feed(
    lane: &Lane,
    log: &TelemetryLog,
    destination: &str,
    limit: u32,
) -> Result<Feed, DeliveryError> {
    let cursor = log
        .cursor(destination)?
        .ok_or_else(|| StoreError::UnknownCursor(destination.to_owned()))?
        .last_seq;
    if lane.live().covers(cursor) {
        return Ok(Feed {
            envelopes: lane.live().after(cursor, limit as usize),
            replay: false,
            skipped_before: None,
        });
    }
    let replay = log.replay(destination, limit)?;
    let envelopes = replay
        .events
        .iter()
        .map(|event| pb::Envelope::decode(event.payload.as_slice()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Feed {
        envelopes,
        replay: true,
        skipped_before: replay.skipped_before,
    })
}
