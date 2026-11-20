//! The lane as the core's batch sink: the contract hands it each accepted batch,
//! and it returns once the batch is committed (PM-TEL-002).

use std::sync::{Arc, Mutex, PoisonError};

use powermug_contract::proto::v1::EventBatch;
use powermug_contract::sink::{BatchSink, Durability, Refusal};
use powermug_pivot::proto::v1::CommunicationState;

use crate::clock::Clock;
use crate::decode::batch_events;
use crate::lane::{Lane, SessionId};

/// Feeds the adapter stream of one partition into its lane.
///
/// The lane is shared, so the core can read its image while the stream runs.
#[derive(Debug, Clone)]
pub struct LaneSink {
    lane: Arc<Mutex<Lane>>,
    clock: Clock,
}

impl LaneSink {
    #[must_use]
    pub fn new(lane: Arc<Mutex<Lane>>, clock: Clock) -> Self {
        Self { lane, clock }
    }
}

impl BatchSink for LaneSink {
    fn commit(&mut self, batch: &EventBatch) -> Result<Durability, Refusal> {
        let times = self.clock.now();
        let mut lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        let events = batch_events(batch, lane.table(), times.wall_ns)
            .map_err(|err| Refusal::new(err.to_string()))?;
        let committed = lane
            .ingest(&events, times)
            .map_err(|err| Refusal::new(err.to_string()))?;
        Ok(match committed {
            Some(_) => Durability::Durable,
            None => Durability::NotJournaled,
        })
    }

    fn communication(&mut self, state: &CommunicationState) -> Result<(), Refusal> {
        let session = state
            .session_id
            .parse::<u32>()
            .map(SessionId)
            .map_err(|_| {
                Refusal::new(format!("session id {} is not a number", state.session_id))
            })?;
        let times = self.clock.now();
        let mut lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        let result = if state.connected {
            lane.session_restored(session, &state.reason, times)
        } else {
            lane.session_lost(session, &state.reason, times)
        };
        result
            .map(|_| ())
            .map_err(|err| Refusal::new(err.to_string()))
    }
}
