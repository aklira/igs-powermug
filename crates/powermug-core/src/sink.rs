//! The lane as the core's batch sink: the contract hands it each accepted batch,
//! and it returns once the batch is committed (PM-TEL-002).

use std::sync::{Arc, Mutex, PoisonError};

use powermug_contract::proto::v1::EventBatch;
use powermug_contract::sink::{BatchSink, Refusal};

use crate::clock::Clock;
use crate::decode::batch_events;
use crate::lane::Lane;

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
    fn commit(&mut self, batch: &EventBatch) -> Result<(), Refusal> {
        let times = self.clock.now();
        let mut lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        let events = batch_events(batch, lane.table(), times.wall_ns)
            .map_err(|err| Refusal::new(err.to_string()))?;
        lane.ingest(&events, times)
            .map(|_| ())
            .map_err(|err| Refusal::new(err.to_string()))
    }
}
