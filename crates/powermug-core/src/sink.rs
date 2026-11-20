//! The lane as the core's batch sink: the contract hands it each accepted batch,
//! and it returns once the batch is committed (PM-TEL-002).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use powermug_contract::proto::v1::EventBatch;
use powermug_contract::sink::{BatchSink, Refusal};

use crate::decode::batch_events;
use crate::lane::Lane;

/// Feeds the adapter stream of one partition into its lane.
///
/// The lane is shared, so the core can read its image while the stream runs.
#[derive(Debug, Clone)]
pub struct LaneSink {
    lane: Arc<Mutex<Lane>>,
}

impl LaneSink {
    #[must_use]
    pub fn new(lane: Arc<Mutex<Lane>>) -> Self {
        Self { lane }
    }
}

fn now_ns() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
}

impl BatchSink for LaneSink {
    fn commit(&mut self, batch: &EventBatch) -> Result<(), Refusal> {
        let mut lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        let events = batch_events(batch, lane.table(), now_ns())
            .map_err(|err| Refusal::new(err.to_string()))?;
        lane.ingest(&events)
            .map(|_| ())
            .map_err(|err| Refusal::new(err.to_string()))
    }
}
