//! The lane as the core's batch sink: the contract hands it each accepted batch,
//! and it returns once the batch is committed (PM-TEL-002). The envelopes of the
//! committed events are published to the subscribers of the North adapters.

use std::sync::{Arc, Mutex, PoisonError};

use powermug_contract::proto::v1::EventBatch;
use powermug_contract::sink::{BatchSink, Durability, NorthFeed, Refusal};
use powermug_pivot::proto::v1::{CommunicationState, Envelope};
use tokio::sync::broadcast;

use crate::clock::Clock;
use crate::decode::batch_events;
use crate::delivery;
use crate::lane::{Lane, SessionId};

/// Events kept for a subscriber that is slow to read them.
const PUBLISH_CAPACITY: usize = 4_096;

/// The North feed of one declared destination, read through the lane: its events come from the
/// live ring or the journal, and its acknowledgements move the cursor the store keeps.
struct LaneFeed {
    lane: Arc<Mutex<Lane>>,
    destination: String,
    clock: Clock,
}

impl NorthFeed for LaneFeed {
    fn cursor(&mut self) -> Result<i64, Refusal> {
        let lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        let cursor = lane
            .log()
            .cursor(&self.destination)
            .map_err(|err| Refusal::new(err.to_string()))?;
        cursor.map(|cursor| cursor.last_seq).ok_or_else(|| {
            Refusal::new(format!("destination {} is not declared", self.destination))
        })
    }

    fn next(&mut self, after: i64, limit: u32) -> Result<Vec<(i64, Envelope)>, Refusal> {
        let lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        let batch =
            delivery::after(&lane, after, limit).map_err(|err| Refusal::new(err.to_string()))?;
        if let Some(first) = batch.skipped_before {
            // Retention removed events the destination had not acknowledged: they are reported,
            // never skipped silently.
            return Err(Refusal::new(format!(
                "events {} to {} were removed by retention before the destination acknowledged them",
                after + 1,
                first - 1
            )));
        }
        Ok(batch.items)
    }

    fn acknowledge(&mut self, seq: i64) -> Result<(), Refusal> {
        let now_ns = self.clock.now().wall_ns;
        let mut lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        lane.log_mut()
            .acknowledge(&self.destination, seq, now_ns)
            .map(|_| ())
            .map_err(|err| Refusal::new(err.to_string()))
    }
}

/// Feeds the adapter stream of one partition into its lane.
///
/// The lane is shared, so the core can read its image while the stream runs.
#[derive(Debug, Clone)]
pub struct LaneSink {
    lane: Arc<Mutex<Lane>>,
    clock: Clock,
    published: broadcast::Sender<Envelope>,
}

impl LaneSink {
    #[must_use]
    pub fn new(lane: Arc<Mutex<Lane>>, clock: Clock) -> Self {
        let (published, _) = broadcast::channel(PUBLISH_CAPACITY);
        Self {
            lane,
            clock,
            published,
        }
    }

    /// Sends the envelopes the lane committed to the subscribers. Without a
    /// subscriber the envelopes are dropped, as nothing waits for them.
    fn publish(&self, lane: &mut Lane) {
        for envelope in lane.take_published() {
            let _ = self.published.send(envelope);
        }
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
            .map_err(|err| Refusal::new(err.to_string()));
        self.publish(&mut lane);
        Ok(match committed? {
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
        self.publish(&mut lane);
        result
            .map(|_| ())
            .map_err(|err| Refusal::new(err.to_string()))
    }

    fn snapshot(&mut self, session_id: &str) -> Result<Vec<Envelope>, Refusal> {
        let session = session_id
            .parse::<u32>()
            .map(SessionId)
            .map_err(|_| Refusal::new(format!("session id {session_id} is not a number")))?;
        let lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(lane.session_state(session))
    }

    fn subscribe(&mut self) -> broadcast::Receiver<Envelope> {
        self.published.subscribe()
    }

    /// A destination named in the capabilities has a cursor and a feed. An unnamed subscriber has
    /// none: it is served the live broadcast only.
    fn north_feed(&mut self, destination: &str) -> Result<Option<Box<dyn NorthFeed>>, Refusal> {
        if destination.is_empty() {
            return Ok(None);
        }
        let declared = {
            let lane = self.lane.lock().unwrap_or_else(PoisonError::into_inner);
            lane.log()
                .cursor(destination)
                .map_err(|err| Refusal::new(err.to_string()))?
        };
        if declared.is_none() {
            return Err(Refusal::new(format!(
                "destination {destination} is not declared"
            )));
        }
        Ok(Some(Box::new(LaneFeed {
            lane: Arc::clone(&self.lane),
            destination: destination.to_owned(),
            clock: self.clock,
        })))
    }
}
