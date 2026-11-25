//! The live ring of a lane: the last journaled envelopes, in sequence order, kept in memory for
//! the destinations that are caught up (FUN-SAF-002, FUN-SAF-005). A destination behind the ring
//! reads the journal instead (see [`crate::delivery`]). Class C is not journaled, so it is not in
//! the ring: it has no sequence number and no destination can acknowledge it.

use std::collections::VecDeque;

use powermug_pivot::proto::v1 as pb;

/// The envelopes kept by default: enough for a destination that is a few seconds behind.
pub const DEFAULT_CAPACITY: usize = 4_096;

/// The last `capacity` journaled envelopes of a lane.
#[derive(Debug, Clone)]
pub struct LiveRing {
    capacity: usize,
    events: VecDeque<(i64, pb::Envelope)>,
    head: i64,
}

impl LiveRing {
    /// An empty ring of `capacity` envelopes (at least one).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            events: VecDeque::new(),
            head: 0,
        }
    }

    /// Adds the envelope of journal sequence `seq`, evicting the oldest one when full.
    pub fn push(&mut self, seq: i64, envelope: pb::Envelope) {
        if self.events.len() == self.capacity {
            self.events.pop_front();
        }
        self.events.push_back((seq, envelope));
        self.head = seq;
    }

    /// The sequence number of the last envelope pushed: zero before any.
    #[must_use]
    pub fn head(&self) -> i64 {
        self.head
    }

    /// The first sequence number the ring still holds: `head + 1` when it is empty.
    #[must_use]
    pub fn oldest(&self) -> i64 {
        self.events.front().map_or(self.head + 1, |(seq, _)| *seq)
    }

    /// Whether a destination that acknowledged `cursor` can be served from the ring: every event
    /// after the cursor is still in it.
    #[must_use]
    pub fn covers(&self, cursor: i64) -> bool {
        cursor + 1 >= self.oldest()
    }

    /// Up to `limit` envelopes after `cursor`, in sequence order.
    #[must_use]
    pub fn after(&self, cursor: i64, limit: usize) -> Vec<pb::Envelope> {
        self.events
            .iter()
            .filter(|(seq, _)| *seq > cursor)
            .take(limit)
            .map(|(_, envelope)| envelope.clone())
            .collect()
    }
}
