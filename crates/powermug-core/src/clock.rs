//! The core's clocks: a monotonic clock for deadlines, and the wall clock for
//! the time of processing (SPECIFICATIONS §5.1 and §7.8).

use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// The time of one call into the lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Times {
    /// Monotonic nanoseconds since the clock started. Deadlines use it.
    pub monotonic_ns: u64,
    /// Wall-clock nanoseconds since the Unix epoch. Process times use it.
    pub wall_ns: i64,
}

/// Reads both clocks, the monotonic one from the moment the clock was made.
#[derive(Debug, Clone, Copy)]
pub struct Clock {
    origin: Instant,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }

    /// The current times.
    #[must_use]
    pub fn now(&self) -> Times {
        let monotonic = self.origin.elapsed();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Times {
            monotonic_ns: u64::try_from(monotonic.as_nanos()).unwrap_or(u64::MAX),
            wall_ns: i64::try_from(wall.as_nanos()).unwrap_or(i64::MAX),
        }
    }
}
