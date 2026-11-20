//! Cycle supervision: detection of values that a station stops renewing
//! (SPECIFICATIONS §7.8, PM-CYC-001 to PM-CYC-004).
//!
//! Each supervised point has a deadline: the reception time of its last
//! accepted value, plus `period` and `grace`, on the monotonic clock. Deadlines
//! are kept in a hashed timing wheel with 100 ms slots. Each timer carries the
//! generation of its point, so a timer replaced by a newer value is ignored.
//!
//! Times are monotonic nanoseconds, supplied by the caller. This module does
//! not read a clock.

use std::time::Duration;

use powermug_pivot::PointIndex;
use thiserror::Error;

/// Width of one wheel slot.
pub const TICK_NS: u64 = 100_000_000;
/// Slots of the wheel: 4,096 slots of 100 ms cover 409.6 s before a timer
/// wraps. Timers beyond that wait in their slot for another lap.
const SLOTS: usize = 4_096;

/// Supervision parameters of a point group (§7.8). Defaults: 30 s, 0 s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Supervision {
    pub period: Duration,
    pub grace: Duration,
}

impl Default for Supervision {
    fn default() -> Self {
        Self {
            period: Duration::from_secs(30),
            grace: Duration::ZERO,
        }
    }
}

/// State of a point in cycle supervision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleState {
    /// Not supervised: no deadline is ever armed.
    Unsupervised,
    /// Supervised, but no accepted value has arrived yet.
    NoValue,
    /// An accepted value is within its deadline.
    Fresh,
    /// The deadline passed: the stale event was written, and nothing re-arms
    /// the point until a new accepted value (PM-CYC-002, PM-CYC-003).
    Stale,
    /// The session is lost: no stale event is written (PM-CYC-004).
    Suspended,
}

/// A point whose deadline passed, returned once per expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expired {
    pub point: PointIndex,
    /// The deadline that passed, in monotonic nanoseconds.
    pub deadline_ns: u64,
}

/// A point index outside the supervisor's table.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("point index {0} is outside the supervisor")]
pub struct OutOfRange(pub u32);

#[derive(Debug, Clone, Copy)]
struct Entry {
    params: Option<Supervision>,
    state: CycleState,
    generation: u64,
    deadline_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Timer {
    point: u32,
    generation: u64,
    deadline_ns: u64,
}

#[derive(Debug)]
struct Wheel {
    slots: Vec<Vec<Timer>>,
    current_tick: u64,
}

impl Wheel {
    fn new(now_ns: u64) -> Self {
        Self {
            slots: vec![Vec::new(); SLOTS],
            current_tick: now_ns / TICK_NS,
        }
    }

    fn slot_of(tick: u64) -> usize {
        usize::try_from(tick % SLOTS as u64).expect("a slot index fits usize")
    }

    fn insert(&mut self, timer: Timer) {
        let tick = (timer.deadline_ns / TICK_NS).max(self.current_tick);
        self.slots[Self::slot_of(tick)].push(timer);
    }

    /// Moves the wheel to `now_ns`, collecting the timers whose deadline has
    /// passed. Timers of a later lap are put back in their slot.
    fn advance(&mut self, now_ns: u64, fired: &mut Vec<Timer>) {
        let target = now_ns / TICK_NS;
        if target < self.current_tick {
            return;
        }
        // A jump of a full lap or more visits each slot once.
        let steps = (target - self.current_tick + 1).min(SLOTS as u64);
        for step in 0..steps {
            let slot = Self::slot_of(self.current_tick + step);
            for timer in std::mem::take(&mut self.slots[slot]) {
                if timer.deadline_ns <= now_ns {
                    fired.push(timer);
                } else {
                    self.insert(timer);
                }
            }
        }
        self.current_tick = target;
    }
}

/// Cycle supervision of the points of one lane.
#[derive(Debug)]
pub struct CycleSupervisor {
    entries: Vec<Entry>,
    wheel: Wheel,
    fired: Vec<Timer>,
}

impl CycleSupervisor {
    /// A supervisor for `points` points, whose clock reads `now_ns`.
    #[must_use]
    pub fn new(points: usize, now_ns: u64) -> Self {
        Self {
            entries: vec![
                Entry {
                    params: None,
                    state: CycleState::Unsupervised,
                    generation: 0,
                    deadline_ns: 0,
                };
                points
            ],
            wheel: Wheel::new(now_ns),
            fired: Vec::new(),
        }
    }

    fn entry_mut(&mut self, point: PointIndex) -> Result<&mut Entry, OutOfRange> {
        self.entries
            .get_mut(point.0 as usize)
            .ok_or(OutOfRange(point.0))
    }

    /// Supervises `point` with `params`. The point waits for its first value.
    ///
    /// # Errors
    ///
    /// Returns [`OutOfRange`] for a point outside the table.
    pub fn supervise(&mut self, point: PointIndex, params: Supervision) -> Result<(), OutOfRange> {
        let entry = self.entry_mut(point)?;
        entry.params = Some(params);
        if entry.state == CycleState::Unsupervised {
            entry.state = CycleState::NoValue;
        }
        Ok(())
    }

    /// Records an accepted value received at `received_ns`. Arms the deadline,
    /// and moves the point to `Fresh` from any state (PM-CYC-003, PM-CYC-004).
    /// An unsupervised point is left alone.
    ///
    /// # Errors
    ///
    /// Returns [`OutOfRange`] for a point outside the table.
    pub fn accepted(&mut self, point: PointIndex, received_ns: u64) -> Result<(), OutOfRange> {
        let entry = self.entry_mut(point)?;
        let Some(params) = entry.params else {
            return Ok(());
        };
        entry.generation += 1;
        entry.state = CycleState::Fresh;
        entry.deadline_ns = received_ns
            .saturating_add(nanos(params.period))
            .saturating_add(nanos(params.grace));
        let timer = Timer {
            point: point.0,
            generation: entry.generation,
            deadline_ns: entry.deadline_ns,
        };
        self.wheel.insert(timer);
        Ok(())
    }

    /// Suspends `point` while its session is lost. Any armed deadline is
    /// cancelled, so no stale event is written for it.
    ///
    /// # Errors
    ///
    /// Returns [`OutOfRange`] for a point outside the table.
    pub fn suspend(&mut self, point: PointIndex) -> Result<(), OutOfRange> {
        let entry = self.entry_mut(point)?;
        if entry.params.is_some() {
            entry.generation += 1;
            entry.state = CycleState::Suspended;
        }
        Ok(())
    }

    /// Advances the clock to `now_ns` and returns the points that became stale.
    /// Each point is returned once per expiry: it then stays `Stale` until an
    /// accepted value re-arms it.
    pub fn expire(&mut self, now_ns: u64) -> Vec<Expired> {
        let mut fired = std::mem::take(&mut self.fired);
        self.wheel.advance(now_ns, &mut fired);
        let mut expired = Vec::new();
        for timer in fired.drain(..) {
            let Some(entry) = self.entries.get_mut(timer.point as usize) else {
                continue;
            };
            let current = entry.generation == timer.generation;
            if current && entry.state == CycleState::Fresh {
                entry.state = CycleState::Stale;
                expired.push(Expired {
                    point: PointIndex(timer.point),
                    deadline_ns: timer.deadline_ns,
                });
            }
        }
        self.fired = fired;
        expired
    }

    /// The state of `point`, or `None` outside the table.
    #[must_use]
    pub fn state(&self, point: PointIndex) -> Option<CycleState> {
        self.entries.get(point.0 as usize).map(|entry| entry.state)
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
