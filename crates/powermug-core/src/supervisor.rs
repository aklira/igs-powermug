//! Adapter supervision: lifecycle, restart budget, backoff with jitter, quarantine
//! (§6.2, PM-ADP-003).
//!
//! The state machine is pure: it takes the current time and a jitter value as inputs and
//! returns decisions. The process handling (spawning, killing, reading exit status) belongs
//! to the caller, which makes every rule testable without a clock or a process.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Restart policy of one adapter instance (PM-ADP-003, all values (H)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Delay before the first restart.
    pub initial_backoff: Duration,
    /// Upper bound of the delay.
    pub max_backoff: Duration,
    /// Restarts allowed inside `budget_window`.
    pub budget: usize,
    /// Length of the window in which restarts are counted.
    pub budget_window: Duration,
    /// Running time after which the consecutive-failure count is cleared (H). A process
    /// that crashes right after its handshake keeps its backoff.
    pub stable_after: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            budget: 5,
            budget_window: Duration::from_secs(600),
            stable_after: Duration::from_secs(60),
        }
    }
}

/// Lifecycle states of an instance, as in §6.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// The process is being started.
    Spawning,
    /// The process is running.
    Running,
    /// The process died; it restarts at `at`.
    Backoff {
        /// The instant of the restart.
        at: Instant,
    },
    /// The restart budget is spent. Only a new revision or the local action
    /// `restart_adapter` releases the instance.
    Quarantined,
    /// Stopped on request. It does not restart.
    Stopped,
}

/// The decision taken after an unexpected exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Restart at the given instant.
    RestartAt(Instant),
    /// The budget is spent: quarantine and raise an alert.
    Quarantine,
}

/// Why a supervisor refused a transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The transition does not apply in the current state.
    WrongState(State),
}

/// Supervises one adapter instance.
#[derive(Debug, Clone)]
pub struct Supervisor {
    policy: Policy,
    state: State,
    /// Instants of the restarts still inside the budget window, oldest first.
    restarts: VecDeque<Instant>,
    /// Consecutive unexpected exits, used for the exponential delay.
    failures: u32,
    /// When the current process reached `Running`.
    running_since: Option<Instant>,
}

impl Supervisor {
    /// A supervisor in state `Spawning` with the given policy.
    #[must_use]
    pub fn new(policy: Policy) -> Self {
        Self {
            policy,
            state: State::Spawning,
            restarts: VecDeque::new(),
            failures: 0,
            running_since: None,
        }
    }

    /// The current state.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// The process has started (handshake done) at `now`.
    ///
    /// # Errors
    ///
    /// Refused unless the state is `Spawning`.
    pub fn started(&mut self, now: Instant) -> Result<(), Refusal> {
        match self.state {
            State::Spawning => {
                self.state = State::Running;
                self.running_since = Some(now);
                Ok(())
            }
            other => Err(Refusal::WrongState(other)),
        }
    }

    /// The process exited without being asked to. `jitter` is a value in `[0, 1]` that
    /// spreads the delay: the delay lies between half and all of the exponential value.
    ///
    /// # Errors
    ///
    /// Refused unless the state is `Running` or `Spawning`.
    pub fn crashed(&mut self, now: Instant, jitter: f64) -> Result<Decision, Refusal> {
        match self.state {
            State::Running | State::Spawning => {}
            other => return Err(Refusal::WrongState(other)),
        }
        // A process that ran long enough was healthy: its failures do not add up.
        if let Some(since) = self.running_since {
            if now.duration_since(since) >= self.policy.stable_after {
                self.failures = 0;
            }
        }
        self.running_since = None;
        self.failures = self.failures.saturating_add(1);
        self.expire_window(now);
        if self.restarts.len() >= self.policy.budget {
            self.state = State::Quarantined;
            return Ok(Decision::Quarantine);
        }
        let delay = self.delay(jitter);
        let at = now + delay;
        self.restarts.push_back(now);
        self.state = State::Backoff { at };
        Ok(Decision::RestartAt(at))
    }

    /// The restart instant has been reached: the caller spawns the process.
    ///
    /// # Errors
    ///
    /// Refused unless the state is `Backoff` and its instant has passed.
    pub fn restart_due(&mut self, now: Instant) -> Result<(), Refusal> {
        match self.state {
            State::Backoff { at } if now >= at => {
                self.state = State::Spawning;
                Ok(())
            }
            other => Err(Refusal::WrongState(other)),
        }
    }

    /// Releases a quarantined instance: a new revision or the local action `restart_adapter`.
    /// The budget starts afresh.
    ///
    /// # Errors
    ///
    /// Refused unless the state is `Quarantined`.
    pub fn release(&mut self) -> Result<(), Refusal> {
        match self.state {
            State::Quarantined => {
                self.restarts.clear();
                self.failures = 0;
                self.running_since = None;
                self.state = State::Spawning;
                Ok(())
            }
            other => Err(Refusal::WrongState(other)),
        }
    }

    /// Stops the instance on request. It never restarts from here.
    pub fn stop(&mut self) {
        self.state = State::Stopped;
    }

    /// Delay after `failures` consecutive exits: `initial * 2^(failures-1)`, capped at the
    /// maximum, then reduced by jitter to a value between half and all of it.
    fn delay(&self, jitter: f64) -> Duration {
        let exponent = self.failures.saturating_sub(1).min(31);
        let base = self
            .policy
            .initial_backoff
            .saturating_mul(1u32 << exponent)
            .min(self.policy.max_backoff);
        let factor = 0.5 + 0.5 * jitter.clamp(0.0, 1.0);
        base.mul_f64(factor)
    }

    fn expire_window(&mut self, now: Instant) {
        while let Some(front) = self.restarts.front() {
            if now.duration_since(*front) >= self.policy.budget_window {
                self.restarts.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simulated clock: every test advances time explicitly, never through `Instant::now`.
    fn origin() -> Instant {
        Instant::now()
    }

    #[test]
    fn a_crash_schedules_a_restart_within_the_backoff_bounds() {
        let start = origin();
        let mut full = Supervisor::new(Policy::default());
        full.started(start).unwrap();
        // jitter 1.0 gives the full delay: 1 s for the first failure.
        let Decision::RestartAt(at) = full.crashed(start, 1.0).unwrap() else {
            panic!("expected a restart");
        };
        assert_eq!(at - start, Duration::from_secs(1));

        let mut half = Supervisor::new(Policy::default());
        half.started(start).unwrap();
        // jitter 0.0 gives half the delay.
        let Decision::RestartAt(at) = half.crashed(start, 0.0).unwrap() else {
            panic!("expected a restart");
        };
        assert_eq!(at - start, Duration::from_millis(500));
    }

    #[test]
    fn the_delay_doubles_and_is_capped_while_crashes_follow_each_other() {
        let policy = Policy {
            budget: 100,
            ..Policy::default()
        };
        let mut sup = Supervisor::new(policy);
        let mut now = origin();
        let mut delays = Vec::new();
        for _ in 0..8 {
            sup.started(now).unwrap();
            // The process dies at once: no stable run, so the backoff keeps growing.
            let Decision::RestartAt(at) = sup.crashed(now, 1.0).unwrap() else {
                panic!("restart expected");
            };
            delays.push(at - now);
            now = at;
            sup.restart_due(now).unwrap();
        }
        assert_eq!(delays[0], Duration::from_secs(1));
        assert_eq!(delays[1], Duration::from_secs(2));
        assert_eq!(delays[2], Duration::from_secs(4));
        assert_eq!(delays[5], Duration::from_secs(32));
        assert_eq!(delays[6], Duration::from_secs(60), "capped at max_backoff");
        assert_eq!(delays[7], Duration::from_secs(60));
    }

    #[test]
    fn the_budget_quarantines_the_sixth_crash_inside_the_window() {
        let mut sup = Supervisor::new(Policy::default());
        let mut now = origin();
        for i in 0..5 {
            sup.started(now).unwrap();
            let decision = sup.crashed(now, 0.5).unwrap();
            assert!(
                matches!(decision, Decision::RestartAt(_)),
                "crash {i} still within budget"
            );
            let State::Backoff { at } = sup.state() else {
                panic!("backoff expected");
            };
            now = at;
            sup.restart_due(now).unwrap();
        }
        sup.started(now).unwrap();
        assert_eq!(sup.crashed(now, 0.5).unwrap(), Decision::Quarantine);
        assert_eq!(sup.state(), State::Quarantined);
    }

    #[test]
    fn restarts_older_than_the_window_do_not_count() {
        let mut sup = Supervisor::new(Policy::default());
        let start = origin();
        for _ in 0..5 {
            sup.started(start).unwrap();
            let _ = sup.crashed(start, 0.5).unwrap();
            sup.restart_due(start + Duration::from_secs(3600)).unwrap();
        }
        // Five restarts at `start` are outside the 10-minute window one hour later.
        let later = start + Duration::from_secs(3600);
        sup.started(later).unwrap();
        assert!(matches!(
            sup.crashed(later, 0.5).unwrap(),
            Decision::RestartAt(_)
        ));
    }

    #[test]
    fn a_quarantined_instance_only_leaves_by_release() {
        let mut sup = Supervisor::new(Policy {
            budget: 0,
            ..Policy::default()
        });
        let now = origin();
        sup.started(now).unwrap();
        assert_eq!(sup.crashed(now, 0.5).unwrap(), Decision::Quarantine);
        assert!(sup.restart_due(now + Duration::from_secs(3600)).is_err());
        sup.release().unwrap();
        assert_eq!(sup.state(), State::Spawning);
    }

    #[test]
    fn a_stopped_instance_never_restarts() {
        let mut sup = Supervisor::new(Policy::default());
        let now = origin();
        sup.started(now).unwrap();
        sup.stop();
        assert!(sup.crashed(now, 0.5).is_err());
        assert_eq!(sup.state(), State::Stopped);
    }

    #[test]
    fn a_restart_before_its_instant_is_refused() {
        let mut sup = Supervisor::new(Policy::default());
        let now = origin();
        sup.started(now).unwrap();
        let _ = sup.crashed(now, 1.0).unwrap();
        assert!(sup.restart_due(now).is_err(), "backoff not yet over");
        assert!(sup.restart_due(now + Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn a_crash_after_a_stable_run_starts_the_backoff_again() {
        let mut sup = Supervisor::new(Policy::default());
        let start = origin();
        sup.started(start).unwrap();
        let _ = sup.crashed(start, 1.0).unwrap(); // first failure: 1 s
        let back = start + Duration::from_secs(1);
        sup.restart_due(back).unwrap();
        sup.started(back).unwrap();
        // The process ran for 61 s, longer than stable_after (60 s): the count restarts.
        let died = back + Duration::from_secs(61);
        let Decision::RestartAt(at) = sup.crashed(died, 1.0).unwrap() else {
            panic!("restart expected");
        };
        assert_eq!(at - died, Duration::from_secs(1));
    }

    #[test]
    fn a_crash_right_after_a_start_keeps_the_backoff_growing() {
        let mut sup = Supervisor::new(Policy::default());
        let start = origin();
        sup.started(start).unwrap();
        let _ = sup.crashed(start, 1.0).unwrap(); // 1 s
        let back = start + Duration::from_secs(1);
        sup.restart_due(back).unwrap();
        sup.started(back).unwrap();
        // Died after 2 s of running: not stable, so the next delay doubles.
        let died = back + Duration::from_secs(2);
        let Decision::RestartAt(at) = sup.crashed(died, 1.0).unwrap() else {
            panic!("restart expected");
        };
        assert_eq!(at - died, Duration::from_secs(2));
    }
}
