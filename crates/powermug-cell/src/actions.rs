//! The host of the local actions of the cell (SPECIFICATIONS 11.4, PM-ACT-001 to 003): what the cell
//! does for each action the executor decides to run. The executor decides and audits each request;
//! this host carries the action out, and refuses what the cell cannot do yet. No action here changes
//! ACLs, command policy, partition ownership or configuration.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use powermug_core::actions::{Action, ActionHost};
use powermug_core::{Clock, Lane};
use tokio::sync::Notify;

/// How long a `shed_class_c` order lasts. The revision does not give another duration yet (H).
pub const SHED_WINDOW: Duration = Duration::from_secs(60);

/// The local actions the cell can carry out, with the handles they need.
#[derive(Debug)]
pub struct CellHost {
    /// The restart request of each South session, which its supervisor waits on.
    restarts: BTreeMap<u32, Arc<Notify>>,
    /// The lanes, each with the site it serves.
    lanes: Vec<(String, Arc<Mutex<Lane>>)>,
    clock: Clock,
    shed_window: Duration,
}

impl CellHost {
    #[must_use]
    pub fn new(
        restarts: BTreeMap<u32, Arc<Notify>>,
        lanes: Vec<(String, Arc<Mutex<Lane>>)>,
        shed_window: Duration,
    ) -> Self {
        Self {
            restarts,
            lanes,
            clock: Clock::new(),
            shed_window,
        }
    }
}

impl ActionHost for CellHost {
    /// Runs `action` on `target`: a South session for `restart_adapter`, a site for the others.
    ///
    /// A restart is requested, not awaited: the supervisor replaces the process at its next wait, as
    /// a planned restart, which is not a failure. A request made while the adapter is between two
    /// starts is kept until the next wait, so the process may be replaced once more.
    fn run(&mut self, action: Action, target: &str) -> Result<(), String> {
        match action {
            Action::RestartAdapter => {
                let session: u32 = target
                    .parse()
                    .map_err(|_| format!("{target} is not a session"))?;
                let restart = self
                    .restarts
                    .get(&session)
                    .ok_or_else(|| format!("no adapter for session {session}"))?;
                restart.notify_one();
                Ok(())
            }
            Action::ShedClassC => {
                let window = i64::try_from(self.shed_window.as_nanos()).unwrap_or(i64::MAX);
                let until = self.clock.now().wall_ns.saturating_add(window);
                let mut served = 0_usize;
                for (site, lane) in &self.lanes {
                    if site == target {
                        lane.lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .force_shed(until);
                        served += 1;
                    }
                }
                if served == 0 {
                    Err(format!("no lane of site {target}"))
                } else {
                    Ok(())
                }
            }
            Action::Alert => {
                // The audited request is the alert: there is no operator channel to call yet.
                eprintln!("alert for {target}");
                Ok(())
            }
            Action::GeneralInterrogation => {
                Err("the contract has no request for a general interrogation yet".into())
            }
            Action::SwitchRedundantConnection => {
                Err("the contract has no request to switch a redundant connection yet".into())
            }
            Action::PurgeTmp | Action::FailoverCell | Action::QuarantineProcessor => {
                Err(format!("{} is not run by this cell yet", action.name()))
            }
        }
    }
}
