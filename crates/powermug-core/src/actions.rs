//! Local actions (SPECIFICATIONS 11.4, PM-ACT-001 to PM-ACT-003, [GW] 12.8): a closed catalogue of
//! actions the cell runs on its own, each bounded by a budget, idempotent on its `action_id`, and
//! audited with its trigger. No action changes ACLs, command policy, partition ownership, or
//! configuration, and none resolves an `IN_DOUBT` command: the executor has no access to them.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// The closed catalogue. A name outside it is refused (PM-ACT-001).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    RestartAdapter,
    ShedClassC,
    GeneralInterrogation,
    SwitchRedundantConnection,
    Alert,
    PurgeTmp,
    FailoverCell,
    QuarantineProcessor,
}

impl Action {
    /// The catalogue name of an action, if the name is in the catalogue.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "restart_adapter" => Self::RestartAdapter,
            "shed_class_c" => Self::ShedClassC,
            "general_interrogation" => Self::GeneralInterrogation,
            "switch_redundant_connection" => Self::SwitchRedundantConnection,
            "alert" => Self::Alert,
            "purge_tmp" => Self::PurgeTmp,
            "failover_cell" => Self::FailoverCell,
            "quarantine_processor" => Self::QuarantineProcessor,
            _ => return None,
        })
    }

    /// The catalogue name of the action.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::RestartAdapter => "restart_adapter",
            Self::ShedClassC => "shed_class_c",
            Self::GeneralInterrogation => "general_interrogation",
            Self::SwitchRedundantConnection => "switch_redundant_connection",
            Self::Alert => "alert",
            Self::PurgeTmp => "purge_tmp",
            Self::FailoverCell => "failover_cell",
            Self::QuarantineProcessor => "quarantine_processor",
        }
    }

    /// Whether this cell runs the action. The signed remediations (`purge_tmp`, `failover_cell`)
    /// and the processor quarantine need their own pieces (P5, P8) and are refused until then.
    #[must_use]
    pub fn runs_here(self) -> bool {
        matches!(
            self,
            Self::RestartAdapter
                | Self::ShedClassC
                | Self::GeneralInterrogation
                | Self::SwitchRedundantConnection
                | Self::Alert
        )
    }
}

/// The budget of an action: at most `max` runs in any window of `window_ns` nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub max: usize,
    pub window_ns: i64,
}

/// What the cell does for an action. The executor decides; the host carries out.
pub trait ActionHost {
    /// Runs `action` on `target` (a session or a site).
    ///
    /// # Errors
    ///
    /// Returns the reason when the action cannot be carried out.
    fn run(&mut self, action: Action, target: &str) -> Result<(), String>;
}

/// The outcome of one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done,
    Refused(String),
    /// The `action_id` was already handled: nothing is run again.
    Duplicate,
}

/// One audited request: its action, its trigger (an indicator, or a signed request), and its outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub action_id: String,
    pub action: String,
    pub trigger: String,
    pub outcome: Outcome,
    pub at_ns: i64,
}

/// A request to run an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub action_id: String,
    /// The catalogue name, as the revision or the signed request gives it.
    pub action: String,
    /// The session or the site the action is for.
    pub target: String,
    /// The indicator that triggered the action, or the signed request that asks for it.
    pub trigger: String,
}

/// Runs the local actions of a cell, within their budgets, once per `action_id`.
#[derive(Debug)]
pub struct Executor {
    budgets: BTreeMap<Action, Budget>,
    recent: BTreeMap<Action, VecDeque<i64>>,
    handled: BTreeSet<String>,
    audit: Vec<Record>,
}

impl Executor {
    /// An executor with the budgets of the revision. An action without a budget is not allowed
    /// to run: a bound is required (PM8).
    #[must_use]
    pub fn new(budgets: BTreeMap<Action, Budget>) -> Self {
        Self {
            budgets,
            recent: BTreeMap::new(),
            handled: BTreeSet::new(),
            audit: Vec::new(),
        }
    }

    /// Handles one request. Every handled request is audited.
    pub fn execute(
        &mut self,
        host: &mut dyn ActionHost,
        request: &Request,
        now_ns: i64,
    ) -> Outcome {
        if self.handled.contains(&request.action_id) {
            return Outcome::Duplicate;
        }
        let outcome = self.decide(host, request, now_ns);
        self.handled.insert(request.action_id.clone());
        self.audit.push(Record {
            action_id: request.action_id.clone(),
            action: request.action.clone(),
            trigger: request.trigger.clone(),
            outcome: outcome.clone(),
            at_ns: now_ns,
        });
        outcome
    }

    fn decide(&mut self, host: &mut dyn ActionHost, request: &Request, now_ns: i64) -> Outcome {
        let Some(action) = Action::from_name(&request.action) else {
            return Outcome::Refused(format!("{} is not in the catalogue", request.action));
        };
        if !action.runs_here() {
            return Outcome::Refused(format!("{} is not run by this cell yet", action.name()));
        }
        let Some(budget) = self.budgets.get(&action).copied() else {
            return Outcome::Refused(format!("{} has no budget", action.name()));
        };
        let runs = self.recent.entry(action).or_default();
        while runs
            .front()
            .is_some_and(|at| now_ns - at >= budget.window_ns)
        {
            runs.pop_front();
        }
        if runs.len() >= budget.max {
            return Outcome::Refused(format!("{} is out of budget", action.name()));
        }
        match host.run(action, &request.target) {
            Ok(()) => {
                runs.push_back(now_ns);
                Outcome::Done
            }
            Err(reason) => Outcome::Refused(reason),
        }
    }

    /// The audit of the requests handled so far, in order.
    #[must_use]
    pub fn audit(&self) -> &[Record] {
        &self.audit
    }
}
