//! The command plane of Powermug (SPECIFICATIONS 9): the state of a command, its transitions,
//! and its critical journal with the rules of a restart.
//!
//! Nothing here sends a command. The North adapter gives commands to the core, and the South
//! adapter emits them; this crate decides what may be emitted, and records every step.

pub mod authorization;
pub mod journal;
pub mod state;

pub use authorization::{authorize, Acl, Grant, Mode, Refusal, Request, Value, ValueRule};
pub use journal::{
    Action, CommandError, CommandJournal, NewCommand, Phase, Recovery, RecoveryContext, Submission,
    Transition,
};
pub use state::{transitions, State};
