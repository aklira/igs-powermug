//! Powermug core: the parts of the runtime that do not depend on a protocol.
//!
//! P1 provides the adapter supervisor. The data plane, the command plane and the
//! management plane arrive with P2 and P3.

#![forbid(unsafe_code)]

pub mod supervisor;

pub use supervisor::{Decision, Policy, Refusal, State, Supervisor};
