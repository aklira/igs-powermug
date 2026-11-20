//! Powermug core: the parts of the runtime that do not depend on a protocol.
//!
//! The adapter supervisor (P1) and the telemetry data plane (P2): the point
//! table, the process image and the partition lane. The command plane and the
//! management plane arrive with P3.

#![forbid(unsafe_code)]

pub mod image;
pub mod lane;
pub mod supervisor;

pub use image::{OutOfRange, ProcessImage, Slot};
pub use lane::{Lane, LaneError, PointSpec, PointTable};
pub use supervisor::{Decision, Policy, Refusal, State, Supervisor};
