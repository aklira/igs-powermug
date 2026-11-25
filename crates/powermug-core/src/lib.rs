//! Powermug core: the parts of the runtime that do not depend on a protocol.
//!
//! The adapter supervisor (P1) and the telemetry data plane (P2): the point
//! table, the process image and the partition lane. The command plane and the
//! management plane arrive with P3.

#![forbid(unsafe_code)]

pub mod clock;
pub mod cycle;
pub mod decode;
pub mod delivery;
pub mod image;
pub mod indicators;
pub mod lane;
pub mod ring;
pub mod saturation;
pub mod sink;
pub mod snapshot;
pub mod supervisor;

pub use clock::{Clock, Times};
pub use cycle::{CycleState, CycleSupervisor, Expired, Supervision};
pub use image::{OutOfRange, ProcessImage, Slot};
pub use indicators::{Indicator, IndicatorSource};
pub use lane::{Lane, LaneError, PointSpec, PointTable, SessionId, TableError};
pub use sink::LaneSink;
pub use supervisor::{Decision, Policy, Refusal, State, Supervisor};
