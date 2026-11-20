//! Pivot data model of Powermug (SPECIFICATIONS §5).
//!
//! This crate holds the in-memory representation used on the hot path: the
//! normalized [`Quality`] (§5.2) and the compact [`PointEvent`] (PM-PIV-001).
//! The Protobuf schema `powermug.pivot.v1` is compiled by `build.rs` and
//! converted to and from these types in [`convert`].

mod convert;
mod event;
/// Generated bindings of `powermug.pivot.v1`.
#[allow(
    clippy::all,
    clippy::pedantic,
    missing_docs,
    missing_debug_implementations
)]
pub mod proto {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/powermug.pivot.v1.rs"));
    }
}
mod quality;

pub use convert::ConvertError;
pub use event::{EventId, EventIdError, PointEvent, PointIndex, Value, ValueError};
pub use quality::{Detail, Iec104Qds, Quality, QualityError, Validity};
