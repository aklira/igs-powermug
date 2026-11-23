//! IEC 60870-5-104 South adapter of Powermug (SPECIFICATIONS §6, §7.1).
//!
//! This crate converts the ASDUs of a controlled station, as decoded by
//! `igs-iec104`, into pivot envelopes of `powermug.pivot.v1`. The connection,
//! the general interrogation and the transport to the core are not written yet.

pub mod command;
pub mod convert;
pub mod mapping;
pub mod run;
pub mod serve;
pub mod settings;

pub use convert::{asdu as convert_asdu, Context, ConvertError, Converted, Unmapped};
pub use mapping::{DuplicateObject, Mapping};
pub use run::{capabilities, run, Options, Report, RunError};
pub use serve::{serve, ServeError, ServeOptions, ServeReport};
pub use settings::{MappedPoint, Settings, SettingsError};
