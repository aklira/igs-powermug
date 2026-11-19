//! Revision model, RFC 8785 identity and signed-plan verification for Powermug.
//!
//! This crate is shared by the core and the `powermug` CLI (PM3). Its behaviour is
//! specified in `specifications/SPECIFICATIONS.md` §10 and must match gridloop's
//! `ibtnm.signing` and `ibtnm.canonical` byte for byte.

#![forbid(unsafe_code)]

pub mod canonical;
pub mod error;
pub mod load;
pub mod signed;

pub use error::{LoadError, SignatureError};
pub use load::parse_strict_json;
pub use signed::{verify_signed_plan, VerifiedPlan};
