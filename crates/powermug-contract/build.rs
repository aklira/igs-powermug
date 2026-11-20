//! Generates the Rust messages of `powermug.adapter.v1`.
//!
//! The pivot types are not generated again: `powermug.pivot.v1` is taken from
//! the `powermug-pivot` crate, so an event has one Rust type on both sides.
//! Services are not generated yet; they need the transport, which is not
//! written.

use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let file = "powermug/adapter/v1/adapter.proto";

    println!("cargo::rerun-if-changed={}", root.display());

    let descriptors = protox::compile([file], [&root])
        .unwrap_or_else(|err| panic!("cannot compile {file}: {err}"));

    prost_build::Config::new()
        .extern_path(".powermug.pivot.v1", "::powermug_pivot::proto::v1")
        .compile_fds(descriptors)
        .unwrap_or_else(|err| panic!("cannot generate Rust for {file}: {err}"));
}
