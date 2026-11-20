//! Generates the Rust bindings of `powermug.pivot.v1`.
//!
//! The schema is compiled by `protox`, a pure-Rust Protobuf compiler, so the
//! build needs neither `protoc` nor `buf`.

use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let file = "powermug/pivot/v1/pivot.proto";

    println!("cargo::rerun-if-changed={}", root.display());

    let descriptors = protox::compile([file], [&root])
        .unwrap_or_else(|err| panic!("cannot compile {file}: {err}"));

    prost_build::Config::new()
        .compile_fds(descriptors)
        .unwrap_or_else(|err| panic!("cannot generate Rust for {file}: {err}"));
}
