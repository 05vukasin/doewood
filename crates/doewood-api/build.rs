//! Compiles `proto/doewood/v1/*.proto` with protox (pure Rust, so `cargo build` needs no
//! `protoc`), generates the tonic servers and clients, and writes the encoded descriptor set
//! that gRPC reflection serves.

use std::path::PathBuf;

use prost::Message as _;

const FILES: &[&str] = &[
    "doewood/v1/common.proto",
    "doewood/v1/config.proto",
    "doewood/v1/jobs.proto",
    "doewood/v1/records.proto",
    "doewood/v1/graph.proto",
    "doewood/v1/search.proto",
    "doewood/v1/summarize.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("../../proto");
    println!("cargo:rerun-if-changed={}", root.display());
    let files: Vec<PathBuf> = FILES.iter().map(|f| root.join(f)).collect();
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    let fds = protox::compile(&files, [&root])?;
    let out = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(out.join("doewood_v1_descriptor.bin"), fds.encode_to_vec())?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;
    Ok(())
}
