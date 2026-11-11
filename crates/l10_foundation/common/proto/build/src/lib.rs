//! Protobuf code generation for SPACE build scripts.
//!
//! prost-build prints no `cargo:rerun-if-changed` lines, and tonic-prost-build
//! 0.14 stores its `emit_rerun_if_changed` flag without reading it. A build
//! script therefore has to declare its `.proto` inputs itself, and a
//! hand-written list tends to name the compiled files but not everything they
//! import. An edit to an unlisted import then leaves the crate fresh with stale
//! generated code. [`compile_protos`] declares every file protoc read instead,
//! so the compiled files and their transitive imports are covered without a
//! list to keep in sync, and an unrelated `.proto` edit reruns nothing.

use std::path::{Path, PathBuf};

use prost::Message;
use prost_types::FileDescriptorSet;

/// Import prefix of the well-known types that ship with protoc.
const WELL_KNOWN_TYPES: &str = "google/protobuf/";

#[derive(Debug, thiserror::Error)]
pub enum CompileProtosError {
    #[error("OUT_DIR is not set; compile protos from a Cargo build script")]
    OutDirUnset,
    #[error(transparent)]
    Compile(std::io::Error),
    #[error("failed to read protoc descriptor set {}", path.display())]
    ReadDescriptorSet {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to decode protoc descriptor set {}", path.display())]
    DecodeDescriptorSet {
        path: PathBuf,
        #[source]
        source: prost::DecodeError,
    },
}

/// Compiles `protos` with `builder` and declares their `.proto` inputs to Cargo.
///
/// The declared inputs are the files protoc read from `includes`: the compiled
/// files and everything they import, directly or transitively. The
/// `google/protobuf/` well-known types belong to the toolchain and are never
/// declared, just as protoc itself is not, even when a caller passes their
/// directory in `includes`. Their paths and modification times vary by machine,
/// so declaring them would rebuild crates that no repository change touched.
pub fn compile_protos(
    builder: tonic_prost_build::Builder,
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> Result<(), CompileProtosError> {
    let out_dir = std::env::var_os("OUT_DIR").ok_or(CompileProtosError::OutDirUnset)?;
    let descriptor_set = Path::new(&out_dir).join("protoc_descriptor_set.bin");
    compile_protos_with_descriptor_set(builder, &descriptor_set, protos, includes)
}

/// Like [`compile_protos`], and keeps protoc's descriptor set at `descriptor_set`.
///
/// Use this when the crate embeds the descriptor set, for example to serve
/// gRPC reflection. The set carries every imported file and its source info.
pub fn compile_protos_with_descriptor_set(
    builder: tonic_prost_build::Builder,
    descriptor_set: &Path,
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> Result<(), CompileProtosError> {
    let protos: Vec<&Path> = protos.iter().map(AsRef::as_ref).collect();
    let includes: Vec<&Path> = includes.iter().map(AsRef::as_ref).collect();
    builder
        // If tonic-prost-build starts honoring this flag, it will watch whole
        // include directories, which reruns this script on any `.proto` edit.
        .emit_rerun_if_changed(false)
        .file_descriptor_set_path(descriptor_set)
        .compile_protos(protos.as_slice(), includes.as_slice())
        .map_err(CompileProtosError::Compile)?;

    let bytes =
        std::fs::read(descriptor_set).map_err(|source| CompileProtosError::ReadDescriptorSet {
            path: descriptor_set.to_path_buf(),
            source,
        })?;
    let descriptors = FileDescriptorSet::decode(bytes.as_slice()).map_err(|source| {
        CompileProtosError::DecodeDescriptorSet {
            path: descriptor_set.to_path_buf(),
            source,
        }
    })?;
    // prost-build runs protoc with --include_imports, so the set names every
    // file protoc parsed. protoc resolved each name against the include
    // directories in order, and the first match is the file it read.
    for name in descriptors
        .file
        .iter()
        .filter_map(|file| file.name.as_deref())
        .filter(|name| !name.starts_with(WELL_KNOWN_TYPES))
    {
        if let Some(path) = includes
            .iter()
            .map(|dir| dir.join(name))
            .find(|path| path.is_file())
        {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    Ok(())
}
