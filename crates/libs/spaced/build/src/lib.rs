//! Build-time generation of spaced protobuf services.
//!
//! Public host/guest messages are rollout-compatible; the private identity-worker
//! protocol is version-handshaken within one guest executable. Keep deprecated
//! fields and services until their documented minimum-version conditions hold.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
#[error(
    "protobuf well-known type includes not found; install protobuf includes or set PROTOC_INCLUDE"
)]
pub struct ProtobufIncludeDirError;

pub fn protobuf_include_dir() -> Result<PathBuf, ProtobufIncludeDirError> {
    let mut candidates = Vec::new();
    if let Ok(path) = std::env::var("PROTOC_INCLUDE") {
        candidates.push(PathBuf::from(path));
    }
    if let Ok(protoc) = std::env::var("PROTOC") {
        push_protoc_include_candidate(&mut candidates, protoc.trim());
    }
    // Build dependencies execute on HOST, even when generating code for a
    // different TARGET. Search the host PATH without requiring a Unix shell.
    if let Some(path) = std::env::var_os("PATH") {
        let executable = format!("protoc{}", std::env::consts::EXE_SUFFIX);
        for directory in std::env::split_paths(&path) {
            let protoc = directory.join(&executable);
            if protoc.is_file() {
                push_protoc_include_candidate(&mut candidates, &protoc.to_string_lossy());
                break;
            }
        }
    }
    candidates.push(PathBuf::from("/usr/include"));
    candidates.push(PathBuf::from("/usr/local/include"));
    candidates.push(PathBuf::from("/opt/homebrew/include"));

    candidates
        .into_iter()
        .find(|path| path.join("google/protobuf/duration.proto").exists())
        .ok_or(ProtobufIncludeDirError)
}

fn push_protoc_include_candidate(candidates: &mut Vec<PathBuf>, protoc: &str) {
    if protoc.is_empty() {
        return;
    }
    if let Some(parent) = std::path::Path::new(protoc)
        .parent()
        .and_then(|path| path.parent())
    {
        candidates.push(parent.join("include"));
    }
}
