//! A teardown suspend capture's record of the execution that took it.
//!
//! A backup requested again by local artifact id registers under the
//! execution generation whose teardown suspend captured the artifact, so the
//! control plane's fence refuses the capture of an execution it superseded
//! for as long as the bytes are on the node. The record is a file of its own
//! in the capture's directory, beside the artifact's files, and names the
//! capture's local artifact id: a reader asking about any other artifact
//! (another identity adopting the directory, or bytes a restore pulled into
//! it) finds no record.
//!
//! It isn't part of the snapshot metadata. That sidecar is uploaded with the
//! snapshot and copied into the metadata of artifacts derived from it, so a
//! restore or a fork elsewhere would inherit an execution that isn't its own.
//! This file is in no upload's artifact list; a sandbox transfer, which moves
//! the whole directory, carries it with the capture it describes. Nor is it in
//! the node's database, so a sandbox manager rolled back to a release without
//! it opens that database unchanged and ignores the file.

use std::path::Path;

use pplx_space_common_core::types::{ExecutionGeneration, LocalArtifactId};

/// The record's file name in the capture's directory.
pub(crate) const CAPTURE_RECORD_FILENAME: &str = "capture.json";

#[derive(serde::Serialize, serde::Deserialize)]
struct CaptureRecord {
    local_artifact_id: LocalArtifactId,
    execution_generation: ExecutionGeneration,
}

/// Record `execution_generation` as the execution whose teardown suspend
/// captured `local_artifact_id` in `artifact_dir`, replacing the file durably.
pub(crate) fn write(
    artifact_dir: &Path,
    local_artifact_id: LocalArtifactId,
    execution_generation: ExecutionGeneration,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(&CaptureRecord {
        local_artifact_id,
        execution_generation,
    })
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    crate::snapshot_upload::replace_file_durably(
        artifact_dir,
        ".capture.json.tmp",
        CAPTURE_RECORD_FILENAME,
        &bytes,
    )
}

/// The execution generation recorded for `local_artifact_id` in
/// `artifact_dir`. `None` when there's no record, or it names another
/// artifact: one a teardown suspend didn't capture, or a capture from before
/// the record existed. A record that can't be parsed is `InvalidData`.
pub(crate) fn read(
    artifact_dir: &Path,
    local_artifact_id: LocalArtifactId,
) -> std::io::Result<Option<ExecutionGeneration>> {
    let bytes = match std::fs::read(artifact_dir.join(CAPTURE_RECORD_FILENAME)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let record: CaptureRecord = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok((record.local_artifact_id == local_artifact_id).then_some(record.execution_generation))
}

#[cfg(test)]
mod tests;
