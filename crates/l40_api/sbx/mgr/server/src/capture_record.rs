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
//!
//! Only a record the capturing teardown wrote as it published the capture
//! orders captures ([`read_capture_order`]). Startup recovery also writes
//! records, from the teardown it finds active, and the capture it finds can be
//! an earlier execution's still in the canonical directory: its record fences
//! re-requests as before but orders nothing, unless it finds the capturing
//! teardown's own record already naming that generation, which it keeps.
//! Neither orders any record written before writers were told apart.

use std::path::Path;

use pplx_space_common_core::types::{ExecutionGeneration, LocalArtifactId};

/// The record's file name in the capture's directory.
pub(crate) const CAPTURE_RECORD_FILENAME: &str = "capture.json";

/// Who wrote a record, which decides whether it may order captures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordWriter {
    /// The teardown suspend that took the capture, as it publishes it.
    CapturingTeardown,
    /// Startup recovery, naming the generation of the teardown it found
    /// active, which needn't be the one that took the capture.
    Recovery,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CaptureRecord {
    local_artifact_id: LocalArtifactId,
    execution_generation: ExecutionGeneration,
    /// Written by the capturing teardown. Absent in every record recovery
    /// writes, and in every record written before the field existed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    written_at_capture: bool,
}

/// Record `execution_generation` as the execution whose teardown suspend
/// captured `local_artifact_id` in `artifact_dir`, replacing the file durably.
/// Recovery leaves the capturing teardown's own record of the same capture and
/// generation as it is: the teardown wrote it before the node went down, and
/// recovery's would say the same thing with less evidence.
pub(crate) fn write(
    artifact_dir: &Path,
    local_artifact_id: LocalArtifactId,
    execution_generation: ExecutionGeneration,
    writer: RecordWriter,
) -> std::io::Result<()> {
    // A record that can't be read keeps nothing, and is replaced as before.
    if writer == RecordWriter::Recovery
        && read_capture_order(artifact_dir, local_artifact_id)
            .is_ok_and(|recorded| recorded == Some(execution_generation))
    {
        return Ok(());
    }
    let bytes = serde_json::to_vec(&CaptureRecord {
        local_artifact_id,
        execution_generation,
        written_at_capture: writer == RecordWriter::CapturingTeardown,
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
    Ok(read_record(artifact_dir, local_artifact_id)?.map(|record| record.execution_generation))
}

/// The execution generation that took `local_artifact_id`, as only the
/// capturing teardown's own record shows it, for ordering captures. `None`
/// for a record recovery wrote or one older than the distinction, as for no
/// record at all.
pub(crate) fn read_capture_order(
    artifact_dir: &Path,
    local_artifact_id: LocalArtifactId,
) -> std::io::Result<Option<ExecutionGeneration>> {
    Ok(read_record(artifact_dir, local_artifact_id)?
        .filter(|record| record.written_at_capture)
        .map(|record| record.execution_generation))
}

fn read_record(
    artifact_dir: &Path,
    local_artifact_id: LocalArtifactId,
) -> std::io::Result<Option<CaptureRecord>> {
    let bytes = match std::fs::read(artifact_dir.join(CAPTURE_RECORD_FILENAME)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let record: CaptureRecord = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok((record.local_artifact_id == local_artifact_id).then_some(record))
}

#[cfg(test)]
mod tests;
