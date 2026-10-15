//! Codec-admission bypass for decodes of small snapshot artifact classes.
//!
//! Frame-header admission (`acquire_codec_permit_for_decode`) can only skip
//! a permit when the zstd header pledges a small content size, and the push
//! path's streaming encoders never write that field. Without this bypass,
//! every `metadata` and `vmstate` pull queued behind gigabyte decodes even
//! though their decode costs microseconds, and `metadata` sits on the serial
//! part of every cross-node restore.
//!
//! The artifact class does not prove a pass is small: a corrupt or hostile
//! object can be large on the wire or inflate. A pass bypasses only when its
//! input fits [`CLASS_BYPASS_MAX_INPUT_BYTES`], which bounds the decrypt and
//! frame-parsing work, and then runs with its decoded output capped at
//! [`CLASS_BYPASS_MAX_DECODED_BYTES`]; on overflow the caller discards the
//! output and restarts the pass under a codec permit instead of failing the
//! pull.

use std::future::Future;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;

use pplx_space_common_core::types::{MaterializationMode, SnapshotArtifact};
use pplx_space_common_jobs::{Job, JobError, JobFailureReason};

use crate::codec_admission::CodecPermit;

use super::{
    CompletingWrite, TransferExecutor, TransferPayload, TransferPhase,
    emit_transfer_phase_duration, transfer_artifact_tag,
};

// Legitimate `metadata` and `vmstate` objects are tens of KiB on the wire and
// decoded, so both bounds leave well over an order of magnitude of growth
// headroom.

/// Largest input a class-bypassed pass reads. Decode work is not bounded by
/// output alone: a stream of empty zstd frames parses at roughly 7 ms per MiB
/// while writing nothing, so this keeps even a pathological input to about
/// ten milliseconds of unadmitted decrypt and decode CPU.
pub(super) const CLASS_BYPASS_MAX_INPUT_BYTES: u64 = 1024 * 1024;

/// Largest decoded output a class-bypassed pass writes before it is
/// abandoned and restarted under a permit.
pub(super) const CLASS_BYPASS_MAX_DECODED_BYTES: u64 = 16 * 1024 * 1024;

/// Whether decodes of `artifact` skip codec admission under the output cap.
pub(super) fn bypasses_decode_admission(artifact: Option<SnapshotArtifact>) -> bool {
    match artifact {
        Some(SnapshotArtifact::Metadata | SnapshotArtifact::VmState) => true,
        Some(
            SnapshotArtifact::Memory
            | SnapshotArtifact::Rootfs
            | SnapshotArtifact::RootfsBtrfsSend
            | SnapshotArtifact::EncryptedSnapshotVolume
            | SnapshotArtifact::EncryptedRootfsCow,
        )
        | None => false,
    }
}

/// How one decode pass was admitted.
pub(super) enum DecodeGate {
    /// Holds a codec permit, or needs none (`None`: the frame header proved
    /// the decode small, or the pass decodes nothing): run to completion.
    Permit(Option<CodecPermit>),
    /// Skipped admission by artifact class: abandon the pass once it
    /// writes more than [`CLASS_BYPASS_MAX_DECODED_BYTES`].
    Capped,
}

impl DecodeGate {
    pub(super) fn holds_permit(&self) -> bool {
        matches!(self, Self::Permit(Some(_)))
    }

    /// Split into the permit the blocking pass must hold and its output cap.
    pub(super) fn into_parts(self) -> (Option<CodecPermit>, Option<u64>) {
        match self {
            Self::Permit(permit) => (permit, None),
            Self::Capped => (None, Some(CLASS_BYPASS_MAX_DECODED_BYTES)),
        }
    }
}

/// Outcome of a decode pass that may have run under an output cap.
#[derive(Debug)]
pub(super) enum CappedDecode<T> {
    Complete(T),
    /// The pass hit its cap. Its partial output has been discarded and its
    /// input is intact, so the caller can restart it under a permit.
    OverCap,
}

#[cfg(test)]
impl<T> CappedDecode<T> {
    pub(super) fn expect_complete(self) -> T {
        match self {
            Self::Complete(value) => value,
            Self::OverCap => panic!("decode unexpectedly exceeded its output cap"),
        }
    }
}

/// The payload of the `io::Error` a [`DecodedOutputCap`] raises, so callers
/// can tell an overflow apart from a real I/O or decode failure after it has
/// crossed the zstd and crypto layers.
#[derive(Debug, thiserror::Error)]
#[error("decoded output exceeded the {max_bytes}-byte cap for an unadmitted decode")]
struct DecodedOutputCapExceeded {
    max_bytes: u64,
}

/// Write adapter that fails once more than `max_bytes` would pass through.
/// A `None` cap passes everything.
pub(super) struct DecodedOutputCap<W> {
    inner: W,
    max_bytes: Option<u64>,
    written: u64,
}

impl<W> DecodedOutputCap<W> {
    pub(super) fn new(inner: W, max_bytes: Option<u64>) -> Self {
        Self {
            inner,
            max_bytes,
            written: 0,
        }
    }
}

/// Whether `error`, possibly re-raised or wrapped as a source by an outer
/// writer or decoder, came from a [`DecodedOutputCap`] overflow.
pub(super) fn is_output_cap_exceeded(error: &std::io::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = current {
        if error.is::<DecodedOutputCapExceeded>() {
            return true;
        }
        // `io::Error::source` skips its own payload, so step into it.
        current = match error.downcast_ref::<std::io::Error>() {
            Some(io) => io.get_ref().map(|inner| inner as _),
            None => error.source(),
        };
    }
    false
}

impl<W: Write> Write for DecodedOutputCap<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(max_bytes) = self.max_bytes {
            // Refuse the whole buffer rather than writing a prefix: the
            // output is discarded on overflow anyway.
            let len = u64::try_from(buf.len()).unwrap_or(u64::MAX);
            if self.written.saturating_add(len) > max_bytes {
                return Err(std::io::Error::other(DecodedOutputCapExceeded {
                    max_bytes,
                }));
            }
        }
        let n = self.inner.write(buf)?;
        self.written = self
            .written
            .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: CompletingWrite> CompletingWrite for DecodedOutputCap<W> {
    fn complete(self) -> std::io::Result<()> {
        self.inner.complete()
    }
}

/// Lift a [`DecodedOutputCap`] overflow out of an I/O result.
pub(super) fn over_cap_or<T>(result: std::io::Result<T>) -> std::io::Result<CappedDecode<T>> {
    match result {
        Ok(value) => Ok(CappedDecode::Complete(value)),
        Err(error) if is_output_cap_exceeded(&error) => Ok(CappedDecode::OverCap),
        Err(error) => Err(error),
    }
}

/// `zstd::stream::copy_decode` under an optional output cap.
pub(super) fn copy_decode_capped(
    reader: impl Read,
    writer: impl Write,
    max_bytes: Option<u64>,
) -> Result<CappedDecode<()>, JobError> {
    over_cap_or(zstd::stream::copy_decode(
        reader,
        DecodedOutputCap::new(writer, max_bytes),
    ))
    .map_err(|e| JobError::Permanent(format!("zstd decompress: {e}")))
}

impl TransferExecutor {
    /// Run one pull's decode `pass` of `path` under codec admission.
    ///
    /// `wait_phase` names the metric for time spent waiting on a permit;
    /// `None` means the pass decodes nothing and runs unadmitted. Classes
    /// that [`bypasses_decode_admission`] first run under
    /// [`DecodeGate::Capped`] when their input fits
    /// [`CLASS_BYPASS_MAX_INPUT_BYTES`],
    /// and a pass that overflows is restarted once under a codec permit,
    /// taken even if the frame header pledges a small size; the restart is
    /// uncapped and cannot overflow again.
    /// Returns when the completed pass started, for its duration metric.
    pub(super) async fn run_admitted_decode<T, E, Fut>(
        &self,
        job: &Job,
        p: &TransferPayload,
        mode: MaterializationMode,
        wait_phase: Option<TransferPhase>,
        path: &Path,
        pass: impl Fn(DecodeGate) -> Fut,
    ) -> Result<(Instant, Result<T, E>), JobError>
    where
        Fut: Future<Output = Result<CappedDecode<T>, E>>,
    {
        let snapshot_artifact = p.effective_snapshot_artifact();
        let artifact = transfer_artifact_tag(snapshot_artifact);
        let emit_wait = |gate: &DecodeGate, waited_since: Instant| {
            if let Some(phase) = wait_phase
                && gate.holds_permit()
            {
                emit_transfer_phase_duration(&job.kind, artifact, phase, mode, waited_since);
            }
        };
        let waited_since = Instant::now();
        let gate = match wait_phase {
            None => DecodeGate::Permit(None),
            Some(_)
                if bypasses_decode_admission(snapshot_artifact)
                    && tokio::fs::metadata(path)
                        .await
                        .is_ok_and(|meta| meta.len() <= CLASS_BYPASS_MAX_INPUT_BYTES) =>
            {
                DecodeGate::Capped
            }
            Some(_) => DecodeGate::Permit(
                self.acquire_codec_permit_for_decode(path, job.priority)
                    .await?,
            ),
        };
        emit_wait(&gate, waited_since);
        let started_at = Instant::now();
        match pass(gate).await {
            Ok(CappedDecode::Complete(value)) => return Ok((started_at, Ok(value))),
            Ok(CappedDecode::OverCap) => {}
            Err(error) => return Ok((started_at, Err(error))),
        }

        tracing::warn!(
            job_id = %job.job_id,
            subject_id = %p.subject_id,
            artifact,
            max_decoded_bytes = CLASS_BYPASS_MAX_DECODED_BYTES,
            "unadmitted decode exceeded its output cap; restarting under codec admission"
        );
        pplx_space_metrics::volume_manager::transfer_decode_bypass_overflow(&job.kind, artifact);
        // The overflow proved this object is not the small artifact its
        // class implies, so the restart takes a permit unconditionally,
        // whatever size the frame header pledges.
        let waited_since = Instant::now();
        let gate = DecodeGate::Permit(Some(self.acquire_codec_permit(job.priority).await?));
        emit_wait(&gate, waited_since);
        let started_at = Instant::now();
        match pass(gate).await {
            Ok(CappedDecode::Complete(value)) => Ok((started_at, Ok(value))),
            Ok(CappedDecode::OverCap) => Err(JobError::permanent_with_reason(
                JobFailureReason::Internal,
                "an uncapped decode reported an output-cap overflow",
            )),
            Err(error) => Ok((started_at, Err(error))),
        }
    }
}

#[cfg(test)]
mod tests;
