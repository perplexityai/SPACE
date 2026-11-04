//! Snapshot compression into a bounded multipart upload. Durable jobs continue
//! to own the staged inode; each producer owns its descriptor and live-state
//! slot, and holds codec admission whenever encoding can run.

use std::io;
use std::time::Duration;

use object_store::{
    StreamingUpload, StreamingUploadError, StreamingUploadOptions, UploadBufferBudget, UploadSource,
};
use pplx_space_metrics::volume_manager::{
    StreamPushPhase, stream_push_payload, stream_push_producer,
};
use pplx_space_object_store as object_store;
use tokio::io::AsyncWrite;

use super::*;

mod btrfs;

const MIB: usize = 1024 * 1024;
const PART_BYTES: usize = 64 * MIB;
const PIPE_BYTES: usize = 8 * MIB;
/// Service part budget when no deployment sets one: sixteen parts shared by
/// every upload on the node. Zero selects file uploads.
pub const DEFAULT_STREAM_PUSH_BUFFER_BUDGET_BYTES: usize = 16 * PART_BYTES;

/// Deployment policy for snapshot uploads. Clones of a streaming policy share
/// its one aggregate part budget; this must not be recreated for each job.
#[derive(Clone, Debug, Default)]
pub enum SnapshotUploadMode {
    #[default]
    File,
    Streaming(StreamingPushBudget),
}

/// Validated against the fixed part size before admitting any producers.
#[derive(Clone, Debug)]
pub struct StreamingPushBudget(UploadBufferBudget);

impl SnapshotUploadMode {
    pub fn from_buffer_budget_bytes(bytes: usize) -> Result<Self, StreamingUploadError> {
        if bytes == 0 {
            return Ok(Self::File);
        }
        if bytes < PART_BYTES {
            return Err(StreamingUploadError::InvalidConfiguration(
                "streaming push budget must hold at least one 64 MiB part; use zero for file uploads",
            ));
        }
        Ok(Self::Streaming(StreamingPushBudget(
            UploadBufferBudget::new(bytes)?,
        )))
    }

    pub fn buffer_budget_bytes(&self) -> usize {
        match self {
            Self::File => 0,
            Self::Streaming(budget) => budget.0.capacity_bytes(),
        }
    }
}

/// Clones share a single part budget, including across executor clones. The
/// service must separately allow a pipe, output buffer and zstd workspace per
/// admitted producer, plus transport overhead. No part allocation happens at
/// construction.
#[derive(Clone, Debug)]
pub(super) struct StreamPushConfig {
    budget: UploadBufferBudget,
    producers: Arc<tokio::sync::Semaphore>,
    part_bytes: usize,
    min_source_bytes: u64,
}

impl StreamPushConfig {
    pub(super) fn new(mode: &SnapshotUploadMode, max_producers: usize) -> Option<Self> {
        let SnapshotUploadMode::Streaming(budget) = mode else {
            return None;
        };
        Some(Self {
            budget: budget.0.clone(),
            producers: Arc::new(tokio::sync::Semaphore::new(max_producers)),
            part_bytes: PART_BYTES,
            min_source_bytes: CODEC_ADMISSION_MIN_INPUT_BYTES,
        })
    }

    fn enabled(artifact: Option<SnapshotArtifact>) -> bool {
        match artifact {
            Some(
                SnapshotArtifact::Memory
                | SnapshotArtifact::Rootfs
                | SnapshotArtifact::RootfsBtrfsSend
                | SnapshotArtifact::EncryptedSnapshotVolume
                | SnapshotArtifact::EncryptedRootfsCow,
            ) => true,
            Some(SnapshotArtifact::VmState | SnapshotArtifact::Metadata) | None => false,
        }
    }

    pub(super) fn options(
        &self,
        payload: &TransferPayload,
        logical_bytes: u64,
    ) -> Option<StreamingUploadOptions> {
        // Only immutable private staged sources are eligible. Small sources
        // keep the short file path and its codec-admission bypass;
        // SPACEAEA envelope encryption keeps its existing file path.
        if payload.staged_path.is_none()
            || payload.encryption.is_some()
            || logical_bytes < self.min_source_bytes
            || !Self::enabled(payload.effective_snapshot_artifact())
        {
            return None;
        }
        // Deliberately loose bound for zstd expansion (including frame/block
        // overhead), independent of compressBound's single-shot assumptions.
        // Large inputs that cannot fit 10,000 parts fall back BEFORE producing.
        let max_bytes = logical_bytes.checked_mul(2)?.checked_add(MIB as u64)?;
        StreamingUploadOptions::new(self.part_bytes, 16, max_bytes).ok()
    }
}

pub(super) struct StreamPushAttempt<'a> {
    pub uploader: &'a dyn StreamingUpload,
    pub config: &'a StreamPushConfig,
    pub options: StreamingUploadOptions,
    pub original_size: u64,
    pub cancellation: CancellationToken,
}

impl TransferExecutor {
    pub(super) async fn execute_stream_push(
        &self,
        job: &Job,
        payload: &TransferPayload,
        creds: &object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        attempt: StreamPushAttempt<'_>,
    ) -> Result<i64, JobError> {
        let StreamPushAttempt {
            uploader,
            config,
            options,
            original_size,
            cancellation,
        } = attempt;
        let cancellation = cancellation.child_token();
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let artifact = transfer_artifact_tag(payload.effective_snapshot_artifact());
        let wait_started = Instant::now();
        // A producer can lend its codec slot during backpressure, but its live
        // buffers, descriptor and blocking task still need a separate bound.
        let producer = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(JobError::Retriable("stream upload cancelled during admission".into())),
            permit = config.producers.clone().acquire_owned() => permit
                .map_err(|_| JobError::Retriable("stream producer admission closed".into()))?,
        };
        let codec = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(JobError::Retriable("stream upload cancelled during admission".into())),
            slot = self.acquire_codec_slot(job.priority) => slot?,
        };
        emit_transfer_phase_duration(
            &job.kind,
            artifact,
            TransferPhase::CompressWait,
            MaterializationMode::Disabled,
            wait_started,
        );
        let file = tokio::fs::File::open(payload.upload_source())
            .await
            .map_err(|e| JobError::Retriable(format!("open staged stream source: {e}")))?
            .into_std()
            .await;
        let source = compression_source(
            file,
            original_size,
            ProducerAdmission {
                codec,
                _producer: producer,
            },
            cancellation,
            artifact,
        );
        let wire_bytes = uploader
            .put_stream(creds, target, source, config.budget.clone(), options)
            .await
            .map_err(|error| {
                match error {
                    StreamingUploadError::Store(error) => classify(&error),
                    StreamingUploadError::InvalidConfiguration(_)
                    | StreamingUploadError::SizeLimit { .. } => {
                        JobError::Permanent(format!("stream snapshot upload: {error}"))
                    }
                    // Retry from the durable staged inode, never append to an old
                    // upload or assume a regenerated stream is byte-identical.
                    error @ (StreamingUploadError::BudgetClosed
                    | StreamingUploadError::Cancelled
                    | StreamingUploadError::Source(_)
                    | StreamingUploadError::Worker(_)) => {
                        JobError::Retriable(format!("stream snapshot upload: {error}"))
                    }
                }
            })?;
        stream_push_payload(
            artifact,
            StreamPushPhase::Uploaded,
            original_size,
            wire_bytes,
        );
        match self.store.head(creds, target).await {
            Ok(Some(meta)) if meta.size == wire_bytes => {
                stream_push_payload(
                    artifact,
                    StreamPushPhase::Verified,
                    original_size,
                    wire_bytes,
                );
                bytes_len_to_job_i64(original_size, "upload source size")
            }
            Ok(Some(meta)) => Err(JobError::Retriable(format!(
                "uploaded object truncated: {} holds {} durable bytes, expected {wire_bytes}",
                target.key, meta.size
            ))),
            Ok(None) => Err(JobError::Retriable(format!(
                "uploaded object missing after put: {}",
                target.key
            ))),
            Err(error) => Err(classify(&error)),
        }
    }
}

struct ProducerAdmission {
    codec: CodecSlot,
    _producer: tokio::sync::OwnedSemaphorePermit,
}

fn compression_source(
    file: std::fs::File,
    size: u64,
    admission: ProducerAdmission,
    cancellation: CancellationToken,
    artifact: &'static str,
) -> UploadSource {
    let (reader, writer) = tokio::io::duplex(PIPE_BYTES);
    let producer_token = cancellation.clone();
    let task = tokio::task::spawn_blocking(move || {
        // Ownership survives a dropped upload future. Closing the pipe or
        // cancelling wakes a backpressured write. Producer admission survives
        // until exit; codec admission is owned whenever encoding can run.
        let started = Instant::now();
        let mut input = SourceReader {
            file,
            cancellation: producer_token.clone(),
            consumed: 0,
            advised_through: 0,
        };
        let mut output = PipeWriter {
            writer,
            runtime: tokio::runtime::Handle::current(),
            cancellation: producer_token,
            admission,
            waited: Duration::ZERO,
        };
        let result = {
            // Sparse inputs produce many tiny encoded writes. Coalesce them
            // before crossing the blocking/async bridge; otherwise each zstd
            // block can wake the uploader for just a handful of bytes.
            let mut buffered = std::io::BufWriter::with_capacity(PIPE_BYTES, &mut output);
            zstd::stream::copy_encode(&mut input, &mut buffered, ZSTD_LEVEL)
                .and_then(|()| buffered.flush())
                .and_then(|()| {
                    if input.consumed == size {
                        Ok(())
                    } else {
                        Err(io::Error::other(
                            "staged source size changed during compression",
                        ))
                    }
                })
        };
        if result.is_ok() {
            // Preserve the file-backed compressor's final source-cache hint.
            page_cache::advise_dontneed(
                &input.file,
                Path::new("staged snapshot"),
                "stream upload input",
            );
        }
        stream_push_producer(artifact, started, output.waited, result.is_ok());
        tracing::debug!(
            artifact,
            logical_bytes = input.consumed,
            producer_ms = started.elapsed().as_millis(),
            pipe_write_ms = output.waited.as_millis(),
            success = result.is_ok(),
            "stream upload producer finished"
        );
        result
    });
    UploadSource::new(
        reader,
        async move { task.await.map_err(io::Error::other)? },
        cancellation,
    )
}

struct SourceReader {
    file: std::fs::File,
    cancellation: CancellationToken,
    consumed: u64,
    advised_through: u64,
}

impl Read for SourceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::other("snapshot compression cancelled"));
        }
        let n = self.file.read(buf)?;
        self.consumed += n as u64;
        // This immutable staged inode is read once per attempt. Release clean
        // pages behind the encoder without evicting its unread input. Retrying
        // still reads the durable file; the hint changes cache residency only.
        const ADVICE_WINDOW: u64 = 64 * MIB as u64;
        let through = self.consumed / ADVICE_WINDOW * ADVICE_WINDOW;
        if through > self.advised_through {
            if let Err(error) = page_cache::advise_range_dontneed(
                &self.file,
                self.advised_through,
                through - self.advised_through,
            ) {
                tracing::warn!(%error, "could not release consumed stream upload input pages");
            }
            self.advised_through = through;
        }
        Ok(n)
    }
}

struct PipeWriter {
    writer: tokio::io::DuplexStream,
    runtime: tokio::runtime::Handle,
    cancellation: CancellationToken,
    admission: ProducerAdmission,
    waited: Duration,
}

impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let started = Instant::now();
        let runtime = self.runtime.clone();
        let result = runtime.block_on(async {
            let count = tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => Err(io::Error::other("snapshot compression cancelled")),
                result = std::future::poll_fn(|cx| {
                    let result = std::pin::Pin::new(&mut self.writer).poll_write(cx, buf);
                    if result.is_pending() {
                        // The synchronous encoder cannot run while Write is
                        // pending. Let queued codec work use this idle slot.
                        self.admission.codec.lend();
                    }
                    result
                }) => result,
            }?;
            if self.admission.codec.is_lent() {
                tokio::select! {
                    biased;
                    _ = self.cancellation.cancelled() => return Err(io::Error::other("snapshot compression cancelled")),
                    reacquired = self.admission.codec.reacquire() =>
                        reacquired.map_err(codec_admission_closed).map_err(io::Error::other)?,
                }
            }
            // Write must not return to zstd until CPU admission is restored.
            Ok(count)
        });
        self.waited += started.elapsed();
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
