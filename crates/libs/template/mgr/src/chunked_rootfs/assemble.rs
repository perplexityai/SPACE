//! Bounded assembly from authenticated leaves. Downloads never populate the
//! reusable registry: only the caller's durable, read-only final image can.

use std::{collections::BTreeMap, fs::File, io, path::Path, sync::Arc};

use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::{Digest, ObjectByteRange};

use super::{Leaf, cache::LeasedChunk, decode_chunk_bytes, delivery::AuthenticatedManifest};

mod tuning;
pub(crate) use tuning::Tuning;

// Bound the two explicit frame buffers per assembly. This is not a process-RSS
// limit: decoder workspace, manifests and filesystem page cache are separate.
const FRAME_BUFFER_BUDGET: u64 = 1 << 30;
const MAX_GAP: u64 = 64 << 10;
// Downloaded range files, including those queued for decoding, remain bounded
// independently of the explicit decoder buffers. This excludes page-cache RSS.
const SCRATCH_BUDGET: u32 = 1 << 30;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RangeConcurrency(std::num::NonZeroUsize);

#[derive(Debug, thiserror::Error)]
#[error("range concurrency must be an integer from 1 through 16")]
pub(crate) struct RangeConcurrencyError;

impl Default for RangeConcurrency {
    fn default() -> Self {
        Self(std::num::NonZeroUsize::new(4).expect("four is nonzero"))
    }
}

impl std::str::FromStr for RangeConcurrency {
    type Err = RangeConcurrencyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let count = value.parse::<usize>().map_err(|_| RangeConcurrencyError)?;
        if !(1..=16).contains(&count) {
            return Err(RangeConcurrencyError);
        }
        Ok(Self(
            std::num::NonZeroUsize::new(count).ok_or(RangeConcurrencyError)?,
        ))
    }
}

impl RangeConcurrency {
    fn for_frame_bytes(self, largest_frame_buffers: u64) -> Result<usize, AssemblyError> {
        if largest_frame_buffers > FRAME_BUFFER_BUDGET {
            return Err(AssemblyError::Invalid(
                "frame buffers exceed assembly budget",
            ));
        }
        let memory_slots = FRAME_BUFFER_BUDGET / largest_frame_buffers.max(1);
        Ok(self.0.get().min(memory_slots as usize))
    }
}

#[async_trait::async_trait]
pub trait RangeFetcher: Sync {
    /// Write exactly the requested compressed bytes. The destination is unique
    /// to this attempt; a late transport completion cannot touch another pull.
    async fn fetch(
        &self,
        object: Digest,
        range: ObjectByteRange,
        destination: &Path,
    ) -> Result<u64, tonic::Status>;
}

#[derive(Debug, thiserror::Error)]
pub enum AssemblyError {
    #[error("assembly I/O: {0}")]
    Io(#[from] io::Error),
    #[error("range transfer: {0}")]
    Transfer(#[from] tonic::Status),
    #[error("chunk validation: {0}")]
    Chunk(#[from] super::DecodeChunkError),
    #[error("assembly task: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("assembly invariant failed: {0}")]
    Invalid(&'static str),
}

#[derive(Default, Debug, serde::Serialize)]
pub struct Stats {
    pub downloaded_bytes: u64,
    pub downloaded_logical_bytes: u64,
    pub reused_logical_bytes: u64,
    pub reflinked_logical_bytes: u64,
    pub zero_logical_bytes: u64,
    pub requests: u64,
    pub overfetch_bytes: u64,
}

#[derive(Debug)]
struct Frame {
    digest: Digest,
    length: u64,
    compressed_offset: u64,
    compressed_length: u64,
    destinations: Vec<u64>,
}

#[derive(Debug)]
struct Request {
    object: Digest,
    start: u64,
    end: u64,
    payload_bytes: u64,
    frames: Vec<Frame>,
}

fn plan(
    manifest: &AuthenticatedManifest,
    local: &BTreeMap<Digest, LeasedChunk>,
    request_target: tuning::RequestTarget,
) -> Result<(Vec<Request>, Stats), AssemblyError> {
    let mut missing = BTreeMap::<Digest, Frame>::new();
    let mut offset = 0;
    let mut stats = Stats::default();
    for leaf in manifest.index().leaves() {
        match leaf {
            Leaf::Zero { length } => stats.zero_logical_bytes += length,
            Leaf::Zstd { length, digest } => {
                if let Some(source) = local.get(digest) {
                    if source.length() != *length {
                        return Err(AssemblyError::Invalid("cached chunk length"));
                    }
                    stats.reused_logical_bytes += length;
                } else {
                    let location = &manifest.locations()[digest];
                    missing
                        .entry(*digest)
                        .or_insert_with(|| Frame {
                            digest: *digest,
                            length: *length,
                            compressed_offset: location.compressed_offset,
                            compressed_length: location.compressed_length,
                            destinations: Vec::new(),
                        })
                        .destinations
                        .push(offset);
                    stats.downloaded_logical_bytes += length;
                }
            }
        }
        offset += leaf.length();
    }
    let mut frames: Vec<_> = missing.into_values().collect();
    frames.sort_by_key(|frame| {
        (
            manifest.locations()[&frame.digest].object,
            frame.compressed_offset,
        )
    });
    let mut requests: Vec<Request> = Vec::new();
    for frame in frames {
        let object = manifest.locations()[&frame.digest].object;
        let end = frame.compressed_offset + frame.compressed_length;
        if let Some(previous) = requests.last_mut() {
            let gap = frame.compressed_offset.saturating_sub(previous.end);
            let new_payload = previous.payload_bytes + frame.compressed_length;
            // Never merge across objects, never overlap, and cap aggregate
            // overfetch at 1/16 of useful bytes as well as 64 KiB per gap.
            if previous.object == object
                && frame.compressed_offset >= previous.end
                && gap <= MAX_GAP
                && end - previous.start <= request_target.bytes()
                && end - previous.start - new_payload <= new_payload / 16
            {
                previous.end = end;
                previous.payload_bytes = new_payload;
                previous.frames.push(frame);
                continue;
            }
        }
        requests.push(Request {
            object,
            start: frame.compressed_offset,
            end,
            payload_bytes: frame.compressed_length,
            frames: vec![frame],
        });
    }
    stats.requests = requests.len() as u64;
    for request in &requests {
        stats.downloaded_bytes += request.end - request.start;
        stats.overfetch_bytes += request.end - request.start - request.payload_bytes;
    }
    Ok((requests, stats))
}

/// `output` must be a newly created, empty file in unpublished staging. The
/// caller selects its backing policy before this function allocates extents.
/// At most four compressed ranges and four decoded chunks exist concurrently;
/// repeated leaves share a single fetch/decode. Zero leaves remain holes.
pub async fn assemble(
    manifest: Arc<AuthenticatedManifest>,
    local: BTreeMap<Digest, LeasedChunk>,
    output: Arc<File>,
    scratch: &Path,
    fetcher: &impl RangeFetcher,
) -> Result<Stats, AssemblyError> {
    assemble_with_tuning(manifest, local, output, scratch, fetcher, Tuning::default()).await
}

/// Experimental tuning leaves verification, reuse and final durability intact.
/// Range jobs own their temporary files until decoding finishes, including on
/// error. Requested concurrency is reduced for large frame-buffer footprints.
pub(crate) async fn assemble_with_tuning(
    manifest: Arc<AuthenticatedManifest>,
    local: BTreeMap<Digest, LeasedChunk>,
    output: Arc<File>,
    scratch: &Path,
    fetcher: &impl RangeFetcher,
    tuning: Tuning,
) -> Result<Stats, AssemblyError> {
    if output.metadata()?.len() != 0 {
        return Err(AssemblyError::Invalid("output is not empty"));
    }
    output.set_len(manifest.index().logical_bytes())?;
    let (requests, mut stats) = plan(&manifest, &local, tuning.request_target)?;
    let largest_frame_buffers = requests
        .iter()
        .flat_map(|request| &request.frames)
        .map(|frame| frame.length.saturating_add(frame.compressed_length))
        .max()
        .unwrap_or(0);
    let concurrency = tuning.concurrency.for_frame_bytes(largest_frame_buffers)?;
    let local_manifest = Arc::clone(&manifest);
    let local_output = Arc::clone(&output);
    let reuse = async move {
        tokio::task::spawn_blocking(move || -> io::Result<u64> {
            let mut offset = 0;
            let mut cloned = 0;
            for leaf in local_manifest.index().leaves() {
                if let Leaf::Zstd { digest, length } = leaf
                    && let Some(source) = local.get(digest)
                    && source.copy_to(&local_output, offset)?
                {
                    cloned += length;
                }
                offset += leaf.length();
            }
            // Dropping `local` releases leases only after every descriptor copy is
            // complete. A cancelled async waiter cannot release them prematurely.
            Ok(cloned)
        })
        .await
        .map_err(AssemblyError::from)?
        .map_err(AssemblyError::from)
    };

    let remote = transfer_requests(
        requests,
        &manifest,
        &output,
        scratch,
        fetcher,
        concurrency,
        tuning,
    );
    stats.reflinked_logical_bytes = if tuning.scheduling.overlap_reuse() {
        // join!, not try_join!: neither side may release staging or leases while
        // the other side still owns a writable descriptor or an in-flight copy.
        let (reused, transferred) = tokio::join!(reuse, remote);
        let reused = reused?;
        transferred?;
        reused
    } else {
        let reused = reuse.await?;
        remote.await?;
        reused
    };
    tokio::task::spawn_blocking(move || output.sync_all()).await??;
    Ok(stats)
}

struct Downloaded {
    request: Request,
    input: File,
    _temporary: tempfile::TempDir,
    _scratch_permit: tokio::sync::OwnedSemaphorePermit,
}

async fn fetch_request(
    request: Request,
    manifest: &AuthenticatedManifest,
    scratch: &Path,
    fetcher: &impl RangeFetcher,
    scratch_budget: Arc<tokio::sync::Semaphore>,
) -> Result<Downloaded, AssemblyError> {
    let length = request.end - request.start;
    if length > u64::from(SCRATCH_BUDGET) {
        return Err(AssemblyError::Invalid("range exceeds scratch budget"));
    }
    let permit = scratch_budget
        .acquire_many_owned(length as u32)
        .await
        .map_err(|_| AssemblyError::Invalid("scratch budget closed"))?;
    let scratch = scratch.to_owned();
    let temporary = tokio::task::spawn_blocking(move || tempfile::tempdir_in(scratch)).await??;
    let path = temporary.path().join("range");
    let object = &manifest.objects()[&request.object];
    let range = ObjectByteRange::new(
        request.start,
        length,
        object.compressed_bytes,
        object.version_id.clone(),
    )
    .map_err(|_| AssemblyError::Invalid("authenticated range"))?;
    let actual = fetcher.fetch(request.object, range, &path).await?;
    if actual != length {
        return Err(AssemblyError::Invalid("transport byte count"));
    }
    tokio::task::spawn_blocking(move || {
        let input = File::open(path)?;
        if input.metadata()?.len() != length {
            return Err(AssemblyError::Invalid("range file length"));
        }
        Ok(Downloaded {
            request,
            input,
            _temporary: temporary,
            _scratch_permit: permit,
        })
    })
    .await?
}

async fn decode_downloaded(
    downloaded: Downloaded,
    output: Arc<File>,
    decoders: Arc<tokio::sync::Semaphore>,
    parallel_frames: bool,
    concurrency: usize,
) -> Result<(), AssemblyError> {
    if parallel_frames {
        let downloaded = Arc::new(downloaded);
        let results: Vec<Result<(), AssemblyError>> =
            stream::iter(0..downloaded.request.frames.len())
                .map(|index| {
                    let downloaded = Arc::clone(&downloaded);
                    let output = Arc::clone(&output);
                    let decoders = Arc::clone(&decoders);
                    async move {
                        let permit = decoders
                            .acquire_owned()
                            .await
                            .map_err(|_| AssemblyError::Invalid("decoder budget closed"))?;
                        tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            let frame = downloaded
                                .request
                                .frames
                                .get(index)
                                .ok_or(AssemblyError::Invalid("frame index exceeds request"))?;
                            decode_frame(
                                frame,
                                downloaded.request.start,
                                &downloaded.input,
                                &output,
                            )
                        })
                        .await?
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;
        // Keep every range file and output descriptor alive until its final
        // blocking decoder returns, including when another decoder failed.
        for result in results {
            result?;
        }
        Ok(())
    } else {
        let permit = decoders
            .acquire_owned()
            .await
            .map_err(|_| AssemblyError::Invalid("decoder budget closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // Capture the whole owner, not only its input/request fields:
            // cancellation must not unlink a running decoder's range file.
            let downloaded = downloaded;
            decode_request(&downloaded.request, &downloaded.input, &output)
        })
        .await?
    }
}

async fn transfer_requests(
    requests: Vec<Request>,
    manifest: &AuthenticatedManifest,
    output: &Arc<File>,
    scratch: &Path,
    fetcher: &impl RangeFetcher,
    concurrency: usize,
    tuning: Tuning,
) -> Result<(), AssemblyError> {
    let scratch_budget = Arc::new(tokio::sync::Semaphore::new(SCRATCH_BUDGET as usize));
    let decoders = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let decode = |downloaded: Result<Downloaded, AssemblyError>| {
        let output = Arc::clone(output);
        let decoders = Arc::clone(&decoders);
        async move {
            decode_downloaded(
                downloaded?,
                output,
                decoders,
                tuning.scheduling.parallel_frames(),
                concurrency,
            )
            .await
        }
    };

    let results: Vec<Result<(), AssemblyError>> = if tuning.scheduling.pipeline() {
        // Two bounded stages: a free network slot can start another range while
        // completed ranges wait for CPU. The independent byte permit prevents
        // the queue from growing past the scratch budget for large frames.
        stream::iter(requests)
            .map(|request| {
                fetch_request(
                    request,
                    manifest,
                    scratch,
                    fetcher,
                    Arc::clone(&scratch_budget),
                )
            })
            .buffer_unordered(concurrency)
            .map(decode)
            .buffer_unordered(concurrency)
            .collect()
            .await
    } else {
        stream::iter(requests)
            .map(|request| {
                let scratch_budget = Arc::clone(&scratch_budget);
                let decoders = Arc::clone(&decoders);
                let output = Arc::clone(output);
                async move {
                    let downloaded =
                        fetch_request(request, manifest, scratch, fetcher, scratch_budget).await?;
                    decode_downloaded(
                        downloaded,
                        output,
                        decoders,
                        tuning.scheduling.parallel_frames(),
                        concurrency,
                    )
                    .await
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await
    };
    // Drain every task before returning an error. Cleanup must not race a
    // decoder that still holds a writable output descriptor.
    for result in results {
        result?;
    }
    Ok(())
}

fn decode_request(request: &Request, input: &File, output: &File) -> Result<(), AssemblyError> {
    if input.metadata()?.len() != request.end - request.start {
        return Err(AssemblyError::Invalid("range file length"));
    }
    for frame in &request.frames {
        decode_frame(frame, request.start, input, output)?;
    }
    Ok(())
}

fn decode_frame(
    frame: &Frame,
    start: u64,
    input: &File,
    output: &File,
) -> Result<(), AssemblyError> {
    use std::os::unix::fs::FileExt;
    let mut compressed = vec![0; frame.compressed_length as usize];
    input.read_exact_at(&mut compressed, frame.compressed_offset - start)?;
    let decoded = decode_chunk_bytes(compressed.as_slice(), frame.digest, frame.length)?;
    for offset in &frame.destinations {
        output.write_all_at(&decoded, *offset)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
