//! Bounded, replayable multipart uploads from a caller-owned producer.
//!
//! The budget follows the allocation through hashing, HTTP bodies and SDK
//! retries. A slow reader or a budget waiter is polled alongside uploads: it
//! must never stop an acknowledged part from releasing the memory it needs.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::display::DisplayErrorContext;
use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::{
    AbortGuard, ObjectCredentials, ObjectStore, ObjectStoreError, ObjectTarget, S3ObjectStore,
    S3Operation, content_md5, observe_s3_error,
};

const MIB: usize = 1024 * 1024;
const MAX_PARTS: u64 = 10_000;

/// Upload-specific failure. A whole-transfer retry needs a new producer and
/// multipart upload; only individual part retries replay retained bytes.
#[derive(Debug, thiserror::Error)]
pub enum StreamingUploadError {
    #[error("invalid streaming upload configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("upload buffer budget closed")]
    BudgetClosed,
    #[error("upload cancelled")]
    Cancelled,
    #[error("upload producer failed: {0}")]
    Source(#[from] io::Error),
    #[error("upload buffer worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
    #[error("upload exceeds its declared maximum of {max_bytes} bytes")]
    SizeLimit { max_bytes: u64 },
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
}

/// A service-owned aggregate budget. Clone this same handle into every upload,
/// including uploads using different store instances. There is no global or
/// per-object default that could silently multiply a service's memory limit.
///
/// Accounts for full part allocations, including short tails, during filling,
/// hashing, queuing, requests and retries. SDK body clones share the allocation
/// and its permit. HTTP/TLS buffers, producer/codec workspace and source buffers
/// are separate overhead which the service must include in its memory budget.
#[derive(Clone, Debug)]
pub struct UploadBufferBudget {
    permits: Arc<Semaphore>,
    bytes: usize,
}

impl UploadBufferBudget {
    pub fn new(bytes: usize) -> Result<Self, StreamingUploadError> {
        if bytes == 0 || !bytes.is_multiple_of(MIB) || bytes / MIB > Semaphore::MAX_PERMITS {
            return Err(StreamingUploadError::InvalidConfiguration(
                "buffer budget must be a positive whole number of MiB within semaphore limits",
            ));
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(bytes / MIB)),
            bytes,
        })
    }

    pub fn capacity_bytes(&self) -> usize {
        self.bytes
    }

    async fn allocate(&self, size: usize) -> Result<PartBuffer, StreamingUploadError> {
        let permit = Arc::clone(&self.permits)
            .acquire_many_owned((size / MIB) as u32)
            .await
            .map_err(|_| StreamingUploadError::BudgetClosed)?;
        // Zeroing a large part also belongs off runtime workers. Ownership of
        // the permit stays in this closure if its awaiter is cancelled.
        Ok(tokio::task::spawn_blocking(move || PartBuffer {
            data: vec![0; size].into_boxed_slice(),
            len: 0,
            _permit: permit,
        })
        .await?)
    }
}

/// Validated per-object limits. Part sizes are a policy-bounded 5–256 MiB in
/// whole MiB. The maximum encoded size is mandatory because an unknown-length
/// stream cannot increase already-uploaded parts when it hits S3's part limit.
/// A caller that cannot bound its encoded output must retain its file path.
#[derive(Clone, Copy, Debug)]
pub struct StreamingUploadOptions {
    part_size: usize,
    concurrency: usize,
    max_bytes: u64,
}

impl StreamingUploadOptions {
    pub fn new(
        part_size: usize,
        concurrency: usize,
        max_bytes: u64,
    ) -> Result<Self, StreamingUploadError> {
        if !(5 * MIB..=256 * MIB).contains(&part_size) || !part_size.is_multiple_of(MIB) {
            return Err(StreamingUploadError::InvalidConfiguration(
                "part size must be 5–256 whole MiB",
            ));
        }
        if !(1..=64).contains(&concurrency) {
            return Err(StreamingUploadError::InvalidConfiguration(
                "part concurrency must be 1–64",
            ));
        }
        if max_bytes > part_size as u64 * MAX_PARTS {
            return Err(StreamingUploadError::InvalidConfiguration(
                "maximum size exceeds 10,000 parts; choose larger parts or use a file",
            ));
        }
        Ok(Self {
            part_size,
            concurrency,
            max_bytes,
        })
    }
}

/// Reader plus an independent producer result. EOF does not authorize object
/// completion: `finished` must report that the producer finalized successfully.
/// It normally awaits a running producer task, mapping its join error to I/O.
/// The producer must run independently of polling this completion future.
///
/// `cancel` is a transfer-scoped child token shared with the producer. The
/// producer must respond to it and reader closure, and retain its source and
/// admission guards until it actually exits. Dropping this source closes the
/// reader and cancels the token, even if the upload future itself is dropped.
/// The service owns codec admission, bounded source buffering and job deadlines.
pub struct UploadSource {
    reader: Option<Box<dyn AsyncRead + Unpin + Send>>,
    finished: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
    joined: bool,
    cancel: CancellationToken,
    _cancel_on_drop: DropGuard,
}

impl UploadSource {
    pub fn new(
        reader: impl AsyncRead + Unpin + Send + 'static,
        finished: impl Future<Output = io::Result<()>> + Send + 'static,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            reader: Some(Box::new(reader)),
            finished: Box::pin(finished),
            joined: false,
            _cancel_on_drop: cancel.clone().drop_guard(),
            cancel,
        }
    }

    async fn finish(&mut self) -> io::Result<()> {
        let result = self.finished.as_mut().await;
        self.joined = true;
        result
    }

    fn stop(&mut self) {
        self.cancel.cancel();
        self.reader.take();
    }
}

/// Optional capability selected through [`ObjectStore::streaming_upload`]
/// before starting a producer. No installed caller changes behavior merely
/// because this capability is available.
#[async_trait]
pub trait StreamingUpload: Send + Sync {
    /// Uploads finalized producer output, returning its actual wire length.
    /// Uses the existing SDK request/retry timeouts; the service must enforce
    /// its end-to-end deadline, including producer and budget waits.
    ///
    /// Normal failure cancels/closes and joins the producer, and awaits a
    /// best-effort multipart abort. Dropping the future cancels the producer
    /// and schedules best-effort abort; it cannot await producer exit. Ambiguous
    /// create/complete responses and process death require bucket lifecycle
    /// cleanup. An error never authorizes deletion of a completed object.
    async fn put_stream(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        source: UploadSource,
        budget: UploadBufferBudget,
        options: StreamingUploadOptions,
    ) -> Result<u64, StreamingUploadError>;
}

struct PartBuffer {
    data: Box<[u8]>,
    len: usize,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for PartBuffer {
    fn as_ref(&self) -> &[u8] {
        self.data
            .get(..self.len)
            .expect("part length fits its allocation")
    }
}

struct Part {
    body: Bytes,
    md5: String,
    eof: bool,
}

/// A replayable, zero-copy stream rather than SdkBody's eager byte variant.
/// The latter makes SigV4 hash the entire part again on the runtime worker.
/// Keep the file path's UNSIGNED-PAYLOAD contract and enforce Content-MD5.
struct PartBody(Bytes);

impl http_body::Body for PartBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if self.0.is_empty() {
            return Poll::Ready(None);
        }
        let size = self.0.len().min(MIB);
        Poll::Ready(Some(Ok(http_body::Frame::data(self.0.split_to(size)))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.0.len() as u64)
    }
}

fn part_body(bytes: Bytes) -> ByteStream {
    ByteStream::new(SdkBody::retryable(move || {
        SdkBody::from_body_1_x(PartBody(bytes.clone()))
    }))
}

async fn read_part(
    source: &mut UploadSource,
    budget: &UploadBufferBudget,
    options: StreamingUploadOptions,
    total: u64,
) -> Result<Part, StreamingUploadError> {
    let mut buffer = budget.allocate(options.part_size).await?;
    let mut eof = false;
    while buffer.len < buffer.data.len() {
        // Read at most one byte past the declared limit, including a zero
        // maximum. Never publish a prefix of an oversized producer stream.
        let remaining = options.max_bytes.saturating_sub(total + buffer.len as u64);
        let end = buffer.len
            + (remaining + 1)
                .min((buffer.data.len() - buffer.len) as u64)
                .min(MIB as u64) as usize;
        let count = source
            .reader
            .as_mut()
            .expect("reader is open until completion")
            .read(
                buffer
                    .data
                    .get_mut(buffer.len..end)
                    .expect("read window fits part"),
            )
            .await?;
        if count == 0 {
            eof = true;
            break;
        }
        buffer.len += count;
        if total + buffer.len as u64 > options.max_bytes {
            return Err(StreamingUploadError::SizeLimit {
                max_bytes: options.max_bytes,
            });
        }
        // AsyncRead may be immediately ready (for example, an in-memory
        // source). Bound copying per poll as well as hashing off-runtime. A
        // 1 MiB quantum also avoids waking a blocking producer for every small
        // fragment of space released in its bounded pipe.
        tokio::task::yield_now().await;
    }
    // Bytes owns the permit too: HTTP/SDK clones cannot outlive its charge.
    Ok(tokio::task::spawn_blocking(move || {
        let md5 = content_md5::of_bytes(buffer.as_ref());
        Part {
            body: Bytes::from_owner(buffer),
            md5,
            eof,
        }
    })
    .await?)
}

#[async_trait]
impl StreamingUpload for S3ObjectStore {
    async fn put_stream(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        mut source: UploadSource,
        budget: UploadBufferBudget,
        options: StreamingUploadOptions,
    ) -> Result<u64, StreamingUploadError> {
        let mut abort = None;
        let cancel = source.cancel.clone();
        let result = if budget.capacity_bytes() < options.part_size {
            Err(StreamingUploadError::InvalidConfiguration(
                "budget must hold at least one full part",
            ))
        } else {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(StreamingUploadError::Cancelled),
                result = self.upload_stream(creds, target, &mut source, &budget, options, &mut abort) => result,
            }
        };
        // upload_stream has dropped its pending reads and request futures.
        // Close the pipe before joining, so a blocked encoder can exit.
        source.stop();
        if let Some(guard) = abort {
            guard.abort().await;
        }
        if !source.joined
            && let Err(error) = source.finish().await
        {
            tracing::warn!(%error, "upload producer failed while stopping after upload failure");
        }
        result
    }
}

impl S3ObjectStore {
    async fn upload_stream(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        source: &mut UploadSource,
        budget: &UploadBufferBudget,
        options: StreamingUploadOptions,
        abort: &mut Option<AbortGuard>,
    ) -> Result<u64, StreamingUploadError> {
        let first = read_part(source, budget, options, 0).await?;
        if first.body.is_empty() {
            source.finish().await?;
            self.put_bytes(creds, target, first.body).await?;
            return Ok(0);
        }
        if first.eof {
            // The whole encoded object fits one part. Keep the single-PUT
            // latency of file uploads, but require producer finalization and
            // retain the budgeted body and precomputed digest through retries.
            source.finish().await?;
            let size = first.body.len() as u64;
            self.base
                .put_object()
                .bucket(target.bucket)
                .key(target.key)
                .content_length(size as i64)
                .content_md5(first.md5)
                .body(part_body(first.body))
                .customize()
                .config_override(self.op_config(creds, &target, S3Operation::PutObject))
                .send()
                .await
                .map_err(|err| {
                    observe_s3_error(S3Operation::PutObject, target.bucket, target.key, &err);
                    ObjectStoreError::Put(DisplayErrorContext(&err).to_string())
                })?;
            return Ok(size);
        }
        let created = self
            .base
            .create_multipart_upload()
            .bucket(target.bucket)
            .key(target.key)
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::CreateMultipartUpload))
            .send()
            .await
            .map_err(|err| {
                observe_s3_error(
                    S3Operation::CreateMultipartUpload,
                    target.bucket,
                    target.key,
                    &err,
                );
                ObjectStoreError::Multipart(DisplayErrorContext(&err).to_string())
            })?;
        let upload_id = created
            .upload_id()
            .ok_or(ObjectStoreError::MissingField("upload_id"))?;
        *abort = Some(AbortGuard::new(self, creds, &target, upload_id));
        let mut in_flight = FuturesUnordered::new();
        let mut total = first.body.len() as u64;
        let mut eof = first.eof;
        let mut next_number = 2;
        let mut completed = Vec::new();
        in_flight.push(self.upload_stream_part(creds, target, upload_id, 1, first));
        while !eof {
            // Fill the next budgeted part while existing requests run. Waiting
            // for an upload slot before reading would unnecessarily stop the
            // caller's encoder; the shared budget bounds this extra prefetch.
            let read = read_part(source, budget, options, total);
            tokio::pin!(read);
            let part = loop {
                tokio::select! {
                    // A budget/read waiter must not prevent a request from
                    // finishing and releasing the allocation it waits for.
                    result = in_flight.next(), if !in_flight.is_empty() => {
                        completed.push(result.expect("in-flight part")?);
                    }
                    part = &mut read => break part?,
                }
            };
            eof = part.eof;
            if !part.body.is_empty() {
                if in_flight.len() >= options.concurrency {
                    completed.push(in_flight.next().await.expect("in-flight part")?);
                }
                total += part.body.len() as u64;
                in_flight.push(self.upload_stream_part(
                    creds,
                    target,
                    upload_id,
                    next_number,
                    part,
                ));
                next_number += 1;
            }
        }
        while let Some(part) = in_flight.next().await {
            completed.push(part?);
        }
        // EOF can be caused by a codec failure. Never publish until its task
        // confirms successful finalization, including its final output bytes.
        source.finish().await?;
        completed.sort_unstable_by_key(|part| part.part_number());
        self.base
            .complete_multipart_upload()
            .bucket(target.bucket)
            .key(target.key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            )
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::CompleteMultipartUpload))
            .send()
            .await
            .map_err(|err| {
                observe_s3_error(
                    S3Operation::CompleteMultipartUpload,
                    target.bucket,
                    target.key,
                    &err,
                );
                ObjectStoreError::Multipart(DisplayErrorContext(&err).to_string())
            })?;
        abort.take().expect("created upload is guarded").disarm();
        Ok(total)
    }

    async fn upload_stream_part(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        upload_id: &str,
        number: i32,
        part: Part,
    ) -> Result<CompletedPart, StreamingUploadError> {
        let uploaded = self
            .base
            .upload_part()
            .bucket(target.bucket)
            .key(target.key)
            .upload_id(upload_id)
            .part_number(number)
            .content_length(part.body.len() as i64)
            .content_md5(part.md5)
            .body(part_body(part.body))
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::UploadPart))
            .send()
            .await
            .map_err(|err| {
                observe_s3_error(S3Operation::UploadPart, target.bucket, target.key, &err);
                ObjectStoreError::Multipart(DisplayErrorContext(&err).to_string())
            })?;
        let etag = uploaded
            .e_tag()
            .ok_or(ObjectStoreError::MissingField("e_tag"))?;
        Ok(CompletedPart::builder()
            .part_number(number)
            .e_tag(etag)
            .build())
    }
}

#[cfg(test)]
mod tests;
