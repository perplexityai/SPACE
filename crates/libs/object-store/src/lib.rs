//! Object-store client used by the builder (uploads) and template-managers
//! (downloads) to move VM image artifacts through a cloud object store.
//!
//! The trait is stateless w.r.t. identity and location: every operation takes
//! `(ObjectCredentials, ObjectTarget)` so a single store instance can address
//! any bucket in any region with any credentials. Object key layout, key
//! prefixes, and credential sourcing live with the caller.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{Region, RequestChecksumCalculation, StalledStreamProtectionConfig};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::{Client, Config};
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinSet;
use tracing::{Instrument, debug, info, warn};

use pplx_space_common_core::types::{MULTIPART_CONCURRENCY, MULTIPART_PART_SIZE};
use pplx_space_s3_http_client::{DEFAULT_POOL_IDLE_TIMEOUT, s3_http_client};

mod attempts;
mod content_md5;
mod copy;
mod download;
#[cfg(test)]
mod test_support;
mod timeouts;
mod upload;

use attempts::{AttemptObserver, AttemptTarget};
use timeouts::{S3Timeouts, read_secs};

pub use download::FinishingWrite;
use download::{ReorderWindow, SequentialWriter, open_scratch};
pub use upload::{
    StreamingUpload, StreamingUploadError, StreamingUploadOptions, UploadBufferBudget, UploadSource,
};

// Amortize file-body reads without buffering an entire multipart part.
const UPLOAD_READ_BUFFER_SIZE: usize = 1024 * 1024;

/// Static credentials passed by the caller to authorize a single operation.
///
/// The credentials must remain valid for the duration of the call. Long
/// multipart uploads carry one snapshot through every part; if the caller's
/// credentials can expire mid-upload, the caller should retry.
#[derive(Clone, Debug)]
pub struct ObjectCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl ObjectCredentials {
    /// Resolves one snapshot from the AWS default credential chain — on EKS,
    /// the IRSA web-identity token the pod's ServiceAccount carries.
    ///
    /// A snapshot, not a provider: every operation in this crate takes static
    /// credentials, so a caller whose work can outlive the session's lifetime
    /// has to re-resolve and retry.
    pub async fn from_default_chain() -> Result<Self, ObjectCredentialsError> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let credentials = config
            .credentials_provider()
            .ok_or(ObjectCredentialsError::NoProvider)?
            .provide_credentials()
            .await?;
        Ok(Self {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(str::to_string),
        })
    }
}

/// Why [`ObjectCredentials::from_default_chain`] could not produce credentials.
#[derive(Debug, thiserror::Error)]
pub enum ObjectCredentialsError {
    /// No provider at all is configured for this process. On EKS this means the
    /// pod has no workload identity: the ServiceAccount is missing its IRSA
    /// annotation, or the region the chain signs with is unset.
    #[error("no AWS credential provider is configured for this process")]
    NoProvider,
    #[error("resolving AWS credentials failed: {0}")]
    Resolve(#[from] aws_credential_types::provider::error::CredentialsError),
}

/// Addresses one object on a specific endpoint/region.
///
/// Borrowed for the duration of the call so callers can pass references into
/// existing strings without an extra allocation.
#[derive(Clone, Copy, Debug)]
pub struct ObjectTarget<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub region: &'a str,
    pub endpoint: Option<&'a str>,
    pub path_style: bool,
}

/// Metadata returned by [`ObjectStore::head`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMeta {
    /// Object size in bytes as reported by the store.
    pub size: u64,
    /// Provider-supplied entity tag, if available.
    pub etag: Option<String>,
}

/// Errors raised by [`ObjectStore`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("local I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("S3 head_object failed: {0}")]
    Head(String),
    #[error("S3 put_object failed: {0}")]
    Put(String),
    #[error("S3 multipart upload failed: {0}")]
    Multipart(String),
    #[error("S3 server-side copy failed: {0}")]
    Copy(String),
    #[error("S3 get_object failed: {0}")]
    Get(String),
    /// Object does not exist at `key`. Surfaces from `get_bytes` and
    /// `get_file` when S3 returns `NoSuchKey`. `head` already returns
    /// `Ok(None)` for the same condition (Option-shaped existence
    /// check), so this variant only appears on read paths that assume
    /// the object exists.
    #[error("object not found at key: {key}")]
    NotFound { key: String },
    /// Credentials are invalid or are not authorized for the requested object.
    #[error("object-store access denied: {message}")]
    PermissionDenied { message: String },
    /// The process-wide download buffer budget semaphore was closed.
    /// Should be unreachable: the semaphore lives for the process lifetime.
    #[error("download buffer budget semaphore closed")]
    DownloadBudgetClosed,
    /// The caller's decode-admission hook refused or failed to admit the
    /// streaming transform.
    #[error(transparent)]
    DecodeAdmission(#[from] DecodeAdmissionError),
    /// Defensive invariant breach: chunks remain but no reader is in
    /// flight to produce them.
    #[error(
        "ranged download stalled at chunk {next_chunk} of {chunk_count} with no readers in flight"
    )]
    DownloadStalled {
        next_chunk: usize,
        chunk_count: usize,
    },
    /// A ranged GET returned a body whose length does not match the
    /// requested range.
    #[error("range {range} returned {got} bytes, expected {expected}")]
    RangeLengthMismatch {
        range: String,
        got: usize,
        expected: u64,
    },
    /// A ranged GET's body stream failed mid-read.
    #[error("range {range} body read failed: {message}")]
    RangeBodyRead { range: String, message: String },
    #[error("S3 response missing required field: {0}")]
    MissingField(&'static str),
}

/// Abstraction over an object-store backend.
///
/// Admission hook for the CPU-bound transform of a streaming-decode
/// download. Called once with the object's wire size after the initial GET
/// but before any body chunk is fetched, so the returned permit can bound
/// concurrent decode streams without ever deadlocking against the download
/// buffer budget (a stream that holds no permit fetches no chunks).
#[async_trait]
pub trait DecodeAdmission: Send + Sync {
    async fn admit(&self, object_size: u64) -> Result<Option<DecodeGuard>, DecodeAdmissionError>;
}

/// Opaque RAII guard returned by [`DecodeAdmission::admit`]. The download
/// machinery holds it for as long as the admitted decode work can run —
/// including detached blocking writes that outlive a cancelled transfer —
/// and drops it when the last write exits.
pub type DecodeGuard = Box<dyn std::any::Any + Send>;

/// A [`DecodeAdmission`] hook failed to admit the streaming transform.
/// Exhaustive: admission has exactly one failure mode — the caller's
/// limiter failed — and it retains the caller's typed error as a source so
/// failure semantics stay machine-readable across the crate boundary.
#[derive(Debug, thiserror::Error)]
pub enum DecodeAdmissionError {
    #[error("decode admission failed: {source}")]
    Admission {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl DecodeAdmissionError {
    pub fn admission(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Admission {
            source: Box::new(source),
        }
    }
}

/// Factory wrapping the scratch file the sequential writer appends to.
/// Runs on the blocking pool per chunk and may perform CPU work (e.g. zstd
/// decode).
pub type WrapWriter = dyn Fn(std::fs::File) -> Box<dyn FinishingWrite> + Send + Sync;

/// Request to run a [`ObjectStore::get_file_streamed`] download with an
/// inline byte transform on the sequential-writer path.
pub struct StreamDecode<'a> {
    /// Admission for the transform's CPU work; consulted only when the
    /// transform will actually run inline (the ranged single-writer path).
    pub admission: &'a dyn DecodeAdmission,
    /// See [`WrapWriter`].
    pub wrap_writer: &'a WrapWriter,
}

/// Outcome of [`ObjectStore::get_file_streamed`]. The byte counts are
/// always wire bytes (the object's size), regardless of what a transform
/// expanded them to.
pub enum StreamedGetFile {
    /// The download used a path that cannot stream (single GET, no ETag,
    /// or a store without streaming support): `dst` holds the raw wire
    /// bytes and the caller must run its legacy transform pass.
    Fallback { bytes: u64 },
    /// The transform ran inline on the ranged single-writer path. `decoded`
    /// reports whether it actually decoded the stream rather than passing
    /// it through (e.g. the content was not zstd).
    Streamed { bytes: u64, decoded: bool },
}

/// Parameters of one ranged single-writer download attempt.
struct RangedDownloadPlan<'a> {
    size: u64,
    etag: String,
    ranges: Vec<MultipartPartRange>,
    /// Optional transform wrapped around the scratch file (streaming
    /// decode). `None` writes wire bytes untouched.
    wrap_writer: Option<&'a WrapWriter>,
    /// Admission guard for the transform's CPU work. Owned by the
    /// sequential writer so it survives exactly as long as the decode can
    /// run — including a detached blocking write after cancellation — and
    /// releases when the last write exits.
    decode_guard: Option<DecodeGuard>,
}

/// Each operation carries its own credentials and target so the same store
/// instance can serve any bucket in any region.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Select this capability before starting a producer. Unsupported backends
    /// keep their file-backed path; there is no implicit buffering fallback.
    fn streaming_upload(&self) -> Option<&dyn StreamingUpload> {
        None
    }

    /// Returns metadata for `target`, or `None` if the object does not exist.
    async fn head(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError>;

    /// Uploads `body` as a single object.
    async fn put_bytes(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        body: Bytes,
    ) -> Result<(), ObjectStoreError>;

    /// Copies one object to another location without the bytes passing through
    /// this process, which is what lets a caller move a multi-GiB artifact
    /// between buckets. `source_size` is the size the caller already probed;
    /// see the S3 implementation for why it is a parameter.
    ///
    /// Defaults to refusing rather than falling back to get-then-put: the
    /// fallback would silently stream tens of gigabytes through a caller that
    /// asked precisely not to, so a backend that cannot copy server-side should
    /// say so.
    async fn copy_object(
        &self,
        _creds: &ObjectCredentials,
        source: ObjectTarget<'_>,
        _dest: ObjectTarget<'_>,
        _source_size: u64,
    ) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::Copy(format!(
            "this object store cannot copy {}/{} server-side",
            source.bucket, source.key,
        )))
    }

    /// Reads the entire object body into memory. Use only for small
    /// JSON-shaped artifacts (metadata, manifest); large blobs should go
    /// through [`Self::get_file`].
    async fn get_bytes(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError>;

    /// Uploads the file at `src`. The implementation chooses single-shot or
    /// multipart based on size. Returns the number of bytes uploaded.
    async fn put_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        src: &Path,
    ) -> Result<u64, ObjectStoreError>;

    /// Streams the object body to a file at `dst`. Returns the number of
    /// bytes written.
    async fn get_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        dst: &Path,
    ) -> Result<u64, ObjectStoreError>;

    /// Like [`Self::get_file`], but applies `stream`'s transform inline on
    /// the ranged single-writer path so `dst` receives transformed (e.g.
    /// decoded) bytes without a second read-back pass. Paths that cannot
    /// stream safely fall back to the plain download and report
    /// `streamed: false`, leaving the wire bytes at `dst` for the caller's
    /// legacy transform pass. The default implementation never streams.
    async fn get_file_streamed(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        dst: &Path,
        stream: StreamDecode<'_>,
    ) -> Result<StreamedGetFile, ObjectStoreError> {
        let _ = &stream;
        let bytes = self.get_file(creds, target, dst).await?;
        Ok(StreamedGetFile::Fallback { bytes })
    }
}

/// S3 / S3-compatible object store implementation.
///
/// The struct holds only a base SDK client used to share the underlying
/// HTTP connection pool across requests; region, endpoint, addressing
/// style, and credentials are supplied per call via
/// [`aws_sdk_s3::operation::OperationCustomizable::config_override`].
///
/// Transfer tuning is resolved once at construction from env vars, falling
/// back to the [`MULTIPART_PART_SIZE`] / [`MULTIPART_CONCURRENCY`] consts
/// when unset, unparseable, or out of range. See [`Self::new`] for bounds.
#[derive(Clone)]
pub struct S3ObjectStore {
    base: Client,
    timeouts: S3Timeouts,
    /// The observer the base client's interceptor reports through, for
    /// failures the SDK cannot see: a response body read after `send()`.
    attempts: AttemptObserver,
    multipart_part_size: usize,
    multipart_concurrency: usize,
    download_reader_concurrency: usize,
    download_chunk_size: usize,
    /// Process-wide budget for buffered download chunks, in chunk-sized
    /// permits. Readers acquire a permit before fetching and release it only
    /// after the writer consumes the chunk, so total chunk memory across all
    /// concurrent downloads stays under the configured byte budget instead
    /// of scaling with `pull_concurrency × reader_concurrency`.
    download_buffer_permits: Arc<Semaphore>,
}

impl Default for S3ObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Env var that overrides the multipart part size, in bytes. Accepted range
/// is 8 MiB ..= 5 GiB. S3's hard lower limit is 5 MiB for non-final parts,
/// but SPACE uses an 8 MiB policy floor to avoid brittle tiny upload parts.
const MULTIPART_PART_SIZE_ENV: &str = "SPACE_OBJECT_STORE_MULTIPART_PART_SIZE_BYTES";

/// Env var that overrides the in-flight multipart part concurrency. Accepted
/// range is `1..=64`; the upper bound is a soft policy cap to avoid runaway
/// memory (each in-flight part buffers `part_size` bytes; at the maximum
/// 64 × 64 MiB you'd see ~4 GiB transient use).
const MULTIPART_CONCURRENCY_ENV: &str = "SPACE_OBJECT_STORE_MULTIPART_CONCURRENCY";

/// Env var that overrides how many ranged GET readers one file download runs
/// concurrently. Accepted range is `1..=64`. Readers parallelize only the
/// network fetch — the destination file always has exactly one writer, so
/// this knob never reintroduces concurrent writers on one inode.
const DOWNLOAD_READER_CONCURRENCY_ENV: &str = "SPACE_OBJECT_STORE_DOWNLOAD_READER_CONCURRENCY";

/// Env var that overrides the ranged-download chunk size, in bytes. Accepted
/// range is 8 MiB ..= 256 MiB. Peak buffered memory per download is
/// `chunk_size × reader_concurrency`.
const DOWNLOAD_CHUNK_SIZE_ENV: &str = "SPACE_OBJECT_STORE_DOWNLOAD_CHUNK_SIZE_BYTES";

/// Env var that overrides the process-wide download buffer budget, in bytes.
/// Accepted range is 256 MiB ..= 8 GiB. The budget is enforced as
/// `budget / chunk_size` chunk permits shared by every concurrent download
/// in the process; readers block when it is exhausted, so peak chunk memory
/// no longer scales with the pull-slot count.
const DOWNLOAD_BUFFER_BUDGET_ENV: &str = "SPACE_OBJECT_STORE_DOWNLOAD_BUFFER_BUDGET_BYTES";

/// Env var that overrides how long an idle pooled connection stays eligible
/// for reuse, in whole seconds. Accepted range is `1..=90`; 90 s is hyper's
/// default, which lets S3 close a connection before the pool drops it. Keep
/// it below the endpoint's idle close.
const POOL_IDLE_TIMEOUT_ENV: &str = "SPACE_OBJECT_STORE_POOL_IDLE_TIMEOUT_SECS";

const MULTIPART_PART_SIZE_MIN: usize = 8 * 1024 * 1024;
const MULTIPART_PART_SIZE_MAX: usize = 5 * 1024 * 1024 * 1024;
const MULTIPART_CONCURRENCY_MIN: usize = 1;
const MULTIPART_CONCURRENCY_MAX: usize = 64;
const DEFAULT_DOWNLOAD_READER_CONCURRENCY: usize = 8;
const DEFAULT_DOWNLOAD_CHUNK_SIZE: usize = 32 * 1024 * 1024;
const DOWNLOAD_CHUNK_SIZE_MIN: usize = 8 * 1024 * 1024;
const DOWNLOAD_CHUNK_SIZE_MAX: usize = 256 * 1024 * 1024;
const DEFAULT_DOWNLOAD_BUFFER_BUDGET: usize = 2 * 1024 * 1024 * 1024;
const DOWNLOAD_BUFFER_BUDGET_MIN: usize = 256 * 1024 * 1024;
const DOWNLOAD_BUFFER_BUDGET_MAX: usize = 8 * 1024 * 1024 * 1024;

/// Bounds on `SPACE_OBJECT_STORE_POOL_IDLE_TIMEOUT_SECS`; unset, it is
/// [`DEFAULT_POOL_IDLE_TIMEOUT`].
const POOL_IDLE_TIMEOUT_SECS_MIN: u64 = 1;
const POOL_IDLE_TIMEOUT_SECS_MAX: u64 = 90;

/// Upload stalled-stream grace period. Equal to the SDK's effective default
/// (aws-smithy-runtime's default stalled-stream plugin sets 5 s; the 20 s
/// `DEFAULT_GRACE_PERIOD` constant is only a builder fallback and does not
/// apply), stated here so the value is visible next to the timeouts.
const STALLED_STREAM_GRACE_PERIOD: Duration = Duration::from_secs(5);

type S3Response = aws_smithy_runtime_api::http::Response;
type S3SdkError<E> = SdkError<E, S3Response>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum S3Operation {
    AbortMultipartUpload,
    CompleteMultipartUpload,
    CopyObject,
    CreateMultipartUpload,
    GetObject,
    HeadObject,
    PutObject,
    UploadPart,
    UploadPartCopy,
}

impl S3Operation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AbortMultipartUpload => "AbortMultipartUpload",
            Self::CompleteMultipartUpload => "CompleteMultipartUpload",
            Self::CopyObject => "CopyObject",
            Self::CreateMultipartUpload => "CreateMultipartUpload",
            Self::GetObject => "GetObject",
            Self::HeadObject => "HeadObject",
            Self::PutObject => "PutObject",
            Self::UploadPart => "UploadPart",
            Self::UploadPartCopy => "UploadPartCopy",
        }
    }
}

impl std::fmt::Display for S3Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum S3ErrorKind {
    Construction,
    Dispatch,
    RateLimited,
    Response,
    Service,
    Timeout,
    Unknown,
}

impl S3ErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Construction => "construction",
            Self::Dispatch => "dispatch",
            Self::RateLimited => "rate_limited",
            Self::Response => "response",
            Self::Service => "service",
            Self::Timeout => "timeout",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for S3ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct S3ErrorClassification<'a> {
    kind: S3ErrorKind,
    code: &'a str,
    http_status: Option<u16>,
}

impl S3ErrorClassification<'_> {
    fn http_status_tag(self) -> String {
        self.http_status
            .map_or_else(|| "none".to_string(), |status| status.to_string())
    }
}

fn resolve_part_size<F: FnOnce(&str) -> Option<String>>(
    read: F,
    env_name: &'static str,
    default: usize,
) -> usize {
    read(env_name)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| (MULTIPART_PART_SIZE_MIN..=MULTIPART_PART_SIZE_MAX).contains(&n))
        .unwrap_or(default)
}

fn resolve_concurrency<F: FnOnce(&str) -> Option<String>>(
    read: F,
    env_name: &'static str,
    default: usize,
) -> usize {
    read(env_name)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| (MULTIPART_CONCURRENCY_MIN..=MULTIPART_CONCURRENCY_MAX).contains(&n))
        .unwrap_or(default)
}

fn resolve_multipart_part_size<F: FnOnce(&str) -> Option<String>>(read: F) -> usize {
    resolve_part_size(read, MULTIPART_PART_SIZE_ENV, MULTIPART_PART_SIZE)
}

fn resolve_multipart_concurrency<F: FnOnce(&str) -> Option<String>>(read: F) -> usize {
    resolve_concurrency(read, MULTIPART_CONCURRENCY_ENV, MULTIPART_CONCURRENCY)
}

fn resolve_download_reader_concurrency<F: FnOnce(&str) -> Option<String>>(read: F) -> usize {
    resolve_concurrency(
        read,
        DOWNLOAD_READER_CONCURRENCY_ENV,
        DEFAULT_DOWNLOAD_READER_CONCURRENCY,
    )
}

fn resolve_download_chunk_size<F: FnOnce(&str) -> Option<String>>(read: F) -> usize {
    read(DOWNLOAD_CHUNK_SIZE_ENV)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| (DOWNLOAD_CHUNK_SIZE_MIN..=DOWNLOAD_CHUNK_SIZE_MAX).contains(&n))
        .unwrap_or(DEFAULT_DOWNLOAD_CHUNK_SIZE)
}

fn resolve_download_buffer_budget<F: FnOnce(&str) -> Option<String>>(read: F) -> usize {
    read(DOWNLOAD_BUFFER_BUDGET_ENV)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| (DOWNLOAD_BUFFER_BUDGET_MIN..=DOWNLOAD_BUFFER_BUDGET_MAX).contains(&n))
        .unwrap_or(DEFAULT_DOWNLOAD_BUFFER_BUDGET)
}

fn resolve_pool_idle_timeout<F: FnOnce(&str) -> Option<String>>(read: F) -> Duration {
    read_secs(
        read,
        POOL_IDLE_TIMEOUT_ENV,
        POOL_IDLE_TIMEOUT_SECS_MIN..=POOL_IDLE_TIMEOUT_SECS_MAX,
    )
    .unwrap_or(DEFAULT_POOL_IDLE_TIMEOUT)
}

/// Telemetry-safe endpoint label: host and optional port only, so a custom
/// endpoint URL carrying userinfo or other sensitive components never
/// reaches exported spans.
fn endpoint_span_label(endpoint: Option<&str>) -> String {
    let Some(endpoint) = endpoint else {
        return "aws-default".to_string();
    };
    match endpoint.parse::<http::Uri>() {
        Ok(uri) => match (uri.host(), uri.port_u16()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host.to_string(),
            _ => "invalid-endpoint".to_string(),
        },
        Err(_) => "invalid-endpoint".to_string(),
    }
}

/// Chunk permits backing the process-wide download buffer budget. At least
/// one permit so a lone download always makes progress even when the budget
/// is misconfigured below the chunk size.
fn download_buffer_chunk_permits(budget_bytes: usize, chunk_size: usize) -> usize {
    (budget_bytes / chunk_size.max(1)).max(1)
}

/// The one download buffer budget for the whole process. Every
/// [`S3ObjectStore`] shares this semaphore no matter how many instances a
/// process constructs — a per-instance budget would silently scale total
/// download memory as `store_count × budget`. Sized on first use from the
/// first store's resolved budget and chunk size; later stores with different
/// tuning share the already-initialized budget.
fn global_download_buffer_permits(chunk_permits: usize) -> Arc<Semaphore> {
    static PERMITS: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
    PERMITS
        .get_or_init(|| Arc::new(Semaphore::new(chunk_permits)))
        .clone()
}

fn classify_s3_sdk_error<E>(err: &S3SdkError<E>) -> S3ErrorClassification<'_>
where
    E: ProvideErrorMetadata,
{
    let http_status = err
        .raw_response()
        .map(|response| response.status().as_u16());
    let code = err
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .unwrap_or_else(|| fallback_s3_error_code(err, http_status));

    let kind = if is_s3_rate_limited(Some(code), http_status) {
        S3ErrorKind::RateLimited
    } else {
        match err {
            SdkError::ConstructionFailure(_) => S3ErrorKind::Construction,
            SdkError::TimeoutError(_) => S3ErrorKind::Timeout,
            SdkError::DispatchFailure(_) => S3ErrorKind::Dispatch,
            SdkError::ResponseError(_) => S3ErrorKind::Response,
            SdkError::ServiceError(_) => S3ErrorKind::Service,
            _ => S3ErrorKind::Unknown,
        }
    };

    S3ErrorClassification {
        kind,
        code,
        http_status,
    }
}

fn fallback_s3_error_code<E>(err: &S3SdkError<E>, http_status: Option<u16>) -> &'static str {
    match err {
        SdkError::ConstructionFailure(_) => "construction",
        SdkError::TimeoutError(_) => "timeout",
        SdkError::DispatchFailure(_) => "dispatch",
        SdkError::ResponseError(_) => http_status_code_label(http_status),
        SdkError::ServiceError(_) => http_status_code_label(http_status),
        _ => "unknown",
    }
}

fn http_status_code_label(status: Option<u16>) -> &'static str {
    match status {
        Some(429) => "http_429",
        Some(503) => "http_503",
        Some(500..=599) => "http_5xx",
        Some(400..=499) => "http_4xx",
        Some(_) => "http_other",
        None => "unknown",
    }
}

fn is_s3_rate_limited(code: Option<&str>, http_status: Option<u16>) -> bool {
    matches!(http_status, Some(429 | 503))
        || matches!(
            code,
            Some(
                "SlowDown"
                    | "Throttling"
                    | "ThrottlingException"
                    | "RequestThrottled"
                    | "RequestThrottledException"
                    | "RequestLimitExceeded"
                    | "TooManyRequests"
                    | "TooManyRequestsException"
                    | "TooManyRequestsError"
            )
        )
}

fn observe_s3_error<E>(operation: S3Operation, bucket: &str, key: &str, err: &S3SdkError<E>)
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let classification = classify_s3_sdk_error(err);
    let http_status = classification.http_status_tag();
    pplx_space_metrics::object_store::s3_error(
        operation.as_str(),
        classification.kind.as_str(),
        classification.code,
        &http_status,
    );
    warn!(
        operation = operation.as_str(),
        bucket,
        key,
        error_kind = classification.kind.as_str(),
        error_code = classification.code,
        http_status,
        error = %DisplayErrorContext(err),
        "S3 object-store operation failed"
    );
}

fn s3_base_config(
    timeouts: S3Timeouts,
    pool_idle_timeout: Duration,
    observer: AttemptObserver,
) -> aws_sdk_s3::config::Builder {
    let placeholder_creds = Credentials::new(
        "placeholder",
        "placeholder",
        None,
        None,
        "pplx-space-object-store-base",
    );
    Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(placeholder_creds)
        .http_client(s3_http_client(pool_idle_timeout))
        // aws-sdk-s3 1.79+ defaults to `WhenSupported`, which can add
        // opportunistic CRC trailers to ordinary PUT bodies via streaming
        // chunked encoding. SPACE snapshot uploads have seen those requests
        // stall until S3 returns RequestTimeout, so keep checksums to
        // operations where S3 requires them.
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .stalled_stream_protection(
            StalledStreamProtectionConfig::enabled()
                .grace_period(STALLED_STREAM_GRACE_PERIOD)
                .build(),
        )
        // Every call overrides this with its own class's bounds (see
        // `timeouts`); the base carries the tightest class so a call that
        // names none is still bounded.
        .timeout_config(timeouts.request.timeout_config())
        .interceptor(observer)
}

impl S3ObjectStore {
    /// Constructs a store that delegates region/endpoint/credentials to each
    /// caller. The base client carries placeholder values that are always
    /// overridden by per-operation `config_override`.
    ///
    /// Transfer tuning is read once from env. Out-of-range or unparseable
    /// values fall back silently to the const defaults so a typo in a Helm
    /// value doesn't crash boot. Bounds:
    ///
    /// - `SPACE_OBJECT_STORE_MULTIPART_PART_SIZE_BYTES`: 8 MiB ..= 5 GiB
    ///   (S3's hard limit is 5 MiB; SPACE uses an 8 MiB policy floor).
    ///   Governs multipart uploads only.
    /// - `SPACE_OBJECT_STORE_MULTIPART_CONCURRENCY`: 1..=64 multipart upload
    ///   tasks (soft cap to bound transient memory; each in-flight part
    ///   buffers `part_size` bytes).
    /// - `SPACE_OBJECT_STORE_DOWNLOAD_READER_CONCURRENCY`: 1..=64 concurrent
    ///   ranged GET readers per file download. The destination always has
    ///   exactly one writer regardless of this value.
    /// - `SPACE_OBJECT_STORE_DOWNLOAD_CHUNK_SIZE_BYTES`: 8 MiB ..= 256 MiB.
    ///   Ranged-download fetch granularity; also bounds the reorder buffer
    ///   at `chunk_size × reader_concurrency` per download.
    /// - `SPACE_OBJECT_STORE_DOWNLOAD_BUFFER_BUDGET_BYTES`: 256 MiB ..= 8 GiB.
    ///   Process-wide cap on buffered download chunks across every
    ///   concurrent pull.
    /// - `SPACE_OBJECT_STORE_{REQUEST,BODY_UPLOAD,SERVER_SIDE}_ATTEMPT_TIMEOUT_SECS`:
    ///   5..=21600, defaults 10, 60, and 30. Bound each attempt of an
    ///   operation in that class; see `timeouts` for which operations each
    ///   class holds and the reasoning behind the defaults. The
    ///   body-upload default fits one 256 MiB part, so raise it with the part
    ///   size.
    /// - `SPACE_OBJECT_STORE_{REQUEST,BODY_UPLOAD,SERVER_SIDE}_OPERATION_TIMEOUT_SECS`:
    ///   5..=86400, default three attempt timeouts plus 5 s of retry backoff
    ///   (35, 185, and 95). Bound an operation across all of its retries.
    /// - `SPACE_OBJECT_STORE_POOL_IDLE_TIMEOUT_SECS`: 1..=90, default 3. How
    ///   long an idle pooled connection stays eligible for reuse; keep it
    ///   below the endpoint's idle close (S3: 6 s).
    pub fn new() -> Self {
        let timeouts = S3Timeouts::from_env();
        let pool_idle_timeout = resolve_pool_idle_timeout(|name| std::env::var(name).ok());
        let attempts = AttemptObserver::logging();
        Self::with_base_config(
            s3_base_config(timeouts, pool_idle_timeout, attempts.clone()).build(),
            timeouts,
            pool_idle_timeout,
            attempts,
        )
    }

    /// `attempts` must be the observer `conf` carries, so body-read failures
    /// the SDK never sees reach the same reporter as its own attempts, and
    /// `pool_idle_timeout` the one its HTTP client was built with.
    fn with_base_config(
        conf: Config,
        timeouts: S3Timeouts,
        pool_idle_timeout: Duration,
        attempts: AttemptObserver,
    ) -> Self {
        let multipart_part_size = resolve_multipart_part_size(|name| std::env::var(name).ok());
        let multipart_concurrency = resolve_multipart_concurrency(|name| std::env::var(name).ok());
        let download_reader_concurrency =
            resolve_download_reader_concurrency(|name| std::env::var(name).ok());
        let download_chunk_size = resolve_download_chunk_size(|name| std::env::var(name).ok());
        let download_buffer_budget =
            resolve_download_buffer_budget(|name| std::env::var(name).ok());
        let download_buffer_chunks =
            download_buffer_chunk_permits(download_buffer_budget, download_chunk_size);
        info!(
            multipart_concurrency,
            multipart_part_size,
            download_reader_concurrency,
            download_chunk_size,
            download_buffer_budget,
            download_buffer_chunks,
            request_attempt_timeout_secs = timeouts.request.attempt.as_secs(),
            request_operation_timeout_secs = timeouts.request.operation.as_secs(),
            body_upload_attempt_timeout_secs = timeouts.body_upload.attempt.as_secs(),
            body_upload_operation_timeout_secs = timeouts.body_upload.operation.as_secs(),
            server_side_attempt_timeout_secs = timeouts.server_side.attempt.as_secs(),
            server_side_operation_timeout_secs = timeouts.server_side.operation.as_secs(),
            pool_idle_timeout_secs = pool_idle_timeout.as_secs(),
            "object-store: transfer tuning"
        );
        Self {
            base: Client::from_conf(conf),
            timeouts,
            attempts,
            multipart_part_size,
            multipart_concurrency,
            download_reader_concurrency,
            download_chunk_size,
            download_buffer_permits: global_download_buffer_permits(download_buffer_chunks),
        }
    }

    /// Builds a per-request config override from the caller's creds +
    /// target, bounded by `operation`'s timeout class.
    fn op_config(
        &self,
        creds: &ObjectCredentials,
        target: &ObjectTarget<'_>,
        operation: S3Operation,
    ) -> aws_sdk_s3::config::Builder {
        Self::op_config_with(creds, target, self.timeouts.for_operation(operation))
    }

    /// [`Self::op_config`] for work that outlives the borrow of the store.
    fn op_config_with(
        creds: &ObjectCredentials,
        target: &ObjectTarget<'_>,
        timeouts: TimeoutConfig,
    ) -> aws_sdk_s3::config::Builder {
        let credentials = Credentials::new(
            creds.access_key_id.clone(),
            creds.secret_access_key.clone(),
            creds.session_token.clone(),
            None,
            "pplx-space-object-store",
        );
        let mut builder = aws_sdk_s3::config::Builder::new()
            .region(Region::new(target.region.to_string()))
            .credentials_provider(credentials)
            .force_path_style(target.path_style)
            .timeout_config(timeouts);
        if let Some(endpoint) = target.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        builder
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    fn streaming_upload(&self) -> Option<&dyn StreamingUpload> {
        Some(self)
    }

    async fn head(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        match self
            .base
            .head_object()
            .bucket(target.bucket)
            .key(target.key)
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::HeadObject))
            .send()
            .await
        {
            Ok(resp) => Ok(Some(ObjectMeta {
                size: resp.content_length().unwrap_or(0).max(0).cast_unsigned(),
                etag: resp.e_tag().map(str::to_string),
            })),
            Err(SdkError::ServiceError(svc))
                if matches!(svc.err(), HeadObjectError::NotFound(_)) =>
            {
                Ok(None)
            }
            Err(err) => {
                observe_s3_error(S3Operation::HeadObject, target.bucket, target.key, &err);
                if let Some(error) = classify_terminal_s3_error(&err, target.key) {
                    return Err(error);
                }
                Err(ObjectStoreError::Head(
                    DisplayErrorContext(&err).to_string(),
                ))
            }
        }
    }

    async fn copy_object(
        &self,
        creds: &ObjectCredentials,
        source: ObjectTarget<'_>,
        dest: ObjectTarget<'_>,
        source_size: u64,
    ) -> Result<(), ObjectStoreError> {
        self.copy_object_server_side(creds, source, dest, source_size)
            .await
    }

    async fn put_bytes(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        body: Bytes,
    ) -> Result<(), ObjectStoreError> {
        let digest_body = body.clone();
        let content_md5 = tokio::task::spawn_blocking(move || content_md5::of_bytes(&digest_body))
            .await
            .map_err(|err| ObjectStoreError::Put(err.to_string()))?;
        self.base
            .put_object()
            .bucket(target.bucket)
            .key(target.key)
            .content_md5(content_md5)
            .body(ByteStream::from(body))
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::PutObject))
            .send()
            .await
            .map_err(|err| {
                observe_s3_error(S3Operation::PutObject, target.bucket, target.key, &err);
                ObjectStoreError::Put(DisplayErrorContext(&err).to_string())
            })?;
        Ok(())
    }

    async fn get_bytes(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        let resp = self
            .base
            .get_object()
            .bucket(target.bucket)
            .key(target.key)
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::GetObject))
            .send()
            .await
            .map_err(|err| classify_get_error(err, target.bucket, target.key))?;
        let read_started_at = Instant::now();
        resp.body
            .collect()
            .await
            .map_err(|err| {
                self.single_get_body_failed(&target, read_started_at, &err);
                ObjectStoreError::Get(DisplayErrorContext(&err).to_string())
            })
            .map(|aggregated| aggregated.to_vec())
    }

    async fn put_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        src: &Path,
    ) -> Result<u64, ObjectStoreError> {
        let size = tokio::fs::metadata(src).await?.len();
        let part_ranges = multipart_part_ranges(size, self.multipart_part_size);
        if part_ranges.len() <= 1 {
            info!(
                bucket = target.bucket,
                key = target.key,
                size_bytes = size,
                endpoint = target.endpoint.unwrap_or("aws-default"),
                path_style = target.path_style,
                "uploading object with single put"
            );
            let content_md5 = file_range_content_md5(src, 0, size).await.map_err(|err| {
                let error = err.to_string();
                warn!(
                    bucket = target.bucket,
                    key = target.key,
                    error,
                    "failed to digest file for single put"
                );
                ObjectStoreError::Put(error)
            })?;
            let body = ByteStream::read_from()
                .path(src)
                .buffer_size(UPLOAD_READ_BUFFER_SIZE)
                .build()
                .await
                .map_err(|err| {
                    let error = DisplayErrorContext(&err).to_string();
                    warn!(
                        bucket = target.bucket,
                        key = target.key,
                        error,
                        "failed to read file for single put"
                    );
                    ObjectStoreError::Put(error)
                })?;
            self.base
                .put_object()
                .bucket(target.bucket)
                .key(target.key)
                .content_md5(content_md5)
                .body(body)
                .customize()
                .config_override(self.op_config(creds, &target, S3Operation::PutObject))
                .send()
                .await
                .map_err(|err| {
                    observe_s3_error(S3Operation::PutObject, target.bucket, target.key, &err);
                    let error = DisplayErrorContext(&err).to_string();
                    warn!(
                        bucket = target.bucket,
                        key = target.key,
                        size_bytes = size,
                        endpoint = target.endpoint.unwrap_or("aws-default"),
                        path_style = target.path_style,
                        error,
                        "single put upload failed"
                    );
                    ObjectStoreError::Put(error)
                })?;
            return Ok(size);
        }

        let part_count = part_ranges.len();
        info!(
            bucket = target.bucket,
            key = target.key,
            size_bytes = size,
            part_count,
            part_size_bytes = self.multipart_part_size,
            concurrency = self.multipart_concurrency,
            endpoint = target.endpoint.unwrap_or("aws-default"),
            path_style = target.path_style,
            "starting multipart upload"
        );
        let create = self
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
        let upload_id = create
            .upload_id()
            .ok_or(ObjectStoreError::MissingField("upload_id"))?
            .to_string();
        info!(
            bucket = target.bucket,
            key = target.key,
            upload_id,
            "created multipart upload"
        );

        let abort = AbortGuard::new(self, creds, &target, &upload_id);
        let parts = self
            .upload_parts(creds, &target, &upload_id, src, size)
            .await
            .inspect_err(
                |err| warn!(error = %err, key = target.key, "multipart upload failed; will abort"),
            )?;

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        info!(
            bucket = target.bucket,
            key = target.key,
            upload_id,
            part_count,
            "completing multipart upload"
        );
        self.base
            .complete_multipart_upload()
            .bucket(target.bucket)
            .key(target.key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
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
        abort.disarm();
        info!(
            bucket = target.bucket,
            key = target.key,
            upload_id,
            part_count,
            size_bytes = size,
            "completed multipart upload"
        );
        Ok(size)
    }

    async fn get_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        dst: &Path,
    ) -> Result<u64, ObjectStoreError> {
        let resp = self
            .base
            .get_object()
            .bucket(target.bucket)
            .key(target.key)
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::GetObject))
            .send()
            .await
            .map_err(|err| classify_get_error(err, target.bucket, target.key))?;
        let size = get_object_content_length(target.key, resp.content_length())?;

        if let Some(chunk_ranges) = single_writer_download_ranges(
            size,
            self.download_chunk_size,
            self.download_reader_concurrency,
        ) {
            // Pin every ranged GET to the version the size came from so an
            // object overwritten mid-download yields 412s (job-level retry)
            // instead of a file stitched from two versions. An endpoint that
            // returns no ETag cannot be pinned, and unpinned ranged GETs
            // could assemble equal-length ranges from different object
            // versions — stream the already-open response instead.
            if let Some(etag) = resp.e_tag().map(str::to_string) {
                drop(resp);
                let plan = RangedDownloadPlan {
                    size,
                    etag,
                    ranges: chunk_ranges,
                    wrap_writer: None,
                    decode_guard: None,
                };
                let (total, _transformed) = self
                    .download_ranged_single_writer(creds, &target, dst, plan)
                    .await?;
                return Ok(total);
            }
            warn!(
                bucket = target.bucket,
                key = target.key,
                size_bytes = size,
                "object has no ETag; falling back to a single-stream download \
                 instead of unpinnable ranged GETs"
            );
        }

        self.write_single_get_response(target, dst, size, resp)
            .await
    }

    async fn get_file_streamed(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        dst: &Path,
        stream: StreamDecode<'_>,
    ) -> Result<StreamedGetFile, ObjectStoreError> {
        let resp = self
            .base
            .get_object()
            .bucket(target.bucket)
            .key(target.key)
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::GetObject))
            .send()
            .await
            .map_err(|err| classify_get_error(err, target.bucket, target.key))?;
        let size = get_object_content_length(target.key, resp.content_length())?;

        if let Some(chunk_ranges) = single_writer_download_ranges(
            size,
            self.download_chunk_size,
            self.download_reader_concurrency,
        ) && let Some(etag) = resp.e_tag().map(str::to_string)
        {
            drop(resp);
            // Admission before any body chunk is fetched: a stream that
            // holds no decode guard holds no buffer-budget permits either,
            // so the two semaphores cannot deadlock against each other. The
            // guard is owned by the sequential writer, so it spans the
            // whole overlapped download+decode — including any detached
            // blocking write that outlives a cancelled transfer — and
            // releases when the last write exits.
            let decode_guard = stream.admission.admit(size).await?;
            let plan = RangedDownloadPlan {
                size,
                etag,
                ranges: chunk_ranges,
                wrap_writer: Some(stream.wrap_writer),
                decode_guard,
            };
            let (total, transformed) = self
                .download_ranged_single_writer(creds, &target, dst, plan)
                .await?;
            return Ok(StreamedGetFile::Streamed {
                bytes: total,
                decoded: transformed,
            });
        }

        // Small object or unpinnable endpoint: plain single-stream download,
        // wire bytes land at `dst`, the caller runs its legacy transform.
        let bytes = self
            .write_single_get_response(target, dst, size, resp)
            .await?;
        Ok(StreamedGetFile::Fallback { bytes })
    }
}

impl S3ObjectStore {
    async fn write_single_get_response(
        &self,
        target: ObjectTarget<'_>,
        dst: &Path,
        size: u64,
        resp: GetObjectOutput,
    ) -> Result<u64, ObjectStoreError> {
        info!(
            bucket = target.bucket,
            key = target.key,
            size_bytes = size,
            endpoint = target.endpoint.unwrap_or("aws-default"),
            path_style = target.path_style,
            "downloading object with single get"
        );
        let mut body = resp.body;
        let mut writer = tokio::fs::File::create(dst).await?;
        let read_started_at = Instant::now();
        write_body_to_file(&mut writer, &mut body, size, "object body", |error| {
            self.single_get_body_failed(&target, read_started_at, error);
        })
        .await
    }

    /// Reports a failed read of a whole-object `GetObject` body. Not
    /// retried: without a pinned ETag a re-read could return another
    /// version, and only small objects take this path when one is pinned.
    fn single_get_body_failed(
        &self,
        target: &ObjectTarget<'_>,
        read_started_at: Instant,
        error: &ByteStreamError,
    ) {
        self.attempts.body_read_failed(
            &AttemptTarget {
                bucket: Some(target.bucket.to_string()),
                key: Some(target.key.to_string()),
                ..AttemptTarget::default()
            },
            1,
            false,
            read_started_at.elapsed(),
            error,
        );
    }

    /// Ranged download with parallel readers and one sequential writer.
    ///
    /// Up to `download_reader_concurrency` tasks fetch chunks over separate
    /// connections; completed chunks park in a reorder window until they are
    /// next in file order, and a single writer appends them sequentially
    /// with buffered I/O (see [`download`] for why buffered). Readers
    /// never run more than one window ahead of the writer, so buffered chunks
    /// are bounded at `chunk_size × reader_concurrency`. Exactly one task
    /// writes the destination inode: concurrent ranged writers convoy on
    /// btrfs's inode lock while it waits out in-flight writeback, and a
    /// storm of such downloads is what pushed restore pulls past the kubelet
    /// startup deadline.
    async fn download_ranged_single_writer(
        &self,
        creds: &ObjectCredentials,
        target: &ObjectTarget<'_>,
        dst: &Path,
        plan: RangedDownloadPlan<'_>,
    ) -> Result<(u64, bool), ObjectStoreError> {
        let pipeline_started_at = Instant::now();
        let RangedDownloadPlan {
            size,
            etag,
            ranges,
            wrap_writer,
            decode_guard,
        } = plan;
        let chunk_count = ranges.len();
        let reader_concurrency = self.download_reader_concurrency.max(1);
        let chunk_size = self.download_chunk_size;

        let open_dst = dst.to_path_buf();
        // The guard is born inside the blocking closure: a cancellation
        // that abandons the join result drops the guard on the blocking
        // thread and reclaims the scratch file.
        let (scratch_file, scratch_guard) =
            tokio::task::spawn_blocking(move || open_scratch(&open_dst))
                .await
                .map_err(|err| ObjectStoreError::Get(err.to_string()))??;
        // Building the sink is cheap (a decoder allocation at most); the
        // per-chunk CPU work runs inside the write_chunk blocking calls.
        let sink: Box<dyn FinishingWrite> = match wrap_writer {
            Some(wrap) => wrap(scratch_file),
            None => Box::new(scratch_file),
        };
        let mut writer =
            SequentialWriter::from_parts(sink, scratch_guard, dst.to_path_buf(), decode_guard);

        info!(
            bucket = target.bucket,
            key = target.key,
            size_bytes = size,
            chunk_count,
            chunk_size_bytes = chunk_size,
            reader_concurrency,
            endpoint = target.endpoint.unwrap_or("aws-default"),
            path_style = target.path_style,
            "starting ranged download"
        );

        type FetchedChunk = (usize, Bytes, OwnedSemaphorePermit, u128);
        let mut tasks: JoinSet<Result<FetchedChunk, ObjectStoreError>> = JoinSet::new();
        let mut next_chunk_index = 0;
        let mut in_flight = 0;
        let mut window: ReorderWindow<(Bytes, OwnedSemaphorePermit)> = ReorderWindow::new();
        let mut fetch_work_ms = 0_u64;
        let mut slowest_fetch_ms = 0_u64;
        let mut range_join_wait_ms = 0_u64;
        let mut download_budget_wait_ms = 0_u64;
        let mut sequential_writer_ms = 0_u64;
        let mut max_reorder_pending_chunks = 0_usize;

        while window.next_index() < chunk_count {
            while in_flight < reader_concurrency
                && next_chunk_index < chunk_count
                && next_chunk_index < window.next_index() + reader_concurrency
            {
                // One buffer permit per fetched-but-unwritten chunk, shared
                // process-wide. Block on the budget only while holding no
                // permits at all (nothing in flight, nothing parked): in that
                // state this download cannot be part of a permit cycle, and
                // every permit holder elsewhere either has its next-needed
                // chunk in flight or parked, so it eventually writes and
                // releases. With anything in flight or parked, fall back to
                // try_acquire and let our own completions drive progress.
                let permit = if in_flight == 0 && window.pending_len() == 0 {
                    let budget_wait_started_at = Instant::now();
                    let acquired = self.download_buffer_permits.clone().acquire_owned().await;
                    download_budget_wait_ms = download_budget_wait_ms.saturating_add(
                        u64::try_from(budget_wait_started_at.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                    );
                    match acquired {
                        Ok(permit) => Some(permit),
                        Err(_closed) => return Err(ObjectStoreError::DownloadBudgetClosed),
                    }
                } else {
                    match self.download_buffer_permits.clone().try_acquire_owned() {
                        Ok(permit) => Some(permit),
                        Err(TryAcquireError::NoPermits) => None,
                        Err(TryAcquireError::Closed) => {
                            return Err(ObjectStoreError::DownloadBudgetClosed);
                        }
                    }
                };
                let Some(permit) = permit else {
                    // Budget exhausted: stop fanning out and make progress by
                    // joining in-flight fetches and draining the window.
                    break;
                };

                let chunk_index = next_chunk_index;
                next_chunk_index += 1;
                in_flight += 1;

                let range = ranges
                    .get(chunk_index)
                    .copied()
                    .expect("next_chunk_index is bounded by ranges length");
                let fetch = ChunkFetch {
                    client: self.base.clone(),
                    creds: creds.clone(),
                    bucket: target.bucket.to_string(),
                    key: target.key.to_string(),
                    region: target.region.to_string(),
                    endpoint: target.endpoint.map(str::to_string),
                    path_style: target.path_style,
                    etag: etag.clone(),
                    chunk_index,
                    range_header: byte_range_header(range),
                    length: range.length,
                    timeouts: self.timeouts.for_operation(S3Operation::GetObject),
                    attempts: self.attempts.clone(),
                };

                debug!(
                    bucket = fetch.bucket,
                    key = fetch.key,
                    chunk_index,
                    offset_bytes = range.offset,
                    length_bytes = range.length,
                    range = fetch.range_header,
                    in_flight,
                    reader_concurrency,
                    "scheduling ranged download chunk"
                );

                // Client span per ranged GET: the fan-out issues up to
                // `reader_concurrency` concurrent external calls whose
                // latency would otherwise be invisible between the
                // per-download start/complete logs.
                let span = tracing::info_span!(
                    "s3.client",
                    otel.kind = "client",
                    otel.name = "S3.GetObject.range",
                    otel.status_code = tracing::field::Empty,
                    otel.status_description = tracing::field::Empty,
                    "operation.name" = "s3.client",
                    s3.bucket = %fetch.bucket,
                    s3.key = %fetch.key,
                    s3.range = %fetch.range_header,
                    s3.chunk_index = chunk_index,
                    s3.endpoint = %endpoint_span_label(fetch.endpoint.as_deref()),
                );
                tasks.spawn(
                    async move {
                        let started_at = Instant::now();
                        let outcome = fetch.fetch().await.map(|bytes| {
                            (chunk_index, bytes, permit, started_at.elapsed().as_millis())
                        });
                        // External-client spans must record an outcome on
                        // every exit path, not just expose latency.
                        let current = tracing::Span::current();
                        match &outcome {
                            Ok(_) => {
                                current.record("otel.status_code", "OK");
                            }
                            Err(err) => {
                                current.record("otel.status_code", "ERROR");
                                current.record(
                                    "otel.status_description",
                                    tracing::field::display(err),
                                );
                            }
                        }
                        outcome
                    }
                    .instrument(span),
                );
            }

            let join_wait_started_at = Instant::now();
            let joined = tasks.join_next().await;
            range_join_wait_ms = range_join_wait_ms.saturating_add(
                u64::try_from(join_wait_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            );
            let Some(joined) = joined else {
                return Err(ObjectStoreError::DownloadStalled {
                    next_chunk: window.next_index(),
                    chunk_count,
                });
            };
            in_flight -= 1;
            match joined {
                Ok(Ok((chunk_index, bytes, permit, elapsed_ms))) => {
                    let elapsed_ms = u64::try_from(elapsed_ms).unwrap_or(u64::MAX);
                    fetch_work_ms = fetch_work_ms.saturating_add(elapsed_ms);
                    slowest_fetch_ms = slowest_fetch_ms.max(elapsed_ms);
                    debug!(
                        chunk_index,
                        length_bytes = bytes.len(),
                        elapsed_ms,
                        pending_chunks = window.pending_len() + 1,
                        chunk_count,
                        in_flight,
                        "fetched ranged download chunk"
                    );
                    window.insert(chunk_index, (bytes, permit));
                    max_reorder_pending_chunks =
                        max_reorder_pending_chunks.max(window.pending_len());
                }
                Ok(Err(err)) => {
                    warn!(error = %err, in_flight, "ranged download chunk task failed; aborting remaining tasks");
                    tasks.abort_all();
                    return Err(err);
                }
                Err(join_err) => {
                    warn!(error = %join_err, in_flight, "ranged download chunk task join failed; aborting remaining tasks");
                    tasks.abort_all();
                    return Err(ObjectStoreError::Get(join_err.to_string()));
                }
            }

            while let Some((chunk, permit)) = window.pop_ready() {
                let write_started_at = Instant::now();
                writer = tokio::task::spawn_blocking(move || {
                    writer.write_chunk(&chunk)?;
                    Ok::<_, std::io::Error>(writer)
                })
                .await
                .map_err(|err| ObjectStoreError::Get(err.to_string()))??;
                sequential_writer_ms = sequential_writer_ms.saturating_add(
                    u64::try_from(write_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                );
                // The chunk's bytes are on disk (or in page cache in the
                // buffered fallback); return its share of the process-wide
                // buffer budget.
                drop(permit);
            }
        }

        let total = writer.written();
        let prepare_started_at = Instant::now();
        let writer = tokio::task::spawn_blocking(move || writer.prepare_commit(size))
            .await
            .map_err(|err| ObjectStoreError::Get(err.to_string()))??;
        let prepare_commit_ms =
            u64::try_from(prepare_started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let transformed = writer.transformed();
        // Inline on purpose: commit (the atomic rename) must be unreachable
        // by a cancelled attempt's detached blocking closure — see
        // `SequentialWriter::commit`.
        let commit_started_at = Instant::now();
        writer.commit()?;
        let atomic_commit_ms =
            u64::try_from(commit_started_at.elapsed().as_millis()).unwrap_or(u64::MAX);

        info!(
            bucket = target.bucket,
            key = target.key,
            chunk_count,
            size_bytes = size,
            transformed,
            pipeline_wall_ms =
                u64::try_from(pipeline_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            fetch_work_ms,
            slowest_fetch_ms,
            range_join_wait_ms,
            download_budget_wait_ms,
            sequential_writer_ms,
            prepare_commit_ms,
            atomic_commit_ms,
            max_reorder_pending_chunks,
            "ranged download pipeline timing"
        );
        Ok((total, transformed))
    }

    async fn upload_parts(
        &self,
        creds: &ObjectCredentials,
        target: &ObjectTarget<'_>,
        upload_id: &str,
        src: &Path,
        size: u64,
    ) -> Result<Vec<CompletedPart>, ObjectStoreError> {
        let ranges = multipart_part_ranges(size, self.multipart_part_size);
        info!(
            bucket = target.bucket,
            key = target.key,
            part_count = ranges.len(),
            concurrency = self.multipart_concurrency,
            "uploading multipart parts"
        );

        collect_completed_parts(&ranges, self.multipart_concurrency, |range| {
            let client = self.base.clone();
            let bucket = target.bucket.to_string();
            let key = target.key.to_string();
            let upload_id = upload_id.to_string();
            let path = src.to_path_buf();
            let cfg = self.op_config(creds, target, S3Operation::UploadPart);

            async move {
                let content_md5 = file_range_content_md5(&path, range.offset, range.length)
                    .await
                    .map_err(|err| {
                        let error = err.to_string();
                        warn!(
                            key,
                            upload_id,
                            part_number = range.part_number,
                            offset_bytes = range.offset,
                            length_bytes = range.length,
                            error,
                            "failed to digest multipart part body"
                        );
                        ObjectStoreError::Multipart(error)
                    })?;
                let body = ByteStream::read_from()
                    .path(&path)
                    .offset(range.offset)
                    .length(Length::Exact(range.length))
                    .buffer_size(UPLOAD_READ_BUFFER_SIZE)
                    .build()
                    .await
                    .map_err(|err| {
                        let error = DisplayErrorContext(&err).to_string();
                        warn!(
                            key,
                            upload_id,
                            part_number = range.part_number,
                            offset_bytes = range.offset,
                            length_bytes = range.length,
                            error,
                            "failed to build multipart part body"
                        );
                        ObjectStoreError::Multipart(error)
                    })?;

                let resp = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(range.part_number)
                    .content_md5(content_md5)
                    .body(body)
                    .customize()
                    .config_override(cfg)
                    .send()
                    .await
                    .map_err(|err| {
                        observe_s3_error(S3Operation::UploadPart, &bucket, &key, &err);
                        let error = DisplayErrorContext(&err).to_string();
                        warn!(
                            key,
                            upload_id,
                            part_number = range.part_number,
                            offset_bytes = range.offset,
                            length_bytes = range.length,
                            error,
                            "multipart part upload failed"
                        );
                        ObjectStoreError::Multipart(error)
                    })?;

                resp.e_tag()
                    .ok_or(ObjectStoreError::MissingField("e_tag"))
                    .map(str::to_string)
            }
        })
        .await
    }
}

/// Backoff before each retry of a ranged-download chunk whose response body
/// failed mid-read, so a chunk gets up to three attempts. Each is jittered
/// down to half. Short on purpose: the failure is one connection, and the
/// other readers keep going while this one waits.
const CHUNK_RETRY_BACKOFF: [Duration; 2] = [Duration::from_millis(250), Duration::from_secs(1)];

/// One chunk of a ranged single-writer download, owned so the fetch can run
/// on its own task.
struct ChunkFetch {
    client: Client,
    creds: ObjectCredentials,
    bucket: String,
    key: String,
    region: String,
    endpoint: Option<String>,
    path_style: bool,
    etag: String,
    chunk_index: usize,
    range_header: String,
    length: u64,
    timeouts: TimeoutConfig,
    attempts: AttemptObserver,
}

/// Why one chunk attempt failed.
enum ChunkFailure {
    /// The response body failed mid-read: a stall cut, reset, or early EOF.
    Body(ByteStreamError),
    /// Anything else, which a chunk retry cannot fix.
    Fatal(ObjectStoreError),
}

impl ChunkFetch {
    /// Fetches the chunk, retrying a response body that fails mid-read.
    ///
    /// The SDK retries the request, but the body is read after `send()`
    /// returns, outside its retry loop; without this, one stalled or cut
    /// connection fails the whole download and the caller restarts the
    /// entire transfer. Each retry re-issues the ranged GET pinned to the
    /// same ETag, so an object that changed in between still fails with 412
    /// rather than mixing versions.
    ///
    /// Not retried: a failed request, which the SDK has already retried
    /// (a 412 is not retryable there either); a length mismatch, which a
    /// misbehaving endpoint repeats; and cancellation, which drops this
    /// future. The caller's buffer permit stays held across retries, and a
    /// failed attempt's partial buffer is dropped before the next starts, so
    /// a retry never takes more than its one chunk of the buffer budget.
    async fn fetch(&self) -> Result<Bytes, ObjectStoreError> {
        let started_at = Instant::now();
        let mut attempt = 1;
        loop {
            let attempt_started_at = Instant::now();
            let error = match self.fetch_once().await {
                Ok(bytes) => {
                    if attempt > 1 {
                        self.attempts.body_read_recovered(
                            &self.attempt_target(),
                            attempt,
                            started_at.elapsed(),
                        );
                    }
                    return Ok(bytes);
                }
                Err(ChunkFailure::Fatal(error)) => return Err(error),
                Err(ChunkFailure::Body(error)) => error,
            };
            let backoff = CHUNK_RETRY_BACKOFF.get(attempt as usize - 1);
            self.attempts.body_read_failed(
                &self.attempt_target(),
                attempt,
                backoff.is_some(),
                attempt_started_at.elapsed(),
                &error,
            );
            let Some(backoff) = backoff else {
                return Err(ObjectStoreError::RangeBodyRead {
                    range: self.range_header.clone(),
                    message: DisplayErrorContext(&error).to_string(),
                });
            };
            tokio::time::sleep(backoff.mul_f64(rand::random_range(0.5..=1.0))).await;
            attempt += 1;
        }
    }

    async fn fetch_once(&self) -> Result<Bytes, ChunkFailure> {
        let started_at = Instant::now();
        let target = ObjectTarget {
            bucket: &self.bucket,
            key: &self.key,
            region: &self.region,
            endpoint: self.endpoint.as_deref(),
            path_style: self.path_style,
        };
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(&self.range_header)
            .if_match(&self.etag)
            .customize()
            .config_override(S3ObjectStore::op_config_with(
                &self.creds,
                &target,
                self.timeouts.clone(),
            ))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                let error = classify_get_error(err, &self.bucket, &self.key);
                warn!(
                    key = self.key,
                    chunk_index = self.chunk_index,
                    range = self.range_header,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    error = %error,
                    "ranged download chunk failed"
                );
                return Err(ChunkFailure::Fatal(error));
            }
        };

        let length = self.length;
        let mismatch = |got: usize| {
            ChunkFailure::Fatal(ObjectStoreError::RangeLengthMismatch {
                range: self.range_header.clone(),
                got,
                expected: length,
            })
        };
        // An endpoint or proxy that ignores `Range` can return the full
        // object; reading unboundedly would materialize it under a single
        // chunk permit and bypass the buffer budget. Reject an oversized
        // advertised length up front and cap the streamed read at the
        // requested range.
        if let Some(advertised) = resp.content_length()
            && u64::try_from(advertised).ok() != Some(length)
        {
            return Err(mismatch(usize::try_from(advertised).unwrap_or(usize::MAX)));
        }
        let mut body = resp.body;
        let mut buf = bytes::BytesMut::with_capacity(length as usize);
        while let Some(part) = body.try_next().await.map_err(ChunkFailure::Body)? {
            if buf.len() + part.len() > length as usize {
                return Err(mismatch(buf.len() + part.len()));
            }
            buf.extend_from_slice(&part);
        }
        if buf.len() as u64 != length {
            return Err(mismatch(buf.len()));
        }
        Ok(buf.freeze())
    }

    fn attempt_target(&self) -> AttemptTarget {
        AttemptTarget {
            bucket: Some(self.bucket.clone()),
            key: Some(self.key.clone()),
            range: Some(self.range_header.clone()),
            chunk_index: Some(self.chunk_index),
            ..AttemptTarget::default()
        }
    }
}

/// Drives one part operation per range with bounded concurrency, returning the
/// completed parts in part-number order.
///
/// Shared by the multipart upload and multipart server-side copy paths, which
/// differ only in the per-part call. Everything else — bounding how many run at
/// once, keeping results in part order, cancelling the rest on the first
/// failure, and refusing to complete an upload with a hole in it — is identical
/// and was previously written twice.
///
/// `part` is called once per range and must return a `'static` future yielding
/// that part's ETag, so each part owns what it needs and the work can be spawned
/// rather than polled on one task: an upload part reads and digests hundreds of
/// megabytes for its `Content-MD5`, so running parts side by side is
/// load-bearing, not incidental.
async fn collect_completed_parts<F, Fut>(
    ranges: &[MultipartPartRange],
    concurrency: usize,
    mut part: F,
) -> Result<Vec<CompletedPart>, ObjectStoreError>
where
    F: FnMut(MultipartPartRange) -> Fut,
    Fut: std::future::Future<Output = Result<String, ObjectStoreError>> + Send + 'static,
{
    let part_count = ranges.len();
    let concurrency = concurrency.max(1);
    let mut tasks: JoinSet<Result<(usize, i32, String, u128), ObjectStoreError>> = JoinSet::new();
    let mut completed: Vec<Option<CompletedPart>> = vec![None; part_count];
    let mut next_part_index = 0;
    let mut in_flight = 0;

    while next_part_index < part_count || in_flight > 0 {
        while in_flight < concurrency && next_part_index < part_count {
            let part_index = next_part_index;
            next_part_index += 1;
            in_flight += 1;

            let range = ranges
                .get(part_index)
                .copied()
                .expect("next_part_index is bounded by ranges length");
            let started_at = Instant::now();
            let work = part(range);
            tasks.spawn(async move {
                let etag = work.await?;
                Ok((
                    part_index,
                    range.part_number,
                    etag,
                    started_at.elapsed().as_millis(),
                ))
            });
        }

        let Some(joined) = tasks.join_next().await else {
            break;
        };
        in_flight -= 1;
        match joined {
            Ok(Ok((part_index, part_number, etag, elapsed_ms))) => {
                info!(
                    part_number,
                    %etag,
                    elapsed_ms,
                    completed_parts = completed.iter().filter(|part| part.is_some()).count() + 1,
                    part_count,
                    in_flight,
                    "completed multipart part"
                );
                let Some(slot) = completed.get_mut(part_index) else {
                    return Err(ObjectStoreError::Multipart(format!(
                        "multipart task returned invalid part index {part_index}"
                    )));
                };
                *slot = Some(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(etag)
                        .build(),
                );
            }
            Ok(Err(err)) => {
                warn!(error = %err, in_flight, "multipart part failed; aborting remaining parts");
                tasks.abort_all();
                return Err(err);
            }
            Err(join_err) => {
                warn!(error = %join_err, in_flight, "multipart part task join failed; aborting remaining parts");
                tasks.abort_all();
                return Err(ObjectStoreError::Multipart(join_err.to_string()));
            }
        }
    }

    // A hole here would complete an upload missing a part, so the object would
    // exist and be wrong. Refuse instead.
    completed
        .into_iter()
        .map(|part| part.ok_or(ObjectStoreError::MissingField("part")))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MultipartPartRange {
    part_number: i32,
    offset: u64,
    length: u64,
}

fn multipart_part_ranges(size: u64, part_size: usize) -> Vec<MultipartPartRange> {
    if size == 0 {
        return Vec::new();
    }

    let max_part_size = part_size as u64;
    let min_part_size = MULTIPART_PART_SIZE_MIN as u64;
    let part_count = size.div_ceil(max_part_size);
    let max_valid_part_count = size / min_part_size;
    if part_count <= 1 || part_count > max_valid_part_count {
        return vec![MultipartPartRange {
            part_number: 1,
            offset: 0,
            length: size,
        }];
    }

    let base_length = size / part_count;
    let longer_parts = size % part_count;

    let mut ranges = Vec::with_capacity(part_count as usize);
    let mut offset = 0;
    for idx in 0..part_count {
        let length = base_length + u64::from(idx < longer_parts);
        ranges.push(MultipartPartRange {
            part_number: (ranges.len() + 1) as i32,
            offset,
            length,
        });
        offset += length;
    }

    ranges
}

fn byte_range_header(range: MultipartPartRange) -> String {
    debug_assert!(range.length > 0);
    let end = range.offset + range.length - 1;
    format!("bytes={}-{}", range.offset, end)
}

/// Splits `size` into fixed `chunk_size` ranges plus at most one short tail.
///
/// Unlike [`multipart_part_ranges`] this never rebalances: fixed-size
/// chunks keep range math trivial, and a short tail (down to one byte) is
/// fine for a ranged GET — S3's minimum-part rule applies to uploads only.
fn download_chunk_ranges(size: u64, chunk_size: usize) -> Vec<MultipartPartRange> {
    if size == 0 {
        return Vec::new();
    }
    let chunk = chunk_size as u64;
    let count = size.div_ceil(chunk);
    let mut ranges = Vec::with_capacity(count as usize);
    let mut offset = 0;
    while offset < size {
        let length = chunk.min(size - offset);
        ranges.push(MultipartPartRange {
            part_number: (ranges.len() + 1) as i32,
            offset,
            length,
        });
        offset += length;
    }
    ranges
}

/// Decides whether a download of `size` bytes should fan out over ranged
/// readers. `None` keeps the caller's already-open streaming GET, which is
/// itself a sequential single writer — the right shape for small objects and
/// for `reader_concurrency == 1`, where chunking would only add per-request
/// overhead.
fn single_writer_download_ranges(
    size: u64,
    chunk_size: usize,
    reader_concurrency: usize,
) -> Option<Vec<MultipartPartRange>> {
    if reader_concurrency <= 1 {
        return None;
    }
    let ranges = download_chunk_ranges(size, chunk_size);
    if ranges.len() > 1 { Some(ranges) } else { None }
}

fn get_object_content_length(
    key: &str,
    content_length: Option<i64>,
) -> Result<u64, ObjectStoreError> {
    let size = content_length.ok_or(ObjectStoreError::MissingField("content_length"))?;
    u64::try_from(size).map_err(|_| {
        ObjectStoreError::Get(format!(
            "object {key} returned negative content-length {size}"
        ))
    })
}

/// `on_body_error` sees a failed read of `body` before it becomes the
/// returned error.
async fn write_body_to_file(
    writer: &mut tokio::fs::File,
    body: &mut ByteStream,
    expected_length: u64,
    description: &str,
    on_body_error: impl FnOnce(&ByteStreamError),
) -> Result<u64, ObjectStoreError> {
    let mut written: u64 = 0;
    let mut on_body_error = Some(on_body_error);
    while let Some(chunk) = body.try_next().await.map_err(|err| {
        if let Some(report) = on_body_error.take() {
            report(&err);
        }
        ObjectStoreError::Get(DisplayErrorContext(&err).to_string())
    })? {
        let chunk_len = chunk.len() as u64;
        let next_written = written.checked_add(chunk_len).ok_or_else(|| {
            ObjectStoreError::Get(format!(
                "{description} byte count overflowed while reading response body"
            ))
        })?;
        if next_written > expected_length {
            return Err(ObjectStoreError::Get(format!(
                "{description} returned more bytes than expected: at least {next_written}, expected {expected_length}"
            )));
        }
        writer.write_all(&chunk).await?;
        written = next_written;
    }
    writer.flush().await?;

    if written != expected_length {
        return Err(ObjectStoreError::Get(format!(
            "{description} returned {written} bytes, expected {expected_length}"
        )));
    }

    Ok(written)
}

/// Maps an `aws_sdk_s3::get_object` `SdkError` into `ObjectStoreError`,
/// distinguishing the `NoSuchKey` service variant from every other
/// failure. Callers that pull from a content-addressed bucket use the
/// `NotFound` variant to fall through to a peer source instead of
/// surfacing the error.
fn classify_get_error(
    err: S3SdkError<GetObjectError>,
    bucket: &str,
    key: &str,
) -> ObjectStoreError {
    observe_s3_error(S3Operation::GetObject, bucket, key, &err);
    if let SdkError::ServiceError(svc) = &err
        && matches!(svc.err(), GetObjectError::NoSuchKey(_))
    {
        return ObjectStoreError::NotFound {
            key: key.to_string(),
        };
    }
    if let Some(error) = classify_terminal_s3_error(&err, key) {
        return error;
    }
    ObjectStoreError::Get(DisplayErrorContext(&err).to_string())
}

fn classify_terminal_s3_error<E>(err: &S3SdkError<E>, key: &str) -> Option<ObjectStoreError>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let code = err
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)?;
    match code {
        "NoSuchBucket" | "NoSuchKey" => Some(ObjectStoreError::NotFound {
            key: key.to_string(),
        }),
        "AccessDenied" | "InvalidAccessKeyId" | "InvalidClientTokenId" => {
            Some(ObjectStoreError::PermissionDenied {
                message: DisplayErrorContext(err).to_string(),
            })
        }
        _ => None,
    }
}

/// Digests `length` bytes of `path` at `offset` on the blocking pool; see
/// [`content_md5`] for why every file upload carries one.
async fn file_range_content_md5(path: &Path, offset: u64, length: u64) -> std::io::Result<String> {
    let path = path.to_path_buf();
    // Lives until the digest is awaited; if this future is dropped first (a
    // sibling part failed and `abort_all` ran), the read stops at its next
    // chunk instead of finishing a range nobody will upload.
    let cancel = content_md5::CancelOnDrop::default();
    let cancelled = cancel.flag();
    tokio::task::spawn_blocking(move || {
        content_md5::of_file_range(&path, offset, length, &cancelled)
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Calls `AbortMultipartUpload` if dropped before [`disarm`](AbortGuard::disarm).
///
/// Holds owned copies of the addressing tuple and credentials so the abort
/// can run from `Drop` without touching the original borrows.
struct AbortGuard {
    client: Client,
    timeouts: TimeoutConfig,
    creds: ObjectCredentials,
    bucket: String,
    key: String,
    region: String,
    endpoint: Option<String>,
    path_style: bool,
    upload_id: String,
    armed: bool,
}

impl AbortGuard {
    fn new(
        store: &S3ObjectStore,
        creds: &ObjectCredentials,
        target: &ObjectTarget<'_>,
        upload_id: &str,
    ) -> Self {
        Self {
            client: store.base.clone(),
            timeouts: store
                .timeouts
                .for_operation(S3Operation::AbortMultipartUpload),
            creds: creds.clone(),
            bucket: target.bucket.to_string(),
            key: target.key.to_string(),
            region: target.region.to_string(),
            endpoint: target.endpoint.map(str::to_string),
            path_style: target.path_style,
            upload_id: upload_id.to_string(),
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }

    /// Await cleanup on ordinary failure. Drop still provides a best-effort
    /// fallback if this attempt itself is cancelled.
    async fn abort(self) {
        let target = ObjectTarget {
            bucket: &self.bucket,
            key: &self.key,
            region: &self.region,
            endpoint: self.endpoint.as_deref(),
            path_style: self.path_style,
        };
        let cfg = S3ObjectStore::op_config_with(&self.creds, &target, self.timeouts.clone());
        if let Err(err) = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .customize()
            .config_override(cfg)
            .send()
            .await
        {
            observe_s3_error(
                S3Operation::AbortMultipartUpload,
                &self.bucket,
                &self.key,
                &err,
            );
            warn!(error = %err, key = self.key, "failed to abort multipart upload");
        }
        self.disarm();
    }
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let client = self.client.clone();
        let timeouts = self.timeouts.clone();
        let creds = self.creds.clone();
        let bucket = std::mem::take(&mut self.bucket);
        let key = std::mem::take(&mut self.key);
        let region = std::mem::take(&mut self.region);
        let endpoint = self.endpoint.take();
        let path_style = self.path_style;
        let upload_id = std::mem::take(&mut self.upload_id);
        // Best-effort abort; runs only if a tokio runtime is available.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let target = ObjectTarget {
                    bucket: &bucket,
                    key: &key,
                    region: &region,
                    endpoint: endpoint.as_deref(),
                    path_style,
                };
                let cfg = S3ObjectStore::op_config_with(&creds, &target, timeouts);
                if let Err(err) = client
                    .abort_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .customize()
                    .config_override(cfg)
                    .send()
                    .await
                {
                    observe_s3_error(S3Operation::AbortMultipartUpload, &bucket, &key, &err);
                    warn!(error = %err, key, "failed to abort multipart upload");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests;
