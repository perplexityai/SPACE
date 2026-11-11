//! File-upload PUT handler.
//!
//! The e2b SDK calls `GET /templates/{templateID}/files/{filesHash}` and
//! expects a JSON `{ present, url }`. When `present` is false the SDK
//! follows up with `PUT <url>` carrying the tar bytes. We return an
//! absolute URL under the E2B adapter root so standalone upload clients land
//! back on this handler even when the API client's base URL includes
//! `/e2b/api`. The URL carries a short-lived path-bound capability because
//! current SDK upload clients do not inherit the API client's auth headers.
//!
//! On PUT we stream the request body in `BLOB_UPLOAD_CHUNK_BYTES` chunks
//! through the native template controller to the builder daemon, then persist
//! the daemon's returned sha256 and authenticated owner in `TemplateBuildState`
//! for `(templateID, filesHash)`. Subsequent `COPY` steps with the same
//! `filesHash` in a `POST /v2/.../builds/...` request get rewritten to
//! `Recipe.Step.Copy { blob_sha256 }`.

use std::{
    io::{self, Cursor},
    pin::Pin,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Extension, Json,
    body::Body,
    extract::{OriginalUri, Path as AxumPath, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::TryStreamExt as _;
use hmac::{Hmac, KeyInit, Mac};
use pplx_space_build_mgr_client::{
    BLOB_UPLOAD_CHUNK_BYTES, BLOB_UPLOAD_LIMIT_BYTES, BlobUploadSender,
};
use pplx_space_common_core::types::{OrgId, TemplateName};
use serde::Serialize;
use sha2::Sha256;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::super::super::AppState;
use super::state::RecordBlobError;
use crate::{e2b::errors::E2bApiError, middleware::auth::AuthOrg};

/// gzip magic prefix; the e2b SDK uploads tars with `mode="w:gz"`.
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
const FILE_UPLOAD_OWNER_QUERY: &str = "upload_owner";
const FILE_UPLOAD_TOKEN_PREFIX: &str = "e2bu2_";
const FILE_UPLOAD_TOKEN_TTL_SECS: u64 = 10 * 60;
const FILE_UPLOAD_TOKEN_CLOCK_SKEW_SECS: u64 = 60;
const FILE_UPLOAD_TOKEN_PURPOSE: &[u8] = b"e2b-template-file-upload-v2\0";
const PUBLIC_SIGNING_SECRET_ENV: &str = "SPACE_PUBLIC_CURSOR_SECRET";
const MAX_CONCURRENT_GZIP_DECODERS: usize = 2;
const FILE_UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const FILE_UPLOAD_DEADLINE: Duration = Duration::from_secs(15 * 60);
const GZIP_DECODER_RETRY_AFTER: Duration = Duration::from_secs(1);

static GZIP_DECODER_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_GZIP_DECODERS)));

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, thiserror::Error)]
enum UploadBodyError {
    #[error("read upload body: {0}")]
    Read(#[source] io::Error),
    #[error("upload body is empty")]
    Empty,
    #[error("decompressed body exceeds {max_bytes}-byte cap")]
    TooLarge { max_bytes: u64 },
    #[error("upload reader returned {read} bytes for a {capacity}-byte buffer")]
    ReadOverflow { read: usize, capacity: usize },
    #[error("builder stopped reading upload body")]
    ReceiverClosed,
    #[error("gzip decode task failed: {0}")]
    DecodeTask(#[source] tokio::task::JoinError),
    #[error("gzip decoder capacity is exhausted")]
    DecoderCapacity,
    #[error("upload body was idle for {seconds} seconds")]
    IdleTimeout { seconds: u64 },
    #[error("upload exceeded its {seconds}-second deadline")]
    DeadlineExceeded { seconds: u64 },
    #[error("write compressed upload to gzip decoder: {0}")]
    DecoderInput(#[source] io::Error),
}

#[derive(Clone)]
struct UploadLimits {
    gzip_decoder_slots: Arc<Semaphore>,
    idle_timeout: Duration,
    deadline: Duration,
}

impl UploadLimits {
    fn production() -> Self {
        Self {
            gzip_decoder_slots: Arc::clone(&GZIP_DECODER_SLOTS),
            idle_timeout: FILE_UPLOAD_IDLE_TIMEOUT,
            deadline: FILE_UPLOAD_DEADLINE,
        }
    }
}

async fn read_upload_body<R>(
    mut reader: Pin<&mut R>,
    buffer: &mut [u8],
    idle_timeout: Duration,
) -> Result<usize, UploadBodyError>
where
    R: AsyncRead + ?Sized,
{
    timeout(idle_timeout, reader.as_mut().read(buffer))
        .await
        .map_err(|_| UploadBodyError::IdleTimeout {
            seconds: idle_timeout.as_secs(),
        })?
        .map_err(UploadBodyError::Read)
}

async fn forward_upload_reader(
    mut reader: Pin<Box<dyn AsyncRead + Send>>,
    upload: &BlobUploadSender,
    max_bytes: u64,
    idle_timeout: Duration,
) -> Result<u64, UploadBodyError> {
    let mut buffer = vec![0; BLOB_UPLOAD_CHUNK_BYTES];
    let mut size_bytes = 0_u64;
    loop {
        let read = read_upload_body(reader.as_mut(), &mut buffer, idle_timeout).await?;
        if read == 0 {
            break;
        }
        size_bytes = checked_upload_size(size_bytes, read, max_bytes)?;
        upload
            .send_data(upload_chunk(&buffer, read)?)
            .await
            .map_err(|_| UploadBodyError::ReceiverClosed)?;
    }
    Ok(size_bytes)
}

async fn feed_gzip_decoder<R>(
    reader: R,
    mut decoder_input: tokio::io::DuplexStream,
    idle_timeout: Duration,
) -> Result<(), UploadBodyError>
where
    R: AsyncRead + Send + 'static,
{
    let mut reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(reader);
    let mut buffer = vec![0; BLOB_UPLOAD_CHUNK_BYTES];
    loop {
        let read = read_upload_body(reader.as_mut(), &mut buffer, idle_timeout).await?;
        if read == 0 {
            break;
        }
        decoder_input
            .write_all(upload_bytes(&buffer, read)?)
            .await
            .map_err(UploadBodyError::DecoderInput)?;
    }
    decoder_input
        .shutdown()
        .await
        .map_err(UploadBodyError::DecoderInput)
}

async fn forward_decoded_upload(
    mut receiver: mpsc::Receiver<Vec<u8>>,
    decode: tokio::task::JoinHandle<Result<u64, UploadBodyError>>,
    upload: &BlobUploadSender,
) -> Result<u64, UploadBodyError> {
    while let Some(data) = receiver.recv().await {
        upload
            .send_data(data)
            .await
            .map_err(|_| UploadBodyError::ReceiverClosed)?;
    }
    decode.await.map_err(UploadBodyError::DecodeTask)?
}

async fn forward_gzip_upload<R>(
    reader: R,
    upload: &BlobUploadSender,
    max_bytes: u64,
    limits: &UploadLimits,
) -> Result<u64, UploadBodyError>
where
    R: AsyncRead + Send + 'static,
{
    let permit = Arc::clone(&limits.gzip_decoder_slots)
        .try_acquire_owned()
        .map_err(|_| UploadBodyError::DecoderCapacity)?;
    let (decoder_input, decoder_reader) = tokio::io::duplex(BLOB_UPLOAD_CHUNK_BYTES);
    let (sender, receiver) = mpsc::channel(2);
    let bridge = SyncIoBridge::new(decoder_reader);
    let decode = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut decoder = flate2::read::GzDecoder::new(bridge);
        let mut buffer = vec![0; BLOB_UPLOAD_CHUNK_BYTES];
        let mut size_bytes = 0_u64;
        loop {
            let read =
                std::io::Read::read(&mut decoder, &mut buffer).map_err(UploadBodyError::Read)?;
            if read == 0 {
                break;
            }
            size_bytes = checked_upload_size(size_bytes, read, max_bytes)?;
            sender
                .blocking_send(upload_chunk(&buffer, read)?)
                .map_err(|_| UploadBodyError::ReceiverClosed)?;
        }
        Ok(size_bytes)
    });
    let feed = feed_gzip_decoder(reader, decoder_input, limits.idle_timeout);
    let forward = forward_decoded_upload(receiver, decode, upload);
    tokio::pin!(feed, forward);

    tokio::select! {
        feed_result = feed.as_mut() => match feed_result {
            Ok(()) => forward.await,
            Err(error) => Err(error),
        },
        decode_result = forward.as_mut() => decode_result,
    }
}

fn checked_upload_size(
    size_bytes: u64,
    read: usize,
    max_bytes: u64,
) -> Result<u64, UploadBodyError> {
    size_bytes
        .checked_add(read as u64)
        .filter(|size| *size <= max_bytes)
        .ok_or(UploadBodyError::TooLarge { max_bytes })
}

fn upload_chunk(buffer: &[u8], read: usize) -> Result<Vec<u8>, UploadBodyError> {
    upload_bytes(buffer, read).map(<[u8]>::to_vec)
}

fn upload_bytes(buffer: &[u8], read: usize) -> Result<&[u8], UploadBodyError> {
    buffer.get(..read).ok_or(UploadBodyError::ReadOverflow {
        read,
        capacity: buffer.len(),
    })
}

async fn stream_upload_body(
    body: Body,
    sender: mpsc::Sender<pplx_space_build_mgr_client::proto::UploadBlobChunk>,
    max_bytes: u64,
) -> Result<u64, UploadBodyError> {
    stream_upload_body_with_limits(body, sender, max_bytes, UploadLimits::production()).await
}

async fn stream_upload_body_with_limits(
    body: Body,
    sender: mpsc::Sender<pplx_space_build_mgr_client::proto::UploadBlobChunk>,
    max_bytes: u64,
    limits: UploadLimits,
) -> Result<u64, UploadBodyError> {
    let deadline = limits.deadline;
    timeout(
        deadline,
        stream_upload_body_inner(body, sender, max_bytes, &limits),
    )
    .await
    .map_err(|_| UploadBodyError::DeadlineExceeded {
        seconds: deadline.as_secs(),
    })?
}

async fn stream_upload_body_inner(
    body: Body,
    sender: mpsc::Sender<pplx_space_build_mgr_client::proto::UploadBlobChunk>,
    max_bytes: u64,
    limits: &UploadLimits,
) -> Result<u64, UploadBodyError> {
    let upload = BlobUploadSender::begin(sender)
        .await
        .map_err(|_| UploadBodyError::ReceiverClosed)?;
    let stream = body
        .into_data_stream()
        .map_err(|error| io::Error::other(error.to_string()));
    let mut reader = BufReader::new(StreamReader::new(stream));
    let mut prefix = Vec::with_capacity(GZIP_MAGIC.len());
    while prefix.len() < GZIP_MAGIC.len() {
        let mut buffer = [0; GZIP_MAGIC.len()];
        let remaining = GZIP_MAGIC.len() - prefix.len();
        let capacity = buffer.len();
        let destination = buffer
            .get_mut(..remaining)
            .ok_or(UploadBodyError::ReadOverflow {
                read: remaining,
                capacity,
            })?;
        let read =
            read_upload_body(Pin::new(&mut reader), destination, limits.idle_timeout).await?;
        if read == 0 {
            break;
        }
        let bytes = buffer
            .get(..read)
            .ok_or(UploadBodyError::ReadOverflow { read, capacity })?;
        prefix.extend_from_slice(bytes);
    }
    let is_gzip = prefix == GZIP_MAGIC;
    let reader = Cursor::new(prefix).chain(reader);
    let size_bytes = if is_gzip {
        forward_gzip_upload(reader, &upload, max_bytes, limits).await?
    } else {
        forward_upload_reader(Box::pin(reader), &upload, max_bytes, limits.idle_timeout).await?
    };
    if size_bytes == 0 {
        return Err(UploadBodyError::Empty);
    }
    upload
        .commit()
        .await
        .map_err(|_| UploadBodyError::ReceiverClosed)?;
    Ok(size_bytes)
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub sha256: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
}

pub(in crate::routes::e2b) fn mint_file_upload_query(
    org_id: OrgId,
    path: &str,
) -> Result<String, E2bApiError> {
    let now = unix_timestamp()?;
    let expires_at = now.saturating_add(FILE_UPLOAD_TOKEN_TTL_SECS);
    let secret = file_upload_signing_secret()?;
    let owner = mint_file_upload_capability_at(org_id, path, expires_at, secret.as_bytes())?;
    Ok(format!("{FILE_UPLOAD_OWNER_QUERY}={owner}"))
}

pub(in crate::routes::e2b) fn file_upload_capability_org(uri: &Uri) -> Option<OrgId> {
    let token = uri
        .query()
        .and_then(|query| query_parameter(query, FILE_UPLOAD_OWNER_QUERY))?;
    let now = unix_timestamp().ok()?;
    let secret = file_upload_signing_secret().ok()?;
    verify_file_upload_capability_at(uri.path(), token, now, secret.as_bytes())
}

fn mint_file_upload_capability_at(
    org_id: OrgId,
    path: &str,
    expires_at: u64,
    secret: &[u8],
) -> Result<String, E2bApiError> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| E2bApiError::internal("invalid file upload signing key"))?;
    update_file_upload_mac(&mut mac, org_id, path, expires_at);
    Ok(format!(
        "{FILE_UPLOAD_TOKEN_PREFIX}{org_id}_{expires_at}_{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()),
    ))
}

fn verify_file_upload_capability_at(
    path: &str,
    token: &str,
    now: u64,
    secret: &[u8],
) -> Option<OrgId> {
    let token = token.strip_prefix(FILE_UPLOAD_TOKEN_PREFIX)?;
    let (org_id, token) = token.split_once('_')?;
    let org_id = org_id.parse::<OrgId>().ok()?;
    let (expires_at, signature) = token.split_once('_')?;
    let expires_at = expires_at.parse::<u64>().ok()?;
    if expires_at < now
        || expires_at
            > now
                .saturating_add(FILE_UPLOAD_TOKEN_TTL_SECS)
                .saturating_add(FILE_UPLOAD_TOKEN_CLOCK_SKEW_SECS)
    {
        return None;
    }
    let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
    let mut mac = HmacSha256::new_from_slice(secret).ok()?;
    update_file_upload_mac(&mut mac, org_id, path, expires_at);
    mac.verify_slice(&signature).ok()?;
    Some(org_id)
}

fn update_file_upload_mac(mac: &mut HmacSha256, org_id: OrgId, path: &str, expires_at: u64) {
    mac.update(FILE_UPLOAD_TOKEN_PURPOSE);
    mac.update(org_id.as_uuid().as_bytes());
    mac.update(path.as_bytes());
    mac.update(b"\0");
    mac.update(&expires_at.to_be_bytes());
}

fn query_parameter<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        (name == key && !value.is_empty()).then_some(value)
    })
}

pub(in crate::routes::e2b) fn file_upload_signing_secret() -> Result<String, E2bApiError> {
    #[cfg(test)]
    const TEST_SECRET: &str = "space-api-unit-test-public-cursor-secret";

    let secret = std::env::var(PUBLIC_SIGNING_SECRET_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    #[cfg(test)]
    let secret = secret.or_else(|| Some(TEST_SECRET.to_string()));

    secret.ok_or_else(|| {
        E2bApiError::internal(format!(
            "{PUBLIC_SIGNING_SECRET_ENV} is not configured for file upload signing",
        ))
    })
}

fn unix_timestamp() -> Result<u64, E2bApiError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| E2bApiError::internal("system clock is before the Unix epoch"))
}

fn upload_body_error(error: UploadBodyError) -> E2bApiError {
    match error {
        error @ UploadBodyError::DecoderCapacity => {
            E2bApiError::new(StatusCode::SERVICE_UNAVAILABLE, error.to_string())
                .with_retry_after(GZIP_DECODER_RETRY_AFTER)
        }
        error
        @ (UploadBodyError::IdleTimeout { .. } | UploadBodyError::DeadlineExceeded { .. }) => {
            E2bApiError::new(StatusCode::REQUEST_TIMEOUT, error.to_string())
        }
        error => E2bApiError::bad_request(error.to_string()),
    }
}

/// `PUT /e2b/api/v1/internal/e2b-files/{templateID}/{filesHash}`
///
/// The SDK lands here after we hand it a URL from
/// `GET /templates/{templateID}/files/{hash}`. Stream the body to the
/// daemon, and record the returned digest with the verified upload owner.
pub async fn put_uploaded_file(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    org: Option<Extension<AuthOrg>>,
    AxumPath((template_id, files_hash)): AxumPath<(String, String)>,
    body: Body,
) -> Result<impl IntoResponse, E2bApiError> {
    // A signed capability owns the upload even if a client also supplies an
    // API key. Every admitted upload has an owner; the former path-only
    // capability let any holder write build inputs for any template.
    let Some(org_id) =
        file_upload_capability_org(&uri).or_else(|| org.map(|Extension(AuthOrg(org))| org.id))
    else {
        return Err(E2bApiError::unauthorized());
    };
    // Validate the templateID URL segment before keying it into persisted
    // state — the same value is later passed into buildmgr,
    // which formats it into a filesystem path.
    let template_id = TemplateName::parse(template_id)
        .map_err(|err| E2bApiError::bad_request(format!("invalid templateID: {err}")))?;
    let (sender, receiver) = mpsc::channel(2);
    let forward_body = stream_upload_body(body, sender, BLOB_UPLOAD_LIMIT_BYTES);
    let upload_blob = state
        .template_controller
        .upload_template_build_blob(ReceiverStream::new(receiver));
    let (body_result, upload_result) = tokio::join!(forward_body, upload_blob);
    let (size_bytes, resp) = match (body_result, upload_result) {
        (Ok(size_bytes), Ok(resp)) => (size_bytes, resp),
        (Err(UploadBodyError::ReceiverClosed), Err(error)) | (Ok(_), Err(error)) => {
            return Err(E2bApiError::internal(format!(
                "BuilderApi/UploadBuildBlob rpc: {error}"
            )));
        }
        (Err(error), _) => return Err(upload_body_error(error)),
    };

    state
        .template_build_state
        .record_blob(org_id, &template_id, &files_hash, &resp.sha256)
        .await
        .map_err(|err| match err {
            RecordBlobError::Conflict => E2bApiError::new(StatusCode::CONFLICT, err.to_string()),
            RecordBlobError::Storage(error) => {
                super::template_build_sql_error("record template build blob", &error)
            }
        })?;

    Ok((
        StatusCode::OK,
        Json(UploadResponse {
            sha256: resp.sha256,
            size_bytes,
        }),
    ))
}

#[cfg(test)]
mod tests;
