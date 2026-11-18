//! Native `files:*` on the data plane.
//!
//! The data plane carries a request's bytes and the lifecycle API decides it.
//! Once the body is checked, [`access`] asks the API's activation listener to
//! prepare the sandbox call: it authorizes the request, then runs the
//! idempotency binding, drain gate, admission charge and wake. The call
//! itself then goes straight to the sandbox's runtime ([`call`]). Field
//! checks, worker requests and the mapping of a failed call are
//! `pplx-space-native-files`'s, which the API's own handlers use too, so a
//! request gets the same answer from either server. The API keeps those
//! handlers, and serves whatever reaches it.
//!
//! A request whose body is not small enough to check first is authorized
//! from its headers before its body is read, in an RPC of its own: every
//! write, and any request whose body size is not known or is more than
//! [`CHECKED_BEFORE_AUTHORIZATION_MAX_BYTES`]. A small body is checked first
//! so that a request the API admits costs it one RPC, not two. One that
//! fails its checks is authorized before it is answered, so the API's
//! refusal still comes first, as it does on the API's own route. A small
//! body that has not arrived within [`SMALL_BODY_DEADLINE`] is authorized
//! from its headers before the rest of it is waited for, so a client that
//! stalls it is refused as promptly as one that sends a large body.

mod access;
mod admission;
mod call;
mod mutations;
mod read;
mod write;

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path},
    http::{
        Extensions, HeaderMap, HeaderValue, Method, Request, StatusCode, Version, header,
        request::Parts,
    },
    middleware,
    response::{IntoResponse, Response},
    routing::any,
};
use futures_util::StreamExt as _;
use http_body_util::BodyExt as _;
use pplx_space_accounts::OrgId;
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_native_files::FileOperation;
use pplx_space_native_files::limits::{
    WRITE_FILE_BATCH_BODY_LIMIT_BYTES, WRITE_FILE_BODY_LIMIT_BYTES,
};
use pplx_space_public_api::PublicSandboxId;
use pplx_space_public_api_http::metrics::{ResponseKind, ResponseOrigin};
use pplx_space_public_api_http::{PublicApiError, normalize_public_error};
use pplx_space_sandbox_model::SandboxId;
use serde::de::DeserializeOwned;
use tokio::time::Instant;

pub(crate) use access::NativeFileAccessClient;
pub use access::NativeFileAccessConfigError;
pub use admission::{NativeUploadAdmissionArgs, NativeUploadAdmissionConfig};
use call::Grant;

use super::DataPlaneState;
use crate::upload_admission::UploadAdmission;

const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
/// The largest known body size a request is checked with before the API has
/// authorized it. Every body but a write's is a path or two and a few
/// fields, well under this; an unauthenticated client can make a replica
/// hold no more than this per request, less than the headers it already
/// sends the API to be authorized.
const CHECKED_BEFORE_AUTHORIZATION_MAX_BYTES: u64 = 16 * 1024;
/// How long a body checked before authorization has to arrive, from when its
/// request is received. A client sends a body this small with its headers,
/// so one that has not is slow or stalled; it is authorized from its headers
/// before more is waited for, which costs an admitted request one more RPC.
const SMALL_BODY_DEADLINE: Duration = Duration::from_millis(100);

/// Whether this data plane answers Native `files:*`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum NativeFilesMode {
    /// The routes are not mounted.
    #[default]
    Off,
    /// The routes are mounted, under `/v1` and `/api/v1` as on the API.
    Serve,
}

/// What the files routes need beyond the rest of the data plane.
pub(crate) struct NativeFiles {
    access: NativeFileAccessClient,
    /// Admission for writes ([`admission`]), apart from ECW's uploads.
    /// `None` admits every write.
    uploads: Option<UploadAdmission>,
}

impl NativeFiles {
    pub(crate) fn new(
        access: NativeFileAccessClient,
        upload_admission: Option<&NativeUploadAdmissionConfig>,
    ) -> Self {
        Self {
            access,
            uploads: upload_admission.map(|config| UploadAdmission::new(&config.budgets())),
        }
    }
}

/// The request body limit `operation`'s route has on the API.
fn body_limit(operation: FileOperation) -> usize {
    match operation {
        FileOperation::Write => WRITE_FILE_BODY_LIMIT_BYTES,
        FileOperation::WriteBatch => WRITE_FILE_BATCH_BODY_LIMIT_BYTES,
        // The framework's default.
        _ => 2 * 1024 * 1024,
    }
}

/// The files routes, at the paths the API mounts them on.
pub(super) fn router() -> Router<DataPlaneState> {
    let mut router = Router::new();
    for prefix in ["/v1", "/api/v1"] {
        for operation in FileOperation::ALL {
            let handler = handler(operation);
            let path = format!("{prefix}{}", operation.route().relative_runtime_path());
            router = router.route(
                &path,
                handler.layer(DefaultBodyLimit::max(body_limit(operation))),
            );
        }
    }
    router.layer(middleware::from_fn(
        pplx_space_public_api_http::with_public_request_id,
    ))
}

fn handler(operation: FileOperation) -> axum::routing::MethodRouter<DataPlaneState> {
    // Every method reaches the handler: the API answers a wrong method only
    // after it has authorized the request, so the 405 waits until then too.
    match operation {
        FileOperation::Stat => any(read::stat_file),
        FileOperation::List => any(read::list_files),
        FileOperation::Read => any(read::read_file),
        FileOperation::Mkdir => any(mutations::mkdir_file),
        FileOperation::Remove => any(mutations::remove_file),
        FileOperation::Rename => any(mutations::rename_file),
        FileOperation::ChangesEnable => any(mutations::enable_file_change_tracking),
        FileOperation::ChangesDisable => any(mutations::disable_file_change_tracking),
        FileOperation::Modified => any(mutations::modified_files),
        FileOperation::Write => any(write::write_file),
        FileOperation::WriteBatch => any(write::write_file_batch),
    }
}

/// A files request, apart from its body.
pub(super) struct FileRequest {
    files: Arc<NativeFiles>,
    /// What the API is asked about the request.
    access: pplx_space_node_api_client::proto::NativeFileRequest,
    pub(super) headers: HeaderMap,
    /// The route's, such as its body limit, for reading the body.
    extensions: Extensions,
    pub(super) version: Version,
    /// Whether the API has authorized the request from its headers already.
    /// One it has not is authorized when it is prepared, after its body is
    /// checked.
    authorized: bool,
}

/// Who a request the API authorized from its headers acts for.
pub(super) struct Authorized {
    /// The sandbox the path names, as the API parsed it.
    pub(super) sandbox_id: SandboxId,
    /// The org the request acts for.
    pub(super) org_id: OrgId,
}

impl FileRequest {
    /// Take `request` as `operation` without asking the API yet when its
    /// body is small enough to check first and arrives within
    /// [`SMALL_BODY_DEADLINE`], or else have the API authorize it from its
    /// headers first, and answer its refusal if it refuses. Pass the outcome
    /// of the body's checks to [`Self::checked`]. The body is handed back
    /// whole, with any of it read so far.
    pub(super) async fn receive(
        state: &DataPlaneState,
        operation: FileOperation,
        request: Request<Body>,
    ) -> Result<(Self, Body), Refusal> {
        if !checks_before_authorization(&request) {
            let (request, _, body) = Self::authorize(state, operation, request).await?;
            return Ok((request, body));
        }
        let deadline = Instant::now() + SMALL_BODY_DEADLINE;
        let (files, parts, body) = accept(state, request)?;
        let access = access::file_request(operation, &parts);
        let arrived = Arrived::read(body, deadline).await;
        // One whose client is slow to send it is refused, if the API would
        // refuse it, before the rest is waited for. Preparing it still waits
        // until all of it is checked.
        let authorized = arrived.rest.is_some();
        if authorized {
            files.access.authorize(access.clone()).await?;
        }
        Ok((
            Self::from_parts(files, access, parts, authorized),
            arrived.into_body(),
        ))
    }

    /// Have the API authorize `request` as `operation` from its headers
    /// alone, and answer its refusal if it refuses. The body is handed back
    /// unread.
    pub(super) async fn authorize(
        state: &DataPlaneState,
        operation: FileOperation,
        request: Request<Body>,
    ) -> Result<(Self, Authorized, Body), Refusal> {
        let (files, mut parts, body) = accept(state, request)?;
        let access = access::file_request(operation, &parts);
        let authorization = files.access.authorize(access.clone()).await?;
        if parts.method != Method::POST {
            return Err(method_not_allowed().into());
        }
        // The API parsed the same segment and org, so failing to here is a
        // server fault, not the client's.
        let sandbox_id = Path::<String>::from_request_parts(&mut parts, &())
            .await
            .ok()
            .and_then(|Path(segment)| segment.parse::<PublicSandboxId>().ok())
            .ok_or_else(PublicApiError::internal)?;
        let org_id = authorization
            .org_id
            .parse()
            .map_err(|_| PublicApiError::internal())?;
        let authorized = Authorized {
            sandbox_id: sandbox_id.into(),
            org_id,
        };
        Ok((
            Self::from_parts(files, access, parts, true),
            authorized,
            body,
        ))
    }

    fn from_parts(
        files: Arc<NativeFiles>,
        access: pplx_space_node_api_client::proto::NativeFileRequest,
        parts: Parts,
        authorized: bool,
    ) -> Self {
        Self {
            files,
            access,
            headers: parts.headers,
            extensions: parts.extensions,
            version: parts.version,
            authorized,
        }
    }

    /// `checks`, the outcome of the body's checks, unless the API refuses
    /// the request: a request whose body failed them before the API
    /// authorized it is authorized first, so that it is answered as the
    /// API's route answers it.
    pub(super) async fn checked<T>(&self, checks: Result<T, Refusal>) -> Result<T, Refusal> {
        let refusal = match checks {
            Ok(checked) => return Ok(checked),
            Err(refusal) => refusal,
        };
        if !self.authorized {
            self.files.access.authorize(self.access.clone()).await?;
        }
        Err(refusal)
    }

    /// The request again, around `body`.
    fn with_body(&self, body: Body) -> Request<Body> {
        let mut request = Request::new(body);
        *request.headers_mut() = self.headers.clone();
        *request.extensions_mut() = self.extensions.clone();
        *request.version_mut() = self.version;
        request
    }

    /// Have the API prepare the sandbox call: authorize the request and do
    /// everything its handler does before calling the sandbox, with
    /// `fingerprint` the body's for an operation whose key is bound.
    pub(super) async fn prepare(
        &self,
        fingerprint: Option<&pplx_space_public_api::RequestFingerprint>,
    ) -> Result<Grant, Refusal> {
        self.files
            .access
            .prepare(self.access.clone(), fingerprint)
            .await
    }
}

/// `request`'s parts and body, unless this replica takes no files request
/// now.
fn accept(
    state: &DataPlaneState,
    request: Request<Body>,
) -> Result<(Arc<NativeFiles>, Parts, Body), Refusal> {
    // A replica that is leaving takes no new request; the retry reaches one
    // that is not, as a refused stream's does.
    if state.drain_started.is_cancelled() {
        return Err(draining().into());
    }
    let Some(files) = state.native_files.clone() else {
        // The routes are mounted only with a client to the API.
        return Err(PublicApiError::internal().into());
    };
    let (parts, body) = request.into_parts();
    Ok((files, parts, body))
}

/// Whether `request`'s body is checked before the API authorizes it: a POST
/// whose body size is known and at most
/// [`CHECKED_BEFORE_AUTHORIZATION_MAX_BYTES`], and that does not wait for
/// `100 Continue` to send it. Any other is authorized first, so an
/// unauthenticated client is refused before it sends more.
fn checks_before_authorization(request: &Request<Body>) -> bool {
    request.method() == Method::POST
        && !request.headers().contains_key(header::EXPECT)
        && http_body::Body::size_hint(request.body())
            .exact()
            .is_some_and(|size| size <= CHECKED_BEFORE_AUTHORIZATION_MAX_BYTES)
}

/// What arrived of a small body before its request was authorized.
struct Arrived {
    /// Its data, and the error that ended it if one did.
    chunks: Vec<Result<Bytes, axum::Error>>,
    /// The rest of it, when it had not ended.
    rest: Option<Body>,
}

impl Arrived {
    /// Read `body` until it ends, `deadline` passes, or more of it arrives
    /// than a body checked before authorization may have.
    async fn read(mut body: Body, deadline: Instant) -> Self {
        let mut chunks = Vec::new();
        let mut len = 0;
        while len <= CHECKED_BEFORE_AUTHORIZATION_MAX_BYTES {
            let Ok(frame) = tokio::time::timeout_at(deadline, body.frame()).await else {
                break;
            };
            match frame {
                None => return Self { chunks, rest: None },
                Some(Err(error)) => {
                    chunks.push(Err(error));
                    return Self { chunks, rest: None };
                }
                // The JSON extractor drops trailers too.
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        len += data.len() as u64;
                        chunks.push(Ok(data));
                    }
                }
            }
        }
        Self {
            chunks,
            rest: Some(body),
        }
    }

    /// The body again: what arrived, then the rest as it arrives.
    fn into_body(self) -> Body {
        let rest = futures_util::stream::iter(self.rest.map(Body::into_data_stream)).flatten();
        Body::from_stream(futures_util::stream::iter(self.chunks).chain(rest))
    }
}

/// The body as `T`, rejected as the API's `Json` extractor rejects it: the
/// same body limit, `Content-Type` check and statuses, in the same envelope.
pub(super) async fn json_body<T: DeserializeOwned>(
    request: &FileRequest,
    body: Body,
) -> Result<T, Refusal> {
    axum::Json::<T>::from_request(request.with_body(body), &())
        .await
        .map(|axum::Json(value)| value)
        .map_err(|rejection| normalize_public_error(rejection.into_response()).into())
}

/// The response that ends a files request early: a refusal the API rendered,
/// or one this server rendered as the API's route would. Boxed, since every
/// step's `Result` carries one.
pub(super) struct Refusal(Box<Response>);

impl From<Response> for Refusal {
    fn from(response: Response) -> Self {
        Self(Box::new(response))
    }
}

impl From<PublicApiError> for Refusal {
    fn from(error: PublicApiError) -> Self {
        error.into_response().into()
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        *self.0
    }
}

/// The body as `T` if it says it is JSON, as the API's `Option<Json<T>>`
/// extractor reads it: no `Content-Type` is no body.
pub(super) async fn optional_json_body<T: DeserializeOwned>(
    request: &FileRequest,
    body: Body,
) -> Result<Option<T>, Refusal> {
    <axum::Json<T> as axum::extract::OptionalFromRequest<()>>::from_request(
        request.with_body(body),
        &(),
    )
    .await
    .map(|body| body.map(|axum::Json(value)| value))
    .map_err(|rejection| normalize_public_error(rejection.into_response()).into())
}

/// The request's `Idempotency-Key`, checked as the API checks it. Its value
/// matters only to the API, which binds it while preparing the call.
pub(super) fn idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, Refusal> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY_HEADER) else {
        return Err(PublicApiError::bad_request("Idempotency-Key header is required").into());
    };
    IdempotencyKey::parse_header(value.as_bytes())
        .map_err(|error| PublicApiError::bad_request(error.to_string()).into())
}

/// A field check's refusal, as the API answers it.
pub(super) fn rejected(error: impl Into<PublicApiError>) -> Refusal {
    error.into().into()
}

/// A files request refused because this replica is draining: the Native
/// `service_unavailable` envelope with the drain's `Retry-After`, and on
/// HTTP/1 the connection closed after it.
fn draining() -> Response {
    let mut response = PublicApiError::service_unavailable("data plane is draining; retry")
        .with_classification(ResponseOrigin::DataPlane, ResponseKind::Admission)
        .with_retry_after(crate::drain::DRAINING_RETRY_AFTER)
        .into_response();
    crate::drain::mark_draining_response(&mut response);
    response
}

/// The API's answer to a files route called with another method than POST:
/// the router's 405, normalized into the Native envelope.
fn method_not_allowed() -> Response {
    let mut response = StatusCode::METHOD_NOT_ALLOWED.into_response();
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    normalize_public_error(response)
}

#[cfg(test)]
mod tests;
