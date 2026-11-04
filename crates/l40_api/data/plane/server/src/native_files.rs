//! Native `files:*` on the data plane.
//!
//! The data plane carries a request's bytes and the lifecycle API decides it.
//! Before the body is read, [`access`] asks the API's activation listener to
//! authorize the request from its headers. Once the body is checked, it asks
//! the API to prepare the sandbox call: the idempotency binding, drain gate,
//! admission charge and wake. The call itself then goes straight to the
//! sandbox's runtime ([`call`]). Field checks, worker requests and the
//! mapping of a failed call are `pplx-space-native-files`'s, which the API's
//! own handlers use too, so a request gets the same answer from either
//! server. The API keeps those handlers, and serves whatever reaches it.

mod access;
mod admission;
mod call;
mod mutations;
mod read;
mod write;

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path},
    http::{Extensions, HeaderMap, HeaderValue, Method, Request, StatusCode, Version, header},
    middleware,
    response::{IntoResponse, Response},
    routing::any,
};
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_native_files::FileOperation;
use pplx_space_native_files::limits::{
    WRITE_FILE_BATCH_BODY_LIMIT_BYTES, WRITE_FILE_BODY_LIMIT_BYTES,
};
use pplx_space_public_api::PublicSandboxId;
use pplx_space_public_api_http::{PublicApiError, normalize_public_error};
use serde::de::DeserializeOwned;

pub(crate) use access::NativeFileAccessClient;
pub use access::NativeFileAccessConfigError;
pub use admission::{NativeUploadAdmissionArgs, NativeUploadAdmissionConfig};
use call::Grant;

use super::DataPlaneState;
use crate::upload_admission::UploadAdmission;

const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

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

/// A request the API has authorized, with its body still unread.
pub(super) struct FileRequest {
    files: Arc<NativeFiles>,
    /// What the API was asked to authorize; preparing asks it again.
    access: pplx_space_node_api_client::proto::NativeFileRequest,
    pub(super) headers: HeaderMap,
    /// The route's, such as its body limit, for reading the body.
    extensions: Extensions,
    pub(super) version: Version,
    /// The sandbox the path names, as the API parsed it.
    pub(super) sandbox_id: SandboxId,
    /// The org the request acts for.
    pub(super) org_id: OrgId,
}

impl FileRequest {
    /// Have the API authorize `request` as `operation` from its headers
    /// alone, and answer its refusal if it refuses. The body is handed back
    /// unread.
    pub(super) async fn authorize(
        state: &DataPlaneState,
        operation: FileOperation,
        request: Request<Body>,
    ) -> Result<(Self, Body), Refusal> {
        // A replica that is leaving takes no new request; the retry reaches
        // one that is not, as a refused stream's does.
        if state.drain_started.is_cancelled() {
            return Err(draining().into());
        }
        let Some(files) = state.native_files.clone() else {
            // The routes are mounted only with a client to the API.
            return Err(PublicApiError::internal().into());
        };
        let (mut parts, body) = request.into_parts();
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
        Ok((
            Self {
                files,
                access,
                headers: parts.headers,
                extensions: parts.extensions,
                version: parts.version,
                sandbox_id: sandbox_id.into(),
                org_id,
            },
            body,
        ))
    }

    /// Have the API prepare the sandbox call: everything its handler does
    /// before calling the sandbox, with `fingerprint` the body's for an
    /// operation whose key is bound.
    /// The request again, around `body`.
    fn with_body(&self, body: Body) -> Request<Body> {
        let mut request = Request::new(body);
        *request.headers_mut() = self.headers.clone();
        *request.extensions_mut() = self.extensions.clone();
        *request.version_mut() = self.version;
        request
    }

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
