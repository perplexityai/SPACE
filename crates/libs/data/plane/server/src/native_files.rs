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
mod call;
mod read;

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::FromRequest,
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::any,
};
use pplx_space_native_files::FileOperation;
use pplx_space_public_api_http::{PublicApiError, normalize_public_error};
use serde::de::DeserializeOwned;

pub(crate) use access::NativeFileAccessClient;
pub use access::NativeFileAccessConfigError;
use call::Grant;

use super::DataPlaneState;

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
}

impl NativeFiles {
    pub(crate) fn new(access: NativeFileAccessClient) -> Self {
        Self { access }
    }
}

/// The files routes, at the paths the API mounts them on.
pub(super) fn router() -> Router<DataPlaneState> {
    let mut router = Router::new();
    for prefix in ["/v1", "/api/v1"] {
        for operation in FileOperation::ALL {
            let Some(handler) = handler(operation) else {
                continue;
            };
            let path = format!("{prefix}{}", operation.route().relative_runtime_path());
            router = router.route(&path, handler);
        }
    }
    router.layer(middleware::from_fn(
        pplx_space_public_api_http::with_public_request_id,
    ))
}

fn handler(operation: FileOperation) -> Option<axum::routing::MethodRouter<DataPlaneState>> {
    // Every method reaches the handler: the API answers a wrong method only
    // after it has authorized the request, so the 405 waits until then too.
    Some(match operation {
        FileOperation::Stat => any(read::stat_file),
        FileOperation::List => any(read::list_files),
        FileOperation::Read => any(read::read_file),
        _ => return None,
    })
}

/// A request the API has authorized, with its body still unread.
pub(super) struct FileRequest {
    files: Arc<NativeFiles>,
    /// What the API was asked to authorize; preparing asks it again.
    access: pplx_space_node_api_client::proto::NativeFileRequest,
    pub(super) headers: HeaderMap,
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
        let Some(files) = state.native_files.clone() else {
            // The routes are mounted only with a client to the API.
            return Err(PublicApiError::internal().into());
        };
        let (parts, body) = request.into_parts();
        let access = access::file_request(operation, &parts);
        files.access.authorize(access.clone()).await?;
        if parts.method != Method::POST {
            return Err(method_not_allowed().into());
        }
        Ok((
            Self {
                files,
                access,
                headers: parts.headers,
            },
            body,
        ))
    }

    /// Have the API prepare the sandbox call: everything its handler does
    /// before calling the sandbox, with `fingerprint` the body's for an
    /// operation whose key is bound.
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
    headers: &HeaderMap,
    body: Body,
) -> Result<T, Refusal> {
    axum::Json::<T>::from_request(request_with(headers, body), &())
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

fn request_with(headers: &HeaderMap, body: Body) -> Request<Body> {
    let mut request = Request::new(body);
    *request.headers_mut() = headers.clone();
    request
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
