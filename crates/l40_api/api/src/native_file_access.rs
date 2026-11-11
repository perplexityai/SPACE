//! The native file access RPCs, served on the activation listener. The
//! data-plane edge carries a Native `files:*` request's bytes and asks this
//! API to decide the request: each call runs the request's headers through
//! the router its public route runs behind
//! ([`crate::routes::public::native_file_access_router`]), so credentials,
//! owner scope, the idempotency binding, drain, admission and wake are the
//! public route's own, and a refusal is the response that route renders.
use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, Method, header},
};
use pplx_space_native_files::FileOperation;
use pplx_space_node_api_client::proto::{
    AuthorizeNativeFileRequestRequest, AuthorizeNativeFileRequestResponse,
    DataPlaneBackendEndpoint, NativeFileAuthorization, NativeFileOperation, NativeFileRequest,
    NativeFileRouteGrant, NativeHttpHeader, NativeHttpResponse, PrepareNativeFileCallRequest,
    PrepareNativeFileCallResponse, SpaceApiRouteToken, authorize_native_file_request_response,
    native_file_access_api_server::{NativeFileAccessApi, NativeFileAccessApiServer},
    prepare_native_file_call_response,
};
use pplx_space_public_api::RequestFingerprint;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tower::ServiceExt as _;

use crate::routes::public::{
    FileAccessAuthorization, FileAccessCall, FileAccessDecision, FileAccessGrant, FileAccessStep,
};

/// The largest request decoded: one request's headers.
const MAX_REQUEST_BYTES: usize = 256 * 1024;
/// The largest rendered refusal passed back. Native error envelopes are far
/// smaller.
const MAX_RENDERED_BODY_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = MAX_RENDERED_BODY_BYTES + 64 * 1024;
/// The transport label of a grant's backend, as data-plane routes name it.
const BACKEND_TRANSPORT_GRPC: &str = "grpc";

#[derive(Clone)]
pub(crate) struct NativeFileAccessService {
    router: axum::Router,
    shutdown: CancellationToken,
}

impl NativeFileAccessService {
    /// The service over `router`, which in production is
    /// [`crate::routes::public::native_file_access_router`].
    pub(crate) fn new(router: axum::Router, shutdown: CancellationToken) -> Self {
        Self { router, shutdown }
    }

    pub(crate) fn into_server(self) -> NativeFileAccessApiServer<Self> {
        NativeFileAccessApiServer::new(self)
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(MAX_RESPONSE_BYTES)
    }

    /// Run `request` through the router as `step`: the decision it was
    /// admitted with, or the response it was refused with.
    async fn decide(
        &self,
        request: Option<NativeFileRequest>,
        step: FileAccessStep,
    ) -> Result<Result<FileAccessDecision, NativeHttpResponse>, Status> {
        if self.shutdown.is_cancelled() {
            return Err(Status::unavailable("activation API is draining"));
        }
        let request = request.ok_or_else(|| Status::invalid_argument("request is required"))?;
        let operation = file_operation(request.operation)?;
        if let FileAccessStep::Prepare { fingerprint: None } = &step
            && operation.binds_idempotency_key()
        {
            return Err(Status::invalid_argument(format!(
                "request_fingerprint is required for files:{operation}"
            )));
        }
        let call = FileAccessCall { operation, step };
        let response = self
            .router
            .clone()
            .oneshot(http_request(operation, request, call)?)
            .await
            .unwrap_or_else(|never| match never {});
        if let Some(decision) = response.extensions().get::<FileAccessDecision>() {
            return Ok(Ok(decision.clone()));
        }
        rendered(response).await.map(Err)
    }
}

#[tonic::async_trait]
impl NativeFileAccessApi for NativeFileAccessService {
    async fn authorize_native_file_request(
        &self,
        request: Request<AuthorizeNativeFileRequestRequest>,
    ) -> Result<Response<AuthorizeNativeFileRequestResponse>, Status> {
        use authorize_native_file_request_response::Outcome;

        let outcome = match self
            .decide(request.into_inner().request, FileAccessStep::Authorize)
            .await?
        {
            Ok(FileAccessDecision::Authorized(authorization)) => {
                Outcome::Authorized(proto_authorization(authorization))
            }
            Ok(FileAccessDecision::Granted(_)) => {
                return Err(Status::internal("authorization answered with a grant"));
            }
            Err(refused) => Outcome::Refused(refused),
        };
        Ok(Response::new(AuthorizeNativeFileRequestResponse {
            outcome: Some(outcome),
        }))
    }

    async fn prepare_native_file_call(
        &self,
        request: Request<PrepareNativeFileCallRequest>,
    ) -> Result<Response<PrepareNativeFileCallResponse>, Status> {
        use prepare_native_file_call_response::Outcome;

        let request = request.into_inner();
        let fingerprint = request
            .request_fingerprint
            .map(RequestFingerprint::parse)
            .transpose()
            .map_err(|error| {
                Status::invalid_argument(format!("request_fingerprint is invalid: {error}"))
            })?;
        let outcome = match self
            .decide(request.request, FileAccessStep::Prepare { fingerprint })
            .await?
        {
            Ok(FileAccessDecision::Granted(grant)) => Outcome::Granted(proto_grant(grant)),
            Ok(FileAccessDecision::Authorized(_)) => {
                return Err(Status::internal("preparation answered without a grant"));
            }
            Err(refused) => Outcome::Refused(refused),
        };
        Ok(Response::new(PrepareNativeFileCallResponse {
            outcome: Some(outcome),
        }))
    }
}

fn file_operation(operation: i32) -> Result<FileOperation, Status> {
    match NativeFileOperation::try_from(operation) {
        Ok(NativeFileOperation::Stat) => Ok(FileOperation::Stat),
        Ok(NativeFileOperation::List) => Ok(FileOperation::List),
        Ok(NativeFileOperation::Read) => Ok(FileOperation::Read),
        Ok(NativeFileOperation::Write) => Ok(FileOperation::Write),
        Ok(NativeFileOperation::WriteBatch) => Ok(FileOperation::WriteBatch),
        Ok(NativeFileOperation::Mkdir) => Ok(FileOperation::Mkdir),
        Ok(NativeFileOperation::Remove) => Ok(FileOperation::Remove),
        Ok(NativeFileOperation::Rename) => Ok(FileOperation::Rename),
        Ok(NativeFileOperation::ChangesEnable) => Ok(FileOperation::ChangesEnable),
        Ok(NativeFileOperation::ChangesDisable) => Ok(FileOperation::ChangesDisable),
        Ok(NativeFileOperation::Modified) => Ok(FileOperation::Modified),
        Ok(NativeFileOperation::Unspecified) | Err(_) => Err(Status::invalid_argument(
            "operation is not a native file operation",
        )),
    }
}

/// The request the client sent, without its body: the route's path for
/// `operation` and the client's headers.
fn http_request(
    operation: FileOperation,
    request: NativeFileRequest,
    call: FileAccessCall,
) -> Result<axum::http::Request<Body>, Status> {
    let segment = request.sandbox_id;
    if segment.is_empty() || segment.contains(['/', '?', '#']) {
        return Err(Status::invalid_argument(
            "sandbox_id must be one path segment",
        ));
    }
    let path = operation.route().path().replace("{sandbox_id}", &segment);
    let mut http = axum::http::Request::builder()
        .method(Method::POST)
        .uri(path)
        .body(Body::empty())
        .map_err(|error| Status::invalid_argument(format!("sandbox_id is invalid: {error}")))?;
    let headers = http.headers_mut();
    for NativeHttpHeader { name, value } in request.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| Status::invalid_argument(format!("header name {name:?} is invalid")))?;
        let value = HeaderValue::from_bytes(&value)
            .map_err(|_| Status::invalid_argument(format!("header {name} has an invalid value")))?;
        headers.append(name, value);
    }
    // The body stays with the edge.
    headers.remove(header::CONTENT_LENGTH);
    headers.remove(header::TRANSFER_ENCODING);
    if !request.request_id.is_empty() {
        let request_id = HeaderValue::from_str(&request.request_id)
            .map_err(|_| Status::invalid_argument("request_id is not a header value"))?;
        headers.insert(HeaderName::from_static("x-request-id"), request_id);
    }
    http.extensions_mut().insert(call);
    Ok(http)
}

async fn rendered(response: axum::response::Response) -> Result<NativeHttpResponse, Status> {
    let (parts, body) = response.into_parts();
    let body = axum::body::to_bytes(body, MAX_RENDERED_BODY_BYTES)
        .await
        .map_err(|error| Status::internal(format!("read the rendered refusal: {error}")))?;
    Ok(NativeHttpResponse {
        status: parts.status.as_u16().into(),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| NativeHttpHeader {
                name: name.as_str().to_owned(),
                value: value.as_bytes().to_vec(),
            })
            .collect(),
        body: body.to_vec(),
    })
}

fn proto_authorization(authorization: FileAccessAuthorization) -> NativeFileAuthorization {
    NativeFileAuthorization {
        org_id: authorization.org_id.to_string(),
        owner_user_id: authorization.owner.map(|owner| owner.to_string()),
    }
}

fn proto_grant(grant: FileAccessGrant) -> NativeFileRouteGrant {
    NativeFileRouteGrant {
        authorization: Some(proto_authorization(grant.authorization)),
        sandbox_id: grant.sandbox_id.to_string(),
        backend: Some(DataPlaneBackendEndpoint {
            node_id: grant
                .node_id
                .map(|node_id| node_id.to_string())
                .unwrap_or_default(),
            endpoint: grant.endpoint,
            transport: Some(BACKEND_TRANSPORT_GRPC.to_string()),
        }),
        space_api_route_token: grant
            .space_api_route_token
            .map(|token| SpaceApiRouteToken { token }),
        guest_idempotency_key: grant.guest_key.map(|key| key.as_str().to_owned()),
    }
}

#[cfg(test)]
mod tests;
