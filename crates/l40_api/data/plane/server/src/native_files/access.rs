//! The lifecycle API's native file access RPCs, served on its activation
//! listener, as the files routes call them.

use std::time::Duration;

use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use pplx_space_common_grpc::internal_transport::{ClientTransport, Service, TransportError};
use pplx_space_common_svc::bearer_token::{SpaceBearerToken, insert_space_bearer_token};
use pplx_space_common_svc::bootstrap::{GrpcClientTrace, wrap_grpc_client_channel};
use pplx_space_native_files::FileOperation;
use pplx_space_node_api_client::proto::{
    AuthorizeNativeFileRequestRequest, NativeFileAuthorization, NativeFileOperation,
    NativeFileRequest, NativeHttpHeader, NativeHttpResponse, PrepareNativeFileCallRequest,
    authorize_native_file_request_response,
    native_file_access_api_client::NativeFileAccessApiClient, prepare_native_file_call_response,
};
use pplx_space_public_api::RequestFingerprint;
use pplx_space_public_api_http::PublicApiError;
use tonic::transport::{Channel, Endpoint};

use super::Refusal;
use super::call::Grant;

/// The API's activation listener, which serves these RPCs beside the ingress
/// activation ones.
const ENDPOINT_ENV: &str = "SPACE_INGRESS_ACTIVATION_ENDPOINT";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Authorization reads credentials and the sandbox row.
const AUTHORIZE_TIMEOUT: Duration = Duration::from_secs(15);
/// Preparation can wake the sandbox. The API bounds that wait below its node
/// RPC timeout and answers a structured, retryable error when it runs out, so
/// this waits a little longer than that timeout for the answer.
const PREPARE_TIMEOUT: Duration = pplx_space_common_core::restore_ownership::NODE_RPC_TIMEOUT
    .saturating_add(Duration::from_secs(15));
/// `Retry-After` when the API could not be asked. A retry may reach another
/// replica, or the API's own routes.
const UNAVAILABLE_RETRY_AFTER: Duration = Duration::from_secs(1);

/// Headers that describe this hop's connection rather than the request.
const HOP_BY_HOP: [HeaderName; 6] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::TE,
    header::TRAILER,
    header::UPGRADE,
];

#[derive(Debug, thiserror::Error)]
pub enum NativeFileAccessConfigError {
    #[error("{ENDPOINT_ENV} is required to serve Native files")]
    MissingEndpoint,
    #[error("{ENDPOINT_ENV} is not a valid endpoint: {0}")]
    Endpoint(#[source] tonic::transport::Error),
    #[error("native file access transport: {0}")]
    Transport(#[source] TransportError),
}

#[derive(Clone)]
pub(crate) struct NativeFileAccessClient {
    client: NativeFileAccessApiClient<GrpcClientTrace<Channel>>,
}

impl NativeFileAccessClient {
    /// One reusable channel to the API's activation listener, dialed on first
    /// use.
    pub(crate) async fn from_env() -> Result<Self, NativeFileAccessConfigError> {
        let address = std::env::var(ENDPOINT_ENV)
            .ok()
            .filter(|address| !address.trim().is_empty())
            .ok_or(NativeFileAccessConfigError::MissingEndpoint)?;
        let endpoint = Endpoint::from_shared(address)
            .map_err(NativeFileAccessConfigError::Endpoint)?
            .connect_timeout(CONNECT_TIMEOUT);
        let channel = ClientTransport::from_env(Service::IngressActivation, endpoint.uri())
            .await
            .and_then(|transport| transport.connect_endpoint(endpoint))
            .map_err(NativeFileAccessConfigError::Transport)?
            .connect_lazy();
        Ok(Self::new(channel))
    }

    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            client: NativeFileAccessApiClient::new(wrap_grpc_client_channel(channel)),
        }
    }

    /// Who the request acts for, or the response the API refused it with.
    pub(super) async fn authorize(
        &self,
        request: NativeFileRequest,
    ) -> Result<NativeFileAuthorization, Refusal> {
        use authorize_native_file_request_response::Outcome;

        let rpc = rpc_request(
            AuthorizeNativeFileRequestRequest {
                request: Some(request),
            },
            AUTHORIZE_TIMEOUT,
        )?;
        let response = self
            .client
            .clone()
            .authorize_native_file_request(rpc)
            .await
            .map_err(|status| Refusal::from(rpc_failure("authorize", &status)))?
            .into_inner();
        match response.outcome {
            Some(Outcome::Authorized(authorization)) => Ok(authorization),
            Some(Outcome::Refused(refused)) => Err(rendered(refused).into()),
            None => Err(missing_outcome("authorize").into()),
        }
    }

    /// Where the sandbox call goes, or the response the API refused it with.
    pub(super) async fn prepare(
        &self,
        request: NativeFileRequest,
        fingerprint: Option<&RequestFingerprint>,
    ) -> Result<Grant, Refusal> {
        use prepare_native_file_call_response::Outcome;

        let rpc = rpc_request(
            PrepareNativeFileCallRequest {
                request: Some(request),
                request_fingerprint: fingerprint.map(|fingerprint| fingerprint.as_str().to_owned()),
            },
            PREPARE_TIMEOUT,
        )?;
        let response = self
            .client
            .clone()
            .prepare_native_file_call(rpc)
            .await
            .map_err(|status| Refusal::from(rpc_failure("prepare", &status)))?
            .into_inner();
        match response.outcome {
            Some(Outcome::Granted(grant)) => Grant::from_proto(grant).map_err(|error| {
                tracing::warn!(%error, "native file access granted an unusable route");
                Refusal::from(PublicApiError::internal())
            }),
            Some(Outcome::Refused(refused)) => Err(rendered(refused).into()),
            None => Err(missing_outcome("prepare").into()),
        }
    }
}

/// What the API is asked about `parts`, a request for `operation`.
pub(super) fn file_request(operation: FileOperation, parts: &Parts) -> NativeFileRequest {
    NativeFileRequest {
        operation: proto_operation(operation) as i32,
        // Every files route ends `/{sandbox_id}/files:<action>`.
        sandbox_id: parts
            .uri
            .path()
            .rsplit('/')
            .nth(1)
            .unwrap_or_default()
            .to_owned(),
        headers: parts
            .headers
            .iter()
            .filter(|(name, _)| !HOP_BY_HOP.contains(name))
            .map(|(name, value)| NativeHttpHeader {
                name: name.as_str().to_owned(),
                value: value.as_bytes().to_vec(),
            })
            .collect(),
        request_id: pplx_space_public_api_http::current_request_id().unwrap_or_default(),
    }
}

fn proto_operation(operation: FileOperation) -> NativeFileOperation {
    match operation {
        FileOperation::Stat => NativeFileOperation::Stat,
        FileOperation::List => NativeFileOperation::List,
        FileOperation::Read => NativeFileOperation::Read,
        FileOperation::Write => NativeFileOperation::Write,
        FileOperation::WriteBatch => NativeFileOperation::WriteBatch,
        FileOperation::Mkdir => NativeFileOperation::Mkdir,
        FileOperation::Remove => NativeFileOperation::Remove,
        FileOperation::Rename => NativeFileOperation::Rename,
        FileOperation::ChangesEnable => NativeFileOperation::ChangesEnable,
        FileOperation::ChangesDisable => NativeFileOperation::ChangesDisable,
        FileOperation::Modified => NativeFileOperation::Modified,
    }
}

fn rpc_request<T>(message: T, timeout: Duration) -> Result<tonic::Request<T>, Refusal> {
    let mut request = tonic::Request::new(message);
    request.set_timeout(timeout);
    insert_space_bearer_token(request.metadata_mut(), SpaceBearerToken::ClusterApi).map_err(
        |error| {
            tracing::warn!(%error, "cannot attach the native file access credential");
            Refusal::from(unavailable())
        },
    )?;
    Ok(request)
}

/// The response the API rendered, as it rendered it.
fn rendered(refused: NativeHttpResponse) -> Response {
    let mut response = Response::new(Body::from(refused.body));
    *response.status_mut() = u16::try_from(refused.status)
        .ok()
        .and_then(|status| StatusCode::from_u16(status).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let headers = response.headers_mut();
    for NativeHttpHeader { name, value } in refused.headers {
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(&value),
        ) else {
            continue;
        };
        // This response's own body sets its framing.
        if HOP_BY_HOP.contains(&name)
            || name == header::CONTENT_LENGTH
            || name == header::TRANSFER_ENCODING
        {
            continue;
        }
        headers.append(name, value);
    }
    response
}

/// The API could not be asked, or did not answer: nothing was decided, so the
/// request may be sent again.
fn rpc_failure(rpc: &'static str, status: &tonic::Status) -> Response {
    tracing::warn!(
        rpc,
        code = ?status.code(),
        message = status.message(),
        "native file access call failed"
    );
    match status.code() {
        tonic::Code::InvalidArgument
        | tonic::Code::Internal
        | tonic::Code::PermissionDenied
        | tonic::Code::Unauthenticated
        | tonic::Code::FailedPrecondition => PublicApiError::internal().into_response(),
        _ => unavailable(),
    }
}

fn missing_outcome(rpc: &'static str) -> Response {
    tracing::warn!(rpc, "native file access answered without an outcome");
    PublicApiError::internal().into_response()
}

fn unavailable() -> Response {
    PublicApiError::service_unavailable("native file access is unavailable; retry the request")
        .with_retry_after(UNAVAILABLE_RETRY_AFTER)
        .into_response()
}
