//! The sandbox call of a prepared files request, sent straight to the
//! sandbox's runtime with the route the API granted.

use std::fmt;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use axum::http::StatusCode;
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::bootstrap::{GrpcClientTrace, wrap_grpc_client_channel};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Token, insert_space_api_route_ed25519_token,
};
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_native_files::{
    FileCallFailure, FileOperation, WorkerFailure, file_call_error, unprotected_mutation_error,
};
use pplx_space_node_api_client::NodeApiOperationBudget;
use pplx_space_node_api_client::proto::{NativeFileRouteGrant, worker_api_client::WorkerApiClient};
use pplx_space_public_api::RequestFingerprint;
use pplx_space_public_api_http::PublicApiError;
use tonic::transport::Channel;

use super::{FileRequest, Refusal};
use crate::DataPlaneState;
use crate::native::node_api_request;
use crate::state::BackendEndpoint;

pub(super) type WorkerClient = WorkerApiClient<GrpcClientTrace<Channel>>;

/// Where a prepared request's sandbox call goes.
#[derive(Clone, Debug)]
pub(crate) struct Grant {
    /// The sandbox's canonical id.
    pub(super) sandbox_id: SandboxId,
    /// The runtime the API reached for this request.
    endpoint: BackendEndpoint,
    route_token: Option<SpaceApiRouteEd25519Token>,
    /// The key the sandbox replays the request by, for an operation it
    /// replays by key.
    guest_key: Option<IdempotencyKey>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GrantError {
    #[error("sandbox id {0:?} is invalid")]
    SandboxId(String),
    #[error("the grant names no runtime")]
    Endpoint,
    #[error("route token is invalid")]
    RouteToken,
    #[error("guest idempotency key is invalid")]
    GuestKey,
}

impl Grant {
    pub(super) fn from_proto(grant: NativeFileRouteGrant) -> Result<Self, GrantError> {
        let sandbox_id = grant
            .sandbox_id
            .parse()
            .map_err(|_| GrantError::SandboxId(grant.sandbox_id.clone()))?;
        let endpoint = grant
            .backend
            .map(|backend| backend.endpoint)
            .unwrap_or_default()
            .parse()
            .map_err(|_| GrantError::Endpoint)?;
        let route_token = grant
            .space_api_route_token
            .map(|token| SpaceApiRouteEd25519Token::parse(token.token))
            .transpose()
            .map_err(|_| GrantError::RouteToken)?;
        let guest_key = grant
            .guest_idempotency_key
            .map(IdempotencyKey::parse)
            .transpose()
            .map_err(|_| GrantError::GuestKey)?;
        Ok(Self {
            sandbox_id,
            endpoint,
            route_token,
            guest_key,
        })
    }

    /// The key the sandbox replays this request by. The API grants one for
    /// every operation it replays by key, so its absence is a server fault.
    pub(super) fn guest_key(&self) -> Result<IdempotencyKey, Refusal> {
        self.guest_key.clone().ok_or_else(|| {
            tracing::warn!(sandbox_id = %self.sandbox_id, "granted without a guest key");
            PublicApiError::internal().into()
        })
    }

    fn request<T>(&self, message: T) -> Result<tonic::Request<T>, WorkerCallError> {
        let mut request = node_api_request(message)
            .map_err(|status| WorkerCallError::NotSent(status.message().to_owned()))?;
        if let Some(route_token) = &self.route_token {
            insert_space_api_route_ed25519_token(request.metadata_mut(), route_token).map_err(
                |error| WorkerCallError::NotSent(format!("attach the route token: {error}")),
            )?;
        }
        Ok(request)
    }
}

/// When a failed call reaches the runtime again over a fresh preparation,
/// once, as the API's spaced client does.
#[derive(Clone, Copy, Debug)]
pub(super) enum Reconnect {
    /// The node-api is draining: unary calls.
    WhenDraining,
    /// The node-api is draining or the connection closed before the stream
    /// opened: stream opens.
    BeforeStream,
}

impl Reconnect {
    fn applies(self, status: &tonic::Status) -> bool {
        match self {
            Self::WhenDraining => pplx_space_node_api_client::is_node_api_draining_status(status),
            Self::BeforeStream => pplx_space_node_api_client::should_reconnect_stream_open(status),
        }
    }
}

/// A prepared sandbox call: the route it was granted, and how to get another.
pub(super) struct SandboxCall<'a> {
    state: &'a DataPlaneState,
    request: &'a FileRequest,
    fingerprint: Option<RequestFingerprint>,
    /// Replaced when a reconnect is granted another route. A `parents` write
    /// shares the call between its writes and its mkdirs, so it is not `&mut`.
    grant: Mutex<Grant>,
}

impl<'a> SandboxCall<'a> {
    pub(super) async fn prepare(
        state: &'a DataPlaneState,
        request: &'a FileRequest,
        fingerprint: Option<RequestFingerprint>,
    ) -> Result<Self, Refusal> {
        let grant = request.prepare(fingerprint.as_ref()).await?;
        Ok(Self {
            state,
            request,
            fingerprint,
            grant: Mutex::new(grant),
        })
    }

    /// The route the call goes to now.
    pub(super) fn grant(&self) -> Grant {
        self.grant
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Send `message` once, as the API's spaced client sends a write stream:
    /// nothing reconnects it, since its body may already be partly sent.
    pub(super) async fn send<Msg, Resp, Call, Fut>(
        &self,
        operation: &'static str,
        message: Msg,
        call: Call,
    ) -> Result<Resp, CallFailure>
    where
        Call: Fn(WorkerClient, tonic::Request<Msg>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    {
        self.attempt(operation, None, message, &call)
            .await
            .map_err(CallFailure::Worker)
    }

    /// Send `message` with `call`, within `budget` when it has one. A failure
    /// `reconnect` applies to prepares the request again, which reaches the
    /// runtime its sandbox has now, and sends it once more.
    pub(super) async fn call<Msg, Resp, Call, Fut>(
        &self,
        operation: &'static str,
        reconnect: Reconnect,
        budget: Option<NodeApiOperationBudget>,
        message: Msg,
        call: Call,
    ) -> Result<Resp, CallFailure>
    where
        Msg: Clone,
        Call: Fn(WorkerClient, tonic::Request<Msg>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    {
        match self
            .attempt(operation, budget, message.clone(), &call)
            .await
        {
            Err(WorkerCallError::Status(status)) if reconnect.applies(&status) => {
                // Later calls to this runtime need a new connection too.
                self.state
                    .backend_channels
                    .forget_endpoint(&self.grant().endpoint);
                let grant = self
                    .request
                    .prepare(self.fingerprint.as_ref())
                    .await
                    .map_err(CallFailure::Refused)?;
                *self
                    .grant
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = grant;
                self.attempt(operation, budget, message, &call)
                    .await
                    .map_err(CallFailure::Worker)
            }
            result => result.map_err(CallFailure::Worker),
        }
    }

    async fn attempt<Msg, Resp, Call, Fut>(
        &self,
        operation: &'static str,
        budget: Option<NodeApiOperationBudget>,
        message: Msg,
        call: &Call,
    ) -> Result<Resp, WorkerCallError>
    where
        Call: Fn(WorkerClient, tonic::Request<Msg>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    {
        let grant = self.grant();
        let channel = self
            .state
            .backend_channels
            .channel_for_key(&grant.endpoint, grant.sandbox_id.as_uuid())
            .await
            .map_err(|error| {
                tracing::warn!(
                    sandbox_id = %grant.sandbox_id,
                    endpoint = %grant.endpoint,
                    %error,
                    "cannot reach the granted sandbox runtime"
                );
                WorkerCallError::NotSent("sandbox runtime is not reachable".to_owned())
            })?;
        let pending = call(
            WorkerApiClient::new(wrap_grpc_client_channel(channel)),
            grant.request(message)?,
        );
        let response = match budget {
            Some(budget) => tokio::time::timeout(budget.timeout(), pending)
                .await
                .map_err(|_| {
                    tracing::warn!(
                        operation,
                        sandbox_id = %grant.sandbox_id,
                        timeout_ms = budget.timeout().as_millis(),
                        "sandbox runtime operation timed out"
                    );
                    WorkerCallError::TimedOut(operation)
                })?,
            None => pending.await,
        };
        response
            .map(tonic::Response::into_inner)
            .map_err(WorkerCallError::Status)
    }
}

/// Why a prepared sandbox call failed.
pub(super) enum CallFailure {
    /// Preparing it again, to reconnect, was refused.
    Refused(Refusal),
    Worker(WorkerCallError),
}

impl CallFailure {
    /// The response `operation`'s route answers with, as the API's answers.
    pub(super) fn into_refusal(self, operation: FileOperation) -> Refusal {
        match self {
            Self::Refused(refusal) => refusal,
            Self::Worker(error) => worker_error(operation, &error).into(),
        }
    }
}

impl fmt::Display for CallFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(_) => formatter.write_str("the API refused to prepare the call again"),
            Self::Worker(error) => error.fmt(formatter),
        }
    }
}

/// A `parents` write reads only whether its first attempt found the path
/// missing; a refusal is the answer either way.
impl FileCallFailure for CallFailure {
    fn status(&self) -> StatusCode {
        match self {
            Self::Refused(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Worker(error) => error.status(),
        }
    }

    fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Refused(_) => None,
            Self::Worker(error) => error.retry_after(),
        }
    }

    fn is_guest_path_not_found(&self) -> bool {
        matches!(self, Self::Worker(error) if error.is_guest_path_not_found())
    }

    fn is_refused_before_start(&self) -> bool {
        matches!(self, Self::Worker(error) if error.is_refused_before_start())
    }

    fn is_lock_contention(&self) -> bool {
        false
    }
}

/// The public error for `operation`'s failed sandbox call.
fn worker_error(operation: FileOperation, error: &WorkerCallError) -> PublicApiError {
    if operation.is_unprotected_mutation() {
        unprotected_mutation_error(error).into()
    } else {
        file_call_error(error).into()
    }
}

/// A sandbox call that failed at the runtime or on the way to it.
#[derive(Debug)]
pub(super) enum WorkerCallError {
    Status(tonic::Status),
    /// No answer within the operation's budget: `operation` may still
    /// complete.
    TimedOut(&'static str),
    /// Nothing was sent.
    NotSent(String),
}

impl WorkerCallError {
    fn worker_failure(&self) -> Option<WorkerFailure> {
        match self {
            Self::Status(status) => Some(WorkerFailure::from_status(status)),
            Self::TimedOut(_) | Self::NotSent(_) => None,
        }
    }
}

impl fmt::Display for WorkerCallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => WorkerFailure::from_status(status).fmt(formatter),
            Self::TimedOut(operation) => {
                write!(
                    formatter,
                    "sandbox runtime operation timed out: {operation}"
                )
            }
            Self::NotSent(message) => formatter.write_str(message),
        }
    }
}

impl FileCallFailure for WorkerCallError {
    fn status(&self) -> StatusCode {
        match self {
            Self::Status(status) => WorkerFailure::from_status(status).status(),
            // The API's own budget answers 408 for a call it stopped waiting
            // on.
            Self::TimedOut(_) => StatusCode::REQUEST_TIMEOUT,
            Self::NotSent(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn retry_after(&self) -> Option<Duration> {
        self.worker_failure()
            .and_then(|failure| failure.retry_after())
    }

    fn is_guest_path_not_found(&self) -> bool {
        self.worker_failure()
            .is_some_and(|failure| failure.is_guest_path_not_found())
    }

    fn is_refused_before_start(&self) -> bool {
        match self {
            Self::Status(status) => WorkerFailure::from_status(status).is_refused_before_start(),
            Self::TimedOut(_) => false,
            Self::NotSent(_) => true,
        }
    }

    fn is_lock_contention(&self) -> bool {
        false
    }
}
