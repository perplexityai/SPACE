use crate::clients::BackendClients;
use crate::node_ctl;
use crate::proto::worker_api_internal_server::WorkerApiInternal;
use crate::proto::worker_api_server::WorkerApi;
use crate::proto::{TriggerStateReconciliationRequest, TriggerStateReconciliationResponse};
use crate::restart_handoff::RpcReplaySafety;
use crate::uds_per_sandbox_limiter::{SandboxBudget, UdsPerSandboxLimiter, UdsPerSandboxPermit};
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_grpc::{fwd_retry, record_sandbox_id};
use pplx_space_common_svc::auth::{
    NetworkPolicyMutationAuthority, NodeApiAuthenticatedCaller, WaitReadyLifecycleAuthority,
};
use pplx_space_common_svc::space_api_route::{
    SpaceApiBridgeCallerPolicy, WaitReadyCallPurpose, set_space_api_bridge_caller_policy,
    space_api_route_capability_presence, take_wait_ready_call_purpose, wait_ready_call_purpose,
};
use pplx_space_node_mgr_client::{CacheDiskSchedulingEffect, NodeResourceStatusReader};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

pub struct WorkerApiService {
    clients: BackendClients,
    uds_per_sandbox_limiter: UdsPerSandboxLimiter,
    service_port_ingress_limiter: UdsPerSandboxLimiter,
    resource_status_reader: NodeResourceStatusReader,
    shutdown_token: CancellationToken,
}

/// Reject policy-bearing requests from callers node-api did not classify as the
/// trusted control plane: `SetSandboxNetworkPolicy` and
/// `SetSandboxAutoLifecyclePolicy`, a `CreateSandbox` or `AdoptPoolSandbox` with a
/// policy override or a nonzero generation, an activation auto-lifecycle policy on
/// `StartSandbox`, `ResumeSandbox` or the internal Wake, and the internal Wake's
/// runtime upgrade. Policy requests carry an attacker-controllable `authority` and
/// `generation`; a data-plane service-account token that reached the backend with
/// one could pin a node-local or stale-generation policy that cluster
/// reconciliation can never override. Absent an authenticated caller (auth layer
/// disabled, e.g. local dev/tests) the request forwards as before.
fn require_policy_mutation_authority<T>(request: &Request<T>) -> Result<(), Status> {
    check_policy_mutation_authority(
        request
            .extensions()
            .get::<NodeApiAuthenticatedCaller>()
            .map(NodeApiAuthenticatedCaller::network_policy_authority),
    )
}

fn check_policy_mutation_authority(
    authority: Option<NetworkPolicyMutationAuthority>,
) -> Result<(), Status> {
    match authority {
        Some(NetworkPolicyMutationAuthority::Denied) => Err(Status::permission_denied(
            "caller is not authorized to mutate network or auto-lifecycle policy",
        )),
        _ => Ok(()),
    }
}

fn classify_wait_ready_caller<T>(request: &Request<T>) -> SpaceApiBridgeCallerPolicy {
    classify_wait_ready_policy(
        request
            .extensions()
            .get::<NodeApiAuthenticatedCaller>()
            .map(NodeApiAuthenticatedCaller::wait_ready_lifecycle_authority),
        wait_ready_call_purpose(request.metadata()),
    )
}

fn classify_wait_ready_policy(
    authority: Option<WaitReadyLifecycleAuthority>,
    purpose: WaitReadyCallPurpose,
) -> SpaceApiBridgeCallerPolicy {
    match (authority, purpose) {
        (Some(WaitReadyLifecycleAuthority::Granted), WaitReadyCallPurpose::LifecycleProbe) => {
            SpaceApiBridgeCallerPolicy::TrustedLifecycle
        }
        _ => SpaceApiBridgeCallerPolicy::RouteTokenRequired,
    }
}

fn prepare_wait_ready_forward<T>(request: &mut Request<T>) {
    let policy = classify_wait_ready_caller(request);
    let route_token = space_api_route_capability_presence(request.metadata());
    take_wait_ready_call_purpose(request.metadata_mut());
    pplx_space_metrics::auth::node_api_wait_ready_policy(policy.as_str(), route_token.as_str());
    set_space_api_bridge_caller_policy(request.metadata_mut(), policy);
}

impl WorkerApiService {
    pub fn new(clients: BackendClients) -> Self {
        Self::with_resource_status_reader(clients, NodeResourceStatusReader::new())
    }

    pub fn with_resource_status_reader(
        clients: BackendClients,
        resource_status_reader: NodeResourceStatusReader,
    ) -> Self {
        Self::with_resource_status_reader_and_shutdown_token(
            clients,
            resource_status_reader,
            CancellationToken::new(),
        )
    }

    pub fn with_resource_status_reader_and_shutdown_token(
        clients: BackendClients,
        resource_status_reader: NodeResourceStatusReader,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            clients,
            uds_per_sandbox_limiter: shared_uds_per_sandbox_limiter(),
            service_port_ingress_limiter: shared_service_port_ingress_limiter(),
            resource_status_reader,
            shutdown_token,
        }
    }

    /// The served `WorkerApi`. A reset `SpacedWrite` body is an interrupted
    /// upload, not an empty one: tonic would otherwise decode it as the end of
    /// the stream and let a partial file through.
    pub fn into_grpc_service(
        self,
    ) -> pplx_space_common_grpc::PreserveInputErrors<
        crate::proto::worker_api_server::WorkerApiServer<Self>,
    > {
        pplx_space_common_grpc::PreserveInputErrors::for_methods(
            crate::proto::worker_api_server::WorkerApiServer::new(self),
            &["SpacedWrite"],
        )
    }

    async fn ensure_disk_allocation_open(&self, method: &'static str) -> Result<(), Status> {
        let status = self
            .resource_status_reader
            .read(
                self.clients.node(),
                Request::new(pplx_space_node_mgr_client::proto::NodeResourceStatusRequest {}),
            )
            .await
            .map_err(|error| {
                Status::resource_exhausted(format!(
                    "{method} rejected by node disk admission: node resource status unavailable: {error}"
                ))
            })?;
        if let Some(error) = status.fallback_error() {
            tracing::warn!(
                method,
                %error,
                "node-manager resource status unavailable; reusing recent status for disk admission"
            );
        }
        let cache_disk =
            pplx_space_node_mgr_client::cache_disk_status(&status.response).map_err(|error| {
            Status::resource_exhausted(format!(
                "{method} rejected by node disk admission: node resource status unavailable: {error}"
            ))
        })?;

        match cache_disk.scheduling_effect() {
            CacheDiskSchedulingEffect::Open => Ok(()),
            CacheDiskSchedulingEffect::WarnOnly => {
                tracing::warn!(
                    method,
                    reason = %cache_disk.admission_reason,
                    detail = %cache_disk.admission_detail,
                    "node disk admission warning threshold crossed"
                );
                Ok(())
            }
            CacheDiskSchedulingEffect::Full | CacheDiskSchedulingEffect::Unavailable => {
                Err(Status::resource_exhausted(format!(
                    "{method} rejected by node disk admission: {}",
                    cache_disk.admission_reason
                )))
            }
        }
    }

    async fn acquire_sbx_permit(
        &self,
        method: &'static str,
        sandbox_id: &str,
    ) -> Result<UdsPerSandboxPermit, Status> {
        self.uds_per_sandbox_limiter
            .acquire(method, sandbox_id)
            .await
    }

    async fn acquire_service_port_ingress_permit(
        &self,
        sandbox_id: &str,
    ) -> Result<UdsPerSandboxPermit, Status> {
        self.service_port_ingress_limiter
            .acquire("ServicePortIngress", sandbox_id)
            .await
    }

    async fn limited_sbx_operation<T, F, Fut>(
        &self,
        method: &'static str,
        sandbox_id: &str,
        operation: F,
    ) -> Result<T, Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, Status>> + Send,
    {
        let _permit = self.acquire_sbx_permit(method, sandbox_id).await?;
        operation().await
    }

    async fn sbx_unary_with_handoff<Req, Resp, F, Fut>(
        &self,
        method: &'static str,
        safety: RpcReplaySafety,
        request: Request<Req>,
        call: F,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Send + Sync + 'static,
        Resp: Send + 'static,
        F: Fn(pplx_space_sbx_mgr_client::SandboxManagerGrpcClient, Request<Req>) -> Fut + Send,
        Fut: Future<Output = Result<Response<Resp>, Status>> + Send,
    {
        crate::restart_handoff::call_with_restart_handoff(
            method,
            safety,
            request,
            self.clients.sbx(),
            call,
        )
        .await
    }

    async fn limited_sbx_unary<Req, Resp, F, Fut>(
        &self,
        budget: SandboxBudget,
        method: &'static str,
        request: Request<Req>,
        call: F,
    ) -> Result<Response<Resp>, Status>
    where
        Req: SandboxScopedRequest + Clone + Send + Sync + 'static,
        Resp: Send + 'static,
        F: Fn(pplx_space_sbx_mgr_client::SandboxManagerGrpcClient, Request<Req>) -> Fut + Send,
        Fut: Future<Output = Result<Response<Resp>, Status>> + Send,
    {
        let sandbox_id = request.get_ref().sandbox_id().to_owned();
        record_sandbox_id(&tracing::Span::current(), &sandbox_id);
        let _permit = self
            .uds_per_sandbox_limiter
            .acquire_in(budget, method, &sandbox_id)
            .await?;
        crate::restart_handoff::call_with_restart_handoff(
            method,
            RpcReplaySafety::RequiresProof,
            request,
            self.clients.sbx(),
            call,
        )
        .await
    }

    async fn limited_sbx_server_stream<Req, Item, S, F, Fut>(
        &self,
        method: &'static str,
        request: Request<Req>,
        call: F,
    ) -> Result<Response<PinnedResponseStream<Item>>, Status>
    where
        Req: SandboxScopedRequest + Clone + Send + Sync + 'static,
        Item: Send + 'static,
        S: Stream<Item = Result<Item, Status>> + Send + 'static,
        F: Fn(pplx_space_sbx_mgr_client::SandboxManagerGrpcClient, Request<Req>) -> Fut + Send,
        Fut: Future<Output = Result<Response<S>, Status>> + Send,
    {
        let sandbox_id = request.get_ref().sandbox_id().to_owned();
        record_sandbox_id(&tracing::Span::current(), &sandbox_id);
        let client = self.clients.sbx();
        let permit = self.acquire_sbx_permit(method, &sandbox_id).await?;
        let response = crate::restart_handoff::call_with_restart_handoff(
            method,
            RpcReplaySafety::RequiresProof,
            request,
            client,
            call,
        )
        .await?;
        Ok(response.map(|stream| guard_stream(stream, permit)))
    }
}

type PinnedResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
type ServicePortIngressFrame = pplx_space_net_gateway_client::proto::ServicePortIngressFrame;
type ServicePortIngressInbound = tonic::Streaming<ServicePortIngressFrame>;
type ServicePortIngressForwardState = (ServicePortIngressInbound, SandboxId, CancellationToken);
const SERVICE_PORT_INGRESS_FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(5);

fn shared_uds_per_sandbox_limiter() -> UdsPerSandboxLimiter {
    static LIMITER: OnceLock<UdsPerSandboxLimiter> = OnceLock::new();
    LIMITER.get_or_init(UdsPerSandboxLimiter::control).clone()
}

fn shared_service_port_ingress_limiter() -> UdsPerSandboxLimiter {
    static LIMITER: OnceLock<UdsPerSandboxLimiter> = OnceLock::new();
    LIMITER
        .get_or_init(UdsPerSandboxLimiter::service_port_ingress)
        .clone()
}

fn guard_stream<T, S>(stream: S, permit: UdsPerSandboxPermit) -> PinnedResponseStream<T>
where
    T: Send + 'static,
    S: Stream<Item = Result<T, Status>> + Send + 'static,
{
    let held_permit = permit;
    pin_stream(stream.map(move |item| {
        let _ = &held_permit;
        item
    }))
}

fn pin_stream<T, S>(stream: S) -> PinnedResponseStream<T>
where
    T: Send + 'static,
    S: Stream<Item = Result<T, Status>> + Send + 'static,
{
    Box::pin(stream)
}

async fn read_service_port_ingress_first_frame<F>(
    message: F,
    frame_timeout: Duration,
) -> Result<ServicePortIngressFrame, Status>
where
    F: Future<Output = Result<Option<ServicePortIngressFrame>, Status>>,
{
    match timeout(frame_timeout, message).await {
        Ok(Ok(Some(frame))) => Ok(frame),
        Ok(Ok(None)) => Err(Status::invalid_argument(
            "service port ingress start frame is required",
        )),
        Ok(Err(status)) => Err(status),
        Err(_) => Err(Status::deadline_exceeded(
            "service port ingress start frame timed out",
        )),
    }
}

async fn next_service_port_ingress_frame(
    (mut inbound, sandbox_id, shutdown_token): ServicePortIngressForwardState,
) -> Option<(ServicePortIngressFrame, ServicePortIngressForwardState)> {
    tokio::select! {
        result = inbound.message() => {
            match result {
                Ok(Some(frame)) => Some((frame, (inbound, sandbox_id, shutdown_token))),
                Ok(None) => None,
                Err(status) => {
                    tracing::debug!(
                        %sandbox_id,
                        %status,
                        "node-api service port ingress inbound stream ended with error"
                    );
                    None
                }
            }
        }
        _ = shutdown_token.cancelled() => None,
    }
}

trait SandboxScopedRequest {
    fn sandbox_id(&self) -> &str;
}

macro_rules! sandbox_scoped_requests {
    ($($request:ty),+ $(,)?) => {
        $(
            impl SandboxScopedRequest for $request {
                fn sandbox_id(&self) -> &str {
                    &self.sandbox_id
                }
            }
        )+
    };
}

sandbox_scoped_requests!(
    pplx_space_sbx_mgr_client::proto::StartRequest,
    pplx_space_sbx_mgr_client::proto::PauseRequest,
    pplx_space_sbx_mgr_client::proto::ResumeRequest,
    pplx_space_sbx_mgr_client::proto::WakeRequest,
    pplx_space_sbx_mgr_client::proto::CreateSnapshotRequest,
    pplx_space_sbx_mgr_client::proto::SuspendRequest,
    pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendRequest,
    pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupRequest,
    pplx_space_sbx_mgr_client::proto::StatsRequest,
    pplx_space_sbx_mgr_client::proto::UpdateRequest,
    pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyRequest,
    pplx_space_sbx_mgr_client::proto::TouchAutoLifecycleRequest,
    pplx_space_sbx_mgr_client::proto::SetNetworkPolicyRequest,
    pplx_space_sbx_mgr_client::proto::SpacedGetStatusRequest,
    pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest,
    pplx_space_sbx_mgr_client::proto::SpacedStatRequest,
    pplx_space_sbx_mgr_client::proto::SpacedListRequest,
    pplx_space_sbx_mgr_client::proto::SpacedReadRequest,
    pplx_space_sbx_mgr_client::proto::SpacedWriteRequest,
    pplx_space_sbx_mgr_client::proto::SpacedWriteBatchRequest,
    pplx_space_sbx_mgr_client::proto::SpacedMkdirRequest,
    pplx_space_sbx_mgr_client::proto::SpacedRemoveRequest,
    pplx_space_sbx_mgr_client::proto::SpacedRenameRequest,
    pplx_space_sbx_mgr_client::proto::SpacedNormalizeWatchRootRequest,
    pplx_space_sbx_mgr_client::proto::SpacedWatchRequest,
    pplx_space_sbx_mgr_client::proto::SpacedCreateWatcherRequest,
    pplx_space_sbx_mgr_client::proto::SpacedGetWatcherEventsRequest,
    pplx_space_sbx_mgr_client::proto::SpacedRemoveWatcherRequest,
    pplx_space_sbx_mgr_client::proto::SpacedEnableFileChangesRequest,
    pplx_space_sbx_mgr_client::proto::SpacedDisableFileChangesRequest,
    pplx_space_sbx_mgr_client::proto::SpacedGetModifiedFilesRequest,
    pplx_space_sbx_mgr_client::proto::SpacedStartProcessRequest,
    pplx_space_sbx_mgr_client::proto::SpacedConnectProcessRequest,
    pplx_space_sbx_mgr_client::proto::SpacedListProcessesRequest,
    pplx_space_sbx_mgr_client::proto::SpacedSendSignalRequest,
    pplx_space_sbx_mgr_client::proto::SpacedSendInputRequest,
    pplx_space_sbx_mgr_client::proto::SpacedCloseStdinRequest,
    pplx_space_sbx_mgr_client::proto::SpacedResizePtyRequest,
    pplx_space_sbx_mgr_client::proto::SpacedKillProcessRequest,
    pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeRequest,
    pplx_space_sbx_mgr_client::proto::SpacedCloseLoopbackBridgeRequest,
    pplx_space_sbx_mgr_client::proto::RollbackRequest,
);

/// Forward a request to a backend client RPC, converting the error to `Status`.
macro_rules! fwd {
    ($client:expr, $rpc:ident, $req:expr) => {
        $client.$rpc($req).await.map_err(Into::into)
    };
}

macro_rules! fwd_sbx {
    ($service:expr, $method:literal, $rpc:ident, $req:expr) => {
        $service
            .limited_sbx_unary(
                SandboxBudget::Shared,
                $method,
                $req,
                |mut client, request| async move { client.$rpc(request).await },
            )
            .await
    };
}

// Only for SpacedKillProcess, SpacedSendSignal and SpacedListProcesses (which
// also backs single-process reads): they stop or inspect a sandbox's processes,
// so they take reserved permits that streams and other calls cannot use up.
macro_rules! fwd_sbx_reserved {
    ($service:expr, $method:literal, $rpc:ident, $req:expr) => {
        $service
            .limited_sbx_unary(
                SandboxBudget::Reserved,
                $method,
                $req,
                |mut client, request| async move { client.$rpc(request).await },
            )
            .await
    };
}

// For state-writing RPCs that deliberately skip the per-sandbox limiter: bulk
// or non-sandbox-scoped operations, and sandbox stop, kill and delete, which
// must stay available while a sandbox's calls hold its permits. They still
// receive the restart-drain marker, so they must still replay across a handoff.
macro_rules! fwd_sbx_handoff {
    ($service:expr, $method:literal, $rpc:ident, $req:expr) => {
        $service
            .sbx_unary_with_handoff(
                $method,
                RpcReplaySafety::RequiresProof,
                $req,
                |mut client, request| async move { client.$rpc(request).await },
            )
            .await
    };
}

// Read-only RPCs are inherently replay-safe, so they also replay ambiguous
// transport failures through the handoff machinery. During the post-handoff
// RPC drain the interceptor marks every method — reads included — so these
// must go through the handoff helper too.
macro_rules! fwd_sbx_read {
    ($service:expr, $method:literal, $rpc:ident, $req:expr) => {
        $service
            .sbx_unary_with_handoff(
                $method,
                RpcReplaySafety::ReadOnly,
                $req,
                |mut client, request| async move { client.$rpc(request).await },
            )
            .await
    };
}

// Forward in the caller's task: a caller that disconnects then drops the
// sandbox-manager call, which is how an abandoned process start is cancelled.
// Spawning the forward into an owned task would break that chain.
macro_rules! fwd_sbx_stream {
    ($service:expr, $method:literal, $rpc:ident, $req:expr) => {
        $service
            .limited_sbx_server_stream($method, $req, |mut client, request| async move {
                client.$rpc(request).await
            })
            .await
    };
}

fn allow_port_request_params(req: &pplx_space_node_mgr_client::proto::AllowPortRequest) -> String {
    let protocol = pplx_space_node_mgr_client::proto::NetworkProtocol::try_from(req.protocol)
        .map(|protocol| protocol.as_str_name().to_ascii_lowercase())
        .unwrap_or_else(|_| req.protocol.to_string());
    pplx_space_common_svc::format_grpc_request_params([
        ("sandbox_id", req.sandbox_id.clone()),
        ("port", req.port.to_string()),
        ("protocol", protocol),
    ])
}

fn reject_public_netns_path(rpc: &'static str, netns_path: &Option<String>) -> Result<(), Status> {
    if netns_path.is_some() {
        return Err(Status::invalid_argument(format!(
            "{rpc}.netns_path is internal-only; use WorkerApiInternal"
        )));
    }
    Ok(())
}

fn reject_public_suspend_restore_context(
    rpc: &'static str,
    storage_uri: &Option<String>,
    source_node: &Option<String>,
) -> Result<(), Status> {
    if storage_uri.is_some() || source_node.is_some() {
        return Err(Status::invalid_argument(format!(
            "{rpc}.suspend_restore context is internal-only; use WorkerApiInternal"
        )));
    }
    Ok(())
}

fn reject_public_operation_activation_attempt(operation: &Option<String>) -> Result<(), Status> {
    if operation.is_some() {
        return Err(Status::invalid_argument(
            "StartSandbox.operation_activation_attempt_id is internal-only; use WorkerApiInternal",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl WorkerApi for WorkerApiService {
    type SpacedListStream = PinnedResponseStream<pplx_space_sbx_mgr_client::proto::SpacedFileInfo>;
    type SpacedReadStream = PinnedResponseStream<pplx_space_sbx_mgr_client::proto::SpacedFileChunk>;
    type SpacedWatchStream =
        PinnedResponseStream<pplx_space_sbx_mgr_client::proto::SpacedFileEvent>;
    type SpacedStartProcessStream =
        PinnedResponseStream<pplx_space_sbx_mgr_client::proto::SpacedProcessEvent>;
    type SpacedConnectProcessStream =
        PinnedResponseStream<pplx_space_sbx_mgr_client::proto::SpacedProcessEvent>;
    type ServicePortIngressStream = PinnedResponseStream<ServicePortIngressFrame>;

    // ── Sandbox lifecycle (sandbox-manager) ──────────────────────────────────

    async fn create_sandbox(
        &self,
        mut r: Request<pplx_space_sbx_mgr_client::proto::CreateRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::CreateResponse>, Status> {
        // Before any check, so a refused create names its sandbox too.
        if let Some(sandbox_id) = r.get_ref().sandbox_id.as_deref() {
            record_sandbox_id(&tracing::Span::current(), sandbox_id);
        }
        crate::template_lookup::bind_create_caller_scope(&mut r)?;
        // Overrides and nonzero generations require policy authority. A generation
        // persists even without an override and can block later sandbox-specific
        // policy corrections. No override and generation zero retain inheritance.
        {
            let req = r.get_ref();
            if req.network_policy.is_some()
                || req.auto_lifecycle.is_some()
                || req.network_policy_generation != 0
            {
                require_policy_mutation_authority(&r)?;
            }
        }
        if r.get_ref().autostart {
            self.ensure_disk_allocation_open("CreateSandbox").await?;
        }
        let response = fwd_sbx_handoff!(self, "CreateSandbox", create, r)?;
        // A create that names no sandbox learns its ID from the response.
        record_sandbox_id(&tracing::Span::current(), &response.get_ref().sandbox_id);
        Ok(response)
    }

    async fn start_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::StartRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::StartResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        reject_public_netns_path("StartSandbox", &r.get_ref().netns_path)?;
        reject_public_operation_activation_attempt(&r.get_ref().operation_activation_attempt_id)?;
        reject_public_suspend_restore_context(
            "StartSandbox",
            &r.get_ref().suspend_restore_storage_uri,
            &r.get_ref().suspend_restore_source_node,
        )?;
        if r.get_ref().activation_auto_lifecycle.is_some() {
            require_policy_mutation_authority(&r)?;
        }
        self.ensure_disk_allocation_open("StartSandbox").await?;
        fwd_sbx!(self, "StartSandbox", start, r)
    }

    async fn stop_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::StopRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::StopResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        fwd_sbx_handoff!(self, "StopSandbox", stop, r)
    }

    async fn kill_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::KillRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::KillResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        fwd_sbx_handoff!(self, "KillSandbox", kill, r)
    }

    async fn delete_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::DeleteRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::DeleteResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        fwd_sbx_handoff!(self, "DeleteSandbox", delete, r)
    }

    async fn sandbox_state(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::StateRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::StateResponse>, Status> {
        if let [sandbox_id] = r.get_ref().sandbox_ids.as_slice() {
            record_sandbox_id(&tracing::Span::current(), sandbox_id);
        }
        fwd_sbx_read!(self, "SandboxState", state, r)
    }

    // ── Sandbox state management (sandbox-manager) ───────────────────────────

    async fn pause_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::PauseRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::PauseResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        fwd_sbx!(self, "PauseSandbox", pause, r)
    }

    async fn resume_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::ResumeRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::ResumeResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        if r.get_ref().activation_auto_lifecycle.is_some() {
            require_policy_mutation_authority(&r)?;
        }
        fwd_sbx!(self, "ResumeSandbox", resume, r)
    }

    async fn create_sandbox_snapshot(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::CreateSnapshotRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::CreateSnapshotResponse>, Status> {
        fwd_sbx!(self, "CreateSandboxSnapshot", create_snapshot, r)
    }

    async fn suspend_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SuspendRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SuspendResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        fwd_sbx!(self, "SuspendSandbox", suspend, r)
    }

    async fn prepare_teardown_suspend(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse>, Status>
    {
        fwd_sbx!(self, "PrepareTeardownSuspend", prepare_teardown_suspend, r)
    }

    async fn enqueue_sandbox_artifact_backup(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupResponse>, Status>
    {
        fwd_sbx!(
            self,
            "EnqueueSandboxArtifactBackup",
            enqueue_artifact_backup,
            r
        )
    }

    // ── Sandbox operations (sandbox-manager) ─────────────────────────────────

    async fn sandbox_stats(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::StatsRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::StatsResponse>, Status> {
        fwd_sbx!(self, "SandboxStats", stats, r)
    }

    async fn update_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::UpdateRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::UpdateResponse>, Status> {
        fwd_sbx!(self, "UpdateSandbox", update, r)
    }

    async fn set_sandbox_auto_lifecycle_policy(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyResponse>, Status>
    {
        require_policy_mutation_authority(&r)?;
        fwd_sbx!(
            self,
            "SetSandboxAutoLifecyclePolicy",
            set_auto_lifecycle_policy,
            r
        )
    }

    async fn touch_sandbox_auto_lifecycle(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::TouchAutoLifecycleRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::TouchAutoLifecycleResponse>, Status>
    {
        fwd_sbx!(self, "TouchSandboxAutoLifecycle", touch_auto_lifecycle, r)
    }

    async fn set_sandbox_network_policy(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SetNetworkPolicyRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SetNetworkPolicyResponse>, Status> {
        require_policy_mutation_authority(&r)?;
        fwd_sbx!(self, "SetSandboxNetworkPolicy", set_network_policy, r)
    }

    async fn spaced_get_status(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedGetStatusRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedGetStatusResponse>, Status> {
        let sandbox_id = r.get_ref().sandbox_id.clone();
        let client = self.clients.sbx();
        self.limited_sbx_operation("SpacedGetStatus", &sandbox_id, move || {
            crate::restart_handoff::call_with_restart_handoff(
                "SpacedGetStatus",
                RpcReplaySafety::ReadOnly,
                r,
                client,
                |mut client, request| async move { client.spaced_get_status(request).await },
            )
        })
        .await
    }

    async fn spaced_wait_ready(
        &self,
        mut r: Request<pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedWaitReadyResponse>, Status> {
        prepare_wait_ready_forward(&mut r);
        let sandbox_id = r.get_ref().sandbox_id.clone();
        let client = self.clients.sbx();
        self.limited_sbx_operation("SpacedWaitReady", &sandbox_id, move || {
            crate::restart_handoff::call_with_restart_handoff(
                "SpacedWaitReady",
                RpcReplaySafety::ReadOnly,
                r,
                client,
                |mut client, request| async move { client.spaced_wait_ready(request).await },
            )
        })
        .await
    }

    async fn spaced_stat(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedStatRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedStatResponse>, Status> {
        fwd_sbx!(self, "SpacedStat", spaced_stat, r)
    }

    async fn spaced_list(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedListRequest>,
    ) -> Result<Response<Self::SpacedListStream>, Status> {
        fwd_sbx_stream!(self, "SpacedList", spaced_list, r)
    }

    async fn spaced_read(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedReadRequest>,
    ) -> Result<Response<Self::SpacedReadStream>, Status> {
        fwd_sbx_stream!(self, "SpacedRead", spaced_read, r)
    }

    // Client-streaming: the request body is consumed as it streams and
    // cannot be rebuilt, so a restart-drain marker cannot be replayed here.
    // It surfaces to the caller, which owns any retry of the whole stream.
    async fn spaced_write(
        &self,
        r: Request<tonic::Streaming<pplx_space_sbx_mgr_client::proto::SpacedWriteRequest>>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedWriteResponse>, Status> {
        let metadata =
            pplx_space_common_svc::space_api_route::space_api_route_token_metadata(r.metadata());
        let node_permit = self
            .uds_per_sandbox_limiter
            .acquire_node("SpacedWrite")
            .await?;
        let mut input = r.into_inner();
        let first = crate::client_stream::first(&mut input, "SpacedWrite")
            .await?
            .ok_or_else(|| {
                Status::invalid_argument("SpacedWrite stream must contain at least one message")
            })?;
        let sandbox_id = first.sandbox_id.clone();
        let _permit = self
            .uds_per_sandbox_limiter
            .acquire_with_node("SpacedWrite", &sandbox_id, node_permit)
            .await?;
        let mut client = self.clients.sbx();
        crate::client_stream::forward(
            input,
            Some(first),
            "SpacedWrite",
            move |message| {
                if message.sandbox_id != sandbox_id {
                    return Err(Status::invalid_argument(
                        "SpacedWrite stream changed sandbox_id after the first message",
                    ));
                }
                Ok(())
            },
            move |stream| async move {
                let mut request = Request::new(stream);
                *request.metadata_mut() = metadata;
                client.spaced_write(request).await
            },
        )
        .await
    }

    async fn spaced_write_batch(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedWriteBatchRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedWriteBatchResponse>, Status> {
        fwd_sbx!(self, "SpacedWriteBatch", spaced_write_batch, r)
    }

    async fn spaced_mkdir(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedMkdirRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedMkdirResponse>, Status> {
        fwd_sbx!(self, "SpacedMkdir", spaced_mkdir, r)
    }

    async fn spaced_remove(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedRemoveRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedRemoveResponse>, Status> {
        fwd_sbx!(self, "SpacedRemove", spaced_remove, r)
    }

    async fn spaced_rename(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedRenameRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedRenameResponse>, Status> {
        fwd_sbx!(self, "SpacedRename", spaced_rename, r)
    }

    async fn spaced_watch(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedWatchRequest>,
    ) -> Result<Response<Self::SpacedWatchStream>, Status> {
        fwd_sbx_stream!(self, "SpacedWatch", spaced_watch, r)
    }

    async fn spaced_create_watcher(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedCreateWatcherRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedCreateWatcherResponse>, Status>
    {
        fwd_sbx!(self, "SpacedCreateWatcher", spaced_create_watcher, r)
    }

    async fn spaced_normalize_watch_root(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedNormalizeWatchRootRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedNormalizeWatchRootResponse>, Status>
    {
        fwd_sbx!(
            self,
            "SpacedNormalizeWatchRoot",
            spaced_normalize_watch_root,
            r
        )
    }

    async fn spaced_get_watcher_events(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedGetWatcherEventsRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedGetWatcherEventsResponse>, Status>
    {
        fwd_sbx!(self, "SpacedGetWatcherEvents", spaced_get_watcher_events, r)
    }

    async fn spaced_remove_watcher(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedRemoveWatcherRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedRemoveWatcherResponse>, Status>
    {
        fwd_sbx!(self, "SpacedRemoveWatcher", spaced_remove_watcher, r)
    }

    async fn spaced_enable_file_changes(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedEnableFileChangesRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedEnableFileChangesResponse>, Status>
    {
        fwd_sbx!(
            self,
            "SpacedEnableFileChanges",
            spaced_enable_file_changes,
            r
        )
    }

    async fn spaced_disable_file_changes(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedDisableFileChangesRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedDisableFileChangesResponse>, Status>
    {
        fwd_sbx!(
            self,
            "SpacedDisableFileChanges",
            spaced_disable_file_changes,
            r
        )
    }

    async fn spaced_get_modified_files(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedGetModifiedFilesRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedGetModifiedFilesResponse>, Status>
    {
        fwd_sbx!(self, "SpacedGetModifiedFiles", spaced_get_modified_files, r)
    }

    async fn spaced_start_process(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedStartProcessRequest>,
    ) -> Result<Response<Self::SpacedStartProcessStream>, Status> {
        fwd_sbx_stream!(self, "SpacedStartProcess", spaced_start_process, r)
    }

    async fn spaced_connect_process(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedConnectProcessRequest>,
    ) -> Result<Response<Self::SpacedConnectProcessStream>, Status> {
        fwd_sbx_stream!(self, "SpacedConnectProcess", spaced_connect_process, r)
    }

    async fn spaced_list_processes(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedListProcessesRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedListProcessesResponse>, Status>
    {
        fwd_sbx_reserved!(self, "SpacedListProcesses", spaced_list_processes, r)
    }

    async fn spaced_send_signal(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedSendSignalRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedSendSignalResponse>, Status> {
        fwd_sbx_reserved!(self, "SpacedSendSignal", spaced_send_signal, r)
    }

    async fn spaced_send_input(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedSendInputRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedSendInputResponse>, Status> {
        fwd_sbx!(self, "SpacedSendInput", spaced_send_input, r)
    }

    async fn spaced_close_stdin(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedCloseStdinRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedCloseStdinResponse>, Status> {
        fwd_sbx!(self, "SpacedCloseStdin", spaced_close_stdin, r)
    }

    async fn spaced_resize_pty(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedResizePtyRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedResizePtyResponse>, Status> {
        fwd_sbx!(self, "SpacedResizePty", spaced_resize_pty, r)
    }

    async fn spaced_kill_process(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedKillProcessRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedKillProcessResponse>, Status> {
        fwd_sbx_reserved!(self, "SpacedKillProcess", spaced_kill_process, r)
    }

    async fn spaced_open_loopback_bridge(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeResponse>, Status>
    {
        fwd_sbx!(
            self,
            "SpacedOpenLoopbackBridge",
            spaced_open_loopback_bridge,
            r
        )
    }

    async fn spaced_close_loopback_bridge(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::SpacedCloseLoopbackBridgeRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::SpacedCloseLoopbackBridgeResponse>, Status>
    {
        fwd_sbx!(
            self,
            "SpacedCloseLoopbackBridge",
            spaced_close_loopback_bridge,
            r
        )
    }

    async fn service_port_ingress(
        &self,
        r: Request<tonic::Streaming<ServicePortIngressFrame>>,
    ) -> Result<Response<Self::ServicePortIngressStream>, Status> {
        let mut metadata =
            pplx_space_common_svc::space_api_route::space_api_route_token_metadata(r.metadata());
        // Temporary rollout opt-in; remove after completion framing is universal.
        if let Some(value) = r
            .metadata()
            .get(pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA)
        {
            metadata.insert(
                pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA,
                value.clone(),
            );
        }
        let mut inbound = r.into_inner();
        let first = read_service_port_ingress_first_frame(
            inbound.message(),
            SERVICE_PORT_INGRESS_FIRST_FRAME_TIMEOUT,
        )
        .await?;
        let (sandbox_id, port) = match first.message.as_ref() {
            Some(
                pplx_space_net_gateway_client::proto::service_port_ingress_frame::Message::Start(
                    start,
                ),
            ) => (
                start
                    .sandbox_id
                    .parse::<SandboxId>()
                    .map_err(|_| Status::invalid_argument("sandbox_id is invalid"))?,
                pplx_space_common_core::types::GuestPort::try_from_u16(
                    u16::try_from(start.port)
                        .map_err(|_| Status::invalid_argument("service port out of range"))?,
                )
                .map_err(|_| Status::invalid_argument("service port is invalid"))?,
            ),
            _ => {
                return Err(Status::invalid_argument(
                    "service port ingress first frame must be start",
                ));
            }
        };
        let trace_context = pplx_space_common_grpc::SandboxTraceContext::new(sandbox_id, port);
        trace_context.record(&tracing::Span::current());
        let sandbox_id_string = sandbox_id.to_string();
        let ingress_permit = self
            .acquire_service_port_ingress_permit(&sandbox_id_string)
            .await?;
        let shutdown_token = self.shutdown_token.child_token();
        let rest = futures_util::stream::unfold(
            (inbound, sandbox_id, shutdown_token),
            next_service_port_ingress_frame,
        );
        let request_stream = tokio_stream::iter(std::iter::once(first)).chain(rest);
        let mut request = Request::new(request_stream);
        *request.metadata_mut() = metadata;
        request.extensions_mut().insert(trace_context);
        // Bidirectional stream: frames are consumed as they arrive and cannot
        // be rebuilt, so a restart-drain marker at establishment is not
        // replayed; the ingress caller owns reconnecting the whole stream.
        let response = self.clients.sbx().service_port_ingress(request).await?;
        Ok(response.map(|stream| guard_stream(stream, ingress_permit)))
    }

    // ── Sandbox snapshots (sandbox-manager) ──────────────────────────────────

    async fn list_snapshots(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::ListSnapshotsRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::ListSnapshotsResponse>, Status> {
        fwd_sbx_read!(self, "ListSandboxSnapshots", list_snapshots, r)
    }

    async fn rollback_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::RollbackRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::RollbackResponse>, Status> {
        reject_public_netns_path("RollbackSandbox", &r.get_ref().netns_path)?;
        fwd_sbx!(self, "RollbackSandbox", rollback, r)
    }

    // ── Template management (template-manager) ──────────────────────────────

    async fn delete_template(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::DeleteTemplateRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::DeleteTemplateResponse>, Status>
    {
        pplx_space_common_svc::auth::template::require_template_mutation_authority(&r)?;
        fwd!(self.clients.tmpl(), delete_template, r)
    }

    async fn delete_template_build(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::DeleteTemplateBuildRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::DeleteTemplateBuildResponse>, Status>
    {
        pplx_space_common_svc::auth::template::require_template_mutation_authority(&r)?;
        fwd!(self.clients.tmpl(), delete_template_build, r)
    }

    // ── Network (node-manager) ───────────────────────────────────────────────

    async fn expose_port(
        &self,
        r: Request<pplx_space_node_mgr_client::proto::ExposePortRequest>,
    ) -> Result<Response<pplx_space_node_mgr_client::proto::ExposePortResponse>, Status> {
        fwd!(self.clients.node(), expose_port, r)
    }

    async fn allow_port(
        &self,
        r: Request<pplx_space_node_mgr_client::proto::AllowPortRequest>,
    ) -> Result<Response<pplx_space_node_mgr_client::proto::AllowPortResponse>, Status> {
        let request_params = allow_port_request_params(r.get_ref());
        pplx_space_common_svc::record_safe_grpc_request_params(&request_params);
        let result = self.clients.node().allow_port(r).await;
        if let Err(status) = &result {
            pplx_space_common_svc::log_failed_grpc_request_params(status, &request_params);
        }
        result
    }

    // ── Custom orchestration ─────────────────────────────────────────────────

    async fn trigger_state_reconciliation(
        &self,
        r: Request<TriggerStateReconciliationRequest>,
    ) -> Result<Response<TriggerStateReconciliationResponse>, Status> {
        node_ctl::trigger_state_reconciliation(r.into_inner()).await?;
        Ok(Response::new(TriggerStateReconciliationResponse {}))
    }

    // ── Template transfer ────────────────────────────────────────────────────

    async fn pull_template(
        &self,
        mut r: Request<crate::proto::PullTemplateRequest>,
    ) -> Result<Response<crate::proto::PullTemplateResponse>, Status> {
        pplx_space_common_svc::auth::template_transfer::require_template_transfer_authority(&r)?;
        let scope = crate::template_lookup::bind_caller_scope(&mut r)?;
        let req = r.into_inner();
        let resp = crate::transfer::pull_template(&mut self.clients.tmpl(), &req, scope).await?;
        Ok(Response::new(resp))
    }

    type NodeExportTemplateStream =
        tonic::Streaming<pplx_space_template_mgr_client::proto::ExportTemplateChunk>;

    async fn node_export_template(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::ExportTemplateRequest>,
    ) -> Result<Response<Self::NodeExportTemplateStream>, Status> {
        let mut r = r;
        pplx_space_common_svc::auth::template_transfer::prepare_template_export(&mut r)?;
        Ok(self.clients.tmpl().export_template(r).await?)
    }

    async fn list_node_templates(
        &self,
        _r: Request<crate::proto::ListNodeTemplatesRequest>,
    ) -> Result<Response<crate::proto::ListNodeTemplatesResponse>, Status> {
        let request =
            Request::new(pplx_space_template_mgr_client::proto::ListTemplatesRequest::default());
        let resp = fwd_retry!(
            "template-manager",
            self.clients.tmpl(),
            list_templates,
            request
        )
        .map_err(|e| Status::internal(format!("list_templates: {e}")))?;

        let templates = resp
            .into_inner()
            .templates
            .into_iter()
            .map(|t| crate::proto::NodeTemplateBuild {
                name: t.name,
                content_hash: t.content_hash,
                build_date: t.built_at,
                artifact_size_bytes: t.artifact_size_bytes,
                cache_size_bytes: t.cache_size_bytes,
                disk_size_mb: t.disk_size_mb,
                runtime: t.runtime,
                arch: t.arch,
                snapshots: t
                    .snapshots
                    .into_iter()
                    .map(|s| crate::proto::SnapshotInfo {
                        vcpu: s.vcpu,
                        memory_mib: s.memory_mib,
                        snapshot_hash: s.snapshot_hash,
                    })
                    .collect(),
            })
            .collect();

        Ok(Response::new(crate::proto::ListNodeTemplatesResponse {
            builds: templates,
        }))
    }
}

#[tonic::async_trait]
impl WorkerApiInternal for WorkerApiService {
    async fn adopt_pool_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::AdoptPoolSandboxRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::AdoptPoolSandboxResponse>, Status> {
        // As with cold create, policy overrides and nonzero generations require
        // the control-plane capability, including generation-only requests.
        {
            let req = r.get_ref();
            if req.network_policy.is_some()
                || req.auto_lifecycle.is_some()
                || req.network_policy_generation != 0
            {
                require_policy_mutation_authority(&r)?;
            }
        }
        let response = fwd_sbx_handoff!(self, "AdoptPoolSandbox", adopt_pool_sandbox, r)?;
        record_sandbox_id(&tracing::Span::current(), &response.get_ref().sandbox_id);
        Ok(response)
    }

    async fn wake_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::WakeRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::WakeResponse>, Status> {
        record_sandbox_id(&tracing::Span::current(), &r.get_ref().sandbox_id);
        // A runtime upgrade discards a suspended sandbox's memory under an
        // authorization the caller names, so it takes the same control-plane
        // authority as a policy write.
        if r.get_ref().runtime_upgrade.is_some()
            || r.get_ref()
                .start_context
                .as_ref()
                .is_some_and(|context| context.activation_auto_lifecycle.is_some())
        {
            require_policy_mutation_authority(&r)?;
        }
        fwd_sbx!(self, "InternalWakeSandbox", wake, r)
    }
}

#[cfg(test)]
mod tests;
