use std::time::Duration;

use pplx_space_common_core::sql;
use tonic::transport::Endpoint;
use tonic::{Code, Request, Status};

use pplx_space_common_core::types::{
    ExecutionGeneration, GuestPort, NodeDrainPhase, NodeDrainState, NodeId, NodeManagedBy, PodUid,
    RouteGeneration, SandboxActivationAttemptId, SandboxId,
};
use pplx_space_common_grpc::sandbox_service_route_not_ready_status;
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Authority, insert_space_api_route_ed25519_token,
};
use pplx_space_node_api_client::{NodeApiClient, WorkerApiClient};
use pplx_space_node_mgr_client::proto::{AllowPortRequest, NetworkProtocol};
use pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest;
use pplx_space_sbx_mgr_client::proto::{
    SpacedLoopbackBridgeStatus, SpacedOpenLoopbackBridgeRequest,
};

use crate::route_identity::{SandboxRouteIdentityParts, route_identity_for_signing};

const SANDBOX_SERVICE_ROUTE_APPLY_RPC_TIMEOUT: Duration = Duration::from_secs(3);
const SANDBOX_SERVICE_ROUTE_LOOPBACK_WAIT_TIMEOUT: prost_types::Duration = prost_types::Duration {
    seconds: 2,
    nanos: 0,
};

#[derive(Debug, Clone)]
pub(super) struct SandboxServicePortRouteIdentity {
    pub sandbox_id: SandboxId,
    pub active_node_id: NodeId,
    pub route_generation: RouteGeneration,
    pub execution_generation: Option<ExecutionGeneration>,
    pub pod_uid: Option<PodUid>,
    pub substrate_kind: Option<NodeManagedBy>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
}

pub(super) async fn ensure_sandbox_service_port_ingress_path(
    pool: &sqlx::PgPool,
    identity: SandboxServicePortRouteIdentity,
    port: GuestPort,
    backend_endpoint: &str,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<(), Status> {
    ensure_sandbox_service_route_node_not_draining(
        pool,
        identity.sandbox_id,
        identity.active_node_id,
    )
    .await?;
    // Temporary rollout fallback: old workers only prepare localhost bridges
    // through this RPC. Remove this explicit setup once every worker performs
    // it at attachment; the local protection flag is not a worker capability.
    apply_open_sandbox_service_port(
        identity,
        port.get().get(),
        backend_endpoint,
        route_ed25519_authority,
    )
    .await
}

#[cfg(test)]
#[path = "sandbox_service_ports_tests.rs"]
mod sandbox_service_ports_tests;

async fn ensure_sandbox_service_route_node_not_draining(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    active_node_id: NodeId,
) -> Result<(), Status> {
    // Read the mode alongside the phase. A non-NULL phase is not on its own a
    // reason to refuse: under a gentle drain the sandbox is deliberately left
    // where it is and keeps serving traffic, and opening a service port *is*
    // serving traffic. Blocking on the phase alone made ingress the one access
    // path a gentle drain still broke, while every user-API path stayed open.
    //
    // `NodeDrainState::blocks_placement` is the shared truth table, so this
    // guard and the user-API drain gates cannot drift: `forced` + `draining`
    // races the evacuation pipeline, and `drained` / `terminating` block in
    // either mode because the node is about to be destroyed.
    let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(sql!(
        "SELECT drain_phase, drain_mode FROM nodes WHERE id = $1"
    ))
    .bind(active_node_id.as_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        Status::internal(format!(
            "load sandbox service route node drain state: {error}"
        ))
    })?;
    let Some((drain_phase, drain_mode)) = row else {
        return Ok(());
    };
    let drain_phase = drain_phase
        .as_deref()
        .map(NodeDrainPhase::parse)
        .transpose()
        .map_err(|err| Status::internal(format!("node has an unrecognized drain phase: {err}")))?;
    let drain_state = NodeDrainState::from_columns(drain_phase, drain_mode.as_deref());
    if let Some(state) = drain_state.filter(|state| state.blocks_placement()) {
        return Err(Status::failed_precondition(format!(
            "sandbox service route cannot open port while sandbox {sandbox_id} is on a \
             {phase} node draining in {mode} mode",
            phase = state.phase,
            mode = state.mode,
        )));
    }
    Ok(())
}

async fn apply_open_sandbox_service_port(
    identity: SandboxServicePortRouteIdentity,
    port: u16,
    backend_endpoint: &str,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<(), Status> {
    let mut node_api = worker_node_api(backend_endpoint, SANDBOX_SERVICE_ROUTE_APPLY_RPC_TIMEOUT)
        .map_err(|status| {
        sandbox_service_route_not_ready_status(
            Code::Unavailable,
            format!("sandbox service route node endpoint is invalid: {status}"),
        )
    })?;
    let mut client = WorkerApiClient::new(&mut node_api);
    let sandbox_id = identity.sandbox_id.to_string();
    let port = u32::from(port);

    client
        .allow_port(
            AllowPortRequest {
                sandbox_id: sandbox_id.clone(),
                port,
                protocol: NetworkProtocol::Tcp as i32,
            },
            SANDBOX_SERVICE_ROUTE_APPLY_RPC_TIMEOUT,
        )
        .await
        .map_err(|status| {
            sandbox_service_route_not_ready_status(
                Code::Unavailable,
                format!("sandbox service route allow port failed for port {port}: {status}"),
            )
        })?;

    let request = open_loopback_bridge_request(
        identity,
        SpacedOpenLoopbackBridgeRequest {
            sandbox_id,
            operation: Some(OpenLoopbackBridgeRequest {
                port,
                wait_timeout: Some(SANDBOX_SERVICE_ROUTE_LOOPBACK_WAIT_TIMEOUT),
            }),
        },
        route_ed25519_authority,
    )?;
    let response = client
        .spaced_open_loopback_bridge_with_metadata(request, SANDBOX_SERVICE_ROUTE_APPLY_RPC_TIMEOUT)
        .await
        .map_err(|status| {
            sandbox_service_route_not_ready_status(
                Code::Unavailable,
                format!("sandbox service route loopback bridge failed for port {port}: {status}"),
            )
        })?
        .into_inner();
    let status = SpacedLoopbackBridgeStatus::try_from(response.status)
        .unwrap_or(SpacedLoopbackBridgeStatus::Unspecified);
    match status {
        SpacedLoopbackBridgeStatus::Direct | SpacedLoopbackBridgeStatus::LoopbackForwarded => {
            Ok(())
        }
        SpacedLoopbackBridgeStatus::WaitingForListener
        | SpacedLoopbackBridgeStatus::BindConflict
        | SpacedLoopbackBridgeStatus::Failed
        | SpacedLoopbackBridgeStatus::Unspecified => Err(sandbox_service_route_not_ready_status(
            Code::Unavailable,
            format!(
                "sandbox service route loopback bridge not ready for port {port}: status={status:?} detail={}",
                response.detail
            ),
        )),
    }
}

fn open_loopback_bridge_request(
    identity: SandboxServicePortRouteIdentity,
    message: SpacedOpenLoopbackBridgeRequest,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<Request<SpacedOpenLoopbackBridgeRequest>, Status> {
    let mut request = Request::new(message);
    let route_identity = route_identity_for_signing(&SandboxRouteIdentityParts {
        sandbox_id: identity.sandbox_id,
        route_generation: identity.route_generation,
        current_node_id: identity.active_node_id,
        substrate_kind: identity.substrate_kind,
        execution_generation: identity.execution_generation,
        activation_attempt_id: identity.activation_attempt_id,
        pod_uid: identity.pod_uid,
    })?;
    let authority = route_ed25519_authority.ok_or_else(|| {
        Status::failed_precondition("SPACE API route signing authority is unavailable")
    })?;
    let token = authority
        .mint_space_api_bridge(route_identity)
        .map_err(|error| Status::internal(format!("mint route token: {error}")))?;
    insert_space_api_route_ed25519_token(request.metadata_mut(), &token)
        .map_err(|error| Status::internal(format!("attach route token: {error}")))?;
    Ok(request)
}

pub(super) fn worker_node_api(
    endpoint: &str,
    rpc_timeout: Duration,
) -> Result<NodeApiClient, Status> {
    let endpoint = normalize_node_api_endpoint(endpoint)?;
    let endpoint = Endpoint::from_shared(endpoint)
        .map_err(|error| Status::internal(format!("invalid node-api endpoint: {error}")))?
        .tcp_nodelay(true)
        .connect_timeout(SANDBOX_SERVICE_ROUTE_APPLY_RPC_TIMEOUT)
        .timeout(rpc_timeout)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(60));
    Ok(NodeApiClient::with_service_name(
        "worker-node-api",
        endpoint,
    ))
}

fn normalize_node_api_endpoint(endpoint: &str) -> Result<String, Status> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(Status::failed_precondition(
            "sandbox service route has empty node-api endpoint",
        ));
    }
    Ok(pplx_space_common_grpc::internal_transport::qualify_endpoint(endpoint))
}
