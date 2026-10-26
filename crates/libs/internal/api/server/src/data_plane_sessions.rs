use pplx_space_common_core::sql;
use std::time::Duration;

use sqlx::FromRow;
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use tonic::{Code, Request, Status};
use uuid::Uuid;

use pplx_space_cluster_trigger_queue::enqueue_triggers_tx;
use pplx_space_common_core::types::{
    EcwCredentialEpoch, ExecutionGeneration, GuestPort, NodeId, NodeManagedBy, OrgId, PodUid,
    RouteGeneration, SandboxActivationAttemptId, SandboxId, SandboxLifecycle,
};
use pplx_space_common_grpc::sandbox_service_route_not_ready_status;
use pplx_space_common_svc::auth::DataPlaneSessionRefreshAuthority;
use pplx_space_common_svc::ecw_credential::{
    EcwCredentialKeyring, EcwCredentialKind, EcwCredentialMatch,
};
use pplx_space_common_svc::space_api_route::SpaceApiRoutePurpose;
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Authority, SpaceApiRouteEd25519Token, insert_space_api_route_ed25519_token,
};
use pplx_space_common_svc::types::{ReconcileReason, ReconcileRef};
use pplx_space_metrics::internal_api::EcwCredentialVerificationResult;
use pplx_space_node_api_client::WorkerApiClient;
use pplx_space_node_api_client::proto::ImplicitWakeActivity;
use pplx_space_public_api::SessionKind;
use pplx_space_sandbox_store as sandbox_store;
use pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest;

use crate::proto::{
    BindDataPlaneSessionProcessRequest, BindDataPlaneSessionProcessResponse,
    DataPlaneBackendEndpoint, DataPlaneReconnectReason, DataPlaneRevocationState,
    DataPlaneRevocationStatus, GetDataPlaneOrgRevocationEpochRequest,
    GetDataPlaneOrgRevocationEpochResponse, GetDataPlaneSessionRevocationStateRequest,
    GetDataPlaneSessionRevocationStateResponse, GetSandboxRuntimeAccessRequest,
    GetSandboxRuntimeAccessResponse, RecordDataPlaneSessionReconnectRequest,
    RecordDataPlaneSessionReconnectResponse, ResolveDataPlaneSessionRouteRequest,
    ResolveDataPlaneSessionRouteResponse, SandboxRuntimeAccess, SandboxRuntimeAccessDecision,
    SandboxServiceRouteCredentialKind, SandboxServiceRouteKind, SandboxServiceRouteTransport,
    SpaceApiRouteToken as ProtoSpaceApiRouteToken, UnstableResetSandboxServiceRouteRequest,
    UnstableResetSandboxServiceRouteResponse, UnstableResetSandboxServiceRouteResult,
    UnstableResolveSandboxServiceRouteRequest, UnstableResolveSandboxServiceRouteResponse,
};
use crate::route_identity::{SandboxRouteIdentityParts, route_identity_for_signing};
use crate::sandbox_service_ports::{
    SandboxServicePortRouteIdentity, ensure_sandbox_service_port_ingress_path, worker_node_api,
};

mod ecw_authorization;
mod process_binding;
mod revocation;
mod route_rows;
mod route_token;
mod runtime_access;
mod service_route;
mod service_route_reset;
mod session_route;

use ecw_authorization::*;
pub(crate) use process_binding::*;
pub(crate) use revocation::*;
use route_rows::*;
use route_token::*;
pub(crate) use runtime_access::*;
pub(crate) use service_route::*;
pub(crate) use service_route_reset::*;
pub(crate) use session_route::*;

const BACKEND_TRANSPORT_GRPC: &str = "grpc";

fn required(value: &str, field: &str) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    Ok(value.to_string())
}

fn optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests;

fn activation_may_repair(ingress: bool, error: &Status) -> bool {
    ingress
        && matches!(
            error.code(),
            Code::Unavailable | Code::FailedPrecondition | Code::NotFound | Code::Aborted
        )
}

/// No data-plane route resolves to a sandbox that is `restoring`: its
/// restore can still fail and be rolled back, and the rollback retires the
/// execution and deletes its pod, discarding whatever it served. The
/// resolvers turn this refusal into a wait on the restore through the
/// activation owner, then retry, and return it (retryable `Unavailable`)
/// when the restore didn't reach `started` in their budget.
fn refuse_restoring_route(lifecycle: SandboxLifecycle) -> Result<(), Status> {
    if lifecycle == SandboxLifecycle::Restoring {
        Err(sandbox_service_route_not_ready_status(
            Code::Unavailable,
            "sandbox is restoring; retry once the restore completes",
        ))
    } else {
        Ok(())
    }
}

fn require_started_ingress(lifecycle: SandboxLifecycle) -> Result<(), Status> {
    if lifecycle == SandboxLifecycle::Started {
        Ok(())
    } else {
        Err(sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            "sandbox execution is not started",
        ))
    }
}

/// Preparing the existing path is idempotent and sends no application bytes.
/// Reserve at least three quarters of the remaining budget for activation and
/// attachment when the current worker stalls, with a one-second probe ceiling.
async fn prepare_current_ingress_worker(
    deadline: tokio::time::Instant,
    can_activate: bool,
    prepare: impl std::future::Future<Output = Result<(), Status>>,
) -> Result<(), Status> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let budget = if can_activate {
        (remaining / 4).min(Duration::from_secs(1))
    } else {
        remaining
    };
    tokio::time::timeout(budget, prepare)
        .await
        .map_err(|_| Status::unavailable("current ingress worker preparation timed out"))?
}
