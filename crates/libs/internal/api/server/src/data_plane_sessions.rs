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

const DEFAULT_NODE_API_PORT: u16 = 9800;
const BACKEND_TRANSPORT_GRPC: &str = "grpc";
const RESET_ROUTE_DIAGNOSTIC_MAX_CHARS: usize = 512;
const SANDBOX_RUNTIME_CONVERGENCE_RETRY_AFTER_MS: u64 = 5_000;
const POST_WAKE_LISTENER_REBIND_GRACE_SECS: f64 = 150.0;
const SANDBOX_SERVICE_ROUTE_COMMAND_READY_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const SANDBOX_SERVICE_ROUTE_COMMAND_READY_WAIT_TIMEOUT: prost_types::Duration =
    prost_types::Duration {
        seconds: 8,
        nanos: 0,
    };

#[derive(Debug, Clone, FromRow)]
struct SessionRouteRow {
    session_id: String,
    protocol_session_id: Option<String>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    kind: String,
    target_port: Option<i32>,
    target_protocol: Option<String>,
    target_path: Option<String>,
    adapter_state_json: String,
    route_generation: RouteGeneration,
    current_node_id: Option<NodeId>,
    backend_endpoint: Option<String>,
    backend_transport: Option<String>,
    revocation_epoch: i64,
    revocation_status: String,
    revocation_reason: Option<String>,
    revocation_updated_at: OffsetDateTime,
    org_revocation_epoch: i64,
    scopes: Vec<String>,
    token_hash: Option<String>,
    token_expires_at: Option<OffsetDateTime>,
    sandbox_active_node_id: Option<NodeId>,
    node_address: Option<String>,
    node_managed_by: Option<String>,
    k8s_node_name: Option<String>,
    direct_node_api_endpoint: Option<String>,
    sandbox_pod_uid: Option<PodUid>,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    backend_process_id: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
struct RevocationRow {
    session_id: String,
    protocol_session_id: Option<String>,
    org_id: OrgId,
    revocation_epoch: i64,
    revocation_status: String,
    revocation_reason: Option<String>,
    revocation_updated_at: OffsetDateTime,
    org_revocation_epoch: i64,
}

#[derive(Debug, Clone, FromRow)]
struct OrgRevocationEpochRow {
    epoch: i64,
    updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, FromRow)]
struct SandboxServiceRouteRow {
    sandbox_id: SandboxId,
    org_id: OrgId,
    template_id: Option<String>,
    lifecycle: String,
    active_node_id: Option<NodeId>,
    node_address: Option<String>,
    node_managed_by: Option<NodeManagedBy>,
    k8s_node_name: Option<String>,
    direct_node_api_endpoint: Option<String>,
    sandbox_pod_uid: Option<PodUid>,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    ecw_credential_epoch: EcwCredentialEpoch,
    command_ready_pending: bool,
}

#[derive(Debug, Clone, FromRow)]
struct SandboxRuntimeAccessRow {
    lifecycle: String,
    lifecycle_changed_recently: bool,
}

#[derive(Debug, Clone, FromRow)]
struct UnstableResetSandboxServiceRouteRow {
    active_node_id: NodeId,
}

#[derive(Debug, Clone, FromRow)]
struct BoundProcessRow {
    session_id: String,
    protocol_session_id: Option<String>,
    backend_process_id: String,
    newly_bound: bool,
}

#[derive(Debug, Clone, FromRow)]
struct ExistingProcessBindingRow {
    backend_process_id: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
struct ReconnectRow {
    session_id: String,
    protocol_session_id: Option<String>,
    reconnect_count: i64,
    last_reconnect_reason: String,
}

pub(super) async fn resolve_session_route(
    activation: Option<&dyn crate::ingress_activation::IngressActivation>,
    pool: &sqlx::PgPool,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
    refresh_authority: Option<DataPlaneSessionRefreshAuthority>,
    request: ResolveDataPlaneSessionRouteRequest,
) -> Result<ResolveDataPlaneSessionRouteResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let token_sha256 = optional(request.token_sha256.as_deref());
    let required_scope = optional(request.required_scope.as_deref());
    let mut row = fetch_route_row(pool, &session_id, protocol_session_id.as_deref()).await?;
    validate_attach_authorization(
        &row,
        refresh_authority,
        token_sha256.as_deref(),
        required_scope.as_deref(),
    )?;
    let _waiter = if matches!(
        row.kind.parse::<SessionKind>(),
        Ok(SessionKind::Port | SessionKind::Protocol)
    ) {
        Some(
            pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(Some(
                row.sandbox_id,
            ))?,
        )
    } else {
        None
    };
    if matches!(
        row.kind.parse::<SessionKind>(),
        Ok(SessionKind::Port | SessionKind::Protocol)
    ) && let Some(activation) = activation
    {
        validate_ingress_session(&row)?;
        activation
            .prepare(
                row.sandbox_id,
                pplx_space_common_svc::ingress_admission::preparation_timeout(),
            )
            .await?;
        row = fetch_route_row(pool, &session_id, protocol_session_id.as_deref()).await?;
        validate_ingress_session(&row)?;
        validate_attach_authorization(
            &row,
            refresh_authority,
            token_sha256.as_deref(),
            required_scope.as_deref(),
        )?;
    }
    let current_node_id = row.sandbox_active_node_id.ok_or_else(|| {
        Status::failed_precondition("data-plane session sandbox is not assigned to a node")
    })?;
    let backend_endpoint = resolve_backend_endpoint(&row).ok_or_else(|| {
        Status::failed_precondition(missing_backend_endpoint_message(
            "data-plane session",
            row.sandbox_active_node_id,
            row.node_managed_by.as_deref(),
            row.k8s_node_name.as_deref(),
        ))
    })?;
    let backend_transport = row
        .backend_transport
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(BACKEND_TRANSPORT_GRPC)
        .to_string();

    if row.current_node_id != Some(current_node_id)
        || row.backend_endpoint.as_deref() != Some(backend_endpoint.as_str())
        || row.backend_transport.as_deref() != Some(backend_transport.as_str())
    {
        row = update_route_row(
            pool,
            &session_id,
            current_node_id,
            &backend_endpoint,
            &backend_transport,
        )
        .await?;
    }

    let route_generation = row.route_generation.get();
    let kind = row.kind.parse::<SessionKind>().map_err(|error| {
        Status::internal(format!("data-plane session kind is invalid: {error}"))
    })?;
    data_plane_session_transport(request.requested_transport, kind)?;
    if matches!(kind, SessionKind::Port | SessionKind::Protocol) {
        let port = row
            .target_port
            .and_then(|value| u16::try_from(value).ok())
            .and_then(|value| GuestPort::try_from_u16(value).ok())
            .ok_or_else(|| Status::failed_precondition("service session target port is invalid"))?;
        ensure_sandbox_service_port_ingress_path(
            pool,
            SandboxServicePortRouteIdentity {
                sandbox_id: row.sandbox_id,
                active_node_id: current_node_id,
                route_generation: row
                    .execution_generation
                    .and_then(|value| u64::try_from(value.get()).ok())
                    .and_then(|value| RouteGeneration::new(value).ok())
                    .ok_or_else(|| {
                        Status::failed_precondition(
                            "service session execution generation is missing",
                        )
                    })?,
                execution_generation: row.execution_generation,
                pod_uid: row.sandbox_pod_uid.clone(),
                substrate_kind: row
                    .node_managed_by
                    .as_deref()
                    .and_then(|value| value.parse().ok()),
                activation_attempt_id: row.activation_attempt_id,
            },
            port,
            &backend_endpoint,
            route_ed25519_authority,
        )
        .await?;
    }
    let space_api_route_token = space_api_route_token_for_session(
        &row,
        row.current_node_id.unwrap_or(current_node_id),
        route_ed25519_authority,
    )?;
    Ok(ResolveDataPlaneSessionRouteResponse {
        session_id: row.session_id,
        protocol_session_id: row.protocol_session_id,
        route_generation,
        current_node_id: row.current_node_id.unwrap_or(current_node_id).to_string(),
        backend: Some(DataPlaneBackendEndpoint {
            node_id: current_node_id.to_string(),
            endpoint: row.backend_endpoint.unwrap_or(backend_endpoint),
            transport: Some(backend_transport),
        }),
        revocation: Some(revocation_state(
            row.revocation_epoch,
            &row.revocation_status,
            row.revocation_updated_at,
            row.revocation_reason,
            row.org_revocation_epoch,
        )?),
        sandbox_id: row.sandbox_id.to_string(),
        kind: row.kind,
        target_port: row
            .target_port
            .and_then(|value| u16::try_from(value).ok())
            .map(u32::from),
        target_protocol: row.target_protocol,
        target_path: row.target_path,
        adapter_state_json: row.adapter_state_json,
        backend_process_id: row.backend_process_id,
        org_id: row.org_id.to_string(),
        space_api_route_token,
    })
}

pub(super) async fn bind_session_process(
    pool: &sqlx::PgPool,
    request: BindDataPlaneSessionProcessRequest,
) -> Result<BindDataPlaneSessionProcessResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let process_id = required(&request.process_id, "process_id")?;
    let row = sqlx::query_as::<_, BoundProcessRow>(sql!(
        "WITH target AS (
             SELECT session_id,
                    protocol_session_id,
                    backend_process_id IS NULL AS newly_bound
             FROM data_plane_sessions
             WHERE session_id = $1
               AND ($2::TEXT IS NULL OR protocol_session_id = $2)
               AND kind IN ('terminal', 'process')
               AND (expires_at IS NULL OR expires_at > NOW())
               AND (backend_process_id IS NULL OR backend_process_id = $3)
             FOR UPDATE
         ),
         updated AS (
             UPDATE data_plane_sessions d
             SET backend_process_id = COALESCE(d.backend_process_id, $3),
                 backend_process_bound_at = CASE
                     WHEN d.backend_process_id IS NULL THEN NOW()
                     ELSE d.backend_process_bound_at
                 END,
                 updated_at = NOW()
             FROM target
             WHERE d.session_id = target.session_id
             RETURNING d.session_id,
                       d.protocol_session_id,
                       d.backend_process_id,
                       target.newly_bound
         )
         SELECT session_id,
                protocol_session_id,
                backend_process_id,
                newly_bound
         FROM updated"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .bind(&process_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("bind data-plane session process: {error}")))?;

    if let Some(row) = row {
        return Ok(BindDataPlaneSessionProcessResponse {
            session_id: row.session_id,
            protocol_session_id: row.protocol_session_id,
            process_id: row.backend_process_id,
            newly_bound: row.newly_bound,
        });
    }

    let existing = sqlx::query_as::<_, ExistingProcessBindingRow>(sql!(
        "SELECT backend_process_id
         FROM data_plane_sessions
         WHERE session_id = $1
           AND ($2::TEXT IS NULL OR protocol_session_id = $2)
           AND kind IN ('terminal', 'process')
           AND (expires_at IS NULL OR expires_at > NOW())"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("lookup data-plane session process: {error}")))?;

    match existing.and_then(|row| row.backend_process_id) {
        Some(existing_process_id) => Err(Status::already_exists(format!(
            "data-plane session already bound to process {existing_process_id}"
        ))),
        None => Err(Status::not_found(
            "process-backed data-plane session not found",
        )),
    }
}

pub(super) async fn record_session_reconnect(
    pool: &sqlx::PgPool,
    request: RecordDataPlaneSessionReconnectRequest,
) -> Result<RecordDataPlaneSessionReconnectResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let reason = reconnect_reason(request.reason)?;
    let row = sqlx::query_as::<_, ReconnectRow>(sql!(
        "UPDATE data_plane_sessions
         SET reconnect_count = reconnect_count + 1,
             last_reconnect_reason = $3,
             updated_at = NOW()
         WHERE session_id = $1
           AND ($2::TEXT IS NULL OR protocol_session_id = $2)
           AND (expires_at IS NULL OR expires_at > NOW())
         RETURNING session_id,
                   protocol_session_id,
                   reconnect_count,
                   last_reconnect_reason"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .bind(reason.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("record data-plane session reconnect: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))?;

    let reconnect_count = u64::try_from(row.reconnect_count).map_err(|_| {
        Status::internal(format!(
            "stored reconnect_count is invalid: {}",
            row.reconnect_count
        ))
    })?;
    let last_reconnect_reason = reconnect_reason_from_storage(&row.last_reconnect_reason)?;
    Ok(RecordDataPlaneSessionReconnectResponse {
        session_id: row.session_id,
        protocol_session_id: row.protocol_session_id,
        reconnect_count,
        last_reconnect_reason: last_reconnect_reason as i32,
    })
}

pub(super) async fn unstable_resolve_sandbox_service_route(
    activation: Option<&dyn crate::ingress_activation::IngressActivation>,
    pool: &sqlx::PgPool,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
    ecw_credential_keyring: &EcwCredentialKeyring,
    request: UnstableResolveSandboxServiceRouteRequest,
) -> Result<UnstableResolveSandboxServiceRouteResponse, Status> {
    let sandbox_id = required(&request.sandbox_id, "sandbox_id")?
        .parse::<SandboxId>()
        .map_err(|_| Status::invalid_argument("sandbox_id must be a UUID"))?;
    let port = sandbox_service_route_port(request.port)?;
    let route_kind = sandbox_service_route_kind(request.route_kind)?;
    let transport = sandbox_service_route_transport(request.transport, route_kind)?;
    let mut row = fetch_sandbox_service_route_row(pool, sandbox_id).await?;
    validate_unstable_sandbox_service_route_authorization(
        pool,
        &row,
        route_kind,
        ecw_credential_keyring,
        &request,
    )
    .await?;

    let _waiter = if transport == SandboxServiceRouteTransport::ServicePortIngress {
        Some(
            pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(Some(
                sandbox_id,
            ))?,
        )
    } else {
        None
    };
    if transport == SandboxServiceRouteTransport::ServicePortIngress
        && let Some(activation) = activation
    {
        activation
            .prepare(
                sandbox_id,
                pplx_space_common_svc::ingress_admission::preparation_timeout(),
            )
            .await?;
        row = fetch_sandbox_service_route_row(pool, sandbox_id).await?;
        validate_unstable_sandbox_service_route_authorization(
            pool,
            &row,
            route_kind,
            ecw_credential_keyring,
            &request,
        )
        .await?;
    }

    let current_node_id = row.active_node_id.ok_or_else(|| {
        sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            "sandbox service route sandbox is not assigned to a node",
        )
    })?;
    let backend_endpoint = resolve_sandbox_service_backend_endpoint(&row).ok_or_else(|| {
        sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            missing_backend_endpoint_message(
                "sandbox service route",
                row.active_node_id,
                row.node_managed_by.as_ref().map(|value| value.as_str()),
                row.k8s_node_name.as_deref(),
            ),
        )
    })?;
    let route_generation =
        required_route_generation(row.execution_generation, "sandbox service route")?;
    let (bridge_route_token, service_port_ingress_token) = match transport {
        SandboxServiceRouteTransport::Direct => {
            let bridge_route_token = space_api_route_token_for_sandbox_service_route(
                &row,
                current_node_id,
                port,
                SpaceApiRoutePurpose::SpaceApiBridge,
                route_ed25519_authority,
            )?;
            ensure_sandbox_service_route_command_ready(
                pool,
                &row,
                &backend_endpoint,
                bridge_route_token.as_ref(),
            )
            .await?;
            (bridge_route_token, None)
        }
        SandboxServiceRouteTransport::ServicePortIngress => {
            let ingress_port = GuestPort::try_from_u16(
                u16::try_from(port).map_err(|_| Status::invalid_argument("port is invalid"))?,
            )
            .map_err(|_| Status::invalid_argument("port is invalid"))?;
            // Activation normally clears this marker. Keep the existing probe
            // for deployments without an activation adapter or a pending result.
            if row.command_ready_pending {
                let readiness_route_token = space_api_route_token_for_sandbox_service_route(
                    &row,
                    current_node_id,
                    port,
                    SpaceApiRoutePurpose::SpaceApiBridge,
                    route_ed25519_authority,
                )?;
                ensure_sandbox_service_route_command_ready(
                    pool,
                    &row,
                    &backend_endpoint,
                    readiness_route_token.as_ref(),
                )
                .await?;
            }
            ensure_sandbox_service_port_ingress_path(
                pool,
                SandboxServicePortRouteIdentity {
                    sandbox_id: row.sandbox_id,
                    active_node_id: current_node_id,
                    route_generation,
                    execution_generation: row.execution_generation,
                    pod_uid: row.sandbox_pod_uid.clone(),
                    substrate_kind: row.node_managed_by,
                    activation_attempt_id: row.activation_attempt_id,
                },
                ingress_port,
                &backend_endpoint,
                route_ed25519_authority,
            )
            .await?;
            let service_port_ingress_token = space_api_route_token_for_sandbox_service_route(
                &row,
                current_node_id,
                port,
                SpaceApiRoutePurpose::ServicePortIngress,
                route_ed25519_authority,
            )?;
            (None, service_port_ingress_token)
        }
        SandboxServiceRouteTransport::Unspecified => unreachable!("transport is normalized"),
    };

    Ok(UnstableResolveSandboxServiceRouteResponse {
        sandbox_id: row.sandbox_id.to_string(),
        org_id: row.org_id.to_string(),
        port,
        lifecycle: row.lifecycle,
        route_generation: route_generation.get(),
        current_node_id: current_node_id.to_string(),
        backend: Some(DataPlaneBackendEndpoint {
            node_id: current_node_id.to_string(),
            endpoint: backend_endpoint,
            transport: Some(BACKEND_TRANSPORT_GRPC.to_string()),
        }),
        template_id: row.template_id,
        space_api_route_token: bridge_route_token,
        service_port_ingress_token,
    })
}

pub(super) async fn reset_sandbox_service_route(
    pool: &sqlx::PgPool,
    request: UnstableResetSandboxServiceRouteRequest,
) -> Result<UnstableResetSandboxServiceRouteResponse, Status> {
    let route = request
        .route
        .ok_or_else(|| Status::invalid_argument("route is required"))?;
    let sandbox_id = required(&route.sandbox_id, "route.sandbox_id")?
        .parse::<SandboxId>()
        .map_err(|_| Status::invalid_argument("route.sandbox_id must be a UUID"))?;
    let port = sandbox_service_route_port(route.port)?;
    let port_i32 = i32::try_from(port).map_err(|_| Status::invalid_argument("port is invalid"))?;
    let route_generation = route_generation_from_wire(request.route_generation)?;
    let kind = sandbox_service_route_key_kind(route.kind)?;
    if kind != SandboxServiceRouteKind::ServicePort {
        return Ok(UnstableResetSandboxServiceRouteResponse {
            result: UnstableResetSandboxServiceRouteResult::Noop as i32,
            reconcile_enqueued: false,
        });
    }

    let diagnostic = bounded_reset_route_diagnostic(request.diagnostic_message.as_deref());
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Status::internal(format!("begin reset sandbox service route: {error}")))?;

    let reset = sqlx::query_as::<_, UnstableResetSandboxServiceRouteRow>(sql!(
        "WITH current_route AS (
             SELECT s.id AS sandbox_id, s.active_node_id
             FROM sandboxes s
             JOIN sandbox_k8s_executions e
               ON e.sandbox_id = s.id
              AND e.active
              AND e.generation = $3
             WHERE s.id = $1
               AND s.active_node_id IS NOT NULL
         ),
         updated AS (
             UPDATE sandbox_port_exposures spe
             SET apply_state = 'pending',
                 last_applied_node_id = NULL,
                 last_error = $4,
                 last_bridge_status = NULL,
                 last_applied_bridge_generation = NULL,
                 last_observed_at = NULL,
                 updated_at = NOW()
             FROM current_route
             WHERE spe.sandbox_id = current_route.sandbox_id
               AND spe.protocol = 'tcp'
               AND spe.guest_port = $2
               AND spe.desired_state = 'open'
               AND spe.apply_state = 'applied'
               AND spe.last_applied_node_id = current_route.active_node_id
             RETURNING current_route.active_node_id
         )
         SELECT active_node_id FROM updated"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(port_i32)
    .bind(route_generation)
    .bind(diagnostic)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| Status::internal(format!("reset sandbox service route: {error}")))?;

    let Some(reset) = reset else {
        tx.commit().await.map_err(|error| {
            Status::internal(format!("commit reset sandbox service route no-op: {error}"))
        })?;
        return Ok(UnstableResetSandboxServiceRouteResponse {
            result: UnstableResetSandboxServiceRouteResult::Noop as i32,
            reconcile_enqueued: false,
        });
    };

    enqueue_sandbox_desired_state_reconcile_tx(&mut tx, sandbox_id, reset.active_node_id).await?;
    tx.commit().await.map_err(|error| {
        Status::internal(format!("commit reset sandbox service route: {error}"))
    })?;

    Ok(UnstableResetSandboxServiceRouteResponse {
        result: UnstableResetSandboxServiceRouteResult::Reset as i32,
        reconcile_enqueued: true,
    })
}

pub(super) async fn revocation_state_for_session(
    pool: &sqlx::PgPool,
    request: GetDataPlaneSessionRevocationStateRequest,
) -> Result<GetDataPlaneSessionRevocationStateResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let row = sqlx::query_as::<_, RevocationRow>(sql!(
        "SELECT d.session_id,
                d.protocol_session_id,
                d.org_id,
                d.revocation_epoch,
                d.revocation_status,
                d.revocation_reason,
                d.revocation_updated_at,
                COALESCE(o.epoch, 0) AS org_revocation_epoch
         FROM data_plane_sessions d
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
         WHERE d.session_id = $1
           AND ($2::TEXT IS NULL OR d.protocol_session_id = $2)
           AND (d.expires_at IS NULL OR d.expires_at > NOW())"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("get data-plane revocation state: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))?;

    Ok(GetDataPlaneSessionRevocationStateResponse {
        session_id: row.session_id,
        protocol_session_id: row.protocol_session_id,
        revocation: Some(revocation_state(
            row.revocation_epoch,
            &row.revocation_status,
            row.revocation_updated_at,
            row.revocation_reason,
            row.org_revocation_epoch,
        )?),
        org_id: row.org_id.to_string(),
    })
}

pub(super) async fn org_revocation_epoch(
    pool: &sqlx::PgPool,
    request: GetDataPlaneOrgRevocationEpochRequest,
) -> Result<GetDataPlaneOrgRevocationEpochResponse, Status> {
    let org_id = required(&request.org_id, "org_id")?
        .parse::<Uuid>()
        .map_err(|_| Status::invalid_argument("org_id must be a UUID"))?;
    let row = sqlx::query_as::<_, OrgRevocationEpochRow>(sql!(
        "SELECT epoch, updated_at
         FROM data_plane_org_revocation_epochs
         WHERE org_id = $1"
    ))
    .bind(org_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("get data-plane org revocation epoch: {error}")))?
    .unwrap_or(OrgRevocationEpochRow {
        epoch: 0,
        updated_at: None,
    });

    Ok(GetDataPlaneOrgRevocationEpochResponse {
        org_id: org_id.to_string(),
        epoch: i64_to_u64(row.epoch, "org_revocation_epoch")?,
        updated_at: row.updated_at.map(timestamp),
    })
}

pub(super) async fn sandbox_runtime_access(
    pool: &sqlx::PgPool,
    request: GetSandboxRuntimeAccessRequest,
) -> Result<GetSandboxRuntimeAccessResponse, Status> {
    let sandbox_id = required(&request.sandbox_id, "sandbox_id")?
        .parse::<SandboxId>()
        .map_err(|_| Status::invalid_argument("sandbox_id must be a UUID"))?;
    let row = fetch_sandbox_runtime_access_row(pool, sandbox_id).await?;

    Ok(GetSandboxRuntimeAccessResponse {
        access: Some(sandbox_runtime_access_from_row(row)),
    })
}

async fn fetch_route_row(
    pool: &sqlx::PgPool,
    session_id: &str,
    protocol_session_id: Option<&str>,
) -> Result<SessionRouteRow, Status> {
    sqlx::query_as::<_, SessionRouteRow>(sql!(
        "SELECT d.session_id,
                d.protocol_session_id,
                d.org_id,
                d.sandbox_id,
                d.kind,
                d.target_port,
                d.target_protocol,
                d.target_path,
                d.adapter_state::TEXT AS adapter_state_json,
                d.route_generation,
                d.current_node_id,
                d.backend_endpoint,
                d.backend_transport,
                d.revocation_epoch,
                d.revocation_status,
                d.revocation_reason,
                d.revocation_updated_at,
                COALESCE(o.epoch, 0) AS org_revocation_epoch,
                d.scopes,
                d.token_hash,
                d.token_expires_at,
                COALESCE(direct_route.current_node_id, s.active_node_id)
                    AS sandbox_active_node_id,
                HOST(n.address) AS node_address,
                COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                nk.k8s_node_name AS k8s_node_name,
                nd.node_api_endpoint AS direct_node_api_endpoint,
                COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                    AS sandbox_pod_uid,
                COALESCE(e.generation, direct_route.execution_generation)
                    AS execution_generation,
                COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                    AS activation_attempt_id,
                d.backend_process_id
         FROM data_plane_sessions d
         JOIN sandboxes s ON s.id = d.sandbox_id
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes n
           ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
         LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         WHERE d.session_id = $1
           AND ($2::TEXT IS NULL OR d.protocol_session_id = $2)
           AND (d.expires_at IS NULL OR d.expires_at > NOW())"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("resolve data-plane session route: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))
}

fn i64_to_u64(value: i64, field: &str) -> Result<u64, Status> {
    u64::try_from(value).map_err(|_| Status::internal(format!("{field} is negative")))
}

async fn update_route_row(
    pool: &sqlx::PgPool,
    session_id: &str,
    current_node_id: NodeId,
    backend_endpoint: &str,
    backend_transport: &str,
) -> Result<SessionRouteRow, Status> {
    sqlx::query_as::<_, SessionRouteRow>(sql!(
        "UPDATE data_plane_sessions d
         SET current_node_id = $2,
             backend_endpoint = $3,
             backend_transport = $4,
             route_generation = CASE
                 WHEN d.current_node_id IS DISTINCT FROM $2
                   OR d.backend_endpoint IS DISTINCT FROM $3
                   OR d.backend_transport IS DISTINCT FROM $4
                 THEN d.route_generation + 1
                 ELSE d.route_generation
             END,
             updated_at = NOW()
         FROM sandboxes s
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = s.org_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes n
           ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
         LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         WHERE d.session_id = $1
           AND s.id = d.sandbox_id
         RETURNING d.session_id,
                   d.protocol_session_id,
                   d.org_id,
                   d.sandbox_id,
                   d.kind,
                   d.target_port,
                   d.target_protocol,
                   d.target_path,
                   d.adapter_state::TEXT AS adapter_state_json,
                   d.route_generation,
                   d.current_node_id,
                   d.backend_endpoint,
                   d.backend_transport,
                   d.revocation_epoch,
                   d.revocation_status,
                   d.revocation_reason,
                   d.revocation_updated_at,
                   COALESCE(o.epoch, 0) AS org_revocation_epoch,
                   d.scopes,
                   d.token_hash,
                   d.token_expires_at,
                   COALESCE(direct_route.current_node_id, s.active_node_id)
                       AS sandbox_active_node_id,
                   HOST(n.address) AS node_address,
                   COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                   nk.k8s_node_name AS k8s_node_name,
                   nd.node_api_endpoint AS direct_node_api_endpoint,
                   COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                       AS sandbox_pod_uid,
                   COALESCE(e.generation, direct_route.execution_generation)
                       AS execution_generation,
                   COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                       AS activation_attempt_id,
                   d.backend_process_id"
    ))
    .bind(session_id)
    .bind(current_node_id)
    .bind(backend_endpoint)
    .bind(backend_transport)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("update data-plane session route: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))
}

/// Explicit registered endpoints win; database INET host values need IPv6 brackets.
fn node_api_backend_endpoint(direct: Option<&str>, address: Option<&str>) -> Option<String> {
    direct
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            let ip = address?.trim().parse::<std::net::IpAddr>().ok()?;
            let address = std::net::SocketAddr::new(ip, DEFAULT_NODE_API_PORT);
            Some(format!(
                "{}{address}",
                pplx_space_common_grpc::internal_transport::advertised_endpoint_prefix()
            ))
        })
}

fn resolve_backend_endpoint(row: &SessionRouteRow) -> Option<String> {
    node_api_backend_endpoint(
        row.direct_node_api_endpoint.as_deref(),
        row.node_address.as_deref(),
    )
}

fn missing_backend_endpoint_message(
    context: &str,
    active_node_id: Option<NodeId>,
    node_managed_by: Option<&str>,
    k8s_node_name: Option<&str>,
) -> String {
    let node_id = active_node_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let managed_by = node_managed_by
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("-");
    let k8s_node_name = k8s_node_name
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("-");
    format!(
        "{context} has no backend endpoint (active_node_id={node_id}, managed_by={managed_by}, k8s_node_name={k8s_node_name}; expected node_direct.node_api_endpoint or nodes.address)",
    )
}

fn space_api_route_token_for_session(
    row: &SessionRouteRow,
    current_node_id: NodeId,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<Option<ProtoSpaceApiRouteToken>, Status> {
    let kind = row.kind.parse::<SessionKind>().map_err(|error| {
        Status::internal(format!("data-plane session kind is invalid: {error}"))
    })?;
    let (purpose, service_port) = match kind {
        SessionKind::Terminal | SessionKind::Process | SessionKind::FileWatch => {
            (SpaceApiRoutePurpose::SpaceApiBridge, None)
        }
        SessionKind::Port | SessionKind::Protocol => {
            let port = row
                .target_port
                .and_then(|value| u16::try_from(value).ok())
                .and_then(|value| GuestPort::try_from_u16(value).ok())
                .ok_or_else(|| {
                    Status::failed_precondition("service session target port is invalid")
                })?;
            (SpaceApiRoutePurpose::ServicePortIngress, Some(port))
        }
    };
    let execution_generation = row.execution_generation;
    let signed_route_generation =
        required_route_generation(execution_generation, "data-plane session")?;
    space_api_route_token(
        SpaceApiRouteTokenParams {
            purpose,
            sandbox_id: row.sandbox_id,
            route_generation: signed_route_generation,
            current_node_id,
            substrate_kind: row
                .node_managed_by
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|error| {
                    Status::internal(format!(
                        "data-plane session node substrate is invalid: {error}"
                    ))
                })?,
            execution_generation,
            pod_uid: row.sandbox_pod_uid.clone(),
            activation_attempt_id: row.activation_attempt_id,
            service_port,
        },
        route_ed25519_authority,
    )
}

fn data_plane_session_transport(
    requested_transport: Option<i32>,
    kind: SessionKind,
) -> Result<SandboxServiceRouteTransport, Status> {
    let transport = requested_transport
        .map(SandboxServiceRouteTransport::try_from)
        .transpose()
        .map_err(|_| Status::invalid_argument("requested_transport is invalid"))?
        .unwrap_or({
            if matches!(kind, SessionKind::Port | SessionKind::Protocol) {
                SandboxServiceRouteTransport::ServicePortIngress
            } else {
                SandboxServiceRouteTransport::Direct
            }
        });
    match (kind, transport) {
        (
            SessionKind::Port | SessionKind::Protocol,
            SandboxServiceRouteTransport::ServicePortIngress,
        ) => Ok(SandboxServiceRouteTransport::ServicePortIngress),
        (
            SessionKind::Port | SessionKind::Protocol,
            SandboxServiceRouteTransport::Direct | SandboxServiceRouteTransport::Unspecified,
        ) => Err(Status::invalid_argument(
            "port and protocol sessions require service port ingress transport",
        )),
        (_, SandboxServiceRouteTransport::Direct) => Ok(transport),
        (_, SandboxServiceRouteTransport::Unspecified) => Ok(SandboxServiceRouteTransport::Direct),
        (_, SandboxServiceRouteTransport::ServicePortIngress) => Err(Status::invalid_argument(
            "service port ingress transport requires a port or protocol session",
        )),
    }
}

fn space_api_route_token_for_sandbox_service_route(
    row: &SandboxServiceRouteRow,
    current_node_id: NodeId,
    port: u32,
    purpose: SpaceApiRoutePurpose,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<Option<ProtoSpaceApiRouteToken>, Status> {
    let execution_generation = row.execution_generation;
    let signed_route_generation =
        required_route_generation(execution_generation, "sandbox service route")?;
    let service_port = if purpose == SpaceApiRoutePurpose::ServicePortIngress {
        let port = u16::try_from(port)
            .ok()
            .and_then(|port| GuestPort::try_from_u16(port).ok())
            .ok_or_else(|| Status::invalid_argument("service port must be 1-65535"))?;
        Some(port)
    } else {
        None
    };
    space_api_route_token(
        SpaceApiRouteTokenParams {
            purpose,
            sandbox_id: row.sandbox_id,
            route_generation: signed_route_generation,
            current_node_id,
            substrate_kind: row.node_managed_by,
            execution_generation,
            pod_uid: row.sandbox_pod_uid.clone(),
            activation_attempt_id: row.activation_attempt_id,
            service_port,
        },
        route_ed25519_authority,
    )
}

fn required_route_generation(
    execution_generation: Option<ExecutionGeneration>,
    route_kind: &str,
) -> Result<RouteGeneration, Status> {
    match execution_generation {
        Some(generation) => RouteGeneration::new(
            u64::try_from(generation.get())
                .map_err(|_| Status::internal("execution generation is invalid"))?,
        )
        .map_err(|error| Status::internal(error.to_string())),
        None => Err(Status::failed_precondition(format!(
            "{route_kind} has no authoritative execution generation"
        ))),
    }
}

struct SpaceApiRouteTokenParams {
    purpose: SpaceApiRoutePurpose,
    sandbox_id: SandboxId,
    route_generation: RouteGeneration,
    current_node_id: NodeId,
    substrate_kind: Option<NodeManagedBy>,
    execution_generation: Option<ExecutionGeneration>,
    pod_uid: Option<PodUid>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    service_port: Option<GuestPort>,
}

fn space_api_route_token(
    params: SpaceApiRouteTokenParams,
    route_ed25519_authority: Option<&SpaceApiRouteEd25519Authority>,
) -> Result<Option<ProtoSpaceApiRouteToken>, Status> {
    let complete_identity = route_identity_for_signing(&SandboxRouteIdentityParts {
        sandbox_id: params.sandbox_id,
        route_generation: params.route_generation,
        current_node_id: params.current_node_id,
        substrate_kind: params.substrate_kind,
        execution_generation: params.execution_generation,
        activation_attempt_id: params.activation_attempt_id,
        pod_uid: params.pod_uid.clone(),
    })?;
    let authority = route_ed25519_authority.ok_or_else(|| {
        Status::failed_precondition("SPACE API route signing authority is unavailable")
    })?;
    let token = match params.purpose {
        SpaceApiRoutePurpose::SpaceApiBridge => authority.mint_space_api_bridge(complete_identity),
        SpaceApiRoutePurpose::ServicePortIngress => authority.mint_service_port_ingress(
            complete_identity,
            params
                .service_port
                .expect("ServicePort ingress claims require a port"),
            pplx_space_common_svc::space_api_route::SpaceApiRouteProtocol::Tcp,
        ),
    }
    .map_err(|error| Status::internal(format!("mint SPACE API route token: {error}")))?;
    Ok(Some(ProtoSpaceApiRouteToken {
        token: token.secret().to_owned(),
    }))
}

async fn fetch_sandbox_service_route_row(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<SandboxServiceRouteRow, Status> {
    sqlx::query_as::<_, SandboxServiceRouteRow>(sql!(
        "SELECT s.id AS sandbox_id,
                s.org_id,
                s.template_id,
                s.lifecycle,
                COALESCE(direct_route.current_node_id, s.active_node_id) AS active_node_id,
                HOST(n.address) AS node_address,
                COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                nk.k8s_node_name AS k8s_node_name,
                nd.node_api_endpoint AS direct_node_api_endpoint,
                COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                    AS sandbox_pod_uid,
                COALESCE(e.generation, direct_route.execution_generation)
                    AS execution_generation,
                COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                    AS activation_attempt_id,
                COALESCE(ecw_credential.epoch, 1) AS ecw_credential_epoch,
                s.command_ready_pending
         FROM sandboxes s
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes n
           ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
         LEFT JOIN sandbox_k8s_executions e
           ON e.sandbox_id = s.id
          AND e.active
          AND e.terminated_at IS NULL
         LEFT JOIN sandbox_ecw_credential_epochs ecw_credential
           ON ecw_credential.sandbox_id = s.id
         WHERE s.id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("resolve sandbox service route: {error}")))?
    .ok_or_else(|| Status::not_found("sandbox not found"))
}

async fn ensure_sandbox_service_route_command_ready(
    pool: &sqlx::PgPool,
    row: &SandboxServiceRouteRow,
    backend_endpoint: &str,
    space_api_route_token: Option<&ProtoSpaceApiRouteToken>,
) -> Result<(), Status> {
    if !row.command_ready_pending {
        return Ok(());
    }

    if row.lifecycle.parse::<SandboxLifecycle>().ok() != Some(SandboxLifecycle::Started) {
        return Err(sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            "sandbox service route command readiness requires a started sandbox",
        ));
    }

    let sandbox_id = row.sandbox_id;
    let mut node_api = worker_node_api(
        backend_endpoint,
        SANDBOX_SERVICE_ROUTE_COMMAND_READY_RPC_TIMEOUT,
    )
    .map_err(|status| {
        Status::unavailable(format!(
            "sandbox service route command readiness node endpoint is invalid: {status}"
        ))
    })?;
    let mut worker = WorkerApiClient::new(&mut node_api);
    let mut request = Request::new(SpacedWaitReadyRequest {
        sandbox_id: sandbox_id.to_string(),
        timeout: Some(SANDBOX_SERVICE_ROUTE_COMMAND_READY_WAIT_TIMEOUT),
    });
    if let Some(token) = space_api_route_token {
        let token = token
            .token
            .parse::<SpaceApiRouteEd25519Token>()
            .map_err(|error| Status::internal(format!("route token is invalid: {error}")))?;
        insert_space_api_route_ed25519_token(request.metadata_mut(), &token).map_err(|error| {
            Status::internal(format!("route token metadata is invalid: {error}"))
        })?;
    }
    worker
        .spaced_wait_ready_with_metadata(request, SANDBOX_SERVICE_ROUTE_COMMAND_READY_RPC_TIMEOUT)
        .await
        .map_err(sandbox_service_route_command_ready_status)?;

    let claimed = clear_sandbox_command_ready_pending(pool, row.sandbox_id).await?;
    if !claimed && !sandbox_lifecycle_is_started(pool, row.sandbox_id).await? {
        return Err(sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            "sandbox service route command readiness lost race with lifecycle transition",
        ));
    }
    Ok(())
}

fn sandbox_service_route_command_ready_status(status: Status) -> Status {
    match status.code() {
        Code::DeadlineExceeded | Code::Cancelled => sandbox_service_route_not_ready_status(
            Code::DeadlineExceeded,
            "sandbox service route command readiness timed out",
        ),
        Code::FailedPrecondition | Code::Unavailable => sandbox_service_route_not_ready_status(
            Code::FailedPrecondition,
            format!("sandbox service route command readiness not ready: {status}"),
        ),
        _ => Status::unavailable(format!(
            "sandbox service route command readiness failed: {status}"
        )),
    }
}

async fn clear_sandbox_command_ready_pending(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, Status> {
    // This CAS must match the substrate access-side clear in
    // `KubernetesStore::clear_command_ready_pending`: the `lifecycle =
    // 'started'` guard makes both access paths lose to the command-ready
    // reaper when it has already claimed the sandbox for teardown, instead of
    // clearing a marker on a runtime that is no longer safe to hand back.
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
         SET command_ready_pending = FALSE
         WHERE id = $1 AND lifecycle = 'started' AND command_ready_pending"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(pool)
    .await
    .map_err(|error| {
        Status::internal(format!(
            "clear sandbox service route command readiness marker: {error}"
        ))
    })?;
    Ok(result.rows_affected() > 0)
}

async fn sandbox_lifecycle_is_started(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, Status> {
    let lifecycle: Option<String> = sandbox_store::get(pool, sandbox_id)
        .await
        .map_err(|error| {
            Status::internal(format!("fetch sandbox service route lifecycle: {error}"))
        })?
        .map(|record| record.lifecycle);

    Ok(lifecycle
        .as_deref()
        .and_then(|value| value.parse::<SandboxLifecycle>().ok())
        == Some(SandboxLifecycle::Started))
}

async fn fetch_sandbox_runtime_access_row(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<SandboxRuntimeAccessRow, Status> {
    sqlx::query_as::<_, SandboxRuntimeAccessRow>(sql!(
        "SELECT lifecycle,
                lifecycle_changed_at > NOW() - make_interval(secs => $2)
                    AS lifecycle_changed_recently
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(POST_WAKE_LISTENER_REBIND_GRACE_SECS)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("get sandbox runtime access: {error}")))?
    .ok_or_else(|| Status::not_found("sandbox not found"))
}

fn sandbox_runtime_access_from_row(row: SandboxRuntimeAccessRow) -> SandboxRuntimeAccess {
    let lifecycle = row
        .lifecycle
        .trim()
        .to_ascii_lowercase()
        .parse::<SandboxLifecycle>()
        .ok();
    let decision = sandbox_runtime_access_decision(lifecycle, row.lifecycle_changed_recently);
    SandboxRuntimeAccess {
        decision: decision as i32,
        retry_after_ms: if decision == SandboxRuntimeAccessDecision::RetryableConverging {
            SANDBOX_RUNTIME_CONVERGENCE_RETRY_AFTER_MS
        } else {
            0
        },
    }
}

fn sandbox_runtime_access_decision(
    lifecycle: Option<SandboxLifecycle>,
    lifecycle_changed_recently: bool,
) -> SandboxRuntimeAccessDecision {
    match lifecycle {
        Some(
            SandboxLifecycle::Failed | SandboxLifecycle::Quarantined | SandboxLifecycle::Stopped,
        ) => SandboxRuntimeAccessDecision::TerminalUnavailable,
        Some(
            SandboxLifecycle::Suspended
            | SandboxLifecycle::Suspending
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Paused
            | SandboxLifecycle::Restoring
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Starting,
        ) => SandboxRuntimeAccessDecision::RetryableConverging,
        Some(SandboxLifecycle::Started) if lifecycle_changed_recently => {
            SandboxRuntimeAccessDecision::RetryableConverging
        }
        Some(
            SandboxLifecycle::Created
            | SandboxLifecycle::Creating
            | SandboxLifecycle::Started
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Deleting
            | SandboxLifecycle::Checkpointing,
        ) => SandboxRuntimeAccessDecision::Ready,
        None => SandboxRuntimeAccessDecision::Unknown,
    }
}

fn resolve_sandbox_service_backend_endpoint(row: &SandboxServiceRouteRow) -> Option<String> {
    node_api_backend_endpoint(
        row.direct_node_api_endpoint.as_deref(),
        row.node_address.as_deref(),
    )
}

async fn enqueue_sandbox_desired_state_reconcile_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    active_node_id: NodeId,
) -> Result<(), Status> {
    let reference = ReconcileRef::sandbox_desired_state(sandbox_id);
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id,
             facet,
             element,
             requested_at,
             updated_at
         )
         VALUES ($1, $2, $3, NOW(), NOW())
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET generation = node_reconcile_queue.generation + 1,
             requested_at = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL
                 THEN EXCLUDED.requested_at
                 ELSE node_reconcile_queue.requested_at
             END,
             next_attempt_at = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL
                 THEN NOW()
                 WHEN node_reconcile_queue.claim_token = ''
                      AND node_reconcile_queue.attempts = 0
                 THEN NOW()
                 ELSE node_reconcile_queue.next_attempt_at
             END,
             attempts = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL THEN 0
                 ELSE node_reconcile_queue.attempts
             END,
             last_error = CASE
                 WHEN node_reconcile_queue.dead_at IS NOT NULL
                      OR node_reconcile_queue.attempts = 0
                 THEN ''
                 ELSE node_reconcile_queue.last_error
             END,
             dead_at = NULL,
             dead_reason = '',
             updated_at = NOW()"
    ))
    .bind(active_node_id.as_uuid())
    .bind(reference.facet_str())
    .bind(reference.element_string().unwrap_or_default())
    .execute(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("enqueue sandbox route reconcile: {error}")))?;

    enqueue_triggers_tx(
        tx,
        [active_node_id],
        ReconcileReason::SandboxPortExposureUpdated,
    )
    .await
    .map_err(|error| Status::internal(format!("trigger sandbox route reconcile: {error}")))?;
    Ok(())
}

fn sandbox_service_route_port(port: u32) -> Result<u32, Status> {
    if port == 0 || port > u32::from(u16::MAX) {
        return Err(Status::invalid_argument("port must be between 1 and 65535"));
    }
    Ok(port)
}

fn sandbox_service_route_kind(route_kind: i32) -> Result<SandboxServiceRouteKind, Status> {
    let route_kind = SandboxServiceRouteKind::try_from(route_kind)
        .map_err(|_| Status::invalid_argument("route_kind is invalid"))?;
    match route_kind {
        SandboxServiceRouteKind::Unspecified => {
            Err(Status::invalid_argument("route_kind is required"))
        }
        SandboxServiceRouteKind::Agent | SandboxServiceRouteKind::ServicePort => Ok(route_kind),
    }
}

fn sandbox_service_route_transport(
    transport: i32,
    route_kind: SandboxServiceRouteKind,
) -> Result<SandboxServiceRouteTransport, Status> {
    let transport = SandboxServiceRouteTransport::try_from(transport)
        .map_err(|_| Status::invalid_argument("transport is invalid"))?;
    match (route_kind, transport) {
        (SandboxServiceRouteKind::Agent, SandboxServiceRouteTransport::Unspecified) => {
            Ok(SandboxServiceRouteTransport::Direct)
        }
        (SandboxServiceRouteKind::ServicePort, SandboxServiceRouteTransport::Unspecified) => {
            Ok(SandboxServiceRouteTransport::ServicePortIngress)
        }
        (SandboxServiceRouteKind::Agent, SandboxServiceRouteTransport::Direct) => {
            Ok(SandboxServiceRouteTransport::Direct)
        }
        (
            SandboxServiceRouteKind::ServicePort,
            SandboxServiceRouteTransport::ServicePortIngress,
        ) => Ok(SandboxServiceRouteTransport::ServicePortIngress),
        (SandboxServiceRouteKind::ServicePort, SandboxServiceRouteTransport::Direct) => Err(
            Status::invalid_argument("service port routes require service port ingress transport"),
        ),
        (SandboxServiceRouteKind::Agent, SandboxServiceRouteTransport::ServicePortIngress) => {
            Err(Status::invalid_argument(
                "service port ingress transport requires service port route kind",
            ))
        }
        (SandboxServiceRouteKind::Unspecified, _) => {
            Err(Status::invalid_argument("route_kind is required"))
        }
    }
}

fn sandbox_service_route_key_kind(route_kind: i32) -> Result<SandboxServiceRouteKind, Status> {
    let route_kind = SandboxServiceRouteKind::try_from(route_kind)
        .map_err(|_| Status::invalid_argument("route.kind is invalid"))?;
    match route_kind {
        SandboxServiceRouteKind::Unspecified => {
            Err(Status::invalid_argument("route.kind is required"))
        }
        SandboxServiceRouteKind::Agent | SandboxServiceRouteKind::ServicePort => Ok(route_kind),
    }
}

fn route_generation_from_wire(route_generation: u64) -> Result<RouteGeneration, Status> {
    RouteGeneration::new(route_generation)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn bounded_reset_route_diagnostic(diagnostic: Option<&str>) -> String {
    let diagnostic = diagnostic
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("sandbox service route reset requested");
    diagnostic
        .chars()
        .take(RESET_ROUTE_DIAGNOSTIC_MAX_CHARS)
        .collect()
}

async fn validate_unstable_sandbox_service_route_authorization(
    pool: &sqlx::PgPool,
    row: &SandboxServiceRouteRow,
    route_kind: SandboxServiceRouteKind,
    ecw_credential_keyring: &EcwCredentialKeyring,
    request: &UnstableResolveSandboxServiceRouteRequest,
) -> Result<(), Status> {
    match route_kind {
        SandboxServiceRouteKind::Agent => {
            if token_matches_ecw_credential(
                ecw_credential_keyring,
                row.sandbox_id,
                row.ecw_credential_epoch,
                credential_proof_sha256(
                    request,
                    SandboxServiceRouteCredentialKind::SandboxAgentToken,
                ),
                EcwCredentialKind::Envd,
            )? || api_key_belongs_to_org(
                pool,
                credential_proof_sha256(request, SandboxServiceRouteCredentialKind::OrgApiKey),
                row.org_id,
            )
            .await?
            {
                return Ok(());
            }
            // The traffic token is deliberately a narrower capability handed
            // to consumers of exposed service ports; it never authorizes the
            // envd route, which grants guest file and process control.
            Err(Status::unauthenticated(
                "invalid sandbox agent route credentials",
            ))
        }
        SandboxServiceRouteKind::ServicePort => {
            if token_matches_ecw_credential(
                ecw_credential_keyring,
                row.sandbox_id,
                row.ecw_credential_epoch,
                credential_proof_sha256(
                    request,
                    SandboxServiceRouteCredentialKind::SandboxTrafficToken,
                ),
                EcwCredentialKind::Traffic,
            )? {
                return Ok(());
            }
            Err(Status::permission_denied(
                "invalid sandbox service route credentials",
            ))
        }
        SandboxServiceRouteKind::Unspecified => {
            Err(Status::invalid_argument("route_kind is required"))
        }
    }
}

fn credential_proof_sha256(
    request: &UnstableResolveSandboxServiceRouteRequest,
    kind: SandboxServiceRouteCredentialKind,
) -> Option<&str> {
    request
        .credential_proofs
        .iter()
        .filter(|proof| proof.kind == kind as i32)
        .map(|proof| proof.sha256.trim())
        .find(|sha256| !sha256.is_empty())
}

async fn api_key_belongs_to_org(
    pool: &sqlx::PgPool,
    api_key_sha256: Option<&str>,
    org_id: OrgId,
) -> Result<bool, Status> {
    let Some(api_key_sha256) = api_key_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(false);
    };

    crate::api_key_store::authorizes_ecw_sandbox(pool, api_key_sha256, org_id)
        .await
        .map_err(|error| {
            Status::internal(format!("validate sandbox service route api key: {error}"))
        })
}

fn token_matches_ecw_credential(
    keyring: &EcwCredentialKeyring,
    sandbox_id: SandboxId,
    credential_epoch: EcwCredentialEpoch,
    candidate_sha256: Option<&str>,
    kind: EcwCredentialKind,
) -> Result<bool, Status> {
    let Some(candidate_sha256) = candidate_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(false);
    };
    let result = keyring
        .match_sha256(sandbox_id, credential_epoch, kind, candidate_sha256)
        .map_err(|error| {
            pplx_space_metrics::internal_api::ecw_credential_verification(
                kind.as_str(),
                EcwCredentialVerificationResult::ConfigurationError,
            );
            Status::internal(format!("verify ECW credential: {error}"))
        })?;
    let metric_result = match result {
        EcwCredentialMatch::V2Current => EcwCredentialVerificationResult::V2Current,
        EcwCredentialMatch::V2Previous => EcwCredentialVerificationResult::V2Previous,
        EcwCredentialMatch::NoMatch => EcwCredentialVerificationResult::Invalid,
    };
    pplx_space_metrics::internal_api::ecw_credential_verification(kind.as_str(), metric_result);
    Ok(result != EcwCredentialMatch::NoMatch)
}

fn validate_ingress_session(row: &SessionRouteRow) -> Result<(), Status> {
    validate_ingress_session_access(
        &row.revocation_status,
        row.token_expires_at,
        OffsetDateTime::now_utc(),
    )
}

fn validate_ingress_session_access(
    status: &str,
    expires: Option<OffsetDateTime>,
    now: OffsetDateTime,
) -> Result<(), Status> {
    if revocation_status(status)? != DataPlaneRevocationStatus::Active {
        return Err(Status::permission_denied("ingress session is revoked"));
    }
    if !expires.is_some_and(|expires| expires > now) {
        return Err(Status::unauthenticated("ingress session token is expired"));
    }
    Ok(())
}

fn validate_attach_authorization(
    row: &SessionRouteRow,
    refresh_authority: Option<DataPlaneSessionRefreshAuthority>,
    token_sha256: Option<&str>,
    required_scope: Option<&str>,
) -> Result<(), Status> {
    if token_sha256.is_none() && required_scope.is_none() {
        // The data-plane edge refreshes the route of a session it already
        // attached without re-sending the attach secret. Any other caller
        // holding only a session ID must prove the attach, or the ID alone
        // would mint a route token for another tenant's session.
        return match refresh_authority {
            Some(DataPlaneSessionRefreshAuthority::Granted) => Ok(()),
            _ => Err(Status::unauthenticated(
                "data-plane attach token is required",
            )),
        };
    }

    let token_sha256 = token_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unauthenticated("data-plane attach token is required"))?;
    let stored_hash = row
        .token_hash
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unauthenticated("data-plane session has no attach token"))?;
    if stored_hash
        .as_bytes()
        .ct_eq(token_sha256.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(Status::unauthenticated("invalid data-plane attach token"));
    }

    match row.token_expires_at {
        Some(expires_at) if expires_at > OffsetDateTime::now_utc() => {}
        Some(_) => return Err(Status::unauthenticated("data-plane attach token expired")),
        None => {
            return Err(Status::unauthenticated(
                "data-plane attach token has no expiry",
            ));
        }
    }

    if let Some(required_scope) = required_scope
        && !row.scopes.iter().any(|scope| scope == required_scope)
    {
        return Err(Status::permission_denied(
            "data-plane attach token does not grant requested scope",
        ));
    }

    Ok(())
}

fn revocation_state(
    epoch: i64,
    status: &str,
    updated_at: OffsetDateTime,
    reason: Option<String>,
    org_epoch: i64,
) -> Result<DataPlaneRevocationState, Status> {
    Ok(DataPlaneRevocationState {
        epoch: i64_to_u64(epoch, "revocation_epoch")?,
        status: revocation_status(status)? as i32,
        updated_at: Some(timestamp(updated_at)),
        reason,
        org_epoch: i64_to_u64(org_epoch, "org_revocation_epoch")?,
    })
}

fn revocation_status(status: &str) -> Result<DataPlaneRevocationStatus, Status> {
    match status {
        "active" => Ok(DataPlaneRevocationStatus::Active),
        "revoking" => Ok(DataPlaneRevocationStatus::Revoking),
        "revoked" => Ok(DataPlaneRevocationStatus::Revoked),
        other => Err(Status::internal(format!(
            "stored data-plane revocation status is invalid: {other}"
        ))),
    }
}

fn reconnect_reason(value: i32) -> Result<DataPlaneReconnectReason, Status> {
    let reason = DataPlaneReconnectReason::try_from(value)
        .map_err(|_| Status::invalid_argument("reconnect reason is invalid"))?;
    if reason == DataPlaneReconnectReason::Unspecified {
        return Err(Status::invalid_argument("reconnect reason is required"));
    }
    Ok(reason)
}

fn reconnect_reason_from_storage(value: &str) -> Result<DataPlaneReconnectReason, Status> {
    match value {
        "backend_restart" => Ok(DataPlaneReconnectReason::BackendRestart),
        "sandbox_migration" => Ok(DataPlaneReconnectReason::SandboxMigration),
        "route_generation_changed" => Ok(DataPlaneReconnectReason::RouteGenerationChanged),
        "worker_unavailable" => Ok(DataPlaneReconnectReason::WorkerUnavailable),
        "data_plane_restart" => Ok(DataPlaneReconnectReason::DataPlaneRestart),
        other => Err(Status::internal(format!(
            "stored reconnect reason is invalid: {other}"
        ))),
    }
}

trait DataPlaneReconnectReasonExt {
    fn as_str(&self) -> &'static str;
}

impl DataPlaneReconnectReasonExt for DataPlaneReconnectReason {
    fn as_str(&self) -> &'static str {
        match self {
            DataPlaneReconnectReason::BackendRestart => "backend_restart",
            DataPlaneReconnectReason::SandboxMigration => "sandbox_migration",
            DataPlaneReconnectReason::RouteGenerationChanged => "route_generation_changed",
            DataPlaneReconnectReason::WorkerUnavailable => "worker_unavailable",
            DataPlaneReconnectReason::DataPlaneRestart => "data_plane_restart",
            DataPlaneReconnectReason::Unspecified => "unspecified",
        }
    }
}

fn timestamp(value: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}

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
