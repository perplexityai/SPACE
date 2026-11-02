use super::*;
use axum::http::StatusCode;
use pplx_space_common_core::types::{
    ExecutionGeneration, GuestUserName, NodeManagedBy, PodUid, ProcessId, RouteGeneration,
    SandboxActivationAttemptId, SandboxId, SandboxRouteIdentity, SubstrateKind,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Authority, SpaceApiRouteEd25519Token, insert_space_api_route_ed25519_token,
};
use pplx_space_node_api_client::NodeApiOperationBudget;
use regex::Regex;
use std::{future::Future, sync::LazyLock};
use tonic::Code;

const RUNTIME_OPERATION_TIMEOUT_EVENT: &str = "sandbox_runtime_operation_timed_out";
const PROCESS_STREAM_OPEN_TIMEOUT_EVENT: &str = "sandbox_runtime_process_stream_open_timed_out";
static STREAM_OPEN_TRANSPORT_CLOSE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)connection (closed|reset)|goaway|h2 protocol error|http2 error|rst_stream|socket closed|transport error|transport is closing",
    )
    .expect("stream-open transport close regex compiles")
});

pub struct SpacedListOptions {
    pub path: String,
    pub depth: Option<u32>,
    pub user: Option<GuestUserName>,
}

impl SandboxController {
    pub(crate) async fn control_runtime_client(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        action: super::lifecycle::SandboxAction,
    ) -> Result<RuntimeClient, ControllerError> {
        // Drain gate runs **before** the first `runtime_client(...)` call. The
        // kubernetes substrate's `runtime_client` does in-line recovery on
        // non-dormant recoverable lifecycles (`ensure_pod_exists` +
        // `poll_until_command_ready`) when the pod IP is gone, so a guard
        // placed in the `Err(_)` arm fires too late — by then the substrate
        // has already created a replacement pod targeted at
        // `target_node_id`, which on a draining node is exactly the race
        // we're trying to prevent. Use the unconditional gate (not the
        // dormant-only one) because the k8s recovery / wake path fires for
        // any unreachable runtime, including non-dormant lifecycles.
        //
        // `action` is the caller's verb (`Update`, etc.) so the 409 body
        // names the operation the user actually invoked instead of the
        // misleading literal `wake`.
        super::lifecycle::ensure_resolved_node_not_draining(pool, sandbox.id, action).await?;
        // Pass the limiter into the first `runtime_client` call: on the k8s
        // substrate it does in-line recovery (recreating a pod) for a
        // recoverable non-dormant lifecycle whose pod is gone, and that
        // recreation is a real activation the fallback charge below never sees
        // (recovery makes the call succeed, so we never reach the `Err` arm).
        // A live pod that is merely reconnected to charges nothing.
        match self
            .substrate
            .runtime_client(
                sandbox.id,
                crate::controllers::substrate::RecoveryCharge::enforce(
                    self.cluster_admission.as_ref(),
                ),
            )
            .await
        {
            Ok(client) => Ok(client),
            // A recovery admission rejection (429) from the first call must
            // propagate — not fall into the dormant-wake fallback below, which
            // no-ops the charge for a live lifecycle and then `wake_execution`s
            // the pod back anyway, re-opening the lost-pod bypass. Only genuine
            // "not assigned / unreachable" errors should reach the wake path.
            Err(err) if err.status() == StatusCode::TOO_MANY_REQUESTS => Err(err),
            // Any other failure wakes and reconnects only where a wake can
            // give the row a runtime; otherwise, including for a label that
            // does not parse, the last arm returns the error.
            Err(_)
                if sandbox
                    .managed_by
                    .parse::<SubstrateKind>()
                    .is_ok_and(SubstrateKind::wakes_after_failed_control_connect) =>
            {
                // Fallback wake for a dormant sandbox (the non-recovering
                // `runtime_client` path errored). `wake_execution` spends a real
                // node slot for a suspended (Restore) or created (Cold) sandbox.
                // Fails open on limiter errors.
                super::lifecycle::charge_control_wake_activation(
                    self.cluster_admission.as_ref(),
                    sandbox,
                )
                .await?;
                self.substrate
                    .wake_execution(
                        SandboxApiMetricIdentity::native("spaced"),
                        sandbox,
                        DEFAULT_BLOCKING_WAIT_TIMEOUT,
                        None,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await
                    .map_err(ControllerError::from)?;
                self.substrate
                    .runtime_client(
                        sandbox.id,
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await
            }
            Err(err) => Err(err),
        }
    }

    pub(super) async fn spaced_client(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        activity: Option<ExecutionActivityKind>,
    ) -> Result<SpacedSandboxClient, ControllerError> {
        let client = connect_spaced_runtime_client(
            &self.substrate,
            self.cluster_admission.as_ref(),
            true, // initial connect → charge the dormant entrypoint wake
            pool,
            sandbox,
            metric_identity,
            activity,
        )
        .await?;
        let authoritative_sandbox = fetch_authoritative_route_sandbox(pool, sandbox).await?;
        Ok(SpacedSandboxClient {
            client,
            substrate: self.substrate.clone(),
            pool: pool.clone(),
            sandbox: authoritative_sandbox.clone(),
            space_api_route_token: space_api_bridge_route_token(
                self.route_ed25519_authority.as_deref(),
                &authoritative_sandbox,
            )?,
            route_ed25519_authority: self.route_ed25519_authority.clone(),
            metric_identity,
            activity,
            cluster_admission: self.cluster_admission.clone(),
        })
    }

    pub(super) async fn reachable_spaced_client(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        activity: Option<ExecutionActivityKind>,
    ) -> Result<SpacedSandboxClient, ControllerError> {
        super::lifecycle::ensure_access_path_not_blocked_by_drain(
            pool,
            sandbox,
            super::lifecycle::SandboxAction::Spaced,
        )
        .await?;
        let client = self
            .substrate
            .reachable_runtime_client(sandbox.id)
            .await?
            .ok_or_else(|| {
                ControllerError::service_unavailable("sandbox runtime is not reachable")
            })?;
        let authoritative_sandbox = fetch_authoritative_route_sandbox(pool, sandbox).await?;
        Ok(SpacedSandboxClient {
            client,
            substrate: self.substrate.clone(),
            pool: pool.clone(),
            sandbox: authoritative_sandbox.clone(),
            space_api_route_token: space_api_bridge_route_token(
                self.route_ed25519_authority.as_deref(),
                &authoritative_sandbox,
            )?,
            route_ed25519_authority: self.route_ed25519_authority.clone(),
            metric_identity,
            activity,
            cluster_admission: self.cluster_admission.clone(),
        })
    }

    pub async fn spaced_wait_ready(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        id: SandboxId,
        timeout: Option<prost_types::Duration>,
    ) -> Result<SpacedWaitReadyResponse, ControllerError> {
        let mut client = self
            .spaced_client(pool, sandbox, metric_identity, None)
            .await?;
        client
            .spaced_wait_ready(SpacedWaitReadyRequest {
                sandbox_id: id.to_string(),
                timeout,
            })
            .await
    }

    pub async fn spaced_stat(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        id: SandboxId,
        path: String,
        user: Option<GuestUserName>,
    ) -> Result<SpacedStatResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client
            .spaced_stat(SpacedStatRequest {
                sandbox_id: id.to_string(),
                operation: Some(spaced_ops::StatRequest {
                    path,
                    user: user.map(|user| user.as_str().to_string()),
                }),
            })
            .await
    }

    pub async fn spaced_list(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        id: SandboxId,
        options: SpacedListOptions,
    ) -> Result<SpacedFileInfoStream, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client
            .spaced_list(SpacedListRequest {
                sandbox_id: id.to_string(),
                operation: Some(spaced_ops::ListRequest {
                    path: options.path,
                    depth: options.depth,
                    user: options.user.map(|user| user.as_str().to_string()),
                }),
            })
            .await
    }

    pub async fn spaced_read(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedReadRequest,
    ) -> Result<SpacedFileChunkStream, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_read(request).await
    }

    pub async fn spaced_write(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: impl Stream<Item = SpacedWriteRequest> + Send + 'static,
    ) -> Result<SpacedWriteResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_write(request).await
    }

    pub async fn spaced_write_batch(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedWriteBatchRequest,
    ) -> Result<SpacedWriteBatchResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_write_batch(request).await
    }

    pub async fn spaced_mkdir(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedMkdirRequest,
    ) -> Result<SpacedMkdirResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_mkdir(request).await
    }

    pub async fn spaced_remove(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedRemoveRequest,
    ) -> Result<SpacedRemoveResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_remove(request).await
    }

    pub async fn spaced_rename(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedRenameRequest,
    ) -> Result<SpacedRenameResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_rename(request).await
    }

    pub async fn spaced_watch(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedWatchRequest,
    ) -> Result<SpacedFileEventStream, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_watch(request).await
    }

    pub async fn spaced_create_watcher(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedCreateWatcherRequest,
    ) -> Result<SpacedCreateWatcherResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_create_watcher(request).await
    }

    pub async fn spaced_normalize_watch_root(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedNormalizeWatchRootRequest,
    ) -> Result<SpacedNormalizeWatchRootResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_normalize_watch_root(request).await
    }

    pub async fn spaced_get_watcher_events(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedGetWatcherEventsRequest,
    ) -> Result<SpacedGetWatcherEventsResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_get_watcher_events(request).await
    }

    pub async fn spaced_remove_watcher(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedRemoveWatcherRequest,
    ) -> Result<SpacedRemoveWatcherResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_remove_watcher(request).await
    }

    pub async fn spaced_enable_file_changes(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedEnableFileChangesRequest,
    ) -> Result<SpacedEnableFileChangesResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_enable_file_changes(request).await
    }

    pub async fn spaced_disable_file_changes(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedDisableFileChangesRequest,
    ) -> Result<SpacedDisableFileChangesResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_disable_file_changes(request).await
    }

    pub async fn spaced_get_modified_files(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedGetModifiedFilesRequest,
    ) -> Result<SpacedGetModifiedFilesResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_get_modified_files(request).await
    }

    pub async fn spaced_start_process(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedStartProcessRequest,
        open_timeout: Duration,
    ) -> Result<SpacedProcessEventStream, ControllerError> {
        let mut client = self
            .reachable_spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Command),
            )
            .await?;
        tokio::time::timeout(open_timeout, client.spaced_start_process(request))
            .await
            .map_err(|elapsed| {
                process_stream_open_timeout_error(
                    "Process.Start",
                    sandbox.id,
                    open_timeout,
                    elapsed,
                )
            })?
    }

    pub async fn spaced_connect_process(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedConnectProcessRequest,
        open_timeout: Duration,
    ) -> Result<SpacedProcessEventStream, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        tokio::time::timeout(open_timeout, client.spaced_connect_process(request))
            .await
            .map_err(|elapsed| {
                process_stream_open_timeout_error(
                    "Process.Connect",
                    sandbox.id,
                    open_timeout,
                    elapsed,
                )
            })?
    }

    /// Running processes, and with `include_exited` also the exited ones the
    /// sandbox still retains. With `process_id`, only that one, from a
    /// sandbox whose guest supports the filter; an older guest lists all.
    pub async fn spaced_list_processes(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        id: SandboxId,
        include_exited: bool,
        process_id: Option<&ProcessId>,
    ) -> Result<SpacedListProcessesResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client
            .spaced_list_processes(SpacedListProcessesRequest {
                sandbox_id: id.to_string(),
                operation: Some(spaced_ops::ListProcessesRequest {
                    include_exited,
                    process_id: process_id.map(|process_id| process_id.as_str().to_owned()),
                }),
            })
            .await
    }

    pub async fn spaced_send_signal(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedSendSignalRequest,
    ) -> Result<SpacedSendSignalResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_send_signal(request).await
    }

    pub async fn spaced_kill_process(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        request: SpacedKillProcessRequest,
    ) -> Result<SpacedKillProcessResponse, ControllerError> {
        let mut client = self
            .spaced_client(
                pool,
                sandbox,
                metric_identity,
                Some(ExecutionActivityKind::Access),
            )
            .await?;
        client.spaced_kill_process(request).await
    }
}

pub(crate) async fn connect_spaced_runtime_client(
    substrate: &Substrate,
    admission: Option<&std::sync::Arc<crate::cluster_admission::ClusterSandboxAdmission>>,
    // Whether to run the dormant-entrypoint charge for the initial implicit
    // wake. The first connect of a request charges it (created/stopped/suspended
    // → a real activation). A mid-stream reconnect passes `false`: its
    // `sandbox` is the stale clone from request start, so a dormant lifecycle
    // there would re-charge a token already spent (or 429 the retry). The
    // lost-pod recreate on reconnect is still charged via `charge` in the
    // substrate recovery branch.
    charge_entrypoint: bool,
    pool: &PgPool,
    sandbox: &Sandbox,
    metric_identity: SandboxApiMetricIdentity,
    activity: Option<ExecutionActivityKind>,
) -> Result<RuntimeClient, ControllerError> {
    // Both branches go through code paths that can re-place a sandbox on a
    // draining node: `access_runtime_client` runs `ensure_pod_exists` for
    // dormant + recoverable-live lifecycles, and the plain `runtime_client`
    // on the kubernetes substrate also calls `ensure_pod_exists` whenever
    // the pod IP is gone. Gate before either substrate call regardless of
    // `activity`, so status / wait_ready (`None`) can't race the drain
    // reconciler either.
    super::lifecycle::ensure_access_path_not_blocked_by_drain(
        pool,
        sandbox,
        super::lifecycle::SandboxAction::Spaced,
    )
    .await?;

    let charge = crate::controllers::substrate::RecoveryCharge::enforce(admission);

    // The dormant entrypoint charge covers created/stopped/suspended implicit
    // wakes on the initial connect; the non-dormant lost-pod recreate is charged
    // inside the substrate recovery branch via `charge`.
    if charge_entrypoint && let Some(activity) = activity {
        super::lifecycle::charge_implicit_access_activation(admission, sandbox, activity).await?;
    }

    match activity {
        Some(activity) => {
            substrate
                .access_runtime_client(
                    metric_identity,
                    sandbox,
                    activity,
                    DEFAULT_BLOCKING_WAIT_TIMEOUT,
                    charge,
                )
                .await
        }
        None => substrate.runtime_client(sandbox.id, charge).await,
    }
}

pub(super) fn space_api_bridge_route_token(
    authority: Option<&SpaceApiRouteEd25519Authority>,
    sandbox: &Sandbox,
) -> Result<Option<SpaceApiRouteEd25519Token>, ControllerError> {
    let authority = authority.ok_or_else(|| {
        ControllerError::internal("SPACE API route signing authority is unavailable")
    })?;
    let identity = complete_space_api_route_identity(sandbox)?;
    authority
        .mint_space_api_bridge(identity)
        .map(Some)
        .map_err(|error| {
            ControllerError::internal(format!("mint SPACE API Ed25519 route token: {error}"))
        })
}

fn complete_space_api_route_identity(
    sandbox: &Sandbox,
) -> Result<SandboxRouteIdentity, ControllerError> {
    let (
        Some(route_generation),
        Some(current_node_id),
        Some(substrate_kind),
        Some(execution_generation),
        Some(activation_attempt_id),
    ) = (
        sandbox.active_execution_generation,
        sandbox.active_node_id,
        sandbox.active_node_managed_by,
        sandbox.active_execution_generation,
        sandbox.active_activation_attempt_id,
    )
    else {
        return Err(ControllerError::internal(format!(
            "sandbox {} has incomplete authoritative route identity",
            sandbox.id
        )));
    };
    let route_generation = u64::try_from(route_generation)
        .ok()
        .and_then(|value| RouteGeneration::new(value).ok())
        .ok_or_else(|| ControllerError::internal("route generation is invalid"))?;
    let execution_generation = ExecutionGeneration::new(execution_generation)
        .map_err(|error| ControllerError::internal(error.to_string()))?;
    SandboxRouteIdentity::try_new(
        sandbox.id,
        route_generation,
        current_node_id,
        substrate_kind,
        execution_generation,
        activation_attempt_id,
        sandbox.active_pod_uid.clone(),
    )
    .map_err(|error| ControllerError::internal(error.to_string()))
}

#[derive(sqlx::FromRow)]
struct AuthoritativeRouteIdentityRow {
    active_node_id: Option<NodeId>,
    active_node_managed_by: Option<NodeManagedBy>,
    active_execution_generation: Option<i64>,
    active_pod_uid: Option<PodUid>,
    active_activation_attempt_id: Option<SandboxActivationAttemptId>,
}

async fn fetch_authoritative_route_sandbox(
    pool: &PgPool,
    sandbox: &Sandbox,
) -> Result<Sandbox, ControllerError> {
    let route_identity = sqlx::query_as::<_, AuthoritativeRouteIdentityRow>(sql!(
        "SELECT COALESCE(direct_route.current_node_id, s.active_node_id) AS active_node_id,
                COALESCE(direct_route.substrate_kind, active_node.managed_by)
                    AS active_node_managed_by,
                COALESCE(active_execution.generation, direct_route.execution_generation)
                    AS active_execution_generation,
                COALESCE(active_execution.pod_uid, direct_route.pod_uid) AS active_pod_uid,
                COALESCE(
                    active_execution.route_activation_id,
                    direct_route.activation_attempt_id
                ) AS active_activation_attempt_id
         FROM sandboxes s
         LEFT JOIN sandbox_direct_route_identities direct_route
             ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes active_node
             ON active_node.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN LATERAL (
             SELECT e.generation,
                    NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
                    e.route_activation_id
             FROM sandbox_k8s_executions e
             WHERE e.sandbox_id = s.id
               AND e.active
             ORDER BY e.generation DESC
             LIMIT 1
         ) active_execution ON TRUE
         WHERE s.id = $1"
    ))
    .bind(sandbox.id)
    .fetch_optional(pool)
    .await
    .map_err(|error| db_contention_error("reload authoritative sandbox route identity", &error))?
    .ok_or_else(|| ControllerError::not_found("sandbox no longer exists"))?;

    let mut authoritative = sandbox.clone();
    authoritative.active_node_id = route_identity.active_node_id;
    authoritative.active_node_managed_by = route_identity.active_node_managed_by;
    authoritative.active_execution_generation = route_identity.active_execution_generation;
    authoritative.active_pod_uid = route_identity.active_pod_uid;
    authoritative.active_activation_attempt_id = route_identity.active_activation_attempt_id;
    Ok(authoritative)
}

fn process_stream_open_timeout_error(
    operation: &'static str,
    sandbox_id: SandboxId,
    open_timeout: Duration,
    _elapsed: tokio::time::error::Elapsed,
) -> ControllerError {
    tracing::warn!(
        event_type = PROCESS_STREAM_OPEN_TIMEOUT_EVENT,
        operation,
        %sandbox_id,
        open_timeout_ms = open_timeout.as_millis(),
        "sandbox runtime process stream open timed out after runtime client acquisition",
    );
    ControllerError::new(
        StatusCode::REQUEST_TIMEOUT,
        "sandbox runtime process stream open timed out before receiving a backend stream",
    )
}

pub(super) struct SpacedSandboxClient {
    client: RuntimeClient,
    substrate: Substrate,
    pool: PgPool,
    sandbox: Sandbox,
    space_api_route_token: Option<SpaceApiRouteEd25519Token>,
    route_ed25519_authority: Option<std::sync::Arc<SpaceApiRouteEd25519Authority>>,
    metric_identity: SandboxApiMetricIdentity,
    activity: Option<ExecutionActivityKind>,
    /// Carried so a mid-stream `reconnect_node_api` can enforce recovery
    /// admission when the reconnect discovers a lost pod and recreates it.
    cluster_admission: Option<std::sync::Arc<crate::cluster_admission::ClusterSandboxAdmission>>,
}

#[derive(Debug)]
enum SpacedRuntimeCallError {
    Status(tonic::Status),
    Timeout(ControllerError),
}

impl From<SpacedRuntimeCallError> for ControllerError {
    fn from(err: SpacedRuntimeCallError) -> Self {
        match err {
            SpacedRuntimeCallError::Status(status) => status.into(),
            SpacedRuntimeCallError::Timeout(err) => err,
        }
    }
}

fn runtime_operation_timeout_error(
    operation: &'static str,
    sandbox_id: SandboxId,
    timeout: Duration,
    _elapsed: tokio::time::error::Elapsed,
) -> ControllerError {
    tracing::warn!(
        event_type = RUNTIME_OPERATION_TIMEOUT_EVENT,
        operation,
        %sandbox_id,
        timeout_ms = timeout.as_millis(),
        "sandbox runtime operation timed out",
    );
    ControllerError::new(
        StatusCode::REQUEST_TIMEOUT,
        format!("sandbox runtime operation timed out: {operation}"),
    )
}

async fn timeout_runtime_call<T, F>(
    sandbox_id: SandboxId,
    operation: &'static str,
    budget: NodeApiOperationBudget,
    future: F,
) -> Result<tonic::Response<T>, SpacedRuntimeCallError>
where
    F: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    let timeout = budget.timeout();
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|elapsed| {
            SpacedRuntimeCallError::Timeout(runtime_operation_timeout_error(
                operation, sandbox_id, timeout, elapsed,
            ))
        })?
        .map_err(SpacedRuntimeCallError::Status)
}

macro_rules! spaced_client_call {
    ($method:ident, $request:ty, $response:ty, $budget:expr) => {
        async fn $method(&mut self, request: $request) -> Result<$response, ControllerError> {
            let runtime_request = self.runtime_request(request.clone())?;
            let response = match timeout_runtime_call(
                self.sandbox.id,
                stringify!($method),
                $budget,
                self.client.$method(runtime_request),
            )
            .await
            {
                Ok(response) => response,
                Err(SpacedRuntimeCallError::Status(status))
                    if pplx_space_node_api_client::is_node_api_draining_status(&status) =>
                {
                    self.reconnect_node_api().await?;
                    let runtime_request = self.runtime_request(request)?;
                    timeout_runtime_call(
                        self.sandbox.id,
                        stringify!($method),
                        $budget,
                        self.client.$method(runtime_request),
                    )
                    .await?
                }
                Err(err) => {
                    self.retire_after_call_failure(&err);
                    return Err(err.into());
                }
            };
            Ok(response.into_inner())
        }
    };
}

macro_rules! spaced_unbounded_stream_open_call {
    ($method:ident, $request:ty, $response:ty) => {
        async fn $method(&mut self, request: $request) -> Result<$response, ControllerError> {
            let runtime_request = self.runtime_request(request.clone())?;
            let response = match self.client.$method(runtime_request).await {
                Ok(response) => response,
                Err(status) if should_retry_node_api_stream_open(&status) => {
                    self.reconnect_node_api().await?;
                    let runtime_request = self.runtime_request(request)?;
                    self.client.$method(runtime_request).await?
                }
                Err(status) => {
                    self.retire_after_transport_failure(&status);
                    return Err(status.into());
                }
            };
            Ok(response.into_inner())
        }
    };
}

macro_rules! spaced_stream_open_call {
    ($method:ident, $request:ty, $response:ty, $budget:expr) => {
        async fn $method(&mut self, request: $request) -> Result<$response, ControllerError> {
            let runtime_request = self.runtime_request(request.clone())?;
            let response = match timeout_runtime_call(
                self.sandbox.id,
                stringify!($method),
                $budget,
                self.client.$method(runtime_request),
            )
            .await
            {
                Ok(response) => response,
                Err(SpacedRuntimeCallError::Status(status))
                    if should_retry_node_api_stream_open(&status) =>
                {
                    self.reconnect_node_api().await?;
                    let runtime_request = self.runtime_request(request)?;
                    timeout_runtime_call(
                        self.sandbox.id,
                        stringify!($method),
                        $budget,
                        self.client.$method(runtime_request),
                    )
                    .await?
                }
                Err(err) => {
                    self.retire_after_call_failure(&err);
                    return Err(err.into());
                }
            };
            Ok(response.into_inner())
        }
    };
}

fn should_retry_node_api_stream_open(status: &tonic::Status) -> bool {
    pplx_space_node_api_client::is_node_api_draining_status(status)
        || is_tonic_transport_close_status(status)
}

/// A failure after which later calls should reach the pod over a new
/// connection: a drain or a closed transport, or a timeout, which is how a
/// peer lost without closing the socket looks.
fn should_retire_kept_connection(status: &tonic::Status) -> bool {
    should_retry_node_api_stream_open(status)
        || matches!(status.code(), Code::DeadlineExceeded | Code::Cancelled)
}

fn is_tonic_transport_close_status(status: &tonic::Status) -> bool {
    if !matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::Internal
    ) {
        return false;
    }

    STREAM_OPEN_TRANSPORT_CLOSE_RE.is_match(status.message())
}

impl SpacedSandboxClient {
    fn runtime_request<T>(&self, request: T) -> Result<tonic::Request<T>, ControllerError> {
        let mut request = tonic::Request::new(request);
        if let Some(route_token) = self.space_api_route_token.as_ref() {
            insert_space_api_route_ed25519_token(request.metadata_mut(), route_token).map_err(
                |error| {
                    ControllerError::service_unavailable(format!(
                        "attach SPACE API Ed25519 route token: {error}"
                    ))
                },
            )?;
        }
        Ok(request)
    }

    /// Stop sharing this request's cached channel after a failure that shows
    /// later calls need a new connection. The connection usually closed
    /// already, which retires it anyway; a draining node-api keeps it open.
    fn retire_after_transport_failure(&self, status: &tonic::Status) {
        if should_retire_kept_connection(status) {
            self.client.retire_kept_connection();
        }
    }

    /// [`Self::retire_after_transport_failure`], plus a call that ran out its
    /// budget: a peer that vanished without closing the socket shows up as a
    /// timeout, and the next caller's dial is what notices it is gone.
    fn retire_after_call_failure(&self, err: &SpacedRuntimeCallError) {
        match err {
            SpacedRuntimeCallError::Status(status) => self.retire_after_transport_failure(status),
            SpacedRuntimeCallError::Timeout(_) => self.client.retire_kept_connection(),
        }
    }

    async fn reconnect_node_api(&mut self) -> Result<(), ControllerError> {
        // A mid-stream reconnect normally re-attaches to the existing live pod
        // (free — the dormant-entrypoint charge no-ops for a live lifecycle, so
        // the original wake isn't double-counted). But if the reconnect finds
        // the pod gone and recreates it, that is a fresh activation, so enforce
        // recovery admission rather than exempting the whole path.
        //
        // Every reconnect follows a draining node-api or a closed transport,
        // so a cached channel to the same pod must not be handed back: retire
        // it and dial afresh, as a reconnect always has.
        self.client.retire_kept_connection();
        self.client = connect_spaced_runtime_client(
            &self.substrate,
            self.cluster_admission.as_ref(),
            // Mid-stream reconnect: the initial wake was already charged, and
            // `self.sandbox` is the stale request-start clone, so skip the
            // dormant entrypoint charge. Recovery (lost-pod recreate) still
            // charges via the recovery policy derived by the helper.
            false,
            &self.pool,
            &self.sandbox,
            self.metric_identity,
            self.activity,
        )
        .await?;
        self.sandbox = fetch_authoritative_route_sandbox(&self.pool, &self.sandbox).await?;
        self.space_api_route_token =
            space_api_bridge_route_token(self.route_ed25519_authority.as_deref(), &self.sandbox)?;
        Ok(())
    }

    spaced_client_call!(
        spaced_stat,
        SpacedStatRequest,
        SpacedStatResponse,
        NodeApiOperationBudget::FilesystemMetadata
    );
    async fn spaced_write_batch(
        &mut self,
        request: SpacedWriteBatchRequest,
    ) -> Result<SpacedWriteBatchResponse, ControllerError> {
        let runtime_request = self.runtime_request(request.clone())?;
        let response = match self.client.spaced_write_batch(runtime_request).await {
            Ok(response) => response,
            Err(status) if pplx_space_node_api_client::is_node_api_draining_status(&status) => {
                self.reconnect_node_api().await?;
                let runtime_request = self.runtime_request(request)?;
                self.client.spaced_write_batch(runtime_request).await?
            }
            Err(status) => {
                self.retire_after_transport_failure(&status);
                return Err(status.into());
            }
        };
        Ok(response.into_inner())
    }
    spaced_client_call!(
        spaced_mkdir,
        SpacedMkdirRequest,
        SpacedMkdirResponse,
        NodeApiOperationBudget::FilesystemMutation
    );
    spaced_client_call!(
        spaced_remove,
        SpacedRemoveRequest,
        SpacedRemoveResponse,
        NodeApiOperationBudget::FilesystemMutation
    );
    spaced_client_call!(
        spaced_rename,
        SpacedRenameRequest,
        SpacedRenameResponse,
        NodeApiOperationBudget::FilesystemMutation
    );
    spaced_client_call!(
        spaced_create_watcher,
        SpacedCreateWatcherRequest,
        SpacedCreateWatcherResponse,
        NodeApiOperationBudget::FilesystemMutation
    );
    spaced_client_call!(
        spaced_normalize_watch_root,
        SpacedNormalizeWatchRootRequest,
        SpacedNormalizeWatchRootResponse,
        NodeApiOperationBudget::FilesystemMetadata
    );
    spaced_client_call!(
        spaced_get_watcher_events,
        SpacedGetWatcherEventsRequest,
        SpacedGetWatcherEventsResponse,
        NodeApiOperationBudget::FilesystemMetadata
    );
    spaced_client_call!(
        spaced_remove_watcher,
        SpacedRemoveWatcherRequest,
        SpacedRemoveWatcherResponse,
        NodeApiOperationBudget::FilesystemMutation
    );
    spaced_client_call!(
        spaced_enable_file_changes,
        SpacedEnableFileChangesRequest,
        SpacedEnableFileChangesResponse,
        NodeApiOperationBudget::FilesystemMutation
    );
    spaced_client_call!(
        spaced_disable_file_changes,
        SpacedDisableFileChangesRequest,
        SpacedDisableFileChangesResponse,
        NodeApiOperationBudget::FilesystemMutation
    );
    spaced_client_call!(
        spaced_get_modified_files,
        SpacedGetModifiedFilesRequest,
        SpacedGetModifiedFilesResponse,
        NodeApiOperationBudget::FilesystemMetadata
    );
    spaced_client_call!(
        spaced_list_processes,
        SpacedListProcessesRequest,
        SpacedListProcessesResponse,
        NodeApiOperationBudget::ProcessMetadata
    );
    spaced_client_call!(
        spaced_send_signal,
        SpacedSendSignalRequest,
        SpacedSendSignalResponse,
        NodeApiOperationBudget::ProcessControl
    );
    spaced_client_call!(
        spaced_kill_process,
        SpacedKillProcessRequest,
        SpacedKillProcessResponse,
        NodeApiOperationBudget::ProcessControl
    );
    spaced_stream_open_call!(
        spaced_list,
        SpacedListRequest,
        SpacedFileInfoStream,
        NodeApiOperationBudget::FilesystemMetadata
    );
    spaced_stream_open_call!(
        spaced_read,
        SpacedReadRequest,
        SpacedFileChunkStream,
        NodeApiOperationBudget::FilesystemTransferOpen
    );

    async fn spaced_write<S>(&mut self, request: S) -> Result<SpacedWriteResponse, ControllerError>
    where
        S: Stream<Item = SpacedWriteRequest> + Send + 'static,
    {
        let runtime_request = self.runtime_request(request)?;
        match self.client.spaced_write(runtime_request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => {
                self.retire_after_transport_failure(&status);
                Err(status.into())
            }
        }
    }
    spaced_stream_open_call!(
        spaced_watch,
        SpacedWatchRequest,
        SpacedFileEventStream,
        NodeApiOperationBudget::FilesystemTransferOpen
    );
    async fn spaced_wait_ready(
        &mut self,
        request: SpacedWaitReadyRequest,
    ) -> Result<SpacedWaitReadyResponse, ControllerError> {
        let runtime_request = self.runtime_request(request.clone())?;
        let response = match self.client.spaced_wait_ready(runtime_request).await {
            Ok(response) => response,
            Err(status) if pplx_space_node_api_client::is_node_api_draining_status(&status) => {
                self.reconnect_node_api().await?;
                let runtime_request = self.runtime_request(request)?;
                self.client.spaced_wait_ready(runtime_request).await?
            }
            Err(status) => {
                self.retire_after_transport_failure(&status);
                return Err(status.into());
            }
        };
        Ok(response.into_inner())
    }
    spaced_unbounded_stream_open_call!(
        spaced_start_process,
        SpacedStartProcessRequest,
        SpacedProcessEventStream
    );
    spaced_unbounded_stream_open_call!(
        spaced_connect_process,
        SpacedConnectProcessRequest,
        SpacedProcessEventStream
    );
}

#[cfg(test)]
mod tests;
