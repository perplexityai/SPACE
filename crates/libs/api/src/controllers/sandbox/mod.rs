use futures_core::Stream;
use pplx_space_common_core::sql;
#[cfg(test)]
use pplx_space_common_core::types::AutoLifecycleRuntimeState;
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, DEFAULT_TEMPLATE_TAG, Email, EnvMap, EnvSanitizationIssue, NodeId, OrgId,
    PolicyAuthority, RuntimeKind, SandboxAutoLifecyclePatch, SandboxId, SandboxLifecycle, SemVer,
    TemplateDesiredState, TemplateName, TemplateReference, TemplateTag, TemplateVisibility,
};
use pplx_space_common_svc::types::{NetworkPolicyTarget, ReconcileFacet};
use pplx_space_metrics::sandbox::api::{
    ReconnectAttempt, ReconnectEntrypoint, ReconnectOutcome, ReconnectReason,
    ReconnectTerminalPhase, SandboxApiMetricIdentity, SandboxApiSurface,
};
use pplx_space_net_common::{NetworkPolicy, NetworkPolicyV1, proto::NetworkPolicySource};
use pplx_space_node_mgr_client::proto::{AllowPortRequest, ExposePortRequest, NetworkProtocol};
use pplx_space_sbx_mgr_client::operations as spaced_ops;
use pplx_space_sbx_mgr_client::proto::{
    AutoLifecycleTouchResult, CreateSnapshotRequest, KillRequest, ListSnapshotsRequest,
    PauseRequest, ResumeRequest, RollbackRequest as SbxRollbackRequest, SandboxInfo, SnapshotType,
    SpacedConnectProcessRequest, SpacedCreateWatcherRequest, SpacedCreateWatcherResponse,
    SpacedDisableFileChangesRequest, SpacedDisableFileChangesResponse,
    SpacedEnableFileChangesRequest, SpacedEnableFileChangesResponse, SpacedFileChunk,
    SpacedFileEvent, SpacedFileInfo, SpacedGetModifiedFilesRequest, SpacedGetModifiedFilesResponse,
    SpacedGetStatusRequest, SpacedGetStatusResponse, SpacedGetWatcherEventsRequest,
    SpacedGetWatcherEventsResponse, SpacedKillProcessRequest, SpacedKillProcessResponse,
    SpacedListProcessesRequest, SpacedListProcessesResponse, SpacedListRequest, SpacedMkdirRequest,
    SpacedMkdirResponse, SpacedNormalizeWatchRootRequest, SpacedNormalizeWatchRootResponse,
    SpacedPingGuestRequest, SpacedPingGuestResponse, SpacedProcessEvent, SpacedReadRequest,
    SpacedRemoveRequest, SpacedRemoveResponse, SpacedRemoveWatcherRequest,
    SpacedRemoveWatcherResponse, SpacedRenameRequest, SpacedRenameResponse,
    SpacedSendSignalRequest, SpacedSendSignalResponse, SpacedStartProcessRequest,
    SpacedStatRequest, SpacedStatResponse, SpacedWaitReadyRequest, SpacedWaitReadyResponse,
    SpacedWatchRequest, SpacedWriteBatchRequest, SpacedWriteBatchResponse, SpacedWriteRequest,
    SpacedWriteResponse, StatsRequest, StopRequest, TouchAutoLifecycleRequest,
    TouchAutoLifecycleResponse, UpdateRequest, WakeRequest,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use std::time::Duration;
use uuid::Uuid;

use super::substrate::{
    ActivationOptions, ActorRef, CreateExecutionRequest, ExecutionActivityKind, NodeApiTarget,
    NodeInventoryEntry, RuntimeClient, SandboxResources, Substrate, SubstrateAdapter,
    SubstrateError, TemplateRef, WaitReadyTimeoutPhase,
};
use super::{ControllerError, ControllerRetryAfterCause, db_contention_error};
use crate::controllers::auto_lifecycle::{
    policy_from_proto, status_from_proto, validate_v1_policy,
};
use crate::models::{
    CreateSandboxRequest, NetworkPolicyCondition, NetworkPolicyStatus, Sandbox,
    SandboxActivationRequest, SandboxNetworkPolicyPatch, SandboxNodeSummary, SandboxPortExposure,
    SandboxPortProtocol, SandboxPortVisibility, SandboxResourceType, SandboxResponse,
    UpdateSandboxRequest, User,
};
use crate::reconcile_queue::{
    FinalizeSandboxDbDeleteError, ReconcileRef, any_creating_snapshot_for_sandbox,
    delete_reconcile_state_for_elements, enqueue_node_reconcile_refs_and_trigger_tx,
    enqueue_sandbox_reconcile_and_trigger, enqueue_sandbox_reconcile_and_trigger_tx,
    finalize_sandbox_db_delete_user_initiated, sandbox_reconcile_cleanup_refs,
};
#[derive(Debug, Clone, FromRow)]
struct SandboxObservedState {
    sandbox_id: SandboxId,
    node_id: Option<NodeId>,
    node_name: Option<String>,
    node_role: Option<String>,
    node_managed_by: Option<String>,
    node_api_endpoint: Option<String>,
    substrate_node_id: Option<String>,
    template: Option<String>,
    status: Option<String>,
    spaced_version: Option<SemVer>,
    pid: Option<i32>,
    namespace: Option<String>,
    uptime: Option<String>,
    sandbox_owner: Option<String>,
    image_hash: Option<String>,
    snapshots_enabled: bool,
    disk_usage_bytes: Option<i64>,
    pool_template: Option<String>,
    failure_reason: Option<String>,
    observed_created_at: Option<String>,
    observed_at: time::OffsetDateTime,
    release_tags: Vec<String>,
    observed_network_policy: Option<sqlx::types::Json<NetworkPolicy>>,
    observed_network_policy_generation: Option<i64>,
    observed_network_policy_source: Option<String>,
    observed_network_policy_authority: Option<String>,
    observed_network_policy_at: Option<time::OffsetDateTime>,
    network_policy_conditions: sqlx::types::Json<Vec<NetworkPolicyCondition>>,
}

/// Budget for blocking create/restore/wake/access waits. Sized below the
/// per-request node-RPC channel timeout (see
/// [`super::substrate::NODE_RPC_BLOCKING_BUDGET`]) so SPACE returns a
/// structured, retryable response — with the sandbox id — before tonic severs
/// the channel with a bare `Cancelled` (the opaque 408 cliff).
const DEFAULT_BLOCKING_WAIT_TIMEOUT: Duration = super::substrate::NODE_RPC_BLOCKING_BUDGET;
const SANDBOX_PORT_APPLY_APPLIED: &str = "applied";
const SANDBOX_PORT_DESIRED_OPEN: &str = "open";
const SANDBOX_PORT_BRIDGE_DIRECT: &str = "direct";
const SANDBOX_PORT_BRIDGE_LOOPBACK_FORWARDED: &str = "loopback_forwarded";
const SANDBOX_PORT_OBSERVATION_FRESHNESS_SECS: i64 = 60;
const SANDBOX_PORT_PROXY_FAILURE_BRIDGE_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long after a lifecycle flip a `started` sandbox is still treated as
/// plausibly waking by [`SandboxWakeSignal::is_plausibly_waking`].
///
/// A wake/restore finalization flips the row to `started` while the in-guest
/// dynamic-port listeners are still being re-established, and the same UPDATE
/// clears the restore lease (the `sandboxes_clear_restore_lease` trigger
/// nulls the lease on any write that leaves the row non-`restoring`), so the
/// recency of the flip is the only durable post-flip wake signal. The grace
/// must cover the listener re-arm worst case: the guest normally re-binds
/// within seconds, and the per-node exposure reconciler re-arms a missed
/// listener within one reconcile tick (~60s). Two ticks plus margin keeps a
/// listener that needed the reconciler classified as retryable instead of
/// dead-ending the client's retry budget.
pub(crate) const POST_WAKE_LISTENER_REBIND_GRACE: Duration = Duration::from_secs(150);

fn duration_from_millis(milliseconds: u64) -> prost_types::Duration {
    prost_types::Duration {
        seconds: (milliseconds / 1_000) as i64,
        nanos: ((milliseconds % 1_000) * 1_000_000) as i32,
    }
}

struct ResolvedCreateTemplate {
    template_release_id: Uuid,
    content_hash: String,
    default_auto_lifecycle: AutoLifecyclePolicy,
    default_network_policy: NetworkPolicyV1,
    default_network_policy_generation: u64,
}

#[derive(Debug)]
struct CreateTemplateDefaults {
    network_policy: sqlx::types::Json<NetworkPolicy>,
    network_policy_generation: i64,
    default_auto_lifecycle: Option<sqlx::types::Json<AutoLifecyclePolicy>>,
}

fn validate_create_template_reference(template: &TemplateReference) -> Result<(), ControllerError> {
    if template.artifact_digest_prefix().is_some() {
        return Err(ControllerError::bad_request(
            "template digest prefixes are not supported yet; use a full sha256 digest",
        ));
    }
    Ok(())
}

/// Builds the not-found error for an unresolvable template reference.
///
/// The create-time authorization gate reuses this for cross-org denials so a
/// denied caller sees byte-identical output to a truly-absent template and
/// cannot probe which template names exist in other orgs.
fn template_release_not_found(
    template: &TemplateReference,
    runtime: RuntimeKind,
) -> ControllerError {
    let template_name = template.name().as_str();
    if let Some(digest) = template.artifact_digest() {
        return ControllerError::not_found(format!(
            "no release found for template '{template_name}' hash '{digest}'"
        ));
    }
    match template.tag() {
        Some(tag) => ControllerError::not_found(format!(
            "no release found for template '{template_name}' runtime '{}' tag '{}'",
            runtime.alias(),
            tag.as_str(),
        )),
        None => ControllerError::bad_request("template must use a tag or full digest"),
    }
}

/// Parses a `templates.visibility` value read from the database.
///
/// The column CHECK keeps stored values inside the enum's vocabulary, so a
/// parse failure means schema/code drift and surfaces as an internal error.
fn parse_stored_visibility(
    template_name: &str,
    visibility: &str,
) -> Result<TemplateVisibility, ControllerError> {
    TemplateVisibility::parse(visibility).map_err(|error| {
        ControllerError::internal(format!(
            "template '{template_name}' has an invalid stored visibility: {error}"
        ))
    })
}

/// Outcome of the template authorization gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TemplateAccess {
    /// The `templates` row exists and the caller may use it.
    Authorized { visibility: TemplateVisibility },
    /// No `templates` row exists under the requested name.
    Absent,
    /// The row is another org's org-specific template. Callers must surface
    /// this exactly like an unknown template (404, identical message) so the
    /// gate doesn't leak which template names exist in other orgs.
    Denied,
}

/// Gates the `templates` row by name before any release/tag/digest lookup.
///
/// The read is deliberately unfiltered — no org predicate in SQL — so the
/// authorization decision happens in one place, in code, against the parsed
/// [`TemplateVisibility`].
pub(crate) async fn gate_template_access(
    pool: &PgPool,
    template_name: &TemplateName,
    caller_org: OrgId,
) -> Result<TemplateAccess, ControllerError> {
    let template_name = template_name.as_str();
    let Some((visibility, owner_org, desired_state)) =
        sqlx::query_as::<_, (String, Option<Uuid>, String)>(sql!(
            "SELECT visibility, org_id, desired_state
         FROM templates
         WHERE name = $1"
        ))
        .bind(template_name)
        .fetch_optional(pool)
        .await
        .map_err(|err| db_contention_error("failed to load template for authorization", &err))?
    else {
        return Ok(TemplateAccess::Absent);
    };

    if desired_state != TemplateDesiredState::Present.as_str() {
        return Ok(TemplateAccess::Absent);
    }

    let visibility = parse_stored_visibility(template_name, &visibility)?;
    if visibility.permits(owner_org.map(OrgId::from_uuid), caller_org) {
        Ok(TemplateAccess::Authorized { visibility })
    } else {
        tracing::warn!(
            template_name,
            caller_org = %caller_org,
            "template authorization denied; reporting not-found",
        );
        Ok(TemplateAccess::Denied)
    }
}

async fn resolve_template_release(
    pool: &PgPool,
    template: &TemplateReference,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<(Uuid, String), ControllerError> {
    validate_create_template_reference(template)?;
    let template_name = template.name().as_str();

    if gate_template_access(pool, template.name(), caller_org).await? == TemplateAccess::Denied {
        return Err(template_release_not_found(template, runtime));
    }

    // The release lookups repeat the gate's visibility predicate in SQL:
    // the gate and the lookups are separate READ COMMITTED statements, so a
    // template published or flipped to org_specific in between must still
    // not resolve cross-org at read time.
    if let Some(digest) = template.artifact_digest() {
        let resolved = crate::template_releases_store::resolve_template_release_by_digest(
            pool,
            template.name(),
            digest,
            runtime,
            caller_org,
        )
        .await
        .map_err(|err| db_contention_error("failed to resolve template release by hash", &err))?;

        return resolved.ok_or_else(|| template_release_not_found(template, runtime));
    }

    let tag = template
        .tag()
        .ok_or_else(|| ControllerError::bad_request("template must use a tag or full digest"))?;
    let runtime_alias = runtime.alias();
    if let Some(release) =
        resolve_template_release_by_tag(pool, template.name(), tag, runtime, caller_org).await?
    {
        return Ok(release);
    }

    if tag.as_str() != DEFAULT_TEMPLATE_TAG {
        tracing::warn!(
            template_name_len = template_name.len(),
            runtime = runtime_alias,
            requested_tag_len = tag.as_str().len(),
            fallback_tag = DEFAULT_TEMPLATE_TAG,
            "template tag not found; falling back to latest",
        );
        if let Some(release) = resolve_template_release_by_tag(
            pool,
            template.name(),
            &TemplateTag::latest(),
            runtime,
            caller_org,
        )
        .await?
        {
            return Ok(release);
        }
    }

    Err(template_release_not_found(template, runtime))
}

pub(crate) async fn resolve_template_release_by_tag(
    pool: &PgPool,
    template_name: &TemplateName,
    tag: &TemplateTag,
    runtime: RuntimeKind,
    caller_org: OrgId,
) -> Result<Option<(Uuid, String)>, ControllerError> {
    crate::template_releases_store::resolve_template_release_by_tag(
        pool,
        template_name,
        tag,
        runtime,
        caller_org,
    )
    .await
    .map_err(|err| db_contention_error("failed to resolve template release by tag", &err))
}

async fn load_create_template_defaults(
    pool: &PgPool,
    template_name: &str,
    content_hash: &str,
    runtime: &str,
) -> Result<CreateTemplateDefaults, ControllerError> {
    let (network_policy, network_policy_generation) =
        sqlx::query_as::<_, (sqlx::types::Json<NetworkPolicy>, i64)>(sql!(
            "SELECT network_policy_json, network_policy_generation
         FROM templates
         WHERE name = $1
           AND desired_state = $2"
        ))
        .bind(template_name)
        .bind(TemplateDesiredState::Present.as_str())
        .fetch_optional(pool)
        .await
        .map_err(|err| db_contention_error("failed to load template network policy", &err))?
        .unwrap_or_else(|| (sqlx::types::Json(NetworkPolicyV1::deny_all()), 0));

    let default_auto_lifecycle =
        sqlx::query_scalar::<_, sqlx::types::Json<AutoLifecyclePolicy>>(sql!(
            "WITH desired AS (
             SELECT default_auto_lifecycle_json
             FROM template_default_auto_lifecycle_policies
             WHERE template = $1
         ),
         observed AS (
             SELECT nti.default_auto_lifecycle_json
             FROM node_template_inventory nti
             JOIN nodes n ON n.id = nti.node_id
             WHERE nti.template_name = $1
               AND nti.content_hash = $2
               AND (nti.runtime IS NULL OR nti.runtime = '' OR nti.runtime = $3)
               AND nti.default_auto_lifecycle_json IS NOT NULL
               AND n.role = 'builder'
             ORDER BY nti.observed_at DESC
             LIMIT 1
         )
         SELECT default_auto_lifecycle_json FROM desired
         UNION ALL
         SELECT default_auto_lifecycle_json FROM observed
         WHERE NOT EXISTS (SELECT 1 FROM desired)
         LIMIT 1"
        ))
        .bind(template_name)
        .bind(content_hash)
        .bind(runtime)
        .fetch_optional(pool)
        .await
        .map_err(|err| db_contention_error("failed to load template auto lifecycle", &err))?;

    Ok(CreateTemplateDefaults {
        network_policy,
        network_policy_generation,
        default_auto_lifecycle,
    })
}

fn create_auto_lifecycle_policy(
    policy: Option<AutoLifecyclePolicy>,
) -> Result<AutoLifecyclePolicy, ControllerError> {
    let mut policy = policy.unwrap_or_else(AutoLifecyclePolicy::disabled);
    validate_v1_policy(&policy).map_err(|error| {
        ControllerError::bad_request(format!("invalid auto_lifecycle: {error}"))
    })?;
    policy.normalize_in_place();
    Ok(policy)
}

fn reconnect_entrypoint(identity: SandboxApiMetricIdentity) -> Option<ReconnectEntrypoint> {
    match (identity.surface, identity.op) {
        (SandboxApiSurface::Ecw, "connect") => Some(ReconnectEntrypoint::Connect),
        (_, "access") => Some(ReconnectEntrypoint::Dataplane),
        _ => None,
    }
}

fn parse_reconnect_lifecycle(lifecycle: &str) -> Option<SandboxLifecycle> {
    lifecycle
        .trim()
        .to_ascii_lowercase()
        .parse::<SandboxLifecycle>()
        .ok()
}

fn reconnect_initial_lifecycle_tag(lifecycle: &str) -> &'static str {
    parse_reconnect_lifecycle(lifecycle).map_or("unknown", SandboxLifecycle::as_str)
}

fn initial_lifecycle_terminal_phase(lifecycle: &str) -> ReconnectTerminalPhase {
    match parse_reconnect_lifecycle(lifecycle) {
        Some(
            SandboxLifecycle::Suspended
            | SandboxLifecycle::Suspending
            | SandboxLifecycle::Restoring,
        ) => ReconnectTerminalPhase::Restore,
        Some(
            SandboxLifecycle::Paused
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Started
            | SandboxLifecycle::Starting,
        ) => ReconnectTerminalPhase::Wake,
        Some(
            SandboxLifecycle::Created
            | SandboxLifecycle::Creating
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Stopped
            | SandboxLifecycle::Deleting
            | SandboxLifecycle::Checkpointing
            | SandboxLifecycle::Quarantined
            | SandboxLifecycle::Failed,
        )
        | None => ReconnectTerminalPhase::Api,
    }
}

fn initial_lifecycle_may_need_wake(lifecycle: &str) -> bool {
    matches!(
        parse_reconnect_lifecycle(lifecycle),
        Some(
            SandboxLifecycle::Paused
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Suspended
                | SandboxLifecycle::Suspending
                | SandboxLifecycle::Restoring
        )
    )
}

fn successful_access_reconnect_outcome(
    initial_lifecycle: &str,
) -> Option<(ReconnectOutcome, ReconnectTerminalPhase, ReconnectReason)> {
    if initial_lifecycle_may_need_wake(initial_lifecycle) {
        Some((
            ReconnectOutcome::WakeSubmitted,
            ReconnectTerminalPhase::Ready,
            ReconnectReason::Ok,
        ))
    } else {
        None
    }
}

fn wait_ready_terminal_phase(phase: WaitReadyTimeoutPhase) -> ReconnectTerminalPhase {
    match phase {
        WaitReadyTimeoutPhase::PodNeverReady => ReconnectTerminalPhase::PodNeverReady,
        WaitReadyTimeoutPhase::ProxyUnreachable => ReconnectTerminalPhase::ProxyUnreachable,
        WaitReadyTimeoutPhase::GuestNotReady => ReconnectTerminalPhase::GuestNotReady,
    }
}

fn wait_ready_reason(phase: WaitReadyTimeoutPhase) -> ReconnectReason {
    match phase {
        WaitReadyTimeoutPhase::PodNeverReady => ReconnectReason::PodNeverReady,
        WaitReadyTimeoutPhase::ProxyUnreachable => ReconnectReason::ProxyUnreachable,
        WaitReadyTimeoutPhase::GuestNotReady => ReconnectReason::GuestNotReady,
    }
}

fn retry_after_cause_terminal_phase(cause: ControllerRetryAfterCause) -> ReconnectTerminalPhase {
    match cause {
        ControllerRetryAfterCause::RestoreInFlight => ReconnectTerminalPhase::Restore,
        ControllerRetryAfterCause::PodNeverReady => ReconnectTerminalPhase::PodNeverReady,
        ControllerRetryAfterCause::ProxyUnreachable => ReconnectTerminalPhase::ProxyUnreachable,
        ControllerRetryAfterCause::GuestNotReady => ReconnectTerminalPhase::GuestNotReady,
        // Cluster-admission backpressure is a front-door 429, not a reconnect
        // outcome; it shouldn't reach the reconnect terminal path. Map it to the
        // API phase defensively.
        ControllerRetryAfterCause::ClusterActivationAdmission
        | ControllerRetryAfterCause::PoolSlotRelease => ReconnectTerminalPhase::Api,
    }
}

fn retry_after_cause_reason(cause: ControllerRetryAfterCause) -> ReconnectReason {
    match cause {
        ControllerRetryAfterCause::RestoreInFlight => ReconnectReason::RestoreInFlight,
        ControllerRetryAfterCause::PodNeverReady => ReconnectReason::PodNeverReady,
        ControllerRetryAfterCause::ProxyUnreachable => ReconnectReason::ProxyUnreachable,
        ControllerRetryAfterCause::GuestNotReady => ReconnectReason::GuestNotReady,
        // See note above: not a reconnect cause. Treat as a retryable wait.
        ControllerRetryAfterCause::ClusterActivationAdmission
        | ControllerRetryAfterCause::PoolSlotRelease => ReconnectReason::WaitBudget,
    }
}

fn retry_after_cause_outcome(cause: ControllerRetryAfterCause) -> ReconnectOutcome {
    match cause {
        ControllerRetryAfterCause::RestoreInFlight => ReconnectOutcome::WakeInProgress,
        ControllerRetryAfterCause::PodNeverReady
        | ControllerRetryAfterCause::ProxyUnreachable
        | ControllerRetryAfterCause::GuestNotReady
        // See note above: not a reconnect cause. Treat as retryable (exhausted).
        | ControllerRetryAfterCause::ClusterActivationAdmission
        | ControllerRetryAfterCause::PoolSlotRelease => ReconnectOutcome::Exhausted,
    }
}

fn reconnect_timeout_phase_and_reason(
    initial_lifecycle: &str,
    err: &SubstrateError,
) -> (ReconnectTerminalPhase, ReconnectReason) {
    if let Some(phase) = err.wait_ready_timeout_phase() {
        return (wait_ready_terminal_phase(phase), wait_ready_reason(phase));
    }
    (
        initial_lifecycle_terminal_phase(initial_lifecycle),
        ReconnectReason::WaitBudget,
    )
}

fn reconnect_error_outcome(
    initial_lifecycle: &str,
    err: &SubstrateError,
) -> (ReconnectOutcome, ReconnectTerminalPhase, ReconnectReason) {
    match err {
        SubstrateError::RestoreInFlight { .. }
        | SubstrateError::RestoreSourcePending { .. }
        | SubstrateError::SuspendInFlight { .. }
        | SubstrateError::ActivationInFlight { .. } => (
            ReconnectOutcome::WakeInProgress,
            ReconnectTerminalPhase::Restore,
            ReconnectReason::RestoreInFlight,
        ),
        SubstrateError::RestoreContextUnavailable { .. } => (
            ReconnectOutcome::WakeFailed,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::RestoreContextUnavailable,
        ),
        SubstrateError::Timeout { .. } => {
            let (phase, reason) = reconnect_timeout_phase_and_reason(initial_lifecycle, err);
            (ReconnectOutcome::Exhausted, phase, reason)
        }
        SubstrateError::Conflict { .. }
        | SubstrateError::FailedPrecondition { .. }
        | SubstrateError::RetainedFilesystemOnlyCopy { .. }
        | SubstrateError::EvacuationClaimLost { .. } => (
            ReconnectOutcome::Rejected,
            ReconnectTerminalPhase::Api,
            ReconnectReason::Conflict,
        ),
        SubstrateError::NotFound { .. } => (
            ReconnectOutcome::Rejected,
            ReconnectTerminalPhase::Api,
            ReconnectReason::NotFound,
        ),
        // A deferred restore claim's cluster-activation charge was rejected
        // (429/backpressure) — the same control-plane-saturation signature as a
        // direct-wake admission 429. Classify it identically (exhausted/
        // wait_budget) so restore-path and direct-wake admission rejections land
        // on one outcome/reason series instead of being split across
        // `rejected/unavailable` vs `exhausted/wait_budget`.
        SubstrateError::AdmissionRejected { .. } => (
            ReconnectOutcome::Exhausted,
            ReconnectTerminalPhase::Api,
            ReconnectReason::WaitBudget,
        ),
        SubstrateError::Unavailable { .. } => (
            ReconnectOutcome::WakeFailed,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::Unavailable,
        ),
        // Not `wake_failed`: a lost lock race wrote nothing and the identical
        // request succeeds on retry, so counting it as a wake fault would put
        // the database's contention into the series that says the sandbox path
        // is broken. `exhausted` for the same reason `AdmissionRejected` uses
        // it — control-plane saturation shed this attempt — but with its own
        // reason, because "the bound is firing" is the signal the per-
        // environment `lock_timeout` rollout watches, and folding it into
        // `wait_budget` would hide it behind restore-budget exhaustion.
        SubstrateError::LockContention { .. } => (
            ReconnectOutcome::Exhausted,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::LockContention,
        ),
        SubstrateError::Failed { .. } => (
            ReconnectOutcome::WakeFailed,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::Failed,
        ),
        SubstrateError::Cancelled { .. } => (
            ReconnectOutcome::WakeFailed,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::Cancelled,
        ),
        SubstrateError::Unsupported { .. } | SubstrateError::NodeTombstoned { .. } => (
            ReconnectOutcome::Rejected,
            ReconnectTerminalPhase::Api,
            ReconnectReason::Unsupported,
        ),
    }
}

fn reconnect_controller_error_outcome(
    initial_lifecycle: &str,
    err: &ControllerError,
) -> (ReconnectOutcome, ReconnectTerminalPhase, ReconnectReason) {
    if err.is_restore_context_unavailable() {
        return (
            ReconnectOutcome::WakeFailed,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::RestoreContextUnavailable,
        );
    }
    // Mirroring `reconnect_error_outcome`'s `LockContention` arm.
    // A contention error is a 503 carrying neither a cause nor a `retry_after`,
    // so without this it falls to the `SERVICE_UNAVAILABLE` arm below and is
    // labelled `wake_failed` / `unavailable` — the wake-fault series a lost
    // race must never enter, and with no `lock_contention` reason for the
    // `lock_timeout` rollout to watch.
    //
    // This is load-bearing today, not pre-emptive. The call site below at
    // `access_sandbox_ip`'s error arm carries a `ControllerError` straight from
    // the substrate: the Kubernetes impl converts `suspend_restore_plan` and
    // `resolve_pod_ip` with `ControllerError::from`, both are `db_context`
    // classified, and `From<SubstrateError>` turns `LockContention` into
    // `ControllerError::lock_contention`. So every ECW data-plane access that
    // loses a row lock on `sandboxes` or `sandbox_k8s_executions` reaches here
    // as a contention error.
    if err.is_lock_contention() {
        return (
            ReconnectOutcome::Exhausted,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::LockContention,
        );
    }
    if let Some(cause) = err.retry_after_cause() {
        return (
            retry_after_cause_outcome(cause),
            retry_after_cause_terminal_phase(cause),
            retry_after_cause_reason(cause),
        );
    }
    if err.retry_after().is_some() {
        return (
            ReconnectOutcome::Exhausted,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::WaitBudget,
        );
    }

    match err.status() {
        axum::http::StatusCode::CONFLICT | axum::http::StatusCode::PRECONDITION_FAILED => (
            ReconnectOutcome::Rejected,
            ReconnectTerminalPhase::Api,
            ReconnectReason::Conflict,
        ),
        axum::http::StatusCode::NOT_FOUND => (
            ReconnectOutcome::Rejected,
            ReconnectTerminalPhase::Api,
            ReconnectReason::NotFound,
        ),
        axum::http::StatusCode::SERVICE_UNAVAILABLE => (
            ReconnectOutcome::WakeFailed,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::Unavailable,
        ),
        axum::http::StatusCode::REQUEST_TIMEOUT | axum::http::StatusCode::GATEWAY_TIMEOUT => (
            ReconnectOutcome::Exhausted,
            initial_lifecycle_terminal_phase(initial_lifecycle),
            ReconnectReason::Timeout,
        ),
        // Cluster-activation backpressure. The rate-limited case carries a
        // `ClusterActivationAdmission` cause and is classified above; the
        // empty-cluster (zero-capacity) case is a bare 429 with only a
        // Retry-After header and no cause, so it lands here. Both are admission
        // exhaustion, not a substrate failure — classify them the same so a
        // capacity outage isn't misattributed on the retry-storm dashboard.
        axum::http::StatusCode::TOO_MANY_REQUESTS => (
            ReconnectOutcome::Exhausted,
            ReconnectTerminalPhase::Api,
            ReconnectReason::WaitBudget,
        ),
        _ => (
            ReconnectOutcome::WakeFailed,
            ReconnectTerminalPhase::Api,
            ReconnectReason::Substrate,
        ),
    }
}

#[derive(Clone, Copy)]
struct ReconnectAttemptTags {
    entrypoint: ReconnectEntrypoint,
    outcome: ReconnectOutcome,
    initial_lifecycle: &'static str,
    terminal_phase: ReconnectTerminalPhase,
    reason: ReconnectReason,
}

fn reconnect_attempt_tags(
    identity: SandboxApiMetricIdentity,
    initial_lifecycle: &str,
    outcome: ReconnectOutcome,
    terminal_phase: ReconnectTerminalPhase,
    reason: ReconnectReason,
) -> Option<ReconnectAttemptTags> {
    Some(ReconnectAttemptTags {
        entrypoint: reconnect_entrypoint(identity)?,
        outcome,
        initial_lifecycle: reconnect_initial_lifecycle_tag(initial_lifecycle),
        terminal_phase,
        reason,
    })
}

pub(crate) fn record_reconnect_attempt(
    identity: SandboxApiMetricIdentity,
    initial_lifecycle: &str,
    outcome: ReconnectOutcome,
    terminal_phase: ReconnectTerminalPhase,
    reason: ReconnectReason,
    started_at: std::time::Instant,
) {
    let Some(tags) =
        reconnect_attempt_tags(identity, initial_lifecycle, outcome, terminal_phase, reason)
    else {
        return;
    };
    pplx_space_metrics::sandbox::api::reconnect_attempt(ReconnectAttempt {
        entrypoint: tags.entrypoint,
        outcome: tags.outcome,
        initial_lifecycle: tags.initial_lifecycle,
        terminal_phase: tags.terminal_phase,
        reason: tags.reason,
        // The identity flows unchanged from the connect entrypoint, so the
        // coarse client class stamped there rides through every downstream
        // reconnect emission (route rejections and the controller wake path).
        client_class: identity.client_class,
        started_at,
    });
}

fn record_reconnect_substrate_error(
    identity: SandboxApiMetricIdentity,
    initial_lifecycle: &str,
    err: &SubstrateError,
    started_at: std::time::Instant,
) {
    let (outcome, terminal_phase, reason) = reconnect_error_outcome(initial_lifecycle, err);
    record_reconnect_attempt(
        identity,
        initial_lifecycle,
        outcome,
        terminal_phase,
        reason,
        started_at,
    );
}

fn resolve_activation_options(
    request: Option<&SandboxActivationRequest>,
    default_blocking: bool,
) -> Result<ActivationOptions, ControllerError> {
    let blocking = request
        .and_then(|request| request.blocking)
        .unwrap_or(default_blocking);
    let ready_timeout = match request.and_then(|request| request.ready_timeout_secs) {
        Some(0) => {
            return Err(ControllerError::bad_request(
                "ready_timeout_secs must be greater than 0",
            ));
        }
        // Clamp caller-supplied waits to the ALB-aware budget. Without this a
        // restore/wake/start caller that overrides `ready_timeout_secs` above
        // the budget would push the handler past the gateway ceiling and back
        // into the opaque 504 this bound exists to prevent.
        Some(timeout) => Duration::from_secs(u64::from(timeout)).min(DEFAULT_BLOCKING_WAIT_TIMEOUT),
        None => DEFAULT_BLOCKING_WAIT_TIMEOUT,
    };
    Ok(ActivationOptions {
        blocking,
        ready_timeout,
    })
}

fn normalize_sandbox_port_protocol(protocol: Option<SandboxPortProtocol>) -> &'static str {
    protocol.unwrap_or_default().as_str()
}

fn normalize_sandbox_port_visibility(visibility: Option<SandboxPortVisibility>) -> &'static str {
    visibility.unwrap_or_default().as_str()
}

fn validate_sandbox_port(port: u16) -> Result<u16, ControllerError> {
    if port == 0 {
        return Err(ControllerError::bad_request("port must be 1-65535"));
    }
    Ok(port)
}

async fn load_tcp_sandbox_port_exposure(
    pool: &PgPool,
    sandbox_id: SandboxId,
    port: u16,
) -> Result<Option<SandboxPortExposure>, ControllerError> {
    sqlx::query_as::<_, SandboxPortExposure>(sql!(
        "SELECT sandbox_id,
                protocol,
                guest_port,
                visibility,
                source,
                desired_state,
                apply_state,
                last_applied_node_id,
                last_error,
                last_bridge_status,
                last_applied_bridge_generation,
                last_observed_at,
                created_at,
                updated_at
         FROM sandbox_port_exposures
         WHERE sandbox_id = $1
           AND protocol = 'tcp'
           AND guest_port = $2"
    ))
    .bind(sandbox_id)
    .bind(port as i32)
    .fetch_optional(pool)
    .await
    .map_err(|err| db_contention_error("failed to load sandbox port exposure", &err))
}

async fn mark_sandbox_port_exposure_applied(
    pool: &PgPool,
    sandbox_id: SandboxId,
    port: u16,
    active_node_id: NodeId,
    observation: &SandboxPortExposureApplyObservation,
) -> Result<SandboxPortExposure, ControllerError> {
    sqlx::query_as::<_, SandboxPortExposure>(sql!(
        "UPDATE sandbox_port_exposures
         SET apply_state = 'applied',
             last_applied_node_id = $3,
             last_error = NULL,
             last_bridge_status = $4,
             last_applied_bridge_generation = $5,
             last_observed_at = NOW(),
             updated_at = NOW()
         WHERE sandbox_id = $1
           AND protocol = 'tcp'
           AND guest_port = $2
           AND desired_state = 'open'
         RETURNING sandbox_id,
                   protocol,
                   guest_port,
                   visibility,
                   source,
                   desired_state,
                   apply_state,
                   last_applied_node_id,
                   last_error,
                   last_bridge_status,
                   last_applied_bridge_generation,
                   last_observed_at,
                   created_at,
                   updated_at"
    ))
    .bind(sandbox_id)
    .bind(port as i32)
    .bind(active_node_id)
    .bind(observation.bridge_status)
    .bind(observation.service_generation.as_deref())
    .fetch_optional(pool)
    .await
    .map_err(|err| db_contention_error("failed to mark sandbox port exposure applied", &err))?
    .ok_or_else(|| ControllerError::conflict("sandbox port exposure is no longer open"))
}

async fn mark_sandbox_port_exposure_failed(
    pool: &PgPool,
    sandbox_id: SandboxId,
    port: u16,
    active_node_id: NodeId,
    error: &ControllerError,
    observation: Option<&SandboxPortExposureApplyObservation>,
) {
    let error_message = error.to_string();
    let bridge_status = observation.map(|observation| observation.bridge_status);
    let service_generation =
        observation.and_then(|observation| observation.service_generation.as_deref());
    if let Err(update_error) = sqlx::query(sql!(
        "UPDATE sandbox_port_exposures
         SET apply_state = 'failed',
             last_error = $4,
             last_bridge_status = COALESCE($5::TEXT, last_bridge_status),
             last_applied_bridge_generation =
                 COALESCE($6::TEXT, last_applied_bridge_generation),
             last_observed_at =
                 CASE WHEN $5::TEXT IS NOT NULL THEN NOW() ELSE last_observed_at END,
             updated_at = NOW()
         WHERE sandbox_id = $1
           AND protocol = 'tcp'
           AND guest_port = $2
           AND desired_state = 'open'
           AND NOT (
               apply_state = 'applied'
               AND last_applied_node_id IS NOT DISTINCT FROM $3
               AND last_bridge_status IN ('direct', 'loopback_forwarded')
               AND last_observed_at >= NOW() - ($7::BIGINT * INTERVAL '1 second')
           )"
    ))
    .bind(sandbox_id)
    .bind(port as i32)
    .bind(active_node_id)
    .bind(error_message)
    .bind(bridge_status)
    .bind(service_generation)
    .bind(SANDBOX_PORT_OBSERVATION_FRESHNESS_SECS)
    .execute(pool)
    .await
    {
        tracing::warn!(
            sandbox_id = %sandbox_id,
            port,
            error = %update_error,
            "failed to mark sandbox port exposure failed"
        );
    }
}

#[derive(Debug, Clone)]
struct SandboxPortExposureApplyObservation {
    bridge_status: &'static str,
    service_generation: Option<String>,
}

#[derive(Debug, Clone)]
struct SandboxPortExposureApplyError {
    error: ControllerError,
    observation: Option<SandboxPortExposureApplyObservation>,
}

impl SandboxPortExposureApplyError {
    fn new(
        error: ControllerError,
        observation: Option<SandboxPortExposureApplyObservation>,
    ) -> Self {
        Self { error, observation }
    }
}

impl From<ControllerError> for SandboxPortExposureApplyError {
    fn from(error: ControllerError) -> Self {
        Self::new(error, None)
    }
}

impl From<tonic::Status> for SandboxPortExposureApplyError {
    fn from(status: tonic::Status) -> Self {
        ControllerError::from(status).into()
    }
}

fn sandbox_port_exposure_ready_on_node(
    exposure: &SandboxPortExposure,
    node_id: Option<NodeId>,
) -> bool {
    node_id.is_some()
        && exposure.apply_state == SANDBOX_PORT_APPLY_APPLIED
        && exposure.last_applied_node_id == node_id
        && matches!(
            exposure.last_bridge_status.as_deref(),
            Some(SANDBOX_PORT_BRIDGE_DIRECT | SANDBOX_PORT_BRIDGE_LOOPBACK_FORWARDED)
        )
        && exposure
            .last_observed_at
            .is_some_and(sandbox_port_observation_is_fresh)
}

fn sandbox_port_observation_is_fresh(observed_at: time::OffsetDateTime) -> bool {
    observed_at
        >= time::OffsetDateTime::now_utc()
            - time::Duration::seconds(SANDBOX_PORT_OBSERVATION_FRESHNESS_SECS)
}

/// Whether a native create may be admitted against a warm-pool slot; see
/// [`SandboxController::plan_admission_pool_claim`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionPoolPlan {
    /// Claim a ready slot of this template release.
    Eligible { template_release_id: Uuid },
    /// The request cannot adopt a slot, or something the decision needs is
    /// unknown at admission.
    Ineligible,
    /// The template did not resolve to a release. The create's own
    /// execution reports why.
    TemplateMiss,
}

#[derive(Clone)]
pub struct SandboxController {
    falcon_us2_egress: Option<crate::falcon_egress::FalconUs2Egress>,
    tls_interception_exemptions:
        Option<crate::tls_interception_exemptions::TlsInterceptionExemptions>,
    federated_grant_audience: pplx_space_common_core::types::FederatedGrantAudience,
    substrate: Substrate,
    cluster_admission: Option<std::sync::Arc<crate::cluster_admission::ClusterSandboxAdmission>>,
    route_ed25519_authority: Option<
        std::sync::Arc<
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
        >,
    >,
}

pub type SpacedFileInfoStream = tonic::Streaming<SpacedFileInfo>;
pub type SpacedFileChunkStream = tonic::Streaming<SpacedFileChunk>;
pub type SpacedFileEventStream = tonic::Streaming<SpacedFileEvent>;
pub type SpacedProcessEventStream = tonic::Streaming<SpacedProcessEvent>;
#[derive(Debug, thiserror::Error)]
pub enum SandboxControllerInitError {
    #[error(transparent)]
    TlsInterceptionExemptions(
        #[from] crate::tls_interception_exemptions::TlsInterceptionExemptionsConfigError,
    ),
    #[error(transparent)]
    FalconEgress(#[from] crate::falcon_egress::FalconEgressConfigError),
    #[error(transparent)]
    Substrate(#[from] super::substrate::SubstrateError),
    #[error(transparent)]
    FederatedIdentity(#[from] crate::federated_identity::FederatedIdentityPolicyError),
}

impl SandboxController {
    pub fn with_tls_interception_exemptions(
        mut self,
        exemptions: Option<crate::tls_interception_exemptions::TlsInterceptionExemptions>,
    ) -> Self {
        self.tls_interception_exemptions = exemptions;
        self
    }

    pub(crate) fn with_falcon_us2_egress(
        mut self,
        egress: Option<crate::falcon_egress::FalconUs2Egress>,
    ) -> Self {
        self.falcon_us2_egress = egress;
        self
    }

    pub fn with_federated_grant_audience(
        mut self,
        audience: pplx_space_common_core::types::FederatedGrantAudience,
    ) -> Self {
        self.federated_grant_audience = audience;
        self
    }

    pub fn new(substrate: Substrate) -> Self {
        Self {
            falcon_us2_egress: None,
            tls_interception_exemptions: None,
            federated_grant_audience: crate::federated_identity::FederatedIdentityPolicy::default()
                .grant_audience,
            substrate,
            cluster_admission: None,
            route_ed25519_authority: None,
        }
    }

    pub fn with_cluster_admission(
        substrate: Substrate,
        cluster_admission: crate::cluster_admission::ClusterSandboxAdmission,
    ) -> Self {
        Self {
            falcon_us2_egress: None,
            tls_interception_exemptions: None,
            federated_grant_audience: crate::federated_identity::FederatedIdentityPolicy::default()
                .grant_audience,
            substrate,
            cluster_admission: Some(std::sync::Arc::new(cluster_admission)),
            route_ed25519_authority: None,
        }
    }

    pub fn with_route_ed25519_authority(
        mut self,
        authority: Option<
            std::sync::Arc<
                pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
            >,
        >,
    ) -> Self {
        self.route_ed25519_authority = authority;
        self
    }

    pub fn managed_by(&self) -> &'static str {
        self.substrate.managed_by()
    }

    pub fn substrate_capabilities(&self) -> super::substrate::SubstrateCapabilities {
        self.substrate.capabilities()
    }

    pub async fn set_node_schedulability(
        &self,
        request: super::substrate::NodeSchedulabilityRequest<'_>,
        desired: super::substrate::NodeSchedulability,
    ) -> Result<(), SubstrateError> {
        self.substrate
            .set_node_schedulability(request, desired)
            .await
    }

    pub async fn resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        self.substrate.resolve_node_api_endpoint(target).await
    }

    /// Used by the admin `:force-upload-local-snapshots` endpoint to
    /// trigger an on-node S3 upload for an already-suspended sandbox
    /// whose snapshot is still pinned to local disk. Returns once the
    /// on-node sbx-mgr has enqueued the upload on its durable worker.
    pub async fn enqueue_suspended_snapshot_backup(
        &self,
        sandbox_id: SandboxId,
        target: &NodeApiTarget<'_>,
    ) -> Result<(), SubstrateError> {
        self.substrate
            .enqueue_artifact_backup(
                sandbox_id,
                pplx_space_common_core::types::ArtifactKind::Full,
                pplx_space_common_core::types::BackupSource::CurrentInstance,
                target,
            )
            .await
            .map(|_| ())
    }

    pub async fn list_inventory_nodes(
        &self,
        role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        self.substrate.list_inventory_nodes(role_filter).await
    }

    pub async fn sandbox_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, ControllerError> {
        self.substrate.sandbox_ip(sandbox_id).await
    }

    pub async fn access_sandbox_ip(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
    ) -> Result<Option<String>, ControllerError> {
        let reconnect_started_at = std::time::Instant::now();
        let initial_lifecycle = sandbox.lifecycle.as_str();
        let metric_identity = SandboxApiMetricIdentity::native("access");
        lifecycle::ensure_access_path_not_blocked_by_drain(
            pool,
            sandbox,
            lifecycle::SandboxAction::Access,
        )
        .await
        .inspect_err(|_| {
            record_reconnect_attempt(
                metric_identity,
                initial_lifecycle,
                ReconnectOutcome::Rejected,
                ReconnectTerminalPhase::Api,
                ReconnectReason::DrainBlocked,
                reconnect_started_at,
            );
        })?;
        // Charge the cluster activation limiter when this access will restore a
        // suspended sandbox or boot a created one (the implicit data-plane wake
        // vector). Live/paused reconnects classify as no-op and don't spend a
        // token. Fails open on limiter errors, so an Err here is a genuine
        // activation rejection (429/backpressure) — record it on the reconnect
        // series so a control-plane-throttled data-plane retry storm isn't
        // invisible to the retry-storm monitor.
        lifecycle::charge_implicit_access_activation(
            self.cluster_admission.as_ref(),
            sandbox,
            ExecutionActivityKind::Access,
        )
        .await
        .inspect_err(|err| {
            let (outcome, terminal_phase, reason) =
                reconnect_controller_error_outcome(initial_lifecycle, err);
            record_reconnect_attempt(
                metric_identity,
                initial_lifecycle,
                outcome,
                terminal_phase,
                reason,
                reconnect_started_at,
            );
        })?;
        match self
            .substrate
            .access_sandbox_ip(
                metric_identity,
                sandbox,
                DEFAULT_BLOCKING_WAIT_TIMEOUT,
                crate::controllers::substrate::RecoveryCharge::enforce(
                    self.cluster_admission.as_ref(),
                ),
            )
            .await
        {
            Ok(result) => {
                if let Some((outcome, terminal_phase, reason)) =
                    successful_access_reconnect_outcome(initial_lifecycle)
                {
                    record_reconnect_attempt(
                        metric_identity,
                        initial_lifecycle,
                        outcome,
                        terminal_phase,
                        reason,
                        reconnect_started_at,
                    );
                }
                Ok(result)
            }
            Err(err) => {
                let (outcome, terminal_phase, reason) =
                    reconnect_controller_error_outcome(initial_lifecycle, &err);
                record_reconnect_attempt(
                    metric_identity,
                    initial_lifecycle,
                    outcome,
                    terminal_phase,
                    reason,
                    reconnect_started_at,
                );
                Err(err)
            }
        }
    }

    /// Read the sandbox's wake-convergence signal fresh from Postgres.
    ///
    /// Used by data-plane failure handlers to decide whether an upstream
    /// connect failure happened while the sandbox was plausibly mid-wake. The
    /// sandbox row a request loaded at admission can be stale by the time the
    /// upstream connect fails, so this is a deliberate re-read at failure
    /// time — a single primary-key point lookup on a path that is rare by
    /// construction (it only runs after a proxied request already failed).
    pub async fn sandbox_wake_signal(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxWakeSignal>, ControllerError> {
        fetch_sandbox_wake_signal(pool, sandbox_id)
            .await
            .map_err(|err| db_contention_error("failed to fetch sandbox wake signal", &err))
    }
}

/// Fetch a [`SandboxWakeSignal`] with a single primary-key point read.
///
/// The signal is the lifecycle plus the recency of the last lifecycle flip
/// (`lifecycle_changed_at`, maintained by the `sandboxes_lifecycle_changed_at`
/// trigger). The restore lease is deliberately not read: the
/// `sandboxes_clear_restore_lease` trigger nulls the lease inside the same
/// UPDATE that flips the row out of `restoring`, so an active lease can only
/// ever be observed together with `lifecycle = 'restoring'` — which already
/// classifies as waking on its own. The recency comparison runs in SQL
/// against the database's `NOW()` so it uses the same clock that wrote
/// `lifecycle_changed_at`.
pub(crate) async fn fetch_sandbox_wake_signal(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxWakeSignal>, sqlx::Error> {
    let row: Option<(String, bool)> = sqlx::query_as(sql!(
        "SELECT lifecycle,
                lifecycle_changed_at > NOW() - make_interval(secs => $2)
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(POST_WAKE_LISTENER_REBIND_GRACE.as_secs_f64())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(lifecycle, lifecycle_changed_recently)| {
        SandboxWakeSignal {
            // Normalize before parsing: the lifecycle column is fed from
            // substrate-reported status, so tolerate casing/whitespace
            // rather than silently treating `Restoring` as unknown.
            lifecycle: lifecycle
                .trim()
                .to_ascii_lowercase()
                .parse::<SandboxLifecycle>()
                .ok(),
            lifecycle_changed_recently,
        }
    }))
}

/// Wake-convergence signal for a sandbox, read fresh from the `sandboxes` row
/// at data-plane failure time (see [`SandboxController::sandbox_wake_signal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxWakeSignal {
    /// `None` when the stored lifecycle string didn't parse as a known state.
    pub lifecycle: Option<SandboxLifecycle>,
    /// Whether `lifecycle_changed_at` was within
    /// [`POST_WAKE_LISTENER_REBIND_GRACE`] of the database's `NOW()` at read
    /// time.
    pub lifecycle_changed_recently: bool,
}

impl SandboxWakeSignal {
    /// Whether an upstream connect failure observed *now* is plausibly the
    /// sandbox still waking rather than a genuine fault.
    ///
    /// True when the lifecycle is mid-wake — the suspend/restore cycle, or
    /// `starting`, which also covers the initial-create boot, where the guest
    /// equally hasn't bound its listeners yet and a bounded retryable shape
    /// converges — or when the row says `started` but the flip is recent: the
    /// post-wake listener-rebind window (see
    /// [`POST_WAKE_LISTENER_REBIND_GRACE`]). The recency arm also matches a
    /// sandbox that finished its *first* boot within the grace; that is the
    /// same tradeoff as `starting`, a retryable shape for a window bounded by
    /// the grace instead of a dead-end for a sandbox that is converging.
    ///
    /// Kept exhaustive (no `_` wildcard) so a future lifecycle forces a
    /// conscious decision here.
    pub fn is_plausibly_waking(self) -> bool {
        match self.lifecycle {
            Some(
                SandboxLifecycle::Suspended
                | SandboxLifecycle::Suspending
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Starting,
            ) => true,
            // The post-wake flip already landed but the guest may still be
            // re-establishing dynamic-port listeners; the restore lease was
            // trigger-cleared by the same UPDATE, so flip recency is the only
            // remaining wake evidence.
            Some(SandboxLifecycle::Started) => self.lifecycle_changed_recently,
            Some(
                SandboxLifecycle::Created
                | SandboxLifecycle::Creating
                | SandboxLifecycle::Stopping
                | SandboxLifecycle::Stopped
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Checkpointing
                | SandboxLifecycle::Quarantined
                | SandboxLifecycle::Failed,
            )
            | None => false,
        }
    }

    /// Whether the sandbox is gone for good (`failed` / `stopped`): retrying
    /// the data-plane request can never succeed. Checked *before*
    /// [`Self::is_plausibly_waking`] by the failure-shape mapper so terminal
    /// truth can never be masked by a waking signal.
    pub fn is_terminal(self) -> bool {
        matches!(
            self.lifecycle,
            Some(
                SandboxLifecycle::Failed
                    | SandboxLifecycle::Quarantined
                    | SandboxLifecycle::Stopped
            )
        )
    }
}

#[cfg(test)]
mod reconnect_metrics_tests;

mod ingress;
pub(crate) mod lifecycle;
mod org;
mod ports;
mod queries;
mod responses;
#[cfg(test)]
mod sandbox_portability_tests;
pub(crate) mod spaced;
pub(crate) use ingress::ingress_activation;
mod wake_completion;
pub use spaced::SpacedListOptions;
#[cfg(test)]
mod tests;
mod updates;

use responses::*;

pub(crate) use org::sandbox_matches_federated_owner;
pub use org::{SandboxAccessScope, SandboxPageCursor};
pub(crate) use queries::org_byok_enabled;
#[cfg(test)]
pub(crate) use queries::pooled_prepare_error_is_reclaimable;
pub use queries::{FederatedSandboxCreateContext, RestoreForkDiskAsNewContext};
pub use responses::{
    fetch_sandbox, sandbox_query, sandbox_to_response, update_and_fetch_post_wake_sandbox,
};
pub(crate) use updates::{AccessIdleTimeoutOutcome, AutoLifecycleTouchOutcome};
