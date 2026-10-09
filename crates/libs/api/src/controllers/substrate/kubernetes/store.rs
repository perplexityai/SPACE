//! Postgres access for the Kubernetes substrate.
//!
//! Every `sqlx` error here is classified through `DbContext::db_context`
//! (`super::super::substrate_db_error`): a contention SQLSTATE becomes a
//! `SubstrateError::LockContention` — a 503 the public and ECW translators
//! keep attributed to this database — and everything else stays a
//! `SubstrateError::Failed` 500. `db_context` is the only mapper any
//! statement here should need; a bare `SubstrateError::Failed` on a `sqlx`
//! result is a call site that will report a lost lock race as permanent.

use std::{collections::HashMap, time::Duration};

use super::PodIp;
use crate::controllers::substrate::ExecutionIdentity;
use pplx_space_cluster_pool::WarmPoolStore;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExactStorageUri, ExecutionGeneration, HostCpu, LocalArtifactId,
    NodeId, NodeRole, PodUid, SandboxActivationAttemptId, SandboxId, SandboxLifecycle,
    SandboxOwner, SandboxTransitionTargetState, SnapshotState, StorageUri, UserId,
};
use pplx_space_common_svc::types::{FACET_SANDBOX_DESIRED_STATE, SANDBOX_LOCAL_SUSPEND_KEY};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::sandbox::api::RouteIdentityDecisionReason;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_sandbox_portability::{
    SuspendedTargetNodeAssignment, clear_current_local_artifact_marker_assignments,
    failed_repair_revert_eligibility, fresh_ready_snapshot_exists,
    missing_suspending_pod_revert_eligibility, newest_suspend_snapshot_activity_current,
    no_newer_full_snapshot, restore_retry_source_snapshot_is_current,
    restore_retry_suspended_revert_snapshot_exists, snapshot_is_current_local_artifact,
    started_suspend_revert_eligibility, suspended_k8s_finalization_sql,
    write_current_local_artifact_marker_assignments,
};
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::{controllers::sandbox::fetch_sandbox as fetch_api_sandbox, models::Sandbox};

use crate::controllers::substrate::{
    DbContext, PublishedLocalArtifact, SubstrateError, reject_deleting_current_local_artifact,
};

#[derive(Clone)]
pub(super) struct K8sStore {
    pool: PgPool,
    namespace: String,
}

/// Where a sandbox's pod landed. Groups the identity columns
/// [`K8sStore::persist_pod_node_placement_after_pod_exists`] writes so adding
/// the retry disposition keeps its signature under clippy's
/// `too_many_arguments` ceiling, as `PodProxyWaitTarget` does for the
/// proxy-reachability wait.
pub(super) struct PodNodePlacement<'a> {
    pub sandbox_id: SandboxId,
    pub node_name: &'a str,
    pub node_uid: &'a str,
    pub node_id: NodeId,
    pub role: NodeRole,
    pub address: Option<&'a str>,
}

/// What a caller's retry of the *whole operation* would do, for statements that
/// run after its pod already exists.
///
/// Only the calling operation knows this, which is why it is a parameter rather
/// than a property of the statement: the same `UPDATE` is safe to report
/// retryable on a wake and unsafe on a create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PlacementRetryDisposition {
    /// The operation mints a fresh `SandboxId` on retry and carries no
    /// idempotency key (`create`), so a retryable answer once the pod is live
    /// buys a second sandbox under an id the caller never learns.
    RetryDuplicatesExecution,
    /// The operation names an existing sandbox (`start`, `wake`, `access`), so
    /// a retry re-drives that same row and converges. Contention here stays a
    /// retryable 503.
    RetryConverges,
}

struct CommittedSuspendArtifactRow {
    execution_state: ActiveExecutionState,
    artifact_id: Option<LocalArtifactId>,
    artifact_kind: Option<ArtifactKind>,
    artifact_origin: Option<ArtifactOrigin>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SuspendExecutionClaim {
    Claimed,
    Existing,
    Completed(PublishedLocalArtifact),
}

impl SuspendExecutionClaim {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Existing => "existing",
            Self::Completed(_) => "completed",
        }
    }
}

impl std::fmt::Display for SuspendExecutionClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why generation-scoped pod-tracking cleanup is running. Only a
/// missing/terminal-pod *observation* defers to suspend finalization:
/// explicit teardown and activation-attempt cleanup delete the pod
/// themselves and have no suspend finalizer behind them, so they must
/// always retire tracking.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PodTrackingCleanupCause {
    ExplicitTeardown,
    MissingPodObservation,
}

/// Whether a pod-tracking teardown bumps `sandboxes.placement_changed_at`.
///
/// Delete-shaped teardowns (`delete_execution`, failed/cancelled create
/// cleanup) [`Bump`](Self::Bump) it — placement genuinely ended and there is
/// nothing to keep recoverable. The recoverability-preserving retire path
/// (`retire_execution`, used by the failure reconcilers) [`Preserve`](Self::Preserve)s
/// it: a row with no `current_local_artifact_id` falls into the legacy branch
/// of `fresh_ready_snapshot_exists`, which gates a `state='ready'` snapshot on
/// `sn.timestamp >= placement_changed_at`. Bumping the column to `NOW()` while
/// retiring would push it past the snapshot's (older) creation timestamp and
/// flip an already-selectable ready snapshot to "stale" — making a recoverable
/// sandbox unrecoverable even though the snapshot rows are untouched. Mirrors
/// the same preservation in `suspended_execution_reaper::retire_stale_execution`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PlacementClock {
    Bump,
    Preserve,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartedSuspendRecoveryProof {
    ExecutionRetired,
    FailedRecovery { generation: ExecutionGeneration },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartedSuspendRecoveryOperation {
    PodTrackingCleanup,
    TerminalPodCleanup,
    StalePodReplacementCleanup,
    StartedRecoveryFailure,
}

impl StartedSuspendRecoveryOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PodTrackingCleanup => "pod_tracking_cleanup",
            Self::TerminalPodCleanup => "terminal_pod_cleanup",
            Self::StalePodReplacementCleanup => "stale_pod_replacement_cleanup",
            Self::StartedRecoveryFailure => "started_recovery_failure",
        }
    }
}

impl std::fmt::Display for StartedSuspendRecoveryOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Lifecycle state a readiness observer asserts for a Kubernetes node.
///
/// The readiness-derived writers only ever assert these two states:
/// `kube_node_sync` from Kubernetes Node readiness and the admin inventory
/// listing from node-api pod readiness. The other `nodes.state` values
/// (`draining`, `terminating`, …) are owned by the drain reconciler and
/// demotion through their own writes, never through the role upsert — so this
/// closed set keeps the upsert path from re-encoding the string vocabulary at
/// every call site and makes invalid states unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeLifecycleState {
    Ready,
    Registered,
}

impl NodeLifecycleState {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            NodeLifecycleState::Ready => "ready",
            NodeLifecycleState::Registered => "registered",
        }
    }

    /// Map a readiness signal to the lifecycle state it asserts.
    pub(super) fn from_ready(ready: bool) -> Self {
        if ready {
            NodeLifecycleState::Ready
        } else {
            NodeLifecycleState::Registered
        }
    }
}

impl std::fmt::Display for NodeLifecycleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("unknown node lifecycle state: {0:?}")]
pub(super) struct ParseNodeLifecycleStateError(String);

impl std::str::FromStr for NodeLifecycleState {
    type Err = ParseNodeLifecycleStateError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ready" => Ok(NodeLifecycleState::Ready),
            "registered" => Ok(NodeLifecycleState::Registered),
            other => Err(ParseNodeLifecycleStateError(other.to_string())),
        }
    }
}

/// Which caller owns the node lifecycle `state` column on a role upsert.
///
/// The lifecycle owner (`kube_node_sync`, which derives state from Kubernetes
/// Node readiness) uses `Manage`: it writes `state` on both insert and
/// conflict-update. Every other writer — pod placement, and the admin
/// inventory listing (which derives state from node-api *pod* readiness) —
/// uses `InsertOnly`: it supplies the initial state only when inserting a
/// brand-new row and must never overwrite an existing node's state, or it
/// races the sync loop and oscillates live `ready` nodes back to `registered`.
#[derive(Debug, Clone, Copy)]
pub(super) enum NodeStateWrite {
    /// Write `state` on both insert and conflict-update (lifecycle owner).
    Manage(NodeLifecycleState),
    /// Write `state` only when inserting a new row; preserve it on conflict.
    InsertOnly(NodeLifecycleState),
}

struct K8sNodeRoleUpsert<'a> {
    node_name: &'a str,
    node_uid: &'a str,
    node_id: NodeId,
    role: NodeRole,
    address: Option<&'a str>,
    state: NodeStateWrite,
}

#[derive(sqlx::FromRow)]
pub(super) struct ActiveExecutionStatusRow {
    pub(super) generation: i64,
    pub(super) pod_name: String,
    pub(super) pod_uid: Option<PodUid>,
    pub(super) pod_ip: Option<String>,
    pub(super) k8s_node_name: Option<String>,
    pub(super) current_node_id: Option<NodeId>,
    #[sqlx(try_from = "String")]
    pub(super) state: ActiveExecutionState,
    pub(super) lifecycle: String,
    pub(super) create_age_expired: bool,
    pub(super) activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(super) route_activation_id: Option<SandboxActivationAttemptId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActivationFenceOwnership {
    Owned(SandboxActivationAttemptId),
    Joined(SandboxActivationAttemptId),
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActivationRoutePublication {
    PreserveExisting,
    InitializeFirst,
    AdoptLegacyResident,
}

pub(super) struct ReconciledActivationFence {
    pub(super) execution: ActiveExecutionStatusRow,
    ownership: ActivationFenceOwnership,
}

impl ReconciledActivationFence {
    pub(super) const fn ownership(&self) -> ActivationFenceOwnership {
        self.ownership
    }

    pub(super) const fn owned_attempt_id(&self) -> Option<SandboxActivationAttemptId> {
        match self.ownership {
            ActivationFenceOwnership::Owned(id) => Some(id),
            ActivationFenceOwnership::Joined(_) | ActivationFenceOwnership::Completed => None,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub(super) struct SandboxExecutionRow {
    pub(super) generation: i64,
    pub(super) pod_name: String,
    pub(super) pod_materialized: bool,
    #[sqlx(try_from = "String")]
    pub(super) state: ActiveExecutionState,
    pub(super) create_age_expired: bool,
    pub(super) activation_attempt_id:
        Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    pub(super) route_activation_id: Option<SandboxActivationAttemptId>,
}

impl SandboxExecutionRow {
    pub(super) fn bootstrap_route_activation_id(&self) -> Option<SandboxActivationAttemptId> {
        // Published route wins; the operation is a fallback only for legacy rows.
        self.route_activation_id.or(self.activation_attempt_id)
    }
}

pub(super) fn classify_activation_route_publication(
    row: &SandboxExecutionRow,
    created: bool,
) -> Result<ActivationRoutePublication, SubstrateError> {
    if row.route_activation_id.is_some() {
        return Ok(ActivationRoutePublication::PreserveExisting);
    }
    if !row.pod_materialized
        && (created
            || matches!(
                row.state,
                ActiveExecutionState::Creating | ActiveExecutionState::Recovering
            ))
    {
        return Ok(ActivationRoutePublication::InitializeFirst);
    }
    if row.pod_materialized {
        return Ok(ActivationRoutePublication::AdoptLegacyResident);
    }
    // Neither proof of a resident nor authority to materialize a new route.
    // The Started access-recovery owner must first claim Recovering; do not
    // send a pod-less row to the resident adoption UPDATE's pod-UID guard.
    // Centralize this counter here so both allocation entry points emit once.
    pplx_space_metrics::sandbox::api::route_identity_decision(
        RouteIdentityDecisionReason::RotationRequiredUnclassified,
    );
    Err(SubstrateError::Unavailable {
        message: format!(
            "route-less unmaterialized execution in {} requires a lifecycle recovery claim before route initialization",
            row.state,
        ),
    })
}

fn normalize_pod_uid(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[derive(Debug)]
pub(super) struct SandboxExecutionClaim {
    pub(super) row: SandboxExecutionRow,
    pub(super) created: bool,
    route_publication: Option<ActivationRoutePublication>,
}

fn record_route_identity_decision(claim: &SandboxExecutionClaim, replaced: bool) {
    let reason = if replaced && claim.route_publication.is_some() {
        Some(RouteIdentityDecisionReason::ExecutionReplaced)
    } else {
        match claim.route_publication {
            Some(ActivationRoutePublication::InitializeFirst) => {
                Some(RouteIdentityDecisionReason::FirstIdentityInitialized)
            }
            Some(ActivationRoutePublication::AdoptLegacyResident) => {
                Some(RouteIdentityDecisionReason::ExistingExecutionTransition)
            }
            Some(ActivationRoutePublication::PreserveExisting) | None => None,
        }
    };
    if let Some(reason) = reason {
        pplx_space_metrics::sandbox::api::route_identity_decision(reason);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StartedRecoveryRestoreSource {
    Local { source_node_name: String },
    Remote { storage_uri: ExactStorageUri },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StartedRecoveryRestoreContext {
    Ready(StartedRecoveryRestoreSource),
    Pending,
    Unavailable,
}

#[derive(Debug)]
pub(super) enum StartedRecoveryReplacement {
    Replaced {
        claim: Box<SandboxExecutionClaim>,
        source: StartedRecoveryRestoreSource,
    },
    Pending,
    Unavailable,
    OwnershipLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartedRecoveryClaim {
    CreatedExecution {
        generation: i64,
    },
    ExistingExecution {
        generation: i64,
    },
    InFlight {
        generation: i64,
        lease_remaining: Duration,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartedRecoveryCompletion {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActiveExecutionState {
    Creating,
    Created,
    Recovering,
    RecoveryFailed,
    Running,
    Suspended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FailedRecoveryPodAbsenceOutcome {
    NotMatched,
    ClaimPreserved,
    ConvergedSuspended,
}

impl ActiveExecutionState {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Created => "created",
            Self::Recovering => "recovering",
            Self::RecoveryFailed => "recovery_failed",
            Self::Running => "running",
            Self::Suspended => "suspended",
        }
    }
}

impl std::fmt::Display for ActiveExecutionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("unknown active execution state: {0}")]
pub(super) struct ParseActiveExecutionStateError(String);

impl std::str::FromStr for ActiveExecutionState {
    type Err = ParseActiveExecutionStateError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "creating" => Ok(Self::Creating),
            "created" => Ok(Self::Created),
            "recovering" => Ok(Self::Recovering),
            "recovery_failed" => Ok(Self::RecoveryFailed),
            "running" => Ok(Self::Running),
            "suspended" => Ok(Self::Suspended),
            other => Err(ParseActiveExecutionStateError(other.to_string())),
        }
    }
}

impl<'row> sqlx::FromRow<'row, PgRow> for CommittedSuspendArtifactRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx::Error> {
        let execution_state = row
            .try_get::<String, _>("execution_state")?
            .try_into()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let artifact_id = row
            .try_get::<Option<Uuid>, _>("artifact_id")?
            .map(LocalArtifactId::from_uuid);
        let artifact_kind = row
            .try_get::<Option<String>, _>("artifact_kind")?
            .map(ArtifactKind::try_from)
            .transpose()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let artifact_origin = row
            .try_get::<Option<String>, _>("artifact_origin")?
            .map(ArtifactOrigin::try_from)
            .transpose()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        Ok(Self {
            execution_state,
            artifact_id,
            artifact_kind,
            artifact_origin,
        })
    }
}

impl TryFrom<String> for ActiveExecutionState {
    type Error = ParseActiveExecutionStateError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl StartedRecoveryCompletion {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "running",
            Self::Failed => "recovery_failed",
        }
    }
}

impl std::fmt::Display for StartedRecoveryCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StartedRecoveryObservation {
    Pending,
    Succeeded {
        generation: i64,
        pod_name: String,
        pod_ip: String,
    },
    Failed,
}

/// What a caller that lost the user restore-lease CAS observes while it
/// waits for the lease owner's outcome, instead of surfacing every collision
/// to clients as an immediate error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum UserRestoreObservation {
    /// The owner finished: the row is `started` and the active execution is
    /// routable. Followers can bind to it directly.
    Routable {
        generation: i64,
        pod_name: String,
        pod_ip: String,
    },
    /// A restore owner still holds the lease (or is mid-completion); keep
    /// polling.
    InFlight { lease_remaining: Duration },
    /// No live lease guards the row; the caller may retry its own claim and
    /// let the claim CAS adjudicate the current lifecycle.
    Claimable,
    /// The row reached `started` without a routable execution and no lease
    /// is live — the started-without-restore-context shape. Neither joining
    /// nor claiming can converge it; callers must not drive a fresh pod.
    StartedUnroutable,
}

pub(super) struct PreparedWakeSandbox {
    pub(super) sandbox: Sandbox,
    pub(super) restore_started: bool,
    pub(super) restore_claim: Option<RestoreClaim>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RestoreClaim {
    FreshFromSuspended { restore_lease_token: Option<Uuid> },
    RedroveRestoring { restore_lease_token: Option<Uuid> },
}

impl RestoreClaim {
    pub(super) fn restore_lease_token(self) -> Option<Uuid> {
        match self {
            Self::FreshFromSuspended {
                restore_lease_token,
            }
            | Self::RedroveRestoring {
                restore_lease_token,
            } => restore_lease_token,
        }
    }
}

#[derive(sqlx::FromRow)]
struct RestoreClaimRow {
    lifecycle: String,
    restore_lease_token: Option<Uuid>,
}

fn recovery_lease_ttl_secs() -> i64 {
    i64::try_from(crate::controllers::substrate::NODE_RPC_TIMEOUT_SECS)
        .expect("node RPC timeout seconds fit i64")
}

fn emit_started_suspend_recovery_event(
    sandbox_id: SandboxId,
    operation: StartedSuspendRecoveryOperation,
) {
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::LifecycleRevertedToSuspended,
        SandboxEventSource::Substrate,
    )
    .emit_with(
        SandboxEventDetails::default()
            .operation(operation.as_str())
            .reason("recoverable_suspend_snapshot")
            .trigger(SandboxEventTrigger::System)
            .with_from_state("started")
            .with_to_state("suspended")
            .target_state("suspended"),
    );
}

/// What a full snapshot row says about the host that captured its vCPU state.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SnapshotCaptureHost {
    /// Recorded at capture; absent on snapshots that predate the field.
    pub(super) recorded: Option<HostCpu>,
    /// Kubernetes name of the node that uploaded the snapshot, if that node
    /// is still mapped. Its live label stands in for an unrecorded CPU.
    pub(super) source_node_name: Option<String>,
    /// The snapshot's timestamp, in Unix seconds. A Node object created after
    /// it can't be the machine that captured it, even under the same name.
    pub(super) snapshot_epoch_seconds: f64,
}

impl SnapshotCaptureHost {
    /// For this raw read, a recorded value that isn't a valid identity reads
    /// as unknown, with a warning, rather than failing the restore: the writer
    /// validates it, so only an out-of-band edit gets here, and unknown is the
    /// state every snapshot from before the field already restores in.
    fn new(
        recorded: Option<String>,
        source_node_name: Option<String>,
        snapshot_epoch_seconds: f64,
    ) -> Self {
        let recorded = recorded.and_then(|value| match HostCpu::parse(value.as_str()) {
            Ok(host_cpu) => Some(host_cpu),
            Err(error) => {
                tracing::warn!(
                    recorded = %value,
                    %error,
                    "ignoring an invalid recorded snapshot capture host CPU"
                );
                None
            }
        });
        Self {
            recorded,
            source_node_name,
            snapshot_epoch_seconds,
        }
    }
}

impl K8sStore {
    pub(super) fn new(pool: PgPool, namespace: String) -> Self {
        Self { pool, namespace }
    }

    pub(super) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(super) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(super) async fn template_network_policy(
        &self,
        template: &str,
    ) -> Result<Option<NetworkPolicyV1>, SubstrateError> {
        let policy = sqlx::query_scalar::<_, Option<sqlx::types::Json<NetworkPolicyV1>>>(sql!(
            "SELECT network_policy_json
             FROM templates
             WHERE name = $1"
        ))
        .bind(template)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to load template network policy")?;
        Ok(policy.flatten().map(|policy| policy.0))
    }

    pub(super) fn sandbox_owner_for_user(&self, user_id: UserId) -> SandboxOwner {
        SandboxOwner::platform_user(user_id)
    }

    fn execution_pod_name(sandbox_id: SandboxId, generation: i64) -> String {
        format!(
            "space-sbx-{}-g{}",
            sandbox_id.as_uuid().simple(),
            generation
        )
    }

    /// Record generation 1 of a freshly inserted sandbox as already retired,
    /// so its first execution is allocated generation 2.
    ///
    /// For a cold create under an ID a warm-pool slot reserved: the slot's
    /// pod was built as this ID's generation 1 (`space-sbx-<id>-g1`) and may
    /// still exist while the pool collects it. Generation allocation is
    /// `MAX(generation) + 1` everywhere (create, wake, restore, recovery), so
    /// one retired row moves this execution and every later one past that
    /// pod's name, route identity, and readiness identity. The row is
    /// inactive, so nothing routes to it. A no-op when the sandbox already
    /// has execution rows.
    pub(super) async fn retire_pool_reserved_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        let pod_name = Self::execution_pod_name(sandbox_id, 1);
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, state, active,
                 terminated_at
             )
             SELECT $1, 1, $2, $3, 'replaced', FALSE, NOW()
             WHERE NOT EXISTS (
                 SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1
             )
             ON CONFLICT DO NOTHING"
        ))
        .bind(sandbox_id)
        .bind(&pod_name)
        .bind(&self.namespace)
        .execute(&self.pool)
        .await
        .db_context("failed to retire the pool-reserved execution generation")?;
        Ok(())
    }

    async fn active_execution_row(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxExecutionRow>, SubstrateError> {
        sqlx::query_as::<_, SandboxExecutionRow>(sql!(
            "SELECT generation, pod_name,
                    NULLIF(BTRIM(pod_uid), '') IS NOT NULL AS pod_materialized,
                    state,
                    updated_at <= NOW() - make_interval(secs => $2) AS create_age_expired,
                    activation_attempt_id, route_activation_id
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .bind(super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to query active Kubernetes execution row")
    }

    async fn insert_active_execution_claim_in(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let generation = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COALESCE(MAX(generation), 0) + 1
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1"
        ))
        .bind(sandbox_uuid)
        .fetch_one(&mut *conn)
        .await
        .db_context("failed to allocate Kubernetes execution generation")?;
        let pod_name = Self::execution_pod_name(sandbox_uuid, generation);

        let row = sqlx::query_as::<_, SandboxExecutionRow>(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, state, active
             )
             VALUES ($1, $2, $3, $4, 'creating', TRUE)
             ON CONFLICT DO NOTHING
             RETURNING generation, pod_name, FALSE AS pod_materialized, state,
                       FALSE AS create_age_expired, activation_attempt_id,
                       route_activation_id"
        ))
        .bind(sandbox_uuid)
        .bind(generation)
        .bind(&pod_name)
        .bind(&self.namespace)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to insert Kubernetes execution row")?;

        let created = row.is_some();
        let row = match row {
            Some(row) => row,
            None => self
                .active_execution_row(conn, sandbox_uuid)
                .await?
                .ok_or_else(|| SubstrateError::Failed {
                    message: "failed to create active Kubernetes execution row".to_string(),
                })?,
        };

        Ok(SandboxExecutionClaim {
            row,
            created,
            route_publication: None,
        })
    }

    /// Apply the pod metadata update to the (caller-locked) active execution
    /// row. Callers must have taken the row lock first
    /// (`lock_active_execution_state_in`) — the guarded UPDATE alone cannot
    /// distinguish "row already current" from "row retired underneath".
    async fn update_active_execution_pod_metadata(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<(), SubstrateError> {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = $1::TEXT,
                 pod_ip = $2::INET,
                 state = CASE
                     WHEN state = 'recovering' THEN state
                     WHEN $2::TEXT IS NOT NULL THEN 'running'
                     WHEN $1::TEXT IS NOT NULL THEN 'created'
                     ELSE state
                 END,
                 updated_at = CASE WHEN state = 'recovering' THEN updated_at ELSE NOW() END
             WHERE sandbox_id = $3 AND generation = $4 AND active
               AND (
                 pod_uid IS DISTINCT FROM $1::TEXT
                 OR pod_ip IS DISTINCT FROM $2::INET
                 OR (
                     state NOT IN ('running', 'recovering')
                     AND (
                         $2::TEXT IS NOT NULL
                         OR ($1::TEXT IS NOT NULL AND state <> 'created')
                     )
                 )
               )"
        ))
        .bind(pod_uid)
        .bind(pod_ip)
        .bind(sandbox_id)
        .bind(generation)
        .execute(conn)
        .await
        .db_context("failed to persist pod metadata")?;
        Ok(())
    }

    /// Lock the active execution row of `generation` and return its current
    /// state, or `None` when no such row exists. `FOR UPDATE` follows the
    /// update chain to the latest committed row version and re-evaluates the
    /// predicate, so a retirement that commits while we wait on the lock
    /// yields `None` here — unlike a plain read, whose statement-start
    /// snapshot could still show the pre-retirement active version.
    async fn lock_active_execution_state_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let state: Option<String> = sqlx::query_scalar(sql!(
            "SELECT state
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = $2 AND active
             FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to lock active Kubernetes execution row")?;
        state
            .map(|state| {
                state
                    .parse::<ActiveExecutionState>()
                    .map_err(|err| SubstrateError::Failed {
                        message: format!("failed to parse active execution state: {err}"),
                    })
            })
            .transpose()
    }

    async fn update_active_execution_node_name(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        node_name: &str,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar::<_, bool>(sql!(
            "WITH updated AS (
                UPDATE sandbox_k8s_executions
                SET k8s_node_name = $1::TEXT,
                    updated_at = CASE WHEN state = 'recovering' THEN updated_at ELSE NOW() END
                WHERE sandbox_id = $2 AND active
                  AND k8s_node_name IS DISTINCT FROM $1::TEXT
                RETURNING 1
             )
             SELECT EXISTS (
                SELECT 1
                FROM sandbox_k8s_executions
                WHERE sandbox_id = $2 AND active
             )"
        ))
        .bind(node_name)
        .bind(sandbox_id)
        .fetch_one(conn)
        .await
        .db_context("failed to persist sandbox pod node name")
    }

    async fn finish_active_execution_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        state: &str,
    ) -> Result<(), SubstrateError> {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2 AND active"
        ))
        .bind(state)
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await
        .db_context("failed to finish active Kubernetes execution")?;
        self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
            .await?;
        Ok(())
    }

    /// An [`ExecutionIdentity`] as SQL binds — the fields the table has a
    /// column for.
    ///
    /// Both guards on this path — the tracking release and the UID backfill —
    /// take their predicate from here rather than each choosing columns, which
    /// is what stops them drifting apart from each other and from
    /// [`ExecutionIdentity::reconcile`]. `resource_version` has no column, so
    /// it is the one field the database cannot enforce; the live-pod half of
    /// the check covers that.
    fn identity_binds(
        identity: &ExecutionIdentity,
    ) -> (
        Option<i64>,
        Option<SandboxActivationAttemptId>,
        Option<String>,
        Option<SandboxActivationAttemptId>,
    ) {
        (
            identity.generation,
            identity.activation_attempt_id,
            identity
                .recorded
                .pod_uid()
                .map(|uid| uid.as_str().to_string()),
            identity.recorded.route_activation_id(),
        )
    }

    /// [`Self::finish_active_execution_generation_in`] with the fenced
    /// execution's full identity in the predicate.
    ///
    /// A generation is not by itself a unique execution identity: activation
    /// retries and cross-node redrives reuse one, which is why the attempt-
    /// scoped variant exists at all. So a replacement that took the generation
    /// over after the fenced delete would satisfy a generation-only CAS and be
    /// marked inactive by a teardown that never saw it. Both guards use
    /// `IS NOT DISTINCT FROM` because NULL is a value the caller observed, not
    /// an absence of one — a completed execution's attempt is NULL, and an
    /// unmaterialized pod's UID is too, so treating either as "unconstrained"
    /// would fail open on exactly the states most likely to be replaced.
    async fn finish_fenced_execution_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let (generation, activation_attempt_id, pod_uid, route_activation_id) =
            Self::identity_binds(identity);
        let finished = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2
               AND active
               AND generation IS NOT DISTINCT FROM $3
               AND activation_attempt_id IS NOT DISTINCT FROM $4
               AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $5::TEXT
               AND route_activation_id IS NOT DISTINCT FROM $6"
        ))
        .bind(state)
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .bind(pod_uid)
        .bind(route_activation_id)
        .execute(&mut **tx)
        .await
        .db_context("failed to finish fenced Kubernetes execution")?
        .rows_affected()
            > 0;
        if finished {
            self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
                .await?;
        }
        Ok(finished)
    }

    async fn finish_active_execution_generation_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        generation: i64,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let finished = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2 AND active AND generation = $3"
        ))
        .bind(state)
        .bind(sandbox_id)
        .bind(generation)
        .execute(&mut **tx)
        .await
        .db_context("failed to finish active Kubernetes execution")?
        .rows_affected()
            > 0;
        if finished {
            self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
                .await?;
        }
        Ok(finished)
    }

    async fn finish_active_execution_generation_attempt_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let finished = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2
               AND active
               AND generation = $3
               AND activation_attempt_id = $4"
        ))
        .bind(state)
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&mut **tx)
        .await
        .db_context("failed to finish activation-fenced Kubernetes execution")?
        .rows_affected()
            > 0;
        if finished {
            self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
                .await?;
        }
        Ok(finished)
    }

    async fn invalidate_open_port_exposure_observations_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        sqlx::query(sql!(
            "UPDATE sandbox_port_exposures
             SET apply_state = 'pending',
                 last_applied_node_id = NULL,
                 last_error = NULL,
                 last_bridge_status = NULL,
                 last_applied_bridge_generation = NULL,
                 last_observed_at = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND desired_state = 'open'"
        ))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await
        .db_context("failed to invalidate sandbox port exposure observations")?;
        Ok(())
    }

    pub(super) async fn require_sandbox(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Sandbox, SubstrateError> {
        fetch_api_sandbox(&self.pool, sandbox_id)
            .await
            .db_context("failed to load sandbox row")?
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            })
    }

    /// Read only after publishing the execution claim under the sandbox row
    /// lock. A timeout accepted before that claim may have updated the policy
    /// since the activation caller loaded its original sandbox snapshot.
    pub(super) async fn auto_lifecycle_policy_after_claim(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<pplx_space_common_core::types::AutoLifecyclePolicy, SubstrateError> {
        pplx_space_sandbox_store::auto_lifecycle_policy(&self.pool, sandbox_id)
            .await
            .map_err(sqlx::Error::from)
            .db_context("failed to reload claimed sandbox auto lifecycle policy")?
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            })
    }

    pub(super) async fn prior_state_exists(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                  WHERE s.id = $1
                    AND (
                      s.current_local_artifact_id IS NOT NULL
                      OR EXISTS (
                          SELECT 1
                            FROM snapshots sn
                           WHERE sn.sandbox_id = s.id
                             AND sn.type = 'full'
                             AND sn.state IN ('creating', 'ready')
                      )
                    )
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .db_context("failed to query sandbox prior state")
    }

    /// Tracked pod names for generations strictly older than `generation`.
    ///
    /// The unfenced teardown deletes *every* recorded pod name, which is how
    /// stale generations get collected — nothing else reliably does, since a
    /// retired sandbox leaves the failed-cleanup scan. A fenced teardown can't
    /// use that list wholesale: on a race it includes a successor's pod, which
    /// the caller never observed. Bounding it below the fenced generation
    /// keeps the collection and excludes the successor by construction.
    pub(super) async fn tracked_execution_pod_names_before_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Vec<String>, SubstrateError> {
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT pod_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation < $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to query prior-generation Kubernetes execution pods")
    }

    /// Pod names on *inactive* execution rows.
    ///
    /// No successor can own one: a successor's pod is recorded on the active
    /// row. So this is the set a caller that observed no active execution may
    /// still collect — the orphan left when a create cleanup marked its row
    /// inactive and then failed its Kubernetes delete, which no other pass
    /// retries because they all select active rows.
    pub(super) async fn inactive_execution_pod_names(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<String>, SubstrateError> {
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT pod_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND NOT active"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .db_context("failed to query inactive Kubernetes execution pods")
    }

    /// Return every pod name recorded for the sandbox so sandbox deletion can
    /// issue idempotent deletes for stale generations as well as the active one.
    pub(super) async fn tracked_execution_pod_names(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<String>, SubstrateError> {
        sqlx::query_scalar::<_, String>(
            "SELECT pod_name FROM sandbox_k8s_executions WHERE sandbox_id = $1",
        )
        .bind(sandbox_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .db_context("failed to query Kubernetes execution pods")
    }

    async fn lock_sandbox_for_execution_mutation_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        // Multi-row execution mutations lock the sandbox before execution
        // rows so concurrent creation and cleanup cannot cycle.
        sqlx::query_scalar::<_, Uuid>(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .fetch_optional(conn)
            .await
            .map(|row| row.is_some())
            .db_context("failed to lock sandbox row for Kubernetes execution")
    }

    async fn lock_sandbox_execution_lifecycle_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxLifecycle>, SubstrateError> {
        let lifecycle: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(conn)
        .await
        .db_context("failed to lock sandbox for execution materialization")?;
        lifecycle
            .map(|lifecycle| {
                lifecycle
                    .parse::<SandboxLifecycle>()
                    .map_err(|err| SubstrateError::Failed {
                        message: format!("failed to parse sandbox execution lifecycle: {err}"),
                    })
            })
            .transpose()
    }

    async fn lock_sandbox_for_execution_lifecycle_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        expected_lifecycle: SandboxLifecycle,
    ) -> Result<(), SubstrateError> {
        let Some(lifecycle) = Self::lock_sandbox_execution_lifecycle_in(conn, sandbox_id).await?
        else {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        };
        if lifecycle == SandboxLifecycle::Suspending {
            return Err(SubstrateError::suspend_in_flight(sandbox_id));
        }
        if lifecycle != expected_lifecycle {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox cannot materialize a {expected_lifecycle} execution while lifecycle is {lifecycle}"
                ),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn ensure_active_execution_claim(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes execution transaction")?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await?;

        if !sandbox_exists {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }

        let claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, None)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes execution transaction")?;

        Ok(claim)
    }

    pub(super) async fn ensure_active_execution_claim_for_lifecycle(
        &self,
        sandbox_id: SandboxId,
        expected_lifecycle: SandboxLifecycle,
        restore_lease_token: Option<Uuid>,
        activation_attempt: Option<(
            pplx_space_common_core::types::SandboxActivationAttemptId,
            SandboxTransitionTargetState,
        )>,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes materialization transaction")?;

        Self::lock_sandbox_for_execution_lifecycle_in(&mut tx, sandbox_id, expected_lifecycle)
            .await?;
        if let Some(restore_lease_token) = restore_lease_token {
            let renewed = sqlx::query_scalar::<_, bool>(sql!(
                "UPDATE sandboxes
                    SET restore_lease_expires_at =
                        NOW() + make_interval(secs => $3)
                  WHERE id = $1
                    AND restore_lease_token = $2
                    AND restore_lease_expires_at > NOW()
                RETURNING TRUE"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(restore_lease_token)
            .bind(super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
            .fetch_optional(&mut *tx)
            .await
            .db_context("failed to renew restore materialization ownership")?;
            if renewed.is_none() {
                let lease_remaining_secs = sqlx::query_scalar::<_, f64>(sql!(
                    "SELECT GREATEST(
                                EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                                0.0
                            )::float8
                       FROM sandboxes
                      WHERE id = $1"
                ))
                .bind(sandbox_id.as_uuid())
                .fetch_one(&mut *tx)
                .await
                .db_context("failed to query replacement restore lease")?;
                return Err(Self::restore_ownership_collision(
                    sandbox_id,
                    lease_remaining_secs,
                ));
            }
        }
        let claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, activation_attempt)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes materialization transaction")?;

        record_route_identity_decision(&claim, false);

        Ok(claim)
    }

    async fn ensure_active_execution_claim_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        activation_attempt: Option<(
            pplx_space_common_core::types::SandboxActivationAttemptId,
            SandboxTransitionTargetState,
        )>,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut claim = match self.active_execution_row(&mut *tx, sandbox_uuid).await? {
            Some(row) => SandboxExecutionClaim {
                row,
                created: false,
                route_publication: None,
            },
            None => {
                self.insert_active_execution_claim_in(&mut *tx, sandbox_id)
                    .await?
            }
        };

        if let Some((activation_attempt_id, target_state)) = activation_attempt {
            let route_publication =
                classify_activation_route_publication(&claim.row, claim.created)?;
            let activation_attempt_id = Self::assign_activation_attempt_in(
                tx,
                sandbox_id,
                claim.row.generation,
                activation_attempt_id,
                target_state,
                route_publication,
            )
            .await?;
            claim.row.activation_attempt_id = Some(activation_attempt_id);
            if route_publication == ActivationRoutePublication::InitializeFirst {
                claim.row.route_activation_id = Some(activation_attempt_id);
            }
            claim.route_publication = Some(route_publication);
        }

        Ok(claim)
    }

    async fn started_recovery_lease_remaining(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Duration, SubstrateError> {
        let remaining_millis = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT GREATEST(
                        CEIL(EXTRACT(EPOCH FROM (
                            updated_at + make_interval(secs => $3) - NOW()
                        )) * 1000)::BIGINT,
                        0::BIGINT
                    )
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation >= $2 AND active
             ORDER BY generation DESC
             LIMIT 1"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(recovery_lease_ttl_secs())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to read started recovery lease")?;
        let Some(remaining_millis) = remaining_millis else {
            return Ok(Duration::ZERO);
        };
        let remaining_millis =
            u64::try_from(remaining_millis).map_err(|err| SubstrateError::Failed {
                message: format!("invalid started recovery lease duration: {err}"),
            })?;
        Ok(Duration::from_millis(remaining_millis))
    }

    async fn fresh_started_recovery(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<(i64, Duration)>, SubstrateError> {
        let row = sqlx::query_as::<_, (i64, i64)>(sql!(
            "SELECT generation,
                    GREATEST(
                        CEIL(EXTRACT(EPOCH FROM (
                            updated_at + make_interval(secs => $3) - NOW()
                        )) * 1000)::BIGINT,
                        0::BIGINT
                    ) AS lease_remaining_millis
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active AND state = $2
               AND updated_at > NOW() - make_interval(secs => $3)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(ActiveExecutionState::Recovering.as_str())
        .bind(recovery_lease_ttl_secs())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to inspect started recovery lease")?;

        row.map(|(generation, remaining_millis)| {
            u64::try_from(remaining_millis)
                .map(|remaining| (generation, Duration::from_millis(remaining)))
                .map_err(|err| SubstrateError::Failed {
                    message: format!("invalid started recovery lease duration: {err}"),
                })
        })
        .transpose()
    }

    pub(super) async fn claim_started_recovery(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<StartedRecoveryClaim, SubstrateError> {
        if let Some((generation, lease_remaining)) = self.fresh_started_recovery(sandbox_id).await?
        {
            return Ok(StartedRecoveryClaim::InFlight {
                generation,
                lease_remaining,
            });
        }

        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes recovery transaction")?;
        let Some(lifecycle) =
            Self::lock_sandbox_execution_lifecycle_in(&mut tx, sandbox_id).await?
        else {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        };
        match lifecycle {
            SandboxLifecycle::Started => {}
            SandboxLifecycle::Suspending => {
                return Err(SubstrateError::suspend_in_flight(sandbox_id));
            }
            lifecycle => {
                return Err(SubstrateError::Conflict {
                    message: format!(
                        "sandbox cannot materialize a {} execution while lifecycle is {lifecycle}",
                        SandboxLifecycle::Started,
                    ),
                });
            }
        }

        let claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, None)
            .await?;
        let generation = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = 'recovering', updated_at = NOW()
             WHERE sandbox_id = $1 AND generation = $2 AND active
               AND (
                 ($3 AND state = 'creating')
                 OR (
                   NOT $3
                   AND (
                     state IN ('created', 'running', 'recovery_failed')
                     OR (
                       state IN ('creating', 'recovering')
                       AND updated_at <= NOW() - make_interval(secs => $4)
                     )
                   )
                 )
               )
             RETURNING generation"
        ))
        .bind(sandbox_uuid)
        .bind(claim.row.generation)
        .bind(claim.created)
        .bind(recovery_lease_ttl_secs())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to claim started sandbox recovery")?;

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes recovery transaction")?;

        match generation {
            Some(generation) if claim.created => {
                Ok(StartedRecoveryClaim::CreatedExecution { generation })
            }
            Some(generation) => Ok(StartedRecoveryClaim::ExistingExecution { generation }),
            None => Ok(StartedRecoveryClaim::InFlight {
                generation: claim.row.generation,
                lease_remaining: self
                    .started_recovery_lease_remaining(sandbox_id, claim.row.generation)
                    .await?,
            }),
        }
    }

    pub(super) async fn started_recovery_restore_context(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> Result<StartedRecoveryRestoreContext, SubstrateError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .db_context("failed to acquire started recovery restore connection")?;
        self.started_recovery_restore_context_in(&mut conn, sandbox_id, generation)
            .await
    }

    async fn started_recovery_restore_context_in(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> Result<StartedRecoveryRestoreContext, SubstrateError> {
        let no_newer = no_newer_full_snapshot("s", "sn");
        let query = format!(
            "SELECT sn.state,
                    nk.k8s_node_name,
                    (
                        SELECT ssl.storage_connection_string
                          FROM snapshot_storage_locations ssl
                         WHERE ssl.snapshot_id = sn.id
                           AND ssl.orphaned_at IS NULL
                         ORDER BY ssl.created_at DESC
                         LIMIT 1
                    ) AS storage_uri
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.active
                AND e.state = 'recovering'
               JOIN snapshots sn
                 ON sn.sandbox_id = s.id
                AND sn.local_artifact_id = s.current_local_artifact_id
                AND sn.artifact_kind = s.current_local_artifact_kind
                AND sn.type = 'full'
                AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
               LEFT JOIN nodes n
                 ON n.id = sn.source_node_id
                AND n.role = 'worker'
                AND n.state = 'ready'
                AND n.drain_phase IS NULL
               LEFT JOIN node_k8s nk ON nk.node_id = n.id
              WHERE s.id = $1
                AND s.lifecycle = 'started'
                AND s.current_local_artifact_origin = 'suspend'
                AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
                AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
                AND {no_newer}
              ORDER BY sn.timestamp DESC, sn.id DESC
              LIMIT 1"
        );
        let candidate = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
            sqlx::AssertSqlSafe(sql!(query.as_str())),
        )
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to select started recovery restore context")?;
        let Some((state, source_node_name, storage_uri)) = candidate else {
            return Ok(StartedRecoveryRestoreContext::Unavailable);
        };
        match state
            .parse::<SnapshotState>()
            .map_err(|err| SubstrateError::Failed {
                message: format!("invalid started recovery snapshot state: {err}"),
            })? {
            SnapshotState::Creating => Ok(source_node_name.map_or(
                StartedRecoveryRestoreContext::Pending,
                |source_node_name| {
                    StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                        source_node_name,
                    })
                },
            )),
            SnapshotState::Ready => {
                let Some(storage_uri) = storage_uri else {
                    return Ok(StartedRecoveryRestoreContext::Unavailable);
                };
                let storage_uri =
                    ExactStorageUri::parse(&storage_uri).map_err(|err| SubstrateError::Failed {
                        message: format!("invalid started recovery restore storage URI: {err}"),
                    })?;
                Ok(StartedRecoveryRestoreContext::Ready(
                    StartedRecoveryRestoreSource::Remote { storage_uri },
                ))
            }
            SnapshotState::Failed => Ok(StartedRecoveryRestoreContext::Unavailable),
            // A `deleting` row is claimed for prune and de-advertised; restore
            // must not route to it even though its bytes may still exist. A
            // marker-matched row in this state also means the retention
            // guard's current-artifact exclusion was breached — surface that
            // loudly rather than silently reporting no restore source.
            SnapshotState::Deleting => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    "started recovery restore context: marker-matched snapshot \
                     is claimed 'deleting'; treating the restore source as gone"
                );
                Ok(StartedRecoveryRestoreContext::Unavailable)
            }
        }
    }

    pub(super) async fn replace_started_recovery_execution(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        allowed_local_source: Option<&str>,
    ) -> Result<StartedRecoveryReplacement, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start started recovery handoff transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await? {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }

        let previous_attempt = sqlx::query_as::<_, (Option<Uuid>, Option<String>)>(sql!(
            "SELECT e.activation_attempt_id, e.activation_attempt_target_state
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.active
                AND e.state = 'recovering'
              WHERE s.id = $1
                AND s.lifecycle = 'started'
              FOR UPDATE OF e"
        ))
        .bind(sandbox_uuid)
        .bind(generation.get())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock started recovery execution")?;
        let Some((activation_attempt_id, activation_target)) = previous_attempt else {
            tx.commit()
                .await
                .db_context("failed to commit lost started recovery handoff")?;
            return Ok(StartedRecoveryReplacement::OwnershipLost);
        };
        let activation_target = activation_target
            .map(|target| target.parse::<SandboxTransitionTargetState>())
            .transpose()
            .map_err(|err| SubstrateError::Failed {
                message: format!("invalid started recovery activation target: {err}"),
            })?;

        let source = match self
            .started_recovery_restore_context_in(&mut tx, sandbox_id, generation)
            .await?
        {
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                source_node_name,
            }) if allowed_local_source == Some(source_node_name.as_str()) => {
                StartedRecoveryRestoreSource::Local { source_node_name }
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                ..
            })
            | StartedRecoveryRestoreContext::Pending => {
                tx.commit()
                    .await
                    .db_context("failed to commit pending started recovery handoff")?;
                return Ok(StartedRecoveryReplacement::Pending);
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Remote {
                storage_uri,
            }) => StartedRecoveryRestoreSource::Remote { storage_uri },
            StartedRecoveryRestoreContext::Unavailable => {
                tx.commit()
                    .await
                    .db_context("failed to commit unavailable started recovery handoff")?;
                return Ok(StartedRecoveryReplacement::Unavailable);
            }
        };

        if !self
            .finish_active_execution_generation_in(
                &mut tx,
                sandbox_uuid,
                generation.get(),
                "replaced",
            )
            .await?
        {
            return Err(SubstrateError::Failed {
                message: "started recovery execution changed during atomic handoff".to_string(),
            });
        }

        let mut claim = self
            .insert_active_execution_claim_in(&mut tx, sandbox_id)
            .await?;
        if !claim.created {
            return Err(SubstrateError::Failed {
                message: "failed to allocate started recovery successor execution".to_string(),
            });
        }
        let updated = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET state = 'recovering',
                    activation_attempt_id = $3,
                    route_activation_id = $3,
                    activation_attempt_expires_at = CASE
                        WHEN $3::UUID IS NULL THEN NULL
                        ELSE NOW() + make_interval(secs => $5)
                    END,
                    activation_attempt_target_state = $4,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND state = 'creating'"
        ))
        .bind(sandbox_uuid)
        .bind(claim.row.generation)
        .bind(activation_attempt_id)
        .bind(activation_target.map(SandboxTransitionTargetState::as_str))
        .bind(super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
        .execute(&mut *tx)
        .await
        .db_context("failed to preserve started recovery ownership")?
        .rows_affected();
        if updated != 1 {
            return Err(SubstrateError::Failed {
                message: "failed to preserve started recovery ownership".to_string(),
            });
        }
        claim.row.state = ActiveExecutionState::Recovering;
        claim.row.activation_attempt_id = activation_attempt_id
            .map(pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid);
        claim.row.route_activation_id = claim.row.activation_attempt_id;

        tx.commit()
            .await
            .db_context("failed to commit started recovery handoff")?;
        if activation_attempt_id.is_some() {
            pplx_space_metrics::sandbox::api::route_identity_decision(
                RouteIdentityDecisionReason::ExecutionReplaced,
            );
        }
        Ok(StartedRecoveryReplacement::Replaced {
            claim: Box::new(claim),
            source,
        })
    }

    pub(super) async fn complete_started_recovery(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
        completion: StartedRecoveryCompletion,
    ) -> Result<(), SubstrateError> {
        if completion == StartedRecoveryCompletion::Failed {
            let generation =
                ExecutionGeneration::new(generation).map_err(|err| SubstrateError::Failed {
                    message: format!(
                        "invalid started recovery execution generation {generation}: {err}"
                    ),
                })?;
            return self
                .complete_failed_started_recovery(sandbox_id, generation, activation_attempt_id)
                .await;
        }

        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = $3, updated_at = NOW()
             WHERE sandbox_id = $1 AND generation >= $2 AND active AND state = $4"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(completion.as_str())
        .bind(ActiveExecutionState::Recovering.as_str())
        .execute(&self.pool)
        .await
        .db_context_with(|| {
            format!("failed to complete started sandbox recovery as {completion}")
        })?;
        Ok(())
    }

    async fn complete_failed_started_recovery(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
    ) -> Result<(), SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start failed recovery transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await? {
            return Ok(());
        }

        let failed_generation = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = $3,
                 activation_attempt_id = NULL,
                 activation_attempt_expires_at = NULL,
                 activation_attempt_target_state = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND state = $4
               AND (
                 (
                   $5::UUID IS NULL
                   AND activation_attempt_id IS NULL
                   AND generation = $2
                 )
                 OR (
                   $5::UUID IS NOT NULL
                   AND activation_attempt_id = $5
                   AND generation >= $2
                 )
               )
             RETURNING generation"
        ))
        .bind(sandbox_uuid)
        .bind(generation.get())
        .bind(StartedRecoveryCompletion::Failed.as_str())
        .bind(ActiveExecutionState::Recovering.as_str())
        .bind(activation_attempt_id)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to complete started sandbox recovery as failed")?;
        let generation = failed_generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|err| SubstrateError::Failed {
                message: format!("invalid failed started recovery generation: {err}"),
            })?;
        let recovered = match generation {
            Some(generation) => {
                self.recover_started_after_failed_recovery_in(&mut tx, sandbox_uuid, generation)
                    .await?
            }
            None => false,
        };
        if recovered
            && !self
                .finish_active_execution_generation_in(
                    &mut tx,
                    sandbox_uuid,
                    generation
                        .expect("recovered execution has a generation")
                        .get(),
                    ActiveExecutionState::RecoveryFailed.as_str(),
                )
                .await?
        {
            return Err(SubstrateError::Failed {
                message: "failed to retire converged started recovery execution".to_string(),
            });
        }

        tx.commit()
            .await
            .db_context("failed to commit failed recovery transaction")?;
        if recovered {
            let generation = generation.expect("recovered execution has a generation");
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation = %generation,
                "recovered started sandbox to suspended after terminal recovery failure"
            );
            emit_started_suspend_recovery_event(
                sandbox_uuid,
                StartedSuspendRecoveryOperation::StartedRecoveryFailure,
            );
        }
        Ok(())
    }

    pub(super) async fn observe_started_recovery(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<StartedRecoveryObservation, SubstrateError> {
        let row = sqlx::query_as::<_, (i64, String, String, Option<String>)>(sql!(
            "SELECT generation, state, pod_name, host(pod_ip)
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation >= $2 AND active
               AND state IN ($3, $4, $5)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(ActiveExecutionState::Creating.as_str())
        .bind(ActiveExecutionState::Recovering.as_str())
        .bind(ActiveExecutionState::Running.as_str())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to observe started sandbox recovery")?;

        let Some((generation, state, pod_name, pod_ip)) = row else {
            return Ok(StartedRecoveryObservation::Failed);
        };
        let state =
            state
                .parse::<ActiveExecutionState>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse started recovery execution state: {err}"),
                })?;

        Ok(match (state, pod_ip) {
            (ActiveExecutionState::Running, Some(pod_ip)) => {
                StartedRecoveryObservation::Succeeded {
                    generation,
                    pod_name,
                    pod_ip,
                }
            }
            (ActiveExecutionState::Creating | ActiveExecutionState::Recovering, _) => {
                StartedRecoveryObservation::Pending
            }
            _ => StartedRecoveryObservation::Failed,
        })
    }

    /// Post-CAS-loss snapshot of the restore gate. Fresh execution work keeps
    /// ownership live after the original lease deadline so an already-issued
    /// Kubernetes or worker request cannot overlap a replacement owner.
    pub(super) async fn fetch_restore_ownership_snapshot(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(SandboxLifecycle, Option<String>, f64), SubstrateError> {
        let (lifecycle, marker, lease_remaining_secs, execution_remaining_secs): (
            String,
            Option<String>,
            f64,
            f64,
        ) = sqlx::query_as(sql!(
            "SELECT lifecycle,
                    evacuation_restore_target,
                    GREATEST(
                        EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                        0.0
                    )::float8,
                    GREATEST(
                        CASE
                          WHEN e.state IN ('creating', 'created', 'recovering', 'running')
                          THEN EXTRACT(EPOCH FROM (
                              e.updated_at + make_interval(secs => $2) - NOW()
                          ))
                          ELSE 0.0
                        END,
                        0.0
                    )::float8
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id AND e.active
              WHERE s.id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to query sandbox restore ownership")?
        .ok_or_else(|| SubstrateError::NotFound {
            message: format!("sandbox {sandbox_id} not found"),
        })?;
        let lifecycle =
            lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse sandbox restore lifecycle: {err}"),
                })?;
        let ownership_remaining_secs = if lifecycle == SandboxLifecycle::Restoring {
            lease_remaining_secs.max(execution_remaining_secs)
        } else {
            lease_remaining_secs
        };
        Ok((lifecycle, marker, ownership_remaining_secs))
    }

    fn restore_ownership_collision(
        sandbox_id: SandboxId,
        ownership_remaining_secs: f64,
    ) -> SubstrateError {
        SubstrateError::restore_in_flight_with_lease(
            format!(
                "sandbox restore is already in flight for sandbox {sandbox_id}; \
                 retry the same sandbox after its current ownership window expires"
            ),
            Duration::from_secs_f64(ownership_remaining_secs),
        )
    }

    /// Observe the outcome of a user restore whose lease another request
    /// holds. One snapshot query so the lifecycle, lease, marker, and active
    /// execution routability are read consistently.
    pub(super) async fn observe_user_restore(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<UserRestoreObservation, SubstrateError> {
        self.observe_user_restores(&[sandbox_id])
            .await?
            .remove(&sandbox_id)
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox {sandbox_id} not found"),
            })
    }

    /// Observe all locally-joined user restores in one database round trip.
    /// The API process owns one polling loop which supplies this complete set;
    /// request futures subscribe to its per-sandbox watch channels instead of
    /// issuing independent queries while a restore is in flight.
    pub(super) async fn observe_user_restores(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, UserRestoreObservation>, SubstrateError> {
        if sandbox_ids.is_empty() {
            return Ok(HashMap::new());
        }

        type Row = (
            Uuid,
            String,
            Option<String>,
            f64,
            f64,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let row = sqlx::query_as::<_, Row>(sql!(
            "SELECT s.id,
                    s.lifecycle,
                    s.evacuation_restore_target,
                    GREATEST(EXTRACT(EPOCH FROM (s.restore_lease_expires_at - NOW())), 0.0)::float8,
                    GREATEST(
                        CASE
                          WHEN e.state IN ('creating', 'created', 'recovering', 'running')
                          THEN EXTRACT(EPOCH FROM (
                              e.updated_at + make_interval(secs => $2) - NOW()
                          ))
                          ELSE 0.0
                        END,
                        0.0
                    )::float8,
                    e.generation,
                    e.state,
                    e.pod_name,
                    host(e.pod_ip)
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id AND e.active
              WHERE s.id = ANY($1)"
        ))
        .bind(
            sandbox_ids
                .iter()
                .map(|sandbox_id| sandbox_id.as_uuid())
                .collect::<Vec<_>>(),
        )
        .bind(super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to batch-observe user restores")?;

        row.into_iter()
            .map(
                |(
                    sandbox_id,
                    lifecycle,
                    marker,
                    lease_remaining_secs,
                    execution_remaining_secs,
                    generation,
                    state,
                    pod_name,
                    pod_ip,
                )| {
                    let sandbox_id = SandboxId::from_uuid(sandbox_id);
                    let lifecycle = lifecycle.parse::<SandboxLifecycle>().map_err(|err| {
                        SubstrateError::Failed {
                            message: format!(
                                "failed to parse user restore lifecycle for {sandbox_id}: {err}"
                            ),
                        }
                    })?;
                    let ownership_remaining_secs = if lifecycle == SandboxLifecycle::Restoring {
                        lease_remaining_secs.max(execution_remaining_secs)
                    } else {
                        lease_remaining_secs
                    };
                    let observation = if lifecycle == SandboxLifecycle::Started {
                        match (generation, state, pod_name, pod_ip) {
                            (Some(generation), Some(state), Some(pod_name), Some(pod_ip))
                                if state.parse::<ActiveExecutionState>().map_err(|err| {
                                    SubstrateError::Failed {
                                        message: format!(
                                            "failed to parse user restore execution state for \
                                             {sandbox_id}: {err}"
                                        ),
                                    }
                                })? == ActiveExecutionState::Running =>
                            {
                                UserRestoreObservation::Routable {
                                    generation,
                                    pod_name,
                                    pod_ip,
                                }
                            }
                            _ => UserRestoreObservation::StartedUnroutable,
                        }
                    } else if matches!(
                        lifecycle,
                        SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
                    ) && marker.is_none()
                        && ownership_remaining_secs > 0.0
                    {
                        UserRestoreObservation::InFlight {
                            lease_remaining: Duration::from_secs_f64(ownership_remaining_secs),
                        }
                    } else {
                        UserRestoreObservation::Claimable
                    };
                    Ok((sandbox_id, observation))
                },
            )
            .collect()
    }

    pub(super) async fn replace_active_execution_claim_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        state: &str,
        activation_attempt: Option<(SandboxActivationAttemptId, SandboxTransitionTargetState)>,
    ) -> Result<Option<SandboxExecutionClaim>, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes execution transaction")?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await?;

        if !sandbox_exists {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }

        let recovery_in_flight = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT state = $3
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = $2 AND active"
        ))
        .bind(sandbox_uuid)
        .bind(generation)
        .bind(ActiveExecutionState::Recovering.as_str())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to inspect replaced Kubernetes execution state")?
        .unwrap_or(false);

        let replaced = self
            .finish_active_execution_generation_in(&mut tx, sandbox_uuid, generation, state)
            .await?;
        if !replaced {
            tx.commit()
                .await
                .db_context("failed to commit Kubernetes execution transaction")?;
            return Ok(None);
        }

        let mut claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, activation_attempt)
            .await?;
        if recovery_in_flight {
            let marked = sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                 SET state = $3, updated_at = NOW()
                 WHERE sandbox_id = $1 AND generation = $2 AND active AND state = $4"
            ))
            .bind(sandbox_uuid)
            .bind(claim.row.generation)
            .bind(ActiveExecutionState::Recovering.as_str())
            .bind(ActiveExecutionState::Creating.as_str())
            .execute(&mut *tx)
            .await
            .db_context("failed to preserve recovery on replacement execution")?
            .rows_affected();
            if marked != 1 {
                return Err(SubstrateError::Failed {
                    message: "failed to preserve recovery on replacement execution".to_string(),
                });
            }
            claim.row.state = ActiveExecutionState::Recovering;
        }
        tx.commit()
            .await
            .db_context("failed to commit Kubernetes execution transaction")?;

        record_route_identity_decision(&claim, true);

        Ok(Some(claim))
    }

    #[cfg(test)]
    pub(super) async fn ensure_active_execution_row(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<SandboxExecutionRow, SubstrateError> {
        let claim = self.ensure_active_execution_claim(sandbox_id).await?;
        let row = claim.row;
        Ok(row)
    }

    pub(super) async fn begin_activation_execution_row(
        &self,
        sandbox_id: SandboxId,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
    ) -> Result<SandboxExecutionRow, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start activation execution transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await? {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }
        let claim = self
            .ensure_active_execution_claim_in(
                &mut tx,
                sandbox_id,
                Some((activation_attempt_id, target_state)),
            )
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit activation execution transaction")?;
        record_route_identity_decision(&claim, false);
        Ok(claim.row)
    }

    pub(super) async fn assign_activation_attempt(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
    ) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start activation-attempt transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }
        let row = self
            .active_execution_row(&mut tx, sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Conflict {
                message: "execution changed before activation attempt assignment".to_string(),
            })?;
        let route_publication = classify_activation_route_publication(&row, false)?;
        let activation_attempt_id = Self::assign_activation_attempt_in(
            &mut tx,
            sandbox_id,
            generation,
            activation_attempt_id,
            target_state,
            route_publication,
        )
        .await?;
        tx.commit()
            .await
            .db_context("failed to commit activation-attempt transaction")?;
        match route_publication {
            ActivationRoutePublication::InitializeFirst => {
                pplx_space_metrics::sandbox::api::route_identity_decision(
                    RouteIdentityDecisionReason::FirstIdentityInitialized,
                );
            }
            ActivationRoutePublication::AdoptLegacyResident => {
                pplx_space_metrics::sandbox::api::route_identity_decision(
                    RouteIdentityDecisionReason::ExistingExecutionTransition,
                );
            }
            ActivationRoutePublication::PreserveExisting => {}
        }
        Ok(activation_attempt_id)
    }

    /// Publish the route selected by a legacy resident Wake only after the
    /// manager returned the exact typed acknowledgement. This upgrades rows
    /// created before `route_activation_id` existed without recreating the
    /// unsafe publish-before-adopt ordering.
    pub(super) async fn publish_legacy_route_identity_after_ack(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
        expected_pod_uid: &str,
    ) -> Result<(), SubstrateError> {
        let published = sqlx::query_scalar::<_, Uuid>(sql!(
            "UPDATE sandbox_k8s_executions
                SET route_activation_id = $3,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND activation_attempt_id = $3
                AND route_activation_id IS NULL
                AND pod_uid = $4
              RETURNING route_activation_id"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .bind(expected_pod_uid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("failed to publish acknowledged legacy route identity: {error}"),
        })?;
        if published == Some(activation_attempt_id.as_uuid()) {
            return Ok(());
        }
        let current = sqlx::query_scalar::<_, Option<Uuid>>(sql!(
            "SELECT route_activation_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND pod_uid = $3"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(expected_pod_uid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("failed to verify acknowledged legacy route identity: {error}"),
        })?;
        if current.flatten() == Some(activation_attempt_id.as_uuid()) {
            Ok(())
        } else {
            Err(SubstrateError::Conflict {
                message: "execution changed before acknowledged legacy route publication"
                    .to_string(),
            })
        }
    }

    /// Use one database clock to distinguish the execution retired by STOP
    /// from a replacement that booted before its START published completion.
    pub(super) async fn execution_predates_current_stop(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                SELECT 1 FROM sandbox_k8s_executions execution
                JOIN sandboxes sandbox ON sandbox.id = execution.sandbox_id
                WHERE execution.sandbox_id = $1 AND execution.generation = $2
                  AND execution.active AND sandbox.lifecycle = 'stopped'
                  AND execution.created_at <= sandbox.lifecycle_changed_at
            )"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_one(&self.pool)
        .await
        .db_context("failed to identify execution retired by stop")
    }

    pub(super) async fn complete_activation_attempt(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<bool, SubstrateError> {
        let changed = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
               FROM sandboxes sandbox
              WHERE sandbox.id = execution.sandbox_id
                AND execution.sandbox_id = $1
                AND execution.generation = $2
                AND execution.active
                AND execution.activation_attempt_id = $3
                AND execution.route_activation_id IS NOT NULL
                AND LOWER(TRIM(sandbox.lifecycle)) = execution.activation_attempt_target_state"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&self.pool)
        .await
        .db_context("failed to complete activation attempt")?;
        Ok(changed.rows_affected() == 1)
    }

    pub(super) async fn release_activation_attempt_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) {
        if let Err(error) = self
            .complete_activation_attempt(sandbox_id, generation, activation_attempt_id)
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %activation_attempt_id,
                %error,
                "deferred activation-attempt release to completion receipt or lease expiry"
            );
        }
    }

    /// Release ownership when this caller proves either that no Wake was
    /// dispatched or that its activation reached command readiness. A completed
    /// activation may already have been stopped again; its exact lease can be
    /// released without changing the newer lifecycle or published route identity.
    /// Potentially dispatched, unfinished work must retain its lease.
    pub(super) async fn release_finished_activation_attempt_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
    ) {
        if let Err(error) = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
              WHERE sandbox_id = $1 AND generation = $2 AND active
                AND activation_attempt_id = $3"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&self.pool)
        .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %activation_attempt_id,
                %error,
                "failed to release finished activation; lease expiry will allow recovery"
            );
        }
    }

    pub(super) async fn abandon_activation_attempt_superseded_by_suspend(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<bool, SubstrateError> {
        let changed = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
               FROM sandboxes sandbox
              WHERE sandbox.id = execution.sandbox_id
                AND execution.sandbox_id = $1
                AND execution.generation = $2
                AND execution.active
                AND execution.activation_attempt_id = $3
                AND LOWER(TRIM(sandbox.lifecycle)) IN ('suspended', 'restoring')"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&self.pool)
        .await
        .db_context("failed to abandon suspend-superseded activation attempt")?;
        Ok(changed.rows_affected() == 1)
    }

    pub(super) async fn abandon_activation_attempt_superseded_by_suspend_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) {
        if let Err(error) = self
            .abandon_activation_attempt_superseded_by_suspend(
                sandbox_id,
                generation,
                activation_attempt_id,
            )
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %activation_attempt_id,
                %error,
                "failed to abandon suspend-superseded activation attempt"
            );
        }
    }

    async fn assign_activation_attempt_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        requested_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
        route_publication: ActivationRoutePublication,
    ) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, SubstrateError> {
        let current_attempt =
            sqlx::query_as::<_, (Option<Uuid>, Option<String>, f64, Option<Uuid>)>(sql!(
                "SELECT activation_attempt_id,
                    activation_attempt_target_state,
                    GREATEST(
                        EXTRACT(EPOCH FROM (activation_attempt_expires_at - NOW())),
                        0.0
                    )::float8,
                    route_activation_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2 AND active
              FOR UPDATE"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation)
            .fetch_optional(&mut *conn)
            .await
            .db_context("failed to inspect activation attempt owner")?;
        let Some((current_attempt_id, current_target, lease_remaining_secs, route_owner)) =
            current_attempt
        else {
            return Err(SubstrateError::Conflict {
                message: "execution changed before activation attempt assignment".to_string(),
            });
        };
        if let Some(owner_attempt_id) = current_attempt_id
            && lease_remaining_secs > 0.0
        {
            return Err(SubstrateError::ActivationInFlight {
                message: format!(
                    "another activation attempt owns this execution for {}",
                    current_target.as_deref().unwrap_or("an unknown target")
                ),
                lease_remaining: Duration::from_secs_f64(lease_remaining_secs),
                same_target: current_target.as_deref() == Some(target_state.as_str()),
                owner_attempt_id:
                    pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
                        owner_attempt_id,
                    ),
            });
        }
        if route_publication == ActivationRoutePublication::PreserveExisting
            && route_owner.is_none()
        {
            pplx_space_metrics::sandbox::api::route_identity_decision(
                RouteIdentityDecisionReason::RotationRequiredUnclassified,
            );
            return Err(SubstrateError::Conflict {
                message: "resident execution has no published route identity; refusing activation assignment"
                    .to_string(),
            });
        }

        // A resident route may precede an expired operation owner. Respect
        // both IDs so a delayed request cannot move the current lease backward.
        let allocation_floor = route_owner.into_iter().chain(current_attempt_id).max();
        let activation_attempt_id = match allocation_floor {
            Some(route_owner) => {
                let route_owner =
                    pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
                        route_owner,
                    );
                if requested_attempt_id > route_owner {
                    requested_attempt_id
                } else {
                    pplx_space_common_core::types::SandboxActivationAttemptId::new_after(
                        route_owner,
                    )
                    .ok_or_else(|| SubstrateError::Failed {
                        message: "activation attempt identity space exhausted".to_string(),
                    })?
                }
            }
            None => requested_attempt_id,
        };
        let assigned = match route_publication {
            ActivationRoutePublication::PreserveExisting => {
                sqlx::query_scalar::<_, Uuid>(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND route_activation_id IS NOT NULL
                      RETURNING activation_attempt_id"
                ))
                .bind(sandbox_id.as_uuid())
                .bind(generation)
                .bind(activation_attempt_id)
                .bind(target_state.as_str())
                .bind(super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
                .fetch_optional(&mut *conn)
                .await
            }
            ActivationRoutePublication::InitializeFirst => {
                sqlx::query_scalar::<_, Uuid>(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            route_activation_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND state IN ('creating', 'recovering')
                        AND NULLIF(BTRIM(pod_uid), '') IS NULL
                        AND route_activation_id IS NULL
                      RETURNING activation_attempt_id"
                ))
                .bind(sandbox_id.as_uuid())
                .bind(generation)
                .bind(activation_attempt_id)
                .bind(target_state.as_str())
                .bind(super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
                .fetch_optional(&mut *conn)
                .await
            }
            ActivationRoutePublication::AdoptLegacyResident => {
                sqlx::query_scalar::<_, Uuid>(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND route_activation_id IS NULL
                        AND NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                      RETURNING activation_attempt_id"
                ))
                .bind(sandbox_id.as_uuid())
                .bind(generation)
                .bind(activation_attempt_id)
                .bind(target_state.as_str())
                .bind(super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
                .fetch_optional(&mut *conn)
                .await
            }
        }
        // Classified like every other statement in this file; `db_context`
        // renders the same `"{what}: {err}"` message the hand-rolled mapper did.
        .db_context("failed to assign activation attempt")?
        .ok_or_else(|| SubstrateError::Conflict {
            message: match route_publication {
                ActivationRoutePublication::PreserveExisting => {
                    "execution changed or has no published route before activation assignment"
                }
                ActivationRoutePublication::InitializeFirst => {
                    "execution changed before first route initialization"
                }
                ActivationRoutePublication::AdoptLegacyResident => {
                    "legacy resident execution changed before route adoption"
                }
            }
            .to_string(),
        })?;
        Ok(pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(assigned))
    }

    /// CAS-clear the `command_ready_pending` marker once an access has confirmed
    /// the guest command API is up. Returns whether this call **claimed** the
    /// row — i.e. cleared a marker that was still set on a `started` sandbox.
    ///
    /// The `lifecycle = 'started'` guard makes this lose to the command-ready
    /// reaper: if the reaper flipped the row to `failed` (and its trigger
    /// cleared the marker) between the access's readiness probe and this clear,
    /// the CAS claims nothing and the caller learns the pod is being torn down
    /// rather than handing back a doomed client. Conversely, if the access
    /// clears the marker first, the reaper's own `started + marker` CAS claims
    /// nothing and backs off. A `false` return is either that lost race or a
    /// benign already-cleared-by-another-access — the caller re-reads lifecycle
    /// to tell them apart.
    pub(super) async fn clear_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        let result = sqlx::query(sql!(
            "UPDATE sandboxes
             SET command_ready_pending = FALSE
             WHERE id = $1 AND lifecycle = 'started' AND command_ready_pending"
        ))
        .bind(sandbox_id)
        .execute(&self.pool)
        .await
        .db_context("failed to clear command_ready_pending")?;
        Ok(result.rows_affected() > 0)
    }

    /// Persist pod metadata onto the active execution row of `generation`.
    ///
    /// Returns the row's **resulting** state — read back under the same row
    /// lock the update ran under — or `None` when the write was a no-op
    /// because no active row of that generation existed (retired or replaced
    /// by another generation, including a retirement that committed while
    /// this call waited on the row lock). Callers that treat "metadata
    /// persisted" as evidence (e.g. the sweeper's execution hydration) must
    /// branch on the returned state instead of assuming the write landed:
    /// a `recovering` row keeps its state by design (the recovery flow owns
    /// the promote to `running`), so its result is `Some(Recovering)`, not a
    /// routable execution.
    pub(super) async fn persist_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let pod_uid = normalize_pod_uid(pod_uid);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod metadata transaction")?;

        if Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        self.update_active_execution_pod_metadata(&mut tx, sandbox_id, generation, pod_uid, pod_ip)
            .await?;
        // Re-read the resulting state inside the transaction (sees our own
        // update; the row is still locked, so it cannot change underneath).
        let resulting_state = Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
            .await?
            .ok_or_else(|| SubstrateError::Failed {
                message: "locked active Kubernetes execution row disappeared mid-transaction"
                    .to_string(),
            })?;

        tx.commit()
            .await
            .db_context("failed to commit pod metadata transaction")?;
        Ok(Some(resulting_state))
    }

    /// Refresh the dial address only while the complete selected route still
    /// owns the active execution. Unlike `persist_pod_metadata`, this must
    /// never adopt a different Pod UID: callers use the result immediately to
    /// send an authenticated node-api request.
    pub(super) async fn persist_selected_route_pod_ip(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
        route_activation_id: SandboxActivationAttemptId,
        pod_uid: &PodUid,
        pod_ip: PodIp,
    ) -> Result<bool, SubstrateError> {
        let result = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_ip = $1::INET,
                 updated_at = CASE
                     WHEN pod_ip IS DISTINCT FROM $1::INET THEN NOW()
                     ELSE updated_at
                 END
             WHERE sandbox_id = $2
               AND generation = $3
               AND active
               AND pod_name = $4
               AND COALESCE(route_activation_id, activation_attempt_id) = $5
               AND NULLIF(BTRIM(pod_uid), '') = $6
             RETURNING 1"
        ))
        .bind(pod_ip.to_string())
        .bind(sandbox_id)
        .bind(generation)
        .bind(pod_name)
        .bind(route_activation_id.as_uuid())
        .bind(pod_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| SubstrateError::Failed {
            message: format!("failed to persist selected Kubernetes route pod IP: {err}"),
        })?;
        Ok(result.is_some())
    }

    pub(super) async fn claim_active_execution_pod_materialization(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_lifecycle: SandboxLifecycle,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod materialization transaction")?;

        Self::lock_sandbox_for_execution_lifecycle_in(&mut tx, sandbox_id, expected_lifecycle)
            .await?;

        let claimed = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = CASE WHEN state = 'recovering' THEN state ELSE 'creating' END,
                 pod_uid = NULL,
                 pod_ip = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1 AND generation = $2 AND active
             RETURNING 1"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to claim pod materialization")?
        .is_some();

        tx.commit()
            .await
            .db_context("failed to commit pod materialization transaction")?;
        Ok(claimed)
    }

    pub(super) async fn persist_materialized_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_lifecycle: SandboxLifecycle,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let pod_uid = normalize_pod_uid(pod_uid);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod materialization transaction")?;

        let lifecycle = Self::lock_sandbox_execution_lifecycle_in(&mut tx, sandbox_id).await?;
        if lifecycle != Some(expected_lifecycle)
            || Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
                .await?
                .is_none()
        {
            tx.commit()
                .await
                .db_context("failed to commit pod materialization transaction")?;
            return Ok(None);
        }

        self.update_active_execution_pod_metadata(&mut tx, sandbox_id, generation, pod_uid, pod_ip)
            .await?;
        let resulting_state = Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
            .await?
            .ok_or_else(|| SubstrateError::Failed {
                message: "locked active Kubernetes execution row disappeared mid-transaction"
                    .to_string(),
            })?;

        tx.commit()
            .await
            .db_context("failed to commit pod materialization transaction")?;
        Ok(Some(resulting_state))
    }

    pub(super) async fn persist_pod_node_placement(
        &self,
        sandbox_id: SandboxId,
        node_name: &str,
        node_uid: &str,
        node_id: NodeId,
        role: NodeRole,
        address: Option<&str>,
    ) -> Result<(), SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod node placement transaction")?;

        self.upsert_k8s_node_role(
            &mut tx,
            K8sNodeRoleUpsert {
                node_name,
                node_uid,
                node_id,
                role,
                address,
                state: NodeStateWrite::InsertOnly(NodeLifecycleState::Registered),
            },
        )
        .await?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await?;
        if sandbox_exists {
            let active_execution = self
                .update_active_execution_node_name(&mut tx, sandbox_id, node_name)
                .await?;

            if active_execution {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                     SET active_node_id = $1,
                         target_node_id = $1,
                         placement = 'on_node',
                         placement_changed_at = NOW()
                     WHERE id = $2
                       AND (
                         active_node_id IS DISTINCT FROM $1
                         OR target_node_id IS DISTINCT FROM $1
                         OR placement IS DISTINCT FROM 'on_node'
                       )"
                ))
                .bind(node_id)
                .bind(sandbox_id)
                .execute(&mut *tx)
                .await
                // Classified like every sibling in this transaction; the
                // post-side-effect demotion is applied to the whole function's
                // result below.
                .db_context("failed to persist sandbox Kubernetes placement")?;
            }
        }

        tx.commit()
            .await
            .db_context("failed to commit pod node placement transaction")?;
        Ok(())
    }

    /// [`Self::persist_pod_node_placement`], demoting the whole result when the
    /// calling operation's retry would not converge.
    ///
    /// This function only ever runs once the pod exists — recording its node
    /// placement is the point. Where the operation that reached here mints a
    /// fresh `SandboxId` on retry, *nothing* it does may answer retryable,
    /// including the transaction's own `begin` and `commit`. Wrapping the
    /// result once is what makes that true of all five statements inside;
    /// exempting only the `UPDATE` left `begin`, the role upsert, the row lock,
    /// and the `commit` telling the caller to retry a create whose pod is
    /// already live.
    ///
    /// The disposition is the caller's to state because it is a property of the
    /// operation, not of these statements: `start`/`wake`/`access` re-drive a
    /// sandbox the caller already names, so their retry converges on the same
    /// row and a bounded lock wait should stay a 503. Demoting those too would
    /// turn the bound this change exists to make safe back into a 500.
    pub(super) async fn persist_pod_node_placement_after_pod_exists(
        &self,
        placement: PodNodePlacement<'_>,
        retry: PlacementRetryDisposition,
    ) -> Result<(), SubstrateError> {
        let PodNodePlacement {
            sandbox_id,
            node_name,
            node_uid,
            node_id,
            role,
            address,
        } = placement;
        let result = self
            .persist_pod_node_placement(sandbox_id, node_name, node_uid, node_id, role, address)
            .await;
        match retry {
            PlacementRetryDisposition::RetryDuplicatesExecution => {
                result.map_err(SubstrateError::demoted_after_side_effect)
            }
            PlacementRetryDisposition::RetryConverges => result,
        }
    }

    pub(super) async fn persist_k8s_node_inventory(
        &self,
        node_name: &str,
        node_uid: &str,
        node_id: NodeId,
        role: NodeRole,
        address: Option<&str>,
        state: NodeStateWrite,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes node inventory transaction")?;

        let changed = self
            .upsert_k8s_node_role(
                &mut tx,
                K8sNodeRoleUpsert {
                    node_name,
                    node_uid,
                    node_id,
                    role,
                    address,
                    state,
                },
            )
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes node inventory transaction")?;
        Ok(changed)
    }

    /// Demote stale ready rows missing from a credible full-cluster Kubernetes
    /// membership snapshot without deleting node or sandbox placement records.
    pub(super) async fn demote_unobserved_k8s_nodes(
        &self,
        live_node_names: &[String],
        missing_grace: Duration,
        max_per_tick: u32,
    ) -> Result<Vec<String>, SubstrateError> {
        if live_node_names.is_empty() {
            return Ok(Vec::new());
        }

        let missing_grace_secs = i64::try_from(missing_grace.as_secs()).unwrap_or(i64::MAX);
        let mut changed = sqlx::query_scalar::<_, String>(sql!(
            "WITH candidates AS MATERIALIZED (
                 SELECT candidate.id,
                        candidate.last_heartbeat,
                        candidate_k8s.updated_at AS k8s_updated_at
                 FROM nodes AS candidate
                 JOIN node_k8s AS candidate_k8s
                   ON candidate_k8s.node_id = candidate.id
                 WHERE candidate.managed_by = 'k8s'
                   AND candidate.state = 'ready'
                   AND candidate.drain_phase IS NULL
                   AND NOT (candidate_k8s.k8s_node_name = ANY($1::TEXT[]))
                   AND (
                        candidate.last_heartbeat IS NULL
                        OR candidate.last_heartbeat
                           < NOW() - ($2 * INTERVAL '1 second')
                   )
                   AND candidate_k8s.updated_at
                       < NOW() - ($2 * INTERVAL '1 second')
                 ORDER BY candidate_k8s.updated_at, candidate.id
                 LIMIT $3
             )
             UPDATE nodes AS n
             SET state = 'registered'
             FROM node_k8s AS nk
             JOIN candidates AS candidate
               ON candidate.id = nk.node_id
             WHERE n.id = candidate.id
               AND n.managed_by = 'k8s'
               AND n.state = 'ready'
               AND n.drain_phase IS NULL
               AND n.last_heartbeat IS NOT DISTINCT FROM candidate.last_heartbeat
               AND nk.updated_at = candidate.k8s_updated_at
             RETURNING nk.k8s_node_name"
        ))
        .bind(live_node_names)
        .bind(missing_grace_secs)
        .bind(i64::from(max_per_tick))
        .fetch_all(&self.pool)
        .await
        .db_context("failed to demote unobserved Kubernetes nodes")?;
        changed.sort_unstable();
        Ok(changed)
    }

    async fn upsert_k8s_node_role(
        &self,
        conn: &mut PgConnection,
        upsert: K8sNodeRoleUpsert<'_>,
    ) -> Result<bool, SubstrateError> {
        let K8sNodeRoleUpsert {
            node_name,
            node_uid,
            node_id,
            role,
            address,
            state,
        } = upsert;

        // `manage_state` decides whether a conflict-update writes `state`.
        // The lifecycle owner writes it on every upsert; insert-only writers
        // (pod placement, the admin inventory listing) supply an initial state
        // for a new row only and must leave an existing node's state alone.
        let (state, manage_state) = match state {
            NodeStateWrite::Manage(value) => (value.as_str(), true),
            NodeStateWrite::InsertOnly(value) => (value.as_str(), false),
        };

        // Tombstone gate. The bootstrap path on `cluster.rs` refuses
        // re-registration, but inventory upserts (kube_node_sync, late
        // pod placement on a not-yet-cordoned terminated node) reach this
        // function too. Without this check they re-create the `nodes`
        // row under the same deterministic id and subsequent heartbeats
        // resolve a live row before they hit the bootstrap guard, so the
        // ghost row returns. The whole transaction is rolled back.
        if is_node_k8s_tombstoned(conn, node_id, "check").await? {
            return Err(SubstrateError::NodeTombstoned {
                message: format!(
                    "kube node {node_name} role {role} is tombstoned (admin-terminated); \
                     refusing to re-create the SPACE node row"
                ),
            });
        }

        let node_changed = sqlx::query_scalar::<_, bool>(sql!(
            "WITH upsert AS (
                 INSERT INTO nodes (id, address, role, managed_by, state)
                 VALUES ($1, $2::INET, $3, 'k8s', $4)
                 ON CONFLICT (id) DO UPDATE
                 SET address = COALESCE(EXCLUDED.address, nodes.address),
                     role = EXCLUDED.role,
                     managed_by = EXCLUDED.managed_by,
                     state = CASE WHEN $5 THEN EXCLUDED.state ELSE nodes.state END
                 WHERE nodes.managed_by = 'k8s'
                   AND (
                     nodes.address IS DISTINCT FROM COALESCE(EXCLUDED.address, nodes.address)
                     OR nodes.role IS DISTINCT FROM EXCLUDED.role
                     OR ($5 AND nodes.state IS DISTINCT FROM EXCLUDED.state)
                   )
                 RETURNING TRUE AS changed
             )
             SELECT changed FROM upsert
             UNION ALL
             SELECT FALSE FROM nodes
             WHERE id = $1
               AND managed_by = 'k8s'
               AND NOT EXISTS (SELECT 1 FROM upsert)"
        ))
        .bind(node_id)
        .bind(address)
        .bind(role.as_str())
        .bind(state)
        .bind(manage_state)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to upsert Kubernetes node registry")?;

        let node_changed = match node_changed {
            Some(changed) => changed,
            None => {
                // The upsert returned no row. Under READ COMMITTED a
                // concurrent insert of this same node id can commit while our
                // `INSERT ... ON CONFLICT` waits on the unique index: the
                // ON CONFLICT arm then locks and reads that row, the
                // change-detection WHERE skips the now-redundant update
                // (address and role already match, and placement does not own
                // `state`), and the fallback SELECT — bound to our stale
                // statement snapshot — cannot see the row, so the whole query
                // returns nothing. Only a genuinely non-Kubernetes conflict
                // should abort the caller, so re-check node ownership with a
                // fresh statement snapshot before erroring.
                let existing_is_k8s = sqlx::query_scalar::<_, bool>(sql!(
                    "SELECT TRUE FROM nodes WHERE id = $1 AND managed_by = 'k8s'"
                ))
                .bind(node_id)
                .fetch_optional(&mut *conn)
                .await
                .db_context("failed to re-check Kubernetes node ownership")?
                .is_some();

                if !existing_is_k8s {
                    return Err(SubstrateError::Failed {
                        message: format!(
                            "Kubernetes node uid {node_uid} role {role} for {node_name} conflicts with a non-Kubernetes node"
                        ),
                    });
                }
                // The node already exists as a Kubernetes row; the concurrent
                // writer established it and this upsert changed nothing.
                false
            }
        };

        // Detect same-name kube Node UID replacement BEFORE the upsert
        // below overwrites `node_k8s.k8s_node_uid`. If a replacement
        // node arrives and we leave `nodes.drain_phase` intact, the
        // node-drain reconciler will resolve the providerID of the
        // REPLACEMENT instance and ASG-terminate it on the operator's
        // behalf — the original (now-gone) instance was the intended
        // target. Clear drain state atomically with the UID change so
        // in-progress drains are abandoned (operator can re-drain
        // against the new identity if they still want to).
        //
        // `drain_mode` is cleared with them. It records *how* a drain is being
        // serviced, so it means nothing once the phase is gone — and leaving it
        // set is worse than meaningless: the admin listing and `space-admin-cli get-node`
        // read the column independently of the phase, so a surviving mode makes
        // a node that is not draining report `drain_phase: null` next to
        // `drain_mode: gentle`.
        let drain_cleared = sqlx::query(sql!(
            "UPDATE nodes
             SET drain_phase = NULL,
                 drain_mode = NULL,
                 drain_started_at = NULL,
                 drain_last_error = NULL
             WHERE id = $1
               AND EXISTS (
                 SELECT 1 FROM node_k8s
                 WHERE node_id = $1 AND k8s_node_uid IS DISTINCT FROM $2
               )"
        ))
        .bind(node_id)
        .bind(node_uid)
        .execute(&mut *conn)
        .await
        .db_context("failed to clear drain state on UID change")?
        .rows_affected();
        if drain_cleared > 0 {
            tracing::warn!(
                node_id = %node_id,
                node_name = %node_name,
                new_k8s_node_uid = %node_uid,
                "k8s Node UID changed for existing node row; cleared drain_phase/drain_mode/drain_started_at/drain_last_error — \
                 any in-progress drain was abandoned"
            );
        }

        let node_k8s_changed = sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
             VALUES ($1, $2, $3)
             ON CONFLICT (node_id) DO UPDATE
             SET k8s_node_name = EXCLUDED.k8s_node_name,
                 k8s_node_uid = EXCLUDED.k8s_node_uid,
                 updated_at = NOW()
             WHERE node_k8s.k8s_node_name IS DISTINCT FROM EXCLUDED.k8s_node_name
                OR node_k8s.k8s_node_uid IS DISTINCT FROM EXCLUDED.k8s_node_uid"
        ))
        .bind(node_id)
        .bind(node_name)
        .bind(node_uid)
        .execute(&mut *conn)
        .await
        .db_context("failed to upsert Kubernetes node metadata")?
        .rows_affected()
            > 0;

        // Race-close re-check, paired with the pre-check above. A
        // concurrent `:terminate` that commits during our row-lock wait
        // would not have been visible to the pre-check but is now
        // visible — returning Err here causes the caller's transaction
        // to roll back, undoing the resurrection insert. See
        // `bootstrap_k8s_origin_node` for the same pattern.
        if is_node_k8s_tombstoned(conn, node_id, "re-check").await? {
            return Err(SubstrateError::NodeTombstoned {
                message: format!(
                    "kube node {node_name} role {role} was tombstoned during upsert; \
                     rolling back to keep the SPACE node row deleted"
                ),
            });
        }
        Ok(node_changed || drain_cleared > 0 || node_k8s_changed)
    }

    pub(super) async fn restore_source_node_name(
        &self,
        sandbox: &Sandbox,
    ) -> Result<Option<String>, SubstrateError> {
        if let Some(node_id) = sandbox.target_node_id.or(sandbox.active_node_id) {
            if let Some(node_name) = sqlx::query_scalar::<_, String>(sql!(
                "SELECT k8s_node_name FROM node_k8s WHERE node_id = $1"
            ))
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to query Kubernetes restore node preference")?
            {
                return Ok(Some(node_name));
            }

            tracing::warn!(
                sandbox_id = %sandbox.id,
                node_id = %node_id,
                "sandbox restore node id has no Kubernetes node mapping; falling back to pod tracking"
            );
        }

        // Preserve the node that held the suspended bytes after placement
        // pointers have been cleared.
        let executions_query = sql!(
            "SELECT ske.k8s_node_name
               FROM sandbox_k8s_executions ske
              WHERE ske.sandbox_id = $1
                AND ske.k8s_node_name IS NOT NULL
              ORDER BY ske.generation DESC
              LIMIT 1"
        );
        sqlx::query_scalar::<_, String>(executions_query)
            .bind(sandbox.id)
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to query Kubernetes restore node preference")
    }

    pub(super) async fn selected_suspend_restore_storage_uri(
        &self,
        sandbox: &Sandbox,
        evacuation: bool,
    ) -> Result<Option<String>, SubstrateError> {
        // Three snapshot-selection modes:
        //
        // * **Suspended user restore** (`evacuation = false`): the placement-
        //   freshness gate — only a snapshot newer than the last placement is
        //   canonical, so a `wake → modify → re-suspend` cycle can't restore
        //   stale bytes.
        // * **Restore retry** (`lifecycle = restoring`): pick the newest ready
        //   snapshot from before the sandbox entered restore. A failed restore
        //   branch can publish a later artifact, but retry pods need the source
        //   state that restore originally started from. If the last placement
        //   predates restore, it still bounds stale prior-cycle snapshots; if
        //   placement moved during restore, it belongs to the retry branch and
        //   must not strand the original source snapshot. When current local
        //   artifact identity exists, legacy markerless snapshots are not
        //   enough proof that the remote row belongs to this restore source.
        // * **Node-drain cohort restore** (`evacuation = true`): require the
        //   selected snapshot to be the **newest** full snapshot for the same
        //   stale-prior-cycle and failed-placement reasons.
        let restore_retry = sandbox.lifecycle.trim().eq_ignore_ascii_case("restoring");
        let snapshot_guard = if restore_retry && !evacuation {
            format!(
                "AND {}",
                restore_retry_source_snapshot_is_current("s", "sn")
            )
        } else {
            let legacy_snapshot_guard = if evacuation {
                no_newer_full_snapshot("s", "sn")
            } else {
                fresh_ready_snapshot_exists("s")
            };
            // The marked branch uses the shared fragment rather than an
            // inline copy: this is the selector that resolves a marked
            // row's restore source, and the retention sweeper's
            // never-prune-the-current-artifact guard is built from the same
            // fragment. Sharing it is what makes "the sweep cannot delete
            // what restore would select" a property instead of a comment.
            let marked_snapshot_guard = snapshot_is_current_local_artifact("s", "sn");
            format!(
                "AND (
                    {marked_snapshot_guard}
                    OR (s.current_local_artifact_id IS NULL AND {legacy_snapshot_guard})
                )"
            )
        };
        // `sql!` is expanded here, not in the shared body: it stamps the
        // statement with *its own* `module_path!()`/`line!()`, which is what
        // attributes an in-flight query in `pg_stat_activity` and DBM back to
        // the code that issued it. Hoisting it into the helper would give both
        // selectors one identical locator and silently move this one.
        let query = sql!(format!(
            "SELECT ssl.storage_connection_string
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle IN ('suspended', 'restoring')
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'full'
                AND sn.state = 'ready'
                {snapshot_guard}
              ORDER BY sn.timestamp DESC
              LIMIT 1"
        ));
        self.fetch_restore_storage_uri(query, sandbox.id, "suspend")
            .await
    }

    /// Evidence of which CPU captured the full snapshot stored at
    /// `storage_uri`: the identity recorded in its `execution_metadata`, and
    /// the Kubernetes name of the node that uploaded it, for a snapshot that
    /// predates the recorded field.
    ///
    /// Keyed by the exact URI rather than re-running a selector, because the
    /// restore plan may come from a live pod's annotation instead of from
    /// [`Self::selected_suspend_restore_storage_uri`]. `None` when no full
    /// snapshot of this sandbox has that location.
    pub(super) async fn snapshot_capture_host(
        &self,
        sandbox_id: SandboxId,
        storage_uri: &str,
    ) -> Result<Option<SnapshotCaptureHost>, SubstrateError> {
        let row: Option<(Option<String>, Option<String>, f64)> = sqlx::query_as(sql!(
            "SELECT sn.execution_metadata->>'host_cpu', nk.k8s_node_name,
                    EXTRACT(EPOCH FROM sn.timestamp)::float8
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
               LEFT JOIN node_k8s nk ON nk.node_id = sn.source_node_id
              WHERE sn.sandbox_id = $1
                AND sn.type = 'full'
                AND ssl.storage_connection_string = $2
              ORDER BY sn.timestamp DESC
              LIMIT 1"
        ))
        .bind(sandbox_id)
        .bind(storage_uri)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to query Kubernetes restore snapshot capture host")?;
        Ok(
            row.map(|(host_cpu, source_node_name, snapshot_epoch_seconds)| {
                SnapshotCaptureHost::new(host_cpu, source_node_name, snapshot_epoch_seconds)
            }),
        )
    }

    /// The newest ready full snapshot of a markerless sandbox, when restoring
    /// from it provably loses no work: it covers the last recorded command and
    /// access, and no other kind of full snapshot ties or beats it
    /// ([`newest_suspend_snapshot_activity_current`], the same proof the
    /// failed-restore repair loop restores by). Returns its location and
    /// capture evidence, or `None` when the proof doesn't hold.
    ///
    /// The proof is only sound while no runtime can change the sandbox's
    /// state; the caller establishes that before calling. For a `restoring`
    /// row it also requires that no full snapshot postdates the restore's
    /// start, which keeps an artifact published by the restore itself out.
    ///
    /// The placement clock is deliberately not consulted. A cross-node restore
    /// that failed on its target moves `placement_changed_at` past the
    /// snapshot it was restoring, which is what turns the next wake into a
    /// local-only restore on that target; see
    /// `KubernetesSubstrate::cpu_incompatible_local_source_redirect`.
    pub(super) async fn activity_current_markerless_snapshot(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<(String, SnapshotCaptureHost)>, SubstrateError> {
        let activity_current = newest_suspend_snapshot_activity_current("s");
        let row: Option<(String, Option<String>, Option<String>, f64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(format!(
                "SELECT ssl.storage_connection_string, sn.execution_metadata->>'host_cpu',
                        nk.k8s_node_name, EXTRACT(EPOCH FROM sn.timestamp)::float8
                   FROM sandboxes s
                   JOIN snapshots sn ON sn.sandbox_id = s.id
                   JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                   LEFT JOIN node_k8s nk ON nk.node_id = sn.source_node_id
                  WHERE s.id = $1
                    AND s.current_local_artifact_id IS NULL
                    -- A restore under way can publish an artifact of its own
                    -- (a failed branch). The restore retry selector skips such
                    -- rows; this refuses to redirect at all while one exists.
                    AND NOT (
                        s.lifecycle = 'restoring'
                        AND EXISTS (
                            SELECT 1
                              FROM snapshots later
                             WHERE later.sandbox_id = s.id
                               AND later.type = 'full'
                               AND later.timestamp >= s.lifecycle_changed_at
                        )
                    )
                    AND ssl.orphaned_at IS NULL
                    AND sn.type = 'full'
                    AND sn.state = 'ready'
                    -- A newer full snapshot still uploading, or failed, may hold
                    -- later state, so it blocks falling back to an older ready
                    -- one. Stricter than the restore retry selector, which
                    -- ignores some failed retries; stricter only withholds the
                    -- redirect.
                    AND NOT EXISTS (
                        SELECT 1
                          FROM snapshots newer
                         WHERE newer.sandbox_id = s.id
                           AND newer.type = 'full'
                           AND newer.state <> 'ready'
                           AND newer.timestamp > sn.timestamp
                    )
                    AND {activity_current}
                  -- The fragment's EXISTS proves some snapshot covers the
                  -- activity clocks with no other kind of full snapshot tying
                  -- or beating it, so the newest ready full row selected here
                  -- is a suspend (or legacy) snapshot at least that new.
                  ORDER BY sn.timestamp DESC
                  LIMIT 1"
            ))))
            .bind(sandbox_id)
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to query Kubernetes activity-current restore snapshot")?;
        let Some((storage_uri, host_cpu, source_node_name, snapshot_epoch_seconds)) = row else {
            return Ok(None);
        };
        StorageUri::parse(&storage_uri).map_err(|err| SubstrateError::Failed {
            message: format!("invalid activity-current restore storage URI: {err}"),
        })?;
        Ok(Some((
            storage_uri,
            SnapshotCaptureHost::new(host_cpu, source_node_name, snapshot_epoch_seconds),
        )))
    }

    /// Run a restore-source selector and validate what it returns.
    ///
    /// Shared by the suspend and stopped selectors, which each keep their own
    /// whole `SELECT`: the storage guidance requires a statement to stay
    /// syntactically whole and reviewable at its query site, so only the
    /// binding and the result processing are factored out here, never a
    /// predicate. The cost is that a filter belonging to both — the
    /// non-orphaned-location requirement, say — has to be fixed in two
    /// places; that is the trade the guidance makes deliberately, in exchange
    /// for each query being readable where it is written.
    ///
    /// `query` arrives already `sql!`-stamped by its caller, so each selector
    /// keeps its own locator token and stays separable in `pg_stat_activity`
    /// and DBM. Do not re-stamp it here.
    ///
    /// `label` names the caller in the two error messages. Parsing the URI is
    /// validation rather than conversion: the caller wants the wire string,
    /// and a value the catalog holds but `StorageUri` rejects is a stored-data
    /// fault worth failing on rather than handing to a node.
    async fn fetch_restore_storage_uri(
        &self,
        query: String,
        sandbox_id: SandboxId,
        label: &str,
    ) -> Result<Option<String>, SubstrateError> {
        let storage_uri: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
            .bind(sandbox_id)
            .fetch_optional(&self.pool)
            .await
            // `db_context_with` rather than `db_context`: the message
            // interpolates the caller's `label`, so it cannot be a
            // `&'static str`.
            .db_context_with(|| format!("failed to select Kubernetes {label} restore URI"))?;
        if let Some(storage_uri) = &storage_uri {
            StorageUri::parse(storage_uri).map_err(|err| SubstrateError::Failed {
                message: format!("invalid selected {label} restore storage URI: {err}"),
            })?;
        }
        Ok(storage_uri)
    }

    /// Disk-only sibling of [`Self::selected_suspend_restore_storage_uri`]:
    /// the remote artifact a `stopped` sandbox is restored from once its
    /// rootfs has been evacuated off its node.
    ///
    /// The suspend selector cannot serve this shape. It excludes a stopped
    /// disk row on two independent filters — `s.lifecycle IN ('suspended',
    /// 'restoring')` and `sn.type = 'full'` — and neither is incidental. A
    /// `full` artifact's file set is `vmstate` + `memory` + rootfs
    /// (`snapshot_upload::COMMON_VM_ARTIFACTS`, via
    /// `vm_artifacts_for_format`), where `Disk` contributes the rootfs alone
    /// (`artifact_files_for_kind`; both kinds then append the metadata
    /// document). A stopped sandbox has no VM to have produced the first two.
    /// And `stopped` is outside the lifecycle set that selector serves.
    ///
    /// The guard is `snapshot_is_current_local_artifact` and nothing else.
    /// Composing that shared fragment rather than an inline equivalent is what
    /// keeps the retention sweeper from pruning the row this selector loads;
    /// the fragment's own docs in `pplx_space_sandbox_portability` own that
    /// argument. One thing those docs do *not* cover, because it is specific
    /// to disk rows: `SnapshotRetentionDbStore`'s `list_candidates` and
    /// `claim_prune` also filter `type = 'full'`, so a disk row never reaches
    /// the guard at all. The shared spelling is what keeps the property true
    /// if that narrowing is ever lifted — it is not what enforces it now.
    ///
    /// ## Why there is no markerless legacy branch
    ///
    /// The suspend selector carries one, for rollout-skew rows that predate
    /// the artifact-identity marker. A stopped evacuation has no comparable
    /// population to be compatible with, for a blunter reason than the
    /// suspend case: **no control-plane writer sets
    /// `sandboxes.current_local_artifact_kind` to `'disk'` at all.** Three
    /// Postgres writers set it non-NULL, and each is blocked by a *different*
    /// mechanism — worth naming individually, because the obvious answer
    /// ("they all require `ArtifactOrigin::Suspend`") is not what stops the
    /// third one:
    ///
    /// * `finish_suspended_execution` (this file, both the marker-assignment
    ///   and the direct-`UPDATE` positions) takes the kind from a
    ///   `PublishedLocalArtifact`, and `PublishedLocalArtifact::from_published`
    ///   *rejects* an artifact whose kind is not the `expected_kind` its caller
    ///   passes. `Disk` parses off the wire fine; the runtime check is what
    ///   refuses it, and both callers pass `ArtifactKind::Full`.
    /// * `persist_optional_current_local_artifact` (shared `substrate/mod.rs`,
    ///   sole production caller on the Direct substrate) takes `origin` as a
    ///   free parameter, and that caller passes `Suspend` with the same
    ///   `Full`-checked artifact.
    /// * the census-recovery publish accepts a node-published kind of `disk`
    ///   off the wire — nothing rejects it at the boundary. What filters it is
    ///   one SQL conjunct, `current_local_artifact_kind = 'full'` inside
    ///   `node_published_suspend_evidence`, which gates whether the published
    ///   kind is projected at all. Relaxing that conjunct to admit stop-origin
    ///   pins — the natural place to build the missing writer — is what will
    ///   make this selector live.
    ///
    /// (The node-local writer `publish_stopped_artifact` does mint a disk pin,
    /// but in SQLite, in a table this query never reads; the control-plane pin
    /// is a separate value.) So this selector returns `None` for every row that
    /// exists today.
    ///
    /// That is an enumeration of *current* writers, which is weaker than a
    /// schema guarantee: the artifact-identity migration has permitted
    /// `('disk', 'stop')` since it landed, so a row left by some past version
    /// is not excluded by argument. Such a row with a markerless snapshot
    /// simply selects nothing here and the restart falls back to the
    /// node-only shape, which is the safe direction.
    ///
    /// Requiring the pin is also the only thing that ties the URI to *these*
    /// bytes — `BeginArtifactBackup` accepts `(Disk, Snapshot)`, so a
    /// user-taken disk snapshot is a plausible sibling row, and a
    /// timestamp-only freshness rule would happily load it.
    pub(super) async fn selected_stopped_restore_storage_uri(
        &self,
        sandbox: &Sandbox,
    ) -> Result<Option<String>, SubstrateError> {
        let pinned = snapshot_is_current_local_artifact("s", "sn");
        // Newest-first because a retried backup of one artifact mints a fresh
        // `snapshots` row per attempt while keeping the same
        // `local_artifact_id`: the pin can match several rows, and the latest
        // registered location is the one to load. `sn.id` breaks a timestamp
        // tie so that claim is actually true — two attempts registering within
        // one statement timestamp would otherwise pick nondeterministically.
        // Same `(timestamp, id)` total order the retention store relies on for
        // the same reason, though it spells it as a row-value comparison in a
        // keep-N probe (`(newer.timestamp, newer.id) > (s.timestamp, s.id)`)
        // rather than an `ORDER BY` — don't go looking for a matching clause
        // there. The suspend sibling has no tiebreak and is not changed here:
        // its ordering is load-bearing for a guard that shields every matching
        // row, so narrowing it is a separate decision.
        // Stamped here rather than in the shared body, for the reason the
        // suspend selector's own call site gives.
        let query = sql!(format!(
            "SELECT ssl.storage_connection_string
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle = 'stopped'
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'disk'
                AND sn.state = 'ready'
                AND {pinned}
              ORDER BY sn.timestamp DESC, sn.id DESC
              LIMIT 1"
        ));
        self.fetch_restore_storage_uri(query, sandbox.id, "stopped")
            .await
    }

    pub(super) async fn finish_suspended_execution(
        &self,
        sandbox_id: SandboxId,
        expected_generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
        published_artifact: super::super::PublishedLocalArtifact,
    ) -> Result<(), SubstrateError> {
        let artifact_id = published_artifact.id.as_uuid();
        let artifact_kind = published_artifact.kind.as_str();
        let artifact_origin = ArtifactOrigin::Suspend.as_str();
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start suspended pod transaction")?;

        let lifecycle = sqlx::query_scalar::<_, String>(sql!(
            "SELECT lifecycle
              FROM sandboxes
              WHERE id = $1
                AND lifecycle IN ('suspending', 'suspended')
                AND current_local_artifact_id = $2
                AND current_local_artifact_kind = $3
                AND current_local_artifact_origin = $4
              FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(artifact_kind)
        .bind(artifact_origin)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock suspend artifact for pod retirement")?;
        let Some(lifecycle) = lifecycle else {
            return Err(SubstrateError::Unavailable {
                message: "sandbox changed before suspended pod retirement".to_string(),
            });
        };

        let execution_active = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation = $2
                AND pod_uid = $3
                AND state = $4
              FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(expected_generation.get())
        .bind(expected_pod_uid.as_str())
        .bind(ActiveExecutionState::Suspended.as_str())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock suspended Kubernetes execution")?;
        let Some(execution_active) = execution_active else {
            return Err(SubstrateError::Unavailable {
                message: "sandbox execution changed before suspended pod retirement".to_string(),
            });
        };
        let already_finalized = lifecycle == SandboxLifecycle::Suspended.as_str()
            && !execution_active
            && !sqlx::query_scalar::<_, bool>(sql!(
                "SELECT EXISTS (
                     SELECT 1
                       FROM sandbox_k8s_executions
                      WHERE sandbox_id = $1
                        AND generation > $2
                 )"
            ))
            .bind(sandbox_id)
            .bind(expected_generation.get())
            .fetch_one(&mut *tx)
            .await
            .db_context("failed to check for successor Kubernetes execution")?;
        let pending_finalization =
            lifecycle == SandboxLifecycle::Suspending.as_str() && execution_active;
        if !pending_finalization && !already_finalized {
            return Err(SubstrateError::Unavailable {
                message: "sandbox or execution changed before suspended pod retirement".to_string(),
            });
        }

        if pending_finalization {
            Self::finalize_suspended_sandbox_in(
                &mut tx,
                sandbox_id,
                artifact_id,
                artifact_kind,
                artifact_origin,
            )
            .await?;
        }

        // The claimed pod is gone once the suspend finalizes; drop the slot
        // receipt with it. The stale-claimed sweep decides ownership from
        // slot state rather than the retired compatibility projection.
        let slot_deleted =
            WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut tx, sandbox_id)
                .await
                .db_context("failed to delete claimed pool slot")?;
        if slot_deleted {
            tracing::info!(
                sandbox_id = %sandbox_id,
                path = "suspend_finalize",
                "deleted claimed pool slot inline with pod teardown"
            );
        }

        tx.commit()
            .await
            .db_context("failed to commit suspended pod transaction")?;
        Ok(())
    }

    async fn finalize_suspended_sandbox_in(
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        artifact_kind: &str,
        artifact_origin: &str,
    ) -> Result<(), SubstrateError> {
        let marker_assignments = write_current_local_artifact_marker_assignments("$2", "$3", "$4");
        let finalization = suspended_k8s_finalization_sql(
            "s",
            "candidate",
            &marker_assignments,
            SuspendedTargetNodeAssignment::Preserve,
        );
        let query = format!(
            "WITH candidate AS MATERIALIZED (
                 SELECT id
                   FROM sandboxes
                  WHERE id = $1
             ),
             {retire_active_execution_cte},
             {invalidate_open_port_exposures_cte}
             UPDATE sandboxes s
                SET {sandbox_assignments}
               FROM candidate
              WHERE s.id = candidate.id",
            retire_active_execution_cte = finalization.retire_active_execution_cte(),
            invalidate_open_port_exposures_cte = finalization.invalidate_open_port_exposures_cte(),
            sandbox_assignments = finalization.sandbox_assignments(),
        );

        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(artifact_id)
            .bind(artifact_kind)
            .bind(artifact_origin)
            .execute(&mut **tx)
            .await
            .db_context("failed to mark suspended sandbox unplaced")?;
        Ok(())
    }

    pub(super) async fn persist_or_accept_suspend_commit(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
        published_artifact: super::super::PublishedLocalArtifact,
    ) -> Result<(), SubstrateError> {
        let artifact_id = published_artifact.id.as_uuid();
        let artifact_kind = published_artifact.kind.as_str();
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes suspend commit transaction")?;
        let sandbox_exists = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id
               FROM sandboxes
              WHERE id = $1
              FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock sandbox for Kubernetes suspend commit")?
        .is_some();
        if !sandbox_exists {
            return Err(SubstrateError::Unavailable {
                message: "sandbox disappeared before its suspend commit was persisted".to_string(),
            });
        }
        // A fresh suspend publishes a newly minted local artifact id, so it
        // cannot collide with an older claimed snapshot. This fence protects
        // completion retries that replay an artifact after its snapshot row
        // has already moved to `deleting`.
        reject_deleting_current_local_artifact(&mut tx, sandbox_id, published_artifact).await?;
        let execution_updated = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET state = $4,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND pod_uid = $3
                AND state IN ($4, $5)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .bind(ActiveExecutionState::Suspended.as_str())
        .bind(ActiveExecutionState::Running.as_str())
        .execute(&mut *tx)
        .await
        .db_context("failed to mark Kubernetes execution suspend committed")?;
        if execution_updated.rows_affected() == 0 {
            let forgotten_execution_committed = sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions e
                    SET state = $4,
                        updated_at = NOW()
                   FROM sandboxes s
                  WHERE e.sandbox_id = $1
                    AND e.generation = $2
                    AND e.pod_uid = $3
                    AND NOT e.active
                    AND e.state = 'forgotten'
                    AND s.id = e.sandbox_id
                    AND s.lifecycle = 'suspending'
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions successor
                         WHERE successor.sandbox_id = e.sandbox_id
                           AND successor.generation > e.generation
                    )"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation.get())
            .bind(expected_pod_uid.as_str())
            .bind(ActiveExecutionState::Suspended.as_str())
            .execute(&mut *tx)
            .await
            .db_context("failed to commit suspend after terminal Kubernetes pod cleanup")?;
            if forgotten_execution_committed.rows_affected() == 1 {
                Self::finalize_suspended_sandbox_in(
                    &mut tx,
                    sandbox_id,
                    artifact_id,
                    artifact_kind,
                    ArtifactOrigin::Suspend.as_str(),
                )
                .await?;
                tx.commit().await.db_context(
                    "failed to commit finalized Kubernetes suspend after pod cleanup",
                )?;
                return Ok(());
            }

            let exact_projection = sqlx::query_scalar::<_, bool>(sql!(
                "SELECT EXISTS (
                     SELECT 1
                       FROM sandboxes s
                       JOIN sandbox_k8s_executions e
                         ON e.sandbox_id = s.id
                        AND e.generation = $2
                        AND e.pod_uid = $3
                      WHERE s.id = $1
                        AND s.lifecycle = 'suspended'
                        AND s.current_local_artifact_id = $4
                        AND s.current_local_artifact_kind = $5
                        AND s.current_local_artifact_origin = $6
                        AND e.state = $7
                        AND NOT e.active
                        AND NOT EXISTS (
                            SELECT 1
                              FROM sandbox_k8s_executions successor
                             WHERE successor.sandbox_id = s.id
                               AND successor.generation > e.generation
                        )
                 )"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation.get())
            .bind(expected_pod_uid.as_str())
            .bind(artifact_id)
            .bind(artifact_kind)
            .bind(ArtifactOrigin::Suspend.as_str())
            .bind(ActiveExecutionState::Suspended.as_str())
            .fetch_one(&mut *tx)
            .await
            .db_context("failed to accept projected Kubernetes suspend commit")?;
            if exact_projection {
                tx.commit()
                    .await
                    .db_context("failed to commit accepted Kubernetes suspend projection")?;
                return Ok(());
            }
            return Err(SubstrateError::Unavailable {
                message:
                    "sandbox execution changed before its suspend commit was persisted or projected"
                        .to_string(),
            });
        }
        let updated = sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'suspending',
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = $3,
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1
                AND lifecycle = 'suspending'"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(artifact_id)
        .bind(artifact_kind)
        .execute(&mut *tx)
        .await
        .db_context("failed to persist Kubernetes suspend commit")?;
        if updated.rows_affected() != 1 {
            return Err(SubstrateError::Unavailable {
                message: "sandbox execution changed before its suspend commit was persisted"
                    .to_string(),
            });
        }
        tx.commit()
            .await
            .db_context("failed to commit Kubernetes suspend transaction")?;
        Ok(())
    }

    pub(super) async fn claim_suspend_execution(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
    ) -> Result<SuspendExecutionClaim, SubstrateError> {
        for previous in [SandboxLifecycle::Started, SandboxLifecycle::Paused] {
            let claimed = sqlx::query(sql!(
                "UPDATE sandboxes s
                    SET lifecycle = 'suspending'
                   FROM sandbox_k8s_executions e
                  WHERE s.id = $1
                    AND s.lifecycle = $4
                    AND e.sandbox_id = s.id
                    AND e.generation = $2
                    AND e.active
                    AND e.pod_uid = $3
                    AND e.state = 'running'"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation.get())
            .bind(expected_pod_uid.as_str())
            .bind(previous.as_str())
            .execute(&self.pool)
            .await
            .db_context("failed to claim Kubernetes suspend execution")?;
            if claimed.rows_affected() == 1 {
                return Ok(SuspendExecutionClaim::Claimed);
            }
        }

        let existing: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
                  WHERE s.id = $1
                    AND s.lifecycle = 'suspending'
                    AND e.generation = $2
                    AND e.active
                    AND e.pod_uid = $3
                    AND e.state IN ('running', 'suspended')
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .fetch_one(&self.pool)
        .await
        .db_context("failed to verify Kubernetes suspend execution claim")?;
        if existing {
            return Ok(SuspendExecutionClaim::Existing);
        }

        match self
            .committed_suspend_artifact(sandbox_id, generation, expected_pod_uid)
            .await
        {
            Ok(Some(artifact)) => Ok(SuspendExecutionClaim::Completed(artifact)),
            Ok(None) | Err(SubstrateError::Unavailable { .. }) => {
                Err(SubstrateError::Unavailable {
                    message: "sandbox execution changed before suspend ownership was claimed"
                        .to_string(),
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Match the node's exact suspend observation to an unfinished backup.
    /// The committed sandbox artifact marker is absent or from the prior cycle
    /// until finalization. This separate evidence authorizes only waiting.
    pub(super) async fn local_suspend_backup_pending(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
                   JOIN node_facet_observed_status observed
                     ON observed.node_id = s.active_node_id
                    AND observed.facet = $4
                    AND observed.element = s.id::TEXT
                   JOIN snapshots sn
                     ON sn.sandbox_id = s.id
                    AND sn.source_node_id = observed.node_id
                    AND sn.local_artifact_id::TEXT =
                        observed.observed_payload -> $5::TEXT ->> 'local_artifact_id'
                  WHERE s.id = $1
                    AND s.lifecycle = 'started'
                    AND e.generation = $2
                    AND e.pod_uid = $3
                    AND e.active
                    AND e.state = 'recovering'
                    AND observed.observed_payload -> $5::TEXT ->> 'execution_generation' = e.generation::TEXT
                    AND observed.observed_payload -> $5::TEXT ->> 'pod_uid' = e.pod_uid
                    AND sn.type = 'full'
                    AND sn.artifact_kind = 'full'
                    AND sn.artifact_origin = 'suspend'
                    AND sn.state = 'creating'
                    AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
                    AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(SANDBOX_LOCAL_SUSPEND_KEY)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("failed to observe local suspend backup: {error}"),
        })
    }

    pub(super) async fn committed_suspend_artifact(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
    ) -> Result<Option<super::super::PublishedLocalArtifact>, SubstrateError> {
        let marker: Option<CommittedSuspendArtifactRow> = sqlx::query_as(sql!(
            "SELECT e.state AS execution_state,
                    s.current_local_artifact_id AS artifact_id,
                    s.current_local_artifact_kind AS artifact_kind,
                    s.current_local_artifact_origin AS artifact_origin
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.pod_uid = $3
              WHERE s.id = $1
                AND (
                    (s.lifecycle = 'suspending' AND e.active)
                    OR (
                        s.lifecycle = 'suspended'
                        AND e.state = 'suspended'
                        AND NOT e.active
                        AND NOT EXISTS (
                            SELECT 1
                              FROM sandbox_k8s_executions successor
                             WHERE successor.sandbox_id = s.id
                               AND successor.generation > e.generation
                        )
                    )
                )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to load committed Kubernetes suspend artifact")?;
        let Some(marker) = marker else {
            return Err(SubstrateError::Unavailable {
                message: "committed Kubernetes suspend execution changed before pod retirement"
                    .to_string(),
            });
        };
        if marker.execution_state != ActiveExecutionState::Suspended {
            return Ok(None);
        }
        match (
            marker.artifact_id,
            marker.artifact_kind,
            marker.artifact_origin,
        ) {
            (None, None, None) => Ok(None),
            (Some(artifact_id), Some(artifact_kind), Some(artifact_origin)) => {
                if artifact_origin != ArtifactOrigin::Suspend {
                    return Err(SubstrateError::Failed {
                        message: format!(
                            "committed Kubernetes suspend has {artifact_origin} artifact origin"
                        ),
                    });
                }
                Ok(Some(super::super::PublishedLocalArtifact {
                    id: artifact_id,
                    kind: artifact_kind,
                }))
            }
            _ => Err(SubstrateError::Failed {
                message: "committed Kubernetes suspend has incomplete artifact identity"
                    .to_string(),
            }),
        }
    }

    #[cfg(test)]
    pub(super) async fn finish_active_execution_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start active execution transaction")?;
        let finished = self
            .finish_active_execution_generation_in(&mut tx, sandbox_id, generation, state)
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit active execution transaction")?;
        Ok(finished)
    }

    pub(super) async fn abandon_created_unmaterialized_create_claim(
        &self,
        sandbox_id: SandboxId,
        claim: &SandboxExecutionClaim,
    ) -> Result<bool, SubstrateError> {
        if !claim.created {
            return Ok(false);
        }
        self.abandon_unmaterialized_create_claim(sandbox_id, claim)
            .await
    }

    /// Retire an unmaterialized `creating` claim regardless of whether this
    /// request created it. Used by the cross-node re-drive failure path: the
    /// pinned pod has already been deleted, so a pre-existing `creating` claim
    /// (`claim.created == false` on a retry) no longer has anything in flight,
    /// and leaving it would send every subsequent ensure call down the
    /// create-in-flight early return until the stale-create age expires. The
    /// UPDATE CASes on `creating + pod_uid IS NULL + pod_ip IS NULL`, so a row
    /// a concurrent attempt already materialized is never touched.
    pub(super) async fn abandon_unmaterialized_create_claim(
        &self,
        sandbox_id: SandboxId,
        claim: &SandboxExecutionClaim,
    ) -> Result<bool, SubstrateError> {
        // The attempt is always fenced. The restore lease is intentionally
        // unfenced (`None`) here because these in-request handlers run while
        // their owner still holds it.
        self.abandon_unmaterialized_create_claim_by_generation(
            sandbox_id,
            claim.row.generation,
            None,
            claim.row.activation_attempt_id,
        )
        .await
    }

    /// [`Self::abandon_unmaterialized_create_claim`] keyed by generation
    /// directly — for the re-drive cancellation guard, which fires from a
    /// `Drop` impl and cannot borrow the claim.
    ///
    /// The activation-attempt identity fences retries that reuse a generation.
    /// `restore_lease_token` additionally ownership-fences restore takeover
    /// (with the same `None`-is-unguarded contract as
    /// [`Self::revert_lifecycle_after_rollback_best_effort`]). A dropped leader
    /// can outlive its lease; once a follower adopts the still-`creating`
    /// generation, either changed fence makes the delayed retirement a no-op.
    ///
    /// The token is read after locking the sandbox row `FOR UPDATE`, following
    /// the sandbox -> execution lock order the rest of this store uses. The
    /// lease takeover in [`Self::begin_restore_lifecycle`] locks this same row
    /// `FOR UPDATE` before overwriting `restore_lease_token`, so taking the lock
    /// here serializes A's cleanup against B's takeover: either A reads the row
    /// first and retires its own still-`creating` generation, or A blocks until
    /// B commits, then reads B's token and no-ops. An unlocked read (the token
    /// as a subquery inside the retirement UPDATE) instead lets A retire B's
    /// adopted claim on a stale READ COMMITTED snapshot while B's own
    /// materialization races on a different row and affects zero rows.
    pub(super) async fn abandon_unmaterialized_create_claim_by_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        restore_lease_token: Option<Uuid>,
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes create claim transaction")?;
        if !Self::locked_lease_owner_matches(&mut tx, sandbox_id, restore_lease_token).await? {
            tx.commit()
                .await
                .db_context("failed to commit Kubernetes create claim transaction")?;
            return Ok(false);
        }
        let abandoned = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = 'replaced',
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND generation = $2
               AND activation_attempt_id IS NOT DISTINCT FROM $3
               AND state = 'creating'
               AND pod_uid IS NULL
               AND pod_ip IS NULL"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&mut *tx)
        .await
        .db_context("failed to abandon Kubernetes create claim")?
        .rows_affected()
            > 0;
        if abandoned {
            self.invalidate_open_port_exposure_observations_in(&mut tx, sandbox_id)
                .await?;
        }
        tx.commit()
            .await
            .db_context("failed to commit Kubernetes create claim transaction")?;
        Ok(abandoned)
    }

    /// Lock the sandbox row `FOR UPDATE` and report whether `restore_lease_token`
    /// still owns the restore lease. `None` is the unfenced in-request contract
    /// (owner alive, lease held) and always matches. Locking here — instead of
    /// reading the token in an unlocked subquery — serializes the re-drive
    /// cancellation cleanup against the `FOR UPDATE` lease takeover in
    /// [`Self::begin_restore_lifecycle`], following the sandbox -> execution
    /// lock order: a lapsed leader either wins the lock and cleans its own
    /// generation, or blocks until the follower commits and then reads the
    /// follower's token and no-ops.
    async fn locked_lease_owner_matches(
        tx: &mut PgConnection,
        sandbox_id: SandboxId,
        restore_lease_token: Option<Uuid>,
    ) -> Result<bool, SubstrateError> {
        let current_token = sqlx::query_scalar::<_, Option<Uuid>>(sql!(
            "SELECT restore_lease_token FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock sandbox for create claim cleanup")?
        .flatten();
        Ok(match restore_lease_token {
            Some(expected) => current_token == Some(expected),
            None => true,
        })
    }

    /// Reset an active generation's pod tracking back to unmaterialized
    /// (`creating`, null UID/IP) after the cross-node re-drive confirmed the
    /// previously created source-pinned pod is deleted. That pod's UID is
    /// persisted before the scheduler's verdict is known, so an `Unschedulable`
    /// pin leaves the row `created` with `pod_ip` NULL even though the pod is
    /// now gone. Left as-is, the re-drive cancellation guard's `creating`-scoped
    /// retirement can't match, so a client disconnect mid-re-drive would strand
    /// the cross-node successor as an orphan VM once the transient-lifecycle
    /// sweeper reverts the sandbox to a dormant lifecycle. Resetting to
    /// `creating` restores the invariant the guard and the re-drive failure
    /// handler both rely on.
    ///
    /// Attempt- and lease-fenced (same lease contract as
    /// [`Self::abandon_unmaterialized_create_claim_by_generation`]) and scoped
    /// to a not-yet-routable row (`pod_ip IS NULL`), so a successor another
    /// attempt already materialized to a running pod is never clobbered.
    pub(super) async fn reset_active_execution_to_creating_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        restore_lease_token: Option<Uuid>,
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start create claim reset transaction")?;
        if !Self::locked_lease_owner_matches(&mut tx, sandbox_id, restore_lease_token).await? {
            tx.commit()
                .await
                .db_context("failed to commit create claim reset transaction")?;
            return Ok(false);
        }
        let reset = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = NULL,
                 pod_ip = NULL,
                 state = 'creating',
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND generation = $2
               AND activation_attempt_id IS NOT DISTINCT FROM $3
               AND pod_ip IS NULL
               AND state IN ('creating', 'created')"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&mut *tx)
        .await
        .db_context("failed to reset create claim to creating")?
        .rows_affected()
            > 0;
        tx.commit()
            .await
            .db_context("failed to commit create claim reset transaction")?;
        Ok(reset)
    }

    /// Best-effort lifecycle revert for the restore/wake rollback paths in
    /// [`super::execution`]. Reverts the row to `lifecycle` (in practice
    /// `"suspended"`) when a Kubernetes restore or wake fails mid-flight.
    /// CAS-guarded against `('failed', 'quarantined', 'deleted', 'cleanup_required')` so
    /// a rollback can't overwrite a terminal lifecycle the
    /// transient-lifecycle sweeper just claimed (the sweeper decided the
    /// operation was wedged; the rollback's "revert to my last known
    /// state" intent is wrong once the row has been retired).
    ///
    /// **Returns Ok regardless of whether the write applied.** The
    /// rollback is best-effort by design — `failed` is the correct
    /// resting state for a swept row, and callers don't act on the
    /// no-write case. The function name encodes this contract; do
    /// **not** call this from paths that depend on "the row is now in
    /// `lifecycle` after this returns." Use a dedicated CAS-guarded
    /// UPDATE inline if you need to detect race-loss (see the
    /// post-restore / post-wake CAS in
    /// `crates/libs/api/src/controllers/sandbox/lifecycle.rs` for the
    /// pattern).
    /// Best-effort revert of a sandbox's lifecycle (used by the restore
    /// rollback path to put a failed restore back to `suspended`).
    ///
    /// `restore_lease_token` token-guards the revert: when `Some`, the UPDATE
    /// only matches if the row still holds that exact lease token. This closes
    /// a stale-rollback race the restore lease introduced — a fresh attempt's
    /// clean-failure rollback could be delayed past the lease TTL, by which
    /// point a retry has claimed a *new* token and is actively restoring;
    /// without the guard the stale rollback would flip the newer restore back
    /// to `suspended` (and the clear-trigger would drop the newer lease),
    /// orphaning live work. `None` skips the guard (evacuation reverts, which
    /// are marker-gated and tokenless, and test-only lifecycle setup).
    pub(super) async fn revert_lifecycle_after_rollback_best_effort(
        &self,
        sandbox_id: SandboxId,
        lifecycle: &str,
        restore_lease_token: Option<Uuid>,
    ) -> Result<(), SubstrateError> {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = $1
              WHERE id = $2
                AND lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')
                AND ($3::uuid IS NULL OR restore_lease_token = $3)"
        ))
        .bind(lifecycle)
        .bind(sandbox_id)
        .bind(restore_lease_token)
        .execute(&self.pool)
        .await
        .db_context_with(|| format!("failed to revert sandbox lifecycle to {lifecycle}"))?;
        Ok(())
    }

    pub(super) async fn begin_restore_lifecycle(
        &self,
        sandbox_id: SandboxId,
        require_evacuation_marker: bool,
    ) -> Result<RestoreClaim, SubstrateError> {
        // `require_evacuation_marker` makes the restore claim atomic on the
        // node-drain `evacuation_restore_target` marker. The cluster-
        // controller's deferred cohort restore passes `true`: between listing
        // a restore-pending sandbox and this claim, a concurrent user restore
        // (allowed once the snapshot is portable) can clear the marker.
        // Folding the marker into the claim statement means the drain restore
        // loses the CAS in that race instead of restoring a sandbox the user
        // took over. The user restore path passes `false` — it must restore
        // regardless of the marker, and it is what clears it.
        //
        // Both claims accept an already-`restoring` row, but on different
        // conditions. The evacuation claim accepts a `restoring` row with the
        // marker still set — crash recovery after the controller dies between
        // this claim and finalize. The user claim accepts a markerless
        // `restoring` row only when no live restore lease exists: an expired
        // lease means a prior user attempt timed out and retry may re-drive,
        // while an unexpired lease means a restore is still active and must be
        // reported as retryable rather than driven twice. A `restoring` row
        // that still carries the marker is a drain restore in flight, and the
        // user path must NOT grab it. (A `suspended` row with a marker is still
        // a clean user takeover: the user wins `suspended → restoring`, clears
        // the marker, and the drain's marker-required claim then loses.)
        let restore_lease_token = (!require_evacuation_marker).then(Uuid::now_v7);
        let restore_claim = if require_evacuation_marker {
            sqlx::query_as::<_, RestoreClaimRow>(sql!(
                "WITH candidate AS (
                     SELECT id, lifecycle
                       FROM sandboxes
                      WHERE id = $1
                        AND LOWER(TRIM(lifecycle)) IN ('suspended', 'restoring')
                        AND evacuation_restore_target IS NOT NULL
                      FOR UPDATE
                 ),
                 updated AS (
                     UPDATE sandboxes s
                        SET lifecycle = 'restoring'
                       FROM candidate c
                     WHERE s.id = c.id
                      RETURNING c.lifecycle
                 )
                 SELECT u.lifecycle, args.restore_lease_token
                   FROM updated u
                   CROSS JOIN (
                     SELECT $2::bigint AS ttl_secs, $3::uuid AS restore_lease_token
                   ) args
                  WHERE args.ttl_secs >= 0"
            ))
        } else {
            // Preserve `lifecycle_changed_at` across `restoring -> restoring`
            // re-drives: restore selection uses it as the immutable boundary
            // between the original source window and artifacts published by a
            // failed restore branch. The lifecycle trigger still sets it for a
            // fresh `suspended -> restoring` claim. The restore lease protects
            // an active re-drive from the transient-lifecycle sweeper.
            sqlx::query_as::<_, RestoreClaimRow>(sql!(
                "WITH candidate AS (
                     SELECT id, lifecycle
                       FROM sandboxes
                      WHERE id = $1
                        AND (
                          LOWER(TRIM(lifecycle)) = 'suspended'
                          OR (
                            LOWER(TRIM(lifecycle)) = 'restoring'
                            AND evacuation_restore_target IS NULL
                            AND (
                              restore_lease_expires_at IS NULL
                              OR restore_lease_expires_at <= NOW()
                            )
                          )
                        )
                        AND (
                          restore_lease_expires_at IS NULL
                          OR restore_lease_expires_at <= NOW()
                        )
                        AND NOT EXISTS (
                          SELECT 1
                            FROM sandbox_k8s_executions e
                           WHERE e.sandbox_id = sandboxes.id
                             AND e.active
                             AND e.state IN ('creating', 'created', 'recovering', 'running')
                             AND e.updated_at >
                                 NOW() - make_interval(secs => $4)
                        )
                      FOR UPDATE
                 ),
                 updated AS (
                     UPDATE sandboxes s
                        SET lifecycle = 'restoring',
                            evacuation_restore_target = NULL,
                            restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                            restore_lease_token = $3
                       FROM candidate c
                      WHERE s.id = c.id
                      RETURNING c.lifecycle
                 )
                 SELECT lifecycle, $3::uuid AS restore_lease_token FROM updated"
            ))
        }
        .bind(sandbox_id.as_uuid())
        .bind(recovery_lease_ttl_secs())
        .bind(restore_lease_token)
        .bind(super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to begin sandbox restore")?;

        let Some(restore_claim) = restore_claim else {
            let (lifecycle, marker, ownership_remaining_secs) =
                self.fetch_restore_ownership_snapshot(sandbox_id).await?;
            // When the marker was required and the row is still in the
            // suspend/restore window, the CAS lost to a concurrent user
            // restore that cleared the marker — surface that distinctly so the
            // drain re-drive treats it as "no longer mine; drop it" rather
            // than a generic failure.
            if require_evacuation_marker
                && matches!(
                    lifecycle,
                    SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
                )
            {
                return Err(SubstrateError::EvacuationClaimLost {
                    message: format!(
                        "evacuation restore claim lost for sandbox {sandbox_id}: \
                         evacuation_restore_target cleared (user restore took over)"
                    ),
                });
            }
            if !require_evacuation_marker
                && marker.is_none()
                && ownership_remaining_secs > 0.0
                && matches!(
                    lifecycle,
                    SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
                )
            {
                return Err(Self::restore_ownership_collision(
                    sandbox_id,
                    ownership_remaining_secs,
                ));
            }
            return Err(SubstrateError::Conflict {
                message: format!("sandbox cannot be restored while lifecycle is {lifecycle}"),
            });
        };

        match restore_claim.lifecycle.trim().to_ascii_lowercase().as_str() {
            "suspended" => Ok(RestoreClaim::FreshFromSuspended {
                restore_lease_token: restore_claim.restore_lease_token,
            }),
            "restoring" => Ok(RestoreClaim::RedroveRestoring {
                restore_lease_token: restore_claim.restore_lease_token,
            }),
            lifecycle => Err(SubstrateError::Failed {
                message: format!("restore claim returned unexpected lifecycle {lifecycle}"),
            }),
        }
    }

    async fn claim_markerless_restoring_restore_lease(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RestoreClaim>, SubstrateError> {
        let restore_lease_token = Uuid::now_v7();
        let restore_claim = sqlx::query_as::<_, RestoreClaimRow>(sql!(
            "WITH candidate AS (
                 SELECT id, lifecycle
                   FROM sandboxes
                  WHERE id = $1
                    AND LOWER(TRIM(lifecycle)) = 'restoring'
                    AND evacuation_restore_target IS NULL
                    AND (
                      restore_lease_expires_at IS NULL
                      OR restore_lease_expires_at <= NOW()
                    )
                    AND NOT EXISTS (
                      SELECT 1
                        FROM sandbox_k8s_executions e
                       WHERE e.sandbox_id = sandboxes.id
                         AND e.active
                         AND e.state IN ('creating', 'created', 'recovering', 'running')
                         AND e.updated_at > NOW() - make_interval(secs => $4)
                    )
                  FOR UPDATE
             ),
             updated AS (
                 UPDATE sandboxes s
                    SET restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                        restore_lease_token = $3
                   FROM candidate c
                  WHERE s.id = c.id
                  RETURNING c.lifecycle
             )
             SELECT lifecycle, $3::uuid AS restore_lease_token FROM updated"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(recovery_lease_ttl_secs())
        .bind(restore_lease_token)
        .bind(super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to claim restoring sandbox restore lease")?;

        if let Some(restore_claim) = restore_claim {
            return match restore_claim.lifecycle.trim().to_ascii_lowercase().as_str() {
                "restoring" => Ok(Some(RestoreClaim::RedroveRestoring {
                    restore_lease_token: restore_claim.restore_lease_token,
                })),
                lifecycle => Err(SubstrateError::Failed {
                    message: format!(
                        "restore lease claim returned unexpected lifecycle {lifecycle}"
                    ),
                }),
            };
        }

        let (lifecycle, marker, ownership_remaining_secs) =
            self.fetch_restore_ownership_snapshot(sandbox_id).await?;

        if lifecycle == SandboxLifecycle::Restoring
            && marker.is_none()
            && ownership_remaining_secs > 0.0
        {
            return Err(Self::restore_ownership_collision(
                sandbox_id,
                ownership_remaining_secs,
            ));
        }

        if lifecycle == SandboxLifecycle::Restoring && marker.is_some() {
            return Ok(None);
        }

        Err(SubstrateError::Conflict {
            message: format!(
                "sandbox {sandbox_id} changed lifecycle while claiming its restore lease"
            ),
        })
    }

    pub(super) async fn release_user_restore_lease(
        &self,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Result<(), SubstrateError> {
        // Expire ownership so a clean pre-work failure can retry immediately,
        // but retain the paired token and attempt clock at NOW(). The transient
        // sweeper requires a full quiet window after that clock before changing
        // `restoring`, while the next retry atomically replaces the stale pair.
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW()
              WHERE id = $1
                AND restore_lease_token = $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(restore_lease_token)
        .execute(&self.pool)
        .await
        .db_context("failed to release sandbox restore lease")?;
        Ok(())
    }

    pub(super) async fn prepare_wake_sandbox(
        &self,
        sandbox: &Sandbox,
        sandbox_id: SandboxId,
        preclaimed_restore_claim: Option<RestoreClaim>,
    ) -> Result<PreparedWakeSandbox, SubstrateError> {
        if super::is_suspended_lifecycle(&sandbox.lifecycle) {
            // Reached only by the user wake entry (the evacuation restore
            // already flipped the row to `restoring` before this point), so
            // the marker is not required for this claim.
            let restore_claim = match preclaimed_restore_claim {
                Some(restore_claim) => restore_claim,
                None => self.begin_restore_lifecycle(sandbox_id, false).await?,
            };
            return Ok(PreparedWakeSandbox {
                sandbox: self.require_sandbox(sandbox_id).await?,
                restore_started: true,
                restore_claim: Some(restore_claim),
            });
        }

        if sandbox.lifecycle.trim().eq_ignore_ascii_case("restoring") {
            // Markerless user restores must acquire the lease before a retry
            // can re-drive. Marker-set evacuation restores keep the preexisting
            // bounded takeover behavior; the marker owner still controls them.
            let restore_claim = match preclaimed_restore_claim {
                Some(restore_claim) => Some(restore_claim),
                None => {
                    self.claim_markerless_restoring_restore_lease(sandbox_id)
                        .await?
                }
            };
            return Ok(PreparedWakeSandbox {
                sandbox: sandbox.clone(),
                restore_started: false,
                restore_claim,
            });
        }

        Ok(PreparedWakeSandbox {
            sandbox: sandbox.clone(),
            restore_started: false,
            restore_claim: None,
        })
    }

    /// A pod that dies while its sandbox is `suspending` belongs to the
    /// suspend flow: finalization retires the execution as `suspended`, and
    /// the transient-lifecycle sweeper ages out suspends that never finish.
    /// Forgetting the execution here instead clears `active_node_id`, which
    /// makes the publish ingest drop the node's final `suspended` record —
    /// the projector requires an actively-placed publisher — and strands
    /// the row in `suspending`. Read inside the caller's transaction, after
    /// the per-sandbox lock, so the answer cannot race a lifecycle change.
    async fn suspend_owns_pod_teardown_in(
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        let suspending: Option<bool> = sqlx::query_scalar(sql!(
            "SELECT LOWER(TRIM(lifecycle)) = 'suspending' FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut **tx)
        .await
        .db_context("failed to read sandbox lifecycle for pod tracking cleanup")?;
        Ok(suspending.unwrap_or(false))
    }

    pub(super) async fn finalize_missing_suspending_pod_with_current_snapshot(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: &str,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start missing suspend pod transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }

        let eligibility = missing_suspending_pod_revert_eligibility("s");
        let clear_marker = eligibility.clear_marker;
        let eligible = eligibility.eligible;
        let query = format!(
            "SELECT {clear_marker}
               FROM sandboxes s
              WHERE s.id = $1
                AND LOWER(TRIM(s.lifecycle)) = 'suspending'
                AND s.active_node_id IS NOT NULL
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.generation = $2
                       AND e.pod_uid = $3
                       AND e.active
                       AND e.state IN ('running', 'suspended')
                )
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions successor
                     WHERE successor.sandbox_id = s.id
                       AND successor.generation > $2
                )
                AND {eligible}",
        );
        let clear_marker = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(generation)
            .bind(pod_uid)
            .fetch_optional(&mut *tx)
            .await
            .db_context("failed to select missing suspended pod finalization")?;
        let Some(clear_marker) = clear_marker else {
            return Ok(false);
        };

        self.finish_active_execution_in(&mut tx, sandbox_id, SandboxLifecycle::Suspended.as_str())
            .await?;

        let marker_assignments = clear_current_local_artifact_marker_assignments("s", "$2");
        let sandbox_assignments = suspended_k8s_finalization_sql(
            "s",
            "candidate",
            &marker_assignments,
            SuspendedTargetNodeAssignment::Preserve,
        )
        .sandbox_assignments();
        let query = format!("UPDATE sandboxes s SET {sandbox_assignments} WHERE s.id = $1");
        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(clear_marker)
            .execute(&mut *tx)
            .await
            .db_context("failed to finalize missing suspended pod")?;
        WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut tx, sandbox_id)
            .await
            .db_context("failed to delete claimed pool slot")?;
        tx.commit()
            .await
            .db_context("failed to commit missing suspend pod transaction")?;
        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::SuspendCompleted,
            SandboxEventSource::Substrate,
        )
        .emit_with(
            SandboxEventDetails::default()
                .reason("missing_pod_current_snapshot")
                .execution_generation(generation.to_string()),
        );
        Ok(true)
    }

    pub(super) async fn forget_pod_tracking(
        &self,
        sandbox_id: SandboxId,
        placement_clock: PlacementClock,
    ) -> Result<(), SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(());
        }

        self.finish_active_execution_in(&mut tx, sandbox_id, "forgotten")
            .await?;
        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        let recovered_started = self
            .recover_started_after_suspend_cleanup_in(&mut tx, sandbox_id)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        if recovered_started {
            tracing::info!(
                sandbox_id = %sandbox_id,
                ?placement_clock,
                "recovered started sandbox to suspended during pod tracking cleanup"
            );
            emit_started_suspend_recovery_event(
                sandbox_id,
                StartedSuspendRecoveryOperation::PodTrackingCleanup,
            );
        }
        Ok(())
    }

    pub(super) async fn forget_pod_tracking_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        cause: PodTrackingCleanupCause,
    ) -> Result<bool, SubstrateError> {
        self.finish_execution_and_clear_pod_tracking_if_generation(
            sandbox_id,
            generation,
            None,
            "forgotten",
            StartedSuspendRecoveryOperation::TerminalPodCleanup,
            cause,
            PlacementClock::Bump,
        )
        .await
    }

    /// Clear pod tracking for a sandbox that has **no** active execution,
    /// proving that inside the same locked transaction.
    ///
    /// A caller that observed no execution has no generation to CAS on, so
    /// this is what makes its teardown safe: an execution created between the
    /// caller's observation and here makes the write claim nothing, and the
    /// caller reports the supersession instead of clearing a row it never saw.
    /// The sandbox lock is the same one every multi-row execution mutation
    /// takes, so the check and the clear cannot straddle a concurrent create.
    pub(super) async fn forget_pod_tracking_if_no_active_execution(
        &self,
        sandbox_id: SandboxId,
        placement_clock: PlacementClock,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(true);
        }
        let active_exists: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active
             )"
        ))
        .bind(sandbox_id)
        .fetch_one(&mut *tx)
        .await
        .db_context("failed to re-check execution absence")?;
        if active_exists {
            return Ok(false);
        }

        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        Ok(true)
    }

    /// Fill in a `pod_uid` that was never persisted, and nothing else.
    ///
    /// [`Self::persist_pod_metadata`] assigns rather than patches: it sets
    /// `pod_ip` to whatever it is handed and recomputes `state` from it, so
    /// calling it with a UID alone clears a live pod's IP and downgrades a
    /// `running` execution to `created`. This exists because the fence's
    /// backfill has exactly one field to contribute and must not touch a row
    /// the create path may have completed in the meantime — hence
    /// `pod_uid IS NULL`, which also makes it unable to overwrite a newer UID.
    /// The attempt guard is there for the same reason the release CAS has one:
    /// a replacement can reuse the generation with a NULL UID, and writing the
    /// old pod's identity onto it would be worse than writing nothing.
    pub(super) async fn backfill_missing_pod_uid(
        &self,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        pod_uid: &str,
    ) -> Result<bool, SubstrateError> {
        let (generation, activation_attempt_id, _, route_activation_id) =
            Self::identity_binds(identity);
        let filled = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET pod_uid = $4, updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation IS NOT DISTINCT FROM $2
                AND active
                AND activation_attempt_id IS NOT DISTINCT FROM $3
                AND NULLIF(BTRIM(pod_uid), '') IS NULL
                AND route_activation_id IS NOT DISTINCT FROM $5"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .bind(pod_uid)
        .bind(route_activation_id)
        .execute(&self.pool)
        .await
        .db_context("failed to backfill pod uid")?;
        Ok(filled.rows_affected() > 0)
    }

    /// [`Self::forget_pod_tracking`] for a pod-identity-fenced teardown:
    /// generation-guarded, so a successor execution installed while the
    /// teardown was running keeps its tracking, and returning `false` instead
    /// of silently retiring it. Takes the `placement_clock` the caller's
    /// teardown mode requires — a retire must
    /// [`Preserve`](PlacementClock::Preserve) it (see that type's docs), which
    /// is the one thing [`Self::forget_pod_tracking_if_generation`] cannot
    /// express.
    pub(super) async fn forget_fenced_pod_tracking(
        &self,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        placement_clock: PlacementClock,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }
        if !self
            .finish_fenced_execution_in(&mut tx, sandbox_id, identity, "forgotten")
            .await?
        {
            return Ok(false);
        }
        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        let recovered_started = self
            .recover_started_after_suspend_cleanup_in(&mut tx, sandbox_id)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        if recovered_started {
            emit_started_suspend_recovery_event(
                sandbox_id,
                StartedSuspendRecoveryOperation::PodTrackingCleanup,
            );
        }
        Ok(true)
    }

    pub(super) async fn converge_failed_recovery_after_pod_absence_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_pod_uid: Option<&str>,
        expected_pod_ip: Option<&str>,
        expected_k8s_node_name: Option<&str>,
    ) -> Result<FailedRecoveryPodAbsenceOutcome, SubstrateError> {
        let generation =
            ExecutionGeneration::new(generation).map_err(|error| SubstrateError::Failed {
                message: format!("invalid failed recovery generation: {error}"),
            })?;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start failed recovery pod-absence transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(FailedRecoveryPodAbsenceOutcome::NotMatched);
        }

        let cleared = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = NULL,
                 pod_ip = NULL,
                 k8s_node_name = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND generation = $2
               AND active
               AND state = $3
               AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $4
               AND pod_ip IS NOT DISTINCT FROM $5::INET
               AND k8s_node_name IS NOT DISTINCT FROM $6"
        ))
        .bind(sandbox_id)
        .bind(generation.get())
        .bind(ActiveExecutionState::RecoveryFailed.as_str())
        .bind(expected_pod_uid)
        .bind(expected_pod_ip)
        .bind(expected_k8s_node_name)
        .execute(&mut *tx)
        .await
        .db_context("failed to clear missing recovery pod metadata")?
        .rows_affected()
            > 0;
        if !cleared {
            tx.commit()
                .await
                .db_context("failed to commit unmatched pod-absence transaction")?;
            return Ok(FailedRecoveryPodAbsenceOutcome::NotMatched);
        }

        let recovered = self
            .recover_started_after_failed_recovery_in(&mut tx, sandbox_id, generation)
            .await?;
        if recovered
            && !self
                .finish_active_execution_generation_in(
                    &mut tx,
                    sandbox_id,
                    generation.get(),
                    ActiveExecutionState::RecoveryFailed.as_str(),
                )
                .await?
        {
            return Err(SubstrateError::Failed {
                message: "failed to retire pod-absent converged recovery execution".to_string(),
            });
        }

        tx.commit()
            .await
            .db_context("failed to commit failed recovery pod absence")?;
        if recovered {
            emit_started_suspend_recovery_event(
                sandbox_id,
                StartedSuspendRecoveryOperation::StartedRecoveryFailure,
            );
            Ok(FailedRecoveryPodAbsenceOutcome::ConvergedSuspended)
        } else {
            Ok(FailedRecoveryPodAbsenceOutcome::ClaimPreserved)
        }
    }

    pub(super) async fn forget_pod_tracking_if_generation_and_attempt(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<bool, SubstrateError> {
        self.finish_execution_and_clear_pod_tracking_if_generation(
            sandbox_id,
            generation,
            Some(activation_attempt_id),
            "forgotten",
            StartedSuspendRecoveryOperation::TerminalPodCleanup,
            PodTrackingCleanupCause::ExplicitTeardown,
            PlacementClock::Bump,
        )
        .await
    }

    pub(super) async fn cleanup_replaced_pod_tracking_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<bool, SubstrateError> {
        self.finish_execution_and_clear_pod_tracking_if_generation(
            sandbox_id,
            generation,
            None,
            "replaced",
            StartedSuspendRecoveryOperation::StalePodReplacementCleanup,
            PodTrackingCleanupCause::ExplicitTeardown,
            PlacementClock::Bump,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_execution_and_clear_pod_tracking_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
        state: &str,
        recovery_operation: StartedSuspendRecoveryOperation,
        cause: PodTrackingCleanupCause,
        placement_clock: PlacementClock,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }

        if cause == PodTrackingCleanupCause::MissingPodObservation
            && state == "forgotten"
            && Self::suspend_owns_pod_teardown_in(&mut tx, sandbox_id).await?
        {
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation,
                "deferring generation-scoped pod tracking cleanup: suspend finalization owns \
                 this pod teardown"
            );
            return Ok(false);
        }

        let finished = if let Some(activation_attempt_id) = activation_attempt_id {
            self.finish_active_execution_generation_attempt_in(
                &mut tx,
                sandbox_id,
                generation,
                activation_attempt_id,
                state,
            )
            .await?
        } else {
            self.finish_active_execution_generation_in(&mut tx, sandbox_id, generation, state)
                .await?
        };
        if !finished {
            tx.commit()
                .await
                .db_context("failed to commit pod tracking cleanup transaction")?;
            return Ok(false);
        }

        // Create-cleanup abandonment is delete-shaped (a never-succeeded pod)
        // and passes `Bump`. A fenced retire passes `Preserve` for the reason
        // [`PlacementClock`] documents.
        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        // Recovery relies on the active execution being retired in this same
        // transaction before the started-row check runs.
        let recovered_started = self
            .recover_started_after_suspend_cleanup_in(&mut tx, sandbox_id)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        if recovered_started {
            tracing::info!(
                sandbox_id = %sandbox_id,
                "recovered started sandbox to suspended during generation-scoped pod tracking cleanup"
            );
            emit_started_suspend_recovery_event(sandbox_id, recovery_operation);
        }
        Ok(true)
    }

    async fn clear_pod_tracking_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        placement_clock: PlacementClock,
    ) -> Result<(), SubstrateError> {
        // A pool-claimed sandbox leaves its `claimed` slot row behind after
        // bind. Retiring it here with the execution keeps the stale-claimed
        // sweep's working set bounded by live capacity instead of delete
        // churn. No-op for non-pooled sandboxes and for `cleanup_pending`
        // slots (the failed-boot path still owns those).
        let slot_deleted = WarmPoolStore::delete_claimed_slot_for_retired_execution(tx, sandbox_id)
            .await
            .db_context("failed to delete claimed pool slot")?;
        if slot_deleted {
            tracing::info!(
                sandbox_id = %sandbox_id,
                path = "pod_tracking_clear",
                "deleted claimed pool slot inline with pod teardown"
            );
        }

        // The placement-clock bump is conditional: a recoverability-preserving
        // retire keeps `placement_changed_at` so the legacy snapshot-freshness
        // gate doesn't strand a markerless row's ready snapshot. See
        // [`PlacementClock`].
        let query = match placement_clock {
            PlacementClock::Bump => sqlx::query(sql!(
                "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL,
                     placement_changed_at = NOW()
                 WHERE id = $1"
            )),
            PlacementClock::Preserve => sqlx::query(sql!(
                "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL
                 WHERE id = $1"
            )),
        };
        query
            .bind(sandbox_id)
            .execute(&mut **tx)
            .await
            .db_context("failed to mark sandbox unplaced")?;
        Ok(())
    }

    async fn recover_markerless_restoring_after_execution_cleanup_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        // Callers hold the sandbox row lock and retire the authoritative
        // execution earlier in this transaction. Execution creation takes the
        // same lock, so a successor cannot appear between this proof and commit.
        let safe_revert_snapshot = restore_retry_suspended_revert_snapshot_exists("s");
        let query = format!(
            "UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    current_local_artifact_id = NULL,
                    current_local_artifact_kind = NULL,
                    current_local_artifact_origin = NULL,
                    command_ready_pending = FALSE,
                    restore_lease_expires_at = NULL,
                    restore_lease_token = NULL
              WHERE s.id = $1
                AND s.lifecycle = 'restoring'
                AND s.evacuation_restore_target IS NULL
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                )
                AND {safe_revert_snapshot}"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .execute(&mut **tx)
            .await
            .db_context("failed to recover markerless restoring sandbox after execution cleanup")?;
        Ok(())
    }

    async fn recover_started_after_suspend_cleanup_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        self.recover_started_with_current_suspend_snapshot_in(
            tx,
            sandbox_id,
            StartedSuspendRecoveryProof::ExecutionRetired,
        )
        .await
    }

    async fn recover_started_after_failed_recovery_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> Result<bool, SubstrateError> {
        self.recover_started_with_current_suspend_snapshot_in(
            tx,
            sandbox_id,
            StartedSuspendRecoveryProof::FailedRecovery { generation },
        )
        .await
    }

    async fn recover_started_with_current_suspend_snapshot_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        proof: StartedSuspendRecoveryProof,
    ) -> Result<bool, SubstrateError> {
        let (generation, execution_proof) = match proof {
            StartedSuspendRecoveryProof::ExecutionRetired => (
                None,
                "NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                 )
                 AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.pod_ip IS NOT NULL
                 )",
            ),
            StartedSuspendRecoveryProof::FailedRecovery { generation } => (
                Some(generation.get()),
                "EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.generation = args.generation
                       AND e.active
                       AND e.state = 'recovery_failed'
                       AND e.pod_uid IS NULL
                       AND e.pod_ip IS NULL
                       AND e.k8s_node_name IS NULL
                 )
                 AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions newer
                     WHERE newer.sandbox_id = s.id
                       AND newer.active
                       AND newer.generation > args.generation
                 )",
            ),
        };
        let eligibility = match proof {
            StartedSuspendRecoveryProof::ExecutionRetired => {
                started_suspend_revert_eligibility("s")
            }
            StartedSuspendRecoveryProof::FailedRecovery { .. } => {
                failed_repair_revert_eligibility("s")
            }
        };
        let clear_marker = eligibility.clear_marker;
        let eligible = eligibility.eligible;
        let artifact_marker_assignments =
            clear_current_local_artifact_marker_assignments("s", "pre.clear_marker");
        let query = format!(
            "WITH args AS (
                 SELECT $2::BIGINT AS generation
             ),
             pre AS (
                 SELECT id,
                        {clear_marker} AS clear_marker
                   FROM sandboxes s
                  WHERE s.id = $1
             )
             UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    command_ready_pending = FALSE,
                    {artifact_marker_assignments}
               FROM pre, args
              WHERE s.id = pre.id
                AND s.lifecycle = 'started'
                AND s.placement = 'unplaced'
                AND s.active_node_id IS NULL
                AND {execution_proof}
                AND {eligible}"
        );
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(generation)
            .execute(&mut **tx)
            .await
            .db_context("failed to recover started sandbox with current suspend snapshot")?;
        Ok(result.rows_affected() > 0)
    }

    pub(super) async fn active_execution_is_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT EXISTS(
                 SELECT 1
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1
                    AND generation = $2
                    AND active
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .fetch_one(&self.pool)
        .await
        .db_context("failed to check active Kubernetes execution generation")
    }

    pub(super) async fn active_execution_status(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<ActiveExecutionStatusRow>, SubstrateError> {
        sqlx::query_as(sql!(
            "SELECT e.generation, e.pod_name, NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
                    host(e.pod_ip) AS pod_ip, s.active_node_id AS current_node_id,
                    e.k8s_node_name, e.state, s.lifecycle,
                    e.updated_at <= NOW() - make_interval(secs => $2) AS create_age_expired,
                    e.activation_attempt_id,
                    e.route_activation_id
             FROM sandbox_k8s_executions e
             JOIN sandboxes s ON s.id = e.sandbox_id
             WHERE e.sandbox_id = $1 AND e.active"
        ))
        .bind(sandbox_id)
        .bind(super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to query pod metadata")
    }

    pub(super) async fn reconcile_activation_fence(
        &self,
        sandbox_id: SandboxId,
        requested_attempt_id: SandboxActivationAttemptId,
        joined_attempt_id: Option<SandboxActivationAttemptId>,
        target_state: SandboxTransitionTargetState,
    ) -> Result<ReconciledActivationFence, SubstrateError> {
        let execution = self
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "sandbox has no active Kubernetes execution".to_string(),
            })?;
        let ownership = match (joined_attempt_id, execution.activation_attempt_id) {
            (Some(expected), Some(current)) if expected == current => {
                ActivationFenceOwnership::Joined(current)
            }
            (Some(_), Some(_)) => {
                return Err(SubstrateError::Unavailable {
                    message: "activation owner changed while joining; retry".to_string(),
                });
            }
            (None, Some(current)) if current == requested_attempt_id => {
                ActivationFenceOwnership::Owned(current)
            }
            (None, Some(_)) => {
                return Err(SubstrateError::Unavailable {
                    message: "activation owner changed before runtime work began; retry"
                        .to_string(),
                });
            }
            (_, None)
                if super::normalize_lifecycle(&execution.lifecycle) == target_state.as_str()
                    && execution.state == ActiveExecutionState::Running
                    && execution.pod_ip.is_some() =>
            {
                ActivationFenceOwnership::Completed
            }
            (_, None) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "activation attempt disappeared before reaching {target_state}; retry"
                    ),
                });
            }
        };
        if execution.route_activation_id.is_some()
            && execution.activation_attempt_id != execution.route_activation_id
        {
            let reason = if super::normalize_lifecycle(&execution.lifecycle)
                == target_state.as_str()
                && execution.state == ActiveExecutionState::Running
                && execution.pod_ip.is_some()
            {
                RouteIdentityDecisionReason::ResidentTargetIdentityReused
            } else {
                RouteIdentityDecisionReason::ExistingExecutionTransition
            };
            pplx_space_metrics::sandbox::api::route_identity_decision(reason);
        }
        Ok(ReconciledActivationFence {
            execution,
            ownership,
        })
    }
}

async fn is_node_k8s_tombstoned(
    conn: &mut PgConnection,
    node_id: NodeId,
    op: &str,
) -> Result<bool, SubstrateError> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1)"
    ))
    .bind(node_id)
    .fetch_one(conn)
    .await
    .db_context_with(|| format!("failed to {op} node tombstone"))
}

#[cfg(test)]
mod k8s_store_postgres_tests;
