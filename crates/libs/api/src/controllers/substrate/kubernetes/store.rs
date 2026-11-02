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
    ArtifactKind, ArtifactOrigin, ExactStorageUri, ExecutionGeneration, LocalArtifactId, NodeId,
    NodeRole, PodUid, SandboxActivationAttemptId, SandboxId, SandboxLifecycle, SandboxOwner,
    SandboxTransitionTargetState, SnapshotState, StorageUri, UserId,
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
    missing_suspending_pod_revert_eligibility, no_newer_full_snapshot,
    restore_retry_source_snapshot_is_current, restore_retry_suspended_revert_snapshot_exists,
    snapshot_is_current_local_artifact, started_suspend_revert_eligibility,
    suspended_k8s_finalization_sql, write_current_local_artifact_marker_assignments,
};
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::{controllers::sandbox::fetch_sandbox as fetch_api_sandbox, models::Sandbox};

use crate::controllers::substrate::{
    DbContext, PublishedLocalArtifact, SubstrateError, reject_deleting_current_local_artifact,
};

mod activation_fence;
mod execution_rows;
mod node_inventory;
mod pod_placement;
mod pod_tracking;
mod restore_lease;
mod restore_source;
mod runtime_upgrade;
mod started_recovery;
mod suspend;

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

/// The restore source for a stopped sandbox: the node name a pod would be
/// pinned to, and whether a stopped evacuation is why the row has no placement.
///
/// Defaults to "no node, not evacuating" so a vanished row gives the safe answer
/// on both counts: nothing to bind a pod to, and no refusal for a sandbox that
/// no longer exists.
#[derive(Debug, Clone, Default)]
pub(super) struct UnplacedRestoreSource {
    pub(super) node_name: Option<String>,
    pub(super) stopped_evacuation_unplaced: bool,
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
    FreshFromSuspended {
        restore_lease_token: Option<Uuid>,
    },
    /// An evacuation claim that took a released `stopped` row (marker
    /// `'stopped'`, no `active_node_id`) to `restoring`. Evacuation-only, so
    /// it never carries a lease token.
    ///
    /// Its own variant because a clean failure must not roll it back:
    /// `restore_failure_should_revert_to_suspended` matches only
    /// `FreshFromSuspended`, so this one stays `restoring`. Reverting to
    /// `'suspended'` would invent a snapshot the sandbox never had, and
    /// reverting to `'stopped'` would leave the lifecycle set that
    /// `sandboxes_clear_evacuation_restore_target` exempts, clearing the
    /// marker and the restore backoff with it, and the sandbox would sit on no
    /// node outside the evacuation claim's window. Left `restoring` with the
    /// marker, it stays inside that window, and the claim's next call takes it
    /// as `RedroveRestoring`; the drain's restore sweep lists such a row
    /// (the stopped arm of `restore_pending_evacuations_query` admits
    /// `restoring`). Re-placing it also needs its restore plan to resolve the
    /// disk artifact for a `restoring` row, which nothing does yet:
    /// `selected_stopped_restore_storage_uri` serves only `lifecycle =
    /// 'stopped'`.
    FreshFromStopped,
    RedroveRestoring {
        restore_lease_token: Option<Uuid>,
    },
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
            Self::FreshFromStopped => None,
        }
    }
}

/// What [`K8sStore::revert_restore_claim_to_suspended`] did with a failed
/// fresh restore claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RestoreClaimRevert {
    /// The row reverted to `suspended` and the execution the claim created
    /// was retired as `replaced`, so its pod, `pod_name`, is unadoptable.
    ExecutionRetired { pod_name: String },
    /// The row reverted to `suspended` with no active execution left.
    LifecycleOnly,
    /// The row reverted to `suspended` with an execution still active,
    /// leaving a same-generation pod on a `suspended` row: either the claim's
    /// own execution, because restore selection isn't proven to choose a
    /// durable pre-restore source, or one the claim reused rather than
    /// created, whose pod may hold state of its own.
    ExecutionLeftActive,
    /// The row is no longer this claim's `restoring` row; nothing changed.
    NotReverted,
}

impl RestoreClaimRevert {
    pub(super) const fn metric_outcome(
        &self,
    ) -> pplx_space_metrics::sandbox::restore::RevertOutcome {
        use pplx_space_metrics::sandbox::restore::RevertOutcome;
        match self {
            Self::ExecutionRetired { .. } => RevertOutcome::ExecutionRetired,
            Self::LifecycleOnly => RevertOutcome::LifecycleOnly,
            Self::ExecutionLeftActive => RevertOutcome::ExecutionLeftActive,
            Self::NotReverted => RevertOutcome::NotReverted,
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
}

#[cfg(test)]
mod k8s_store_postgres_tests;
