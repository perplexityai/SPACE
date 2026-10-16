use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use super::*;
use crate::controllers::placement_fence::{Writer, check_placement_fence, fence_refusal_message};
use crate::controllers::substrate::{
    AdmissionPoolClaim, AdmittedPoolClaim, AdmittedPoolClaimLookup, CreateBootMode,
    PooledClaimRefusal, PooledExecutionClaim, PooledExecutionClaimRequest,
    PooledExecutionPoolShape, PooledPrepareFailureCleanup, SubstrateCapabilities,
};
use crate::controllers::{db_contention_error, internal_error, pool_store_error};
use crate::reconcile_queue::finalize_sandbox_db_delete_tx;
use axum::http::StatusCode;
use pplx_space_cluster_pool::WarmPoolStore;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{DiskIoLimits, NodeId};
use pplx_space_common_svc::space_api_route::wait_ready_lifecycle_request;
use pplx_space_common_svc::sql::node_placement_fence::PlacementFence;
use pplx_space_common_svc::types::ReconcileReason;
use pplx_space_events::{
    CreateShapeSource, SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger,
    SandboxEventType,
};
use pplx_space_net_common::TlsInterception;
use pplx_space_public_api::OperationId;
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use tracing::Instrument;

const MAX_POOLED_PREPARE_RECLAIM_ATTEMPTS: usize = 3;
const MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS: usize = 2;
const POOLED_PREPARE_IN_PLACE_RETRY_DELAY_MS: u64 = 50;
const POOLED_PREPARE_RECLAIMABLE_ERROR_FRAGMENTS: &[&str] = &[
    "connection reset",
    "connection refused",
    "deadline exceeded",
    "goaway",
    "h2 protocol error",
    "http2 error",
    "node-api not reachable",
    "rst_stream",
    "service is currently unavailable",
    "socket closed",
    "transport error",
    "transport is closing",
];

/// What [`finalize_created_sandbox`] did.
///
/// Three outcomes rather than a `bool` because the two failures want different
/// words at the call site: a lost CAS means another supervisor owns the row, and
/// a fenced node means the placement itself must not be recorded. Both roll the
/// create back, and reporting them as one would put the CAS message —
/// "abandoned while in flight (node likely draining)" — on an event whose cause
/// is known exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalizeOutcome {
    Finalized,
    /// The lifecycle CAS matched no row.
    CasLost,
    /// The termination fence refused the node this create landed on.
    Fenced(PlacementFence),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubstrateCreateFailureDisposition {
    Deleted,
    RetainedForRetry,
}

async fn clean_up_substrate_create_failure(
    pool: &PgPool,
    sandbox_id: SandboxId,
    error: &SubstrateError,
) -> Result<SubstrateCreateFailureDisposition, sqlx::Error> {
    // `RestoreInFlight` usually means substrate work still owns convergence. On
    // the Kubernetes pod-placement fence path an armed `CancelGuard` that
    // recorded durable delete intent UID-deletes the pod before retiring the
    // exact execution, then conditionally removes the never-succeeded parent. Deleting the sandbox here would
    // cascade that ownership record before the guard can lock it, leaving the
    // pod live and untracked. An ordinary restore collision retains its parent
    // for retry; an ownerless joined create remains visible to reconciliation.
    if matches!(error, SubstrateError::RestoreInFlight { .. }) {
        // Unless the row is already claimed for deletion — by the cancel guard
        // or a concurrent delete: it is retained only until that cleanup
        // removes it, so the caller must not be told to retry this sandbox.
        //
        // The lifecycle is read in the statement that takes the row lock, so a
        // delete committing while this waits is seen: a plain read before the
        // lock would decide on the pre-delete lifecycle. The same lock also
        // serves the shell delete below. Execution creation takes this row lock,
        // and a `NOT EXISTS` folded into a `DELETE` that waited on it would be
        // evaluated against the pre-wait snapshot, so it could miss an execution
        // committed during the wait and cascade it away; the `DELETE` runs as a
        // later statement instead.
        let mut tx = pool.begin().await?;
        let lifecycle: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await?;
        match lifecycle.as_deref() {
            Some("deleting") | None => {
                tx.commit().await?;
                return Ok(SubstrateCreateFailureDisposition::Deleted);
            }
            Some("creating" | "restoring") => {}
            Some(_) => {
                tx.commit().await?;
                return Ok(SubstrateCreateFailureDisposition::RetainedForRetry);
            }
        }
        // A create whose substrate already let go of it. The readiness wait
        // deletes the pod and forgets the tracking itself before returning a
        // fence refusal, so no active execution is left for a retry to converge,
        // and a fork's executor would wait on a `restoring` row rather than
        // re-create it. Delete the shell, as for any other failed create; a row
        // that still has an active execution belongs to substrate work and stays.
        //
        // A fork restore refused at placement doesn't race this: its claim
        // answers `NotCovered`, and the adapter runs the ordinary cleanup inline
        // (`CancelGuard::release_now`) before returning, so the execution is
        // normally retired when this looks. An active execution remains, and
        // the row is kept, in three cases:
        //
        // * the pod had no UID: the claim then answers `NotOwned`, not
        //   `NotCovered` (`kubernetes/support.rs:1177`), so `release_now` never
        //   runs, and the guard's `Drop` task leaves the pod to reconciliation
        //   (`kubernetes/support.rs:1364`);
        // * the retirement's own write failed (`kubernetes/support.rs:1533`, in
        //   `release_claim_with_pod`);
        // * the retirement's generation-and-attempt fence lost to a newer
        //   execution, which is then the active one (`kubernetes/support.rs:1538`,
        //   same function).
        //
        // The joined-redrive path has no guard and keeps its row with an active
        // execution.
        //
        // Only on Kubernetes: a direct row has no execution rows, so their
        // absence proves nothing there.
        let deleted = sqlx::query(sql!(
            "DELETE FROM sandboxes s
              WHERE s.id = $1
                AND s.lifecycle IN ('creating', 'restoring')
                AND s.managed_by = 'k8s'
                AND NOT EXISTS (
                    SELECT 1 FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id AND e.active
                )"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(if deleted.rows_affected() > 0 {
            SubstrateCreateFailureDisposition::Deleted
        } else {
            SubstrateCreateFailureDisposition::RetainedForRetry
        });
    }
    // Every other error leaves the row alone once it is `deleting`. A Kubernetes
    // create whose placement failed after its pod existed has already recorded
    // durable delete intent (`CancelGuard::establish_durable_cleanup`), and the
    // guard's cleanup deletes the row after its pod delete. Deleting it here first
    // would cascade the execution record that cleanup claims ownership through,
    // and the pod would be left live with nothing tracking it — whichever error
    // the placement failed with.
    sqlx::query(sql!(
        "DELETE FROM sandboxes WHERE id = $1 AND lifecycle <> 'deleting'"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(pool)
    .await?;
    Ok(SubstrateCreateFailureDisposition::Deleted)
}

#[cfg(test)]
impl FinalizeOutcome {
    /// True only for [`Self::Finalized`], so a test cannot spell "succeeded" as
    /// "not `CasLost`" and pass on a fenced create.
    ///
    /// Test-only on purpose: production matches all three variants exhaustively,
    /// which is what makes adding a fourth outcome a compile error at the call
    /// site rather than a silently-mishandled one.
    const fn is_finalized(self) -> bool {
        matches!(self, Self::Finalized)
    }
}

async fn finalize_created_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    placement: &str,
    active_node_id: Option<NodeId>,
    command_ready: bool,
    restore_operation_id: Option<OperationId>,
) -> Result<FinalizeOutcome, sqlx::Error> {
    // Termination fence. This writer is easy to miss and was: the
    // placement-only `UPDATE` further down `create_sandbox` is the obvious one,
    // but on the **restore** path (`restore_operation_id.is_some()`) that update
    // is skipped, leaving this statement as the only controller-level write of
    // `active_node_id`. So the fence has to be here too, and in the same
    // transaction as the write. (On the Kubernetes substrate the column is also
    // written earlier by `persist_pod_node_placement`, off the pod-IP watch —
    // separately fenced, and not a substitute for this one, which is the write
    // the direct substrate's restore actually goes through.)
    //
    // The transaction is opened unconditionally, which costs a BEGIN and a COMMIT
    // on the terminal-finalize path (`lifecycle = 'failed'`, no node) that has
    // nothing to lock. That was measured and then deliberately kept: avoiding it
    // means extracting the `UPDATE` into its own executor-generic function so the
    // node-less path can run it on the pool — which moves the write out of the
    // function that fences it. The fence and the write being visibly adjacent is
    // what makes this auditable and what
    // `scripts/source_guards/placement_fence_writers.py` checks, and the path in
    // question is a failed restore, not a hot one. Two round trips is the right
    // price for that.
    let mut tx = pool.begin().await?;
    if let Some(node_id) = active_node_id {
        let fence =
            check_placement_fence(&mut tx, node_id, sandbox_id, Writer::CreateFinalize).await?;
        if fence.refuses() {
            return Ok(FinalizeOutcome::Fenced(fence));
        }
    }
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT NULL
                 THEN lifecycle
                 ELSE $1
             END,
             placement = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT NULL
                 THEN placement
                 ELSE $2
             END,
             active_node_id = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT NULL
                 THEN active_node_id
                 ELSE $3
             END,
             placement_changed_at = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND $1 = 'started'
                      AND $2 = 'on_node'
                      AND $3 IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT DISTINCT FROM $3
                 THEN COALESCE(placement_changed_at, NOW())
                 ELSE placement_changed_at
             END,
             command_ready_pending = CASE
                 WHEN lifecycle = 'creating' THEN $5
                 ELSE command_ready_pending
             END,
             preserve_local_artifact_on_restore_failure = CASE
                 WHEN lifecycle = 'restoring' AND $1 IN ('started', 'failed') THEN FALSE
                 ELSE preserve_local_artifact_on_restore_failure
             END
         WHERE id = $4
           AND (
               ($6::uuid IS NULL AND lifecycle = 'creating')
               OR (
                   $6::uuid IS NOT NULL
                   AND lifecycle = 'restoring'
                   AND restore_lease_token = $6
                   AND $1 IN ('started', 'failed')
               )
               OR (
                   $1 = 'started'
                   AND lifecycle = 'started'
                   AND placement = $2
                   AND active_node_id IS NOT DISTINCT FROM $3
               )
           )"
    ))
    .bind(lifecycle)
    .bind(placement)
    .bind(active_node_id)
    .bind(sandbox_id.as_uuid())
    .bind(!command_ready)
    .bind(restore_operation_id.map(|operation_id| operation_id.as_uuid()))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(if result.rows_affected() == 1 {
        FinalizeOutcome::Finalized
    } else {
        FinalizeOutcome::CasLost
    })
}

struct PooledCreateInput<'a> {
    federated_grant_audience: pplx_space_common_core::types::FederatedGrantAudience,
    pool: &'a PgPool,
    req: &'a CreateSandboxRequest,
    env: &'a BTreeMap<String, String>,
    metric_identity: SandboxApiMetricIdentity,
    planned_sandbox_id: SandboxId,
    /// The caller already published `planned_sandbox_id` (for example in an
    /// accepted create operation), so the sandbox must be created under exactly
    /// that ID. A warm-pool slot carries its own pre-reserved ID, so a pinned
    /// create cannot adopt one.
    sandbox_id_pinned: bool,
    create_owner: &'a SandboxCreateOwner,
    auto_lifecycle: &'a AutoLifecyclePolicy,
    backend: &'a str,
    requested_template: &'a TemplateReference,
    template: &'a str,
    content_hash: &'a str,
    template_release_id: Uuid,
    template_network_policy: &'a NetworkPolicyV1,
    template_network_policy_generation: u64,
    network_policy_override: Option<&'a NetworkPolicyV1>,
    egress_profiles: crate::egress_profiles::EgressProfileSelection,
    boot_mode: CreateBootMode,
    federated_owner: Option<&'a crate::federated_identity::FederatedOwner>,
    federated_service_grant: Option<&'a crate::federated_identity::FederatedServiceGrant>,
    /// This create will produce an encrypted sandbox (org enrollment ∧ the
    /// global volume-encryption switch). See the pool bypass in
    /// `try_create_from_pool_with_substrate` for why these cold-create today.
    volume_encryption: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PoolCreateObservation {
    outcome: pplx_space_metrics::warm_pool::CreatePathOutcome,
}

impl PoolCreateObservation {
    const fn cold_direct() -> Self {
        Self {
            outcome: pplx_space_metrics::warm_pool::CreatePathOutcome::ColdDirect,
        }
    }

    const fn error() -> Self {
        Self {
            outcome: pplx_space_metrics::warm_pool::CreatePathOutcome::Error,
        }
    }

    const fn after_pool_miss() -> Self {
        Self {
            outcome: pplx_space_metrics::warm_pool::CreatePathOutcome::OrdinaryMiss,
        }
    }

    const fn after_pool_recovery_failure() -> Self {
        Self {
            outcome: pplx_space_metrics::warm_pool::CreatePathOutcome::ColdFallback,
        }
    }
}

fn pool_bypass_observation(
    placement_requested: bool,
    restore_requested: bool,
) -> Option<PoolCreateObservation> {
    (placement_requested || restore_requested).then_some(PoolCreateObservation::cold_direct())
}

struct PoolCreateResult {
    response: Option<SandboxResponse>,
    observation: PoolCreateObservation,
}

struct CreatePathEmitter<'a> {
    template: &'a str,
    content_hash: &'a str,
    surface: &'a str,
    started_at: Instant,
    observation: PoolCreateObservation,
    claim_duration_nanos: Arc<AtomicU64>,
    request_succeeded: bool,
}

impl CreatePathEmitter<'_> {
    fn set_observation(&mut self, observation: PoolCreateObservation) {
        if self.observation.outcome != pplx_space_metrics::warm_pool::CreatePathOutcome::Error {
            return;
        }
        self.observation = observation;
    }

    fn mark_request_succeeded(&mut self) {
        self.request_succeeded = true;
    }
}

impl Drop for CreatePathEmitter<'_> {
    fn drop(&mut self) {
        let outcome = if self.request_succeeded {
            self.observation.outcome
        } else {
            pplx_space_metrics::warm_pool::CreatePathOutcome::Error
        };
        pplx_space_metrics::warm_pool::create_path(pplx_space_metrics::warm_pool::CreatePath {
            template: self.template,
            content_hash: self.content_hash,
            surface: self.surface,
            outcome,
            started_at: self.started_at,
            claim_duration_ms: claim_duration_ms(&self.claim_duration_nanos),
        });
    }
}

struct ClaimDurationGuard {
    started_at: Instant,
    accumulated_nanos: Arc<AtomicU64>,
}

impl ClaimDurationGuard {
    fn start(accumulated_nanos: Arc<AtomicU64>) -> Self {
        Self {
            started_at: Instant::now(),
            accumulated_nanos,
        }
    }
}

impl Drop for ClaimDurationGuard {
    fn drop(&mut self) {
        let elapsed_nanos = u64::try_from(self.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.accumulated_nanos
            .fetch_add(elapsed_nanos, Ordering::Relaxed);
    }
}

fn claim_duration_ms(accumulated_nanos: &AtomicU64) -> f64 {
    accumulated_nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0
}

impl PoolCreateResult {
    fn warm(
        response: SandboxResponse,
        outcome: pplx_space_metrics::warm_pool::CreatePathOutcome,
    ) -> Self {
        Self {
            response: Some(response),
            observation: PoolCreateObservation { outcome },
        }
    }

    fn cold(observation: PoolCreateObservation) -> Self {
        Self {
            response: None,
            observation,
        }
    }
}

struct PooledPrepareAttemptError {
    error: ControllerError,
    attempts: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PooledPrepareFailureDisposition {
    Propagate,
    FallbackToOnDemand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PooledPrepareFailureAction {
    Propagate,
    ReclaimPool,
    FallbackToOnDemand,
}

/// The template a create resolves: the request's, or `seed:latest`.
fn create_template_reference(req: &CreateSandboxRequest) -> TemplateReference {
    req.template.clone().unwrap_or_else(|| {
        "seed:latest"
            .parse::<TemplateReference>()
            .expect("seed:latest is a valid template reference")
    })
}

/// The auto-lifecycle policy a create runs with: the request's, or the
/// resolved template's default.
fn effective_create_auto_lifecycle(
    req: &CreateSandboxRequest,
    resolved_template: &ResolvedCreateTemplate,
) -> Result<AutoLifecyclePolicy, ControllerError> {
    match req.auto_lifecycle.as_ref() {
        Some(policy) => create_auto_lifecycle_policy(Some(policy.clone())),
        None => {
            create_auto_lifecycle_policy(Some(resolved_template.default_auto_lifecycle.clone()))
        }
    }
}

fn create_runtime_from_request(
    runtime: Option<&str>,
) -> Result<(RuntimeKind, &'static str), ControllerError> {
    let requested_runtime = runtime.unwrap_or("fc");
    let runtime = RuntimeKind::parse(requested_runtime)
        .map_err(|error| ControllerError::bad_request(format!("invalid runtime: {error}")))?;
    Ok((runtime, runtime.alias()))
}

#[derive(Debug, Clone)]
pub(crate) struct SandboxCreateOwner {
    pub(crate) user_id: UserId,
    pub(crate) org_id: OrgId,
    pub(crate) email: String,
}

impl SandboxCreateOwner {
    fn local(actor: &User) -> Self {
        Self {
            user_id: actor.id,
            org_id: actor.org_id,
            email: actor.email.clone(),
        }
    }
}

impl From<crate::federated_identity::FederatedOwnerRecord> for SandboxCreateOwner {
    fn from(owner: crate::federated_identity::FederatedOwnerRecord) -> Self {
        Self {
            user_id: owner.user_id,
            org_id: owner.org_id,
            email: owner.email,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FederatedSandboxCreateContext<'a> {
    owner: Option<&'a crate::federated_identity::FederatedOwner>,
    service_grant: Option<&'a crate::federated_identity::FederatedServiceGrant>,
    egress_profiles: Option<&'a crate::egress_profiles::EgressProfileSelection>,
}

impl<'a> FederatedSandboxCreateContext<'a> {
    pub fn new(
        owner: Option<&'a crate::federated_identity::FederatedOwner>,
        service_grant: Option<&'a crate::federated_identity::FederatedServiceGrant>,
    ) -> Self {
        Self {
            owner,
            service_grant,
            egress_profiles: None,
        }
    }

    pub(crate) fn with_egress_profiles(
        mut self,
        profiles: Option<&'a crate::egress_profiles::EgressProfileSelection>,
    ) -> Self {
        self.egress_profiles = profiles;
        self
    }
}

pub struct RestoreForkDiskAsNewContext<'a> {
    pub sandbox_id: SandboxId,
    pub federated: FederatedSandboxCreateContext<'a>,
    pub snapshot: crate::controllers::substrate::SnapshotRef<'a>,
    pub operation_id: OperationId,
    pub restore_lease_seconds: u32,
}

/// Whether the owner org has BYOK (per-sandbox volume encryption) enabled.
/// BYOK sandboxes must cold-create with an encrypted rootfs, so they bypass the
/// warm pool — see [`crate::controllers::substrate::pool_eligible_for_create`].
/// A missing org row defaults to `false` (pool-eligible); the create's own
/// owner resolution is the authority on whether the org exists.
pub(crate) async fn org_byok_enabled(
    pool: &PgPool,
    org_id: OrgId,
) -> Result<bool, ControllerError> {
    let row: Option<(bool,)> =
        sqlx::query_as(sql!("SELECT byok_enabled FROM organizations WHERE id = $1"))
            .bind(org_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| db_contention_error("failed to read org BYOK flag", &err))?;
    Ok(row.map(|(enabled,)| enabled).unwrap_or(false))
}

/// The owner org's encryption flag, or `None` when the org row is missing.
/// For decisions that must fail closed on an unknown org, unlike
/// [`org_byok_enabled`].
async fn org_encryption_flag(pool: &PgPool, org_id: OrgId) -> Result<Option<bool>, sqlx::Error> {
    sqlx::query_scalar(sql!("SELECT byok_enabled FROM organizations WHERE id = $1"))
        .bind(org_id)
        .fetch_optional(pool)
        .await
}

/// Cluster-wide opt-in for stamping `volume_encryption` on new sandboxes.
/// `organizations.byok_enabled` is an advisory flag shared with the
/// credential-encryption feature, so volume encryption additionally requires
/// this opt-in — otherwise flipping an org's advisory bit would silently start
/// encrypting its sandboxes. Parsed with the shared flag vocabulary: `0`/`false`
/// are off like unset, empty, and unrecognized values. Only consulted at
/// create: the flag stamped on the row is immutable, so toggling this never
/// changes how an existing sandbox starts, suspends, or restores.
fn byok_volume_encryption_engaged() -> bool {
    pplx_space_common_core::env::env_flag_var("SPACE_BYOK_VOLUME_ENCRYPTION", false)
}

#[derive(Debug, thiserror::Error)]
enum PooledCreateRollbackError {
    #[error("failed to clean failed pooled execution claim: {0}")]
    ClaimCleanup(#[source] ControllerError),
    #[error("failed to begin pooled sandbox rollback cleanup transaction: {0}")]
    BeginCleanup(#[source] sqlx::Error),
    #[error("failed to clean pooled sandbox rollback database state: {0}")]
    Cleanup(#[source] sqlx::Error),
}

fn pooled_prepare_failure_disposition(
    boot_mode: CreateBootMode,
) -> PooledPrepareFailureDisposition {
    match boot_mode {
        CreateBootMode::Fallthrough => PooledPrepareFailureDisposition::FallbackToOnDemand,
        CreateBootMode::Pool | CreateBootMode::Cold | CreateBootMode::BaseSnapshot => {
            PooledPrepareFailureDisposition::Propagate
        }
    }
}

/// The error a terminal pooled-create arm hands back, given whether the claim
/// was actually *released* — not merely whether cleanup returned `Ok`.
///
/// The distinction is load-bearing and substrate-specific. The trait default
/// rolls the claim back and deletes the `sandboxes` row
/// (`PooledPrepareFailureCleanup::FinalizeSandboxDbDelete`); the Kubernetes
/// override only marks the slot `cleanup_pending` and returns
/// `DeferredToReconciler`, which the caller early-returns on, leaving a
/// `paused` row behind. `paused` is durable, so no transient-lifecycle sweeper
/// ages it out. Treating that `Ok` as a release is what this parameter exists
/// to prevent.
///
/// Where the claim was not released the answer must not be retryable: `create`
/// mints a fresh `SandboxId` and carries no idempotency key, so the retry buys
/// a second warm slot and a second row while the first stays under an id the
/// caller never learns.
///
/// `without_retry_after` is not sufficient on its own — it clears the hint
/// fields but leaves `kind`, and both surface translators rebuild a retryable
/// answer from the kind alone before they ever look at the status
/// (`public::sandboxes::controller_error`, `e2b::translate::controller_error`).
fn pooled_create_terminal_error(error: ControllerError, claim_released: bool) -> ControllerError {
    if claim_released {
        error
    } else {
        error.demoted_after_side_effect()
    }
}

pub(crate) fn pooled_prepare_error_is_reclaimable(error: &ControllerError) -> bool {
    // Ahead of the fragment match, because a lost lock race is the most
    // retryable outcome on this path and would never match it. The fragment
    // list names transport signatures ("connection reset", "transport
    // error", ...); the contention message is
    // "operation conflicted with a concurrent change; retry the request",
    // which contains none of them. Without this a contention abort would be
    // labelled `non_retryable_service_unavailable` and fall straight through
    // to a cold create.
    //
    // This predicate gates two loops, and the contention arm is right for
    // both: `prepare_pooled_execution_with_retries` retries in place on the
    // same claim and the same pod after a fixed delay — which is the whole
    // remedy for a transient row-lock collision, since nothing about the slot
    // was wrong — and `pooled_prepare_failure_action` then re-claims a
    // different slot.
    //
    // What it does not fix: an abort caused by DDL holding `ACCESS EXCLUSIVE`
    // on `sandboxes` or `sandbox_k8s_executions` is a property of the table,
    // not the slot, so both loops re-hit it and the request spends its
    // attempts before falling back. That is a latency cost during a migration,
    // not a correctness one, and the alternative — treating contention as
    // non-retryable — gives up the in-place retry that resolves the common
    // row-level case.
    if error.is_lock_contention() {
        return true;
    }
    if !matches!(
        error.status(),
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::REQUEST_TIMEOUT
    ) {
        return false;
    }

    let message = error.to_string().to_ascii_lowercase();
    POOLED_PREPARE_RECLAIMABLE_ERROR_FRAGMENTS
        .iter()
        .any(|fragment| message.contains(fragment))
}

fn pooled_prepare_failure_reason(error: &ControllerError) -> &'static str {
    match (error.status(), pooled_prepare_error_is_reclaimable(error)) {
        (StatusCode::SERVICE_UNAVAILABLE, true) => "transient_service_unavailable",
        (StatusCode::REQUEST_TIMEOUT, true) => "transient_request_timeout",
        (StatusCode::SERVICE_UNAVAILABLE, false) => "non_retryable_service_unavailable",
        (StatusCode::REQUEST_TIMEOUT, false) => "non_retryable_request_timeout",
        (StatusCode::GATEWAY_TIMEOUT, _) => "non_retryable_gateway_timeout",
        (StatusCode::CONFLICT, _) => "non_retryable_conflict",
        (StatusCode::INTERNAL_SERVER_ERROR, _) => "non_retryable_internal",
        _ => "non_retryable_other",
    }
}

fn pooled_prepare_failure_action(
    boot_mode: CreateBootMode,
    error: &ControllerError,
    prepare_attempt: usize,
) -> PooledPrepareFailureAction {
    match pooled_prepare_failure_disposition(boot_mode) {
        PooledPrepareFailureDisposition::Propagate => PooledPrepareFailureAction::Propagate,
        PooledPrepareFailureDisposition::FallbackToOnDemand
            if prepare_attempt < MAX_POOLED_PREPARE_RECLAIM_ATTEMPTS
                && pooled_prepare_error_is_reclaimable(error) =>
        {
            PooledPrepareFailureAction::ReclaimPool
        }
        PooledPrepareFailureDisposition::FallbackToOnDemand => {
            PooledPrepareFailureAction::FallbackToOnDemand
        }
    }
}

struct KubernetesPoolAssignment {
    resources: SandboxResources,
    disk_io_limits: DiskIoLimits,
    source: CreateShapeSource,
}

/// Stamps `shape_source` on a `create_completed` event and counts it.
fn with_shape_source(
    details: SandboxEventDetails,
    source: Option<CreateShapeSource>,
    runtime: &str,
    template: &str,
) -> SandboxEventDetails {
    let Some(source) = source else {
        return details;
    };
    pplx_space_metrics::sandbox::api::create_shape_source(source.as_str(), runtime, template);
    details.shape_source(source)
}

struct ColdCreateShapeInput<'a> {
    capabilities: SubstrateCapabilities,
    runtime: RuntimeKind,
    boot_mode: CreateBootMode,
    template: &'a TemplateName,
    content_hash: &'a str,
    template_release_id: Uuid,
    requested: SandboxResources,
    /// A fork restores the source snapshot, whose shape is fixed.
    restoring: bool,
}

/// Resources for a create that did not adopt a warm-pool sandbox.
///
/// A Kubernetes fallthrough create takes a configured pool's shape, or the
/// shape of the template's preferred enabled warm-pool policy when the release
/// has no materialized pool config. Otherwise
/// a Kubernetes create fills the vCPU and memory the request left unset from
/// the template's build shape: the node shim boots a pod without those
/// annotations at fixed defaults (2 vCPU, 1024 MiB), whatever the template
/// was built for. The filled values do not make the request a custom shape;
/// the boot mode was already chosen from the request alone. Forks and direct
/// creates are unchanged: a snapshot fixes their shape on the node, and a
/// direct cold boot already falls back to the template's metadata.
async fn resolve_cold_create_resources(
    pool: &PgPool,
    input: ColdCreateShapeInput<'_>,
) -> Result<(SandboxResources, DiskIoLimits, Option<CreateShapeSource>), ControllerError> {
    let ColdCreateShapeInput {
        capabilities,
        runtime,
        boot_mode,
        template,
        content_hash,
        template_release_id,
        requested,
        restoring,
    } = input;
    if !capabilities.resolves_cold_create_shape || runtime != RuntimeKind::Firecracker {
        return Ok((requested, DiskIoLimits::default(), None));
    }
    if boot_mode == CreateBootMode::Fallthrough
        && let Some(pool_assignment) = resolve_kubernetes_pool_configured_resources(
            pool,
            template.as_str(),
            template_release_id,
        )
        .await?
    {
        tracing::debug!(
            template = %template,
            content_hash,
            template_release_id = %template_release_id,
            vcpu = ?pool_assignment.resources.vcpu,
            memory_mib = ?pool_assignment.resources.memory_mib,
            "using Kubernetes pool resources for cold fallthrough create"
        );
        return Ok((
            pool_assignment.resources,
            pool_assignment.disk_io_limits,
            Some(pool_assignment.source),
        ));
    }
    if restoring {
        return Ok((requested, DiskIoLimits::default(), None));
    }
    if requested.vcpu.is_some() && requested.memory_mib.is_some() {
        return Ok((
            requested,
            DiskIoLimits::default(),
            Some(CreateShapeSource::Request),
        ));
    }
    let shape =
        crate::template_inventory_store::release_build_shape(pool, template, content_hash, runtime)
            .await
            .map_err(|err| db_contention_error("failed to load template build shape", &err))?;
    let Some(shape) = shape else {
        return Ok((
            requested,
            DiskIoLimits::default(),
            Some(CreateShapeSource::Default),
        ));
    };
    let resources = SandboxResources {
        vcpu: requested.vcpu.or(Some(shape.vcpu.get().get())),
        memory_mib: requested
            .memory_mib
            .or(u32::try_from(shape.memory.as_mib_floor()).ok()),
        disk_mib: requested.disk_mib,
    };
    tracing::debug!(
        template = %template,
        content_hash,
        vcpu = ?resources.vcpu,
        memory_mib = ?resources.memory_mib,
        "using the template's build shape for unset create resources"
    );
    Ok((
        resources,
        DiskIoLimits::default(),
        Some(CreateShapeSource::Build),
    ))
}

fn validate_inherited_pool_disk(disk_mib: Option<u32>) -> Result<(), ControllerError> {
    if let Some(disk_mib) = disk_mib {
        pplx_space_common_core::sandbox_storage::validate_rootfs_mib(disk_mib).map_err(|error| {
            ControllerError::conflict(format!(
                "configured warm-pool disk_mib is unsupported: {error}; an administrator must update or disable the pool configuration"
            ))
        })?;
    }
    Ok(())
}

// The policy fallback deliberately mirrors the zero-demand policy priority in
// `pplx-space-cluster-pool`. `retain_releases` controls which releases receive
// warm slots; it must not limit miss sizing. Any enabled template policy
// supplies the shape for every cold fallthrough create so the persisted shape
// counts as demand for that policy.
async fn resolve_kubernetes_pool_configured_resources(
    pool: &PgPool,
    template_name: &str,
    template_release_id: Uuid,
) -> Result<Option<KubernetesPoolAssignment>, ControllerError> {
    if let Some(assignment) = enabled_pool_assignment(pool, template_release_id).await? {
        return Ok(Some(assignment));
    }

    let policy_assignment = WarmPoolStore::new(pool.clone())
        .preferred_enabled_policy_assignment(template_name)
        .await
        .map_err(|err| pool_store_error("failed to load warm-pool policy resources", &err))?;
    let Some(policy_assignment) = policy_assignment else {
        return Ok(None);
    };

    // Prefer a config materialized while the policy lookups were in flight.
    if let Some(assignment) = enabled_pool_assignment(pool, template_release_id).await? {
        return Ok(Some(assignment));
    }
    parse_kubernetes_pool_assignment(policy_assignment, CreateShapeSource::Policy).map(Some)
}

async fn enabled_pool_assignment(
    pool: &PgPool,
    template_release_id: Uuid,
) -> Result<Option<KubernetesPoolAssignment>, ControllerError> {
    WarmPoolStore::new(pool.clone())
        .preferred_enabled_plaintext_assignment(template_release_id)
        .await
        .map_err(|err| pool_store_error("failed to load Kubernetes pool resources", &err))?
        .map(|assignment| {
            parse_kubernetes_pool_assignment(assignment, CreateShapeSource::PoolConfig)
        })
        .transpose()
}

fn parse_kubernetes_pool_assignment(
    assignment: pplx_space_cluster_pool::PoolAssignment,
    source: CreateShapeSource,
) -> Result<KubernetesPoolAssignment, ControllerError> {
    validate_inherited_pool_disk(assignment.disk_mib)?;
    let memory_mib = u32::try_from(assignment.memory.as_mib_floor())
        .map_err(|_| ControllerError::internal("Kubernetes pool config has invalid memory_mib"))?;
    Ok(KubernetesPoolAssignment {
        resources: SandboxResources {
            vcpu: Some(assignment.vcpu.get().get()),
            memory_mib: Some(memory_mib),
            disk_mib: assignment.disk_mib,
        },
        disk_io_limits: assignment.disk_io_limits,
        source,
    })
}

/// The TLS interception mode a create resolves to: a sandbox override takes
/// precedence over the resolved template default.
fn effective_tls_interception(
    network_policy_override: Option<&NetworkPolicyV1>,
    template_network_policy: &NetworkPolicyV1,
) -> TlsInterception {
    network_policy_override
        .unwrap_or(template_network_policy)
        .tls_interception
}

impl PooledCreateInput<'_> {
    fn tls_interception(&self) -> TlsInterception {
        effective_tls_interception(self.network_policy_override, self.template_network_policy)
    }
}

fn emit_create_sandbox_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    details: SandboxEventDetails,
) {
    SandboxEvent::new(sandbox_id, event_type, SandboxEventSource::Api).emit_with(details);
}

struct SandboxCreateEventContext<'a> {
    requested_template: &'a TemplateReference,
    template: &'a str,
    content_hash: &'a str,
    runtime: &'a str,
    boot_mode: CreateBootMode,
    resources: &'a SandboxResources,
    federated_owner: Option<&'a crate::federated_identity::FederatedOwner>,
    volume_encryption: bool,
    tls_interception: TlsInterception,
}

impl SandboxCreateEventContext<'_> {
    fn details(&self) -> SandboxEventDetails {
        // Stamp the immutable per-sandbox encryption decision on every create
        // event so a create timeline alone says whether the sandbox was
        // provisioned on an encrypted rootfs.
        let mut details = SandboxEventDetails::default()
            .trigger(SandboxEventTrigger::User)
            .bool_field(
                pplx_space_events::SandboxEventBoolField::VolumeEncryption,
                self.volume_encryption,
            )
            .owner_source(if self.federated_owner.is_some() {
                pplx_space_common_core::types::SandboxOwnerSource::FederatedIdentity.as_str()
            } else {
                pplx_space_common_core::types::SandboxOwnerSource::Local.as_str()
            })
            .requested_template(self.requested_template.to_string())
            .template_id(self.template)
            .template_content_hash(self.content_hash)
            .runtime(self.runtime)
            .boot_type(self.boot_mode.as_str())
            .tls_interception(self.tls_interception.as_str());

        if let Some(owner) = self.federated_owner {
            details = details
                .upstream_issuer(owner.issuer.as_str())
                .upstream_org_id(owner.org_id.as_str())
                .upstream_subject_id(owner.subject_id.as_str());
        }

        if let Some(vcpu) = self.resources.vcpu {
            details = details.vcpu(vcpu.to_string());
        }
        if let Some(memory_mib) = self.resources.memory_mib {
            details = details.memory_mib(memory_mib.to_string());
        }
        if let Some(disk_mib) = self.resources.disk_mib {
            details = details.disk_mib(disk_mib.to_string());
        }

        details
    }
}

fn federated_service_grant_store_error(
    err: crate::federated_identity::FederatedGrantStoreError,
) -> ControllerError {
    match err {
        crate::federated_identity::FederatedGrantStoreError::OwnerMismatch => {
            ControllerError::bad_request(
                "federated service grant owner does not match sandbox owner",
            )
        }
        crate::federated_identity::FederatedGrantStoreError::StaleRenewalOfExpiredGrant => {
            // The offered grant is not newer than a stored grant that has
            // already expired: nothing usable can be persisted. Retryable —
            // the caller's next mint produces a genuinely newer grant.
            ControllerError::service_unavailable(
                "grant renewal is older than the stored, already-expired grant; re-mint and retry",
            )
        }
        crate::federated_identity::FederatedGrantStoreError::Upsert(err) => {
            db_contention_error("failed to upsert sandbox federated service grant", &err)
        }
    }
}

fn pool_slot_claimed_event_details(
    claim: &PooledExecutionClaim,
    context: &SandboxCreateEventContext<'_>,
) -> SandboxEventDetails {
    let mut details = context
        .details()
        .claim_id(claim.claim_id.to_string())
        .pod_name(&claim.pod_name)
        .execution_generation(claim.execution_generation.to_string());

    if let Some(pool_slot_id) = claim.pool_slot_id {
        details = details.pool_slot_id(pool_slot_id.to_string());
    }
    if !claim.pod_ip.is_empty() {
        details = details.pod_ip(&claim.pod_ip);
    }

    details
}

struct SandboxCreateExecutionMode<'a> {
    sandbox_id: SandboxId,
    /// `sandbox_id` is visible to the caller and must be the created ID.
    sandbox_id_pinned: bool,
    /// The warm-pool slot this create was admitted against. Its reserved ID is
    /// `sandbox_id`, so this is the one way a pinned create may adopt a slot.
    admitted_pool_claim: Option<AdmittedPoolClaim>,
    federated: FederatedSandboxCreateContext<'a>,
    restore_from: Option<crate::controllers::substrate::SnapshotRef<'a>>,
    restore_operation_id: Option<OperationId>,
    restore_lease_seconds: u32,
}

fn pool_attempt_resources(pool_shape: Option<PooledExecutionPoolShape>) -> SandboxResources {
    SandboxResources {
        vcpu: pool_shape.map(|shape| shape.vcpu),
        memory_mib: pool_shape.map(|shape| shape.memory_mib),
        disk_mib: pool_shape.and_then(|shape| shape.disk_mib),
    }
}

impl SandboxController {
    pub async fn from_env(pool: &PgPool) -> Result<Self, SandboxControllerInitError> {
        let policy = crate::federated_identity::FederatedIdentityPolicy::from_env()?;
        let egress_profiles = crate::egress_profiles::EgressProfiles::from_env()?;
        let tls_interception_exemptions =
            crate::tls_interception_exemptions::TlsInterceptionExemptions::from_env()?;
        Ok(Self::new(Substrate::from_env(pool).await?)
            .with_federated_grant_audience(policy.grant_audience)
            .with_egress_profiles(egress_profiles)
            .with_tls_interception_exemptions(tls_interception_exemptions))
    }

    pub async fn list_responses(
        &self,
        pool: &PgPool,
        db_sandboxes: &[Sandbox],
        include_observed_only: bool,
    ) -> Result<Vec<SandboxResponse>, ControllerError> {
        let ids = db_sandboxes
            .iter()
            .map(|sandbox| sandbox.id)
            .collect::<Vec<_>>();
        let observed = fetch_sandbox_observed_states(pool, &ids).await?;
        let mut responses = db_sandboxes
            .iter()
            .map(|sandbox| sandbox_to_cached_response(sandbox, observed.get(&sandbox.id)))
            .collect::<Vec<_>>();
        if include_observed_only {
            let observed_only = fetch_observed_only_sandboxes(pool, &ids).await?;
            responses.extend(
                observed_only
                    .into_iter()
                    .map(|observed| observed_only_sandbox_response(&observed)),
            );
        }
        Ok(responses)
    }

    pub async fn upsert_federated_service_grant(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        owner: &crate::federated_identity::FederatedOwner,
        grant: &crate::federated_identity::FederatedServiceGrant,
    ) -> Result<(), ControllerError> {
        crate::federated_identity::upsert_sandbox_federated_service_grant(
            pool,
            sandbox_id,
            owner,
            grant,
            self.federated_grant_audience.as_str(),
            crate::federated_identity::FEDERATED_OWNER_GRANT_PURPOSE,
        )
        .await
        .map_err(federated_service_grant_store_error)
    }

    pub async fn create(
        &self,
        pool: &PgPool,
        actor: &User,
        req: &CreateSandboxRequest,
        metric_identity: SandboxApiMetricIdentity,
        federated: FederatedSandboxCreateContext<'_>,
    ) -> Result<SandboxResponse, ControllerError> {
        self.create_with_sandbox_id_inner(
            pool,
            actor,
            req,
            metric_identity,
            SandboxCreateExecutionMode {
                sandbox_id: SandboxId::new(),
                sandbox_id_pinned: false,
                admitted_pool_claim: None,
                federated,
                restore_from: None,
                restore_operation_id: None,
                restore_lease_seconds: 0,
            },
        )
        .await
    }

    pub async fn create_with_sandbox_id(
        &self,
        pool: &PgPool,
        actor: &User,
        req: &CreateSandboxRequest,
        metric_identity: SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        federated: FederatedSandboxCreateContext<'_>,
    ) -> Result<SandboxResponse, ControllerError> {
        self.create_with_sandbox_id_inner(
            pool,
            actor,
            req,
            metric_identity,
            SandboxCreateExecutionMode {
                sandbox_id,
                sandbox_id_pinned: true,
                admitted_pool_claim: None,
                federated,
                restore_from: None,
                restore_operation_id: None,
                restore_lease_seconds: 0,
            },
        )
        .await
    }

    /// Create under `sandbox_id`, which is the reserved ID of the warm-pool
    /// slot `admitted` claimed when the create was admitted. Adopts that slot
    /// when it is still claimed for this create and fit to adopt; otherwise
    /// builds cold under the same ID, past the slot pod's generation. While a
    /// slot this create will not adopt still owns a pod under the ID, answers
    /// a retryable error carrying [`ControllerRetryAfterCause::PoolSlotRelease`].
    // `create_with_sandbox_id`'s arguments plus the admitted claim.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_with_admitted_pool_claim(
        &self,
        pool: &PgPool,
        actor: &User,
        req: &CreateSandboxRequest,
        metric_identity: SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        admitted: AdmittedPoolClaim,
        federated: FederatedSandboxCreateContext<'_>,
    ) -> Result<SandboxResponse, ControllerError> {
        self.create_with_sandbox_id_inner(
            pool,
            actor,
            req,
            metric_identity,
            SandboxCreateExecutionMode {
                sandbox_id,
                sandbox_id_pinned: true,
                admitted_pool_claim: Some(admitted),
                federated,
                restore_from: None,
                restore_operation_id: None,
                restore_lease_seconds: 0,
            },
        )
        .await
    }

    /// Whether a failed adoption under `sandbox_id` still owes a pool cleanup
    /// that will tear the sandbox down, so its row does not confirm a create.
    pub async fn admitted_pool_cleanup_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, ControllerError> {
        self.substrate
            .admitted_pool_cleanup_pending(sandbox_id)
            .await
    }

    /// Decide, before a create is recorded, whether it may be admitted
    /// against a warm-pool slot, and of which template release.
    ///
    /// Eligible only for a request the executor would adopt the slot for
    /// ([`Self::admitted_pool_adoption_eligible`]) and that the slot's shape
    /// already serves: no custom vcpu, memory, or disk, and no node-pool pin.
    /// Fails closed: anything this cannot establish here (the org's row, its
    /// encryption setting, the runtime, the template release, the lifecycle
    /// policy) answers ineligible, and the create runs cold as it would
    /// without admission. An org with encryption enabled is always
    /// ineligible, whatever the cluster's encryption opt-in says, because
    /// every warm-pool slot is plaintext.
    pub async fn plan_admission_pool_claim(
        &self,
        pool: &PgPool,
        org_id: OrgId,
        req: &CreateSandboxRequest,
    ) -> AdmissionPoolPlan {
        if req.vcpu.is_some()
            || req.memory_mib.is_some()
            || req.disk_mib.is_some()
            || req.node_pool.is_some()
        {
            return AdmissionPoolPlan::Ineligible;
        }
        let Ok((runtime, backend)) = create_runtime_from_request(req.runtime.as_deref()) else {
            return AdmissionPoolPlan::Ineligible;
        };
        let Ok(boot_mode) = CreateBootMode::from_api(req.boot_mode.as_deref(), false) else {
            return AdmissionPoolPlan::Ineligible;
        };
        match org_encryption_flag(pool, org_id).await {
            Ok(Some(false)) => {}
            Ok(Some(true) | None) => return AdmissionPoolPlan::Ineligible,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to read the org encryption flag; admitting without a pool claim"
                );
                return AdmissionPoolPlan::Ineligible;
            }
        }
        let template_reference = create_template_reference(req);
        let Ok(resolved_template) = self
            .resolve_create_template(pool, &template_reference, runtime, org_id)
            .await
        else {
            return AdmissionPoolPlan::TemplateMiss;
        };
        let Ok(auto_lifecycle) = effective_create_auto_lifecycle(req, &resolved_template) else {
            return AdmissionPoolPlan::Ineligible;
        };
        if !self.admitted_pool_adoption_eligible(req, &auto_lifecycle, boot_mode, backend) {
            return AdmissionPoolPlan::Ineligible;
        }
        AdmissionPoolPlan::Eligible {
            template_release_id: resolved_template.template_release_id,
        }
    }

    /// Claim a ready slot of `template_release_id` inside `tx`, the
    /// transaction that records the create. See
    /// [`SubstrateAdapter::claim_admission_pool_slot`].
    pub async fn claim_admission_pool_slot(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
    ) -> Result<Option<AdmissionPoolClaim>, ControllerError> {
        self.substrate
            .claim_admission_pool_slot(tx, template_release_id)
            .await
    }

    pub async fn restore_fork_disk_as_new(
        &self,
        pool: &PgPool,
        actor: &User,
        req: &CreateSandboxRequest,
        metric_identity: SandboxApiMetricIdentity,
        restore: RestoreForkDiskAsNewContext<'_>,
    ) -> Result<SandboxResponse, ControllerError> {
        self.create_with_sandbox_id_inner(
            pool,
            actor,
            req,
            metric_identity,
            SandboxCreateExecutionMode {
                sandbox_id: restore.sandbox_id,
                sandbox_id_pinned: true,
                admitted_pool_claim: None,
                federated: restore.federated,
                restore_from: Some(restore.snapshot),
                restore_operation_id: Some(restore.operation_id),
                restore_lease_seconds: restore.restore_lease_seconds,
            },
        )
        .await
    }

    async fn create_with_sandbox_id_inner(
        &self,
        pool: &PgPool,
        actor: &User,
        req: &CreateSandboxRequest,
        metric_identity: SandboxApiMetricIdentity,
        mode: SandboxCreateExecutionMode<'_>,
    ) -> Result<SandboxResponse, ControllerError> {
        if let Some(size_mib) = req.disk_mib {
            pplx_space_common_core::sandbox_storage::validate_rootfs_mib(size_mib)
                .map_err(|error| ControllerError::bad_request(format!("disk_mib: {error}")))?;
        }
        let SandboxCreateExecutionMode {
            sandbox_id,
            sandbox_id_pinned,
            admitted_pool_claim,
            federated,
            restore_from,
            restore_operation_id,
            restore_lease_seconds,
        } = mode;
        let egress_profiles = if let Some(selected) = federated.egress_profiles {
            selected.clone()
        } else if restore_from.is_some() {
            return Err(ControllerError::internal(
                "fork restore requires an explicit egress profile selection",
            ));
        } else {
            self.egress_profiles
                .select(None, federated.owner.is_some())
                .map_err(|error| ControllerError::bad_request(error.to_string()))?
        };
        if restore_from.is_some() && req.node_pool.is_some() {
            return Err(ControllerError::bad_request(
                "node-pool placement is not supported for fork disk restores",
            ));
        }
        let federated_owner = federated.owner;
        let federated_service_grant = federated.service_grant;
        if federated_owner.is_some() != federated_service_grant.is_some() {
            return Err(ControllerError::bad_request(
                "federated sandbox creates require a federated service grant",
            ));
        }
        let env = sanitize_create_env(&req.env);
        let (runtime, backend) = create_runtime_from_request(req.runtime.as_deref())?;
        let backend = backend.to_string();
        let template_reference = create_template_reference(req);
        let template_name = template_reference.name().clone();
        let template = template_name.as_str();
        let sandbox_uuid = sandbox_id;
        let create_owner = match federated_owner {
            Some(owner) => crate::federated_identity::materialize_owner(pool, owner)
                .await?
                .into(),
            None => SandboxCreateOwner::local(actor),
        };
        let resolved_template = self
            .resolve_create_template(pool, &template_reference, runtime, create_owner.org_id)
            .await?;
        let resolved_template_release_id = resolved_template.template_release_id;
        let resolved_content_hash = resolved_template.content_hash.clone();
        let create_path_started_at = Instant::now();
        let create_path_claim_duration_nanos = Arc::new(AtomicU64::new(0));
        let mut create_path = CreatePathEmitter {
            template,
            content_hash: &resolved_content_hash,
            surface: metric_identity.surface.as_str(),
            started_at: create_path_started_at,
            observation: PoolCreateObservation::error(),
            claim_duration_nanos: Arc::clone(&create_path_claim_duration_nanos),
            request_succeeded: false,
        };
        let auto_lifecycle = effective_create_auto_lifecycle(req, &resolved_template)?;
        let template_network_policy = resolved_template.default_network_policy;
        // Fork restoration reuses the source's saved effective policy. Re-resolving
        // names here would add new destinations without removing old profile allows.
        let network_policy_override = if restore_from.is_some() {
            req.network_policy.clone()
        } else {
            self.egress_profiles
                .resolve(&egress_profiles)?
                .compose_override(req.network_policy.clone(), Some(&template_network_policy))?
        };
        let network_policy_override = crate::tls_interception_exemptions::compose(
            self.tls_interception_exemptions.as_ref(),
            network_policy_override,
            Some(&template_network_policy),
        );
        let tls_interception =
            effective_tls_interception(network_policy_override.as_ref(), &template_network_policy);
        let tls_interception_on = !tls_interception.is_off();
        let network_policy_override_json = network_policy_override
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| {
                ControllerError::internal(format!("failed to serialize network policy: {error}"))
            })?;
        let template_network_policy_generation =
            resolved_template.default_network_policy_generation;
        let requested_resources = SandboxResources {
            vcpu: req.vcpu,
            memory_mib: req.memory_mib,
            disk_mib: req.disk_mib,
        };
        let boot_mode = CreateBootMode::from_api(
            req.boot_mode.as_deref(),
            requested_resources.has_custom_vm_shape(),
        )?;

        // Immutable per-sandbox encryption stamp: decided exactly once, here.
        // It gates the pool bypass below, lands on the control-plane row, and
        // rides the substrate create to the node row.
        let volume_encryption =
            org_byok_enabled(pool, create_owner.org_id).await? && byok_volume_encryption_engaged();
        // A placed create must land in its pool. Warm-pool pods are pre-created
        // on general nodes, so a pin can't adopt one — bypass the warm pool when
        // a pin is set, and reject the incompatible `boot_mode=pool` + pin combo.
        let placement_requested = req.node_pool.is_some();
        if placement_requested && boot_mode == CreateBootMode::Pool {
            return Err(ControllerError::bad_request(
                "node_pool placement is incompatible with boot_mode=pool",
            ));
        }
        if restore_from.is_some() && volume_encryption {
            return Err(ControllerError::bad_request(
                "encrypted sandboxes cannot be fork targets",
            ));
        }
        let pooled_input = PooledCreateInput {
            federated_grant_audience: self.federated_grant_audience.clone(),
            pool,
            req,
            env: &env,
            metric_identity,
            planned_sandbox_id: sandbox_id,
            sandbox_id_pinned,
            create_owner: &create_owner,
            auto_lifecycle: &auto_lifecycle,
            backend: &backend,
            requested_template: &template_reference,
            template,
            content_hash: &resolved_content_hash,
            template_release_id: resolved_template_release_id,
            egress_profiles: egress_profiles.clone(),
            template_network_policy: &template_network_policy,
            template_network_policy_generation,
            network_policy_override: network_policy_override.as_ref(),
            boot_mode,
            federated_owner,
            federated_service_grant,
            volume_encryption,
        };
        // Set when this create was admitted against a warm-pool slot and is
        // building cold instead: the slot's pod used this ID's generation 1.
        let mut skip_pool_reserved_generation = false;
        if let Some(admitted) = admitted_pool_claim {
            let adoption_eligible = restore_from.is_none()
                && self.admitted_pool_adoption_eligible(req, &auto_lifecycle, boot_mode, &backend);
            let PoolCreateResult {
                response,
                observation,
            } = Self::try_create_from_admitted_pool_claim_with_substrate(
                &self.substrate,
                pooled_input,
                admitted,
                adoption_eligible,
                Arc::clone(&create_path_claim_duration_nanos),
            )
            .await?;
            create_path.set_observation(observation);
            if let Some(response) = response {
                create_path.mark_request_succeeded();
                pplx_space_metrics::sandbox::api::sandbox_created(tls_interception_on);
                return Ok(response);
            }
            skip_pool_reserved_generation = true;
        } else if let Some(observation) =
            pool_bypass_observation(placement_requested, restore_from.is_some())
        {
            create_path.set_observation(observation);
        } else {
            let PoolCreateResult {
                response,
                observation,
            } = self
                .try_create_from_pool(pooled_input, Arc::clone(&create_path_claim_duration_nanos))
                .await?;
            create_path.set_observation(observation);
            if let Some(response) = response {
                create_path.mark_request_succeeded();
                pplx_space_metrics::sandbox::api::sandbox_created(tls_interception_on);
                return Ok(response);
            }
        }
        if boot_mode == CreateBootMode::Pool {
            return Err(ControllerError::service_unavailable(
                "boot_mode=pool requested but no warm-pool sandbox is available",
            ));
        }
        let (resources, disk_io_limits, shape_source) = resolve_cold_create_resources(
            pool,
            ColdCreateShapeInput {
                capabilities: self.substrate.capabilities(),
                runtime,
                boot_mode,
                template: &template_name,
                content_hash: &resolved_content_hash,
                template_release_id: resolved_template_release_id,
                requested: requested_resources,
                restoring: restore_from.is_some(),
            },
        )
        .await?;

        self.admit_cluster_activation(crate::cluster_admission::ClusterActivationClass::Cold)
            .await?;

        // Store the resolved hash as the sandbox's immutable build pin; wake and restore
        // keep using it even if a tag moves later.
        let egress_profile_names = egress_profiles.as_names();
        let insert = pplx_space_sandbox_store::SandboxCreate {
            id: sandbox_uuid,
            name: req.name.as_deref(),
            user_id: create_owner.user_id,
            org_id: create_owner.org_id,
            managed_by: self.substrate.managed_by(),
            template,
            content_hash: &resolved_content_hash,
            vcpu: resources.vcpu.map(|value| value as i32),
            memory_mib: resources.memory_mib.map(|value| value as i32),
            disk_mib: resources.disk_mib.map(|value| value as i32),
            disk_io_limits,
            env: &env,
            metadata: &req.metadata,
            auto_lifecycle: &auto_lifecycle,
            network_policy_override_json: network_policy_override_json.as_ref(),
            egress_profiles: &egress_profile_names,
        };
        match (federated_owner, federated_service_grant) {
            (None, None) => {
                pplx_space_sandbox_store::insert_unplaced(
                    pool,
                    &insert,
                    volume_encryption,
                    restore_operation_id.map(|operation_id| operation_id.as_uuid()),
                    i64::from(restore_lease_seconds),
                )
                .await
                .map_err(|err| sandbox_insert_error(err.into()))?;
            }
            (Some(owner), Some(grant)) => {
                let mut tx = pool.begin().await.map_err(|err| {
                    db_contention_error(
                        "failed to start federated sandbox create transaction",
                        &err,
                    )
                })?;
                pplx_space_sandbox_store::insert_unplaced(
                    &mut *tx,
                    &insert,
                    volume_encryption,
                    restore_operation_id.map(|operation_id| operation_id.as_uuid()),
                    i64::from(restore_lease_seconds),
                )
                .await
                .map_err(|err| sandbox_insert_error(err.into()))?;
                crate::federated_identity::upsert_sandbox_federated_service_grant_tx(
                    &mut tx,
                    sandbox_uuid,
                    owner,
                    grant,
                    self.federated_grant_audience.as_str(),
                    crate::federated_identity::FEDERATED_OWNER_GRANT_PURPOSE,
                )
                .await
                .map_err(federated_service_grant_store_error)?;
                tx.commit().await.map_err(|err| {
                    db_contention_error(
                        "failed to commit federated sandbox create transaction",
                        &err,
                    )
                })?;
            }
            _ => {
                return Err(ControllerError::bad_request(
                    "federated sandbox creates require a federated service grant",
                ));
            }
        };
        if skip_pool_reserved_generation
            && let Err(error) = self
                .substrate
                .skip_admitted_pool_generation(sandbox_id)
                .await
        {
            // Nothing has been created yet; remove the row so a retry starts
            // clean rather than allocating the slot pod's generation.
            if let Err(delete_err) = sqlx::query("DELETE FROM sandboxes WHERE id = $1")
                .bind(sandbox_uuid)
                .execute(pool)
                .await
            {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %delete_err,
                    "failed to delete sandbox row after reserving its execution generation failed"
                );
                return Err(error.demoted_after_side_effect());
            }
            return Err(error);
        }
        let create_event_context = SandboxCreateEventContext {
            requested_template: &template_reference,
            template,
            content_hash: &resolved_content_hash,
            runtime: &backend,
            boot_mode,
            resources: &resources,
            federated_owner,
            volume_encryption,
            tls_interception,
        };
        let create_event_details = || create_event_context.details();
        emit_create_sandbox_event(
            sandbox_id,
            SandboxEventType::CreateRequested,
            create_event_details(),
        );

        let template_ref = TemplateRef {
            template: &template_name,
            content_hash: Some(&resolved_content_hash),
            boot_mode,
            runtime: &backend,
        };
        let actor_email = Email::parse(create_owner.email.clone()).map_err(|error| {
            ControllerError::bad_request(format!("invalid actor email: {error}"))
        })?;
        let actor_ref = ActorRef::new(create_owner.user_id, create_owner.org_id, actor_email);

        let outcome = match self
            .substrate
            .create_execution(CreateExecutionRequest {
                metric_identity,
                sandbox_id,
                actor: &actor_ref,
                template_ref: &template_ref,
                resources: &resources,
                disk_io_limits,
                env: &env,
                auto_lifecycle: &auto_lifecycle,
                network_policy_override: network_policy_override.as_ref(),
                network_policy_generation: 1,
                template_network_policy: &template_network_policy,
                template_network_policy_generation,
                restore_from,
                wait_until_ready: req.wait_until_ready,
                volume_encryption,
                node_pool: req.node_pool.as_ref(),
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                pplx_space_metrics::sandbox::api::lifecycle_error(
                    sandbox_uuid,
                    metric_identity,
                    "substrate",
                );
                emit_create_sandbox_event(
                    sandbox_id,
                    SandboxEventType::CreateFailed,
                    create_event_details().error(err.to_string()),
                );
                let disposition =
                    match clean_up_substrate_create_failure(pool, sandbox_id, &err).await {
                        Ok(disposition) => Some(disposition),
                        Err(delete_err) => {
                            tracing::warn!(
                                sandbox_id = %sandbox_id,
                                error = %delete_err,
                                "failed to delete sandbox row after substrate create failure"
                            );
                            None
                        }
                    };
                let surfaced = ControllerError::from(err);
                return Err(match disposition {
                    Some(SubstrateCreateFailureDisposition::RetainedForRetry) => surfaced,
                    // With the row gone, this create did NOT leave node work
                    // converging (a timeout here failed the substrate's
                    // preserve check, e.g. the pod never scheduled), so strip
                    // any retry marker — ECW/native clients must not
                    // retry/resume a deleted sandbox.
                    Some(SubstrateCreateFailureDisposition::Deleted) => {
                        surfaced.without_retry_after()
                    }
                    // When the delete itself failed the row survives in
                    // `creating`, and stripping the marker is not enough: it
                    // leaves `kind` set, which the surface translators rebuild a
                    // retryable answer from. Demote instead, so a `create` retry
                    // cannot add a second row alongside the one still awaiting
                    // the sweeper.
                    None => surfaced.demoted_after_side_effect(),
                });
            }
        };

        let outcome_sandbox_uuid = outcome.sandbox_id;
        let mut lifecycle = outcome.initial_lifecycle;
        let mut placement = outcome.initial_placement;

        if restore_operation_id.is_none()
            && let Some(active_node_id) = outcome.active_node_id
        {
            // Termination fence. This write used to run straight against the
            // pool, which made it one of the writers that touched `nodes` not at
            // all and so could not be ordered against the provider-terminate
            // path. It runs in a transaction now purely so the fence's row lock
            // is still held when the `UPDATE` commits.
            //
            // **A refusal has to tear the sandbox down, not just decline the
            // write.** By here the substrate create has *succeeded* and a guest
            // is running on the node. Returning an error while leaving the row
            // would strand a sandbox whose `active_node_id` is NULL against a
            // running pod — the state that is invisible to every term of the
            // drain gate. So `rollback_created_sandbox` deletes the execution and
            // the row, and the client is told to retry onto a node that is not
            // leaving. `finalize_created_sandbox` below reaches the same rollback
            // for the same reason; the pooled create is the one that can simply
            // roll its transaction back, because nothing has been recorded yet.
            let fenced = async {
                let mut tx = pool.begin().await?;
                let fence = check_placement_fence(
                    &mut tx,
                    active_node_id,
                    outcome.sandbox_id,
                    Writer::PostCreatePlacement,
                )
                .await?;
                if fence.refuses() {
                    return Ok(Some(fence));
                }
                sqlx::query(sql!(
                    "UPDATE sandboxes
                     SET active_node_id = $1,
                         placement_changed_at = NOW()
                     WHERE id = $2"
                ))
                .bind(active_node_id)
                .bind(outcome_sandbox_uuid)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok::<_, sqlx::Error>(None)
            }
            .await;
            // **An error here needs the same teardown as a refusal.** By this
            // point the substrate create has succeeded and a guest is running on
            // the node; returning the error straight up leaves that guest with a
            // sandbox row that records no node — the state invisible to every
            // term of the drain gate. The refusal path below always rolled back,
            // but the error path reached the `?` first, and the fence's added
            // transaction gave it new ways to fail (a `55P03` on the row lock, a
            // cancelled wait).
            let fenced = match fenced {
                Ok(fenced) => fenced,
                Err(err) => {
                    let message = "Sandbox started but DB placement update failed";
                    emit_create_sandbox_event(
                        outcome.sandbox_id,
                        SandboxEventType::CreateFailed,
                        create_event_details().error(format!("{message}: {err}")),
                    );
                    self.rollback_created_sandbox(pool, &outcome).await;
                    return Err(internal_error(message, err));
                }
            };
            if let Some(fence) = fenced {
                let message = fence_refusal_message(active_node_id, fence);
                emit_create_sandbox_event(
                    outcome.sandbox_id,
                    SandboxEventType::CreateFailed,
                    create_event_details().error(message.clone()),
                );
                self.rollback_created_sandbox(pool, &outcome).await;
                return Err(ControllerError::service_unavailable(message));
            }
        }

        // A budget-exhausted slow create is preserved before the guest is
        // command-ready (`command_ready == false`). File-change tracking needs
        // a command-ready guest; enabling it here would start *another*
        // readiness wait past the ALB budget, and returning the sandbox
        // without it would silently miss changes the caller asked to track.
        // So for a file-change create that didn't reach readiness, roll back
        // and return a retryable error — a fast retry of the create sets
        // tracking up. (file_changes is native-only; ECW never sets it, and
        // the non-file-change slow create still gets the preserve path below.)
        if req
            .file_changes
            .as_ref()
            .is_some_and(|file_changes| file_changes.enabled)
            && !outcome.command_ready
        {
            emit_create_sandbox_event(
                outcome.sandbox_id,
                SandboxEventType::CreateFailed,
                create_event_details().error(
                    "create exceeded its readiness budget before file-change tracking \
                     could be enabled",
                ),
            );
            self.rollback_created_sandbox(pool, &outcome).await;
            return Err(ControllerError::service_unavailable(
                "sandbox create exceeded its readiness budget before file-change tracking \
                 could be enabled; retry",
            ));
        }

        if let Some(file_changes) = req.file_changes.clone()
            && file_changes.enabled
            && outcome.command_ready
        {
            // `binding.is_none()` here means a genuinely unstarted execution
            // (e.g. a non-blocking create) that must be started before
            // file-change tracking can be wired up.
            if outcome.binding.is_none() {
                if let Err(err) = self
                    .substrate
                    .start_execution(
                        metric_identity,
                        outcome.sandbox_id,
                        ActivationOptions {
                            blocking: true,
                            ready_timeout: DEFAULT_BLOCKING_WAIT_TIMEOUT,
                        },
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await
                {
                    emit_create_sandbox_event(
                        outcome.sandbox_id,
                        SandboxEventType::CreateFailed,
                        create_event_details().error(err.to_string()),
                    );
                    // The one arm in this block that does *not* roll back, so
                    // the sandbox created above is still there. `start_execution`
                    // reads `sandboxes` (`require_sandbox`, classified), so
                    // without this a lost lock race would answer a marked 503
                    // on a `create` — and the retry mints a fresh `SandboxId`,
                    // provisioning a second sandbox while this one runs until
                    // the transient-lifecycle sweeper ages it out. The siblings
                    // below may stay retryable because they delete the sandbox
                    // first; this one has established nothing.
                    return Err(ControllerError::from(err).demoted_after_side_effect());
                }
                lifecycle = "started";
                placement = "on_node";
            }

            let mut client = match self
                .substrate
                .runtime_client(
                    outcome.sandbox_id,
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
            {
                Ok(client) => client,
                Err(err) => {
                    emit_create_sandbox_event(
                        outcome.sandbox_id,
                        SandboxEventType::CreateFailed,
                        create_event_details().error(err.to_string()),
                    );
                    self.rollback_created_sandbox(pool, &outcome).await;
                    // Rolled back (sandbox deleted) — never hand the client a
                    // "retry the same sandbox" marker for a sandbox that's gone.
                    return Err(err.without_retry_after());
                }
            };
            if let Err(err) = client
                .spaced_wait_ready(wait_ready_lifecycle_request(SpacedWaitReadyRequest {
                    sandbox_id: outcome.sandbox_id.to_string(),
                    timeout: Some(duration_from_millis(30_000)),
                }))
                .await
            {
                emit_create_sandbox_event(
                    outcome.sandbox_id,
                    SandboxEventType::CreateFailed,
                    create_event_details().error(err.to_string()),
                );
                self.rollback_created_sandbox(pool, &outcome).await;
                return Err(ControllerError::internal(format!(
                    "sandbox file-change tracking could not wait for readiness: {err}"
                )));
            }

            if let Err(err) = client
                .spaced_enable_file_changes(SpacedEnableFileChangesRequest {
                    sandbox_id: outcome.sandbox_id.to_string(),
                    operation: Some(spaced_ops::EnableFileChangesRequest {
                        roots: file_changes.roots,
                        include_globs: file_changes.include_globs,
                        exclude_globs: file_changes.exclude_globs,
                        use_default_excludes: file_changes.use_default_excludes,
                        user: None,
                    }),
                })
                .await
            {
                emit_create_sandbox_event(
                    outcome.sandbox_id,
                    SandboxEventType::CreateFailed,
                    create_event_details().error(err.to_string()),
                );
                self.rollback_created_sandbox(pool, &outcome).await;
                return Err(ControllerError::internal(format!(
                    "sandbox file-change tracking could not be enabled: {err}"
                )));
            }
        }

        // The finalizer owns `creating -> started`, but a matching durable
        // activation-completion receipt may converge the same execution to
        // `started` while this request is still waiting for command readiness.
        // Accept that exact started/on-node/same-node state as idempotent.
        // Every other transition remains a CAS loss: in particular, a drain
        // sweep may flip the row to `failed`, which must not be resurrected.
        // `command_ready_pending` records "reported started before the guest
        // command API was confirmed ready" — set when the substrate preserved a
        // slow create (`command_ready == false`). The access path confirms and
        // clears it on first use; until then data-plane ops get the
        // SDK-recoverable `sandbox_timeout`.
        let finalized = match finalize_created_sandbox(
            pool,
            outcome.sandbox_id,
            lifecycle,
            placement,
            outcome.active_node_id,
            outcome.command_ready,
            restore_operation_id,
        )
        .await
        {
            Ok(finalized) => finalized,
            // Same reasoning as the placement update above: the guest is running
            // by now, so an error has to tear it down rather than leave it with a
            // row that records no node. `finalize_created_sandbox` became
            // transactional to hold the fence's row lock, which added `55P03` and
            // cancelled-wait to the ways this can fail.
            Err(err) => {
                let message = "Sandbox started but DB update failed";
                emit_create_sandbox_event(
                    outcome.sandbox_id,
                    SandboxEventType::CreateFailed,
                    create_event_details().error(format!("{message}: {err}")),
                );
                self.rollback_created_sandbox(pool, &outcome).await;
                return Err(internal_error(message, err));
            }
        };

        // Both failures land on the same rollback and the same retryable 503, and
        // differ only in what they say — see [`FinalizeOutcome`]. The fenced arm
        // is not reachable through the placement update above on the restore
        // path, which skips it, so this is the fence for that path.
        match finalized {
            FinalizeOutcome::Finalized => {}
            FinalizeOutcome::CasLost => {
                tracing::warn!(
                    sandbox_id = %outcome.sandbox_id,
                    "sandbox create lost the lifecycle CAS — the drain abandon-stuck-creating \
                     sweep (or another supervisor) flipped the row out of 'creating' while \
                     this create was in flight; rolling back substrate-side state"
                );
                emit_create_sandbox_event(
                    outcome.sandbox_id,
                    SandboxEventType::CreateFailed,
                    create_event_details()
                        .error("lifecycle CAS lost during create")
                        .logged_by_caller(),
                );
                self.rollback_created_sandbox(pool, &outcome).await;
                return Err(ControllerError::service_unavailable(
                    "sandbox create was abandoned while in flight (node likely draining); retry",
                ));
            }
            FinalizeOutcome::Fenced(fence) => {
                let message = outcome
                    .active_node_id
                    .map(|node_id| fence_refusal_message(node_id, fence))
                    .unwrap_or_else(|| {
                        // Unreachable: the fence is only consulted when there is
                        // a node. Kept as a message rather than an `expect` so a
                        // future refactor that widens the fence degrades into a
                        // vaguer error instead of panicking on the create path.
                        "the node this sandbox landed on is leaving the fleet; retry".to_string()
                    });
                emit_create_sandbox_event(
                    outcome.sandbox_id,
                    SandboxEventType::CreateFailed,
                    create_event_details().error(message.clone()),
                );
                self.rollback_created_sandbox(pool, &outcome).await;
                return Err(ControllerError::service_unavailable(message));
            }
        }

        // The substrate's readiness return precedes lifecycle publication.
        // Release only this request's completed lease now; otherwise an
        // immediate STOP/START can join CREATE's lease and reuse its dead pod.
        self.substrate
            .complete_activation_lease(outcome.sandbox_id, outcome.lease_completion)
            .await;

        let sandbox = fetch_sandbox(pool, outcome_sandbox_uuid)
            .await
            .and_then(|opt| opt.ok_or(sqlx::Error::RowNotFound))
            // Not classified: post-side-effect, as above.
            .map_err(|err| internal_error("Sandbox started but DB fetch failed", err))?;

        // When the readiness wait exhausted its ALB-aware budget the substrate
        // returns `command_ready: false`: the pod is running and the row is now
        // `started` (non-sweepable), but the runtime is still converging, so
        // record `create_pending` rather than `create_completed`. The client
        // gets the sandbox id and its data-plane retry reaches the runtime once
        // ready.
        let details = create_event_details()
            .lifecycle_state(&sandbox.lifecycle)
            .placement(&sandbox.placement);
        let (create_event, details) = if outcome.command_ready {
            let details = with_shape_source(details, shape_source, &backend, template);
            (SandboxEventType::CreateCompleted, details)
        } else {
            (SandboxEventType::CreatePending, details)
        };
        emit_create_sandbox_event(sandbox.id, create_event, details);

        create_path.mark_request_succeeded();
        pplx_space_metrics::sandbox::api::sandbox_created(tls_interception_on);
        Ok(sandbox_to_response(&sandbox, None))
    }

    async fn try_create_from_pool(
        &self,
        input: PooledCreateInput<'_>,
        claim_duration_nanos: Arc<AtomicU64>,
    ) -> Result<PoolCreateResult, ControllerError> {
        Self::try_create_from_pool_with_substrate(&self.substrate, input, claim_duration_nanos)
            .await
    }

    async fn try_create_from_pool_with_substrate<A: SubstrateAdapter + ?Sized>(
        substrate: &A,
        input: PooledCreateInput<'_>,
        claim_duration_nanos: Arc<AtomicU64>,
    ) -> Result<PoolCreateResult, ControllerError> {
        // A warm member already runs on a plaintext rootfs and dm-crypt is set
        // up pre-boot, so adoption cannot encrypt it in place; an encryptable
        // member is only safe to adopt once claim-time re-homing stamps and
        // applies volume_encryption. Until that orchestration exists, encrypted
        // creates always cold-create, recorded as a direct cold create rather
        // than a pool miss.
        if input.volume_encryption {
            tracing::debug!(
                org_id = %input.create_owner.org_id,
                template = input.template,
                "BYOK create: bypassing warm pool, cold-creating an encrypted sandbox"
            );
            return Ok(PoolCreateResult::cold(PoolCreateObservation::cold_direct()));
        }

        // A warm-pool slot is booted under its own pre-reserved sandbox ID
        // (pod name, pod metadata, and guest runtime all carry it), and
        // adoption returns that ID. When the caller has already published the
        // planned ID, adopting a slot would create the sandbox under a
        // different ID than the one the caller tracks. Cold-create instead.
        if input.sandbox_id_pinned {
            tracing::debug!(
                sandbox_id = %input.planned_sandbox_id,
                template = input.template,
                "pinned sandbox ID: bypassing warm pool, cold-creating"
            );
            return Ok(PoolCreateResult::cold(PoolCreateObservation::cold_direct()));
        }

        let file_changes_enabled = input
            .req
            .file_changes
            .as_ref()
            .is_some_and(|file_changes| file_changes.enabled);
        let implicit_wake_enabled =
            input.auto_lifecycle.wake_on_access() || input.auto_lifecycle.wake_on_command();

        // Substrates that can apply env to a pooled sandbox at adoption stay
        // eligible for env-bearing creates. Without this guard, a substrate that
        // cannot set env before wake would claim a warm pod and silently drop
        // the variables relative to a cold create.
        let has_request_env = !input.req.env.is_empty();
        let eligible = crate::controllers::substrate::pool_eligible_with_env(
            substrate.pool_eligible_for_request(
                input.boot_mode,
                input.backend,
                input.req.wait_until_ready,
                input.req.disk_mib,
                file_changes_enabled,
                implicit_wake_enabled,
            ),
            substrate.pool_supports_request_env(),
            has_request_env,
        );
        if !eligible {
            tracing::debug!(
                backend = input.backend,
                template = input.template,
                content_hash = input.content_hash,
                boot_mode = ?input.boot_mode,
                wait_until_ready = input.req.wait_until_ready,
                disk_mib = ?input.req.disk_mib,
                file_changes_enabled,
                implicit_wake_enabled,
                has_request_env,
                "sandbox create is not warm-pool eligible"
            );
            return Ok(PoolCreateResult::cold(PoolCreateObservation::cold_direct()));
        }

        let actor_email = Email::parse(input.create_owner.email.clone()).map_err(|error| {
            ControllerError::bad_request(format!("invalid actor email: {error}"))
        })?;
        let actor_ref = ActorRef::new(
            input.create_owner.user_id,
            input.create_owner.org_id,
            actor_email,
        );
        let claim_request = pooled_execution_claim_request(&input, &actor_ref);

        let mut prepare_attempt = 1;
        loop {
            let claim_span = tracing::info_span!(
                "api.create_sandbox.warm_pool.claim",
                template = input.template,
                content_hash = input.content_hash,
                vcpu = ?input.req.vcpu,
                memory_mib = ?input.req.memory_mib,
                prepare_attempt,
                result = tracing::field::Empty,
                claim_id = tracing::field::Empty,
                sandbox_id = tracing::field::Empty,
            );
            let claim_started_at = Instant::now();
            let claim_duration_guard = ClaimDurationGuard::start(Arc::clone(&claim_duration_nanos));
            let claim_result = substrate
                .claim_pooled_execution(&claim_request)
                .instrument(claim_span.clone())
                .await;
            drop(claim_duration_guard);
            let claim_attempt = match claim_result {
                Ok(claim_attempt) => claim_attempt,
                Err(error) => {
                    claim_span.record("result", "error");
                    pplx_space_metrics::warm_pool::acquire_duration(
                        input.template,
                        pplx_space_metrics::warm_pool::AcquireOutcome::Error,
                        claim_started_at,
                    );
                    pplx_space_metrics::sandbox::api::create_step_error(
                        SandboxId::from_uuid(Uuid::nil()),
                        input.metric_identity,
                        "fc",
                        Some(input.template),
                        "warm_pool_claim",
                        "claim_failed",
                        None,
                    );
                    return Err(error);
                }
            };
            let claim = match claim_attempt.claim {
                Some(claim) => {
                    claim_span.record("result", "claimed");
                    claim_span.record("claim_id", tracing::field::display(claim.claim_id));
                    claim_span.record("sandbox_id", tracing::field::display(claim.sandbox_id));
                    pplx_space_metrics::sandbox::api::create_step_latency(
                        claim.sandbox_id,
                        pplx_space_metrics::sandbox::api::CreateStep {
                            identity: input.metric_identity,
                            backend: "fc",
                            template: Some(input.template),
                            step: "warm_pool_claim",
                            result: "claimed",
                            started_at: claim_started_at,
                            phase: None,
                        },
                    );
                    claim
                }
                None => {
                    claim_span.record("result", "miss");
                    claim_span.record(
                        "sandbox_id",
                        tracing::field::display(input.planned_sandbox_id),
                    );
                    pplx_space_metrics::warm_pool::acquire_duration(
                        input.template,
                        pplx_space_metrics::warm_pool::AcquireOutcome::Miss,
                        claim_started_at,
                    );
                    pplx_space_metrics::sandbox::api::create_step_latency(
                        SandboxId::from_uuid(Uuid::nil()),
                        pplx_space_metrics::sandbox::api::CreateStep {
                            identity: input.metric_identity,
                            backend: "fc",
                            template: Some(input.template),
                            step: "warm_pool_claim",
                            result: "miss",
                            started_at: claim_started_at,
                            phase: None,
                        },
                    );
                    emit_create_sandbox_event(
                        input.planned_sandbox_id,
                        SandboxEventType::PoolSlotMissed,
                        SandboxCreateEventContext {
                            requested_template: input.requested_template,
                            template: input.template,
                            content_hash: input.content_hash,
                            runtime: input.backend,
                            boot_mode: input.boot_mode,
                            resources: &pool_attempt_resources(claim_attempt.pool_shape),
                            federated_owner: input.federated_owner,
                            volume_encryption: input.volume_encryption,
                            tls_interception: input.tls_interception(),
                        }
                        .details()
                        .result("miss"),
                    );
                    if prepare_attempt > 1 {
                        tracing::warn!(
                            attempts = prepare_attempt - 1,
                            max_attempts = MAX_POOLED_PREPARE_RECLAIM_ATTEMPTS,
                            "warm-pool re-claim found no replacement after transient prepare failure; falling back to on-demand create"
                        );
                    }
                    // A strict `boot_mode=pool` create has no cold path to
                    // fall through to — `queries.rs`'s caller turns this miss
                    // into a 503 — so a refusal no retry can clear has to be
                    // named here or the client is told to try again forever.
                    // Every other boot mode builds cold and re-resolves the
                    // same boundary, which is where the error belongs for
                    // those.
                    if input.boot_mode == CreateBootMode::Pool {
                        validate_inherited_pool_disk(
                            claim_attempt.pool_shape.and_then(|shape| shape.disk_mib),
                        )?;
                        // The substrate chose the class; this only renders it.
                        // Hardcoding `conflict` here is what made a reaped
                        // `templates` row answer 409 for a control-plane fault.
                        match &claim_attempt.refusal {
                            Some(PooledClaimRefusal::PlacementIncompatible { message }) => {
                                return Err(ControllerError::conflict(message.clone()));
                            }
                            Some(PooledClaimRefusal::PlacementUnresolvable { message }) => {
                                return Err(ControllerError::internal(message.clone()));
                            }
                            None => {}
                        }
                    }
                    let observation = if prepare_attempt == 1 {
                        PoolCreateObservation::after_pool_miss()
                    } else {
                        PoolCreateObservation::after_pool_recovery_failure()
                    };
                    return Ok(PoolCreateResult::cold(observation));
                }
            };

            let sandbox_uuid = match commit_pooled_sandbox_claim(
                input.pool,
                substrate,
                &input,
                input.create_owner,
                &claim_request,
                &claim,
                substrate.managed_by(),
            )
            .await
            {
                Ok(sandbox_uuid) => sandbox_uuid,
                Err(error) => {
                    if let Err(rollback_error) =
                        substrate.rollback_pooled_execution_claim(&claim).await
                    {
                        tracing::warn!(
                            sandbox_id = %claim.sandbox_id,
                            %rollback_error,
                            "failed to roll back substrate pool claim after API commit failure"
                        );
                    }
                    // Every statement inside `commit_pooled_sandbox_claim` runs
                    // after `claim_pooled_execution` fired the substrate's
                    // adopt/prepare, and the rollback above is best-effort — it
                    // logs and drops its own failure. So none of them may tell
                    // the caller to retry: `create` mints a fresh `SandboxId`
                    // with no idempotency key, and a retry would claim a second
                    // warm slot while the first stays live under an id the
                    // caller never learns. Demoted here, once, rather than
                    // statement by statement inside — the hazard belongs to the
                    // path, not to any one statement on it.
                    return Err(error.demoted_after_side_effect());
                }
            };
            let resources = pool_attempt_resources(claim_attempt.pool_shape);
            let create_event_context = SandboxCreateEventContext {
                requested_template: input.requested_template,
                template: input.template,
                content_hash: input.content_hash,
                runtime: input.backend,
                boot_mode: input.boot_mode,
                resources: &resources,
                federated_owner: input.federated_owner,
                volume_encryption: input.volume_encryption,
                tls_interception: input.tls_interception(),
            };
            let create_event_details = || create_event_context.details();
            emit_create_sandbox_event(
                sandbox_uuid,
                SandboxEventType::CreateRequested,
                create_event_details(),
            );
            emit_create_sandbox_event(
                sandbox_uuid,
                SandboxEventType::PoolSlotClaimed,
                pool_slot_claimed_event_details(&claim, &create_event_context),
            );

            let in_place_prepare_attempts = match Self::prepare_pooled_execution_with_retries(
                substrate,
                input.metric_identity,
                &claim_request,
                &claim,
                input.pool,
                input.req.wait_until_ready,
                prepare_attempt,
            )
            .await
            {
                Ok(attempts) => attempts,
                Err(PooledPrepareAttemptError {
                    error,
                    attempts: in_place_prepare_attempts,
                }) => {
                    // The failed claim is not resumable by the caller, so drop
                    // the "converging, retry this resource" marker here. This
                    // only covers the arms below where the rollback succeeds;
                    // where it fails the claim may still be live, and those two
                    // arms demote further — stripping the marker leaves `kind`
                    // set, which is enough for a translator to rebuild a
                    // retryable answer.
                    let error = error.without_retry_after();
                    let reason = pooled_prepare_failure_reason(&error);
                    let num_retries = in_place_prepare_attempts.saturating_sub(1);
                    pplx_space_metrics::sandbox::api::create_step_error(
                        claim.sandbox_id,
                        input.metric_identity,
                        "fc",
                        Some(input.template),
                        "warm_pool_prepare",
                        "prepare_failed",
                        None,
                    );
                    emit_create_sandbox_event(
                        sandbox_uuid,
                        SandboxEventType::CreateFailed,
                        create_event_details()
                            .error(error.to_string())
                            .logged_by_caller(),
                    );
                    let rollback_result = Self::rollback_failed_pooled_create_with_substrate(
                        substrate, input.pool, &claim,
                    )
                    .await;
                    match (
                        pooled_prepare_failure_action(input.boot_mode, &error, prepare_attempt),
                        rollback_result,
                    ) {
                        (PooledPrepareFailureAction::ReclaimPool, Ok(_)) => {
                            tracing::warn!(
                                sandbox_id = %claim.sandbox_id,
                                claim_id = %claim.claim_id,
                                reason,
                                num_retries,
                                max_retries = MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS - 1,
                                reclaim_attempt = prepare_attempt,
                                max_reclaim_attempts = MAX_POOLED_PREPARE_RECLAIM_ATTEMPTS,
                                error = %error,
                                error_debug = ?error,
                                "warm pool claim failed, retrying"
                            );
                            prepare_attempt += 1;
                            continue;
                        }
                        (PooledPrepareFailureAction::FallbackToOnDemand, Ok(_)) => {
                            tracing::warn!(
                                sandbox_id = %claim.sandbox_id,
                                claim_id = %claim.claim_id,
                                reason,
                                num_retries,
                                max_retries = MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS - 1,
                                reclaim_attempts = prepare_attempt,
                                terminal_action = "fallback_to_on_demand",
                                retryable_prepare_error = pooled_prepare_error_is_reclaimable(&error),
                                error = %error,
                                error_debug = ?error,
                                "warm pool claim failed terminally"
                            );
                            return Ok(PoolCreateResult::cold(
                                PoolCreateObservation::after_pool_recovery_failure(),
                            ));
                        }
                        (
                            action @ (PooledPrepareFailureAction::ReclaimPool
                            | PooledPrepareFailureAction::FallbackToOnDemand),
                            Err(rollback_error),
                        ) => {
                            let terminal_action = match action {
                                PooledPrepareFailureAction::ReclaimPool => {
                                    "reclaim_rollback_failed"
                                }
                                PooledPrepareFailureAction::FallbackToOnDemand => {
                                    "fallback_rollback_failed"
                                }
                                PooledPrepareFailureAction::Propagate => unreachable!(),
                            };
                            tracing::warn!(
                                sandbox_id = %claim.sandbox_id,
                                claim_id = %claim.claim_id,
                                reason,
                                num_retries,
                                max_retries = MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS - 1,
                                reclaim_attempts = prepare_attempt,
                                terminal_action,
                                error = %error,
                                error_debug = ?error,
                                rollback_error = %rollback_error,
                                "warm pool claim failed terminally"
                            );
                            return Err(pooled_create_terminal_error(error, false));
                        }
                        (PooledPrepareFailureAction::Propagate, Ok(cleanup)) => {
                            tracing::warn!(
                                sandbox_id = %claim.sandbox_id,
                                claim_id = %claim.claim_id,
                                reason,
                                num_retries,
                                max_retries = MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS - 1,
                                reclaim_attempts = prepare_attempt,
                                terminal_action = "propagate",
                                error = %error,
                                error_debug = ?error,
                                "warm pool claim failed terminally"
                            );
                            // Only a `FinalizeSandboxDbDelete` cleanup is a
                            // release. The Kubernetes override defers to the
                            // reconciler and leaves the `paused` row, so its
                            // `Ok` is not grounds to advertise a retry.
                            return Err(pooled_create_terminal_error(
                                error,
                                cleanup == PooledPrepareFailureCleanup::FinalizeSandboxDbDelete,
                            ));
                        }
                        (PooledPrepareFailureAction::Propagate, Err(rollback_error)) => {
                            tracing::warn!(
                                sandbox_id = %claim.sandbox_id,
                                claim_id = %claim.claim_id,
                                reason,
                                num_retries,
                                max_retries = MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS - 1,
                                reclaim_attempts = prepare_attempt,
                                terminal_action = "propagate_rollback_failed",
                                error = %error,
                                error_debug = ?error,
                                rollback_error = %rollback_error,
                                "warm pool claim failed terminally"
                            );
                            return Err(pooled_create_terminal_error(error, false));
                        }
                    }
                }
            };

            if in_place_prepare_attempts > 1 {
                tracing::info!(
                    sandbox_id = %claim.sandbox_id,
                    claim_id = %claim.claim_id,
                    num_retries = in_place_prepare_attempts - 1,
                    max_retries = MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS - 1,
                    reclaim_attempt = prepare_attempt,
                    "warm-pool sandbox prepare recovered after retrying claimed pod"
                );
            }

            let sandbox = if input.req.wait_until_ready {
                // The claim is committed and the guest prepared by now, so a
                // failure here has to hand the warm slot back and drop the row —
                // the same cleanup a prepare failure gets. Before the fence this
                // call could only fail on the lifecycle CAS; it can now also
                // refuse (the node started leaving) or error on the row lock, and
                // propagating either straight up left the slot adopted and the
                // sandbox row behind after the request had already failed.
                match post_wake_update(input.pool, sandbox_uuid, claim.active_node_id).await {
                    Ok(PostWakeUpdate::Updated(sandbox)) => *sandbox,
                    // Another supervisor owns the row now; rolling it back would
                    // delete their sandbox.
                    Ok(PostWakeUpdate::LifecycleCasLost) => return Err(post_wake_cas_lost()),
                    Err(error) => {
                        // Same mapping as this path's prepare-failure arm: only a
                        // `FinalizeSandboxDbDelete` cleanup counts as releasing the claim.
                        let released = match Self::rollback_failed_pooled_create_with_substrate(
                            substrate, input.pool, &claim,
                        )
                        .await
                        {
                            Ok(cleanup) => {
                                cleanup == PooledPrepareFailureCleanup::FinalizeSandboxDbDelete
                            }
                            Err(rollback_error) => {
                                tracing::warn!(
                                    sandbox_id = %claim.sandbox_id,
                                    claim_id = %claim.claim_id,
                                    error = %error,
                                    %rollback_error,
                                    "pooled post-wake failed and its claim rollback also failed"
                                );
                                false
                            }
                        };
                        return Err(pooled_create_terminal_error(error, released));
                    }
                }
            } else {
                fetch_sandbox(input.pool, sandbox_uuid)
                    .await
                    .and_then(|opt| opt.ok_or(sqlx::Error::RowNotFound))
                    .map_err(|err| {
                        // Not classified: the pooled sandbox is already
                        // claimed and bound, so a retry takes another slot.
                        internal_error("pooled sandbox created but DB fetch failed", err)
                    })?
            };
            emit_create_sandbox_event(
                sandbox.id,
                SandboxEventType::CreateCompleted,
                with_shape_source(
                    create_event_details()
                        .lifecycle_state(&sandbox.lifecycle)
                        .placement(&sandbox.placement),
                    Some(CreateShapeSource::PoolConfig),
                    input.backend,
                    input.template,
                ),
            );

            if prepare_attempt > 1 {
                tracing::info!(
                    sandbox_id = %claim.sandbox_id,
                    claim_id = %claim.claim_id,
                    attempts = prepare_attempt,
                    "warm-pool sandbox claim recovered after transient prepare failure"
                );
            }

            // A hit means the request actually received warm capacity, so it is
            // emitted only here: bind committed, prepare succeeded, and the
            // adopted sandbox is being returned. Emitting it at the claim above
            // counted reclaimed and failed handoffs as hits. The sample keeps
            // the claim clock so hit, miss, and error stay comparable.
            pplx_space_metrics::warm_pool::acquire_duration(
                input.template,
                pplx_space_metrics::warm_pool::AcquireOutcome::Hit,
                claim_started_at,
            );

            let outcome = match (prepare_attempt, in_place_prepare_attempts) {
                (1, 1) => pplx_space_metrics::warm_pool::CreatePathOutcome::WarmHit,
                (1, _) => pplx_space_metrics::warm_pool::CreatePathOutcome::SameClaimRetry,
                (_, _) => pplx_space_metrics::warm_pool::CreatePathOutcome::LaterWarmRecovery,
            };
            return Ok(PoolCreateResult::warm(
                sandbox_to_response(&sandbox, None),
                outcome,
            ));
        }
    }

    /// Whether this create's request is one a warm-pool slot can serve, by
    /// the same eligibility test an ordinary pooled create uses. An admitted
    /// create always waits for readiness, so that is what is asked.
    ///
    /// Shared by admission ([`Self::plan_admission_pool_claim`]), which
    /// claims a slot only when this holds, and by the executor, which adopts
    /// the claimed slot only when it still holds. A node-pool pin never
    /// adopts: warm-pool pods are pre-created on general nodes.
    fn admitted_pool_adoption_eligible(
        &self,
        req: &CreateSandboxRequest,
        auto_lifecycle: &AutoLifecyclePolicy,
        boot_mode: CreateBootMode,
        backend: &str,
    ) -> bool {
        if req.node_pool.is_some() {
            return false;
        }
        let file_changes_enabled = req
            .file_changes
            .as_ref()
            .is_some_and(|file_changes| file_changes.enabled);
        let implicit_wake_enabled =
            auto_lifecycle.wake_on_access() || auto_lifecycle.wake_on_command();
        crate::controllers::substrate::pool_eligible_with_env(
            self.substrate.pool_eligible_for_request(
                boot_mode,
                backend,
                true,
                req.disk_mib,
                file_changes_enabled,
                implicit_wake_enabled,
            ),
            self.substrate.pool_supports_request_env(),
            !req.env.is_empty(),
        )
    }

    /// Adopt the slot this create was admitted against, or report that the
    /// create must build cold under the same ID.
    ///
    /// Never claims a different slot: the create's public ID is this slot's
    /// reserved ID. Returns a warm response on adoption, a cold result when no
    /// pool slot owns a pod under the ID any more, and a retryable
    /// [`ControllerRetryAfterCause::PoolSlotRelease`] error while one still
    /// does (including after a failed adoption, whose slot the pool collects
    /// before the retry builds cold). An admitted create always adopts with
    /// `wait_until_ready`, so a returned sandbox is running and command-ready.
    async fn try_create_from_admitted_pool_claim_with_substrate<A: SubstrateAdapter + ?Sized>(
        substrate: &A,
        input: PooledCreateInput<'_>,
        admitted: AdmittedPoolClaim,
        adoption_eligible: bool,
        claim_duration_nanos: Arc<AtomicU64>,
    ) -> Result<PoolCreateResult, ControllerError> {
        let actor_email = Email::parse(input.create_owner.email.clone()).map_err(|error| {
            ControllerError::bad_request(format!("invalid actor email: {error}"))
        })?;
        let actor_ref = ActorRef::new(
            input.create_owner.user_id,
            input.create_owner.org_id,
            actor_email,
        );
        let claim_request = pooled_execution_claim_request(&input, &actor_ref);

        let claim_started_at = Instant::now();
        let claim_duration_guard = ClaimDurationGuard::start(claim_duration_nanos);
        let lookup = substrate
            .lookup_admitted_pooled_execution(&claim_request, admitted, input.planned_sandbox_id)
            .await;
        drop(claim_duration_guard);
        let (claim, pool_shape) = match lookup? {
            AdmittedPoolClaimLookup::Released => {
                tracing::info!(
                    sandbox_id = %input.planned_sandbox_id,
                    claim_id = %admitted.claim_id,
                    "admitted warm-pool slot is gone; building cold under the same ID"
                );
                return Ok(PoolCreateResult::cold(
                    PoolCreateObservation::after_pool_recovery_failure(),
                ));
            }
            AdmittedPoolClaimLookup::ReleasePending => {
                return Err(admitted_pool_slot_release_pending());
            }
            AdmittedPoolClaimLookup::Adoptable { claim, pool_shape } => (*claim, pool_shape),
        };

        if claim.sandbox_id != input.planned_sandbox_id
            || input.volume_encryption
            || !adoption_eligible
        {
            tracing::info!(
                sandbox_id = %input.planned_sandbox_id,
                claim_id = %claim.claim_id,
                volume_encryption = input.volume_encryption,
                adoption_eligible,
                "not adopting the admitted warm-pool slot; releasing it"
            );
            substrate.rollback_pooled_execution_claim(&claim).await?;
            return Err(admitted_pool_slot_release_pending());
        }

        let sandbox_uuid = match commit_pooled_sandbox_claim(
            input.pool,
            substrate,
            &input,
            input.create_owner,
            &claim_request,
            &claim,
            substrate.managed_by(),
        )
        .await
        {
            Ok(sandbox_uuid) => sandbox_uuid,
            Err(error) => {
                // The sandbox row did not commit, so nothing references the
                // slot. Take it out of the inventory; the retry waits for the
                // pool to collect it and then builds cold under this ID.
                if let Err(rollback_error) = substrate.rollback_pooled_execution_claim(&claim).await
                {
                    tracing::warn!(
                        sandbox_id = %claim.sandbox_id,
                        %rollback_error,
                        "failed to roll back admitted pool claim after API commit failure"
                    );
                }
                tracing::warn!(
                    sandbox_id = %claim.sandbox_id,
                    claim_id = %claim.claim_id,
                    %error,
                    "admitted warm-pool slot bind failed"
                );
                return Err(admitted_pool_slot_release_pending());
            }
        };
        let resources = pool_attempt_resources(Some(pool_shape));
        let create_event_context = SandboxCreateEventContext {
            requested_template: input.requested_template,
            template: input.template,
            content_hash: input.content_hash,
            runtime: input.backend,
            boot_mode: input.boot_mode,
            resources: &resources,
            federated_owner: input.federated_owner,
            volume_encryption: input.volume_encryption,
            tls_interception: input.tls_interception(),
        };
        let create_event_details = || create_event_context.details();
        emit_create_sandbox_event(
            sandbox_uuid,
            SandboxEventType::CreateRequested,
            create_event_details(),
        );
        emit_create_sandbox_event(
            sandbox_uuid,
            SandboxEventType::PoolSlotClaimed,
            pool_slot_claimed_event_details(&claim, &create_event_context),
        );

        let in_place_prepare_attempts = match Self::prepare_pooled_execution_with_retries(
            substrate,
            input.metric_identity,
            &claim_request,
            &claim,
            input.pool,
            true,
            1,
        )
        .await
        {
            Ok(attempts) => attempts,
            Err(PooledPrepareAttemptError { error, attempts }) => {
                let error = error.without_retry_after();
                pplx_space_metrics::sandbox::api::create_step_error(
                    claim.sandbox_id,
                    input.metric_identity,
                    "fc",
                    Some(input.template),
                    "warm_pool_prepare",
                    "prepare_failed",
                    None,
                );
                emit_create_sandbox_event(
                    sandbox_uuid,
                    SandboxEventType::CreateFailed,
                    create_event_details()
                        .error(error.to_string())
                        .logged_by_caller(),
                );
                let rollback = Self::rollback_failed_pooled_create_with_substrate(
                    substrate, input.pool, &claim,
                )
                .await;
                tracing::warn!(
                    sandbox_id = %claim.sandbox_id,
                    claim_id = %claim.claim_id,
                    attempts,
                    error = %error,
                    rollback_ok = rollback.is_ok(),
                    "admitted warm-pool slot failed to prepare"
                );
                // A completed rollback either removed the sandbox row or left
                // it to the pool's cleanup, which removes it; both end with
                // no slot under this ID, so the retry builds cold. A failed
                // rollback may leave the claim live, so stop here.
                return match rollback {
                    Ok(_) => Err(admitted_pool_slot_release_pending()),
                    Err(_) => Err(pooled_create_terminal_error(error, false)),
                };
            }
        };

        // Same cleanup as `try_create_from_pool_with_substrate`'s post-wake: the
        // slot is claimed and the guest prepared, and the fence can now refuse
        // or error on the node-row lock here, so a failure has to hand the slot
        // back and drop the row rather than leave both behind.
        let sandbox = match post_wake_update(input.pool, sandbox_uuid, claim.active_node_id).await {
            Ok(PostWakeUpdate::Updated(sandbox)) => *sandbox,
            // Another supervisor owns the row now; rolling it back would delete
            // their sandbox.
            Ok(PostWakeUpdate::LifecycleCasLost) => return Err(post_wake_cas_lost()),
            Err(error) => {
                // Same mapping as this path's prepare failure: a completed rollback
                // either removed the row or left it to the pool's cleanup, so the
                // retry builds cold once the slot is released; a failed rollback
                // may leave the claim live, so stop here.
                return match Self::rollback_failed_pooled_create_with_substrate(
                    substrate, input.pool, &claim,
                )
                .await
                {
                    Ok(_) => Err(admitted_pool_slot_release_pending()),
                    Err(rollback_error) => {
                        tracing::warn!(
                            sandbox_id = %claim.sandbox_id,
                            claim_id = %claim.claim_id,
                            error = %error,
                            %rollback_error,
                            "admitted pooled post-wake failed and its claim rollback also failed"
                        );
                        Err(pooled_create_terminal_error(error, false))
                    }
                };
            }
        };
        emit_create_sandbox_event(
            sandbox.id,
            SandboxEventType::CreateCompleted,
            with_shape_source(
                create_event_details()
                    .lifecycle_state(&sandbox.lifecycle)
                    .placement(&sandbox.placement),
                Some(CreateShapeSource::PoolConfig),
                input.backend,
                input.template,
            ),
        );
        pplx_space_metrics::warm_pool::acquire_duration(
            input.template,
            pplx_space_metrics::warm_pool::AcquireOutcome::Hit,
            claim_started_at,
        );
        let outcome = if in_place_prepare_attempts == 1 {
            pplx_space_metrics::warm_pool::CreatePathOutcome::WarmHit
        } else {
            pplx_space_metrics::warm_pool::CreatePathOutcome::SameClaimRetry
        };
        Ok(PoolCreateResult::warm(
            sandbox_to_response(&sandbox, None),
            outcome,
        ))
    }

    async fn prepare_pooled_execution_with_retries<A: SubstrateAdapter + ?Sized>(
        substrate: &A,
        metric_identity: SandboxApiMetricIdentity,
        claim_request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        pool: &PgPool,
        wait_until_ready: bool,
        reclaim_attempt: usize,
    ) -> Result<usize, PooledPrepareAttemptError> {
        let mut in_place_attempt = 1;
        loop {
            let prepare = async {
                // The claim publishes execution ownership before preparation.
                // A policy accepted before that boundary must not be replaced
                // by the create request's earlier snapshot, including on retry.
                let policy =
                    pplx_space_sandbox_store::auto_lifecycle_policy(pool, claim.sandbox_id)
                        .await
                        .map_err(|error| {
                            db_contention_error(
                                "failed to reload pooled lifecycle policy",
                                &error.into(),
                            )
                        })?
                        .ok_or_else(|| ControllerError::not_found("sandbox"))?;
                substrate
                    .prepare_pooled_execution(
                        metric_identity,
                        claim_request,
                        claim,
                        &policy,
                        wait_until_ready,
                        DEFAULT_BLOCKING_WAIT_TIMEOUT,
                    )
                    .await
            };
            match prepare.await {
                Ok(()) => return Ok(in_place_attempt),
                Err(error)
                    if in_place_attempt < MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS
                        && pooled_prepare_error_is_reclaimable(&error) =>
                {
                    let reason = pooled_prepare_failure_reason(&error);
                    tracing::warn!(
                        sandbox_id = %claim.sandbox_id,
                        claim_id = %claim.claim_id,
                        reason,
                        retry_number = in_place_attempt,
                        max_retries = MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS - 1,
                        reclaim_attempt,
                        error = %error,
                        error_debug = ?error,
                        "warm pool prepare failed, retrying claimed pod"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(
                        POOLED_PREPARE_IN_PLACE_RETRY_DELAY_MS,
                    ))
                    .await;
                    in_place_attempt += 1;
                }
                Err(error) => {
                    return Err(PooledPrepareAttemptError {
                        error,
                        attempts: in_place_attempt,
                    });
                }
            }
        }
    }

    /// Returns *which* cleanup ran, not just that one did. The two outcomes
    /// are not interchangeable for the caller's disposition: the trait default
    /// rolls the claim back and this function then deletes the `sandboxes`
    /// row, while the Kubernetes override only marks the slot
    /// `cleanup_pending` and leaves a `paused` row for the reconciler. Only
    /// the former is a release.
    async fn rollback_failed_pooled_create_with_substrate<A: SubstrateAdapter + ?Sized>(
        substrate: &A,
        pool: &PgPool,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, PooledCreateRollbackError> {
        let sandbox_id = claim.sandbox_id;
        let cleanup = substrate
            .cleanup_failed_pooled_prepare(claim)
            .await
            .map_err(PooledCreateRollbackError::ClaimCleanup)?;
        if cleanup == PooledPrepareFailureCleanup::DeferredToReconciler {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                claim_id = %claim.claim_id,
                "warm-pool sandbox boot failed after claim; marked slot cleanup_pending"
            );
            return Ok(cleanup);
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(PooledCreateRollbackError::BeginCleanup)?;
        finalize_sandbox_db_delete_tx(&mut tx, sandbox_id)
            .await
            .map_err(PooledCreateRollbackError::Cleanup)?;
        tx.commit()
            .await
            .map_err(PooledCreateRollbackError::Cleanup)?;
        Ok(cleanup)
    }

    async fn resolve_create_template(
        &self,
        pool: &PgPool,
        template: &TemplateReference,
        runtime: RuntimeKind,
        caller_org: OrgId,
    ) -> Result<ResolvedCreateTemplate, ControllerError> {
        let runtime_alias = runtime.alias();
        let (template_release_id, content_hash) =
            resolve_template_release(pool, template, runtime, caller_org).await?;
        let defaults = load_create_template_defaults(
            pool,
            template.name().as_str(),
            &content_hash,
            runtime_alias,
        )
        .await?;
        let policy =
            create_auto_lifecycle_policy(defaults.default_auto_lifecycle.map(|policy| policy.0))?;

        let default_network_policy = defaults.network_policy.0;

        Ok(ResolvedCreateTemplate {
            template_release_id,
            content_hash,
            default_auto_lifecycle: policy,
            default_network_policy,
            default_network_policy_generation: u64::try_from(defaults.network_policy_generation)
                .unwrap_or(0),
        })
    }

    pub(super) async fn template_default_auto_lifecycle(
        &self,
        pool: &PgPool,
        template: &str,
        content_hash: Option<&str>,
        runtime: RuntimeKind,
        caller_org: OrgId,
    ) -> Result<AutoLifecyclePolicy, ControllerError> {
        let template = TemplateName::parse(template)
            .map_err(|error| ControllerError::bad_request(format!("invalid template: {error}")))?;
        let template = if let Some(content_hash) = content_hash {
            format!("{template}@{content_hash}")
                .parse::<TemplateReference>()
                .map_err(|error| {
                    ControllerError::bad_request(format!("invalid template: {error}"))
                })?
        } else {
            TemplateReference::latest(template)
        };

        Ok(self
            .resolve_create_template(pool, &template, runtime, caller_org)
            .await?
            .default_auto_lifecycle)
    }

    pub async fn tracked_response(
        &self,
        pool: &PgPool,
        sandbox: Sandbox,
    ) -> Result<SandboxResponse, ControllerError> {
        let sandbox = fetch_sandbox(pool, sandbox.id)
            .await
            .ok()
            .flatten()
            .unwrap_or(sandbox);
        let observed = fetch_sandbox_observed_states(pool, &[sandbox.id]).await?;
        Ok(sandbox_to_cached_response(
            &sandbox,
            observed.get(&sandbox.id),
        ))
    }

    pub async fn untracked_response(
        &self,
        pool: &PgPool,
        id: SandboxId,
    ) -> Result<Option<SandboxResponse>, ControllerError> {
        let observed = fetch_sandbox_observed_states(pool, &[id]).await?;
        Ok(observed.get(&id).map(observed_only_sandbox_response))
    }
}

fn sanitize_create_env(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let (env, issues) = EnvMap::sanitize(env);
    warn_env_sanitization_issues("api_sandbox_create", &issues);
    env.into()
}

fn warn_env_sanitization_issues(context: &'static str, issues: &[EnvSanitizationIssue]) {
    if issues.is_empty() {
        return;
    }
    tracing::warn!(
        context = context,
        env_issue_count = issues.len(),
        env_issues = ?issues,
        "dropped invalid env entries"
    );
}

fn sandbox_insert_error(error: sqlx::Error) -> ControllerError {
    let sandbox_id_conflict = match &error {
        sqlx::Error::Database(database_error) => {
            database_error.code().as_deref() == Some("23505")
                && database_error.constraint() == Some("sandboxes_pkey")
        }
        _ => false,
    };
    if sandbox_id_conflict {
        ControllerError::sandbox_already_exists()
    } else {
        db_contention_error("failed to insert sandbox record", &error)
    }
}

async fn insert_pooled_sandbox(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    input: &PooledCreateInput<'_>,
    create_owner: &SandboxCreateOwner,
    claim: &PooledExecutionClaim,
    managed_by: &str,
) -> Result<SandboxId, ControllerError> {
    // Termination fence: the first statement of the transaction whose `INSERT`
    // below commits `active_node_id`, so the lock it takes is held for that
    // write. Taken here rather than after the insert because a lock acquired
    // once the row is already written orders nothing.
    //
    // Of the six fenced writers this is the cleanest refusal. Nothing has been
    // recorded yet, so the error rolls the whole transaction back, and
    // `rollback_pooled_execution_claim` at the call site hands the warm slot
    // back — the claimed pod is on a node that is about to stop existing, and
    // releasing the slot is the only useful thing left to do with it. 503 rather
    // than 409: the client's create is fine, this node is not, and a retry gets
    // a different one.
    //
    // A claim with no node attaches nothing, so there is nothing to order.
    if let Some(node_id) = claim.active_node_id {
        let fence = check_placement_fence(tx, node_id, claim.sandbox_id, Writer::PooledCreate)
            .await
            .map_err(|err| {
                ControllerError::internal(format!(
                    "failed to check the pooled-create placement fence: {err}"
                ))
            })?;
        if fence.refuses() {
            return Err(ControllerError::service_unavailable(fence_refusal_message(
                node_id, fence,
            )));
        }
    }

    let vcpu_i32 = claim.vcpu.map(i32::try_from).transpose().map_err(|err| {
        ControllerError::bad_request(format!("vcpu is too large for pool: {err}"))
    })?;
    let memory_mib_i32 = claim
        .memory_mib
        .map(i32::try_from)
        .transpose()
        .map_err(|err| {
            ControllerError::bad_request(format!("memory_mib is too large for pool: {err}"))
        })?;
    let disk_mib_i32 = claim
        .disk_mib
        .map(i32::try_from)
        .transpose()
        .map_err(|err| {
            ControllerError::bad_request(format!("disk_mib is too large for pool: {err}"))
        })?;
    let network_policy_override_json = input
        .network_policy_override
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| {
            ControllerError::internal(format!("failed to serialize network policy: {error}"))
        })?;

    pplx_space_sandbox_store::insert_pooled(
        &mut **tx,
        &pplx_space_sandbox_store::SandboxCreate {
            id: claim.sandbox_id,
            name: input.req.name.as_deref(),
            user_id: create_owner.user_id,
            org_id: create_owner.org_id,
            managed_by,
            template: input.template,
            content_hash: &claim.content_hash,
            vcpu: vcpu_i32,
            memory_mib: memory_mib_i32,
            disk_mib: disk_mib_i32,
            disk_io_limits: claim.disk_io_limits,
            env: input.env,
            metadata: &input.req.metadata,
            auto_lifecycle: input.auto_lifecycle,
            network_policy_override_json: network_policy_override_json.as_ref(),
            egress_profiles: &input.egress_profiles.as_names(),
        },
        claim.active_node_id,
    )
    .await
    // Classified like its five siblings in this transaction. The
    // post-side-effect hazard is handled once by the caller, which applies
    // `demoted_after_side_effect` — see `try_create_from_pool`.
    .map_err(|err| db_contention_error("failed to insert pooled sandbox", &err.into()))?;

    Ok(claim.sandbox_id)
}

fn pooled_execution_claim_request<'a>(
    input: &PooledCreateInput<'a>,
    actor: &'a ActorRef,
) -> PooledExecutionClaimRequest<'a> {
    PooledExecutionClaimRequest {
        metric_identity: input.metric_identity,
        actor,
        template: input.template,
        content_hash: Some(input.content_hash),
        template_release_id: Some(input.template_release_id),
        runtime: input.backend,
        vcpu: input.req.vcpu,
        memory_mib: input.req.memory_mib,
        env: input.env,
        auto_lifecycle: input.auto_lifecycle,
        network_policy_override: input.network_policy_override,
        network_policy_generation: 1,
        template_network_policy: input.template_network_policy,
        template_network_policy_generation: input.template_network_policy_generation,
        volume_encryption: input.volume_encryption,
    }
}

/// How long an admitted create waits before re-checking a pool slot that
/// still owns a pod under its ID.
const ADMITTED_POOL_SLOT_RELEASE_RETRY: Duration = Duration::from_secs(15);

fn admitted_pool_slot_release_pending() -> ControllerError {
    ControllerError::service_unavailable(
        "a warm-pool slot still holds this sandbox ID; waiting for the pool to release it",
    )
    .with_retry_after_header(ADMITTED_POOL_SLOT_RELEASE_RETRY)
    .with_retry_after_cause(ControllerRetryAfterCause::PoolSlotRelease)
}

async fn commit_pooled_sandbox_claim<A: SubstrateAdapter + ?Sized>(
    pool: &PgPool,
    substrate: &A,
    input: &PooledCreateInput<'_>,
    create_owner: &SandboxCreateOwner,
    claim_request: &PooledExecutionClaimRequest<'_>,
    claim: &PooledExecutionClaim,
    managed_by: &str,
) -> Result<SandboxId, ControllerError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_contention_error("failed to start pooled sandbox commit", &err))?;
    let sandbox_uuid =
        insert_pooled_sandbox(&mut tx, input, create_owner, claim, managed_by).await?;
    upsert_pooled_federated_service_grant(&mut tx, sandbox_uuid, input).await?;
    substrate
        .persist_pooled_execution_claim(&mut tx, claim_request, claim)
        .await?;
    enqueue_pooled_network_policy_reconcile_if_needed(
        &mut tx,
        input.network_policy_override,
        sandbox_uuid,
        claim.active_node_id,
    )
    .await
    .map_err(|err| {
        db_contention_error("failed to enqueue pooled network policy reconcile", &err)
    })?;
    tx.commit()
        .await
        .map_err(|err| db_contention_error("failed to commit pool claim", &err))?;
    Ok(sandbox_uuid)
}

async fn upsert_pooled_federated_service_grant(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_uuid: SandboxId,
    input: &PooledCreateInput<'_>,
) -> Result<(), ControllerError> {
    match (input.federated_owner, input.federated_service_grant) {
        (Some(owner), Some(grant)) => {
            crate::federated_identity::upsert_sandbox_federated_service_grant_tx(
                tx,
                sandbox_uuid,
                owner,
                grant,
                input.federated_grant_audience.as_str(),
                crate::federated_identity::FEDERATED_OWNER_GRANT_PURPOSE,
            )
            .await
            .map_err(federated_service_grant_store_error)
        }
        (None, None) => Ok(()),
        _ => Err(ControllerError::bad_request(
            "federated sandbox creates require a federated service grant",
        )),
    }
}

async fn enqueue_pooled_network_policy_reconcile_if_needed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    network_policy: Option<&NetworkPolicyV1>,
    sandbox_id: SandboxId,
    active_node_id: Option<NodeId>,
) -> Result<(), sqlx::Error> {
    if network_policy.is_none() {
        return Ok(());
    }

    let Some(active_node_id) = active_node_id else {
        return Ok(());
    };

    let refs = [ReconcileRef::network_policy(NetworkPolicyTarget::sandbox(
        sandbox_id,
    ))];
    enqueue_node_reconcile_refs_and_trigger_tx(
        tx,
        active_node_id,
        &refs,
        ReconcileReason::SandboxNetworkPolicyUpdated,
    )
    .await
    .map(|_| ())?;

    Ok(())
}

#[cfg(test)]
mod create_env_tests;
#[cfg(test)]
mod create_postgres_tests;
#[cfg(test)]
mod placement_fence_tests;
#[cfg(test)]
mod pooled_create_fallback_tests;
#[cfg(test)]
mod pooled_event_details_tests;
