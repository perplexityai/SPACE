//! Node drain's give-up for a resident with no pod.
//!
//! The evacuation suspend can't succeed for a `started` or `paused` sandbox
//! with no active execution, or whose active execution never bound a pod (a
//! started recovery that stalled before materializing one, say). The substrate
//! reports that as [`SubstrateError::SandboxNoPod`]. Retrying the suspend every
//! tick would keep the node in `draining` forever, and the retry's own claim
//! and revert stamps `sandboxes.lifecycle_changed_at` each time, so the repair
//! loops that age rows on that clock never see this one age.
//!
//! The give-up reverts such a row to `suspended`, off the node, once four
//! things are proven and a durable suspend snapshot can restore it:
//!
//! - **No pod, by an authoritative read.** Every pod name the sandbox has had
//!   is read uncached from the apiserver, each in the namespace its execution
//!   row recorded. An execution row (name and namespace) is written before any
//!   pod is created for the sandbox, so no pod of the sandbox's can exist
//!   outside that set. A warm-pool pod not yet bound can, but only a resident
//!   with no execution row, which parks, could have one. The database's
//!   empty `pod_uid` isn't proof: it lags a pod created just before a crash.
//!   An unreadable name blocks.
//! - **No VM.** A pod can be force-deleted while its Firecracker VM keeps
//!   running, and the node keeps publishing that VM in
//!   `sandbox_observed_states`. Any observed row that isn't a `failed`,
//!   pid-less record blocks. That record is tolerated because the snapshot
//!   the revert restores is proven current. That's the failed-restorable repair's liveness
//!   test without its `active_node_id IS NULL` clause, which a row still
//!   placed on the draining node can't satisfy. That repair uses the clause to
//!   keep its purge (below) from racing a publish by the assigned node. Here
//!   a publish that read the row before this commit can re-insert the purged
//!   record, which then reads `failed` through the observed-state overlay on
//!   the unplaced row until `observed_projection_gc` removes it. The guard is
//!   a plain read of rows a publish writes without the sandbox lock, so a
//!   first-ever live publish committing inside this transaction isn't seen;
//!   that needs a VM running with no pod and never yet published.
//! - **No progress, no pod evidence.** No execution row has advanced
//!   `updated_at` within the staleness window, no active one carries a pod
//!   UID, and none carries a pod IP. Every pod create for an existing
//!   generation is preceded by a write that stamps `updated_at` under the
//!   sandbox lock: the substrate's materialization claim (recovering rows
//!   included), or the cross-node redrive's reset to `creating`. A pod for a
//!   new generation adds a generation, which the transaction's re-check of the
//!   newest generation catches. So a pod created after the reads can't slip past
//!   inside the window. What remains is a creator hung for longer than the
//!   window between its stamp and its pod create. Started recovery's claim
//!   and re-claim advance the clock too; the drain's lifecycle flap doesn't. `k8s_node_name` isn't
//!   evidence: it survives re-materialization and a recovery re-claim of an
//!   old generation, so it would park a row whose pod is long gone.
//! - **The node's own answer.** The control plane's records can't see a
//!   rootfs written after the newest snapshot, and after a crash or lost node
//!   state the sandbox's cache directory (its rootfs and local artifacts) can
//!   outlive its sbx-mgr row, so no database state proves the node holds
//!   nothing newer. The draining node is asked about the newest generation
//!   through `PrepareTeardownSuspend` in observe-only mode: the same
//!   generation-fenced record the pod teardown uses to release an execution.
//!   Observe-only never starts a suspend. Only one answer proves the local
//!   state safe to leave: a local delete receipt covering that generation.
//!   A receipt covers the generation it names and every older one, so
//!   nothing of any generation is left to lose ([`node_vouches`]). Every other answer
//!   parks the row: pending, a superseded generation, a refusal (a
//!   quarantined row), a durable suspend (the node holds a suspended local
//!   row; a node publishing normally also reports it as a `suspended`
//!   observed record, which the VM guard refuses first), or an unreadable
//!   node. Older generations aren't asked: their receipt can't cover a newer
//!   one, and missing pod history doesn't prove the newer one never ran,
//!   since `pod_materialized_at` and `pod_uid` are written only once the pod
//!   watcher records a pod, and nothing is recorded before a pod is created.
//!
//! Liveness gives way to that. A newest generation that never reached the
//! node answers pending, and no receipt can cover it, so a started recovery
//! that stalled before materializing its pod parks and isn't released.
//! A node that can't be reached never vouches, so a forced drain of a dead
//! node parks on its pod-less residents. A delete receipt is kept on the
//! node for a week, and one that never had a generation is never written;
//! past that, a node that did delete the generation answers pending and the
//! row parks. All of these park visibly, and wait for an operator.
//!
//! Then, in one transaction under the row locks, the row becomes `suspended`
//! and unplaced. The revert re-checks the snapshot state in its own `UPDATE`,
//! so a snapshot reaped since the precheck can't revert a row with no restore
//! source. It also waits out any full snapshot still uploading, which may be
//! newer than the snapshot it would restore.
//!
//! Recoverability uses
//! [`failed_repair_revert_eligibility`], conjoined with
//! [`newest_full_snapshot_unambiguous`] so the snapshot a later wake
//! restores is the one the eligibility proved current. The eligibility anchors the
//! newest-snapshot proof on the activity clocks rather than
//! `lifecycle_changed_at`. The drain's flap moved that clock after any
//! snapshot this row could have, so the lifecycle-anchored variant would
//! reject every markerless row. That eligibility is for callers that prove no
//! runtime can modify the artifact and retire the execution under the same
//! lock, which is what this transaction does, and it's the proof started
//! recovery's own failed-recovery path applies to `started` rows.
//!
//! **It never fails a row.** A row nothing can restore, or one the node
//! doesn't vouch for, stays resident and parks the drain, counted as
//! `not_eligible` with the guard's reason. Failing would unplace it, and the
//! node could then terminate with what may be the sandbox's last bytes.
//!
//! **A resident with no execution row at all** recorded no pod name and no
//! generation, so neither the pod read nor the node's answer is possible. It
//! parks under reason `no_execution_row`.

use std::future::Future;
use std::pin::Pin;

use futures_util::StreamExt;

use pplx_space_api::controllers::substrate::SubstrateError;
use pplx_space_api::controllers::substrate::kubernetes::{
    GenerationTeardownEvidence, KubernetesSubstrate,
};
use pplx_space_cluster_pool::{PoolError, WarmPoolStore};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeDrainMode, NodeId, PositiveDuration, SandboxId, SandboxLifecycle,
    SubstrateKind,
};
use pplx_space_sandbox_portability::{
    SuspendedK8sFinalizationSql, SuspendedTargetNodeAssignment,
    clear_current_local_artifact_marker_assignments, failed_repair_revert_eligibility,
    newest_full_snapshot_unambiguous, pod_less_drain_revert_loses_no_newer_bytes,
    suspended_k8s_finalization_sql,
};
use sqlx::PgPool;

use crate::orphaned_sandboxes::{PodPresenceStatus, classify_pod_presence};

use super::super::PHASE_DRAINING;
use super::{DRAIN_COMPLETION_LOCK_TIMEOUT, is_lock_timeout};

/// What [`super::NodeDrainSink::recover_pod_less_sandbox`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodLessRecovery {
    /// A durable suspend snapshot made the sandbox recoverable; it's now
    /// `suspended` and unplaced, and its next wake restores the snapshot.
    RevertedToSuspended,
    /// A guard held; the row is untouched and the next tick tries again.
    NotEligible(PodLessHold),
}

impl PodLessRecovery {
    /// The `outcome` tag of `node_drain.sandbox_no_pod`.
    pub fn metric_tag(self) -> &'static str {
        match self {
            Self::RevertedToSuspended => "reverted_to_suspended",
            Self::NotEligible(_) => "not_eligible",
        }
    }

    /// The `reason` tag of `node_drain.sandbox_no_pod`: the hold's tag, else
    /// `none`.
    pub fn reason_tag(self) -> &'static str {
        match self {
            Self::NotEligible(hold) => hold.tag(),
            Self::RevertedToSuspended => "none",
        }
    }
}

/// Why the give-up left a row in place. Each hold either clears on its own,
/// with no one acting, or can keep the row parked indefinitely;
/// [`Self::clears_on_its_own`] is an exhaustive match, so a new hold has to
/// say which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodLessHold {
    // Clears on its own: the row ages in, leaves the candidate set, or the
    // next tick's retry wins the race it lost.
    /// An execution advanced `updated_at` inside the staleness window.
    RecentProgress,
    /// The row left the draining node, changed lifecycle, gained an
    /// evacuation marker or is gone, or the node stopped force-draining.
    RowDrifted,
    /// No execution row named a pod to read: the rows changed between reads.
    NoPodName,
    /// A newer generation appeared after the pod reads and the node's answer.
    GenerationChanged,
    /// The snapshot state changed between the guards and the revert.
    SnapshotChanged,
    /// A row lock wasn't granted inside the bound, or the transaction lost a
    /// deadlock.
    LockContended,

    // Can park the drain indefinitely: nothing in the give-up's reach
    // changes it.
    /// A node still publishes a live VM, or a suspended local row, for it.
    VmObserved,
    /// The active execution carries a pod UID, or any execution a pod IP.
    PodBound,
    /// A recorded pod name reads as present or unready.
    PodPresent,
    /// A recorded pod name couldn't be read.
    PodUnreadable,
    /// No durable suspend snapshot can restore it.
    NoRestoreSource,
    /// Reverting could lose newer bytes or restore an ambiguous snapshot: a
    /// marked artifact with no durable copy, an upload in flight, a newer
    /// failed upload, a newer full snapshot in another state, or a tie.
    HeldForDataSafety,
    /// No execution row, so no generation to ask the node about.
    NoExecutionRow,
    /// No delete receipt covers the newest generation: the node never
    /// learned it, still holds a row for it without a durable artifact, or
    /// has no row and no receipt for the sandbox.
    NodePending,
    /// The node holds a suspended local row.
    NodeSuspended,
    /// The node's local state belongs to a newer execution, or a
    /// generation-fenced stop or activation rollback retired this one.
    NodeSuperseded,
    /// The node refused the generation: a quarantined row, a job for this
    /// generation with no row left, or a failed job it won't retry.
    NodeRefused,
    /// The node couldn't be asked.
    NodeUnreadable,
}

impl PodLessHold {
    /// The `reason` tag of `node_drain.sandbox_no_pod`.
    pub fn tag(self) -> &'static str {
        match self {
            Self::RecentProgress => "recent_progress",
            Self::RowDrifted => "row_drifted",
            Self::VmObserved => "vm_observed",
            Self::PodBound => "pod_bound",
            Self::NoPodName => "no_pod_name",
            Self::PodPresent => "pod_present",
            Self::PodUnreadable => "pod_unreadable",
            Self::GenerationChanged => "generation_changed",
            Self::SnapshotChanged => "snapshot_changed",
            Self::LockContended => "lock_contended",
            Self::NoRestoreSource => "no_restore_source",
            Self::HeldForDataSafety => "held_for_data_safety",
            Self::NoExecutionRow => "no_execution_row",
            Self::NodePending => "node_pending",
            Self::NodeSuspended => "node_suspended",
            Self::NodeSuperseded => "node_superseded",
            Self::NodeRefused => "node_refused",
            Self::NodeUnreadable => "node_unreadable",
        }
    }

    /// Whether the hold clears with no one acting. One that doesn't can
    /// keep the drain parked indefinitely, so it WARNs.
    pub fn clears_on_its_own(self) -> bool {
        match self {
            Self::RecentProgress
            | Self::RowDrifted
            | Self::NoPodName
            | Self::GenerationChanged
            | Self::SnapshotChanged
            | Self::LockContended => true,
            Self::VmObserved
            | Self::PodBound
            | Self::PodPresent
            | Self::PodUnreadable
            | Self::NoRestoreSource
            | Self::HeldForDataSafety
            | Self::NoExecutionRow
            | Self::NodePending
            | Self::NodeSuspended
            | Self::NodeSuperseded
            | Self::NodeRefused
            | Self::NodeUnreadable => false,
        }
    }
}

/// One pod name the sandbox has had, with the namespace and generation its
/// execution row recorded for it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct PodRef {
    /// Decoded as [`ExecutionGeneration`], so a stored value that isn't one
    /// fails the database read rather than reaching the node.
    pub(crate) generation: ExecutionGeneration,
    pub(crate) pod_namespace: String,
    pub(crate) pod_name: String,
}

/// Uncached pod reads, behind a seam so the absence decision can be tested
/// without a cluster.
pub(crate) trait PodAbsenceProber: Sync {
    fn probe(
        &self,
        pod_namespace: &str,
        pod_name: &str,
    ) -> impl Future<Output = PodPresenceStatus> + Send;
}

impl PodAbsenceProber for KubernetesSubstrate {
    async fn probe(&self, pod_namespace: &str, pod_name: &str) -> PodPresenceStatus {
        match self
            .read_sandbox_pod_uncached_in_namespace(pod_namespace, pod_name)
            .await
        {
            Ok(Some(pod)) => classify_pod_presence(&pod),
            Ok(None) => PodPresenceStatus::Missing,
            Err(err) => {
                // The reconciler WARNs `pod_unreadable` once per node per
                // pacing window.
                tracing::debug!(
                    pod_namespace,
                    pod_name,
                    error = %err,
                    "node_drain: could not read pod for the pod-less give-up"
                );
                PodPresenceStatus::Unknown
            }
        }
    }
}

/// Uncached pod reads one give-up keeps in flight at once.
pub(crate) const POD_READ_CONCURRENCY: usize = 8;

/// `Ok(())` only when every name in `refs` reads as missing or terminal. The
/// error is the hold. An empty set never
/// proves absence: with nothing to read, a pod the database doesn't know about
/// can't be ruled out.
pub(crate) async fn pods_proven_absent<P: PodAbsenceProber>(
    prober: &P,
    refs: &[PodRef],
) -> Result<(), PodLessHold> {
    if refs.is_empty() {
        return Err(PodLessHold::NoPodName);
    }
    // The reads are independent, so a sandbox with many generations doesn't
    // pay their latencies end to end. `buffered` yields answers in the
    // newest-first order, so a blocking answer ends the probe once the reads
    // ahead of it have answered, and dropping the stream stops the rest.
    // Boxed with an explicit `Send` bound: left to inference, the mapping
    // closure is higher-ranked over the borrow and the sink's future loses
    // its `Send` proof. Futures are lazy, so collecting them starts no read.
    let reads: Vec<Pin<Box<dyn Future<Output = PodPresenceStatus> + Send + '_>>> = refs
        .iter()
        .map(|pod| Box::pin(prober.probe(&pod.pod_namespace, &pod.pod_name)) as _)
        .collect();
    let mut reads = futures_util::stream::iter(reads).buffered(POD_READ_CONCURRENCY);
    while let Some(status) = reads.next().await {
        match status {
            PodPresenceStatus::Missing
            | PodPresenceStatus::TerminalSucceeded
            | PodPresenceStatus::TerminalFailed => {}
            PodPresenceStatus::Present | PodPresenceStatus::Unready => {
                return Err(PodLessHold::PodPresent);
            }
            PodPresenceStatus::Unknown => return Err(PodLessHold::PodUnreadable),
        }
    }
    Ok(())
}

/// Every pod name the sandbox's execution rows recorded, newest first: a
/// live pod is likeliest under the newest name, so its answer comes first.
pub(crate) async fn pod_refs(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Vec<PodRef>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT generation, pod_namespace, pod_name
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await
}

/// The row-state guards, read once before the pod reads and again under the
/// row locks.
#[derive(Debug, Clone, Copy, sqlx::FromRow)]
struct Guards {
    /// Still the resident the suspend failed on: the lifecycle the claim
    /// reverted to, on this node, no evacuation marker, and the node still
    /// force-draining (gentle mode doesn't touch residents).
    row_matches: bool,
    /// A node publishes the sandbox as anything but a `failed`, pid-less
    /// record.
    vm_observed: bool,
    /// An active execution carries a pod UID, or any execution a pod IP.
    pod_bound: bool,
    /// An execution advanced `updated_at` within the staleness window.
    recent_progress: bool,
    /// A durable suspend snapshot passes [`failed_repair_revert_eligibility`].
    restorable: bool,
    /// Reverting now loses nothing newer and restores the snapshot the
    /// eligibility proved current:
    /// [`pod_less_drain_revert_loses_no_newer_bytes`] and
    /// [`newest_full_snapshot_unambiguous`] hold.
    revert_safe: bool,
    max_generation: Option<ExecutionGeneration>,
}

impl Guards {
    /// The newest generation, which the transaction must still see and the
    /// node is asked about, when every guard passes; else the
    /// hold.
    fn generation(self) -> Result<ExecutionGeneration, PodLessHold> {
        let Some(generation) = self.max_generation else {
            return Err(if self.row_matches {
                PodLessHold::NoExecutionRow
            } else {
                PodLessHold::RowDrifted
            });
        };
        if !self.row_matches {
            Err(PodLessHold::RowDrifted)
        } else if self.vm_observed {
            Err(PodLessHold::VmObserved)
        } else if self.pod_bound {
            Err(PodLessHold::PodBound)
        } else if self.recent_progress {
            Err(PodLessHold::RecentProgress)
        } else if !self.restorable {
            // Read here so a row the revert can't take never reaches the pod
            // reads or the node; the revert re-checks both in its own
            // statement.
            Err(PodLessHold::NoRestoreSource)
        } else if !self.revert_safe {
            Err(PodLessHold::HeldForDataSafety)
        } else {
            Ok(generation)
        }
    }
}

/// The parameters every guard read binds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PodLessCandidate {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) source_node_id: NodeId,
    pub(crate) pre_lifecycle: SandboxLifecycle,
    pub(crate) stale: PositiveDuration,
}

impl PodLessCandidate {
    fn stale_secs(self) -> i64 {
        i64::try_from(self.stale.get().as_secs()).unwrap_or(i64::MAX)
    }
}

async fn read_guards<'e, E>(
    executor: E,
    candidate: PodLessCandidate,
) -> Result<Option<Guards>, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    let query = format!(
        "SELECT COALESCE((
                    s.lifecycle = $3
                    AND s.active_node_id = $2
                    AND s.evacuation_restore_target IS NULL
                    AND s.managed_by = $5
                    AND EXISTS (
                        SELECT 1
                          FROM nodes n
                         WHERE n.id = $2
                           AND n.drain_phase = $6
                           AND n.drain_mode = $7
                    )
                ), FALSE) AS row_matches,
                EXISTS (
                    SELECT 1
                      FROM sandbox_observed_states o
                     WHERE o.sandbox_id = s.id
                       AND (o.status IS DISTINCT FROM 'failed' OR o.pid IS NOT NULL)
                ) AS vm_observed,
                EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND (
                           e.pod_ip IS NOT NULL
                           OR (e.active AND NULLIF(BTRIM(e.pod_uid), '') IS NOT NULL)
                       )
                ) AS pod_bound,
                EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.updated_at > NOW() - make_interval(secs => $4::BIGINT)
                ) AS recent_progress,
                COALESCE({eligible}, FALSE) AS restorable,
                COALESCE({loses_no_newer_bytes} AND {unambiguous}, FALSE) AS revert_safe,
                (
                    SELECT MAX(e.generation)
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                ) AS max_generation
           FROM sandboxes s
          WHERE s.id = $1",
        eligible = failed_repair_revert_eligibility("s").eligible,
        loses_no_newer_bytes = pod_less_drain_revert_loses_no_newer_bytes("s"),
        unambiguous = newest_full_snapshot_unambiguous("s"),
    );
    sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(candidate.sandbox_id)
        .bind(candidate.source_node_id)
        .bind(candidate.pre_lifecycle.as_str())
        .bind(candidate.stale_secs())
        .bind(SubstrateKind::Kubernetes)
        .bind(PHASE_DRAINING)
        .bind(NodeDrainMode::Forced)
        .fetch_optional(executor)
        .await
}

/// Every guard, evaluated once. Run lock-free before any pod read, and again
/// inside the transaction under the row locks, which must find the same shape.
pub(crate) async fn precheck<'e, E>(
    executor: E,
    candidate: PodLessCandidate,
) -> Result<Result<ExecutionGeneration, PodLessHold>, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    Ok(read_guards(executor, candidate)
        .await?
        .map_or(Err(PodLessHold::RowDrifted), Guards::generation))
}

/// The source node's teardown record for one generation, behind a seam so the
/// decision can be tested without a node.
pub(crate) trait GenerationTeardownObserver: Sync {
    fn observe(
        &self,
        generation: ExecutionGeneration,
    ) -> impl Future<Output = Result<GenerationTeardownEvidence, SubstrateError>> + Send;
}

/// `Ok(())` when the node vouches that the sandbox's local state is gone: a
/// local delete receipt covers `newest`, the newest generation the database
/// knows. Anything else is the hold.
///
/// Only the newest generation is asked. A receipt covers the generation it
/// names and every older one, so a delete of the newest answers for all of
/// them, while a delete of an older one says nothing about a newer
/// generation that may have run without the database recording its pod.
pub(crate) async fn node_vouches<O: GenerationTeardownObserver>(
    observer: &O,
    newest: ExecutionGeneration,
) -> Result<(), PodLessHold> {
    match observer.observe(newest).await {
        Ok(GenerationTeardownEvidence::Deleted) => Ok(()),
        Ok(GenerationTeardownEvidence::Pending) => Err(PodLessHold::NodePending),
        Ok(GenerationTeardownEvidence::Durable) => Err(PodLessHold::NodeSuspended),
        Ok(GenerationTeardownEvidence::Superseded) => Err(PodLessHold::NodeSuperseded),
        Ok(GenerationTeardownEvidence::Refused) => Err(PodLessHold::NodeRefused),
        Err(_) => Err(PodLessHold::NodeUnreadable),
    }
}

/// The give-up's write: re-check every guard under the row locks, then revert
/// the row and retire its executions.
///
/// Call only once [`node_vouches`] has. `expected` is the newest generation
/// seen before the pod reads, the one the node vouched for. A newer one
/// means a pod may have been created since, unread, that the node didn't
/// vouch for, so the transaction backs out.
///
/// Locks the node row (shared), then the sandbox row, then every execution
/// row. Node before sandbox is the order pod placement and the drained-node
/// terminator take; sandbox before execution is started recovery's. A pod
/// watcher persisting pod metadata locks the execution row, so it either
/// committed first (the row then shows a pod UID or fresh `updated_at`, and
/// this backs out) or waits for this commit and finds its execution retired.
pub(crate) async fn recover_pod_less_sandbox_tx(
    pool: &PgPool,
    candidate: PodLessCandidate,
    expected: ExecutionGeneration,
) -> Result<PodLessRecovery, sqlx::Error> {
    match recover_pod_less_sandbox_locked(pool, candidate, expected).await {
        // A lock wait past the bound is contention, not a fault: the next tick
        // retries without a warning.
        // So is losing a deadlock. A node's publish can lock the observed row
        // before the sandbox row, the reverse of this order; Postgres aborts
        // whichever side it picks, and when that's the publish, the node
        // republishes on its next cycle.
        Err(error) if is_lock_timeout(&error) || is_deadlock(&error) => {
            Ok(PodLessRecovery::NotEligible(PodLessHold::LockContended))
        }
        result => result,
    }
}

async fn recover_pod_less_sandbox_locked(
    pool: &PgPool,
    candidate: PodLessCandidate,
    expected: ExecutionGeneration,
) -> Result<PodLessRecovery, sqlx::Error> {
    let mut tx = pool.begin().await?;
    // Each lock wait is bounded like drain completion's lock on the same row.
    // Once it holds the node row shared, every writer of that row (pod
    // placement, inventory, bootstrap) waits on this transaction. The bound
    // applies to each lock acquisition, the node row's included, so the worst
    // case is 5s for every contended row it locks: the sandbox, its
    // executions, and the rows its writes touch (slot, exposures, observed).
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(DRAIN_COMPLETION_LOCK_TIMEOUT.as_millis().to_string())
        .execute(&mut *tx)
        .await?;
    // The guards' drain-phase and mode test is a plain read, which can't
    // authorize a later write: an `:undrain` committing between it and the
    // arm's `UPDATE` would go unseen. The shared lock makes the undrain wait
    // for this commit, or, if it committed first, the guard read below sees
    // it. The drained-node terminator's flip fences the same way.
    sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR SHARE"))
        .bind(candidate.source_node_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(candidate.sandbox_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!(
        "SELECT 1
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation
            FOR UPDATE"
    ))
    .bind(candidate.sandbox_id)
    .execute(&mut *tx)
    .await?;

    let eligible = precheck(&mut *tx, candidate).await?.and_then(|generation| {
        if generation == expected {
            Ok(())
        } else {
            Err(PodLessHold::GenerationChanged)
        }
    });
    if let Err(hold) = eligible {
        tx.rollback().await?;
        return Ok(PodLessRecovery::NotEligible(hold));
    }

    let Some(finalization) = revert_to_suspended(&mut tx, candidate.sandbox_id).await? else {
        // The guards just passed under the locks, so only a snapshot change
        // that the locks don't cover gets here.
        tx.rollback().await?;
        return Ok(PodLessRecovery::NotEligible(PodLessHold::SnapshotChanged));
    };

    retire_executions(&mut tx, candidate.sandbox_id, &finalization).await?;
    WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut tx, candidate.sandbox_id)
        .await
        .map_err(|err| match err {
            PoolError::Sql(err) => err,
            other => sqlx::Error::Protocol(format!("delete claimed pool slot: {other}")),
        })?;
    reset_open_port_exposures(&mut tx, candidate.sandbox_id, &finalization).await?;
    // The observed guard tolerated a `failed`, pid-less record. Left behind,
    // it would keep a reverted row reading `failed` through the observed-state
    // overlay, and once the node is gone nothing would publish over it.
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states
          WHERE sandbox_id = $1 AND status = 'failed' AND pid IS NULL"
    ))
    .bind(candidate.sandbox_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(PodLessRecovery::RevertedToSuspended)
}

/// The revert. Matches nothing unless a durable suspend snapshot passes
/// [`failed_repair_revert_eligibility`] and both
/// [`pod_less_drain_revert_loses_no_newer_bytes`] and
/// [`newest_full_snapshot_unambiguous`] hold, all evaluated by this
/// statement: the same predicates the precheck reads.
async fn revert_to_suspended(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
) -> Result<Option<SuspendedK8sFinalizationSql>, sqlx::Error> {
    let eligibility = failed_repair_revert_eligibility("s");
    let marker_assignments =
        clear_current_local_artifact_marker_assignments("s", "pre.clear_marker");
    let finalization = suspended_k8s_finalization_sql(
        "s",
        "pre",
        &marker_assignments,
        SuspendedTargetNodeAssignment::Clear,
    );
    let query = format!(
        "WITH pre AS (
             SELECT id,
                    {clear_marker} AS clear_marker
               FROM sandboxes s
              WHERE s.id = $1
         )
         UPDATE sandboxes s
            SET {assignments}
           FROM pre
          WHERE s.id = pre.id
            AND {eligible}
            AND {loses_no_newer_bytes}
            AND {unambiguous}",
        clear_marker = eligibility.clear_marker,
        assignments = finalization.sandbox_assignments(),
        eligible = eligibility.eligible,
        loses_no_newer_bytes = pod_less_drain_revert_loses_no_newer_bytes("s"),
        unambiguous = newest_full_snapshot_unambiguous("s"),
    );
    let reverted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    Ok((reverted > 0).then_some(finalization))
}

async fn retire_executions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    finalization: &SuspendedK8sFinalizationSql,
) -> Result<(), sqlx::Error> {
    let query = format!(
        "UPDATE sandbox_k8s_executions e
            SET {assignments}
          WHERE e.sandbox_id = $1
            AND e.active",
        assignments = finalization.active_execution_assignments("e"),
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Postgres `40P01` (`deadlock_detected`).
pub(crate) fn is_deadlock(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "40P01")
}

async fn reset_open_port_exposures(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    finalization: &SuspendedK8sFinalizationSql,
) -> Result<(), sqlx::Error> {
    let query = format!(
        "UPDATE sandbox_port_exposures
            SET {assignments}
          WHERE sandbox_id = $1
            AND desired_state = 'open'",
        assignments = finalization.open_port_exposure_assignments(),
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
