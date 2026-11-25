//! Guarded writes, and the give-up's candidate lock, for sandboxes given up
//! as lost because the node holding their only copy is gone, and the scan
//! that finds the `started` ones ([`started_lost_candidates`]).

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::{
    inflight_suspend_upload_from_other_node_exists, snapshot_durable_for_source_release,
    started_restore_source_absent,
};
use pplx_space_sql_locator::sql;

/// Quiet period a same-node `creating` snapshot row must have sat without a
/// worker heartbeat (`COALESCE(last_attempt_at, timestamp)`) before the
/// mark-lost give-up treats it as provably dead. EC2 `terminated` already
/// proves the upload *worker* died with the instance — what it cannot bound
/// is a `FinalizeArtifactBackup(READY)` RPC that left the node before the
/// instance died and is still queued in the API path with all bytes already
/// durable in S3. Such an RPC lives for seconds (gRPC deadline), so ten
/// quiet minutes is overwhelmingly past it: a row still inside the window
/// defers the flip (the sandbox stays parked and the next ticks re-check);
/// one quiet for longer can no longer become `ready` and is finalized
/// `failed` so the reap converges.
pub const DEAD_UPLOAD_SETTLE: Duration = Duration::from_mins(10);

/// A `started` Kubernetes sandbox whose recovery failed on a source node that
/// may be gone, from [`started_lost_candidates`].
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StartedLostCandidate {
    #[sqlx(rename = "id")]
    pub sandbox_id: SandboxId,
    /// The active `recovery_failed` execution's generation.
    pub generation: i64,
    /// The source node, `sandboxes.active_node_id`.
    pub active_node_id: NodeId,
    /// Whether the source node's `nodes` row still exists.
    pub node_row_exists: bool,
    pub k8s_node_name: Option<String>,
    pub k8s_node_uid: Option<String>,
}

/// One keyset page, ordered by id, of the candidates for
/// [`mark_started_lost`]: at most `limit` rows strictly after `after`
/// (`None` starts a scan), each with its source node's `nodes` and
/// `node_k8s` identity when those rows exist. `dead_upload_settle` is the
/// settle window of [`started_restore_source_absent`].
///
/// The execution must track no pod. A recovery that materialized a
/// replacement pod records its identity before the pod's placement moves
/// `active_node_id`, so a tracked pod may be live on another node; the
/// failed-recovery convergence likewise waits for an authoritative absence
/// check to clear it.
///
/// A sandbox whose source node is in any drain phase is left to `node_drain`
/// and `drained_node_terminator`, which own residents of a drained node. The
/// restore-source condition is the one the flip re-checks
/// ([`started_restore_source_absent`]).
///
/// A plain read; the settle window compares with `NOW()`, so it reads the
/// clock of the executor's transaction.
pub async fn started_lost_candidates(
    executor: impl Executor<'_>,
    after: Option<SandboxId>,
    limit: u32,
    dead_upload_settle: Duration,
) -> Result<Vec<StartedLostCandidate>, DbError> {
    /// Built once: every input is a literal or a bind placeholder.
    static LIST_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        let no_restore_source = started_restore_source_absent("s", "$4");
        sql!(
            format!(
                "SELECT s.id,
                        e.generation,
                        s.active_node_id,
                        n.id IS NOT NULL AS node_row_exists,
                        nk.k8s_node_name,
                        nk.k8s_node_uid
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e
                     ON e.sandbox_id = s.id
                    AND e.active
                    AND e.state = 'recovery_failed'
                    AND NULLIF(BTRIM(e.pod_uid), '') IS NULL
                    AND e.pod_ip IS NULL
                    AND e.k8s_node_name IS NULL
                   LEFT JOIN nodes n ON n.id = s.active_node_id
                   LEFT JOIN node_k8s nk ON nk.node_id = n.id
                  WHERE s.managed_by = $1
                    AND s.lifecycle = 'started'
                    AND s.active_node_id IS NOT NULL
                    AND (n.id IS NULL OR (n.managed_by = $1 AND n.drain_phase IS NULL))
                    AND ($2::UUID IS NULL OR s.id > $2)
                    AND {no_restore_source}
                  ORDER BY s.id
                  LIMIT $3"
            )
            .as_str()
        )
    });
    Ok(
        sqlx::query_as(sqlx::AssertSqlSafe(LIST_CANDIDATES.as_str()))
            .bind(SubstrateKind::Kubernetes)
            .bind(after)
            .bind(i64::from(limit))
            .bind(dead_upload_settle.as_secs().cast_signed())
            .fetch_all(executor.into_sqlx())
            .await?,
    )
}

/// The guarded flip of `sandboxes` from `started` to `failed`. Returns
/// whether the row flipped.
///
/// Every condition the give-up decided on is re-checked in the `WHERE`: the
/// row is still a `started` Kubernetes sandbox on `source_node_id`, its active
/// execution is still the `recovery_failed` one at `generation` (a wake that
/// re-armed it to `recovering`, or replaced it, refuses) and still tracks no
/// pod, and no snapshot
/// could restore it without a rollback ([`started_restore_source_absent`],
/// with `dead_upload_settle` as its settle window).
///
/// The caller owns everything around it: the sandbox row lock, the re-check
/// that the node is still gone, and, in the same transaction, the write-off:
/// `pplx_space_snapshot_store::lost::fail_quiet_full_uploads_from_node`, then
/// [`write_off_lost_sandboxes`].
///
/// The caller must pass its open transaction.
pub async fn mark_started_lost<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    generation: i64,
    dead_upload_settle: Duration,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    /// Built once: every input is a literal or a bind placeholder.
    static MARK_STARTED_LOST: LazyLock<String> = LazyLock::new(|| {
        let no_restore_source = started_restore_source_absent("s", "$4");
        sql!(
            format!(
                "UPDATE sandboxes s
                    SET lifecycle = 'failed'
                  WHERE s.id = $1
                    AND s.managed_by = $5
                    AND s.lifecycle = 'started'
                    AND s.active_node_id = $2
                    AND EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.active
                           AND e.state = 'recovery_failed'
                           AND e.generation = $3
                           AND NULLIF(BTRIM(e.pod_uid), '') IS NULL
                           AND e.pod_ip IS NULL
                           AND e.k8s_node_name IS NULL
                    )
                    AND {no_restore_source}"
            )
            .as_str()
        )
    });
    let flipped = sqlx::query(sqlx::AssertSqlSafe(MARK_STARTED_LOST.as_str()))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(generation)
        .bind(dead_upload_settle.as_secs().cast_signed())
        .bind(SubstrateKind::Kubernetes)
        .execute(tx.into_sqlx())
        .await?
        .rows_affected();
    Ok(flipped == 1)
}

/// Unplace sandboxes just flipped to `failed` as lost: `unplaced` with no
/// `active_node_id`, so the rows stop pinning the gone node. `target_node_id`
/// is kept as the last place the sandbox lived, and `placement_changed_at`
/// isn't bumped, since it tracks placement for snapshot freshness and the
/// rows are terminal.
///
/// Guarded only by `lifecycle = 'failed'`: the caller flips the rows in the
/// same transaction and holds their locks.
async fn release_lost_placement<C>(tx: &mut C, ids: &[SandboxId]) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'unplaced',
                active_node_id = NULL
          WHERE id = ANY($1)
            AND lifecycle = 'failed'"
    ))
    .bind(ids)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Clean up after `ids` were flipped to `failed` in this transaction because
/// `node_id` is gone. Run it after the flip, in the flip's transaction, and
/// after `pplx_space_snapshot_store::lost::fail_quiet_full_uploads_from_node`
/// has failed the node's dead uploads.
///
/// - **The backup queue.** A gone node sends no census to replace its
///   `node_local_backup_pending` set, and the table's only foreign key is on
///   `sandbox_id`, so the rows would stay until the sandbox row is deleted.
///   Scoped to `node_id`: a sandbox can hold a queue row on a live node too,
///   and that row is the live node's pending work. Lock order is
///   sandboxes-then-queue, while the census publish takes the queue row first;
///   that can only deadlock against a census from the gone node itself.
/// - **Executions.** The active execution is written off as `forgotten`, the
///   pod-tracking write-off state: its pod, if one ran, died with the node.
/// - **Placement.** Released through `release_lost_placement`, so the row
///   stops pinning the gone node.
///
/// Port exposures are left alone: re-queueing them for a terminal sandbox
/// would only create work that can never apply.
///
/// The caller must pass its open transaction.
pub async fn write_off_lost_sandboxes<C>(
    tx: &mut C,
    node_id: NodeId,
    ids: &[SandboxId],
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "DELETE FROM node_local_backup_pending
          WHERE node_id = $1 AND sandbox_id = ANY($2)"
    ))
    .bind(node_id)
    .bind(ids)
    .execute((&mut *tx).into_sqlx())
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'forgotten',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE sandbox_id = ANY($1)
            AND active"
    ))
    .bind(ids)
    .execute((&mut *tx).into_sqlx())
    .await?;
    release_lost_placement(tx, ids).await
}

/// `$1` = node id, `$2` = the kube uid observed at the EC2 cross-check,
/// `$3` = the candidate ids the caller has already `FOR UPDATE`-claimed in
/// the same transaction (only those may flip — see the inline comment),
/// `$4` = the [`DEAD_UPLOAD_SETTLE`] quiet seconds for same-node
/// `creating` rows, `$5` = [`SubstrateKind::Kubernetes`]. Mark-lost give-up for the
/// KubeNodeGone recovery: flips every `suspended` sandbox pinned to the node
/// via `target_node_id` that has no recoverable copy anywhere to
/// `lifecycle = 'failed'`, returning the flipped ids.
///
/// The caller has already confirmed the node's backing EC2 instance is
/// **terminated**. That premise drives the two deliberate differences from
/// the drain's per-sandbox [`crate::drain::mark_sandbox_lost_query`]:
///
/// * No on-node `ArtifactMissing` probe precedes the flip — none is possible
///   (there is no node to ask) and none is needed: the local artifact died
///   with the instance, so "no durable snapshot" alone proves
///   unrecoverability.
/// * The in-flight-upload veto is scoped to uploads from **other** nodes
///   ([`inflight_suspend_upload_from_other_node_exists`]) instead of the
///   node-agnostic check: an upload still `creating` from the terminated
///   node can never finalize (the upload worker and its staged copies died
///   with the box), and letting that dead row veto the flip would re-create
///   the permanent park this give-up exists to break. An upload from any
///   still-live node may yet finalize `ready`, so it still vetoes.
///
/// The portability test is the same marker-aware
/// [`snapshot_durable_for_source_release`] the snapshot guard and
/// `mark_sandbox_lost_query` use, so "blocks the reap" and "eligible for
/// give-up" can't drift: a durably-portable suspended sandbox neither blocks
/// nor is flipped; a local-only one both blocks and (here) is flipped.
/// Mid-flight rows (`suspending` / `restoring`) are deliberately out of
/// scope — they keep blocking and stay parked for their own sweepers.
///
/// The `EXISTS` guards re-check the node's `drained` + Kubernetes-managed
/// phase under the v1 engine (`drain_engine IS NULL`: a v2 drain's node, or
/// one under an engine this build doesn't know, is never v1's to give up)
/// and the `node_k8s.k8s_node_uid` identity (`IS NOT DISTINCT FROM $2`)
/// **inside the UPDATE**, mirroring the terminator's
/// `drained_node::delete_if_orphan`: a same-name kube replacement or an
/// admin undrain that lands after the EC2
/// cross-check makes this match nothing, so the terminal flip can never
/// outlive the observation that justified it.
///
/// **These guards alone are snapshot reads, not serialization.** Unlike the
/// hard-DELETE — which modifies the `nodes` row itself and therefore
/// serializes on its row lock — this UPDATE only reads `nodes` / `node_k8s`,
/// so an undrain or uid bump committing concurrently would be invisible to
/// the statement snapshot. The caller (the terminator's
/// `NodeDbStore::mark_unrecoverable_sandboxes_lost`) closes that hole by
/// `FOR SHARE`-locking and re-checking both guard rows in the same
/// transaction before running this statement; run it only under that
/// protocol.
pub fn mark_unrecoverable_sandboxes_lost_query() -> String {
    let durable = snapshot_durable_for_source_release("sandboxes");
    let inflight_from_other_node =
        inflight_suspend_upload_from_other_node_exists("sandboxes", "$1");
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE lifecycle = 'suspended'
            AND target_node_id = $1
            -- Settle guard on same-node creating uploads ($4 = quiet
            -- seconds, see DEAD_UPLOAD_SETTLE): EC2 'terminated' proves the
            -- worker is dead but not that a READY finalize for already-
            -- durable bytes isn't still in the API path. A row heartbeated
            -- (or created) inside the window defers the flip; the sandbox
            -- keeps blocking the reap and later ticks re-check.
            AND NOT EXISTS (
                SELECT 1 FROM snapshots sn
                 WHERE sn.sandbox_id = sandboxes.id
                   AND sn.source_node_id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'creating'
                   AND COALESCE(sn.last_attempt_at, sn.timestamp)
                       >= NOW() - make_interval(secs => $4::BIGINT)
            )
            -- Only rows the caller FOR UPDATE-claimed may flip. Under READ
            -- COMMITTED the claim only fences rows that matched at its own
            -- snapshot; without this restriction a row entering the
            -- candidate set between the claim and this statement would flip
            -- un-fenced, re-opening the FK-lock race with a concurrent
            -- BeginArtifactBackup insert. Late arrivals wait for the next
            -- tick's claim instead.
            AND id = ANY($3)
            AND NOT {durable}
            AND NOT {inflight_from_other_node}
            AND EXISTS (
                SELECT 1 FROM nodes
                WHERE id = $1 AND drain_phase = 'drained' AND managed_by = $5
                  AND drain_engine IS NULL
            )
            AND EXISTS (
                SELECT 1 FROM node_k8s
                WHERE node_id = $1
                  AND k8s_node_uid IS NOT DISTINCT FROM $2
            )
          RETURNING id"
    )
}

/// `$1` = node id. Built once: the fragment is
/// [`snapshot_durable_for_source_release`] over a fixed alias.
///
/// The durability check is wrapped in a scalar subquery, `NOT (SELECT …)`: a
/// bare `NOT EXISTS` is pullable, and the planner may realise it as a hash or
/// merge anti-join over the whole `snapshots` ⋈ `snapshot_storage_locations`
/// product. Inside a scalar subquery it stays a per-row `SubPlan`, whose
/// `sn.sandbox_id = s.id` makes each execution an index probe. `FOR UPDATE`
/// locks the rows of its own `FROM` list only, so it locks `sandboxes` rows and
/// nothing else: the durability check is a sublink.
static LOCK_UNRECOVERABLE_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
    let durable = snapshot_durable_for_source_release("s");
    sql!(format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.lifecycle = 'suspended'
            AND s.target_node_id = $1
            AND NOT (SELECT {durable})
          ORDER BY s.id
          FOR UPDATE"
    ))
});

/// Lock the `suspended` sandboxes pinned to `node_id` that have no durable
/// copy, in `id` order, and return their ids: the drained-node terminator's
/// gone-node give-up claim, taken before [`mark_unrecoverable_sandboxes_lost`]
/// so the flip can act only on rows it claimed.
///
/// It claims only rows without a durable copy, because those are the only rows
/// [`mark_unrecoverable_sandboxes_lost`] can flip: the flip re-checks
/// `AND NOT {durable}` on the claimed ids. A row that loses durability after
/// the claim isn't claimed this tick; it still blocks the reap through the
/// drain-gate blockers, so the next tick's give-up claims it.
///
/// The `FOR UPDATE` row locks last until the caller's transaction ends. They
/// conflict with the `KEY SHARE` foreign-key lock a concurrent artifact backup
/// insert takes on the sandbox row, which the flip's own `NO KEY UPDATE` lock
/// does not.
///
/// The caller must pass its open transaction.
pub async fn lock_unrecoverable_candidates<C>(
    tx: &mut C,
    node_id: NodeId,
) -> Result<Vec<SandboxId>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(
        sqlx::query_scalar(sqlx::AssertSqlSafe(LOCK_UNRECOVERABLE_CANDIDATES.as_str()))
            .bind(node_id)
            .fetch_all(tx.into_sqlx())
            .await?,
    )
}

/// Fail the `claimed` `suspended` sandboxes pinned to a drained, gone node that
/// have no recoverable copy anywhere ([`mark_unrecoverable_sandboxes_lost_query`]),
/// returning the ids it flipped. `expected_uid` is the `node_k8s.k8s_node_uid`
/// the cross-check observed, and `dead_upload_settle` the quiet window for the
/// node's own `creating` uploads.
///
/// Writes an **inference**: the terminator's give-up on the cloud provider's
/// verdict. Its node guards are snapshot reads, so it runs only in the
/// transaction that claimed `claimed` ([`lock_unrecoverable_candidates`]) and
/// locked the `nodes` and `node_k8s` rows.
///
/// The caller must pass its open transaction.
pub async fn mark_unrecoverable_sandboxes_lost<C>(
    tx: &mut C,
    node_id: NodeId,
    expected_uid: Option<&str>,
    claimed: &[SandboxId],
    dead_upload_settle: Duration,
) -> Result<Vec<SandboxId>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let query = mark_unrecoverable_sandboxes_lost_query();
    Ok(
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(expected_uid)
            .bind(claimed)
            .bind(dead_upload_settle.as_secs().cast_signed())
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(tx.into_sqlx())
            .await?,
    )
}

#[cfg(test)]
mod giveup_claim_tests;

#[cfg(test)]
mod tests;
