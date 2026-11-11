//! Guarded writes for sandboxes given up as lost because the node holding
//! their only copy is gone.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    inflight_suspend_upload_from_other_node_exists, snapshot_durable_for_source_release,
    started_restore_source_absent,
};
use sqlx::{Postgres, Transaction};

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
pub async fn mark_started_lost(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    generation: i64,
    dead_upload_settle: Duration,
) -> Result<bool, sqlx::Error> {
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
        .execute(&mut **tx)
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
pub async fn release_lost_placement(
    tx: &mut Transaction<'_, Postgres>,
    ids: &[SandboxId],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'unplaced',
                active_node_id = NULL
          WHERE id = ANY($1)
            AND lifecycle = 'failed'"
    ))
    .bind(ids)
    .execute(&mut **tx)
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
/// - **Placement.** Released through [`release_lost_placement`], so the row
///   stops pinning the gone node.
///
/// Port exposures are left alone: re-queueing them for a terminal sandbox
/// would only create work that can never apply.
pub async fn write_off_lost_sandboxes(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    ids: &[SandboxId],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_local_backup_pending
          WHERE node_id = $1 AND sandbox_id = ANY($2)"
    ))
    .bind(node_id)
    .bind(ids)
    .execute(&mut **tx)
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
    .execute(&mut **tx)
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
/// phase and the `node_k8s.k8s_node_uid` identity (`IS NOT DISTINCT FROM
/// $2`) **inside the UPDATE**, mirroring the terminator's
/// `delete_drained_node_if_orphan_query`: a same-name kube replacement or an
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
            )
            AND EXISTS (
                SELECT 1 FROM node_k8s
                WHERE node_id = $1
                  AND k8s_node_uid IS NOT DISTINCT FROM $2
            )
          RETURNING id"
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
/// transaction that claimed `claimed` and locked the `nodes` and `node_k8s`
/// rows.
pub async fn mark_unrecoverable_sandboxes_lost(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    expected_uid: Option<&str>,
    claimed: &[SandboxId],
    dead_upload_settle: Duration,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let query = mark_unrecoverable_sandboxes_lost_query();
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(expected_uid)
        .bind(claimed)
        .bind(dead_upload_settle.as_secs().cast_signed())
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&mut **tx)
        .await
}

#[cfg(test)]
mod tests;
