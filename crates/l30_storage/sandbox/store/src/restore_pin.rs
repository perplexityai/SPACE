//! The operator's way out of a refused restore: list the captures a sandbox
//! could restore from, and pin one.
//!
//! An unpinned sandbox whose history is unordered
//! ([`pplx_space_sandbox_portability::restore_order_unordered`]) refuses to
//! restore off its node, since capture order can't tell which of its
//! captures is the latest. An operator who knows which capture holds the
//! sandbox's latest state pins it here, and the restore loads exactly that
//! artifact.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    capture_registered_at, pinnable_pre_claim_suspend_capture, pinnable_suspend_capture,
    pre_claim_restore_order_unordered, restore_order_unordered, restoring_row_accepts_operator_pin,
};
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;

/// A sandbox's restore state, as an operator choosing a pin sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestorePinState {
    pub lifecycle: String,
    /// The token [`pin_restore_source`] requires unchanged.
    pub lifecycle_changed_at: Option<time::OffsetDateTime>,
    pub pinned_artifact: Option<LocalArtifactId>,
    /// The node a restore would load the local copy from, resolved as the
    /// Kubernetes restore resolves its source node: the live worker on the
    /// host the placement (`target_node_id`, else `active_node_id`) maps to,
    /// else on the host the newest execution named. A retire that keeps the
    /// sandbox recoverable clears the placement but keeps that execution, and
    /// a node holding a local copy restores it in place of any pinned backup.
    /// Falls back to the placement for a worker with no Kubernetes mapping.
    pub restore_node_id: Option<NodeId>,
    /// The unpinned restore refuses: its captures can't be ordered.
    pub unordered: bool,
    /// When `lifecycle` is `restoring`, whether [`pin_restore_source`] accepts a
    /// pre-claim capture ([`restoring_row_accepts_operator_pin`]).
    pub restoring_pinnable: bool,
    /// The captures [`pin_restore_source`] accepts, latest first registration
    /// first.
    pub captures: Vec<PinnableCapture>,
}

/// One capture an operator may pin: a full suspend artifact with a ready
/// backup and a live location, that no capture outranks and no later
/// capture of the same rank follows.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct PinnableCapture {
    pub local_artifact_id: LocalArtifactId,
    /// The capture generation its backups record, if any: the one any backup
    /// of the artifact records, as [`capture_rank`] reads it.
    pub generation: Option<i64>,
    /// When its first backup registered.
    pub first_registered_at: time::OffsetDateTime,
    /// The node its newest ready backup was uploaded from.
    pub source_node_id: Option<NodeId>,
}

#[derive(sqlx::FromRow)]
struct StateRow {
    lifecycle: String,
    lifecycle_changed_at: Option<time::OffsetDateTime>,
    current_local_artifact_id: Option<LocalArtifactId>,
    restore_node_id: Option<NodeId>,
    unordered: bool,
    restoring_pinnable: bool,
}

/// The node a restore of `sandbox_alias` would load a local copy from, as SQL.
///
/// Kubernetes resolves a host name, as `restore_source_node_name` does: the
/// one the placement (`target_node_id`, else `active_node_id`) maps to, whatever
/// state that registration is in, else the one the newest execution named. The
/// node is the live worker registered under that name, as `k8s_worker_ready`
/// picks it. A builder on the same host has its own row under the name and
/// holds no sandbox, and a worker that registered again has a new row under it.
///
/// `kubernetes` is the bind placeholder of [`SubstrateKind::Kubernetes`], the
/// substrate whose nodes carry that mapping.
///
/// A placement with no Kubernetes mapping and no execution to name a host is
/// its own node, as the Direct restore reads it: the worker `target_node_id`
/// names, or `active_node_id` when there's no target. A target that's gone
/// isn't replaced by `active_node_id`.
fn restore_node_id_sql(sandbox_alias: &str, kubernetes: &str) -> String {
    format!(
        "COALESCE(\n\
             (SELECT nk.node_id\n\
                FROM node_k8s nk\n\
                JOIN nodes n ON n.id = nk.node_id\n\
               WHERE nk.k8s_node_name = COALESCE(\n\
                         (SELECT placed.k8s_node_name\n\
                            FROM node_k8s placed\n\
                           WHERE placed.node_id = COALESCE({sandbox_alias}.target_node_id, {sandbox_alias}.active_node_id)),\n\
                         (SELECT ske.k8s_node_name\n\
                            FROM sandbox_k8s_executions ske\n\
                           WHERE ske.sandbox_id = {sandbox_alias}.id\n\
                             AND ske.k8s_node_name IS NOT NULL\n\
                           ORDER BY ske.generation DESC\n\
                           LIMIT 1))\n\
                 AND n.managed_by = {kubernetes}\n\
                 AND n.role = 'worker'\n\
                 AND COALESCE(n.state, 'registered') != 'deleted'\n\
               ORDER BY COALESCE(n.state, 'registered') = 'ready' DESC, nk.updated_at DESC, n.created_at DESC\n\
               LIMIT 1),\n\
             (SELECT n.id\n\
                FROM nodes n\n\
               WHERE n.id = COALESCE({sandbox_alias}.target_node_id, {sandbox_alias}.active_node_id)\n\
                 AND n.role = 'worker'\n\
                 AND COALESCE(n.state, 'registered') != 'deleted'\n\
                 AND NOT EXISTS (\n\
                     SELECT 1 FROM node_k8s mapped WHERE mapped.node_id = n.id))\n\
         )"
    )
}

/// Read `sandbox_id`'s restore state and the captures an operator may pin.
/// `None` for a missing sandbox.
pub async fn restore_pin_state(
    executor: impl sqlx::PgExecutor<'_> + Copy,
    sandbox_id: SandboxId,
) -> Result<Option<RestorePinState>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static STATE: LazyLock<String> = LazyLock::new(|| {
        let restore_node_id = restore_node_id_sql("s", "$2");
        let unordered = restore_order_unordered("s");
        let pre_claim_unordered = pre_claim_restore_order_unordered("s");
        let restoring_pinnable = restoring_row_accepts_operator_pin("s");
        sql!(format!(
            "SELECT s.lifecycle, s.lifecycle_changed_at, s.current_local_artifact_id,
                    {restore_node_id} AS restore_node_id,
                    CASE WHEN s.lifecycle = 'restoring' THEN {pre_claim_unordered}
                         ELSE {unordered}
                    END AS unordered,
                    {restoring_pinnable} AS restoring_pinnable
               FROM sandboxes s
              WHERE s.id = $1"
        ))
    });
    /// Built once: every input is a literal or a bind placeholder.
    static CAPTURES: LazyLock<String> = LazyLock::new(|| {
        let pinnable = pinnable_suspend_capture("s", "sn");
        let pinnable_pre_claim = pinnable_pre_claim_suspend_capture("s", "sn");
        let captured = capture_registered_at("sn");
        sql!(format!(
            "SELECT DISTINCT ON (sn.local_artifact_id)
                    sn.local_artifact_id,
                    COALESCE(sn.generation, (
                        SELECT MIN(same_artifact.generation)
                          FROM snapshots same_artifact
                         WHERE same_artifact.sandbox_id = s.id
                           AND same_artifact.type = sn.type
                           AND same_artifact.local_artifact_id = sn.local_artifact_id
                           AND same_artifact.artifact_kind IS NOT DISTINCT FROM sn.artifact_kind
                           AND same_artifact.generation IS NOT NULL
                    )) AS generation,
                    {captured} AS first_registered_at,
                    sn.source_node_id
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
              WHERE s.id = $1
                AND CASE WHEN s.lifecycle = 'restoring' THEN {pinnable_pre_claim}
                         ELSE {pinnable}
                    END
              ORDER BY sn.local_artifact_id, sn.timestamp DESC, sn.id DESC"
        ))
    });
    let Some(state) = sqlx::query_as::<_, StateRow>(sqlx::AssertSqlSafe(STATE.as_str()))
        .bind(sandbox_id)
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(executor)
        .await?
    else {
        return Ok(None);
    };
    let mut captures: Vec<PinnableCapture> = sqlx::query_as(sqlx::AssertSqlSafe(CAPTURES.as_str()))
        .bind(sandbox_id)
        .fetch_all(executor)
        .await?;
    captures.sort_by_key(|capture| std::cmp::Reverse(capture.first_registered_at));
    Ok(Some(RestorePinState {
        lifecycle: state.lifecycle,
        lifecycle_changed_at: state.lifecycle_changed_at,
        pinned_artifact: state.current_local_artifact_id,
        restore_node_id: state.restore_node_id,
        unordered: state.unordered,
        restoring_pinnable: state.restoring_pinnable,
        captures,
    }))
}

/// **Guarded write.** Pin `sandbox_id` to `artifact`, a capture an operator
/// chose, and return whether it did.
///
/// It pins an unpinned row whose `lifecycle_changed_at` is still
/// `expected_lifecycle_changed_at` — the value the operator read with the
/// captures (a sandbox that changed lifecycle since may hold a later state) —
/// and whose restore node still resolves to `expected_restore_node_id` (the
/// node the caller asked for a local copy, if any), either while `suspended`,
/// or while `restoring` with no active owner and a pinnable pre-claim capture
/// ([`restoring_row_accepts_operator_pin`]), and only to a capture
/// [`restore_pin_state`] lists: one the selector could take, so a pin can't
/// restore a capture another one provably follows. Updating
/// `current_local_artifact_*` alone keeps `lifecycle = 'restoring'` and leaves
/// `lifecycle_changed_at` untouched, so any post-claim snapshot remains fenced.
/// Otherwise it writes nothing.
///
/// Writes an **assertion**: the operator's word that `artifact` holds the
/// sandbox's latest state. Of the captures listed, only they can tell.
///
/// A node that still holds the sandbox suspended restores its own local copy
/// whatever the pin names, so the caller asks that node first, and pins only
/// its copy, or when it holds none.
pub async fn pin_restore_source(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    artifact: LocalArtifactId,
    expected_lifecycle_changed_at: time::OffsetDateTime,
    expected_restore_node_id: Option<NodeId>,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static PIN: LazyLock<String> = LazyLock::new(|| {
        let restore_node_id = restore_node_id_sql("s", "$5");
        let pinnable = pinnable_suspend_capture("s", "sn");
        let pinnable_pre_claim = pinnable_pre_claim_suspend_capture("s", "sn");
        let restoring_pinnable = restoring_row_accepts_operator_pin("s");
        sql!(format!(
            "UPDATE sandboxes s
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE s.id = $1
                AND s.lifecycle_changed_at = $3
                AND ({restore_node_id}) IS NOT DISTINCT FROM $4
                AND s.current_local_artifact_id IS NULL
                AND (
                    (s.lifecycle = 'suspended'
                     AND EXISTS (
                         SELECT 1
                           FROM snapshots sn
                          WHERE sn.local_artifact_id = $2
                            AND {pinnable}
                     ))
                    OR
                    ({restoring_pinnable}
                     AND EXISTS (
                         SELECT 1
                           FROM snapshots sn
                          WHERE sn.local_artifact_id = $2
                            AND {pinnable_pre_claim}
                     ))
                )"
        ))
    });
    let result = sqlx::query(sqlx::AssertSqlSafe(PIN.as_str()))
        .bind(sandbox_id)
        .bind(artifact.as_uuid())
        .bind(expected_lifecycle_changed_at)
        .bind(expected_restore_node_id)
        .bind(SubstrateKind::Kubernetes)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests;
