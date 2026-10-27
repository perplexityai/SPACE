//! Guarded writes for sandboxes given up as lost because the node holding
//! their only copy is gone.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::started_restore_source_absent;
use sqlx::{Postgres, Transaction};

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
/// that the node is still gone, and the write-off of the
/// execution and placement (see [`release_lost_placement`]) in the same
/// transaction.
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
