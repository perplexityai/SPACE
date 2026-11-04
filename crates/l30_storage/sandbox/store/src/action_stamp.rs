//! Post-action stamps: best-effort writes of a lifecycle action's
//! user-facing result onto `sandboxes`, made by the api's lifecycle
//! dispatcher after the substrate call succeeds (for example `paused` after
//! a pause, or `started`/`on_node` after a start). They are not repairs made
//! while answering a read.
//!
//! Both verbs share one guard: they refuse a row that is `failed`,
//! `quarantined`, `deleted` or `cleanup_required`. That exclusion is the
//! safety contract with the transient-lifecycle sweeper: if the sweeper
//! failed the row (and tore its execution down) while the substrate call was
//! in flight, a later stamp must not resurrect it as live. Nothing else is
//! re-checked, so a stamp overwrites any other stored lifecycle, including
//! `deleting`, `stopping`, `suspending` and `suspended`, which another intent
//! may have written while the substrate call was in flight. This module keeps
//! that behaviour exactly as the inline statements had it; the lifecycle
//! characterization tests pin it as a known defect, and a later behaviour
//! change fixes it.
//!
//! A refused stamp matches zero rows and is not reported; the callers treat
//! these writes as best-effort and only log a database error.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;

/// **Operation step.** Stamp `lifecycle` onto the row, ending a lifecycle
/// action, unless the row is terminal (see the module docs for the guard and
/// what it leaves unguarded). `lifecycle` is bound as given; the caller
/// passes a stored lifecycle value.
pub async fn stamp_action_lifecycle(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
    lifecycle: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $1
          WHERE id = $2
            AND lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')"
    ))
    .bind(lifecycle)
    .bind(id)
    .execute(executor)
    .await?;
    Ok(())
}

/// **Operation step.** Stamp a successful start, `started` and `on_node`,
/// onto the row unless it is terminal, dropping a `disk` artifact pin and
/// keeping a `full` one. Same guard as [`stamp_action_lifecycle`]; it does
/// not touch the node assignment. The api's `maybe_mark_started_after_start`
/// documents why the `disk` pin goes.
pub async fn stamp_started_after_start(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started',
                placement = 'on_node',
                current_local_artifact_id = CASE
                    WHEN current_local_artifact_kind = 'disk' THEN NULL
                    ELSE current_local_artifact_id
                END,
                current_local_artifact_kind = CASE
                    WHEN current_local_artifact_kind = 'disk' THEN NULL
                    ELSE current_local_artifact_kind
                END,
                current_local_artifact_origin = CASE
                    WHEN current_local_artifact_kind = 'disk' THEN NULL
                    ELSE current_local_artifact_origin
                END
          WHERE id = $1
            AND lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')"
    ))
    .bind(id)
    .execute(executor)
    .await?;
    Ok(())
}
