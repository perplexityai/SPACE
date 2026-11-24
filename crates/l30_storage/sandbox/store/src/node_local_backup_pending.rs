//! `node_local_backup_pending`: the local artifacts each node reports it has
//! not yet backed up durably, one row per `(node, sandbox)`. The rows go with
//! their sandbox through the `sandbox_id` foreign key's cascade.
//!
//! The census mirrors a node's report here inside its publish transaction
//! ([`upsert_reported`], [`delete_durable_absent`], [`depth`]), a finalize
//! clears the row of the artifact it made durable ([`delete_finalized`]),
//! and [`crate::lost::write_off_lost_sandboxes`] removes a gone node's rows.
//! Both deletes carry their durability proof from
//! `pplx-space-sandbox-portability` rather than spelling it here, so the
//! drain that reads the queue agrees with them on what "durable" means. That
//! read is [`queued_candidates`]: a node's rows that still owe a backup.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    artifact_has_durable_copy, inflight_suspend_upload_exists,
    queued_artifact_recoverable_without_node, snapshot_durable_for_source_release,
};
use pplx_space_snapshot_model::{ArtifactKind, LocalArtifactId};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

use crate::SandboxStoreError;

/// Upsert one row per reported entry on `node_id` (an observed projection),
/// the three slices being the entries' columns in order, replacing the
/// tracked artifact of an existing `(node, sandbox)` row. Entries naming a
/// sandbox that has no row when the statement runs are skipped; one deleted
/// concurrently can still fail the foreign key and abort the statement.
/// The three slices are index-aligned and of equal length.
/// Returns the rows written, unchanged ones included, so the shortfall from
/// the report is the skipped entries.
pub async fn upsert_reported(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    sandbox_ids: &[SandboxId],
    artifact_ids: &[LocalArtifactId],
    artifact_kinds: &[ArtifactKind],
) -> Result<u64, sqlx::Error> {
    let artifact_kinds: Vec<&'static str> =
        artifact_kinds.iter().map(|kind| kind.as_str()).collect();
    let result = sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending (node_id, sandbox_id, artifact_id, artifact_kind)
         SELECT $1, reported.sandbox_id, reported.artifact_id, reported.artifact_kind
           FROM UNNEST($2::UUID[], $3::UUID[], $4::TEXT[])
                AS reported(sandbox_id, artifact_id, artifact_kind)
          WHERE EXISTS (
                    SELECT 1 FROM sandboxes WHERE sandboxes.id = reported.sandbox_id
                )
         ON CONFLICT (node_id, sandbox_id) DO UPDATE
            SET artifact_id = EXCLUDED.artifact_id,
                artifact_kind = EXCLUDED.artifact_kind"
    ))
    .bind(node_id)
    .bind(sandbox_ids)
    .bind(artifact_ids)
    .bind(&artifact_kinds)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// The size of one node's `node_local_backup_pending` rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingBackupDepth {
    /// Every row the node has.
    pub depth: i64,
    /// The rows whose sandbox is not among the ids the node reported.
    pub retained_absent: i64,
}

/// Count `node_id`'s pending-local-backup rows, and those of them for a
/// sandbox outside `reported_sandbox_ids`.
pub async fn depth(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    reported_sandbox_ids: &[SandboxId],
) -> Result<PendingBackupDepth, SandboxStoreError> {
    let (depth, retained_absent): (i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*),
                COUNT(*) FILTER (WHERE NOT (sandbox_id = ANY($2)))
           FROM node_local_backup_pending
          WHERE node_id = $1"
    ))
    .bind(node_id.as_uuid())
    .bind(reported_sandbox_ids)
    .fetch_one(executor)
    .await?;
    Ok(PendingBackupDepth {
        depth,
        retained_absent,
    })
}

/// Built once: every input is a literal, so the composed text is identical
/// on every census, and this runs for each node each minute. Same shape as
/// internal-api-server's `sandbox_quarantine_store` recovery queries, and the
/// `line!()` the `sql!` locator captures is fixed either way.
///
/// At module scope rather than inside [`delete_durable_absent`] so
/// `tests::the_delete_guard_resolves_through_indexes_not_a_history_scan` can
/// EXPLAIN the statement this function executes instead of a copy of it: a
/// copy pins the shared predicate but not the `USING`/`WHERE` scaffolding
/// around it.
static DELETE_DURABLE_ABSENT: LazyLock<String> = LazyLock::new(|| {
    let recoverable = queued_artifact_recoverable_without_node(
        "sandbox",
        "queued.artifact_id",
        "queued.artifact_kind",
        "queued.node_id",
    );
    sql!(
        format!(
            "DELETE FROM node_local_backup_pending queued
              USING sandboxes sandbox
              WHERE queued.node_id = $1
                AND sandbox.id = queued.sandbox_id
                AND NOT (queued.sandbox_id = ANY($2))
                AND {recoverable}"
        )
        .as_str()
    )
});

/// Delete `node_id`'s rows for the sandboxes not in `reported_sandbox_ids`
/// that [`queued_artifact_recoverable_without_node`] proves recoverable
/// without that node's disk (an observed projection). A row that is absent
/// and not provably recoverable stays. Returns the rows deleted.
pub async fn delete_durable_absent(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    reported_sandbox_ids: &[SandboxId],
) -> Result<u64, sqlx::Error> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(DELETE_DURABLE_ABSENT.as_str()))
            .bind(node_id.as_uuid())
            .bind(reported_sandbox_ids)
            .execute(executor)
            .await?
            .rows_affected(),
    )
}

/// Delete `node_id`'s row for exactly this artifact of `sandbox_id`, when
/// [`artifact_has_durable_copy`] finds that artifact durable (an operation
/// step). All four identity parts are required: keying on `(node, sandbox)`
/// alone would let finalizing an old artifact remove a newer artifact's row.
/// Returns whether a row was removed.
pub async fn delete_finalized(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a bind placeholder, so the composed text is
    /// identical on every finalize. Same shape as `DELETE_DURABLE_ABSENT`.
    static DELETE_FINALIZED: LazyLock<String> = LazyLock::new(|| {
        let durable = artifact_has_durable_copy("$2", "$3", "$4");
        sql!(
            format!(
                "DELETE FROM node_local_backup_pending
                  WHERE node_id = $1
                    AND sandbox_id = $2
                    AND artifact_id = $3
                    AND artifact_kind = $4
                    AND {durable}"
            )
            .as_str()
        )
    });
    let removed = sqlx::query(sqlx::AssertSqlSafe(DELETE_FINALIZED.as_str()))
        .bind(node_id.as_uuid())
        .bind(sandbox_id.as_uuid())
        .bind(artifact_id.as_uuid())
        .bind(artifact_kind.as_str())
        .execute(executor)
        .await?
        .rows_affected()
        > 0;
    Ok(removed)
}

/// The `LIMIT` both [`queued_candidates`] statements carry, spelled into their
/// text as a literal so the planner's `tuple_fraction` bias survives
/// `plan_cache_mode = force_generic_plan`. The same page as
/// [`crate::pinned_local_only::PAGE_LIMIT`].
pub const QUEUED_CANDIDATES_LIMIT: usize = 513;

/// One queued row of a node that still owes a durable backup, with the
/// artifact the node reported. `artifact_kind` is the stored text, for the
/// caller to parse.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct QueuedBackupCandidate {
    pub sandbox_id: SandboxId,
    pub artifact_id: LocalArtifactId,
    pub artifact_kind: String,
}

/// `$1` = node id. Every row is re-checked against the same two fragments
/// [`crate::pinned_local_only::page`] applies, over `sandboxes s` rather than
/// the queue row, so this read can only narrow that one. Both `q.node_id` and
/// `s.target_node_id` are matched, for the same reason. The durability probe
/// stays inside a scalar subquery, as there.
/// Why the queue only narrows and both node columns are matched is argued on
/// `list_pinned_local_candidates` in `pplx-space-cluster-controller-server`'s
/// `node_drain/store.rs`.
static QUEUED_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
    let snapshot_exists = snapshot_durable_for_source_release("s");
    let inflight_upload = inflight_suspend_upload_exists("s");
    sql!(format!(
        "SELECT q.sandbox_id, q.artifact_id, q.artifact_kind
           FROM node_local_backup_pending q
           JOIN sandboxes s ON s.id = q.sandbox_id
          WHERE q.node_id = $1
            AND s.target_node_id = $1
            AND q.artifact_kind = 'full'
            AND s.lifecycle = 'suspended'
            AND NOT (SELECT {snapshot_exists})
            AND NOT {inflight_upload}
          LIMIT 513"
    ))
});

/// [`QUEUED_CANDIDATES`] without the rows whose sandbox's
/// `current_local_artifact_id` is NULL.
static QUEUED_CANDIDATES_EXCLUDING_LEGACY_ROWS: LazyLock<String> = LazyLock::new(|| {
    let snapshot_exists = snapshot_durable_for_source_release("s");
    let inflight_upload = inflight_suspend_upload_exists("s");
    sql!(format!(
        "SELECT q.sandbox_id, q.artifact_id, q.artifact_kind
           FROM node_local_backup_pending q
           JOIN sandboxes s ON s.id = q.sandbox_id
          WHERE q.node_id = $1
            AND s.target_node_id = $1
            AND q.artifact_kind = 'full'
            AND s.lifecycle = 'suspended'
            AND NOT (SELECT {snapshot_exists})
            AND NOT {inflight_upload}
            AND s.current_local_artifact_id IS NOT NULL
          LIMIT 513"
    ))
});

/// Up to [`QUEUED_CANDIDATES_LIMIT`] of `node_id`'s queued `full` rows whose
/// sandbox is still `suspended`, pinned to `node_id`, without a durable copy
/// and without an upload in flight, in no particular order. With
/// `exclude_legacy_rows`, only rows whose sandbox names its current local
/// artifact. A read; it takes no lock.
pub async fn queued_candidates(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    exclude_legacy_rows: bool,
) -> Result<Vec<QueuedBackupCandidate>, sqlx::Error> {
    let statement = if exclude_legacy_rows {
        QUEUED_CANDIDATES_EXCLUDING_LEGACY_ROWS.as_str()
    } else {
        QUEUED_CANDIDATES.as_str()
    };
    sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .fetch_all(executor)
        .await
}

#[cfg(test)]
mod tests;
