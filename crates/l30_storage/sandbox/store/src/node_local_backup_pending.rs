//! `node_local_backup_pending`: the local artifacts each node reports it has
//! not yet backed up durably, one row per `(node, sandbox)`. The rows go with
//! their sandbox through the `sandbox_id` foreign key's cascade.
//!
//! The census mirrors a node's report here inside its publish transaction;
//! [`crate::lost::write_off_lost_sandboxes`] removes a gone node's rows.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{ArtifactKind, LocalArtifactId};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

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
