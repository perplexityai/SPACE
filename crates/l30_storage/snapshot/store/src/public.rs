//! The snapshots the public API advertises for a sandbox an organization
//! owns: fork copies are internal, and `deleting` rows are claimed for prune
//! and read as gone. Text columns such as `type` and `state` are left for the
//! caller to parse against its wire enums.

use pplx_space_accounts::OrgId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;

/// One advertised snapshot, as the public surface shows it.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct PublicSnapshotRow {
    pub id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub snapshot_type: String,
    pub state: String,
    pub created_at: OffsetDateTime,
    pub label: Option<String>,
    pub retention_tier: Option<String>,
    pub size_bytes: Option<i64>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub failure_reason: Option<String>,
}

/// Up to `limit` advertised snapshots of `sandbox_id`, which `org_id` owns,
/// newest first, strictly after the `(created_at, id)` `cursor` when given.
pub async fn page(
    executor: impl Executor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    cursor: Option<(OffsetDateTime, SnapshotId)>,
    limit: i64,
) -> Result<Vec<PublicSnapshotRow>, DbError> {
    let (cursor_created_at, cursor_id) = cursor
        .map(|(created_at, id)| (Some(created_at), Some(id)))
        .unwrap_or((None, None));
    // `deleting` rows are claimed for prune and de-advertised: they must
    // read as gone on this surface, and `PublicSnapshotState` deliberately
    // has no such wire variant.
    Ok(sqlx::query_as::<_, PublicSnapshotRow>(sql!(
        "SELECT sn.id,
                sn.sandbox_id,
                sn.type AS snapshot_type,
                sn.state,
                sn.timestamp AS created_at,
                sn.label,
                sn.retention_tier,
                sn.size_bytes,
                sn.vcpu,
                sn.memory_mib,
                sn.template_id,
                sn.content_hash,
                sn.failure_reason
         FROM snapshots sn
         JOIN sandboxes s ON s.id = sn.sandbox_id
         WHERE sn.sandbox_id = $1
           AND s.org_id = $2
           AND sn.artifact_origin IS DISTINCT FROM 'fork'
           AND sn.state <> 'deleting'
           AND (
               $3::TIMESTAMPTZ IS NULL
               OR (sn.timestamp, sn.id) < ($3::TIMESTAMPTZ, $4::UUID)
           )
         ORDER BY sn.timestamp DESC, sn.id DESC
         LIMIT $5"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(org_id)
    .bind(cursor_created_at)
    .bind(cursor_id)
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Advertised snapshot `snapshot_id` of `sandbox_id`, which `org_id` owns;
/// `None` when there's no such snapshot, or it is a fork copy or claimed for
/// prune.
pub async fn get(
    executor: impl Executor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
) -> Result<Option<PublicSnapshotRow>, DbError> {
    // Excluding `deleting` turns a get on a claimed row into a 404, matching
    // the list surface above.
    Ok(sqlx::query_as::<_, PublicSnapshotRow>(sql!(
        "SELECT sn.id,
                sn.sandbox_id,
                sn.type AS snapshot_type,
                sn.state,
                sn.timestamp AS created_at,
                sn.label,
                sn.retention_tier,
                sn.size_bytes,
                sn.vcpu,
                sn.memory_mib,
                sn.template_id,
                sn.content_hash,
                sn.failure_reason
         FROM snapshots sn
         JOIN sandboxes s ON s.id = sn.sandbox_id
         WHERE sn.sandbox_id = $1
           AND sn.id = $2
           AND s.org_id = $3
           AND sn.artifact_origin IS DISTINCT FROM 'fork'
           AND sn.state <> 'deleting'"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(snapshot_id)
    .bind(org_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
