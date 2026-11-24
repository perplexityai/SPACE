//! The organization that owns a sandbox and the snapshot storage that
//! organization has configured, which credmgr's storage authorization starts
//! from.

use pplx_space_accounts::OrgId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// The organization that owns a sandbox, with the organization's configured
/// snapshot base URI, unparsed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxOrgStorage {
    pub organization_id: OrgId,
    pub snapshot_base_object_storage_uri: Option<String>,
}

/// `sandbox_id`'s [`SandboxOrgStorage`], or `None` when there's no such
/// sandbox.
pub async fn sandbox_org_storage(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxOrgStorage>, sqlx::Error> {
    let row: Option<(uuid::Uuid, Option<String>)> = sqlx::query_as(sql!(
        "SELECT s.org_id, o.snapshot_base_object_storage_uri
             FROM sandboxes s
             JOIN organizations o ON o.id = s.org_id
             WHERE s.id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(executor)
    .await?;
    Ok(row.map(
        |(organization_id, snapshot_base_object_storage_uri)| SandboxOrgStorage {
            organization_id: OrgId::from_uuid(organization_id),
            snapshot_base_object_storage_uri,
        },
    ))
}
