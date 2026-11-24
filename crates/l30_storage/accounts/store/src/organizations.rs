//! `organizations` rows: lookups, the organization tree, and the local and
//! federated creates.

use pplx_space_accounts::OrgId;
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Organization {
    pub id: OrgId,
    pub name: String,
    pub slug: String,
    pub parent_id: Option<Uuid>,
    pub theme_primary: Option<String>,
    pub theme_secondary: Option<String>,
    pub logo_url: Option<String>,
    /// Advisory BYOK opt-in flag. Signals callers to route writes
    /// through credmgr + populate the matching `*_encrypted_fields`
    /// sidecar; does not itself encrypt or decrypt anything.
    pub byok_enabled: bool,
    /// Per-org object storage override for base snapshot uploads.
    /// `s3://bucket/prefix?region=...` URI. When set, snapshot files
    /// are uploaded to the org's own bucket instead of the cluster
    /// default. NULL falls through to the cluster-wide env-var config.
    pub snapshot_base_object_storage_uri: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

pub async fn get_organization(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<Option<Organization>, sqlx::Error> {
    sqlx::query_as::<_, Organization>(sql!(
        "SELECT id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at
         FROM organizations WHERE id = $1"
    ))
    .bind(org_id)
    .fetch_optional(executor)
    .await
}

/// Every organization, by name.
pub async fn list_organizations(
    executor: impl PgExecutor<'_>,
) -> Result<Vec<Organization>, sqlx::Error> {
    sqlx::query_as::<_, Organization>(sql!(
        "SELECT id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at
         FROM organizations ORDER BY name, id"
    ))
    .fetch_all(executor)
    .await
}

/// The id of every organization.
pub async fn organization_ids(executor: impl PgExecutor<'_>) -> Result<Vec<OrgId>, sqlx::Error> {
    sqlx::query_scalar::<_, OrgId>(sql!("SELECT id FROM organizations"))
        .fetch_all(executor)
        .await
}

/// `org_id` and every organization below it through `parent_id`.
pub async fn organization_subtree_ids(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<Vec<OrgId>, sqlx::Error> {
    sqlx::query_scalar::<_, OrgId>(sql!(
        "WITH RECURSIVE org_tree AS (
            SELECT id FROM organizations WHERE id = $1
            UNION ALL
            SELECT o.id FROM organizations o JOIN org_tree t ON o.parent_id = t.id
        )
        SELECT id FROM org_tree"
    ))
    .bind(org_id)
    .fetch_all(executor)
    .await
}

/// Whether `target_org_id` is `root_org_id` or below it through `parent_id`.
pub async fn organization_subtree_contains(
    executor: impl PgExecutor<'_>,
    root_org_id: OrgId,
    target_org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "WITH RECURSIVE org_tree AS (
            SELECT id FROM organizations WHERE id = $1
            UNION ALL
            SELECT o.id FROM organizations o JOIN org_tree t ON o.parent_id = t.id
        )
        SELECT EXISTS(SELECT 1 FROM org_tree WHERE id = $2)"
    ))
    .bind(root_org_id)
    .bind(target_org_id)
    .fetch_one(executor)
    .await
}

pub async fn organization_exists(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM organizations WHERE id = $1)"
    ))
    .bind(org_id)
    .fetch_one(executor)
    .await
}

/// Whether the organization exists and is a local one, not materialized for
/// a federated owner.
pub async fn organization_has_local_owner(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM organizations WHERE id = $1 AND owner_source = 'local')"
    ))
    .bind(org_id)
    .fetch_one(executor)
    .await
}

/// The organization's BYOK flag, or `None` when the organization is missing.
pub async fn organization_byok_enabled(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<Option<bool>, sqlx::Error> {
    sqlx::query_scalar(sql!("SELECT byok_enabled FROM organizations WHERE id = $1"))
        .bind(org_id)
        .fetch_optional(executor)
        .await
}

/// A local organization to create.
pub struct NewOrganization {
    pub id: OrgId,
    pub name: String,
    pub slug: String,
    pub parent_id: Option<Uuid>,
    pub theme_primary: Option<String>,
    pub theme_secondary: Option<String>,
    pub logo_url: Option<String>,
}

pub async fn create_organization(
    executor: impl PgExecutor<'_>,
    org: NewOrganization,
) -> Result<Organization, sqlx::Error> {
    sqlx::query_as::<_, Organization>(sql!(
        "INSERT INTO organizations (
             id, name, slug, parent_id, theme_primary, theme_secondary, logo_url
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                   byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at"
    ))
    .bind(org.id)
    .bind(org.name)
    .bind(org.slug)
    .bind(org.parent_id)
    .bind(org.theme_primary)
    .bind(org.theme_secondary)
    .bind(org.logo_url)
    .fetch_one(executor)
    .await
}

/// Changes to an organization. `None` leaves a field as it is; for a
/// nullable field, `Some(None)` clears it.
pub struct OrganizationPatch {
    pub name: Option<String>,
    pub slug: Option<String>,
    pub theme_primary: Option<Option<String>>,
    pub theme_secondary: Option<Option<String>>,
    pub logo_url: Option<Option<String>>,
    pub snapshot_base_object_storage_uri: Option<Option<String>>,
    pub byok_enabled: Option<bool>,
}

/// Apply `patch` to the organization; `None` when there is none.
pub async fn update_organization(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    patch: OrganizationPatch,
) -> Result<Option<Organization>, sqlx::Error> {
    sqlx::query_as::<_, Organization>(sql!(
        "UPDATE organizations
         SET name = COALESCE($2, name),
             slug = COALESCE($3, slug),
             theme_primary = CASE WHEN $4 THEN $5 ELSE theme_primary END,
             theme_secondary = CASE WHEN $6 THEN $7 ELSE theme_secondary END,
             logo_url = CASE WHEN $8 THEN $9 ELSE logo_url END,
             snapshot_base_object_storage_uri =
                 CASE WHEN $10 THEN $11 ELSE snapshot_base_object_storage_uri END,
             byok_enabled = COALESCE($12, byok_enabled),
             updated_at = NOW()
         WHERE id = $1
         RETURNING id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                   byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at"
    ))
    .bind(org_id)
    .bind(patch.name)
    .bind(patch.slug)
    .bind(patch.theme_primary.is_some())
    .bind(patch.theme_primary.flatten())
    .bind(patch.theme_secondary.is_some())
    .bind(patch.theme_secondary.flatten())
    .bind(patch.logo_url.is_some())
    .bind(patch.logo_url.flatten())
    .bind(patch.snapshot_base_object_storage_uri.is_some())
    .bind(patch.snapshot_base_object_storage_uri.flatten())
    .bind(patch.byok_enabled)
    .fetch_optional(executor)
    .await
}

/// Delete the organization; `false` when there was none. A row that
/// references it through a foreign key without `ON DELETE CASCADE` or
/// `ON DELETE SET NULL` fails the delete with that key's error; the others
/// are deleted or cleared with it.
pub async fn delete_organization(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!("DELETE FROM organizations WHERE id = $1"))
        .bind(org_id)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// The organization created for an upstream organization, if there is one.
pub async fn federated_organization_id(
    executor: impl PgExecutor<'_>,
    upstream_issuer: &str,
    upstream_org_id: &str,
) -> Result<Option<OrgId>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT id
         FROM organizations
         WHERE owner_source = 'federated_identity'
           AND upstream_issuer = $1
           AND upstream_org_id = $2"
    ))
    .bind(upstream_issuer)
    .bind(upstream_org_id)
    .fetch_optional(executor)
    .await
}

/// The organization a federated owner's upstream organization maps to.
pub struct FederatedOrganization<'a> {
    /// Used only when no organization maps to the upstream one yet.
    pub proposed_id: OrgId,
    pub name: &'a str,
    pub slug: &'a str,
    pub upstream_issuer: &'a str,
    pub upstream_org_id: &'a str,
}

/// Create the organization for an upstream organization, or find the one
/// already created for it, and return its id. Concurrent calls for one
/// upstream organization return the same id.
pub async fn upsert_federated_organization(
    executor: impl PgExecutor<'_>,
    org: FederatedOrganization<'_>,
) -> Result<OrgId, sqlx::Error> {
    let (org_id,): (OrgId,) = sqlx::query_as(sql!(
        "INSERT INTO organizations (
             id, name, slug, owner_source, upstream_issuer, upstream_org_id
         )
         VALUES ($1, $2, $3, 'federated_identity', $4, $5)
         ON CONFLICT (upstream_issuer, upstream_org_id)
             WHERE owner_source = 'federated_identity'
         DO UPDATE SET name = organizations.name
         RETURNING id"
    ))
    .bind(org.proposed_id)
    .bind(org.name)
    .bind(org.slug)
    .bind(org.upstream_issuer)
    .bind(org.upstream_org_id)
    .fetch_one(executor)
    .await?;
    Ok(org_id)
}
