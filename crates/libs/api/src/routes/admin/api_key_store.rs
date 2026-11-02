//! Postgres operations for organization-scoped API keys.

use pplx_space_common_core::{
    sql,
    types::{ApiKeyId, OrgId, UserId},
};
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;

use crate::api_surface::ApiSurface;

#[derive(Clone, Debug, FromRow)]
pub(super) struct ApiKeyRow {
    pub(super) id: ApiKeyId,
    pub(super) org_id: OrgId,
    pub(super) name: String,
    pub(super) created_by: Option<UserId>,
    pub(super) created_by_email: Option<String>,
    pub(super) allowed_surfaces: Vec<ApiSurface>,
    pub(super) last_used_at: Option<OffsetDateTime>,
    pub(super) revoked_at: Option<OffsetDateTime>,
    pub(super) created_at: OffsetDateTime,
    pub(super) updated_at: OffsetDateTime,
}

pub(super) async fn organization_exists(
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

/// An organization's keys, newest first; revoked ones only with
/// `include_revoked`.
pub(super) async fn list_for_organization(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    include_revoked: bool,
) -> Result<Vec<ApiKeyRow>, sqlx::Error> {
    sqlx::query_as::<_, ApiKeyRow>(sql!(
        "SELECT k.id,
                k.org_id,
                k.name,
                k.created_by,
                u.email AS created_by_email,
                k.allowed_surfaces,
                k.last_used_at,
                k.revoked_at,
                k.created_at,
                k.updated_at
           FROM api_keys k
           LEFT JOIN users u ON u.id = k.created_by
          WHERE k.org_id = $1
            AND ($2 OR k.revoked_at IS NULL)
          ORDER BY k.created_at DESC, k.id DESC"
    ))
    .bind(org_id)
    .bind(include_revoked)
    .fetch_all(executor)
    .await
}

pub(super) struct CreateApiKey<'a> {
    pub(super) id: ApiKeyId,
    pub(super) org_id: OrgId,
    pub(super) name: &'a str,
    pub(super) key_hash: &'a str,
    pub(super) created_by: UserId,
    pub(super) allowed_surfaces: &'a [ApiSurface],
}

pub(super) async fn create(
    executor: impl PgExecutor<'_>,
    input: CreateApiKey<'_>,
) -> Result<ApiKeyRow, sqlx::Error> {
    // Return the creator label in the insert statement so a successful write
    // cannot be followed by a failed reload that loses the one-time secret
    // response. The CTE makes the inserted row available to that joined
    // projection without opening a second database round trip.
    sqlx::query_as::<_, ApiKeyRow>(sql!(
        "WITH inserted AS (
             INSERT INTO api_keys (
                 id, org_id, name, key_hash, created_by, allowed_surfaces,
                 federated_owner_assertions_enabled
             )
             VALUES ($1, $2, $3, $4, $5, $6, false)
             RETURNING id, org_id, name, created_by, allowed_surfaces,
                       last_used_at, revoked_at, created_at, updated_at
         )
         SELECT k.id,
                k.org_id,
                k.name,
                k.created_by,
                u.email AS created_by_email,
                k.allowed_surfaces,
                k.last_used_at,
                k.revoked_at,
                k.created_at,
                k.updated_at
           FROM inserted k
           LEFT JOIN users u ON u.id = k.created_by"
    ))
    .bind(input.id)
    .bind(input.org_id)
    .bind(input.name)
    .bind(input.key_hash)
    .bind(input.created_by)
    .bind(input.allowed_surfaces)
    .fetch_one(executor)
    .await
}

/// What [`revoke`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RevokeOutcome {
    Revoked,
    AlreadyRevoked,
    NotFound,
}

/// Revoke an organization's key, keeping its row so it lists as revoked.
///
/// Revocation replaces `key_hash` with `revoked:<id>`, which no SHA-256 hex
/// digest equals, rather than relying on readers to filter on `revoked_at`:
/// every lookup by `key_hash`, including those of releases that predate the
/// column, stops matching the key in the same statement. `revoked_at` is kept
/// from the first revocation.
pub(super) async fn revoke(
    executor: impl PgExecutor<'_> + Copy,
    org_id: OrgId,
    api_key_id: ApiKeyId,
) -> Result<RevokeOutcome, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE api_keys
            SET revoked_at = NOW(),
                updated_at = NOW(),
                key_hash = 'revoked:' || id::text
          WHERE id = $1
            AND org_id = $2
            AND revoked_at IS NULL"
    ))
    .bind(api_key_id)
    .bind(org_id)
    .execute(executor)
    .await?;
    if result.rows_affected() == 1 {
        return Ok(RevokeOutcome::Revoked);
    }
    let exists = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM api_keys WHERE id = $1 AND org_id = $2)"
    ))
    .bind(api_key_id)
    .bind(org_id)
    .fetch_one(executor)
    .await?;
    Ok(if exists {
        RevokeOutcome::AlreadyRevoked
    } else {
        RevokeOutcome::NotFound
    })
}

#[cfg(test)]
mod tests;
