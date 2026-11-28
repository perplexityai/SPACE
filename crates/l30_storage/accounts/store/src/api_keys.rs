//! `api_keys` rows: the authentication lookups by key hash, recording a key's
//! use, and an organization's key listing, creates and revocation.

use pplx_space_accounts::{ApiKeyId, OrgId, SandboxOwnerSource, UserId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{ApiSurface, Organization, User, UserRole};

#[derive(Debug, Clone, FromRow)]
pub struct ApiKey {
    pub id: Uuid,
    pub org_id: OrgId,
    pub name: String,
    pub key_hash: String,
    pub created_by: Option<Uuid>,
    pub last_used_at: Option<OffsetDateTime>,
    pub federated_owner_assertions_enabled: bool,
    /// Surfaces this credential may authenticate against. Enforced at the
    /// lookup rather than by each caller, so a new surface cannot forget.
    pub allowed_surfaces: Vec<ApiSurface>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// A key found by its hash, with its organization and the user it acts as:
/// its creator, or for a key without one, the organization's oldest user.
#[derive(Debug, Clone)]
pub struct ApiKeyAuth {
    pub api_key: ApiKey,
    pub org: Organization,
    pub actor: Option<User>,
}

/// The key with this hash, when it is scoped to `surface`, with its
/// organization and actor.
pub async fn api_key_auth_by_hash(
    executor: impl Executor<'_>,
    key_hash: &str,
    surface: ApiSurface,
) -> Result<Option<ApiKeyAuth>, DbError> {
    let row = sqlx::query_as::<_, ApiKeyAuthRow>(sql!(
        "SELECT
             k.id                AS api_key_id,
             k.org_id             AS api_key_org_id,
             k.name               AS api_key_name,
             k.key_hash           AS api_key_key_hash,
             k.created_by         AS api_key_created_by,
             k.last_used_at       AS api_key_last_used_at,
             k.federated_owner_assertions_enabled AS api_key_federated_owner_assertions_enabled,
             k.allowed_surfaces   AS api_key_allowed_surfaces,
             k.created_at         AS api_key_created_at,
             k.updated_at         AS api_key_updated_at,
             o.id                 AS org_id,
             o.name               AS org_name,
             o.slug               AS org_slug,
             o.parent_id          AS org_parent_id,
             o.theme_primary      AS org_theme_primary,
             o.theme_secondary    AS org_theme_secondary,
             o.logo_url           AS org_logo_url,
             o.byok_enabled       AS org_byok_enabled,
             o.snapshot_base_object_storage_uri AS org_snapshot_base_object_storage_uri,
             o.created_at         AS org_created_at,
             o.updated_at         AS org_updated_at,
             u.id                 AS user_id,
             u.email              AS user_email,
             u.password_hash      AS user_password_hash,
             u.display_name       AS user_display_name,
             u.role               AS user_role,
             u.owner_source       AS user_owner_source,
             u.created_at         AS user_created_at,
             u.updated_at         AS user_updated_at
         FROM api_keys k
         JOIN organizations o ON o.id = k.org_id
         LEFT JOIN LATERAL (
             SELECT *
             FROM users
             WHERE org_id = k.org_id
               AND (k.created_by IS NULL OR id = k.created_by)
             ORDER BY created_at ASC
             LIMIT 1
         ) u ON TRUE
         WHERE k.key_hash = $1
           AND $2 = ANY(k.allowed_surfaces)"
    ))
    .bind(key_hash)
    .bind(surface.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(ApiKeyAuthRow::into_auth))
}

/// Wide row used by [`api_key_auth_by_hash`] to pull api_key + org + actor in
/// one JOIN. Each column is aliased so the same logical field name from
/// different source tables doesn't collide in SQLx's column lookup.
#[derive(FromRow)]
struct ApiKeyAuthRow {
    api_key_id: Uuid,
    api_key_org_id: OrgId,
    api_key_name: String,
    api_key_key_hash: String,
    api_key_created_by: Option<Uuid>,
    api_key_last_used_at: Option<OffsetDateTime>,
    api_key_federated_owner_assertions_enabled: bool,
    api_key_allowed_surfaces: Vec<ApiSurface>,
    api_key_created_at: OffsetDateTime,
    api_key_updated_at: OffsetDateTime,
    org_id: OrgId,
    org_name: String,
    org_slug: String,
    org_parent_id: Option<Uuid>,
    org_theme_primary: Option<String>,
    org_theme_secondary: Option<String>,
    org_logo_url: Option<String>,
    org_byok_enabled: bool,
    org_snapshot_base_object_storage_uri: Option<String>,
    org_created_at: OffsetDateTime,
    org_updated_at: OffsetDateTime,
    user_id: Option<UserId>,
    user_email: Option<String>,
    user_password_hash: Option<String>,
    user_display_name: Option<String>,
    user_role: Option<UserRole>,
    user_owner_source: Option<SandboxOwnerSource>,
    user_created_at: Option<OffsetDateTime>,
    user_updated_at: Option<OffsetDateTime>,
}

impl ApiKeyAuthRow {
    fn into_auth(self) -> ApiKeyAuth {
        let api_key = ApiKey {
            id: self.api_key_id,
            org_id: self.api_key_org_id,
            name: self.api_key_name,
            key_hash: self.api_key_key_hash,
            created_by: self.api_key_created_by,
            last_used_at: self.api_key_last_used_at,
            federated_owner_assertions_enabled: self.api_key_federated_owner_assertions_enabled,
            allowed_surfaces: self.api_key_allowed_surfaces,
            created_at: self.api_key_created_at,
            updated_at: self.api_key_updated_at,
        };
        let org = Organization {
            id: self.org_id,
            name: self.org_name,
            slug: self.org_slug,
            parent_id: self.org_parent_id,
            theme_primary: self.org_theme_primary,
            theme_secondary: self.org_theme_secondary,
            logo_url: self.org_logo_url,
            byok_enabled: self.org_byok_enabled,
            snapshot_base_object_storage_uri: self.org_snapshot_base_object_storage_uri,
            created_at: self.org_created_at,
            updated_at: self.org_updated_at,
        };
        let actor = match (
            self.user_id,
            self.user_email,
            self.user_password_hash,
            self.user_display_name,
            self.user_role,
            self.user_owner_source,
            self.user_created_at,
            self.user_updated_at,
        ) {
            (
                Some(id),
                Some(email),
                Some(password_hash),
                Some(display_name),
                Some(role),
                Some(owner_source),
                Some(created_at),
                Some(updated_at),
            ) => Some(User {
                id,
                email,
                password_hash,
                display_name,
                org_id: org.id,
                role,
                owner_source,
                created_at,
                updated_at,
            }),
            _ => None,
        };
        ApiKeyAuth {
            api_key,
            org,
            actor,
        }
    }
}

/// Set the key's `last_used_at` to the database's `NOW()` when it is unset or
/// older than `stale_after`, unless the key is revoked or another transaction
/// holds a lock on its row. `false` when it wrote nothing: the key is missing,
/// revoked, recently used or locked by another transaction.
///
/// The row lock (`FOR UPDATE SKIP LOCKED`) serves only this statement, so
/// concurrent uses of one key neither wait on each other nor write twice. The
/// staleness check compares with `NOW()`, the start of the transaction the
/// statement runs in.
pub async fn advance_api_key_last_used_at(
    executor: impl Executor<'_>,
    api_key_id: Uuid,
    stale_after: std::time::Duration,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "UPDATE api_keys
            SET last_used_at = NOW()
          WHERE id = (
              SELECT id
              FROM api_keys
              WHERE id = $1
                AND revoked_at IS NULL
                AND (last_used_at IS NULL
                     OR last_used_at < NOW() - make_interval(secs => $2))
              FOR UPDATE SKIP LOCKED
          )"
    ))
    .bind(api_key_id)
    .bind(stale_after.as_secs_f64())
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

/// What [`revoke_organization_api_key`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiKeyRevocation {
    Revoked,
    AlreadyRevoked,
    NotFound,
}

/// Revoke an organization's key, keeping its row so it lists as revoked. A
/// key of another organization is [`ApiKeyRevocation::NotFound`] and stays as
/// it is.
///
/// Revocation replaces `key_hash` with `revoked:<id>`, which no SHA-256 hex
/// digest equals, rather than relying on readers to filter on `revoked_at`:
/// every lookup by `key_hash`, including those of releases that predate the
/// column, stops matching the key in the same statement. `revoked_at` is kept
/// from the first revocation.
///
/// Its `EXISTS` reads the snapshot from before the `UPDATE`, which revokes
/// without removing the row, so a key this statement revokes still counts as
/// found.
pub async fn revoke_organization_api_key(
    executor: impl Executor<'_>,
    org_id: OrgId,
    api_key_id: ApiKeyId,
) -> Result<ApiKeyRevocation, DbError> {
    let (revoked, found) = sqlx::query_as::<_, (bool, bool)>(sql!(
        "WITH revoked AS (
             UPDATE api_keys
                SET revoked_at = NOW(),
                    updated_at = NOW(),
                    key_hash = 'revoked:' || id::text
              WHERE id = $1
                AND org_id = $2
                AND revoked_at IS NULL
          RETURNING id
         )
         SELECT EXISTS(SELECT 1 FROM revoked),
                EXISTS(SELECT 1 FROM api_keys WHERE id = $1 AND org_id = $2)"
    ))
    .bind(api_key_id)
    .bind(org_id)
    .fetch_one(executor.into_sqlx())
    .await?;
    Ok(match (revoked, found) {
        (true, _) => ApiKeyRevocation::Revoked,
        (false, true) => ApiKeyRevocation::AlreadyRevoked,
        (false, false) => ApiKeyRevocation::NotFound,
    })
}

/// Whether a key with this hash belongs to `org_id` and is scoped to the ECW
/// surface.
pub async fn ecw_api_key_authorizes_org(
    executor: impl Executor<'_>,
    key_hash: &str,
    org_id: OrgId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM api_keys
             WHERE org_id = $1 AND key_hash = $2
               AND 'ecw' = ANY(allowed_surfaces)
         )"
    ))
    .bind(org_id)
    .bind(key_hash)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// A key as an organization's key listing shows it, with its creator's email.
#[derive(Clone, Debug, FromRow)]
pub struct ApiKeyRow {
    pub id: ApiKeyId,
    pub org_id: OrgId,
    pub name: String,
    pub created_by: Option<UserId>,
    pub created_by_email: Option<String>,
    pub allowed_surfaces: Vec<ApiSurface>,
    pub last_used_at: Option<OffsetDateTime>,
    pub revoked_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// An organization's keys, newest first; revoked ones only with
/// `include_revoked`.
pub async fn list_organization_api_keys(
    executor: impl Executor<'_>,
    org_id: OrgId,
    include_revoked: bool,
) -> Result<Vec<ApiKeyRow>, DbError> {
    Ok(sqlx::query_as::<_, ApiKeyRow>(sql!(
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
           LEFT JOIN users u ON u.id = k.created_by AND u.org_id = k.org_id
          WHERE k.org_id = $1
            AND ($2 OR k.revoked_at IS NULL)
          ORDER BY k.created_at DESC, k.id DESC"
    ))
    .bind(org_id)
    .bind(include_revoked)
    .fetch_all(executor.into_sqlx())
    .await?)
}

pub struct CreateApiKey<'a> {
    pub id: ApiKeyId,
    pub org_id: OrgId,
    pub name: &'a str,
    pub key_hash: &'a str,
    pub created_by: UserId,
    pub allowed_surfaces: &'a [ApiSurface],
}

pub async fn create_api_key(
    executor: impl Executor<'_>,
    input: CreateApiKey<'_>,
) -> Result<ApiKeyRow, DbError> {
    // Return the creator label in the insert statement so a successful write
    // cannot be followed by a failed reload that loses the one-time secret
    // response. The CTE makes the inserted row available to that joined
    // projection without opening a second database round trip.
    Ok(sqlx::query_as::<_, ApiKeyRow>(sql!(
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
           LEFT JOIN users u ON u.id = k.created_by AND u.org_id = k.org_id"
    ))
    .bind(input.id)
    .bind(input.org_id)
    .bind(input.name)
    .bind(input.key_hash)
    .bind(input.created_by)
    .bind(input.allowed_surfaces)
    .fetch_one(executor.into_sqlx())
    .await?)
}
