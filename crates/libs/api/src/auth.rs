use pplx_space_common_core::types::{OrgId, SandboxOwnerSource, UserId};

use crate::api_surface::ApiSurface;
use pplx_space_common_core::sql;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::models::{ApiKey, Organization, User};
use crate::user_role::UserRole;

pub struct ApiKeyAuthContext {
    pub api_key: ApiKey,
    pub org: Organization,
    /// Resolved actor for the key. `None` if the key has `created_by` set but
    /// the user no longer exists, or if the org has no users at all.
    pub actor: Option<User>,
}

pub fn hash_password(password: &str) -> Result<String, bcrypt::BcryptError> {
    bcrypt::hash(password, 12)
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, bcrypt::BcryptError> {
    if hash == "!" {
        return Ok(false);
    }
    bcrypt::verify(password, hash)
}

/// Returns (raw_token, token_hash). Send raw_token to the client; store token_hash in the DB.
pub fn generate_session_token() -> (String, String) {
    let mut bytes = [0u8; 32];
    rand::fill(&mut bytes);
    let raw_token = hex::encode(bytes);
    let token_hash = hash_token(&raw_token);
    (raw_token, token_hash)
}

pub fn hash_token(raw_token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_token.as_bytes());
    hex::encode(hasher.finalize())
}

pub async fn get_session_user(
    pool: &PgPool,
    raw_token: &str,
) -> Result<Option<(User, Organization)>, sqlx::Error> {
    let token_hash = hash_token(raw_token);

    let row = sqlx::query_as::<_, User>(sql!(
        "SELECT u.* FROM users u
         JOIN sessions s ON s.user_id = u.id
         WHERE s.token_hash = $1 AND s.expires_at > NOW()"
    ))
    .bind(&token_hash)
    .fetch_optional(pool)
    .await?;

    let Some(user) = row else {
        return Ok(None);
    };
    if !user.password_login_configured() {
        return Ok(None);
    }

    let org = sqlx::query_as::<_, Organization>(sql!("SELECT * FROM organizations WHERE id = $1"))
        .bind(user.org_id)
        .fetch_one(pool)
        .await?;

    Ok(Some((user, org)))
}

/// Resolve the local SPACE user an external console identity maps to.
///
/// The email is matched exactly against `users.email`; nothing is created or
/// changed. Only users eligible for console sign-in qualify (see
/// [`User::external_console_sign_in_allowed`]). The user's own role and
/// organization authorize the request afterwards, exactly as for a password
/// session.
pub async fn get_external_console_user(
    pool: &PgPool,
    email: &str,
) -> Result<Option<(User, Organization)>, sqlx::Error> {
    let row = sqlx::query_as::<_, User>(sql!("SELECT * FROM users WHERE email = $1"))
        .bind(email)
        .fetch_optional(pool)
        .await?;

    let Some(user) = row else {
        return Ok(None);
    };
    if !user.external_console_sign_in_allowed() {
        return Ok(None);
    }

    let org = sqlx::query_as::<_, Organization>(sql!("SELECT * FROM organizations WHERE id = $1"))
        .bind(user.org_id)
        .fetch_one(pool)
        .await?;

    Ok(Some((user, org)))
}

/// Resolve a service credential for one surface.
///
/// The surface is part of the lookup rather than a check the caller applies
/// afterwards, so a credential issued for one surface cannot authenticate on
/// another even if a future caller forgets to ask. A key that exists but is
/// not scoped to `surface` is indistinguishable from no key at all, which is
/// the right answer for an auth boundary.
pub async fn get_api_key_org(
    pool: &PgPool,
    raw_api_key: &str,
    surface: ApiSurface,
) -> Result<Option<ApiKeyAuthContext>, sqlx::Error> {
    let key_hash = hash_token(raw_api_key);

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
    .bind(&key_hash)
    .bind(surface.as_str())
    .fetch_optional(pool)
    .await?;

    Ok(row.map(ApiKeyAuthRow::into_context))
}

pub(crate) async fn org_has_local_owner(pool: &PgPool, org_id: OrgId) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM organizations WHERE id = $1 AND owner_source = 'local')"
    ))
    .bind(org_id)
    .fetch_one(pool)
    .await
}

/// Wide row used by `get_api_key_org` to pull api_key + org + actor in one
/// JOIN. Each column is aliased so the same logical field name from different
/// source tables doesn't collide in SQLx's column lookup.
#[derive(sqlx::FromRow)]
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
    fn into_context(self) -> ApiKeyAuthContext {
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
        ApiKeyAuthContext {
            api_key,
            org,
            actor,
        }
    }
}

pub async fn api_key_belongs_to_org(
    pool: &PgPool,
    raw_api_key: &str,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    Ok(get_api_key_org(pool, raw_api_key, ApiSurface::Ecw)
        .await?
        .is_some_and(|context| context.org.id == org_id))
}

#[cfg(test)]
#[path = "auth/surface_scope_tests.rs"]
mod surface_scope_tests;
