//! `users` rows: lookups by id, email, organization and console session, the
//! share-locked sign-in read, the joined user-and-organization read, and the
//! local and federated creates.

use pplx_space_accounts::{OrgId, SandboxOwnerSource, UserId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Organization, UserRole};

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct User {
    pub id: UserId,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub display_name: String,
    pub org_id: OrgId,
    pub role: UserRole,
    #[serde(skip_serializing)]
    pub owner_source: SandboxOwnerSource,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

pub async fn get_user(
    executor: impl Executor<'_>,
    user_id: UserId,
) -> Result<Option<User>, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE id = $1"
    ))
    .bind(user_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The user with exactly this email.
pub async fn user_by_email(
    executor: impl Executor<'_>,
    email: &str,
) -> Result<Option<User>, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE email = $1"
    ))
    .bind(email)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The user with exactly this email, share-locked so its password hash and
/// email cannot change until the caller's transaction ends. The caller must
/// pass its open transaction.
pub async fn share_lock_user_by_email<C>(tx: &mut C, email: &str) -> Result<Option<User>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE email = $1 FOR SHARE"
    ))
    .bind(email)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// The user of the console session with this token hash, while the session
/// has not expired. Expiry is compared with the database's `NOW()`, the start
/// of the transaction the statement runs in.
pub async fn user_by_session_token_hash(
    executor: impl Executor<'_>,
    token_hash: &str,
) -> Result<Option<User>, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "SELECT u.id, u.email, u.password_hash, u.display_name, u.org_id, u.role,
                u.owner_source, u.created_at, u.updated_at
         FROM users u
         JOIN sessions s ON s.user_id = u.id
         WHERE s.token_hash = $1 AND s.expires_at > NOW()"
    ))
    .bind(token_hash)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// A user with its organization, read in one statement.
#[derive(Debug, Clone)]
pub struct UserWithOrganization {
    pub user: User,
    pub org: Organization,
}

/// The statement [`user_with_organization`] runs. Its select list is the one
/// [`UserOrganizationRow`] decodes.
pub(crate) const USER_WITH_ORGANIZATION_SQL: &str = sql!(
    "SELECT u.id AS user_id, u.email AS user_email, u.password_hash AS user_password_hash,
            u.display_name AS user_display_name, u.role AS user_role,
            u.owner_source AS user_owner_source, u.created_at AS user_created_at,
            u.updated_at AS user_updated_at,
            o.id AS org_id, o.name AS org_name, o.slug AS org_slug, o.parent_id AS org_parent_id,
            o.theme_primary AS org_theme_primary, o.theme_secondary AS org_theme_secondary,
            o.logo_url AS org_logo_url, o.byok_enabled AS org_byok_enabled,
            o.snapshot_base_object_storage_uri AS org_snapshot_base_object_storage_uri,
            o.created_at AS org_created_at, o.updated_at AS org_updated_at
     FROM users u
     JOIN organizations o ON o.id = u.org_id
     WHERE u.id = $1"
);

/// The user and its organization. When either is missing, a [`DbError`] whose
/// `kind()` is [`DbErrorKind::NotFound`]: the driver's `RowNotFound`, which
/// callers still see after converting.
///
/// [`DbErrorKind::NotFound`]: pplx_space_postgres_pool::DbErrorKind::NotFound
pub async fn user_with_organization(
    executor: impl Executor<'_>,
    user_id: UserId,
) -> Result<UserWithOrganization, DbError> {
    sqlx::query_as::<_, UserOrganizationRow>(USER_WITH_ORGANIZATION_SQL)
        .bind(user_id)
        .fetch_one(executor.into_sqlx())
        .await
        .map(UserOrganizationRow::into_user_with_organization)
        .map_err(DbError::from)
}

/// One `users u JOIN organizations o` row, each column aliased so the two
/// tables' shared names (`id`, `name`, `created_at`, ...) do not collide.
///
/// Two statements decode it: [`USER_WITH_ORGANIZATION_SQL`]
/// ([`user_with_organization`]) and
/// [`crate::organizations::FEDERATED_ORGANIZATION_USER_SQL`]
/// ([`crate::organizations::federated_organization_user`]). Their select lists
/// must stay identical and change together: a column added to `users` or
/// `organizations` and to [`User`] or [`Organization`] is added to both lists
/// and to this row. A store test compares the two lists.
#[derive(FromRow)]
pub(crate) struct UserOrganizationRow {
    user_id: UserId,
    user_email: String,
    user_password_hash: String,
    user_display_name: String,
    user_role: UserRole,
    user_owner_source: SandboxOwnerSource,
    user_created_at: OffsetDateTime,
    user_updated_at: OffsetDateTime,
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
}

impl UserOrganizationRow {
    pub(crate) fn into_user_with_organization(self) -> UserWithOrganization {
        let user = User {
            id: self.user_id,
            email: self.user_email,
            password_hash: self.user_password_hash,
            display_name: self.user_display_name,
            // The join is on `o.id = u.org_id`, so the two are the same value.
            org_id: self.org_id,
            role: self.user_role,
            owner_source: self.user_owner_source,
            created_at: self.user_created_at,
            updated_at: self.user_updated_at,
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
        UserWithOrganization { user, org }
    }
}

/// The user, only when it belongs to `org_id`.
pub async fn organization_user(
    executor: impl Executor<'_>,
    org_id: OrgId,
    user_id: UserId,
) -> Result<Option<User>, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users
         WHERE id = $1 AND org_id = $2"
    ))
    .bind(user_id)
    .bind(org_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Whether `user_id` is a user of `org_id`.
pub async fn user_in_organization(
    executor: impl Executor<'_>,
    org_id: OrgId,
    user_id: UserId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND org_id = $2)"
    ))
    .bind(user_id)
    .bind(org_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The organization's oldest user.
pub async fn first_organization_user_id(
    executor: impl Executor<'_>,
    org_id: OrgId,
) -> Result<Option<UserId>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT id FROM users WHERE org_id = $1 ORDER BY created_at, id LIMIT 1"
    ))
    .bind(org_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The organization's users, by email.
pub async fn list_organization_users(
    executor: impl Executor<'_>,
    org_id: OrgId,
) -> Result<Vec<User>, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE org_id = $1 ORDER BY email ASC, id ASC"
    ))
    .bind(org_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// A local user to create.
pub struct NewUser {
    pub id: UserId,
    pub email: String,
    pub password_hash: String,
    pub display_name: String,
    pub org_id: OrgId,
    pub role: UserRole,
}

/// Create a local user. A taken email fails with the unique violation.
pub async fn create_user(executor: impl Executor<'_>, user: NewUser) -> Result<User, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, email, password_hash, display_name, org_id, role, owner_source,
                   created_at, updated_at"
    ))
    .bind(user.id)
    .bind(user.email)
    .bind(user.password_hash)
    .bind(user.display_name)
    .bind(user.org_id)
    .bind(user.role)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Changes to a user's profile; `None` leaves a field as it is.
pub struct UserProfilePatch {
    pub email: Option<String>,
    pub password_hash: Option<String>,
    pub display_name: Option<String>,
}

/// Apply `patch` to the user; `None` when there is none. A taken email fails
/// with the unique violation.
pub async fn update_user_profile(
    executor: impl Executor<'_>,
    user_id: UserId,
    patch: UserProfilePatch,
) -> Result<Option<User>, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "UPDATE users
         SET email = COALESCE($2, email),
             password_hash = COALESCE($3, password_hash),
             display_name = COALESCE($4, display_name),
             updated_at = NOW()
         WHERE id = $1
         RETURNING id, email, password_hash, display_name, org_id, role, owner_source,
                   created_at, updated_at"
    ))
    .bind(user_id)
    .bind(patch.email)
    .bind(patch.password_hash)
    .bind(patch.display_name)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Give the user `role`; `None` when there is none.
pub async fn set_user_role(
    executor: impl Executor<'_>,
    user_id: UserId,
    role: UserRole,
) -> Result<Option<User>, DbError> {
    Ok(sqlx::query_as::<_, User>(sql!(
        "UPDATE users SET role = $2, updated_at = NOW() WHERE id = $1
         RETURNING id, email, password_hash, display_name, org_id, role, owner_source,
                   created_at, updated_at"
    ))
    .bind(user_id)
    .bind(role)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Set the user's password hash.
pub async fn set_user_password_hash(
    executor: impl Executor<'_>,
    user_id: UserId,
    password_hash: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2"
    ))
    .bind(password_hash)
    .bind(user_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete the user; `false` when there was none. A row that references it
/// through a foreign key without `ON DELETE CASCADE` or `ON DELETE SET NULL`
/// fails the delete with that key's error; the others are deleted or cleared
/// with it.
pub async fn delete_user(executor: impl Executor<'_>, user_id: UserId) -> Result<bool, DbError> {
    let result = sqlx::query(sql!("DELETE FROM users WHERE id = $1"))
        .bind(user_id)
        .execute(executor.into_sqlx())
        .await?;
    Ok(result.rows_affected() > 0)
}

/// The user a federated owner's upstream subject maps to, in its
/// organization.
pub struct FederatedUser<'a> {
    /// Used only when no user maps to the upstream subject yet.
    pub proposed_id: UserId,
    /// Used only when no user maps to the upstream subject yet.
    pub proposed_email: &'a str,
    pub display_name: &'a str,
    pub org_id: OrgId,
    pub upstream_issuer: &'a str,
    pub upstream_subject_id: &'a str,
}

/// The id and email of a federated owner's user.
#[derive(Debug, Clone, FromRow)]
pub struct FederatedUserRecord {
    pub id: UserId,
    pub email: String,
}

/// Create the user for an upstream subject in its organization, or find the
/// one already created for it. Concurrent calls for one subject return the
/// same user.
pub async fn upsert_federated_user(
    executor: impl Executor<'_>,
    user: FederatedUser<'_>,
) -> Result<FederatedUserRecord, DbError> {
    Ok(sqlx::query_as::<_, FederatedUserRecord>(sql!(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         )
         VALUES (
             $1, $2, 'federated-user-no-local-password', $3, $4, 'member',
             'federated_identity', $5, $6
         )
         ON CONFLICT (org_id, upstream_issuer, upstream_subject_id)
             WHERE owner_source = 'federated_identity'
         DO UPDATE SET display_name = users.display_name
         RETURNING id, email"
    ))
    .bind(user.proposed_id)
    .bind(user.proposed_email)
    .bind(user.display_name)
    .bind(user.org_id)
    .bind(user.upstream_issuer)
    .bind(user.upstream_subject_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}
