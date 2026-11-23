//! `users` rows: lookups by id, email and organization, and the local and
//! federated creates.

use pplx_space_accounts::{OrgId, SandboxOwnerSource, UserId};
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;

use crate::UserRole;

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
    executor: impl PgExecutor<'_>,
    user_id: UserId,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE id = $1"
    ))
    .bind(user_id)
    .fetch_optional(executor)
    .await
}

/// The user with exactly this email.
pub async fn user_by_email(
    executor: impl PgExecutor<'_>,
    email: &str,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE email = $1"
    ))
    .bind(email)
    .fetch_optional(executor)
    .await
}

/// The user, only when it belongs to `org_id`.
pub async fn organization_user(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    user_id: UserId,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users
         WHERE id = $1 AND org_id = $2"
    ))
    .bind(user_id)
    .bind(org_id)
    .fetch_optional(executor)
    .await
}

/// Whether `user_id` is a user of `org_id`.
pub async fn user_in_organization(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    user_id: UserId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND org_id = $2)"
    ))
    .bind(user_id)
    .bind(org_id)
    .fetch_one(executor)
    .await
}

/// The organization's oldest user.
pub async fn first_organization_user_id(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<Option<UserId>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT id FROM users WHERE org_id = $1 ORDER BY created_at, id LIMIT 1"
    ))
    .bind(org_id)
    .fetch_optional(executor)
    .await
}

/// The organization's users, by email.
pub async fn list_organization_users(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<Vec<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE org_id = $1 ORDER BY email ASC, id ASC"
    ))
    .bind(org_id)
    .fetch_all(executor)
    .await
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
pub async fn create_user(
    executor: impl PgExecutor<'_>,
    user: NewUser,
) -> Result<User, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
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
    .fetch_one(executor)
    .await
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
    executor: impl PgExecutor<'_>,
    user_id: UserId,
    patch: UserProfilePatch,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
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
    .fetch_optional(executor)
    .await
}

/// Give the user `role`; `None` when there is none.
pub async fn set_user_role(
    executor: impl PgExecutor<'_>,
    user_id: UserId,
    role: UserRole,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
        "UPDATE users SET role = $2, updated_at = NOW() WHERE id = $1
         RETURNING id, email, password_hash, display_name, org_id, role, owner_source,
                   created_at, updated_at"
    ))
    .bind(user_id)
    .bind(role)
    .fetch_optional(executor)
    .await
}

/// Set the user's password hash.
pub async fn set_user_password_hash(
    executor: impl PgExecutor<'_>,
    user_id: UserId,
    password_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2"
    ))
    .bind(password_hash)
    .bind(user_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete the user; `false` when there was none. A row that references it
/// through a foreign key without `ON DELETE CASCADE` or `ON DELETE SET NULL`
/// fails the delete with that key's error; the others are deleted or cleared
/// with it.
pub async fn delete_user(
    executor: impl PgExecutor<'_>,
    user_id: UserId,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!("DELETE FROM users WHERE id = $1"))
        .bind(user_id)
        .execute(executor)
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
    executor: impl PgExecutor<'_>,
    user: FederatedUser<'_>,
) -> Result<FederatedUserRecord, sqlx::Error> {
    sqlx::query_as::<_, FederatedUserRecord>(sql!(
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
    .fetch_one(executor)
    .await
}
