//! Durable owner mappings. Transactions are supplied by the identity module.

use super::{
    FederatedOwner,
    identity::{FederatedOwnerRecord, MaterializeOwnerError},
};
use crate::models::{Organization, User};
use crate::user_role::UserRole;
use pplx_space_common_core::{
    sql,
    types::{OrgId, SandboxOwnerSource, UserId},
};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// Columns of one `users u JOIN organizations o` row, aliased so the two
/// tables' shared names (`id`, `name`, `created_at`, ...) do not collide.
const USER_ORG_COLUMNS: &str = "
    u.id AS user_id, u.email AS user_email, u.password_hash AS user_password_hash,
    u.display_name AS user_display_name, u.role AS user_role,
    u.owner_source AS user_owner_source, u.created_at AS user_created_at,
    u.updated_at AS user_updated_at,
    o.id AS org_id, o.name AS org_name, o.slug AS org_slug, o.parent_id AS org_parent_id,
    o.theme_primary AS org_theme_primary, o.theme_secondary AS org_theme_secondary,
    o.logo_url AS org_logo_url, o.byok_enabled AS org_byok_enabled,
    o.snapshot_base_object_storage_uri AS org_snapshot_base_object_storage_uri,
    o.created_at AS org_created_at, o.updated_at AS org_updated_at";

#[derive(sqlx::FromRow)]
struct UserOrgRow {
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

impl UserOrgRow {
    fn into_pair(self) -> (User, Organization) {
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
        (user, org)
    }
}

/// Load the durable effective principal after federated owner materialization.
///
/// One statement: a user whose org row is missing is the same `RowNotFound`
/// the former user-then-org pair returned from its second read.
pub(super) async fn get_user_org(
    pool: &PgPool,
    user_id: UserId,
) -> Result<(User, Organization), sqlx::Error> {
    let query = format!(
        "SELECT {USER_ORG_COLUMNS}
         FROM users u
         JOIN organizations o ON o.id = u.org_id
         WHERE u.id = $1"
    );
    sqlx::query_as::<_, UserOrgRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(user_id)
        .fetch_one(pool)
        .await
        .map(UserOrgRow::into_pair)
}

/// Read-only lookup of an already-materialized federated owner. A miss means
/// the caller must take the race-safe materialization path.
///
/// The org and subject predicates are the ones the former org-then-user pair
/// applied, joined on the subject's org, so a missing org or a missing subject
/// is still the same `None`.
pub(super) async fn find_federated_user_org(
    pool: &PgPool,
    owner: &FederatedOwner,
) -> Result<Option<(User, Organization)>, sqlx::Error> {
    let query = format!(
        "SELECT {USER_ORG_COLUMNS}
         FROM organizations o
         JOIN users u
           ON u.org_id = o.id
          AND u.owner_source = 'federated_identity'
          AND u.upstream_issuer = $1
          AND u.upstream_subject_id = $3
         WHERE o.owner_source = 'federated_identity'
           AND o.upstream_issuer = $1
           AND o.upstream_org_id = $2"
    );
    sqlx::query_as::<_, UserOrgRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(owner.issuer.as_str())
        .bind(owner.org_id.as_str())
        .bind(owner.subject_id.as_str())
        .fetch_optional(pool)
        .await
        .map(|row| row.map(UserOrgRow::into_pair))
}

pub(super) async fn materialize_owner_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    owner: &FederatedOwner,
) -> Result<FederatedOwnerRecord, MaterializeOwnerError> {
    let proposed_org_id = OrgId::new();
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
    .bind(proposed_org_id)
    .bind(format!(
        "Federated {} org {}",
        owner.issuer.as_str(),
        owner.org_id.as_str()
    ))
    .bind(format!("federated-{proposed_org_id}"))
    .bind(owner.issuer.as_str())
    .bind(owner.org_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(MaterializeOwnerError::Organization)?;

    let proposed_user_id = UserId::new();
    let email = format!("federated-{proposed_user_id}@space.invalid");
    let (user_id, email): (UserId, String) = sqlx::query_as(sql!(
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
    .bind(proposed_user_id)
    .bind(&email)
    .bind(format!(
        "Federated {} user {}",
        owner.issuer.as_str(),
        owner.subject_id.as_str()
    ))
    .bind(org_id)
    .bind(owner.issuer.as_str())
    .bind(owner.subject_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(MaterializeOwnerError::User)?;

    Ok(FederatedOwnerRecord {
        user_id,
        org_id,
        email,
    })
}
