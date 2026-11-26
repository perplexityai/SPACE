//! Owning store for an organization's key inventory and the per-sandbox
//! volume DEK wraps made under it.
//!
//! * `org_master_keys`: each organization's KEKs, at most one active (not
//!   retired) per organization, enforced by a partial unique index.
//! * `org_branching_keys`: the broker-wrapped intermediate key for a KEK
//!   that the intermediate-key broker issued, keyed by the KEK's id.
//! * `sandbox_volume_encryption`: one wrapped volume DEK per sandbox,
//!   naming the KEK (`master_key_id`) that wrapped it.
//!
//! The three tables are credential-fenced: only the credentials roles can
//! read them, so run these verbs on a credentials pool.
//!
//! Verbs are executor-generic free functions, so a caller that needs two of
//! them to see one state composes them in its own transaction. The stored
//! provider and key identifier are returned as text: the caller parses them
//! and reports a malformed row against the KEK it names. Verbs arrive with
//! their first caller.

use pplx_space_accounts::OrgId;
use pplx_space_byok_model::{KekProvider, MasterKeyIdentifier};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use time::OffsetDateTime;
use uuid::Uuid;

/// One `org_master_keys` row.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct MasterKeyRecord {
    /// `org_master_keys.id`: the `cmk_id` a wrap names.
    pub id: Uuid,
    pub org_id: OrgId,
    pub provider: String,
    pub key_identifier: String,
    pub retired_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    /// Trigger-maintained.
    pub updated_at: OffsetDateTime,
}

/// One of the organization's KEKs, as [`org_master_key`] reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrgMasterKey {
    pub provider: String,
    pub key_identifier: String,
    /// `retired_at` is set: the KEK may still unwrap, but wraps nothing new.
    pub retired: bool,
}

/// A sandbox's `sandbox_volume_encryption` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeWrapRecord {
    pub org_id: OrgId,
    /// `org_master_keys.id` of the KEK that wrapped the DEK.
    pub master_key_id: Uuid,
    pub wrapped_dek: Vec<u8>,
}

/// The organization's active (not retired) KEK, or `None` when it has none.
pub async fn active_master_key(
    executor: impl Executor<'_>,
    org: OrgId,
) -> Result<Option<MasterKeyRecord>, DbError> {
    Ok(sqlx::query_as::<_, MasterKeyRecord>(sql!(
        "SELECT id, org_id, provider, key_identifier, retired_at, created_at, updated_at \
         FROM org_master_keys WHERE org_id = $1 AND retired_at IS NULL"
    ))
    .bind(org.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The KEK `cmk_id` when it belongs to `org`, retired or not. `None` when no
/// such row belongs to `org`, whether or not the id exists for another
/// organization.
pub async fn org_master_key(
    executor: impl Executor<'_>,
    cmk_id: Uuid,
    org: OrgId,
) -> Result<Option<OrgMasterKey>, DbError> {
    let row: Option<(String, String, bool)> = sqlx::query_as(sql!(
        "SELECT provider, key_identifier, retired_at IS NOT NULL \
         FROM org_master_keys WHERE id = $1 AND org_id = $2"
    ))
    .bind(cmk_id)
    .bind(org.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(|(provider, key_identifier, retired)| OrgMasterKey {
        provider,
        key_identifier,
        retired,
    }))
}

/// Insert an active KEK for `org` and return its id. `None` when the
/// organization already has an active KEK: the partial unique index on
/// active keys made this insert a no-op, and the other writer's row stands.
pub async fn insert_active_master_key(
    executor: impl Executor<'_>,
    org: OrgId,
    provider: KekProvider,
    key_identifier: &MasterKeyIdentifier,
) -> Result<Option<Uuid>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO org_master_keys (org_id, provider, key_identifier) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (org_id) WHERE retired_at IS NULL DO NOTHING \
         RETURNING id"
    ))
    .bind(org.as_uuid())
    .bind(provider.as_str())
    .bind(key_identifier.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Record the broker-wrapped intermediate key of the KEK `cmk_id`. Run it in
/// the transaction that inserted the KEK, so neither row commits alone.
pub async fn insert_branching_key(
    executor: impl Executor<'_>,
    cmk_id: Uuid,
    org: OrgId,
    wrapped_key: &[u8],
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO org_branching_keys (cmk_id, org_id, wrapped_key) \
         VALUES ($1, $2, $3)"
    ))
    .bind(cmk_id)
    .bind(org.as_uuid())
    .bind(wrapped_key)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// The newest broker-wrapped intermediate key of `org`'s KEK whose provider
/// key identifier is `key_identifier`. The join pins both rows to `org`, so
/// another organization's key reads as `None`.
pub async fn branching_key(
    executor: impl Executor<'_>,
    org: OrgId,
    key_identifier: &MasterKeyIdentifier,
) -> Result<Option<Vec<u8>>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT b.wrapped_key FROM org_branching_keys b \
         JOIN org_master_keys k ON k.id = b.cmk_id AND k.org_id = b.org_id \
         WHERE k.org_id = $1 AND k.key_identifier = $2 \
         ORDER BY b.created_at DESC LIMIT 1"
    ))
    .bind(org.as_uuid())
    .bind(key_identifier.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The sandbox's volume wrap, or `None` when none has been stored.
pub async fn volume_wrap(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<VolumeWrapRecord>, DbError> {
    let row: Option<(OrgId, Uuid, Vec<u8>)> = sqlx::query_as(sql!(
        "SELECT org_id, master_key_id, wrapped_dek \
         FROM sandbox_volume_encryption WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(
        row.map(|(org_id, master_key_id, wrapped_dek)| VolumeWrapRecord {
            org_id,
            master_key_id,
            wrapped_dek,
        }),
    )
}

/// Store the sandbox's volume wrap unless it already has one. `false` when
/// another writer's wrap is already stored: this one is discarded, and the
/// caller reads the stored wrap with [`volume_wrap`].
pub async fn insert_volume_wrap(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    org_id: OrgId,
    master_key_id: Uuid,
    wrapped_dek: &[u8],
) -> Result<bool, DbError> {
    let inserted = sqlx::query(sql!(
        "INSERT INTO sandbox_volume_encryption (sandbox_id, org_id, master_key_id, wrapped_dek) \
         VALUES ($1, $2, $3, $4) ON CONFLICT (sandbox_id) DO NOTHING"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .bind(master_key_id)
    .bind(wrapped_dek)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(inserted == 1)
}

#[cfg(test)]
mod tests;
