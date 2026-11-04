//! Owning store for sandbox-scoped federated service grants.
//!
//! It is its own crate so that code reading or writing those grants can
//! depend on the store without depending on `pplx-space-common-svc`.
//! `pplx-space-common-svc` re-exports it as `sql::federated_service_grants`.

use pplx_space_common_core::types::{OrgId, SandboxId};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxOwnerSource;
use sqlx::{PgPool, Postgres};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum FederatedGrantStoreError {
    #[error("sandbox owner does not match federated service grant owner")]
    OwnerMismatch,
    #[error("grant renewal is not newer than the stored grant, and the stored grant has expired")]
    StaleRenewalOfExpiredGrant,
    #[error("failed to upsert sandbox federated service grant: {0}")]
    Upsert(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum FreshestLiveFederatedGrantError {
    #[error("freshest live federated service grant query failed: {0}")]
    Query(#[source] sqlx::Error),
}

/// Borrowed upstream owner fields used to authorize a grant write.
pub struct FederatedGrantOwnerRef<'a> {
    pub issuer: &'a str,
    pub org_id: &'a str,
    pub subject_id: &'a str,
}

/// Borrowed grant envelope persisted by the store.
pub struct FederatedServiceGrantRef<'a> {
    pub bytes: &'a [u8],
    pub expires_at: OffsetDateTime,
    pub issued_at: Option<OffsetDateTime>,
}

/// The freshest usable owner grant for an organization.
pub struct FreshestLiveFederatedGrant {
    pub sandbox_id: SandboxId,
    pub upstream_subject_id: String,
    pub grant_blob: Vec<u8>,
    pub expires_at_unix: i64,
}

pub async fn upsert_sandbox_federated_service_grant(
    pool: &PgPool,
    sandbox_id: SandboxId,
    owner: FederatedGrantOwnerRef<'_>,
    grant: FederatedServiceGrantRef<'_>,
    grant_audience: &str,
    grant_purpose: &str,
) -> Result<(), FederatedGrantStoreError> {
    // The guarded upsert and fallback probe must observe one transaction.
    // The savepoint below also requires this transaction boundary.
    let mut tx = pool.begin().await?;
    upsert_sandbox_federated_service_grant_on_conn(
        &mut tx,
        sandbox_id,
        owner,
        grant,
        grant_audience,
        grant_purpose,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn upsert_sandbox_federated_service_grant_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    owner: FederatedGrantOwnerRef<'_>,
    grant: FederatedServiceGrantRef<'_>,
    grant_audience: &str,
    grant_purpose: &str,
) -> Result<(), FederatedGrantStoreError> {
    upsert_sandbox_federated_service_grant_on_conn(
        tx,
        sandbox_id,
        owner,
        grant,
        grant_audience,
        grant_purpose,
    )
    .await
}

/// Stores the producer-owned grant envelope after verifying the sandbox owner.
async fn upsert_sandbox_federated_service_grant_on_conn(
    executor: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
    owner: FederatedGrantOwnerRef<'_>,
    grant: FederatedServiceGrantRef<'_>,
    grant_audience: &str,
    grant_purpose: &str,
) -> Result<(), FederatedGrantStoreError> {
    // Postgres validates CHECK constraints before conflict arbitration. The
    // savepoint lets an expired offered row be classified by the fallback
    // probe without leaving the caller's transaction aborted.
    sqlx::query(sql!("SAVEPOINT sfsg_upsert"))
        .execute(&mut *executor)
        .await?;
    let row: Result<Option<(Uuid,)>, sqlx::Error> = sqlx::query_as(sql!(
        "INSERT INTO sandbox_federated_service_grants (
             sandbox_id, grant_audience, grant_purpose, grant_blob, grant_expires_at,
             grant_issued_at
         )
         SELECT s.id, $6, $7, $8, $9::timestamptz, $10::timestamptz
         FROM sandboxes s
         JOIN users owner_user ON owner_user.id = s.user_id
         JOIN organizations owner_org ON owner_org.id = s.org_id
         WHERE s.id = $1
           AND owner_user.owner_source = $2
           AND owner_org.owner_source = $2
           AND owner_user.upstream_issuer = $3
           AND owner_org.upstream_issuer = $3
           AND owner_org.upstream_org_id = $4
           AND owner_user.upstream_subject_id = $5
         ON CONFLICT (sandbox_id, grant_audience, grant_purpose) DO UPDATE
         SET grant_blob = EXCLUDED.grant_blob,
             grant_expires_at = EXCLUDED.grant_expires_at,
             grant_issued_at = EXCLUDED.grant_issued_at
         -- Producer order is monotone. Stamped grants supersede unstamped
         -- legacy rows; an unstamped replay never supersedes a stamped row.
         WHERE CASE
             WHEN EXCLUDED.grant_issued_at IS NOT NULL
                  AND sandbox_federated_service_grants.grant_issued_at IS NOT NULL
             THEN EXCLUDED.grant_issued_at > sandbox_federated_service_grants.grant_issued_at
             WHEN EXCLUDED.grant_issued_at IS NOT NULL THEN TRUE
             WHEN sandbox_federated_service_grants.grant_issued_at IS NOT NULL THEN FALSE
             ELSE EXCLUDED.grant_expires_at > sandbox_federated_service_grants.grant_expires_at
         END
         RETURNING sandbox_id"
    ))
    .bind(sandbox_id)
    .bind(SandboxOwnerSource::FederatedIdentity.as_str())
    .bind(owner.issuer)
    .bind(owner.org_id)
    .bind(owner.subject_id)
    .bind(grant_audience)
    .bind(grant_purpose)
    .bind(grant.bytes)
    .bind(grant.expires_at)
    .bind(grant.issued_at)
    .fetch_optional(&mut *executor)
    .await;

    let row = match row {
        Ok(row) => row,
        Err(sqlx::Error::Database(db_error))
            if db_error.constraint()
                == Some("sandbox_federated_service_grants_expires_after_update") =>
        {
            sqlx::query(sql!("ROLLBACK TO SAVEPOINT sfsg_upsert"))
                .execute(&mut *executor)
                .await?;
            None
        }
        Err(error) => return Err(error.into()),
    };
    sqlx::query(sql!("RELEASE SAVEPOINT sfsg_upsert"))
        .execute(&mut *executor)
        .await?;

    if row.is_some() {
        return Ok(());
    }
    // No RETURNING row means owner mismatch, an ordering skip, or an expired
    // offer. One probe distinguishes the owner fault from whether the stored
    // grant remains usable, using the database clock as the expiry arbiter.
    let stored_unexpired: Option<Option<bool>> = sqlx::query_scalar(sql!(
        "SELECT g.grant_expires_at > NOW()
         FROM sandboxes s
         JOIN users owner_user ON owner_user.id = s.user_id
         JOIN organizations owner_org ON owner_org.id = s.org_id
         LEFT JOIN sandbox_federated_service_grants g
             ON g.sandbox_id = s.id
             AND g.grant_audience = $2
             AND g.grant_purpose = $3
         WHERE s.id = $1
           AND owner_user.owner_source = $4
           AND owner_org.owner_source = $4
           AND owner_user.upstream_issuer = $5
           AND owner_org.upstream_issuer = $5
           AND owner_org.upstream_org_id = $6
           AND owner_user.upstream_subject_id = $7"
    ))
    .bind(sandbox_id)
    .bind(grant_audience)
    .bind(grant_purpose)
    .bind(SandboxOwnerSource::FederatedIdentity.as_str())
    .bind(owner.issuer)
    .bind(owner.org_id)
    .bind(owner.subject_id)
    .fetch_optional(executor)
    .await?;
    match stored_unexpired {
        Some(Some(true)) => Ok(()),
        Some(Some(false)) | Some(None) => Err(FederatedGrantStoreError::StaleRenewalOfExpiredGrant),
        None => Err(FederatedGrantStoreError::OwnerMismatch),
    }
}

/// Return the freshest unexpired grant for an org, audience, and purpose.
///
/// Expiry is rounded up to whole seconds so a row accepted by
/// `grant_expires_at > now()` cannot become locally expired solely because of
/// timestamp precision loss at this boundary.
pub async fn freshest_live_federated_grant(
    pool: &PgPool,
    org_id: OrgId,
    grant_audience: &str,
    grant_purpose: &str,
) -> Result<Option<FreshestLiveFederatedGrant>, FreshestLiveFederatedGrantError> {
    let row: Option<(SandboxId, String, Vec<u8>, i64)> = sqlx::query_as(sql!(
        "SELECT grants.sandbox_id, owner_user.upstream_subject_id, grants.grant_blob,
                ceil(extract(epoch FROM grants.grant_expires_at))::bigint
         FROM sandbox_federated_service_grants grants
         JOIN sandboxes s ON s.id = grants.sandbox_id
         JOIN users owner_user ON owner_user.id = s.user_id
         WHERE s.org_id = $1
           AND grants.grant_audience = $2
           AND grants.grant_purpose = $3
           AND grants.grant_expires_at > now()
           AND owner_user.upstream_subject_id IS NOT NULL
         ORDER BY grants.grant_expires_at DESC
         LIMIT 1"
    ))
    .bind(org_id)
    .bind(grant_audience)
    .bind(grant_purpose)
    .fetch_optional(pool)
    .await
    .map_err(FreshestLiveFederatedGrantError::Query)?;

    Ok(row.map(
        |(sandbox_id, upstream_subject_id, grant_blob, expires_at_unix)| {
            FreshestLiveFederatedGrant {
                sandbox_id,
                upstream_subject_id,
                grant_blob,
                expires_at_unix,
            }
        },
    ))
}

/// Return an unexpired grant for an org, preferring the requesting sandbox's
/// own grant over a fresher sibling grant. When the requesting sandbox has no
/// live grant, the org-wide freshest grant is the fallback.
///
/// Expiry is rounded up to whole seconds for the same reason as
/// [`freshest_live_federated_grant`].
pub async fn live_federated_grant_prefer_sandbox(
    pool: &PgPool,
    org_id: OrgId,
    grant_audience: &str,
    grant_purpose: &str,
    sandbox_id: SandboxId,
) -> Result<Option<FreshestLiveFederatedGrant>, FreshestLiveFederatedGrantError> {
    let row: Option<(SandboxId, String, Vec<u8>, i64)> = sqlx::query_as(sql!(
        "SELECT grants.sandbox_id, owner_user.upstream_subject_id, grants.grant_blob,
                ceil(extract(epoch FROM grants.grant_expires_at))::bigint
         FROM sandbox_federated_service_grants grants
         JOIN sandboxes s ON s.id = grants.sandbox_id
         JOIN users owner_user ON owner_user.id = s.user_id
         WHERE s.org_id = $1
           AND grants.grant_audience = $2
           AND grants.grant_purpose = $3
           AND grants.grant_expires_at > now()
           AND owner_user.upstream_subject_id IS NOT NULL
         ORDER BY (grants.sandbox_id = $4) DESC, grants.grant_expires_at DESC
         LIMIT 1"
    ))
    .bind(org_id)
    .bind(grant_audience)
    .bind(grant_purpose)
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await
    .map_err(FreshestLiveFederatedGrantError::Query)?;

    Ok(row.map(
        |(sandbox_id, upstream_subject_id, grant_blob, expires_at_unix)| {
            FreshestLiveFederatedGrant {
                sandbox_id,
                upstream_subject_id,
                grant_blob,
                expires_at_unix,
            }
        },
    ))
}
