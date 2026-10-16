//! Service-asserted product policy bindings, separate from sandbox execution.

use pplx_space_common_core::{
    sql,
    types::{SandboxId, SandboxLifecycle},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyBinding {
    pub user_id: i64,
    pub organization_uuid: Option<Uuid>,
    pub session_id: String,
    pub context_uuid: Option<Uuid>,
}

impl PolicyBinding {
    pub fn validate(&self) -> Result<(), InvalidRegistration> {
        if self.user_id <= 0
            || self.session_id.is_empty()
            || self.session_id.len() > 128
            || !self
                .session_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(InvalidRegistration);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid lifecycle registration")]
pub struct InvalidRegistration;

#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error(transparent)]
    Invalid(#[from] InvalidRegistration),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Zero is the initial absent registration; positive revisions are compare-and-swap guards.
pub async fn register(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    enabled: bool,
    binding: &PolicyBinding,
    expected_revision: i64,
) -> Result<Option<i64>, RegistrationError> {
    binding.validate()?;
    if !(0..i64::MAX).contains(&expected_revision) {
        return Err(InvalidRegistration.into());
    }
    let revision = if expected_revision == 0 {
        sqlx::query_scalar(sql!(
            "INSERT INTO sandbox_lifecycle_registrations
                (sandbox_id, enabled, revision, product_user_id, product_organization_uuid,
                 product_session_id, product_context_uuid)
             SELECT id, $2, 1, $3, $4, $5, $6 FROM sandboxes WHERE id = $1
             ON CONFLICT (sandbox_id) DO NOTHING RETURNING revision"
        ))
        .bind(sandbox_id)
        .bind(enabled)
        .bind(binding.user_id)
        .bind(binding.organization_uuid)
        .bind(&binding.session_id)
        .bind(binding.context_uuid)
        .fetch_optional(executor)
        .await?
    } else {
        sqlx::query_scalar(sql!(
            "UPDATE sandbox_lifecycle_registrations
             SET enabled = $2, revision = revision + 1, product_user_id = $3,
                 product_organization_uuid = $4, product_session_id = $5, product_context_uuid = $6
             WHERE sandbox_id = $1 AND revision = $7 RETURNING revision"
        ))
        .bind(sandbox_id)
        .bind(enabled)
        .bind(binding.user_id)
        .bind(binding.organization_uuid)
        .bind(&binding.session_id)
        .bind(binding.context_uuid)
        .bind(expected_revision)
        .fetch_optional(executor)
        .await?
    };
    Ok(revision)
}

pub async fn revision(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<i64>, RegistrationError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT revision FROM sandbox_lifecycle_registrations WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?)
}

#[derive(Debug, sqlx::FromRow)]
pub struct Candidate {
    pub sandbox_id: SandboxId,
    pub registration_revision: i64,
    pub user_id: i64,
    pub organization_uuid: Option<Uuid>,
    pub session_id: String,
    pub context_uuid: Option<Uuid>,
    pub lifecycle: SandboxLifecycle,
    pub last_access_at: Option<time::OffsetDateTime>,
}

/// Activity here is an asynchronous observation, never an execution claim.
pub async fn candidates(
    executor: impl sqlx::PgExecutor<'_>,
    after: Option<SandboxId>,
    limit: u16,
) -> Result<Vec<Candidate>, RegistrationError> {
    if !(1..=501).contains(&limit) {
        return Err(InvalidRegistration.into());
    }
    Ok(sqlx::query_as(sql!(
        "SELECT r.sandbox_id, r.revision AS registration_revision,
                r.product_user_id AS user_id, r.product_organization_uuid AS organization_uuid,
                r.product_session_id AS session_id, r.product_context_uuid AS context_uuid,
                s.lifecycle, s.last_access_at
         FROM sandbox_lifecycle_registrations r JOIN sandboxes s ON s.id = r.sandbox_id
         WHERE r.enabled AND ($1::UUID IS NULL OR r.sandbox_id > $1)
         ORDER BY r.sandbox_id LIMIT $2"
    ))
    .bind(after)
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await?)
}

#[cfg(test)]
mod tests;
