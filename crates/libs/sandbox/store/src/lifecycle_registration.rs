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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub enabled: bool,
    pub revision: i64,
    pub binding: PolicyBinding,
}

/// Zero denotes SQL NULL; positive revisions guard updates to an existing binding.
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
    let registration = Registration {
        enabled,
        revision: expected_revision + 1,
        binding: binding.clone(),
    };
    let changed = sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_registration = $2
         WHERE id = $1 AND (
             ($3 = '0' AND lifecycle_registration IS NULL)
             OR lifecycle_registration ->> 'revision' = $3
         )"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(&registration))
    .bind(expected_revision.to_string())
    .execute(executor)
    .await?
    .rows_affected();
    Ok((changed == 1).then_some(registration.revision))
}

pub async fn revision(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<i64>, RegistrationError> {
    Ok(sqlx::query_scalar::<_, Option<i64>>(sql!(
        "SELECT (lifecycle_registration ->> 'revision')::BIGINT FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?
    .flatten())
}

#[derive(Debug, sqlx::FromRow)]
pub struct Candidate {
    pub sandbox_id: SandboxId,
    pub registration: sqlx::types::Json<Registration>,
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
        "SELECT id AS sandbox_id, lifecycle_registration AS registration, lifecycle, last_access_at
         FROM sandboxes
         WHERE lifecycle_registration ->> 'enabled' = 'true' AND ($1::UUID IS NULL OR id > $1)
         ORDER BY id LIMIT $2"
    ))
    .bind(after)
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await?)
}

#[cfg(test)]
mod tests;
