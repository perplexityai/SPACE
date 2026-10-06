//! `template_system_publication_grants`: the API records one before it
//! dispatches a system template build. A system publication by a publisher that
//! also holds tenant authority (build-manager) needs a fresh one; see
//! `pplx_space_common_svc::auth::build::system_publication_requires_grant`.
//!
//! A grant stays fresh for seven days, the longest a dispatched build can
//! plausibly wait in a builder's queue; the API sweeps older rows.

use pplx_space_common_core::{sql, types::TemplateName};
use sqlx::PgExecutor;
use tonic::Status;

pub(super) fn missing() -> Status {
    Status::permission_denied("system template publication requires an API system build")
}

/// Whether a fresh grant covers a system publication of `template`.
pub(super) async fn covers(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM template_system_publication_grants
             WHERE template_name = $1
               AND granted_at > NOW() - INTERVAL '7 days')"
    ))
    .bind(template.as_str())
    .fetch_one(executor)
    .await
}

/// Spend one fresh grant for `template` in the caller's transaction, oldest
/// first, so each system build publishes at most one release. Concurrent
/// publications skip each other's locked rows and consume grants of their own.
pub(super) async fn consume(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<bool, sqlx::Error> {
    let consumed = sqlx::query_scalar::<_, uuid::Uuid>(sql!(
        "DELETE FROM template_system_publication_grants
         WHERE id = (
             SELECT id
             FROM template_system_publication_grants
             WHERE template_name = $1
               AND granted_at > NOW() - INTERVAL '7 days'
             ORDER BY granted_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED)
         RETURNING id"
    ))
    .bind(template.as_str())
    .fetch_optional(executor)
    .await?;
    Ok(consumed.is_some())
}
