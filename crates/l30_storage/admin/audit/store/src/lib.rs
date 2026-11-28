//! Owning store for the admin audit log in control-plane Postgres: the
//! `admin_audit_events` rows, one per persisted admin request.
//!
//! The API's admin gate writes a row with [`insert`] and its audit route
//! lists them with [`list_newest`]. The cluster controller's retention loop
//! deletes aged rows with [`delete_older_than`], caps the table with
//! [`delete_beyond_newest`] and reports [`count`]. The age-based delete
//! compares `created_at` with the database clock in its SQL, so it compares
//! against the start of the transaction the caller runs it in.
//!
//! Every verb takes an executor, so a caller can run it in its own
//! transaction.

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// One persisted admin request.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct AdminAuditEvent {
    pub id: Uuid,
    pub actor_user_id: UserId,
    pub actor_org_id: OrgId,
    pub actor_email: String,
    pub method: String,
    pub path: String,
    pub status_code: i32,
    pub success: bool,
    pub duration_ms: i64,
    pub target_org_id: Option<OrgId>,
    /// Read as text so a type a newer release writes still decodes.
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub target_user_id: Option<UserId>,
    pub created_at: OffsetDateTime,
}

/// The columns of a new row; `created_at` takes the database default.
#[derive(Debug, Clone, Copy)]
pub struct NewAdminAuditEvent<'a> {
    pub id: Uuid,
    pub actor_user_id: UserId,
    pub actor_org_id: OrgId,
    pub actor_email: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub status_code: i32,
    pub success: bool,
    pub duration_ms: i64,
    pub target_org_id: Option<OrgId>,
    pub target_type: Option<&'a str>,
    pub target_id: Option<&'a str>,
    pub target_user_id: Option<UserId>,
}

/// The newest `limit` events, newest first.
pub async fn list_newest(
    executor: impl Executor<'_>,
    limit: i64,
) -> Result<Vec<AdminAuditEvent>, DbError> {
    Ok(sqlx::query_as::<_, AdminAuditEvent>(sql!(
        "SELECT
             id,
             actor_user_id,
             actor_org_id,
             actor_email,
             method,
             path,
             status_code,
             success,
             duration_ms,
             target_org_id,
             target_type,
             target_id,
             target_user_id,
             created_at
         FROM admin_audit_events
         ORDER BY created_at DESC, id DESC
         LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?)
}

pub async fn insert(
    executor: impl Executor<'_>,
    event: NewAdminAuditEvent<'_>,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO admin_audit_events (
             id,
             actor_user_id,
             actor_org_id,
             actor_email,
             method,
             path,
             status_code,
             success,
             duration_ms,
             target_org_id,
             target_type,
             target_id,
             target_user_id
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
    ))
    .bind(event.id)
    .bind(event.actor_user_id)
    .bind(event.actor_org_id)
    .bind(event.actor_email)
    .bind(event.method)
    .bind(event.path)
    .bind(event.status_code)
    .bind(event.success)
    .bind(event.duration_ms)
    .bind(event.target_org_id)
    .bind(event.target_type)
    .bind(event.target_id)
    .bind(event.target_user_id)
    .execute(executor.into_sqlx())
    .await?;

    Ok(())
}

/// Delete up to `limit` of the events ranked after the newest `keep`, oldest
/// first, and return how many it deleted.
pub async fn delete_beyond_newest(
    executor: impl Executor<'_>,
    keep: i64,
    limit: i64,
) -> Result<usize, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "WITH ranked AS (
             SELECT id,
                    ROW_NUMBER() OVER (ORDER BY created_at DESC, id DESC) AS row_num
               FROM admin_audit_events
         ),
         candidates AS (
             SELECT id
               FROM ranked
              WHERE row_num > $1
              ORDER BY row_num DESC
              LIMIT $2
         )
         DELETE FROM admin_audit_events e
          USING candidates c
          WHERE e.id = c.id
          RETURNING e.id::TEXT"
    ))
    .bind(keep)
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?
    .len())
}

/// Delete up to `limit` of the events created more than `retention_secs`
/// before the database clock, oldest first, and return how many it deleted.
pub async fn delete_older_than(
    executor: impl Executor<'_>,
    retention_secs: i64,
    limit: i64,
) -> Result<usize, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "WITH candidates AS (
                 SELECT id
                   FROM admin_audit_events
                  WHERE created_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                  ORDER BY created_at ASC, id ASC
                  LIMIT $2
             )
             DELETE FROM admin_audit_events e
              USING candidates c
              WHERE e.id = c.id
              RETURNING e.id::TEXT"
    ))
    .bind(retention_secs)
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?
    .len())
}

/// How many events the table holds.
pub async fn count(executor: impl Executor<'_>) -> Result<i64, DbError> {
    Ok(
        sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*) FROM admin_audit_events"))
            .fetch_one(executor.into_sqlx())
            .await?,
    )
}
