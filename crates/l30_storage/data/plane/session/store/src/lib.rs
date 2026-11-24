//! Storage for the control-plane Postgres `data_plane_sessions` and
//! `data_plane_org_revocation_epochs` tables: the sessions the public API
//! creates for the data plane to attach to, and each org's revocation epoch,
//! which advances each time a session in the org is first revoked.
//!
//! The public API inserts sessions, reads their process bindings and advances
//! an org's epoch here; the internal API reads an org's epoch here. Other
//! statements on these tables stay inline in their callers: guarded session
//! transitions, `FOR UPDATE` reads, reads and writes filtered by expiry
//! against the database clock (the public API's session reads among them),
//! the route reads and writes that join the sandbox's placement, and session
//! retention.

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, types::Json};
use time::OffsetDateTime;

/// A session as the public API returns it, from `data_plane_sessions`.
///
/// The vocabulary columns stay text so a value written by a newer build still
/// reads; the caller parses them.
///
/// The public API's session reads and its revoke's `RETURNING`, still inline
/// in `pplx-space-api`, decode this row from their own column lists, so a
/// field added here must be added to those lists too until they move here.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct Session {
    pub session_id: String,
    pub sandbox_id: SandboxId,
    pub kind: String,
    pub state: String,
    pub reconnect_count: i64,
    pub last_reconnect_reason: Option<String>,
    pub scopes: Vec<String>,
    pub target_port: Option<i32>,
    pub target_protocol: Option<String>,
    pub target_path: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
}

/// A session to insert. The caller mints the id and the token, and chooses
/// the deadlines.
#[derive(Debug, Clone, Copy)]
pub struct NewSession<'a> {
    pub session_id: &'a str,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub sandbox_id: SandboxId,
    pub kind: &'a str,
    pub state: &'a str,
    pub scopes: &'a [String],
    pub token_hash: &'a str,
    pub token_expires_at: OffsetDateTime,
    pub attach_deadline_at: OffsetDateTime,
    pub target_port: Option<i32>,
    pub target_protocol: Option<&'a str>,
    pub target_path: Option<&'a str>,
    pub adapter_state: &'a serde_json::Value,
    pub expires_at: OffsetDateTime,
}

/// A session bound to a guest process, from `data_plane_sessions`.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct SessionProcessBinding {
    pub session_id: String,
    pub backend_process_id: String,
}

/// An org's data-plane revocation epoch, from
/// `data_plane_org_revocation_epochs`.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct OrgRevocationEpoch {
    pub epoch: i64,
    pub updated_at: OffsetDateTime,
}

/// Insert a session and return it as stored.
pub async fn insert_session(
    executor: impl sqlx::PgExecutor<'_>,
    session: &NewSession<'_>,
) -> Result<Session, sqlx::Error> {
    sqlx::query_as::<_, Session>(sql!(
        "INSERT INTO data_plane_sessions (
             session_id,
             org_id,
             actor_user_id,
             sandbox_id,
             kind,
             state,
             scopes,
             token_hash,
             token_expires_at,
             attach_deadline_at,
             target_port,
             target_protocol,
             target_path,
             adapter_state,
             expires_at
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7,
             $8, $9, $10, $11, $12, $13, $14, $15
         )
         RETURNING session_id,
                   sandbox_id,
                   kind,
                   state,
                   reconnect_count,
                   last_reconnect_reason,
                   scopes,
                   target_port,
                   target_protocol,
                   target_path,
                   created_at,
                   updated_at,
                   expires_at"
    ))
    .bind(session.session_id)
    .bind(session.org_id)
    .bind(session.actor_user_id)
    .bind(session.sandbox_id)
    .bind(session.kind)
    .bind(session.state)
    .bind(session.scopes)
    .bind(session.token_hash)
    .bind(session.token_expires_at)
    .bind(session.attach_deadline_at)
    .bind(session.target_port)
    .bind(session.target_protocol)
    .bind(session.target_path)
    .bind(Json(session.adapter_state))
    .bind(session.expires_at)
    .fetch_one(executor)
    .await
}

/// The guest process id the data plane bound `session_id` to; `None` when the
/// session is gone or has bound none. Expired and revoked sessions are read
/// too.
pub async fn bound_process_id(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    session_id: &str,
) -> Result<Option<String>, sqlx::Error> {
    let process_id = sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT backend_process_id
         FROM data_plane_sessions
         WHERE org_id = $1
           AND sandbox_id = $2
           AND session_id = $3"
    ))
    .bind(org_id)
    .bind(sandbox_id)
    .bind(session_id)
    .fetch_optional(executor)
    .await?;
    Ok(process_id.flatten())
}

/// The sessions of `sandbox_id` the data plane bound to any of
/// `process_ids`. Expired and revoked sessions are read too.
pub async fn sessions_bound_to_processes(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    process_ids: &[String],
) -> Result<Vec<SessionProcessBinding>, sqlx::Error> {
    sqlx::query_as::<_, SessionProcessBinding>(sql!(
        "SELECT session_id,
                backend_process_id
         FROM data_plane_sessions
         WHERE org_id = $1
           AND sandbox_id = $2
           AND backend_process_id = ANY($3)"
    ))
    .bind(org_id)
    .bind(sandbox_id)
    .bind(process_ids)
    .fetch_all(executor)
    .await
}

/// Advance the org's revocation epoch by one, starting it at 1.
pub async fn advance_org_revocation_epoch(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO data_plane_org_revocation_epochs (org_id, epoch, updated_at)
         VALUES ($1, 1, NOW())
         ON CONFLICT (org_id) DO UPDATE
             SET epoch = data_plane_org_revocation_epochs.epoch + 1,
                 updated_at = EXCLUDED.updated_at"
    ))
    .bind(org_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// The org's revocation epoch; `None` when no session in the org has been
/// revoked.
pub async fn org_revocation_epoch(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
) -> Result<Option<OrgRevocationEpoch>, sqlx::Error> {
    sqlx::query_as::<_, OrgRevocationEpoch>(sql!(
        "SELECT epoch, updated_at
         FROM data_plane_org_revocation_epochs
         WHERE org_id = $1"
    ))
    .bind(org_id)
    .fetch_optional(executor)
    .await
}

#[cfg(test)]
mod tests;
