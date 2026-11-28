//! Storage for the control-plane Postgres `data_plane_sessions` and
//! `data_plane_org_revocation_epochs` tables: the sessions the public API
//! creates for the data plane to attach to, and each org's revocation epoch,
//! which advances each time a session in the org is first revoked.
//!
//! The public API inserts, reads, lists and revokes sessions, refreshes a
//! replayed session's attach token, reads their process bindings and
//! advances an org's epoch here. The internal API binds a session to its
//! process, closes it when that process exits, records reconnects and reads
//! revocation state and an org's epoch here. The cluster controller's
//! retention loop deletes ended sessions with [`delete_ended_sessions`].
//!
//! Every verb except [`lock_live_revocation_status`], [`revoke_live_session`]
//! and [`direct::repoint_session_route`] takes an executor, so a caller can
//! run it in its own transaction. Those three take a transaction or
//! connection and refuse a pool: the caller passes its open transaction, and
//! the lock [`lock_live_revocation_status`] takes lasts until that
//! transaction ends.
//! Statements that compare with the database clock (`NOW()`) keep the
//! comparison in their SQL: the caller's executor decides which transaction's
//! start time they compare against. Guarded transitions
//! ([`revoke_live_session`], [`close_exited_process_session`],
//! [`bind_session_process`], [`refresh_live_attach_token`]) return their
//! outcome; matching no row is another writer winning or nothing left to do,
//! and the caller decides what to do with it.
//!
//! The internal API resolves and refreshes a session's route through
//! [`direct`], whose statements join the sandbox's placement and the Direct
//! substrate's tables.

pub mod direct;

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, types::Json};
use time::OffsetDateTime;

/// A session as the public API returns it, from `data_plane_sessions`.
///
/// The vocabulary columns stay text so a value written by a newer build still
/// reads; the caller parses them.
///
/// [`insert_session`], [`live_session`], [`list_live_sessions`] and
/// [`revoke_live_session`] each list these columns, so a field added here
/// must be added to all four.
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

/// Where a page of [`list_live_sessions`] starts: the sessions after this one
/// in its newest-first order, those with a smaller `(created_at, session_id)`.
#[derive(Debug, Clone, Copy)]
pub struct SessionCursor<'a> {
    pub created_at: OffsetDateTime,
    pub session_id: &'a str,
}

/// A terminal or process session as [`bind_session_process`] left it.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct BoundSessionProcess {
    pub session_id: String,
    pub protocol_session_id: Option<String>,
    pub backend_process_id: String,
    /// Whether this call bound the process, rather than finding it bound.
    pub newly_bound: bool,
}

/// A session's reconnect count as [`record_reconnect`] left it.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct SessionReconnect {
    pub session_id: String,
    pub protocol_session_id: Option<String>,
    pub reconnect_count: i64,
    pub last_reconnect_reason: String,
}

/// A session's revocation state with its org's epoch, from
/// `data_plane_sessions` and `data_plane_org_revocation_epochs`.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct SessionRevocation {
    pub session_id: String,
    pub protocol_session_id: Option<String>,
    pub org_id: OrgId,
    pub revocation_epoch: i64,
    pub revocation_status: String,
    pub revocation_reason: Option<String>,
    pub revocation_updated_at: OffsetDateTime,
    /// 0 when no session in the org has been revoked.
    pub org_revocation_epoch: i64,
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
    executor: impl Executor<'_>,
    session: &NewSession<'_>,
) -> Result<Session, DbError> {
    Ok(sqlx::query_as::<_, Session>(sql!(
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
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The guest process id the data plane bound `session_id` to; `None` when the
/// session is gone or has bound none. Expired and revoked sessions are read
/// too.
pub async fn bound_process_id(
    executor: impl Executor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    session_id: &str,
) -> Result<Option<String>, DbError> {
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
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(process_id.flatten())
}

/// The sessions of `sandbox_id` the data plane bound to any of
/// `process_ids`. Expired and revoked sessions are read too.
pub async fn sessions_bound_to_processes(
    executor: impl Executor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    process_ids: &[String],
) -> Result<Vec<SessionProcessBinding>, DbError> {
    Ok(sqlx::query_as::<_, SessionProcessBinding>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Advance the org's revocation epoch by one, starting it at 1.
pub async fn advance_org_revocation_epoch(
    executor: impl Executor<'_>,
    org_id: OrgId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO data_plane_org_revocation_epochs (org_id, epoch, updated_at)
         VALUES ($1, 1, NOW())
         ON CONFLICT (org_id) DO UPDATE
             SET epoch = data_plane_org_revocation_epochs.epoch + 1,
                 updated_at = EXCLUDED.updated_at"
    ))
    .bind(org_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// The org's revocation epoch; `None` when no session in the org has been
/// revoked.
pub async fn org_revocation_epoch(
    executor: impl Executor<'_>,
    org_id: OrgId,
) -> Result<Option<OrgRevocationEpoch>, DbError> {
    Ok(sqlx::query_as::<_, OrgRevocationEpoch>(sql!(
        "SELECT epoch, updated_at
         FROM data_plane_org_revocation_epochs
         WHERE org_id = $1"
    ))
    .bind(org_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The session `session_id` of `sandbox_id`, unless it is gone or has
/// expired by the database clock.
pub async fn live_session(
    executor: impl Executor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    session_id: &str,
) -> Result<Option<Session>, DbError> {
    Ok(sqlx::query_as::<_, Session>(sql!(
        "SELECT session_id,
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
       expires_at
FROM data_plane_sessions
WHERE org_id = $1
  AND sandbox_id = $2
  AND session_id = $3
  AND (expires_at IS NULL OR expires_at > NOW())"
    ))
    .bind(org_id)
    .bind(sandbox_id)
    .bind(session_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Up to `limit` of the sessions of `sandbox_id` that have not expired by
/// the database clock, newest first, of `kind` when one is given, and
/// starting after `cursor` when one is given.
pub async fn list_live_sessions(
    executor: impl Executor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    kind: Option<&str>,
    cursor: Option<SessionCursor<'_>>,
    limit: i64,
) -> Result<Vec<Session>, DbError> {
    Ok(sqlx::query_as::<_, Session>(sql!(
        "SELECT session_id,
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
                    expires_at
             FROM data_plane_sessions
             WHERE org_id = $1
               AND sandbox_id = $2
               AND ($3::TEXT IS NULL OR kind = $3)
               AND (
                   $4::TIMESTAMPTZ IS NULL
                   OR (created_at, session_id) < ($4::TIMESTAMPTZ, $5::TEXT)
               )
               AND (expires_at IS NULL OR expires_at > NOW())
             ORDER BY created_at DESC, session_id DESC
             LIMIT $6"
    ))
    .bind(org_id)
    .bind(sandbox_id)
    .bind(kind)
    .bind(cursor.map(|cursor| cursor.created_at))
    .bind(cursor.map(|cursor| cursor.session_id))
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Lock the session `session_id` of `sandbox_id` and read its revocation
/// status, unless it is gone or has expired by the database clock.
///
/// The row lock (`FOR UPDATE`) lasts until the caller's transaction ends.
/// It takes a `Tx` or a `PgConnection` and refuses a pool, but cannot tell
/// whether a `PgConnection` has a transaction open: the caller must pass its
/// open transaction.
pub async fn lock_live_revocation_status<C>(
    tx: &mut C,
    org_id: OrgId,
    sandbox_id: SandboxId,
    session_id: &str,
) -> Result<Option<String>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT revocation_status
             FROM data_plane_sessions
             WHERE org_id = $1
               AND sandbox_id = $2
               AND session_id = $3
               AND (expires_at IS NULL OR expires_at > NOW())
             FOR UPDATE"
    ))
    .bind(org_id)
    .bind(sandbox_id)
    .bind(session_id)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// Revoke the session `session_id` of `sandbox_id` and return it as revoked;
/// `None` when it is gone or has expired by the database clock.
///
/// Its token is cleared and its first revocation reason kept. Its revocation
/// epoch advances only when `advance_epoch`, which the caller decides from
/// the status it read under [`lock_live_revocation_status`]. The caller must
/// pass its open transaction.
pub async fn revoke_live_session<C>(
    tx: &mut C,
    org_id: OrgId,
    sandbox_id: SandboxId,
    session_id: &str,
    reason: &str,
    advance_epoch: bool,
) -> Result<Option<Session>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, Session>(sql!(
        "UPDATE data_plane_sessions
             SET revocation_epoch = CASE WHEN $5::BOOL THEN revocation_epoch + 1 ELSE revocation_epoch END,
                 revocation_status = 'revoked',
                 revocation_reason = COALESCE(revocation_reason, $4),
                 revocation_updated_at = NOW(),
                 state = 'revoked',
                 token_hash = NULL,
                 token_expires_at = NOW(),
                 attach_deadline_at = NULL,
                 updated_at = NOW()
             WHERE org_id = $1
               AND sandbox_id = $2
               AND session_id = $3
               AND (expires_at IS NULL OR expires_at > NOW())
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
    .bind(org_id)
    .bind(sandbox_id)
    .bind(session_id)
    .bind(reason)
    .bind(advance_epoch)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// Give the session `session_id` a new attach token, and report whether it
/// did: `false` when its revocation has started (its `revocation_status` is
/// no longer `active`), it has expired by the database clock, or it is gone.
pub async fn refresh_live_attach_token(
    executor: impl Executor<'_>,
    session_id: &str,
    token_hash: &str,
    token_expires_at: OffsetDateTime,
    attach_deadline_at: OffsetDateTime,
) -> Result<bool, DbError> {
    let refreshed = sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET token_hash = $2,
             token_expires_at = $3,
             attach_deadline_at = $4,
             updated_at = NOW()
         WHERE session_id = $1
           AND revocation_status = 'active'
           AND (expires_at IS NULL OR expires_at > NOW())"
    ))
    .bind(session_id)
    .bind(token_hash)
    .bind(token_expires_at)
    .bind(attach_deadline_at)
    .execute(executor.into_sqlx())
    .await?;
    Ok(refreshed.rows_affected() > 0)
}

/// Bind the terminal or process session `session_id` to `process_id`, and
/// return it as bound; `None` when it is gone, has expired by the database
/// clock, is of another kind, carries another protocol session id than
/// `protocol_session_id` (when one is given), or is already bound to another
/// process.
///
/// Binding the process it is already bound to changes no binding and reports
/// `newly_bound: false`. When `records_exit`, a pending session opens. The
/// `FOR UPDATE` in the statement serves only this statement, so the verb
/// takes any executor. Like the `UPDATE`'s own row lock, it is held until the
/// transaction the statement runs in ends; on a pool, that is the statement.
pub async fn bind_session_process(
    executor: impl Executor<'_>,
    session_id: &str,
    protocol_session_id: Option<&str>,
    process_id: &str,
    records_exit: bool,
) -> Result<Option<BoundSessionProcess>, DbError> {
    Ok(sqlx::query_as::<_, BoundSessionProcess>(sql!(
        "WITH target AS (
             SELECT session_id,
                    protocol_session_id,
                    backend_process_id IS NULL AS newly_bound
             FROM data_plane_sessions
             WHERE session_id = $1
               AND ($2::TEXT IS NULL OR protocol_session_id = $2)
               AND kind IN ('terminal', 'process')
               AND (expires_at IS NULL OR expires_at > NOW())
               AND (backend_process_id IS NULL OR backend_process_id = $3)
             FOR UPDATE
         ),
         updated AS (
             UPDATE data_plane_sessions d
             SET backend_process_id = COALESCE(d.backend_process_id, $3),
                 backend_process_bound_at = CASE
                     WHEN d.backend_process_id IS NULL THEN NOW()
                     ELSE d.backend_process_bound_at
                 END,
                 -- The bound process has started: the session is open until
                 -- the caller reports its exit or the session is revoked. A
                 -- caller that does not report exits leaves it pending.
                 state = CASE
                     WHEN $4::BOOLEAN AND d.state = 'pending' THEN 'open'
                     ELSE d.state
                 END,
                 updated_at = NOW()
             FROM target
             WHERE d.session_id = target.session_id
             RETURNING d.session_id,
                       d.protocol_session_id,
                       d.backend_process_id,
                       target.newly_bound
         )
         SELECT session_id,
                protocol_session_id,
                backend_process_id,
                newly_bound
         FROM updated"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .bind(process_id)
    .bind(records_exit)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The guest process id the terminal or process session `session_id` is
/// bound to; `None` when it is gone, has expired by the database clock, is
/// of another kind, carries another protocol session id than
/// `protocol_session_id` (when one is given), or has bound none.
pub async fn live_process_session_bound_process_id(
    executor: impl Executor<'_>,
    session_id: &str,
    protocol_session_id: Option<&str>,
) -> Result<Option<String>, DbError> {
    let process_id = sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT backend_process_id
         FROM data_plane_sessions
         WHERE session_id = $1
           AND ($2::TEXT IS NULL OR protocol_session_id = $2)
           AND kind IN ('terminal', 'process')
           AND (expires_at IS NULL OR expires_at > NOW())"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(process_id.flatten())
}

/// Close the terminal or process session `session_id` because its bound
/// process `process_id` exited, and report whether it closed: only a pending
/// or open session bound to `process_id` that has not expired by the
/// database clock closes, and one that carries another protocol session id
/// than `protocol_session_id` (when one is given) does not.
pub async fn close_exited_process_session(
    executor: impl Executor<'_>,
    session_id: &str,
    protocol_session_id: Option<&str>,
    process_id: &str,
) -> Result<bool, DbError> {
    let closed = sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET state = 'closed',
             updated_at = NOW()
         WHERE session_id = $1
           AND ($2::TEXT IS NULL OR protocol_session_id = $2)
           AND kind IN ('terminal', 'process')
           AND backend_process_id = $3
           AND state IN ('pending', 'open')
           AND (expires_at IS NULL OR expires_at > NOW())"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .bind(process_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(closed.rows_affected() > 0)
}

/// Count a reconnect of the session `session_id` for `reason`, and return
/// its new count; `None` when it is gone, has expired by the database clock,
/// or carries another protocol session id than `protocol_session_id` (when
/// one is given).
pub async fn record_reconnect(
    executor: impl Executor<'_>,
    session_id: &str,
    protocol_session_id: Option<&str>,
    reason: &str,
) -> Result<Option<SessionReconnect>, DbError> {
    Ok(sqlx::query_as::<_, SessionReconnect>(sql!(
        "UPDATE data_plane_sessions
         SET reconnect_count = reconnect_count + 1,
             last_reconnect_reason = $3,
             updated_at = NOW()
         WHERE session_id = $1
           AND ($2::TEXT IS NULL OR protocol_session_id = $2)
           AND (expires_at IS NULL OR expires_at > NOW())
         RETURNING session_id,
                   protocol_session_id,
                   reconnect_count,
                   last_reconnect_reason"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .bind(reason)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The revocation state of the session `session_id` and its org's epoch;
/// `None` when it is gone, has expired by the database clock, or carries
/// another protocol session id than `protocol_session_id` (when one is
/// given).
pub async fn live_session_revocation(
    executor: impl Executor<'_>,
    session_id: &str,
    protocol_session_id: Option<&str>,
) -> Result<Option<SessionRevocation>, DbError> {
    Ok(sqlx::query_as::<_, SessionRevocation>(sql!(
        "SELECT d.session_id,
                d.protocol_session_id,
                d.org_id,
                d.revocation_epoch,
                d.revocation_status,
                d.revocation_reason,
                d.revocation_updated_at,
                COALESCE(o.epoch, 0) AS org_revocation_epoch
         FROM data_plane_sessions d
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
         WHERE d.session_id = $1
           AND ($2::TEXT IS NULL OR d.protocol_session_id = $2)
           AND (d.expires_at IS NULL OR d.expires_at > NOW())"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Delete up to `limit` ended sessions, least recently updated first, and
/// return how many it deleted. A session has ended once `grace_secs` have
/// passed, by the database clock, since it expired, or, for a session that
/// is revoked, failed or (for a kind other than terminal or process) closed,
/// since it was last updated.
///
/// A row bound to a process id other than its session id comes from a guest
/// that minted the id itself. The process routes resolve the session's
/// process id through it for as long as the process runs, which can be long
/// past the session, so it stays until the sandbox is deleted and takes it
/// along. Other guests bind the session id. A closed terminal or process
/// session is a command that exited; it stays until it expires, as an open
/// one does, so that reading it, replaying its create and reattaching to read
/// the exit keep working for the session's lifetime.
pub async fn delete_ended_sessions(
    executor: impl Executor<'_>,
    grace_secs: i64,
    limit: i64,
) -> Result<usize, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "WITH candidates AS (
                 SELECT session_id
                   FROM data_plane_sessions
                  WHERE (
                            (
                                expires_at IS NOT NULL
                            AND expires_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                            )
                         OR (
                                (
                                    state IN ('revoked', 'failed')
                                 OR (state = 'closed' AND kind NOT IN ('terminal', 'process'))
                                )
                            AND updated_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                            )
                        )
                    AND (backend_process_id IS NULL OR backend_process_id = session_id)
                  ORDER BY updated_at ASC, session_id ASC
                  LIMIT $2
             )
             DELETE FROM data_plane_sessions AS d
              USING candidates AS c
              WHERE d.session_id = c.session_id
              RETURNING d.session_id"
    ))
    .bind(grace_secs)
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?
    .len())
}

#[cfg(test)]
mod tests;
