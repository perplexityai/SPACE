//! Console password `sessions`: creating one at sign-in and revoking them.

use pplx_space_accounts::UserId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

/// A session to create for a signed-in user.
pub struct NewSession<'a> {
    pub id: Uuid,
    pub user_id: UserId,
    pub token_hash: &'a str,
    /// Lifetime from the database's current time.
    pub ttl_seconds: i64,
}

pub async fn create_session(
    executor: impl PgExecutor<'_>,
    session: NewSession<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at)
         VALUES ($1, $2, $3, NOW() + ($4::bigint * INTERVAL '1 second'))"
    ))
    .bind(session.id)
    .bind(session.user_id)
    .bind(session.token_hash)
    .bind(session.ttl_seconds)
    .execute(executor)
    .await?;
    Ok(())
}

/// Revoke the session with this token hash, if there is one.
pub async fn delete_session_by_token_hash(
    executor: impl PgExecutor<'_>,
    token_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!("DELETE FROM sessions WHERE token_hash = $1"))
        .bind(token_hash)
        .execute(executor)
        .await?;
    Ok(())
}

/// Revoke every session of the user.
pub async fn delete_user_sessions(
    executor: impl PgExecutor<'_>,
    user_id: UserId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!("DELETE FROM sessions WHERE user_id = $1"))
        .bind(user_id)
        .execute(executor)
        .await?;
    Ok(())
}
