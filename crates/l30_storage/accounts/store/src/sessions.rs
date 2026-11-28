//! Console password `sessions`: creating one at sign-in and revoking them.

use pplx_space_accounts::UserId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
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
    executor: impl Executor<'_>,
    session: NewSession<'_>,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at)
         VALUES ($1, $2, $3, NOW() + ($4::bigint * INTERVAL '1 second'))"
    ))
    .bind(session.id)
    .bind(session.user_id)
    .bind(session.token_hash)
    .bind(session.ttl_seconds)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Revoke the session with this token hash, if there is one.
pub async fn delete_session_by_token_hash(
    executor: impl Executor<'_>,
    token_hash: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!("DELETE FROM sessions WHERE token_hash = $1"))
        .bind(token_hash)
        .execute(executor.into_sqlx())
        .await?;
    Ok(())
}

/// Revoke every session of the user.
pub async fn delete_user_sessions(
    executor: impl Executor<'_>,
    user_id: UserId,
) -> Result<(), DbError> {
    sqlx::query(sql!("DELETE FROM sessions WHERE user_id = $1"))
        .bind(user_id)
        .execute(executor.into_sqlx())
        .await?;
    Ok(())
}
