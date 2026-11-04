use clap::Parser;
use pplx_space_common_svc::{core::types::Email, sql};
use sqlx::{PgPool, postgres::PgPoolOptions};

#[derive(Parser)]
pub struct SetUserPasswordArgs {
    /// Control-plane Postgres URI with permission to update users and sessions.
    #[arg(long)]
    pub database_url: String,
    /// Existing console user's email. Does not create users or change roles.
    #[arg(long)]
    pub email: Email,
    /// Bootstrap only: leave an already provisioned password and sessions unchanged.
    #[arg(long)]
    pub if_locked: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum SetUserPasswordError {
    #[error("SPACE_USER_PASSWORD must contain a nonempty password of at most 72 bytes")]
    InvalidPassword,
    #[error("password hashing task failed: {0}")]
    HashTask(#[source] tokio::task::JoinError),
    #[error("password hashing failed: {0}")]
    Hash(#[source] bcrypt::BcryptError),
    #[error("user does not exist; no password was changed")]
    UnknownUser,
    #[error("could not set user password and revoke sessions: {0}")]
    Database(#[from] sqlx::Error),
}

pub async fn run(args: SetUserPasswordArgs) -> Result<(), SetUserPasswordError> {
    let password =
        std::env::var("SPACE_USER_PASSWORD").map_err(|_| SetUserPasswordError::InvalidPassword)?;
    let hash = hash_password(password).await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&args.database_url)
        .await?;
    set_password_hash(&pool, &args.email, &hash, args.if_locked).await
}

async fn hash_password(password: String) -> Result<String, SetUserPasswordError> {
    if password.is_empty() || password.len() > 72 {
        return Err(SetUserPasswordError::InvalidPassword);
    }
    tokio::task::spawn_blocking(move || bcrypt::hash(password, 12))
        .await
        .map_err(SetUserPasswordError::HashTask)?
        .map_err(SetUserPasswordError::Hash)
}

async fn set_password_hash(
    pool: &PgPool,
    email: &Email,
    hash: &str,
    if_locked: bool,
) -> Result<(), SetUserPasswordError> {
    let mut tx = pool.begin().await?;
    let user: Option<(sqlx::types::Uuid, String)> = sqlx::query_as(sql!(
        "SELECT id, password_hash FROM users WHERE email = $1 FOR UPDATE"
    ))
    .bind(email.expose())
    .fetch_optional(&mut *tx)
    .await?;
    let (user_id, current_hash) = user.ok_or(SetUserPasswordError::UnknownUser)?;
    if if_locked && current_hash != "!" {
        tx.commit().await?;
        return Ok(());
    }
    sqlx::query(sql!(
        "UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2"
    ))
    .bind(hash)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!("DELETE FROM sessions WHERE user_id = $1"))
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
