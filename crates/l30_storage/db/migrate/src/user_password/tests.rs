use super::*;
use pplx_space_db_test_fixtures::{SEED_USER_ID, pg_test_db};
use pplx_space_test_env::TestResult;

#[tokio::test]
async fn setting_password_unlocks_user_and_revokes_only_their_sessions() -> TestResult {
    let db = pg_test_db().await?;
    let email = Email::parse("root@local")?;
    let original_role: String = sqlx::query_scalar(sql!("SELECT role FROM users WHERE id = $1"))
        .bind(SEED_USER_ID)
        .fetch_one(&db.pool)
        .await?;
    sqlx::query(sql!("UPDATE users SET password_hash = '!' WHERE id = $1"))
        .bind(SEED_USER_ID)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at)
         SELECT space_uuid_v7(), id, email, NOW() + INTERVAL '1 hour' FROM users"
    ))
    .execute(&db.pool)
    .await?;
    let other_sessions: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sessions WHERE user_id <> $1"))
            .bind(SEED_USER_ID)
            .fetch_one(&db.pool)
            .await?;
    let password = "operator-supplied-test-password";
    let hash = hash_password(password.to_owned()).await?;

    set_password_hash(&db.pool, &email, &hash, true).await?;

    let (stored_hash, role): (String, String) =
        sqlx::query_as(sql!("SELECT password_hash, role FROM users WHERE id = $1"))
            .bind(SEED_USER_ID)
            .fetch_one(&db.pool)
            .await?;
    assert!(bcrypt::verify(password, &stored_hash)?);
    assert_eq!(role, original_role);
    let remaining: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sessions"))
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(remaining, other_sessions);
    let own_sessions: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sessions WHERE user_id = $1"))
            .bind(SEED_USER_ID)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(own_sessions, 0);
    sqlx::query(sql!(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at)
         VALUES (space_uuid_v7(), $1, 'new-session', NOW() + INTERVAL '1 hour')"
    ))
    .bind(SEED_USER_ID)
    .execute(&db.pool)
    .await?;
    set_password_hash(&db.pool, &email, "!", true).await?;
    let unchanged: String =
        sqlx::query_scalar(sql!("SELECT password_hash FROM users WHERE id = $1"))
            .bind(SEED_USER_ID)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(unchanged, stored_hash);
    let own_sessions: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sessions WHERE user_id = $1"))
            .bind(SEED_USER_ID)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(own_sessions, 1);

    assert!(matches!(
        set_password_hash(
            &db.pool,
            &Email::parse("absent@example.test")?,
            &hash,
            false
        )
        .await,
        Err(SetUserPasswordError::UnknownUser)
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_passwords_are_rejected_before_hashing() {
    for password in [String::new(), "x".repeat(73)] {
        assert!(matches!(
            hash_password(password).await,
            Err(SetUserPasswordError::InvalidPassword)
        ));
    }
}
