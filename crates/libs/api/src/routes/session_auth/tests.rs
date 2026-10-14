use pplx_space_common_core::types::{OrgId, UserId};

use axum::{Router, routing::get};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::pg_test_db;
use sqlx::PgPool;
use uuid::Uuid;

use super::{ConsoleSubstrate, SESSION_TTL_SECONDS, create_session_for_password, delete_session};
use crate::{
    auth::{hash_password, hash_token},
    models::LoginRequest,
};

use pplx_space_test_env::TestResult;

#[test]
fn auth_route_patterns_are_valid() {
    async fn h() -> &'static str {
        ""
    }

    let protected = Router::new()
        .route("/me", get(h))
        .route("/cluster", get(h))
        .route("/session", axum::routing::delete(h));
    let _router: Router = Router::new().nest(
        "/auth/v1",
        Router::new()
            .route("/sessions", axum::routing::post(h))
            .merge(protected),
    );
}

#[test]
fn substrate_serializes_as_ui_contract() -> TestResult {
    assert_eq!(serde_json::to_value(ConsoleSubstrate::Space)?, "space");
    assert_eq!(serde_json::to_value(ConsoleSubstrate::K8s)?, "k8s");
    Ok(())
}

#[test]
fn console_substrate_is_kubernetes_only_for_the_kubernetes_label() {
    for (managed_by, expected) in [
        ("k8s", ConsoleSubstrate::K8s),
        ("space", ConsoleSubstrate::Space),
        ("kubernetes", ConsoleSubstrate::Space),
        ("", ConsoleSubstrate::Space),
    ] {
        assert_eq!(
            ConsoleSubstrate::from_managed_by(managed_by),
            expected,
            "{managed_by:?}"
        );
    }
}

#[tokio::test]
async fn login_creates_uuid_v7_session_and_delete_revokes_it() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let password = Uuid::now_v7().to_string();
    let (org_id, user_id) = seed_user(pool, "auth@example.com", &password).await?;

    let response = create_session_for_password(
        pool,
        LoginRequest {
            email: "auth@example.com".to_owned(),
            password,
        },
    )
    .await?;

    assert_eq!(response.user.id, user_id);
    assert_eq!(response.org.id, org_id);

    let token_hash = hash_token(&response.token);
    let (session_id, ttl_seconds): (Uuid, i64) = sqlx::query_as(sql!(
        "SELECT id, FLOOR(EXTRACT(EPOCH FROM (expires_at - NOW())))::bigint
         FROM sessions
         WHERE token_hash = $1"
    ))
    .bind(&token_hash)
    .fetch_one(pool)
    .await?;

    assert_eq!((session_id.as_bytes()[6] >> 4) & 0x0f, 7);
    assert!(ttl_seconds > SESSION_TTL_SECONDS - 60);
    assert!(ttl_seconds <= SESSION_TTL_SECONDS);

    delete_session(pool, &response.token).await?;
    let remaining: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sessions WHERE token_hash = $1"))
            .bind(token_hash)
            .fetch_one(pool)
            .await?;
    assert_eq!(remaining, 0);

    Ok(())
}

#[tokio::test]
async fn disabled_password_markers_reject_login_and_existing_sessions() -> TestResult {
    let db = pg_test_db().await?;
    for (label, marker) in [
        ("locked", "!"),
        ("legacy-service-account", "disabled-service-account"),
    ] {
        let email = format!("{label}@example.com");
        let password = Uuid::now_v7().to_string();
        let (_, user_id) = seed_user(&db.pool, &email, &password).await?;
        let login = create_session_for_password(
            &db.pool,
            LoginRequest {
                email: email.clone(),
                password: password.clone(),
            },
        )
        .await?;

        sqlx::query(sql!("UPDATE users SET password_hash = $2 WHERE id = $1"))
            .bind(user_id)
            .bind(marker)
            .execute(&db.pool)
            .await?;

        let result = create_session_for_password(&db.pool, LoginRequest { email, password }).await;
        assert!(matches!(
            result,
            Err(super::AuthRouteError::InvalidCredentials)
        ));
        assert!(
            crate::auth::get_session_user(&db.pool, &login.token)
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn federated_user_rejects_password_login_and_existing_sessions() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::new();
    let user_id = UserId::new();
    let email = "federated-login@example.invalid";
    let password = Uuid::now_v7().to_string();
    sqlx::query(sql!(
        "INSERT INTO organizations (
             id, name, slug, owner_source, upstream_issuer, upstream_org_id
         ) VALUES ($1, 'Federated Auth Org', $2, 'federated_identity', $3, $4)"
    ))
    .bind(org_id)
    .bind(format!("federated-auth-{org_id}"))
    .bind("https://identity.example.invalid")
    .bind(format!("org-{org_id}"))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         ) VALUES ($1, $2, $3, 'Federated Auth User', $4, 'member',
                   'federated_identity', $5, $6)"
    ))
    .bind(user_id)
    .bind(email)
    .bind(hash_password(&password)?)
    .bind(org_id)
    .bind("https://identity.example.invalid")
    .bind(format!("subject-{user_id}"))
    .execute(&db.pool)
    .await?;

    let result = create_session_for_password(
        &db.pool,
        LoginRequest {
            email: email.to_owned(),
            password,
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(super::AuthRouteError::InvalidCredentials)
    ));

    let (raw_token, token_hash) = crate::auth::generate_session_token();
    sqlx::query(sql!(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at)
         VALUES ($1, $2, $3, NOW() + INTERVAL '1 hour')"
    ))
    .bind(Uuid::now_v7())
    .bind(user_id)
    .bind(token_hash)
    .execute(&db.pool)
    .await?;
    assert!(
        crate::auth::get_session_user(&db.pool, &raw_token)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn login_rejects_wrong_password() -> TestResult {
    let db = pg_test_db().await?;
    seed_user(&db.pool, "wrong@example.com", &Uuid::now_v7().to_string()).await?;

    let error = create_session_for_password(
        &db.pool,
        LoginRequest {
            email: "wrong@example.com".to_owned(),
            password: Uuid::now_v7().to_string(),
        },
    )
    .await
    .expect_err("wrong password must be rejected");

    assert!(matches!(error, super::AuthRouteError::InvalidCredentials));
    Ok(())
}

#[tokio::test]
async fn concurrent_password_reset_rejects_old_password_login() -> TestResult {
    let db = pg_test_db().await?;
    let email = "reset@example.com";
    let old_password = Uuid::now_v7().to_string();
    let new_password = Uuid::now_v7().to_string();
    let (_, user_id) = seed_user(&db.pool, email, &old_password).await?;
    let new_hash = hash_password(&new_password)?;
    let mut reset = db.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM users WHERE id = $1 FOR UPDATE"))
        .bind(user_id)
        .fetch_one(&mut *reset)
        .await?;
    sqlx::query(sql!("UPDATE users SET password_hash = $1 WHERE id = $2"))
        .bind(new_hash)
        .bind(user_id)
        .execute(&mut *reset)
        .await?;
    sqlx::query(sql!("DELETE FROM sessions WHERE user_id = $1"))
        .bind(user_id)
        .execute(&mut *reset)
        .await?;

    let login = create_session_for_password(
        &db.pool,
        LoginRequest {
            email: email.to_owned(),
            password: old_password,
        },
    );
    let finish_reset = async {
        // Wait for the real login to contend with the uncommitted reset.
        // An unlocked password read instead waits at the session's foreign key.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let blocked: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (
                        SELECT 1 FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND pg_backend_pid() = ANY(pg_blocking_pids(pid))
                    )"
                ))
                .fetch_one(&mut *reset)
                .await?;
                if blocked {
                    return Ok::<(), sqlx::Error>(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await??;
        reset.commit().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    let (login_result, reset_result) = tokio::join!(login, finish_reset);

    reset_result?;
    assert!(matches!(
        login_result,
        Err(super::AuthRouteError::InvalidCredentials)
    ));
    let remaining: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sessions WHERE user_id = $1"))
            .bind(user_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(remaining, 0);
    create_session_for_password(
        &db.pool,
        LoginRequest {
            email: email.to_owned(),
            password: new_password,
        },
    )
    .await?;
    Ok(())
}

async fn seed_user(pool: &PgPool, email: &str, password: &str) -> TestResult<(OrgId, UserId)> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Auth Org', $2)"
    ))
    .bind(org_id)
    .bind(format!("auth-{}", org_id))
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, $3, 'Auth User', $4, 'member')"
    ))
    .bind(user_id)
    .bind(email)
    .bind(hash_password(password)?)
    .bind(org_id)
    .execute(pool)
    .await?;

    Ok((org_id, user_id))
}

/// Console login must answer a lost lock race with a paced retryable 503, not
/// the 500 the `#[from] sqlx::Error` derive gave every SQLSTATE.
///
/// The `Retry-After` is the second half and not decoration: an un-hinted
/// retryable 503 makes the console retry at RTT against the row that is
/// already contended, replacing one sub-second wait with a burst.
#[tokio::test]
async fn a_lost_lock_race_on_login_is_a_paced_retryable_503() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let error = super::AuthRouteError::from(
            pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await,
        );
        let response = axum::response::IntoResponse::into_response(error);
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "{sqlstate} must be retryable"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .expect("a retryable 503 must pace the retry"),
            "1"
        );
    }

    // A real failure keeps the 500 and gains no retry hint.
    let error = super::AuthRouteError::from(
        pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await,
    );
    let response = axum::response::IntoResponse::into_response(error);
    assert_eq!(
        response.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(
        response
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none()
    );
    Ok(())
}
