//! Native bearer resolution in one statement answers exactly what the session
//! lookup followed by the Native key lookup answered.

use std::str::FromStr as _;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, UserId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_observability::test_capture::CapturedEventsLayer;
use pplx_space_test_env::TestResult;
use sqlx::ConnectOptions as _;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tracing_subscriber::layer::SubscriberExt as _;
use uuid::Uuid;

use crate::api_surface::ApiSurface;
use crate::auth::{
    NativeCredential, get_api_key_org, get_session_user, hash_token, resolve_native_credential,
};

const SEED_ORG_ID: &str = "00000000-0000-0000-0000-000000000002";

/// What a resolution decided, in terms both implementations can be compared
/// on.
#[derive(Debug, PartialEq, Eq)]
enum Resolved {
    Session {
        user: UserId,
        org: OrgId,
    },
    ApiKey {
        key: Uuid,
        org: OrgId,
        actor: Option<UserId>,
    },
}

/// The previous resolution: a session lookup, then on a miss the Native key
/// lookup.
async fn sequential(pool: &PgPool, token: &str) -> TestResult<Option<Resolved>> {
    if let Some((user, org)) = get_session_user(pool, token).await? {
        return Ok(Some(Resolved::Session {
            user: user.id,
            org: org.id,
        }));
    }
    Ok(get_api_key_org(pool, token, ApiSurface::Native)
        .await?
        .map(|context| Resolved::ApiKey {
            key: context.api_key.id,
            org: context.org.id,
            actor: context.actor.map(|user| user.id),
        }))
}

async fn combined(pool: &PgPool, token: &str) -> TestResult<Option<Resolved>> {
    Ok(resolve_native_credential(pool, token)
        .await?
        .map(|credential| match credential {
            NativeCredential::Session { user, org } => {
                assert_eq!(user.org_id, org.id, "the session's org is its user's");
                Resolved::Session {
                    user: user.id,
                    org: org.id,
                }
            }
            NativeCredential::ApiKey(context) => Resolved::ApiKey {
                key: context.api_key.id,
                org: context.org.id,
                actor: context.actor.map(|user| user.id),
            },
        }))
}

async fn seed_user(pool: &PgPool, password_hash: &str) -> TestResult<UserId> {
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, $3, 'Session User', $4, 'member')"
    ))
    .bind(user_id)
    .bind(format!("session-{user_id}@test"))
    .bind(password_hash)
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .execute(pool)
    .await?;
    Ok(user_id)
}

async fn seed_session(pool: &PgPool, user: UserId, token: &str, live: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sessions (user_id, token_hash, expires_at)
         VALUES ($1, $2, CASE WHEN $3 THEN NOW() + INTERVAL '1 hour'
                              ELSE NOW() - INTERVAL '1 second' END)"
    ))
    .bind(user)
    .bind(hash_token(token))
    .bind(live)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_key(
    pool: &PgPool,
    token: &str,
    surfaces: &[&str],
    created_by: Option<UserId>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces, created_by)
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(Uuid::now_v7())
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .bind(token)
    .bind(hash_token(token))
    .bind(surfaces)
    .bind(created_by)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn one_statement_resolves_what_the_two_lookups_resolved() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let member = seed_user(pool, "not-disabled").await?;
    let login_disabled = seed_user(pool, "!").await?;

    seed_session(pool, member, "live_session", true).await?;
    seed_session(pool, member, "expired_session", false).await?;
    seed_key(pool, "native_key", &["native"], None).await?;
    seed_key(pool, "native_key_by_member", &["native"], Some(member)).await?;
    seed_key(pool, "ecw_key", &["ecw"], None).await?;
    // One token that is both, so precedence is observable: a session that
    // resolves wins; an expired one, or one whose user cannot log in, does not.
    seed_session(pool, member, "session_and_key", true).await?;
    seed_key(pool, "session_and_key", &["native"], None).await?;
    seed_session(pool, member, "expired_session_and_key", false).await?;
    seed_key(pool, "expired_session_and_key", &["native"], None).await?;
    seed_session(pool, login_disabled, "disabled_session_and_key", true).await?;
    seed_key(pool, "disabled_session_and_key", &["native"], None).await?;
    seed_session(pool, login_disabled, "disabled_session", true).await?;

    for (token, expected) in [
        ("live_session", "session"),
        ("expired_session", "none"),
        ("native_key", "api_key"),
        ("native_key_by_member", "api_key"),
        ("ecw_key", "none"),
        ("session_and_key", "session"),
        ("expired_session_and_key", "api_key"),
        ("disabled_session_and_key", "api_key"),
        ("disabled_session", "none"),
        ("unknown", "none"),
    ] {
        let before = sequential(pool, token).await?;
        let after = combined(pool, token).await?;
        assert_eq!(after, before, "{token}");
        let kind = match &after {
            Some(Resolved::Session { .. }) => "session",
            Some(Resolved::ApiKey { .. }) => "api_key",
            None => "none",
        };
        assert_eq!(kind, expected, "{token}");
    }
    assert!(
        matches!(
            combined(pool, "native_key_by_member").await?,
            Some(Resolved::ApiKey { actor: Some(actor), .. }) if actor == member
        ),
        "a key's actor is still its creator"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_native_bearer_costs_one_statement_whatever_it_is() -> TestResult {
    let db = pg_test_db().await?;
    let member = seed_user(&db.pool, "not-disabled").await?;
    seed_session(&db.pool, member, "live_session", true).await?;
    seed_key(&db.pool, "native_key", &["native"], None).await?;

    let options = PgConnectOptions::from_str(&db.database_url)?
        .log_statements(log::LevelFilter::Info)
        .log_slow_statements(log::LevelFilter::Off, Duration::from_secs(60));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    // Connect before counting, so only the lookups' own statements count.
    sqlx::query(sql!("SELECT 1")).execute(&pool).await?;

    let statements = async |lookup: &str, token: &str| -> TestResult<usize> {
        let layer = CapturedEventsLayer::new().with_target("sqlx::query");
        let events = layer.events();
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        // Another test's thread may have cached these callsites as disabled.
        tracing::callsite::rebuild_interest_cache();
        let _ = match lookup {
            "sequential" => sequential(&pool, token).await?,
            _ => combined(&pool, token).await?,
        };
        Ok(events.events().len())
    };

    for token in ["live_session", "native_key", "unknown"] {
        // A session hit read its user and then its org; anything else missed
        // the session lookup and then tried the key.
        assert_eq!(statements("sequential", token).await?, 2, "before: {token}");
        assert_eq!(statements("combined", token).await?, 1, "after: {token}");
    }
    pool.close().await;
    Ok(())
}
