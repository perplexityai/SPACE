use super::{
    DatabaseConnectOptionsError, DbOptions, SidelineOptions, connect_postgres_lazy_as,
    connect_space_user_sideline_pool_lazy, database_connect_options, jittered_lifetime,
    pool_options_with, pool_utilization, postgres_pool_options,
};
use pplx_space_db_identity::DatabaseIdentity;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use std::num::NonZeroU32;
use std::time::Duration;

const ADMIN_DATABASE_URL: &str =
    "postgres://space_admin:admin@db.example:5432/space?sslmode=require";

#[tokio::test]
async fn lazy_pool_does_not_require_database_at_startup() {
    for identity in [DatabaseIdentity::SpaceUser, DatabaseIdentity::SpaceCreds] {
        let pool = connect_postgres_lazy_as(
            "postgres://space_admin:admin@127.0.0.1:1/space?sslmode=disable",
            "test-service",
            identity,
            "pw",
            DbOptions::default(),
        )
        .unwrap();

        assert_eq!(pool.size(), 0);
    }
}

#[test]
fn lazy_pool_requires_a_password() {
    let error = connect_postgres_lazy_as(
        ADMIN_DATABASE_URL,
        "test-service",
        DatabaseIdentity::SpaceUser,
        "",
        DbOptions::default(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        DatabaseConnectOptionsError::MissingPassword {
            identity: DatabaseIdentity::SpaceUser,
            ..
        }
    ));
}

#[test]
fn identity_options_preserve_endpoint_and_replace_only_credentials() {
    for identity in [DatabaseIdentity::SpaceUser, DatabaseIdentity::SpaceCreds] {
        let options =
            database_connect_options(ADMIN_DATABASE_URL, "test-service", identity, "pw").unwrap();
        let debug = format!("{options:?}");

        assert!(debug.contains("db.example"), "{debug}");
        assert!(debug.contains("5432"), "{debug}");
        assert!(debug.contains("space"), "{debug}");
        assert!(debug.contains(identity.as_str()), "{debug}");
        assert!(!debug.contains("space_admin"), "{debug}");
        assert!(!debug.contains("admin"), "{debug}");
    }
}

#[test]
fn managed_identities_require_their_projected_password() {
    for identity in [DatabaseIdentity::SpaceUser, DatabaseIdentity::SpaceCreds] {
        let error =
            database_connect_options(ADMIN_DATABASE_URL, "test-service", identity, "").unwrap_err();

        assert!(matches!(
            error,
            DatabaseConnectOptionsError::MissingPassword {
                identity: missing_identity,
                ..
            } if missing_identity == identity
        ));
    }
}

#[test]
fn utilization_is_in_use_over_cap() {
    assert_eq!(pool_utilization(0, 25), Some(0.0));
    assert_eq!(pool_utilization(5, 25), Some(0.2));
    assert_eq!(pool_utilization(25, 25), Some(1.0));
}

#[test]
fn zero_cap_emits_nothing_instead_of_nan() {
    assert_eq!(pool_utilization(0, 0), None);
    assert_eq!(pool_utilization(3, 0), None);
}

#[test]
fn lifetime_jitter_spreads_retirement_around_target() {
    let lifetime = Duration::from_secs(30 * 60);
    let jitter_fraction = 1.0 / 6.0;

    assert_eq!(
        jittered_lifetime(lifetime, jitter_fraction, 0.0),
        Duration::from_secs(25 * 60)
    );
    assert_eq!(jittered_lifetime(lifetime, jitter_fraction, 0.5), lifetime);
    assert_eq!(
        jittered_lifetime(lifetime, jitter_fraction, 1.0),
        Duration::from_secs(35 * 60)
    );
}

#[test]
fn pool_retires_connections_after_release_and_keeps_a_floor() {
    let options = postgres_pool_options(DatabaseIdentity::SpaceUser);

    assert_eq!(options.get_max_lifetime(), None);
    assert!(options.get_min_connections() > 0);
    assert!(options.get_min_connections() <= options.get_max_connections());
}

/// A sideline pool's cap must win over the service-wide env knobs.
///
/// This is the whole property the constructor exists for: the caller asks for a
/// ceiling precisely so a blocked sideline cannot hold a share of the database
/// proportional to the *general* pool's budget. `postgres_pool_options()` reads
/// both knobs from the environment first, so the cap only holds because the
/// later setters win — reorder them, or fold the env read back in, and every
/// sideline pool in every service silently inherits the general cap with
/// nothing failing.
///
/// `min_connections` is asserted too, in the hostile direction: an env floor
/// above the cap must clamp down to it, not exceed it (sqlx rejects
/// `min > max`).
#[tokio::test]
async fn a_capped_pool_ignores_the_service_wide_connection_env() {
    // Set pool overrides before child startup so sibling tests keep their configuration.
    let pool = {
        let password = std::process::id().to_string();
        let mut vars: Vec<(&str, Option<&std::ffi::OsStr>)> = [
            "POSTGRES_POOL_MAX_CONNECTIONS",
            "POSTGRES_POOL_MIN_CONNECTIONS",
        ]
        .iter()
        .map(|key| (*key, None))
        .collect();
        vars.extend([
            (
                "POSTGRES_POOL_MAX_CONNECTIONS",
                Some(std::ffi::OsStr::new("50")),
            ),
            (
                "POSTGRES_POOL_MIN_CONNECTIONS",
                Some(std::ffi::OsStr::new("10")),
            ),
            (
                crate::SPACE_DB_USER_PASSWORD_ENV,
                Some(std::ffi::OsStr::new(&password)),
            ),
        ]);
        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        connect_space_user_sideline_pool_lazy(
            "postgres://space_admin:admin@127.0.0.1:1/space?sslmode=disable",
            "capped-pool-test",
            SidelineOptions {
                max_connections: NonZeroU32::new(3).expect("3 is non-zero"),
                statement_timeout: None,
            },
        )
        .expect("capped pool options must build")
    };

    assert_eq!(
        pool.options().get_max_connections(),
        3,
        "the explicit cap must override POSTGRES_POOL_MAX_CONNECTIONS"
    );
    assert_eq!(
        pool.options().get_min_connections(),
        3,
        "an env floor above the cap must clamp to the cap, never exceed it"
    );
}

/// `POSTGRES_CREDS_POOL_MAX_CONNECTIONS` caps only the `space_creds` pool;
/// the `space_user` pool beside it keeps `POSTGRES_POOL_MAX_CONNECTIONS`.
#[tokio::test]
async fn the_creds_pool_cap_applies_only_to_space_creds() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "POSTGRES_POOL_MAX_CONNECTIONS",
            Some(std::ffi::OsStr::new("50")),
        ),
        (
            "POSTGRES_CREDS_POOL_MAX_CONNECTIONS",
            Some(std::ffi::OsStr::new("4")),
        ),
        ("POSTGRES_POOL_MIN_CONNECTIONS", None),
    ]) {
        return;
    }
    let url = "postgres://space_admin:admin@127.0.0.1:1/space?sslmode=disable";
    let password = std::process::id().to_string();
    let max = |identity| {
        connect_postgres_lazy_as(
            url,
            "creds-cap-test",
            identity,
            &password,
            DbOptions::default(),
        )
        .unwrap()
        .options()
        .get_max_connections()
    };
    assert_eq!(max(DatabaseIdentity::SpaceUser), 50);
    assert_eq!(max(DatabaseIdentity::SpaceCreds), 4);
}

/// Pins the always-on query spans: SQLx keeps the statement level only in
/// `PgConnectOptions`' private log settings, which `Debug` exposes.
#[tokio::test]
async fn service_pools_emit_query_events_at_info() {
    let url = "postgres://space_admin:admin@127.0.0.1:1/space?sslmode=disable";
    let identity = DatabaseIdentity::SpaceUser;
    // The pool is lazy and never connects, so any non-empty password works.
    // A runtime value keeps CodeQL's hard-coded-credential check quiet.
    let password = std::process::id().to_string();
    let pool = connect_postgres_lazy_as(
        url,
        "query-span-test",
        identity,
        &password,
        DbOptions::default(),
    )
    .unwrap();

    let options = format!("{:?}", pool.connect_options());
    assert!(
        options.contains("statements_level: Info"),
        "service pools must emit sqlx::query events at INFO: {options}"
    );
}

/// `DbOptions::statement_timeout` holds on every connection the pool opens,
/// not only the first, and a pool without it keeps the role's setting.
///
/// Built from the crate's pool options over the test database's own login:
/// the managed identities are not roles in the test cluster. The role's value
/// is a role-in-database setting, so it dies with the test database.
#[tokio::test]
async fn statement_timeout_applies_to_every_connection_of_its_pool() -> TestResult {
    let Some(test_db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let database_name = sqlx::query_scalar::<_, String>(sql!("SELECT current_database()"))
        .fetch_one(&test_db.pool)
        .await?;
    let role_setting = format!(
        "ALTER ROLE CURRENT_USER IN DATABASE \"{database_name}\" SET statement_timeout = '7s'"
    );
    sqlx::query(sqlx::AssertSqlSafe(role_setting.as_str()))
        .execute(&test_db.pool)
        .await?;

    let two = NonZeroU32::new(2).expect("2 is non-zero");
    for (statement_timeout, expected) in
        [(Some(Duration::from_millis(1500)), "1500ms"), (None, "7s")]
    {
        let options = DbOptions {
            max_connections: Some(two),
            statement_timeout,
        };
        let pool = pool_options_with(DatabaseIdentity::SpaceUser, options)
            .connect_lazy(&test_db.database_url)?;
        // Held together, so they are two connections rather than one reused.
        let mut first = pool.acquire().await?;
        let mut second = pool.acquire().await?;
        let mut pids = Vec::new();
        for conn in [&mut first, &mut second] {
            let (pid, setting) = sqlx::query_as::<_, (i32, String)>(sql!(
                "SELECT pg_backend_pid(), current_setting('statement_timeout')"
            ))
            .fetch_one(&mut **conn)
            .await?;
            assert_eq!(setting, expected, "{statement_timeout:?}");
            pids.push(pid);
        }
        assert_ne!(
            pids[0], pids[1],
            "the two checks must run on separate connections"
        );
        drop((first, second));
        pool.close().await;
    }
    Ok(())
}
