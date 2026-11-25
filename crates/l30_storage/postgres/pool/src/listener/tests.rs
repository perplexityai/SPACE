use pplx_space_db_test_fixtures::try_raw_pg_test_db;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::postgres::PgListener;

use super::set_listener_keepalives;

/// The listener's session reports the keepalive and `tcp_user_timeout`
/// values after setup.
///
/// Postgres reports `0` for these settings on a Unix-socket session, whatever
/// was set. The shared fixture connects over TCP, which the first check pins,
/// so the values read back are the ones in force.
#[tokio::test(flavor = "current_thread")]
async fn listener_session_reports_the_keepalives() -> TestResult {
    let Some(db) = try_raw_pg_test_db().await? else {
        return Ok(());
    };
    let mut listener = PgListener::connect_with(&db.pool).await?;

    let over_tcp: bool = sqlx::query_scalar(sql!("SELECT inet_server_addr() IS NOT NULL"))
        .fetch_one(&mut listener)
        .await?;
    assert!(
        over_tcp,
        "the fixture's connection must be TCP for these settings to read back"
    );

    set_listener_keepalives(&mut listener).await?;

    for (setting, expected) in [
        (sql!("SHOW tcp_keepalives_idle"), "30"),
        (sql!("SHOW tcp_keepalives_interval"), "10"),
        (sql!("SHOW tcp_keepalives_count"), "3"),
        (sql!("SHOW tcp_user_timeout"), "30000"),
    ] {
        let value: String = sqlx::query_scalar(setting).fetch_one(&mut listener).await?;
        assert_eq!(value, expected, "{setting}");
    }
    Ok(())
}
