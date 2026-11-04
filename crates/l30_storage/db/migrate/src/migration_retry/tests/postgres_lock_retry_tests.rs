//! Live-Postgres coverage for the migration lock-timeout retry.
//!
//! Every test recreates the production shape: a conflicting `ACCESS
//! EXCLUSIVE` holder on the target table, and a migration whose DDL is
//! bounded by a short `lock_timeout`.

use std::borrow::Cow;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use pplx_space_common_svc::sql;
use pplx_space_db_test_fixtures::try_raw_pg_test_db_with_max_connections;
use sqlx::migrate::{Migration, MigrationType};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgConnection, PgPool, Postgres, SqlSafeStr as _, Transaction};

use super::super::*;
use super::PROBE_VERSION;
use crate::pg_test_support::TestResult;

/// Long enough that the first attempt cannot win the lock even on a badly
/// loaded machine, and short enough to keep the suite quick.
const BLOCKER_HOLD: Duration = Duration::from_secs(2);

/// Per-attempt bound every probe migration imposes, standing in for the `1s`
/// or `5s` a real migration file sets.
const PROBE_LOCK_TIMEOUT: &str = "250ms";

/// Millisecond-scale schedule so a test spans the same number of attempts a
/// real deploy would, in a fraction of the wall clock. The window is left
/// wide so the attempt budget is always what binds here.
fn fast_policy(attempts: u32) -> LockRetryPolicy {
    LockRetryPolicy {
        attempts: NonZeroU32::new(attempts).expect("test attempt budget is non-zero"),
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_millis(200),
        max_total_wait: Duration::from_secs(60),
    }
}

fn probe_migrator(sql: String, no_tx: bool) -> Migrator {
    Migrator::with_migrations(vec![Migration::new(
        PROBE_VERSION,
        Cow::Borrowed("probe"),
        MigrationType::Simple,
        sqlx::AssertSqlSafe(sql).into_sql_str(),
        no_tx,
    )])
}

/// A pass that applies one slow migration successfully before the bounded
/// `ALTER` loses its race, so the two clocks — since run start, and since the
/// first failure — read very differently at the moment `may_retry` is asked.
fn slow_then_bounded_migrator() -> Migrator {
    Migrator::with_migrations(vec![
        Migration::new(
            PROBE_VERSION - 1,
            Cow::Borrowed("slow but fine"),
            MigrationType::Simple,
            sqlx::AssertSqlSafe("SELECT pg_sleep(1.2)".to_owned()).into_sql_str(),
            false,
        ),
        Migration::new(
            PROBE_VERSION,
            Cow::Borrowed("probe"),
            MigrationType::Simple,
            sqlx::AssertSqlSafe(format!(
                "SET LOCAL lock_timeout = '{PROBE_LOCK_TIMEOUT}';
                 ALTER TABLE lock_probe ADD COLUMN observed_at TIMESTAMPTZ;"
            ))
            .into_sql_str(),
            false,
        ),
    ])
}

/// The production shape: sqlx sends the file as one simple-query batch and
/// wraps it in a transaction, so the `SET LOCAL` binds to that transaction
/// and the whole file rolls back together.
fn bounded_alter_migrator() -> Migrator {
    probe_migrator(
        format!(
            "SET LOCAL lock_timeout = '{PROBE_LOCK_TIMEOUT}';
             ALTER TABLE lock_probe ADD COLUMN observed_at TIMESTAMPTZ;"
        ),
        false,
    )
}

async fn create_probe_table(pool: &PgPool) -> TestResult {
    sqlx::query(sql!("CREATE TABLE lock_probe (id BIGINT PRIMARY KEY)"))
        .execute(pool)
        .await?;
    Ok(())
}

async fn create_second_probe_table(pool: &PgPool) -> TestResult {
    sqlx::query(sql!(
        "CREATE TABLE lock_probe_second (id BIGINT PRIMARY KEY)"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Takes `ACCESS EXCLUSIVE` on the probe table and holds it until the
/// returned transaction is rolled back or dropped.
async fn lock_probe_table(holder: &mut PgConnection) -> TestResult<Transaction<'_, Postgres>> {
    let mut blocker = holder.begin().await?;
    sqlx::query(sql!("LOCK TABLE lock_probe IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await?;
    Ok(blocker)
}

/// Backend pid of an open connection, so a sampler can exclude it.
async fn backend_pid(conn: &mut PgConnection) -> TestResult<i32> {
    Ok(sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(conn)
        .await?)
}

/// Counts backends in this database idling inside an open transaction, other
/// than `ignore` and the sampler itself.
///
/// Matches the `idle in transaction` *prefix* rather than the full
/// `idle in transaction (aborted)`, and leans on excluding the blocker's pid to
/// tell the two apart: with the blocker excluded, the only backend that can be
/// idling in a transaction is a migration whose `ROLLBACK` was never sent.
///
/// The control below only proves the prefix still matches *something*, since
/// what it counts is the blocker's plain `idle in transaction`. A rename of the
/// `(aborted)` variant alone to something outside the prefix would zero the
/// count with the control still passing, and there is no second assertion
/// behind it: the attempt count and the absent column hold whether or not the
/// flush happened, so the sample count is the only thing that sees it.
///
/// Not keyed on `xact_start`: Postgres clears it when a transaction aborts, so
/// `xact_start IS NOT NULL` misses exactly the state this needs to see.
async fn count_idle_in_transaction(conn: &mut PgConnection, ignore: i32) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        r"
        SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND pid <> $1
          AND state LIKE 'idle in transaction%'
        "
    ))
    .bind(ignore)
    .fetch_one(conn)
    .await?)
}

/// Samples every 50ms until `deadline`, counting how many samples catch a
/// backend other than `blocker` holding a transaction open.
///
/// Returns the hit count and the sample count, so a caller cannot pass a
/// deadline so short that zero samples silently read as "clean".
async fn count_idle_in_transaction_samples(
    url: &str,
    blocker: i32,
    deadline: Duration,
) -> TestResult<(u32, u32)> {
    let mut conn = PgConnection::connect(url).await?;

    // Positive control on the mechanism itself: counting *without* excluding the
    // blocker must find its open transaction, or the `state` predicate no longer
    // matches what Postgres reports and the hit count below means nothing.
    let control = count_idle_in_transaction(&mut conn, 0).await?;
    assert!(
        control > 0,
        "the blocker is idling in an open transaction but the `state` predicate \
         finds nothing; it no longer matches what Postgres reports, so the hit \
         count below would prove nothing"
    );

    let started = Instant::now();
    let mut caught = 0;
    let mut taken = 0;
    while started.elapsed() < deadline {
        if count_idle_in_transaction(&mut conn, blocker).await? > 0 {
            caught += 1;
        }
        taken += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    conn.close().await?;
    Ok((caught, taken))
}

async fn probe_column_exists(pool: &PgPool) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        r"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'lock_probe' AND column_name = 'observed_at'
        )
        "
    ))
    .fetch_one(pool)
    .await?)
}

async fn applied_probe_versions(pool: &PgPool) -> TestResult<Vec<i64>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT version FROM _sqlx_migrations ORDER BY version"
    ))
    .fetch_all(pool)
    .await?)
}

/// Asserts the failure was the probe migration losing its lock race. The
/// attempt count is the assertion each test makes for itself.
fn assert_lock_timeout_on_probe(failure: &MigrationRunFailed) {
    let MigrateError::ExecuteMigration(source, version) = &failure.source else {
        panic!("expected a per-migration failure, got {:?}", failure.source);
    };
    assert_eq!(*version, PROBE_VERSION);
    // The literal, not `is_lock_not_available` — an always-true mutation of the
    // production predicate must not slip past every test that calls this.
    assert_eq!(
        sqlstate(source).as_deref(),
        Some("55P03"),
        "expected lock_not_available, got {source:?}"
    );
}

#[tokio::test]
async fn lock_timeout_retries_until_the_blocker_releases() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    create_probe_table(&fixture.pool).await?;

    let mut holder = PgConnection::connect(&fixture.database_url).await?;
    let blocker = lock_probe_table(&mut holder).await?;

    let migrator = bounded_alter_migrator();
    let (run, release) = tokio::join!(run_up(&fixture.pool, &migrator, fast_policy(12)), async {
        tokio::time::sleep(BLOCKER_HOLD).await;
        blocker.rollback().await
    });
    release?;
    let attempt = run?;

    assert!(
        attempt > 1,
        "the blocker outlived one lock_timeout, so the run must have retried; applied on attempt {attempt}"
    );
    assert!(probe_column_exists(&fixture.pool).await?);
    assert_eq!(
        applied_probe_versions(&fixture.pool).await?,
        [PROBE_VERSION]
    );
    Ok(())
}

#[tokio::test]
async fn exhausted_retries_report_the_attempt_count() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    create_probe_table(&fixture.pool).await?;

    let mut holder = PgConnection::connect(&fixture.database_url).await?;
    let blocker = lock_probe_table(&mut holder).await?;

    let failure = run_up(&fixture.pool, &bounded_alter_migrator(), fast_policy(3))
        .await
        .expect_err("the blocker never releases, so every attempt must lose");

    assert_eq!(failure.attempt, 3);
    assert_eq!(failure.budget, 3);
    assert_eq!(
        failure.to_string(),
        "migration run failed on attempt 3 of 3"
    );
    assert_lock_timeout_on_probe(&failure);

    blocker.rollback().await?;
    assert!(!probe_column_exists(&fixture.pool).await?);
    assert!(applied_probe_versions(&fixture.pool).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn the_window_stops_a_run_before_its_attempt_budget() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    create_probe_table(&fixture.pool).await?;

    let mut holder = PgConnection::connect(&fixture.database_url).await?;
    let blocker = lock_probe_table(&mut holder).await?;

    // The window reads zero at the first failure, so it allows one retry; the
    // second attempt puts the retry clock at ~350ms, past 320ms on its own, so
    // the run stops on attempt 2 even though the budget allows 12. This is the
    // bound that keeps a long inherited `lock_timeout` from multiplying into a
    // Job-deadline kill.
    //
    // 320ms is chosen for margin rather than tightness. A third attempt would
    // need a window over 550ms — the pre-sleep check adds the pending 200ms
    // backoff to a 350ms clock — or, at 320ms, attempt 2 failing ~20ms in
    // rather than at its 250ms `lock_timeout`. Load can only push that edge
    // further away.
    //
    // The one edge load can move is the post-sleep re-read: stopping on attempt 1
    // instead needs the flush round trip plus the 100ms sleep to reach 320ms,
    // which the previous 260ms left less room for.
    let policy = LockRetryPolicy {
        max_total_wait: Duration::from_millis(320),
        ..fast_policy(12)
    };
    let failure = run_up(&fixture.pool, &bounded_alter_migrator(), policy)
        .await
        .expect_err("the blocker never releases, so the attempt must lose");

    assert_eq!(
        failure.attempt, 2,
        "the window must bind before the attempt budget"
    );
    assert_eq!(failure.budget, 12);
    assert_lock_timeout_on_probe(&failure);

    blocker.rollback().await?;
    assert!(!probe_column_exists(&fixture.pool).await?);
    Ok(())
}

#[tokio::test]
async fn an_invalid_migration_fails_on_the_first_attempt() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };

    let migrator = probe_migrator(
        "ALTER TABLE no_such_table ADD COLUMN observed_at INT;".to_owned(),
        false,
    );
    let failure = run_up(&fixture.pool, &migrator, fast_policy(5))
        .await
        .expect_err("a missing table cannot be waited out");

    assert_eq!(failure.attempt, 1, "an invalid statement must not retry");
    let MigrateError::ExecuteMigration(source, _) = &failure.source else {
        panic!("expected a per-migration failure, got {:?}", failure.source);
    };
    assert_eq!(
        sqlstate(source).as_deref(),
        Some("42P01"),
        "expected undefined_table, got {source:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_no_transaction_lock_timeout_fails_on_the_first_attempt() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    create_probe_table(&fixture.pool).await?;

    // A `-- no-transaction` file must stay one statement, so its bound comes
    // from the session rather than an in-file `SET LOCAL`.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _metadata| {
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
                    "SET lock_timeout = '{PROBE_LOCK_TIMEOUT}'"
                ))))
                .execute(conn)
                .await
                .map(|_| ())
            })
        })
        .connect(&fixture.database_url)
        .await?;

    let mut holder = PgConnection::connect(&fixture.database_url).await?;
    let blocker = lock_probe_table(&mut holder).await?;

    let migrator = probe_migrator(
        "ALTER TABLE lock_probe ADD COLUMN observed_at TIMESTAMPTZ".to_owned(),
        true,
    );
    let failure = run_up(&pool, &migrator, fast_policy(3))
        .await
        .expect_err("the blocker never releases, so the attempt must lose");

    assert_eq!(
        failure.attempt, 1,
        "a no-transaction migration can leave partial state, so it must not retry"
    );
    assert_lock_timeout_on_probe(&failure);

    blocker.rollback().await?;
    assert!(!probe_column_exists(&fixture.pool).await?);
    Ok(())
}

#[tokio::test]
async fn the_backoff_does_not_hold_the_failed_transaction_open() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    create_probe_table(&fixture.pool).await?;
    create_second_probe_table(&fixture.pool).await?;

    // A two-DDL file, the common shape: the first `ALTER` succeeds and takes
    // `ACCESS EXCLUSIVE` on `lock_probe_second`, then the second loses its race
    // on the blocked `lock_probe`. sqlx only queues the rolled-back
    // transaction's `ROLLBACK`, so unless `run_up` flushes it the server keeps
    // the aborted transaction — and that first table's lock — for the whole
    // backoff.
    let migrator = probe_migrator(
        format!(
            "SET LOCAL lock_timeout = '{PROBE_LOCK_TIMEOUT}';
             ALTER TABLE lock_probe_second ADD COLUMN observed_at TIMESTAMPTZ;
             ALTER TABLE lock_probe ADD COLUMN observed_at TIMESTAMPTZ;"
        ),
        false,
    );

    let mut holder = PgConnection::connect(&fixture.database_url).await?;
    let blocker_pid = backend_pid(&mut holder).await?;
    let blocker = lock_probe_table(&mut holder).await?;

    // Two attempts with a long backoff between them, so an unflushed rollback
    // would be visible across most of the sampling window.
    let policy = LockRetryPolicy {
        attempts: NonZeroU32::new(2).expect("two attempts is non-zero"),
        initial_backoff: Duration::from_secs(3),
        max_backoff: Duration::from_secs(3),
        max_total_wait: Duration::from_secs(60),
    };
    // Stop sampling well before attempt 2 fails at ~3.5s (250ms + 3s backoff +
    // 250ms), so the count can only be reporting on the backoff itself. An
    // unflushed rollback is visible from ~250ms onward, so this window still
    // carries ~37 samples of signal — the figure the mutation actually measures.
    let (run, sampled) = tokio::join!(
        run_up(&fixture.pool, &migrator, policy),
        count_idle_in_transaction_samples(
            &fixture.database_url,
            blocker_pid,
            Duration::from_millis(2200)
        )
    );

    let failure = run.expect_err("the blocker never releases, so every attempt must lose");
    assert_eq!(failure.attempt, 2);
    let (caught, taken) = sampled?;
    assert!(
        taken > 20,
        "only {taken} samples taken; the window collapsed"
    );
    // Not zero-tolerance: the failed transaction legitimately exists for the
    // moment between the statement failing and the flush, and a scheduler stall
    // can stretch that past a 50ms sample. The discrimination is not 0-vs-1 — an
    // unflushed rollback spans the whole backoff and shows up in most samples
    // (measured: 37 of 42), so a handful of transient hits still reads clean and
    // a real regression sits an order of magnitude above this threshold.
    assert!(
        caught <= 4,
        "the rolled-back migration transaction was left open during the backoff \
         ({caught} of {taken} samples); its `ROLLBACK` was queued and never flushed"
    );

    blocker.rollback().await?;
    assert!(!probe_column_exists(&fixture.pool).await?);
    Ok(())
}

#[tokio::test]
async fn migrations_that_applied_do_not_spend_the_retry_window() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    create_probe_table(&fixture.pool).await?;

    let mut holder = PgConnection::connect(&fixture.database_url).await?;
    let blocker = lock_probe_table(&mut holder).await?;

    // The window is smaller than the successful migration's own runtime, so a
    // clock started at run start would stand at ~1450ms by the first failure
    // (1.2s sleep + the 250ms lock wait) and deny every retry. Measured from
    // the failure it reads zero, and the run gets its full budget: attempt 2
    // lands at ~300ms of retry time. Both margins are wide on purpose — 650ms
    // of slack before the window would cut attempt 3 short, 500ms before a
    // run-start clock would stop failing — because this is the tightest-timed
    // test in the file and it must not become a flake. This is the batch case
    // the retry matters most for: the biggest release has the most contention.
    let policy = LockRetryPolicy {
        attempts: NonZeroU32::new(3).expect("three attempts is non-zero"),
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(50),
        max_total_wait: Duration::from_millis(1000),
    };
    let failure = run_up(&fixture.pool, &slow_then_bounded_migrator(), policy)
        .await
        .expect_err("the blocker never releases, so the bounded ALTER must lose");

    assert_eq!(
        failure.attempt, 3,
        "a 1.2s migration that applied cleanly must not consume the retry window"
    );
    assert_lock_timeout_on_probe(&failure);
    // The slow migration committed on the first attempt and was skipped after.
    assert_eq!(
        applied_probe_versions(&fixture.pool).await?,
        [PROBE_VERSION - 1]
    );

    blocker.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_leaves_the_migration_advisory_lock_held_after_a_failure() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    create_probe_table(&fixture.pool).await?;

    let mut holder = PgConnection::connect(&fixture.database_url).await?;
    let blocker = lock_probe_table(&mut holder).await?;

    // Deliberately sqlx's own `run`, not `run_up`: this pins the premise the
    // connection pinning in `run_up` exists for. `run_direct` takes a
    // session-level `pg_advisory_lock` before it applies anything and reaches
    // its `unlock` only on success, so a failed run leaves the lock held.
    //
    // What this does not cover: that `run_up` actually reuses the session.
    // Stacked advisory locks collapse to one `pg_locks` row so the attempt
    // count is not observable, and an unpinned implementation would block on
    // this lock rather than fail, so a test would hang instead of asserting.
    // If sqlx ever starts releasing on the error path, this test fails and the
    // pinning can be reconsidered.
    bounded_alter_migrator()
        .run(&fixture.pool)
        .await
        .expect_err("the blocker never releases, so the migration must lose");

    let mut observer = PgConnection::connect(&fixture.database_url).await?;
    let held: i64 = sqlx::query_scalar(sql!(
        r"
        SELECT count(*) FROM pg_locks
        WHERE locktype = 'advisory'
          AND granted
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
        "
    ))
    .fetch_one(&mut observer)
    .await?;
    observer.close().await?;

    assert_eq!(
        held, 1,
        "sqlx released the migration advisory lock on its error path; the \
         connection pinning in `run_up` may no longer be needed"
    );

    blocker.rollback().await?;
    Ok(())
}
