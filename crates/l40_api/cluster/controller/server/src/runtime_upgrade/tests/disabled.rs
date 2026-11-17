//! Disabled means no reads or writes of the campaign tables.

use super::*;

/// Lock-waiters on the campaign tables: any access to a table another
/// transaction holds `ACCESS EXCLUSIVE` waits, a plain read included.
async fn waiters(pool: &PgPool) -> TestResult<i64> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*)
         FROM pg_locks l
         JOIN pg_class c ON c.oid = l.relation
         WHERE NOT l.granted
           AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND c.relname IN ('runtime_upgrade_campaigns', 'runtime_upgrade_items')"
    ))
    .fetch_one(pool)
    .await?)
}

/// Poll [`waiters`] until one shows up, for at most ten seconds.
async fn wait_for_waiter(pool: &PgPool) -> TestResult<i64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting = waiters(pool).await?;
        if waiting > 0 || tokio::time::Instant::now() >= deadline {
            return Ok(waiting);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Run the loop under `config` while another transaction holds
/// both campaign tables exclusively, and report how many of the loop's
/// statements waited on them.
async fn waiters_while_running(db: &TestDb, config: ReconcilerConfig) -> TestResult<i64> {
    let mut locker = db.pool.begin().await?;
    sqlx::query(sql!(
        "LOCK TABLE runtime_upgrade_campaigns, runtime_upgrade_items IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut *locker)
    .await?;
    let enabled = config.enabled;
    let shutdown = CancellationToken::new();
    // No subjects: the enabled run's tick, unblocked when the lock is
    // released, finds nothing to dispatch before it sees the shutdown.
    let driver = FakeDriver::default();
    let (_, observed) = tokio::join!(
        run(
            config,
            db.pool.clone(),
            Drivers::default().with(driver.clone()),
            shutdown.clone()
        ),
        async {
            // Enabled, wait until its first read is blocked; disabled, give
            // it the same 400ms window to touch the tables, which it mustn't.
            let observed = if enabled {
                wait_for_waiter(&db.pool).await
            } else {
                tokio::time::sleep(Duration::from_millis(400)).await;
                waiters(&db.pool).await
            };
            // Release the tables so a blocked tick can finish and see the
            // shutdown.
            let released = locker.rollback().await;
            shutdown.cancel();
            released.map_err(Into::into).and(observed)
        }
    );
    assert!(driver.upgraded().is_empty());
    if !enabled {
        assert!(
            driver.state().finds.is_empty(),
            "disabled, the driver isn't asked"
        );
    }
    observed
}

#[tokio::test(flavor = "current_thread")]
async fn a_disabled_reconciler_never_touches_the_campaign_tables() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    insert_campaign(&db.pool, seed()).await?;
    let tick = Duration::from_millis(100);

    let disabled = ReconcilerConfig {
        enabled: false,
        tick,
        ..ReconcilerConfig::default()
    };
    assert_eq!(waiters_while_running(&db, disabled).await?, 0);

    // The probe sees an enabled loop's first read wait on the lock.
    let enabled = ReconcilerConfig {
        enabled: true,
        tick,
        ..ReconcilerConfig::default()
    };
    assert_eq!(waiters_while_running(&db, enabled).await?, 1);
    Ok(())
}
