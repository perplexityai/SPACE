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
           AND c.relname IN ('runtime_upgrade_campaigns', 'runtime_upgrade_items')"
    ))
    .fetch_one(pool)
    .await?)
}

/// Run the loop under `config` for a second while another transaction holds
/// both campaign tables exclusively, and report how many of the loop's
/// statements waited on them.
async fn waiters_while_running(db: &TestDb, config: ReconcilerConfig) -> TestResult<i64> {
    let mut locker = db.pool.begin().await?;
    sqlx::query(sql!(
        "LOCK TABLE runtime_upgrade_campaigns, runtime_upgrade_items IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut *locker)
    .await?;
    let shutdown = CancellationToken::new();
    let dispatcher = FakeDispatcher::default();
    let (_, observed) = tokio::join!(
        run(
            config,
            db.pool.clone(),
            K8S,
            dispatcher.clone(),
            shutdown.clone()
        ),
        async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let observed = waiters(&db.pool).await;
            // Release the tables so a blocked tick can finish and see the
            // shutdown.
            let released = locker.rollback().await;
            shutdown.cancel();
            released.map_err(Into::into).and(observed)
        }
    );
    assert!(dispatcher.calls().is_empty());
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
