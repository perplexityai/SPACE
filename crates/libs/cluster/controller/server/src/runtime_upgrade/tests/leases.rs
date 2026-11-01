//! Leases: re-driving a dead worker's items, renewing a long upgrade's lease,
//! and dropping work whose lease was lost.

use super::*;

/// A reconciler whose leases last `lease`, renewed every third of it.
fn leasing(
    pool: &PgPool,
    dispatcher: FakeDispatcher,
    lease: Duration,
) -> Reconciler<FakeDispatcher> {
    Reconciler::new(
        pool.clone(),
        K8S,
        dispatcher,
        ReconcilerConfig {
            enabled: true,
            lease,
            ..ReconcilerConfig::default()
        },
    )
}

async fn lease_token(
    pool: &PgPool,
    campaign: CampaignId,
    sandbox: SandboxId,
) -> TestResult<Option<uuid::Uuid>> {
    Ok(sqlx::query_scalar::<_, Option<uuid::Uuid>>(sql!(
        "SELECT lease_token FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn an_item_stuck_in_flight_past_its_lease_is_redriven() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let sandbox = stopped_on_source(&db.pool, node).await?;
    let still_leased = stopped_on_source(&db.pool, node).await?;
    // A worker that died mid-upgrade left its lease behind, lapsed, beside
    // one that's still running.
    for (subject, expires) in [(sandbox, "-1 second"), (still_leased, "10 minutes")] {
        sqlx::query(sql!(
            "INSERT INTO runtime_upgrade_items (
                 campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
                 lease_expires_at
             )
             VALUES ($1, 'sandbox', $2, 'in_flight', 1, $3, now() + $4::interval)"
        ))
        .bind(campaign)
        .bind(subject)
        .bind(uuid::Uuid::now_v7())
        .bind(expires)
        .execute(&db.pool)
        .await?;
    }
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let report = tick(&reconciler).await;

    assert_eq!((report.requeued, report.leased), (1, 1));
    assert_eq!(dispatcher.upgraded(), vec![sandbox]);
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        ("done".to_owned(), None, 2)
    );
    assert_eq!(item(&db.pool, campaign, still_leased).await?.0, "in_flight");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_long_upgrade_keeps_renewing_its_lease() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let sandbox = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher {
        delay: Duration::from_millis(4500),
        ..FakeDispatcher::default()
    };
    let reconciler = leasing(&db.pool, dispatcher.clone(), Duration::from_secs(3));
    // Count the renewals: updates that move an in-flight lease's expiry.
    sqlx::raw_sql(sql!(
        "CREATE TABLE renewals (n BIGINT NOT NULL);
         INSERT INTO renewals VALUES (0);
         CREATE FUNCTION count_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF OLD.phase = 'in_flight' AND NEW.phase = 'in_flight' THEN
                 UPDATE renewals SET n = n + 1;
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER count_renewal AFTER UPDATE ON runtime_upgrade_items
             FOR EACH ROW EXECUTE FUNCTION count_renewal();"
    ))
    .execute(&db.pool)
    .await?;

    // Past the first lease's end, a re-drive finds nothing lapsed.
    let ((), redriven) = tokio::join!(
        async {
            tick(&reconciler).await;
        },
        async {
            tokio::time::sleep(Duration::from_secs(4)).await;
            store::redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await
        }
    );

    assert_eq!(redriven?, store::RedriveReport::default());
    assert_eq!(item(&db.pool, campaign, sandbox).await?.0, "done");
    // Every second across a 4.5-second upgrade, not as fast as it can.
    let renewals = sqlx::query_scalar::<_, i64>(sql!("SELECT n FROM renewals"))
        .fetch_one(&db.pool)
        .await?;
    assert!((3..=5).contains(&renewals), "{renewals} renewals");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_lost_lease_drops_the_upgrade_and_records_nothing() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let sandbox = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher {
        delay: Duration::from_secs(30),
        ..FakeDispatcher::default()
    };
    let reconciler = leasing(&db.pool, dispatcher.clone(), Duration::from_secs(3));
    let thief = uuid::Uuid::now_v7();

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let started = tokio::time::Instant::now();
    let ((), stolen) = tokio::join!(
        async {
            tick(&reconciler).await;
        },
        async {
            // Another holder takes the item, as a re-drive and a new lease
            // would.
            tokio::time::sleep(Duration::from_millis(500)).await;
            sqlx::query(sql!(
                "UPDATE runtime_upgrade_items SET lease_token = $2
                 WHERE campaign_id = $1 AND phase = 'in_flight'"
            ))
            .bind(campaign)
            .bind(thief)
            .execute(&db.pool)
            .await
        }
    );

    assert_eq!(stolen?.rows_affected(), 1);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the upgrade was dropped at the next renewal"
    );
    assert_eq!(dispatcher.state.lock().unwrap().finished, 0);
    assert_eq!(item(&db.pool, campaign, sandbox).await?.0, "in_flight");
    assert_eq!(lease_token(&db.pool, campaign, sandbox).await?, Some(thief));
    assert!(counters.finish().iter().any(|counter| {
        counter.name == "runtime_upgrade.campaign.item_outcome"
            && counter.tags.contains(&"outcome:lease_lost".to_owned())
    }));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_redrive_that_uses_up_the_budget_pauses_before_anything_more_is_leased() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            failure_budget: 1,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let exhausted = stopped_on_source(&db.pool, node).await?;
    let waiting = stopped_on_source(&db.pool, node).await?;
    // The last allowed lease on `exhausted` lapsed.
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
             lease_expires_at
         )
         VALUES ($1, 'sandbox', $2, 'in_flight', $3, $4, now() - interval '1 second')"
    ))
    .bind(campaign)
    .bind(exhausted)
    .bind(MAX_ATTEMPTS as i64)
    .bind(uuid::Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let report = tick(&reconciler).await;

    assert_eq!(report.paused, vec![campaign]);
    assert_eq!(report.leased, 0);
    assert!(dispatcher.calls().is_empty());
    assert_eq!(item(&db.pool, campaign, exhausted).await?.0, "failed");
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "paused");
    // Never recorded: the pause came before this tick looked for subjects.
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT count(*) FROM runtime_upgrade_items WHERE subject_id = $1"
        ))
        .bind(waiting)
        .fetch_one(&db.pool)
        .await?,
        0
    );
    Ok(())
}

/// Make every lease renewal fail, as a database outage would: a trigger
/// refuses updates that move an in-flight item's lease.
async fn refuse_renewals(pool: &PgPool) -> TestResult {
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF OLD.phase = 'in_flight' AND NEW.phase = 'in_flight' THEN
                 RAISE EXCEPTION 'renewals are down';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER refuse_renewal BEFORE UPDATE ON runtime_upgrade_items
             FOR EACH ROW EXECUTE FUNCTION refuse_renewal();"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failing_renewals_drop_the_upgrade_once_the_lease_has_lapsed() -> TestResult {
    // Renewals run every second of a three-second lease. Failing from the
    // start, the work goes when the first lease lapses; after one good
    // renewal at one second, a lease past that.
    for (fail_after, lapses_at) in [(None, 3.0), (Some(Duration::from_millis(1500)), 4.0)] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
        let campaign = insert_campaign(&db.pool, seed()).await?;
        let node = node(&db.pool).await?;
        stopped_on_source(&db.pool, node).await?;
        if fail_after.is_none() {
            refuse_renewals(&db.pool).await?;
        }
        let dispatcher = FakeDispatcher {
            delay: Duration::from_secs(30),
            ..FakeDispatcher::default()
        };
        let reconciler = leasing(&db.pool, dispatcher.clone(), Duration::from_secs(3));

        let started = tokio::time::Instant::now();
        let ((), refused) = tokio::join!(
            async {
                tick(&reconciler).await;
            },
            async {
                match fail_after {
                    Some(after) => {
                        tokio::time::sleep(after).await;
                        refuse_renewals(&db.pool).await
                    }
                    None => Ok(()),
                }
            }
        );
        refused?;
        let elapsed = started.elapsed().as_secs_f64();

        assert!(
            (lapses_at - 0.5..lapses_at + 1.5).contains(&elapsed),
            "{fail_after:?}: the upgrade ran {elapsed}s, expected about {lapses_at}s"
        );
        assert_eq!(dispatcher.state.lock().unwrap().finished, 0);
        assert_eq!(counts(&db.pool, campaign).await?.in_flight, 1);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_dispatch_that_never_returns_fails_at_the_deadline() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            failure_budget: 1,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let hung = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher {
        delay: Duration::from_secs(3600),
        ..FakeDispatcher::default()
    };
    let reconciler = Reconciler::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        ReconcilerConfig {
            enabled: true,
            lease: Duration::from_secs(3),
            item_deadline: Duration::from_secs(2),
            ..ReconcilerConfig::default()
        },
    );

    let started = tokio::time::Instant::now();
    tick(&reconciler).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_secs(2) && elapsed < Duration::from_secs(4),
        "dropped at the deadline, after {elapsed:?}"
    );
    assert_eq!(dispatcher.state.lock().unwrap().finished, 0);
    assert_eq!(
        item(&db.pool, campaign, hung).await?,
        (
            "failed".to_owned(),
            Some("the upgrade didn't finish within 2s".to_owned()),
            1
        )
    );
    assert_eq!(
        lease_token(&db.pool, campaign, hung).await?,
        None,
        "the lease is released"
    );
    // The failure counts against the budget.
    assert_eq!(tick(&reconciler).await.paused, vec![campaign]);
    Ok(())
}
