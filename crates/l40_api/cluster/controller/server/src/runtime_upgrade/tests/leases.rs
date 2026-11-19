//! Leases: re-driving a dead worker's items, renewing a long upgrade's lease,
//! dropping work whose lease was lost, and the item deadline.

use super::*;

/// A reconciler whose leases last `lease`, renewed every third of it.
fn leasing(pool: &PgPool, driver: FakeDriver, lease: Duration) -> Reconciler {
    Reconciler::new(
        pool.clone(),
        Drivers::default().with(driver),
        ReconcilerConfig { lease, ..config() },
    )
}

async fn lease_token(
    pool: &PgPool,
    campaign: CampaignId,
    subject: Uuid,
) -> TestResult<Option<Uuid>> {
    Ok(sqlx::query_scalar::<_, Option<Uuid>>(sql!(
        "SELECT lease_token FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(subject)
    .fetch_one(pool)
    .await?)
}

/// Seed an in-flight item of `campaign` for `subject` whose lease expires
/// `expires` from now (an interval, negative for lapsed), `attempts` in.
async fn in_flight(
    pool: &PgPool,
    campaign: CampaignId,
    subject: Uuid,
    attempts: i64,
    expires: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
             lease_expires_at
         )
         VALUES ($1, $2, $3, 'in_flight', $4, $5, now() + $6::interval)"
    ))
    .bind(campaign)
    .bind(SubjectKind::TemplateRelease.as_str())
    .bind(subject)
    .bind(attempts)
    .bind(Uuid::now_v7())
    .bind(expires)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_item_stuck_in_flight_past_its_lease_is_redriven() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(2);
    let (lapsed, still_leased) = (found[0], found[1]);
    // A worker that died mid-upgrade left its lease behind, lapsed, beside
    // one that's still running.
    in_flight(&db.pool, campaign, lapsed, 1, "-1 second").await?;
    in_flight(&db.pool, campaign, still_leased, 1, "10 minutes").await?;
    let driver = FakeDriver::with_subjects(&found);
    let reconciler = reconciler(&db.pool, driver.clone());

    let report = tick(&reconciler).await;

    assert_eq!((report.requeued, report.leased), (1, 1));
    assert_eq!(driver.upgraded(), vec![lapsed]);
    assert_eq!(
        item(&db.pool, campaign, lapsed).await?,
        ("done".to_owned(), None, 2)
    );
    assert_eq!(item(&db.pool, campaign, still_leased).await?.0, "in_flight");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_redrive_that_uses_up_the_budget_pauses_before_anything_more_is_leased() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            failure_budget: 1,
            ..seed()
        },
    )
    .await?;
    let found = subjects(2);
    let (exhausted, waiting) = (found[0], found[1]);
    // The last allowed lease on `exhausted` lapsed.
    in_flight(
        &db.pool,
        campaign,
        exhausted,
        MAX_ATTEMPTS as i64,
        "-1 second",
    )
    .await?;
    let driver = FakeDriver::with_subjects(&found);
    let reconciler = reconciler(&db.pool, driver.clone());

    let report = tick(&reconciler).await;

    assert_eq!(report.paused, vec![campaign]);
    assert_eq!(report.leased, 0);
    assert!(driver.upgraded().is_empty());
    assert!(
        driver.state().finds.is_empty(),
        "paused before looking for subjects"
    );
    assert_eq!(item(&db.pool, campaign, exhausted).await?.0, "failed");
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "paused");
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

#[tokio::test(flavor = "current_thread")]
async fn a_long_upgrade_keeps_renewing_its_lease() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(1);
    let driver = FakeDriver {
        delay: Duration::from_millis(1500),
        ..FakeDriver::with_subjects(&found)
    };
    let reconciler = leasing(&db.pool, driver.clone(), Duration::from_secs(1));
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
            tokio::time::sleep(Duration::from_millis(1300)).await;
            store::redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await
        }
    );

    assert_eq!(redriven?, store::RedriveReport::default());
    assert_eq!(item(&db.pool, campaign, found[0]).await?.0, "done");
    // Every third of a second across a 1.5-second upgrade, not as fast as
    // it can.
    let renewals = sqlx::query_scalar::<_, i64>(sql!("SELECT n FROM renewals"))
        .fetch_one(&db.pool)
        .await?;
    assert!((2..=6).contains(&renewals), "{renewals} renewals");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_lost_lease_drops_the_upgrade_and_records_nothing() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(1);
    let driver = FakeDriver {
        delay: Duration::from_secs(30),
        ..FakeDriver::with_subjects(&found)
    };
    // Renewed at 1s, held locally until 2.7s: only the refused renewal
    // drops the upgrade before 2s.
    let reconciler = leasing(&db.pool, driver.clone(), Duration::from_secs(3));
    let thief = Uuid::now_v7();

    let started = tokio::time::Instant::now();
    let ((), stolen) = tokio::join!(
        async {
            tick(&reconciler).await;
        },
        async {
            // Another holder takes the item, as a re-drive and a new lease
            // would, once the upgrade is under way.
            driver.wait_started().await;
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
        started.elapsed() < Duration::from_secs(2),
        "the upgrade was dropped at the next renewal, before the local deadline"
    );
    assert_eq!(driver.state().finished, 0);
    assert_eq!(item(&db.pool, campaign, found[0]).await?.0, "in_flight");
    assert_eq!(
        lease_token(&db.pool, campaign, found[0]).await?,
        Some(thief)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_that_never_returns_fails_at_the_deadline() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            failure_budget: 1,
            ..seed()
        },
    )
    .await?;
    let found = subjects(1);
    let driver = FakeDriver {
        delay: Duration::from_secs(3600),
        ..FakeDriver::with_subjects(&found)
    };
    let reconciler = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(driver.clone()),
        ReconcilerConfig {
            lease: Duration::from_secs(3),
            item_deadline: Duration::from_secs(1),
            ..config()
        },
    );

    // The deadline runs from the lease, after the tick starts, so the tick
    // takes at least that long; a bound well past it catches a hang.
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(30), tick(&reconciler))
        .await
        .expect("the dispatch was dropped, not left hanging");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_secs(1),
        "not dropped before the deadline: {elapsed:?}"
    );
    assert_eq!(driver.state().finished, 0);
    assert_eq!(
        item(&db.pool, campaign, found[0]).await?,
        (
            "failed".to_owned(),
            Some("the upgrade didn't finish within 1s".to_owned()),
            1
        )
    );
    assert_eq!(
        lease_token(&db.pool, campaign, found[0]).await?,
        None,
        "the lease is released"
    );
    // The failure counts against the budget.
    assert_eq!(tick(&reconciler).await.paused, vec![campaign]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_outcome_write_that_hangs_is_given_up_within_a_lease() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(1);
    // The upgrade returns only once the item's row is held, so its outcome
    // write is the one that blocks.
    let release = Arc::new(tokio::sync::Notify::new());
    let driver = FakeDriver {
        release: Some(Arc::clone(&release)),
        ..FakeDriver::with_subjects(&found)
    };
    let reconciler = leasing(&db.pool, driver.clone(), Duration::from_secs(1));

    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let (elapsed, locked) = tokio::join!(
        async {
            let started = tokio::time::Instant::now();
            tick(&reconciler).await;
            let elapsed = started.elapsed();
            let _ = done_tx.send(());
            elapsed
        },
        async {
            // Hold the item's row once the upgrade is under way, so writing
            // its outcome blocks without an answer.
            driver.wait_started().await;
            let mut locker = db.pool.begin().await?;
            sqlx::query(sql!(
                "SELECT 1 FROM runtime_upgrade_items WHERE campaign_id = $1 FOR UPDATE"
            ))
            .bind(campaign)
            .execute(&mut *locker)
            .await?;
            release.notify_one();
            let _ = tokio::time::timeout(Duration::from_secs(3), done_rx).await;
            // Read while the row is still held: once it's released the
            // abandoned write, already on the server, may land.
            let phase = item(&db.pool, campaign, found[0]).await?.0;
            locker.rollback().await?;
            TestResult::Ok(phase)
        }
    );
    let phase_while_held = locked?;

    assert!(
        elapsed < Duration::from_millis(2500),
        "the outcome write was given up, not left waiting: {elapsed:?}"
    );
    assert_eq!(driver.state().finished, 1);
    assert_eq!(
        phase_while_held, "in_flight",
        "the tick moved on while the outcome write was still blocked"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_abort_drops_the_upgrade_in_flight_and_drives_nothing_more() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_cluster: 1,
            ..seed()
        },
    )
    .await?;
    let found = subjects(3);
    let driver = FakeDriver {
        delay: Duration::from_secs(30),
        ..FakeDriver::with_subjects(&found)
    };
    // Renewed at 1s, held locally until 2.7s: only the refused renewal
    // drops the upgrade before 2s.
    let reconciler = leasing(&db.pool, driver.clone(), Duration::from_secs(3));

    let started = tokio::time::Instant::now();
    let ((), aborted) = tokio::join!(
        async {
            tick(&reconciler).await;
        },
        async {
            driver.wait_started().await;
            store::abort_campaign(&db.pool, campaign, &store::PhaseChange::default()).await
        }
    );

    assert_eq!(aborted?.draining, 1);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the upgrade was dropped at the next renewal, before the local deadline"
    );
    assert_eq!(driver.upgraded(), vec![found[0]]);
    assert_eq!(
        driver.state().finished,
        0,
        "the dropped upgrade recorded nothing"
    );
    // Later ticks lease nothing for the aborted campaign. Once its lease
    // lapses, a tick returns the item to pending.
    assert_eq!(tick(&reconciler).await, TickReport::default());
    assert_eq!(
        counts(&db.pool, campaign).await?.in_flight,
        1,
        "still leased"
    );
    // Every lease write happened before now, so the lease the worker last
    // wrote ends within one lease length of now. Then lapse it rather than
    // wait it out.
    assert!(
        sqlx::query_scalar::<_, bool>(sql!(
            "SELECT lease_expires_at <= clock_timestamp() + interval '3 seconds'
             FROM runtime_upgrade_items
             WHERE campaign_id = $1 AND phase = 'in_flight'"
        ))
        .bind(campaign)
        .fetch_one(&db.pool)
        .await?,
        "the lease outlives its length"
    );
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items SET lease_expires_at = now() - interval '1 second'
         WHERE campaign_id = $1 AND phase = 'in_flight'"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;
    assert_eq!(tick(&reconciler).await, TickReport::default());
    assert_eq!(driver.upgraded(), vec![found[0]]);
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 3,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

/// The abort caveat, raced: an aborted campaign's upgrade is still running
/// inside its lease when a new campaign for the same source starts and finds
/// the same subject. The subject is upgraded by one worker at a time, and its
/// upgrade takes effect once.
#[tokio::test(flavor = "current_thread")]
async fn a_new_campaign_never_upgrades_a_subject_an_aborted_one_still_holds() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let new_campaign = || store::NewCampaign {
        from: environment(0),
        to: environment(1),
        kind: CampaignKind::Templates,
        canary: false,
        max_in_flight_per_node: 1,
        max_in_flight_per_cluster: 2,
        failure_budget: 3,
        include_running: false,
        reason: "move off a blocked environment".to_owned(),
        created_by: None,
    };
    let aborted = store::create_campaign(&db.pool, &new_campaign()).await?;
    let found = subjects(1);
    let subject = found[0];
    let driver = FakeDriver {
        delay: Duration::from_millis(50),
        ..FakeDriver::with_subjects(&found)
    };
    // The aborted campaign's upgrade would run a minute; the new one's is
    // quick.
    driver.state().delays.push_back(Duration::from_secs(60));
    // The aborted worker stops at its first refused renewal, a third of the
    // lease after the renewal before its upgrade.
    let reconciler = leasing(&db.pool, driver.clone(), Duration::from_secs(2));

    assert_eq!(reconciler.tick().await.leased, 1);
    driver.wait_started().await;
    store::abort_campaign(&db.pool, aborted, &store::PhaseChange::default()).await?;
    let replacement = store::create_campaign(&db.pool, &new_campaign()).await?;

    // While the aborted campaign's lease lives, the new campaign records the
    // subject but doesn't lease it. Hold the lease live outright, so no tick
    // can see it lapse partway: its renewals are refused now that the
    // campaign is finished, so they can't move it either.
    let held = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() + interval '1 hour'
         WHERE campaign_id = $1 AND subject_id = $2 AND phase = 'in_flight'"
    ))
    .bind(aborted)
    .bind(subject)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        held.rows_affected(),
        1,
        "the aborted campaign holds the subject"
    );
    for _ in 0..3 {
        let report = reconciler.tick().await;
        assert_eq!(report.leased, 0, "the subject is still held");
    }
    assert_eq!(item(&db.pool, replacement, subject).await?.0, "pending");
    // The aborted campaign's worker stops at its next renewal, before the
    // lease lapses. Once it has, lapse the lease outright, and the new
    // campaign leases the subject and upgrades it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while driver
        .state()
        .running_by_subject
        .get(&subject)
        .copied()
        .unwrap_or(0)
        > 0
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the aborted worker stopped"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = now() - interval '1 second'
         WHERE campaign_id = $1 AND phase = 'in_flight'"
    ))
    .bind(aborted)
    .execute(&db.pool)
    .await?;
    let mut leased = 0;
    for _ in 0..3 {
        leased += reconciler.tick().await.leased;
        reconciler.settle().await;
    }
    assert_eq!(leased, 1);

    {
        let state = driver.state();
        assert_eq!(state.upgraded, vec![subject, subject]);
        assert_eq!(
            state.max_running_one_subject, 1,
            "never two upgrades at once"
        );
        assert_eq!(
            state.finished, 1,
            "the aborted one was dropped, not finished"
        );
        assert!(state.applied.contains(&subject));
        assert_eq!(state.already_on_target, 0, "no second effect");
    }
    // One outcome: the new campaign's.
    assert_eq!(item(&db.pool, replacement, subject).await?.0, "done");
    assert_eq!(item(&db.pool, aborted, subject).await?.0, "pending");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_dispatch_that_starts_after_its_lease_lapsed_never_runs_the_upgrade() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(1);
    store::insert_pending(
        &db.pool,
        campaign,
        SubjectKind::TemplateRelease,
        &found,
        MAX_ATTEMPTS,
    )
    .await?;
    let lease = Duration::from_secs(1);
    let leased = store::lease_items(&db.pool, campaign, lease, 10)
        .await?
        .pop()
        .expect("leased");
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|row| row.id == campaign)
        .expect("listed");
    let driver = FakeDriver::with_subjects(&found);
    let worker =
        super::super::worker::Worker::new(db.pool.clone(), lease, Duration::from_secs(900));

    // Requested two seconds ago under a one-second lease: the task started
    // after the lease's local deadline.
    let leased_at = tokio::time::Instant::now() - Duration::from_secs(2);
    worker
        .drive_item(Arc::new(driver.clone()), campaign_row, leased, leased_at)
        .await;

    assert!(driver.upgraded().is_empty(), "the driver was never asked");
    assert_eq!(
        item(&db.pool, campaign, found[0]).await?.0,
        "in_flight",
        "nothing recorded"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_dispatch_past_its_item_deadline_never_runs_the_upgrade() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(1);
    store::insert_pending(
        &db.pool,
        campaign,
        SubjectKind::TemplateRelease,
        &found,
        MAX_ATTEMPTS,
    )
    .await?;
    let lease = Duration::from_secs(3);
    let leased = store::lease_items(&db.pool, campaign, lease, 10)
        .await?
        .pop()
        .expect("leased");
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|row| row.id == campaign)
        .expect("listed");
    // An upgrade that's ready at once.
    let driver = FakeDriver::with_subjects(&found);
    let deadline = Duration::from_millis(100);
    let worker = super::super::worker::Worker::new(db.pool.clone(), lease, deadline);

    // Requested half a second ago: the lease is still held, but the item's
    // deadline has passed.
    let leased_at = tokio::time::Instant::now() - Duration::from_millis(500);
    worker
        .drive_item(Arc::new(driver.clone()), campaign_row, leased, leased_at)
        .await;

    assert!(driver.upgraded().is_empty(), "the driver was never asked");
    assert_eq!(
        item(&db.pool, campaign, found[0]).await?.0,
        "failed",
        "the item failed at its deadline"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_dispatch_of_a_campaign_aborted_since_its_lease_never_runs_the_upgrade() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let found = subjects(1);
    store::insert_pending(
        &db.pool,
        campaign,
        SubjectKind::TemplateRelease,
        &found,
        MAX_ATTEMPTS,
    )
    .await?;
    let lease = Duration::from_secs(60);
    let leased_at = tokio::time::Instant::now();
    let leased = store::lease_items(&db.pool, campaign, lease, 10)
        .await?
        .pop()
        .expect("leased");
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|row| row.id == campaign)
        .expect("listed");
    store::abort_campaign(&db.pool, campaign, &store::PhaseChange::default()).await?;
    let driver = FakeDriver::with_subjects(&found);
    let worker =
        super::super::worker::Worker::new(db.pool.clone(), lease, Duration::from_secs(900));

    // The lease is live, but its campaign is aborted.
    worker
        .drive_item(Arc::new(driver.clone()), campaign_row, leased, leased_at)
        .await;

    assert!(driver.upgraded().is_empty(), "the driver was never asked");
    assert_eq!(item(&db.pool, campaign, found[0]).await?.0, "in_flight");
    Ok(())
}
