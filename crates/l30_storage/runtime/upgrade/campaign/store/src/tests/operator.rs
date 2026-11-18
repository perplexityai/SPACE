//! Reads, the generation, and the leases and phase changes that the operator
//! verbs build on.

use super::*;

async fn generation(pool: &PgPool, campaign: CampaignId) -> TestResult<u64> {
    Ok(get_record(pool, campaign)
        .await?
        .expect("exists")
        .campaign
        .generation)
}

/// Fail `count` pending items of `campaign` through their leases.
async fn fail_items(db: &TestDb, campaign: CampaignId, count: usize) -> TestResult {
    pending_on(db, campaign, None, count).await?;
    let leased = lease(db, campaign).await?;
    assert_eq!(leased.len(), count, "every item leased");
    for item in leased {
        assert!(
            finish_item(
                &db.pool,
                &item,
                &ItemOutcome::Failed(ItemError::new("boot failed")),
            )
            .await?,
            "the failure was recorded"
        );
    }
    Ok(())
}

/// Run `set` as the SET list of an UPDATE of `campaign`, binding `user`, if
/// given, as `$2`, and return the generation the schema's trigger left.
async fn update_campaign(
    pool: &PgPool,
    campaign: CampaignId,
    set: &str,
    user: Option<Uuid>,
) -> TestResult<i64> {
    let query =
        format!("UPDATE runtime_upgrade_campaigns SET {set} WHERE id = $1 RETURNING generation");
    let mut update = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(query)).bind(campaign);
    if let Some(user) = user {
        update = update.bind(user);
    }
    Ok(update.fetch_one(pool).await?)
}

/// Insert a user in the seed org.
async fn insert_user(pool: &PgPool) -> TestResult<Uuid> {
    let user = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Campaign Operator', $3, 'member')"
    ))
    .bind(user)
    .bind(format!("campaign-operator-{user}@example.com"))
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(user)
}

#[tokio::test(flavor = "current_thread")]
async fn the_schema_raises_the_generation_on_a_change_and_overrides_a_writers_value() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let paused = CampaignPhase::Paused.as_str();
    let rolling = CampaignPhase::Rolling.as_str();

    // Updates that change no other column leave it alone, whatever they
    // assign to it.
    for set in [
        "phase = phase",
        "generation = 7",
        "generation = generation + 1",
        "updated_at = now()",
        "updated_by_user_id = NULL",
    ] {
        assert_eq!(
            update_campaign(&db.pool, campaign, set, None).await?,
            0,
            "{set}"
        );
    }

    // A change raises it by exactly one, and a value the writer assigns is
    // overridden: -1 doesn't reach the CHECK, and 100 doesn't stick.
    let changes = [
        format!("phase = '{paused}'"),
        format!("phase = '{rolling}', generation = 0"),
        format!("phase = '{paused}', generation = -1"),
        format!("phase = '{rolling}', generation = 100"),
    ];
    for (raised, set) in (1..).zip(&changes) {
        assert_eq!(
            update_campaign(&db.pool, campaign, set, None).await?,
            raised,
            "{set}"
        );
    }

    // The user columns don't count: neither the ON DELETE SET NULL cascade
    // from deleting a user nor setting them raises it. This update sets them
    // with a phase change, which does raise it, so the cascade is the first
    // update that touches only the user columns.
    let operator = insert_user(&db.pool).await?;
    let set = format!("phase = '{paused}', created_by_user_id = $2, updated_by_user_id = $2");
    assert_eq!(
        update_campaign(&db.pool, campaign, &set, Some(operator)).await?,
        5,
        "{set}"
    );
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    let operator_id = Some(pplx_space_accounts::UserId::from_uuid(operator));
    assert_eq!(record.created_by, operator_id, "the update set it");
    assert_eq!(record.updated_by, operator_id, "the update set it");
    sqlx::query(sql!("DELETE FROM users WHERE id = $1"))
        .bind(operator)
        .execute(&db.pool)
        .await?;
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.created_by, None, "the cascade ran");
    assert_eq!(record.updated_by, None, "the cascade ran");
    assert_eq!(record.campaign.generation, 5, "the cascade");
    let other = insert_user(&db.pool).await?;
    assert_eq!(
        update_campaign(
            &db.pool,
            campaign,
            "created_by_user_id = $2, updated_by_user_id = $2",
            Some(other),
        )
        .await?,
        5,
        "setting the user columns"
    );
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    let other_id = Some(pplx_space_accounts::UserId::from_uuid(other));
    assert_eq!(record.created_by, other_id, "the update set it");
    assert_eq!(record.updated_by, other_id, "the update set it");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn item_progress_leaves_the_generation_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    pending_on(&db, campaign, None, 2).await?;
    let leased = lease(&db, campaign).await?;
    assert_eq!(leased.len(), 2, "both items leased");
    assert!(renew_lease(&db.pool, &leased[0], LEASE).await?);
    assert!(finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?);
    lapse_leases(&db.pool, campaign).await?;
    assert_eq!(
        redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await?,
        RedriveReport {
            requeued: 1,
            failed: 0
        }
    );
    let counts = item_counts(&db.pool, campaign).await?;
    assert_eq!((counts.done, counts.pending), (1, 1), "{counts:?}");
    assert!(!pause_if_over_budget(&db.pool, campaign).await?);
    assert_eq!(generation(&db.pool, campaign).await?, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn records_list_in_id_order_and_page() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let first = create_campaign(&db.pool, &new_campaign(30, CampaignKind::Sandboxes)).await?;
    let second = create_campaign(
        &db.pool,
        &NewCampaign {
            created_by: Some(pplx_space_accounts::UserId::from_uuid(SEED_USER_ID)),
            ..new_campaign(31, CampaignKind::Templates)
        },
    )
    .await?;
    let finished = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Aborted,
            from: environment(32),
            ..CampaignSeed::default()
        },
    )
    .await?;

    // UUIDv7 ids order by creation only to the millisecond, so the expected
    // order is the ids' own.
    let mut by_id = vec![first, second, finished];
    by_id.sort();
    let all = list_records(&db.pool, false, None, 10).await?;
    assert_eq!(all.iter().map(|r| r.campaign.id).collect::<Vec<_>>(), by_id);
    let page = list_records(&db.pool, false, Some(by_id[0]), 1).await?;
    assert_eq!(
        page.iter().map(|r| r.campaign.id).collect::<Vec<_>>(),
        [by_id[1]]
    );
    let mut unfinished_by_id = vec![first, second];
    unfinished_by_id.sort();
    let unfinished = list_records(&db.pool, true, None, 10).await?;
    assert_eq!(
        unfinished.iter().map(|r| r.campaign.id).collect::<Vec<_>>(),
        unfinished_by_id
    );

    let record = get_record(&db.pool, second).await?.expect("exists");
    assert_eq!(record.reason, "move off a blocked environment");
    assert_eq!(
        record.created_by,
        Some(pplx_space_accounts::UserId::from_uuid(SEED_USER_ID))
    );
    assert_eq!(record.campaign.kind, CampaignKind::Templates);
    assert_eq!(record.finished_at, None);
    assert_eq!(
        get_record(&db.pool, CampaignId(Uuid::now_v7())).await?,
        None
    );

    assert_eq!(
        unfinished_from(&db.pool, CampaignKind::Templates, environment(31)).await?,
        Some(second)
    );
    assert_eq!(
        unfinished_from(&db.pool, CampaignKind::Sandboxes, environment(31)).await?,
        None
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_live_lease_in_any_other_campaign_holds_the_subject_and_a_lapsed_one_doesnt() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let subject = Uuid::now_v7();
    let other = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(45),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(46),
            ..CampaignSeed::default()
        },
    )
    .await?;
    for target in [other, campaign] {
        insert_pending(
            &db.pool,
            target,
            SubjectKind::Sandbox,
            &[subject],
            MAX_ATTEMPTS,
        )
        .await?;
    }
    let held = lease(&db, other).await?;
    assert_eq!(held.len(), 1);
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "held by the other campaign"
    );
    // A template release with the same id is a different subject.
    let releases = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::Templates,
            from: environment(47),
            ..CampaignSeed::default()
        },
    )
    .await?;
    insert_pending(
        &db.pool,
        releases,
        SubjectKind::TemplateRelease,
        &[subject],
        MAX_ATTEMPTS,
    )
    .await?;
    assert_eq!(lease(&db, releases).await?.len(), 1);

    lapse_leases(&db.pool, other).await?;
    assert_eq!(
        lease(&db, campaign).await?.len(),
        1,
        "a lapsed lease holds nothing"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_aborted_campaigns_lease_isnt_renewed_and_holds_its_subject_until_it_lapses()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let subject = Uuid::now_v7();
    let aborted = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(50),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(51),
            ..CampaignSeed::default()
        },
    )
    .await?;
    for target in [aborted, campaign] {
        insert_pending(
            &db.pool,
            target,
            SubjectKind::Sandbox,
            &[subject],
            MAX_ATTEMPTS,
        )
        .await?;
    }
    let held = lease(&db, aborted).await?.remove(0);
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'aborted', finished_at = now() WHERE id = $1"
    ))
    .bind(aborted)
    .execute(&db.pool)
    .await?;

    assert!(
        !renew_lease(&db.pool, &held, LEASE).await?,
        "a finished campaign's lease isn't renewed"
    );
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "the aborted campaign's live lease still holds the subject"
    );
    assert_eq!(
        release_lapsed_in_finished(&db.pool).await?,
        0,
        "a live lease isn't released"
    );

    lapse_leases(&db.pool, aborted).await?;
    assert_eq!(release_lapsed_in_finished(&db.pool).await?, 1);
    assert_eq!(item_phase(&db.pool, aborted, subject).await?, "pending");
    assert_eq!(
        lease(&db, campaign).await?.len(),
        1,
        "the lapsed lease holds nothing"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_drained_canary_with_nothing_done_completes_and_one_with_a_done_item_waits() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let canary = |byte| CampaignSeed {
        phase: CampaignPhase::Canary,
        from: environment(byte),
        ..CampaignSeed::default()
    };
    // No subjects at all.
    let empty = insert_campaign(&db.pool, canary(50)).await?;
    assert!(complete_if_drained(&db.pool, empty).await?);
    // Every subject skipped.
    let skipped = insert_campaign(&db.pool, canary(51)).await?;
    pending_on(&db, skipped, None, 1).await?;
    let leased = lease(&db, skipped).await?;
    finish_item(
        &db.pool,
        &leased[0],
        &ItemOutcome::Skipped(ItemError::new("gone")),
    )
    .await?;
    assert!(complete_if_drained(&db.pool, skipped).await?);
    // A done canary waits for the operator's resume.
    let done = insert_campaign(&db.pool, canary(52)).await?;
    pending_on(&db, done, None, 1).await?;
    let leased = lease(&db, done).await?;
    assert!(finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?);
    assert_eq!(item_counts(&db.pool, done).await?.done, 1);
    assert!(!complete_if_drained(&db.pool, done).await?);
    // One whose subject failed, under its budget, completes like a rolling
    // campaign would, with the failure in its counts.
    let failed = insert_campaign(&db.pool, canary(54)).await?;
    pending_on(&db, failed, None, 1).await?;
    let leased = lease(&db, failed).await?;
    finish_item(
        &db.pool,
        &leased[0],
        &ItemOutcome::Failed(ItemError::new("boot failed")),
    )
    .await?;
    assert!(complete_if_drained(&db.pool, failed).await?);
    assert_eq!(item_counts(&db.pool, failed).await?.failed, 1);

    for (campaign, phase) in [
        (empty, CampaignPhase::Completed),
        (skipped, CampaignPhase::Completed),
        (done, CampaignPhase::Canary),
        (failed, CampaignPhase::Completed),
    ] {
        assert_eq!(
            get_record(&db.pool, campaign)
                .await?
                .expect("exists")
                .campaign
                .phase,
            phase
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_locked_read_of_a_source_holds_off_a_create_until_its_transaction_ends() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let campaign = new_campaign(53, CampaignKind::Templates);
    let mut reader = db.pool.begin().await?;
    assert_eq!(
        unfinished_from_locked(&mut reader, CampaignKind::Templates, campaign.from).await?,
        None
    );
    let create = create_campaign(&db.pool, &campaign);
    tokio::pin!(create);
    // The create blocks on the lock the reader's transaction holds.
    tokio::select! {
        created = &mut create => panic!("the create didn't wait for the reader: {created:?}"),
        waited = wait_for_lock_waiter(&db.pool) => waited?,
    }
    reader.commit().await?;
    create.await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_budget_pause_waiting_on_a_retry_counts_the_failures_after_it() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    fail_items(&db, campaign, 3).await?;
    let budget = get_record(&db.pool, campaign)
        .await?
        .expect("exists")
        .campaign
        .failure_budget;
    assert_eq!(item_counts(&db.pool, campaign).await?.failed, 3);
    assert!(
        3 >= budget,
        "the failures alone reach the budget of {budget}"
    );
    // An operator's retry holds the campaign row while it returns the
    // failures to pending, as a resume with retry_failed does. The UPDATE
    // changes nothing; it's there for the row lock it takes.
    let mut retry = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = phase WHERE id = $1"
    ))
    .bind(campaign)
    .execute(&mut *retry)
    .await?;
    let retried = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items SET phase = 'pending', attempts = 0
         WHERE campaign_id = $1 AND phase = 'failed'"
    ))
    .bind(campaign)
    .execute(&mut *retry)
    .await?
    .rows_affected();
    assert_eq!(retried, 3, "the retry returns every failure");

    let pause = pause_if_over_budget(&db.pool, campaign);
    tokio::pin!(pause);
    tokio::select! {
        paused = &mut pause => panic!("the pause didn't wait for the retry: {paused:?}"),
        waited = wait_for_lock_waiter(&db.pool) => waited?,
    }
    retry.commit().await?;

    assert!(
        !pause.await?,
        "counted after the retry, the failures are gone"
    );
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Rolling);
    Ok(())
}
