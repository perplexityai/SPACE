//! Operator verbs and reads: pause, resume and abort, the generation that
//! guards them, the leases and phase changes they build on, and the records
//! the admin API reports.

use super::*;

fn change(expected_generation: Option<u64>) -> PhaseChange {
    PhaseChange {
        actor: Some(pplx_space_accounts::UserId::from_uuid(SEED_USER_ID)),
        expected_generation,
    }
}

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
    for item in lease(db, campaign).await? {
        finish_item(
            &db.pool,
            &item,
            &ItemOutcome::Failed(ItemError::new("boot failed")),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pause_and_resume_raise_the_generation_and_record_the_actor() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    assert_eq!(generation(&db.pool, campaign).await?, 0);

    let paused = pause_campaign(&db.pool, campaign, &change(Some(0))).await?;
    assert_eq!(
        paused,
        PhaseChanged {
            phase: CampaignPhase::Paused,
            generation: 1,
            retried: 0,
            draining: 0,
        }
    );
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Paused);
    assert_eq!(record.updated_by, change(None).actor);

    // No item is done yet, so a resume goes back to canary.
    let resumed = resume_campaign(&db.pool, campaign, &change(Some(1)), false).await?;
    assert_eq!(resumed.phase, CampaignPhase::Canary);
    assert_eq!(resumed.generation, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stale_generation_is_refused_and_changes_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    // The reconciler's budget pause moved the campaign after an operator
    // read it at generation 0.
    fail_items(&db, campaign, 3).await?;
    assert!(pause_if_over_budget(&db.pool, campaign).await?);
    assert_eq!(generation(&db.pool, campaign).await?, 1);

    let refused = resume_campaign(&db.pool, campaign, &change(Some(0)), true).await;
    assert!(
        matches!(
            refused,
            Err(ChangePhaseError::Stale {
                generation: 1,
                expected: 0
            })
        ),
        "{refused:?}"
    );
    assert_eq!(generation(&db.pool, campaign).await?, 1);
    assert_eq!(
        item_counts(&db.pool, campaign).await?.failed,
        3,
        "nothing retried"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_resume_at_the_failure_budget_needs_the_failed_items_retried() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    fail_items(&db, campaign, 3).await?;
    assert!(pause_if_over_budget(&db.pool, campaign).await?);

    let refused = resume_campaign(&db.pool, campaign, &change(None), false).await;
    assert!(
        matches!(
            refused,
            Err(ChangePhaseError::AtFailureBudget {
                failed: 3,
                failure_budget: 3
            })
        ),
        "{refused:?}"
    );

    let resumed = resume_campaign(&db.pool, campaign, &change(None), true).await?;
    assert_eq!(resumed.retried, 3);
    assert_eq!(resumed.phase, CampaignPhase::Canary);
    let rows = sqlx::query_as::<_, (String, i64, Option<String>)>(sql!(
        "SELECT phase, attempts, last_error FROM runtime_upgrade_items WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert_eq!(
            row,
            ("pending".to_owned(), 0, Some("boot failed".to_owned())),
            "pending afresh, keeping the error until a new outcome"
        );
    }
    // Leasing works again: the budget counts only new failures.
    assert_eq!(lease(&db, campaign).await?.len(), 1, "one canary");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_resume_with_a_done_item_rolls() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let canary = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            ..CampaignSeed::default()
        },
    )
    .await?;
    pending_on(&db, canary, None, 2).await?;

    let refused = resume_campaign(&db.pool, canary, &change(None), false).await;
    assert!(
        matches!(
            refused,
            Err(ChangePhaseError::NotResumable(CampaignPhase::Canary))
        ),
        "a canary with nothing done isn't resumable: {refused:?}"
    );
    let leased = lease(&db, canary).await?;
    finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?;
    let resumed = resume_campaign(&db.pool, canary, &change(None), false).await?;
    assert_eq!(resumed.phase, CampaignPhase::Rolling);

    // Paused and resumed again: an item is done, so it keeps rolling.
    pause_campaign(&db.pool, canary, &change(None)).await?;
    let resumed = resume_campaign(&db.pool, canary, &change(None), false).await?;
    assert_eq!(resumed.phase, CampaignPhase::Rolling);
    assert_eq!(resumed.generation, 3);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn transitions_a_phase_doesnt_allow_are_refused() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rolling = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let paused = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Paused,
            from: environment(5),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let completed = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Completed,
            from: environment(6),
            ..CampaignSeed::default()
        },
    )
    .await?;

    assert!(matches!(
        resume_campaign(&db.pool, rolling, &change(None), false).await,
        Err(ChangePhaseError::NotResumable(CampaignPhase::Rolling))
    ));
    assert!(matches!(
        pause_campaign(&db.pool, paused, &change(None)).await,
        Err(ChangePhaseError::AlreadyPaused)
    ));
    assert!(matches!(
        pause_campaign(&db.pool, completed, &change(None)).await,
        Err(ChangePhaseError::Finished(CampaignPhase::Completed))
    ));
    assert!(matches!(
        resume_campaign(&db.pool, completed, &change(None), true).await,
        Err(ChangePhaseError::Finished(CampaignPhase::Completed))
    ));
    assert!(matches!(
        pause_campaign(&db.pool, CampaignId(Uuid::now_v7()), &change(None)).await,
        Err(ChangePhaseError::NotFound)
    ));
    // A refused transition wins over a stale generation, so the caller
    // learns the phase.
    assert!(matches!(
        pause_campaign(&db.pool, paused, &change(Some(9))).await,
        Err(ChangePhaseError::AlreadyPaused)
    ));
    for campaign in [rolling, paused, completed] {
        assert_eq!(generation(&db.pool, campaign).await?, 0);
    }
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
    assert!(renew_lease(&db.pool, &leased[0], LEASE).await?);
    finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?;
    lapse_leases(&db.pool, campaign).await?;
    redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await?;
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
async fn an_abort_ends_the_campaign_and_fences_its_leases() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = create_campaign(&db.pool, &new_campaign(40, CampaignKind::Sandboxes)).await?;
    let subjects = pending_on(&db, campaign, None, 4).await?;
    let leased = lease(&db, campaign).await?;
    assert_eq!(leased.len(), 4);
    finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?;

    let aborted = abort_campaign(&db.pool, campaign, &change(Some(0))).await?;
    assert_eq!(
        aborted,
        PhaseChanged {
            phase: CampaignPhase::Aborted,
            generation: 1,
            retried: 0,
            draining: 3,
        }
    );
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Aborted);
    assert!(record.finished_at.is_some());
    assert_eq!(record.updated_by, change(None).actor);
    // What was done stays done.
    assert_eq!(item_counts(&db.pool, campaign).await?.done, 1);
    // The workers can't renew a finished campaign's lease, so they stop at
    // their next renewal. One that finishes first still records the truth.
    for item in &leased[1..] {
        assert!(!renew_lease(&db.pool, item, LEASE).await?);
    }
    assert!(finish_item(&db.pool, &leased[1], &ItemOutcome::Done).await?);
    // Nothing leases, records or completes the campaign again.
    assert!(lease(&db, campaign).await?.is_empty());
    assert_eq!(
        insert_pending(
            &db.pool,
            campaign,
            SubjectKind::Sandbox,
            &subjects,
            MAX_ATTEMPTS
        )
        .await?,
        0
    );
    assert!(!complete_if_drained(&db.pool, campaign).await?);
    assert!(!pause_if_over_budget(&db.pool, campaign).await?);
    assert!(list_unfinished(&db.pool).await?.is_empty());

    // Once the leases lapse, the loop's sweep returns the items to pending.
    assert_eq!(release_lapsed_in_finished(&db.pool).await?, 0, "still live");
    lapse_leases(&db.pool, campaign).await?;
    assert_eq!(release_lapsed_in_finished(&db.pool).await?, 2);
    assert_eq!(
        item_counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 2,
            done: 2,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

/// The abort caveat: an aborted campaign's worker may still be mid-upgrade
/// when a new campaign for the same source starts. The new campaign doesn't
/// lease that subject while the aborted campaign's lease lives, so the two
/// never hold it at once.
#[tokio::test(flavor = "current_thread")]
async fn a_subject_an_aborted_campaign_holds_isnt_leased_to_a_new_campaign() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let first = create_campaign(&db.pool, &new_campaign(44, CampaignKind::Sandboxes)).await?;
    let subjects = pending_on(&db, first, None, 1).await?;
    let held = lease(&db, first).await?;
    assert_eq!(held.len(), 1);
    abort_campaign(&db.pool, first, &change(None)).await?;

    // A new campaign starts at once and records the same subject, plus one
    // more that nobody holds.
    let second = create_campaign(&db.pool, &new_campaign(44, CampaignKind::Sandboxes)).await?;
    let free = Uuid::now_v7();
    let both = [subjects[0], free];
    insert_pending(&db.pool, second, SubjectKind::Sandbox, &both, MAX_ATTEMPTS).await?;
    let leased = lease(&db, second).await?;
    assert_eq!(
        leased
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [free],
        "the held subject waits for the aborted campaign's lease"
    );
    assert_eq!(item_phase(&db.pool, second, subjects[0]).await?, "pending");

    // The old worker can't extend its hold past the lease it has.
    assert!(!renew_lease(&db.pool, &held[0], LEASE).await?);
    lapse_leases(&db.pool, first).await?;
    let leased = lease(&db, second).await?;
    assert_eq!(
        leased
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [subjects[0]]
    );
    // And the aborted worker, too late, records its outcome only on its own
    // campaign's item, not over the new holder's.
    assert!(
        finish_item(&db.pool, &held[0], &ItemOutcome::Done).await?,
        "its own item, which nothing re-drove yet, takes the outcome"
    );
    assert_eq!(
        item_phase(&db.pool, second, subjects[0]).await?,
        "in_flight"
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
    abort_campaign(&db.pool, aborted, &change(None)).await?;

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
async fn a_paused_or_canary_campaign_aborts_and_a_finished_one_refuses() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (byte, phase) in [(41, CampaignPhase::Paused), (42, CampaignPhase::Canary)] {
        let campaign = insert_campaign(
            &db.pool,
            CampaignSeed {
                phase,
                from: environment(byte),
                ..CampaignSeed::default()
            },
        )
        .await?;
        let aborted = abort_campaign(&db.pool, campaign, &change(None)).await?;
        assert_eq!(aborted.phase, CampaignPhase::Aborted, "{phase}");
        assert!(matches!(
            abort_campaign(&db.pool, campaign, &change(None)).await,
            Err(ChangePhaseError::Finished(CampaignPhase::Aborted))
        ));
        assert!(matches!(
            resume_campaign(&db.pool, campaign, &change(None), true).await,
            Err(ChangePhaseError::Finished(CampaignPhase::Aborted))
        ));
    }
    let completed = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Completed,
            from: environment(43),
            ..CampaignSeed::default()
        },
    )
    .await?;
    assert!(matches!(
        abort_campaign(&db.pool, completed, &change(None)).await,
        Err(ChangePhaseError::Finished(CampaignPhase::Completed))
    ));
    assert!(matches!(
        abort_campaign(&db.pool, CampaignId(Uuid::now_v7()), &change(None)).await,
        Err(ChangePhaseError::NotFound)
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_abort_at_a_stale_generation_changes_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    pending_on(&db, campaign, None, 1).await?;
    let leased = lease(&db, campaign).await?;
    pause_campaign(&db.pool, campaign, &change(None)).await?;

    let refused = abort_campaign(&db.pool, campaign, &change(Some(0))).await;
    assert!(
        matches!(
            refused,
            Err(ChangePhaseError::Stale {
                generation: 1,
                expected: 0
            })
        ),
        "{refused:?}"
    );
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Paused);
    assert_eq!(record.finished_at, None);
    assert!(
        renew_lease(&db.pool, &leased[0], LEASE).await?,
        "the lease is untouched"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_abort_inside_a_transaction_older_than_the_campaign_succeeds() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(4).await?
    else {
        return Ok(());
    };
    // The caller's transaction starts first, so its now() is from before the
    // campaign's created_at.
    let mut caller = db.pool.begin().await?;
    sqlx::query(sql!("SELECT now()"))
        .execute(&mut *caller)
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;

    let aborted = abort_campaign(&mut *caller, campaign, &change(None)).await?;
    assert_eq!(aborted.phase, CampaignPhase::Aborted);
    caller.commit().await?;
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Aborted);
    assert!(record.finished_at.expect("finished") >= record.created_at);
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
    finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?;
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
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items SET phase = 'pending', attempts = 0
         WHERE campaign_id = $1 AND phase = 'failed'"
    ))
    .bind(campaign)
    .execute(&mut *retry)
    .await?;

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
