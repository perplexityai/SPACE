//! Campaign kinds: subject-kind routing, cross-kind sandbox handoff, and the
//! kinds that run without calling a node (`templates` and
//! `sandbox_memory_drops`).

use super::*;

fn templates(seed: CampaignSeed) -> CampaignSeed {
    CampaignSeed {
        kind: CampaignKind::Templates,
        ..seed
    }
}

fn force_restarts(seed: CampaignSeed) -> CampaignSeed {
    CampaignSeed {
        kind: CampaignKind::SandboxForceRestarts,
        ..seed
    }
}

fn memory_drops(seed: CampaignSeed) -> CampaignSeed {
    CampaignSeed {
        kind: CampaignKind::SandboxMemoryDrops,
        ..seed
    }
}

/// `count` template release ids recorded as pending items of `campaign`.
async fn pending_releases(
    db: &TestDb,
    campaign: CampaignId,
    count: usize,
) -> TestResult<Vec<Uuid>> {
    let releases: Vec<Uuid> = (0..count).map(|_| Uuid::now_v7()).collect();
    let recorded = insert_pending(
        &db.pool,
        campaign,
        SubjectKind::TemplateRelease,
        &releases,
        MAX_ATTEMPTS,
    )
    .await?;
    assert_eq!(recorded, count as u64);
    Ok(releases)
}

#[tokio::test(flavor = "current_thread")]
async fn items_go_only_into_campaigns_of_their_subject_kind() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandboxes = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let restarts = insert_campaign(&db.pool, force_restarts(CampaignSeed::default())).await?;
    let drops = insert_campaign(&db.pool, memory_drops(CampaignSeed::default())).await?;
    let releases = insert_campaign(&db.pool, templates(CampaignSeed::default())).await?;
    let subjects = [Uuid::now_v7()];

    for sandbox_campaign in [sandboxes, restarts, drops] {
        assert_eq!(
            insert_pending(
                &db.pool,
                sandbox_campaign,
                SubjectKind::TemplateRelease,
                &subjects,
                MAX_ATTEMPTS
            )
            .await?,
            0
        );
    }
    for sandbox_campaign in [restarts, drops] {
        assert_eq!(
            insert_pending(
                &db.pool,
                sandbox_campaign,
                SubjectKind::Sandbox,
                &subjects,
                MAX_ATTEMPTS
            )
            .await?,
            1
        );
    }
    assert_eq!(
        insert_pending(
            &db.pool,
            releases,
            SubjectKind::Sandbox,
            &subjects,
            MAX_ATTEMPTS
        )
        .await?,
        0
    );
    assert_eq!(
        insert_pending(
            &db.pool,
            releases,
            SubjectKind::TemplateRelease,
            &subjects,
            MAX_ATTEMPTS
        )
        .await?,
        1
    );
    let kind = sqlx::query_scalar::<_, String>(sql!(
        "SELECT subject_kind FROM runtime_upgrade_items WHERE campaign_id = $1"
    ))
    .bind(releases)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(kind, "template_release");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_items_are_paced_by_the_cluster_cap_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // A per-node cap of 1 would hold a sandbox campaign to one item per
    // node; template releases have no node, so only the cluster cap counts.
    let campaign = insert_campaign(
        &db.pool,
        templates(CampaignSeed {
            per_node: 1,
            per_cluster: 3,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    let releases = pending_releases(&db, campaign, 5).await?;

    let first = lease(&db, campaign).await?;
    assert_eq!(first.len(), 3);
    assert!(first.iter().all(|item| item.node_id.is_none()
        && item.subject_kind == SubjectKind::TemplateRelease));
    assert_eq!(
        first.iter().map(|item| item.subject_id).collect::<Vec<_>>(),
        releases[..3]
    );
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "the cluster cap is full"
    );

    finish_item(&db.pool, &first[0], &ItemOutcome::Done).await?;
    let second = lease(&db, campaign).await?;
    assert_eq!(
        second
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [releases[3]]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_template_item_renews_finishes_and_redrives_under_its_own_kind() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, templates(CampaignSeed::default())).await?;
    pending_releases(&db, campaign, 2).await?;
    let leased = lease(&db, campaign).await?;
    assert_eq!(leased.len(), 2);

    assert!(renew_lease(&db.pool, &leased[0], LEASE).await?);
    assert!(finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?);
    lapse_leases(&db.pool, campaign).await?;
    assert_eq!(
        redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await?,
        RedriveReport {
            requeued: 1,
            failed: 0,
        }
    );
    assert!(!finish_item(&db.pool, &leased[1], &ItemOutcome::Done).await?);
    assert_eq!(
        item_counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 1,
            done: 1,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_template_canary_leases_one_release_and_waits() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        templates(CampaignSeed {
            phase: CampaignPhase::Canary,
            per_cluster: 5,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    pending_releases(&db, campaign, 3).await?;
    let canary = lease(&db, campaign).await?;
    assert_eq!(canary.len(), 1);
    assert!(lease(&db, campaign).await?.is_empty());
    finish_item(&db.pool, &canary[0], &ItemOutcome::Done).await?;
    assert!(lease(&db, campaign).await?.is_empty(), "the canary waits");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completion_ignores_skipped_items_and_raises_the_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, templates(CampaignSeed::default())).await?;
    pending_releases(&db, campaign, 2).await?;
    let leased = lease(&db, campaign).await?;
    finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?;
    finish_item(
        &db.pool,
        &leased[1],
        &ItemOutcome::Skipped(ItemError::new("the release was deleted")),
    )
    .await?;

    assert!(complete_if_drained(&db.pool, campaign).await?);
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Completed);
    assert_eq!(record.campaign.generation, 1);
    assert!(record.finished_at.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn memory_drop_items_lease_when_their_last_publishing_node_is_unreachable() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let unreachable = NodeId::new();
    insert_ready_worker_node(&db.pool, unreachable).await?;
    upsert_snapshot(
        &db.pool,
        unreachable,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;

    let drop_campaign = insert_campaign(&db.pool, memory_drops(CampaignSeed::default())).await?;
    let restart_campaign =
        insert_campaign(&db.pool, force_restarts(CampaignSeed::default())).await?;
    let dropped = insert_sandbox(
        &db.pool,
        SubstrateKind::Kubernetes,
        SandboxLifecycle::Suspended,
        Some(unreachable),
    )
    .await?;
    let restarted = insert_sandbox(
        &db.pool,
        SubstrateKind::Kubernetes,
        SandboxLifecycle::Suspended,
        Some(unreachable),
    )
    .await?;
    insert_pending(
        &db.pool,
        drop_campaign,
        SubjectKind::Sandbox,
        &[dropped],
        MAX_ATTEMPTS,
    )
    .await?;
    insert_pending(
        &db.pool,
        restart_campaign,
        SubjectKind::Sandbox,
        &[restarted],
        MAX_ATTEMPTS,
    )
    .await?;

    assert!(
        lease(&db, restart_campaign).await?.is_empty(),
        "node-paced sandbox kinds wait for a reachable publishing node"
    );
    let leased = lease(&db, drop_campaign).await?;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].subject_id, dropped);
    assert_eq!(leased[0].subject_kind, SubjectKind::Sandbox);
    assert_eq!(leased[0].node_id, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn memory_drop_items_ignore_per_node_cap_and_pace_by_cluster_cap() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let live = node(&db.pool).await?;
    let campaign = insert_campaign(
        &db.pool,
        memory_drops(CampaignSeed {
            per_node: 1,
            per_cluster: 3,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    let mut sandboxes = Vec::new();
    for _ in 0..4 {
        sandboxes.push(
            insert_sandbox(
                &db.pool,
                SubstrateKind::Kubernetes,
                SandboxLifecycle::Suspended,
                Some(live),
            )
            .await?,
        );
    }
    insert_pending(
        &db.pool,
        campaign,
        SubjectKind::Sandbox,
        &sandboxes,
        MAX_ATTEMPTS,
    )
    .await?;

    let first = lease(&db, campaign).await?;
    assert_eq!(first.len(), 3);
    assert!(
        first
            .iter()
            .all(|item| item.node_id.is_none() && item.subject_kind == SubjectKind::Sandbox)
    );
    assert_eq!(
        first.iter().map(|item| item.subject_id).collect::<Vec<_>>(),
        sandboxes[..3]
    );
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "the cluster cap is full"
    );

    finish_item(&db.pool, &first[0], &ItemOutcome::Done).await?;
    let second = lease(&db, campaign).await?;
    assert_eq!(
        second
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [sandboxes[3]]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_in_flight_is_shared_between_force_restarts_and_memory_drops() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let live = node(&db.pool).await?;
    let s1 = insert_sandbox(
        &db.pool,
        SubstrateKind::Kubernetes,
        SandboxLifecycle::Suspended,
        Some(live),
    )
    .await?;
    let s2 = insert_sandbox(
        &db.pool,
        SubstrateKind::Kubernetes,
        SandboxLifecycle::Suspended,
        Some(live),
    )
    .await?;

    let restarts = insert_campaign(
        &db.pool,
        force_restarts(CampaignSeed {
            per_node: 1,
            per_cluster: 1,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    let drops = insert_campaign(
        &db.pool,
        memory_drops(CampaignSeed {
            per_node: 1,
            per_cluster: 2,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    for campaign in [restarts, drops] {
        insert_pending(
            &db.pool,
            campaign,
            SubjectKind::Sandbox,
            &[s1, s2],
            MAX_ATTEMPTS,
        )
        .await?;
    }

    let restart_claim = lease(&db, restarts).await?;
    assert_eq!(
        restart_claim
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [s1]
    );

    let drop_claim = lease(&db, drops).await?;
    assert_eq!(
        drop_claim
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [s2],
        "s1 is held in flight by sandbox_force_restarts"
    );

    finish_item(&db.pool, &restart_claim[0], &ItemOutcome::Done).await?;
    assert!(
        lease(&db, restarts).await?.is_empty(),
        "s2 is held in flight by sandbox_memory_drops"
    );

    let drop_next = lease(&db, drops).await?;
    assert_eq!(
        drop_next
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [s1]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn expired_final_leases_salvage_done_defer_transient_and_respect_renewals() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        memory_drops(CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    let s_retryable = Uuid::now_v7();
    let s_renewed = Uuid::now_v7();
    let s_salvaged = Uuid::now_v7();
    let s_deferred = Uuid::now_v7();
    insert_pending(
        &db.pool,
        campaign,
        SubjectKind::Sandbox,
        &[s_retryable, s_renewed, s_salvaged, s_deferred],
        MAX_ATTEMPTS,
    )
    .await?;

    let claimed = lease(&db, campaign).await?;
    assert_eq!(claimed.len(), 4);

    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET attempts = CASE
                 WHEN subject_id = $2 THEN $3
                 ELSE $4
             END,
             last_error = 'prior timeout',
             lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .bind(s_retryable)
    .bind((MAX_ATTEMPTS - 1) as i64)
    .bind(MAX_ATTEMPTS as i64)
    .execute(&db.pool)
    .await?;

    let candidates = expired_final_leases(&db.pool, campaign, MAX_ATTEMPTS).await?;
    assert_eq!(
        candidates
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [s_renewed, s_salvaged, s_deferred],
        "only expired leases at or past min_attempts are returned"
    );
    let find_candidate = |subject: Uuid| {
        candidates
            .iter()
            .find(|item| item.subject_id == subject)
            .cloned()
            .expect("expired final lease candidate")
    };

    // Simulate a concurrent lease extension landing before the redrive helpers
    // execute so lease_expires_at > clock_timestamp() fences every write.
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() + INTERVAL '10 minutes'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(s_renewed)
    .execute(&db.pool)
    .await?;
    assert!(
        !finish_expired_done(&db.pool, &find_candidate(s_renewed)).await?,
        "renewed lease must not be overwritten as done"
    );
    assert_eq!(
        bump_expired_final_attempt(&db.pool, &find_candidate(s_renewed)).await?,
        MAX_ATTEMPTS,
        "renewed lease must not have attempts bumped"
    );

    // Committed expired final lease salvages to done and clears last_error.
    assert!(finish_expired_done(&db.pool, &find_candidate(s_salvaged)).await?);
    let salvaged_row: (String, Option<String>, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT phase, last_error, lease_token
         FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(s_salvaged)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(salvaged_row, ("done".to_owned(), None, None));

    // Transient completion-check failure bumps attempts past MAX_ATTEMPTS while
    // keeping the row in_flight + expired; redrive_expired_checked spares
    // s_deferred when omitted from fail_lease_tokens (and spares renewed
    // s_renewed even if its token is passed) while requeuing s_retryable.
    let deferred_candidate = find_candidate(s_deferred);
    assert_eq!(
        bump_expired_final_attempt(&db.pool, &deferred_candidate).await?,
        MAX_ATTEMPTS + 1
    );
    let redriven = redrive_expired_checked(
        &db.pool,
        campaign,
        MAX_ATTEMPTS,
        &[find_candidate(s_renewed).lease_token],
    )
    .await?;
    assert_eq!(redriven.requeued, 1, "s_retryable returns to pending");
    assert_eq!(
        redriven.failed, 0,
        "s_deferred and renewed s_renewed are spared"
    );
    assert_eq!(
        item_phase(&db.pool, campaign, s_renewed).await?,
        "in_flight"
    );
    assert_eq!(
        item_phase(&db.pool, campaign, s_retryable).await?,
        "pending"
    );
    assert_eq!(
        item_phase(&db.pool, campaign, s_deferred).await?,
        "in_flight"
    );

    // Once the caller passes s_deferred's lease_token (e.g. uncommitted or cap
    // reached), redrive_expired_checked fails it.
    let capped = redrive_expired_checked(
        &db.pool,
        campaign,
        MAX_ATTEMPTS,
        &[deferred_candidate.lease_token],
    )
    .await?;
    assert_eq!((capped.requeued, capped.failed), (0, 1));
    assert_eq!(item_phase(&db.pool, campaign, s_deferred).await?, "failed");
    Ok(())
}
