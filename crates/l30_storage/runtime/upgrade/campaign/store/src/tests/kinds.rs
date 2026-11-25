//! The verbs over a campaign kind whose subjects have no node: template
//! releases.

use super::*;

fn templates(seed: CampaignSeed) -> CampaignSeed {
    CampaignSeed {
        kind: CampaignKind::Templates,
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
    let force_restarts = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::SandboxForceRestarts,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let releases = insert_campaign(&db.pool, templates(CampaignSeed::default())).await?;
    let subjects = [Uuid::now_v7()];

    for sandbox_campaign in [sandboxes, force_restarts] {
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
    assert_eq!(
        insert_pending(
            &db.pool,
            force_restarts,
            SubjectKind::Sandbox,
            &subjects,
            MAX_ATTEMPTS
        )
        .await?,
        1
    );
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
