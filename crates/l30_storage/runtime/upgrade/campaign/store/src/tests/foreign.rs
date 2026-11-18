//! Campaigns of a kind outside [`CampaignKind::RUNTIME`] sharing the table:
//! the reads, the operator's phase changes and the finished-campaign sweep
//! neither see nor change them.

use time::OffsetDateTime;

use super::test_support::insert_foreign_campaign;
use super::*;

/// The subject kind of a foreign campaign's items.
const FOREIGN_SUBJECT: &str = "test_foreign_subject";

fn change(expected_generation: Option<u64>) -> PhaseChange {
    PhaseChange {
        actor: Some(pplx_space_accounts::UserId::from_uuid(SEED_USER_ID)),
        expected_generation,
    }
}

/// A campaign row's phase, generation, writer and timestamps, read raw.
type CampaignRaw = (
    String,
    i64,
    Option<Uuid>,
    Option<OffsetDateTime>,
    OffsetDateTime,
);

async fn campaign_raw(pool: &PgPool, campaign: CampaignId) -> TestResult<CampaignRaw> {
    Ok(sqlx::query_as::<_, CampaignRaw>(sql!(
        "SELECT phase, generation, updated_by_user_id, finished_at, updated_at
         FROM runtime_upgrade_campaigns
         WHERE id = $1"
    ))
    .bind(campaign)
    .fetch_one(pool)
    .await?)
}

/// An item row's subject, phase, attempts, error and lease, read raw.
type ItemRaw = (
    Uuid,
    String,
    i64,
    Option<String>,
    Option<Uuid>,
    Option<OffsetDateTime>,
    OffsetDateTime,
);

async fn items_raw(pool: &PgPool, campaign: CampaignId) -> TestResult<Vec<ItemRaw>> {
    Ok(sqlx::query_as::<_, ItemRaw>(sql!(
        "SELECT subject_id, phase, attempts, last_error, lease_token, lease_expires_at,
                updated_at
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
         ORDER BY subject_id"
    ))
    .bind(campaign)
    .fetch_all(pool)
    .await?)
}

/// Insert an item of `campaign` for a fresh subject of `subject_kind` in
/// `phase`, with one attempt; an in-flight one holds a lease that lapsed a
/// minute ago.
async fn insert_item(
    pool: &PgPool,
    campaign: CampaignId,
    subject_kind: &str,
    phase: ItemPhase,
) -> TestResult<Uuid> {
    let subject = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
             lease_expires_at
         )
         VALUES ($1, $2, $3, $4, 1,
                 CASE WHEN $5 THEN $6::uuid END,
                 CASE WHEN $5 THEN now() - interval '1 minute' END)"
    ))
    .bind(campaign)
    .bind(subject_kind)
    .bind(subject)
    .bind(phase.as_str())
    .bind(phase == ItemPhase::InFlight)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(subject)
}

fn ids(campaigns: &[CampaignRecord]) -> Vec<CampaignId> {
    campaigns.iter().map(|record| record.campaign.id).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn unfinished_lists_only_runtime_campaigns() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    insert_foreign_campaign(&db.pool, CampaignSeed::default()).await?;
    insert_foreign_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Paused,
            ..CampaignSeed::default()
        },
    )
    .await?;
    assert_eq!(
        list_unfinished(&db.pool).await?,
        Vec::new(),
        "foreign campaigns alone list as none, not as an error"
    );

    let templates = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::Templates,
            ..CampaignSeed::default()
        },
    )
    .await?;
    insert_foreign_campaign(&db.pool, CampaignSeed::default()).await?;
    let sandboxes = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    assert_eq!(
        list_unfinished(&db.pool)
            .await?
            .iter()
            .map(|campaign| campaign.id)
            .collect::<Vec<_>>(),
        [templates, sandboxes]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn records_page_past_foreign_campaigns() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // Each a millisecond or more after the last, so their UUIDv7 ids sort in
    // this order: runtime and foreign campaigns interleaved, finished and not.
    let mut inserted = Vec::new();
    for (byte, foreign, phase) in [
        (11, false, CampaignPhase::Rolling),
        (12, true, CampaignPhase::Rolling),
        (13, false, CampaignPhase::Aborted),
        (14, true, CampaignPhase::Aborted),
        (15, false, CampaignPhase::Paused),
    ] {
        tokio::time::sleep(Duration::from_millis(2)).await;
        let seed = CampaignSeed {
            from: environment(byte),
            kind: CampaignKind::Templates,
            phase,
            ..CampaignSeed::default()
        };
        inserted.push(if foreign {
            insert_foreign_campaign(&db.pool, seed).await?
        } else {
            insert_campaign(&db.pool, seed).await?
        });
    }
    assert!(inserted.is_sorted(), "ids follow insertion: {inserted:?}");
    let [rolling, foreign_rolling, aborted, foreign_aborted, paused] = inserted[..] else {
        unreachable!()
    };

    // A page of one at a time returns each runtime campaign once, in order,
    // with no empty page where a foreign one sits.
    for (unfinished_only, expected) in [
        (false, vec![rolling, aborted, paused]),
        (true, vec![rolling, paused]),
    ] {
        let mut after = None;
        let mut paged = Vec::new();
        loop {
            let page = list_records(&db.pool, unfinished_only, after, 1).await?;
            let Some(record) = page.first() else {
                break;
            };
            assert_eq!(page.len(), 1);
            paged.push(record.campaign.id);
            after = Some(record.campaign.id);
        }
        assert_eq!(paged, expected, "unfinished_only {unfinished_only}");
        assert_eq!(
            ids(&list_records(&db.pool, unfinished_only, None, 2).await?),
            expected[..2],
            "a page fills past foreign campaigns"
        );
        assert_eq!(
            ids(&list_records(&db.pool, unfinished_only, None, 10).await?),
            expected
        );
    }

    for foreign in [foreign_rolling, foreign_aborted] {
        assert_eq!(get_record(&db.pool, foreign).await?, None);
    }
    assert_eq!(
        get_record(&db.pool, rolling)
            .await?
            .map(|record| record.campaign.kind),
        Some(CampaignKind::Templates)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_malformed_runtime_campaign_still_fails_every_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    insert_foreign_campaign(&db.pool, CampaignSeed::default()).await?;
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'warming' WHERE id = $1"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;

    let unknown_phase = |error: ReadCampaignError| {
        assert!(
            matches!(&error, ReadCampaignError::UnknownValue { campaign_id, .. } if *campaign_id == campaign),
            "{error:?}"
        );
    };
    unknown_phase(
        list_unfinished(&db.pool)
            .await
            .expect_err("list_unfinished"),
    );
    for unfinished_only in [false, true] {
        unknown_phase(
            list_records(&db.pool, unfinished_only, None, 10)
                .await
                .expect_err("list_records"),
        );
    }
    unknown_phase(
        get_record(&db.pool, campaign)
            .await
            .expect_err("get_record"),
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn operator_changes_find_no_foreign_campaign_and_change_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // Each verb against a campaign in a phase that allows it: a rolling one
    // pauses and aborts, and a paused one with a done item and failed items
    // at its budget resumes when they're retried.
    let rolling = CampaignSeed::default();
    let paused = CampaignSeed {
        phase: CampaignPhase::Paused,
        failure_budget: 1,
        ..CampaignSeed::default()
    };
    let absent = CampaignId(Uuid::now_v7());
    for verb in ["pause", "resume", "abort"] {
        let seed = if verb == "resume" {
            paused.clone()
        } else {
            rolling.clone()
        };
        let foreign = insert_foreign_campaign(&db.pool, seed.clone()).await?;
        let runtime = insert_campaign(&db.pool, seed).await?;
        for (campaign, subject_kind) in [(foreign, FOREIGN_SUBJECT), (runtime, "sandbox")] {
            insert_item(&db.pool, campaign, subject_kind, ItemPhase::Done).await?;
            insert_item(&db.pool, campaign, subject_kind, ItemPhase::Failed).await?;
            insert_item(&db.pool, campaign, subject_kind, ItemPhase::InFlight).await?;
        }
        let apply = |campaign| {
            let pool = db.pool.clone();
            async move {
                let change = change(Some(0));
                match verb {
                    "pause" => pause_campaign(&pool, campaign, &change).await,
                    "resume" => resume_campaign(&pool, campaign, &change, true).await,
                    _ => abort_campaign(&pool, campaign, &change).await,
                }
            }
        };

        let campaign_before = campaign_raw(&db.pool, foreign).await?;
        let items_before = items_raw(&db.pool, foreign).await?;
        assert!(
            matches!(apply(absent).await, Err(ChangePhaseError::NotFound)),
            "{verb} an absent campaign"
        );
        let refused = apply(foreign).await;
        assert!(
            matches!(refused, Err(ChangePhaseError::NotFound)),
            "{verb} a foreign campaign: {refused:?}"
        );
        assert_eq!(campaign_raw(&db.pool, foreign).await?, campaign_before);
        assert_eq!(items_raw(&db.pool, foreign).await?, items_before);

        // The same change applies to a runtime campaign in the same state.
        let applied = apply(runtime).await?;
        assert_eq!(applied.generation, 1, "{verb}");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_finished_sweep_releases_only_runtime_items() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let aborted = CampaignSeed {
        phase: CampaignPhase::Aborted,
        ..CampaignSeed::default()
    };
    let runtime = insert_campaign(&db.pool, aborted.clone()).await?;
    let foreign = insert_foreign_campaign(&db.pool, aborted).await?;
    let subject = insert_item(&db.pool, runtime, "sandbox", ItemPhase::InFlight).await?;
    insert_item(&db.pool, foreign, FOREIGN_SUBJECT, ItemPhase::InFlight).await?;
    let foreign_before = items_raw(&db.pool, foreign).await?;

    assert_eq!(release_lapsed_in_finished(&db.pool).await?, 1);
    assert_eq!(item_phase(&db.pool, runtime, subject).await?, "pending");
    assert_eq!(items_raw(&db.pool, foreign).await?, foreign_before);
    assert_eq!(foreign_before[0].1, "in_flight");
    Ok(())
}
