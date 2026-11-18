//! Campaigns of a kind outside [`CampaignKind::RUNTIME`] beside runtime
//! ones: the loop neither trips over them nor changes them. The store's
//! tests cover each scoped read and verb.

use pplx_space_runtime_upgrade_campaign_store::test_support::insert_foreign_campaign;

use super::*;

/// A campaign row and its items, read raw.
async fn snapshot(pool: &PgPool, campaign: CampaignId) -> TestResult<(String, Vec<String>)> {
    let row = sqlx::query_scalar::<_, String>(sql!(
        "SELECT row_to_json(c)::text FROM runtime_upgrade_campaigns c WHERE id = $1"
    ))
    .bind(campaign)
    .fetch_one(pool)
    .await?;
    let items = sqlx::query_scalar::<_, String>(sql!(
        "SELECT row_to_json(i)::text FROM runtime_upgrade_items i
         WHERE campaign_id = $1
         ORDER BY subject_id"
    ))
    .bind(campaign)
    .fetch_all(pool)
    .await?;
    Ok((row, items))
}

/// Insert an item of `campaign` for a fresh subject of `subject_kind`, in
/// flight under a lease that lapsed a minute ago.
async fn insert_lapsed_item(
    pool: &PgPool,
    campaign: CampaignId,
    subject_kind: &str,
) -> TestResult<Uuid> {
    let subject = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
             lease_expires_at
         )
         VALUES ($1, $2, $3, 'in_flight', 1, $4, now() - interval '1 minute')"
    ))
    .bind(campaign)
    .bind(subject_kind)
    .bind(subject)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(subject)
}

#[tokio::test(flavor = "current_thread")]
async fn the_loop_releases_lapsed_leases_of_aborted_runtime_campaigns_only() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let aborted = CampaignSeed {
        phase: CampaignPhase::Aborted,
        ..seed()
    };
    let runtime = insert_campaign(&db.pool, aborted.clone()).await?;
    let subject = insert_lapsed_item(&db.pool, runtime, "template_release").await?;
    let foreign = insert_foreign_campaign(&db.pool, aborted).await?;
    insert_lapsed_item(&db.pool, foreign, "test_foreign_subject").await?;
    // An unfinished foreign campaign is all the loop's list would find, if
    // it saw foreign campaigns at all.
    let unfinished_foreign = insert_foreign_campaign(&db.pool, seed()).await?;
    let foreign_before = snapshot(&db.pool, foreign).await?;
    let unfinished_before = snapshot(&db.pool, unfinished_foreign).await?;
    let driver = FakeDriver::default();
    let reconciler = reconciler(&db.pool, driver.clone());

    // A tick whose list fails returns before the sweep, so the released
    // runtime item shows the foreign-only list came back empty, not failed.
    assert_eq!(tick(&reconciler).await, TickReport::default());
    assert_eq!(
        item(&db.pool, runtime, subject).await?,
        (
            "pending".to_owned(),
            Some("the lease expired before an outcome was recorded".to_owned()),
            1
        )
    );
    assert_eq!(snapshot(&db.pool, foreign).await?, foreign_before);
    assert_eq!(
        snapshot(&db.pool, unfinished_foreign).await?,
        unfinished_before
    );
    assert!(driver.state().finds.is_empty(), "nothing to drive");
    Ok(())
}
