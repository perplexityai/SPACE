//! A campaign of a kind outside [`CampaignKind::RUNTIME`] beside runtime
//! campaigns: the loop neither trips over it nor changes it.

use pplx_space_runtime_upgrade_campaign_store::test_support::insert_foreign_campaign;

use super::*;

/// A driver that records the campaigns it's asked to plan and holds each.
#[derive(Clone)]
struct RecordingDriver {
    kind: CampaignKind,
    planned: Arc<Mutex<Vec<CampaignId>>>,
}

impl RecordingDriver {
    fn new(kind: CampaignKind) -> Self {
        Self {
            kind,
            planned: Arc::default(),
        }
    }

    fn planned(&self) -> Vec<CampaignId> {
        self.planned.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl CampaignDriver for RecordingDriver {
    fn kind(&self) -> CampaignKind {
        self.kind
    }

    async fn plan(&self, campaign: &Campaign) -> Result<Plan, String> {
        self.planned.lock().unwrap().push(campaign.id);
        Ok(Plan::Hold("test_hold"))
    }

    async fn find_subjects(
        &self,
        _campaign: &Campaign,
        _plan: ScanPlan,
        _after: Option<Uuid>,
        _limit: u32,
    ) -> Result<SubjectPage, String> {
        Err("a held campaign isn't scanned".to_owned())
    }

    async fn upgrade(&self, _campaign: &Campaign, _item: &LeasedItem) -> ItemOutcome {
        unreachable!("a held campaign leases nothing")
    }
}

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

/// Insert an item of `campaign` for a fresh subject of `subject_kind`: in
/// flight under a lease that lapsed a minute ago when `in_flight`, pending
/// otherwise.
async fn insert_item(
    pool: &PgPool,
    campaign: CampaignId,
    subject_kind: &str,
    in_flight: bool,
) -> TestResult<Uuid> {
    let subject = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
             lease_expires_at
         )
         VALUES ($1, $2, $3, CASE WHEN $4 THEN 'in_flight' ELSE 'pending' END, 1,
                 CASE WHEN $4 THEN $5::uuid END,
                 CASE WHEN $4 THEN now() - interval '1 minute' END)"
    ))
    .bind(campaign)
    .bind(subject_kind)
    .bind(subject)
    .bind(in_flight)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(subject)
}

#[tokio::test(flavor = "current_thread")]
async fn the_loop_drives_only_the_runtime_campaigns_beside_a_foreign_one() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let templates = insert_campaign(&db.pool, seed()).await?;
    let foreign = insert_foreign_campaign(&db.pool, seed()).await?;
    insert_item(&db.pool, foreign, "test_foreign_subject", true).await?;
    insert_item(&db.pool, foreign, "test_foreign_subject", false).await?;
    let sandboxes = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::Sandboxes,
            ..seed()
        },
    )
    .await?;
    let foreign_before = snapshot(&db.pool, foreign).await?;
    let template_driver = RecordingDriver::new(CampaignKind::Templates);
    let sandbox_driver = RecordingDriver::new(CampaignKind::Sandboxes);
    let reconciler = Reconciler::new(
        db.pool.clone(),
        Drivers::default()
            .with(template_driver.clone())
            .with(sandbox_driver.clone()),
        config(),
    );

    assert_eq!(tick(&reconciler).await, TickReport::default());
    assert_eq!(template_driver.planned(), [templates]);
    assert_eq!(sandbox_driver.planned(), [sandboxes]);
    assert_eq!(
        snapshot(&db.pool, foreign).await?,
        foreign_before,
        "its lapsed lease isn't re-driven, nor anything else changed"
    );
    Ok(())
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
    let subject = insert_item(&db.pool, runtime, "template_release", true).await?;
    let foreign = insert_foreign_campaign(&db.pool, aborted).await?;
    insert_item(&db.pool, foreign, "test_foreign_subject", true).await?;
    // An unfinished foreign campaign is all the loop's list would find, if
    // it saw foreign campaigns at all.
    let unfinished_foreign = insert_foreign_campaign(&db.pool, seed()).await?;
    let foreign_before = snapshot(&db.pool, foreign).await?;
    let unfinished_before = snapshot(&db.pool, unfinished_foreign).await?;
    let driver = FakeDriver::default();
    let reconciler = reconciler(&db.pool, driver.clone());

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
