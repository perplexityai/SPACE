//! Durable `sandbox_force_restarts` progress markers (`TARGET_STARTING` and
//! `TARGET_START_SETTLED`) across item-completion, lease-expiry, abort-sweep,
//! and stranded-selection verbs.

use super::*;

async fn item_last_error(
    pool: &PgPool,
    campaign: CampaignId,
    sandbox: Uuid,
) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT last_error
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND subject_kind = 'sandbox'
           AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

async fn expire_item_lease(pool: &PgPool, campaign: CampaignId, sandbox: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE campaign_id = $1
           AND subject_kind = 'sandbox'
           AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(sandbox)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn starting_and_settled_target_markers_preserve_and_clear_across_store_verbs() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let worker = node(&db.pool).await?;
    let store_db = Db::from(db.pool.clone());
    let phase_change = PhaseChange {
        actor: Some(pplx_space_accounts::UserId::from_uuid(SEED_USER_ID)),
        expected_generation: None,
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::SandboxForceRestarts,
            phase: CampaignPhase::Rolling,
            per_node: 4,
            per_cluster: 4,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let sandboxes = pending_on(&db, campaign, Some(worker), 3).await?;
    let [sbx_flow, sbx_skip, sbx_lapsed] = [sandboxes[0], sandboxes[1], sandboxes[2]];
    let mut conn = db.pool.acquire().await?;

    // 1. `clear_leased_starting_on_target` fences stale tokens and preserves
    //    `continuing_run == true` when the active lease clears.
    let leased_flow = lease_selected_items(&mut *conn, campaign, &[sbx_flow], LEASE, 1)
        .await?
        .pop()
        .expect("sbx_flow leased");
    assert!(mark_run_started(&db.pool, &leased_flow).await?);
    assert!(mark_leased_starting_on_target(&db.pool, &leased_flow).await?);
    let mut stale_token = leased_flow.clone();
    stale_token.lease_token = Uuid::now_v7();
    assert!(
        !clear_leased_starting_on_target(&db.pool, &stale_token).await?,
        "stale lease token must not clear starting-on-target"
    );
    assert_eq!(
        starting_on_target(&db.pool, &leased_flow).await?,
        Some(true)
    );
    assert!(clear_leased_starting_on_target(&db.pool, &leased_flow).await?);
    assert_eq!(
        starting_on_target(&db.pool, &leased_flow).await?,
        Some(false)
    );
    assert_eq!(continuing_run(&db.pool, &leased_flow).await?, Some(true));

    // 2. `redrive_expired` preserves `TARGET_STARTING` as `"while starting on
    //    target: lease expired"`, and `started_pending_target_sandboxes` sets
    //    `starting_on_target` only when `sandboxes.lifecycle == 'starting'`.
    assert!(mark_leased_starting_on_target(&db.pool, &leased_flow).await?);
    expire_item_lease(&db.pool, campaign, sbx_flow).await?;
    let redrove = redrive_expired(&mut *conn, campaign, MAX_ATTEMPTS).await?;
    assert_eq!(redrove.requeued, 1);
    assert_eq!(
        item_last_error(&db.pool, campaign, sbx_flow)
            .await?
            .as_deref(),
        Some("while starting on target: the lease expired before an outcome was recorded")
    );
    // Unbound execution on `'stopped'` matches the general unbound branch, not
    // `starting_on_target`.
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states
         SET execution_metadata = '{}'::jsonb
         WHERE sandbox_id = $1"
    ))
    .bind(sbx_flow)
    .execute(&db.pool)
    .await?;
    let stopped_unbound = started_pending_target_sandboxes(
        &db.pool,
        campaign,
        SubstrateKind::Kubernetes,
        &[],
        None,
        4,
    )
    .await?;
    let stopped_entry = stopped_unbound
        .items
        .iter()
        .find(|entry| entry.sandbox_id == sbx_flow)
        .expect("unbound stopped item selected");
    assert!(
        !stopped_entry.starting_on_target,
        "stopped sandbox must not project starting_on_target = true"
    );
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sbx_flow)
        .bind(SandboxLifecycle::Starting.as_str())
        .execute(&db.pool)
        .await?;
    let starting_unbound = started_pending_target_sandboxes(
        &db.pool,
        campaign,
        SubstrateKind::Kubernetes,
        &[],
        None,
        4,
    )
    .await?;
    let starting_entry = starting_unbound
        .items
        .iter()
        .find(|entry| entry.sandbox_id == sbx_flow)
        .expect("starting item selected");
    assert!(starting_entry.starting_on_target);

    // 3. `mark_started_on_target` promotes a `pending` and `failed` prefixed
    //    `TARGET_STARTING` row to `TARGET_START_SETTLED`, and `finish_item(Done)`
    //    clears `last_error` to `NULL`.
    let leased_failed = lease_selected_items(&mut *conn, campaign, &[sbx_flow], LEASE, 1)
        .await?
        .pop()
        .expect("sbx_flow re-leased");
    assert_eq!(
        starting_on_target(&db.pool, &leased_failed).await?,
        Some(true)
    );
    assert!(
        finish_item(
            &db.pool,
            &leased_failed,
            &ItemOutcome::Failed(ItemError::new("start rpc timeout")),
        )
        .await?
    );
    assert_eq!(
        item_last_error(&db.pool, campaign, sbx_flow)
            .await?
            .as_deref(),
        Some("while starting on target: start rpc timeout")
    );
    assert!(mark_started_on_target(&db.pool, campaign, SubjectKind::Sandbox, sbx_flow).await?);
    assert_eq!(
        item_last_error(&db.pool, campaign, sbx_flow)
            .await?
            .as_deref(),
        Some("after target start settled: start rpc timeout")
    );
    assert_eq!(
        pause_campaign(&store_db, campaign, &phase_change)
            .await?
            .phase,
        CampaignPhase::Paused
    );
    let resumed = resume_campaign(&store_db, campaign, &phase_change, true).await?;
    assert_eq!(resumed.retried, 1);
    let leased_done = lease_selected_items(&mut *conn, campaign, &[sbx_flow], LEASE, 1)
        .await?
        .pop()
        .expect("sbx_flow leased after retry_failed");
    assert_eq!(
        starting_on_target(&db.pool, &leased_done).await?,
        Some(false)
    );
    assert_eq!(started_on_target(&db.pool, &leased_done).await?, Some(true));
    assert!(finish_item(&db.pool, &leased_done, &ItemOutcome::Done).await?);
    assert_eq!(item_last_error(&db.pool, campaign, sbx_flow).await?, None);
    assert_eq!(
        pause_campaign(&store_db, campaign, &phase_change)
            .await?
            .phase,
        CampaignPhase::Paused
    );
    assert_eq!(
        resume_campaign(&store_db, campaign, &phase_change, false)
            .await?
            .phase,
        CampaignPhase::Rolling
    );

    // 4. `finish_item(Skipped)` clears `TARGET_STARTING` to the plain skip
    //    message; `release_lapsed_in_finished` preserves `"while starting on
    //    target: lease expired"` on an aborted campaign.
    let leased_skip = lease_selected_items(&mut *conn, campaign, &[sbx_skip], LEASE, 1)
        .await?
        .pop()
        .expect("sbx_skip leased");
    assert!(mark_run_started(&db.pool, &leased_skip).await?);
    assert!(mark_leased_starting_on_target(&db.pool, &leased_skip).await?);
    assert!(
        finish_item(
            &db.pool,
            &leased_skip,
            &ItemOutcome::Skipped(ItemError::new("external user stop superseded")),
        )
        .await?
    );
    assert_eq!(
        item_last_error(&db.pool, campaign, sbx_skip)
            .await?
            .as_deref(),
        Some("external user stop superseded")
    );

    let leased_lapsed = lease_selected_items(&mut *conn, campaign, &[sbx_lapsed], LEASE, 1)
        .await?
        .pop()
        .expect("sbx_lapsed leased");
    assert!(mark_run_started(&db.pool, &leased_lapsed).await?);
    assert!(mark_leased_starting_on_target(&db.pool, &leased_lapsed).await?);
    expire_item_lease(&db.pool, campaign, sbx_lapsed).await?;
    assert_eq!(
        abort_campaign(&db.pool, campaign, &phase_change)
            .await?
            .phase,
        CampaignPhase::Aborted
    );
    assert_eq!(release_lapsed_in_finished(&mut *conn).await?, 1);
    assert_eq!(
        item_last_error(&db.pool, campaign, sbx_lapsed)
            .await?
            .as_deref(),
        Some("while starting on target: the lease expired before an outcome was recorded")
    );
    Ok(())
}
