//! The twin's commit and the plan's holds: the old index, pools, a moved
//! default, workers that don't read twins, and an occupied slot.

use super::*;

/// While the old `(template_name, hash)` index stands no release can have a
/// twin: the item fails, saying so, and nothing is half made.
#[tokio::test(flavor = "multi_thread")]
async fn the_old_index_fails_the_item_with_its_reason() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (_, hash) = fixture.release("seed", 1, None).await?;
    sqlx::query(
        "CREATE UNIQUE INDEX idx_template_releases_template_name_hash_unique
             ON template_releases (template_name, hash)",
    )
    .execute(fixture.pool())
    .await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items WHERE campaign_id = $1",
    )
    .bind(campaign)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "failed");
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("unique index still stands")),
        "{error:?}"
    );
    assert_eq!(fixture.twin("seed", &hash).await?, None);
    Ok(())
}

/// A twin wins resolution as soon as it commits, so it commits with its
/// manual pools: when copying them fails, the twin isn't made either.
#[tokio::test(flavor = "multi_thread")]
async fn a_twin_commits_with_its_manual_pools() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (original, hash) = fixture.release("seed", 1, None).await?;
    fixture.manual_pool(original).await?;
    sqlx::query(
        "CREATE FUNCTION refuse_pool_configs() RETURNS trigger LANGUAGE plpgsql AS
             $$ BEGIN RAISE EXCEPTION 'pool configs refused'; END $$",
    )
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "CREATE TRIGGER refuse_pool_configs BEFORE INSERT ON kubernetes_pool_configs
             FOR EACH ROW EXECUTE FUNCTION refuse_pool_configs()",
    )
    .execute(fixture.pool())
    .await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items WHERE campaign_id = $1",
    )
    .bind(campaign)
    .fetch_one(fixture.pool())
    .await?;
    assert_ne!(phase, "done");
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("copying the release's pool configs")),
        "{error:?}"
    );
    assert_eq!(fixture.twin("seed", &hash).await?, None);
    Ok(())
}

/// The plan sees the target as the default when an item is leased, but the
/// default can move while the item is in flight. The twin's commit rechecks
/// it under the policy row's share lock: a default write that lands first
/// releases the item, and no twin commits for a target that's no longer the
/// default.
#[tokio::test(flavor = "multi_thread")]
async fn a_default_moved_mid_item_commits_no_twin() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;

    // A default write in flight, holding the policy row: the plan still
    // reads E1, so the item is leased and moved up to the twin's commit.
    let mut write = fixture.pool().begin().await?;
    set_default(
        &mut write,
        fixture.e0,
        &PolicyChange {
            actor: None,
            reason: PolicyChangeReason::new("roll back").expect("reason"),
        },
    )
    .await?;
    let holder: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *write)
        .await?;
    let reconciler = fixture.reconciler();
    let pool = fixture.pool().clone();
    let ((), committed) = tokio::join!(
        async {
            fixture.tick(&reconciler).await;
            fixture.tick(&reconciler).await;
        },
        async {
            // Commit the default write only once the twin's commit waits on
            // the policy row.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                // Something waits on the default write's backend: the
                // twin's commit, on the policy row.
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_stat_activity
                         WHERE $1 = ANY(pg_blocking_pids(pid))
                     )",
                )
                .bind(holder)
                .fetch_one(&pool)
                .await?;
                if waiting {
                    break;
                }
                if tokio::time::Instant::now() > deadline {
                    return Err("the twin's commit never waited on the policy row".into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            write.commit().await?;
            TestResult::Ok(())
        }
    );
    committed?;

    assert_eq!(fixture.twin("seed", &hash).await?, None);
    let (phase, attempts, error): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT phase, attempts, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(
        (phase.as_str(), attempts),
        ("pending", 0),
        "released, not charged"
    );
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("default moved")),
        "{error:?}"
    );
    Ok(())
}

/// A twin-metadata write that fails after the default moved off the target
/// releases the item, as the commit's recheck would have, rather than
/// spending one of its attempts.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_write_after_the_default_moved_releases_the_item() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler_with(Arc::new(HookedObjects {
        inner: Arc::clone(&fixture.objects),
        pool: fixture.pool().clone(),
        template: "seed",
        on_write: OnWrite::MoveDefaultAndFail(fixture.e0),
    }));
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    let (phase, attempts, error): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT phase, attempts, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(fixture.twin("seed", &hash).await?, None);
    assert_eq!(
        (phase.as_str(), attempts),
        ("pending", 0),
        "released, not charged"
    );
    let error = error.unwrap_or_default();
    assert!(error.contains("the cluster default moved off"), "{error}");
    Ok(())
}

/// A worker that doesn't report twin support, registered after the plan
/// passed and before the twin's commit, releases the item at the commit: no
/// twin commits, the item isn't charged, and the plan then holds.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_doesnt_read_twins_at_commit_releases_the_item() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler_with(Arc::new(HookedObjects {
        inner: Arc::clone(&fixture.objects),
        pool: fixture.pool().clone(),
        template: "seed",
        on_write: OnWrite::AddWorkerThatDoesntReadTwins,
    }));
    fixture.tick(&reconciler).await;

    let (phase, attempts, error): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT phase, attempts, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(fixture.twin("seed", &hash).await?, None);
    assert_eq!(
        (phase.as_str(), attempts),
        ("pending", 0),
        "released, not charged"
    );
    let error = error.unwrap_or_default();
    assert!(
        error.contains("don't report reading twin metadata at the twin's commit"),
        "{error}"
    );
    Ok(())
}

/// A worker whose snapshot goes stale while the twin's commit waits on the
/// policy row's lock releases the item: the commit's worker check measures
/// freshness when it runs, not when its transaction began.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_gone_stale_during_the_commits_lock_wait_releases_the_item() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;

    // Hold the policy row without changing it, so the default recheck passes.
    let mut holder = fixture.pool().begin().await?;
    sqlx::query("SELECT 1 FROM runtime_environment_policy WHERE singleton FOR UPDATE")
        .execute(&mut *holder)
        .await?;
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await?;
    let reconciler = fixture.reconciler();
    let pool = fixture.pool().clone();
    let ((), released) = tokio::join!(fixture.tick(&reconciler), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE $1 = ANY(pg_blocking_pids(pid))
                 )",
            )
            .bind(holder_pid)
            .fetch_one(&pool)
            .await?;
            if waiting {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err("the twin's commit never waited on the policy row".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        // Let the snapshot go stale while the commit waits: after its
        // transaction began, so a check at the transaction's start would
        // still count it fresh, and before the check runs.
        sqlx::query("UPDATE node_status_snapshots SET stale_after = clock_timestamp()")
            .execute(&pool)
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        holder.rollback().await?;
        TestResult::Ok(())
    });
    released?;
    // The tick returns once its dispatch has finished.
    reconciler.settle().await;

    assert_eq!(fixture.twin("seed", &hash).await?, None);
    let (phase, attempts, error): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT phase, attempts, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(
        (phase.as_str(), attempts),
        ("pending", 0),
        "released, not charged"
    );
    let error = error.unwrap_or_default();
    assert!(
        error.contains("don't report reading twin metadata at the twin's commit"),
        "{error}"
    );
    Ok(())
}

/// A release without storage locations that already holds the build's slot
/// in the target's partition keeps a twin out. The item fails naming that
/// release, not the old `(template_name, hash)` index.
#[tokio::test(flavor = "multi_thread")]
async fn a_storage_less_release_in_the_target_slot_fails_the_item_by_name() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (_, hash) = fixture.release("seed", 1, None).await?;
    let occupant: Uuid = sqlx::query_scalar(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
         VALUES ('seed', 'fc', $1, jsonb_build_object('environment', $2::TEXT), clock_timestamp())
         RETURNING id",
    )
    .bind(&hash)
    .bind(fixture.e1.to_string())
    .fetch_one(fixture.pool())
    .await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id <> $2",
    )
    .bind(campaign)
    .bind(occupant)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "failed");
    let error = error.unwrap_or_default();
    assert!(
        error.contains(&format!("release {occupant} has no storage locations"))
            && !error.contains("unique index"),
        "{error}"
    );
    Ok(())
}

/// The plan holds a campaign whose target isn't the default, or isn't
/// compiled, and scans once its target is the default.
#[tokio::test(flavor = "multi_thread")]
async fn the_plan_scans_only_toward_a_compiled_default() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let driver = fixture.driver();
    let campaign = |to| Campaign {
        id: CampaignId(Uuid::now_v7()),
        from: fixture.e0,
        to,
        kind: CampaignKind::Templates,
        phase: CampaignPhase::Rolling,
        max_in_flight_per_node: 1,
        max_in_flight_per_cluster: 1,
        failure_budget: 1,
        include_running: false,
        generation: 0,
    };
    assert_eq!(
        driver.plan(&campaign(fixture.e1)).await?,
        Plan::Hold("target_not_default")
    );
    let mut unbooted = fixture.driver();
    unbooted.boots_twins = false;
    fixture.set_default(fixture.e1).await?;
    assert_eq!(
        unbooted.plan(&campaign(fixture.e1)).await?,
        Plan::Hold("twins_not_booted"),
        "a cluster whose pods wouldn't boot a twin's environment makes none"
    );
    fixture.set_default(fixture.e0).await?;
    let unknown = EnvironmentReference::new(pplx_space_content_digest::Digest::sha256([0x5a; 32]));
    fixture.set_default(unknown).await?;
    assert_eq!(
        driver.plan(&campaign(unknown)).await?,
        Plan::Hold("target_not_compiled")
    );
    fixture.set_default(fixture.e1).await?;
    assert!(matches!(
        driver.plan(&campaign(fixture.e1)).await?,
        Plan::Scan(ScanPlan { moves: true, .. })
    ));
    Ok(())
}

/// The plan holds a campaign while a worker has a template-manager that
/// doesn't report reading a twin's own metadata, as a node from before that
/// support doesn't: a twin wins resolution once it commits, so a create there
/// would pull a build it can't read. It scans once every worker reports it,
/// and holds again with no worker registered.
#[tokio::test(flavor = "multi_thread")]
async fn the_plan_holds_until_every_worker_reads_twins() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let driver = fixture.driver();
    let campaign = Campaign {
        id: CampaignId(Uuid::now_v7()),
        from: fixture.e0,
        to: fixture.e1,
        kind: CampaignKind::Templates,
        phase: CampaignPhase::Rolling,
        max_in_flight_per_node: 1,
        max_in_flight_per_cluster: 1,
        failure_budget: 1,
        include_running: false,
        generation: 0,
    };
    fixture.set_default(fixture.e1).await?;
    let worker = pplx_space_accounts::NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(fixture.pool(), worker).await?;
    worker_snapshot(
        fixture.pool(),
        worker,
        &[fixture.e0, fixture.e1],
        r#"{"reachable": true, "reconciliations": []}"#,
    )
    .await?;
    assert_eq!(
        driver.plan(&campaign).await?,
        Plan::Hold("nodes_dont_read_twins"),
        "a worker from before twin metadata"
    );
    worker_snapshot(
        fixture.pool(),
        worker,
        &[fixture.e0, fixture.e1],
        r#"{"reachable": true, "reads_twin_metadata": true}"#,
    )
    .await?;
    assert!(matches!(
        driver.plan(&campaign).await?,
        Plan::Scan(ScanPlan { moves: true, .. })
    ));
    sqlx::query("DELETE FROM nodes")
        .execute(fixture.pool())
        .await?;
    assert_eq!(
        driver.plan(&campaign).await?,
        Plan::Hold("nodes_dont_read_twins"),
        "no registered worker is no evidence"
    );
    Ok(())
}
