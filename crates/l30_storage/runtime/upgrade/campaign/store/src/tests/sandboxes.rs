//! The reads a sandbox campaign's driver finds and re-reads its sandboxes
//! with.

use super::*;

const K8S: &str = SubstrateKind::Kubernetes.as_str();

async fn candidates(
    pool: &PgPool,
    campaign: CampaignId,
    lifecycles: &[SandboxLifecycle],
    after: Option<Uuid>,
    limit: u32,
) -> TestResult<Vec<Uuid>> {
    Ok(
        sandbox_candidates(pool, campaign, K8S, lifecycles, after, limit, MAX_ATTEMPTS)
            .await?
            .into_iter()
            .map(|candidate| candidate.sandbox_id)
            .collect(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn candidates_are_published_sandboxes_of_the_substrate_and_lifecycles_without_items()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let node = node(&db.pool).await?;
    let k8s = SubstrateKind::Kubernetes;
    let mut stopped = Vec::new();
    for _ in 0..3 {
        stopped.push(insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, Some(node)).await?);
    }
    stopped.sort();
    let started = insert_sandbox(&db.pool, k8s, SandboxLifecycle::Started, Some(node)).await?;
    // Out of scope: another substrate, another lifecycle, or never published.
    insert_sandbox(
        &db.pool,
        SubstrateKind::Space,
        SandboxLifecycle::Stopped,
        Some(node),
    )
    .await?;
    insert_sandbox(&db.pool, k8s, SandboxLifecycle::Suspended, Some(node)).await?;
    insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, None).await?;

    let stopped_only = [SandboxLifecycle::Stopped];
    let page = sandbox_candidates(
        &db.pool,
        campaign,
        K8S,
        &stopped_only,
        None,
        2,
        MAX_ATTEMPTS,
    )
    .await?;
    assert_eq!(
        page.iter()
            .map(|candidate| candidate.sandbox_id)
            .collect::<Vec<_>>(),
        stopped[..2]
    );
    assert_eq!(page[0].runtime_version.as_deref(), Some("1.15.1"));
    assert_eq!(
        page[0].execution_metadata.as_ref(),
        Some(&serde_json::json!({ "environment": environment(0).to_string() }))
    );
    assert_eq!(
        candidates(&db.pool, campaign, &stopped_only, Some(stopped[1]), 2).await?,
        stopped[2..]
    );

    // A sandbox the campaign already has is no longer a candidate.
    insert_pending(
        &db.pool,
        campaign,
        SubjectKind::Sandbox,
        &stopped[..1],
        MAX_ATTEMPTS,
    )
    .await?;
    let with_running = [SandboxLifecycle::Stopped, SandboxLifecycle::Started];
    let mut all = candidates(&db.pool, campaign, &with_running, None, 10).await?;
    all.sort();
    let mut expected = vec![stopped[1], stopped[2], started];
    expected.sort();
    assert_eq!(all, expected);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidates_bound_to_another_environment_are_left_out() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let node = node(&db.pool).await?;
    let k8s = SubstrateKind::Kubernetes;
    let on_source = insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, Some(node)).await?;
    let mut by_metadata = Vec::new();
    for metadata in [
        serde_json::json!({ "environment": environment(9).to_string() }),
        // Not a string: unbound, so the caller attributes it.
        serde_json::json!({ "environment": 7 }),
        serde_json::json!({}),
    ] {
        let sandbox = insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, Some(node)).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_observed_states SET execution_metadata = $2 WHERE sandbox_id = $1"
        ))
        .bind(sandbox)
        .bind(metadata)
        .execute(&db.pool)
        .await?;
        by_metadata.push(sandbox);
    }

    let mut found = candidates(&db.pool, campaign, &[SandboxLifecycle::Stopped], None, 10).await?;
    found.sort();
    let mut expected = vec![on_source, by_metadata[1], by_metadata[2]];
    expected.sort();
    assert_eq!(found, expected);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn only_a_skipped_item_with_leases_left_is_a_candidate_again() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let sandboxes = pending_on(&db, campaign, Some(node), 4).await?;
    let leased = lease(&db, campaign).await?;
    let outcome_of = |sandbox: Uuid| match sandboxes.iter().position(|id| *id == sandbox) {
        Some(0) => ItemOutcome::Skipped(ItemError::new("the sandbox had started")),
        Some(1) => ItemOutcome::Done,
        Some(2) => ItemOutcome::Failed(ItemError::new("the boot failed")),
        _ => ItemOutcome::Retry(ItemError::new("busy")),
    };
    for item in &leased {
        finish_item(&db.pool, item, &outcome_of(item.subject_id)).await?;
    }
    // A fifth sandbox, skipped after using up its leases, and a sixth,
    // retired.
    let stopped = |pool| {
        insert_sandbox(
            pool,
            SubstrateKind::Kubernetes,
            SandboxLifecycle::Stopped,
            Some(node),
        )
    };
    let exhausted = stopped(&db.pool).await?;
    skipped(&db.pool, campaign, exhausted, MAX_ATTEMPTS as i64).await?;
    let retired = stopped(&db.pool).await?;
    skipped(&db.pool, campaign, retired, 1).await?;
    retire_skipped(&db.pool, campaign, SubjectKind::Sandbox, &[retired]).await?;

    // Only the skipped sandbox with leases left is a candidate again; the
    // others keep their items.
    assert_eq!(
        candidates(&db.pool, campaign, &[SandboxLifecycle::Stopped], None, 10).await?,
        vec![sandboxes[0]]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_subject_sandbox_reads_its_lifecycle_and_published_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = node(&db.pool).await?;
    let published = insert_sandbox(
        &db.pool,
        SubstrateKind::Kubernetes,
        SandboxLifecycle::Paused,
        Some(node),
    )
    .await?;
    let unpublished = insert_sandbox(
        &db.pool,
        SubstrateKind::Kubernetes,
        SandboxLifecycle::Stopped,
        None,
    )
    .await?;

    let read = subject_sandbox(&db.pool, published)
        .await?
        .expect("the sandbox exists");
    assert_eq!(read.lifecycle, "paused");
    let execution = read
        .execution
        .expect("the sandbox's publish carries an execution");
    assert_eq!(execution.sandbox_id, published);
    assert_eq!(
        execution
            .execution_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("environment"))
            .and_then(serde_json::Value::as_str),
        Some(environment(0).to_string().as_str())
    );
    assert_eq!(execution.runtime_version.as_deref(), Some("1.15.1"));
    let read = subject_sandbox(&db.pool, unpublished)
        .await?
        .expect("an unpublished sandbox still exists");
    assert_eq!((read.lifecycle.as_str(), read.execution), ("stopped", None));
    assert_eq!(subject_sandbox(&db.pool, Uuid::now_v7()).await?, None);
    Ok(())
}

/// A campaign and a start judge a publish by one rule: a sandbox is a
/// candidate, and its subject read carries an execution, exactly when the
/// start's read of its published execution finds one.
#[tokio::test(flavor = "current_thread")]
async fn a_campaign_and_a_start_agree_on_which_publishes_are_evidence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let node = node(&db.pool).await?;
    // (execution_metadata, runtime_version, state_format_version), and
    // whether the start reads it as evidence.
    let shapes: [(Option<serde_json::Value>, Option<&str>, Option<&str>); 5] = [
        (
            Some(serde_json::json!({ "environment": environment(0).to_string() })),
            None,
            None,
        ),
        (None, Some("1.15.1"), None),
        (None, None, Some("9.0.0")),
        // An object without an environment is still a published execution.
        (Some(serde_json::json!({})), None, None),
        (None, None, None),
    ];
    let mut sandboxes = Vec::new();
    for (metadata, runtime_version, state_format_version) in shapes {
        let sandbox = insert_sandbox(
            &db.pool,
            SubstrateKind::Kubernetes,
            SandboxLifecycle::Stopped,
            Some(node),
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE sandbox_observed_states
             SET execution_metadata = $2, runtime_version = $3, state_format_version = $4
             WHERE sandbox_id = $1"
        ))
        .bind(sandbox)
        .bind(metadata)
        .bind(runtime_version)
        .bind(state_format_version)
        .execute(&db.pool)
        .await?;
        sandboxes.push(sandbox);
    }

    let found = candidates(&db.pool, campaign, &[SandboxLifecycle::Stopped], None, 10).await?;
    let mut evidence = Vec::new();
    for sandbox in &sandboxes {
        let start = pplx_space_sandbox_teardown::observed_states::last_published_execution(
            &db.pool,
            pplx_space_sandbox_model::SandboxId::from_uuid(*sandbox),
        )
        .await?
        .is_some();
        let subject = subject_sandbox(&db.pool, *sandbox)
            .await?
            .expect("the sandbox exists")
            .execution
            .is_some();
        assert_eq!(
            found.contains(sandbox),
            start,
            "candidate vs start for {sandbox}"
        );
        assert_eq!(subject, start, "subject read vs start for {sandbox}");
        evidence.push(start);
    }
    assert_eq!(evidence, [true, true, true, true, false]);
    Ok(())
}
