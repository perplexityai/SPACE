//! The reads a sandbox campaign's driver finds and re-reads its sandboxes
//! with.

use super::*;

const K8S: SubstrateKind = SubstrateKind::Kubernetes;

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
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(published)
    .execute(&db.pool)
    .await?;
    let initial_changed_at = subject_sandbox(&db.pool, published)
        .await?
        .expect("the sandbox exists")
        .lifecycle_changed_at;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1"
    ))
    .bind(published)
    .execute(&db.pool)
    .await?;
    let stopped = subject_sandbox(&db.pool, published)
        .await?
        .expect("the sandbox exists");
    assert_eq!(stopped.lifecycle, "stopped");
    assert!(stopped.lifecycle_changed_at > initial_changed_at);
    let read = subject_sandbox(&db.pool, unpublished)
        .await?
        .expect("an unpublished sandbox still exists");
    assert_eq!(
        (
            read.lifecycle.as_str(),
            read.pinned_stop_disk,
            read.execution,
            read.stop_disk_execution,
        ),
        ("stopped", false, None, None)
    );

    // When `node_drain` evacuates a stopped sandbox off its node, it pins a
    // `'ready'` `'disk'`/`'stop'` snapshot on `sandboxes` and tears down the
    // node projection; `subject_sandbox` reports `pinned_stop_disk = true` and
    // exposes that disk snapshot's recorded execution via `stop_disk_execution`
    // and `effective_execution()`.
    let artifact_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, org_id, type, state, local_artifact_id,
             artifact_kind, artifact_origin, runtime, runtime_version,
             state_format_name, state_format_version, execution_metadata
         )
         VALUES ($1, $2, $3, 'disk', 'ready', $4, 'disk', 'stop',
                 'firecracker', '1.15.1', 'firecracker-snapshot', '9.0.0', $5)"
    ))
    .bind(Uuid::now_v7())
    .bind(unpublished)
    .bind(pplx_space_db_test_fixtures::SEED_ORG_ID)
    .bind(artifact_id)
    .bind(serde_json::json!({ "environment": environment(0).to_string() }))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'disk',
             current_local_artifact_origin = 'stop'
         WHERE id = $1"
    ))
    .bind(unpublished)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    let evacuated = subject_sandbox(&db.pool, unpublished)
        .await?
        .expect("evacuated stopped sandbox exists");
    assert!(evacuated.pinned_stop_disk);
    assert_eq!(evacuated.execution, None);
    let effective = evacuated
        .effective_execution()
        .expect("evacuated stopped disk carries recorded execution");
    assert_eq!(
        effective
            .execution_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("environment"))
            .and_then(serde_json::Value::as_str),
        Some(environment(0).to_string().as_str())
    );
    assert_eq!(effective.runtime_version.as_deref(), Some("1.15.1"));
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

#[tokio::test(flavor = "current_thread")]
async fn external_stop_since_matches_only_active_or_post_fence_stop_and_kill_operations()
-> TestResult {
    use pplx_space_public_api::{
        OperationKind, OperationState, OperationTargetType, PublicSandboxId,
    };
    use pplx_space_sandbox_model::SandboxId;

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = node(&db.pool).await?;
    let other_org = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, slug, name) VALUES ($1, 'other-org', 'Other Org')"
    ))
    .bind(other_org)
    .execute(&db.pool)
    .await?;

    let since = time::OffsetDateTime::from_unix_timestamp(1_760_000_000)?;
    let before = since - time::Duration::seconds(10);
    let after = since + time::Duration::seconds(5);

    struct Case {
        name: &'static str,
        org_override: Option<Uuid>,
        kind: OperationKind,
        target_type: Option<OperationTargetType>,
        state: OperationState,
        requested_at: time::OffsetDateTime,
        completed_at: Option<time::OffsetDateTime>,
        expected: ExternalOperation,
        expected_activation: ExternalOperation,
    }

    let cases = [
        Case {
            name: "queued stop requested before since",
            org_override: None,
            kind: OperationKind::StopSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Queued,
            requested_at: before,
            completed_at: None,
            expected: ExternalOperation::InFlight,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "running kill requested before since",
            org_override: None,
            kind: OperationKind::KillSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Running,
            requested_at: before,
            completed_at: None,
            expected: ExternalOperation::InFlight,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "queued delete requested before since",
            org_override: None,
            kind: OperationKind::DeleteSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Queued,
            requested_at: before,
            completed_at: None,
            expected: ExternalOperation::InFlight,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "running rollback requested before since",
            org_override: None,
            kind: OperationKind::RollbackSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Running,
            requested_at: before,
            completed_at: None,
            expected: ExternalOperation::InFlight,
            expected_activation: ExternalOperation::InFlight,
        },
        Case {
            name: "succeeded stop completed exactly at since",
            org_override: None,
            kind: OperationKind::StopSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(since),
            expected: ExternalOperation::Succeeded,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "succeeded kill completed after since",
            org_override: None,
            kind: OperationKind::KillSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::Succeeded,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "succeeded rollback completed after since",
            org_override: None,
            kind: OperationKind::RollbackSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::Succeeded,
            expected_activation: ExternalOperation::Succeeded,
        },
        Case {
            name: "succeeded stop without completed_at requested after since",
            org_override: None,
            kind: OperationKind::StopSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Succeeded,
            requested_at: after,
            completed_at: None,
            expected: ExternalOperation::Succeeded,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "historical succeeded stop completed before since",
            org_override: None,
            kind: OperationKind::StopSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(before),
            expected: ExternalOperation::None,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "failed stop after since",
            org_override: None,
            kind: OperationKind::StopSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Failed,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::None,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "canceled kill after since",
            org_override: None,
            kind: OperationKind::KillSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Canceled,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::None,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "expired stop after since",
            org_override: None,
            kind: OperationKind::StopSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Expired,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::None,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "succeeded start operation after since",
            org_override: None,
            kind: OperationKind::StartSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::None,
            expected_activation: ExternalOperation::Succeeded,
        },
        Case {
            name: "non-sandbox target type",
            org_override: None,
            kind: OperationKind::RollbackSandbox,
            target_type: Some(OperationTargetType::Session),
            state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::None,
            expected_activation: ExternalOperation::None,
        },
        Case {
            name: "different organization",
            org_override: Some(other_org),
            kind: OperationKind::RollbackSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            expected: ExternalOperation::None,
            expected_activation: ExternalOperation::None,
        },
    ];

    for case in cases {
        let sandbox = insert_sandbox(
            &db.pool,
            SubstrateKind::Kubernetes,
            SandboxLifecycle::Stopped,
            Some(node),
        )
        .await?;
        assert_eq!(
            external_stop_since(&db.pool, sandbox, since, since).await?,
            ExternalOperation::None
        );

        let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        ))
        .bind(Uuid::now_v7())
        .bind(case.org_override.unwrap_or(SEED_ORG_ID))
        .bind(SEED_USER_ID)
        .bind(case.kind.as_str())
        .bind(case.target_type.as_ref().map(OperationTargetType::as_str))
        .bind(target_id)
        .bind(case.state.as_str())
        .bind(case.requested_at)
        .bind(case.completed_at)
        .execute(&db.pool)
        .await?;

        assert_eq!(
            external_stop_since(&db.pool, sandbox, since, since).await?,
            case.expected,
            "{}",
            case.name
        );
        assert_eq!(
            external_activation_since(&db.pool, sandbox, since).await?,
            case.expected_activation,
            "activation {}",
            case.name
        );
    }

    for kind in [
        OperationKind::StartSandbox,
        OperationKind::WakeSandbox,
        OperationKind::RestoreSandbox,
    ] {
        let sandbox = insert_sandbox(
            &db.pool,
            SubstrateKind::Kubernetes,
            SandboxLifecycle::Stopped,
            Some(node),
        )
        .await?;
        let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(SEED_USER_ID)
        .bind(kind.as_str())
        .bind(OperationTargetType::Sandbox.as_str())
        .bind(target_id)
        .bind(OperationState::Running.as_str())
        .bind(before)
        .bind(Option::<time::OffsetDateTime>::None)
        .execute(&db.pool)
        .await?;
        assert_eq!(
            external_activation_since(&db.pool, sandbox, since).await?,
            ExternalOperation::InFlight
        );
        assert_eq!(
            external_stop_since(&db.pool, sandbox, since, since).await?,
            ExternalOperation::None
        );
    }

    // Pre-dispatch window `[initiated_at, dispatched_at)`:
    // - `already_completed: true` stop, `auto_restart: true` rollback, or a stop
    //   superseded by a newer succeeded activation before `dispatched_at` yield `None`;
    // - genuine pre-dispatch stop or `auto_restart: false` rollback yields `Succeeded`;
    // - any succeeded stop/rollback at or after `dispatched_at` yields `Succeeded`.
    let initiated_at = since;
    let before_mid_wake = since + time::Duration::seconds(4);
    let mid = since + time::Duration::seconds(5);
    let mid_wake = since + time::Duration::seconds(6);
    let dispatched_at = since + time::Duration::seconds(10);

    for (label, kind, payload, result, wake_completed_at, expected_pre, expected_post) in [
        (
            "already_completed stop in pre-dispatch window",
            OperationKind::StopSandbox,
            serde_json::json!({}),
            serde_json::json!({"controller_response": {"already_completed": true}}),
            None,
            ExternalOperation::None,
            ExternalOperation::Succeeded,
        ),
        (
            "auto_restart rollback in pre-dispatch window",
            OperationKind::RollbackSandbox,
            serde_json::json!({"rollback": {"auto_restart": true}}),
            serde_json::json!({}),
            None,
            ExternalOperation::None,
            ExternalOperation::Succeeded,
        ),
        (
            "non-restarting rollback in pre-dispatch window",
            OperationKind::RollbackSandbox,
            serde_json::json!({"rollback": {"auto_restart": false}}),
            serde_json::json!({}),
            None,
            ExternalOperation::Succeeded,
            ExternalOperation::Succeeded,
        ),
        (
            "stop followed by wake before dispatched_at",
            OperationKind::StopSandbox,
            serde_json::json!({}),
            serde_json::json!({}),
            Some(mid_wake),
            ExternalOperation::None,
            ExternalOperation::Succeeded,
        ),
        (
            "wake before stop in pre-dispatch window",
            OperationKind::StopSandbox,
            serde_json::json!({}),
            serde_json::json!({}),
            Some(before_mid_wake),
            ExternalOperation::Succeeded,
            ExternalOperation::Succeeded,
        ),
        (
            "unrecovered user stop in pre-dispatch window",
            OperationKind::StopSandbox,
            serde_json::json!({}),
            serde_json::json!({}),
            None,
            ExternalOperation::Succeeded,
            ExternalOperation::Succeeded,
        ),
    ] {
        let sandbox = insert_sandbox(
            &db.pool,
            SubstrateKind::Kubernetes,
            SandboxLifecycle::Stopped,
            Some(node),
        )
        .await?;
        let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, payload_json, result_json, requested_at, completed_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, 'succeeded', $7, $8, $9, $9)"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(SEED_USER_ID)
        .bind(kind.as_str())
        .bind(OperationTargetType::Sandbox.as_str())
        .bind(&target_id)
        .bind(payload)
        .bind(result)
        .bind(mid)
        .execute(&db.pool)
        .await?;
        if let Some(wake_at) = wake_completed_at {
            sqlx::query(sql!(
                "INSERT INTO public_operations (
                     operation_id, org_id, actor_user_id, kind, target_type, target_id,
                     state, requested_at, completed_at
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, 'succeeded', $7, $7)"
            ))
            .bind(Uuid::now_v7())
            .bind(SEED_ORG_ID)
            .bind(SEED_USER_ID)
            .bind(OperationKind::WakeSandbox.as_str())
            .bind(OperationTargetType::Sandbox.as_str())
            .bind(&target_id)
            .bind(wake_at)
            .execute(&db.pool)
            .await?;
        }
        assert_eq!(
            external_stop_since(&db.pool, sandbox, initiated_at, dispatched_at).await?,
            expected_pre,
            "{label}"
        );
        assert_eq!(
            external_stop_since(&db.pool, sandbox, initiated_at, mid).await?,
            expected_post,
            "{label} when completed at dispatched_at"
        );
    }

    assert_eq!(
        external_stop_since(&db.pool, Uuid::now_v7(), since, since).await?,
        ExternalOperation::None
    );
    assert_eq!(
        external_activation_since(&db.pool, Uuid::now_v7(), since).await?,
        ExternalOperation::None
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn force_restart_candidates_include_unpublished_active_and_mid_transitions_and_exclude_other_bound_environments()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let node = node(&db.pool).await?;
    let k8s = SubstrateKind::Kubernetes;

    let started_on_source =
        insert_sandbox(&db.pool, k8s, SandboxLifecycle::Started, Some(node)).await?;
    // Unpublished settled `started` row (`o.sandbox_id IS NULL`): included so
    // the campaign holds completion until node publication lands.
    let unpublished_started =
        insert_sandbox(&db.pool, k8s, SandboxLifecycle::Started, None).await?;
    // Unpublished settled `stopped` row: excluded because `stopped` isn't in
    // either scanned lifecycle set.
    let unpublished_stopped =
        insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, None).await?;
    // Unpublished/projection-pruned `restoring` row (`o.sandbox_id IS NULL`): included
    // in mid-transition scan with `execution: None` so the campaign holds completion
    // until restore settles and publishes execution metadata.
    let unpublished_restoring =
        insert_sandbox(&db.pool, k8s, SandboxLifecycle::Restoring, None).await?;
    // Published `restoring` row on `environment(0)` (`campaign.from`): included with
    // `Some(execution)`.
    let published_restoring_source =
        insert_sandbox(&db.pool, k8s, SandboxLifecycle::Restoring, Some(node)).await?;
    // Published `restoring` row already bound to `environment(1)` (`campaign.to`):
    // excluded because it's bound to another environment.
    let published_restoring_target =
        insert_sandbox(&db.pool, k8s, SandboxLifecycle::Restoring, Some(node)).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states SET execution_metadata = $2 WHERE sandbox_id = $1"
    ))
    .bind(published_restoring_target)
    .bind(serde_json::json!({ "environment": environment(1).to_string() }))
    .execute(&db.pool)
    .await?;

    let settled = [SandboxLifecycle::Started, SandboxLifecycle::Paused];
    let transitions = [SandboxLifecycle::Restoring, SandboxLifecycle::Starting];
    let rows = force_restart_candidates(
        &db.pool,
        campaign,
        K8S,
        &settled,
        &transitions,
        None,
        16,
        MAX_ATTEMPTS,
    )
    .await?;
    let mut returned: Vec<Uuid> = rows.iter().map(|row| row.sandbox_id).collect();
    returned.sort();
    let mut expected = vec![
        started_on_source,
        unpublished_started,
        unpublished_restoring,
        published_restoring_source,
    ];
    expected.sort();
    assert_eq!(returned, expected);
    assert!(!returned.contains(&unpublished_stopped));
    assert!(!returned.contains(&published_restoring_target));

    for id in [unpublished_started, unpublished_restoring] {
        let rowless = rows
            .iter()
            .find(|row| row.sandbox_id == id)
            .expect("unpublished candidate returned");
        assert!(rowless.execution.is_none());
    }
    let published = rows
        .iter()
        .find(|row| row.sandbox_id == published_restoring_source)
        .expect("published restoring candidate returned");
    assert!(
        published
            .execution
            .as_ref()
            .and_then(|execution| execution.execution_metadata.as_ref())
            .is_some()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stopped_execution_ownership_classifies_drain_wake_and_in_flight_transitions() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = node(&db.pool).await?;
    let k8s = SubstrateKind::Kubernetes;

    let sandbox = insert_sandbox(&db.pool, k8s, SandboxLifecycle::Stopped, Some(node)).await?;
    let stopped_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(&db.pool)
    .await?;
    let snap_at = stopped_at + time::Duration::seconds(5);
    let halted_at = stopped_at + time::Duration::seconds(6);
    let replaced_at = stopped_at + time::Duration::seconds(7);
    let later_snap_at = halted_at + time::Duration::seconds(2);

    let expected_pod = pplx_space_k8s_ids::PodUid::parse("pod-gen-1")?;
    let wrong_pod = pplx_space_k8s_ids::PodUid::parse("pod-other")?;
    let artifact_1 = Uuid::now_v7();
    let artifact_2 = Uuid::now_v7();

    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             pod_materialized_at, state, active, created_at, updated_at
         )
         VALUES ($1, 1, 'sbx-g1', 'space', $2, $3, 'stopped', TRUE, $3, $3)"
    ))
    .bind(sandbox)
    .bind(expected_pod.as_str())
    .bind(stopped_at - time::Duration::seconds(5))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, org_id, type, state, local_artifact_id,
             artifact_kind, artifact_origin, runtime, state_format_name, timestamp
         )
         VALUES ($1, $2, $3, 'disk', 'ready', $4, 'disk', 'stop', 'firecracker', 'firecracker-snapshot', $5),
                ($6, $2, $3, 'disk', 'ready', $7, 'disk', 'stop', 'firecracker', 'firecracker-snapshot', $8)"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(SEED_ORG_ID)
    .bind(artifact_1)
    .bind(snap_at)
    .bind(Uuid::now_v7())
    .bind(artifact_2)
    .bind(later_snap_at)
    .execute(&db.pool)
    .await?;

    // 1. Untouched stopped execution (`lifecycle_changed_at == stopped_at`, `G1` active): Owned,
    //    while a mismatched `pod_uid` before any post-stop re-materialization is Superseded.
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Owned
    );
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &wrong_pod)
            .await?,
        StoppedExecutionOwnership::Superseded
    );

    // 2. Active forced-drain stopped evacuation (`evacuation_restore_target = 'stopped'` on a
    //    force-draining node, before remote snapshot upload registers a `'ready'` row): InFlight.
    //    If `:undrain` cancels the forced drain while leaving `evacuation_restore_target = 'stopped'`
    //    and the local disk pin on `G1`: Owned (doesn't wedge in `InFlight` or fail as `Superseded`).
    let unregistered_artifact = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_phase = 'draining',
                drain_mode = 'forced'
          WHERE id = $1"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2,
                evacuation_restore_target = 'stopped',
                current_local_artifact_id = $3,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(node)
    .bind(unregistered_artifact)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::InFlight
    );
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_phase = NULL,
                drain_mode = NULL
          WHERE id = $1"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Owned
    );
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = NULL,
                current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact_1)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Owned
    );

    // 2b. Same-generation pod re-materialization (`claim_pod_materialization` clears `G1.pod_uid`
    //     or `record_pod_metadata` writes `pod-gen-1b` while `lifecycle_changed_at == stopped_at`):
    //     InFlight while `G1.activation_attempt_id` is live, Owned if aborted before VM boot.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = NULL,
                state = 'creating',
                activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                updated_at = $3
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .bind(Uuid::now_v7())
    .bind(stopped_at + time::Duration::seconds(1))
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::InFlight
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = 'pod-gen-1b',
                state = 'created'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::InFlight
    );
    // If a distinct same-generation pod `'pod-gen-1b'` completed or expired its
    // activation attempt (for example, `'pod-gen-1'` never stopped, `G1`
    // re-materialized onto `'pod-gen-1b'`, and `'pod-gen-1b'` later exited to
    // `'stopped'` at `stopped_at`), `expected_pod` is Superseded.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = NULL,
                activation_attempt_expires_at = NULL
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = NULL,
                activation_attempt_id = NULL,
                activation_attempt_expires_at = NULL
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Owned
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = $2,
                state = 'stopped'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .bind(expected_pod.as_str())
    .execute(&db.pool)
    .await?;

    // 3. Interrupted start/wake at `lifecycle_changed_at == stopped_at` (`G1` retired as
    //    `'replaced'`, `G2` active): InFlight while `G2.activation_attempt_id` is live,
    //    Superseded if `G2` materialized a pod/route and retained a potentially dispatched
    //    `activation_attempt_id` past lease expiry, Owned if `G2` never materialized a pod
    //    before expiry or explicitly released/gave up `activation_attempt_id` before VM boot.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = NULL,
                current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL
          WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'replaced', active = FALSE, terminated_at = $2, updated_at = $2
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .bind(halted_at)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &wrong_pod)
            .await?,
        StoppedExecutionOwnership::Superseded,
        "retired G1 with non-NULL pod_uid != expected_pod must remain Superseded"
    );
    let g2_attempt = Uuid::now_v7();
    let g2_route = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             pod_materialized_at, state, active,
             activation_attempt_id, activation_attempt_expires_at, route_activation_id
         )
         VALUES ($1, 2, 'sbx-g2', 'space', 'pod-gen-2', $4, 'running', TRUE, $2, NOW() + INTERVAL '5 minutes', $3)"
    ))
    .bind(sandbox)
    .bind(g2_attempt)
    .bind(g2_route)
    .bind(halted_at)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::InFlight
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NULL
          WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::InFlight,
        "NULL activation_attempt_expires_at on materialized/routed G2 is an unexpired live attempt"
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded,
        "materialized/routed G2 retaining activation_attempt_id past expiry may have dispatched Wake"
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = NULL,
                pod_materialized_at = NULL,
                state = 'creating',
                route_activation_id = NULL
          WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Owned,
        "unmaterialized G2 whose attempt expired before pod creation never dispatched Wake"
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = 'pod-gen-2',
                pod_materialized_at = $2,
                state = 'running',
                route_activation_id = $3,
                activation_attempt_id = NULL,
                activation_attempt_expires_at = NULL
          WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox)
    .bind(halted_at)
    .bind(g2_route)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded,
        "active G2 without an in-flight attempt or pinned stop disk is a settled post-wake execution"
    );

    // 4. Realistic `node_drain` stopped evacuation (`G1.state = 'stopped'`, `artifact_1`
    //    pinned): InFlight while unplaced (`evacuation_restore_target = 'stopped'`,
    //    `active_node_id IS NULL`), Owned once stopped re-placement settles on `G2`
    //    with a non-NULL `route_activation_id` and `lifecycle_changed_at > stopped_at`.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'stopped'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'off_node',
                active_node_id = NULL,
                evacuation_restore_target = 'stopped',
                current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact_1)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::InFlight
    );
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'on_node',
                active_node_id = $2,
                evacuation_restore_target = NULL,
                lifecycle_changed_at = $3
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(node)
    .bind(replaced_at)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Owned,
        "stopped re-placement sets non-NULL route_activation_id on G2 and must stay Owned"
    );

    // 5. Wrong `pod_uid`, wrong `generation`, `terminated_at < stopped_at`, post-wake
    //    `'replaced'` retirement with `lifecycle_changed_at > stopped_at`, or snapshot
    //    captured after `G1.terminated_at`: Superseded.
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &wrong_pod)
            .await?,
        StoppedExecutionOwnership::Superseded
    );
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 2, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded
    );
    assert_eq!(
        stopped_execution_ownership(
            &db.pool,
            sandbox,
            halted_at + time::Duration::seconds(1),
            halted_at + time::Duration::seconds(1),
            1,
            &expected_pod,
        )
        .await?,
        StoppedExecutionOwnership::Superseded
    );
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, snap_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded,
        "settled_stopped_at < stopped_at must fail closed to Superseded while terminated_at >= stopped_at"
    );
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, replaced_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Owned,
        "undispatched start rollback after drain retirement (settled_stopped_at = replaced_at > halted_at >= stopped_at) stays Owned"
    );
    sqlx::query(sql!(
        "UPDATE sandboxes SET current_local_artifact_id = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact_2)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded
    );
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, replaced_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded,
        "snapshot captured after G1.terminated_at must stay Superseded even when settled_stopped_at == replaced_at"
    );
    sqlx::query(sql!(
        "UPDATE sandboxes SET current_local_artifact_id = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact_1)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'replaced'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, stopped_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded,
        "implicit wake retires G1 as 'replaced' and advances lifecycle_changed_at on re-stop"
    );
    assert_eq!(
        stopped_execution_ownership(&db.pool, sandbox, stopped_at, replaced_at, 1, &expected_pod)
            .await?,
        StoppedExecutionOwnership::Superseded,
        "'replaced' retirement with a pinned stop disk must stay Superseded even when lifecycle_changed_at == settled_stopped_at"
    );
    Ok(())
}
