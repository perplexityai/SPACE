use super::*;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db};
use pplx_space_placement::StoredSandboxLifecycle;
use pplx_space_test_env::TestResult;

async fn lifecycle(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?,
    )
}

async fn fixture(pool: &PgPool, node: NodeId) -> TestResult<Candidate> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement,
        active_node_id, target_node_id, managed_by, network_policy_generation, network_policy_override_json)
        VALUES ($1, '00000000-0000-0000-0000-000000000010',
        '00000000-0000-0000-0000-000000000001', 'paused', 'on_node', $2, $2, $3, 1, $4)"
    ))
    .bind(id)
    .bind(node)
    .bind(SubstrateKind::Kubernetes)
    .bind(sqlx::types::Json(pplx_space_net_common::NetworkPolicyV1::default()))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name,
        pod_namespace, pod_uid, pod_ip, state, active, route_activation_id)
        VALUES ($1, 7, $2, 'sandboxes', 'pod-uid-7', '192.0.2.1', 'running', TRUE, $3)"
    ))
    .bind(id)
    .bind(format!("sandbox-{id}"))
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(candidates(pool, node, &[id])
        .await?
        .pop()
        .expect("paused resident"))
}

fn started(candidate: &Candidate) -> SandboxInfo {
    SandboxInfo {
        sandbox_id: candidate.id.to_string(),
        lifecycle: Some(SandboxLifecycleState::Started as i32),
        execution_generation: Some(candidate.generation),
        pod_uid: Some(candidate.pod_uid.clone()),
        activation_attempt_id: Some(candidate.route_activation_id.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn resident_resume_requires_fresh_state_and_exact_execution() -> TestResult {
    let db = pg_test_db().await?;
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    let candidate = fixture(&db.pool, node).await?;
    let observed = started(&candidate);
    assert!(confirms_started(&candidate, &observed));
    for state in [
        None,
        Some(SandboxLifecycleState::Paused as i32),
        Some(SandboxLifecycleState::Resuming as i32),
    ] {
        assert!(!confirms_started(
            &candidate,
            &SandboxInfo {
                lifecycle: state,
                ..observed.clone()
            }
        ));
    }
    for changed in [
        SandboxInfo {
            state_degraded: true,
            ..observed.clone()
        },
        SandboxInfo {
            sandbox_id: SandboxId::new().to_string(),
            ..observed.clone()
        },
        SandboxInfo {
            execution_generation: Some(8),
            ..observed.clone()
        },
        SandboxInfo {
            execution_generation: None,
            ..observed.clone()
        },
        SandboxInfo {
            pod_uid: Some("replacement".into()),
            ..observed.clone()
        },
        SandboxInfo {
            activation_attempt_id: Some(Uuid::now_v7().to_string()),
            ..observed.clone()
        },
        SandboxInfo {
            activation_attempt_id: None,
            ..observed
        },
    ] {
        assert!(!confirms_started(&candidate, &changed));
    }
    assert!(complete(&db.pool, node, &candidate).await?);
    assert!(
        !complete(&db.pool, node, &candidate).await?,
        "completion is idempotent"
    );
    assert_eq!(lifecycle(&db.pool, candidate.id).await?, "started");
    Ok(())
}

#[tokio::test]
async fn resident_resume_confirmation_cannot_overwrite_lifecycle_or_execution_writers() -> TestResult
{
    let db = pg_test_db().await?;
    let node = NodeId::new();
    let other = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    insert_ready_worker_node(&db.pool, other).await?;
    // Include a same-value Pause completion. The normal updated_at trigger
    // need not change for it; the tuple version must still fence our probe.
    for stored in StoredSandboxLifecycle::all() {
        let candidate = fixture(&db.pool, node).await?;
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(candidate.id)
            .bind(stored.as_str())
            .execute(&db.pool)
            .await?;
        assert!(!complete(&db.pool, node, &candidate).await?, "{stored:?}");
        assert_eq!(lifecycle(&db.pool, candidate.id).await?, stored.as_str());
    }
    for change in [
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1",
        "UPDATE sandbox_k8s_executions SET state = 'recovering' WHERE sandbox_id = $1",
        "UPDATE sandbox_k8s_executions SET generation = 8 WHERE sandbox_id = $1",
        "UPDATE sandbox_k8s_executions SET pod_uid = 'replacement' WHERE sandbox_id = $1",
        "UPDATE sandbox_k8s_executions SET route_activation_id = NULL WHERE sandbox_id = $1",
        "UPDATE sandbox_k8s_executions SET activation_attempt_id = route_activation_id WHERE sandbox_id = $1",
    ] {
        let candidate = fixture(&db.pool, node).await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(change)))
            .bind(candidate.id)
            .execute(&db.pool)
            .await?;
        assert!(!complete(&db.pool, node, &candidate).await?, "{change}");
        assert_eq!(lifecycle(&db.pool, candidate.id).await?, "paused");
    }
    let candidate = fixture(&db.pool, node).await?;
    assert!(
        candidates(&db.pool, other, &[candidate.id])
            .await?
            .is_empty()
    );
    assert!(!complete(&db.pool, other, &candidate).await?);
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(candidate.id)
    .bind(other)
    .execute(&db.pool)
    .await?;
    assert!(!complete(&db.pool, node, &candidate).await?);
    Ok(())
}

#[tokio::test]
async fn resident_resume_rechecks_an_execution_retired_while_completion_waits() -> TestResult {
    let db = pg_test_db().await?;
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    let candidate = fixture(&db.pool, node).await?;
    let mut retirement = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT sandbox_id FROM sandbox_k8s_executions WHERE sandbox_id = $1 FOR UPDATE"
    ))
    .bind(candidate.id)
    .fetch_one(&mut *retirement)
    .await?;
    let completion = complete(&db.pool, node, &candidate);
    tokio::pin!(completion);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut completion)
            .await
            .is_err(),
        "completion must serialize with execution retirement"
    );
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(candidate.id)
    .execute(&mut *retirement)
    .await?;
    retirement.commit().await?;
    assert!(!tokio::time::timeout(Duration::from_secs(2), completion).await??);
    assert_eq!(lifecycle(&db.pool, candidate.id).await?, "paused");
    Ok(())
}
