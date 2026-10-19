//! The create-executor tests in the parent module whose path differs by
//! substrate, run against the Kubernetes substrate under the same names.

use super::*;
use crate::controllers::substrate::kubernetes::tests::substrate_with_store_and_client;
use crate::test_support::kubernetes::{kubernetes, pod_creates, unavailable_apiserver};

/// Same outcome as on direct, from a different failure. Direct finds no
/// worker, a 503. Kubernetes reaches the apiserver, whose 503 on the pod
/// create the substrate reports as a failed create (`SubstrateError::Failed`),
/// a controller 500. Native create execution retries any controller 5xx
/// other than a known template-snapshot incompatibility
/// (`controller_error_result`), so the operation is queued for retry either
/// way.
#[tokio::test(flavor = "current_thread")]
async fn create_executor_retries_unpinned_transient_substrate_failure() {
    let db = pg_test_db().await.expect("Postgres fixture");
    let operation = seed_unpinned_create_operation(&db.pool).await;
    let (client, log) = unavailable_apiserver();
    let controller = SandboxController::new(kubernetes(substrate_with_store_and_client(
        db.pool.clone(),
        client,
    )));
    let executor = CreateSandboxOperationExecutor::new(db.pool.clone(), controller);

    let result = executor.execute(&operation).await;

    assert_eq!(result.state, OperationState::Queued, "{result:?}");
    assert_eq!(result.terminal_error_code, None, "{result:?}");
    assert!(result.retry_after.is_some(), "{result:?}");
    assert!(pod_creates(&log) > 0, "{result:?}");
}

/// Seeds the reserved warm-pool slot behind an admitted create, in `state`.
async fn insert_reserved_pool_slot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    claim: CreateSandboxPoolClaim,
    state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, 'seed', NULL, NULL)
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'seed', 'fc', 'sha256:admitted-pool')"
    ))
    .bind(release_id)
    .execute(pool)
    .await?;
    let config_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 1, 1, 1, 'space', true)"
    ))
    .bind(config_id)
    .bind(release_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             id, config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, claim_id, ready_at, claimed_at
         )
         VALUES (
             $1, $2, $3, $4, 'seed', 'sha256:admitted-pool', 4, 2048,
             'space-sbx-admitted-g1', 'space', 'admitted-pod-uid',
             '192.0.2.1'::inet, 'worker-1', $5, $6, NOW(), NOW()
         )"
    ))
    .bind(claim.pool_slot_id)
    .bind(config_id)
    .bind(release_id)
    .bind(sandbox_id)
    .bind(state)
    .bind(claim.claim_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// With the reserved slot still `claimed`, the outcome is the same as on
/// direct: each re-execution reports success and starts no execution. The
/// `Succeeded` check is what catches a re-execution that re-enters create:
/// the fixture tags no template release `latest`, so such a create fails
/// resolving the template before it reaches the substrate, and the
/// `sandbox_k8s_executions` count stays zero either way. Unlike direct, whose
/// `admitted_pool_cleanup_pending` is the trait default `false`, Kubernetes
/// reads the reserved slot: once the failed adoption's slot is
/// `cleanup_pending`, a re-execution waits for that cleanup as workflow
/// progress instead of reporting the row as created.
#[tokio::test]
async fn admitted_create_re_execution_after_success_does_not_create_again() -> TestResult {
    use crate::controllers::substrate::kubernetes::tests::substrate_with_store;

    let db = pg_test_db().await?;
    let org_id = seed_org(&db.pool).await?;
    let user_id = seed_user(&db.pool, org_id).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        SandboxSeed {
            org_id,
            user_id,
            lifecycle: SandboxLifecycle::Started,
            placement: SeedSandboxPlacement::OnNode(NodeId::new()),
        },
    )
    .await?;
    let claim = admitted_pool_claim();
    insert_reserved_pool_slot(&db.pool, sandbox_id, claim, "claimed").await?;
    let executor = CreateSandboxOperationExecutor::new(
        db.pool.clone(),
        SandboxController::new(kubernetes(substrate_with_store(db.pool.clone()))),
    );
    let mut operation = claimed_create_operation_for(sandbox_id, org_id, user_id);
    let mut payload: CreateSandboxOperationPayload =
        serde_json::from_value(operation.payload.clone())?;
    payload.pool_claim = Some(claim);
    operation.payload = payload.into_json();

    for _ in 0..2 {
        let result = executor.execute(&operation).await;
        assert_eq!(result.state, OperationState::Succeeded, "{result:?}");
    }

    let sandboxes: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(sandboxes, 1);
    let executions: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        executions, 0,
        "re-execution must not start another execution"
    );

    sqlx::query(sql!(
        "UPDATE kubernetes_pool_slots SET state = 'cleanup_pending' WHERE id = $1"
    ))
    .bind(claim.pool_slot_id)
    .execute(&db.pool)
    .await?;
    let result = executor.execute(&operation).await;
    assert_eq!(result.state, OperationState::Queued, "{result:?}");
    assert_eq!(
        result.requeue_reason,
        PublicOperationRequeueReason::WorkflowProgress,
        "{result:?}"
    );
    Ok(())
}
