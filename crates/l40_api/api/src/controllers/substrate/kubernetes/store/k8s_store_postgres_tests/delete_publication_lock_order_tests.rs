use super::*;
use pplx_space_internal_api_server::{proto::PublishedSandboxState, publish_sandbox_states};

#[tokio::test]
async fn unfenced_teardown_and_activation_completion_do_not_deadlock() -> TestResult {
    let fixture = postgres_store_with_max_connections(4).await?;
    let pool = &fixture.pool;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let attempt = Uuid::now_v7();
    insert_sandbox(pool, sandbox_id, "paused").await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.99', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2, placement = 'on_node' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
            sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
            active, state, activation_attempt_id, activation_attempt_expires_at,
            activation_attempt_target_state, route_activation_id
         ) VALUES ($1, 7, 'sandbox-pod', 'space', 'pod-uid', '10.0.0.123',
                   TRUE, 'running', $2, NOW() + INTERVAL '5 minutes', 'started', $2)"
    ))
    .bind(sandbox_id)
    .bind(attempt)
    .execute(pool)
    .await?;

    // A BEFORE UPDATE row trigger runs after Postgres has locked the execution
    // tuple but before the publication can project the sandbox row.
    sqlx::raw_sql(
        "CREATE FUNCTION pause_activation_completion() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_xact_lock(14701);
             RETURN NEW;
         END $$;
         CREATE TRIGGER pause_activation_completion BEFORE UPDATE OF activation_attempt_id
         ON sandbox_k8s_executions FOR EACH ROW
         WHEN (OLD.activation_attempt_id IS NOT NULL AND NEW.activation_attempt_id IS NULL)
         EXECUTE FUNCTION pause_activation_completion();",
    )
    .execute(pool)
    .await?;
    let mut gate = pool.begin().await?;
    let gate_pid: i32 = sqlx::query_scalar(sql!(
        "SELECT pg_backend_pid() FROM pg_advisory_xact_lock(14701)"
    ))
    .fetch_one(&mut *gate)
    .await?;
    let mut publisher = pool.begin().await?;
    let publisher_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *publisher)
        .await?;
    let publish = tokio::spawn(async move {
        let report = PublishedSandboxState {
            sandbox_id: sandbox_id.to_string(),
            present: true,
            status: Some("started".to_owned()),
            transition_receipt: Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
                id: Uuid::now_v7().to_string(),
                kind: "activation_completed".to_owned(),
                target_state: "started".to_owned(),
                execution_generation: 7,
                activation_attempt_id: Some(attempt.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        publish_sandbox_states(&mut publisher, node_id, &[report]).await?;
        publisher.commit().await?;
        TestResult::Ok(())
    });
    wait_until_blocked_by(pool, gate_pid).await?;
    let teardown = async {
        // DELETE commits its intent before it can tear down the execution.
        // The publication's statement has already read the preceding state.
        let mut intent = pool.begin().await?;
        pplx_space_sandbox_store::delete::mark_deleting(&mut *intent, sandbox_id).await?;
        intent.commit().await?;
        fixture
            .store
            .forget_pod_tracking(sandbox_id, PlacementClock::Bump)
            .await?;
        TestResult::Ok(())
    };
    let release = async {
        wait_until_blocked_by(pool, publisher_pid).await?;
        gate.rollback().await?;
        TestResult::Ok(())
    };
    let (teardown_result, release_result) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(teardown, release)
    })
    .await?;
    release_result?;
    let publish_result = tokio::time::timeout(Duration::from_secs(10), publish).await??;
    assert!(
        teardown_result.is_ok() && publish_result.is_ok(),
        "teardown={teardown_result:?}, publish={publish_result:?}"
    );
    let row = execution_by_generation_row(pool, sandbox_id, 7).await?;
    assert!(!row.active, "teardown must finish the execution");
    assert_eq!(row.lifecycle, "deleting");
    Ok(())
}
