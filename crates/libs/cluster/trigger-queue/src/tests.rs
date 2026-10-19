use pplx_space_common_core::types::NodeId;

use super::*;
use pplx_space_db_test_fixtures::pg_test_db;
use sqlx::PgPool;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn insert_node(
    pool: &PgPool,
    node_id: NodeId,
    role: &str,
    managed_by: &str,
    direct_endpoint: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, role, managed_by, state)
         VALUES ($1, $2, $3, 'registered')"
    ))
    .bind(node_id)
    .bind(role)
    .bind(managed_by)
    .execute(pool)
    .await?;
    if let Some(endpoint) = direct_endpoint {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, name, node_api_endpoint)
             VALUES ($1, $2, $3)"
        ))
        .bind(node_id)
        .bind(format!("node-{node_id}"))
        .bind(endpoint)
        .execute(pool)
        .await?;
    }
    Ok(())
}

#[test]
fn normalize_trigger_reason_defaults_empty_reason() {
    assert_eq!(normalize_trigger_reason("   "), "unspecified");
}

#[test]
fn normalize_trigger_reason_truncates_long_reason() {
    let reason = "a".repeat(RECONCILE_REASON_MAX_CHARS + 8);
    assert_eq!(
        normalize_trigger_reason(&reason).len(),
        RECONCILE_REASON_MAX_CHARS
    );
}

#[tokio::test(flavor = "current_thread")]
async fn enqueue_claim_and_complete_coalesces_reasons() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(
        &db.pool,
        node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;

    enqueue_triggers(
        &db.pool,
        [node_id],
        ReconcileReason::SandboxDesiredStateUpdated,
    )
    .await?;
    enqueue_triggers(
        &db.pool,
        [node_id],
        ReconcileReason::SandboxNetworkPolicyUpdated,
    )
    .await?;

    let claim = claim_triggers(&db.pool, 10).await?;
    assert_eq!(claim.triggers.len(), 1);
    assert_eq!(
        claim.triggers[0].reasons,
        vec![
            ReconcileReason::SandboxNetworkPolicyUpdated,
            ReconcileReason::SandboxDesiredStateUpdated,
        ]
    );
    assert_eq!(
        claim.triggers[0].diagnostic_reason,
        "sandbox_network_policy_updated,sandbox_desired_state_updated"
    );

    complete_claim(&db.pool, &claim.claim_token, &[node_id], &[]).await?;
    let count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn enqueue_rejects_cluster_role_nodes() -> TestResult {
    let db = pg_test_db().await?;
    let worker_node_id = NodeId::new();
    let cluster_node_id = NodeId::new();
    insert_node(
        &db.pool,
        worker_node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;
    insert_node(
        &db.pool,
        cluster_node_id,
        SpaceNodeRole::Cluster.as_str(),
        "space",
        Some("127.0.0.1:9801"),
    )
    .await?;

    let error = enqueue_triggers(
        &db.pool,
        [worker_node_id, cluster_node_id],
        ReconcileReason::ManualNodeReconcile,
    )
    .await
    .expect_err("cluster-role trigger target should be rejected");
    assert!(error.to_string().contains("worker or builder"));

    let count: i64 =
        sqlx::query_scalar(sql!("SELECT COUNT(*)::BIGINT FROM node_reconcile_triggers"))
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_failures_dead_letter_trigger() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(
        &db.pool,
        node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;
    enqueue_triggers(&db.pool, [node_id], ReconcileReason::ManualNodeReconcile).await?;

    for attempt in 0..TRIGGER_MAX_ATTEMPTS {
        sqlx::query(sql!(
            "UPDATE node_reconcile_triggers
             SET next_attempt_at = NOW()
             WHERE node_id = $1"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        let claim = claim_triggers(&db.pool, 10).await?;
        assert_eq!(claim.triggers.len(), 1, "attempt {attempt}");
        complete_claim(
            &db.pool,
            &claim.claim_token,
            &[],
            &[(node_id, format!("failure-{attempt}"))],
        )
        .await?;
    }

    let dead_lettered: bool = sqlx::query_scalar(sql!(
        "SELECT dead_at IS NOT NULL FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(dead_lettered);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn permanent_failure_dead_letters_immediately() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(
        &db.pool,
        node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;
    enqueue_triggers(&db.pool, [node_id], ReconcileReason::ManualNodeReconcile).await?;
    let claim = claim_triggers(&db.pool, 10).await?;
    dead_letter_claim(
        &db.pool,
        &claim.claim_token,
        &[(node_id, "invalid endpoint".to_string())],
    )
    .await?;

    let (dead_lettered, attempts): (bool, i32) = sqlx::query_as(sql!(
        "SELECT dead_at IS NOT NULL, attempts
         FROM node_reconcile_triggers
         WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(dead_lettered);
    assert_eq!(attempts, TRIGGER_MAX_ATTEMPTS);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failed_trigger_can_recover_on_next_claim() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(
        &db.pool,
        node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;
    enqueue_triggers(&db.pool, [node_id], ReconcileReason::ManualNodeReconcile).await?;
    let claim = claim_triggers(&db.pool, 10).await?;
    complete_claim(
        &db.pool,
        &claim.claim_token,
        &[],
        &[(node_id, "temporary failure".to_string())],
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_triggers SET next_attempt_at = NOW() WHERE node_id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let retry_claim = claim_triggers(&db.pool, 10).await?;
    assert_eq!(retry_claim.triggers.len(), 1);
    complete_claim(&db.pool, &retry_claim.claim_token, &[node_id], &[]).await?;

    let count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_claim_can_be_taken_over() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(
        &db.pool,
        node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;
    enqueue_triggers(&db.pool, [node_id], ReconcileReason::ManualNodeReconcile).await?;
    let first_claim = claim_triggers(&db.pool, 10).await?;
    assert_eq!(first_claim.triggers.len(), 1);
    sqlx::query(sql!(
        "UPDATE node_reconcile_triggers
         SET claimed_at = NOW() - INTERVAL '120 seconds'
         WHERE node_id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let second_claim = claim_triggers(&db.pool, 10).await?;
    assert_eq!(second_claim.triggers.len(), 1);
    assert_ne!(first_claim.claim_token, second_claim.claim_token);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sweep_dead_triggers_removes_old_dead_letters() -> TestResult {
    let db = pg_test_db().await?;
    let old_node_id = NodeId::new();
    let fresh_node_id = NodeId::new();
    insert_node(
        &db.pool,
        old_node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;
    insert_node(
        &db.pool,
        fresh_node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9801"),
    )
    .await?;
    enqueue_triggers(
        &db.pool,
        [old_node_id, fresh_node_id],
        ReconcileReason::ManualNodeReconcile,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_triggers
         SET dead_at = CASE
             WHEN node_id = $1 THEN NOW() - INTERVAL '2 hours'
             ELSE NOW()
         END"
    ))
    .bind(old_node_id)
    .execute(&db.pool)
    .await?;

    let deleted = sweep_dead_triggers(&db.pool, 3600, 10).await?;
    assert_eq!(deleted, 1);
    let remaining: Vec<NodeId> =
        sqlx::query_scalar(sql!("SELECT node_id FROM node_reconcile_triggers"))
            .fetch_all(&db.pool)
            .await?;
    assert_eq!(remaining, vec![fresh_node_id]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fetch_targets_requires_live_direct_row_for_space_nodes() -> TestResult {
    let db = pg_test_db().await?;
    let direct_node_id = NodeId::new();
    let missing_direct_node_id = NodeId::new();
    let cluster_node_id = NodeId::new();
    insert_node(
        &db.pool,
        direct_node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        Some("127.0.0.1:9800"),
    )
    .await?;
    insert_node(
        &db.pool,
        missing_direct_node_id,
        SpaceNodeRole::Worker.as_str(),
        "space",
        None,
    )
    .await?;
    insert_node(
        &db.pool,
        cluster_node_id,
        SpaceNodeRole::Cluster.as_str(),
        "space",
        Some("127.0.0.1:9801"),
    )
    .await?;

    // A populated live node inventory never lists Space nodes, so it must not
    // dead-letter the live Direct worker either.
    let live_node_names = HashSet::from(["worker-a".to_string()]);
    for live_node_names in [None, Some(&live_node_names)] {
        let targets = fetch_trigger_targets(
            &db.pool,
            &[direct_node_id, missing_direct_node_id, cluster_node_id],
            live_node_names,
        )
        .await?;
        assert_eq!(targets.triggerable.len(), 1, "{live_node_names:?}");
        assert_eq!(targets.triggerable[0].id, direct_node_id);
        assert!(
            targets.permanently_unavailable.is_empty(),
            "{live_node_names:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fetch_targets_treats_empty_live_snapshot_as_unknown_membership() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(
        &db.pool,
        node_id,
        SpaceNodeRole::Worker.as_str(),
        "k8s",
        None,
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'worker-a')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let targets = fetch_trigger_targets(&db.pool, &[node_id], Some(&HashSet::new())).await?;

    assert_eq!(
        targets
            .triggerable
            .iter()
            .map(|target| target.id)
            .collect::<Vec<_>>(),
        vec![node_id]
    );
    assert!(targets.permanently_unavailable.is_empty());
    Ok(())
}
