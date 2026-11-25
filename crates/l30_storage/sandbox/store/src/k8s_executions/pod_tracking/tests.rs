//! The pod tracking writes change the row only while their guard holds, and
//! a row that fails any part of it stays as it was.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{backfill_missing_pod_uid, clear_absent_recovery_pod};

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'started', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    Ok(id)
}

/// The active generation `1` of `sandbox_id` in `state`, on `node-a` at
/// `10.1.0.7`, last updated an hour ago.
async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
    pod_uid: Option<&str>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    route_activation_id: Option<SandboxActivationAttemptId>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             k8s_node_name, state, active, activation_attempt_id, route_activation_id,
             updated_at)
         VALUES ($1, 1, 'pod-' || $1::TEXT, 'sandboxes', $2, '10.1.0.7', 'node-a', $3,
                 TRUE, $4, $5, NOW() - INTERVAL '1 hour')"
    ))
    .bind(sandbox_id)
    .bind(pod_uid)
    .bind(state)
    .bind(activation_attempt_id)
    .bind(route_activation_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `(pod_uid, host(pod_ip), k8s_node_name, pod_materialized_at IS NOT NULL,
/// updated_at)` of generation `1`.
type ExecutionColumns = (
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
    time::OffsetDateTime,
);

async fn read_execution(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<ExecutionColumns> {
    Ok(sqlx::query_as::<_, ExecutionColumns>(sql!(
        "SELECT pod_uid, host(pod_ip), k8s_node_name, pod_materialized_at IS NOT NULL,
                updated_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Retire generation `1` of `sandbox_id`, leaving its other columns as
/// they are.
async fn retire_execution(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn backfill_missing_pod_uid_fills_only_an_unset_uid_under_the_observed_identity() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = SandboxActivationAttemptId::new();
    let route = SandboxActivationAttemptId::new();
    let other = SandboxActivationAttemptId::new();

    let sandbox = insert_sandbox(pool).await?;
    insert_execution(
        pool,
        sandbox,
        "creating",
        Some(" "),
        Some(attempt),
        Some(route),
    )
    .await?;
    let before = read_execution(pool, sandbox).await?;

    // Another generation, attempt or route, or `NULL` where the row has one.
    let mut tx = pool.begin().await?;
    for (generation, attempt_id, route_id) in [
        (Some(2), Some(attempt), Some(route)),
        (None, Some(attempt), Some(route)),
        (Some(1), Some(other), Some(route)),
        (Some(1), None, Some(route)),
        (Some(1), Some(attempt), Some(other)),
        (Some(1), Some(attempt), None),
    ] {
        assert!(
            !backfill_missing_pod_uid(&mut tx, sandbox, generation, attempt_id, "uid-a", route_id)
                .await?
        );
    }
    tx.commit().await?;
    assert_eq!(read_execution(pool, sandbox).await?, before);

    // A blank UID counts as unset.
    let mut tx = pool.begin().await?;
    assert!(
        backfill_missing_pod_uid(
            &mut tx,
            sandbox,
            Some(1),
            Some(attempt),
            "uid-a",
            Some(route)
        )
        .await?
    );
    tx.commit().await?;
    let (pod_uid, pod_ip, node, materialized, updated_at) = read_execution(pool, sandbox).await?;
    assert_eq!(pod_uid.as_deref(), Some("uid-a"));
    assert_eq!(pod_ip.as_deref(), Some("10.1.0.7"));
    assert_eq!(node.as_deref(), Some("node-a"));
    assert!(materialized);
    assert!(updated_at > before.4);

    // A UID that is set is never overwritten.
    let filled = read_execution(pool, sandbox).await?;
    let mut tx = pool.begin().await?;
    assert!(
        !backfill_missing_pod_uid(
            &mut tx,
            sandbox,
            Some(1),
            Some(attempt),
            "uid-b",
            Some(route)
        )
        .await?
    );
    tx.commit().await?;
    assert_eq!(read_execution(pool, sandbox).await?, filled);

    // `NULL` identities match `None`.
    let unclaimed = insert_sandbox(pool).await?;
    insert_execution(pool, unclaimed, "creating", None, None, None).await?;
    let mut tx = pool.begin().await?;
    assert!(backfill_missing_pod_uid(&mut tx, unclaimed, Some(1), None, "uid-c", None).await?);
    tx.commit().await?;
    assert_eq!(
        read_execution(pool, unclaimed).await?.0.as_deref(),
        Some("uid-c")
    );

    // A retired row that matches every other part of the guard.
    let retired = insert_sandbox(pool).await?;
    insert_execution(pool, retired, "creating", None, Some(attempt), Some(route)).await?;
    retire_execution(pool, retired).await?;
    let retired_before = read_execution(pool, retired).await?;
    let mut tx = pool.begin().await?;
    assert!(
        !backfill_missing_pod_uid(
            &mut tx,
            retired,
            Some(1),
            Some(attempt),
            "uid-a",
            Some(route)
        )
        .await?
    );
    tx.commit().await?;
    assert_eq!(read_execution(pool, retired).await?, retired_before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn clear_absent_recovery_pod_clears_only_the_pod_the_caller_found_absent() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let generation = ExecutionGeneration::new(1)?;

    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, "recovery_failed", Some("uid-a"), None, None).await?;
    let before = read_execution(pool, sandbox).await?;

    // Another state, UID, address or node than the row records, and a
    // generation that was never minted.
    let mut tx = pool.begin().await?;
    for (generation, state, uid, ip, node) in [
        (
            generation,
            "recovering",
            Some("uid-a"),
            Some("10.1.0.7"),
            Some("node-a"),
        ),
        (
            generation,
            "recovery_failed",
            Some("uid-b"),
            Some("10.1.0.7"),
            Some("node-a"),
        ),
        (
            generation,
            "recovery_failed",
            None,
            Some("10.1.0.7"),
            Some("node-a"),
        ),
        (
            generation,
            "recovery_failed",
            Some("uid-a"),
            Some("10.1.0.8"),
            Some("node-a"),
        ),
        (
            generation,
            "recovery_failed",
            Some("uid-a"),
            None,
            Some("node-a"),
        ),
        (
            generation,
            "recovery_failed",
            Some("uid-a"),
            Some("10.1.0.7"),
            Some("node-b"),
        ),
        (
            generation,
            "recovery_failed",
            Some("uid-a"),
            Some("10.1.0.7"),
            None,
        ),
        (
            ExecutionGeneration::new(2)?,
            "recovery_failed",
            Some("uid-a"),
            Some("10.1.0.7"),
            Some("node-a"),
        ),
    ] {
        assert!(
            !clear_absent_recovery_pod(&mut tx, sandbox, generation, state, uid, ip, node).await?
        );
    }
    tx.commit().await?;
    assert_eq!(read_execution(pool, sandbox).await?, before);

    let mut tx = pool.begin().await?;
    assert!(
        clear_absent_recovery_pod(
            &mut tx,
            sandbox,
            generation,
            "recovery_failed",
            Some("uid-a"),
            Some("10.1.0.7"),
            Some("node-a"),
        )
        .await?
    );
    tx.commit().await?;
    let (pod_uid, pod_ip, node, materialized, updated_at) = read_execution(pool, sandbox).await?;
    assert_eq!((pod_uid, pod_ip, node), (None, None, None));
    assert!(materialized, "the cleared UID leaves its stamp");
    assert!(updated_at > before.4);

    // Once cleared, the same pod no longer matches.
    let cleared = read_execution(pool, sandbox).await?;
    let mut tx = pool.begin().await?;
    assert!(
        !clear_absent_recovery_pod(
            &mut tx,
            sandbox,
            generation,
            "recovery_failed",
            Some("uid-a"),
            Some("10.1.0.7"),
            Some("node-a"),
        )
        .await?
    );
    tx.commit().await?;
    assert_eq!(read_execution(pool, sandbox).await?, cleared);

    // A retired row that still records the same pod.
    let retired = insert_sandbox(pool).await?;
    insert_execution(pool, retired, "recovery_failed", Some("uid-a"), None, None).await?;
    retire_execution(pool, retired).await?;
    let retired_before = read_execution(pool, retired).await?;
    let mut tx = pool.begin().await?;
    assert!(
        !clear_absent_recovery_pod(
            &mut tx,
            retired,
            generation,
            "recovery_failed",
            Some("uid-a"),
            Some("10.1.0.7"),
            Some("node-a"),
        )
        .await?
    );
    tx.commit().await?;
    assert_eq!(read_execution(pool, retired).await?, retired_before);
    Ok(())
}
