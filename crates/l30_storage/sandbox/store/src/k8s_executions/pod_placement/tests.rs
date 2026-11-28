//! The pod placement writes change the row only while their guard holds, and
//! a row that fails any part of it stays as it was.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{claim_pod_materialization, record_confirmed_pod_ip, refresh_selected_route_pod_ip};

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

/// Generation `generation` of `sandbox_id`, named `pod-<sandbox>-<generation>`,
/// tracking `pod_uid` at `10.1.0.7` under `route_activation_id`, last updated
/// an hour ago.
async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
    pod_uid: Option<&str>,
    route_activation_id: Option<SandboxActivationAttemptId>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             state, active, route_activation_id, updated_at)
         VALUES ($1, $2, 'pod-' || $1::TEXT || '-' || $2::TEXT, 'sandboxes', $3,
                 '10.1.0.7', $4, $5, $6, NOW() - INTERVAL '1 hour')"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(state)
    .bind(active)
    .bind(route_activation_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn pod_name(sandbox_id: SandboxId, generation: i64) -> String {
    format!("pod-{sandbox_id}-{generation}")
}

/// `(state, pod_uid, host(pod_ip), pod_materialized_at IS NOT NULL,
/// updated_at)` of one execution.
type ExecutionColumns = (
    String,
    Option<String>,
    Option<String>,
    bool,
    time::OffsetDateTime,
);

async fn read_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<ExecutionColumns> {
    Ok(sqlx::query_as::<_, ExecutionColumns>(sql!(
        "SELECT state, pod_uid, host(pod_ip), pod_materialized_at IS NOT NULL, updated_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_selected_route_pod_ip_writes_only_under_the_whole_route() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let route = SandboxActivationAttemptId::new();
    let other_route = SandboxActivationAttemptId::new();

    let sandbox = insert_sandbox(pool).await?;
    insert_execution(
        pool,
        sandbox,
        1,
        "forgotten",
        false,
        Some("uid-old"),
        Some(route),
    )
    .await?;
    insert_execution(
        pool,
        sandbox,
        2,
        "created",
        true,
        Some("uid-a"),
        Some(route),
    )
    .await?;
    let before = read_execution(pool, sandbox, 2).await?;
    let retired_before = read_execution(pool, sandbox, 1).await?;
    let name = pod_name(sandbox, 2);

    // Each part of the route that no longer owns the row: another
    // generation (a retired one), pod, route or UID.
    for (generation, pod, route_id, uid) in [
        (1, pod_name(sandbox, 1), route, "uid-old"),
        (3, name.clone(), route, "uid-a"),
        (2, "pod-other".to_string(), route, "uid-a"),
        (2, name.clone(), other_route, "uid-a"),
        (2, name.clone(), route, "uid-b"),
    ] {
        assert!(
            !refresh_selected_route_pod_ip(
                pool, sandbox, generation, &pod, route_id, uid, "10.1.0.9"
            )
            .await?
        );
    }
    assert_eq!(read_execution(pool, sandbox, 2).await?, before);
    assert_eq!(read_execution(pool, sandbox, 1).await?, retired_before);

    // The whole route: a `created` row becomes `running` at the new address.
    assert!(
        refresh_selected_route_pod_ip(pool, sandbox, 2, &name, route, "uid-a", "10.1.0.9").await?
    );
    let (state, pod_uid, pod_ip, _, updated_at) = read_execution(pool, sandbox, 2).await?;
    assert_eq!(state, "running");
    assert_eq!(pod_uid.as_deref(), Some("uid-a"));
    assert_eq!(pod_ip.as_deref(), Some("10.1.0.9"));
    assert!(updated_at > before.4);

    // The same address on a `running` row matches but leaves `updated_at`.
    assert!(
        refresh_selected_route_pod_ip(pool, sandbox, 2, &name, route, "uid-a", "10.1.0.9").await?
    );
    assert_eq!(read_execution(pool, sandbox, 2).await?.4, updated_at);

    // Before a route is published, the activation attempt is the route.
    let unpublished = insert_sandbox(pool).await?;
    insert_execution(pool, unpublished, 1, "created", true, Some("uid-c"), None).await?;
    let attempt = SandboxActivationAttemptId::new();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET activation_attempt_id = $2 WHERE sandbox_id = $1"
    ))
    .bind(unpublished)
    .bind(attempt)
    .execute(pool)
    .await?;
    let name = pod_name(unpublished, 1);
    assert!(
        !refresh_selected_route_pod_ip(pool, unpublished, 1, &name, route, "uid-c", "10.1.0.9")
            .await?
    );
    assert!(
        refresh_selected_route_pod_ip(pool, unpublished, 1, &name, attempt, "uid-c", "10.1.0.9")
            .await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn claim_pod_materialization_claims_only_the_active_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 1, "forgotten", false, Some("uid-old"), None).await?;
    insert_execution(pool, sandbox, 2, "running", true, Some("uid-a"), None).await?;
    let retired_before = read_execution(pool, sandbox, 1).await?;
    let before = read_execution(pool, sandbox, 2).await?;

    // A retired generation, and one that was never minted.
    let mut tx = pool.begin().await?;
    assert!(!claim_pod_materialization(&mut *tx, sandbox, 1).await?);
    assert!(!claim_pod_materialization(&mut *tx, sandbox, 3).await?);
    tx.commit().await?;
    assert_eq!(read_execution(pool, sandbox, 1).await?, retired_before);
    assert_eq!(read_execution(pool, sandbox, 2).await?, before);

    let mut tx = pool.begin().await?;
    assert!(claim_pod_materialization(&mut *tx, sandbox, 2).await?);
    tx.commit().await?;
    let (state, pod_uid, pod_ip, materialized, updated_at) =
        read_execution(pool, sandbox, 2).await?;
    assert_eq!(state, "creating");
    assert_eq!((pod_uid, pod_ip), (None, None));
    assert!(materialized, "the cleared UID leaves its stamp");
    assert!(updated_at > before.4);

    // A `recovering` row keeps its state; one that never had a pod gets no
    // stamp.
    let recovering = insert_sandbox(pool).await?;
    insert_execution(pool, recovering, 1, "recovering", true, None, None).await?;
    let mut tx = pool.begin().await?;
    assert!(claim_pod_materialization(&mut *tx, recovering, 1).await?);
    tx.commit().await?;
    let (state, _, _, materialized, _) = read_execution(pool, recovering, 1).await?;
    assert_eq!(state, "recovering");
    assert!(!materialized);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn record_confirmed_pod_ip_promotes_unassigned_and_avoids_locking_confirmed_rows()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    // 1. `created` with `pod_ip IS NULL` promotes to `running` at the confirmed IP.
    let created = insert_sandbox(pool).await?;
    insert_execution(pool, created, 1, "created", true, Some("uid-a"), None).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_ip = NULL WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(created)
    .execute(pool)
    .await?;
    let before = read_execution(pool, created, 1).await?;

    // Wrong generation or UID leaves the row untouched.
    assert!(!record_confirmed_pod_ip(pool, created, 2, "uid-a", "10.1.0.7").await?);
    assert!(!record_confirmed_pod_ip(pool, created, 1, "uid-b", "10.1.0.7").await?);
    assert_eq!(read_execution(pool, created, 1).await?, before);

    assert!(record_confirmed_pod_ip(pool, created, 1, "uid-a", "10.1.0.7").await?);
    let after_confirm = read_execution(pool, created, 1).await?;
    assert_eq!(after_confirm.0, "running");
    assert_eq!(after_confirm.2.as_deref(), Some("10.1.0.7"));
    assert!(after_confirm.4 > before.4);

    // Different IP on an already-confirmed row is rejected without modifying it.
    assert!(!record_confirmed_pod_ip(pool, created, 1, "uid-a", "10.1.0.8").await?);
    assert_eq!(read_execution(pool, created, 1).await?, after_confirm);

    // Repeated confirmation on `running` with the same IP does not acquire a row
    // lock: it succeeds immediately even while another transaction holds `FOR UPDATE`.
    let mut running_lock_tx = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1 FOR UPDATE"
    ))
    .bind(created)
    .fetch_one(&mut *running_lock_tx)
    .await?;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            record_confirmed_pod_ip(pool, created, 1, "uid-a", "10.1.0.7"),
        )
        .await
        .expect("repeated confirmation on running row must not block on FOR UPDATE")?
    );
    running_lock_tx.rollback().await?;
    assert_eq!(read_execution(pool, created, 1).await?, after_confirm);

    // 2. `recovering` with `pod_ip IS NULL` fills `pod_ip` while keeping `state`
    // and `updated_at`, and repeated confirmation on `recovering` also takes no
    // row lock while recovery holds `FOR UPDATE`.
    let recovering = insert_sandbox(pool).await?;
    insert_execution(pool, recovering, 1, "recovering", true, Some("uid-r"), None).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_ip = NULL,
                updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(recovering)
    .execute(pool)
    .await?;
    let recovering_before = read_execution(pool, recovering, 1).await?;
    assert!(record_confirmed_pod_ip(pool, recovering, 1, "uid-r", "10.1.0.7").await?);
    let recovering_after = read_execution(pool, recovering, 1).await?;
    assert_eq!(recovering_after.0, "recovering");
    assert_eq!(recovering_after.2.as_deref(), Some("10.1.0.7"));
    assert_eq!(recovering_after.4, recovering_before.4);

    let mut recovery_lock_tx = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1 FOR UPDATE"
    ))
    .bind(recovering)
    .fetch_one(&mut *recovery_lock_tx)
    .await?;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            record_confirmed_pod_ip(pool, recovering, 1, "uid-r", "10.1.0.7"),
        )
        .await
        .expect("repeated confirmation on recovering row must not block on FOR UPDATE")?
    );
    recovery_lock_tx.rollback().await?;

    // 3. `running` with `pod_ip IS NULL` (such as a warm-pool claim bound before
    // kubelet populated `pod_ip`) fills `pod_ip` and stamps `updated_at`.
    let running_unassigned = insert_sandbox(pool).await?;
    insert_execution(
        pool,
        running_unassigned,
        1,
        "running",
        true,
        Some("uid-pool"),
        None,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_ip = NULL,
                updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(running_unassigned)
    .execute(pool)
    .await?;
    let pool_before = read_execution(pool, running_unassigned, 1).await?;
    assert!(record_confirmed_pod_ip(pool, running_unassigned, 1, "uid-pool", "10.1.0.7").await?);
    let pool_after = read_execution(pool, running_unassigned, 1).await?;
    assert_eq!(pool_after.0, "running");
    assert_eq!(pool_after.2.as_deref(), Some("10.1.0.7"));
    assert!(pool_after.4 > pool_before.4);
    Ok(())
}
