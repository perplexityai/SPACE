//! Shared fixtures for the Postgres placement-store suites: row factories,
//! ownership-clock manipulation, evidence stand-ins, and outcome expecters.

use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::SandboxId;

use super::*;

pub(crate) use pplx_space_test_env::TestResult;

pub(crate) const NAMESPACE: &str = "sandboxes";

pub(crate) fn store_for(db: &TestDb) -> PostgresPlacementStore {
    PostgresPlacementStore::new(db.pool.clone(), NAMESPACE.to_string())
}

pub(crate) async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'k8s',
                 $2,
                 1)"
    ))
    .bind(id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

pub(crate) async fn set_evacuation_marker(
    pool: &PgPool,
    sandbox: SandboxId,
    target: &str,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn insert_execution(
    pool: &PgPool,
    sandbox: SandboxId,
    generation: i64,
    state: &str,
    age_secs: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active, updated_at)
         VALUES ($1, $2, $3, $4, $5, TRUE, NOW() - make_interval(secs => $6))"
    ))
    .bind(sandbox)
    .bind(generation)
    .bind(format!(
        "test-sbx-{}-g{generation}",
        sandbox.as_uuid().simple()
    ))
    .bind(NAMESPACE)
    .bind(state)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn lifecycle_and_placement(
    pool: &PgPool,
    sandbox: SandboxId,
) -> TestResult<(String, String)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

pub(crate) async fn lease_token(pool: &PgPool, sandbox: SandboxId) -> TestResult<Option<Uuid>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

pub(crate) async fn evacuation_marker(
    pool: &PgPool,
    sandbox: SandboxId,
) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

/// Expire the claim's lease while leaving the execution rows untouched. The
/// row stays `restoring`, so the lease-clearing trigger preserves the pair.
pub(crate) async fn expire_lease(pool: &PgPool, sandbox: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    Ok(())
}

/// Age the sandbox's execution rows past the fresh-execution ownership
/// window while leaving the lease columns untouched.
pub(crate) async fn age_execution_past_ownership(pool: &PgPool, sandbox: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET updated_at = NOW() - make_interval(secs => $2)
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .bind(restore_execution_ownership_ttl_secs() + 1)
    .execute(pool)
    .await?;
    Ok(())
}

/// Expire the claim's lease and age its execution row past the ownership
/// window — the shape a crashed or abandoned attempt leaves behind.
pub(crate) async fn expire_attempt_ownership(pool: &PgPool, sandbox: SandboxId) -> TestResult {
    expire_lease(pool, sandbox).await?;
    age_execution_past_ownership(pool, sandbox).await?;
    Ok(())
}

/// The row's stored lease deadline exactly as the claim-miss snapshot
/// renders it: ceil-to-millisecond of the stored instant.
pub(crate) async fn stored_lease_deadline(
    pool: &PgPool,
    sandbox: SandboxId,
) -> TestResult<jiff::Timestamp> {
    let ms: i64 = sqlx::query_scalar(sql!(
        "SELECT CEIL(EXTRACT(EPOCH FROM restore_lease_expires_at) * 1000)::BIGINT
         FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    Ok(jiff::Timestamp::from_millisecond(ms)?)
}

/// The active execution's ownership deadline (`updated_at` plus the
/// ownership window), ceil-to-millisecond like the claim-miss snapshot.
pub(crate) async fn stored_execution_ownership_deadline(
    pool: &PgPool,
    sandbox: SandboxId,
) -> TestResult<jiff::Timestamp> {
    let ms: i64 = sqlx::query_scalar(sql!(
        "SELECT CEIL(EXTRACT(EPOCH FROM (updated_at + make_interval(secs => $2))) * 1000)::BIGINT
         FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox)
    .bind(restore_execution_ownership_ttl_secs())
    .fetch_one(pool)
    .await?;
    Ok(jiff::Timestamp::from_millisecond(ms)?)
}

/// Register a SPACE node of the given role backed by the given Kubernetes
/// node name, mirroring how the same Kubernetes host registers one SPACE
/// node per role (worker and builder daemons share the host).
pub(crate) async fn register_k8s_node(
    pool: &PgPool,
    role: &str,
    k8s_node_name: &str,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by) VALUES ($1, '10.0.0.1', $2, 'k8s')"
    ))
    .bind(node_id)
    .bind(role)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(k8s_node_name)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Flip the active execution to serving (`running`, pod IP) on the given
/// Kubernetes node name. Leaves `updated_at` untouched, so aged executions
/// stay aged.
pub(crate) async fn set_execution_routable_on(
    pool: &PgPool,
    sandbox: SandboxId,
    k8s_node_name: &str,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running', pod_ip = '10.244.0.7', k8s_node_name = $2
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox)
    .bind(k8s_node_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Persist the routable-pod evidence the completion CAS demands — the
/// active execution serving (`running`, pod IP) on a registered worker
/// node — and return that node's id. Stands in for the bind-pod leg
/// until that operation joins the crate.
pub(crate) async fn mark_execution_routable(
    pool: &PgPool,
    sandbox: SandboxId,
) -> TestResult<NodeId> {
    let node_name = format!("test-node-{}", Uuid::now_v7().simple());
    let node_id = register_k8s_node(pool, "worker", &node_name).await?;
    set_execution_routable_on(pool, sandbox, &node_name).await?;
    Ok(node_id)
}

/// Schedule the row's restore backoff relative to NOW(). Only meaningful
/// while the evacuation marker is set — the marker-clear trigger resets the
/// column the moment the marker is gone.
pub(crate) async fn set_restore_backoff(
    pool: &PgPool,
    sandbox: SandboxId,
    secs_from_now: i64,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET next_restore_attempt_at = NOW() + make_interval(secs => $2)
         WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(secs_from_now)
    .execute(pool)
    .await?;
    Ok(())
}

/// The row's stored backoff deadline exactly as the claim-miss snapshot
/// renders it: ceil-to-millisecond of the stored instant.
pub(crate) async fn stored_backoff_deadline(
    pool: &PgPool,
    sandbox: SandboxId,
) -> TestResult<jiff::Timestamp> {
    let ms: i64 = sqlx::query_scalar(sql!(
        "SELECT CEIL(EXTRACT(EPOCH FROM next_restore_attempt_at) * 1000)::BIGINT
         FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    Ok(jiff::Timestamp::from_millisecond(ms)?)
}

/// Flip the stored lifecycle while keeping the lease columns intact. The
/// lease-clearing trigger would otherwise wipe the token the moment the row
/// leaves `restoring`, which would make every blocking-lifecycle CAS loss
/// indistinguishable from a stale-fence loss.
pub(crate) async fn set_lifecycle_bypassing_lease_trigger(
    pool: &PgPool,
    sandbox: SandboxId,
    lifecycle: &str,
) -> TestResult {
    set_lifecycle_bypassing_triggers(pool, sandbox, lifecycle, false).await
}

/// [`set_lifecycle_bypassing_lease_trigger`] that also keeps the evacuation
/// marker, which `sandboxes_clear_evacuation_restore_target` drops when the
/// row leaves the suspend/restore lifecycles.
pub(crate) async fn set_lifecycle_bypassing_lease_and_marker_triggers(
    pool: &PgPool,
    sandbox: SandboxId,
    lifecycle: &str,
) -> TestResult {
    set_lifecycle_bypassing_triggers(pool, sandbox, lifecycle, true).await
}

async fn set_lifecycle_bypassing_triggers(
    pool: &PgPool,
    sandbox: SandboxId,
    lifecycle: &str,
    keep_marker: bool,
) -> TestResult {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "ALTER TABLE sandboxes DISABLE TRIGGER sandboxes_clear_restore_lease"
    ))
    .execute(&mut *tx)
    .await?;
    if keep_marker {
        sqlx::query(sql!(
            "ALTER TABLE sandboxes DISABLE TRIGGER sandboxes_clear_evacuation_restore_target"
        ))
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox)
        .bind(lifecycle)
        .execute(&mut *tx)
        .await?;
    if keep_marker {
        sqlx::query(sql!(
            "ALTER TABLE sandboxes ENABLE TRIGGER sandboxes_clear_evacuation_restore_target"
        ))
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(sql!(
        "ALTER TABLE sandboxes ENABLE TRIGGER sandboxes_clear_restore_lease"
    ))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn set_active_node(
    pool: &PgPool,
    sandbox: SandboxId,
    node: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) fn expect_claimed(outcome: ClaimOutcome) -> Attempt {
    match outcome {
        ClaimOutcome::Claimed(attempt) => attempt,
        other => panic!("expected a claimed attempt, got {other:?}"),
    }
}

pub(crate) fn expect_already_owned(outcome: ClaimOutcome) -> jiff::Timestamp {
    match outcome {
        ClaimOutcome::AlreadyOwned { lease_expires_at } => lease_expires_at,
        other => panic!("expected an already-owned outcome, got {other:?}"),
    }
}

pub(crate) fn expect_backoff(outcome: ClaimOutcome) -> jiff::Timestamp {
    match outcome {
        ClaimOutcome::Backoff { next_attempt_at } => next_attempt_at,
        other => panic!("expected a backoff outcome, got {other:?}"),
    }
}
