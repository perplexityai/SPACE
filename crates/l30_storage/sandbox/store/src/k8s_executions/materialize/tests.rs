//! The materialization writes record a pod only on the sandbox's active
//! execution, and the pod metadata write only on the generation the caller
//! names.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    record_node_name, record_node_name_fenced, record_pod_metadata, repair_creating_pod_uid,
};

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'started', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
    k8s_node_name: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active,
             k8s_node_name)
         VALUES ($1, $2, $3, 'space-sandboxes', $4, $5, $6)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "pod-{}-g{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(state)
    .bind(active)
    .bind(k8s_node_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every execution row of the sandbox, oldest generation first: generation,
/// state, active, pod UID, pod IP, Kubernetes node and whether
/// `pod_materialized_at` is stamped.
type Row = (
    i64,
    String,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
);

async fn rows(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Vec<Row>> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT generation, state, active, pod_uid, host(pod_ip), k8s_node_name,
                pod_materialized_at IS NOT NULL
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?)
}

fn row(
    generation: i64,
    state: &str,
    active: bool,
    pod: (Option<&str>, Option<&str>),
    k8s_node_name: Option<&str>,
    materialized: bool,
) -> Row {
    (
        generation,
        state.to_string(),
        active,
        pod.0.map(str::to_string),
        pod.1.map(str::to_string),
        k8s_node_name.map(str::to_string),
        materialized,
    )
}

/// The execution's `updated_at`.
async fn updated_at(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<time::OffsetDateTime> {
    Ok(sqlx::query_scalar::<_, time::OffsetDateTime>(sql!(
        "SELECT updated_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn pod_metadata_is_recorded_only_on_the_active_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 1, "replaced", false, None).await?;
    insert_execution(pool, sandbox, 2, "creating", true, None).await?;
    let before = rows(pool, sandbox).await?;

    // A retired generation and one the sandbox never had are left alone.
    for generation in [1, 3] {
        assert!(!record_pod_metadata(pool, sandbox, generation, Some("uid-2"), None).await?);
        assert_eq!(rows(pool, sandbox).await?, before);
    }

    assert!(record_pod_metadata(pool, sandbox, 2, Some("uid-2"), None).await?);
    let created = vec![
        before[0].clone(),
        row(2, "created", true, (Some("uid-2"), None), None, true),
    ];
    assert_eq!(rows(pool, sandbox).await?, created);
    // An execution already current is not written again.
    assert!(!record_pod_metadata(pool, sandbox, 2, Some("uid-2"), None).await?);
    assert_eq!(rows(pool, sandbox).await?, created);

    assert!(record_pod_metadata(pool, sandbox, 2, Some("uid-2"), Some("10.0.0.5")).await?);
    assert_eq!(
        rows(pool, sandbox).await?[1],
        row(
            2,
            "running",
            true,
            (Some("uid-2"), Some("10.0.0.5")),
            None,
            true
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_name_is_recorded_only_while_an_active_execution_exists() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 1, "replaced", false, Some("node-a")).await?;
    let retired = rows(pool, sandbox).await?;

    assert!(!record_node_name(pool, sandbox, "node-b").await?);
    assert_eq!(rows(pool, sandbox).await?, retired);

    insert_execution(pool, sandbox, 2, "running", true, Some("node-a")).await?;
    assert!(record_node_name(pool, sandbox, "node-b").await?);
    let moved = vec![
        retired[0].clone(),
        row(2, "running", true, (None, None), Some("node-b"), false),
    ];
    assert_eq!(rows(pool, sandbox).await?, moved);
    // Unchanged, it still reports the active execution, and writes nothing:
    // a write would move the `running` row's `updated_at` to a later
    // transaction's `NOW()`.
    let last_updated = updated_at(pool, sandbox, 2).await?;
    assert!(record_node_name(pool, sandbox, "node-b").await?);
    assert_eq!(rows(pool, sandbox).await?, moved);
    assert_eq!(updated_at(pool, sandbox, 2).await?, last_updated);

    // `record_node_name_fenced` likewise skips rewriting the execution row when
    // `k8s_node_name` already matches (leaving `xmin` intact) while still
    // returning `Some(updated_at)`, and returns `None` on a lost UID fence.
    let xmin_before: String = sqlx::query_scalar(sql!(
        "SELECT xmin::TEXT FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        record_node_name_fenced(pool, sandbox, 2, Some("uid-wrong"), "node-b").await?,
        None
    );
    assert_eq!(
        record_node_name_fenced(pool, sandbox, 2, None, "node-b").await?,
        Some(last_updated)
    );
    let xmin_after: String = sqlx::query_scalar(sql!(
        "SELECT xmin::TEXT FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert_eq!(xmin_after, xmin_before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repair_creating_pod_uid_never_overwrites_materialized_reset_or_running_row() -> TestResult
{
    use crate::k8s_executions::claim::reset_to_creating;
    use crate::k8s_executions::pod_placement::claim_pod_materialization;

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 1, "replaced", false, None).await?;
    insert_execution(pool, sandbox, 2, "creating", true, None).await?;
    let initial = rows(pool, sandbox).await?;
    let initial_updated = updated_at(pool, sandbox, 2).await?;

    // Blank UIDs and retired/missing generations never mutate the row.
    let mut tx = pool.begin().await?;
    assert!(!repair_creating_pod_uid(&mut *tx, sandbox, 1, "uid-a", initial_updated).await?);
    assert!(!repair_creating_pod_uid(&mut *tx, sandbox, 3, "uid-a", initial_updated).await?);
    assert!(!repair_creating_pod_uid(&mut *tx, sandbox, 2, "   ", initial_updated).await?);
    tx.commit().await?;
    assert_eq!(rows(pool, sandbox).await?, initial);

    // If a same-generation reset (`reset_to_creating`) runs on an unmaterialized
    // `creating` row after pod A observed `initial_updated`, `updated_at`
    // strictly advances so stale `uid-a` repair is rejected while successor
    // `uid-b` repair with `after_reset` succeeds.
    assert!(reset_to_creating(pool, sandbox, 2, None).await?);
    let after_reset = updated_at(pool, sandbox, 2).await?;
    assert!(after_reset > initial_updated);
    let mut tx = pool.begin().await?;
    assert!(!repair_creating_pod_uid(&mut *tx, sandbox, 2, "uid-a", initial_updated).await?);
    assert!(repair_creating_pod_uid(&mut *tx, sandbox, 2, "uid-b", after_reset).await?);
    tx.commit().await?;
    assert_eq!(
        rows(pool, sandbox).await?[1],
        row(2, "created", true, (Some("uid-b"), None), None, true)
    );

    // Once a pod UID was recorded (`pod_materialized_at IS NOT NULL`) and
    // `claim_pod_materialization` resets the row back to `creating` (`pod_uid = NULL`),
    // `repair_creating_pod_uid` refuses even with the post-reset `updated_at`.
    let mut tx = pool.begin().await?;
    assert!(claim_pod_materialization(&mut *tx, sandbox, 2).await?);
    tx.commit().await?;
    let post_claim_updated = updated_at(pool, sandbox, 2).await?;
    let mut tx = pool.begin().await?;
    assert!(!repair_creating_pod_uid(&mut *tx, sandbox, 2, "uid-b", post_claim_updated).await?);
    tx.commit().await?;

    // Once another materializer has recorded `uid-c` and `10.0.0.2` (`running`),
    // a stale repair attempt returns `false` and leaves the row intact.
    assert!(record_pod_metadata(pool, sandbox, 2, Some("uid-c"), Some("10.0.0.2")).await?);
    let running_snapshot = rows(pool, sandbox).await?;
    let running_updated = updated_at(pool, sandbox, 2).await?;
    let mut tx = pool.begin().await?;
    assert!(!repair_creating_pod_uid(&mut *tx, sandbox, 2, "uid-a", running_updated).await?);
    tx.commit().await?;
    assert_eq!(rows(pool, sandbox).await?, running_snapshot);
    assert_eq!(updated_at(pool, sandbox, 2).await?, running_updated);
    Ok(())
}
