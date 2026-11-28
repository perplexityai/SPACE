//! The materialization writes record a pod only on the sandbox's active
//! execution, and the pod metadata write only on the generation the caller
//! names.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{record_node_name, record_pod_metadata};

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

/// The execution's `updated_at`, as text.
async fn updated_at(pool: &PgPool, sandbox_id: SandboxId, generation: i64) -> TestResult<String> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT updated_at::TEXT
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
    Ok(())
}
