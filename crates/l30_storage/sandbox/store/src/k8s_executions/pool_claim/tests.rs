//! [`insert_first_generation`] records the claimed slot's pod as the
//! sandbox's first, active generation, and refuses a sandbox that already has
//! one.

use pplx_space_cluster_pool::ClaimedSlotBindSource;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::insert_first_generation;

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'creating', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    Ok(id)
}

fn source(pod_name: &str, pod_uid: Option<&str>) -> ClaimedSlotBindSource {
    ClaimedSlotBindSource {
        pod_name: pod_name.to_string(),
        pod_namespace: "sandboxes".to_string(),
        pod_uid: pod_uid.map(str::to_string),
        pod_ip: Some("10.1.0.7".to_string()),
        k8s_node_name: Some("node-a".to_string()),
    }
}

/// `(generation, pod_name, pod_namespace, pod_uid, host(pod_ip),
/// k8s_node_name, state, active, route_activation_id,
/// pod_materialized_at IS NOT NULL)` of every execution of `sandbox_id`.
type ExecutionColumns = (
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    bool,
    Option<Uuid>,
    bool,
);

async fn read_executions(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<Vec<ExecutionColumns>> {
    Ok(sqlx::query_as::<_, ExecutionColumns>(sql!(
        "SELECT generation, pod_name, pod_namespace, pod_uid, host(pod_ip), k8s_node_name,
                state, active, route_activation_id, pod_materialized_at IS NOT NULL
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn insert_first_generation_records_the_slots_pod_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let slot_id = Uuid::now_v7();

    let sandbox = insert_sandbox(pool).await?;
    let mut tx = pool.begin().await?;
    insert_first_generation(&mut *tx, sandbox, &source("pod-a", Some("uid-a")), slot_id).await?;
    tx.commit().await?;
    let inserted = read_executions(pool, sandbox).await?;
    assert_eq!(
        inserted,
        [(
            1,
            "pod-a".to_string(),
            "sandboxes".to_string(),
            Some("uid-a".to_string()),
            Some("10.1.0.7".to_string()),
            Some("node-a".to_string()),
            "running".to_string(),
            true,
            Some(slot_id),
            true,
        )]
    );

    // A second claim for the same sandbox fails on the unique indexes and
    // leaves the first.
    let mut tx = pool.begin().await?;
    let error = sqlx::Error::from(
        insert_first_generation(&mut *tx, sandbox, &source("pod-b", Some("uid-b")), slot_id)
            .await
            .expect_err("the sandbox already has generation 1"),
    );
    assert!(
        matches!(&error, sqlx::Error::Database(db_error) if db_error.code().as_deref() == Some("23505")),
        "{error}"
    );
    tx.rollback().await?;
    assert_eq!(read_executions(pool, sandbox).await?, inserted);

    // A slot pod without a UID has not materialized.
    let unmaterialized = insert_sandbox(pool).await?;
    let mut tx = pool.begin().await?;
    insert_first_generation(
        &mut *tx,
        unmaterialized,
        &source("pod-c", Some(" ")),
        slot_id,
    )
    .await?;
    tx.commit().await?;
    assert!(!read_executions(pool, unmaterialized).await?[0].9);
    Ok(())
}
