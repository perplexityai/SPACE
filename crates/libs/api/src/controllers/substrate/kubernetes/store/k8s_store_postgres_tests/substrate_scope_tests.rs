//! Kubernetes node inventory writes only Kubernetes (`'k8s'`) node rows.
//!
//! Each test pairs a Kubernetes node with an otherwise identical node whose
//! `managed_by` is `'space'`, so only the `managed_by` predicates decide
//! which one the inventory statements act on.

use pplx_space_common_core::types::NodeId;
use tokio_util::task::AbortOnDropHandle;

use super::*;

async fn relabel_node(pool: &PgPool, node_id: NodeId, managed_by: &str) -> TestResult {
    sqlx::query(sql!("UPDATE nodes SET managed_by = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(managed_by)
        .execute(pool)
        .await?;
    Ok(())
}

/// Registers a ready Kubernetes worker whose heartbeat and Kubernetes
/// registration are `age` old, so a membership snapshot that omits it may
/// demote it.
async fn insert_stale_ready_k8s_node(
    fixture: &PostgresStore,
    node_id: NodeId,
    node_name: &str,
    age: Duration,
) -> TestResult {
    fixture
        .store
        .persist_k8s_node_inventory(
            node_name,
            &format!("uid-{node_id}"),
            node_id,
            NodeRole::Worker,
            Some("10.0.0.60"),
            NodeStateWrite::Manage(NodeLifecycleState::Ready),
        )
        .await?;
    let age_secs = i64::try_from(age.as_secs())?;
    sqlx::query(sql!(
        "UPDATE nodes SET last_heartbeat = NOW() - ($2 * INTERVAL '1 second') WHERE id = $1"
    ))
    .bind(node_id)
    .bind(age_secs)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE node_k8s SET updated_at = NOW() - ($2 * INTERVAL '1 second') WHERE node_id = $1"
    ))
    .bind(node_id)
    .bind(age_secs)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn node_state(pool: &PgPool, node_id: NodeId) -> TestResult<(String, String)> {
    Ok(
        sqlx::query_as(sql!("SELECT managed_by, state FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(pool)
            .await?,
    )
}

/// A Direct node that still has a `node_k8s` row is never a demotion
/// candidate. With one demotion per tick, the older Direct node would
/// otherwise take the slot and the Kubernetes node would stay `ready`.
#[tokio::test(flavor = "current_thread")]
async fn demotion_selects_only_kubernetes_nodes() -> TestResult {
    let fixture = postgres_store().await?;
    let direct = NodeId::new();
    let kubernetes = NodeId::new();
    let direct_name = format!("space-test-direct-{}", direct.as_uuid().simple());
    let kubernetes_name = format!("space-test-k8s-{}", kubernetes.as_uuid().simple());
    insert_stale_ready_k8s_node(&fixture, direct, &direct_name, Duration::from_secs(7200)).await?;
    insert_stale_ready_k8s_node(
        &fixture,
        kubernetes,
        &kubernetes_name,
        Duration::from_secs(3600),
    )
    .await?;
    relabel_node(&fixture.pool, direct, "space").await?;

    let demoted = fixture
        .store
        .demote_unobserved_k8s_nodes(
            &["space-test-observed".to_string()],
            Duration::from_secs(60),
            1,
        )
        .await?;

    assert_eq!(demoted, vec![kubernetes_name]);
    assert_eq!(
        node_state(&fixture.pool, kubernetes).await?,
        ("k8s".to_string(), "registered".to_string())
    );
    assert_eq!(
        node_state(&fixture.pool, direct).await?,
        ("space".to_string(), "ready".to_string())
    );
    Ok(())
}

/// The demotion UPDATE re-checks `managed_by` on the row it locks. A writer
/// holds the candidate's row lock with an uncommitted relabel; the demotion's
/// candidate scan sees the committed `'k8s'` row, its UPDATE waits on the
/// lock, then re-evaluates the committed row. A relabel to `'space'` is
/// skipped; a write that keeps `'k8s'` is demoted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn demotion_rechecks_the_substrate_of_the_row_it_locks() -> TestResult {
    for (relabel_to, expected) in [("space", None), ("k8s", Some("registered"))] {
        let fixture = postgres_store_with_max_connections(4).await?;
        let node_id = NodeId::new();
        let node_name = format!("space-test-relabel-{}", node_id.as_uuid().simple());
        insert_stale_ready_k8s_node(&fixture, node_id, &node_name, Duration::from_secs(3600))
            .await?;

        let mut writer = fixture.pool.begin().await?;
        let writer_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *writer)
            .await?;
        sqlx::query(sql!("UPDATE nodes SET managed_by = $2 WHERE id = $1"))
            .bind(node_id)
            .bind(relabel_to)
            .execute(&mut *writer)
            .await?;

        // Aborted if the test fails before joining it.
        let store = fixture.store.clone();
        let mut demotion = AbortOnDropHandle::new(tokio::spawn(async move {
            store
                .demote_unobserved_k8s_nodes(
                    &["space-test-observed".to_string()],
                    Duration::from_secs(60),
                    10,
                )
                .await
        }));
        // Commit only once the demotion statement is waiting on this
        // writer's row lock, not on any other lock.
        let waits_on_writer = async {
            loop {
                let blocked: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (
                         SELECT 1
                           FROM pg_stat_activity
                          WHERE datname = current_database()
                            AND wait_event_type = 'Lock'
                            AND $1 = ANY(pg_blocking_pids(pid))
                            AND query LIKE '%WITH candidates AS MATERIALIZED%'
                     )"
                ))
                .bind(writer_pid)
                .fetch_one(&fixture.pool)
                .await?;
                if blocked {
                    return Ok::<(), sqlx::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        // Both arms are bounded: the demotion must neither finish before it
        // waits on the writer nor fail to reach that wait.
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                finished = &mut demotion => Err(format!(
                    "relabel to {relabel_to}: the demotion finished without waiting on the writer: \
                     {finished:?}"
                )),
                waited = waits_on_writer => waited.map_err(|error| error.to_string()),
            }
        })
        .await
        .map_err(|_| {
            format!("relabel to {relabel_to}: the demotion never waited on the writer")
        })??;
        writer.commit().await?;
        let demoted = tokio::time::timeout(Duration::from_secs(5), demotion).await???;

        let (managed_by, state) = node_state(&fixture.pool, node_id).await?;
        assert_eq!(managed_by, relabel_to);
        match expected {
            Some(expected_state) => {
                assert_eq!(demoted, vec![node_name], "relabel to {relabel_to}");
                assert_eq!(state, expected_state, "relabel to {relabel_to}");
            }
            None => {
                assert!(demoted.is_empty(), "relabel to {relabel_to}: {demoted:?}");
                assert_eq!(state, "ready", "relabel to {relabel_to}");
            }
        }
    }
    Ok(())
}

/// The inventory upsert writes new rows as Kubernetes nodes, updates an
/// existing Kubernetes row, and refuses a Direct row at the same id, leaving
/// it unchanged.
#[tokio::test(flavor = "current_thread")]
async fn inventory_upsert_writes_only_kubernetes_rows() -> TestResult {
    let fixture = postgres_store().await?;
    let fresh = NodeId::new();
    let kubernetes = NodeId::new();
    let direct = NodeId::new();
    for (node_id, managed_by) in [(kubernetes, "k8s"), (direct, "space")] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.61'::INET, 'worker', $2, 'registered')"
        ))
        .bind(node_id)
        .bind(managed_by)
        .execute(&fixture.pool)
        .await?;
    }
    let persist = |node_id: NodeId| {
        let store = fixture.store.clone();
        async move {
            store
                .persist_k8s_node_inventory(
                    &format!("space-test-upsert-{}", node_id.as_uuid().simple()),
                    &format!("uid-{node_id}"),
                    node_id,
                    NodeRole::Worker,
                    Some("10.0.0.62"),
                    NodeStateWrite::Manage(NodeLifecycleState::Ready),
                )
                .await
        }
    };
    let row = |node_id: NodeId| {
        let pool = fixture.pool.clone();
        async move {
            sqlx::query_as::<_, (String, String, String)>(sql!(
                "SELECT managed_by, host(address), state FROM nodes WHERE id = $1"
            ))
            .bind(node_id)
            .fetch_one(&pool)
            .await
        }
    };
    let k8s_registrations = |node_id: NodeId| {
        let pool = fixture.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*) FROM node_k8s WHERE node_id = $1"))
                .bind(node_id)
                .fetch_one(&pool)
                .await
        }
    };

    assert!(persist(fresh).await?);
    assert_eq!(
        row(fresh).await?,
        ("k8s".into(), "10.0.0.62".into(), "ready".into())
    );

    assert!(persist(kubernetes).await?);
    assert_eq!(
        row(kubernetes).await?,
        ("k8s".into(), "10.0.0.62".into(), "ready".into())
    );

    let refused = persist(direct).await;
    assert!(
        matches!(refused, Err(SubstrateError::Failed { .. })),
        "{refused:?}"
    );
    assert_eq!(
        row(direct).await?,
        ("space".into(), "10.0.0.61".into(), "registered".into())
    );
    assert_eq!(k8s_registrations(direct).await?, 0);
    Ok(())
}
