//! Demoting unobserved ready nodes against Postgres: which candidates the
//! scan picks, and that the `UPDATE`'s guard leaves a candidate another
//! writer changed after the scan alone.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db, pg_test_db_with_max_connections};
use sqlx::PgPool;

use super::*;

const GRACE_SECS: i64 = 60;
const OBSERVED: &str = "space-test-observed";

/// A `ready` Kubernetes worker whose heartbeat and registration are
/// `age_secs` old, registered under its returned name.
async fn ready_node(pool: &PgPool, age_secs: i64) -> TestDbResult<(NodeId, String)> {
    let node_id = NodeId::new();
    let name = format!("space-test-demote-{}", node_id.as_uuid().simple());
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, last_heartbeat)
         VALUES ($1, '10.0.0.70'::INET, 'worker', 'k8s', 'ready',
                 NOW() - ($2 * INTERVAL '1 second'))"
    ))
    .bind(node_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, updated_at)
         VALUES ($1, $2, 'uid-1', NOW() - ($3 * INTERVAL '1 second'))"
    ))
    .bind(node_id)
    .bind(&name)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok((node_id, name))
}

async fn state(pool: &PgPool, node_id: NodeId) -> TestDbResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT state FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Only stale, undrained, `ready` Kubernetes nodes the snapshot doesn't list
/// are demoted and returned, oldest registration first up to the limit.
#[tokio::test]
async fn only_stale_unlisted_ready_kubernetes_nodes_are_demoted() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (oldest, oldest_name) = ready_node(pool, 7200).await?;
    let (older, older_name) = ready_node(pool, 3600).await?;
    let (beyond_limit, _) = ready_node(pool, 1800).await?;
    let (listed, listed_name) = ready_node(pool, 3600).await?;
    let (recent, _) = ready_node(pool, 10).await?;
    let (draining, _) = ready_node(pool, 3600).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_started_at = NOW() WHERE id = $1"
    ))
    .bind(draining)
    .execute(pool)
    .await?;
    let (direct, _) = ready_node(pool, 3600).await?;
    sqlx::query(sql!("UPDATE nodes SET managed_by = 'space' WHERE id = $1"))
        .bind(direct)
        .execute(pool)
        .await?;

    let mut demoted =
        demote_unobserved_ready_nodes(pool, &[OBSERVED.to_string(), listed_name], GRACE_SECS, 2)
            .await?;
    demoted.sort_unstable();
    let mut expected = vec![oldest_name, older_name];
    expected.sort_unstable();
    assert_eq!(demoted, expected);

    for (node_id, expected) in [
        (oldest, "registered"),
        (older, "registered"),
        (beyond_limit, "ready"),
        (listed, "ready"),
        (recent, "ready"),
        (draining, "ready"),
        (direct, "ready"),
    ] {
        assert_eq!(state(pool, node_id).await?, expected, "{node_id}");
    }
    Ok(())
}

/// The losing case. A writer holds the candidate's `nodes` row with an
/// uncommitted change; the demotion's scan reads the committed row, its
/// `UPDATE` waits on the writer, then re-checks the row the writer committed.
/// A heartbeat moved to another stale instant, or a state other than
/// `ready`, leaves the node unchanged and unreturned; a change to a column
/// the guard doesn't read is demoted, which shows the demotion waited.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_candidate_changed_after_the_scan_is_left_alone() -> TestDbResult<()> {
    for (writer_change, expected_state) in [
        (
            sql!(
                "UPDATE nodes SET last_heartbeat = last_heartbeat - INTERVAL '1 second'
                  WHERE id = $1"
            ),
            "ready",
        ),
        (
            sql!("UPDATE nodes SET state = 'unhealthy' WHERE id = $1"),
            "unhealthy",
        ),
        (
            sql!("UPDATE nodes SET address = '10.0.0.71'::INET WHERE id = $1"),
            "registered",
        ),
    ] {
        let db = pg_test_db_with_max_connections(4).await?;
        let (node_id, name) = ready_node(&db.pool, 3600).await?;

        let mut writer = db.pool.begin().await?;
        let writer_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *writer)
            .await?;
        sqlx::query(writer_change)
            .bind(node_id)
            .execute(&mut *writer)
            .await?;

        let pool = db.pool.clone();
        let demotion = tokio::spawn(async move {
            demote_unobserved_ready_nodes(&pool, &[OBSERVED.to_string()], GRACE_SECS, 10).await
        });
        // Commit only once the demotion is waiting on this writer's row lock.
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
                .fetch_one(&db.pool)
                .await?;
                if blocked {
                    return Ok::<(), sqlx::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let waited = tokio::time::timeout(Duration::from_secs(5), waits_on_writer).await;
        if !matches!(waited, Ok(Ok(()))) {
            demotion.abort();
        }
        waited.map_err(|_| "the demotion never waited on the writer")??;
        writer.commit().await?;

        let returned = tokio::time::timeout(Duration::from_secs(5), demotion).await???;
        let demoted = if expected_state == "registered" {
            vec![name]
        } else {
            Vec::new()
        };
        assert_eq!(returned, demoted, "{expected_state}");
        assert_eq!(
            state(&db.pool, node_id).await?,
            expected_state,
            "{expected_state}"
        );
    }
    Ok(())
}
