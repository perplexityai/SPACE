use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::environment_evidence;
use super::test_support::{SnapshotSeed, upsert_snapshot};

/// The predicate for `node`'s snapshot. It decodes into a plain `bool`,
/// which fails on NULL, so every case below also proves it's never NULL.
async fn evidence(pool: &PgPool, node: NodeId) -> TestResult<bool> {
    let statement = sql!(format!(
        "SELECT {} FROM node_status_snapshots nss WHERE nss.node_id = $1",
        environment_evidence("nss")
    ));
    Ok(
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(statement))
            .bind(node)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn a_snapshot_is_evidence_only_while_every_condition_holds() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fresh = SnapshotSeed::default();
    for (seed, expected, why) in [
        (fresh, true, "complete, fresh and received now"),
        (
            SnapshotSeed {
                reachable: false,
                ..fresh
            },
            true,
            "reachability is the caller's to require",
        ),
        (
            SnapshotSeed {
                inventory: None,
                ..fresh
            },
            false,
            "no inventory report",
        ),
        (
            SnapshotSeed {
                inventory: Some("partial"),
                ..fresh
            },
            false,
            "an incomplete inventory",
        ),
        (
            SnapshotSeed {
                stale_in_secs: -1.0,
                ..fresh
            },
            false,
            "a stale snapshot",
        ),
        // Arrays received after the last observation stretch the window
        // past stale_after; the snapshot is stale all the same.
        (
            SnapshotSeed {
                observed_secs_ago: 30.0,
                stale_in_secs: -1.0,
                environment_refs_secs_ago: Some(5.0),
                ..fresh
            },
            false,
            "a stale snapshot whose arrays came after its observation",
        ),
        (
            SnapshotSeed {
                environment_refs_secs_ago: None,
                ..fresh
            },
            false,
            "no environment arrays",
        ),
        // The window is stale_after - observed_at: 60 s here, whatever is
        // left of it.
        (
            SnapshotSeed {
                observed_secs_ago: 50.0,
                stale_in_secs: 10.0,
                environment_refs_secs_ago: Some(55.0),
                ..fresh
            },
            true,
            "arrays received within the window, measured from now",
        ),
        (
            SnapshotSeed {
                observed_secs_ago: 50.0,
                stale_in_secs: 10.0,
                environment_refs_secs_ago: Some(65.0),
                ..fresh
            },
            false,
            "arrays older than the window",
        ),
    ] {
        let node = NodeId::new();
        insert_ready_worker_node(&db.pool, node).await?;
        upsert_snapshot(&db.pool, node, seed).await?;
        assert_eq!(evidence(&db.pool, node).await?, expected, "{why}");
    }
    Ok(())
}
