//! The `failed` node-report projection racing another writer of the same
//! control-plane Postgres `public.sandboxes` row. Each case holds the other
//! writer's row lock, proves the projection waits on it (`pg_blocking_pids`),
//! commits the write, and asserts the row the projection leaves and the
//! transitions it reports. The node-local sbx-mgr SQLite store is not involved.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db_with_max_connections};
use pplx_space_test_env::TestResult;
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Transaction};

use super::project_node_published_failed_sandboxes;

/// Bounds both the wait for the projection to block and its completion.
const RACE_BUDGET: Duration = Duration::from_secs(10);

/// The persisted Postgres `public.sandboxes.lifecycle` and `active_node_id`.
async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<(String, Option<NodeId>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// A `k8s` row placed on `node_id` in `lifecycle`.
async fn insert_k8s_sandbox(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_override_json,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'on_node', $3, $3, 'k8s', $4, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(node_id)
    .bind(Json(pplx_space_net_common::NetworkPolicyV1::default()))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Opens the conflicting writer's transaction and returns it with its backend
/// pid; the caller's statements on it hold their row locks until it ends.
async fn begin_holder(pool: &PgPool) -> TestResult<(Transaction<'static, Postgres>, i32)> {
    let mut holder = pool.begin().await?;
    let pid = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *holder)
        .await?;
    Ok((holder, pid))
}

/// Whether backend `waiter` is waiting on a lock `holder` holds.
async fn is_blocked_by(pool: &PgPool, waiter: i32, holder: i32) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar(sql!("SELECT $1 = ANY(pg_blocking_pids($2))"))
            .bind(holder)
            .bind(waiter)
            .fetch_one(pool)
            .await?,
    )
}

async fn wait_until_blocked(pool: &PgPool, waiter: i32, holder: i32) -> TestResult {
    tokio::time::timeout(RACE_BUDGET, async {
        while !is_blocked_by(pool, waiter, holder).await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        TestResult::Ok(())
    })
    .await
    .map_err(|_| format!("backend {waiter} never waited on backend {holder}"))?
}

/// The concurrent writer that commits while the `failed` projection waits.
#[derive(Clone, Copy, Debug)]
enum FailedRace {
    /// A user DELETE: lock the row, then persist `deleting` intent.
    Delete,
    /// A quarantine hold committed for the row.
    Quarantine,
    /// A suspend that finalized and unplaced the row.
    SuspendUnplaced,
    /// A pause: the row still qualifies for `failed`.
    Pause,
}

impl FailedRace {
    async fn write(
        self,
        holder: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> TestResult {
        if matches!(self, Self::Delete) {
            sqlx::query(sql!(
                "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
            ))
            .bind(sandbox_id)
            .execute(&mut **holder)
            .await?;
        }
        match self {
            Self::Delete => sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
            )),
            Self::Quarantine => sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'quarantined' WHERE id = $1"
            )),
            Self::SuspendUnplaced => sqlx::query(sql!(
                "UPDATE sandboxes
                    SET lifecycle = 'suspended', placement = 'unplaced', active_node_id = NULL
                  WHERE id = $1"
            )),
            Self::Pause => sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1"
            )),
        }
        .bind(sandbox_id)
        .execute(&mut **holder)
        .await?;
        Ok(())
    }
}

/// A `failed` report for a `started` row reaches the row while another writer
/// holds it. The projection decides on what that writer commits: a `deleting`,
/// a `quarantined` or a row unplaced as `suspended` stands, and a `paused` row
/// still becomes `failed`, reported as leaving `paused`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_projection_decides_on_the_row_committed_while_it_waits() -> TestResult {
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;

    let cases = [
        (FailedRace::Delete, "deleting", Some(node_id), None),
        (FailedRace::Quarantine, "quarantined", Some(node_id), None),
        (FailedRace::SuspendUnplaced, "suspended", None, None),
        (FailedRace::Pause, "failed", Some(node_id), Some("paused")),
    ];
    let mut mismatches = Vec::new();
    for (race, expected_lifecycle, expected_node, expected_from) in cases {
        let sandbox_id = insert_k8s_sandbox(pool, node_id, "started").await?;
        let (mut holder, holder_pid) = begin_holder(pool).await?;
        race.write(&mut holder, sandbox_id).await?;

        let mut projection_tx = pool.begin().await?;
        let waiter_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *projection_tx)
            .await?;
        let rows_json = Json(serde_json::json!([
            { "sandbox_id": sandbox_id.to_string(), "status": "failed" }
        ]));
        let projection = tokio::spawn(async move {
            let moved =
                project_node_published_failed_sandboxes(&mut projection_tx, node_id, rows_json)
                    .await?;
            projection_tx.commit().await?;
            TestResult::<Vec<_>>::Ok(moved)
        });
        wait_until_blocked(pool, waiter_pid, holder_pid).await?;
        holder.commit().await?;
        let moved = tokio::time::timeout(RACE_BUDGET, projection)
            .await
            .map_err(|_| format!("{race:?}: projection did not finish"))???;

        let expected_moved = expected_from
            .map(|from| vec![(sandbox_id, from.to_owned())])
            .unwrap_or_default();
        let (lifecycle, active_node_id) = row(pool, sandbox_id).await?;
        let actual = (lifecycle.as_str(), active_node_id, moved);
        let expected = (expected_lifecycle, expected_node, expected_moved);
        if actual != expected {
            mismatches.push(format!(
                "{race:?}: (lifecycle, active node, reported moves) {actual:?}, expected {expected:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
