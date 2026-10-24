//! Plan regression for [`gone_node_giveup_claim_query`] on a node whose
//! pinned history is large and almost entirely durable.
//!
//! The behavioural tests in `inflight_upload_terminate_gate_tests` cover which
//! rows the give-up claims and flips, over a handful of sandboxes. They can't
//! see how the claim's durability check is planned, and that check now runs
//! over the node's whole pinned `suspended` history to find the few rows
//! without a durable copy.

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::PgPool;
use uuid::Uuid;

use super::gone_node_giveup_claim_query;

use pplx_space_test_env::TestResult;

/// Suspended rows pinned to the node, each with a durable backup: the node's
/// history, which the claim must leave out.
const DURABLE_HISTORY: usize = 3000;

/// Insert `count` suspended sandboxes pinned to `node`.
async fn insert_suspended_rows(pool: &PgPool, node: Uuid, count: usize) -> TestResult<Vec<Uuid>> {
    let ids: Vec<Uuid> = (0..count).map(|_| Uuid::now_v7()).collect();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, network_policy_generation
         )
         SELECT
             id,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s',
             $2, NOW() - INTERVAL '30 days', 1
           FROM UNNEST($1::uuid[]) AS t(id)"
    ))
    .bind(&ids)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(ids)
}

/// Register a `ready` full snapshot and its storage location for every
/// sandbox in `sandbox_ids`, making each one durable.
async fn insert_ready_backups(pool: &PgPool, sandbox_ids: &[Uuid], node: Uuid) -> TestResult {
    sqlx::query(sql!(
        "WITH inserted AS (
             INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
             SELECT space_uuid_v7(), t.sandbox_id, 'full', 'ready', $2, NOW()
               FROM UNNEST($1::uuid[]) AS t(sandbox_id)
             RETURNING id
         )
         INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         SELECT id, 's3://test-bucket/' || id FROM inserted"
    ))
    .bind(sandbox_ids)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

/// One `EXPLAIN (ANALYZE, FORMAT JSON)` plan node, typed so a traversal that
/// misses the root array, the `Plan` wrapper or the `Plans` nesting isn't
/// expressible.
#[derive(Debug, serde::Deserialize)]
struct Explained {
    #[serde(rename = "Plan")]
    plan: PlanNode,
}

#[derive(Debug, serde::Deserialize)]
struct PlanNode {
    #[serde(rename = "Node Type")]
    node_type: String,
    #[serde(rename = "Relation Name")]
    relation: Option<String>,
    #[serde(rename = "Plans", default)]
    children: Vec<PlanNode>,
}

impl PlanNode {
    /// Is the durability probe planned inside a scalar subquery? True iff some
    /// `Result` node, which is how Postgres realises a scalar subquery, has the
    /// `snapshot_storage_locations` access beneath it. Unfenced, that access
    /// hangs off a join with no `Result` above it.
    fn durability_probe_is_fenced(&self) -> bool {
        fn reaches_storage_locations(node: &PlanNode) -> bool {
            node.relation.as_deref() == Some("snapshot_storage_locations")
                || node.children.iter().any(reaches_storage_locations)
        }
        (self.node_type == "Result" && reaches_storage_locations(self))
            || self
                .children
                .iter()
                .any(PlanNode::durability_probe_is_fenced)
    }
}

/// The claim's durability check stays inside a scalar subquery on a node with
/// thousands of durable pinned rows and one casualty, and the claim still
/// returns exactly that casualty.
///
/// The assertion is structural rather than a timing or a cost, because the
/// failure it guards against is a plan choice: a bare `NOT EXISTS` can be
/// pulled up into a hash or merge anti-join over the whole `snapshots` ⋈
/// `snapshot_storage_locations` product, and routine autoanalyze can move the
/// planner onto that plan. A scalar subquery always plans as a `Result` node, at
/// a thousand rows or at millions. Remove the `(SELECT …)` wrapper and the
/// durability join moves out from under it.
#[tokio::test(flavor = "current_thread")]
async fn the_claim_keeps_its_durability_check_fenced() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = Uuid::now_v7();
    let history = insert_suspended_rows(pool, node, DURABLE_HISTORY).await?;
    insert_ready_backups(pool, &history, node).await?;
    let casualty = insert_suspended_rows(pool, node, 1).await?;
    sqlx::query(sql!(
        "ANALYZE sandboxes, snapshots, snapshot_storage_locations"
    ))
    .execute(pool)
    .await?;

    let claim = gone_node_giveup_claim_query();
    let claimed = sqlx::query_scalar::<_, Uuid>(sqlx::AssertSqlSafe(sql!(claim.as_str())))
        .bind(node)
        .fetch_all(pool)
        .await?;
    assert_eq!(
        claimed, casualty,
        "the claim must return exactly the node's one row without a durable copy; \
         {DURABLE_HISTORY} rows of durable history must not leak into it"
    );

    // `TIMING OFF`: only the plan's shape is read, and per-node timing over
    // thousands of subplan executions costs more than the plan does.
    let statement = format!("EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) {claim}");
    let raw: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    let mut explained: Vec<Explained> = serde_json::from_value(raw)?;
    let plan = explained.pop().ok_or("EXPLAIN returned no plan")?.plan;
    assert!(
        plan.durability_probe_is_fenced(),
        "the claim's durability check must be planned inside a scalar subquery (a \
         `Result` node above the `snapshot_storage_locations` access), not as a join \
         the planner is free to re-cost into a hash or merge anti-join; plan was {plan:?}"
    );
    Ok(())
}
