//! Scale regression for [`pinned_local_only_sandboxes_query`].
//!
//! The behavioural tests in `durable_upload_gate_tests` cover which rows the
//! query returns, over fixtures of a handful of sandboxes. They cannot see the
//! cost of scanning a node's lifetime history. `target_node_id` is a placement
//! diagnostic that suspend never clears, so a long-lived node accumulates
//! every sandbox ever suspended from it. A slow blocker query can exhaust the
//! statement timeout before the reconciler reaches `spawn_evacuations`.
//!
//! Two independent pathologies produced that, and the fixtures below pin the
//! mechanism of each rather than a wall-clock, which would be a flake:
//!
//! 1. The in-flight-upload predicate used to be two sibling `EXISTS`
//!    expressions under a `NOT (A OR B)`. Postgres cannot flatten that to an
//!    anti-join, and the legacy branch correlates through the inequality
//!    `sn.timestamp >= s.placement_changed_at`, so it cannot become a hashed
//!    subplan either: it re-executed once per outer row, rescanning every
//!    `state='creating'` full snapshot in the cluster each time.
//!    [`inflight_suspend_upload_exists`] now nests both branches inside one
//!    correlated `EXISTS`, which is logically identical and reachable as an
//!    anti-join. [`pathological_predicate_reexecutes_per_row_but_merged_does_not`]
//!    asserts on the re-execution count directly.
//!
//! 2. Without a `LIMIT` the planner priced the durability `NOT EXISTS` as a
//!    hash anti-join over the whole `snapshots` ⋈ `snapshot_storage_locations`
//!    product. [`enumeration_is_capped_and_still_finds_non_durable_rows`] pins
//!    the cap that gives the planner a `tuple_fraction` instead, together with
//!    the property that capping does not change *which* rows are eligible.

use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_portability::snapshot_durable_for_source_release;
use uuid::Uuid;

use super::*;

use pplx_space_test_env::TestResult;

/// Suspended rows pinned to the draining node that already have a durable
/// backup. This is the node's history: large, and entirely uninteresting to
/// the work queue. Sized so the per-row re-execution in pathology 1 is
/// unambiguous against a materialised scan without making the fixture slow.
const DURABLE_HISTORY: usize = 900;

/// Suspended rows pinned to the node with no snapshot at all — the genuine
/// work queue. Deliberately more than one so an assertion about the returned
/// set cannot pass on an off-by-one, and fewer than the cap so the cap and the
/// answer can be distinguished.
const GENUINE_WORK: usize = 7;

/// `state='creating'` full snapshots elsewhere in the cluster. These belong to
/// *other* nodes' sandboxes and are never an answer to this node's query, but
/// they are exactly the rows the pathological predicate re-read for every
/// outer row.
const FOREIGN_INFLIGHT_UPLOADS: usize = 120;

/// Insert `count` suspended sandboxes pinned to `node` with a NULL
/// `current_local_artifact_id` and a non-NULL `placement_changed_at`.
///
/// Both are the production shape and both are load-bearing for pathology 1:
/// the NULL artifact id selects the legacy branch of the in-flight predicate,
/// and `placement_changed_at` supplies the inequality correlation that makes
/// that branch unhashable. A fixture of artifact-id-carrying rows would take
/// the exact branch, which Postgres *can* hash, and would not reproduce the
/// defect.
async fn insert_legacy_suspended_rows(
    pool: &PgPool,
    node: Uuid,
    count: usize,
) -> TestResult<Vec<Uuid>> {
    let ids: Vec<Uuid> = (0..count).map(|_| Uuid::now_v7()).collect();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, lifecycle_changed_at,
             network_policy_generation
         )
         SELECT
             id,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s',
             $2, NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days', 1
           FROM UNNEST($1::uuid[]) AS t(id)"
    ))
    .bind(&ids)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(ids)
}

/// Register a `state='ready'` full snapshot plus its storage-location row for
/// every sandbox in `sandbox_ids`, making each one durable and therefore
/// ineligible for the work queue.
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

/// Create `count` sandboxes pinned to *other* nodes, each with an in-flight
/// (`state='creating'`) full snapshot. None of them can ever be an answer to
/// `node`'s query; they exist only to be the population the pathological
/// predicate rescans.
async fn insert_foreign_inflight_uploads(pool: &PgPool, count: usize) -> TestResult {
    let other_node = Uuid::now_v7();
    // Reuses the same seeder as the target node's rows rather than retyping its
    // column list: the module's claim that a NULL `current_local_artifact_id`
    // and a non-NULL `placement_changed_at` are load-bearing for pathology 1
    // only holds if both populations are actually seeded that way.
    let ids = insert_legacy_suspended_rows(pool, other_node, count).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         SELECT space_uuid_v7(), t.sandbox_id, 'full', 'creating', $2, NOW()
           FROM UNNEST($1::uuid[]) AS t(sandbox_id)"
    ))
    .bind(&ids)
    .bind(other_node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Build the node's history plus its genuine work queue, then `ANALYZE` so the
/// planner sees the fixture rather than an empty-table default. Returns the
/// ids that must come back from the query.
async fn seed_node_with_history(pool: &PgPool, node: Uuid) -> TestResult<Vec<Uuid>> {
    let history = insert_legacy_suspended_rows(pool, node, DURABLE_HISTORY).await?;
    insert_ready_backups(pool, &history, node).await?;
    let work = insert_legacy_suspended_rows(pool, node, GENUINE_WORK).await?;
    insert_foreign_inflight_uploads(pool, FOREIGN_INFLIGHT_UPLOADS).await?;
    sqlx::query(sql!(
        "ANALYZE sandboxes, snapshots, snapshot_storage_locations"
    ))
    .execute(pool)
    .await?;
    Ok(work)
}

/// One `EXPLAIN (ANALYZE, FORMAT JSON)` statement's result.
///
/// Typed rather than walked as a `serde_json::Value`, following the shape in
/// `pplx_space_api`'s `single_node_occupancy` tests: serde handles the root
/// array, the `Plan` wrapper and the `Plans` nesting declaratively, so a
/// traversal that silently misses one of the three — which reads exactly like
/// "the pathology is absent" — is not expressible.
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
    #[serde(rename = "Filter")]
    filter: Option<String>,
    #[serde(rename = "Index Cond")]
    index_cond: Option<String>,
    #[serde(rename = "Recheck Cond")]
    recheck_cond: Option<String>,
    #[serde(rename = "Actual Loops", default)]
    loops: u64,
    #[serde(rename = "Plans", default)]
    children: Vec<PlanNode>,
}

impl PlanNode {
    /// Sum of `Actual Loops` over every node that reads `snapshots` for
    /// `state='creating'` — i.e. how many times the in-flight-upload
    /// predicate's scan was executed.
    ///
    /// This is the quantity the fix changes. A wall-clock assertion would be a
    /// flake and a buffer-count assertion moves with the page layout; the
    /// re-execution count is the mechanism itself, and it is the same number
    /// whether the fixture holds a thousand rows or millions.
    /// Is the durability probe planned inside a scalar subquery?
    ///
    /// True iff some `Result` node — which is how Postgres realises a scalar
    /// subquery — has the `snapshot_storage_locations` access somewhere beneath
    /// it. Unfenced, that access hangs off a top-level join instead.
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

    fn inflight_scan_executions(&self) -> u64 {
        let reads_snapshots = self.relation.as_deref() == Some("snapshots");
        let mentions_creating = [&self.filter, &self.index_cond, &self.recheck_cond]
            .into_iter()
            .flatten()
            .any(|text| text.contains("'creating'"));
        let own = if reads_snapshots && mentions_creating {
            self.loops
        } else {
            0
        };
        own + self
            .children
            .iter()
            .map(PlanNode::inflight_scan_executions)
            .sum::<u64>()
    }
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` one statement against `node`.
async fn explain(pool: &PgPool, query: &str, node: Uuid) -> TestResult<PlanNode> {
    // `TIMING OFF`: the assertion reads `Actual Loops`, which per-node timing
    // does not affect, and the pathological arm runs its subplan ~900 times —
    // instrumenting every one of those costs more than the plan does.
    let statement = format!("EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) {query}");
    let raw: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    let mut explained: Vec<Explained> = serde_json::from_value(raw)?;
    Ok(explained.pop().ok_or("EXPLAIN returned no plan")?.plan)
}

/// The query as it was written before this change: the in-flight predicate
/// composed as two sibling `EXISTS` expressions through
/// `artifact_identity_or_legacy`, with no `LIMIT`. Kept here so the regression
/// is a differential against the real prior statement rather than against a
/// remembered number.
fn pathological_pinned_local_only_query() -> String {
    let snapshot_exists = snapshot_durable_for_source_release("s");
    let exact = "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = s.id\n\
                AND sn.local_artifact_id = s.current_local_artifact_id\n\
                AND sn.artifact_kind = s.current_local_artifact_kind\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
         )"
    .to_string();
    let legacy = "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = s.id\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
                AND (\n\
                    s.placement_changed_at IS NULL\n\
                    OR sn.timestamp >= s.placement_changed_at\n\
                )\n\
         )"
    .to_string();
    // `artifact_identity_or_legacy`'s composition, inlined: the shared helper is
    // private, and pinning the prior statement here keeps the differential
    // readable at the one place it is used rather than widening the portability
    // crate's API for a test.
    let inflight_upload = format!(
        "(\n\
             (s.current_local_artifact_id IS NOT NULL AND {exact})\n\
             OR (s.current_local_artifact_id IS NULL AND {legacy})\n\
         )"
    );
    format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.target_node_id = $1
            AND s.lifecycle = 'suspended'
            AND NOT {snapshot_exists}
            AND NOT {inflight_upload}"
    )
}

/// Pathology 1, pinned at its mechanism: the sibling-`EXISTS` in-flight
/// predicate re-executes its `state='creating'` scan once per pinned row,
/// while the merged single-`EXISTS` form executes it a bounded number of
/// times regardless of how much history the node carries.
///
/// The fixture's node holds `DURABLE_HISTORY + GENUINE_WORK` pinned rows and
/// the cluster holds `FOREIGN_INFLIGHT_UPLOADS` in-flight uploads that belong
/// to no row on it, which is the production shape: work proportional to
/// (history × unrelated cluster state).
#[tokio::test(flavor = "current_thread")]
async fn pathological_predicate_reexecutes_per_row_but_merged_does_not() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = Uuid::now_v7();
    seed_node_with_history(pool, node).await?;

    let pinned_rows = (DURABLE_HISTORY + GENUINE_WORK) as u64;

    let pathological = explain(pool, &pathological_pinned_local_only_query(), node).await?;
    let pathological_executions = pathological.inflight_scan_executions();
    // Establish that the pathology is reproduced here at all — otherwise the
    // comparison below could hold because *neither* form re-executes, and the
    // test would pass with the fix reverted.
    assert!(
        pathological_executions >= pinned_rows,
        "fixture does not reproduce the per-row re-execution the fix removes: the \
         sibling-EXISTS form ran the in-flight scan {pathological_executions} times \
         for {pinned_rows} pinned rows; plan was {pathological:?}"
    );

    let merged = explain(
        pool,
        &pinned_local_only_sandboxes_query(false, PINNED_LOCAL_ONLY_ENUMERATION_CAP),
        node,
    )
    .await?;
    let merged_executions = merged.inflight_scan_executions();
    assert!(
        merged_executions < pinned_rows,
        "the merged in-flight predicate must not re-execute its `creating` scan per \
         pinned row: ran {merged_executions} times for {pinned_rows} pinned rows; \
         plan was {merged:?}"
    );
    Ok(())
}

/// Pathology 2's guard, plus the property that makes capping safe: the cap
/// bounds how many ids one call returns, and every returned id is genuinely
/// non-durable. A cap that changed *eligibility* rather than page size would
/// silently drop sandboxes out of the promote queue.
#[tokio::test(flavor = "current_thread")]
async fn enumeration_is_capped_and_still_finds_non_durable_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = Uuid::now_v7();
    let expected = seed_node_with_history(pool, node).await?;

    let pinned = sqlx::query_scalar::<_, Uuid>(sqlx::AssertSqlSafe(sql!(
        pinned_local_only_sandboxes_query(false, PINNED_LOCAL_ONLY_ENUMERATION_CAP)
    )))
    .bind(node)
    .fetch_all(pool)
    .await?;

    let mut got = pinned.clone();
    got.sort();
    let mut want = expected.clone();
    want.sort();
    assert_eq!(
        got, want,
        "the capped query must return exactly the node's non-durable suspended rows; \
         {DURABLE_HISTORY} rows of durable history must not leak into the work queue"
    );

    // The cap is a page size, not a filter: shrinking it truncates the page
    // and leaves eligibility untouched, so the short page is a prefix of the
    // full answer rather than a different set.
    let short_cap = NonZeroUsize::new(GENUINE_WORK - 3).unwrap();
    let short_page = sqlx::query_scalar::<_, Uuid>(sqlx::AssertSqlSafe(sql!(
        pinned_local_only_sandboxes_query(false, short_cap)
    )))
    .bind(node)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        short_page.len(),
        short_cap.get(),
        "a cap below the backlog must fill exactly one page; got {short_page:?}"
    );
    for id in &short_page {
        assert!(
            want.binary_search(id).is_ok(),
            "capped page returned {id}, which is not one of the non-durable rows"
        );
    }
    Ok(())
}

/// Pathology 2's guard, and the one that decayed. The durability `NOT EXISTS`
/// is wrapped in a scalar subquery so the planner *cannot* realise it as a hash
/// or merge anti-join over the whole `snapshots` ⋈ `snapshot_storage_locations`
/// product — only as a per-row `SubPlan` whose own `sn.sandbox_id = s.id` makes
/// each execution an index probe.
///
/// A `LIMIT` can bias the planner toward that plan, but routine autoanalyze
/// can change row estimates and switch it to a cluster-wide merge anti-join.
/// The guard must therefore be structural rather than a timing or a cost.
///
/// The assertion is scale-free for the same reason the fence is: a scalar
/// subquery always plans as a `Result` node, at a thousand rows or at
/// millions. Remove the `(SELECT …)` wrapper and the durability join
/// moves out to a top-level join node with no `Result` above it, which is what
/// this catches.
#[tokio::test(flavor = "current_thread")]
async fn durability_check_stays_an_unpullable_subquery() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = Uuid::now_v7();
    seed_node_with_history(pool, node).await?;

    let plan = explain(
        pool,
        &pinned_local_only_sandboxes_query(false, PINNED_LOCAL_ONLY_ENUMERATION_CAP),
        node,
    )
    .await?;

    assert!(
        plan.durability_probe_is_fenced(),
        "the durability check must be planned inside a scalar subquery (a `Result` \
         node above the `snapshot_storage_locations` access), not as a join the \
         planner is free to re-cost into a hash or merge anti-join; plan was {plan:?}"
    );
    Ok(())
}
