//! Plan-structure and page-shape regression for
//! [`pinned_local_only_sandboxes_query`].
//!
//! The behavioural tests in `inflight_upload_occupancy_tests` cover *which*
//! rows the `:force-upload-local-snapshots` enumeration returns, over fixtures
//! of two or three sandboxes. They cannot see the defect this module guards,
//! because that defect is not about which rows come back — for the node whose
//! forced drain this query family stalled the answer was correctly zero — but
//! about how many rows Postgres reads to decide that.
//!
//! `target_node_id` is a placement diagnostic that suspend never clears, so a
//! long-lived node accumulates every sandbox ever suspended from it. The
//! statement's `ORDER BY` is load-bearing for the operator-visible
//! deferred-tail contract, and it needs an index carrying both the node
//! predicate and the sort key or the planner takes the ordering from the
//! primary key's own index and filters the whole table to answer "nothing".
//!
//! Three properties pin the fix, all read straight off the plan as rows
//! examined or executions rather than as timings or costs:
//!
//! 1. **The work is bounded by the node, not the table.**
//!    [`enumeration_reads_only_the_nodes_pinned_rows`] counts the `sandboxes`
//!    rows the plan's scan nodes examined and requires them to stay inside the
//!    node's own pinned history. Deliberately not an assertion about *which*
//!    index: more than one node-bounded plan is acceptable, and pinning the
//!    planner's current choice would fail on a behaviour-preserving change.
//!    [`the_composite_index_is_what_bounds_the_scan`] covers the part that is
//!    not the planner's business — that the composite index is what produces
//!    the bound — by dropping it and requiring the plan to degrade to reading
//!    the whole table.
//! 2. **No `snapshots` scan reads more than the node has.**
//!    [`snapshot_scans_stay_within_the_nodes_snapshots`] counts the rows each
//!    `snapshots` scan examines, across [`PLAN_CASES`], and requires every
//!    one to read no more in total than the node's own snapshots plus one per
//!    execution. That's a total, not a per-sandbox guarantee: a per-execution
//!    scan of this fixture's small in-flight set is accepted and stays under
//!    it. Unfenced and written as a top-level conjunct, the in-flight check
//!    is pulled up into a merge anti-join over the fleet's snapshots, which
//!    exceeds it. Its own catch is an in-flight probe whose per-execution
//!    reads add up past the total, a per-row scan of the table for one, which
//!    property 3 exempts from its read bound. A rewrite to conjuncts that
//!    drops the in-flight fence fails here too, and also fails property 3's
//!    lower bound.
//! 3. **The in-flight probe runs only for non-durable rows.**
//!    [`inflight_check_runs_only_for_non_durable_rows`] counts how often the
//!    in-flight check executes against a cluster with thousands of in-flight
//!    uploads elsewhere, across [`PLAN_CASES`]. It requires at least one
//!    execution per row the page returns and no more than the node's
//!    non-durable rows. Property 2 bounds the reads it can see, which excludes
//!    a bitmap index scan's; this one bounds how many executions there are,
//!    whatever plan serves each. It applies property 2's per-scan read bound
//!    to this larger fixture as well, except to the in-flight probe's own
//!    scan, whose per-execution cost is the planner's choice. That bound is
//!    what catches a rewrite to conjuncts that drops the durability fence:
//!    here the pulled-up check reads every durable snapshot in the fixture,
//!    while on property 2's smaller fixture it stays inside the node.
//!
//! Neither scalar-subquery fence has a structural test of its own. Inside the
//! `CASE` a fence changes nothing these tests measure, so a test for its
//! shape would fail on a refactor that preserves behaviour. Writing the
//! predicate as conjuncts is what makes the fences matter, and properties 2
//! and 3 measure what an unfenced conjunct reads.
//!
//! [`page_is_the_lowest_ids_and_excludes_non_suspended_rows`] covers the
//! behavioural contract: the page is the ascending-id prefix of the eligible
//! set, so a re-invocation picks up the `deferred[]` tail the previous call
//! published, and a non-suspended row pinned to the node is never an answer.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::PgPool;
use std::num::NonZeroUsize;
use uuid::Uuid;

use super::super::nodes::FORCE_UPLOAD_ENUMERATION_CAP;
use super::{list_pinned_local_only_sandboxes, pinned_local_only_sandboxes_query};

use pplx_space_test_env::TestResult;

/// The production entry point, with the typed ids unwrapped so its answer
/// compares directly against the fixture's raw ones.
async fn list_page(pool: &PgPool, node: Uuid, limit: NonZeroUsize) -> TestResult<Vec<Uuid>> {
    let page = list_pinned_local_only_sandboxes(pool, NodeId::from_uuid(node), limit).await?;
    Ok(page.into_iter().map(SandboxId::as_uuid).collect())
}

/// Suspended rows pinned to the node that already have a durable backup. This
/// is the node's history: large, and entirely uninteresting to the operator.
const DURABLE_HISTORY: usize = 900;

/// Suspended rows pinned to the node with no snapshot at all — the sandboxes
/// the operator is forcing an upload for. More than one page's worth at the
/// page size the ordering test uses, so the prefix property is observable.
const GENUINE_WORK: usize = 7;

/// Non-durable rows pinned to the node in every other lifecycle the column
/// takes on a live node. None of them is an answer, and all of them pass the
/// two shared predicates — `lifecycle = 'suspended'` is the only conjunct that
/// excludes them.
const OTHER_LIFECYCLES: [&str; 4] = ["running", "creating", "stopped", "failed"];

/// Durable suspended rows pinned to *other* nodes. These are the fleet: never
/// an answer to the node under test, and the rows a plan that lost the index
/// would read anyway.
///
/// Sized only so the table dwarfs the node: the bound tests compare rows
/// examined against the node's own pinned rows and the whole table, and they
/// need the two to differ by more than an order of magnitude to tell a
/// node-bounded scan from a fleet-wide one.
const FLEET_ELSEWHERE: usize = 10000;

/// `state='creating'` full snapshots belonging to other nodes' sandboxes.
/// The cluster-wide population is small. They are what
/// the in-flight predicate quantifies over, and the fixture keeps it non-empty
/// so that predicate is planned against the shape production has rather than
/// against an empty join side.
///
/// Small enough that a per-execution scan of the whole set, which the
/// statement accepts, stays under the per-scan bound in
/// [`snapshot_scans_stay_within_the_nodes_snapshots`], the node's snapshots
/// plus one per execution: at
/// [`PRODUCTION_PAGE`], [`GENUINE_WORK`] executions of 120 rows is 840, under
/// 900 + 7. Growing this constant or [`GENUINE_WORK`], or shrinking
/// [`DURABLE_HISTORY`], past that margin makes the test reject an accepted
/// plan.
const FOREIGN_INFLIGHT_UPLOADS: usize = 120;

/// Page size the ordering assertions use: smaller than [`GENUINE_WORK`], so a
/// page is a strict prefix of the eligible set.
const SHORT_PAGE: NonZeroUsize = NonZeroUsize::new(3).unwrap();

/// The limit the `:force-upload-local-snapshots` handler actually sends: its
/// cap plus the one sentinel row that signals truncation. The plan tests run
/// at this size as well as [`SHORT_PAGE`], because the literal `LIMIT` is an
/// input to the planner's choice.
const PRODUCTION_PAGE: NonZeroUsize = NonZeroUsize::new(FORCE_UPLOAD_ENUMERATION_CAP + 1).unwrap();

/// Every page size and plan-cache mode the plan tests assert over.
const PLAN_CASES: [(NonZeroUsize, &str); 4] = [
    (SHORT_PAGE, "force_custom_plan"),
    (SHORT_PAGE, "force_generic_plan"),
    (PRODUCTION_PAGE, "force_custom_plan"),
    (PRODUCTION_PAGE, "force_generic_plan"),
];

/// A page comfortably above the backlog, so it returns the whole eligible set
/// and a shorter page is provably a prefix of the same answer.
const FULL_PAGE: NonZeroUsize = NonZeroUsize::new(GENUINE_WORK * 4).unwrap();

/// Insert sandboxes with the supplied ids and matching lifecycles, pinned to
/// `node`, in the order given.
///
/// `placement_changed_at` is non-NULL because it is the correlation the legacy
/// branch of both shared predicates reads; without it those branches take a
/// different path than production's rows do.
///
/// The ids arrive as a slice, in the order given, so the ordering test can
/// insert them descending: a plan that returned rows in physical order would
/// then hand back the highest ids rather than the lowest. `WITH ORDINALITY …
/// ORDER BY t.ord` pins the insertion order to the array order rather than
/// leaving it to `UNNEST`'s unspecified emission order. The composite index
/// already yields the node's rows in id order, so this guards a plan that
/// stops using it; see
/// [`page_is_the_lowest_ids_and_excludes_non_suspended_rows`].
async fn insert_rows(pool: &PgPool, node: Uuid, ids: &[Uuid], lifecycles: &[&str]) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, lifecycle_changed_at,
             network_policy_generation
         )
         SELECT
             t.id,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             t.lifecycle, 'unplaced', 'k8s',
             $3, NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days', 1
           FROM UNNEST($1::uuid[], $2::text[]) WITH ORDINALITY AS t(id, lifecycle, ord)
          ORDER BY t.ord"
    ))
    .bind(ids)
    .bind(lifecycles)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert suspended sandboxes with the supplied ids, pinned to `node`.
async fn insert_suspended_rows(pool: &PgPool, node: Uuid, ids: &[Uuid]) -> TestResult {
    insert_rows(pool, node, ids, &vec!["suspended"; ids.len()]).await
}

/// Insert `count` suspended sandboxes pinned to `node` and return their ids.
async fn insert_generated_suspended_rows(
    pool: &PgPool,
    node: Uuid,
    count: usize,
) -> TestResult<Vec<Uuid>> {
    let ids: Vec<Uuid> = (0..count).map(|_| Uuid::now_v7()).collect();
    insert_suspended_rows(pool, node, &ids).await?;
    Ok(ids)
}

/// Insert one non-durable row per entry of [`OTHER_LIFECYCLES`], pinned to
/// `node`.
async fn insert_other_lifecycle_rows(pool: &PgPool, node: Uuid) -> TestResult<Vec<Uuid>> {
    let ids: Vec<Uuid> = OTHER_LIFECYCLES.iter().map(|_| Uuid::now_v7()).collect();
    insert_rows(pool, node, &ids, &OTHER_LIFECYCLES).await?;
    Ok(ids)
}

/// Register a `state='ready'` full snapshot plus its storage-location row for
/// every sandbox in `sandbox_ids`, making each one durable and therefore not an
/// answer.
async fn insert_ready_backups(pool: &PgPool, sandbox_ids: &[Uuid]) -> TestResult {
    sqlx::query(sql!(
        "WITH inserted AS (
             INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
             SELECT space_uuid_v7(), t.sandbox_id, 'full', 'ready', NULL, NOW()
               FROM UNNEST($1::uuid[]) AS t(sandbox_id)
             RETURNING id
         )
         INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         SELECT id, 's3://test-bucket/' || id FROM inserted"
    ))
    .bind(sandbox_ids)
    .execute(pool)
    .await?;
    Ok(())
}

/// Register a fresh `state='creating'` full snapshot uploading from
/// `source_node` for every sandbox in `sandbox_ids`: the population the
/// in-flight check quantifies over.
async fn insert_inflight_uploads(
    pool: &PgPool,
    sandbox_ids: &[Uuid],
    source_node: Uuid,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         SELECT space_uuid_v7(), t.sandbox_id, 'full', 'creating', $2, NOW()
           FROM UNNEST($1::uuid[]) AS t(sandbox_id)"
    ))
    .bind(sandbox_ids)
    .bind(source_node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Sandboxes on other nodes that [`snapshot_scans_stay_within_the_nodes_snapshots`]
/// seeds beneath the node under test through [`seed_fleet_below_node`].
const FLEET_BELOW_NODE: usize = 3000;

/// `state='creating'` full snapshots on other nodes' sandboxes for
/// [`inflight_check_runs_only_for_non_durable_rows`]. Thousands rather than
/// [`FOREIGN_INFLIGHT_UPLOADS`]'s 120, so the planner costs the in-flight
/// probe against a population of that size. Whether it then picks a plan that
/// scans the whole set per execution is its choice; the test asserts the
/// execution count, which holds either way.
const CROWDED_INFLIGHT_UPLOADS: usize = 4000;

/// `ANALYZE` the tables the statement reads, so the planner sees the fixture
/// rather than an empty-table default.
async fn analyze(pool: &PgPool) -> TestResult {
    sqlx::query(sql!(
        "ANALYZE sandboxes, snapshots, snapshot_storage_locations"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Seed a fleet of `fleet_size` durable sandboxes whose ids all sort below the
/// node under test, the first `inflight_uploads` of them with an in-flight
/// upload, then the node itself, and `ANALYZE`. Returns the node and how many
/// snapshots its own sandboxes carry.
///
/// Ids are UUIDv7, so seeding the fleet first gives it the lower ids. A merge
/// anti-join along `idx_snapshots_sandbox_id` then has to walk the fleet's
/// snapshots to reach the node's, which is what makes that plan visible as
/// reads outside the node. [`seed_fleet`] seeds the fleet last, where the walk
/// passes over only the other node's history.
///
/// The node's durable history is seeded before its [`GENUINE_WORK`], so the
/// node-ordered scan reaches every durable row before a non-durable one and no
/// page size lets the `LIMIT` stop before the whole history has been checked.
/// That's the shape a long-lived node has.
async fn seed_fleet_below_node(
    pool: &PgPool,
    fleet_size: usize,
    inflight_uploads: usize,
) -> TestResult<(Uuid, u64)> {
    assert!(
        fleet_size >= inflight_uploads,
        "fleet must cover the in-flight population it is sliced from"
    );
    let fleet_node = Uuid::now_v7();
    let fleet = insert_generated_suspended_rows(pool, fleet_node, fleet_size).await?;
    insert_ready_backups(pool, &fleet).await?;
    insert_inflight_uploads(pool, &fleet[..inflight_uploads], fleet_node).await?;

    let node = Uuid::now_v7();
    let history = insert_generated_suspended_rows(pool, node, DURABLE_HISTORY).await?;
    insert_ready_backups(pool, &history).await?;
    insert_generated_suspended_rows(pool, node, GENUINE_WORK).await?;
    analyze(pool).await?;

    let node_snapshots: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*)
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
          WHERE s.target_node_id = $1"
    ))
    .bind(node)
    .fetch_one(pool)
    .await?;
    Ok((node, u64::try_from(node_snapshots)?))
}

/// What the fixture holds, and which node answers which question.
struct Fleet {
    /// A node carrying nothing but durable history: every suspended row on it
    /// already has a registered snapshot, so the correct answer is the empty
    /// page. This forces the access-path differential to exhaust its scan.
    ///
    /// With no row to return, no plan can satisfy the `LIMIT` early, so the
    /// pkey-ordered scan has to read the whole table however the fixture is
    /// sized. A node that *does* have candidates lets that scan stop as soon as
    /// it reaches them, so a differential measured against one can pass while
    /// measuring nothing if the candidates happen to hold low ids.
    ///
    /// The empty page is also a first-class outcome of this endpoint rather
    /// than an edge case: the handler answers it with its own early return so a
    /// re-invocation is a cheap idempotent ping (`nodes.rs`,
    /// `apply_force_upload_local_snapshots`).
    quiet_node: Uuid,
    /// A node with a real backlog, for the page-shape assertions.
    work_node: Uuid,
    /// `work_node`'s genuine work queue, ascending — the answer the page must
    /// be a prefix of. Inserted in descending order; see
    /// [`insert_rows`].
    work: Vec<Uuid>,
    /// `work_node`'s non-durable rows in other lifecycles. Not answers.
    other_lifecycles: Vec<Uuid>,
    /// Every `sandboxes` row in the fixture.
    total_rows: u64,
    /// Rows pinned to [`Fleet::quiet_node`].
    quiet_node_rows: u64,
}

/// Seed the fixture and `ANALYZE`, so the planner sees it rather than an
/// empty-table default.
///
/// `fleet_elsewhere` is a parameter because only the two bound tests,
/// [`enumeration_reads_only_the_nodes_pinned_rows`] and
/// [`the_composite_index_is_what_bounds_the_scan`], compare against the fleet;
/// [`page_is_the_lowest_ids_and_excludes_non_suspended_rows`] asserts
/// node-local properties and would otherwise pay for inserts it doesn't read.
/// It passes [`FOREIGN_INFLIGHT_UPLOADS`], which is the floor —
/// the in-flight population is sliced out of the fleet.
async fn seed_fleet(pool: &PgPool, fleet_elsewhere: usize) -> TestResult<Fleet> {
    assert!(
        fleet_elsewhere >= FOREIGN_INFLIGHT_UPLOADS,
        "fleet must cover the in-flight population it is sliced from"
    );
    let quiet_node = Uuid::now_v7();
    let quiet_history = insert_generated_suspended_rows(pool, quiet_node, DURABLE_HISTORY).await?;
    insert_ready_backups(pool, &quiet_history).await?;

    let work_node = Uuid::now_v7();
    let work_history = insert_generated_suspended_rows(pool, work_node, DURABLE_HISTORY).await?;
    insert_ready_backups(pool, &work_history).await?;

    // Descending insertion order: see `insert_rows`.
    let mut work: Vec<Uuid> = (0..GENUINE_WORK).map(|_| Uuid::now_v7()).collect();
    work.sort_unstable();
    let mut descending = work.clone();
    descending.reverse();
    insert_suspended_rows(pool, work_node, &descending).await?;

    let other_lifecycles = insert_other_lifecycle_rows(pool, work_node).await?;

    let fleet_node = Uuid::now_v7();
    let fleet = insert_generated_suspended_rows(pool, fleet_node, fleet_elsewhere).await?;
    insert_ready_backups(pool, &fleet).await?;
    insert_inflight_uploads(pool, &fleet[..FOREIGN_INFLIGHT_UPLOADS], fleet_node).await?;
    analyze(pool).await?;

    // Read the counts back rather than deriving them from the constants, so a
    // fixture that silently seeded less than it meant to cannot make the bound
    // in `enumeration_reads_only_the_nodes_pinned_rows` trivially true.
    let (total_rows, quiet_node_rows): (i64, i64) = sqlx::query_as(sql!(
        "SELECT count(*), count(*) FILTER (WHERE target_node_id = $1) FROM sandboxes"
    ))
    .bind(quiet_node)
    .fetch_one(pool)
    .await?;

    Ok(Fleet {
        quiet_node,
        work_node,
        work,
        other_lifecycles,
        total_rows: u64::try_from(total_rows)?,
        quiet_node_rows: u64::try_from(quiet_node_rows)?,
    })
}

/// One `EXPLAIN (ANALYZE, FORMAT JSON)` statement's result.
///
/// Typed rather than walked as a `serde_json::Value`, following the shape in
/// `node_drain/store/pinned_local_only_scale_tests.rs`: serde handles the root
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
    #[serde(rename = "Relation Name")]
    relation: Option<String>,
    #[serde(rename = "Actual Rows", default)]
    rows: u64,
    #[serde(rename = "Rows Removed by Filter", default)]
    rows_removed: u64,
    #[serde(rename = "Actual Loops", default)]
    loops: u64,
    #[serde(rename = "Index Cond")]
    index_cond: Option<String>,
    #[serde(rename = "Recheck Cond")]
    recheck_cond: Option<String>,
    #[serde(rename = "Filter")]
    filter: Option<String>,
    #[serde(rename = "Plans", default)]
    children: Vec<PlanNode>,
}

impl PlanNode {
    /// How many `sandboxes` rows the plan's scan nodes actually examined: the
    /// rows they emitted plus the rows their own filter discarded, summed over
    /// every scan of the table and multiplied by the loop count so a parallel
    /// plan's per-worker averages add up to the real total.
    ///
    /// This is the quantity the change is about, and it is what the admin
    /// handler's latency is made of. It is also deliberately the *only* thing
    /// asserted about the plan's shape here: which index the planner reaches
    /// for is its business, and more than one node-bounded plan is acceptable.
    /// A wall-clock assertion would be a flake and a cost assertion moves with
    /// every planner release; rows examined is the contract.
    fn sandbox_rows_examined(&self) -> u64 {
        let own = if self.relation.as_deref() == Some("sandboxes") {
            (self.rows + self.rows_removed) * self.loops.max(1)
        } else {
            0
        };
        own + self
            .children
            .iter()
            .map(PlanNode::sandbox_rows_examined)
            .sum::<u64>()
    }

    /// Every `snapshots` scan in the plan, each with the rows it examined
    /// (emitted plus filtered out, times its loops, as in
    /// [`PlanNode::sandbox_rows_examined`]) and its loop count.
    ///
    /// Reported per scan rather than per probe, because a scan can't always be
    /// told apart by its conditions: the durability probe is served by a
    /// partial index whose predicate is its `state`/`type` filter, so the plan
    /// shows no `state` condition on it at all.
    ///
    /// Each entry also says whether the scan is the in-flight probe's, by
    /// [`PlanNode::names_creating`].
    fn snapshot_scans(&self) -> Vec<SnapshotScan> {
        let mut scans = Vec::new();
        self.collect_snapshot_scans(&mut scans);
        scans
    }

    fn collect_snapshot_scans(&self, scans: &mut Vec<SnapshotScan>) {
        if self.relation.as_deref() == Some("snapshots") {
            let loops = self.loops.max(1);
            scans.push(SnapshotScan {
                read: (self.rows + self.rows_removed) * loops,
                loops,
                inflight: self.names_creating(),
            });
        }
        for child in &self.children {
            child.collect_snapshot_scans(scans);
        }
    }

    /// How many times the in-flight check executed: the most loops any of its
    /// `snapshots` scans ran. The probe reads that one table with no join, so
    /// its scan runs once per execution. Raw loops, so a probe that never ran
    /// counts zero.
    fn inflight_probe_executions(&self) -> u64 {
        let own = if self.names_creating() { self.loops } else { 0 };
        self.children
            .iter()
            .map(PlanNode::inflight_probe_executions)
            .fold(own, u64::max)
    }

    /// Is this a `snapshots` scan whose conditions name `state = 'creating'`?
    ///
    /// The in-flight probe is recognised by that literal. An index whose
    /// predicate absorbed it would hide the probe the way
    /// `idx_snapshots_sandbox_ready_latest_covering` hides the durability
    /// probe's `state`, and [`inflight_check_runs_only_for_non_durable_rows`]
    /// would then fail with zero executions rather than pass: adding such an
    /// index means updating this match.
    fn names_creating(&self) -> bool {
        self.relation.as_deref() == Some("snapshots")
            && [&self.index_cond, &self.recheck_cond, &self.filter]
                .into_iter()
                .flatten()
                .any(|cond| cond.contains("'creating'"))
    }
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` one statement against `node`.
async fn explain(pool: &PgPool, query: &str, node: Uuid) -> TestResult<PlanNode> {
    // `TIMING OFF`: every assertion here reads row counts, loops or condition
    // text, which per-node timing does not affect, and instrumenting a subplan
    // that runs once per pinned row costs more than the plan does.
    let statement = format!("EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) {query}");
    let raw: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    let mut explained: Vec<Explained> = serde_json::from_value(raw)?;
    Ok(explained.pop().ok_or("EXPLAIN returned no plan")?.plan)
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` one statement against `node` under a
/// forced `plan_cache_mode`.
///
/// [`explain`] can't exercise a generic plan: a bound parameter reaches the
/// planner as a value, so every plan it produces is a custom one. A generic
/// plan only exists for a prepared statement, so this prepares the query on
/// a connection of its own, forces the mode there, and explains its `EXECUTE`.
async fn explain_prepared(
    pool: &PgPool,
    query: &str,
    node: Uuid,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    use sqlx::Connection as _;

    // Detached, so the prepared statement and the forced mode leave with this
    // connection instead of going back to the pool, however this returns.
    let mut conn = pool.acquire().await?.detach();
    let prepare = format!("PREPARE enumeration(uuid) AS {query}");
    sqlx::query(sqlx::AssertSqlSafe(sql!(prepare.as_str())))
        .execute(&mut conn)
        .await?;
    let set_mode = format!("SET plan_cache_mode = {plan_cache_mode}");
    sqlx::query(sqlx::AssertSqlSafe(sql!(set_mode.as_str())))
        .execute(&mut conn)
        .await?;
    // `EXECUTE` takes no bind parameters of its own, so the node id is a
    // literal here; it is a fixture-generated UUID, never external input.
    let statement =
        format!("EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) EXECUTE enumeration('{node}')");
    let raw: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .fetch_one(&mut conn)
        .await?;
    conn.close().await?;
    let mut explained: Vec<Explained> = serde_json::from_value(raw)?;
    Ok(explained.pop().ok_or("EXPLAIN returned no plan")?.plan)
}

/// The property: an operator forcing uploads on one node must not make
/// Postgres read rows that belong to other nodes. That is read straight off the
/// plan as rows examined, so it holds at any fixture size rather than at a
/// calibrated one.
///
/// Asked against [`Fleet::quiet_node`], whose answer is the empty page. That is
/// the shape the defect lived in: with no row to return there is nothing for a
/// `LIMIT` to stop early on, so a plan that gets its ordering from the primary
/// key has to read the whole table before it can answer "nothing". It is also
/// the ordinary answer — what an operator gets once the backlog has cleared.
#[tokio::test(flavor = "current_thread")]
async fn enumeration_reads_only_the_nodes_pinned_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fleet = seed_fleet(pool, FLEET_ELSEWHERE).await?;
    let node = fleet.quiet_node;
    let pinned_rows = fleet.quiet_node_rows;
    let total_rows = fleet.total_rows;

    // The fixture has to hold enough rows elsewhere for "the node's rows" and
    // "the table's rows" to be distinguishable at all.
    assert!(
        total_rows > pinned_rows * 10,
        "fixture is too uniform to separate a node-bounded scan from a fleet-wide one: \
         {pinned_rows} of {total_rows} rows are pinned to the node under test"
    );

    // Both plan-cache modes, because sqlx prepares the statement and a pooled
    // connection may switch it to a generic plan, and both page sizes, because
    // the literal `LIMIT` feeds the plan choice. The index alone does not hold
    // the bound in both: with the probes written as bare conjuncts, the
    // generic plan on this fixture reads the whole table even with the
    // composite index present.
    for (page, plan_cache_mode) in PLAN_CASES {
        let query = pinned_local_only_sandboxes_query(page);
        let plan = explain_prepared(pool, &query, node, plan_cache_mode).await?;
        assert!(
            plan.sandbox_rows_examined() <= pinned_rows,
            "the enumeration must read no more `sandboxes` rows than the node's own \
             pinned history ({pinned_rows}) (LIMIT {page}, {plan_cache_mode}); it \
             examined {} of {total_rows}. Plan was {plan:?}",
            plan.sandbox_rows_examined()
        );
    }
    Ok(())
}

/// The index is what bounds the scan, and this proves it rather than asserting
/// it.
///
/// The statement and the migration that indexes for it ship separately — the
/// repository forbids a migration beside runtime code in one change — so
/// nothing else in the tree records that they are coupled.
///
/// Checked at runtime, inside this test's own cloned database: drop the index
/// and the plan must degrade to reading the whole table. Doing it here rather
/// than by editing the migration keeps both halves of the differential in one
/// database and one run, and proves the index is present in a *migrated*
/// database rather than only present in a `.sql` file. It degrades that way
/// rather than by sorting, which is worth knowing — the primary key supplies
/// `id` order too, so the planner does not need a `Sort` to answer this query
/// without the composite. It just has to
/// read every row to do it, which is the entire defect. An assertion about a
/// `Sort` appearing would therefore be false, and an assertion that the
/// dropped index is no longer used would be true by construction.
///
/// Measured at [`SHORT_PAGE`] only. Whether the planner falls back to the
/// primary key or to the narrow index plus a sort depends on the page size
/// relative to the table: at [`PRODUCTION_PAGE`] against this fixture's table,
/// the sort is the cheaper fallback and still node-bounded, so the defect
/// doesn't reproduce there. A larger table can change that cost choice.
#[tokio::test(flavor = "current_thread")]
async fn the_composite_index_is_what_bounds_the_scan() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fleet = seed_fleet(pool, FLEET_ELSEWHERE).await?;
    let node = fleet.quiet_node;

    let with_index = explain(pool, &pinned_local_only_sandboxes_query(SHORT_PAGE), node).await?;
    let bounded = with_index.sandbox_rows_examined();
    assert!(
        bounded <= fleet.quiet_node_rows,
        "precondition: with the composite index the scan must stay inside the node's \
         {} pinned rows, but it examined {bounded}; the differential below proves \
         nothing otherwise. Plan was {with_index:?}",
        fleet.quiet_node_rows
    );

    sqlx::query(sql!("DROP INDEX idx_sandboxes_target_node_ordered"))
        .execute(pool)
        .await?;
    sqlx::query(sql!("ANALYZE sandboxes")).execute(pool).await?;

    let without_index = explain(pool, &pinned_local_only_sandboxes_query(SHORT_PAGE), node).await?;
    let unbounded = without_index.sandbox_rows_examined();
    assert!(
        unbounded >= fleet.total_rows,
        "dropping `idx_sandboxes_target_node_ordered` must send this statement back to \
         reading the whole table — that is the defect the index exists to \
         remove. It examined {unbounded} of {} rows, so either \
         the index is not load-bearing or this fixture no longer reproduces the \
         defect, or the statement lost its `ORDER BY` and so no longer needs the \
         primary key's ordering. Plan was {without_index:?}",
        fleet.total_rows
    );
    Ok(())
}

/// One `snapshots` scan from [`PlanNode::snapshot_scans`].
struct SnapshotScan {
    /// Rows examined, summed over loops.
    read: u64,
    loops: u64,
    /// Whether this is the in-flight probe's scan.
    inflight: bool,
}

/// Which `snapshots` scans [`assert_snapshot_scans_stay_on_the_node`] bounds.
#[derive(Clone, Copy)]
enum Scans {
    /// Every scan.
    All,
    /// Every scan but the in-flight probe's, for a fixture whose in-flight set
    /// is large enough that a per-execution scan of it, which the statement's
    /// docs accept, would exceed the bound.
    ExceptInflight,
}

/// Assert that no bounded `snapshots` scan in `plan` reads past the node's own
/// snapshots: see [`snapshot_scans_stay_within_the_nodes_snapshots`] for the bound.
fn assert_snapshot_scans_stay_on_the_node(
    plan: &PlanNode,
    node_snapshots: u64,
    which: Scans,
    page: NonZeroUsize,
    plan_cache_mode: &str,
) {
    let scans: Vec<SnapshotScan> = plan
        .snapshot_scans()
        .into_iter()
        .filter(|scan| matches!(which, Scans::All) || !scan.inflight)
        .collect();
    assert!(
        !scans.is_empty(),
        "the plan must read `snapshots` somewhere, or the bound below proves \
         nothing (LIMIT {page}, {plan_cache_mode}); plan was {plan:?}"
    );
    for SnapshotScan { read, loops, .. } in scans {
        let bound = node_snapshots + loops;
        assert!(
            read <= bound,
            "no `snapshots` scan may read more in total than the node's own snapshots \
             (LIMIT {page}, {plan_cache_mode}): one read {read} rows, above the node's \
             own {node_snapshots} plus {loops} for rounding. Plan was {plan:?}"
        );
    }
}

/// Both checks are fenced in scalar subqueries, so each execution is
/// correlated on the one sandbox being asked about. Asserted as a total per
/// `snapshots` scan, whichever probe it belongs to: no scan reads more than
/// the node's own snapshots plus one per execution.
///
/// Unfenced and written as a top-level conjunct, either check is a sublink
/// the planner may pull up into an anti-join. For the in-flight check, the
/// node-ordered scan makes a merge anti-join along `idx_snapshots_sandbox_id`
/// available, and on this fixture the planner takes it in both plan-cache
/// modes: it walks every snapshot whose sandbox id sorts below the page,
/// whichever node that sandbox belongs to. [`seed_fleet_below_node`] puts the
/// whole fleet below the node, so that walk reads the fleet's snapshots. The
/// durability check, pulled up on this fixture, becomes a nested-loop
/// anti-join that stays inside the node, so this test doesn't catch it;
/// [`inflight_check_runs_only_for_non_durable_rows`] does. Inside the
/// statement's `CASE` no sublink is pulled up, fenced or not. Against the
/// statement as written, what only this test sees is an in-flight probe whose
/// per-execution reads add up past that total, such as a per-row scan of the
/// table, since
/// [`inflight_check_runs_only_for_non_durable_rows`] exempts the in-flight
/// scan from its read bound.
///
/// The bound is the node's own snapshots plus one per execution of the scan.
/// A probe served by a `sandbox_id`-keyed index reads only the probed
/// sandboxes' snapshots, so its scan stays inside the first term; a
/// per-execution scan of the in-flight set stays under it too, by the margin
/// [`FOREIGN_INFLIGHT_UPLOADS`] documents. The second term absorbs
/// `EXPLAIN`'s per-loop rounding, which can add up to one row per execution.
/// Both terms are fixed by the node and the scan, not by how many rows a
/// regressed plan reads, so an anti-join over the fleet, which executes once,
/// exceeds it.
///
/// It doesn't bound how often a probe runs. Nor does it count every per-row
/// scan of a state's snapshots: it reads `snapshots` rows off scan nodes that
/// name the relation, and a `Bitmap Index Scan` names an index instead, so
/// the reads of a `BitmapAnd` over `idx_snapshots_state_type` aren't counted
/// here.
/// [`inflight_check_runs_only_for_non_durable_rows`] bounds the count, which
/// caps that cost whatever plan serves the probe.
#[tokio::test(flavor = "current_thread")]
async fn snapshot_scans_stay_within_the_nodes_snapshots() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let (node, node_snapshots) =
        seed_fleet_below_node(pool, FLEET_BELOW_NODE, FOREIGN_INFLIGHT_UPLOADS).await?;
    for (page, plan_cache_mode) in PLAN_CASES {
        let query = pinned_local_only_sandboxes_query(page);
        let plan = explain_prepared(pool, &query, node, plan_cache_mode).await?;
        assert_snapshot_scans_stay_on_the_node(
            &plan,
            node_snapshots,
            Scans::All,
            page,
            plan_cache_mode,
        );
    }
    Ok(())
}

/// The in-flight check runs only for rows the durability check didn't already
/// exclude, whatever plan the planner gives it.
///
/// The statement guarantees that by evaluation order rather than by plan; see
/// [`pinned_local_only_sandboxes_query`]. Left as two `AND`ed conjuncts, the
/// planner puts the in-flight check first on this fixture, and the check runs
/// for every suspended row the scan reaches, durable history included.
///
/// Read as executions rather than rows: a per-execution scan of the in-flight
/// set is a plan the planner may legitimately choose, so its reads aren't the
/// invariant. How often it runs is: once per non-durable row the scan reaches,
/// against a durable history over a hundred times [`GENUINE_WORK`]. How many
/// rows that is depends on the plan. The ordered index scan stops once the
/// page fills, while the narrow index plus a `Sort`, which the statement's docs
/// also accept, filters every row before sorting. So the bound is a range: at
/// least the rows the page returns, so a probe that stopped running or became
/// a hash or merge join fails, and at most the node's non-durable rows.
#[tokio::test(flavor = "current_thread")]
async fn inflight_check_runs_only_for_non_durable_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let (node, node_snapshots) =
        seed_fleet_below_node(pool, CROWDED_INFLIGHT_UPLOADS, CROWDED_INFLIGHT_UPLOADS).await?;
    for (page, plan_cache_mode) in PLAN_CASES {
        let query = pinned_local_only_sandboxes_query(page);
        let plan = explain_prepared(pool, &query, node, plan_cache_mode).await?;
        // Property 2's per-scan read bound, against the larger fixture, for
        // every scan but the in-flight probe's: a durability check pulled up
        // into an anti-join reads every durable snapshot here, which the
        // execution count below only catches as a side effect. The in-flight
        // probe is left out because a per-execution scan of this in-flight
        // set is a plan the statement accepts.
        assert_snapshot_scans_stay_on_the_node(
            &plan,
            node_snapshots,
            Scans::ExceptInflight,
            page,
            plan_cache_mode,
        );
        // Every non-durable row is an answer, so each row the page returns
        // went through the probe.
        let returned = u64::try_from(page.get().min(GENUINE_WORK))?;
        let non_durable = u64::try_from(GENUINE_WORK)?;
        let executions = plan.inflight_probe_executions();
        assert!(
            (returned..=non_durable).contains(&executions),
            "the in-flight check must run as a per-row probe for between the {returned} \
             rows the page returned and the node's {non_durable} non-durable rows, never \
             for its {DURABLE_HISTORY} durable ones; fewer means it stopped running or \
             became a hash or merge join (LIMIT {page}, {plan_cache_mode}); it ran {executions} \
             times. Plan was {plan:?}"
        );
    }
    Ok(())
}

/// Two properties the statement has to preserve, and one it could break.
///
/// **The page is the ascending-id prefix.** The handler processes the head of
/// the page and publishes the rest as `deferred[]`, so the operator's next call
/// has to pick up where this one stopped rather than resample. The fixture
/// inserts the work rows in *descending* id order, so a plan that returned
/// physical order would give the highest ids, not the lowest.
///
/// What this does **not** reliably prove is the `ORDER BY` itself. Without the
/// clause the page's order is whatever index the planner picks: the composite
/// index returns id order and these assertions stay green, the narrow index
/// returns physical order and they fail. The clause is still load-bearing: it
/// makes the
/// ordering a requirement the planner has to satisfy rather than a property of
/// whichever plan it happens to pick, and a plan that stopped using the index
/// would return an unordered page without it. That argument is not something a
/// fixture at this scale can demonstrate, so it is recorded here rather than
/// asserted. Deleting the `ORDER BY` does fail
/// [`the_composite_index_is_what_bounds_the_scan`], which is what catches the
/// edit in practice.
///
/// **The cap is a page size, not a filter.** A full page returns exactly the
/// eligible set, so a short page is a prefix of the same answer rather than a
/// different one.
///
/// **A non-suspended row is still not an answer.** The fixture seeds a
/// non-durable row in every other lifecycle a live node carries, and checks
/// both directions: the page excludes them, and those rows are separately
/// shown to be pinned to this node with no snapshot at all, so every predicate
/// except `lifecycle = 'suspended'` admits them. Without the second half the
/// exclusion could hold because the shared predicates had already dropped
/// them, and force-uploading a *running* sandbox — reading its live snapshot
/// directory underneath it — would be invisible here.
#[tokio::test(flavor = "current_thread")]
async fn page_is_the_lowest_ids_and_excludes_non_suspended_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fleet = seed_fleet(pool, FOREIGN_INFLIGHT_UPLOADS).await?;
    let node = fleet.work_node;
    let work_ascending = fleet.work;
    let other_lifecycles = fleet.other_lifecycles;

    // Through the production entry point rather than a re-implementation of it:
    // these are the only assertions here that describe what an operator
    // receives, so they have to exercise the bind and the `sql!` site the
    // handler actually uses.
    let full_page = list_page(pool, node, FULL_PAGE).await?;
    assert_eq!(
        full_page,
        work_ascending,
        "a page above the backlog must return exactly the node's non-durable suspended \
         rows, in ascending id order; {DURABLE_HISTORY} rows of durable history and \
         {} non-suspended rows must not leak in",
        other_lifecycles.len()
    );

    let short_page = list_page(pool, node, SHORT_PAGE).await?;
    assert_eq!(
        short_page,
        work_ascending[..SHORT_PAGE.get()],
        "a page below the backlog must be the lowest ids of the same answer — that \
         prefix is what makes the handler's `deferred[]` tail the work the next \
         call picks up"
    );
    // Re-running an unchanged query proves nothing, so move the head instead:
    // a newly eligible sandbox with a lower id than everything on the page must
    // lead the next one. That is the property `deferred[]` rests on — the page
    // is a position in an ordering, not a sample — and it is what a page that
    // merely reflected physical or insertion order would get wrong.
    let intruder = Uuid::parse_str("00000000-0000-0000-0000-000000000001")?;
    insert_suspended_rows(pool, node, &[intruder]).await?;
    let after = list_page(pool, node, SHORT_PAGE).await?;
    assert_eq!(
        after.first(),
        Some(&intruder),
        "a newly eligible sandbox whose id sorts below the whole page must lead \
         the next page; got {after:?}"
    );
    assert_eq!(
        &after[1..],
        &short_page[..SHORT_PAGE.get() - 1],
        "and the rest of the page must shift down by one rather than resample"
    );

    // The exclusion above is only evidence if those rows would otherwise be
    // answers. They are pinned to this node and carry no snapshot at all, so
    // they are non-durable with no upload in flight — every predicate except
    // the lifecycle one admits them. Asserted against the fixture's own state
    // rather than by rewriting the statement's SQL, which would fail on a
    // reformat that changed no behaviour.
    let (pinned_here, with_snapshots): (i64, i64) = sqlx::query_as(sql!(
        "SELECT count(*) FILTER (WHERE s.target_node_id = $2),
                count(*) FILTER (WHERE EXISTS (
                    SELECT 1 FROM snapshots sn WHERE sn.sandbox_id = s.id
                ))
           FROM sandboxes s
          WHERE s.id = ANY($1)"
    ))
    .bind(&other_lifecycles)
    .bind(node)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        (pinned_here, with_snapshots),
        (i64::try_from(other_lifecycles.len())?, 0),
        "the non-suspended rows must all be pinned to this node and carry no \
         snapshot, or their absence from the page holds for some reason other than \
         the lifecycle predicate and proves nothing"
    );
    Ok(())
}
