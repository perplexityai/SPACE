//! Plan-cost regression for [`SANDBOXES_HELD`].
//!
//! The count's behaviour is covered in `tests`; this covers how much it
//! reads. A cached statement can switch to a generic plan, which knows no
//! org, so a plan has to stay cheap without knowing how big the org is. The
//! count's inputs are the org's sandbox rows and the cluster's in-flight
//! operations, and a plan linear in those reads each of them in a few nodes:
//! scanned, filtered, joined or hashed, and appended. What made the count
//! slow was work outside them:
//!
//! * comparing each in-flight create with each of the org's rows, which a
//!   generic plan of a text comparison does in a nested loop, so the work
//!   is their product;
//! * reading the org's retained operation history through
//!   `idx_public_operations_tenant_created`, a bitmap intersected with the
//!   in-flight state index;
//! * scanning either table whole.
//!
//! [`the_count_reads_only_the_orgs_rows_and_the_in_flight_operations`]
//! bounds the rows every plan node handles, under both plan modes, by a few
//! passes over those inputs, and [`seed`] sizes the fixture so each of the
//! three costs above exceeds that bound on its own. Like the plan tests in
//! `routes::admin::node_store`, it reads rows off the plan rather than
//! naming an index or timing the statement: which linear plan the planner
//! takes is its business.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, UserId};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::SANDBOXES_HELD;

/// How many plan nodes may handle each input row. The count's linear plans
/// handle an org row in its scan (the bitmap index entry and the heap row),
/// the append, and, when the anti-join hashes the org's rows, a second scan
/// and the hash; an in-flight operation in its index entry, its heap row,
/// the org filter and the anti-join.
const PASSES: u64 = 6;

/// The small orgs filling the cluster, each with this many sandboxes, one
/// in-flight create and this much retained history.
const SMALL_ORGS: i32 = 300;
const SMALL_ORG_SANDBOXES: i32 = 80;
const SMALL_ORG_HISTORY: i32 = 20;

/// The org counted: live rows, `deleted` tombstones, in-flight creates of
/// which some already have their `creating` row, and retained history.
const ORG_LIVE: i32 = 1_000;
const ORG_TOMBSTONES: i32 = 500;
const ORG_CREATES: i32 = 200;
const ORG_CREATES_WITH_ROWS: i32 = 50;
const ORG_HISTORY: i32 = 30_000;

struct Fixture {
    org_id: OrgId,
    user_id: UserId,
    /// Every row of the org's, live or not.
    org_rows: u64,
    /// Every `queued` or `running` operation in the cluster.
    in_flight: u64,
}

/// One `EXPLAIN (ANALYZE, FORMAT JSON)` statement's result.
#[derive(Debug, serde::Deserialize)]
struct Explained {
    #[serde(rename = "Plan")]
    plan: PlanNode,
}

#[derive(Debug, serde::Deserialize)]
struct PlanNode {
    #[serde(rename = "Node Type")]
    node_type: String,
    #[serde(rename = "Join Type", default)]
    join_type: Option<String>,
    #[serde(rename = "Relation Name", default)]
    relation: Option<String>,
    #[serde(rename = "Index Name", default)]
    index: Option<String>,
    #[serde(rename = "Actual Rows", default)]
    rows: f64,
    #[serde(rename = "Rows Removed by Filter", default)]
    removed_by_filter: f64,
    #[serde(rename = "Rows Removed by Join Filter", default)]
    removed_by_join_filter: f64,
    #[serde(rename = "Rows Removed by Index Recheck", default)]
    removed_by_recheck: f64,
    #[serde(rename = "Actual Loops", default)]
    loops: f64,
    #[serde(rename = "Plans", default)]
    children: Vec<PlanNode>,
}

impl PlanNode {
    /// The rows every node in the plan emitted or discarded, times its
    /// loops: `EXPLAIN` reports both per loop. A join that compares every
    /// pair of its inputs discards the unmatched pairs by its join filter,
    /// so they count here too.
    fn rows_handled(&self) -> f64 {
        let own = (self.rows
            + self.removed_by_filter
            + self.removed_by_join_filter
            + self.removed_by_recheck)
            * self.loops.max(1.0);
        own + self
            .children
            .iter()
            .map(PlanNode::rows_handled)
            .sum::<f64>()
    }

    /// One line per node, indented by depth: its type and join type, the
    /// table and index it reads, and the rows it and its children handled,
    /// so a failure names the node that broke the bound. Only plan
    /// structure, no fixture values.
    fn outline(&self, depth: usize, out: &mut String) {
        use std::fmt::Write as _;

        let _ = write!(
            out,
            "\n{:indent$}{}",
            "",
            self.node_type,
            indent = depth * 2
        );
        if let Some(join_type) = &self.join_type {
            let _ = write!(out, " {join_type}");
        }
        if let Some(relation) = &self.relation {
            let _ = write!(out, " on {relation}");
        }
        if let Some(index) = &self.index {
            let _ = write!(out, " using {index}");
        }
        let _ = write!(out, ": {} rows", self.rows_handled());
        for child in &self.children {
            child.outline(depth + 1, out);
        }
    }
}

/// The count's bound: [`PASSES`] over the org's rows and the cluster's
/// in-flight operations. Each slow plan the module docs list exceeds it on
/// [`seed`]'s fixture, which the test checks before it measures.
///
/// Asked about the org whose count was slow: big next to the others, with
/// many creates in flight, so a create compared with every row costs far
/// more than either alone, and with an operation history far longer than
/// the cluster's in-flight set.
#[tokio::test(flavor = "current_thread")]
async fn the_count_reads_only_the_orgs_rows_and_the_in_flight_operations() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fixture = seed(pool).await?;
    let bound = PASSES * (fixture.org_rows + fixture.in_flight);
    let creates_without_rows = u64::try_from(ORG_CREATES - ORG_CREATES_WITH_ROWS)?;
    let org_live = u64::try_from(ORG_LIVE + ORG_CREATES_WITH_ROWS)?;
    let sandboxes: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes"))
        .fetch_one(pool)
        .await?;
    for (cost, rows) in [
        (
            "each create compared with each row",
            creates_without_rows * org_live,
        ),
        ("the org's operation history", u64::try_from(ORG_HISTORY)?),
        ("a scan of `sandboxes`", u64::try_from(sandboxes)?),
    ] {
        assert!(
            rows > bound,
            "fixture too small: {cost} reads {rows} rows, within the bound of {bound}"
        );
    }

    for plan_cache_mode in ["force_custom_plan", "force_generic_plan"] {
        let plan = explain_prepared(pool, &fixture, plan_cache_mode).await?;
        let handled = plan.rows_handled();
        let mut outline = String::new();
        plan.outline(1, &mut outline);
        assert!(
            handled <= bound as f64,
            "the count must handle no more rows than {PASSES} passes over the org's {} rows \
             and the cluster's {} in-flight operations ({bound}) ({plan_cache_mode}); it \
             handled {handled}. Rows handled by node, children included:{outline}",
            fixture.org_rows,
            fixture.in_flight,
        );
    }
    Ok(())
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` the count for the fixture's org under a
/// forced `plan_cache_mode`.
///
/// A generic plan exists only for a prepared statement, so this prepares
/// the count on a connection of its own, forces the mode there, and explains
/// its `EXECUTE`.
async fn explain_prepared(
    pool: &PgPool,
    fixture: &Fixture,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    use sqlx::Connection as _;

    // Detached, so the prepared statement and the forced mode leave with this
    // connection instead of going back to the pool, however this returns.
    let mut conn = pool.acquire().await?.detach();
    let prepare = format!("PREPARE held(uuid, uuid, text, text, text) AS {SANDBOXES_HELD}");
    sqlx::query(sqlx::AssertSqlSafe(sql!(prepare.as_str())))
        .execute(&mut conn)
        .await?;
    let set_mode = format!("SET plan_cache_mode = {plan_cache_mode}");
    sqlx::query(sqlx::AssertSqlSafe(sql!(set_mode.as_str())))
        .execute(&mut conn)
        .await?;
    // `EXECUTE` takes no bind parameters of its own, so the arguments are
    // literals here: fixture-generated UUIDs and the crate's own constants.
    let statement = format!(
        "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) EXECUTE held('{}', '{}', '{}', '{}', '{}')",
        fixture.org_id,
        fixture.user_id,
        super::ApiProduct::Native.as_str(),
        pplx_space_public_api::OperationKind::CreateSandbox.as_str(),
        pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX,
    );
    let raw: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .fetch_one(&mut conn)
        .await?;
    conn.close().await?;
    let mut explained: Vec<Explained> = serde_json::from_value(raw)?;
    Ok(explained.pop().ok_or("EXPLAIN returned no plan")?.plan)
}

/// A cluster of [`SMALL_ORGS`] small orgs and the counted org, analyzed so
/// both plan modes see its statistics.
async fn seed(pool: &PgPool) -> TestResult<Fixture> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         SELECT CASE WHEN g = 0 THEN $1 ELSE gen_random_uuid() END,
                'plan-' || g, 'plan-' || g
           FROM generate_series(0, $2) g"
    ))
    .bind(org_id)
    .bind(SMALL_ORGS)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         SELECT CASE WHEN o.id = $1 THEN $2 ELSE gen_random_uuid() END,
                o.slug || '@example.test', 'hash', 'Plan User', o.id, 'member'
           FROM organizations o
          WHERE o.slug LIKE 'plan-%'"
    ))
    .bind(org_id)
    .bind(user_id)
    .execute(pool)
    .await?;
    // Small orgs: sandboxes, one in-flight create each, and short histories.
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT gen_random_uuid(), u.id, u.org_id, 'k8s', 'started', 1
           FROM users u, generate_series(1, $2)
          WHERE u.email LIKE 'plan-%' AND u.org_id <> $1"
    ))
    .bind(org_id)
    .bind(SMALL_ORG_SANDBOXES)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, actor_user_id, kind, target_type, target_id, state)
         SELECT gen_random_uuid(), u.org_id, u.id, 'create_sandbox', 'sandbox',
                'sbx_' || gen_random_uuid()::text,
                CASE WHEN g = 0 THEN 'queued' ELSE 'succeeded' END
           FROM users u, generate_series(0, $2) g
          WHERE u.email LIKE 'plan-%' AND u.org_id <> $1"
    ))
    .bind(org_id)
    .bind(SMALL_ORG_HISTORY)
    .execute(pool)
    .await?;
    // The counted org: live rows and tombstones, the rows of the creates
    // past their insert, and those creates' operations.
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, name, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT gen_random_uuid(), CASE WHEN g > $3 + $4 THEN 'in-flight' END, $2, $1, 'k8s',
                CASE WHEN g <= $3 THEN 'started'
                     WHEN g <= $3 + $4 THEN 'deleted'
                     ELSE 'creating' END,
                1
           FROM generate_series(1, $3 + $4 + $5) g"
    ))
    .bind(org_id)
    .bind(user_id)
    .bind(ORG_LIVE)
    .bind(ORG_TOMBSTONES)
    .bind(ORG_CREATES_WITH_ROWS)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, actor_user_id, kind, target_type, target_id, state)
         SELECT gen_random_uuid(), $1, $2, 'create_sandbox', 'sandbox',
                'sbx_' || id::text, 'running'
           FROM sandboxes
          WHERE org_id = $1 AND name = 'in-flight'
         UNION ALL
         SELECT gen_random_uuid(), $1, $2, 'create_sandbox', 'sandbox',
                'sbx_' || gen_random_uuid()::text, 'queued'
           FROM generate_series(1, $3 - $4)
         UNION ALL
         SELECT gen_random_uuid(), $1, $2,
                (ARRAY['create_sandbox', 'delete_sandbox', 'suspend_sandbox'])[1 + g % 3],
                'sandbox', 'sbx_' || gen_random_uuid()::text, 'succeeded'
           FROM generate_series(1, $5) g"
    ))
    .bind(org_id)
    .bind(user_id)
    .bind(ORG_CREATES)
    .bind(ORG_CREATES_WITH_ROWS)
    .bind(ORG_HISTORY)
    .execute(pool)
    .await?;
    sqlx::query(sql!("ANALYZE sandboxes")).execute(pool).await?;
    sqlx::query(sql!("ANALYZE public_operations"))
        .execute(pool)
        .await?;
    let in_flight: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM public_operations WHERE state IN ('queued', 'running')"
    ))
    .fetch_one(pool)
    .await?;
    Ok(Fixture {
        org_id,
        user_id,
        org_rows: u64::try_from(ORG_LIVE + ORG_TOMBSTONES + ORG_CREATES_WITH_ROWS)?,
        in_flight: u64::try_from(in_flight)?,
    })
}
