//! Plan-cost regression for the ID search statements, [`SEARCH_PAGE_SQL`]
//! and [`SEARCH_LIST_SQL`].
//!
//! Which rows a search lists is covered by the api's admin list tests; this
//! covers how many it reads. A search usually matches a few sandboxes of a
//! large table, so the statement has to find them through the index on the
//! reversed ID and the primary key rather than walk the table in list order.
//! A cached statement can switch to a generic plan, which knows none of its
//! ID ranges, so the bound has to hold under both plan modes.
//!
//! [`a_search_reads_only_the_rows_it_matches`] explains the page statement
//! and the unpaged one for an ID label, the four digits of the shortest ID
//! end, an eight-digit ID start of UUIDv7-shaped IDs spread over time, and
//! digits no ID starts or ends with, under both modes, and bounds the rows
//! every plan node handles by [`MAX_ROWS_HANDLED`]. The fixture is many times
//! larger, so a plan that reads the table fails. To show that the bound is
//! what the ID-end index buys, it then drops it and requires the generic plan
//! of each statement to exceed the bound for the label. It reads rows off the
//! plan rather than naming an index or timing the statement: which linear
//! plan the planner takes is its business. Each search binds the ranges
//! [`AdminIdSearch::from_id_digits`] builds, as the api's searches do, so a
//! change to how they are built is explained here too.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::{Connection as _, PgPool};
use uuid::Uuid;

use super::*;

/// Sandboxes in the fixture.
const SANDBOXES: i32 = 5_000;
/// The rows a search for a few sandboxes may handle across its plan: the
/// index entries and heap rows of its matches, their sort, and the joins of
/// the page's full rows.
const MAX_ROWS_HANDLED: f64 = 500.0;

/// The first fixture sandbox's creation time, in Unix milliseconds, and the
/// time between two sandboxes: a UUIDv7 starts with this, so an eight-digit
/// ID start, which spans 65.5 s, matches six or seven of them.
const FIRST_CREATED_MS: i64 = 1_790_000_000_000;
const CREATED_EVERY_MS: i64 = 10_000;

/// The budget and row overhead the page statement is explained with: the
/// api's full list budget and its estimate of a row
/// (`MAX_SANDBOX_LIST_PAGE_BYTES` and `SANDBOX_LIST_ROW_OVERHEAD_BYTES`),
/// which these copy.
const MAX_PAGE_BYTES: i64 = 8 * 1024 * 1024;
const ROW_OVERHEAD_BYTES: i64 = 2 * 1024;

/// Both forced plan modes, custom first.
const PLAN_CACHE_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];

/// The fixture's sandboxes: [`SANDBOXES`] with UUIDv7-shaped IDs, and one
/// more with a known ID.
struct Fixture {
    /// The console's label for the known sandbox: its ID's last eight hex
    /// digits.
    label: String,
    /// The first eight hex digits of a fixture sandbox's ID.
    id_start: String,
}

#[tokio::test(flavor = "current_thread")]
async fn a_search_reads_only_the_rows_it_matches() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fixture = seed(pool).await?;
    let searches = [
        ("an ID label", fixture.label.clone()),
        (
            "the shortest ID end, four digits",
            fixture.label.chars().skip(4).collect(),
        ),
        ("an eight-digit ID start", fixture.id_start.clone()),
        ("digits no ID starts or ends with", "00000000".to_string()),
    ];
    let statements = [
        ("page", Statement::Page),
        ("unpaged list", Statement::Unpaged),
    ];
    for (statement_label, statement) in statements {
        for (search_label, digits) in &searches {
            for plan_cache_mode in PLAN_CACHE_MODES {
                let plan = explain(pool, statement, digits, plan_cache_mode).await?;
                let handled = plan.rows_handled();
                assert!(
                    handled <= MAX_ROWS_HANDLED,
                    "{statement_label}, {search_label} ({plan_cache_mode}): handled {handled} \
                     rows, over {MAX_ROWS_HANDLED}. Rows handled by node:{}",
                    plan.outline()
                );
            }
        }
    }

    sqlx::query(sql!("DROP INDEX idx_sandboxes_id_suffix"))
        .execute(pool)
        .await?;
    for (statement_label, statement) in statements {
        let plan = explain(pool, statement, &fixture.label, "force_generic_plan").await?;
        let handled = plan.rows_handled();
        assert!(
            handled > MAX_ROWS_HANDLED,
            "{statement_label} without the ID-end index: handled {handled} rows, within the \
             bound the index is to buy. Rows handled by node:{}",
            plan.outline()
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Statement {
    Page,
    Unpaged,
}

/// `EXPLAIN (ANALYZE)` a search for `digits` with no other filter, as the
/// first page of 100 or the unpaged list of 100, under a forced
/// `plan_cache_mode`.
async fn explain(
    pool: &PgPool,
    statement: Statement,
    digits: &str,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    // The ranges the api binds for the same digits.
    let search = AdminIdSearch::from_id_digits(digits).ok_or("an ID search")?;
    let (suffix_low, suffix_high) = search.id_suffix();
    let id_prefix = search.id_prefix();
    // Hex digits, `:`, `g` and a UUID's text: nothing to quote.
    let literal =
        |value: Option<String>| value.map_or("NULL".to_string(), |value| format!("'{value}'"));
    let search_values = [
        literal(Some(suffix_low.to_string())),
        literal(Some(suffix_high.to_string())),
        literal(id_prefix.map(|(low, _)| low.to_string())),
        literal(id_prefix.map(|(_, high)| high.to_string())),
    ]
    .join(", ");
    let search_types = "text, text, uuid, uuid";
    let filters = "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL";
    let filter_types =
        "uuid, text[], text, uuid, timestamptz, timestamptz, text, uuid, uuid, uuid, boolean";
    let (prepare, execute) = match statement {
        Statement::Page => (
            format!(
                "PREPARE search({filter_types}, {search_types}, timestamptz, uuid, bigint, \
                 bigint, bigint) AS {}",
                SEARCH_PAGE_SQL.as_str()
            ),
            format!(
                "EXECUTE search({filters}, {search_values}, NULL, NULL, 101, {MAX_PAGE_BYTES}, \
                 {ROW_OVERHEAD_BYTES})"
            ),
        ),
        Statement::Unpaged => (
            format!(
                "PREPARE search({filter_types}, {search_types}, bigint) AS {}",
                SEARCH_LIST_SQL.as_str()
            ),
            format!("EXECUTE search({filters}, {search_values}, 100)"),
        ),
    };
    explain_prepared(pool, &prepare, &execute, plan_cache_mode).await
}

/// [`SANDBOXES`] sandboxes of the seed user, and one more with a fresh
/// UUIDv7, analyzed so both plan modes see their statistics.
async fn seed(pool: &PgPool) -> TestResult<Fixture> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, name, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT (lpad(to_hex($4 + g * $5), 12, '0') || '7' || substr(md5(g::text), 1, 3)
                 || '8' || substr(md5(g::text), 4, 15))::UUID,
                'sandbox-' || g,
                $1, $2, 'k8s', 'started', 1
           FROM generate_series(1, $3) g"
    ))
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SANDBOXES)
    .bind(FIRST_CREATED_MS)
    .bind(CREATED_EVERY_MS)
    .execute(pool)
    .await?;
    let labelled = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'started', 1)"
    ))
    .bind(labelled)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!("ANALYZE sandboxes")).execute(pool).await?;
    let compact = labelled.simple().to_string();
    let middle = format!(
        "{:012x}",
        FIRST_CREATED_MS + i64::from(SANDBOXES / 2) * CREATED_EVERY_MS
    );
    Ok(Fixture {
        label: compact.chars().skip(24).collect(),
        id_start: middle.chars().take(8).collect(),
    })
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
    /// so a failure names the node that broke the bound.
    fn outline(&self) -> String {
        let mut out = String::new();
        self.outline_into(1, &mut out);
        out
    }

    fn outline_into(&self, depth: usize, out: &mut String) {
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
            child.outline_into(depth + 1, out);
        }
    }
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` `execute` under a forced
/// `plan_cache_mode`, after `prepare` names the statement it runs.
///
/// A generic plan exists only for a prepared statement, so this prepares the
/// statement on a connection of its own, forces the mode there, and explains
/// its `EXECUTE`. `EXECUTE` takes no bind parameters of its own, so `execute`
/// spells its arguments as literals.
async fn explain_prepared(
    pool: &PgPool,
    prepare: &str,
    execute: &str,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    // Detached, so the prepared statement and the forced mode leave with this
    // connection instead of going back to the pool, however this returns.
    let mut conn = pool.acquire().await?.detach();
    sqlx::query(sqlx::AssertSqlSafe(sql!(prepare)))
        .execute(&mut conn)
        .await?;
    let set_mode = format!("SET plan_cache_mode = {plan_cache_mode}");
    sqlx::query(sqlx::AssertSqlSafe(sql!(set_mode.as_str())))
        .execute(&mut conn)
        .await?;
    let statement = format!("EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) {execute}");
    let raw: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .fetch_one(&mut conn)
        .await?;
    conn.close().await?;
    let mut explained: Vec<Explained> = serde_json::from_value(raw)?;
    Ok(explained.pop().ok_or("EXPLAIN returned no plan")?.plan)
}
