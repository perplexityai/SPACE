//! `EXPLAIN` helpers for plan-cost tests.
//!
//! A cached statement can switch to a generic plan, which knows none of its
//! arguments: sqlx prepares and caches statements per connection by default,
//! and PostgreSQL's default `plan_cache_mode` moves a prepared statement to
//! its generic plan after five executions when that plan isn't estimated to cost more. A plan
//! that only a custom plan avoids, such as one that drops an `OR` branch for a
//! `NULL` argument, is therefore not the plan a hot statement runs. A plan
//! test explains each statement under both forced `plan_cache_mode`s and
//! bounds the rows its plan handles ([`PlanNode::rows_handled`], or only the
//! per-row work, [`PlanNode::repeated_rows_handled`]), on a fixture large
//! enough that a plan whose cost grows with the square of the fixture breaks
//! the bound. An index scan can also discard entries inside the index, which
//! no row count sees, so a test that bounds per-row work also compares the
//! buffers the plan touched ([`PlanNode::blocks_touched`]) at two fixture
//! sizes. It reads the plan rather than naming an index or timing the
//! statement: which linear plan the planner takes is its business, and
//! neither a row count nor a block count flakes on a loaded machine.

use std::fmt::Write as _;

use sqlx::{Connection as _, PgPool};

use crate::TestDbResult;

/// Both forced plan modes, custom first.
pub const PLAN_CACHE_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];

/// One `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` statement's result.
#[derive(Debug, serde::Deserialize)]
struct Explained {
    #[serde(rename = "Plan")]
    plan: PlanNode,
}

/// One node of an `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plan, with its children.
#[derive(Debug, serde::Deserialize)]
pub struct PlanNode {
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
    #[serde(rename = "Shared Hit Blocks", default)]
    shared_hit_blocks: f64,
    #[serde(rename = "Shared Read Blocks", default)]
    shared_read_blocks: f64,
    #[serde(rename = "Actual Loops", default)]
    loops: f64,
    #[serde(rename = "Plans", default)]
    children: Vec<PlanNode>,
}

impl PlanNode {
    /// The rows every node in the plan emitted or discarded, times its
    /// loops: `EXPLAIN` reports both per loop, as averages a server may round
    /// to whole rows, so each of the four counts can be off by half a row per
    /// loop, an error linear in the loops that can't hide a quadratic plan. A join that
    /// compares every pair of its inputs discards the unmatched pairs by its
    /// join filter, and a correlated subquery that scans a table per outer row
    /// discards the rows it doesn't want by its filter, so both count here too.
    pub fn rows_handled(&self) -> f64 {
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

    /// Per-row work: the rows handled, as [`Self::rows_handled`] counts them,
    /// by the nodes that ran more than once (the inner side of a nested loop,
    /// a correlated subplan), and by every join, which handles the pairs it
    /// emits or discards by its join filter. A plan that rescans a table per
    /// outer row handles about the product of the two row counts in the
    /// first, and a join that pairs every row of one side with every row of
    /// the other emits or discards those pairs in the second, while a node
    /// that reads a table once, such as a hashed subplan's scan, counts in
    /// neither, however large the table. A parallel node reports its
    /// workers' runs as loops, so a parallel scan counts too: keep fixtures
    /// below the planner's parallel thresholds.
    pub fn repeated_rows_handled(&self) -> f64 {
        let own = if self.loops > 1.0 {
            (self.rows
                + self.removed_by_filter
                + self.removed_by_join_filter
                + self.removed_by_recheck)
                * self.loops
        } else if self.join_type.is_some() {
            self.rows + self.removed_by_filter + self.removed_by_join_filter
        } else {
            0.0
        };
        own + self
            .children
            .iter()
            .map(PlanNode::repeated_rows_handled)
            .sum::<f64>()
    }

    /// The shared buffer accesses the plan made, from the cache or not, each
    /// page counted every time it's read: the root node's count, which
    /// includes its children's and every loop's. It counts the index pages an
    /// index scan walks, including entries it discards
    /// inside the index (an index condition on a column after one it doesn't
    /// constrain), which no row count sees. Its absolute value depends on page
    /// layout, so compare it between two fixture sizes: a plan linear in the
    /// fixture touches about proportionally more, one that walks the
    /// fixture's index range per row many times more.
    pub fn blocks_touched(&self) -> f64 {
        self.shared_hit_blocks + self.shared_read_blocks
    }

    /// One line per node, indented by depth: its type and join type, the
    /// table and index it reads, and the rows it handled per loop and its
    /// loops, so a failure names the node that broke the bound. Only plan
    /// structure, no fixture values.
    pub fn outline(&self) -> String {
        let mut out = String::new();
        self.outline_into(1, &mut out);
        out
    }

    fn outline_into(&self, depth: usize, out: &mut String) {
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
        let _ = write!(
            out,
            ": {} rows over {} loops",
            self.rows
                + self.removed_by_filter
                + self.removed_by_join_filter
                + self.removed_by_recheck,
            self.loops
        );
        for child in &self.children {
            child.outline_into(depth + 1, out);
        }
    }
}

/// `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` `execute` under a forced
/// `plan_cache_mode`, after `prepare` names the statement it runs.
///
/// A generic plan exists only for a prepared statement, so this prepares the
/// statement on a connection of its own, forces the mode there, and explains
/// its `EXECUTE`. `EXECUTE` takes no bind parameters of its own, so `execute`
/// spells its arguments as literals: fixture-generated values and the
/// caller's own constants. `prepare` declares each parameter's type as the
/// statement's production caller binds it, so the generic plan is the one
/// that caller's cached statement gets.
pub async fn explain_prepared(
    pool: &PgPool,
    prepare: &str,
    execute: &str,
    plan_cache_mode: &str,
) -> TestDbResult<PlanNode> {
    // Detached, so the prepared statement and the forced mode leave with this
    // connection instead of going back to the pool, however this returns.
    // No `sql!` locator, as for this crate's other statements.
    let mut conn = pool.acquire().await?.detach();
    sqlx::query(sqlx::AssertSqlSafe(prepare))
        .execute(&mut conn)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET plan_cache_mode = {plan_cache_mode}"
    )))
    .execute(&mut conn)
    .await?;
    let raw: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) {execute}"
    )))
    .fetch_one(&mut conn)
    .await?;
    conn.close().await?;
    let mut explained: Vec<Explained> = serde_json::from_value(raw)?;
    Ok(explained.pop().ok_or("EXPLAIN returned no plan")?.plan)
}
