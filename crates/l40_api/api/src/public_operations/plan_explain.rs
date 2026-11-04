//! `EXPLAIN` helpers for the plan-cost tests of this module's statements.
//!
//! A cached statement can switch to a generic plan, which knows none of its
//! arguments, so a plan test explains each statement under both forced
//! `plan_cache_mode`s and bounds the rows its plan handles. It reads rows off
//! the plan rather than naming an index or timing the statement: which linear
//! plan the planner takes is its business.

use pplx_space_common_core::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

/// Both forced plan modes, custom first.
pub(super) const PLAN_CACHE_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];

/// One `EXPLAIN (ANALYZE, FORMAT JSON)` statement's result.
#[derive(Debug, serde::Deserialize)]
struct Explained {
    #[serde(rename = "Plan")]
    plan: PlanNode,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct PlanNode {
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
    pub(super) fn rows_handled(&self) -> f64 {
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
    pub(super) fn outline(&self) -> String {
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
/// spells its arguments as literals: fixture-generated values and the crate's
/// own constants.
pub(super) async fn explain_prepared(
    pool: &PgPool,
    prepare: &str,
    execute: &str,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    use sqlx::Connection as _;

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
