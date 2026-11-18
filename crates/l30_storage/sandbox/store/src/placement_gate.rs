//! The read behind the api's drain gate on placement-affecting calls:
//! whether a sandbox is portable for a restore elsewhere, and the drain
//! columns of its primary node, `COALESCE(target_node_id, active_node_id)`.
//!
//! A plain read: it takes no lock and runs on the executor its caller passes.
//! The caller decides what the columns mean for the request, including the
//! fallback through the sandbox's latest execution.

use std::sync::LazyLock;

use pplx_space_node_model::NodeDrainPhase;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::portable_for_restore_affinity;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// What [`placement_gate`] reads of the sandbox row and its primary node.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct PlacementGateRow {
    /// [`portable_for_restore_affinity`] holds for the row (`FALSE` when it
    /// is NULL). The api's gate ignores the node columns when it holds.
    pub portable: bool,
    /// Raw `nodes.drain_engine`; the caller parses it.
    pub drain_engine: Option<String>,
    pub drain_phase: Option<NodeDrainPhase>,
    /// Raw `nodes.drain_mode`; the caller parses it.
    pub drain_mode: Option<String>,
    /// Whether `COALESCE(target_node_id, active_node_id)` names a node row.
    pub primary_found: bool,
    /// Whether that node has a `node_k8s` row.
    pub mapped: bool,
    /// The drain engines recorded on the sandbox's active and target nodes:
    /// `v2` for a v2 drain in any phase, its cancel pending included, or an
    /// engine this build doesn't know. A v1 drain records none.
    pub drain_engines_named: Vec<String>,
    /// Whether the sandbox row is `stopped`.
    pub stopped: bool,
}

/// Read [`PlacementGateRow`] for `sandbox_id`: the portability check and the
/// primary node's drain columns, in one statement so both read the same
/// sandbox row, with the drain engines its active and target nodes record
/// and whether it is stopped. The primary node is `COALESCE(target_node_id,
/// active_node_id)`. `None` when the sandbox row is missing.
pub async fn placement_gate(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<PlacementGateRow>, sqlx::Error> {
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let portable_predicate = portable_for_restore_affinity("s");
        sql!(format!(
            "SELECT COALESCE(({portable_predicate}), FALSE) AS portable,
                n.drain_engine,
                n.drain_phase,
                n.drain_mode,
                n.id IS NOT NULL AS primary_found,
                EXISTS (SELECT 1 FROM node_k8s nk WHERE nk.node_id = n.id) AS mapped,
                ARRAY(
                    SELECT v.drain_engine FROM nodes v
                     WHERE v.id IN (s.active_node_id, s.target_node_id)
                       AND v.drain_engine IS NOT NULL
                ) AS drain_engines_named,
                COALESCE(s.lifecycle = 'stopped', FALSE) AS stopped
           FROM sandboxes s
           LEFT JOIN nodes n ON n.id = COALESCE(s.target_node_id, s.active_node_id)
          WHERE s.id = $1"
        ))
    });
    sqlx::query_as::<_, PlacementGateRow>(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(sandbox_id)
        .fetch_optional(executor)
        .await
}
