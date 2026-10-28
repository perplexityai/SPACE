//! Read-only evidence from isolated publication workload fixtures.

use pplx_space_common_core::types::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use crate::TestDbResult;

/// Give every freshly seeded run planner statistics before measuring it.
/// ANALYZE takes its usual maintenance locks; it changes no fixture rows.
pub async fn analyze(pool: &PgPool) -> TestDbResult<()> {
    sqlx::query(sql!(
        "ANALYZE nodes, sandboxes, sandbox_k8s_executions,
                 sandbox_direct_route_identities, sandbox_observed_states,
                 sandbox_quarantine_state, sandbox_quarantine_history,
                 node_facet_observed_status, node_status_snapshots,
                 templates, node_template_inventory, snapshots,
                 snapshot_storage_locations"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
pub struct RareOutcome {
    pub lifecycle: String,
    pub unplaced: bool,
    pub active: bool,
    pub quarantined: bool,
}

/// Read a guarded-transition result without explicit locks. An absent row
/// returns `None`, so callers distinguish a missing fixture from a no-op claim.
pub async fn rare_outcome(pool: &PgPool, id: SandboxId) -> TestDbResult<Option<RareOutcome>> {
    Ok(sqlx::query_as(sql!(
        "SELECT s.lifecycle,
                s.placement = 'unplaced' AND s.active_node_id IS NULL AS unplaced,
                EXISTS (SELECT 1 FROM sandbox_k8s_executions e
                        WHERE e.sandbox_id = s.id AND e.active) AS active,
                EXISTS (SELECT 1 FROM sandbox_quarantine_state q
                        WHERE q.sandbox_id = s.id) AS quarantined
           FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?)
}

/// Export whole synthetic projections without explicit locks. Empty tables
/// return empty arrays; timestamps and generated IDs remain unmodified.
pub async fn dump(pool: &PgPool) -> TestDbResult<Vec<(&'static str, Vec<serde_json::Value>)>> {
    let tables = [
        ("nodes", sql!("SELECT to_jsonb(n) FROM nodes n ORDER BY id")),
        (
            "sandboxes",
            sql!("SELECT to_jsonb(s) FROM sandboxes s ORDER BY id"),
        ),
        (
            "sandbox_observed_states",
            sql!("SELECT to_jsonb(s) FROM sandbox_observed_states s ORDER BY sandbox_id"),
        ),
        (
            "node_facet_observed_status",
            sql!(
                "SELECT to_jsonb(s) FROM node_facet_observed_status s ORDER BY node_id, facet, element"
            ),
        ),
        (
            "sandbox_quarantine_state",
            sql!("SELECT to_jsonb(s) FROM sandbox_quarantine_state s ORDER BY sandbox_id"),
        ),
        (
            "sandbox_quarantine_history",
            sql!(
                "SELECT to_jsonb(s) FROM sandbox_quarantine_history s ORDER BY sandbox_id, occurred_at, id"
            ),
        ),
        (
            "node_status_snapshots",
            sql!("SELECT to_jsonb(s) FROM node_status_snapshots s ORDER BY node_id"),
        ),
        (
            "sandbox_k8s_executions",
            sql!(
                "SELECT to_jsonb(s) FROM sandbox_k8s_executions s ORDER BY sandbox_id, generation"
            ),
        ),
        (
            "sandbox_direct_route_identities",
            sql!("SELECT to_jsonb(s) FROM sandbox_direct_route_identities s ORDER BY sandbox_id"),
        ),
    ];
    let mut result = Vec::new();
    for (table, query) in tables {
        result.push((table, sqlx::query_scalar(query).fetch_all(pool).await?));
    }
    Ok(result)
}
