//! Additional inventory and reconciliation projections for row equivalence.

use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use crate::TestDbResult;

const QUERIES: &[(&str, &str)] = &[
    (
        "node_template_inventory",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, template_name, content_hash, runtime, arch, built_at,
                        artifact_size_bytes, has_base_snapshot, base_snapshot_size_bytes,
                        pool_target, pool_current, pool_pending, pool_sandbox_ids, snapshots,
                        default_auto_lifecycle_json, ensure_base_snapshot, snapshot_vcpu,
                        snapshot_memory_mib, observed_at, updated_at, missing_publish_count,
                        vcpu, memory_mib, disk_size_mb, cache_size_bytes,
                        network_policy_authority, default_auto_lifecycle_authority,
                        runtime_version, guest_os, state_format_name, state_format_version,
                        execution_metadata
                 FROM node_template_inventory ORDER BY node_id, template_name, content_hash
             ) observed"
        ),
    ),
    (
        "node_template_components",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, template_name, content_hash, kind, role_key, version, digest, format
                 FROM node_template_components
                 ORDER BY node_id, template_name, content_hash, kind, role_key
             ) observed"
        ),
    ),
    (
        "node_runtime_installations",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, runtime, runtime_version, observed_at, version_sort_key
                 FROM node_runtime_installations ORDER BY node_id, runtime, runtime_version
             ) observed"
        ),
    ),
    (
        "node_runtime_guests",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, runtime, runtime_version, guest_os, guest_arch
                 FROM node_runtime_guests
                 ORDER BY node_id, runtime, runtime_version, guest_os, guest_arch
             ) observed"
        ),
    ),
    (
        "node_runtime_state_formats",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, runtime, runtime_version, state_format_name,
                        state_format_version, can_create, can_load
                 FROM node_runtime_state_formats
                 ORDER BY node_id, runtime, runtime_version, state_format_name, state_format_version
             ) observed"
        ),
    ),
    (
        "node_artifact_components",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, kind, role_key, version_key, digest_key, format_key,
                        version, digest, format, default_for_new, legacy_default, observed_at
                 FROM node_artifact_components
                 ORDER BY node_id, kind, role_key, version_key, digest_key, format_key
             ) observed"
        ),
    ),
    (
        "sandbox_snapshot_inventory",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT sandbox_id, snapshot_id, type, state, timestamp, source_node_id, label,
                        retention_tier, vcpu, memory_mib, size_bytes, template_id, content_hash,
                        locations, observed_at, updated_at
                 FROM sandbox_snapshot_inventory ORDER BY sandbox_id, snapshot_id
             ) observed"
        ),
    ),
    (
        "node_facet_conditions",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, facet, element, condition_type, condition_status,
                        reason, message, last_transition_at, updated_at
                 FROM node_facet_conditions ORDER BY node_id, facet, element, condition_type
             ) observed"
        ),
    ),
    (
        "node_reconcile_queue",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, facet, element, generation, attempts, last_error,
                        claim_token, claim_generation, claimed_at, next_attempt_at,
                        requested_at, updated_at, dead_at, dead_reason, traceparent
                 FROM node_reconcile_queue ORDER BY node_id, facet, element
             ) observed"
        ),
    ),
    (
        "node_reconcile_triggers",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, reason, generation, attempts, last_error, dead_at, dead_reason,
                        claim_token, claim_generation, claimed_at, next_attempt_at,
                        requested_at, updated_at, reasons
                 FROM node_reconcile_triggers ORDER BY node_id
             ) observed"
        ),
    ),
    (
        "node_local_backup_pending",
        sql!(
            "SELECT to_jsonb(observed) FROM (
                 SELECT node_id, sandbox_id, artifact_id, artifact_kind, created_at
                 FROM node_local_backup_pending ORDER BY node_id, sandbox_id
             ) observed"
        ),
    ),
];

/// Read the isolated fixture's explicit projection columns without row locks.
/// No match is an empty array. Clocks and identifiers are retained verbatim.
pub(super) async fn dump(
    pool: &PgPool,
) -> TestDbResult<Vec<(&'static str, Vec<serde_json::Value>)>> {
    let mut result = Vec::new();
    for &(table, query) in QUERIES {
        result.push((table, sqlx::query_scalar(query).fetch_all(pool).await?));
    }
    Ok(result)
}
