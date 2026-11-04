use pplx_space_common_svc::sql::sandbox_residency::{
    ResidencyEvidence, node_heartbeat_fresh, occupies_node_predicate, resident_lifecycle,
};
use pplx_space_sandbox_portability::{
    snapshot_durable_for_source_release, snapshot_is_current_local_artifact,
};

use super::active_occupancy_lifecycles;

// Keep the scalar bind distinct from the fleet statement's UUID[]. Even a
// generic plan knows this is one node, regardless of earlier fleet listings.
pub(super) fn query(ranked: bool, residency: ResidencyEvidence) -> String {
    let active = active_occupancy_lifecycles("s");
    let active_candidate = active_occupancy_lifecycles("sandboxes");
    let resident = resident_lifecycle("facts");
    let present = if ranked {
        occupies_node_predicate("s", "$1::UUID", residency)
    } else {
        "FALSE".to_string()
    };
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let durable = snapshot_durable_for_source_release("s");
    let heartbeat = node_heartbeat_fresh("$1::UUID");

    // These CTEs bound the expensive work before evaluating it. `uploading`
    // groups only this node's in-flight snapshots; `candidates` deduplicates
    // placements and upload sources; `facts` evaluates durability/residency
    // once for both occupancy and drainability on the same database snapshot.
    // LATERAL + OFFSET 0 keeps sandbox lookups parameterized by those IDs.
    // The scalar SELECT around durability prevents decorrelation into a hash
    // anti join over the entire snapshot/storage catalog, even in a generic plan.
    format!(
        "WITH uploading AS MATERIALIZED (
             SELECT sn.sandbox_id,
                    BOOL_OR(s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at) AS legacy_upload,
                    BOOL_OR({ranked} AND (
                        {pinned}
                        OR (s.current_local_artifact_id IS NULL AND (
                            s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at
                        ))
                    )) AS current_upload
               FROM snapshots sn
               CROSS JOIN LATERAL (
                   SELECT placement_changed_at, current_local_artifact_id,
                          current_local_artifact_kind
                     FROM sandboxes WHERE id = sn.sandbox_id OFFSET 0
               ) s
              WHERE sn.source_node_id = $1::UUID
                AND sn.type = 'full' AND sn.state = 'creating'
              GROUP BY sn.sandbox_id
         ), candidates AS MATERIALIZED (
             SELECT id FROM sandboxes
              WHERE active_node_id = $1::UUID AND {active_candidate}
             UNION
             SELECT id FROM sandboxes
              WHERE target_node_id = $1::UUID AND (
                  lifecycle = 'suspended'
                  OR ({ranked} AND (
                      lifecycle = 'suspending'
                      OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                  ))
              )
             UNION
             SELECT sandbox_id FROM uploading
         ), facts AS MATERIALIZED (
             SELECT s.active_node_id = $1::UUID AND {active} AS active,
                    s.target_node_id = $1::UUID AS targeted,
                    s.lifecycle, s.lifecycle_changed_at, s.evacuation_restore_target,
                    CASE WHEN s.active_node_id = $1::UUID AND {active}
                         THEN {present} ELSE FALSE END AS present,
                    COALESCE(u.legacy_upload, FALSE) AS legacy_upload,
                    COALESCE(u.current_upload, FALSE) AS current_upload,
                    CASE WHEN (s.target_node_id = $1::UUID AND s.lifecycle = 'suspended')
                              OR COALESCE(u.legacy_upload OR u.current_upload, FALSE)
                         THEN (SELECT {durable}) ELSE FALSE END AS durable
               FROM candidates c
               CROSS JOIN LATERAL (
                   SELECT id, active_node_id, target_node_id, lifecycle,
                          lifecycle_changed_at, placement_changed_at,
                          current_local_artifact_id, current_local_artifact_kind,
                          evacuation_restore_target
                     FROM sandboxes WHERE id = c.id OFFSET 0
               ) s
               LEFT JOIN uploading u ON u.sandbox_id = s.id
         )
         SELECT $1::UUID AS node_id,
                COUNT(*) FILTER (WHERE active)::BIGINT AS active,
                COUNT(*) FILTER (
                    WHERE ((targeted AND lifecycle = 'suspended') OR legacy_upload)
                      AND NOT durable
                )::BIGINT AS local_snapshots,
                COUNT(*) FILTER (WHERE active AND present)::BIGINT AS terminate_blockers,
                COUNT(*) FILTER (
                    WHERE active AND present AND {resident}
                )::BIGINT AS live_residents,
                COUNT(*) FILTER (
                    WHERE active AND present AND {resident}
                      AND lifecycle_changed_at < NOW() - make_interval(secs => $2::BIGINT)
                )::BIGINT AS live_long_runners,
                COUNT(*) FILTER (
                    WHERE {ranked} AND (
                        (targeted AND (
                            lifecycle = 'suspending'
                            OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                            OR (lifecycle = 'suspended' AND NOT durable)
                        )) OR (current_upload AND NOT durable)
                    )
                )::BIGINT AS drain_blockers,
                {heartbeat} AS heartbeat_fresh
           FROM facts"
    )
}

#[cfg(test)]
mod tests;
