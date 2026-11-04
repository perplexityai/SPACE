use pplx_space_common_svc::sql::sandbox_residency::{
    ResidencyEvidence, node_heartbeat_fresh, occupies_node_predicate, resident_lifecycle,
};
use pplx_space_sandbox_portability::{
    snapshot_durable_for_source_release, snapshot_is_current_local_artifact,
};

use super::active_occupancy_lifecycles;

pub(super) fn query(ranked: bool, residency: ResidencyEvidence) -> String {
    let active_candidate = active_occupancy_lifecycles("sandboxes");
    let resident = resident_lifecycle("a");
    let present = if ranked {
        occupies_node_predicate("sandboxes", "sandboxes.active_node_id", residency)
    } else {
        "FALSE".to_string()
    };
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let durable = snapshot_durable_for_source_release("c");
    let facts_materialization = if ranked { " MATERIALIZED" } else { "" };
    let local = "((targeted AND lifecycle = 'suspended') OR legacy_upload) AND NOT durable";
    let (local_count, fact_filter) = if ranked {
        (format!("COUNT(*) FILTER (WHERE {local})"), String::new())
    } else {
        // Discard portable candidates before GROUP BY can sort/materialize
        // them. COUNT(*) then needs no second durability evaluation.
        ("COUNT(*)".to_string(), format!("WHERE {local}"))
    };
    let targeted_lifecycle = format!("(s.lifecycle = 'suspended' OR ({ranked} AND (
        s.lifecycle = 'suspending' OR (s.lifecycle = 'restoring' AND s.evacuation_restore_target IS NULL))))");
    let heartbeat = node_heartbeat_fresh("scope.node_id");
    let active_materialization = super::row_cte_materialization(ranked, residency);

    // Count active placements directly. Only target/upload candidates need
    // durability probes; a scalar subquery prevents a global history hash join.
    // Share those probes across ranked counters; the unranked aggregate can
    // consume candidates directly without materializing their facts.
    format!(
        "WITH uploading AS MATERIALIZED (
             SELECT sn.source_node_id AS node_id, sn.sandbox_id,
                    -- Preserve legacy occupancy freshness even for a different
                    -- artifact; drain blockers use the stricter identity below.
                    BOOL_OR(s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at) AS legacy_upload,
                    BOOL_OR({ranked} AND (
                        {pinned}
                        OR (s.current_local_artifact_id IS NULL AND (
                            s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at
                        ))
                    )) AS current_upload
               FROM (
                   -- Filter in-flight snapshots first. Without this boundary,
                   -- a generic BitmapAnd can read the source-node index for
                   -- every retained snapshot before intersecting state/type.
                   SELECT source_node_id, sandbox_id, timestamp,
                          local_artifact_id, artifact_kind
                     FROM snapshots WHERE type = 'full' AND state = 'creating'
                     OFFSET 0
               ) sn
               CROSS JOIN LATERAL (
                   SELECT placement_changed_at, current_local_artifact_id,
                          current_local_artifact_kind
                     FROM sandboxes WHERE id = sn.sandbox_id OFFSET 0
               ) s
              WHERE sn.source_node_id = ANY($1::UUID[])
              GROUP BY sn.source_node_id, sn.sandbox_id
         ), active_rows AS{active_materialization} (
             SELECT active_node_id AS node_id, lifecycle, lifecycle_changed_at,
                    {present} AS present
               FROM sandboxes
              WHERE active_node_id = ANY($1::UUID[]) AND {active_candidate}
         ), active AS (
             SELECT a.node_id, COUNT(*)::BIGINT AS active,
                    COUNT(*) FILTER (WHERE present)::BIGINT AS terminate_blockers,
                    COUNT(*) FILTER (WHERE present AND {resident})::BIGINT AS live_residents,
                    COUNT(*) FILTER (WHERE present AND {resident}
                        AND lifecycle_changed_at < NOW() - make_interval(secs => $2::BIGINT)
                    )::BIGINT AS live_long_runners
               FROM active_rows a GROUP BY a.node_id
         ), candidates AS (
             SELECT s.target_node_id AS node_id, s.id, s.lifecycle, s.placement_changed_at,
                    s.current_local_artifact_id, s.current_local_artifact_kind,
                    s.evacuation_restore_target, TRUE AS targeted,
                    COALESCE(u.legacy_upload, FALSE) AS legacy_upload,
                    COALESCE(u.current_upload, FALSE) AS current_upload
               FROM sandboxes s
               LEFT JOIN uploading u ON u.node_id = s.target_node_id AND u.sandbox_id = s.id
              WHERE s.target_node_id = ANY($1::UUID[]) AND {targeted_lifecycle}
             UNION ALL
             -- Uploads on a candidate's target are already represented above.
             -- Other sources must retain their own (node, sandbox) count.
             SELECT u.node_id, s.id, s.lifecycle, s.placement_changed_at,
                    s.current_local_artifact_id, s.current_local_artifact_kind,
                    s.evacuation_restore_target, FALSE AS targeted,
                    u.legacy_upload, u.current_upload
               FROM uploading u
               CROSS JOIN LATERAL (
                   SELECT id, target_node_id, lifecycle, placement_changed_at,
                          current_local_artifact_id, current_local_artifact_kind,
                          evacuation_restore_target
                     FROM sandboxes WHERE id = u.sandbox_id OFFSET 0
               ) s
              WHERE s.target_node_id IS DISTINCT FROM u.node_id OR NOT {targeted_lifecycle}
         ), facts AS{facts_materialization} (
             SELECT c.*,
                    CASE WHEN (targeted AND lifecycle = 'suspended')
                              OR legacy_upload OR current_upload
                         THEN (SELECT {durable}) ELSE FALSE END AS durable
               FROM candidates c
         ), occupancy AS (
             SELECT node_id,
                    {local_count}::BIGINT AS local_snapshots,
                    COUNT(*) FILTER (
                        WHERE {ranked} AND (
                            (targeted AND (
                                lifecycle = 'suspending'
                                OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                                OR (lifecycle = 'suspended' AND NOT durable)
                            )) OR (current_upload AND NOT durable)
                        )
                    )::BIGINT AS drain_blockers
               FROM facts {fact_filter} GROUP BY node_id
         )
         SELECT scope.node_id,
                COALESCE(active.active, 0)::BIGINT AS active,
                COALESCE(occupancy.local_snapshots, 0)::BIGINT AS local_snapshots,
                COALESCE(active.terminate_blockers, 0)::BIGINT AS terminate_blockers,
                COALESCE(active.live_residents, 0)::BIGINT AS live_residents,
                COALESCE(active.live_long_runners, 0)::BIGINT AS live_long_runners,
                COALESCE(occupancy.drain_blockers, 0)::BIGINT AS drain_blockers,
                {heartbeat} AS heartbeat_fresh
           FROM UNNEST($1::UUID[]) AS scope(node_id)
           LEFT JOIN occupancy ON occupancy.node_id = scope.node_id
           LEFT JOIN active ON active.node_id = scope.node_id"
    )
}
