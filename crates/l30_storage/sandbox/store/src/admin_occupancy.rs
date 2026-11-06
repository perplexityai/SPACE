//! The admin console's occupancy reads: what holds a node, or a node group,
//! occupied, and what a drain of it would wait on.
//!
//! Each statement is whole here, built once with every value bound. The
//! shared definitions they interpolate are the residency rules of
//! `pplx_space_sandbox_residency` and the durability rules of
//! `pplx_space_sandbox_portability`. A node's statements come in a fixed set
//! of shapes: without the drainability counters, and with them under each
//! [`ResidencyEvidence`].

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeGroupId, NodeId};
use pplx_space_sandbox_portability::{
    inflight_suspend_upload_from_node_exists, snapshot_durable_for_source_release,
    snapshot_is_current_local_artifact,
};
use pplx_space_sandbox_residency::{
    ResidencyEvidence, node_heartbeat_fresh, non_resident_lifecycle_exclusion,
    occupies_node_predicate, resident_lifecycle,
};

/// One node's occupancy counters, as [`node_occupancy`] and
/// [`fleet_occupancy`] read them. The drainability counters
/// (`terminate_blockers`, `live_residents`, `live_long_runners`,
/// `drain_blockers`) are zero when the read did not ask for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct NodeOccupancyCounts {
    pub node_id: NodeId,
    pub active: i64,
    pub local_snapshots: i64,
    pub terminate_blockers: i64,
    pub live_residents: i64,
    pub live_long_runners: i64,
    pub drain_blockers: i64,
    pub heartbeat_fresh: bool,
}

/// A node group's occupancy, as [`group_occupancy`] reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupOccupancyCounts {
    pub active: i64,
    pub local_snapshots: i64,
}

// Keep the scalar bind distinct from the fleet statement's UUID[]. Even a
// generic plan knows this is one node, regardless of earlier fleet listings.
//
// These CTEs bound the expensive work before evaluating it. `uploading`
// groups only this node's in-flight snapshots; `candidates` deduplicates
// placements and upload sources; `facts` evaluates durability/residency
// once for both occupancy and drainability on the same database snapshot.
// LATERAL + OFFSET 0 keeps sandbox lookups parameterized by those IDs.
// The scalar SELECT around durability prevents decorrelation into a hash
// anti join over the entire snapshot/storage catalog, even in a generic plan.
static NODE_OCCUPANCY: LazyLock<String> = LazyLock::new(|| {
    let active = non_resident_lifecycle_exclusion("s");
    let active_candidate = non_resident_lifecycle_exclusion("sandboxes");
    let resident = resident_lifecycle("facts");
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let durable = snapshot_durable_for_source_release("s");
    let heartbeat = node_heartbeat_fresh("$1::UUID");
    sql!(format!(
        "WITH uploading AS MATERIALIZED (
             SELECT sn.sandbox_id,
                    BOOL_OR(s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at) AS legacy_upload,
                    BOOL_OR(false AND (
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
              WHERE active_node_id = $1::UUID AND ({active_candidate})
             UNION
             SELECT id FROM sandboxes
              WHERE target_node_id = $1::UUID AND (
                  lifecycle = 'suspended'
                  OR (false AND (
                      lifecycle = 'suspending'
                      OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                  ))
              )
             UNION
             SELECT sandbox_id FROM uploading
         ), facts AS MATERIALIZED (
             SELECT s.active_node_id = $1::UUID AND ({active}) AS active,
                    s.target_node_id = $1::UUID AS targeted,
                    s.lifecycle, s.lifecycle_changed_at, s.evacuation_restore_target,
                    CASE WHEN s.active_node_id = $1::UUID AND ({active})
                         THEN FALSE ELSE FALSE END AS present,
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
                    WHERE false AND (
                        (targeted AND (
                            lifecycle = 'suspending'
                            OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                            OR (lifecycle = 'suspended' AND NOT durable)
                        )) OR (current_upload AND NOT durable)
                    )
                )::BIGINT AS drain_blockers,
                {heartbeat} AS heartbeat_fresh
           FROM facts"
    ))
});

/// [`NODE_OCCUPANCY`] with the drainability counters, under `residency`.
fn ranked_node_occupancy(residency: ResidencyEvidence) -> String {
    let active = non_resident_lifecycle_exclusion("s");
    let active_candidate = non_resident_lifecycle_exclusion("sandboxes");
    let resident = resident_lifecycle("facts");
    let present = occupies_node_predicate("s", "$1::UUID", residency);
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let durable = snapshot_durable_for_source_release("s");
    let heartbeat = node_heartbeat_fresh("$1::UUID");
    sql!(format!(
        "WITH uploading AS MATERIALIZED (
             SELECT sn.sandbox_id,
                    BOOL_OR(s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at) AS legacy_upload,
                    BOOL_OR(true AND (
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
              WHERE active_node_id = $1::UUID AND ({active_candidate})
             UNION
             SELECT id FROM sandboxes
              WHERE target_node_id = $1::UUID AND (
                  lifecycle = 'suspended'
                  OR (true AND (
                      lifecycle = 'suspending'
                      OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                  ))
              )
             UNION
             SELECT sandbox_id FROM uploading
         ), facts AS MATERIALIZED (
             SELECT s.active_node_id = $1::UUID AND ({active}) AS active,
                    s.target_node_id = $1::UUID AS targeted,
                    s.lifecycle, s.lifecycle_changed_at, s.evacuation_restore_target,
                    CASE WHEN s.active_node_id = $1::UUID AND ({active})
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
                    WHERE true AND (
                        (targeted AND (
                            lifecycle = 'suspending'
                            OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                            OR (lifecycle = 'suspended' AND NOT durable)
                        )) OR (current_upload AND NOT durable)
                    )
                )::BIGINT AS drain_blockers,
                {heartbeat} AS heartbeat_fresh
           FROM facts"
    ))
}

static RANKED_NODE_OCCUPANCY: LazyLock<String> =
    LazyLock::new(|| ranked_node_occupancy(ResidencyEvidence::LifecycleOnly));
static RANKED_NODE_OCCUPANCY_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| ranked_node_occupancy(ResidencyEvidence::NodeCensusAware));

// Count active placements directly. Only target/upload candidates need
// durability probes; a scalar subquery prevents a global history hash join.
// Share those probes across ranked counters; the unranked aggregate can
// consume candidates directly without materializing their facts, and
// discards portable candidates before GROUP BY can sort/materialize them, so
// COUNT(*) needs no second durability evaluation.
static FLEET_OCCUPANCY: LazyLock<String> = LazyLock::new(|| {
    let active_candidate = non_resident_lifecycle_exclusion("sandboxes");
    let resident = resident_lifecycle("a");
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let durable = snapshot_durable_for_source_release("c");
    let heartbeat = node_heartbeat_fresh("scope.node_id");
    sql!(format!(
        "WITH uploading AS MATERIALIZED (
             SELECT sn.source_node_id AS node_id, sn.sandbox_id,
                    -- Preserve legacy occupancy freshness even for a different
                    -- artifact; drain blockers use the stricter identity below.
                    BOOL_OR(s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at) AS legacy_upload,
                    BOOL_OR(false AND (
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
         ), active_rows AS (
             SELECT active_node_id AS node_id, lifecycle, lifecycle_changed_at,
                    FALSE AS present
               FROM sandboxes
              WHERE active_node_id = ANY($1::UUID[]) AND ({active_candidate})
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
              WHERE s.target_node_id = ANY($1::UUID[]) AND (s.lifecycle = 'suspended' OR (false AND (
        s.lifecycle = 'suspending' OR (s.lifecycle = 'restoring' AND s.evacuation_restore_target IS NULL))))
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
              WHERE s.target_node_id IS DISTINCT FROM u.node_id OR NOT (s.lifecycle = 'suspended' OR (false AND (
        s.lifecycle = 'suspending' OR (s.lifecycle = 'restoring' AND s.evacuation_restore_target IS NULL))))
         ), facts AS (
             SELECT c.*,
                    CASE WHEN (targeted AND lifecycle = 'suspended')
                              OR legacy_upload OR current_upload
                         THEN (SELECT {durable}) ELSE FALSE END AS durable
               FROM candidates c
         ), occupancy AS (
             SELECT node_id,
                    COUNT(*)::BIGINT AS local_snapshots,
                    COUNT(*) FILTER (
                        WHERE false AND (
                            (targeted AND (
                                lifecycle = 'suspending'
                                OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                                OR (lifecycle = 'suspended' AND NOT durable)
                            )) OR (current_upload AND NOT durable)
                        )
                    )::BIGINT AS drain_blockers
               FROM facts WHERE ((targeted AND lifecycle = 'suspended') OR legacy_upload) AND NOT durable GROUP BY node_id
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
    ))
});

static RANKED_FLEET_OCCUPANCY: LazyLock<String> = LazyLock::new(|| {
    let active_candidate = non_resident_lifecycle_exclusion("sandboxes");
    let resident = resident_lifecycle("a");
    let present = occupies_node_predicate(
        "sandboxes",
        "sandboxes.active_node_id",
        ResidencyEvidence::LifecycleOnly,
    );
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let durable = snapshot_durable_for_source_release("c");
    let heartbeat = node_heartbeat_fresh("scope.node_id");
    sql!(format!(
        "WITH uploading AS MATERIALIZED (
             SELECT sn.source_node_id AS node_id, sn.sandbox_id,
                    -- Preserve legacy occupancy freshness even for a different
                    -- artifact; drain blockers use the stricter identity below.
                    BOOL_OR(s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at) AS legacy_upload,
                    BOOL_OR(true AND (
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
         ), active_rows AS (
             SELECT active_node_id AS node_id, lifecycle, lifecycle_changed_at,
                    {present} AS present
               FROM sandboxes
              WHERE active_node_id = ANY($1::UUID[]) AND ({active_candidate})
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
              WHERE s.target_node_id = ANY($1::UUID[]) AND (s.lifecycle = 'suspended' OR (true AND (
        s.lifecycle = 'suspending' OR (s.lifecycle = 'restoring' AND s.evacuation_restore_target IS NULL))))
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
              WHERE s.target_node_id IS DISTINCT FROM u.node_id OR NOT (s.lifecycle = 'suspended' OR (true AND (
        s.lifecycle = 'suspending' OR (s.lifecycle = 'restoring' AND s.evacuation_restore_target IS NULL))))
         ), facts AS MATERIALIZED (
             SELECT c.*,
                    CASE WHEN (targeted AND lifecycle = 'suspended')
                              OR legacy_upload OR current_upload
                         THEN (SELECT {durable}) ELSE FALSE END AS durable
               FROM candidates c
         ), occupancy AS (
             SELECT node_id,
                    COUNT(*) FILTER (WHERE ((targeted AND lifecycle = 'suspended') OR legacy_upload) AND NOT durable)::BIGINT AS local_snapshots,
                    COUNT(*) FILTER (
                        WHERE true AND (
                            (targeted AND (
                                lifecycle = 'suspending'
                                OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                                OR (lifecycle = 'suspended' AND NOT durable)
                            )) OR (current_upload AND NOT durable)
                        )
                    )::BIGINT AS drain_blockers
               FROM facts  GROUP BY node_id
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
    ))
});

// The census flags on `active_rows` are referenced by three aggregates, so the
// CTE is fenced: an inlined CTE would re-evaluate the census proof per
// aggregate.
static RANKED_FLEET_OCCUPANCY_CENSUS_AWARE: LazyLock<String> = LazyLock::new(|| {
    let active_candidate = non_resident_lifecycle_exclusion("sandboxes");
    let resident = resident_lifecycle("a");
    let present = occupies_node_predicate(
        "sandboxes",
        "sandboxes.active_node_id",
        ResidencyEvidence::NodeCensusAware,
    );
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let durable = snapshot_durable_for_source_release("c");
    let heartbeat = node_heartbeat_fresh("scope.node_id");
    sql!(format!(
        "WITH uploading AS MATERIALIZED (
             SELECT sn.source_node_id AS node_id, sn.sandbox_id,
                    -- Preserve legacy occupancy freshness even for a different
                    -- artifact; drain blockers use the stricter identity below.
                    BOOL_OR(s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at) AS legacy_upload,
                    BOOL_OR(true AND (
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
         ), active_rows AS MATERIALIZED (
             SELECT active_node_id AS node_id, lifecycle, lifecycle_changed_at,
                    {present} AS present
               FROM sandboxes
              WHERE active_node_id = ANY($1::UUID[]) AND ({active_candidate})
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
              WHERE s.target_node_id = ANY($1::UUID[]) AND (s.lifecycle = 'suspended' OR (true AND (
        s.lifecycle = 'suspending' OR (s.lifecycle = 'restoring' AND s.evacuation_restore_target IS NULL))))
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
              WHERE s.target_node_id IS DISTINCT FROM u.node_id OR NOT (s.lifecycle = 'suspended' OR (true AND (
        s.lifecycle = 'suspending' OR (s.lifecycle = 'restoring' AND s.evacuation_restore_target IS NULL))))
         ), facts AS MATERIALIZED (
             SELECT c.*,
                    CASE WHEN (targeted AND lifecycle = 'suspended')
                              OR legacy_upload OR current_upload
                         THEN (SELECT {durable}) ELSE FALSE END AS durable
               FROM candidates c
         ), occupancy AS (
             SELECT node_id,
                    COUNT(*) FILTER (WHERE ((targeted AND lifecycle = 'suspended') OR legacy_upload) AND NOT durable)::BIGINT AS local_snapshots,
                    COUNT(*) FILTER (
                        WHERE true AND (
                            (targeted AND (
                                lifecycle = 'suspending'
                                OR (lifecycle = 'restoring' AND evacuation_restore_target IS NULL)
                                OR (lifecycle = 'suspended' AND NOT durable)
                            )) OR (current_upload AND NOT durable)
                        )
                    )::BIGINT AS drain_blockers
               FROM facts  GROUP BY node_id
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
    ))
});

static GROUP_OCCUPANCY: LazyLock<String> = LazyLock::new(|| {
    let snapshot_exists = snapshot_durable_for_source_release("sandboxes");
    let inflight_from_group = inflight_suspend_upload_from_node_exists(
        "sandboxes",
        "ANY(SELECT id FROM nodes WHERE node_group_id = $1)",
    );
    let non_resident = non_resident_lifecycle_exclusion("sandboxes");
    sql!(format!(
        "SELECT
             COUNT(*) FILTER (
                 WHERE (
                         active_node_id IN (SELECT id FROM nodes WHERE node_group_id = $1)
                         AND {non_resident}
                       )
                    -- An in-flight restore/suspend pins a sandbox to a group
                    -- node via target_node_id while active_node_id is still
                    -- NULL (begin_restore_lifecycle clears active_node_id but
                    -- leaves target_node_id on the source). Mirror the
                    -- node-drain blocker (drain_blocking_sandboxes_query):
                    -- `suspending` always occupies; `restoring` occupies for
                    -- non-evacuation restores.
                    OR (
                         target_node_id IN (SELECT id FROM nodes WHERE node_group_id = $1)
                         AND (
                             lifecycle = 'suspending'
                             OR (lifecycle = 'restoring'
                                 AND evacuation_restore_target IS NULL)
                         )
                       )
             )::BIGINT AS active,
             COUNT(*) FILTER (
                 WHERE (
                         lifecycle = 'suspended'
                         AND target_node_id IN (SELECT id FROM nodes WHERE node_group_id = $1)
                         AND NOT {snapshot_exists}
                       )
                    OR ({inflight_from_group} AND NOT {snapshot_exists})
             )::BIGINT AS local_snapshots
         FROM sandboxes"
    ))
});

/// The statement [`node_occupancy`] runs for `ranked` and `residency`, for a
/// caller that inspects its plan.
pub fn node_occupancy_statement(ranked: bool, residency: ResidencyEvidence) -> &'static str {
    match (ranked, residency) {
        (false, _) => &NODE_OCCUPANCY,
        (true, ResidencyEvidence::LifecycleOnly) => &RANKED_NODE_OCCUPANCY,
        (true, ResidencyEvidence::NodeCensusAware) => &RANKED_NODE_OCCUPANCY_CENSUS_AWARE,
    }
}

/// The statement [`fleet_occupancy`] runs for `ranked` and `residency`, for a
/// caller that inspects its plan.
pub fn fleet_occupancy_statement(ranked: bool, residency: ResidencyEvidence) -> &'static str {
    match (ranked, residency) {
        (false, _) => &FLEET_OCCUPANCY,
        (true, ResidencyEvidence::LifecycleOnly) => &RANKED_FLEET_OCCUPANCY,
        (true, ResidencyEvidence::NodeCensusAware) => &RANKED_FLEET_OCCUPANCY_CENSUS_AWARE,
    }
}

/// `node_id`'s occupancy. With `ranked`, also its drainability counters
/// under `residency`, with long runners older than `long_runner_age_secs`.
pub async fn node_occupancy(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    long_runner_age_secs: i64,
    ranked: bool,
    residency: ResidencyEvidence,
) -> Result<NodeOccupancyCounts, sqlx::Error> {
    sqlx::query_as::<_, NodeOccupancyCounts>(sqlx::AssertSqlSafe(node_occupancy_statement(
        ranked, residency,
    )))
    .bind(node_id)
    .bind(long_runner_age_secs)
    .fetch_one(executor)
    .await
}

/// [`node_occupancy`] for every node in `node_ids`, one row each.
pub async fn fleet_occupancy(
    executor: impl sqlx::PgExecutor<'_>,
    node_ids: &[NodeId],
    long_runner_age_secs: i64,
    ranked: bool,
    residency: ResidencyEvidence,
) -> Result<Vec<NodeOccupancyCounts>, sqlx::Error> {
    sqlx::query_as::<_, NodeOccupancyCounts>(sqlx::AssertSqlSafe(fleet_occupancy_statement(
        ranked, residency,
    )))
    .bind(node_ids)
    .bind(long_runner_age_secs)
    .fetch_all(executor)
    .await
}

/// The occupancy of every node in `node_group_id`: live placements, plus
/// in-flight suspends and non-evacuation restores pinned to a group node, and
/// suspended sandboxes or in-flight uploads whose only snapshot is on a group
/// node's disk.
pub async fn group_occupancy(
    executor: impl sqlx::PgExecutor<'_>,
    node_group_id: NodeGroupId,
) -> Result<GroupOccupancyCounts, sqlx::Error> {
    let (active, local_snapshots): (i64, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(GROUP_OCCUPANCY.as_str()))
            .bind(node_group_id)
            .fetch_one(executor)
            .await?;
    Ok(GroupOccupancyCounts {
        active,
        local_snapshots,
    })
}

#[cfg(test)]
mod tests;
