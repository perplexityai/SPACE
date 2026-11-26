//! The admin console's occupancy reads: what holds a node, or a node group,
//! occupied, and what a drain of it would wait on.
//!
//! Each statement is whole here, built once with every value bound. The
//! shared definitions they interpolate are the residency rules of
//! `pplx_space_sandbox_residency` and the durability rules of
//! `pplx_space_sandbox_portability`. A node's statements come in a fixed set
//! of shapes: without the drainability counters, and with them under each
//! [`ResidencyEvidence`], plus the `:terminate` guard's count under each
//! [`ResidencyEvidence`]. The `:force-upload-local-snapshots` enumeration
//! writes its page size into its statement as a literal, for the planner; see
//! [`pinned_local_only_sandboxes`].
//!
//! `batched_occupancy`, the pre-optimization read the api's differential
//! tests compare the node and fleet reads against, is not built once: it is
//! compiled only for tests and assembles its text per call, as it did when the
//! api held it.

use std::num::NonZeroUsize;
use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::NodeGroupId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
#[cfg(any(test, feature = "test-support"))]
use pplx_space_sandbox_portability::drain_gate_blocker_pairs;
use pplx_space_sandbox_portability::{
    drain_gate_blockers_for_node, inflight_suspend_upload_exists,
    inflight_suspend_upload_from_node_exists, snapshot_durable_for_source_release,
    snapshot_is_current_local_artifact,
};
use pplx_space_sandbox_residency::{
    ResidencyEvidence, active_resident_predicate, node_heartbeat_fresh,
    non_resident_lifecycle_exclusion, occupies_node_predicate, resident_lifecycle,
};
use pplx_space_sql_locator::sql;

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
                    -- node-drain blocker (drain_gate_blockers::count_on_node):
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
pub(crate) fn node_occupancy_statement(ranked: bool, residency: ResidencyEvidence) -> &'static str {
    match (ranked, residency) {
        (false, _) => &NODE_OCCUPANCY,
        (true, ResidencyEvidence::LifecycleOnly) => &RANKED_NODE_OCCUPANCY,
        (true, ResidencyEvidence::NodeCensusAware) => &RANKED_NODE_OCCUPANCY_CENSUS_AWARE,
    }
}

/// The statement [`fleet_occupancy`] runs for `ranked` and `residency`, for a
/// caller that inspects its plan.
pub(crate) fn fleet_occupancy_statement(
    ranked: bool,
    residency: ResidencyEvidence,
) -> &'static str {
    match (ranked, residency) {
        (false, _) => &FLEET_OCCUPANCY,
        (true, ResidencyEvidence::LifecycleOnly) => &RANKED_FLEET_OCCUPANCY,
        (true, ResidencyEvidence::NodeCensusAware) => &RANKED_FLEET_OCCUPANCY_CENSUS_AWARE,
    }
}

/// `node_id`'s occupancy. With `ranked`, also its drainability counters
/// under `residency`, with long runners older than `long_runner_age_secs`.
pub async fn node_occupancy(
    executor: impl Executor<'_>,
    node_id: NodeId,
    long_runner_age_secs: i64,
    ranked: bool,
    residency: ResidencyEvidence,
) -> Result<NodeOccupancyCounts, DbError> {
    Ok(
        sqlx::query_as::<_, NodeOccupancyCounts>(sqlx::AssertSqlSafe(node_occupancy_statement(
            ranked, residency,
        )))
        .bind(node_id)
        .bind(long_runner_age_secs)
        .fetch_one(executor.into_sqlx())
        .await?,
    )
}

/// [`node_occupancy`] for every node in `node_ids`, one row each.
pub async fn fleet_occupancy(
    executor: impl Executor<'_>,
    node_ids: &[NodeId],
    long_runner_age_secs: i64,
    ranked: bool,
    residency: ResidencyEvidence,
) -> Result<Vec<NodeOccupancyCounts>, DbError> {
    Ok(
        sqlx::query_as::<_, NodeOccupancyCounts>(sqlx::AssertSqlSafe(fleet_occupancy_statement(
            ranked, residency,
        )))
        .bind(node_ids)
        .bind(long_runner_age_secs)
        .fetch_all(executor.into_sqlx())
        .await?,
    )
}

/// The lifecycle scope of the `active` occupancy count: everything that has
/// neither terminated (`failed` / `deleted`) nor migrated off the node
/// (`suspended`).
///
/// `deleted` is not a value `SandboxLifecycle` produces: it is the tombstone
/// raw SQL writes into the TEXT column.
///
/// The node and fleet statements render the same
/// `non_resident_lifecycle_exclusion`, and the `:terminate` guard reads it
/// through `active_resident_predicate`, so the oracle and the reads it checks
/// share one lifecycle list.
///
/// The `:terminate` guard and the listing's `active` are no longer equal counts,
/// and deliberately so. The guard additionally applies the census discount, so it
/// matches the listing's `terminate_blockers` — not its `active`, which stays raw
/// precisely so an operator can see the phantom rows the discount drops. The figure
/// that refuses is therefore a figure the listing publishes; that equality is pinned
/// by `the_guard_refuses_exactly_what_the_listing_calls_terminate_blockers`.
///
/// The list itself now lives next to `RESIDENT_LIFECYCLES` in the shared residency
/// module as `pplx_space_sandbox_residency::NON_RESIDENT_LIFECYCLES`, rendered by
/// [`non_resident_lifecycle_exclusion`] — the hoist this comment used to prescribe.
/// So `terminate_blockers` no longer merely *predicts* the cluster-controller's
/// drained-node terminator; it and that reconciler read the same definition, and
/// node-drain's `draining → drained` gate consults it too before releasing a node.
/// What remains local to this helper is only the alias-shaped rendering.
///
/// This is deliberately *not* the scope of the live-resident drainability
/// count, which narrows to `RESIDENT_LIFECYCLES`: occupancy blocks
/// `:terminate`, while the ranking predicts what a drain waits on, and a
/// `stopped` sandbox does the first without doing the second.
///
/// `alias` is the SQL alias for the `sandboxes` row in the calling query — pass
/// the bare table name (`"sandboxes"`) when no alias is set.
#[cfg(any(test, feature = "test-support"))]
fn active_occupancy_lifecycles(alias: &str) -> String {
    format!("({})", non_resident_lifecycle_exclusion(alias))
}

/// Whether the per-row occupancy CTE should be `MATERIALIZED`, as the SQL
/// keyword to splice in.
///
/// Materializing is load-bearing only when the row flags actually carry the
/// census absence proof. The aggregates over that CTE reference the projected
/// booleans three times, and an inlined CTE lets Postgres expand those aliases
/// and re-evaluate whatever they stand for per aggregate — restoring the per-row
/// SubPlan over the census relation and pushing the plan back over
/// `jit_above_cost`.
///
/// The node-drain reconciler avoids the same duplication the other way: its
/// resident-count query stays flat and keeps the absence proof under a single
/// `FILTER`, so no second reference to it exists for the planner to expand.
/// That shape is not available here — the ranking needs three counts that all
/// narrow the same proof, so the proof is referenced more than once by
/// construction and the fence is what stops the re-expansion.
///
/// In every other mode nothing on the row is worth fencing: the census flag is
/// the literal TRUE, and the two flags that remain — an `IN` test over a column
/// already on the row, and one `make_interval` comparison against a bound
/// parameter — are each referenced once by the aggregates, so there is no
/// repeated evaluation to prevent. Materializing would still cost: it would spool every active sandbox row in the listed set into a
/// tuplestore, which on a fleet listing is millions of rows and can spill to
/// temporary storage. The ordinary listing path — which asks for no drainability
/// counters at all — must therefore keep the streaming shape it had before the
/// ranking existed.
#[cfg(any(test, feature = "test-support"))]
fn row_cte_materialization(ranking_requested: bool, residency: ResidencyEvidence) -> &'static str {
    if ranking_requested && residency.discounts_absent_rows() {
        " MATERIALIZED"
    } else {
        ""
    }
}

/// The occupancy of every node in `node_ids`, one row each, read by the
/// pre-optimization batched statement. Retained as the oracle for the api's
/// differential tests and benchmarks of [`node_occupancy`] and
/// [`fleet_occupancy`]; nothing in production runs it.
///
/// `long_runner_age_secs` opts into the drainability counters, with long
/// runners older than it; `None` leaves them zero. The long-runner
/// comparison is against the database's `NOW()`, so the caller's executor
/// decides which instant it reads.
#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn batched_occupancy(
    executor: impl Executor<'_>,
    node_ids: &[NodeId],
    long_runner_age_secs: Option<i64>,
    residency: ResidencyEvidence,
) -> Result<Vec<NodeOccupancyCounts>, DbError> {
    // `local_snapshot` counts strand-risk sandboxes for a node: a
    // suspended sandbox pinned to it whose snapshot is **not** yet
    // durably in S3 (attributed to `target_node_id`), plus any sandbox
    // whose suspend-snapshot upload is still in flight *from* it
    // (`snapshots.state='creating'`, attributed to `source_node_id`).
    // UNION deduplicates the suspended and in-flight branches per
    // (node, sandbox), matching the `:terminate` guard's per-sandbox count.
    // When the upload source differs from the target, both nodes count.
    // The shared marker-aware `snapshot_durable_for_source_release`
    // predicate is the single source of truth for "durably restorable from
    // S3": an evacuation-marked sandbox counts as strand risk until its
    // *newest* full snapshot is `ready` (matching the node-drain
    // source-release guards), while a plain user-suspended sandbox uses
    // placement-freshness.
    // Quarantined sandboxes are counted as active occupancy because
    // quarantine preserves the hot-node association for later inspection.
    //
    // Deliberately **not** census-aware, unlike the `:terminate` guard in
    // `admin_occupancy::terminate_occupancy`. This scan is multi-node
    // (`ANY($1::UUID[])`), so the discount's node expression would have to be
    // the per-row `active_node_id` column rather than a bound parameter —
    // the shape `sandbox_residency::node_heartbeat_fresh` documents as
    // costing 30–120 ms per call by forcing a hashed SubPlan over
    // `node_status_snapshots`. This listing refuses nothing, so over-counting
    // shows an operator a high number rather than blocking an operation. The
    // guard, which does refuse, pays for accuracy instead.
    let snapshot_exists = snapshot_durable_for_source_release("s");
    // Durability check for the in-flight UNION branch (which joins
    // `sandboxes s2`): a sandbox already durably restorable must not count
    // as strand risk just because an earlier `creating` row lingers. Uses
    // the same marker-aware predicate as the suspended branch and the
    // in-tx guard so all three agree. (Its inner `EXISTS` reuses the alias
    // `sn`, shadowing the outer `snapshots sn` within the subquery — the
    // subquery is self-contained, correlating only via `s2.id`.)
    let portable_for_snapshot = snapshot_durable_for_source_release("s2");
    let active_lifecycles = active_occupancy_lifecycles("sandboxes");
    // Per-row drainability flags, carried on the `active` branch only —
    // `live_residents` is by definition a subset of `active`, so deriving
    // both from the same scan is what makes the subset relation hold by
    // construction instead of by two queries agreeing. The census predicate
    // is read only in its TRUE direction, per that module's contract: `NOT
    // proven_absent` means "not proven absent", never "proven present".
    //
    // `present` is the census-aware "the node has not proven this row
    // absent" test, projected once per row. The three counts below narrow it
    // by ANDing the projected booleans rather than re-stating the predicate,
    // so the absence proof is written into the statement once and evaluated
    // once per row. Each narrowing answers a different gate's question:
    //
    // - `active` — no narrowing. The occupancy figure the `:terminate` guard
    //   enforces, phantoms included.
    // - `present` — what the drained-node terminator blocks on. It discounts
    //   phantom rows exactly as the terminator does, but keeps `stopped` /
    //   `created` / `deleting` / `quarantined`, which the census reports and
    //   the terminator therefore still refuses to advance past. Ranking on
    //   the raw occupancy count instead would prefer a node holding one
    //   reported `stopped` row over one holding fifty phantoms — backwards.
    //   Under `LifecycleOnly` the discount is a no-op: `present` is the
    //   literal TRUE and this count equals `active` exactly, so the phantom
    //   argument only bites on a cluster that has opted in.
    // - `present AND resident` — what a drain waits on. `RESIDENT_LIFECYCLES`
    //   drops `stopped` / `created` / `deleting` / `quarantined` because
    //   there is no VM the pipeline can evacuate. So
    //   `active` minus this is phantom rows *plus* those non-evacuable
    //   lifecycles: "not blocking a drain" is broader than "not really
    //   there".
    //
    // `residency` is the caller's, read from the same switch the drain
    // reconciler reads, so the counts describe whichever policy the
    // cluster's gates run under rather than a second opinion about it.
    //
    // Without a threshold every flag is the literal FALSE, so Postgres plans
    // no census probe at all and the default listing keeps its old cost.
    let (present_flag, resident_flag, aged_flag) = match long_runner_age_secs {
        Some(_) => {
            // The reconciler's own policy branch, not a second copy of it:
            // under the default `LifecycleOnly` this is the literal `TRUE`,
            // so no census probe is planned at all and every occupying row
            // counts — which is what the drain and terminator gates do in
            // that mode. Reading the same switch is the only way the ranking
            // predicts what those gates will actually do.
            (
                occupies_node_predicate("sandboxes", "sandboxes.active_node_id", residency),
                resident_lifecycle("sandboxes"),
                "sandboxes.lifecycle_changed_at < NOW() - make_interval(secs => $2::BIGINT)"
                    .to_string(),
            )
        }
        None => (
            "FALSE".to_string(),
            "FALSE".to_string(),
            "FALSE".to_string(),
        ),
    };
    let row_cte_materialization =
        row_cte_materialization(long_runner_age_secs.is_some(), residency);
    // Per-node heartbeat freshness, evaluated once per listed node in the
    // `scope` CTE rather than derived later from the separate `list()`
    // read. It is the ranking's first term, so reading it from a different
    // statement than the counts it orders lets a node that flipped
    // reachable between the two sort by one snapshot's visibility and
    // another's occupancy.
    // The ranking's visibility term and the rows' discount term are the same
    // definition, evaluated here against the unnested node id.
    let heartbeat_fresh = node_heartbeat_fresh("scope_input.node_id");
    // The reconciler's own `draining -> drained` gate: two of the states it
    // counts pin through `target_node_id` while `active_node_id` is NULL, so
    // the occupancy branches cannot see them and a node whose drain is held
    // open would otherwise report zero everywhere.
    //
    // Counted from the batched pair form, grouped once for the whole node
    // set. The single-node predicate is not reusable per node here: its two
    // disjuncts attribute to different columns, so a correlated evaluation
    // leaves the planner no indexable driver on `sandboxes` and degrades to a
    // full scan of that table per listed node when sorting by drainability.
    //
    // Gated on the same opt-in as the census probes, and the count travels
    // inside the `Option` alongside the live counters, so a caller cannot
    // read a skipped computation as "no blockers".
    let blockers_cte = match long_runner_age_secs {
        Some(_) => format!(
            "SELECT node_id, COUNT(*)::BIGINT AS drain_blockers\n\
               FROM (\n{}\n) gate_blockers\n\
              GROUP BY node_id",
            drain_gate_blocker_pairs("$1::UUID[]")
        ),
        // Shape-compatible and empty, so the join below yields no row and
        // the COALESCE reports zero without the scan.
        None => "SELECT NULL::UUID AS node_id, 0::BIGINT AS drain_blockers\n\
                  WHERE FALSE"
            .to_string(),
    };
    let query = format!(
        "WITH scope AS (
                 SELECT scope_input.node_id AS node_id,
                        {heartbeat_fresh} AS heartbeat_fresh
                   FROM UNNEST($1::UUID[]) AS scope_input(node_id)
             ),
             blockers AS (
             {blockers_cte}
             ),
             occupancy_rows AS{row_cte_materialization} (
                   -- `sandbox_id` here feeds no aggregate; it is what makes
                   -- this arm's arity match the deduped local-snapshot arm below
                   -- across the `UNION ALL`. Dropping it as unused breaks the
                   -- statement at runtime, not at compile time.
                   SELECT active_node_id AS node_id, id AS sandbox_id,
                          'active' AS kind,
                          ({present_flag}) AS present,
                          ({resident_flag}) AS resident,
                          ({aged_flag}) AS aged
                     FROM sandboxes
                    WHERE active_node_id = ANY($1::UUID[])
                      AND {active_lifecycles}
                   UNION ALL
                   -- One row per (node, sandbox), deduped *here* rather than by a
                   -- DISTINCT aggregate below. `local_snapshots` counts
                   -- sandboxes, and two of these branches can describe one
                   -- sandbox on one node. UNION collapses that pair without
                   -- a correlated in-flight-upload exclusion for every suspended
                   -- row. Keep both nodes when the target differs from the source.
                   --
                   -- Deduping here and keeping a plain `COUNT(*) FILTER` below is
                   -- deliberate: a `COUNT(DISTINCT …)` in the aggregate disables
                   -- hashed aggregation for the whole `GROUP BY`. Measured at
                   -- 200k active rows, that planned a `GroupAggregate` over a
                   -- Sort of *every* occupancy row — active rows included —
                   -- where the plain count plans a `HashAggregate` over the
                   -- `Append` (whose active arm still gathers in parallel). The
                   -- sort here is not removed so much as bounded: this `UNION`
                   -- plans as Sort → Unique over the strand-risk rows alone,
                   -- which is the trade. Paying the unbounded version on the
                   -- default listing, which asks for none of this, would be a
                   -- regression.
                   SELECT node_id, sandbox_id, 'local_snapshot' AS kind,
                          FALSE AS present, FALSE AS resident, FALSE AS aged
                     FROM (
                         SELECT target_node_id AS node_id, s.id AS sandbox_id
                           FROM sandboxes s
                          WHERE target_node_id = ANY($1::UUID[])
                            AND lifecycle = 'suspended'
                            AND NOT {snapshot_exists}
                         UNION
                         -- A sandbox with several `creating` rows from the same
                         -- source counts once, matching the in-tx guard's
                         -- per-sandbox EXISTS. Freshness-gated and
                         -- fresh-ready-excluded so stale prior-cycle remnants and
                         -- already-portable sandboxes don't count.
                         --
                         -- The gate here is the *legacy* one, while the in-tx
                         -- guard's in-flight term goes through the artifact-aware
                         -- fragment, so the two still disagree per node in one
                         -- case: a `creating` row naming an artifact other than
                         -- the sandbox's current one, sourced from a node that is
                         -- not also its target. This arm counts that source node;
                         -- the guard does not. The listing therefore
                         -- **over**-reports — an operator who expects
                         -- `:terminate` to refuse on the strength of it will find
                         -- the termination allowed. Closing that means routing
                         -- this arm through the shared fragment, which is what
                         -- the batched blocker form does and is a change to
                         -- pre-existing strand-risk counting, so it is tracked
                         -- rather than folded in here.
                         SELECT sn.source_node_id AS node_id, sn.sandbox_id
                           FROM snapshots sn
                           JOIN sandboxes s2 ON s2.id = sn.sandbox_id
                          WHERE sn.source_node_id = ANY($1::UUID[])
                            AND sn.type = 'full'
                            AND sn.state = 'creating'
                            AND (
                                s2.placement_changed_at IS NULL
                                OR sn.timestamp >= s2.placement_changed_at
                            )
                            AND NOT {portable_for_snapshot}
                     ) local_snapshot_dedup
             ),
             occupancy AS (
             SELECT node_id,
                    COUNT(*) FILTER (WHERE kind = 'active')::BIGINT AS active,
                    -- Safe as a row count because the branch above already
                    -- deduped per (node, sandbox); see the note there for why
                    -- the dedup is not a `COUNT(DISTINCT …)` here.
                    COUNT(*) FILTER (
                        WHERE kind = 'local_snapshot'
                    )::BIGINT AS local_snapshots,
                    COUNT(*) FILTER (
                        WHERE kind = 'active' AND present
                    )::BIGINT AS terminate_blockers,
                    COUNT(*) FILTER (
                        WHERE kind = 'active' AND present AND resident
                    )::BIGINT AS live_residents,
                    COUNT(*) FILTER (
                        WHERE kind = 'active' AND present AND resident AND aged
                    )::BIGINT AS live_long_runners
               FROM occupancy_rows
              WHERE node_id IS NOT NULL
              GROUP BY node_id
             )
             SELECT scope.node_id,
                    COALESCE(occupancy.active, 0)::BIGINT AS active,
                    COALESCE(occupancy.local_snapshots, 0)::BIGINT AS local_snapshots,
                    COALESCE(occupancy.terminate_blockers, 0)::BIGINT AS terminate_blockers,
                    COALESCE(occupancy.live_residents, 0)::BIGINT AS live_residents,
                    COALESCE(occupancy.live_long_runners, 0)::BIGINT AS live_long_runners,
                    COALESCE(blockers.drain_blockers, 0)::BIGINT AS drain_blockers,
                    scope.heartbeat_fresh
               FROM scope
               LEFT JOIN occupancy ON occupancy.node_id = scope.node_id
               LEFT JOIN blockers ON blockers.node_id = scope.node_id"
    );
    let mut statement =
        sqlx::query_as::<_, NodeOccupancyCounts>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_ids);
    // Bound only when the threshold is in play, because `$2` appears in the
    // statement only then — sqlx rejects a bind the SQL never references.
    if let Some(age_secs) = long_runner_age_secs {
        statement = statement.bind(age_secs);
    }
    Ok(statement.fetch_all(executor.into_sqlx()).await?)
}

/// The occupancy of every node in `node_group_id`: live placements, plus
/// in-flight suspends and non-evacuation restores pinned to a group node, and
/// suspended sandboxes or in-flight uploads whose only snapshot is on a group
/// node's disk.
pub async fn group_occupancy(
    executor: impl Executor<'_>,
    node_group_id: NodeGroupId,
) -> Result<GroupOccupancyCounts, DbError> {
    let (active, local_snapshots): (i64, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(GROUP_OCCUPANCY.as_str()))
            .bind(node_group_id)
            .fetch_one(executor.into_sqlx())
            .await?;
    Ok(GroupOccupancyCounts {
        active,
        local_snapshots,
    })
}

/// What the `:terminate` guard counts, kept separate on purpose from the
/// node listing's counts ([`NodeOccupancyCounts`], which the api reports
/// as `SandboxOccupancy`).
///
/// The two are produced by different statements for different jobs.
/// `SandboxOccupancy` is what the node *listing* reports, and its
/// `local_snapshots` is a diagnostic an operator reads. This one is what the
/// 409 is *decided* on, and its second term is the shared node-drain gate
/// (`drain_gate_blockers_for_node`) so the endpoint and the reconciler cannot
/// disagree about whether a node is safe to terminate.
///
/// Sharing one struct would have forced one meaning on both, and the
/// meanings are genuinely different — see the comment on the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminateOccupancy {
    pub active: i64,
    /// Rows the node-drain gate (`drain_gate_blockers_for_node`) counts
    /// against this node. It is not a count of local snapshots; the listing's
    /// [`NodeOccupancyCounts::local_snapshots`] is.
    pub drain_blockers: i64,
}

/// The statement [`terminate_occupancy`] runs under `residency`.
fn terminate_occupancy_for(residency: ResidencyEvidence) -> String {
    // `active` is `active_resident_predicate` for the node, census term
    // included, so this guard counts exactly the rows node-drain's
    // `draining → drained` gate and `drained_node::mark_terminating` count.
    // The census term matters because this endpoint refuses on `active` and
    // `space-rollout-mng.py` drives it automatically: without it, a worker
    // whose own census disproves its phantom `active_node_id` rows would clear
    // the census-aware drain gate, reach `drained`, and then be refused here
    // forever wherever `SPACE_NODE_DRAIN_NODE_CENSUS_RESIDENCY` is on. `$1` is
    // a bound parameter, which keeps the heartbeat subquery a once-per-execution
    // InitPlan.
    //
    // Quarantined sandboxes remain active placement blockers by design:
    // quarantine preserves the hot-node association until a quarantine-aware
    // release or cleanup path decides it is safe to drop.
    let active = active_resident_predicate("sandboxes", "$1", residency);
    // `drain_blockers` is exactly `drain_gate_blockers_for_node`, so
    // `:terminate` and the drain reconciler agree on whether a node is safe to
    // terminate. The gate holds an evacuation-marked sandbox until its newest
    // full snapshot is `ready` (`snapshot_durable_for_source_release`), and a
    // suspend upload still in flight *from* this node even when the sandbox's
    // `target_node_id` has drifted elsewhere, because its second arm drives
    // from `snapshots.source_node_id`: terminating mid-upload kills the worker
    // holding the only durable copy. It also counts rows pinned to the node by
    // `target_node_id` while `active_node_id` is NULL, such as an in-flight
    // user restore of a suspended sandbox (`drain_gate_target_pinned_blocks`),
    // which the `active` count cannot see.
    //
    // Each count is its own node-scoped subquery.
    //
    // The listing's `local_snapshots` is a different number on purpose. It is
    // reported, not gated on, and it legitimately differs from
    // `drain_blockers`: the listing's in-flight branch inlines the legacy
    // freshness gate while the blocker fragment takes the artifact-exact arm
    // (see the api's `routes/admin/node_store/live_residents_tests.rs`,
    // `an_artifact_mismatched_inflight_upload_counts_the_sandbox_once`).
    let blockers = drain_gate_blockers_for_node("$1");
    sql!(format!(
        "SELECT
     (SELECT COUNT(*)::BIGINT FROM sandboxes WHERE {active}) AS active,
     (SELECT COUNT(*)::BIGINT FROM (
{blockers}
          ) gate_blockers) AS drain_blockers"
    ))
}

static TERMINATE_OCCUPANCY: LazyLock<String> =
    LazyLock::new(|| terminate_occupancy_for(ResidencyEvidence::LifecycleOnly));
static TERMINATE_OCCUPANCY_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| terminate_occupancy_for(ResidencyEvidence::NodeCensusAware));

/// The occupancy lookup used by the `:terminate` guard so the
/// strand-risk count is observed under the same `FOR UPDATE` lock as the
/// drain-phase write.
///
/// Run it on the caller's transaction: that is what keeps the count under
/// the caller's lock.
pub async fn terminate_occupancy(
    executor: impl Executor<'_>,
    node_id: NodeId,
    residency: ResidencyEvidence,
) -> Result<TerminateOccupancy, DbError> {
    let statement: &str = match residency {
        ResidencyEvidence::LifecycleOnly => &TERMINATE_OCCUPANCY,
        ResidencyEvidence::NodeCensusAware => &TERMINATE_OCCUPANCY_CENSUS_AWARE,
    };
    let row: (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .fetch_one(executor.into_sqlx())
        .await?;
    Ok(TerminateOccupancy {
        active: row.0,
        drain_blockers: row.1,
    })
}

/// How many rows [`pinned_local_only_sandboxes`] returns at most: the
/// `:force-upload-local-snapshots` handler's enumeration cap plus the one
/// sentinel row that signals truncation. Written into the statement as a
/// literal; see [`pinned_local_only_sandboxes`].
pub const FORCE_UPLOAD_PAGE: NonZeroUsize = NonZeroUsize::new(257).expect("non-zero");

/// The statement [`pinned_local_only_sandboxes`] runs, with `limit` as its
/// literal `LIMIT`. Production builds it once, at [`FORCE_UPLOAD_PAGE`].
fn pinned_local_only_sandboxes_for(limit: NonZeroUsize) -> String {
    let snapshot_exists = snapshot_durable_for_source_release("s");
    let inflight_upload = inflight_suspend_upload_exists("s");
    format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.target_node_id = $1
            AND s.lifecycle = 'suspended'
            AND CASE WHEN (SELECT {snapshot_exists}) THEN false
                     ELSE NOT (SELECT {inflight_upload}) END
          ORDER BY s.id
          LIMIT {limit}"
    )
}

static PINNED_LOCAL_ONLY_SANDBOXES: LazyLock<String> =
    LazyLock::new(|| sql!(pinned_local_only_sandboxes_for(FORCE_UPLOAD_PAGE)));

/// The statement [`pinned_local_only_sandboxes`] runs, for a caller that
/// inspects its plan.
#[cfg(test)]
fn pinned_local_only_sandboxes_statement() -> &'static str {
    &PINNED_LOCAL_ONLY_SANDBOXES
}

/// The statement [`pinned_local_only_sandboxes`] runs, with `limit` in place
/// of [`FORCE_UPLOAD_PAGE`], for tests that need a page smaller or larger than
/// production's. It has no locator comment.
#[cfg(test)]
fn pinned_local_only_sandboxes_statement_with_limit(limit: NonZeroUsize) -> String {
    pinned_local_only_sandboxes_for(limit)
}

/// Suspended sandboxes whose `target_node_id` is `node_id` and whose snapshot
/// has not yet been registered to S3, as the lowest [`FORCE_UPLOAD_PAGE`]
/// sandbox ids. Used by the api's `:force-upload-local-snapshots` admin
/// handler to enumerate the sandboxes that still need a durable upload before
/// the source node can terminate.
///
/// `pinned_local_only_sandboxes_statement` returns the statement this runs,
/// so the plan-structure regression (`pinned_local_only_plan_tests`)
/// can `EXPLAIN` it; the `sql!` locator comment it starts with changes no
/// plan.
///
/// [`FORCE_UPLOAD_PAGE`] is the handler's enumeration cap plus one, so a
/// returned slice of that length is the unambiguous "more pinned sandboxes
/// exist beyond what's listed" signal — the handler trims the sentinel row
/// and surfaces `truncated: true` to the operator. This bounds DB transfer
/// and JSON response size to O(1) regardless of how big the node's
/// pinned-list actually is.
///
/// ## The page is the ascending-id prefix, and that is a contract
///
/// `ORDER BY s.id` makes the page stable across re-invocations: the handler
/// slices `to_process` from the head and the `deferred` tail in Rust, so a
/// non-deterministic row order would publish a `deferred[]` array that is a
/// sample rather than the work the next call picks up.
///
/// A head that keeps failing therefore keeps being retried while the rows
/// behind it wait. That residual is deliberate and it is loud rather than
/// silent — the same ids come back in `failed[]` and the same `deferred[]` on
/// every call. Reshuffling the page so a stuck sandbox is eventually skipped
/// would hide it instead.
///
/// ## What makes the ordering cheap
///
/// `idx_sandboxes_target_node_ordered` covers `(target_node_id, id)`. The equality
/// on `$1` fixes the leading column, so the scan emits the node's rows already
/// ordered by `id`: the ordering needs no `Sort`, and the `LIMIT` stops at the
/// page size. Work is bounded by the node's own pinned history — which is the
/// question being asked — rather than by the table.
///
/// The index makes that plan available; the predicate's shape keeps the
/// planner on it. Written as two bare conjuncts, the probes can be pulled up
/// into a join-driven plan that reads the whole table even with this index
/// present, and on the test fixture a generic plan does exactly that. The
/// `CASE` and the scalar-subquery fences below each rule that plan out.
///
/// The planner may still pick the narrow `idx_sandboxes_target_node` plus a
/// sort when the table is small enough that the two cost the same. That plan
/// is node-bounded too, which is the property that matters, so the sort's
/// absence is the common case rather than an invariant.
///
/// Without that index the planner takes the ordering from the primary key's
/// own index instead, expecting the `LIMIT` to stop early. For a node whose
/// non-durable set is empty there is nothing to stop early on, so that plan
/// filters the whole table and runs past the 10 s admin-handler budget. Empty
/// is the ordinary answer here: it is what an operator gets once the backlog
/// has cleared.
///
/// ## Why both probes are wrapped in scalar subqueries
///
/// The `CASE` described below already keeps both checks out of reach of
/// sublink pull-up, which Postgres applies only to a top-level `WHERE` or
/// `JOIN … ON` conjunct, including a top-level `NOT` directly over an
/// `EXISTS`. The fences are a second guard for the conjunct form: if the
/// predicate is ever written as conjuncts, they keep the plan this
/// section describes. The rest of the section is about that form.
///
/// `(SELECT {snapshot_exists})` rather than a bare `{snapshot_exists}`, and
/// the same for the in-flight check. The two forms are identical in truth —
/// `EXISTS` yields a non-NULL boolean and the scalar subquery returns exactly
/// one row — but as a top-level conjunct the bare sublink is *pullable*:
/// Postgres may realise it as a nested loop, a hash anti-join, or a merge
/// anti-join over the whole `snapshots` ⋈
/// `snapshot_storage_locations` product. A nested loop keeps the work bounded
/// to the requested sandboxes. Inside a scalar subquery it cannot be pulled up
/// at all, so it is always a per-row `SubPlan` whose own `sn.sandbox_id = s.id`
/// makes each execution a `sandbox_id`-keyed index probe.
///
/// The node-drain twin fences the same predicate for the same reason. Updated
/// statistics can otherwise make a merge anti-join cheaper in the planner's
/// estimate without any query change. The fence removes that alternative.
///
/// The in-flight check is fenced for the same reason, and here this statement
/// departs from its node-drain twin, which leaves that check bare. Bare, it is
/// the planner's cost choice. It can read the cluster's `state='creating'` full
/// snapshots once and anti-join against them, which is cheap, but the
/// node-ordered scan beneath it also makes a merge anti-join
/// along `idx_snapshots_sandbox_id` available, and that one walks every
/// snapshot whose sandbox id sorts below the page, whichever node owns it.
/// Fenced, each execution is correlated on the one sandbox being asked about.
/// That doesn't make each execution cheap. Which index serves it is still a
/// cost choice: the test fixtures' plans use a scan keyed on
/// `idx_snapshots_sandbox_id`, but a planner that overestimates how many
/// snapshots a sandbox has can instead `BitmapAnd` that index with
/// `idx_snapshots_state_type`, whose side reads the cluster's whole in-flight
/// set on every execution. The test fixture doesn't reproduce that plan, and
/// the plan tests' read counts don't include a bitmap index scan's reads,
/// which is why they also bound how often the check runs. The fence bounds which
/// sandboxes the check is about; the evaluation order below bounds how often
/// it runs.
///
/// ## Why the in-flight check sits in a `CASE` branch
///
/// `CASE WHEN (SELECT {snapshot_exists}) THEN false ELSE NOT (SELECT
/// {inflight_upload}) END` rather than two `AND NOT` conjuncts. The two forms
/// are identical in truth, since neither scalar subquery can yield NULL. But
/// Postgres orders the conjuncts of a filter by its own cost estimate, so it's
/// free to evaluate the in-flight check first, on every suspended row the node
/// ever pinned. A long-lived node's pinned history is almost entirely durable,
/// so that order runs the check once per row of history. Under the bitmap plan
/// above, each of those executions also passes over the in-flight set, and the
/// statement's cost becomes history × in-flight set.
///
/// Postgres documents that a `CASE` evaluates only the subexpressions needed
/// to determine its result, in order, so the in-flight check runs only for
/// rows the durability check didn't exclude. That's the node's backlog, which
/// is what an operator came to drain, and it holds whichever plan the planner
/// picks for the check itself. Postgres calls the rule not ironclad and names
/// constant subexpressions folded at planning time and aggregates as the
/// cases that break it. Neither applies: both conditions read the row.
///
/// The fixed order has a cost of its own: the durability check runs on
/// every suspended row, including those the in-flight check alone would have
/// excluded. That cost is bounded by the node's rows, and each execution has
/// an index that fits it exactly: `idx_snapshots_sandbox_ready_latest_covering`
/// is keyed on `sandbox_id` with the probe's own `state = 'ready' AND type =
/// 'full'` as its predicate, and it serves the probe in every plan the tests
/// measure. `snapshot_scans_stay_within_the_nodes_snapshots` and
/// `inflight_check_runs_only_for_non_durable_rows` both bound its reads per
/// scan; only the first bounds the in-flight check's the same way.
///
/// ## Why the page size is a literal
///
/// [`FORCE_UPLOAD_PAGE`] is written into the statement as a literal rather
/// than bound. `LIMIT $2` is not a constant when Postgres builds a *generic*
/// plan, so there is no `tuple_fraction` to apply and the planner loses the
/// low-startup bias the cap exists to buy — and sqlx prepares every
/// statement, so the plan cache decides which kind of plan this gets. The
/// value is a store constant, never caller- or operator-supplied, and
/// `NonZeroUsize` renders as digits.
///
/// ## What the predicates mean
///
/// Routes through the shared marker-aware
/// [`snapshot_durable_for_source_release`] fragment so the "still needs a
/// durable upload" predicate here matches the admin occupancy counters and the
/// node-drain drain-blocker / terminator source-release guards exactly —
/// drifting them produces a node the counter says is stuck while the handler
/// enumerates nothing (or vice versa). An evacuation-marked sandbox whose
/// newest snapshot is already `ready` is **not** enumerated (it needs no
/// further upload), even if a failed-restore placement bump made it
/// non-placement-fresh. (The API drain-gate bypass and restore-affinity hint
/// answer a different question — user-op admission and placement — and keep the
/// freshness predicate; they are intentionally not source-release guards.)
///
/// This is deliberately the authoritative predicate rather than the per-node
/// backup-pending queue the drain reconciler consumes. The endpoint exists to
/// rescue what the automated path missed, so reading the derived state it
/// exists to rescue would make exactly the situation an operator reaches for it
/// in the situation it cannot fix. Keeping the two definitions separate also
/// makes this a drift oracle against that queue.
///
/// Excludes sandboxes whose durable upload is already in flight from any node
/// (`NOT inflight_suspend_upload_exists`), mirroring the node-drain reupload
/// queue (`pinned_local_only::page`):
/// force-reuploading a sandbox
/// whose original upload is still `creating` is redundant and racy, and a
/// resulting newer `failed` snapshot would make `latest_full_snapshot_ready`
/// stop seeing the in-flight upload and could strand an evacuation restore.
pub async fn pinned_local_only_sandboxes(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Vec<SandboxId>, DbError> {
    Ok(sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(
        PINNED_LOCAL_ONLY_SANDBOXES.as_str(),
    ))
    .bind(node_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Postgres-backed plan-structure and page-shape regression for the
/// `:force-upload-local-snapshots` enumeration: the scan stays bounded by the
/// node's own pinned history rather than the fleet, the in-flight probe runs
/// only for non-durable rows, and the page stays the ascending-id prefix the
/// handler's `deferred[]` tail depends on.
#[cfg(test)]
mod pinned_local_only_plan_tests;
#[cfg(test)]
mod tests;
