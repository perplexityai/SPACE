//! Candidate selection queries for [`super::lease_items_after`] and
//! [`super::lease_selected_items`].
//!
//! Kept in a dedicated submodule so rolling-phase hot-path statements remain
//! syntactically whole and directly inspectable by `crate::plan_tests`.

use std::collections::HashMap;
use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::DbError;
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;
use uuid::Uuid;

use super::{
    LeaseHeader, STARTED_HOLD_PREFIX, TARGET_START_SETTLED, TARGET_START_SETTLED_PREFIX,
    UNSTARTED_PREFIX,
};
use crate::{CampaignId, CampaignKind, ItemPhase, SubjectKind};

/// In-flight counts by node, the pending items to consider with their nodes,
/// and the last pending `subject_id` scanned in ring order (if any).
pub(super) type NodeCandidates = (
    Vec<(Option<NodeId>, i64)>,
    Vec<(Uuid, Option<NodeId>)>,
    Option<Uuid>,
);

pub(crate) const NODELESS_ROLLING_FIRST: &str = sql!(
    "WITH in_flight AS (
         SELECT count(*) AS total
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $4
     )
     SELECT
         (SELECT total FROM in_flight),
         ARRAY(
             SELECT i.subject_id
             FROM runtime_upgrade_items i
             WHERE (SELECT total FROM in_flight) < $5
               AND i.campaign_id = $1
               AND i.phase = $2
             ORDER BY i.subject_id
             LIMIT $3
         )"
);

pub(crate) const NODELESS_ROLLING_AFTER: &str = sql!(
    "WITH in_flight AS (
         SELECT count(*) AS total
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $4
     )
     SELECT
         (SELECT total FROM in_flight),
         ARRAY(
             SELECT wrapped.subject_id
             FROM (
                 (
                     SELECT 0 AS wrap_rank, i.subject_id
                     FROM runtime_upgrade_items i
                     WHERE (SELECT total FROM in_flight) < $5
                       AND i.campaign_id = $1
                       AND i.phase = $2
                       AND i.subject_id > $6
                     ORDER BY i.subject_id
                     LIMIT $3
                 )
                 UNION ALL
                 (
                     SELECT 1 AS wrap_rank, i.subject_id
                     FROM runtime_upgrade_items i
                     WHERE (SELECT total FROM in_flight) < $5
                       AND i.campaign_id = $1
                       AND i.phase = $2
                       AND i.subject_id <= $6
                     ORDER BY i.subject_id
                     LIMIT $3
                 )
             ) wrapped
             ORDER BY wrapped.wrap_rank, wrapped.subject_id
             LIMIT $3
         )"
);

const NODELESS_SELECTED: &str = sql!(
    "WITH in_flight AS (
         SELECT count(*) AS total
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $4
     )
     SELECT
         (SELECT total FROM in_flight),
         ARRAY(
             SELECT i.subject_id
             FROM runtime_upgrade_items i
             WHERE (SELECT total FROM in_flight) < $5
               AND i.campaign_id = $1
               AND i.subject_kind = $6
               AND i.phase = $2
               AND i.subject_id = ANY($7)
               AND (
                   NOT $8
                   OR (
                       i.last_error IS NOT NULL
                       AND (
                           CASE
                               WHEN $9
                               THEN NOT starts_with(i.last_error, $10)
                               ELSE i.attempts > 0 OR starts_with(i.last_error, $11)
                           END
                       )
                   )
                   OR NOT EXISTS (
                       SELECT 1
                       FROM runtime_upgrade_items started
                       WHERE started.campaign_id = $1
                         AND started.phase = $2
                         AND started.last_error IS NOT NULL
                         AND (
                             CASE
                                 WHEN $9
                                 THEN NOT starts_with(started.last_error, $10)
                                 ELSE started.attempts > 0 OR starts_with(started.last_error, $11)
                             END
                         )
                   )
               )
             ORDER BY array_position($7, i.subject_id), i.subject_id
             LIMIT $3
         )"
);

pub(crate) const NODELESS_CANARY_FIRST: &str = sql!(
    "WITH in_flight AS (
         SELECT count(*) AS total
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $4
     ),
     started_pending AS MATERIALIZED (
         SELECT i.subject_id
         FROM runtime_upgrade_items i
         WHERE (SELECT total FROM in_flight) < $5
           AND i.campaign_id = $1
           AND i.phase = $2
           AND i.last_error IS NOT NULL
           AND (
               CASE
                   WHEN $6
                   THEN NOT starts_with(i.last_error, $7)
                   ELSE i.attempts > 0 OR starts_with(i.last_error, $8)
               END
           )
         ORDER BY i.subject_id
         LIMIT $3
     ),
     untouched_pending AS (
         SELECT i.subject_id
         FROM runtime_upgrade_items i
         WHERE NOT EXISTS (SELECT 1 FROM started_pending)
           AND (SELECT total FROM in_flight) < $5
           AND i.campaign_id = $1
           AND i.phase = $2
         ORDER BY i.subject_id
         LIMIT $3
     )
     SELECT
         (SELECT total FROM in_flight),
         ARRAY(
             SELECT candidate.subject_id
             FROM (
                 SELECT 0 AS priority, sp.subject_id FROM started_pending sp
                 UNION ALL
                 SELECT 1 AS priority, up.subject_id FROM untouched_pending up
             ) candidate
             ORDER BY candidate.priority, candidate.subject_id
             LIMIT $3
         )"
);

pub(crate) const NODELESS_CANARY_AFTER: &str = sql!(
    "WITH in_flight AS (
         SELECT count(*) AS total
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $4
     ),
     started_forward AS MATERIALIZED (
         SELECT i.subject_id
         FROM runtime_upgrade_items i
         WHERE (SELECT total FROM in_flight) < $5
           AND i.campaign_id = $1
           AND i.phase = $2
           AND i.subject_id > $8
           AND i.last_error IS NOT NULL
           AND (
               CASE
                   WHEN $6
                   THEN NOT starts_with(i.last_error, $7)
                   ELSE i.attempts > 0 OR starts_with(i.last_error, $9)
               END
           )
         ORDER BY i.subject_id
         LIMIT $3
     ),
     started_forward_stats AS MATERIALIZED (
         SELECT count(*)::bigint AS scanned
         FROM started_forward
     ),
     started_wrap AS MATERIALIZED (
         SELECT i.subject_id
         FROM runtime_upgrade_items i
         WHERE (SELECT total FROM in_flight) < $5
           AND (SELECT scanned FROM started_forward_stats) < $3
           AND i.campaign_id = $1
           AND i.phase = $2
           AND i.subject_id <= $8
           AND i.last_error IS NOT NULL
           AND (
               CASE
                   WHEN $6
                   THEN NOT starts_with(i.last_error, $7)
                   ELSE i.attempts > 0 OR starts_with(i.last_error, $9)
               END
           )
         ORDER BY i.subject_id
         LIMIT GREATEST($3 - (SELECT scanned FROM started_forward_stats), 0)
     ),
     has_started AS MATERIALIZED (
         SELECT (
             (SELECT scanned FROM started_forward_stats) > 0
             OR EXISTS (SELECT 1 FROM started_wrap)
         ) AS yes
     ),
     untouched_forward AS MATERIALIZED (
         SELECT i.subject_id
         FROM runtime_upgrade_items i
         WHERE NOT (SELECT yes FROM has_started)
           AND (SELECT total FROM in_flight) < $5
           AND i.campaign_id = $1
           AND i.phase = $2
           AND i.subject_id > $8
         ORDER BY i.subject_id
         LIMIT $3
     ),
     untouched_forward_stats AS MATERIALIZED (
         SELECT count(*)::bigint AS scanned
         FROM untouched_forward
     ),
     untouched_wrap AS MATERIALIZED (
         SELECT i.subject_id
         FROM runtime_upgrade_items i
         WHERE NOT (SELECT yes FROM has_started)
           AND (SELECT total FROM in_flight) < $5
           AND (SELECT scanned FROM untouched_forward_stats) < $3
           AND i.campaign_id = $1
           AND i.phase = $2
           AND i.subject_id <= $8
         ORDER BY i.subject_id
         LIMIT GREATEST($3 - (SELECT scanned FROM untouched_forward_stats), 0)
     )
     SELECT
         (SELECT total FROM in_flight),
         ARRAY(
             SELECT candidate.subject_id
             FROM (
                 SELECT 0 AS priority, 0 AS wrap_rank, sf.subject_id FROM started_forward sf
                 UNION ALL
                 SELECT 0 AS priority, 1 AS wrap_rank, sw.subject_id FROM started_wrap sw
                 UNION ALL
                 SELECT 1 AS priority, 0 AS wrap_rank, uf.subject_id FROM untouched_forward uf
                 UNION ALL
                 SELECT 1 AS priority, 1 AS wrap_rank, uw.subject_id FROM untouched_wrap uw
             ) candidate
             ORDER BY candidate.priority, candidate.wrap_rank, candidate.subject_id
             LIMIT $3
         )"
);

pub(crate) static NODE_BOUND_ROLLING_FIRST: LazyLock<String> = LazyLock::new(|| {
    let evidence = pplx_space_node_status_store::environment_evidence("nss");
    sql!(format!(
        "WITH RECURSIVE in_flight AS (
             SELECT o.node_id, count(*) AS items
             FROM runtime_upgrade_items i
             LEFT JOIN LATERAL (
                 SELECT o_row.node_id
                 FROM sandbox_observed_states o_row
                 WHERE NOT ($6 AND COALESCE(i.last_error = $7 OR starts_with(i.last_error, $8), FALSE))
                   AND o_row.sandbox_id = i.subject_id
                 LIMIT 1
             ) o ON TRUE
             WHERE i.campaign_id = $1
               AND i.phase = $4
             GROUP BY o.node_id
         ),
         in_flight_cluster AS (
             SELECT COALESCE(sum(items), 0)::bigint AS total
             FROM in_flight
         ),
         pages AS (
             SELECT
                 1 AS page_no,
                 step.ready_ids,
                 step.ready_nodes,
                 cardinality(step.ready_ids)::bigint AS total_ready,
                 step.scanned,
                 step.max_id
             FROM LATERAL (
                 SELECT
                     COALESCE(
                         array_agg(w.subject_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_ids,
                     COALESCE(
                         array_agg(obs.node_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_nodes,
                     count(*)::bigint AS scanned,
                     (array_agg(w.subject_id ORDER BY w.subject_id DESC))[1] AS max_id
                 FROM (
                     SELECT i.subject_id, i.last_error
                     FROM runtime_upgrade_items i
                     WHERE (SELECT total FROM in_flight_cluster) < $5
                       AND i.campaign_id = $1
                       AND i.phase = $2
                     ORDER BY i.subject_id
                     LIMIT $3
                     OFFSET 0
                 ) w
                 LEFT JOIN LATERAL (
                     SELECT
                         o.node_id,
                         (nss.reachable IS TRUE AND {evidence}) AS node_ready
                     FROM sandbox_observed_states o
                     LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                     WHERE NOT ($6 AND COALESCE(w.last_error = $7 OR starts_with(w.last_error, $8), FALSE))
                       AND o.sandbox_id = w.subject_id
                     LIMIT 1
                 ) obs ON TRUE
             ) step
             UNION ALL
             SELECT
                 prev.page_no + 1,
                 step.ready_ids,
                 step.ready_nodes,
                 prev.total_ready + cardinality(step.ready_ids)::bigint,
                 step.scanned,
                 step.max_id
             FROM pages prev
             CROSS JOIN LATERAL (
                 SELECT
                     COALESCE(
                         array_agg(w.subject_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_ids,
                     COALESCE(
                         array_agg(obs.node_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_nodes,
                     count(*)::bigint AS scanned,
                     (array_agg(w.subject_id ORDER BY w.subject_id DESC))[1] AS max_id
                 FROM (
                     SELECT i.subject_id, i.last_error
                     FROM runtime_upgrade_items i
                     WHERE i.campaign_id = $1
                       AND i.phase = $2
                       AND i.subject_id > prev.max_id
                     ORDER BY i.subject_id
                     LIMIT $3
                     OFFSET 0
                 ) w
                 LEFT JOIN LATERAL (
                     SELECT
                         o.node_id,
                         (nss.reachable IS TRUE AND {evidence}) AS node_ready
                     FROM sandbox_observed_states o
                     LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                     WHERE NOT ($6 AND COALESCE(w.last_error = $7 OR starts_with(w.last_error, $8), FALSE))
                       AND o.sandbox_id = w.subject_id
                     LIMIT 1
                 ) obs ON TRUE
             ) step
             WHERE prev.total_ready < $3
               AND prev.scanned = $3
               AND prev.max_id IS NOT NULL
         ),
         pending AS (
             SELECT u.subject_id, u.node_id
             FROM pages p
             CROSS JOIN LATERAL unnest(p.ready_ids, p.ready_nodes) WITH ORDINALITY AS u(subject_id, node_id, ord)
             ORDER BY p.page_no, u.ord
             LIMIT $3
         )
         SELECT node_id, items, NULL::uuid AS subject_id FROM in_flight
         UNION ALL
         SELECT node_id, NULL::bigint, subject_id FROM pending",
    ))
});

pub(crate) static NODE_BOUND_ROLLING_AFTER: LazyLock<String> = LazyLock::new(|| {
    let evidence = pplx_space_node_status_store::environment_evidence("nss");
    sql!(format!(
        "WITH in_flight AS (
             SELECT o.node_id, count(*) AS items
             FROM runtime_upgrade_items i
             LEFT JOIN LATERAL (
                 SELECT o_row.node_id
                 FROM sandbox_observed_states o_row
                 WHERE NOT ($7 AND COALESCE(i.last_error = $8 OR starts_with(i.last_error, $9), FALSE))
                   AND o_row.sandbox_id = i.subject_id
                 LIMIT 1
             ) o ON TRUE
             WHERE i.campaign_id = $1
               AND i.phase = $4
             GROUP BY o.node_id
         ),
         in_flight_cluster AS (
             SELECT COALESCE(sum(items), 0)::bigint AS total
             FROM in_flight
         ),
         forward_window AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE (SELECT total FROM in_flight_cluster) < $5
               AND i.campaign_id = $1
               AND i.phase = $2
               AND i.subject_id > $6
             ORDER BY i.subject_id
             LIMIT $3
         ),
         forward_stats AS MATERIALIZED (
             SELECT count(*)::bigint AS scanned
             FROM forward_window
         ),
         wrap_window AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE (SELECT total FROM in_flight_cluster) < $5
               AND (SELECT scanned FROM forward_stats) < $3
               AND i.campaign_id = $1
               AND i.phase = $2
               AND i.subject_id <= $6
             ORDER BY i.subject_id
             LIMIT GREATEST($3 - (SELECT scanned FROM forward_stats), 0)
         ),
         examined_window AS MATERIALIZED (
             SELECT 0 AS wrap_rank, fw.subject_id, fw.last_error FROM forward_window fw
             UNION ALL
             SELECT 1 AS wrap_rank, ww.subject_id, ww.last_error FROM wrap_window ww
         ),
         pending AS (
             SELECT w.subject_id, obs.node_id
             FROM examined_window w
             LEFT JOIN LATERAL (
                 SELECT
                     o.node_id,
                     (nss.reachable IS TRUE AND {evidence}) AS node_ready
                 FROM sandbox_observed_states o
                 LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                 WHERE NOT ($7 AND COALESCE(w.last_error = $8 OR starts_with(w.last_error, $9), FALSE))
                   AND o.sandbox_id = w.subject_id
                 LIMIT 1
             ) obs ON TRUE
             WHERE obs.node_id IS NULL OR obs.node_ready
             ORDER BY w.wrap_rank, w.subject_id
             LIMIT $3
         ),
         cursor_advance AS (
             SELECT CASE
                 WHEN ew.wrap_rank = 1 THEN ew.subject_id
                 WHEN (SELECT scanned FROM forward_stats) < $3
                 THEN '00000000-0000-0000-0000-000000000000'::uuid
                 ELSE ew.subject_id
             END AS next_cursor
             FROM examined_window ew
             ORDER BY ew.wrap_rank DESC, ew.subject_id DESC
             LIMIT 1
         )
         SELECT 0::int2 AS row_kind, node_id, items, NULL::uuid AS subject_id FROM in_flight
         UNION ALL
         SELECT 1::int2 AS row_kind, node_id, NULL::bigint, subject_id FROM pending
         UNION ALL
         SELECT 2::int2 AS row_kind, NULL::uuid, NULL::bigint, next_cursor FROM cursor_advance",
    ))
});

static NODE_BOUND_SELECTED: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH in_flight AS (
             SELECT CASE
                        WHEN $9 AND (i.last_error = $12 OR starts_with(i.last_error, $13))
                        THEN NULL::uuid
                        ELSE o.node_id
                    END AS node_id,
                    count(*) AS items
             FROM runtime_upgrade_items i
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
             WHERE i.campaign_id = $1
               AND i.phase = $4
             GROUP BY 1
         ),
         in_flight_cluster AS (
             SELECT COALESCE(sum(items), 0)::bigint AS total
             FROM in_flight
         ),
         pending AS (
             SELECT i.subject_id,
                    CASE
                        WHEN $9 AND (i.last_error = $12 OR starts_with(i.last_error, $13))
                        THEN NULL::uuid
                        ELSE o.node_id
                    END AS node_id,
                    CASE
                        WHEN $8
                         AND i.last_error IS NOT NULL
                         AND (
                             CASE
                                 WHEN $9
                                 THEN NOT starts_with(i.last_error, $10)
                                 ELSE i.attempts > 0 OR starts_with(i.last_error, $11)
                             END
                         )
                        THEN 0
                        ELSE 1
                    END AS priority
             FROM runtime_upgrade_items i
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
             WHERE (SELECT total FROM in_flight_cluster) < $5
               AND i.campaign_id = $1
               AND i.subject_kind = $6
               AND i.phase = $2
               AND i.subject_id = ANY($7)
               AND (
                   ($9 AND (i.last_error = $12 OR starts_with(i.last_error, $13)))
                   OR o.node_id IS NULL
                   OR (nss.reachable IS TRUE AND {})
               )
               AND (
                   NOT $8
                   OR (
                       i.last_error IS NOT NULL
                       AND (
                           CASE
                               WHEN $9
                               THEN NOT starts_with(i.last_error, $10)
                               ELSE i.attempts > 0 OR starts_with(i.last_error, $11)
                           END
                       )
                   )
                   OR NOT EXISTS (
                       SELECT 1
                       FROM runtime_upgrade_items started
                       WHERE started.campaign_id = $1
                         AND started.phase = $2
                         AND started.last_error IS NOT NULL
                         AND (
                             CASE
                                 WHEN $9
                                 THEN NOT starts_with(started.last_error, $10)
                                 ELSE started.attempts > 0 OR starts_with(started.last_error, $11)
                             END
                         )
                   )
               )
             ORDER BY priority, array_position($7, i.subject_id), i.subject_id
             LIMIT $3
         )
         SELECT node_id, items, NULL::uuid AS subject_id, NULL::int4 AS priority FROM in_flight
         UNION ALL
         SELECT node_id, NULL::bigint, subject_id, priority FROM pending",
        pplx_space_node_status_store::environment_evidence("nss")
    ))
});

pub(crate) static NODE_BOUND_CANARY_FIRST: LazyLock<String> = LazyLock::new(|| {
    let evidence = pplx_space_node_status_store::environment_evidence("nss");
    sql!(format!(
        "WITH RECURSIVE in_flight AS (
             SELECT o.node_id, count(*) AS items
             FROM runtime_upgrade_items i
             LEFT JOIN LATERAL (
                 SELECT o_row.node_id
                 FROM sandbox_observed_states o_row
                 WHERE NOT ($6 AND COALESCE(i.last_error = $9 OR starts_with(i.last_error, $10), FALSE))
                   AND o_row.sandbox_id = i.subject_id
                 LIMIT 1
             ) o ON TRUE
             WHERE i.campaign_id = $1
               AND i.phase = $4
             GROUP BY o.node_id
         ),
         in_flight_cluster AS (
             SELECT COALESCE(sum(items), 0)::bigint AS total
             FROM in_flight
         ),
         started_pending AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE (SELECT total FROM in_flight_cluster) < $5
               AND i.campaign_id = $1
               AND i.phase = $2
               AND i.last_error IS NOT NULL
               AND (
                   CASE
                       WHEN $6
                       THEN NOT starts_with(i.last_error, $7)
                       ELSE i.attempts > 0 OR starts_with(i.last_error, $8)
                   END
               )
             ORDER BY i.subject_id
         ),
         has_started AS MATERIALIZED (
             SELECT EXISTS (SELECT 1 FROM started_pending) AS yes
         ),
         ready_started AS (
             SELECT sp.subject_id, obs.node_id, 0::int4 AS priority
             FROM started_pending sp
             LEFT JOIN LATERAL (
                 SELECT
                     o.node_id,
                     (nss.reachable IS TRUE AND {evidence}) AS node_ready
                 FROM sandbox_observed_states o
                 LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                 WHERE NOT ($6 AND COALESCE(sp.last_error = $9 OR starts_with(sp.last_error, $10), FALSE))
                   AND o.sandbox_id = sp.subject_id
                 LIMIT 1
             ) obs ON TRUE
             WHERE obs.node_id IS NULL OR obs.node_ready
             ORDER BY sp.subject_id
             LIMIT $3
         ),
         untouched_pages AS (
             SELECT
                 1 AS page_no,
                 step.ready_ids,
                 step.ready_nodes,
                 cardinality(step.ready_ids)::bigint AS total_ready,
                 step.scanned,
                 step.max_id
             FROM LATERAL (
                 SELECT
                     COALESCE(
                         array_agg(w.subject_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_ids,
                     COALESCE(
                         array_agg(obs.node_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_nodes,
                     count(*)::bigint AS scanned,
                     (array_agg(w.subject_id ORDER BY w.subject_id DESC))[1] AS max_id
                 FROM (
                     SELECT i.subject_id, i.last_error
                     FROM runtime_upgrade_items i
                     WHERE NOT (SELECT yes FROM has_started)
                       AND (SELECT total FROM in_flight_cluster) < $5
                       AND i.campaign_id = $1
                       AND i.phase = $2
                     ORDER BY i.subject_id
                     LIMIT $3
                     OFFSET 0
                 ) w
                 LEFT JOIN LATERAL (
                     SELECT
                         o.node_id,
                         (nss.reachable IS TRUE AND {evidence}) AS node_ready
                     FROM sandbox_observed_states o
                     LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                     WHERE NOT ($6 AND COALESCE(w.last_error = $9 OR starts_with(w.last_error, $10), FALSE))
                       AND o.sandbox_id = w.subject_id
                     LIMIT 1
                 ) obs ON TRUE
             ) step
             UNION ALL
             SELECT
                 prev.page_no + 1,
                 step.ready_ids,
                 step.ready_nodes,
                 prev.total_ready + cardinality(step.ready_ids)::bigint,
                 step.scanned,
                 step.max_id
             FROM untouched_pages prev
             CROSS JOIN LATERAL (
                 SELECT
                     COALESCE(
                         array_agg(w.subject_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_ids,
                     COALESCE(
                         array_agg(obs.node_id ORDER BY w.subject_id)
                             FILTER (WHERE obs.node_id IS NULL OR obs.node_ready),
                         ARRAY[]::uuid[]
                     ) AS ready_nodes,
                     count(*)::bigint AS scanned,
                     (array_agg(w.subject_id ORDER BY w.subject_id DESC))[1] AS max_id
                 FROM (
                     SELECT i.subject_id, i.last_error
                     FROM runtime_upgrade_items i
                     WHERE i.campaign_id = $1
                       AND i.phase = $2
                       AND i.subject_id > prev.max_id
                     ORDER BY i.subject_id
                     LIMIT $3
                     OFFSET 0
                 ) w
                 LEFT JOIN LATERAL (
                     SELECT
                         o.node_id,
                         (nss.reachable IS TRUE AND {evidence}) AS node_ready
                     FROM sandbox_observed_states o
                     LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                     WHERE NOT ($6 AND COALESCE(w.last_error = $9 OR starts_with(w.last_error, $10), FALSE))
                       AND o.sandbox_id = w.subject_id
                     LIMIT 1
                 ) obs ON TRUE
             ) step
             WHERE prev.total_ready < $3
               AND prev.scanned = $3
               AND prev.max_id IS NOT NULL
         ),
         ready_untouched AS (
             SELECT u.subject_id, u.node_id, 1::int4 AS priority
             FROM untouched_pages p
             CROSS JOIN LATERAL unnest(p.ready_ids, p.ready_nodes) WITH ORDINALITY AS u(subject_id, node_id, ord)
             ORDER BY p.page_no, u.ord
             LIMIT $3
         )
         SELECT 0::int2 AS row_kind, node_id, items, NULL::uuid AS subject_id, NULL::int4 AS priority FROM in_flight
         UNION ALL
         SELECT 1::int2 AS row_kind, node_id, NULL::bigint, subject_id, priority FROM ready_started
         UNION ALL
         SELECT 1::int2 AS row_kind, node_id, NULL::bigint, subject_id, priority FROM ready_untouched",
    ))
});

pub(crate) static NODE_BOUND_CANARY_AFTER: LazyLock<String> = LazyLock::new(|| {
    let evidence = pplx_space_node_status_store::environment_evidence("nss");
    sql!(format!(
        "WITH in_flight AS (
             SELECT o.node_id, count(*) AS items
             FROM runtime_upgrade_items i
             LEFT JOIN LATERAL (
                 SELECT o_row.node_id
                 FROM sandbox_observed_states o_row
                 WHERE NOT ($6 AND COALESCE(i.last_error = $10 OR starts_with(i.last_error, $11), FALSE))
                   AND o_row.sandbox_id = i.subject_id
                 LIMIT 1
             ) o ON TRUE
             WHERE i.campaign_id = $1
               AND i.phase = $4
             GROUP BY o.node_id
         ),
         in_flight_cluster AS (
             SELECT COALESCE(sum(items), 0)::bigint AS total
             FROM in_flight
         ),
         started_forward AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE (SELECT total FROM in_flight_cluster) < $5
               AND i.campaign_id = $1
               AND i.phase = $2
               AND i.subject_id > $8
               AND i.last_error IS NOT NULL
               AND (
                   CASE
                       WHEN $6
                       THEN NOT starts_with(i.last_error, $7)
                       ELSE i.attempts > 0 OR starts_with(i.last_error, $9)
                   END
               )
             ORDER BY i.subject_id
         ),
         started_wrap AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE (SELECT total FROM in_flight_cluster) < $5
               AND i.campaign_id = $1
               AND i.phase = $2
               AND i.subject_id <= $8
               AND i.last_error IS NOT NULL
               AND (
                   CASE
                       WHEN $6
                       THEN NOT starts_with(i.last_error, $7)
                       ELSE i.attempts > 0 OR starts_with(i.last_error, $9)
                   END
               )
             ORDER BY i.subject_id
         ),
         started_window AS MATERIALIZED (
             SELECT 0::int4 AS wrap_rank, sf.subject_id, sf.last_error FROM started_forward sf
             UNION ALL
             SELECT 1::int4 AS wrap_rank, sw.subject_id, sw.last_error FROM started_wrap sw
         ),
         has_started AS MATERIALIZED (
             SELECT EXISTS (SELECT 1 FROM started_window) AS yes
         ),
         ready_started AS (
             SELECT sw.subject_id, obs.node_id, 0::int4 AS priority
             FROM started_window sw
             LEFT JOIN LATERAL (
                 SELECT
                     o.node_id,
                     (nss.reachable IS TRUE AND {evidence}) AS node_ready
                 FROM sandbox_observed_states o
                 LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                 WHERE NOT ($6 AND COALESCE(sw.last_error = $10 OR starts_with(sw.last_error, $11), FALSE))
                   AND o.sandbox_id = sw.subject_id
                 LIMIT 1
             ) obs ON TRUE
             WHERE obs.node_id IS NULL OR obs.node_ready
             ORDER BY sw.wrap_rank, sw.subject_id
             LIMIT $3
         ),
         untouched_forward AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE NOT (SELECT yes FROM has_started)
               AND (SELECT total FROM in_flight_cluster) < $5
               AND i.campaign_id = $1
               AND i.phase = $2
               AND i.subject_id > $8
             ORDER BY i.subject_id
             LIMIT $3
         ),
         untouched_forward_stats AS MATERIALIZED (
             SELECT count(*)::bigint AS scanned
             FROM untouched_forward
         ),
         untouched_wrap AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE NOT (SELECT yes FROM has_started)
               AND (SELECT total FROM in_flight_cluster) < $5
               AND (SELECT scanned FROM untouched_forward_stats) < $3
               AND i.campaign_id = $1
               AND i.phase = $2
               AND i.subject_id <= $8
             ORDER BY i.subject_id
             LIMIT GREATEST($3 - (SELECT scanned FROM untouched_forward_stats), 0)
         ),
         untouched_window AS MATERIALIZED (
             SELECT 0::int4 AS wrap_rank, uf.subject_id, uf.last_error FROM untouched_forward uf
             UNION ALL
             SELECT 1::int4 AS wrap_rank, uw.subject_id, uw.last_error FROM untouched_wrap uw
         ),
         ready_untouched AS (
             SELECT uw.subject_id, obs.node_id, 1::int4 AS priority
             FROM untouched_window uw
             LEFT JOIN LATERAL (
                 SELECT
                     o.node_id,
                     (nss.reachable IS TRUE AND {evidence}) AS node_ready
                 FROM sandbox_observed_states o
                 LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                 WHERE NOT ($6 AND COALESCE(uw.last_error = $10 OR starts_with(uw.last_error, $11), FALSE))
                   AND o.sandbox_id = uw.subject_id
                 LIMIT 1
             ) obs ON TRUE
             WHERE obs.node_id IS NULL OR obs.node_ready
             LIMIT $3
         )
         SELECT 0::int2 AS row_kind, node_id, items, NULL::uuid AS subject_id, NULL::int4 AS aux FROM in_flight
         UNION ALL
         SELECT 1::int2 AS row_kind, node_id, NULL::bigint, subject_id, priority FROM ready_started
         UNION ALL
         SELECT 1::int2 AS row_kind, node_id, NULL::bigint, subject_id, priority FROM ready_untouched
         UNION ALL
         SELECT 2::int2 AS row_kind, NULL::uuid, (SELECT scanned FROM untouched_forward_stats), uw.subject_id, uw.wrap_rank FROM untouched_window uw",
    ))
});

/// For sandbox items: in flight by the node that last published each sandbox,
/// and pending items whose node qualifies or that have none.
pub(super) async fn node_bound_candidates(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    header: &LeaseHeader,
    only_subjects: Option<&[Uuid]>,
    after: Option<Uuid>,
    scan_limit: u32,
) -> Result<NodeCandidates, DbError> {
    debug_assert_eq!(header.subject_kind, SubjectKind::Sandbox);
    let is_force_restarts = header.kind == CampaignKind::SandboxForceRestarts;
    if let Some(selected) = only_subjects {
        type SelectedTuple = (Option<NodeId>, Option<i64>, Option<Uuid>, Option<i32>);
        let mut positions = HashMap::with_capacity(selected.len());
        for (idx, &id) in selected.iter().enumerate() {
            positions.entry(id).or_insert(idx);
        }
        let rows =
            sqlx::query_as::<_, SelectedTuple>(sqlx::AssertSqlSafe(NODE_BOUND_SELECTED.as_str()))
                .bind(campaign_id)
                .bind(ItemPhase::Pending.as_str())
                .bind(i64::from(scan_limit))
                .bind(ItemPhase::InFlight.as_str())
                .bind(header.effective_per_cluster)
                .bind(header.subject_kind.as_str())
                .bind(selected)
                .bind(header.canary)
                .bind(is_force_restarts)
                .bind(UNSTARTED_PREFIX)
                .bind(STARTED_HOLD_PREFIX)
                .bind(TARGET_START_SETTLED)
                .bind(TARGET_START_SETTLED_PREFIX)
                .fetch_all(&mut *tx)
                .await?;
        let mut in_flight = Vec::new();
        let mut pending = Vec::new();
        for (node_id, items, subject_id, priority) in rows {
            match (items, subject_id, priority) {
                (Some(items), _, _) => in_flight.push((node_id, items)),
                (None, Some(subject_id), Some(priority)) => {
                    pending.push((priority, subject_id, node_id));
                }
                _ => {}
            }
        }
        pending.sort_unstable_by_key(|(priority, subject_id, _)| {
            (
                *priority,
                positions.get(subject_id).copied().unwrap_or(usize::MAX),
                *subject_id,
            )
        });
        let last_scanned = pending.last().map(|(_, id, _)| *id);
        let pending = pending
            .into_iter()
            .map(|(_, subject_id, node_id)| (subject_id, node_id))
            .collect();
        return Ok((in_flight, pending, last_scanned));
    }

    if header.canary {
        if let Some(cursor) = after {
            type CanaryAfterTuple = (i16, Option<NodeId>, Option<i64>, Option<Uuid>, Option<i32>);
            let rows = sqlx::query_as::<_, CanaryAfterTuple>(sqlx::AssertSqlSafe(
                NODE_BOUND_CANARY_AFTER.as_str(),
            ))
            .bind(campaign_id)
            .bind(ItemPhase::Pending.as_str())
            .bind(i64::from(scan_limit))
            .bind(ItemPhase::InFlight.as_str())
            .bind(header.effective_per_cluster)
            .bind(is_force_restarts)
            .bind(UNSTARTED_PREFIX)
            .bind(cursor)
            .bind(STARTED_HOLD_PREFIX)
            .bind(TARGET_START_SETTLED)
            .bind(TARGET_START_SETTLED_PREFIX)
            .fetch_all(&mut *tx)
            .await?;
            let mut in_flight = Vec::new();
            let mut pending = Vec::new();
            let mut forward_scanned = None;
            let mut max_forward: Option<Uuid> = None;
            let mut max_wrap: Option<Uuid> = None;
            for (row_kind, node_id, items, subject_id, aux) in rows {
                match (row_kind, items, subject_id, aux) {
                    (0, Some(items), _, _) => in_flight.push((node_id, items)),
                    (1, None, Some(subject_id), Some(priority)) => {
                        pending.push((priority, subject_id, node_id));
                    }
                    (2, scanned, Some(subject_id), Some(wrap_rank)) => {
                        forward_scanned = scanned;
                        if wrap_rank == 1 {
                            max_wrap =
                                Some(max_wrap.map_or(subject_id, |prev| prev.max(subject_id)));
                        } else {
                            max_forward =
                                Some(max_forward.map_or(subject_id, |prev| prev.max(subject_id)));
                        }
                    }
                    _ => {}
                }
            }
            let cursor_advance = if let Some(last_wrap) = max_wrap {
                Some(last_wrap)
            } else if let Some(last_forward) = max_forward {
                if forward_scanned.unwrap_or(0) < i64::from(scan_limit) {
                    Some(Uuid::nil())
                } else {
                    Some(last_forward)
                }
            } else {
                None
            };
            pending.sort_unstable_by_key(|(priority, subject_id, _)| {
                (*priority, *subject_id <= cursor, *subject_id)
            });
            pending.truncate(scan_limit as usize);
            let last_scanned = cursor_advance.or_else(|| pending.last().map(|(_, id, _)| *id));
            let pending = pending
                .into_iter()
                .map(|(_, subject_id, node_id)| (subject_id, node_id))
                .collect();
            return Ok((in_flight, pending, last_scanned));
        }

        type CanaryFirstTuple = (i16, Option<NodeId>, Option<i64>, Option<Uuid>, Option<i32>);
        let rows = sqlx::query_as::<_, CanaryFirstTuple>(sqlx::AssertSqlSafe(
            NODE_BOUND_CANARY_FIRST.as_str(),
        ))
        .bind(campaign_id)
        .bind(ItemPhase::Pending.as_str())
        .bind(i64::from(scan_limit))
        .bind(ItemPhase::InFlight.as_str())
        .bind(header.effective_per_cluster)
        .bind(is_force_restarts)
        .bind(UNSTARTED_PREFIX)
        .bind(STARTED_HOLD_PREFIX)
        .bind(TARGET_START_SETTLED)
        .bind(TARGET_START_SETTLED_PREFIX)
        .fetch_all(&mut *tx)
        .await?;
        let mut in_flight = Vec::new();
        let mut pending = Vec::new();
        for (row_kind, node_id, items, subject_id, priority) in rows {
            match (row_kind, items, subject_id, priority) {
                (0, Some(items), _, _) => in_flight.push((node_id, items)),
                (1, None, Some(subject_id), Some(priority)) => {
                    pending.push((priority, subject_id, node_id));
                }
                _ => {}
            }
        }
        // UNION ALL doesn't promise the branch's order; preserve started-first
        // priority and ascending subject order.
        pending.sort_unstable_by_key(|(priority, subject_id, _)| (*priority, *subject_id));
        pending.truncate(scan_limit as usize);
        let last_scanned = pending.last().map(|(_, id, _)| *id);
        let pending = pending
            .into_iter()
            .map(|(_, subject_id, node_id)| (subject_id, node_id))
            .collect();
        return Ok((in_flight, pending, last_scanned));
    }

    if let Some(cursor) = after {
        type RollingAfterTuple = (i16, Option<NodeId>, Option<i64>, Option<Uuid>);
        let rows = sqlx::query_as::<_, RollingAfterTuple>(sqlx::AssertSqlSafe(
            NODE_BOUND_ROLLING_AFTER.as_str(),
        ))
        .bind(campaign_id)
        .bind(ItemPhase::Pending.as_str())
        .bind(i64::from(scan_limit))
        .bind(ItemPhase::InFlight.as_str())
        .bind(header.effective_per_cluster)
        .bind(cursor)
        .bind(is_force_restarts)
        .bind(TARGET_START_SETTLED)
        .bind(TARGET_START_SETTLED_PREFIX)
        .fetch_all(&mut *tx)
        .await?;
        let mut in_flight = Vec::new();
        let mut pending = Vec::new();
        let mut cursor_advance = None;
        for (row_kind, node_id, items, subject_id) in rows {
            match (row_kind, items, subject_id) {
                (0, Some(items), _) => in_flight.push((node_id, items)),
                (1, None, Some(subject_id)) => pending.push((subject_id, node_id)),
                (2, None, Some(next_cursor)) => cursor_advance = Some(next_cursor),
                _ => {}
            }
        }
        // UNION ALL doesn't promise the branch's order; preserve wrap-around ring
        // order across `> cursor` and `<= cursor`.
        pending.sort_unstable_by_key(|(subject_id, _)| (*subject_id <= cursor, *subject_id));
        let last_scanned = cursor_advance.or_else(|| pending.last().map(|(id, _)| *id));
        return Ok((in_flight, pending, last_scanned));
    }

    let rows = sqlx::query_as::<_, (Option<NodeId>, Option<i64>, Option<Uuid>)>(
        sqlx::AssertSqlSafe(NODE_BOUND_ROLLING_FIRST.as_str()),
    )
    .bind(campaign_id)
    .bind(ItemPhase::Pending.as_str())
    .bind(i64::from(scan_limit))
    .bind(ItemPhase::InFlight.as_str())
    .bind(header.effective_per_cluster)
    .bind(is_force_restarts)
    .bind(TARGET_START_SETTLED)
    .bind(TARGET_START_SETTLED_PREFIX)
    .fetch_all(&mut *tx)
    .await?;
    let mut in_flight = Vec::new();
    let mut pending = Vec::new();
    for (node_id, items, subject_id) in rows {
        match (items, subject_id) {
            (Some(items), None) => in_flight.push((node_id, items)),
            (None, Some(subject_id)) => pending.push((subject_id, node_id)),
            _ => {}
        }
    }
    pending.sort_unstable_by_key(|(subject_id, _)| *subject_id);
    pending.truncate(scan_limit as usize);
    let last_scanned = pending.last().map(|(id, _)| *id);
    Ok((in_flight, pending, last_scanned))
}

/// For items with no node: the in-flight count and the first pending items.
pub(super) async fn nodeless_candidates(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    header: &LeaseHeader,
    only_subjects: Option<&[Uuid]>,
    after: Option<Uuid>,
    scan_limit: u32,
) -> Result<NodeCandidates, DbError> {
    if let Some(selected) = only_subjects {
        let (in_flight, pending) = sqlx::query_as::<_, (i64, Vec<Uuid>)>(NODELESS_SELECTED)
            .bind(campaign_id)
            .bind(ItemPhase::Pending.as_str())
            .bind(i64::from(scan_limit))
            .bind(ItemPhase::InFlight.as_str())
            .bind(header.effective_per_cluster)
            .bind(header.subject_kind.as_str())
            .bind(selected)
            .bind(header.canary)
            .bind(header.kind == CampaignKind::SandboxForceRestarts)
            .bind(UNSTARTED_PREFIX)
            .bind(STARTED_HOLD_PREFIX)
            .fetch_one(&mut *tx)
            .await?;
        let last_scanned = pending.last().copied();
        return Ok((
            vec![(None, in_flight)],
            pending.into_iter().map(|id| (id, None)).collect(),
            last_scanned,
        ));
    }

    if header.canary {
        let is_force_restarts = header.kind == CampaignKind::SandboxForceRestarts;
        let (in_flight, mut pending) = if let Some(cursor) = after {
            let (in_flight, mut pending) =
                sqlx::query_as::<_, (i64, Vec<Uuid>)>(NODELESS_CANARY_AFTER)
                    .bind(campaign_id)
                    .bind(ItemPhase::Pending.as_str())
                    .bind(i64::from(scan_limit))
                    .bind(ItemPhase::InFlight.as_str())
                    .bind(header.effective_per_cluster)
                    .bind(is_force_restarts)
                    .bind(UNSTARTED_PREFIX)
                    .bind(cursor)
                    .bind(STARTED_HOLD_PREFIX)
                    .fetch_one(&mut *tx)
                    .await?;
            pending.sort_unstable_by_key(|id| (*id <= cursor, *id));
            (in_flight, pending)
        } else {
            let (in_flight, mut pending) =
                sqlx::query_as::<_, (i64, Vec<Uuid>)>(NODELESS_CANARY_FIRST)
                    .bind(campaign_id)
                    .bind(ItemPhase::Pending.as_str())
                    .bind(i64::from(scan_limit))
                    .bind(ItemPhase::InFlight.as_str())
                    .bind(header.effective_per_cluster)
                    .bind(is_force_restarts)
                    .bind(UNSTARTED_PREFIX)
                    .bind(STARTED_HOLD_PREFIX)
                    .fetch_one(&mut *tx)
                    .await?;
            pending.sort_unstable();
            (in_flight, pending)
        };
        pending.truncate(scan_limit as usize);
        let last_scanned = pending.last().copied();
        return Ok((
            vec![(None, in_flight)],
            pending.into_iter().map(|id| (id, None)).collect(),
            last_scanned,
        ));
    }

    let (in_flight, pending) = if let Some(cursor) = after {
        sqlx::query_as::<_, (i64, Vec<Uuid>)>(NODELESS_ROLLING_AFTER)
            .bind(campaign_id)
            .bind(ItemPhase::Pending.as_str())
            .bind(i64::from(scan_limit))
            .bind(ItemPhase::InFlight.as_str())
            .bind(header.effective_per_cluster)
            .bind(cursor)
            .fetch_one(&mut *tx)
            .await?
    } else {
        sqlx::query_as::<_, (i64, Vec<Uuid>)>(NODELESS_ROLLING_FIRST)
            .bind(campaign_id)
            .bind(ItemPhase::Pending.as_str())
            .bind(i64::from(scan_limit))
            .bind(ItemPhase::InFlight.as_str())
            .bind(header.effective_per_cluster)
            .fetch_one(&mut *tx)
            .await?
    };
    let last_scanned = pending.last().copied();
    Ok((
        vec![(None, in_flight)],
        pending.into_iter().map(|id| (id, None)).collect(),
        last_scanned,
    ))
}
