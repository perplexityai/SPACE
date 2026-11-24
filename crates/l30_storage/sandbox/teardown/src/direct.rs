//! `node_facet_observed_status` writes that resolve a sandbox's current node
//! through the Direct route table.
//!
//! The failure-episode projection records and clears a node's failure episode
//! only for sandboxes that node currently runs: the `current_node_id` of the
//! sandbox's Direct route identity when it has one, else the row's
//! `active_node_id`.
//! Resolving that joins `sandbox_direct_route_identities`, which
//! `pattern_ratchet.direct_tables` confines to the Direct substrate and the
//! stores' `direct` modules, so these statements live here rather than in
//! [`crate::observed_status`].

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::types::Json;

/// The payload keys a failure episode is recorded under: the episode's own
/// member of the observed payload, and its members for the execution
/// generation, pod UID and activation attempt.
#[derive(Debug, Clone, Copy)]
pub struct FailureEpisodeKeys<'a> {
    pub episode: &'a str,
    pub execution_generation: &'a str,
    pub pod_uid: &'a str,
    pub activation_attempt_id: &'a str,
}

/// Record `node_id`'s failure episode under `keys.episode` in the payload of
/// its `facet` row for each sandbox of `rows` (a JSON array of objects with
/// `sandbox_id`, `published_lifecycle`, `state_degraded`,
/// `execution_generation`, `pod_uid` and `activation_attempt_id`) that the
/// node currently runs, reports `failed`, isn't degraded and carries an
/// execution generation. The episode holds the generation, pod UID and
/// activation attempt under their `keys` members, nulls dropped. A row
/// already holding the same episode is left as it is.
pub async fn record_failure_episodes(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
    keys: FailureEpisodeKeys<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             input.sandbox_id::TEXT,
             jsonb_build_object(
                 $4::TEXT,
                 jsonb_strip_nulls(jsonb_build_object(
                     $5::TEXT, input.execution_generation,
                     $6::TEXT, input.pod_uid,
                     $7::TEXT, input.activation_attempt_id
                 ))
             ),
             NOW(),
             NOW()
         FROM jsonb_to_recordset($2::JSONB) AS input(
             sandbox_id UUID,
             published_lifecycle TEXT,
             state_degraded BOOLEAN,
             execution_generation BIGINT,
             pod_uid TEXT,
             activation_attempt_id TEXT
         )
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
           AND input.published_lifecycle = 'failed'
           AND NOT input.state_degraded
           AND input.execution_generation IS NOT NULL
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload =
                 (COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                     - $4::TEXT)
                 || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> $4::TEXT
               IS DISTINCT FROM EXCLUDED.observed_payload -> $4::TEXT"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .bind(keys.episode)
    .bind(keys.execution_generation)
    .bind(keys.pod_uid)
    .bind(keys.activation_attempt_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Drop `episode_key` from the payload of `node_id`'s `facet` row for each
/// sandbox of `rows` (shaped as for [`record_failure_episodes`]) that the
/// node currently runs and that no longer reports `failed`, or reports it
/// undegraded without an execution generation, where the payload carries
/// `episode_key`, stamping it observed now.
pub async fn clear_failure_episodes(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
    episode_key: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE node_facet_observed_status observed
         SET observed_payload = COALESCE(observed.observed_payload, '{}'::JSONB)
                                    - $4::TEXT,
             observed_at = NOW(),
             updated_at = NOW()
         FROM jsonb_to_recordset($2::JSONB) AS input(
             sandbox_id UUID,
             published_lifecycle TEXT,
             state_degraded BOOLEAN,
             execution_generation BIGINT,
             pod_uid TEXT,
             activation_attempt_id TEXT
         )
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
           AND observed.node_id = $1
           AND observed.facet = $3
           AND observed.element = input.sandbox_id::TEXT
           AND (
               input.published_lifecycle IS DISTINCT FROM 'failed'
               OR (
                   NOT input.state_degraded
                   AND input.execution_generation IS NULL
               )
           )
           AND observed.observed_payload ? $4::TEXT"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .bind(episode_key)
    .execute(executor)
    .await?;
    Ok(())
}
