//! `node_facet_observed_status`: what each node last reported applying for a
//! facet/element pair, keyed by `(node_id, facet, element)`.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use sqlx::types::Json;

/// The primary key of one observed-status row.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ObservedStatusKey {
    pub node_id: NodeId,
    pub facet: String,
    pub element: String,
}

/// The greatest key in the table; `None` when it is empty.
pub async fn greatest_key(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<ObservedStatusKey>, sqlx::Error> {
    sqlx::query_as::<_, ObservedStatusKey>(sql!(
        "SELECT node_id, facet, element
         FROM node_facet_observed_status
         ORDER BY node_id DESC, facet DESC, element DESC
         LIMIT 1"
    ))
    .fetch_optional(executor)
    .await
}

/// Up to `limit` keys in `(after, through]`, ascending: one window of a
/// primary-key range scan.
pub async fn keys_in_window(
    executor: impl sqlx::PgExecutor<'_>,
    after: &ObservedStatusKey,
    through: &ObservedStatusKey,
    limit: i64,
) -> Result<Vec<ObservedStatusKey>, sqlx::Error> {
    sqlx::query_as::<_, ObservedStatusKey>(sql!(
        "SELECT node_id, facet, element
         FROM node_facet_observed_status
         WHERE (node_id, facet, element) > ROW($1, $2, $3)
           AND (node_id, facet, element) <= ROW($4, $5, $6)
         ORDER BY node_id, facet, element
         LIMIT $7"
    ))
    .bind(after.node_id)
    .bind(&after.facet)
    .bind(&after.element)
    .bind(through.node_id)
    .bind(&through.facet)
    .bind(&through.element)
    .bind(limit)
    .fetch_all(executor)
    .await
}

/// Delete up to `limit` rows in the key range `(from, to]` that have not been
/// updated for `orphan_age_secs` (on the database clock) and whose element
/// `node_id` no longer reports: a `network-policy` row whose sandbox has no
/// `sandbox_observed_states` row on that node or whose template is absent
/// from its `node_template_inventory`, or a sandbox or template facet row
/// whose element is likewise gone. Their `node_facet_conditions` rows go
/// with them. Candidates are picked unordered within the window. Returns
/// `(0, statuses deleted, conditions deleted)`.
pub async fn delete_orphaned_in_window(
    executor: impl sqlx::PgExecutor<'_>,
    orphan_age_secs: i64,
    limit: i64,
    from: &ObservedStatusKey,
    to: &ObservedStatusKey,
) -> Result<(i64, i64, i64), sqlx::Error> {
    sqlx::query_as(sql!(
        "WITH stale AS MATERIALIZED (
             SELECT nfos.node_id, nfos.facet, nfos.element
             FROM node_facet_observed_status nfos
             WHERE (nfos.node_id, nfos.facet, nfos.element) > ROW($3, $4, $5)
               AND (nfos.node_id, nfos.facet, nfos.element) <= ROW($6, $7, $8)
               AND nfos.updated_at < NOW() - make_interval(secs => $1::BIGINT)
               AND (
                   (
                       nfos.facet = 'network-policy'
                       AND nfos.element LIKE 'sandbox:%'
                       AND NOT EXISTS (
                           SELECT 1
                           FROM sandbox_observed_states sos
                           WHERE sos.node_id = nfos.node_id
                             AND nfos.element = 'sandbox:' || sos.sandbox_id::TEXT
                       )
                   )
                   OR (
                       nfos.facet = 'network-policy'
                       AND nfos.element LIKE 'template:%'
                       AND NOT EXISTS (
                           SELECT 1
                           FROM node_template_inventory nti
                           WHERE nti.node_id = nfos.node_id
                             AND nfos.element = 'template:' || nti.template_name
                       )
                   )
                   OR (
                       nfos.facet IN ('sandbox-inventory', 'sandbox-desired-state')
                       AND NOT EXISTS (
                           SELECT 1
                           FROM sandbox_observed_states sos
                           WHERE sos.node_id = nfos.node_id
                             AND nfos.element = sos.sandbox_id::TEXT
                       )
                   )
                   OR (
                       nfos.facet IN (
                           'template-inventory',
                           'template-desired-state',
                           'template-base-snapshot-policy'
                       )
                       AND NOT EXISTS (
                           SELECT 1
                           FROM node_template_inventory nti
                           WHERE nti.node_id = nfos.node_id
                             AND nfos.element = nti.template_name
                       )
                   )
               )
             LIMIT $2
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING stale
             WHERE nfc.node_id = stale.node_id
               AND nfc.facet = stale.facet
               AND nfc.element = stale.element
             RETURNING 1
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING stale
             WHERE nfos.node_id = stale.node_id
               AND nfos.facet = stale.facet
               AND nfos.element = stale.element
             RETURNING 1
         )
         SELECT
             0::BIGINT,
             (SELECT COUNT(*)::BIGINT FROM deleted_status),
             (SELECT COUNT(*)::BIGINT FROM deleted_conditions)"
    ))
    .bind(orphan_age_secs)
    .bind(limit)
    .bind(from.node_id)
    .bind(&from.facet)
    .bind(&from.element)
    .bind(to.node_id)
    .bind(&to.facet)
    .bind(&to.element)
    .fetch_one(executor)
    .await
}

/// Ensure `node_id` has a row for the `(facet, element)` ref (`None`, the
/// whole-facet ref), inserting an empty observation or touching
/// `updated_at` on the one there.
pub async fn touch(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    element: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_at, updated_at
         )
         VALUES ($1, $2, COALESCE($3, ''), NOW(), NOW())
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET updated_at = NOW()"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete `node_id`'s row for `facet` and `element`. Returns the rows
/// deleted.
pub async fn delete_for_element(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    element: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM node_facet_observed_status
         WHERE node_id = $1
           AND facet = $2
           AND element = $3"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// Delete `node_id`'s rows for `facet` and any of `elements`. Returns the
/// rows deleted.
pub async fn delete_for_elements(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    elements: &[String],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM node_facet_observed_status
         WHERE node_id = $1
           AND facet = $2
           AND element = ANY($3)"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(elements)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// Record `node_id`'s observed network policy for `facet` on element
/// `template:<template_name>`, for each record of `rows` (a JSON array of
/// objects with `template_name`, `network_policy_json`,
/// `network_policy_generation` and `network_policy_authority`) that carries
/// a generation and names a template the node's `node_template_inventory`
/// lists. A row already holding the same observation is left as it is.
pub async fn upsert_template_network_policies(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 network_policy_json JSONB,
                 network_policy_generation BIGINT,
                 network_policy_authority TEXT
             )
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_generation, observed_payload,
             applied_source, applied_authority, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             'template:' || template_name,
             network_policy_generation,
             network_policy_json,
             NULL,
             network_policy_authority,
             NOW(),
             NOW()
         FROM input
         WHERE network_policy_generation IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM node_template_inventory inventory
               WHERE inventory.node_id = $1 AND inventory.template_name = input.template_name
           )
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_generation = EXCLUDED.observed_generation,
             observed_payload = EXCLUDED.observed_payload,
             applied_source = EXCLUDED.applied_source,
             applied_authority = EXCLUDED.applied_authority,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE (
             node_facet_observed_status.observed_generation,
             node_facet_observed_status.observed_payload,
             node_facet_observed_status.applied_source,
             node_facet_observed_status.applied_authority
         ) IS DISTINCT FROM (
             EXCLUDED.observed_generation,
             EXCLUDED.observed_payload,
             EXCLUDED.applied_source,
             EXCLUDED.applied_authority
         )"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .execute(executor)
    .await?;
    Ok(())
}

/// Record `node_id`'s local suspend handoffs under `key` in the payloads of
/// its `facet` rows, for each record of `rows` (a JSON array of objects with
/// `sandbox_id`, `status`, `published_lifecycle`, `execution_generation`,
/// `pod_uid` and the `current_local_artifact_*` fields) that carries a
/// status. A sandbox published `suspended` with a full local suspend
/// artifact records its execution generation, pod and artifact; any other
/// clears a handoff recorded earlier. Only a Kubernetes-managed sandbox
/// started and active on `node_id` whose active, running or recovering
/// execution matches the reported generation and pod is written, and a row
/// already holding the same handoff is left as it is.
pub async fn record_local_suspend_handoffs(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
    key: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS MATERIALIZED (
             SELECT input.sandbox_id, input.execution_generation, input.pod_uid,
                    CASE WHEN input.published_lifecycle = 'suspended'
                               AND input.current_local_artifact_id IS NOT NULL
                               AND input.current_local_artifact_kind = 'full'
                               AND input.current_local_artifact_origin = 'suspend'
                         THEN jsonb_build_object($4::TEXT, jsonb_build_object(
                             'execution_generation', input.execution_generation,
                             'pod_uid', input.pod_uid,
                             'local_artifact_id', input.current_local_artifact_id
                         ))
                         ELSE '{}'::JSONB END AS payload
               FROM jsonb_to_recordset($2::JSONB) AS input(
                   sandbox_id UUID, status TEXT, published_lifecycle TEXT,
                   execution_generation BIGINT,
                   pod_uid TEXT, current_local_artifact_id UUID,
                   current_local_artifact_kind TEXT, current_local_artifact_origin TEXT
               )
              WHERE input.status IS NOT NULL
         ), candidate AS MATERIALIZED (
             -- Only rows that record or clear a handoff reach the joins.
             SELECT input.* FROM input
              WHERE input.payload ? $4::TEXT
                 OR input.sandbox_id::TEXT IN (
                     SELECT prior.element FROM node_facet_observed_status prior
                      WHERE prior.node_id = $1 AND prior.facet = $3
                        AND prior.observed_payload ? $4::TEXT
                 )
         ), observation AS (
             SELECT candidate.sandbox_id, candidate.payload
               FROM candidate
               JOIN sandboxes s ON s.id = candidate.sandbox_id
               JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
              WHERE s.active_node_id = $1
                AND s.managed_by = $5
                AND s.lifecycle = 'started'
                AND e.active AND e.state IN ('running', 'recovering')
                AND e.generation = candidate.execution_generation
                AND e.pod_uid = candidate.pod_uid
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT $1, $3, observation.sandbox_id::TEXT, observation.payload, NOW(), NOW()
           FROM observation
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload = (COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                                    - $4::TEXT) || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> $4::TEXT
               IS DISTINCT FROM EXCLUDED.observed_payload -> $4::TEXT"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .bind(key)
    .bind(SubstrateKind::Kubernetes)
    .execute(executor)
    .await?;
    Ok(())
}

/// Record `node_id`'s observed network policy for `facet` on element
/// `sandbox:<sandbox_id>`, for each record of `rows` (a JSON array of objects
/// with `sandbox_id` and the `observed_network_policy_*` fields) that carries
/// a generation and names a sandbox with no `sandboxes` row or one active on
/// `node_id`. A row already holding the same observation is left as it is.
pub async fn upsert_sandbox_network_policies(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     observed_network_policy_json JSONB,
                     observed_network_policy_generation BIGINT,
                     observed_network_policy_source TEXT,
                     observed_network_policy_authority TEXT
                 )
             ),
             publishable AS (
                 SELECT input.*
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE s.id IS NULL
                    OR s.active_node_id = $1
             )
             INSERT INTO node_facet_observed_status (
                 node_id, facet, element, observed_generation, observed_payload,
                 applied_source, applied_authority, observed_at, updated_at
             )
             SELECT
                 $1,
                 $3,
                 'sandbox:' || sandbox_id::text,
                 observed_network_policy_generation,
                 observed_network_policy_json,
                 observed_network_policy_source,
                 observed_network_policy_authority,
                 NOW(),
                 NOW()
             FROM publishable
             WHERE observed_network_policy_generation IS NOT NULL
             ON CONFLICT (node_id, facet, element) DO UPDATE
             SET observed_generation = EXCLUDED.observed_generation,
                 observed_payload = EXCLUDED.observed_payload,
                 applied_source = EXCLUDED.applied_source,
                 applied_authority = EXCLUDED.applied_authority,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             WHERE (
                 node_facet_observed_status.observed_generation,
                 node_facet_observed_status.observed_payload,
                 node_facet_observed_status.applied_source,
                 node_facet_observed_status.applied_authority
             ) IS DISTINCT FROM (
                 EXCLUDED.observed_generation,
                 EXCLUDED.observed_payload,
                 EXCLUDED.applied_source,
                 EXCLUDED.applied_authority
             )"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .execute(executor)
    .await?;
    Ok(())
}

/// Merge the `quarantine_recovery` member into the payload of `node_id`'s
/// `facet` row for each sandbox of `rows` (a JSON array of objects with
/// `sandbox_id`, `quarantine_recovery_payload` and
/// `quarantine_recovery_observation_degraded`) that reports a recovery and
/// isn't degraded. A row holding the same recovery is rewritten only once
/// its observation is `heartbeat_secs` old on the database clock.
pub async fn upsert_quarantine_recovery(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
    heartbeat_secs: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 quarantine_recovery_payload JSONB,
                 quarantine_recovery_observation_degraded BOOLEAN
             )
         ),
         publishable AS (
             SELECT input.*
             FROM input
             WHERE NOT COALESCE(input.quarantine_recovery_observation_degraded, FALSE)
               AND input.quarantine_recovery_payload IS NOT NULL
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             publishable.sandbox_id::TEXT,
             jsonb_build_object(
                 'quarantine_recovery',
                 publishable.quarantine_recovery_payload
             ),
             NOW(),
             NOW()
         FROM publishable
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload =
                 COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                 || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> 'quarantine_recovery'
               IS DISTINCT FROM EXCLUDED.observed_payload -> 'quarantine_recovery'
            OR node_facet_observed_status.observed_at
               < NOW() - make_interval(secs => $4::BIGINT)"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .bind(heartbeat_secs)
    .execute(executor)
    .await?;
    Ok(())
}

/// Drop the `quarantine_recovery` member and `key` from the payloads of
/// `node_id`'s rows for `facet` and any of `elements` that carry either,
/// stamping them observed now.
pub async fn clear_quarantine_recovery_and_key(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    elements: &[String],
    key: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE node_facet_observed_status
         SET observed_payload = COALESCE(observed_payload, '{}'::JSONB)
                                    - 'quarantine_recovery'
                                    - $4::TEXT,
             observed_at = NOW(),
             updated_at = NOW()
         WHERE node_id = $1
           AND facet = $2
           AND element = ANY($3)
           AND (
               observed_payload ? 'quarantine_recovery'
               OR observed_payload ? $4::TEXT
           )"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(elements)
    .bind(key)
    .execute(executor)
    .await?;
    Ok(())
}

/// Drop the `quarantine_recovery` member from the payload of `node_id`'s
/// `facet` row for each sandbox of `rows` (a JSON array of objects with
/// `sandbox_id`, `quarantine_recovery_payload` and
/// `quarantine_recovery_observation_degraded`) that reports no recovery and
/// isn't degraded, where the payload carries one, stamping it observed now.
pub async fn clear_quarantine_recovery(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 quarantine_recovery_payload JSONB,
                 quarantine_recovery_observation_degraded BOOLEAN
             )
         ),
         clearable AS (
             SELECT input.sandbox_id
             FROM input
             WHERE NOT COALESCE(input.quarantine_recovery_observation_degraded, FALSE)
               AND input.quarantine_recovery_payload IS NULL
         )
         UPDATE node_facet_observed_status observed
         SET observed_payload = COALESCE(observed.observed_payload, '{}'::JSONB)
                                    - 'quarantine_recovery',
             observed_at = NOW(),
             updated_at = NOW()
         FROM clearable
         WHERE observed.node_id = $1
           AND observed.facet = $3
           AND observed.element = clearable.sandbox_id::TEXT
           AND observed.observed_payload ? 'quarantine_recovery'"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .execute(executor)
    .await?;
    Ok(())
}
