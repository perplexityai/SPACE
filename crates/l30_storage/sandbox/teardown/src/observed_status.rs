//! `node_facet_observed_status`: what each node last reported applying for a
//! facet/element pair, keyed by `(node_id, facet, element)`.

use pplx_space_accounts::NodeId;
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
