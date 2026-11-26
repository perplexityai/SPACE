//! `node_facet_conditions`: the conditions, such as `ApplyFailed`, each node
//! reported for a facet/element pair.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::types::Json;

/// Record that `node_id` failed to apply the `(facet, element)` ref (`None`,
/// the whole-facet ref) with `message`: its `ApplyFailed` condition becomes
/// `True` for `ReconcileFailed`, its transition time moving only when the
/// status changes. The ref's observed-status row must exist
/// ([`crate::observed_status::touch`]).
pub async fn record_apply_failed(
    executor: impl Executor<'_>,
    node_id: NodeId,
    facet: &str,
    element: Option<&str>,
    message: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO node_facet_conditions (
             node_id, facet, element, condition_type, condition_status,
             reason, message, last_transition_at, updated_at
         )
         VALUES (
             $1, $2, COALESCE($3, ''), 'ApplyFailed', 'True',
             'ReconcileFailed', $4, NOW(), NOW()
         )
         ON CONFLICT (node_id, facet, element, condition_type) DO UPDATE
         SET condition_status = EXCLUDED.condition_status,
             reason = EXCLUDED.reason,
             message = EXCLUDED.message,
             last_transition_at = CASE
                 WHEN node_facet_conditions.condition_status IS DISTINCT FROM EXCLUDED.condition_status
                 THEN NOW()
                 ELSE node_facet_conditions.last_transition_at
             END,
             updated_at = NOW()"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .bind(message)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Clear `node_id`'s `ApplyFailed` condition for the `(facet, element)` ref
/// (`None`, the whole-facet ref).
pub async fn clear_apply_failed(
    executor: impl Executor<'_>,
    node_id: NodeId,
    facet: &str,
    element: Option<&str>,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM node_facet_conditions
         WHERE node_id = $1
           AND facet = $2
           AND element = COALESCE($3, '')
           AND condition_type = 'ApplyFailed'"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Clear `node_id`'s `ApplyFailed` conditions for `facet` on element
/// `template:<template_name>`, for each record of `rows` (a JSON array of
/// objects with `template_name` and `network_policy_generation`) that
/// carries a generation and names a template the node's
/// `node_template_inventory` lists.
pub async fn clear_template_apply_failed(
    executor: impl Executor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 network_policy_generation BIGINT
             )
         )
         DELETE FROM node_facet_conditions nfc
         USING input
         WHERE nfc.node_id = $1
           AND nfc.facet = $3
           AND nfc.element = 'template:' || input.template_name
           AND input.network_policy_generation IS NOT NULL
           AND nfc.condition_type = 'ApplyFailed'
           AND EXISTS (
               SELECT 1 FROM node_template_inventory inventory
               WHERE inventory.node_id = $1 AND inventory.template_name = input.template_name
           )"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Clear `node_id`'s `ApplyFailed` conditions for `facet` on element
/// `sandbox:<sandbox_id>`, for each record of `rows` (a JSON array of objects
/// with `sandbox_id` and `observed_network_policy_generation`) that carries a
/// generation and whose sandbox is gone or still active on `node_id`.
pub async fn clear_sandbox_apply_failed(
    executor: impl Executor<'_>,
    node_id: NodeId,
    rows: &serde_json::Value,
    facet: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 observed_network_policy_generation BIGINT
             )
         ),
         publishable AS (
             SELECT input.*
             FROM input
             LEFT JOIN sandboxes s ON s.id = input.sandbox_id
             WHERE (s.id IS NULL OR s.active_node_id = $1)
               AND input.observed_network_policy_generation IS NOT NULL
         )
         DELETE FROM node_facet_conditions nfc
         USING publishable
         WHERE nfc.node_id = $1
           AND nfc.facet = $3
           AND nfc.element = 'sandbox:' || publishable.sandbox_id::text
           AND nfc.condition_type = 'ApplyFailed'"
    ))
    .bind(node_id)
    .bind(Json(rows))
    .bind(facet)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
