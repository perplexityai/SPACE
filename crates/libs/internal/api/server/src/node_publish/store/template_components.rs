use pplx_space_common_core::{sql, types::NodeId};
use sqlx::{PgConnection, types::Json};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplaceTemplateComponentsError {
    #[error("replace node template components: {0}")]
    Prune(#[source] sqlx::Error),
    #[error("insert node template components: {0}")]
    Upsert(#[source] sqlx::Error),
}

/// Replace components in the reported template/release scope, accepting new
/// components only for inventory already on this node. Delete precedes upsert
/// on the caller's connection, with ordinary component-row write locks and no
/// explicit template locks. Zero changed rows is normal; an empty component
/// list clears that reported release rather than skipping its deletion.
pub(crate) async fn replace(
    conn: &mut PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<(), ReplaceTemplateComponentsError> {
    sqlx::query(sql!(
        "WITH scope AS (
             SELECT DISTINCT input.template_name, COALESCE(input.content_hash, '') AS content_hash
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT
             )
         ),
         input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 execution_components JSONB
             )
         ),
         incoming AS (
             SELECT input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    component.kind, component.role_key, component.version,
                    component.digest, component.format
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
             CROSS JOIN LATERAL jsonb_to_recordset(
                 COALESCE(input.execution_components, '[]'::jsonb)
             ) AS component(
                 kind TEXT, role_key TEXT, version TEXT, digest TEXT, format TEXT
             )
         )
         DELETE FROM node_template_components component
         USING scope
         WHERE component.node_id = $1
           AND component.template_name = scope.template_name
           AND component.content_hash = scope.content_hash
           AND NOT EXISTS (
               SELECT 1
               FROM incoming
               WHERE incoming.template_name = component.template_name
                 AND incoming.content_hash = component.content_hash
                 AND incoming.kind = component.kind
                 AND incoming.role_key = component.role_key
           )"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .execute(&mut *conn)
    .await
    .map_err(ReplaceTemplateComponentsError::Prune)?;
    sqlx::query(
        sql!("WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 execution_components JSONB
             )
         ),
         incoming AS (
             SELECT $1 AS node_id, input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    component.kind, component.role_key, component.version,
                    component.digest, component.format
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
             CROSS JOIN LATERAL jsonb_to_recordset(
                 COALESCE(input.execution_components, '[]'::jsonb)
             ) AS component(
                 kind TEXT, role_key TEXT, version TEXT, digest TEXT, format TEXT
             )
         )
         INSERT INTO node_template_components (
             node_id, template_name, content_hash, kind, role_key, version, digest, format
         )
         SELECT node_id, template_name, content_hash, kind, role_key, version, digest, format
         FROM incoming
         ON CONFLICT (node_id, template_name, content_hash, kind, role_key) DO UPDATE
         SET version = EXCLUDED.version,
             digest = EXCLUDED.digest,
             format = EXCLUDED.format
         WHERE (
             node_template_components.version,
             node_template_components.digest,
             node_template_components.format
         ) IS DISTINCT FROM (
             EXCLUDED.version,
             EXCLUDED.digest,
             EXCLUDED.format
         )"),
    )
    .bind(node_id)
    .bind(rows_json)
    .execute(&mut *conn)
    .await
    .map_err(ReplaceTemplateComponentsError::Upsert)?;
    Ok(())
}
