use pplx_space_common_core::{sql, types::NodeId};
use sqlx::PgConnection;

use crate::node_publish::plan::templates::{
    TemplateComponentClaim, TemplateComponentKeyClaim, TemplateReleaseClaim,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PruneTemplateComponentsError {
    #[error("replace node template components: {0}")]
    Storage(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpsertTemplateComponentsError {
    #[error("insert node template components: {0}")]
    Storage(#[source] sqlx::Error),
}

/// Prune components in the reported release scope, recognizing component keys
/// only for this node's existing inventory. Ordinary component-row write locks
/// precede upsert on the caller's connection, with no explicit template locks.
/// Empty scopes do nothing; empty keys clear those scopes. Zero rows is normal.
pub(crate) async fn prune(
    conn: &mut PgConnection,
    node_id: NodeId,
    scopes: &[TemplateReleaseClaim],
    keys: &[TemplateComponentKeyClaim],
) -> Result<(), PruneTemplateComponentsError> {
    if scopes.is_empty() {
        return Ok(());
    }
    sqlx::query(sql!(
        "WITH scope AS (
             SELECT DISTINCT input.template_name, COALESCE(input.content_hash, '') AS content_hash
             FROM UNNEST($2::TEXT[], $3::TEXT[]) AS input(
                 template_name, content_hash
             )
         ),
         input AS (
             SELECT template_name, content_hash, kind, role_key
             FROM UNNEST($4::TEXT[], $5::TEXT[], $6::TEXT[], $7::TEXT[]) AS input(
                 template_name, content_hash, kind, role_key
             )
         ),
         incoming AS (
             SELECT input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    input.kind, input.role_key
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
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
    .bind(
        scopes
            .iter()
            .map(|scope| scope.template.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        scopes
            .iter()
            .map(|scope| scope.content_hash.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        keys.iter()
            .map(|key| key.release.template.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        keys.iter()
            .map(|key| key.release.content_hash.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(keys.iter().map(|key| key.kind.as_str()).collect::<Vec<_>>())
    .bind(
        keys.iter()
            .map(|key| key.role_key.as_str())
            .collect::<Vec<_>>(),
    )
    .execute(&mut *conn)
    .await
    .map_err(PruneTemplateComponentsError::Storage)?;
    Ok(())
}

/// Upsert component values only for inventory already on this node. Conflict
/// checks lock component rows, but unchanged values are not rewritten. Empty
/// claims issue no SQL; claims outside node inventory are a normal zero outcome.
pub(crate) async fn upsert(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[TemplateComponentClaim],
) -> Result<(), UpsertTemplateComponentsError> {
    if claims.is_empty() {
        return Ok(());
    }
    // Preserve claim order and NULL positions in every bound column.
    macro_rules! column {
        ($value:expr) => {
            claims.iter().map($value).collect::<Vec<_>>()
        };
    }
    sqlx::query(
        sql!("WITH input AS (
             SELECT template_name, content_hash, kind, role_key, version, digest, format
             FROM UNNEST(
                 $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[],
                 $6::TEXT[], $7::TEXT[], $8::TEXT[]
             ) AS input(
                 template_name, content_hash, kind, role_key, version, digest, format
             )
         ),
         incoming AS (
             SELECT $1 AS node_id, input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    input.kind, input.role_key, input.version, input.digest, input.format
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
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
    .bind(column!(|claim| claim.key.release.template.as_str()))
    .bind(column!(|claim| claim.key.release.content_hash.as_str()))
    .bind(column!(|claim| claim.key.kind.as_str()))
    .bind(column!(|claim| claim.key.role_key.as_str()))
    .bind(column!(|claim| claim.version.as_deref()))
    .bind(column!(|claim| claim.digest.as_deref()))
    .bind(column!(|claim| claim.format.as_deref()))
    .execute(&mut *conn)
    .await
    .map_err(UpsertTemplateComponentsError::Storage)?;
    Ok(())
}
