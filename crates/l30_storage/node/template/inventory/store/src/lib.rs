//! The template builds nodes report caching (`node_template_inventory`) and
//! their execution components (`node_template_components`), in the
//! control-plane database.
//!
//! Nodes publish their inventory through internal-api, which upserts it and
//! its components and deletes the templates a node reports absent here. The
//! full-inventory prune, internal-api's `prune_absent_node_templates`, still
//! deletes rows directly, in one statement with the `node_facet_observed_status`
//! rows of the templates it removes. The admin node-template routes delete one
//! node's cached copies and list them; template admin and public routes, and
//! the release lookups, read what nodes hold. Each verb takes any executor, so
//! a caller's guard keeps its transaction and locks. The reads that also ask
//! whether a Direct node's registry row is live are in [`direct`].
//!
//! Rows whose `snapshots` JSON a caller decodes are generic over that payload
//! type: the shape belongs to the caller's response, and decoding it in the
//! row keeps a malformed payload a column-decode error.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_node_model::NodeGroupId;
use pplx_space_sql_locator::sql;
use serde::de::DeserializeOwned;
use sqlx::types::Json;
use sqlx::{FromRow, PgExecutor};

pub mod direct;
mod publish;
mod template;

pub use publish::{
    AcceptedTemplateBuild, delete_unpublished_template_components,
    upsert_published_template_components, upsert_published_templates,
};
pub use template::{
    ReleaseShape, TemplatePoolObserved, TemplateVariant, agreed_build_environment,
    release_build_shape, template_inventory_rows, template_pool_observed, template_snapshot_shape,
    template_variants,
};

/// One build cached on a node, as the admin node-template inventory lists it.
#[derive(Debug, Clone, FromRow)]
pub struct NodeTemplateBuild<Snapshots> {
    pub name: String,
    pub content_hash: Option<String>,
    pub build_date: Option<String>,
    pub artifact_size_bytes: Option<i64>,
    pub cache_size_bytes: Option<i64>,
    pub disk_size_mb: Option<i32>,
    pub runtime: Option<String>,
    pub arch: Option<String>,
    pub snapshots: Json<Snapshots>,
}

/// The builds `node_id` caches, by template name then content hash.
pub async fn node_template_builds<Snapshots>(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
) -> Result<Vec<NodeTemplateBuild<Snapshots>>, sqlx::Error>
where
    Snapshots: DeserializeOwned + Send + Unpin + 'static,
{
    sqlx::query_as::<_, NodeTemplateBuild<Snapshots>>(sql!(
        "SELECT template_name AS name,
                NULLIF(content_hash, '') AS content_hash,
                built_at AS build_date,
                artifact_size_bytes,
                cache_size_bytes,
                disk_size_mb,
                runtime,
                arch,
                snapshots
         FROM node_template_inventory
         WHERE node_id = $1
         ORDER BY template_name ASC, content_hash ASC"
    ))
    .bind(node_id)
    .fetch_all(executor)
    .await
}

/// How many builds of `template` `node_id` caches.
pub async fn node_template_build_count(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    template: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT
         FROM node_template_inventory
         WHERE node_id = $1
           AND template_name = $2"
    ))
    .bind(node_id)
    .bind(template)
    .fetch_one(executor)
    .await
}

/// Forget `node_id`'s cached build `content_hash` of `template`.
pub async fn delete_node_template_build(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    template: &str,
    content_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_template_inventory
         WHERE node_id = $1
           AND template_name = $2
           AND content_hash = $3"
    ))
    .bind(node_id)
    .bind(template)
    .bind(content_hash)
    .execute(executor)
    .await?;
    Ok(())
}

/// Forget every cached build of `template` on `node_id`.
pub async fn delete_node_template(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    template: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_template_inventory
         WHERE node_id = $1
           AND template_name = $2"
    ))
    .bind(node_id)
    .bind(template)
    .execute(executor)
    .await?;
    Ok(())
}

/// Forget every cached build of each of `templates` on `node_id`, and return
/// how many rows went.
pub async fn delete_node_templates(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    templates: &[String],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM node_template_inventory
             WHERE node_id = $1 AND template_name = ANY($2)"
    ))
    .bind(node_id)
    .bind(templates)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// How many cached builds (one row per node and build) of org-specific
/// templates the nodes of `node_group_id` hold that an assignment of the
/// group to `new_org_id` would strand: those owned by another org, or, for a
/// release to generic (`None`), every one.
pub async fn stranded_org_specific_templates(
    executor: impl PgExecutor<'_>,
    node_group_id: NodeGroupId,
    new_org_id: Option<OrgId>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)::BIGINT
           FROM node_template_inventory nti
           JOIN nodes n ON n.id = nti.node_id
           JOIN templates t ON t.name = nti.template_name
          WHERE n.node_group_id = $1
            AND t.visibility = 'org_specific'
            AND ($2::UUID IS NULL OR t.org_id IS DISTINCT FROM $2)"
    ))
    .bind(node_group_id)
    .bind(new_org_id)
    .fetch_one(executor)
    .await
}

#[cfg(test)]
mod tests;
