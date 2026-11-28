//! Node groups (`node_groups`) and their template allowlists
//! (`node_group_template_compat`) in the control-plane database.
//!
//! The admin node-group routes, the placement catalog read and the
//! cluster-controller's node-group mirror and labeler run their statements
//! through these verbs. [`groups`] reads group rows with their worker counts,
//! and locks one for an admin route's guarded write; [`locks`] holds the
//! default-row lock `:set-default` takes first and the table lock the
//! labeler's default-group seed takes. The locking verbs take the caller's
//! transaction and their locks hold until it ends; every other verb takes any
//! executor, so it composes into that transaction. The callers keep their
//! `lock_timeout` settings, the template `FOR SHARE` read stays with the admin
//! routes, and so do the decisions about which verbs a request runs.
//!
//! What nodes cache is `pplx-space-node-template-inventory-store`'s; the
//! `nodes.node_group_id` membership the mirror writes stays with the mirror.

use pplx_space_accounts::OrgId;
use pplx_space_node_model::{NodeGroupId, NodeGroupName, NodeGroupTemplatePolicy};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;

mod catalog;
mod compat;
pub mod groups;
pub mod locks;

pub use catalog::{TemplateCatalogRow, template_catalog};
pub use compat::{
    NodeGroupCompatTemplate, clear_template_compat, compat_templates, insert_template_compat,
    prune_cross_org_template_compat,
};

/// A group row to insert. The id is application-generated. The insert
/// reports constraint violations as `DbError`s for the caller to map,
/// among them the unique name and singleton-default indexes, the
/// `organizations` foreign key on `org_id`, and the table's CHECKs.
#[derive(Debug, Clone, Copy)]
pub struct NewNodeGroup<'a> {
    pub id: NodeGroupId,
    pub name: &'a NodeGroupName,
    pub org_id: Option<OrgId>,
    pub template_policy: NodeGroupTemplatePolicy,
    pub is_default: bool,
}

/// A group's name and id.
#[derive(Debug, Clone, FromRow)]
pub struct NodeGroupNameId {
    pub name: String,
    pub id: NodeGroupId,
}

pub async fn insert_node_group(
    executor: impl Executor<'_>,
    group: NewNodeGroup<'_>,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, $2, $3, $4, $5)"
    ))
    .bind(group.id)
    .bind(group.name)
    .bind(group.org_id)
    .bind(group.template_policy)
    .bind(group.is_default)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Whether any generic (org-less) node group exists.
pub async fn generic_node_group_exists(executor: impl Executor<'_>) -> Result<bool, DbError> {
    let count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_groups WHERE org_id IS NULL"
    ))
    .fetch_one(executor.into_sqlx())
    .await?;
    Ok(count > 0)
}

/// Every group's name and id, unordered.
pub async fn node_group_names(
    executor: impl Executor<'_>,
) -> Result<Vec<NodeGroupNameId>, DbError> {
    Ok(
        sqlx::query_as::<_, NodeGroupNameId>(sql!("SELECT name, id FROM node_groups"))
            .fetch_all(executor.into_sqlx())
            .await?,
    )
}

/// The default group's name and id, if a group is the default.
pub async fn default_node_group(
    executor: impl Executor<'_>,
) -> Result<Option<NodeGroupNameId>, DbError> {
    Ok(sqlx::query_as::<_, NodeGroupNameId>(sql!(
        "SELECT id, name FROM node_groups WHERE is_default LIMIT 1"
    ))
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// How many groups are not the default.
pub async fn non_default_node_group_count(executor: impl Executor<'_>) -> Result<i64, DbError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*) FROM node_groups WHERE NOT is_default"
    ))
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Insert `id` as the default generic group named `name`, with the `default`
/// template policy. `false` when a conflicting row (the name, or another
/// default) kept it out.
pub async fn insert_default_node_group(
    executor: impl Executor<'_>,
    id: NodeGroupId,
    name: &NodeGroupName,
) -> Result<bool, DbError> {
    // `RETURNING id` for the row's *presence*, not its value: `ON CONFLICT
    // DO NOTHING` means the only id this can return is the one bound here.
    let inserted: Option<(NodeGroupId,)> = sqlx::query_as(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, $2, NULL, $3, TRUE)
             ON CONFLICT DO NOTHING
             RETURNING id"
    ))
    .bind(id)
    .bind(name.as_str())
    // Bound from the enum rather than written as a `'default'` literal: the
    // enum owns the token, and `insert_node_group` binds this column the same
    // way.
    .bind(NodeGroupTemplatePolicy::Default)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(inserted.is_some())
}

/// Set the group's owner, snapping its template policy back to `default`
/// when `reset_policy` is set.
pub async fn update_node_group_org(
    executor: impl Executor<'_>,
    node_group_id: NodeGroupId,
    new_org_id: Option<OrgId>,
    reset_policy: bool,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE node_groups
            SET org_id = $2,
                template_policy = CASE WHEN $3 THEN 'default' ELSE template_policy END
          WHERE id = $1"
    ))
    .bind(node_group_id)
    .bind(new_org_id)
    .bind(reset_policy)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Clear the default flag from whichever group holds it.
pub async fn clear_default_node_group(executor: impl Executor<'_>) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE node_groups SET is_default = FALSE WHERE is_default"
    ))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Flag `node_group_id` as the default. Fails on the singleton-default unique
/// index while another group still holds the flag.
pub async fn set_default_node_group(
    executor: impl Executor<'_>,
    node_group_id: NodeGroupId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE node_groups SET is_default = TRUE WHERE id = $1"
    ))
    .bind(node_group_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

pub async fn set_node_group_template_policy(
    executor: impl Executor<'_>,
    node_group_id: NodeGroupId,
    policy: NodeGroupTemplatePolicy,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE node_groups SET template_policy = $2 WHERE id = $1"
    ))
    .bind(node_group_id)
    .bind(policy)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete the group row. Compat rows cascade; the `nodes.node_group_id`
/// foreign key (NO ACTION) refuses while a node is still a member.
pub async fn delete_node_group(
    executor: impl Executor<'_>,
    node_group_id: NodeGroupId,
) -> Result<(), DbError> {
    sqlx::query(sql!("DELETE FROM node_groups WHERE id = $1"))
        .bind(node_group_id)
        .execute(executor.into_sqlx())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
