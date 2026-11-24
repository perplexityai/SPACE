//! `node_group_template_compat`: the template names a `restricted` group
//! admits.

use pplx_space_accounts::OrgId;
use pplx_space_node_model::NodeGroupId;
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, PgExecutor};

/// One allowlist entry.
#[derive(Debug, Clone, FromRow)]
pub struct NodeGroupCompatTemplate {
    pub node_group_id: NodeGroupId,
    pub template_name: String,
}

/// The allowlist entries of `node_group_ids`, in template-name order.
pub async fn compat_templates(
    executor: impl PgExecutor<'_>,
    node_group_ids: &[NodeGroupId],
) -> Result<Vec<NodeGroupCompatTemplate>, sqlx::Error> {
    sqlx::query_as::<_, NodeGroupCompatTemplate>(sql!(
        "SELECT node_group_id, template_name
           FROM node_group_template_compat
          WHERE node_group_id = ANY($1::UUID[])
          ORDER BY template_name ASC"
    ))
    .bind(node_group_ids)
    .fetch_all(executor)
    .await
}

/// Remove every allowlist entry of the group.
pub async fn clear_template_compat(
    executor: impl PgExecutor<'_>,
    node_group_id: NodeGroupId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_group_template_compat WHERE node_group_id = $1"
    ))
    .bind(node_group_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Add `names` to the group's allowlist.
pub async fn insert_template_compat(
    executor: impl PgExecutor<'_>,
    node_group_id: NodeGroupId,
    names: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO node_group_template_compat (node_group_id, template_name)
         SELECT $1, name FROM UNNEST($2::TEXT[]) AS t(name)"
    ))
    .bind(node_group_id)
    .bind(names)
    .execute(executor)
    .await?;
    Ok(())
}

/// Remove the entries naming `template_name` from every group not owned by
/// `owner_org_id`, and return how many went.
pub async fn prune_cross_org_template_compat(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    owner_org_id: OrgId,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM node_group_template_compat c
          USING node_groups g
          WHERE c.node_group_id = g.id
            AND c.template_name = $1
            AND g.org_id IS DISTINCT FROM $2"
    ))
    .bind(template_name)
    .bind(owner_org_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}
