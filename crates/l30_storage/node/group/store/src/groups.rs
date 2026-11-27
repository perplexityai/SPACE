//! Group rows as the admin node-group routes show them, with their worker
//! counts: one group, every group, and one group locked for a guarded write.

use pplx_space_accounts::OrgId;
use pplx_space_node_model::{NodeGroupId, NodeGroupName, NodeGroupTemplatePolicy};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;

#[derive(Debug, Clone, FromRow)]
pub struct NodeGroupRow {
    pub id: NodeGroupId,
    pub name: NodeGroupName,
    pub org_id: Option<OrgId>,
    pub template_policy: NodeGroupTemplatePolicy,
    pub is_default: bool,
    pub created_at: OffsetDateTime,
    pub nodes_count: i64,
}

// `nodes_count` counts worker rows only — node groups are worker pools, and
// builder/cluster rows always sit at `node_group_id` NULL because the mirror
// stamps workers only. For the singleton default group the count also
// includes NULL-membership workers: an unlabeled (or unknown-label) worker
// mirrors as NULL, which the data model resolves to the default generic
// group — without the `is_default` arm the shared pool would report zero
// nodes until every worker is explicitly labeled. (A correlated subquery
// rather than a GROUP BY join because [`lock_node_group`] reads the same
// row with `FOR UPDATE OF g`, which Postgres forbids alongside GROUP BY.)

/// `node_group_id`'s row, if the group exists.
pub async fn node_group(
    executor: impl Executor<'_>,
    node_group_id: NodeGroupId,
) -> Result<Option<NodeGroupRow>, DbError> {
    Ok(sqlx::query_as::<_, NodeGroupRow>(sql!(
        "SELECT g.id,
       g.name,
       g.org_id,
       g.template_policy,
       g.is_default,
       g.created_at,
       (SELECT COUNT(*) FROM nodes n
         WHERE n.role = 'worker'
           AND (n.node_group_id = g.id
                OR (g.is_default AND n.node_group_id IS NULL)))::BIGINT AS nodes_count
FROM node_groups g
WHERE g.id = $1"
    ))
    .bind(node_group_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Every group's row, ordered by name.
pub async fn node_groups(executor: impl Executor<'_>) -> Result<Vec<NodeGroupRow>, DbError> {
    Ok(sqlx::query_as::<_, NodeGroupRow>(sql!(
        "SELECT g.id,
       g.name,
       g.org_id,
       g.template_policy,
       g.is_default,
       g.created_at,
       (SELECT COUNT(*) FROM nodes n
         WHERE n.role = 'worker'
           AND (n.node_group_id = g.id
                OR (g.is_default AND n.node_group_id IS NULL)))::BIGINT AS nodes_count
FROM node_groups g
ORDER BY g.name ASC"
    ))
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// [`node_group`] with the group's row locked `FOR UPDATE`; the counted
/// `nodes` rows are not locked. The lock lasts until the caller's
/// transaction ends, so a guarded write validates and writes under it. The
/// caller must pass its open transaction.
pub async fn lock_node_group<C>(
    tx: &mut C,
    node_group_id: NodeGroupId,
) -> Result<Option<NodeGroupRow>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, NodeGroupRow>(sql!(
        "SELECT g.id,
       g.name,
       g.org_id,
       g.template_policy,
       g.is_default,
       g.created_at,
       (SELECT COUNT(*) FROM nodes n
         WHERE n.role = 'worker'
           AND (n.node_group_id = g.id
                OR (g.is_default AND n.node_group_id IS NULL)))::BIGINT AS nodes_count
FROM node_groups g
WHERE g.id = $1
FOR UPDATE OF g"
    ))
    .bind(node_group_id)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
