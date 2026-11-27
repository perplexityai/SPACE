//! Mirroring Kubernetes node-group labels into `nodes.node_group_id`.

use pplx_space_node_model::{NODE_ROLE_WORKER, NodeGroupId, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::{Postgres, SqlSafeStr};

/// The bulk UPDATE, prepared by [`prepare_mirror_worker_node_groups`] and
/// executed by [`mirror_worker_node_groups`], which must send the same text
/// for the execute to find the prepared statement in the connection's cache.
const MIRROR_UPDATE: &str = sql!(
    "UPDATE nodes
        SET node_group_id = u.group_id
       FROM UNNEST($1::TEXT[], $2::UUID[]) AS u(k8s_node_name, group_id)
       JOIN node_k8s nk ON nk.k8s_node_name = u.k8s_node_name
      WHERE nodes.id = nk.node_id
        AND nodes.role = $3
        AND nodes.managed_by = $4
        AND nodes.node_group_id IS DISTINCT FROM u.group_id"
);

/// Prepare [`mirror_worker_node_groups`]'s statement on `executor`'s
/// connection, so the write on that connection is a statement-cache hit.
pub async fn prepare_mirror_worker_node_groups(executor: impl Executor<'_>) -> Result<(), DbError> {
    // The parameter types `mirror_worker_node_groups` binds, so the prepared
    // statement is the one its execute would have prepared itself.
    let parameters = [
        <Vec<String> as sqlx::Type<Postgres>>::type_info(),
        <Vec<Option<NodeGroupId>> as sqlx::Type<Postgres>>::type_info(),
        <&str as sqlx::Type<Postgres>>::type_info(),
        <SubstrateKind as sqlx::Type<Postgres>>::type_info(),
    ];
    sqlx::Executor::prepare_with(
        executor.into_sqlx(),
        MIRROR_UPDATE.into_sql_str(),
        &parameters,
    )
    .await?;
    Ok(())
}

/// Write each `(Kubernetes node name, group id)` pair into the
/// `node_group_id` of the Kubernetes worker rows registered under that name,
/// and return the number of rows that changed. A `None` group id clears the
/// column; rows already matching are untouched (`IS DISTINCT FROM` treats
/// NULLs as comparable).
///
/// Worker rows only: node ids are role-scoped, so one Kubernetes Node may back
/// several `nodes` rows (worker and builder) sharing a Kubernetes node name.
pub async fn mirror_worker_node_groups(
    executor: impl Executor<'_>,
    node_names: &[String],
    group_ids: &[Option<NodeGroupId>],
) -> Result<u64, DbError> {
    let result = sqlx::query(MIRROR_UPDATE)
        .bind(node_names)
        .bind(group_ids)
        .bind(NODE_ROLE_WORKER)
        .bind(SubstrateKind::Kubernetes)
        .execute(executor.into_sqlx())
        .await?;
    Ok(result.rows_affected())
}
