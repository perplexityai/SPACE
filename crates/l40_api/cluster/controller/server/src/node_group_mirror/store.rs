//! The node-group mirror's Postgres operations: the `node_groups` read and the
//! bulk UPDATE of `nodes.node_group_id`, with the transaction it runs in.
//!
//! The read takes any executor. The write is a pair on one connection,
//! [`begin_mirror_write`] then [`mirror_worker_groups`], because the statement
//! the first prepares is cached on that connection. The reconciler owns
//! everything around them: the client-side bounds, what shutdown abandons,
//! which connections it closes, and the COMMIT.

use std::collections::HashMap;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NODE_ROLE_WORKER, NodeGroupId, SubstrateKind};
use sqlx::{Connection, Executor, PgConnection, PgExecutor, Postgres, SqlSafeStr, Transaction};

/// `node_groups` as `name -> id`.
pub(super) async fn group_ids_by_name<'e>(
    executor: impl PgExecutor<'e>,
) -> Result<HashMap<String, NodeGroupId>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, NodeGroupId)>(sql!("SELECT name, id FROM node_groups"))
        .fetch_all(executor)
        .await?;
    Ok(rows.into_iter().collect())
}

/// Open a mirror write's transaction on `conn`, with `statement_timeout` and
/// `idle_in_transaction_session_timeout` set for that transaction only and the
/// UPDATE prepared.
///
/// `is_local = true`: the bounds end with the transaction, so the connection
/// goes back to the pool with the role's own settings. Preparing here makes
/// [`mirror_worker_groups`] on the transaction a statement-cache hit, one round
/// trip, and puts any cache eviction in this setup rather than in the write.
pub(super) async fn begin_mirror_write(
    conn: &mut PgConnection,
    statement_timeout: Duration,
    idle_timeout: Duration,
) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    let mut tx = conn.begin().await?;
    sqlx::query(sql!(
        "SELECT set_config('statement_timeout', $1, true),
                set_config('idle_in_transaction_session_timeout', $2, true)"
    ))
    .bind(format!("{}ms", statement_timeout.as_millis()))
    .bind(format!("{}ms", idle_timeout.as_millis()))
    .execute(&mut *tx)
    .await?;
    // The parameter types `mirror_worker_groups` binds, so the prepared
    // statement is the one its execute would have prepared itself.
    let parameters = [
        <Vec<String> as sqlx::Type<Postgres>>::type_info(),
        <Vec<Option<NodeGroupId>> as sqlx::Type<Postgres>>::type_info(),
        <&str as sqlx::Type<Postgres>>::type_info(),
        <SubstrateKind as sqlx::Type<Postgres>>::type_info(),
    ];
    (&mut *tx)
        .prepare_with(MIRROR_UPDATE.into_sql_str(), &parameters)
        .await?;
    Ok(tx)
}

/// Bulk-write the resolved `(k8s node name, group id)` pairs into
/// `nodes.node_group_id`, matching rows via `node_k8s.k8s_node_name`, and
/// return the number of rows that changed. Doesn't commit.
///
/// A `None` group id clears the column (label absent or unknown). Rows whose
/// mirror already matches are untouched (`IS DISTINCT FROM` treats NULLs as
/// comparable), so steady-state ticks write nothing.
///
/// Scoped to worker rows: node ids are role-scoped, so one Kubernetes Node
/// may back several `nodes` rows (worker + builder) sharing a
/// `node_k8s.k8s_node_name`. Node groups are worker pools — stamping a
/// co-hosted builder row would inflate `nodes_count` and trip the group
/// residue guards. And to Kubernetes-managed rows, the only ones a kube Node
/// label can describe: the mirror runs in the Kubernetes arm of the reconciler
/// set.
///
/// Takes the transaction [`begin_mirror_write`] opened, because the statement
/// it prepared is cached on that connection.
pub(super) async fn mirror_worker_groups(
    tx: &mut Transaction<'_, Postgres>,
    node_names: &[String],
    group_ids: &[Option<NodeGroupId>],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(MIRROR_UPDATE)
        .bind(node_names)
        .bind(group_ids)
        .bind(NODE_ROLE_WORKER)
        .bind(SubstrateKind::Kubernetes)
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

/// The bulk UPDATE, prepared by [`begin_mirror_write`] and executed by
/// [`mirror_worker_groups`], which must send the same text for the execute to
/// find the prepared statement in the cache.
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
