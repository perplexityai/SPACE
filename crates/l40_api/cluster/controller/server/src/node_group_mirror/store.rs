//! The node-group mirror's Postgres operations: the `node_groups` read, through
//! `pplx-space-node-group-store`, and the transaction the bulk UPDATE of
//! `nodes.node_group_id` (`pplx_space_node_registration_store::node_group`)
//! runs in.
//!
//! The read takes any executor. The write is a pair on one connection,
//! [`begin_mirror_write`] then [`mirror_worker_groups`], because the statement
//! the first prepares is cached on that connection. The reconciler owns
//! everything around them: the client-side bounds, what shutdown abandons,
//! which connections it closes, and the COMMIT.

use std::collections::HashMap;
use std::time::Duration;

use pplx_space_node_model::NodeGroupId;
use pplx_space_node_registration_store::node_group;
use pplx_space_postgres_pool::TxSettings as _;
use sqlx::{Connection, PgConnection, Postgres, Transaction};

/// `node_groups` as `name -> id`.
pub(super) async fn group_ids_by_name(
    executor: impl pplx_space_postgres_pool::Executor<'_>,
) -> Result<HashMap<String, NodeGroupId>, sqlx::Error> {
    let rows = pplx_space_node_group_store::node_group_names(executor).await?;
    Ok(rows.into_iter().map(|row| (row.name, row.id)).collect())
}

/// Open a mirror write's transaction on `conn`, with `statement_timeout` and
/// `idle_in_transaction_session_timeout` set for that transaction only and the
/// UPDATE prepared.
///
/// The bounds end with the transaction, so the connection goes back to the
/// pool with the role's own settings. Preparing here makes
/// [`mirror_worker_groups`] on the transaction a statement-cache hit, one round
/// trip, and puts any cache eviction in this setup rather than in the write.
pub(super) async fn begin_mirror_write(
    conn: &mut PgConnection,
    statement_timeout: Duration,
    idle_timeout: Duration,
) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    let mut tx = conn.begin().await?;
    tx.set_local_statement_timeout(statement_timeout).await?;
    tx.set_local_idle_in_transaction_timeout(idle_timeout)
        .await?;
    node_group::prepare_mirror_worker_node_groups(&mut *tx).await?;
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
    node_group::mirror_worker_node_groups(&mut **tx, node_names, group_ids).await
}
