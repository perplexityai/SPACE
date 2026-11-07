//! The EC2 instance a Kubernetes node runs on: recording it on the node's
//! `node_k8s` row, and keeping each instance a different one replaces in
//! `node_k8s_superseded_instances`.

use pplx_space_common_core::types::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;

/// What [`record_k8s_node_instance`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstanceRecorded {
    /// The node has no `node_k8s` row, or its row already names the instance.
    Unchanged,
    /// The row named no instance and now names this one.
    First,
    /// The row named another instance, which is now kept as superseded.
    Superseded,
}

impl InstanceRecorded {
    pub(crate) const fn changed(self) -> bool {
        !matches!(self, Self::Unchanged)
    }
}

/// Record `instance_id` on the node's `node_k8s` row. When the row named a
/// different instance, first keep that instance, with the Kubernetes Node
/// UID the row held, in `node_k8s_superseded_instances`. Writes nothing for a
/// node without a `node_k8s` row, so it creates no registration.
///
/// The read locks the row only when it doesn't already name `instance_id` (it
/// names no instance, or another one), so a report of the instance already
/// recorded takes no lock. A writer that waited on the lock re-reads the row
/// its holder committed: if that already names `instance_id`, nothing is
/// written, and otherwise the committed instance is the one kept. Each
/// replacement is kept exactly once. Runs on the caller's connection, inside
/// the caller's transaction.
pub(crate) async fn record_k8s_node_instance(
    conn: &mut PgConnection,
    node_id: NodeId,
    instance_id: &str,
) -> Result<InstanceRecorded, sqlx::Error> {
    let current = sqlx::query_as::<_, (Option<String>, Option<String>)>(sql!(
        "SELECT instance_id, k8s_node_uid FROM node_k8s
         WHERE node_id = $1 AND instance_id IS DISTINCT FROM $2
         FOR UPDATE"
    ))
    .bind(node_id)
    .bind(instance_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((previous, k8s_node_uid)) = current else {
        return Ok(InstanceRecorded::Unchanged);
    };

    if let Some(previous) = &previous {
        // `clock_timestamp()`, not `NOW()`: the caller's transaction may have
        // started before another writer's replacement committed, and this row,
        // written under the row lock, must date after that one.
        sqlx::query(sql!(
            "INSERT INTO node_k8s_superseded_instances
                 (node_id, instance_id, k8s_node_uid, superseded_at)
             VALUES ($1, $2, $3, clock_timestamp())"
        ))
        .bind(node_id)
        .bind(previous)
        .bind(k8s_node_uid)
        .execute(&mut *conn)
        .await?;
    }

    // `updated_at` dates the row's Kubernetes metadata, which absence
    // demotion reads; an instance report is not that.
    sqlx::query(sql!(
        "UPDATE node_k8s SET instance_id = $2 WHERE node_id = $1"
    ))
    .bind(node_id)
    .bind(instance_id)
    .execute(&mut *conn)
    .await?;

    Ok(if previous.is_some() {
        InstanceRecorded::Superseded
    } else {
        InstanceRecorded::First
    })
}
