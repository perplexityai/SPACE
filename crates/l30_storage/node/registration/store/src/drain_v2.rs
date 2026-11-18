//! The v2 node drain's columns of `nodes`: `drain_engine`, and the
//! `drain_status` JSON a v2 drain keeps its epoch, report and fence in.
//! Every statement and SQL fragment over them is here, so the cluster
//! controller's v2 loop, the admin API, the drain gates and registration
//! read and write them one way.

use pplx_space_accounts::NodeId;
use pplx_space_node_drain::NodeDrainEngine;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{PgPool, Postgres, Transaction};

/// SQL predicate over the `nodes` row aliased `n`: its v2 drain fenced its
/// instance for termination, `drain_status.terminate_instance`, or an earlier
/// build's terminator acted on it, `drain_status.terminated`.
pub const FENCED: &str = "(n.drain_status ? 'terminate_instance'
     OR COALESCE((n.drain_status->>'terminated')::BOOLEAN, FALSE))";

/// A node's drain v2 columns, read under the caller's row lock.
#[derive(Debug, Clone, PartialEq)]
pub struct DrainV2Columns {
    pub engine: NodeDrainEngine,
    pub status: Option<serde_json::Value>,
    /// Whether `node_k8s` records the node's EC2 instance.
    pub recorded_instance: bool,
}

/// The node's v2 drain columns; `None` when there is no such node.
pub async fn drain_v2_columns(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
) -> Result<Option<DrainV2Columns>, sqlx::Error> {
    let row: Option<(Option<String>, Option<serde_json::Value>, bool)> = sqlx::query_as(sql!(
        "SELECT n.drain_engine, n.drain_status, k.instance_id IS NOT NULL
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor)
    .await?;
    Ok(
        row.map(|(engine, status, recorded_instance)| DrainV2Columns {
            engine: NodeDrainEngine::from_column(engine.as_deref()),
            status,
            recorded_instance,
        }),
    )
}

/// Start a v2 drain on a node `update_drain_phase` has just moved to
/// `draining` in this transaction: record the engine and the epoch the node
/// is sealed under, the drain's start in microseconds. A later drain starts
/// later, so its epoch is larger: the node takes epochs only forward. The
/// drain starts waiting for one of the cluster's evacuation slots
/// (`waiting`), which the controller's v2 loop admits it to before it seals
/// the node; until then the drain gate leaves the node's sandboxes open. A
/// drain started over the cancel of an admitted one that hasn't finished
/// starts admitted instead, in the slot that cancel holds: the node may
/// still be sealed under the cancelled epoch, finishing its suspends, and
/// this drain's first call takes over the seal and what it evacuated.
///
/// `drain_engine` is bound from [`NodeDrainEngine`], the one closed set of
/// engines: the column has no CHECK, so no statement spells a value itself.
///
/// A gentle drain, with `gentle_deadline`, records its epoch as
/// `gentle_epoch` rather than `epoch`, and the deadline, in Unix seconds:
/// a cluster controller that predates gentle drains reads no epoch, so it
/// never calls the node for one, which it would drain forced. The newer
/// controller moves the epoch to `epoch` when the drain turns forced or
/// reaches `drained`.
pub async fn start_v2_drain(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    gentle_deadline: Option<i64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_engine = $3::TEXT,
                drain_status = jsonb_build_object(
                    CASE WHEN $2::BIGINT IS NULL THEN 'epoch' ELSE 'gentle_epoch' END,
                    (EXTRACT(EPOCH FROM drain_started_at) * 1000000)::BIGINT,
                    'waiting', NOT COALESCE(
                        drain_engine = 'v2'
                            AND (drain_status->>'cancel')::BOOLEAN
                            AND NOT COALESCE((drain_status->>'waiting')::BOOLEAN, FALSE),
                        FALSE
                    )
                ) || CASE
                    WHEN $2::BIGINT IS NULL THEN '{}'::JSONB
                    ELSE jsonb_build_object('deadline', $2::BIGINT)
                END
          WHERE id = $1 AND drain_phase = 'draining' AND drain_started_at IS NOT NULL"
    ))
    .bind(node_id)
    .bind(gentle_deadline)
    .bind(NodeDrainEngine::V2.column())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Mark an undrained v2 node's epoch for cancel. The controller cancels it on
/// the node, then clears the drain columns.
pub async fn mark_v2_drain_cancel(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('cancel', TRUE)
          WHERE id = $1 AND drain_engine = 'v2' AND drain_status IS NOT NULL"
    ))
    .bind(node_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Whether a v2 node's `drain_status` fences its instance for termination:
/// `terminate_instance`, or the `terminated` marker an earlier build's
/// terminator wrote once it acted on the node, which the controller reads
/// as a fence already dispatched.
pub fn drain_v2_fenced(status: Option<&serde_json::Value>) -> bool {
    status.is_some_and(|status| {
        status.get("terminate_instance").is_some()
            || status
                .get("terminated")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
    })
}

/// The drain's epoch, when a v2 node's stored report lets it be terminated:
/// the node reported `done` under the epoch its drain is sealed under.
pub fn drain_v2_terminable(status: Option<&serde_json::Value>) -> Option<u64> {
    let status = status?;
    let epoch = status.get("epoch").and_then(serde_json::Value::as_u64)?;
    let report = status.get("report")?;
    let sealed_epoch = report
        .get("sealed_epoch")
        .and_then(serde_json::Value::as_u64);
    let done = report.get("done").and_then(serde_json::Value::as_bool);
    (sealed_epoch == Some(epoch) && done == Some(true)).then_some(epoch)
}

/// Whether the v2 drain of the sandbox's resolved node
/// (`COALESCE(target_node_id, active_node_id)`) still works on it: its last
/// report lists the sandbox pending, or evacuated but not yet recorded.
pub async fn v2_drain_still_making_durable(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
           SELECT 1
             FROM sandboxes s
             JOIN nodes n ON n.id = COALESCE(s.target_node_id, s.active_node_id)
            WHERE s.id = $1
              AND n.drain_engine = 'v2'
              AND n.drain_phase = 'draining'
              AND (n.drain_status->'report'->'pending'
                     @> jsonb_build_array(jsonb_build_object('sandbox_id', s.id::TEXT))
                   OR n.drain_status->'report'->'unrecorded'
                     @> jsonb_build_array(jsonb_build_object('sandbox_id', s.id::TEXT))))"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await
}

/// A gentle v2 drain's deadline, `drain_status.deadline`, in Unix seconds.
pub fn drain_v2_deadline(status: Option<&serde_json::Value>) -> Option<i64> {
    status
        .and_then(|status| status.get("deadline"))
        .and_then(serde_json::Value::as_i64)
}
