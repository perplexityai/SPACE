//! The v2 node drain's columns of `nodes`: `drain_engine`, and the
//! `drain_status` JSON a v2 drain keeps its epoch and report in. Every
//! statement and SQL fragment over them is here, so the cluster
//! controller's v2 loop, the admin API and the drain gates read and write
//! them one way.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::{NodeDrainPhase, NodeDrainState, SandboxId, SubstrateKind};
use pplx_space_node_drain::NodeDrainEngine;
use pplx_space_sql_locator::sql;
use sqlx::{PgPool, Postgres, Transaction};

/// SQL expression over the `nodes` row aliased `n`, the one column the drain
/// gates read beside `drain_phase` and `drain_mode`: NULL unless a v2 drain
/// runs on the node (`drain_engine` is NULL for every other row), else
/// whether it still waits for an evacuation slot (`drain_status.waiting`).
/// [`gate_drain_state`] reads the three.
pub const GATE_COLUMN: &str = "CASE WHEN n.drain_engine = 'v2'
    THEN COALESCE((n.drain_status->>'waiting')::BOOLEAN, FALSE)
END";

/// [`GATE_COLUMN`] over a node's `drain_engine` and `drain_status` as read.
pub fn gate_column(engine: Option<&str>, status: Option<&serde_json::Value>) -> Option<bool> {
    (NodeDrainEngine::from_column(engine) == NodeDrainEngine::V2).then(|| {
        status
            .and_then(|status| status.get("waiting"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    })
}

/// The drain state the gates act on, from a node's `drain_phase`,
/// `drain_mode` and [`GATE_COLUMN`]: the engine is v2 where the column is
/// not NULL, and a v2 drain waits for its slot where it is true. `None` when
/// the node has no drain phase.
pub fn gate_drain_state(
    phase: Option<NodeDrainPhase>,
    mode: Option<&str>,
    v2_waiting: Option<bool>,
) -> Option<NodeDrainState> {
    let engine = if v2_waiting.is_some() {
        NodeDrainEngine::V2
    } else {
        NodeDrainEngine::V1
    };
    NodeDrainState::from_columns(phase, mode, engine)
        .map(|state| state.awaiting_evacuation(v2_waiting.unwrap_or(false)))
}

/// SQL expression over a `nodes` row, unqualified: the epoch the row's v2
/// drain is sealed under, `drain_status.epoch`.
const EPOCH: &str = "(drain_status->>'epoch')::BIGINT";

/// One v2 node as [`list_v2_nodes`] reads it.
#[derive(sqlx::FromRow)]
pub struct V2NodeRow {
    pub id: NodeId,
    pub drain_phase: Option<String>,
    pub epoch: Option<i64>,
    pub cancel: bool,
    pub waiting: bool,
    pub report: Option<serde_json::Value>,
    pub instance_id: Option<String>,
    pub heartbeat_fresh: bool,
}

/// Every v2 drain's node of the Kubernetes substrate, with its Kubernetes
/// identity, recorded instance and heartbeat.
static LIST_V2_NODES: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let fresh = pplx_space_sandbox_residency::node_heartbeat_fresh("n.id");
    sql!(format!(
        "SELECT n.id, n.drain_phase,
                {EPOCH} AS epoch,
                COALESCE((n.drain_status->>'cancel')::BOOLEAN, FALSE) AS cancel,
                COALESCE((n.drain_status->>'waiting')::BOOLEAN, FALSE) AS waiting,
                n.drain_status->'report' AS report,
                k.instance_id,
                {fresh} AS heartbeat_fresh
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.drain_engine = 'v2' AND n.managed_by = $1
          ORDER BY n.id"
    ))
});

/// Every v2 drain's node of the Kubernetes substrate, as [`LIST_V2_NODES`]
/// reads it.
pub async fn list_v2_node_rows(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<V2NodeRow>, sqlx::Error> {
    sqlx::query_as(sqlx::AssertSqlSafe(LIST_V2_NODES.as_str()))
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(executor)
        .await
}

static ADMIT_V2_EVACUATION_STATEMENT: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| {
        sql!(format!(
            "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('waiting', FALSE)
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'draining'
            AND {EPOCH} = $2
            AND COALESCE((drain_status->>'waiting')::BOOLEAN, FALSE)"
        ))
    });

/// Admit the waiting v2 drain under `epoch` to an evacuation slot. Returns
/// whether it was admitted: false for a row no longer that epoch's waiting
/// `draining` node.
pub async fn admit_v2_evacuation(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(ADMIT_V2_EVACUATION_STATEMENT.as_str()))
            .bind(node_id)
            .bind(epoch)
            .execute(pool)
            .await?
            .rows_affected()
            == 1,
    )
}

static RECORD_V2_REPORT_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('report', $3::JSONB),
                drain_last_error = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND {EPOCH} = $2"
    ))
});

/// Store the node's report under `epoch`. Returns false when the row is no
/// longer that epoch's v2 drain.
pub async fn record_v2_report(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
    report: serde_json::Value,
) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(RECORD_V2_REPORT_STATEMENT.as_str()))
            .bind(node_id)
            .bind(epoch)
            .bind(report)
            .execute(pool)
            .await?
            .rows_affected()
            == 1,
    )
}

static ADVANCE_V2_DRAINED_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes
            SET drain_phase = 'drained'
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'draining'
            AND {EPOCH} = $2
            AND (drain_status->'report'->>'sealed_epoch')::BIGINT = $2
            AND COALESCE((drain_status->'report'->>'done')::BOOLEAN, FALSE)"
    ))
});

/// `draining -> drained` for the v2 drain under `epoch`, only with a stored
/// report that is `done` under that same epoch.
pub async fn advance_v2_drained(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(ADVANCE_V2_DRAINED_STATEMENT.as_str()))
            .bind(node_id)
            .bind(epoch)
            .execute(pool)
            .await?
            .rows_affected()
            == 1,
    )
}

static FINISH_V2_CANCEL_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes
            SET drain_engine = NULL,
                drain_status = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase IS NULL
            AND {EPOCH} = $2
            AND COALESCE((drain_status->>'cancel')::BOOLEAN, FALSE)"
    ))
});

/// Clear an undrained v2 row's columns once its epoch `epoch` is cancelled on
/// the node.
pub async fn finish_v2_cancel(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(FINISH_V2_CANCEL_STATEMENT.as_str()))
            .bind(node_id)
            .bind(epoch)
            .execute(pool)
            .await?
            .rows_affected()
            == 1,
    )
}

pub async fn record_v2_error(
    pool: &PgPool,
    node_id: NodeId,
    message: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE nodes SET drain_last_error = $2 WHERE id = $1 AND drain_engine = 'v2'"
    ))
    .bind(node_id)
    .bind(message)
    .execute(pool)
    .await?;
    Ok(())
}

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
pub async fn start_v2_drain(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_engine = $2::TEXT,
                drain_status = jsonb_build_object(
                    'epoch', (EXTRACT(EPOCH FROM drain_started_at) * 1000000)::BIGINT,
                    'waiting', NOT COALESCE(
                        drain_engine = 'v2'
                            AND (drain_status->>'cancel')::BOOLEAN
                            AND NOT COALESCE((drain_status->>'waiting')::BOOLEAN, FALSE),
                        FALSE
                    )
                )
          WHERE id = $1 AND drain_phase = 'draining' AND drain_started_at IS NOT NULL"
    ))
    .bind(node_id)
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
