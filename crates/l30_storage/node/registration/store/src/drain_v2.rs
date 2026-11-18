//! The v2 node drain's columns of `nodes`: `drain_engine`, and the
//! `drain_status` JSON a v2 drain keeps its epoch and report in. Every
//! statement and SQL fragment over them is here, so every reader and writer
//! reads and writes them one way.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::SubstrateKind;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

/// SQL expression over a `nodes` row, unqualified: the epoch the row's v2
/// drain is sealed under, `drain_status.epoch`.
const EPOCH: &str = "(drain_status->>'epoch')::BIGINT";

/// One v2 node as [`list_v2_nodes`] reads it.
#[derive(sqlx::FromRow)]
pub struct V2NodeRow {
    pub id: NodeId,
    pub drain_phase: Option<String>,
    pub epoch: Option<i64>,
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
