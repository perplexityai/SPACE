//! The v2 node drain's columns of `nodes`: `drain_engine`, and the
//! `drain_status` JSON a v2 drain keeps its epoch, report and fence in.
//! Every statement and SQL fragment over them is here, so the cluster
//! controller's v2 loop, the admin API, the drain gates and registration
//! read and write them one way.

use pplx_space_accounts::NodeId;
use pplx_space_node_drain::NodeDrainEngine;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};

/// SQL predicate over the `nodes` row aliased `n`: its v2 drain fenced its
/// instance for termination, `drain_status.terminate_instance`, or an earlier
/// build's terminator acted on it, `drain_status.terminated`.
pub const FENCED: &str = "(n.drain_status ? 'terminate_instance'
     OR COALESCE((n.drain_status->>'terminated')::BOOLEAN, FALSE))";

/// SQL expression over a `nodes` row, unqualified: the epoch the row's v2
/// drain is sealed under, `drain_status.epoch`, or `gentle_epoch` while it
/// runs gently.
const EPOCH: &str = "COALESCE(drain_status->>'epoch', drain_status->>'gentle_epoch')::BIGINT";

/// SQL expression over the `nodes` row aliased `n`: the instance its v2
/// drain fenced, `drain_status.terminate_instance`. A row an earlier build's
/// terminator acted on carries `drain_status.terminated` instead, and is
/// fenced on its recorded instance, the one that terminator terminated.
pub const FENCE: &str = "COALESCE(
    n.drain_status->>'terminate_instance',
    CASE WHEN COALESCE((n.drain_status->>'terminated')::BOOLEAN, FALSE)
         THEN (SELECT nk.instance_id FROM node_k8s nk WHERE nk.node_id = n.id)
    END
)";

/// SQL expression over the `nodes` row aliased `n`: Auto Scaling accepted
/// the termination of its fenced instance, `drain_status.dispatched_at`, or
/// an earlier build's terminator acted on it, `drain_status.terminated`.
pub const DISPATCHED: &str = "COALESCE(
    n.drain_status ? 'dispatched_at' OR (n.drain_status->>'terminated')::BOOLEAN,
    FALSE
)";

/// One v2 node as [`list_v2_nodes`] reads it.
#[derive(sqlx::FromRow)]
pub struct V2NodeRow {
    pub id: NodeId,
    pub drain_phase: Option<String>,
    pub epoch: Option<i64>,
    pub gentle: bool,
    pub mode_forced: bool,
    pub deadline: Option<i64>,
    pub deadline_due: bool,
    pub cancel: bool,
    pub waiting: bool,
    pub report: Option<serde_json::Value>,
    pub k8s_node_name: Option<String>,
    pub k8s_node_uid: Option<String>,
    pub instance_id: Option<String>,
    pub heartbeat_fresh: bool,
    pub fence: Option<String>,
    pub dispatched: bool,
    pub dispatched_at: Option<i64>,
    pub terminated_observed_at: Option<i64>,
    pub decrement_capacity: bool,
}

/// Every v2 drain's node of the Kubernetes substrate, with its Kubernetes
/// identity, recorded instance and heartbeat. A gentle drain's epoch is
/// `gentle_epoch` until it turns forced or reaches `drained`. Whether its
/// deadline is due is read by the database clock, the one the escalation's
/// statement reads.
static LIST_V2_NODES: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let fresh = pplx_space_sandbox_residency::node_heartbeat_fresh("n.id");
    sql!(format!(
        "SELECT n.id, n.drain_phase,
                {EPOCH} AS epoch,
                COALESCE(n.drain_status ? 'gentle_epoch', FALSE) AS gentle,
                COALESCE(n.drain_mode = 'forced', FALSE) AS mode_forced,
                (n.drain_status->>'deadline')::BIGINT AS deadline,
                COALESCE(
                    (n.drain_status->>'deadline')::BIGINT <= EXTRACT(EPOCH FROM NOW()),
                    FALSE
                ) AS deadline_due,
                COALESCE((n.drain_status->>'cancel')::BOOLEAN, FALSE) AS cancel,
                COALESCE((n.drain_status->>'waiting')::BOOLEAN, FALSE) AS waiting,
                n.drain_status->'report' AS report,
                k.k8s_node_name, k.k8s_node_uid, k.instance_id,
                {fresh} AS heartbeat_fresh,
                {FENCE} AS fence,
                {DISPATCHED} AS dispatched,
                (n.drain_status->>'dispatched_at')::BIGINT AS dispatched_at,
                (n.drain_status->>'terminated_observed_at')::BIGINT AS terminated_observed_at,
                n.terminate_decrement_capacity AS decrement_capacity
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

/// The locked node's v2 state, as the terminate transaction and the
/// delete read it under the row lock.
#[derive(sqlx::FromRow)]
pub struct LockedV2Node {
    pub drain_engine: Option<String>,
    pub drain_phase: Option<String>,
    pub epoch: Option<i64>,
    pub report_done: bool,
    pub fence: Option<String>,
    pub dispatched: bool,
    pub terminated_observed: bool,
    pub k8s_node_name: Option<String>,
    pub instance_id: Option<String>,
}

/// The locked node's v2 state, by its id `$1`.
static LOCKED_V2_NODE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "SELECT n.drain_engine, n.drain_phase,
                (n.drain_status->>'epoch')::BIGINT AS epoch,
                COALESCE(
                    (n.drain_status->'report'->>'sealed_epoch')::BIGINT
                        = (n.drain_status->>'epoch')::BIGINT
                    AND (n.drain_status->'report'->>'done')::BOOLEAN,
                    FALSE
                ) AS report_done,
                {FENCE} AS fence,
                {DISPATCHED} AS dispatched,
                COALESCE(n.drain_status ? 'terminated_observed_at', FALSE) AS terminated_observed,
                k.k8s_node_name, k.instance_id
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.id = $1"
    ))
});

/// Read the v2 state of `node_id`'s row, which the caller has locked
/// `FOR UPDATE`; `None` when the row is gone.
pub async fn read_locked_v2_node(
    conn: &mut PgConnection,
    node_id: NodeId,
) -> Result<Option<LockedV2Node>, sqlx::Error> {
    sqlx::query_as(sqlx::AssertSqlSafe(LOCKED_V2_NODE.as_str()))
        .bind(node_id)
        .fetch_optional(conn)
        .await
}

impl LockedV2Node {
    pub fn is_v2(&self, phase: &str, epoch: i64) -> bool {
        self.drain_engine.as_deref() == Some("v2")
            && self.drain_phase.as_deref() == Some(phase)
            && self.epoch == Some(epoch)
    }
}

/// Record what holds the locked node from its terminate transaction's fence
/// in `drain_last_error`.
pub async fn record_terminate_held(
    conn: &mut PgConnection,
    node_id: NodeId,
    held: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!("UPDATE nodes SET drain_last_error = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(held)
        .execute(conn)
        .await?;
    Ok(())
}

/// Write the locked node's fence, `drain_status.terminate_instance`, the
/// instance its v2 drain terminates, and clear its last error.
pub async fn write_fence(
    conn: &mut PgConnection,
    node_id: NodeId,
    instance_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('terminate_instance', $2::TEXT),
                drain_last_error = NULL
          WHERE id = $1"
    ))
    .bind(node_id)
    .bind(instance_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// Record that Auto Scaling accepted the termination of the fenced
/// `instance_id`, by the database clock in Unix seconds, so the loop never
/// dispatches it again. False for a row that is no longer that epoch's
/// `terminating` node fenced on that instance with no dispatch recorded.
pub async fn record_v2_dispatched(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
    instance_id: &str,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status
                    || jsonb_build_object('dispatched_at', EXTRACT(EPOCH FROM NOW())::BIGINT),
                drain_last_error = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'terminating'
            AND (drain_status->>'epoch')::BIGINT = $2
            AND drain_status->>'terminate_instance' = $3
            AND NOT drain_status ? 'dispatched_at'"
    ))
    .bind(node_id)
    .bind(epoch)
    .bind(instance_id)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

/// `$1` = node id, `$2` = epoch, `$3` = fenced instance. Records, by the
/// database clock in Unix seconds, that EC2 listed the fenced instance
/// `terminated`, keeping the first time.
static RECORD_TERMINATED_OBSERVED: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes n
                SET drain_status = n.drain_status || jsonb_build_object(
                        'terminated_observed_at',
                        COALESCE(
                            n.drain_status->'terminated_observed_at',
                            to_jsonb(EXTRACT(EPOCH FROM NOW())::BIGINT)
                        )
                    )
              WHERE n.id = $1
                AND n.drain_engine = 'v2'
                AND n.drain_phase = 'terminating'
                AND (n.drain_status->>'epoch')::BIGINT = $2
                AND {FENCE} = $3"
    ))
});

/// Record that EC2 listed the fenced `instance_id` `terminated`: EC2 stops
/// listing a terminated instance after a while, and the delete of the row
/// needs this record when it no longer does. Keeps the first time. False
/// for a row that is no longer that epoch's `terminating` node fenced on
/// that instance.
pub async fn record_v2_terminated_observed(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
    instance_id: &str,
) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(RECORD_TERMINATED_OBSERVED.as_str()))
            .bind(node_id)
            .bind(epoch)
            .bind(instance_id)
            .execute(pool)
            .await?
            .rows_affected()
            == 1,
    )
}

/// `$1` = node id, `$2` = epoch, `$3` = phase, `$4` = fence, `$5` = trace.
/// The let-go statement.
static LET_GO_V2_NODE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes n
            SET drain_phase = NULL,
                drain_mode = NULL,
                drain_started_at = NULL,
                drain_engine = NULL,
                drain_status = NULL,
                drain_last_error = $5
          WHERE n.id = $1
            AND n.drain_engine = 'v2'
            AND {EPOCH} = $2
            AND n.drain_phase IS NOT DISTINCT FROM $3::TEXT
            AND {FENCE} IS NOT DISTINCT FROM $4::TEXT"
    ))
});

/// The let-go statement: clear the phase and the v2 columns of the v2 node
/// under `epoch` in `phase` with fence `fence`, as the caller read them,
/// and record `trace`. Writes no sandbox row. Returns whether it matched.
pub async fn let_go_v2_node(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    epoch: i64,
    phase: Option<&str>,
    fence: Option<&str>,
    trace: &str,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(LET_GO_V2_NODE.as_str()))
        .bind(node_id)
        .bind(epoch)
        .bind(phase)
        .bind(fence)
        .bind(trace)
        .execute(executor)
        .await?
        .rows_affected()
        == 1)
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
            SET drain_phase = 'drained',
                drain_status = CASE
                    WHEN drain_status ? 'gentle_epoch'
                    THEN (drain_status - 'gentle_epoch') || jsonb_build_object('epoch', $2)
                    ELSE drain_status
                END
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'draining'
            AND {EPOCH} = $2
            AND (drain_status->'report'->>'sealed_epoch')::BIGINT = $2
            AND COALESCE((drain_status->'report'->>'done')::BOOLEAN, FALSE)"
    ))
});

/// `draining -> drained` for the v2 drain under `epoch`, only with a stored
/// report that is `done` under that same epoch. A gentle drain's epoch moves
/// to `epoch`: a `drained` node holds nothing, so from here on every reader,
/// a controller that predates gentle drains included, drains it as a forced
/// one.
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

/// Turn the gentle `draining` drain under `epoch` forced, once its deadline
/// has passed by the database clock or an operator has set `drain_mode`
/// forced: `drain_mode` forced, the epoch moved from `gentle_epoch` to
/// `epoch`, and `escalated` (`deadline` or `operator`) with `escalated_at`, in
/// Unix seconds. Returns false for a row that is no longer that epoch's
/// gentle `draining` drain due to turn.
pub async fn escalate_v2_drain(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_mode = 'forced',
                drain_status = (drain_status - 'gentle_epoch') || jsonb_build_object(
                    'epoch', $2,
                    'escalated', CASE WHEN drain_mode = 'forced' THEN 'operator' ELSE 'deadline' END,
                    'escalated_at', EXTRACT(EPOCH FROM NOW())::BIGINT
                )
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'draining'
            AND (drain_status->>'gentle_epoch')::BIGINT = $2
            AND (drain_mode = 'forced'
                 OR (drain_status->>'deadline')::BIGINT <= EXTRACT(EPOCH FROM NOW()))"
    ))
    .bind(node_id)
    .bind(epoch)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
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
