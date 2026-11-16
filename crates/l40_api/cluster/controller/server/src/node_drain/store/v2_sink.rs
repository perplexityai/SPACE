//! [`DrainV2Sink`] for the production sink: the v2 loop's statements over
//! `nodes`, its evacuation record, and its node-api calls.

use std::str::FromStr;

use pplx_space_accounts::OrgId;
use pplx_space_api::controllers::substrate::connect_internal_node_api_client_for_target;
use pplx_space_api::drain_restore_operations::{
    DrainRestoreOperationPayload, enqueue_drain_restore_once,
};
use pplx_space_sandbox_store::drain_restore::V2StoppedEvacuation;
use pplx_space_sandbox_store::sweeper::EvacuationRestoreTarget;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPrior,
};
use uuid::Uuid;

use super::*;
use crate::node_drain::guarded_delete::{self, GuardedDelete, NodeIdentity};
use crate::node_drain::v2::{DrainV2Sink, EvacuationRecord, Fence, TerminatedDelete, V2Node};

/// How long one `DrainNode` call may take; the node answers without waiting.
const DRAIN_NODE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the direct Kubernetes Node GET may take.
const NODE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long an active execution owns a restore: the drain restore claim's
/// window, which the release reads as a restore in flight.
fn execution_ownership_ttl_secs() -> i64 {
    i64::try_from(
        pplx_space_common_core::restore_ownership::RESTORE_EXECUTION_OWNERSHIP_TTL.as_secs(),
    )
    .unwrap_or(i64::MAX)
}

/// One v2 node as [`list_v2_nodes`] reads it.
#[derive(sqlx::FromRow)]
struct V2NodeRow {
    id: NodeId,
    drain_phase: Option<String>,
    epoch: Option<i64>,
    cancel: bool,
    waiting: bool,
    unrecorded: Option<serde_json::Value>,
    k8s_node_name: Option<String>,
    k8s_node_uid: Option<String>,
    instance_id: Option<String>,
    heartbeat_fresh: bool,
    fence: Option<String>,
    dispatched_at: Option<i64>,
    decrement_capacity: bool,
}

/// Every v2 drain's node of the Kubernetes substrate, with its Kubernetes
/// identity, recorded instance and heartbeat.
static LIST_V2_NODES: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let fresh = pplx_space_sandbox_residency::node_heartbeat_fresh("n.id");
    sql!(format!(
        "SELECT n.id, n.drain_phase,
                (n.drain_status->>'epoch')::BIGINT AS epoch,
                COALESCE((n.drain_status->>'cancel')::BOOLEAN, FALSE) AS cancel,
                COALESCE((n.drain_status->>'waiting')::BOOLEAN, FALSE) AS waiting,
                n.drain_status->'report'->'unrecorded' AS unrecorded,
                k.k8s_node_name, k.k8s_node_uid, k.instance_id,
                {fresh} AS heartbeat_fresh,
                n.drain_status->>'terminate_instance' AS fence,
                (n.drain_status->>'dispatched_at')::BIGINT AS dispatched_at,
                n.terminate_decrement_capacity AS decrement_capacity
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.drain_engine = 'v2' AND n.managed_by = $1
          ORDER BY n.id"
    ))
});

pub(crate) async fn list_v2_nodes(pool: &PgPool) -> Result<Vec<V2Node>, sqlx::Error> {
    let rows: Vec<V2NodeRow> = sqlx::query_as(sqlx::AssertSqlSafe(LIST_V2_NODES.as_str()))
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| V2Node {
            id: row.id,
            phase: row.drain_phase,
            epoch: row.epoch.and_then(|epoch| u64::try_from(epoch).ok()),
            cancel: row.cancel,
            waiting: row.waiting,
            unrecorded_since: unrecorded_since(row.unrecorded.as_ref()),
            k8s_node_name: row.k8s_node_name,
            k8s_node_uid: row.k8s_node_uid,
            instance_id: row.instance_id,
            heartbeat_fresh: row.heartbeat_fresh,
            fence: row.fence,
            dispatched_at: row.dispatched_at,
            decrement_capacity: row.decrement_capacity,
        })
        .collect())
}

/// Since when each unrecorded evacuation of a stored report has been. An
/// entry without a `since`, written before the report carried one, starts
/// its clock again.
fn unrecorded_since(
    unrecorded: Option<&serde_json::Value>,
) -> std::collections::BTreeMap<String, i64> {
    unrecorded
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            Some((
                entry.get("sandbox_id")?.as_str()?.to_owned(),
                entry.get("since")?.as_i64()?,
            ))
        })
        .collect()
}

/// Admit the waiting v2 drain under `epoch` to an evacuation slot. Returns
/// whether it was admitted: false for a row no longer that epoch's waiting
/// `draining` node.
pub(crate) async fn admit_v2_evacuation(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('waiting', FALSE)
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'draining'
            AND (drain_status->>'epoch')::BIGINT = $2
            AND COALESCE((drain_status->>'waiting')::BOOLEAN, FALSE)"
    ))
    .bind(node_id)
    .bind(epoch)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

/// The locked node's v2 state, as the terminate transaction and the
/// delete read it under the row lock.
#[derive(sqlx::FromRow)]
struct LockedV2Node {
    drain_engine: Option<String>,
    drain_phase: Option<String>,
    epoch: Option<i64>,
    report_done: bool,
    fence: Option<String>,
    dispatched: bool,
    k8s_node_name: Option<String>,
    instance_id: Option<String>,
}

/// Lock `node_id`'s row `FOR UPDATE` and read its v2 state; `None` when the
/// row is gone.
async fn lock_v2_node(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
) -> Result<Option<LockedV2Node>, sqlx::Error> {
    if !guarded_delete::lock_node(tx, node_id).await? {
        return Ok(None);
    }
    sqlx::query_as(sql!(
        "SELECT n.drain_engine, n.drain_phase,
                (n.drain_status->>'epoch')::BIGINT AS epoch,
                COALESCE(
                    (n.drain_status->'report'->>'sealed_epoch')::BIGINT
                        = (n.drain_status->>'epoch')::BIGINT
                    AND (n.drain_status->'report'->>'done')::BOOLEAN,
                    FALSE
                ) AS report_done,
                n.drain_status->>'terminate_instance' AS fence,
                COALESCE(n.drain_status ? 'dispatched_at', FALSE) AS dispatched,
                k.k8s_node_name, k.instance_id
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await
}

impl LockedV2Node {
    fn is_v2(&self, phase: &str, epoch: i64) -> bool {
        self.drain_engine.as_deref() == Some("v2")
            && self.drain_phase.as_deref() == Some(phase)
            && self.epoch == Some(epoch)
    }
}

/// Release the durably suspended rows that name the `drained` v2 node under
/// `epoch`, in one transaction under the node's row lock, while its report
/// is `done` under that epoch. Returns the ids released; none for a row
/// that is no longer that epoch's `drained` node with its report `done`.
pub(crate) async fn release_v2_suspended(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(node) = lock_v2_node(&mut tx, node_id).await? else {
        return Ok(Vec::new());
    };
    if !node.is_v2("drained", epoch) || !node.report_done {
        return Ok(Vec::new());
    }
    let released = pplx_space_sandbox_store::drain_terminate::release_durable_suspended(
        &mut tx,
        node_id,
        node.k8s_node_name.as_deref(),
        execution_ownership_ttl_secs(),
    )
    .await?;
    tx.commit().await?;
    Ok(released)
}

/// The terminate transaction of the v2 node under `epoch`, under its row
/// lock: it must still be that epoch's `terminating` node, unfenced, with
/// its report `done`; the durably suspended rows naming it are released
/// again; nothing may name it any more (the require-empty predicate,
/// [`pplx_space_sandbox_store::drain_terminate::node_references`]); and then
/// `drain_status.terminate_instance` records `node_k8s.instance_id`, the
/// instance to terminate. A refusal of the predicate records what holds the
/// node in `drain_last_error` and commits the release.
pub(crate) async fn fence_v2_node(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<Fence, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(node) = lock_v2_node(&mut tx, node_id).await? else {
        return Ok(Fence::Moved);
    };
    if !node.is_v2("terminating", epoch) || node.fence.is_some() || !node.report_done {
        return Ok(Fence::Moved);
    }
    let Some(instance_id) = node.instance_id else {
        return Ok(Fence::NoInstance);
    };
    pplx_space_sandbox_store::drain_terminate::release_durable_suspended(
        &mut tx,
        node_id,
        node.k8s_node_name.as_deref(),
        execution_ownership_ttl_secs(),
    )
    .await?;
    let references = pplx_space_sandbox_store::drain_terminate::node_references(
        &mut *tx,
        node_id,
        node.k8s_node_name.as_deref(),
        guarded_delete::REFERENCE_SAMPLE,
    )
    .await?;
    if !references.is_empty() {
        let held = format!("terminate held: {references}");
        sqlx::query(sql!("UPDATE nodes SET drain_last_error = $2 WHERE id = $1"))
            .bind(node_id)
            .bind(&held)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(Fence::Held(held));
    }
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('terminate_instance', $2::TEXT),
                drain_last_error = NULL
          WHERE id = $1"
    ))
    .bind(node_id)
    .bind(&instance_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Fence::Fenced(instance_id))
}

/// Record that Auto Scaling accepted the termination of the fenced
/// `instance_id`, by the database clock in Unix seconds, so the loop never
/// dispatches it again. False for a row that is no longer that epoch's
/// `terminating` node fenced on that instance with no dispatch recorded.
pub(crate) async fn record_v2_dispatched(
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

/// The let-go statement: clear the phase and the v2 columns of the v2 node
/// under `epoch` in `phase` with fence `fence`, as the caller read them,
/// and record `trace`. Writes no sandbox row. Returns whether it matched.
async fn let_go_v2_node(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    epoch: i64,
    phase: Option<&str>,
    fence: Option<&str>,
    trace: &str,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_phase = NULL,
                drain_mode = NULL,
                drain_started_at = NULL,
                drain_engine = NULL,
                drain_status = NULL,
                drain_last_error = $5
          WHERE id = $1
            AND drain_engine = 'v2'
            AND (drain_status->>'epoch')::BIGINT = $2
            AND drain_phase IS NOT DISTINCT FROM $3::TEXT
            AND drain_status->>'terminate_instance' IS NOT DISTINCT FROM $4::TEXT"
    ))
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

/// Let go of the v2 node under `epoch`, in the phase and with the fence
/// the tick read, under its row lock.
pub(crate) async fn let_go_v2(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
    phase: Option<&str>,
    fence: Option<&str>,
    trace: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !guarded_delete::lock_node(&mut tx, node_id).await? {
        return Ok(false);
    }
    let cleared = let_go_v2_node(&mut *tx, node_id, epoch, phase, fence, trace).await?;
    tx.commit().await?;
    Ok(cleared)
}

/// Delete the row of the `terminating` v2 node under `epoch` whose fenced
/// instance `fence` EC2 says is terminated, its Node gone: under the row
/// lock, the row must still carry that fence with a dispatch recorded and
/// record that instance; then the guarded delete runs, holding the uid the
/// tick read, a stale heartbeat and that nothing names the node
/// ([`guarded_delete::delete_locked_node`]). A row or execution guard that
/// refuses lets the node go in the same transaction instead.
pub(crate) async fn delete_v2_terminated(
    pool: &PgPool,
    node: &V2Node,
    epoch: i64,
    fence: &str,
) -> Result<TerminatedDelete, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(locked) = lock_v2_node(&mut tx, node.id).await? else {
        return Ok(TerminatedDelete::Wait("the row is gone".to_owned()));
    };
    if locked.fence.as_deref() != Some(fence) {
        return Ok(if locked.fence.is_none() {
            TerminatedDelete::Unfenced
        } else {
            TerminatedDelete::Wait("the row carries another fence".to_owned())
        });
    }
    if !locked.is_v2("terminating", epoch) || !locked.dispatched {
        return Ok(TerminatedDelete::Wait(
            "the row is no longer the epoch's dispatched terminating node".to_owned(),
        ));
    }
    let deleted = guarded_delete::delete_locked_node(
        &mut tx,
        &NodeIdentity {
            node_id: node.id,
            managed_by: SubstrateKind::Kubernetes,
            k8s_node_name: locked.k8s_node_name.as_deref(),
            k8s_node_uid: node.k8s_node_uid.as_deref(),
            instance_id: Some(fence),
        },
    )
    .await?;
    match deleted {
        GuardedDelete::Deleted => {
            tx.commit().await?;
            Ok(TerminatedDelete::Deleted)
        }
        GuardedDelete::Referenced(references) => {
            let trace = format!("v2 drain let go after terminating {fence}: {references}");
            let_go_v2_node(
                &mut *tx,
                node.id,
                epoch,
                Some("terminating"),
                Some(fence),
                &trace,
            )
            .await?;
            tx.commit().await?;
            Ok(TerminatedDelete::LetGo(trace))
        }
        GuardedDelete::HeartbeatFresh => Ok(TerminatedDelete::Wait(
            "the node's heartbeat is fresh".to_owned(),
        )),
        GuardedDelete::IdentityMoved => Ok(TerminatedDelete::Wait(
            "the node's Kubernetes uid or recorded instance moved".to_owned(),
        )),
    }
}

/// Store the node's report under `epoch`. Returns false when the row is no
/// longer that epoch's v2 drain.
pub(crate) async fn record_v2_report(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
    report: serde_json::Value,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('report', $3::JSONB),
                drain_last_error = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND (drain_status->>'epoch')::BIGINT = $2"
    ))
    .bind(node_id)
    .bind(epoch)
    .bind(report)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

/// `draining -> drained` for the v2 drain under `epoch`, only with a stored
/// report that is `done` under that same epoch.
pub(crate) async fn advance_v2_drained(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_phase = 'drained'
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'draining'
            AND (drain_status->>'epoch')::BIGINT = $2
            AND (drain_status->'report'->>'sealed_epoch')::BIGINT = $2
            AND COALESCE((drain_status->'report'->>'done')::BOOLEAN, FALSE)"
    ))
    .bind(node_id)
    .bind(epoch)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

/// Clear an undrained v2 row's columns once its epoch `epoch` is cancelled on
/// the node.
pub(crate) async fn finish_v2_cancel(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_engine = NULL,
                drain_status = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase IS NULL
            AND (drain_status->>'epoch')::BIGINT = $2
            AND COALESCE((drain_status->>'cancel')::BOOLEAN, FALSE)"
    ))
    .bind(node_id)
    .bind(epoch)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

pub(crate) async fn record_v2_error(
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

/// Record one evacuation the node on `node_id` reported, in one transaction
/// under the sandbox's row lock.
///
/// * **`started` or `paused`**: enqueue the drain restore of `sandbox_id`
///   toward `prior` from the catalog's ready snapshot of `artifact`, at most
///   once per snapshot, once the row is `suspended`. A row still `started`,
///   `paused` or `suspending` on the node isn't projected yet; one that runs
///   on another node, with an active execution there, has moved on.
/// * **`stopped`**: pin the disk and release the node
///   ([`pplx_space_sandbox_store::drain_restore::evacuate_v2_stopped`]),
///   leaving the row `stopped` and unplaced for its next start. A refusal is
///   not yet recorded, never settled.
///
/// A row that no longer needs anything is [`EvacuationRecord::Recorded`].
pub(crate) async fn record_v2_evacuation(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    prior: EvacuationRestoreTarget,
    artifact: Uuid,
) -> Result<EvacuationRecord, String> {
    let sql_error = |error: sqlx::Error| error.to_string();
    let mut tx = pool.begin().await.map_err(sql_error)?;
    if prior == EvacuationRestoreTarget::Stopped {
        let node_name: Option<String> = sqlx::query_scalar(sql!(
            "SELECT k8s_node_name FROM node_k8s WHERE node_id = $1"
        ))
        .bind(node_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(sql_error)?;
        let Some(node_name) = node_name else {
            return Ok(EvacuationRecord::NotYet(
                "the node has no Kubernetes name".to_owned(),
            ));
        };
        let done = pplx_space_sandbox_store::drain_restore::evacuate_v2_stopped(
            &mut tx, sandbox_id, node_id, &node_name, artifact,
        )
        .await
        .map_err(sql_error)?;
        tx.commit().await.map_err(sql_error)?;
        return Ok(match done {
            V2StoppedEvacuation::Released | V2StoppedEvacuation::MovedOn => {
                EvacuationRecord::Recorded
            }
            V2StoppedEvacuation::Refused(reason) => EvacuationRecord::NotYet(reason.to_owned()),
        });
    }
    type Row = (OrgId, String, Option<NodeId>, bool);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT s.org_id, s.lifecycle, s.active_node_id,
                EXISTS (
                  SELECT 1 FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.k8s_node_name IS DISTINCT FROM
                         (SELECT k8s_node_name FROM node_k8s WHERE node_id = $2)
                )
           FROM sandboxes s
          WHERE s.id = $1
          FOR UPDATE OF s"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(sql_error)?;
    let Some((org_id, lifecycle, active_node_id, executes_elsewhere)) = row else {
        return Ok(EvacuationRecord::Recorded);
    };
    match lifecycle.as_str() {
        "suspended" => {}
        "started" | "paused" | "suspending" if active_node_id == Some(node_id) => {
            return Ok(EvacuationRecord::NotYet(
                "the node's suspend isn't projected onto the row yet".to_owned(),
            ));
        }
        // Running again, on an execution elsewhere: a user's call moved it.
        "started" | "paused" | "suspending" if executes_elsewhere => {
            return Ok(EvacuationRecord::Recorded);
        }
        "started" | "paused" | "suspending" => {
            return Ok(EvacuationRecord::NotYet(
                "the row runs nowhere the control plane knows of yet".to_owned(),
            ));
        }
        _ => return Ok(EvacuationRecord::Recorded),
    }
    let artifact = pplx_space_common_core::types::LocalArtifactId::from_uuid(artifact);
    let Some(snapshot) =
        pplx_space_snapshot_store::ready_full_snapshot_of_artifact(&mut *tx, sandbox_id, artifact)
            .await
            .map_err(sql_error)?
    else {
        return Ok(EvacuationRecord::NotYet(
            "the catalog doesn't hold the artifact as ready yet".to_owned(),
        ));
    };
    enqueue_drain_restore_once(
        &mut tx,
        org_id,
        DrainRestoreOperationPayload::new(sandbox_id, prior, snapshot.as_uuid()),
    )
    .await
    .map_err(|error| error.to_string())?;
    tx.commit().await.map_err(sql_error)?;
    Ok(EvacuationRecord::Recorded)
}

fn prior_of(prior: i32) -> Option<EvacuationRestoreTarget> {
    match DrainPrior::try_from(prior).ok()? {
        DrainPrior::Started => Some(EvacuationRestoreTarget::Started),
        DrainPrior::Paused => Some(EvacuationRestoreTarget::Paused),
        DrainPrior::Stopped => Some(EvacuationRestoreTarget::Stopped),
        DrainPrior::Unspecified => None,
    }
}

fn epoch_i64(epoch: u64) -> Result<i64, String> {
    i64::try_from(epoch).map_err(|_| format!("epoch {epoch} is above i64::MAX"))
}

impl DrainV2Sink for SubstrateSink {
    async fn list_v2_nodes(&self) -> Result<Vec<V2Node>, String> {
        list_v2_nodes(&self.pool)
            .await
            .map_err(|error| error.to_string())
    }

    async fn admit_evacuation(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        admit_v2_evacuation(&self.pool, node_id, epoch_i64(epoch)?)
            .await
            .map_err(|error| error.to_string())
    }

    async fn ensure_cordon(&self, node_id: NodeId) -> Result<(), String> {
        NodeDrainSink::ensure_node_unschedulable(self, node_id)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn drain_node(
        &self,
        node_id: NodeId,
        epoch: u64,
        cancel: bool,
        forget: &[String],
    ) -> Result<DrainNodeResponse, String> {
        let target = node_api_row(&self.pool, node_id)
            .await
            .map_err(|error| format!("node endpoint lookup failed: {error}"))?
            .ok_or_else(|| format!("node {node_id} not found"))?;
        let mut client = connect_internal_node_api_client_for_target(
            self.substrate.kubernetes(),
            &target.as_target(),
            DRAIN_NODE_TIMEOUT,
        )
        .await
        .map_err(|error| error.to_string())?;
        client
            .drain_node(DrainNodeRequest {
                epoch,
                cancel,
                forget: forget.to_vec(),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| status.to_string())
    }

    async fn record_evacuation(
        &self,
        node_id: NodeId,
        evacuated: &DrainEvacuated,
    ) -> Result<EvacuationRecord, String> {
        let sandbox_id = SandboxId::from_str(&evacuated.sandbox_id)
            .map_err(|error| format!("invalid sandbox id {}: {error}", evacuated.sandbox_id))?;
        let artifact = Uuid::from_str(&evacuated.artifact_id)
            .map_err(|error| format!("invalid artifact id {}: {error}", evacuated.artifact_id))?;
        let prior = prior_of(evacuated.prior)
            .ok_or_else(|| format!("unknown prior {}", evacuated.prior))?;
        record_v2_evacuation(&self.pool, node_id, sandbox_id, prior, artifact).await
    }

    async fn record_report(
        &self,
        node_id: NodeId,
        epoch: u64,
        report: serde_json::Value,
    ) -> Result<bool, String> {
        record_v2_report(&self.pool, node_id, epoch_i64(epoch)?, report)
            .await
            .map_err(|error| error.to_string())
    }

    async fn advance_drained(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        advance_v2_drained(&self.pool, node_id, epoch_i64(epoch)?)
            .await
            .map_err(|error| error.to_string())
    }

    async fn finish_cancel(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        finish_v2_cancel(&self.pool, node_id, epoch_i64(epoch)?)
            .await
            .map_err(|error| error.to_string())
    }

    async fn record_error(&self, node_id: NodeId, message: &str) {
        if let Err(error) = record_v2_error(&self.pool, node_id, message).await {
            tracing::warn!(node_id = %node_id, %error, "node_drain v2: recording an error failed");
        }
        tracing::warn!(node_id = %node_id, error = message, "node_drain v2");
    }

    async fn release_suspended(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> Result<Vec<SandboxId>, String> {
        release_v2_suspended(&self.pool, node_id, epoch_i64(epoch)?)
            .await
            .map_err(|error| error.to_string())
    }

    async fn fence(&self, node_id: NodeId, epoch: u64) -> Result<Fence, String> {
        fence_v2_node(&self.pool, node_id, epoch_i64(epoch)?)
            .await
            .map_err(|error| error.to_string())
    }

    async fn record_dispatched(
        &self,
        node_id: NodeId,
        epoch: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        record_v2_dispatched(&self.pool, node_id, epoch_i64(epoch)?, instance_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn node_exists(&self, k8s_node_name: &str) -> Result<bool, String> {
        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.substrate.kubernetes().k8s.client.clone());
        match tokio::time::timeout(NODE_LOOKUP_TIMEOUT, nodes.get_opt(k8s_node_name)).await {
            Ok(Ok(node)) => Ok(node.is_some()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("the Node lookup timed out".to_owned()),
        }
    }

    async fn delete_terminated(
        &self,
        node: &V2Node,
        epoch: u64,
        fence: &str,
    ) -> Result<TerminatedDelete, String> {
        delete_v2_terminated(&self.pool, node, epoch_i64(epoch)?, fence)
            .await
            .map_err(|error| error.to_string())
    }

    async fn let_go(&self, node: &V2Node, epoch: u64, trace: &str) -> Result<bool, String> {
        let fence = node
            .fence
            .as_deref()
            .filter(|_| node.phase.as_deref() == Some("terminating"));
        let_go_v2(
            &self.pool,
            node.id,
            epoch_i64(epoch)?,
            node.phase.as_deref(),
            fence,
            trace,
        )
        .await
        .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;
