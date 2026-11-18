//! [`DrainV2Sink`] for the production sink: the v2 loop's statements over
//! `nodes`, its evacuation record, and its node-api calls.

use std::str::FromStr;

use pplx_space_accounts::OrgId;
use pplx_space_api::controllers::substrate::connect_internal_node_api_client_for_target;
use pplx_space_api::drain_restore_operations::{
    DrainRestoreOperationPayload, enqueue_drain_restore_once,
};
use pplx_space_sandbox_store::drain_restore::V2StoppedEvacuation;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPrior,
};
use uuid::Uuid;

use pplx_space_node_registration_store::drain_v2::{
    LockedV2Node, admit_v2_evacuation, advance_v2_drained, escalate_v2_drain, finish_v2_cancel,
    let_go_v2_node, list_v2_node_rows, read_locked_v2_node, record_terminate_held,
    record_v2_dispatched, record_v2_error, record_v2_report, record_v2_terminated_observed,
    write_fence,
};

use super::*;
use crate::node_drain::guarded_delete::{self, GuardedDelete, NodeIdentity};
use crate::node_drain::v2::{
    DrainCallError, DrainV2Sink, EvacuationRecord, Fence, TerminatedDelete, V2Node,
};

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

pub(crate) async fn list_v2_nodes(pool: &PgPool) -> Result<Vec<V2Node>, sqlx::Error> {
    let rows = list_v2_node_rows(pool).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let epoch = row.epoch.and_then(|epoch| u64::try_from(epoch).ok());
            V2Node {
                id: row.id,
                phase: row.drain_phase,
                epoch,
                gentle: row.gentle,
                mode_forced: row.mode_forced,
                deadline: row.deadline,
                deadline_due: row.deadline_due,
                report_holds_only: crate::node_drain::v2::report_holds_only(
                    row.report.as_ref(),
                    epoch,
                ),
                cancel: row.cancel,
                waiting: row.waiting,
                unrecorded_since: unrecorded_since(
                    row.report
                        .as_ref()
                        .and_then(|report| report.get("unrecorded")),
                ),
                k8s_node_name: row.k8s_node_name,
                k8s_node_uid: row.k8s_node_uid,
                instance_id: row.instance_id,
                heartbeat_fresh: row.heartbeat_fresh,
                fence: row.fence,
                dispatched: row.dispatched,
                dispatched_at: row.dispatched_at,
                terminated_observed_at: row.terminated_observed_at,
                decrement_capacity: row.decrement_capacity,
            }
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

/// Lock `node_id`'s row `FOR UPDATE` and read its v2 state; `None` when the
/// row is gone.
async fn lock_v2_node(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
) -> Result<Option<LockedV2Node>, sqlx::Error> {
    if !guarded_delete::lock_node(tx, node_id).await? {
        return Ok(None);
    }
    read_locked_v2_node(tx, node_id).await
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
        record_terminate_held(&mut tx, node_id, &held).await?;
        tx.commit().await?;
        return Ok(Fence::Held(held));
    }
    write_fence(&mut tx, node_id, &instance_id).await?;
    tx.commit().await?;
    Ok(Fence::Fenced(instance_id))
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
/// lock, the row must still carry that fence with a dispatch, and with the
/// record that EC2 listed the instance `terminated`, which an instance EC2
/// no longer lists can't stand in for, and record that instance; then the
/// guarded delete runs, holding the uid the tick read, a stale heartbeat and
/// that nothing names the node ([`guarded_delete::delete_locked_node`]). A
/// row or execution guard that refuses lets the node go in the same
/// transaction instead.
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
    if !locked.terminated_observed {
        return Ok(TerminatedDelete::Wait(
            "the row does not record that EC2 listed the fenced instance terminated".to_owned(),
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

/// The lifecycle a sandbox had on the node before the drain evacuated it,
/// as the node reported it: what the drain owes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EvacuatedPrior {
    /// Restored `started` elsewhere by a drain restore.
    Started,
    /// Left `suspended` once durable: no drain restore; the sandbox's next
    /// resume or wake restores it, on another node.
    Paused,
    /// Released from the node with its disk pinned.
    Stopped,
}

/// Record one evacuation the node on `node_id` reported, in one transaction
/// under the sandbox's row lock.
///
/// * **`started`**: enqueue the drain restore of `sandbox_id` toward
///   `started` from the catalog's ready snapshot of `artifact`, at most once
///   per snapshot, once the row is `suspended`. A row still `started`,
///   `paused` or `suspending` on the node isn't projected yet; one that runs
///   on another node, with an active execution there, has moved on.
/// * **`paused`**: the same, except that nothing is enqueued: once the row
///   is `suspended` and the catalog holds the snapshot ready, the evacuation
///   is recorded, and the row stays `suspended` for its next resume or wake.
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
    prior: EvacuatedPrior,
    artifact: Uuid,
) -> Result<EvacuationRecord, String> {
    let sql_error = |error: sqlx::Error| error.to_string();
    let mut tx = pool.begin().await.map_err(sql_error)?;
    if prior == EvacuatedPrior::Stopped {
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
    if prior == EvacuatedPrior::Paused {
        return Ok(EvacuationRecord::Recorded);
    }
    enqueue_drain_restore_once(
        &mut tx,
        org_id,
        DrainRestoreOperationPayload::new(sandbox_id, snapshot.as_uuid()),
    )
    .await
    .map_err(|error| error.to_string())?;
    tx.commit().await.map_err(sql_error)?;
    Ok(EvacuationRecord::Recorded)
}

fn prior_of(prior: i32) -> Option<EvacuatedPrior> {
    match DrainPrior::try_from(prior).ok()? {
        DrainPrior::Started => Some(EvacuatedPrior::Started),
        DrainPrior::Paused => Some(EvacuatedPrior::Paused),
        DrainPrior::Stopped => Some(EvacuatedPrior::Stopped),
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
        request: DrainNodeRequest,
    ) -> Result<DrainNodeResponse, DrainCallError> {
        let target = node_api_row(&self.pool, node_id)
            .await
            .map_err(|error| {
                DrainCallError::other(format!("node endpoint lookup failed: {error}"))
            })?
            .ok_or_else(|| DrainCallError::other(format!("node {node_id} not found")))?;
        let mut client = connect_internal_node_api_client_for_target(
            self.substrate.kubernetes(),
            &target.as_target(),
            DRAIN_NODE_TIMEOUT,
        )
        .await
        .map_err(|error| DrainCallError::other(error.to_string()))?;
        client
            .drain_node(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| DrainCallError::status(&status))
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

    async fn escalate(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        escalate_v2_drain(&self.pool, node_id, epoch_i64(epoch)?)
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

    async fn record_terminated_observed(
        &self,
        node_id: NodeId,
        epoch: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        record_v2_terminated_observed(&self.pool, node_id, epoch_i64(epoch)?, instance_id)
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
