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
    admit_v2_evacuation, advance_v2_drained, list_v2_node_rows, record_v2_error, record_v2_report,
};

use super::*;
use crate::node_drain::v2::{DrainV2Sink, EvacuationRecord, V2Node};

/// How long one `DrainNode` call may take; the node answers without waiting.
const DRAIN_NODE_TIMEOUT: Duration = Duration::from_secs(60);

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
                waiting: row.waiting,
                unrecorded_since: unrecorded_since(
                    row.report
                        .as_ref()
                        .and_then(|report| report.get("unrecorded")),
                ),
                instance_id: row.instance_id,
                heartbeat_fresh: row.heartbeat_fresh,
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

    async fn drain_node(&self, node_id: NodeId, epoch: u64) -> Result<DrainNodeResponse, String> {
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
                ..DrainNodeRequest::default()
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

    async fn record_error(&self, node_id: NodeId, message: &str) {
        if let Err(error) = record_v2_error(&self.pool, node_id, message).await {
            tracing::warn!(node_id = %node_id, %error, "node_drain v2: recording an error failed");
        }
        tracing::warn!(node_id = %node_id, error = message, "node_drain v2");
    }
}

#[cfg(test)]
mod tests;
