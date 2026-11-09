//! [`DrainV2Sink`] for the production sink: the v2 loop's statements over
//! `nodes`, its evacuation record, and its node-api calls.

use std::str::FromStr;

use pplx_space_api::controllers::substrate::connect_internal_node_api_client_for_target;
use pplx_space_api::drain_restore_operations::{
    DrainRestoreOperationPayload, enqueue_drain_restore_once,
};
use pplx_space_common_core::types::OrgId;
use pplx_space_sandbox_store::drain_restore::V2StoppedEvacuation;
use pplx_space_sandbox_store::sweeper::EvacuationRestoreTarget;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPrior,
};
use uuid::Uuid;

use super::*;
use crate::node_drain::v2::{DrainV2Sink, EvacuationRecord, V2Node};

/// How long one `DrainNode` call may take; the node answers without waiting.
const DRAIN_NODE_TIMEOUT: Duration = Duration::from_secs(60);

/// A v2 node's id, phase, epoch, cancel, terminated and waiting flags, and
/// its stored report's unrecorded evacuations.
type V2NodeRow = (
    NodeId,
    Option<String>,
    Option<i64>,
    bool,
    bool,
    bool,
    Option<serde_json::Value>,
);

/// Every v2 drain's node of the Kubernetes substrate.
pub(crate) async fn list_v2_nodes(pool: &PgPool) -> Result<Vec<V2Node>, sqlx::Error> {
    let rows: Vec<V2NodeRow> = sqlx::query_as(sql!(
        "SELECT id, drain_phase,
                (drain_status->>'epoch')::BIGINT,
                COALESCE((drain_status->>'cancel')::BOOLEAN, FALSE),
                COALESCE((drain_status->>'terminated')::BOOLEAN, FALSE),
                COALESCE((drain_status->>'waiting')::BOOLEAN, FALSE),
                drain_status->'report'->'unrecorded'
           FROM nodes
          WHERE drain_engine = 'v2' AND managed_by = $1
          ORDER BY id"
    ))
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, phase, epoch, cancel, terminated, waiting, unrecorded)| V2Node {
                id,
                phase,
                epoch: epoch.and_then(|epoch| u64::try_from(epoch).ok()),
                cancel,
                terminated,
                waiting,
                unrecorded_since: unrecorded_since(unrecorded.as_ref()),
            },
        )
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

/// Record that the terminator acted on the terminating v2 node under
/// `epoch`, so the loop doesn't call it again. Returns whether it was
/// recorded: false for a row no longer that epoch's terminating v2 node.
pub(crate) async fn record_v2_terminated(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('terminated', TRUE),
                drain_last_error = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'terminating'
            AND (drain_status->>'epoch')::BIGINT = $2"
    ))
    .bind(node_id)
    .bind(epoch)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
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

    async fn record_terminated(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        record_v2_terminated(&self.pool, node_id, epoch_i64(epoch)?)
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;
