//! [`DrainV2Sink`] for the production sink: the v2 loop's statements over
//! `nodes`, its evacuation record, and its node-api calls.

use std::str::FromStr;

use pplx_space_api::controllers::substrate::connect_internal_node_api_client_for_target;
use pplx_space_api::drain_restore_operations::{
    DrainRestoreOperationPayload, enqueue_drain_restore_once,
};
use pplx_space_common_core::types::{NodeDrainEngine, OrgId};
use pplx_space_sandbox_store::drain_restore::DrainRestoreTarget;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPrior,
};
use uuid::Uuid;

use super::*;
use crate::node_drain::v2::{DrainV2Sink, EvacuationRecord, V2Node};

/// How long one `DrainNode` call may take; the node answers without waiting.
const DRAIN_NODE_TIMEOUT: Duration = Duration::from_secs(60);

/// Every v2 drain's node of the Kubernetes substrate.
pub(crate) async fn list_v2_nodes(pool: &PgPool) -> Result<Vec<V2Node>, sqlx::Error> {
    let rows: Vec<(NodeId, Option<String>, Option<i64>, bool)> = sqlx::query_as(sql!(
        "SELECT id, drain_phase,
                (drain_status->>'epoch')::BIGINT,
                COALESCE((drain_status->>'cancel')::BOOLEAN, FALSE)
           FROM nodes
          WHERE drain_engine = 'v2' AND managed_by = $1
          ORDER BY id"
    ))
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, phase, epoch, cancel)| V2Node {
            id,
            phase,
            epoch: epoch.and_then(|epoch| u64::try_from(epoch).ok()),
            cancel,
        })
        .collect())
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

/// Record one evacuation the node on `node_id` reported: in one transaction,
/// under the sandbox's row lock, enqueue the drain restore of `sandbox_id`
/// toward `target` from the catalog's ready snapshot of `artifact` (for
/// `stopped`, after recording that disk as the row's stop pin), at most once
/// per snapshot. A row that no longer needs it is [`EvacuationRecord::Settled`].
pub(crate) async fn record_v2_evacuation(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    target: DrainRestoreTarget,
    artifact: Uuid,
) -> Result<EvacuationRecord, String> {
    let sql_error = |error: sqlx::Error| error.to_string();
    let mut tx = pool.begin().await.map_err(sql_error)?;
    let row: Option<(OrgId, String, Option<NodeId>, Option<String>)> = sqlx::query_as(sql!(
        "SELECT org_id, lifecycle, active_node_id, evacuation_restore_target
           FROM sandboxes
          WHERE id = $1
          FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(sql_error)?;
    let Some((org_id, lifecycle, active_node_id, marker)) = row else {
        return Ok(EvacuationRecord::Settled);
    };
    let kind = if target == DrainRestoreTarget::Stopped {
        "disk"
    } else {
        "full"
    };
    let snapshot: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT sn.id
           FROM snapshots sn
          WHERE sn.sandbox_id = $1
            AND sn.local_artifact_id = $2
            AND sn.type = $3
            AND sn.state = 'ready'
            AND EXISTS (
              SELECT 1 FROM snapshot_storage_locations ssl
               WHERE ssl.snapshot_id = sn.id AND ssl.orphaned_at IS NULL
            )
          ORDER BY sn.timestamp DESC
          LIMIT 1"
    ))
    .bind(sandbox_id)
    .bind(artifact)
    .bind(kind)
    .fetch_optional(&mut *tx)
    .await
    .map_err(sql_error)?;
    let Some(snapshot) = snapshot else {
        return Ok(EvacuationRecord::NotYet(
            "the catalog doesn't hold the artifact as ready yet".to_owned(),
        ));
    };
    let on_node = active_node_id == Some(node_id);
    let enqueue = match (target, lifecycle.as_str()) {
        (DrainRestoreTarget::Started | DrainRestoreTarget::Paused, "suspended") => true,
        (
            DrainRestoreTarget::Started | DrainRestoreTarget::Paused,
            "started" | "paused" | "suspending",
        ) if on_node => {
            return Ok(EvacuationRecord::NotYet(
                "the node's suspend isn't projected onto the row yet".to_owned(),
            ));
        }
        (DrainRestoreTarget::Stopped, "stopped") if marker.is_none() => {
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET current_local_artifact_id = $2,
                        current_local_artifact_kind = 'disk',
                        current_local_artifact_origin = 'stop'
                  WHERE id = $1
                    AND lifecycle = 'stopped'
                    AND evacuation_restore_target IS NULL
                    AND (current_local_artifact_id IS NULL OR current_local_artifact_id = $2)"
            ))
            .bind(sandbox_id)
            .bind(artifact)
            .execute(&mut *tx)
            .await
            .map_err(sql_error)?
            .rows_affected()
                == 1
        }
        _ => false,
    };
    if !enqueue {
        return Ok(EvacuationRecord::Settled);
    }
    enqueue_drain_restore_once(
        &mut tx,
        org_id,
        DrainRestoreOperationPayload::new(sandbox_id, target, snapshot),
    )
    .await
    .map_err(|error| error.to_string())?;
    tx.commit().await.map_err(sql_error)?;
    Ok(EvacuationRecord::Recorded)
}

fn target_of(prior: i32) -> Option<DrainRestoreTarget> {
    match DrainPrior::try_from(prior).ok()? {
        DrainPrior::Started => Some(DrainRestoreTarget::Started),
        DrainPrior::Paused => Some(DrainRestoreTarget::Paused),
        DrainPrior::Stopped => Some(DrainRestoreTarget::Stopped),
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
            .drain_node(DrainNodeRequest { epoch, cancel })
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
        let target = target_of(evacuated.prior)
            .ok_or_else(|| format!("unknown prior {}", evacuated.prior))?;
        record_v2_evacuation(&self.pool, node_id, sandbox_id, target, artifact).await
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

    async fn delete_terminated(&self, node_id: NodeId) -> Result<bool, String> {
        delete_terminating_node_and_record_tombstone(&self.pool, node_id, NodeDrainEngine::V2)
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;
