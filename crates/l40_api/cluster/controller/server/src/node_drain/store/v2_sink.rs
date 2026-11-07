//! [`DrainV2Sink`] for the production sink: the v2 loop's statements over
//! `nodes`, its evacuation record, and its node-api calls.

use std::str::FromStr;

use pplx_space_api::controllers::substrate::connect_internal_node_api_client_for_target;
use pplx_space_api::drain_restore_operations::{
    DrainRestoreOperationPayload, enqueue_drain_restore_once,
};
use pplx_space_common_core::types::OrgId;
use pplx_space_sandbox_store::drain_restore::DrainRestoreTarget;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPrior,
};
use uuid::Uuid;

use super::*;
use crate::node_drain::v2::{DrainV2Sink, EvacuationRecord, V2Node};

/// How long one `DrainNode` call may take; the node answers without waiting.
const DRAIN_NODE_TIMEOUT: Duration = Duration::from_secs(60);

/// Every v2 drain's node of the Kubernetes substrate, with the Kubernetes
/// identity a termination is recorded against.
pub(crate) async fn list_v2_nodes(pool: &PgPool) -> Result<Vec<V2Node>, sqlx::Error> {
    type Row = (
        NodeId,
        Option<String>,
        Option<i64>,
        bool,
        bool,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<Row> = sqlx::query_as(sql!(
        "SELECT n.id, n.drain_phase,
                (n.drain_status->>'epoch')::BIGINT,
                COALESCE((n.drain_status->>'cancel')::BOOLEAN, FALSE),
                n.drain_status ? 'terminated',
                k.k8s_node_uid, k.instance_id
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.drain_engine = 'v2' AND n.managed_by = $1
          ORDER BY n.id"
    ))
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, phase, epoch, cancel, terminated, k8s_node_uid, instance_id)| V2Node {
                id,
                phase,
                epoch: epoch.and_then(|epoch| u64::try_from(epoch).ok()),
                cancel,
                terminated,
                k8s_node_uid,
                instance_id,
            },
        )
        .collect())
}

/// The SQL predicate that the `nodes` row aliased `n` is a v2 drain's node
/// the controller terminated under its current epoch: `:terminate` recorded
/// that epoch's `done` report (`drain_status.terminate`), the node reported
/// `done` under it, and the termination was recorded under it against the
/// uid and instance `node_k8s` still holds. The dead-node verb's evidence
/// for such a node, read where it selects, writes and deletes.
pub(crate) fn v2_terminated_record(n: &str) -> String {
    format!(
        "({n}.drain_engine = 'v2'
          AND {n}.drain_phase = 'terminating'
          AND ({n}.drain_status->'terminate'->>'epoch')::BIGINT
              = ({n}.drain_status->>'epoch')::BIGINT
          AND ({n}.drain_status->'report'->>'sealed_epoch')::BIGINT
              = ({n}.drain_status->>'epoch')::BIGINT
          AND COALESCE(({n}.drain_status->'report'->>'done')::BOOLEAN, FALSE)
          AND ({n}.drain_status->'terminated'->>'epoch')::BIGINT
              = ({n}.drain_status->>'epoch')::BIGINT
          AND ({n}.drain_status->'terminated'->>'k8s_node_uid')
              IS NOT DISTINCT FROM (SELECT k8s_node_uid FROM node_k8s WHERE node_id = {n}.id)
          AND ({n}.drain_status->'terminated'->>'instance_id')
              IS NOT DISTINCT FROM (SELECT instance_id FROM node_k8s WHERE node_id = {n}.id))"
    )
}

/// Record that the terminator acted on the v2 node under `epoch`, against the
/// uid and instance the tick read. Only a terminating node of that epoch,
/// let go by `:terminate` on a `done` report, whose `node_k8s` still holds
/// that identity, and only once.
pub(crate) async fn record_v2_terminated(
    pool: &PgPool,
    node_id: NodeId,
    epoch: i64,
    k8s_node_uid: Option<&str>,
    instance_id: Option<&str>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes n
            SET drain_status = n.drain_status || jsonb_build_object(
                    'terminated', jsonb_build_object(
                        'epoch', $2::BIGINT,
                        'k8s_node_uid', $3::TEXT,
                        'instance_id', $4::TEXT,
                        'at_ms', (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
                    )
                ),
                drain_last_error = NULL
          WHERE n.id = $1
            AND n.drain_engine = 'v2'
            AND n.drain_phase = 'terminating'
            AND NOT (n.drain_status ? 'terminated')
            AND (n.drain_status->>'epoch')::BIGINT = $2
            AND (n.drain_status->'terminate'->>'epoch')::BIGINT = $2
            AND (n.drain_status->'report'->>'sealed_epoch')::BIGINT = $2
            AND COALESCE((n.drain_status->'report'->>'done')::BOOLEAN, FALSE)
            AND $3::TEXT IS NOT DISTINCT FROM
                (SELECT k8s_node_uid FROM node_k8s WHERE node_id = n.id)
            AND $4::TEXT IS NOT DISTINCT FROM
                (SELECT instance_id FROM node_k8s WHERE node_id = n.id)"
    ))
    .bind(node_id)
    .bind(epoch)
    .bind(k8s_node_uid)
    .bind(instance_id)
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
            pplx_space_sandbox_store::drain_restore::record_v2_stop_pin(
                &mut tx, sandbox_id, artifact,
            )
            .await
            .map_err(sql_error)?
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

    async fn record_terminated(&self, node: &V2Node, epoch: u64) -> Result<bool, String> {
        let epoch = i64::try_from(epoch).map_err(|error| error.to_string())?;
        record_v2_terminated(
            &self.pool,
            node.id,
            epoch,
            node.k8s_node_uid.as_deref(),
            node.instance_id.as_deref(),
        )
        .await
        .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;
