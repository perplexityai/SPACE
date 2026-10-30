//! Converge an implicit resident resume without trusting a delayed inventory.

use std::net::SocketAddr;
use std::time::Duration;

use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxId, SubstrateKind},
};
use pplx_space_node_api_client::WorkerApiClient;
use pplx_space_sbx_mgr_client::proto::{SandboxInfo, SandboxLifecycleState, StateRequest};
use sqlx::PgPool;
use tonic::Status;
use uuid::Uuid;

use crate::{OriginNode, proto::PublishedSandboxState};

const PROBE_BUDGET: Duration = Duration::from_secs(2);

#[derive(sqlx::FromRow)]
struct Candidate {
    id: SandboxId,
    row_version: String,
    generation: i64,
    pod_uid: String,
    route_activation_id: Uuid,
}

async fn candidates(
    pool: &PgPool,
    node_id: NodeId,
    ids: &[SandboxId],
) -> Result<Vec<Candidate>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT s.id, s.xmin::TEXT AS row_version, e.generation, e.pod_uid,
                e.route_activation_id
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
          WHERE s.id = ANY($1) AND s.active_node_id = $2
            AND s.managed_by = $3 AND s.lifecycle = 'paused'
            AND s.evacuation_restore_target IS NULL
            AND e.state = 'running' AND e.pod_ip IS NOT NULL
            AND e.pod_uid IS NOT NULL AND e.route_activation_id IS NOT NULL
            AND e.activation_attempt_id IS NULL
          ORDER BY s.id
          LIMIT 64"
    ))
    .bind(ids)
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(pool)
    .await
}

fn confirms_started(candidate: &Candidate, observed: &SandboxInfo) -> bool {
    observed.sandbox_id == candidate.id.to_string()
        && observed.lifecycle == Some(SandboxLifecycleState::Started as i32)
        && !observed.state_degraded
        && observed.execution_generation == Some(candidate.generation)
        && observed.pod_uid.as_deref() == Some(candidate.pod_uid.as_str())
        && observed
            .activation_attempt_id
            .as_deref()
            .and_then(|id| id.parse::<Uuid>().ok())
            == Some(candidate.route_activation_id)
}

async fn complete(
    pool: &PgPool,
    node_id: NodeId,
    candidate: &Candidate,
) -> Result<bool, sqlx::Error> {
    // xmin also changes on a same-state UPDATE: a second Pause may finish
    // while the public row still says paused. Timestamps that change only on
    // lifecycle edges cannot fence that race. This token lives only across
    // the bounded probe, never in a persisted or public contract.
    let mut tx = pool.begin().await?;
    // Also bound PostgreSQL's wait if the request times out and drops this
    // future while the server is still acquiring its row locks.
    sqlx::query(sql!("SET LOCAL lock_timeout = '500ms'"))
        .execute(&mut *tx)
        .await?;
    let current = sqlx::query_scalar::<_, SandboxId>(sql!(
        "SELECT s.id FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1 AND s.active_node_id = $2
            AND s.lifecycle = 'paused' AND s.xmin::TEXT = $3
            AND s.evacuation_restore_target IS NULL
            AND e.sandbox_id = s.id AND e.active AND e.state = 'running'
            AND e.pod_ip IS NOT NULL AND e.generation = $4
            AND e.pod_uid = $5 AND e.route_activation_id = $6
            AND e.activation_attempt_id IS NULL
          FOR UPDATE OF s, e"
    ))
    .bind(candidate.id)
    .bind(node_id)
    .bind(&candidate.row_version)
    .bind(candidate.generation)
    .bind(&candidate.pod_uid)
    .bind(candidate.route_activation_id)
    .fetch_optional(&mut *tx)
    .await?;
    if current.is_none() {
        return Ok(false);
    }
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(candidate.id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Publication has already committed. A report is only a hint to ask for a
/// fresh, read-only observation; it can never itself undo a later Pause.
/// No database lock is held across the RPC. Failed or superseded probes are
/// retried by subsequent inventory publications, without failing this one.
/// Lookup, probe and completion share a two-second budget.
pub(super) async fn reconcile(
    pool: &PgPool,
    origin: &OriginNode,
    reports: &[PublishedSandboxState],
) {
    let Some(address) = origin.address else {
        return;
    };
    let ids: Vec<SandboxId> = reports
        .iter()
        .filter(|report| {
            report.present && !report.state_degraded && report.status.as_deref() == Some("started")
        })
        .filter_map(|report| report.sandbox_id.parse().ok())
        .collect();
    if ids.is_empty() {
        return;
    }
    let work = async {
        let candidates = candidates(pool, origin.id, &ids).await.map_err(|error| {
            Status::internal(format!("read resident resume candidates: {error}"))
        })?;
        if candidates.is_empty() {
            return Ok::<_, Status>(());
        }
        let endpoint = SocketAddr::new(address, pplx_space_node_api_client::DEFAULT_PORT);
        let mut node =
            crate::sandbox_service_ports::worker_node_api(&endpoint.to_string(), PROBE_BUDGET)?;
        let observed = WorkerApiClient::new(&mut node)
            .sandbox_state(
                StateRequest {
                    sandbox_ids: candidates
                        .iter()
                        .map(|candidate| candidate.id.to_string())
                        .collect(),
                },
                PROBE_BUDGET,
            )
            .await?
            .into_inner();
        for candidate in candidates {
            if observed
                .sandboxes
                .iter()
                .any(|state| confirms_started(&candidate, state))
                && complete(pool, origin.id, &candidate)
                    .await
                    .map_err(|error| {
                        Status::internal(format!("complete resident resume: {error}"))
                    })?
            {
                pplx_space_events::SandboxEvent::new(
                    candidate.id,
                    pplx_space_events::SandboxEventType::ResumeCompleted,
                    pplx_space_events::SandboxEventSource::Api,
                )
                .emit_with(
                    pplx_space_events::SandboxEventDetails::default()
                        .with_from_state("paused")
                        .with_to_state("started"),
                );
            }
        }
        Ok(())
    };
    match tokio::time::timeout(PROBE_BUDGET, work).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(node_id = %origin.id, %error, "resident resume confirmation deferred")
        }
        Err(_) => tracing::debug!(node_id = %origin.id, "resident resume confirmation timed out"),
    }
}

#[cfg(test)]
mod tests;
