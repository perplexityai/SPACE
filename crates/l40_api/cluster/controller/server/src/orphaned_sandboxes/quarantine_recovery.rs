//! Repair an abandoned publication using existing quarantine and snapshot rows.
//! This deliberately does not dispose of local artifacts or retire a live pod.

use k8s_openapi::api::core::v1::Pod;
use pplx_space_api::controllers::substrate::{
    NodeApiTarget, ObservedSandboxState, connect_node_api_client_for_target,
    kubernetes::KubernetesSubstrate, sandbox_states_with_client,
};
use pplx_space_common_core::types::{NodeManagedBy, NodeRole, SandboxId};
use pplx_space_events::{SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventType};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

mod store;
#[cfg(test)]
mod tests;

const BATCH_SIZE: i64 = 4;
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct Repair {
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    after: Option<SandboxId>,
}

impl Repair {
    pub(super) fn new(pool: PgPool, kube: Arc<KubernetesSubstrate>) -> Self {
        Self {
            pool,
            kube,
            after: None,
        }
    }

    pub(super) async fn tick(&mut self, dry_run: bool, shutdown: &CancellationToken) {
        let candidates = match store::candidates(&self.pool, self.after, None, BATCH_SIZE).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, "quarantine absence recovery scan failed");
                return;
            }
        };
        if candidates.is_empty() {
            self.after = None;
            return;
        }
        for candidate in candidates {
            if shutdown.is_cancelled() {
                return;
            }
            self.after = Some(candidate.sandbox_id);
            let result = tokio::select! {
                _ = shutdown.cancelled() => return,
                result = tokio::time::timeout(Duration::from_secs(15), self.recover(&candidate, dry_run)) => result,
            };
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(sandbox_id = %candidate.sandbox_id, %error, "quarantine absence recovery deferred")
                }
                Err(_) => {
                    tracing::warn!(sandbox_id = %candidate.sandbox_id, "quarantine absence recovery timed out; retry on next sweep");
                }
            }
        }
    }

    async fn recover(
        &mut self,
        candidate: &store::Candidate,
        dry_run: bool,
    ) -> Result<(), RecoveryError> {
        // A reflector miss is not proof of absence. This bounded GET checks the
        // exact historical pod name against the authoritative API server.
        let pods =
            kube::Api::<Pod>::namespaced(self.kube.k8s.client.clone(), &candidate.pod_namespace);
        if !confirm_absence(
            || async { Ok(pods.get_opt(&candidate.pod_name).await?.is_none()) },
            self.worker_state(candidate),
        )
        .await?
        {
            return Ok(());
        }
        if dry_run {
            tracing::info!(sandbox_id = %candidate.sandbox_id, "quarantine absence recovery candidate (dry run)");
        } else if store::recover(&self.pool, candidate).await? {
            SandboxEvent::new(
                candidate.sandbox_id,
                SandboxEventType::LifecycleRevertedToSuspended,
                SandboxEventSource::ClusterController,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .operation("quarantine_absence_recovery")
                    .with_from_state("quarantined")
                    .with_to_state("suspended")
                    .reason("current_snapshot_and_execution_absent"),
            );
        }
        Ok(())
    }

    async fn worker_state(
        &mut self,
        candidate: &store::Candidate,
    ) -> Result<Option<ObservedSandboxState>, RecoveryError> {
        // Resolve on each bounded probe: a reused endpoint could belong to a
        // replacement worker after node churn and report misleading absence.
        let target = NodeApiTarget {
            managed_by: NodeManagedBy::Kubernetes,
            role: NodeRole::Worker,
            direct_endpoint: None,
            substrate_node_id: Some(&candidate.node_name),
        };
        let (endpoint, mut client) =
            connect_node_api_client_for_target(self.kube.as_ref(), &target, PROBE_TIMEOUT).await?;
        let states =
            sandbox_states_with_client(&mut client, &endpoint, &[candidate.sandbox_id]).await?;
        Ok(states.get(&candidate.sandbox_id).copied())
    }
}

// A positive worker absence reply must be bracketed by authoritative pod
// absence. Errors, incomplete replies, and reappearance never permit recovery.
async fn confirm_absence<P, W>(
    mut pod_absent: impl FnMut() -> P,
    worker_state: W,
) -> Result<bool, RecoveryError>
where
    P: std::future::Future<Output = Result<bool, RecoveryError>>,
    W: std::future::Future<Output = Result<Option<ObservedSandboxState>, RecoveryError>>,
{
    if !pod_absent().await? {
        return Ok(false);
    }
    if !matches!(worker_state.await?, Some(ObservedSandboxState::Missing)) {
        return Ok(false);
    }
    pod_absent().await
}

#[derive(Debug, thiserror::Error)]
enum RecoveryError {
    #[error("pod absence probe failed: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("worker absence probe failed: {0}")]
    Worker(#[from] pplx_space_api::controllers::substrate::SubstrateError),
    #[error("recovery transaction failed: {0}")]
    Database(#[from] sqlx::Error),
}
