//! Bounded ingress waiting over independently owned access activation.
use super::*;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority;
use pplx_space_internal_api_server::ingress_activation::{
    IngressActivation, IngressActivationFactory as Factory,
};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::Status;

pub struct IngressActivationFactory;
struct Activator {
    backend: Arc<dyn AccessBackend>,
    owners: ActivationOwners,
}
struct StoredAccess {
    pool: PgPool,
    controller: SandboxController,
}

#[derive(Clone, Copy)]
struct Observation {
    lifecycle: SandboxLifecycle,
    access_wake: bool,
    restore_owned: bool,
    cooldown: Option<Duration>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AccessOutcome {
    Ready,
    RestoreOwned,
    CoolingDown,
}

#[tonic::async_trait]
trait AccessBackend: Send + Sync {
    async fn observe(&self, id: SandboxId) -> Result<Observation, Status>;
    /// Read-only probe: no wake, recovery, or lifecycle reconciliation.
    async fn reachable(&self, id: SandboxId) -> Result<bool, Status>;
    async fn access(&self, id: SandboxId) -> Result<AccessOutcome, Status>;
}

/// Detached request waiters cannot create unbounded accepted activation tasks.
struct ActivationOwners {
    tasks: TaskTracker,
    slots: Arc<Semaphore>,
    shutdown: CancellationToken,
}
impl ActivationOwners {
    async fn access(
        &self,
        backend: Arc<dyn AccessBackend>,
        id: SandboxId,
    ) -> Result<AccessOutcome, Status> {
        // Recheck in the request future immediately before accepting an owner.
        // A raced cooldown returns without acquiring a slot or spawning work.
        let state = backend.observe(id).await?;
        if self.shutdown.is_cancelled() {
            return Err(Status::unavailable("ingress activation is shutting down"));
        }
        validate_access(state.lifecycle, state.access_wake)?;
        if state.restore_owned {
            return Ok(AccessOutcome::RestoreOwned);
        }
        if state.cooldown.is_some() {
            return Ok(AccessOutcome::CoolingDown);
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("ingress activation capacity exhausted"))?;
        // Dropping this JoinHandle only detaches the waiter. The task retains
        // the permit and runs the existing substrate owner through publication.
        self.tasks
            .spawn(async move {
                let _permit = permit;
                let result =
                    crate::controllers::substrate::activation_cooldown::return_wait_to_request(
                        backend.access(id),
                    )
                    .await;
                if let Err(error) = &result {
                    tracing::warn!(sandbox_id = %id, %error, "ingress access activation failed");
                }
                result
            })
            .await
            .map_err(|error| Status::internal(format!("ingress activation task failed: {error}")))?
    }
}

#[tonic::async_trait]
impl Factory for IngressActivationFactory {
    async fn build(
        &self,
        pool: PgPool,
        authority: Option<Arc<SpaceApiRouteEd25519Authority>>,
        tasks: TaskTracker,
        shutdown: CancellationToken,
    ) -> Result<Arc<dyn IngressActivation>, Status> {
        let substrate = Substrate::from_env(&pool)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let admission = crate::cluster_admission::ClusterSandboxAdmission::new(
            substrate.clone(),
            pplx_space_feature_flags::FeatureFlagClient::from_env(),
        );
        let controller = SandboxController::with_cluster_admission(substrate, admission)
            .with_route_ed25519_authority(authority);
        Ok(Arc::new(Activator {
            backend: Arc::new(StoredAccess { pool, controller }),
            owners: ActivationOwners {
                tasks,
                slots: Arc::new(Semaphore::new(128)),
                shutdown,
            },
        }))
    }
}

#[tonic::async_trait]
impl AccessBackend for StoredAccess {
    async fn observe(&self, id: SandboxId) -> Result<Observation, Status> {
        let sandbox = fetch_sandbox(&self.pool, id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("sandbox not found"))?;
        let admission = pplx_space_sandbox_store::activation_admission_state(&self.pool, id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Observation {
            lifecycle: sandbox
                .lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|error| Status::internal(error.to_string()))?,
            access_wake: ExecutionActivityKind::Access
                .allows_implicit_wake(&sandbox.auto_lifecycle.0),
            restore_owned: admission.restore_owned,
            cooldown: admission.cooldown,
        })
    }
    async fn reachable(&self, id: SandboxId) -> Result<bool, Status> {
        let sandbox = fetch_sandbox(&self.pool, id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("sandbox not found"))?;
        // The earlier observation cannot authorize a later activation. Even if
        // lifecycle changes after this read, this probe cannot initiate work.
        if sandbox.lifecycle != SandboxLifecycle::Started.as_str() || sandbox.command_ready_pending
        {
            return Ok(false);
        }
        super::lifecycle::ensure_access_path_not_blocked_by_drain(
            &self.pool,
            &sandbox,
            super::lifecycle::SandboxAction::Spaced,
        )
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
        self.controller
            .substrate
            .probe_runtime_client(id)
            .await
            .map(|client| client.is_some())
            .map_err(|error| Status::unavailable(error.to_string()))
    }
    async fn access(&self, id: SandboxId) -> Result<AccessOutcome, Status> {
        // Re-read policy/lifecycle at activation entry; the observation is not
        // authority to wake after an intervening policy or lifecycle change.
        let sandbox = fetch_sandbox(&self.pool, id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("sandbox not found"))?;
        let lifecycle = sandbox
            .lifecycle
            .parse::<SandboxLifecycle>()
            .map_err(|error| Status::internal(error.to_string()))?;
        validate_access(
            lifecycle,
            ExecutionActivityKind::Access.allows_implicit_wake(&sandbox.auto_lifecycle.0),
        )?;
        if lifecycle == SandboxLifecycle::Paused {
            // A resident worker is reachable while paused, but bridge setup
            // requires a running guest. Finish the shared wake path, including
            // lifecycle publication, inside the cancellation-independent owner.
            if let Err(error) = self
                .controller
                .action(
                    &self.pool,
                    id,
                    Some(&sandbox),
                    SandboxApiMetricIdentity::native("ingress"),
                    super::lifecycle::SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
                    None,
                )
                .await
            {
                return access_error(error);
            }
            // A concurrent suspend can supersede a successful wake submission.
            if self.observe(id).await?.lifecycle != SandboxLifecycle::Started {
                return Err(Status::unavailable("ingress wake has not reached started"));
            }
            return Ok(AccessOutcome::Ready);
        }
        match spaced::connect_spaced_runtime_client(
            &self.controller.substrate,
            self.controller.cluster_admission.as_ref(),
            true,
            &self.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("ingress"),
            Some(ExecutionActivityKind::Access),
        )
        .await
        {
            Ok(_) => Ok(AccessOutcome::Ready),
            Err(error) => access_error(error),
        }
    }
}

fn access_error(error: ControllerError) -> Result<AccessOutcome, Status> {
    use crate::controllers::ControllerRetryAfterCause;
    match error.retry_after_cause() {
        Some(ControllerRetryAfterCause::RestoreInFlight) => Ok(AccessOutcome::RestoreOwned),
        Some(ControllerRetryAfterCause::ActivationCooldown) => Ok(AccessOutcome::CoolingDown),
        _ if error.status() == axum::http::StatusCode::CONFLICT => {
            Err(Status::failed_precondition(error.to_string()))
        }
        _ => Err(Status::unavailable(error.to_string())),
    }
}

fn validate_access(lifecycle: SandboxLifecycle, access_wake: bool) -> Result<(), Status> {
    if !matches!(
        lifecycle,
        SandboxLifecycle::Started
            | SandboxLifecycle::Paused
            | SandboxLifecycle::Suspended
            | SandboxLifecycle::Restoring
    ) {
        return Err(Status::failed_precondition(
            "sandbox lifecycle cannot serve ingress",
        ));
    }
    if lifecycle != SandboxLifecycle::Started && !access_wake {
        return Err(Status::failed_precondition(
            "automatic access wake is disabled",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl IngressActivation for Activator {
    async fn prepare(&self, id: SandboxId, timeout: Duration) -> Result<(), Status> {
        let work = async {
            if self.owners.shutdown.is_cancelled() {
                return Err(Status::unavailable("ingress activation is shutting down"));
            }
            let mut observing = false;
            let mut delay = Duration::from_millis(100);
            loop {
                let state = self.backend.observe(id).await?;
                validate_access(state.lifecycle, state.access_wake)?;
                if observing && state.lifecycle == SandboxLifecycle::Started {
                    return Ok(());
                }
                if state.restore_owned {
                    if !observing {
                        pplx_space_metrics::internal_api::ingress_preparation_path(true);
                    }
                    observing = true;
                } else if observing {
                    return Err(Status::unavailable(
                        "restore owner ended without a usable route",
                    ));
                } else if let Some(cooldown) = state.cooldown {
                    tokio::time::sleep(cooldown.min(Duration::from_millis(500))).await;
                    continue;
                } else {
                    if state.lifecycle == SandboxLifecycle::Started
                        && self.backend.reachable(id).await?
                    {
                        return Ok(());
                    }
                    pplx_space_metrics::internal_api::ingress_preparation_path(false);
                    match self.owners.access(self.backend.clone(), id).await? {
                        AccessOutcome::Ready => return Ok(()),
                        AccessOutcome::CoolingDown => {}
                        AccessOutcome::RestoreOwned => {
                            pplx_space_metrics::internal_api::ingress_preparation_path(true);
                            observing = true;
                        }
                    }
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(500));
            }
        };
        tokio::time::timeout(timeout, work)
            .await
            .map_err(|_| Status::deadline_exceeded("ingress activation deadline exceeded"))?
    }
}

#[cfg(test)]
mod tests;
