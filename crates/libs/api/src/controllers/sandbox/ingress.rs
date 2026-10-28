//! Bounded ingress waiting over independently owned access activation.
use super::*;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_node_api_client::activation::IngressActivation;
use pplx_space_node_api_client::proto::ImplicitWakeActivity;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::Status;
use tracing::Instrument as _;

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
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AccessOutcome {
    Ready,
    RestoreOwned,
}

#[tonic::async_trait]
trait AccessBackend: Send + Sync {
    async fn observe(&self, id: SandboxId) -> Result<Observation, Status>;
    /// Read-only probe: no wake, recovery, or lifecycle reconciliation.
    async fn reachable(&self, id: SandboxId) -> Result<bool, Status>;
    async fn access(&self, id: SandboxId) -> Result<AccessOutcome, Status>;
    /// Queue, or join, the system wake a session on the sandbox waits for,
    /// when it is at rest and its wake policy lets `activity` wake it.
    async fn queue_implicit_wake(
        &self,
        id: SandboxId,
        activity: ExecutionActivityKind,
    ) -> Result<bool, Status>;
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
        if self.shutdown.is_cancelled() {
            return Err(Status::unavailable("ingress activation is shutting down"));
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("ingress activation capacity exhausted"))?;
        // Dropping this JoinHandle only detaches the waiter. The task retains
        // the permit and runs the existing substrate owner through publication.
        // The waiter normally awaits it, so it runs under the waiter's span.
        self.tasks
            .spawn(
                async move {
                    let _permit = permit;
                    let result = backend.access(id).await;
                    if let Err(error) = &result {
                        tracing::warn!(sandbox_id = %id, %error, "ingress access activation failed");
                    }
                    result
                }
                .in_current_span(),
            )
            .await
            .map_err(|error| Status::internal(format!("ingress activation task failed: {error}")))?
    }
}

/// Reuse the API's controller, admission, caches and lifecycle task ownership.
pub(crate) fn ingress_activation(
    pool: PgPool,
    controller: SandboxController,
    tasks: TaskTracker,
    shutdown: CancellationToken,
) -> Arc<dyn IngressActivation> {
    Arc::new(Activator {
        backend: Arc::new(StoredAccess { pool, controller }),
        owners: ActivationOwners {
            tasks,
            slots: Arc::new(Semaphore::new(128)),
            shutdown,
        },
    })
}

#[tonic::async_trait]
impl AccessBackend for StoredAccess {
    async fn queue_implicit_wake(
        &self,
        id: SandboxId,
        activity: ExecutionActivityKind,
    ) -> Result<bool, Status> {
        let Some(sandbox) = fetch_sandbox(&self.pool, id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
        else {
            return Ok(false);
        };
        crate::routes::public::sessions::queue_implicit_wake(
            &self.pool,
            &sandbox,
            activity,
            crate::public_sandbox_operations::implicit_wake_activity_enabled(),
        )
        .await
        .map_err(|error| {
            Status::unavailable(format!("could not queue the command wake: {error:?}"))
        })
    }
    async fn observe(&self, id: SandboxId) -> Result<Observation, Status> {
        let sandbox = fetch_sandbox(&self.pool, id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("sandbox not found"))?;
        Ok(Observation {
            lifecycle: sandbox
                .lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|error| Status::internal(error.to_string()))?,
            access_wake: ExecutionActivityKind::Access
                .allows_implicit_wake(&sandbox.auto_lifecycle.0),
            restore_owned: pplx_space_sandbox_store::restore_owner_active(&self.pool, id)
                .await
                .map_err(|error| Status::internal(error.to_string()))?,
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
            self.controller
                .action(
                    &self.pool,
                    id,
                    Some(&sandbox),
                    SandboxApiMetricIdentity::native("ingress"),
                    super::lifecycle::SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
                    None,
                )
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
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
            Err(error)
                if error.retry_after_cause()
                    == Some(crate::controllers::ControllerRetryAfterCause::RestoreInFlight) =>
            {
                Ok(AccessOutcome::RestoreOwned)
            }
            // A policy refusal is terminal for this request, unlike a runtime
            // that is merely unavailable.
            Err(error) if error.status() == axum::http::StatusCode::CONFLICT => {
                Err(Status::failed_precondition(error.to_string()))
            }
            Err(error) => Err(Status::unavailable(error.to_string())),
        }
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
    async fn queue_implicit_wake(
        &self,
        id: SandboxId,
        activity: ImplicitWakeActivity,
        _timeout: Duration,
    ) -> Result<bool, Status> {
        if self.owners.shutdown.is_cancelled() {
            return Err(Status::unavailable("ingress activation is shutting down"));
        }
        let activity = match activity {
            ImplicitWakeActivity::Command => ExecutionActivityKind::Command,
            // Unspecified, as from a resolver that does not name it, is the
            // stricter rule.
            ImplicitWakeActivity::Access | ImplicitWakeActivity::Unspecified => {
                ExecutionActivityKind::Access
            }
        };
        self.backend.queue_implicit_wake(id, activity).await
    }

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
                } else {
                    if state.lifecycle == SandboxLifecycle::Started
                        && self.backend.reachable(id).await?
                    {
                        return Ok(());
                    }
                    pplx_space_metrics::internal_api::ingress_preparation_path(false);
                    match self.owners.access(self.backend.clone(), id).await? {
                        AccessOutcome::Ready => return Ok(()),
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
