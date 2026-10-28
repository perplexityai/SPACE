//! Launch-bound recovery activation, serialized with snapshot preparation.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::{ProcessRecoveryToken, ReadyTimeout};
use pplx_space_spaced_common::{RetryContext, proto::bridge_guest};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::{GuestReadySnapshot, SpacedHostService, bridge::BridgeGuestOperationBudget, proto};

#[derive(Clone, Copy)]
enum Phase {
    Quiesced,
    Active,
}

struct Gate {
    barrier: ProcessRecoveryToken,
    phase: Phase,
}

pub(super) struct RecoveryLifecycle {
    activation: ProcessRecoveryToken,
    gate: Mutex<Gate>,
    transaction: Arc<Semaphore>,
}

impl RecoveryLifecycle {
    pub(super) fn new(activation: ProcessRecoveryToken) -> Self {
        Self {
            activation,
            gate: Mutex::new(Gate {
                barrier: ProcessRecoveryToken::new(),
                phase: Phase::Quiesced,
            }),
            transaction: Arc::new(Semaphore::new(1)),
        }
    }

    fn acquire(&self) -> Result<OwnedSemaphorePermit, Status> {
        Arc::clone(&self.transaction)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted("host recovery lifecycle operation in progress")
            })
    }
    async fn acquire_flush(&self) -> Result<OwnedSemaphorePermit, Status> {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.transaction.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::unavailable("recovery activation has not settled"))?
        .map_err(|_| Status::unavailable("recovery lifecycle is closed"))
    }
}

impl SpacedHostService {
    pub(super) fn require_recovery_fence(
        &self,
        fence: Option<pplx_space_spaced_common::recovery::RecoveryFence>,
    ) -> Result<(), Status> {
        let Some(fence) = fence else {
            return Ok(());
        };
        let lifecycle = self.inner.process_recovery.as_ref().ok_or_else(|| {
            pplx_space_common_grpc::guest_capability_unsupported_status(
                pplx_space_common_grpc::GuestCapability::ProcessRecoveryStart,
                "host has no recovery activation",
            )
        })?;
        if lifecycle.activation != fence.activation {
            return Err(
                pplx_space_spaced_common::recovery::RecoveryReason::ActivationChanged.into_status(),
            );
        }
        let gate = lifecycle
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match gate.phase {
            Phase::Active => Ok(()),
            Phase::Quiesced => Err(
                pplx_space_spaced_common::recovery::RecoveryReason::TemporarilyUnavailable
                    .into_status(),
            ),
        }
    }

    pub(super) fn supported_execution_info(
        &self,
        mut info: proto::ProcessExecutionInfo,
    ) -> proto::ProcessExecutionInfo {
        use pplx_space_common_grpc::GuestCapability;
        let active = self
            .inner
            .process_recovery
            .as_ref()
            .is_some_and(|lifecycle| {
                let gate = lifecycle
                    .gate
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                matches!(gate.phase, Phase::Active)
                    && info.activation_id.as_deref()
                        == Some(lifecycle.activation.to_string().as_str())
            });
        info.capabilities
            .retain(|capability| match capability.parse::<GuestCapability>() {
                Ok(GuestCapability::ProcessOutputGapResume) => true,
                Ok(
                    GuestCapability::ProcessRecoveryStart
                    | GuestCapability::ProcessRecoveryHandles
                    | GuestCapability::ProcessMutationReceipts,
                ) => active,
                _ => false,
            });
        info
    }

    pub(super) fn recovery_barrier(&self, guest: &GuestReadySnapshot) -> Option<String> {
        let lifecycle = self.inner.process_recovery.as_ref()?;
        guest.process_recovery_barrier_id.as_ref()?;
        let info = guest.process_execution.as_ref()?;
        let gate = lifecycle
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(gate.phase, Phase::Active)
            && info.activation_id.as_deref() == Some(lifecycle.activation.to_string().as_str())
        {
            None
        } else {
            Some(gate.barrier.to_string())
        }
    }

    pub(super) async fn quiesce_recovery(&self) -> Result<Option<OwnedSemaphorePermit>, Status> {
        let Some(lifecycle) = &self.inner.process_recovery else {
            return Ok(None);
        };
        let transaction = lifecycle.acquire_flush().await?;
        {
            let mut gate = lifecycle
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gate.barrier = ProcessRecoveryToken::new();
            gate.phase = Phase::Quiesced;
        }
        self.clear_guest_ready();
        Ok(Some(transaction))
    }

    pub(super) async fn resume_recovery(
        &self,
        request: proto::ResumeProcessRecoveryRequest,
        context: RetryContext,
    ) -> Result<(), Status> {
        let timeout = context
            .remaining()
            .unwrap_or(Duration::from_secs(20))
            .min(Duration::from_secs(20));
        tokio::time::timeout(timeout, self.resume_recovery_inner(request, context))
            .await
            .map_err(|_| Status::deadline_exceeded("process recovery activation timed out"))?
    }

    async fn resume_recovery_inner(
        &self,
        request: proto::ResumeProcessRecoveryRequest,
        context: RetryContext,
    ) -> Result<(), Status> {
        let expected = ProcessRecoveryToken::parse(&request.expected_barrier_id)
            .map_err(|error| Status::invalid_argument(format!("expected_barrier_id: {error}")))?;
        let lifecycle = self.inner.process_recovery.as_ref().ok_or_else(|| {
            Status::failed_precondition("host has no process recovery activation")
        })?;
        let _transaction = lifecycle.acquire()?;
        {
            let mut gate = lifecycle
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if gate.barrier != expected {
                return Err(Status::failed_precondition(
                    "host process recovery barrier changed",
                ));
            }
            // A lost reply must leave the host requiring lifecycle authorization.
            gate.phase = Phase::Quiesced;
        }
        self.clear_guest_ready();
        let guest = self
            .wait_guest_ready(
                ReadyTimeout::new(Duration::from_secs(10)).expect("bounded readiness timeout"),
                context,
            )
            .await?;
        let expected_barrier_id = guest.process_recovery_barrier_id.ok_or_else(|| {
            pplx_space_common_grpc::guest_capability_unsupported_status(
                pplx_space_common_grpc::GuestCapability::ProcessRecoveryStart,
                "guest does not support recovery activation",
            )
        })?;
        let instance = guest
            .process_execution
            .ok_or_else(|| Status::failed_precondition("guest has no execution identity"))?
            .instance_id;
        let activation = lifecycle.activation;
        let response = self
            .call_bridge_guest_once(
                "bridge_guest_activate_process_recovery",
                BridgeGuestOperationBudget::RuntimeMetadata,
                |mut client| async move {
                    client
                        .activate_process_recovery(bridge_guest::ActivateProcessRecoveryRequest {
                            expected_barrier_id: expected_barrier_id.to_string(),
                            activation_id: activation.to_string(),
                        })
                        .await
                },
            )
            .await?;
        let info = response.process_execution.ok_or_else(|| {
            Status::failed_precondition("guest did not confirm recovery activation")
        })?;
        if info.instance_id != instance
            || info.activation_id.as_deref() != Some(activation.to_string().as_str())
        {
            return Err(Status::failed_precondition(
                "guest recovery activation changed",
            ));
        }
        lifecycle
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = Phase::Active;
        self.clear_guest_ready();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
