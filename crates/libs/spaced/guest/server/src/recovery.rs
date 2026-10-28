//! Serialize recovery activation with snapshot preparation, including delayed RPCs.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::ProcessRecoveryToken;
use pplx_space_spaced_common::proto::bridge_guest::{
    ActivateProcessRecoveryRequest, ActivateProcessRecoveryResponse,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::SpacedGuestService;

struct Gate {
    barrier: ProcessRecoveryToken,
    activation: Option<ProcessRecoveryToken>,
}

pub(super) struct RecoveryLifecycle {
    gate: Mutex<Gate>,
    transaction: Arc<Semaphore>,
}

impl RecoveryLifecycle {
    pub(super) fn new() -> Self {
        Self {
            gate: Mutex::new(Gate {
                barrier: ProcessRecoveryToken::new(),
                activation: None,
            }),
            transaction: Arc::new(Semaphore::new(1)),
        }
    }

    pub(super) fn barrier(&self) -> ProcessRecoveryToken {
        self.gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .barrier
    }

    fn acquire(&self) -> Result<OwnedSemaphorePermit, Status> {
        Arc::clone(&self.transaction)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted("guest recovery lifecycle operation in progress")
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

impl SpacedGuestService {
    pub(super) async fn activate_recovery(
        &self,
        request: ActivateProcessRecoveryRequest,
    ) -> Result<ActivateProcessRecoveryResponse, Status> {
        let barrier = ProcessRecoveryToken::parse(&request.expected_barrier_id)
            .map_err(|error| Status::invalid_argument(format!("expected_barrier_id: {error}")))?;
        let activation = ProcessRecoveryToken::parse(&request.activation_id)
            .map_err(|error| Status::invalid_argument(format!("activation_id: {error}")))?;
        let lifecycle = &self.inner.process_recovery;
        let _transaction = lifecycle.acquire()?;
        {
            let gate = lifecycle
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if gate.barrier != barrier {
                return Err(Status::failed_precondition(
                    "process recovery barrier changed",
                ));
            }
            if gate.activation.is_some_and(|current| current != activation) {
                return Err(Status::failed_precondition(
                    "quiesce before changing process recovery activation",
                ));
            }
        }
        self.inner
            .execution
            .configure_process_recovery(Some(activation))
            .await?;
        lifecycle
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .activation = Some(activation);
        Ok(ActivateProcessRecoveryResponse {
            process_execution: Some(self.inner.execution.process_execution_info()),
        })
    }

    pub(super) async fn flush_with_recovery_barrier(
        &self,
        flush: impl FnOnce() -> Result<(), Status> + Send + 'static,
    ) -> Result<(), Status> {
        let lifecycle = &self.inner.process_recovery;
        let transaction = lifecycle.acquire_flush().await?;
        let active = {
            let mut gate = lifecycle
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Invalidate before waiting: even an abandoned preparation rejects
            // activation RPCs dispatched against its previous barrier.
            gate.barrier = ProcessRecoveryToken::new();
            gate.activation.is_some()
        };
        if active {
            self.inner
                .execution
                .configure_process_recovery(None)
                .await?;
            lifecycle
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .activation = None;
        }
        crate::meta::run_filesystem_flush(self.inner.filesystem_flush_slots.clone(), move || {
            // syncfs outlives a cancelled caller. Keep activation excluded until
            // the flush actually settles, not merely until its waiter drops.
            let _transaction = transaction;
            flush()
        })
        .await
    }
}

#[cfg(test)]
mod tests;
