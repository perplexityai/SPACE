//! Admission and ownership ordering shared by every execution entrypoint.
use std::sync::{Arc, Mutex};
use tokio_util::{
    sync::CancellationToken,
    task::{TaskTracker, task_tracker::TaskTrackerToken},
};
use tonic::Status;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Running,
    Closing,
}

struct Inner {
    phase: Mutex<Phase>,
    operations: TaskTracker,
    closed: CancellationToken,
    cancelled: CancellationToken,
}

#[derive(Clone)]
pub(crate) struct Lifecycle(Arc<Inner>);

impl Lifecycle {
    pub(crate) fn new(cancelled: CancellationToken) -> Self {
        Self(Arc::new(Inner {
            phase: Mutex::new(Phase::Running),
            operations: TaskTracker::new(),
            closed: cancelled.child_token(),
            cancelled,
        }))
    }

    /// Register before the first await. Descendant work must be registered
    /// before this token is released, so closure cannot observe an empty gap.
    pub(crate) fn admit(&self) -> Result<TaskTrackerToken, Status> {
        let phase = self
            .0
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *phase != Phase::Running || self.0.cancelled.is_cancelled() {
            return Err(crate::refused(Status::unavailable("guest shutting down")));
        }
        Ok(self.0.operations.token())
    }

    pub(crate) fn close(&self) {
        let mut phase = self
            .0
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *phase == Phase::Running {
            *phase = Phase::Closing;
        }
        self.0.operations.close();
        self.0.closed.cancel();
    }

    pub(crate) fn closed(&self) -> CancellationToken {
        self.0.closed.clone()
    }

    pub(crate) fn cancel(&self) {
        self.close();
        self.0.cancelled.cancel();
    }

    pub(crate) async fn wait(&self) {
        self.0.operations.wait().await;
    }
}

/// The wait expired; cancellation was requested but owned work has not settled.
/// Keep the runtime alive and call drain again to observe physical completion.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("execution cleanup is incomplete; keep the runtime alive and drain again")]
pub struct IncompleteExecutionDrain;
