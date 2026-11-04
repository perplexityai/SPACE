use super::NodeManagerService;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tonic::Status;

/// A mutation owns its target until host work finishes, including kill/reap or
/// a non-interruptible ioctl. Blocking sections retain this owner too.
pub(super) struct ArtifactWork {
    pub(super) cancel: CancellationToken,
    _target: tokio::sync::OwnedMutexGuard<()>,
    _claim: Arc<crate::slot_pool::SandboxMutationClaim>,
    _permit: crate::lifecycle_limiter::LifecyclePermit,
}

#[cfg(test)]
mod tests;

impl ArtifactWork {
    pub(super) fn check(&self) -> Result<(), Status> {
        if self.cancel.is_cancelled() {
            Err(Status::cancelled("artifact work cancelled"))
        } else {
            Ok(())
        }
    }
}

impl NodeManagerService {
    async fn sandbox_artifact_lock(
        &self,
        sandbox_id: &str,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, Status> {
        let id = sandbox_id
            .parse::<super::SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("sandbox_id: {error}")))?;
        Ok(Self::sandbox_cgroup_family_lock(&self.sandbox_artifact_locks, &id.to_string()).await)
    }

    pub(super) async fn lock_sandbox_artifacts(
        &self,
        sandbox_id: &str,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, Status> {
        Ok(self
            .sandbox_artifact_lock(sandbox_id)
            .await?
            .lock_owned()
            .await)
    }

    /// Disconnect requests cancellation, not abandonment. The task cooperatively
    /// stops before its next phase. Cleanup waits for the same target; a retry
    /// rejects contention without parking with global capacity held.
    pub(super) async fn run_artifact_mutation<T, F, Fut>(
        &self,
        sandbox_id: &str,
        work: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, Status>> + Send + 'static,
    {
        super::validation::validate_sandbox_id(sandbox_id)?;
        let cancel = self.pool.shutdown_token.child_token();
        let _cancel_on_disconnect = cancel.clone().drop_guard();
        let permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::Filesystem)
            .await?;
        let target = self
            .sandbox_artifact_lock(sandbox_id)
            .await?
            .try_lock_owned()
            .map_err(|_| Status::unavailable("sandbox artifact work is in progress; retry"))?;
        let claim = self.claim_sandbox_mutation(sandbox_id)?;
        let owner = Arc::new(ArtifactWork {
            cancel,
            _target: target,
            _claim: claim,
            _permit: permit,
        });
        owner.check()?;
        let mut service = self.clone();
        service.artifact_work = Some(owner.clone());
        self.device_op_tasks
            .spawn(async move {
                let result = work(service).await;
                owner.check()?;
                result
            })
            .await
            .map_err(|error| Status::internal(format!("artifact task failed: {error}")))?
    }

    /// One bounded owner for an entire filesystem operation, including async
    /// children and blocking work after the RPC waiter disconnects. Cleanup and
    /// identity enumeration deliberately do not share this admission lane.
    pub(super) async fn run_owned_filesystem_work<T: Send + 'static>(
        &self,
        work: impl std::future::Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        let permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::Filesystem)
            .await?;
        self.device_op_tasks
            .spawn(async move {
                let _permit = permit;
                work.await
            })
            .await
            .map_err(|error| Status::internal(format!("filesystem task failed: {error}")))?
    }
}
