//! The write that starts a resident resume: `paused → resuming` in the
//! node-local SQLite `sandboxes.state`.

use super::super::cleanup_support::require_state_write_from;
use super::*;

#[cfg(test)]
mod tests;

#[cfg(test)]
tokio::task_local! {
    /// Holds a resume between its pre-wake policy gate and this write, so a
    /// test can move the row in that window.
    static BEGIN_BARRIER: Arc<super::super::cleanup_executor::TestAdmissionBarrier>;
}

impl SandboxManagerService {
    /// Persist `resuming` for the `paused` row `resume_after_wake_lock`
    /// validated before its pre-wake policy gate.
    ///
    /// Stop takes neither the wake nor the artifact lock, and its runtime gate
    /// admits the paused VM because its `fc.pid` resolves, so Stop can settle
    /// `stopped` during the gate. Each branch
    /// therefore writes only over `paused`, and a resume that lost the race
    /// fails here, before any node-manager call: with an activation policy as
    /// `FailedPrecondition`, without one as `Aborted`.
    pub(super) async fn begin_resident_resume(
        &self,
        sandbox_id: &str,
        activation_policy: Option<(&AutoLifecyclePolicy, Option<ExecutionGeneration>)>,
    ) -> Result<(), Status> {
        #[cfg(test)]
        if let Ok(barrier) = BEGIN_BARRIER.try_with(Arc::clone) {
            barrier.entered.notify_one();
            let _permit = barrier.release.acquire().await.unwrap();
        }
        let db = self.db_conn()?;
        if let Some((policy, generation)) = activation_policy {
            if !crate::db::begin_resident_resume_with_policy(&db, sandbox_id, generation, policy)
                .map_err(|error| {
                    Status::internal(format!("resume policy admission failed: {error}"))
                })?
            {
                return Err(Status::failed_precondition(
                    "resident execution changed before resume",
                ));
            }
            return Ok(());
        }
        require_state_write_from(
            crate::db::update_state_from(&db, sandbox_id, &crate::db::edges::RESUME_BEGIN),
            "begin resident resume",
        )
    }
}
