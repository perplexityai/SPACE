use super::*;
use pplx_space_node_mgr_client::execution_fence::ExecutionFence;
use pplx_space_sandbox_model::SandboxActivationAttemptId;

impl NetworkPool {
    pub(crate) fn execution_fence(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<ExecutionFence>, SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        Ok(executions::execution_fence(&db, sandbox_id)?)
    }

    /// Caller holds the sandbox cgroup write gate across supervisor binding
    /// and this publication. Persist both identities before contacting the
    /// supervisor: cleanup can then recover either side of an interrupted bind.
    pub(crate) fn bind_execution(
        &self,
        sandbox_id: &str,
        fence: ExecutionFence,
    ) -> Result<(), SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        executions::bind_execution(&db, sandbox_id, fence)?;
        Ok(())
    }

    pub(crate) fn retire_execution(
        &self,
        sandbox_id: &str,
        expected: SandboxActivationAttemptId,
    ) -> Result<(), SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        executions::retire_execution(&db, sandbox_id, expected)?;
        Ok(())
    }
}
