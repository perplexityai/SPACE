use super::*;
use pplx_space_common_core::types::SandboxActivationAttemptId;
use pplx_space_node_mgr_client::execution_fence::ExecutionFence;

impl NetworkPool {
    pub(crate) fn execution_fence(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<ExecutionFence>, SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let raw: Option<(String, Option<String>)> = db
            .query_row(
                "SELECT execution_id, previous_id FROM sandbox_executions WHERE sandbox_id = ?1",
                params![sandbox_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let parse = |raw: String| {
            raw.parse::<SandboxActivationAttemptId>().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        };
        raw.map(|(current, previous)| {
            Ok(ExecutionFence::new(
                parse(current)?,
                previous.map(parse).transpose()?,
            ))
        })
        .transpose()
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
        db.execute(
            "INSERT INTO sandbox_executions (sandbox_id, execution_id, previous_id) VALUES (?1, ?2, ?3)
             ON CONFLICT (sandbox_id) DO UPDATE SET execution_id = excluded.execution_id,
                                                   previous_id = excluded.previous_id",
            params![sandbox_id, fence.current().to_string(), fence.previous().map(|id| id.to_string())],
        )?;
        Ok(())
    }

    pub(crate) fn retire_execution(
        &self,
        sandbox_id: &str,
        expected: SandboxActivationAttemptId,
    ) -> Result<(), SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        db.execute(
            "DELETE FROM sandbox_executions WHERE sandbox_id = ?1 AND execution_id = ?2",
            params![sandbox_id, expected.to_string()],
        )?;
        Ok(())
    }
}
