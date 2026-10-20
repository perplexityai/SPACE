use super::SandboxManagerService;
use crate::snapshot_restore::RestoreError;
use pplx_space_common_core::types::ExactStorageUri;

impl SandboxManagerService {
    pub(super) fn quarantine_corrupt_restore(
        &self,
        sandbox_id: &str,
        source: &ExactStorageUri,
        scope: &crate::db::RestoreTransferScopeRow,
        recovery_claim: Option<&crate::db::ClaimedQuarantineRecoveryCandidate>,
    ) -> Result<(), RestoreError> {
        let changed = {
            let db = self
                .db_conn()
                .map_err(|_| RestoreError::SandboxDatabaseMutexPoisoned)?;
            crate::db::quarantine_corrupt_restore(&db, sandbox_id, source, scope, recovery_claim)
                .map_err(|source| RestoreError::QuarantineCorruptArtifact { source })?
        };
        if !changed {
            return Err(RestoreError::Other(
                "corrupt restore attempt was superseded before quarantine".into(),
            ));
        }
        crate::node_state_sync::mark_sandbox_dirty(&self.db, sandbox_id);
        Ok(())
    }
}
