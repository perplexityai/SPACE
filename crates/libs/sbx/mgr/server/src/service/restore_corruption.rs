use super::SandboxManagerService;
use crate::snapshot_restore::RestoreError;
use pplx_space_common_core::types::ExactStorageUri;

impl SandboxManagerService {
    pub(super) async fn reject_corrupt_restore_source(
        &self,
        sandbox_id: &str,
        source: &ExactStorageUri,
    ) -> Result<(), RestoreError> {
        let corrupt = {
            let db = self
                .db_conn()
                .map_err(|_| RestoreError::SandboxDatabaseMutexPoisoned)?;
            crate::db::restore_source_is_corrupt(&db, sandbox_id, source)
                .map_err(|source| RestoreError::CorruptionState { source })?
        };
        if corrupt {
            report_corrupt_restore_source(sandbox_id, source).await;
            return Err(RestoreError::CorruptArtifact);
        }
        Ok(())
    }

    pub(super) async fn record_restore_corruption(
        &self,
        sandbox_id: &str,
        source: &ExactStorageUri,
        error: &RestoreError,
    ) -> Result<(), RestoreError> {
        if !error.is_corrupt_artifact() {
            return Ok(());
        }
        // Commit locally before reporting: losing the control-plane response must
        // never cause another download of bytes already proven corrupt.
        {
            let db = self
                .db_conn()
                .map_err(|_| RestoreError::SandboxDatabaseMutexPoisoned)?;
            crate::db::record_corrupt_restore_source(&db, sandbox_id, source)
                .map_err(|source| RestoreError::CorruptionState { source })?;
        }
        report_corrupt_restore_source(sandbox_id, source).await;
        Ok(())
    }
}

async fn report_corrupt_restore_source(sandbox_id: &str, source: &ExactStorageUri) {
    let report = async {
        let mut client = pplx_space_node_api_client::cluster::connect_from_env()
            .await
            .ok()?;
        client
            .report_corrupt_snapshot(
                pplx_space_node_api_client::proto::ReportCorruptSnapshotRequest {
                    sandbox_id: sandbox_id.to_owned(),
                    storage_uri: source.as_wire().to_owned(),
                },
            )
            .await
            .ok()
    };
    if !matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), report).await,
        Ok(Some(_))
    ) {
        tracing::warn!(
            sandbox_id,
            "could not report corrupt snapshot; local restore retries remain blocked"
        );
    }
}
