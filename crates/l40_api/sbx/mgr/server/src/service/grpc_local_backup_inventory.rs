use super::*;

impl SandboxManagerService {
    pub(super) async fn list_pending_local_backups(
        &self,
        request: Request<ListPendingLocalBackupsRequest>,
    ) -> Result<Response<ListPendingLocalBackupsResponse>, Status> {
        self.authorize_peer(
            &request,
            "ListPendingLocalBackups",
            ALLOW_LOCAL_BACKUP_INVENTORY,
        )?;
        let pending = self
            .local_artifacts()
            .list_pending_backups()
            .map_err(|error| Status::internal(format!("list pending local backups: {error}")))?;
        Ok(Response::new(ListPendingLocalBackupsResponse {
            pending: pending
                .into_iter()
                .map(|pending| ProtoPendingLocalBackup {
                    sandbox_id: pending.sandbox_id,
                    local_artifact_id: pending.local_artifact_id.to_string(),
                    artifact_kind: crate::proto::ArtifactKind::from(pending.artifact_kind) as i32,
                })
                .collect(),
        }))
    }
}
