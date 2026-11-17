//! Retained live disks are dependencies of a suspend, not snapshot contents.
use super::*;
use pplx_space_common_core::types::LocalArtifactId;
use pplx_space_execution_common::proto::{BlockVolumeBinding, BlockVolumeExecution};
use pplx_space_node_mgr_client::execution_fence::ExecutionFence;
use pplx_space_node_mgr_client::proto::{
    ActivateBlockVolumeRequest, PrepareBlockVolumeRequest, PublishBlockVolumeCheckpointRequest,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetainedLiveDisk {
    pub volume_id: uuid::Uuid,
    pub checkpoint_id: uuid::Uuid,
    pub disk_generation: u64,
    pub topology_version: u32,
}

impl RetainedLiveDisk {
    pub(super) fn for_capture(
        binding: &BlockVolumeBinding,
        checkpoint: LocalArtifactId,
    ) -> Result<Self, Status> {
        Ok(Self {
            volume_id: binding
                .fence
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("disk fence is missing"))?
                .volume_id
                .parse()
                .map_err(|_| Status::failed_precondition("invalid disk identity"))?,
            checkpoint_id: checkpoint
                .to_string()
                .parse()
                .map_err(|_| Status::internal("invalid checkpoint identity"))?,
            disk_generation: binding.disk_generation,
            topology_version: binding.topology_version,
        })
    }

    fn matches(&self, binding: &BlockVolumeBinding) -> bool {
        binding
            .fence
            .as_ref()
            .is_some_and(|f| f.volume_id == self.volume_id.to_string())
            && binding.disk_generation == self.disk_generation
            && binding.topology_version == self.topology_version
    }
}

impl SandboxManagerService {
    pub(super) fn has_retained_block(&self, id: &str) -> Result<bool, Status> {
        crate::db::read::sandbox_view(&*self.db_conn()?, id)
            .has_retained_block_volume()
            .map_err(|error| Status::internal(format!("read disk dependency: {error}")))
    }

    pub(super) fn refuse_retained_disk_operation(&self, id: &str) -> Result<(), Status> {
        if self.has_retained_block(id)? {
            return Err(Status::failed_precondition(
                "retained live disks support only their own latest suspend checkpoint; snapshot, fork, rollback and export are unsupported",
            ));
        }
        Ok(())
    }

    pub(super) async fn prepare_retained_block(
        &self,
        client: &mut NodeManagerGrpcClient,
        id: &str,
        fence: ExecutionFence,
        identity: &SpaceApiRouteIdentity,
    ) -> Result<Option<BlockVolumeBinding>, Status> {
        if !self.retained_block_volumes {
            self.refuse_retained_disk_operation(id)?;
            return Ok(None);
        }
        if identity.execution_generation.is_none() {
            let sandbox = crate::db::read::sandbox(&*self.db_conn()?, id)
                .map_err(|error| Status::internal(error.to_string()))?
                .ok_or_else(|| Status::not_found("disk preparation sandbox disappeared"))?;
            // Template donors and builder VMs have no control-plane execution
            // and cannot own experiment disks. Do not route their boot through
            // the control-plane disk authority just because this worker opts in.
            if matches!(
                sandbox.user.kind(),
                SandboxOwnerKind::TemplateManager | SandboxOwnerKind::Builder
            ) {
                self.refuse_retained_disk_operation(id)?;
                return Ok(None);
            }
        }
        let missing =
            || Status::failed_precondition("retained disk requires a control-plane execution");
        let execution = BlockVolumeExecution {
            node_id: self.local_node_id.ok_or_else(missing)?.to_string(),
            sandbox_id: id.to_owned(),
            execution_generation: identity
                .execution_generation
                .ok_or_else(missing)?
                .get()
                .try_into()
                .map_err(|_| missing())?,
            activation_attempt_id: identity
                .activation_attempt_id
                .ok_or_else(missing)?
                .to_string(),
        };
        // Admission is asynchronous. A pending AWS attach must not turn into a
        // healthy root-only guest, or block the public API's create response.
        let request = PrepareBlockVolumeRequest {
            execution: Some(execution),
            execution_fence: Some(fence.into()),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        loop {
            match client.prepare_block_volume(request.clone()).await {
                Ok(response) => {
                    let binding = response.into_inner().binding;
                    if binding.is_some() {
                        crate::db::remember_retained_block_volume(&*self.db_conn()?, id).map_err(
                            |error| Status::internal(format!("persist disk dependency: {error}")),
                        )?;
                    } else if self.has_retained_block(id)? {
                        return Err(Status::failed_precondition(
                            "retained disk dependency is missing",
                        ));
                    }
                    return Ok(binding);
                }
                Err(error)
                    if error.code() == tonic::Code::Unavailable
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) async fn activate_retained_block(
        &self,
        client: &mut NodeManagerGrpcClient,
        id: &str,
        fence: ExecutionFence,
        binding: Option<&BlockVolumeBinding>,
        checkpoint: Option<&RetainedLiveDisk>,
    ) -> Result<(), Status> {
        match (binding, checkpoint) {
            (None, None) => return Ok(()),
            (Some(binding), Some(checkpoint)) if checkpoint.matches(binding) => {}
            (Some(_), None) => {}
            _ => {
                return Err(Status::failed_precondition(
                    "checkpoint retained disk does not match",
                ));
            }
        }
        client
            .activate_block_volume(ActivateBlockVolumeRequest {
                sandbox_id: id.to_owned(),
                execution_fence: Some(fence.into()),
                checkpoint_id: checkpoint.map(|c| c.checkpoint_id.to_string()),
            })
            .await?;
        Ok(())
    }

    pub(super) async fn publish_retained_block(
        &self,
        client: &mut NodeManagerGrpcClient,
        sandbox: &crate::db::SandboxRow,
        checkpoint: LocalArtifactId,
        binding: Option<&BlockVolumeBinding>,
    ) -> Result<(), Status> {
        if binding.is_none() {
            return Ok(());
        }
        client
            .publish_block_volume_checkpoint(PublishBlockVolumeCheckpointRequest {
                sandbox_id: sandbox.id.clone(),
                execution_fence: sandbox.node_execution_fence(),
                checkpoint_id: checkpoint.to_string(),
            })
            .await?;
        Ok(())
    }

    pub(super) async fn mount_retained_block(
        &self,
        id: &str,
        binding: Option<&BlockVolumeBinding>,
    ) -> Result<(), Status> {
        let Some(binding) = binding else {
            return Ok(());
        };
        super::activation_support::wait_ready_via_spaced_host(
            id,
            Duration::from_secs(120),
            "retained-disk-mount",
        )
        .await?;
        let sandbox = crate::db::read::sandbox(&*self.db_conn()?, id)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("disk mount sandbox disappeared"))?;
        let client = super::spaced_bridge::connect_spaced_host_live(&sandbox).await?;
        client
            .mount_data_drive(host_proto::MountDataDriveRequest {
                topology_version: binding.topology_version,
                mount: binding.mount.clone(),
                read_only: binding.read_only,
                filesystem_uuid: binding.filesystem_uuid.clone(),
            })
            .await?;
        Ok(())
    }
}

pub(super) fn drive(
    binding: Option<&BlockVolumeBinding>,
) -> Option<pplx_space_execution_spec::GuestDataDrive> {
    binding.map(|b| pplx_space_execution_spec::GuestDataDrive {
        permissions: if b.read_only {
            pplx_space_execution_spec::BlockVolumePermissions::ReadOnly
        } else {
            pplx_space_execution_spec::BlockVolumePermissions::ReadWrite
        },
    })
}

pub(super) fn checkpoint(directory: &str) -> Result<Option<RetainedLiveDisk>, Status> {
    #[derive(serde::Deserialize)]
    struct Dependencies {
        retained_live_disk_v1: Option<RetainedLiveDisk>,
    }
    match std::fs::read(Path::new(directory).join("snapshot.json")) {
        Ok(bytes) => serde_json::from_slice::<Dependencies>(&bytes)
            .map(|metadata| metadata.retained_live_disk_v1)
            .map_err(|error| {
                Status::failed_precondition(format!("invalid checkpoint dependencies: {error}"))
            }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Status::internal(format!(
            "read checkpoint dependencies: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pplx_space_execution_common::proto::BlockVolumeFence;

    #[test]
    fn retained_block_volume_metadata_rejects_changed_disk_generation_or_topology() {
        let volume = uuid::Uuid::now_v7();
        let checkpoint_id = LocalArtifactId::new();
        let mut binding = BlockVolumeBinding {
            fence: Some(BlockVolumeFence {
                volume_id: volume.to_string(),
                ..Default::default()
            }),
            disk_generation: 3,
            topology_version: 1,
            ..Default::default()
        };
        let expected = RetainedLiveDisk::for_capture(&binding, checkpoint_id).unwrap();
        let directory = tempfile::tempdir().unwrap();
        assert!(
            checkpoint(directory.path().to_str().unwrap())
                .unwrap()
                .is_none()
        );
        let path = directory.path().join("snapshot.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "existing_rootfs_metadata": "preserved", "retained_live_disk_v1": expected
            }))
            .unwrap(),
        )
        .unwrap();
        let decoded = checkpoint(directory.path().to_str().unwrap())
            .unwrap()
            .unwrap();
        assert!(decoded.matches(&binding));
        binding.disk_generation += 1;
        assert!(!decoded.matches(&binding));
        binding.disk_generation -= 1;
        binding.topology_version += 1;
        assert!(!decoded.matches(&binding));
        binding.topology_version -= 1;
        binding.fence.as_mut().unwrap().volume_id = uuid::Uuid::now_v7().to_string();
        assert!(!decoded.matches(&binding));
        std::fs::write(&path, br#"{"retained_live_disk_v1":{}}"#).unwrap();
        checkpoint(directory.path().to_str().unwrap()).unwrap_err();
    }
}
