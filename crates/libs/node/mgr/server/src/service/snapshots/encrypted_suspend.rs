//! Encrypted suspend: capture the runtime into the sandbox's encrypted volume
//! and stage its ciphertext as one operation, keyed by the capture's local
//! artifact id. Each step's outcome is recorded in the capture's staging
//! directory, so a retry resumes staging after a confirmed capture and never
//! captures over one whose outcome is unknown.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EncryptedSuspendCapturePhase {
    Prepared { fresh: bool },
    Captured,
    Ambiguous,
}

pub(super) fn encrypted_suspend_capture_phase(
    staging_dir: &Path,
) -> Result<EncryptedSuspendCapturePhase, Status> {
    let directory = super::trusted_paths::open_directory(staging_dir)?;
    let entries = super::artifact_fs::entries(&directory)?;
    if entries
        .iter()
        .any(|name| name == ENCRYPTED_SUSPEND_CAPTURE_RECORD)
    {
        return Ok(EncryptedSuspendCapturePhase::Captured);
    }
    if entries
        .iter()
        .any(|name| name == ENCRYPTED_SUSPEND_CAPTURE_STARTED)
    {
        return Ok(EncryptedSuspendCapturePhase::Ambiguous);
    }
    if entries
        .iter()
        .any(|name| name == ENCRYPTED_SUSPEND_CAPTURE_INTENT)
    {
        let intent = super::trusted_paths::open_regular_file(
            &directory,
            ENCRYPTED_SUSPEND_CAPTURE_INTENT.as_ref(),
        )?;
        let record: serde_json::Value = serde_json::from_reader(intent).map_err(|error| {
            Status::failed_precondition(format!("invalid encrypted capture intent: {error}"))
        })?;
        let fresh = match record
            .get("encrypted_capture_base")
            .and_then(serde_json::Value::as_str)
        {
            Some("fresh") => true,
            Some("existing") => false,
            Some(value) => {
                return Err(Status::failed_precondition(format!(
                    "unknown encrypted capture base {value:?}"
                )));
            }
            None => false,
        };
        return Ok(EncryptedSuspendCapturePhase::Prepared { fresh });
    }
    Err(Status::failed_precondition(
        "encrypted suspend capture has no durable intent",
    ))
}

pub(super) fn transition_encrypted_suspend_capture(
    staging_dir: &Path,
    source_name: &'static str,
    destination_name: &'static str,
) -> Result<(), Status> {
    let directory = super::trusted_paths::open_directory(staging_dir)?;
    let source = super::trusted_paths::open_regular_file(&directory, source_name.as_ref())?;
    if source
        .metadata()
        .map_err(super::artifact_fs::error)?
        .nlink()
        != 1
    {
        return Err(Status::failed_precondition(
            "encrypted suspend capture record is multiply linked",
        ));
    }
    nix::fcntl::renameat(&directory, source_name, &directory, destination_name)
        .map_err(super::artifact_fs::error)?;
    directory.sync_all().map_err(super::artifact_fs::error)
}

impl NodeManagerService {
    pub(in crate::service) async fn create_encrypted_suspend_snapshot(
        &self,
        request: Request<CreateEncryptedSuspendSnapshotRequest>,
    ) -> Result<Response<CreateEncryptedSuspendSnapshotResponse>, Status> {
        self.authorize_peer(
            &request,
            "CreateEncryptedSuspendSnapshot",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = request
            .get_ref()
            .sandbox_id
            .trim()
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("sandbox_id: {error}")))?;
        let local_artifact_id = request
            .get_ref()
            .local_artifact_id
            .trim()
            .parse::<LocalArtifactId>()
            .map_err(|error| Status::invalid_argument(format!("local_artifact_id: {error}")))?;
        let fresh = match EncryptedSuspendSnapshotBase::try_from(request.get_ref().base) {
            Ok(EncryptedSuspendSnapshotBase::Fresh) => true,
            Ok(EncryptedSuspendSnapshotBase::Existing) => false,
            Ok(EncryptedSuspendSnapshotBase::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument(
                    "encrypted suspend snapshot base must be fresh or existing",
                ));
            }
        };
        let execution_fence = request.get_ref().execution_fence.clone();
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox_id.to_string(),
            crate::lifecycle_limiter::LifecycleMethod::Release,
            async move {
                service
                    .create_encrypted_suspend_snapshot_inner(
                        sandbox_id,
                        local_artifact_id,
                        fresh,
                        execution_fence,
                    )
                    .await
            },
        )
        .await
    }

    async fn create_encrypted_suspend_snapshot_inner(
        &self,
        sandbox_id: SandboxId,
        local_artifact_id: LocalArtifactId,
        fresh: bool,
        execution_fence: Option<crate::proto::ExecutionFence>,
    ) -> Result<Response<CreateEncryptedSuspendSnapshotResponse>, Status> {
        self.check_execution(&sandbox_id.to_string(), execution_fence.as_ref(), false)?;
        let staging_dir = PathBuf::from(
            pplx_space_common_core::host::linux::paths::sandbox_suspend_staging_dir(
                &sandbox_id.to_string(),
                local_artifact_id,
            ),
        );
        let phase_dir = staging_dir.clone();
        let phase = self
            .off_runtime("inspect encrypted suspend capture", move || {
                encrypted_suspend_capture_phase(&phase_dir)
            })
            .await?;
        match phase {
            EncryptedSuspendCapturePhase::Ambiguous => {
                return Err(Status::data_loss(
                    "encrypted suspend capture outcome is ambiguous; refusing to overwrite it",
                ));
            }
            EncryptedSuspendCapturePhase::Prepared { fresh: recorded } => {
                if recorded != fresh {
                    return Err(Status::failed_precondition(
                        "encrypted suspend snapshot base changed across retries",
                    ));
                }
                self.prepare_encrypted_volume_inner(
                    sandbox_id,
                    PrepareEncryptedVolumeRequest {
                        sandbox_id: sandbox_id.to_string(),
                        volume_name: pplx_space_common_core::host::linux::paths::ENCRYPTED_SUSPEND_VOLUME_NAME.to_string(),
                        size_bytes: pplx_space_common_core::host::linux::paths::ENCRYPTED_SUSPEND_VOLUME_SIZE_BYTES,
                        fresh,
                        restore_backing_source: None,
                        execution_fence: execution_fence.clone(),
                    },
                )
                .await?;
                let marker_dir = staging_dir.clone();
                if let Err(error) = self
                    .off_runtime("begin encrypted suspend capture", move || {
                        transition_encrypted_suspend_capture(
                            &marker_dir,
                            ENCRYPTED_SUSPEND_CAPTURE_INTENT,
                            ENCRYPTED_SUSPEND_CAPTURE_STARTED,
                        )
                    })
                    .await
                {
                    let marker_dir = staging_dir.clone();
                    self.off_runtime_best_effort(move || {
                        let _ = transition_encrypted_suspend_capture(
                            &marker_dir,
                            ENCRYPTED_SUSPEND_CAPTURE_STARTED,
                            ENCRYPTED_SUSPEND_CAPTURE_INTENT,
                        );
                    })
                    .await;
                    return Err(error);
                }

                let mount_dir = super::encrypted_volumes::volume_mount_dir(&sandbox_id, "snapshot");
                let captured_lease = match self
                    .capture_encrypted_suspend_state(sandbox_id, mount_dir, execution_fence.clone())
                    .await
                {
                    Ok(lease) => lease,
                    Err(EncryptedSnapshotCaptureFailure::NotAttempted(status)) => {
                        let marker_dir = staging_dir.clone();
                        if let Err(marker_error) = self
                            .off_runtime("abort encrypted suspend capture", move || {
                                transition_encrypted_suspend_capture(
                                    &marker_dir,
                                    ENCRYPTED_SUSPEND_CAPTURE_STARTED,
                                    ENCRYPTED_SUSPEND_CAPTURE_INTENT,
                                )
                            })
                            .await
                        {
                            tracing::warn!(
                                %sandbox_id,
                                %marker_error,
                                "failed to restore encrypted capture intent"
                            );
                        }
                        return Err(status);
                    }
                    Err(EncryptedSnapshotCaptureFailure::OutcomeUnknown(status)) => {
                        return Err(status);
                    }
                };

                let marker_dir = staging_dir.clone();
                self.off_runtime("commit encrypted suspend capture", move || {
                    transition_encrypted_suspend_capture(
                        &marker_dir,
                        ENCRYPTED_SUSPEND_CAPTURE_STARTED,
                        ENCRYPTED_SUSPEND_CAPTURE_RECORD,
                    )
                })
                .await?;
                if let Some(lease) = captured_lease {
                    self.adopt_validated_lease(&sandbox_id.to_string(), &lease)?;
                }
            }
            EncryptedSuspendCapturePhase::Captured => {}
        }

        self.stage_retained_encrypted_suspend_under_ownership(
            sandbox_id,
            local_artifact_id,
            fresh,
            execution_fence,
        )
        .await?;
        Ok(Response::new(CreateEncryptedSuspendSnapshotResponse {}))
    }

    /// Capture a suspend into the sandbox's encrypted volume. Firecracker stays
    /// resident and returns the lease to adopt once the capture is durably
    /// recorded. Cloud Hypervisor's encrypted capture stops the VM, as it
    /// does for any encrypted CH capture, and adopts its own lease.
    async fn capture_encrypted_suspend_state(
        &self,
        sandbox_id: SandboxId,
        mount_dir: PathBuf,
        execution_fence: Option<crate::proto::ExecutionFence>,
    ) -> Result<Option<SlotInfo>, EncryptedSnapshotCaptureFailure> {
        use pplx_space_common_core::types::RuntimeKind;

        let runtime = self
            .recorded_execution(sandbox_id.to_string())
            .await
            .map_err(EncryptedSnapshotCaptureFailure::NotAttempted)?
            .map_or(RuntimeKind::Firecracker, |execution| {
                execution.runtime().kind
            });
        if runtime != RuntimeKind::CloudHypervisor {
            return self
                .capture_encrypted_snapshot_files(sandbox_id, mount_dir)
                .await
                .map(|(_, lease)| Some(lease));
        }
        let artifact = |name: &str, relative_path: &str| crate::proto::VmStateArtifact {
            name: name.to_string(),
            relative_path: relative_path.to_string(),
        };
        let capture =
            crate::snapshot_request::StateCapture::try_from(crate::proto::CaptureVmStateRequest {
                sandbox_id: sandbox_id.to_string(),
                environment_reference: None,
                artifacts: vec![
                    artifact("machine-state", ENC_SNAPSHOT_VMSTATE_NAME),
                    artifact("memory", ENC_SNAPSHOT_MEMORY_NAME),
                ],
                storage: Some(crate::proto::VmStateStorage {
                    directory: None,
                    encrypted_volume_mount: Some(mount_dir.to_string_lossy().into_owned()),
                }),
                execution_fence,
            })
            .map_err(EncryptedSnapshotCaptureFailure::NotAttempted)?;
        let SnapshotStorage::Encrypted(mount) = capture.storage.clone() else {
            return Err(EncryptedSnapshotCaptureFailure::NotAttempted(
                Status::internal("encrypted suspend capture lost its volume"),
            ));
        };
        super::trusted_paths::validate_snapshot_volume(
            &sandbox_id.to_string(),
            &mount_dir.to_string_lossy(),
        )
        .map_err(EncryptedSnapshotCaptureFailure::NotAttempted)?;
        self.capture_encrypted_cloud_hypervisor_state(
            Request::new(capture),
            CaptureDestination::Encrypted(mount),
        )
        .await
        .map(|_| None)
        .map_err(EncryptedSnapshotCaptureFailure::OutcomeUnknown)
    }
}
