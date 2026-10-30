//! CH state uses the existing machine-state/memory artifact pair. Only the
//! contained VMM reads provider state; privileged code accepts a bounded, typed
//! configuration and stages fresh inodes through pinned directories.
use super::*;
use pplx_space_common_core::types::{NodeSpacePath, RuntimeKind};
use pplx_space_node_runtime_core::control::StateWorkspace;
use std::io::{Read, Write};

const MAX_MACHINE_STATE_BYTES: u64 = 16 * 1024 * 1024;

fn read_bounded(directory: &std::fs::File, name: &str) -> Result<Vec<u8>, Status> {
    let file = trusted_paths::open_regular_file(directory, name.as_ref())?;
    let mut bytes = Vec::new();
    file.take(MAX_MACHINE_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(artifact_fs::error)?;
    if bytes.len() as u64 > MAX_MACHINE_STATE_BYTES {
        return Err(Status::failed_precondition(
            "runtime machine state exceeds 16 MiB",
        ));
    }
    Ok(bytes)
}

fn memory_file(
    directory: &std::fs::File,
    name: &str,
    expected_bytes: u64,
) -> Result<std::fs::File, Status> {
    let file = trusted_paths::open_regular_file(directory, name.as_ref())?;
    if file.metadata().map_err(artifact_fs::error)?.len() != expected_bytes {
        return Err(Status::failed_precondition(
            "snapshot memory length differs from admitted guest memory",
        ));
    }
    Ok(file)
}

fn configuration(
    directory: &std::fs::File,
) -> Result<pplx_space_node_runtime_core::boot::StandaloneBoot, Status> {
    let bytes = read_bounded(directory, "vm-config.json")?;
    let text = std::str::from_utf8(&bytes).map_err(artifact_fs::error)?;
    pplx_space_node_runtime_core::boot::validate_standalone(
        RuntimeKind::CloudHypervisor,
        text,
        pplx_space_node_runtime_core::boot::BootSource::LinuxDirect,
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))
}

fn workspace(state: &std::fs::File, name: &str, uid: u32) -> Result<std::fs::File, Status> {
    let directory = trusted_paths::open_bind_directory(state, name.as_ref())?;
    for entry in artifact_fs::entries(&directory)? {
        artifact_fs::remove_at(&directory, &entry)?;
    }
    trusted_paths::reown(state, uid)?;
    trusted_paths::set_permissions(state, None, 0o700)?;
    trusted_paths::reown(&directory, uid)?;
    trusted_paths::set_permissions(&directory, None, 0o700)?;
    Ok(directory)
}

fn write_owned(
    directory: &std::fs::File,
    name: &str,
    bytes: &[u8],
    uid: u32,
) -> Result<(), Status> {
    let file = artifact_fs::write_atomic_at(directory, name.as_ref(), |mut file| {
        file.write_all(bytes).map_err(artifact_fs::error)
    })?;
    trusted_paths::reown(&file, uid)?;
    trusted_paths::set_permissions(&file, None, 0o600)
}

// The encrypted filesystem is bound beneath the shared state mount prepared
// before launch. All provider output and restore staging stays on that volume;
// no RAM or CPU-state artifact is staged on the plaintext cache filesystem.
fn state_workspace(
    jail: &std::fs::File,
    mount: Option<&NodeSpacePath>,
    uid: u32,
) -> Result<std::fs::File, Status> {
    let state = trusted_paths::open_bind_directory(jail, "state".as_ref())?;
    let Some(mount) = mount else {
        return Ok(state);
    };
    let volume = trusted_paths::open_directory(mount.as_path())?;
    let target = trusted_paths::open_bind_directory(&state, "encsnap".as_ref())?;
    use std::os::unix::fs::MetadataExt;
    let source_identity = volume.metadata().map_err(artifact_fs::error)?;
    let target_identity = target.metadata().map_err(artifact_fs::error)?;
    trusted_paths::reown(&volume, uid)?;
    if (source_identity.dev(), source_identity.ino())
        != (target_identity.dev(), target_identity.ino())
    {
        trusted_paths::bind_pinned_files(&volume, &target)?;
    }
    Ok(volume)
}

impl NodeManagerService {
    pub(super) async fn capture_cloud_hypervisor_state(
        &self,
        request: Request<StateCapture>,
        destination: CaptureDestination,
    ) -> Result<Response<CaptureVmStateResponse>, Status> {
        if matches!(&destination, CaptureDestination::Encrypted(_)) {
            let id = request.get_ref().sandbox_id.id();
            let guard = self.sandbox_device_stack_lock(&id).await.lock_owned().await;
            let service = self.clone();
            return run_detached(
                self.device_op_tasks.clone(),
                "encrypted CH capture",
                async move {
                    let _guard = guard;
                    let result = service
                        .capture_cloud_hypervisor_inner(request, destination)
                        .await;
                    let sandbox_id = id.to_string();
                    // Stop even after a partial capture. Closing the VMM and its
                    // mount namespace is required before the volume can be sealed.
                    let stopped = stop_runtime_for_encrypted_seal(&service, &sandbox_id).await;
                    let root = pplx_space_node_runtime_core::control::isolation_root(
                        RuntimeKind::CloudHypervisor,
                        &sandbox_id,
                    )
                    .map_err(|e| Status::internal(e.to_string()))?;
                    let drained = service
                        .off_runtime("drain CH encrypted snapshot bind", move || {
                            super::super::cleanup_helpers::drain_stale_mounts(
                                &root.join("state/encsnap"),
                                |path| unmount_if_present(path),
                            )
                            .map_err(Status::from)?;
                            Ok(())
                        })
                        .await;
                    stopped?;
                    drained?;
                    result
                },
            )
            .await?;
        }
        self.capture_cloud_hypervisor_inner(request, destination)
            .await
    }

    async fn capture_cloud_hypervisor_inner(
        &self,
        request: Request<StateCapture>,
        destination: CaptureDestination,
    ) -> Result<Response<CaptureVmStateResponse>, Status> {
        let req = request.into_inner();
        let (paths, mount, workspace_kind) = match destination {
            CaptureDestination::Plain(paths) => (Some(paths), None, StateWorkspace::Plain),
            CaptureDestination::Encrypted(mount) => (None, Some(mount), StateWorkspace::Encrypted),
        };
        let sandbox_id = req.sandbox_id.as_str().to_owned();
        let lease = self
            .pool
            .get(&sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::unavailable("snapshot lease is no longer active"))?;
        let root = pplx_space_node_runtime_core::control::isolation_root(
            RuntimeKind::CloudHypervisor,
            &sandbox_id,
        )
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let root_for_prepare = root.clone();
        let uid = lease.sandbox_uid;
        let (jail, state_root, capture, boot) = self
            .off_runtime("prepare CH capture workspace", move || {
                let jail = trusted_paths::open_directory(&root_for_prepare)?;
                let boot = configuration(&jail)?;
                let state_root = state_workspace(&jail, mount.as_ref(), uid)?;
                let capture = workspace(&state_root, "capture", uid)?;
                Ok((jail, state_root, capture, boot))
            })
            .await?;
        let client = self
            .runtime_control(RuntimeKind::CloudHypervisor, &sandbox_id)
            .await?;
        client
            .capture_state(workspace_kind, boot.memory_bytes())
            .await
            .map_err(|e| super::super::runtime_control::operation_error("CH capture", e))?;
        self.adopt_validated_lease(&sandbox_id, &lease)?;
        self.off_runtime("publish CH state artifacts", move || {
            let state: serde_json::Value =
                serde_json::from_slice(&read_bounded(&capture, "state.json")?)
                    .map_err(artifact_fs::error)?;
            let encoded = boot.capture_state(state).map_err(artifact_fs::error)?;
            if encoded.len() as u64 > MAX_MACHINE_STATE_BYTES {
                return Err(Status::failed_precondition(
                    "encoded runtime state exceeds 16 MiB",
                ));
            }
            let destination = paths.and_then(|paths| paths.dest_dir);
            let export = destination.is_some();
            let output = match destination {
                Some(path) => trusted_paths::create_directory(&path)?,
                None if workspace_kind == StateWorkspace::Encrypted => state_root,
                None => jail,
            };
            artifact_fs::write_atomic_at(&output, "vmstate".as_ref(), |mut file| {
                file.write_all(&encoded).map_err(artifact_fs::error)?;
                trusted_paths::set_permissions(file, Some(gids::SPACE_CACHE_RW), 0o660)
            })?;
            let memory = memory_file(&capture, "memory-ranges", boot.memory_bytes())?;
            let copied =
                crate::contained_copy::stage_open_file(&memory, &output, "memory".as_ref())
                    .map_err(artifact_fs::error)?;
            trusted_paths::set_permissions(&copied, Some(gids::SPACE_CACHE_RW), 0o660)?;
            if export {
                let rootfs = artifact_fs::copy(
                    &PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}/instance/rootfs.ext4")),
                    &output,
                    "rootfs.ext4".as_ref(),
                )?;
                trusted_paths::set_permissions(&rootfs, Some(gids::SPACE_CACHE_RW), 0o660)?;
                trusted_paths::set_permissions(&output, Some(gids::SPACE_CACHE_RW), 0o2770)?;
            }
            output.sync_all().map_err(artifact_fs::error)
        })
        .await?;
        Ok(Response::new(CaptureVmStateResponse {}))
    }

    pub(super) async fn restore_cloud_hypervisor_state(
        &self,
        request: Request<StateRestore>,
        lease: &SlotInfo,
        source: RestoreSource,
        rootfs_limiter: Option<pplx_space_common_core::types::RateLimiter>,
    ) -> Result<Response<RestoreVmStateResponse>, Status> {
        let req = request.into_inner();
        let (source, mount, workspace_kind) = match source {
            RestoreSource::Plain(paths) => (
                paths.source_dir.ok_or_else(|| {
                    Status::invalid_argument("CH restore requires a checkpoint directory")
                })?,
                None,
                StateWorkspace::Plain,
            ),
            RestoreSource::Encrypted(mount) => (
                mount.as_path().to_path_buf(),
                Some(mount),
                StateWorkspace::Encrypted,
            ),
        };
        let _device_guard = if workspace_kind == StateWorkspace::Encrypted {
            Some(
                self.sandbox_device_stack_lock(&req.sandbox_id.id())
                    .await
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };
        let id = req.sandbox_id.as_str().to_owned();
        let root = pplx_space_node_runtime_core::control::isolation_root(
            RuntimeKind::CloudHypervisor,
            &id,
        )
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let uid = lease.sandbox_uid;
        let memory_bytes = self
            .off_runtime("stage CH restore artifacts", move || {
                let source = trusted_paths::open_directory(&source)?;
                let jail = trusted_paths::open_directory(&root)?;
                let mut boot = configuration(&jail)?;
                if let Some(limiter) = rootfs_limiter {
                    boot.set_disk_rate(limiter);
                }
                let (config, state) = boot
                    .restore_state(&read_bounded(&source, "vmstate")?)
                    .map_err(|e| Status::failed_precondition(e.to_string()))?;
                let state_root = state_workspace(&jail, mount.as_ref(), uid)?;
                let restore = workspace(&state_root, "restore", uid)?;
                write_owned(
                    &restore,
                    "config.json",
                    &config.encode().map_err(artifact_fs::error)?,
                    uid,
                )?;
                write_owned(&restore, "state.json", &state, uid)?;
                // Use a private clone. Root never changes ownership of the source,
                // and restore cannot mutate the durable or shared cached artifact.
                let memory = memory_file(&source, "memory", config.memory_bytes())?;
                let copied = crate::contained_copy::stage_open_file(
                    &memory,
                    &restore,
                    "memory-ranges".as_ref(),
                )
                .map_err(artifact_fs::error)?;
                trusted_paths::reown(&copied, uid)?;
                trusted_paths::set_permissions(&copied, None, 0o600)?;
                write_owned(
                    &jail,
                    "vm-config.json",
                    &config.encode().map_err(artifact_fs::error)?,
                    uid,
                )?;
                restore.sync_all().map_err(artifact_fs::error)?;
                Ok(config.memory_bytes())
            })
            .await?;
        let client = self
            .runtime_control(RuntimeKind::CloudHypervisor, &id)
            .await?;
        let finish = async {
            client
                .restore_state(workspace_kind, memory_bytes)
                .await
                .map_err(|e| super::super::runtime_control::operation_error("CH restore", e))?;
            self.adopt_validated_lease(&id, lease)?;
            let socket = pplx_space_node_runtime_core::control::guest_socket_path(
                RuntimeKind::CloudHypervisor,
                &id,
            )
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
            self.off_runtime("publish restored guest socket", move || {
                super::super::permission_tree::rewrite(
                    &socket,
                    false,
                    Some(uid),
                    uid,
                    gids::SPACE_SBXMGR,
                )
                .map_err(artifact_fs::error)
            })
            .await?;
            Ok::<_, Status>(())
        }
        .await;
        if let Err(status) = finish {
            self.rollback_loaded_snapshot(&id, "CH restore completion", &status)
                .await;
            return Err(status);
        }
        Ok(Response::new(RestoreVmStateResponse {}))
    }
}
