use super::cleanup_helpers::*;
use super::disk::*;
use super::node_disk_metrics::SandboxDiskUsageLookup;
use super::validation::*;
use super::*;

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::core::host::linux::identities::gids;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

const BTRFS_SNAPSHOTS_DIR_NAME: &str = "snapshots";

fn prepare_btrfs_snapshot_source(instance_dir: &Path) -> Result<(), Status> {
    ensure_snapshot_export_permissions(instance_dir)
}

pub(super) fn btrfs_snapshot_tier_and_timestamp(name: &str) -> Option<(&str, &str)> {
    if name.starts_with("d-") || name.starts_with("f-") {
        return name
            .strip_prefix("d-")
            .or_else(|| name.strip_prefix("f-"))
            .map(|rest| rest.split_once('-').unwrap_or(("", rest)));
    }

    name.split_once('-')
}

pub(super) fn list_btrfs_snapshot_dirs_in(
    sandbox_root: &Path,
    budget: &mut DirectoryBudget,
) -> std::io::Result<Vec<PathBuf>> {
    budget.directories(&sandbox_root.join(BTRFS_SNAPSHOTS_DIR_NAME))
}

/// A failed/expired size probe cannot hide an enumerated identity used for
/// retention, deletion or rollback.
fn measure_snapshots(
    paths: Vec<PathBuf>,
    measurement_budget: Duration,
    mut measure: impl FnMut(&str, Duration) -> Result<Option<BtrfsDiskUsage>, String>,
) -> Vec<crate::proto::BtrfsSnapshotInfo> {
    let started = Instant::now();
    let mut snapshots = Vec::new();
    for path in paths {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some((tier, timestamp)) = btrfs_snapshot_tier_and_timestamp(name) else {
            continue;
        };
        let remaining = measurement_budget.saturating_sub(started.elapsed());
        let usage = if remaining.is_zero() {
            None
        } else {
            match measure(&path.to_string_lossy(), remaining) {
                Ok(usage) => usage,
                Err(error) => {
                    tracing::warn!(path = %path.display(), %error, "snapshot size unavailable; retaining inventory entry");
                    None
                }
            }
        };
        snapshots.push(crate::proto::BtrfsSnapshotInfo {
            name: name.to_owned(),
            tier: tier.to_owned(),
            timestamp: timestamp.to_owned(),
            exclusive_bytes: usage.as_ref().map(|usage| usage.exclusive_bytes),
            referenced_bytes: usage.map(|usage| usage.total_bytes),
        });
    }
    snapshots.sort_by(|a, b| a.name.cmp(&b.name));
    snapshots
}

#[cfg(target_os = "linux")]
pub(super) fn bind_mount_file(source: &Path, target: &Path) -> Result<(), Status> {
    nix::mount::mount(
        Some(source),
        target,
        None::<&Path>,
        nix::mount::MsFlags::MS_BIND,
        None::<&Path>,
    )
    .map_err(|error| {
        Status::internal(format!(
            "mount --bind {} {} failed: {error}",
            source.display(),
            target.display()
        ))
    })
}

#[cfg(not(target_os = "linux"))]
pub(super) fn bind_mount_file(source: &Path, target: &Path) -> Result<(), Status> {
    Err(Status::unimplemented(format!(
        "bind mount {} -> {} is only supported on Linux",
        source.display(),
        target.display()
    )))
}

/// Jail subdirectory the encrypted snapshot volume is bind-mounted at, so the
/// jailed Firecracker reads/writes vmstate+memory inside it. Kept short and
/// chroot-relative; the FC API paths below are `/{ENC_SNAPSHOT_JAIL_SUBDIR}/…`.
pub(super) const ENC_SNAPSHOT_JAIL_SUBDIR: &str = "encsnap";

/// Upper bound on how many stacked `encsnap` binds the seal path drains — a
/// restore leaves one, a re-suspend can transiently stack a second; the cap
/// guards against an unexpected unmount that never reports "not mounted".
const MAX_ENC_SNAPSHOT_BIND_DRAIN: usize = 8;

/// Retries of the post-capture runtime stop. Firecracker can still be in kernel
/// exit (KVM teardown) when the supervisor's cgroup removal gives up; the stop
/// is idempotent, so re-issuing it once the exit finishes completes the seal.
const ENC_CAPTURE_STOP_ATTEMPTS: usize = 8;
const ENC_CAPTURE_STOP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

/// File names Firecracker writes into (suspend) and reads from (restore) the
/// encrypted snapshot volume. Shared by the FC API paths and the load-side
/// re-own list so they cannot drift apart.
const ENC_SNAPSHOT_VMSTATE_NAME: &str = "vmstate";

const ENC_SNAPSHOT_MEMORY_NAME: &str = "memory";

/// Files the encrypted-snapshot **load** must re-own to the current sandbox
/// uid. The files inside the reopened volume carry the ownership of the
/// *suspending* run, and a wake allocates a fresh slot-derived uid — so
/// re-owning only the bind root lets the jailed Firecracker traverse the
/// directory but still fail opening `vmstate`/`memory` with EACCES
/// ("Failed to open snapshot file: Permission denied"). The suspend side
/// does not need this: Firecracker creates the files itself under the
/// then-current uid.
fn reown_encrypted_snapshot_inputs(volume: &std::fs::File, uid: u32) -> Result<(), Status> {
    use super::trusted_paths::{open_regular_file, reown};
    let vmstate = open_regular_file(volume, ENC_SNAPSHOT_VMSTATE_NAME.as_ref())?;
    let memory = open_regular_file(volume, ENC_SNAPSHOT_MEMORY_NAME.as_ref())?;
    reown(&vmstate, uid)?;
    reown(&memory, uid)?;
    reown(volume, uid)
}

/// Pin both ends of the encrypted-snapshot bind before re-owning the volume.
/// The jail root belongs to the sandbox uid, so its mountpoint is opened
/// relative to the pinned jail rather than looked up again by pathname.
fn pin_encrypted_snapshot_bind(
    mount_dir: &Path,
    jail_root: &Path,
    reown: impl FnOnce(&std::fs::File, u32) -> Result<(), Status>,
) -> Result<(std::fs::File, std::fs::File), Status> {
    let volume = super::trusted_paths::open_directory(mount_dir)?;
    let jail = super::trusted_paths::open_directory(jail_root)?;
    let uid = jail
        .metadata()
        .map_err(|e| Status::internal(e.to_string()))?
        .uid();
    let target =
        super::trusted_paths::open_bind_directory(&jail, ENC_SNAPSHOT_JAIL_SUBDIR.as_ref())?;
    reown(&volume, uid)?;
    Ok((volume, target))
}

fn stage_plaintext_snapshot_inputs(
    source_dir: &Path,
    jail_root: &Path,
) -> Result<(std::fs::File, std::fs::File, std::fs::File), Status> {
    let source = super::trusted_paths::open_directory(source_dir)?;
    let memory = super::trusted_paths::open_regular_file(&source, "memory".as_ref())?;
    let jail = super::trusted_paths::open_directory(jail_root)?;
    let vmstate_source = super::trusted_paths::open_regular_file(&source, "vmstate".as_ref())?;
    let vmstate =
        crate::contained_copy::stage_open_file(&vmstate_source, &jail, "vmstate".as_ref())
            .map_err(|e| Status::internal(format!("stage vmstate: {e}")))?;
    Ok((vmstate, memory, jail))
}

fn chgrp_path(path: &str, gid: u32) -> Result<(), Status> {
    std::os::unix::fs::chown(path, None, Some(gid))
        .map_err(|error| Status::internal(format!("chgrp {gid} {path} failed: {error}")))
}

fn chmod_path(path: &str, mode: u32) -> Result<(), Status> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|error| Status::internal(format!("chmod {mode:o} {path} failed: {error}")))
}

impl NodeManagerService {
    pub(super) async fn get_firecracker_vm_state(
        &self,
        request: Request<GetFirecrackerVmStateRequest>,
    ) -> Result<Response<GetFirecrackerVmStateResponse>, Status> {
        self.authorize_peer(&request, "GetFirecrackerVmState", ALLOW_NODE_STATUS)?;
        let sandbox_id = request
            .get_ref()
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let api_socket = format!("{JAIL_DIR}/firecracker/{sandbox_id}/root/firecracker.sock");
        let state = tokio::time::timeout(
            FIRECRACKER_VM_STATE_TIMEOUT,
            crate::fc_api::FcApiClient::new(api_socket).get_vm_state(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("Firecracker VM-state probe timed out"))?
        .map_err(|error| {
            Status::unavailable(format!("Firecracker VM-state probe failed: {error}"))
        })?;
        let state = match state {
            pplx_space_node_runtime_core::fc_api::FirecrackerVmState::NotStarted => {
                crate::proto::FirecrackerVmState::NotStarted
            }
            pplx_space_node_runtime_core::fc_api::FirecrackerVmState::Running => {
                crate::proto::FirecrackerVmState::Running
            }
            pplx_space_node_runtime_core::fc_api::FirecrackerVmState::Paused => {
                crate::proto::FirecrackerVmState::Paused
            }
        };
        Ok(Response::new(GetFirecrackerVmStateResponse {
            state: state.into(),
        }))
    }

    pub(super) async fn pause_sandbox(
        &self,
        request: Request<PauseSandboxRequest>,
    ) -> Result<Response<PauseSandboxResponse>, Status> {
        self.authorize_peer(&request, "PauseSandbox", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.into_inner();
        self.set_vm_paused(req.sandbox_id, req.execution_fence, true)
            .await?;
        Ok(Response::new(PauseSandboxResponse {}))
    }

    pub(super) async fn resume_sandbox(
        &self,
        request: Request<ResumeSandboxRequest>,
    ) -> Result<Response<ResumeSandboxResponse>, Status> {
        self.authorize_peer(&request, "ResumeSandbox", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.into_inner();
        self.set_vm_paused(req.sandbox_id, req.execution_fence, false)
            .await?;
        Ok(Response::new(ResumeSandboxResponse {}))
    }

    async fn set_vm_paused(
        &self,
        sandbox_id: String,
        fence: Option<crate::proto::ExecutionFence>,
        paused: bool,
    ) -> Result<(), Status> {
        validate_sandbox_id(&sandbox_id)?;
        let owner = sandbox_id.clone();
        let service = self.clone();
        self.run_owned_sandbox_work(&owner, async move {
            let _gate = service.lock_sandbox_vm_cgroup_launch(&sandbox_id).await;
            service.check_execution(&sandbox_id, fence.as_ref(), false)?;
            let api_socket = format!("{JAIL_DIR}/firecracker/{sandbox_id}/root/firecracker.sock");
            let client = crate::fc_api::FcApiClient::new(api_socket);
            set_firecracker_paused(&client, paused, VM_PAUSE_STATE_TIMEOUT).await
        })
        .await
    }

    pub(super) async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        self.authorize_peer(&request, "CreateSnapshot", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        validate_sandbox_id(&request.get_ref().sandbox_id)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        let service = self.clone();
        self.run_owned_sandbox_work(&sandbox_id, async move {
            let _vm_guard = service
                .lock_sandbox_vm_cgroup_launch(&request.get_ref().sandbox_id)
                .await;
            service.check_execution(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                false,
            )?;
            service.create_snapshot_inner(request).await
        })
        .await
    }

    async fn create_snapshot_inner(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;

        // BYOK encrypted snapshot: when an already-mounted encrypted volume is
        // supplied, the jailed Firecracker writes vmstate+memory straight into
        // it so they land compressed+encrypted from t0 (no plaintext copy, no
        // reflink to a plaintext dest_dir).
        if let Some(mount_dir) = req
            .snapshot_volume_mount_dir
            .as_deref()
            .filter(|p| !p.is_empty())
        {
            return self
                .create_encrypted_snapshot(&req.sandbox_id, mount_dir)
                .await;
        }

        let dest_dir = req.dest_dir.as_deref().filter(|path| !path.is_empty());
        let paths = crate::runtime_paths::RuntimePaths::firecracker().create_snapshot_paths(
            &req.sandbox_id,
            &req.snapshot_path,
            &req.mem_file_path,
            dest_dir,
        )?;
        let jail_root = format!("{JAIL_DIR}/firecracker/{}/root", req.sandbox_id);
        let api_socket = format!("{jail_root}/firecracker.sock");
        let client = crate::fc_api::FcApiClient::new(api_socket);
        let lease = self
            .pool
            .get(&req.sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::unavailable("snapshot lease is no longer active"))?;
        let total_started = Instant::now();
        tracing::debug!(
            sandbox_id = %req.sandbox_id,
            snapshot_path = %req.snapshot_path,
            mem_file_path = %req.mem_file_path,
            dest_dir = ?dest_dir,
            "node snapshot create started"
        );
        let phase = Instant::now();
        client
            .create_snapshot(&req.snapshot_path, &req.mem_file_path)
            .await
            .map_err(|e| Status::internal(format!("create snapshot failed: {e}")))?;
        self.adopt_validated_lease(&req.sandbox_id, &lease)?;
        tracing::debug!(
            sandbox_id = %req.sandbox_id,
            snapshot_path = %req.snapshot_path,
            mem_file_path = %req.mem_file_path,
            elapsed_ms = phase.elapsed().as_millis(),
            "node snapshot firecracker create complete"
        );

        // Reflink snapshot files from jail chroot to dest_dir (instant on btrfs).
        // The rootfs is bind-mounted from the instance subvolume — use the
        // instance path so the reflink source is always a regular file.
        // Offloaded: the reflink is a subprocess per file (with a full-copy
        // fallback) and the export chgrp is recursive — none of it belongs on
        // a runtime worker.
        if let Some(dest_dir) = paths.dest_dir {
            let jail_root = jail_root.clone();
            let vmstate_name = paths.vmstate_name.clone();
            let memory_name = paths.memory_name.clone();
            let sandbox_id = req.sandbox_id.clone();
            self.off_runtime("export snapshot artifacts", move || {
                let destination = super::trusted_paths::create_directory(&dest_dir)?;
                for (name, src_path) in [
                    ("vmstate", format!("{jail_root}/{vmstate_name}")),
                    ("memory", format!("{jail_root}/{memory_name}")),
                    (
                        "rootfs.ext4",
                        format!("{CACHE_DIR}/{sandbox_id}/instance/rootfs.ext4"),
                    ),
                ] {
                    let copied = super::artifact_fs::copy(
                        std::path::Path::new(&src_path),
                        &destination,
                        name.as_ref(),
                    )?;
                    super::trusted_paths::set_permissions(
                        &copied,
                        Some(pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW),
                        0o660,
                    )?;
                }
                super::trusted_paths::set_permissions(
                    &destination,
                    Some(pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW),
                    0o2770,
                )?;
                destination.sync_all().map_err(super::artifact_fs::error)
            })
            .await?;
        }

        tracing::debug!(
            sandbox_id = %req.sandbox_id,
            dest_dir = ?dest_dir,
            elapsed_ms = total_started.elapsed().as_millis(),
            "node snapshot create complete"
        );
        Ok(Response::new(CreateSnapshotResponse {}))
    }

    /// BYOK snapshot create: bind-mount the already-mounted encrypted volume
    /// into the jail and have Firecracker write vmstate+memory directly into
    /// it. Because the volume is btrfs(compress-force=zstd) over dm-crypt, the
    /// bytes are compressed-then-encrypted before they ever touch disk — there
    /// is no plaintext copy and no reflink to a plaintext `dest_dir`. The jail
    /// bind is removed before returning so the caller can unmount/seal the
    /// volume; the vmstate+memory files persist in the volume at `mount_dir`.
    async fn create_encrypted_snapshot(
        &self,
        sandbox_id: &str,
        mount_dir: &str,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let typed_sandbox_id = sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        super::trusted_paths::validate_snapshot_volume(sandbox_id, mount_dir)?;
        let jail_root = format!("{JAIL_DIR}/firecracker/{sandbox_id}/root");
        let jail_bind = format!("{jail_root}/{ENC_SNAPSHOT_JAIL_SUBDIR}");
        let api_socket = format!("{jail_root}/firecracker.sock");
        let lease = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::unavailable("snapshot lease is no longer active"))?;
        let fc_snapshot_path = format!("/{ENC_SNAPSHOT_JAIL_SUBDIR}/{ENC_SNAPSHOT_VMSTATE_NAME}");
        let fc_mem_file_path = format!("/{ENC_SNAPSHOT_JAIL_SUBDIR}/{ENC_SNAPSHOT_MEMORY_NAME}");

        // Serialize the whole capture per-sandbox on the SAME device-stack lock
        // prepare/release/cleanup take: the capture mutates the stable
        // per-sandbox jail path ({jail_root}/encsnap), so a deadline-cancelled
        // capture's detached task must exclude the retry's capture/prepare
        // until it finishes — sbx-mgr's wake lock is on the wrong side of the
        // RPC boundary to do that. The owned guard is MOVED INTO the detached
        // unit below, so cancelling the RPC future cannot release it early and
        // let the retry interleave. No lock-ordering inversion: the body's
        // stop-runtime (runtime supervisor) and bind drain take no device-stack
        // lock, so holding it across them cannot deadlock.
        let guard = self
            .sandbox_device_stack_lock(&typed_sandbox_id)
            .await
            .lock_owned()
            .await;

        // Bind -> FC snapshot -> unmount -> stop-runtime -> drain as ONE
        // detached unit holding the lock. `run_detached` spawns it, so it runs
        // to completion even if this RPC future is cancelled by the server
        // deadline: the task that CREATES the jail bind is always the one that
        // unmounts it and releases the lock, closing the window where a mount
        // is owned by neither this future nor cleanup AND the retry-races-the-
        // teardown window. The inner spawn_blocking keeps fork/exec/bind off
        // the async workers. Residual: only process death mid-task
        // can strand the bind, which the sweeper reconciles.
        let service = self.clone();
        let capture = {
            let jail_bind = jail_bind.clone();
            let sandbox_id = sandbox_id.to_string();
            let mount_dir = mount_dir.to_string();
            async move {
                let _guard = guard;
                let bind = {
                    tokio::task::spawn_blocking(move || -> Result<(), Status> {
                        let (volume, target) = pin_encrypted_snapshot_bind(
                            Path::new(&mount_dir),
                            Path::new(&jail_root),
                            super::trusted_paths::reown,
                        )?;
                        super::trusted_paths::bind_pinned_files(&volume, &target)
                    })
                    .await
                    .map_err(|e| {
                        Status::internal(format!("encrypted-snapshot bind task failed: {e}"))
                    })?
                };
                let create_result = match bind {
                    // On a re-suspend, `memory` is the correctly-sized image
                    // loaded by Firecracker. Firecracker 1.15 merges dirty
                    // pages into that existing file, so capture work scales
                    // with the dirty set while the result remains standalone.
                    // The first diff after a cold boot is standalone by the
                    // Firecracker contract and starts from an empty volume.
                    Ok(()) => crate::fc_api::FcApiClient::new(api_socket)
                        .create_snapshot(&fc_snapshot_path, &fc_mem_file_path)
                        .await
                        .map_err(|e| {
                            Status::internal(format!("create encrypted snapshot failed: {e}"))
                        }),
                    Err(error) => Err(error),
                };
                // ALWAYS unmount the bind, so the caller can seal the volume
                // whatever happened above; idempotent if the bind never formed.
                {
                    let jail_bind = jail_bind.clone();
                    let sandbox_id = sandbox_id.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        if let Err(e) = unmount_if_present(&jail_bind) {
                            tracing::warn!(
                                sandbox_id = %sandbox_id,
                                %e,
                                "failed to unmount encrypted-snapshot jail bind"
                            );
                        }
                    })
                    .await;
                }
                create_result?;
                service.adopt_validated_lease(&sandbox_id, &lease)?;

                // A restored sandbox's Firecracker holds the snapshot volume's
                // memory file mmap'd and the restore-time jail bind still
                // references it; both pin the dm-crypt device so the caller's
                // seal (cryptsetup close) would fail "device is still in use".
                // Stop the runtime to drop the mmap, then drain any remaining
                // encsnap bind, so the volume can be sealed. Both are no-ops for
                // a fresh capture (no restore pin).
                stop_runtime_for_encrypted_seal(&service, &sandbox_id).await?;
                let _ = tokio::task::spawn_blocking(move || {
                    for _ in 0..MAX_ENC_SNAPSHOT_BIND_DRAIN {
                        match unmount_if_present(&jail_bind) {
                            Ok(true) => continue,
                            Ok(false) => break,
                            Err(e) => {
                                tracing::warn!(
                                    sandbox_id = %sandbox_id,
                                    %e,
                                    "failed to drain encrypted-snapshot jail bind before seal"
                                );
                                break;
                            }
                        }
                    }
                })
                .await;
                Ok::<(), Status>(())
            }
        };
        run_detached(
            self.device_op_tasks.clone(),
            "encrypted-snapshot capture",
            capture,
        )
        .await??;

        tracing::debug!(
            sandbox_id,
            mount_dir,
            "encrypted snapshot written (vmstate+memory compressed+encrypted at t0)"
        );
        Ok(Response::new(CreateSnapshotResponse {}))
    }

    pub(super) async fn load_snapshot(
        &self,
        request: Request<LoadSnapshotRequest>,
    ) -> Result<Response<LoadSnapshotResponse>, Status> {
        self.authorize_peer(&request, "LoadSnapshot", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        validate_sandbox_id(&request.get_ref().sandbox_id)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        let service = self.clone();
        self.run_owned_sandbox_work(&sandbox_id, async move {
            let _vm_guard = service
                .lock_sandbox_vm_cgroup_launch(&request.get_ref().sandbox_id)
                .await;
            service.check_execution(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                false,
            )?;
            service.load_snapshot_inner(request).await
        })
        .await
    }

    async fn load_snapshot_inner(
        &self,
        request: Request<LoadSnapshotRequest>,
    ) -> Result<Response<LoadSnapshotResponse>, Status> {
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        let jail_root = format!("{JAIL_DIR}/firecracker/{}/root", req.sandbox_id);
        let api_socket = format!("{jail_root}/firecracker.sock");
        let _lifecycle_permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::LoadSnapshot)
            .await?;
        let lease = self
            .pool
            .get(&req.sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::unavailable("snapshot lease is no longer active"))?;

        // BYOK encrypted snapshot: bind-mount the already-mounted, decrypted
        // encrypted-snapshot volume into the jail and load vmstate+memory
        // directly from it. The volume stays mounted for the sandbox's life (FC
        // mmaps the memory file); cleanup releases it on delete. No source_dir
        // staging — the bytes never exist in plaintext on disk.
        if let Some(mount_dir) = req
            .snapshot_volume_mount_dir
            .as_deref()
            .filter(|p| !p.is_empty())
        {
            super::trusted_paths::validate_snapshot_volume(&req.sandbox_id, mount_dir)?;
            {
                let jail_root = jail_root.clone();
                let mount_dir = mount_dir.to_string();
                self.off_runtime("bind encrypted snapshot volume", move || {
                    let (volume, target) = pin_encrypted_snapshot_bind(
                        Path::new(&mount_dir),
                        Path::new(&jail_root),
                        reown_encrypted_snapshot_inputs,
                    )?;
                    super::trusted_paths::bind_pinned_files(&volume, &target)
                })
                .await?;
            }
            let fc_snapshot_path =
                format!("/{ENC_SNAPSHOT_JAIL_SUBDIR}/{ENC_SNAPSHOT_VMSTATE_NAME}");
            let fc_mem_file_path =
                format!("/{ENC_SNAPSHOT_JAIL_SUBDIR}/{ENC_SNAPSHOT_MEMORY_NAME}");
            self.finish_load_snapshot(
                &lease,
                req,
                &jail_root,
                &api_socket,
                &fc_snapshot_path,
                &fc_mem_file_path,
            )
            .await?;
            return Ok(Response::new(LoadSnapshotResponse {}));
        }

        let paths = crate::runtime_paths::RuntimePaths::firecracker().load_snapshot_paths(
            &req.sandbox_id,
            &req.snapshot_path,
            &req.mem_file_path,
            req.source_dir.as_deref(),
        )?;
        let fc_snapshot_path = format!("/{}", paths.vmstate_name);
        let fc_mem_file_path = format!("/{}", paths.memory_name);

        // Place snapshot files in the jail chroot from source_dir.
        // vmstate: small (~11 KB), reflink-copy into jail.
        // memory: large (~1 GB), bind-mount from source to avoid copying.
        //         FC has it mmap'd; writes trigger btrfs CoW on the source (which
        //         is a reflink clone of snapshot.base), so only dirty pages cost disk.
        if let Some(source_dir) = paths.source_dir.as_ref() {
            let source_dir = source_dir.clone();
            let jail_root = jail_root.clone();
            let memory_name = paths.memory_name.clone();
            self.off_runtime("stage snapshot load inputs", move || {
                let (vmstate, memory, jail) =
                    stage_plaintext_snapshot_inputs(&source_dir, Path::new(&jail_root))?;

                // Bind-mount memory (large) — same pattern as rootfs bind mount
                let memory_dst =
                    super::trusted_paths::open_bind_target(&jail, memory_name.as_ref())?;
                let uid = jail
                    .metadata()
                    .map_err(|e| Status::internal(e.to_string()))?
                    .uid();
                super::trusted_paths::reown(&vmstate, uid)?;
                super::trusted_paths::reown(&memory, uid)?;
                super::trusted_paths::bind_pinned_files(&memory, &memory_dst)?;
                Ok(())
            })
            .await?;
        }

        self.finish_load_snapshot(
            &lease,
            req,
            &jail_root,
            &api_socket,
            &fc_snapshot_path,
            &fc_mem_file_path,
        )
        .await?;
        Ok(Response::new(LoadSnapshotResponse {}))
    }

    /// Shared tail of the load-snapshot paths (plaintext source_dir and BYOK
    /// encrypted volume): wait for the FC API socket, load the snapshot with the
    /// given memory file, apply the sandbox's rootfs rate limit, then fix
    /// v.sock permissions.
    async fn finish_load_snapshot(
        &self,
        lease: &SlotInfo,
        request: &LoadSnapshotRequest,
        jail_root: &str,
        api_socket: &str,
        fc_snapshot_path: &str,
        fc_mem_file_path: &str,
    ) -> Result<(), Status> {
        let sandbox_id = request.sandbox_id.as_str();
        // Validate before the load, which cannot be undone short of killing
        // the VM.
        let rootfs_limiter = request
            .rootfs_rate_limit
            .as_ref()
            .map(drive_rate_limiter)
            .transpose()?;
        // Wait for the API socket to appear (FC needs time to create it after launch)
        let api_socket_wait_started = Instant::now();
        loop {
            if std::path::Path::new(api_socket).exists() {
                break;
            }
            if api_socket_wait_started.elapsed() >= FIRECRACKER_API_SOCKET_TIMEOUT {
                return Err(Status::internal(format!(
                    "FC API socket not found after 5s: {api_socket}"
                )));
            }
            tokio::time::sleep(FIRECRACKER_API_SOCKET_POLL).await;
        }
        tracing::debug!(
            sandbox_id,
            poll_interval_ms = FIRECRACKER_API_SOCKET_POLL.as_millis() as u64,
            elapsed_ms = api_socket_wait_started.elapsed().as_millis() as u64,
            "firecracker api socket observed"
        );

        let client = crate::fc_api::FcApiClient::new(api_socket.to_string());
        client
            .load_snapshot(fc_snapshot_path, fc_mem_file_path)
            .await
            .map_err(|e| Status::internal(format!("load snapshot failed: {e}")))?;
        // The loaded drive limiter is the one of the VM the snapshot was taken
        // from. Replace it with the sandbox's own while the VM is still
        // paused; a sandbox must not run under another VM's limits.
        if let Some(limiter) = &rootfs_limiter
            && let Err(error) = client.update_drive_rate_limiter("rootfs", limiter).await
        {
            let status = Status::internal(format!(
                "apply rootfs rate limit after snapshot load failed: {error}"
            ));
            self.rollback_loaded_snapshot(sandbox_id, "rootfs rate limit", &status)
                .await;
            return Err(status);
        }
        self.adopt_validated_lease(sandbox_id, lease)?;

        // Fix v.sock permissions so sbx-manager can connect for health checks.
        // In snapshot-restore mode, FC creates v.sock during load_snapshot (not at launch).
        let vsock_path = format!("{jail_root}/v.sock");
        if std::path::Path::new(&vsock_path).exists() {
            if let Err(status) = chgrp_path(&vsock_path, gids::SPACE_SBXMGR) {
                self.rollback_loaded_snapshot(sandbox_id, "vsock chgrp", &status)
                    .await;
                return Err(status);
            }
            if let Err(status) = chmod_path(&vsock_path, 0o770) {
                self.rollback_loaded_snapshot(sandbox_id, "vsock chmod", &status)
                    .await;
                return Err(status);
            }
        }

        Ok(())
    }

    /// Kill the VM a snapshot was loaded into when a later step of the load
    /// fails, so a retried load starts from a fresh VM.
    async fn rollback_loaded_snapshot(
        &self,
        sandbox_id: &str,
        failed_step: &'static str,
        status: &Status,
    ) {
        let stored_pid = match self.pool.get(sandbox_id) {
            Ok(info) => info.and_then(|info| info.pid),
            Err(error) => {
                tracing::error!(sandbox_id, %error, "snapshot rollback cannot read lease; checking runtime PID evidence");
                None
            }
        };
        let fc_pid =
            stored_pid.or_else(|| read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/fc.pid")));

        let Some(fc_pid) = fc_pid else {
            tracing::error!(
                sandbox_id,
                failed_step,
                error = %status,
                "unable to roll back loaded snapshot because firecracker pid was not found"
            );
            return;
        };

        tracing::warn!(
            sandbox_id,
            pid = fc_pid,
            failed_step,
            error = %status,
            "rolling back loaded snapshot after a failed load step"
        );
        kill_launched_process_after_pid_persist_failure(self, sandbox_id, fc_pid).await;
    }

    pub(super) async fn create_btrfs_snapshot(
        &self,
        request: Request<CreateBtrfsSnapshotRequest>,
    ) -> Result<Response<CreateBtrfsSnapshotResponse>, Status> {
        self.authorize_peer(&request, "CreateBtrfsSnapshot", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        self.run_artifact_mutation(&sandbox_id, |service| async move {
            service.create_btrfs_snapshot_inner(request).await
        })
        .await
    }

    async fn create_btrfs_snapshot_inner(
        &self,
        request: Request<CreateBtrfsSnapshotRequest>,
    ) -> Result<Response<CreateBtrfsSnapshotResponse>, Status> {
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        validate_snapshot_name(&req.snapshot_name)?;
        let gate = self.lock_sandbox_cgroups(&req.sandbox_id).await;
        self.check_filesystem_execution(&req.sandbox_id, req.execution_fence.as_ref())
            .await?;
        let instance_dir = format!("{CACHE_DIR}/{}/instance", req.sandbox_id);
        let snapshots_dir = format!("{CACHE_DIR}/{}/snapshots", req.sandbox_id);
        let snapshot_path = format!("{snapshots_dir}/{}", req.snapshot_name);
        let writable = req.writable;

        self.off_runtime("create btrfs snapshot", move || {
            let _gate = gate;
            if !std::path::Path::new(&instance_dir).exists() {
                return Err(Status::not_found(format!(
                    "instance dir not found: {instance_dir}"
                )));
            }
            prepare_btrfs_snapshot_source(Path::new(&instance_dir))?;
            let snapshots = super::trusted_paths::create_directory(Path::new(&snapshots_dir))?;
            super::trusted_paths::set_permissions(
                &snapshots,
                Some(pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW),
                0o2770,
            )?;
            let source = super::trusted_paths::open_directory(Path::new(&instance_dir))?;
            let name = Path::new(&snapshot_path)
                .file_name()
                .ok_or_else(|| super::artifact_fs::error("missing snapshot name"))?;
            super::artifact_fs::btrfs::snapshot(&source, &snapshots, name, !writable)
                .map_err(super::artifact_fs::error)?;
            Ok(Response::new(CreateBtrfsSnapshotResponse {}))
        })
        .await
    }

    pub(super) async fn delete_btrfs_snapshot(
        &self,
        request: Request<DeleteBtrfsSnapshotRequest>,
    ) -> Result<Response<DeleteBtrfsSnapshotResponse>, Status> {
        self.authorize_peer(&request, "DeleteBtrfsSnapshot", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        validate_snapshot_name(&req.snapshot_name)?;
        let snapshot_path = format!(
            "{CACHE_DIR}/{}/snapshots/{}",
            req.sandbox_id, req.snapshot_name
        );

        let guard = self.lock_sandbox_artifacts(&req.sandbox_id).await?;
        let claim = self.claim_sandbox_mutation(&req.sandbox_id)?;
        self.off_runtime("delete btrfs snapshot", move || {
            let _ownership = (guard, claim);
            if !std::path::Path::new(&snapshot_path).exists() {
                return Err(Status::not_found(format!(
                    "snapshot not found: {snapshot_path}"
                )));
            }

            match delete_cache_artifact_at(&snapshot_path) {
                Ok(true) => Ok(Response::new(DeleteBtrfsSnapshotResponse {})),
                Ok(false) => Err(Status::not_found(format!(
                    "snapshot not found: {snapshot_path}"
                ))),
                Err(error) => Err(Status::internal(format!(
                    "delete snapshot {snapshot_path}: {error}"
                ))),
            }
        })
        .await
    }

    /// Generic "delete this directory under /space/cache" RPC. The
    /// path may be a btrfs subvolume (callers without
    /// `CAP_SYS_ADMIN` can't `rmdir` a subvolume root) or a plain
    /// directory; the handler tries `btrfs subvolume delete` first
    /// and falls back to `remove_dir_all`. Idempotent: a missing
    /// path returns `was_present = false` rather than `NotFound`,
    /// because every caller in practice (snapshot reaper, named-
    /// snapshot delete, `.gc-pending-*` sweep) is doing a
    /// best-effort cleanup and doesn't care which actor won the
    /// race to delete.
    pub(super) async fn delete_cache_artifact(
        &self,
        request: Request<DeleteCacheArtifactRequest>,
    ) -> Result<Response<DeleteCacheArtifactResponse>, Status> {
        self.authorize_peer(&request, "DeleteCacheArtifact", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.get_ref();
        validate_cache_artifact_path(&req.local_path)?;
        let local_path = req.local_path.clone();
        let sandbox_id = std::path::Path::new(&local_path)
            .strip_prefix(CACHE_DIR)
            .ok()
            .and_then(|path| path.components().next())
            .and_then(|part| part.as_os_str().to_str())
            .filter(|id| id.parse::<SandboxId>().is_ok());
        let guard = match sandbox_id {
            Some(id) => Some(self.lock_sandbox_artifacts(id).await?),
            None => None,
        };
        // Capture/export can outlive its caller too. Exclude shared host work,
        // not just btrfs transfers, before unlinking any sandbox artifact.
        let claim = sandbox_id
            .map(|id| self.claim_sandbox_mutation(id))
            .transpose()?;

        self.off_runtime("delete cache artifact", move || {
            let _guard = guard;
            let _claim = claim;
            if !std::path::Path::new(&local_path).exists() {
                return Ok(Response::new(DeleteCacheArtifactResponse {
                    was_present: false,
                }));
            }

            delete_cache_artifact_at(&local_path)
                .map(|was_present| Response::new(DeleteCacheArtifactResponse { was_present }))
                .map_err(|error| {
                    Status::internal(format!("delete cache artifact {local_path}: {error}"))
                })
        })
        .await
    }

    pub(super) async fn list_btrfs_snapshots(
        &self,
        request: Request<ListBtrfsSnapshotsRequest>,
    ) -> Result<Response<ListBtrfsSnapshotsResponse>, Status> {
        self.authorize_peer(&request, "ListBtrfsSnapshots", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let sandbox_id: SandboxId =
            request.get_ref().sandbox_id.parse().map_err(|error| {
                Status::invalid_argument(format!("invalid sandbox id: {error}"))
            })?;
        let sandbox_root = Path::new(CACHE_DIR).join(sandbox_id.to_string());

        // Diagnostic capacity may suppress measurements, but must never reject
        // identity enumeration.
        let measurement_permit = if request.get_ref().skip_size_measurement {
            None
        } else {
            self.blocking_probe_slots.clone().try_acquire_owned().ok()
        };
        let req = request.into_inner();
        let inventory = self.snapshot_inventory.clone();
        let response = self
            .off_runtime("list btrfs snapshots", move || {
                // The blocking job retains admission even if its caller cancels.
                let permit = measurement_permit;
                let measurement_budget = if permit.is_some() {
                    DISK_MEASUREMENT_BUDGET
                } else {
                    Duration::ZERO
                };
                let (paths, next_page_token, elapsed) =
                    inventory.page(sandbox_id, &sandbox_root, &req.page_token)?;
                Ok(ListBtrfsSnapshotsResponse {
                    snapshots: measure_snapshots(
                        paths,
                        measurement_budget.saturating_sub(elapsed),
                        btrfs_du_exclusive,
                    ),
                    next_page_token,
                })
            })
            .await?;

        Ok(Response::new(response))
    }

    pub(super) async fn rollback_btrfs_snapshot(
        &self,
        request: Request<RollbackBtrfsSnapshotRequest>,
    ) -> Result<Response<RollbackBtrfsSnapshotResponse>, Status> {
        self.authorize_peer(
            &request,
            "RollbackBtrfsSnapshot",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        self.run_artifact_mutation(&sandbox_id, |service| async move {
            service.rollback_btrfs_snapshot_inner(request).await
        })
        .await
    }

    async fn rollback_btrfs_snapshot_inner(
        &self,
        request: Request<RollbackBtrfsSnapshotRequest>,
    ) -> Result<Response<RollbackBtrfsSnapshotResponse>, Status> {
        let req = request.get_ref();
        let sandbox_id = req.sandbox_id.clone();
        validate_sandbox_id(&sandbox_id)?;
        validate_snapshot_name(&req.snapshot_name)?;
        let snapshot_name = req.snapshot_name.clone();
        let gate = self.lock_sandbox_cgroups(&sandbox_id).await;
        self.check_filesystem_execution(&sandbox_id, req.execution_fence.as_ref())
            .await?;
        self.off_runtime("rollback btrfs snapshot", move || {
            let _gate = gate;
            if super::runtime::firecracker_before_jail_rebuild(&sandbox_id)?.is_some() {
                return Err(Status::failed_precondition(
                    "stop the runtime before disk rollback",
                ));
            }
            let sandbox = super::trusted_paths::open_directory(Path::new(&format!(
                "{CACHE_DIR}/{sandbox_id}"
            )))?;
            let snapshots = super::artifact_fs::directory(&sandbox, "snapshots".as_ref())
                .map_err(super::artifact_fs::error)?;
            let source = super::artifact_fs::directory(&snapshots, snapshot_name.as_ref())
                .map_err(super::artifact_fs::error)?;
            super::artifact_fs::rollback(&sandbox, &source, gids::SPACE_CACHE_RW)?;

            // Prune snapshots newer than the target
            let target_name = &snapshot_name;
            let mut pruned = Vec::new();
            {
                for entry in super::artifact_fs::entries(&snapshots)? {
                    let name = entry.to_string_lossy().to_string();
                    // Compare by timestamp portion (after the prefix and tier)
                    // Handles "d-1m-20260331T140000Z", "f-1m-...", or legacy "1m-..."
                    pub(super) fn extract_timestamp(s: &str) -> &str {
                        let rest = s
                            .strip_prefix("d-")
                            .or_else(|| s.strip_prefix("f-"))
                            .unwrap_or(s);
                        rest.split_once('-').map(|(_, ts)| ts).unwrap_or(s)
                    }
                    let target_ts = extract_timestamp(target_name);
                    let snap_ts = extract_timestamp(&name);
                    if snap_ts > target_ts
                        && super::artifact_fs::remove_at(&snapshots, &entry).is_ok()
                    {
                        pruned.push(name);
                    }
                }
            }

            Ok(Response::new(RollbackBtrfsSnapshotResponse {
                pruned_snapshots: pruned,
            }))
        })
        .await
    }

    pub(super) async fn get_disk_usage(
        &self,
        request: Request<GetDiskUsageRequest>,
    ) -> Result<Response<GetDiskUsageResponse>, Status> {
        self.authorize_peer(&request, "GetDiskUsage", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let ids = &request.get_ref().sandbox_ids;
        let mut usage_bytes = std::collections::HashMap::new();
        let mut missing_ids = Vec::new();
        for id in ids {
            let Ok(sandbox_id) = SandboxId::from_str(id) else {
                continue;
            };
            match self.disk_usage_cache.lookup(sandbox_id) {
                SandboxDiskUsageLookup::Usage(bytes) => {
                    usage_bytes.insert(id.clone(), bytes);
                }
                SandboxDiskUsageLookup::Missing => missing_ids.push(MissingDiskUsageId {
                    id: id.clone(),
                    reason: MissingDiskUsageReason::InstancePathMissing as i32,
                }),
                SandboxDiskUsageLookup::Unknown => {}
            }
        }
        Ok(Response::new(GetDiskUsageResponse {
            usage_bytes,
            missing_ids,
        }))
    }
}

/// Try `btrfs subvolume delete` first, fall back to
/// `remove_dir_all`. Returns `true` when the path was deleted,
/// `false` if it did not exist at delete time. Errors only when
/// both the subvolume-delete and the directory-walk fallback
/// failed to dispose of an existing path.
///
/// The two-step shape is unavoidable on a btrfs cache: a subvolume
/// root cannot be removed by an unprivileged caller via the usual
/// directory rmdir, so the fallback is needed for plain directories
/// (e.g. suspend snapshots, imported / cross-node staged dirs)
/// while the privileged path is needed for subvolumes (e.g.
/// `d-*` / `f-*` named snapshots and `d-stop-*` stopped artifacts
/// created via `create_btrfs_snapshot`).
///
/// After the leaf delete succeeds, walks up at most two parent
/// levels and removes any that have become empty as a result. The
/// walk is subvolume-aware (an empty parent that is itself a
/// subvolume root is removed via `btrfs subvolume delete`, since
/// a plain `rmdir` would EPERM on a subvol root even when empty)
/// and bounded by `SPACE_CACHE_DIR` so a runaway walk can't reach
/// `/space/cache` or any of its top-level children. The two-level
/// budget covers both shapes the reaper produces:
///
///   - suspend artifact: `<sbx>/snapshot/` → walk to `<sbx>/`.
///   - named/stopped snapshot: `<sbx>/snapshots/<name>/` →
///     `<sbx>/snapshots/` → `<sbx>/`.
///
/// If a parent is non-empty (another artifact, a live `instance/`
/// subvolume, etc.) the walk stops — the artifact deletion alone
/// is what the caller asked for and the parent's other content
/// owns the dir entry.
fn delete_cache_artifact_at(local_path: &str) -> std::io::Result<bool> {
    if !std::path::Path::new(local_path).exists() {
        return Ok(false);
    }
    delete_dir_btrfs_aware(local_path)?;
    // Best-effort empty-parent cleanup. Failures inside the walk
    // do not fail the RPC: the load-bearing contract is "leaf is
    // gone"; parent cleanup is a hygiene step. Errors at any step
    // get logged so a leaked empty dir is visible.
    prune_empty_parents(local_path, 2);
    Ok(true)
}

/// Delete through the shared bounded descriptor walk, including nested
/// subvolumes. Cache path replacements never become a recursive path fallback.
fn delete_dir_btrfs_aware(path: &str) -> std::io::Result<()> {
    super::artifact_fs::delete(std::path::Path::new(path)).map_err(std::io::Error::other)
}

/// Prune ordinary empty parents only. A failed atomic rmdir never grants
/// authority to recursively destroy a subvolume which may have new children.
fn prune_empty_parents(start_path: &str, max_levels: usize) {
    let cache_root = std::path::Path::new(CACHE_DIR);
    let mut current = std::path::Path::new(start_path);
    for _ in 0..max_levels {
        let Some(parent) = current.parent() else {
            return;
        };
        if parent == cache_root || !parent.starts_with(cache_root) {
            return;
        }
        if let Err(error) = super::artifact_fs::remove_empty_parent(parent) {
            tracing::debug!(path = %parent.display(), %error, "empty parent retained");
            return;
        }
        current = parent;
    }
}

/// Existing wire paths are restricted to snapshot artifacts and fork-copy
/// staging, not the entire cache. Containment at use time is descriptor-based.
fn validate_cache_artifact_path(local_path: &str) -> Result<(), Status> {
    let cache_root = CACHE_DIR;
    let prefix = format!("{cache_root}/");
    let Some(suffix) = local_path.strip_prefix(&prefix) else {
        return Err(Status::invalid_argument(format!(
            "local_path must start with `{prefix}`"
        )));
    };
    if suffix.is_empty() {
        return Err(Status::invalid_argument(
            "local_path must name an entry inside the cache root, not the root itself",
        ));
    }
    for component in suffix.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(Status::invalid_argument(
                "local_path must not contain empty, `.`, or `..` components",
            ));
        }
    }
    let mut parts = suffix.split('/');
    let owner = parts
        .next()
        .ok_or_else(|| Status::invalid_argument("missing artifact owner"))?;
    if owner != pplx_space_common_core::host::linux::paths::FORK_COPY_STAGING_DIR_NAME {
        super::trusted_paths::canonical_sandbox_id(owner)?;
    }
    let artifact = parts.next().ok_or_else(|| {
        Status::invalid_argument("artifact path must not name a sandbox or staging root")
    })?;
    let allowed = if owner == pplx_space_common_core::host::linux::paths::FORK_COPY_STAGING_DIR_NAME
    {
        uuid::Uuid::parse_str(artifact).is_ok()
    } else {
        artifact == "snapshot"
            || artifact.starts_with("snapshot.gc-pending-")
            || (matches!(artifact, "snapshots" | "template-snapshot-staging")
                && parts.next().is_some())
    };
    if !allowed {
        return Err(Status::permission_denied(
            "path is not a snapshot or fork-copy artifact",
        ));
    }
    Ok(())
}

fn drive_rate_limiter(
    limit: &pplx_space_node_mgr_client::proto::DriveRateLimit,
) -> Result<pplx_space_common_core::types::RateLimiter, Status> {
    let bucket = |bucket: Option<&pplx_space_node_mgr_client::proto::TokenBucket>, name: &str| {
        bucket
            .map(|bucket| pplx_space_common_core::types::TokenBucket {
                size: bucket.size,
                refill_time_ms: bucket.refill_time_ms,
            })
            .ok_or_else(|| {
                Status::invalid_argument(format!("rootfs_rate_limit.{name} is required"))
            })
    };
    Ok(pplx_space_common_core::types::RateLimiter {
        bandwidth: bucket(limit.bandwidth.as_ref(), "bandwidth")?,
        operations: bucket(limit.operations.as_ref(), "operations")?,
    })
}

/// How long a Firecracker pause or resume may take. Both normally finish in
/// milliseconds; a VMM that does not answer must not keep the sandbox's VM
/// gate, which termination also takes, held indefinitely.
const VM_PAUSE_STATE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Pause or resume the VM behind `client`, failing once `timeout` passes.
pub(super) async fn set_firecracker_paused(
    client: &crate::fc_api::FcApiClient,
    paused: bool,
    timeout: std::time::Duration,
) -> Result<(), Status> {
    let change = async {
        if paused {
            client.pause().await
        } else {
            client.resume().await
        }
    };
    match tokio::time::timeout(timeout, change).await {
        Ok(changed) => {
            changed.map_err(|error| Status::internal(format!("set VM pause state: {error}")))
        }
        Err(_) => Err(Status::deadline_exceeded(format!(
            "Firecracker did not {} the VM within {timeout:?}",
            if paused { "pause" } else { "resume" }
        ))),
    }
}

/// Stops the runtime before the encrypted seal, re-issuing the idempotent stop
/// while it fails server-side because the killed Firecracker is still exiting.
/// Any server-side `Internal` is retried, so a persistent cgroup error surfaces
/// only after the bounded attempts.
async fn stop_runtime_for_encrypted_seal(
    service: &NodeManagerService,
    sandbox_id: &str,
) -> Result<(), Status> {
    let mut attempt = 1;
    loop {
        match stop_sandbox_runtime_with_supervisor(service, sandbox_id, true, None).await {
            Ok(_) => return Ok(()),
            Err(error)
                if error.code() == tonic::Code::Internal && attempt < ENC_CAPTURE_STOP_ATTEMPTS =>
            {
                tracing::warn!(
                    sandbox_id,
                    attempt,
                    %error,
                    "post-capture runtime stop incomplete; retrying before seal"
                );
                attempt += 1;
                tokio::time::sleep(ENC_CAPTURE_STOP_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests;

/// Run `body` as a detached task so its own cleanup completes even if the
/// caller's await is dropped (RPC deadline cancellation). Returns the body's
/// output when the caller is not cancelled; if the caller IS dropped, `body`
/// still runs to completion in the background — the structural guarantee the
/// encrypted-snapshot capture relies on so a created jail bind is always
/// unmounted by the same unit.
async fn run_detached<F, T>(
    tracker: tokio_util::task::TaskTracker,
    label: &str,
    body: F,
) -> Result<T, Status>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tracker
        .spawn(body)
        .await
        .map_err(|e| Status::internal(format!("{label} task failed: {e}")))
}
