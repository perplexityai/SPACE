//! Operator-only live growth of a Firecracker VM's root disk.
//!
//! A sandbox whose disk fills up cannot otherwise get more space: the size is
//! fixed when the sandbox is created. This grows the root disk of a running or
//! paused VM in place, without a reboot:
//!
//! 1. extend the instance `rootfs.ext4` file (sparse, so no data is written);
//! 2. ask Firecracker to re-read the drive, which updates the virtio-blk
//!    capacity and notifies the guest kernel.
//!
//! The guest filesystem is not touched here. The operator grows it from inside
//! the guest afterwards (for ext4: `resize2fs /dev/vda`, which works online).
//!
//! The recorded sandbox size is deliberately left unchanged. Wakes, forks,
//! clones and cross-node transfers reuse the existing disk file as-is and never
//! resize it again; changing the record would instead make a replayed create
//! look incompatible. Encrypted (dm-crypt) root disks are refused: their
//! device stack would also have to grow, which this does not do.

use super::artifact_fs;
use super::*;
use pplx_space_common_core::sandbox_storage::validate_rootfs_mib;
use pplx_space_common_core::types::RuntimeKind;
use serde_json::Value;
use std::os::unix::fs::MetadataExt;

/// Where Firecracker sees a plaintext root disk inside its jail.
const JAILED_ROOTFS: &str = "/rootfs.ext4";
const MIB: u64 = 1024 * 1024;

/// The root drive Firecracker is serving, as reported by `GET /vm/config`.
#[derive(Debug, PartialEq, Eq)]
struct RootDrive {
    id: String,
    path_on_host: String,
}

fn root_drive(config: &Value) -> Result<RootDrive, Status> {
    let drives = config
        .get("drives")
        .and_then(Value::as_array)
        .ok_or_else(|| Status::failed_precondition("VM reports no drive configuration"))?;
    let mut roots = drives
        .iter()
        .filter(|drive| drive.get("is_root_device").and_then(Value::as_bool) == Some(true));
    let (Some(root), None) = (roots.next(), roots.next()) else {
        return Err(Status::failed_precondition(
            "VM must have exactly one root drive",
        ));
    };
    let field = |name: &str| {
        root.get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Status::failed_precondition(format!("root drive lacks {name}")))
    };
    Ok(RootDrive {
        id: field("drive_id")?,
        path_on_host: field("path_on_host")?,
    })
}

/// Grow-only and within the platform maximum. Returns the new size in bytes.
/// Asking for the current size is accepted so a grow whose final Firecracker
/// notification failed can simply be retried.
fn grow_target(size_mib: u32, current_bytes: u64) -> Result<u64, Status> {
    let target = validate_rootfs_mib(size_mib)
        .map_err(|error| Status::invalid_argument(error.to_string()))?
        .as_bytes();
    if target < current_bytes {
        return Err(Status::invalid_argument(format!(
            "size_mib={size_mib} must be larger than the current root disk ({} MiB); \
             shrinking is not supported",
            current_bytes.div_ceil(MIB)
        )));
    }
    Ok(target)
}

/// The instance file, pinned, after proving the jail serves this same inode.
struct PinnedRootfs {
    backing: crate::device_backing::Backing,
    current_bytes: u64,
}

fn pin_served_rootfs(instance: &Path, jailed: &Path) -> Result<PinnedRootfs, Status> {
    let backing = crate::device_backing::Backing::require(
        crate::device_backing::Backing::open(instance, true).map_err(artifact_fs::error)?,
    )
    .map_err(artifact_fs::error)?;
    let pinned = backing.file.metadata().map_err(artifact_fs::error)?;
    // The jailed path is a bind mount of the instance file for plaintext
    // disks. Anything else (an encrypted mapper device, a replaced file) means
    // growing this file would not reach the guest.
    let served = std::fs::symlink_metadata(jailed).map_err(artifact_fs::error)?;
    if !served.is_file() || (served.dev(), served.ino()) != (pinned.dev(), pinned.ino()) {
        return Err(Status::failed_precondition(
            "Firecracker is not serving the instance root disk file",
        ));
    }
    Ok(PinnedRootfs {
        backing,
        current_bytes: pinned.len(),
    })
}

fn api_error(error: crate::fc_api::FcApiError) -> Status {
    Status::unavailable(format!(
        "Firecracker API unavailable (is the sandbox awake on this node?): {error}"
    ))
}

impl NodeManagerService {
    pub(super) async fn grow_sandbox_rootfs(
        &self,
        request: Request<crate::proto::GrowSandboxRootfsRequest>,
    ) -> Result<Response<crate::proto::GrowSandboxRootfsResponse>, Status> {
        self.authorize_peer(&request, "GrowSandboxRootfs", ALLOW_NODE_ADMIN)?;
        let req = request.into_inner();
        let id = req
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if req.execution_fence.is_none() {
            return Err(Status::invalid_argument("execution fence required"));
        }
        // Reject obviously bad sizes before taking any lock.
        validate_rootfs_mib(req.size_mib)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let service = self.clone();
        self.run_owned_sandbox_work(&id.to_string(), async move {
            // Same lock as launch, pause, snapshot and the disk-I/O sweep, so
            // the VM cannot be suspended, replaced or torn down mid-grow.
            let _gate = service.lock_sandbox_vm_cgroup_launch(&req.sandbox_id).await;
            service.check_execution(&req.sandbox_id, req.execution_fence.as_ref(), false)?;
            let execution = service
                .recorded_execution(req.sandbox_id.clone())
                .await?
                .ok_or_else(|| Status::failed_precondition("execution metadata required"))?;
            if execution.runtime().kind != RuntimeKind::Firecracker {
                return Err(Status::failed_precondition(
                    "live root disk growth is supported only for Firecracker",
                ));
            }
            let client = crate::fc_api::FcApiClient::new(format!(
                "{JAIL_DIR}/firecracker/{id}/root/firecracker.sock"
            ));
            let drive = root_drive(&client.configuration().await.map_err(api_error)?)?;
            if drive.path_on_host != JAILED_ROOTFS {
                return Err(Status::failed_precondition(
                    "root disk is not a plaintext file; encrypted root disks cannot be grown live",
                ));
            }

            let instance = PathBuf::from(format!("{CACHE_DIR}/{id}/instance/rootfs.ext4"));
            let jailed = pplx_space_node_runtime_core::control::isolation_root(
                RuntimeKind::Firecracker,
                &req.sandbox_id,
            )
            .map_err(|e| Status::failed_precondition(e.to_string()))?
            .join("rootfs.ext4");
            let pinned = service
                .off_runtime("pin served rootfs", move || {
                    pin_served_rootfs(&instance, &jailed)
                })
                .await?;
            let previous_bytes = pinned.current_bytes;
            let rootfs_bytes = grow_target(req.size_mib, previous_bytes)?;
            if rootfs_bytes > previous_bytes {
                // The file is sparse, but the guest may fill it; count the
                // growth against node disk admission like a new allocation.
                let _reservation = service.disk_admission.reserve(
                    crate::disk_admission::DiskAllocationOperation::GrowSandboxRootfs,
                    Some(rootfs_bytes - previous_bytes),
                )?;
                service
                    .off_runtime("extend rootfs", move || {
                        pinned
                            .backing
                            .resize(rootfs_bytes)
                            .map_err(artifact_fs::error)
                    })
                    .await?;
            }
            // From here the file is larger. If this notification fails, the
            // guest keeps seeing its old size and the spare tail is unused;
            // retrying with the same size_mib only repeats this step.
            client
                .rescan_drive(&drive.id, JAILED_ROOTFS)
                .await
                .map_err(api_error)?;
            tracing::info!(
                sandbox_id = %id,
                previous_bytes,
                rootfs_bytes,
                "grew sandbox root disk live"
            );
            Ok(Response::new(crate::proto::GrowSandboxRootfsResponse {
                previous_bytes,
                rootfs_bytes,
            }))
        })
        .await
    }
}

#[cfg(test)]
mod tests;
