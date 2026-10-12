//! The local (non-Firecracker) `space.node.v1.NodeManager` implementation.
//!
//! Serves the same gRPC surface sandbox-manager and node-api already call, but
//! with no-KVM / no-btrfs semantics: guests boot as `systemd-nspawn`
//! containers, the filesystem is cloned via reflink, networking is synthetic,
//! and the btrfs delegation RPCs return `FailedPrecondition` to force the Full
//! ext4 snapshot codec.

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use pplx_space_node_mgr_client::proto::node_manager_server::NodeManager;
use pplx_space_node_mgr_client::proto::*;
use sha2::{Digest as _, Sha256};
use tonic::{Request, Response, Status};

use crate::config::Config;
use crate::process;
use crate::snapshot;
use crate::state::{Registry, SandboxEntry};
use crate::{NSPAWN_LOG, SPACED_HOST_LOG, paths};

/// Synthetic network constants. The guest is reached over the UDS bridge, so
/// these values only have to be internally consistent, never routable.
const GUEST_GATEWAY: &str = "169.254.0.1";
const HEALTHY: i32 = NetworkPoolStatus::Healthy as i32;
/// Memory advertised to sandbox-manager's admission control (MiB). The cgroup
/// budget is the guest max plus a non-zero headroom; sandbox-manager rejects a
/// zero headroom (`sandbox memory policy is unavailable`) and requires
/// `cgroup - headroom > 0`.
const ADVERTISED_CGROUP_MEMORY_MIB: u64 = 8192;
const ADVERTISED_MEMORY_HEADROOM_MIB: u64 = 512;
const ADVERTISED_GUEST_MEMORY_MAX_MIB: u64 =
    ADVERTISED_CGROUP_MEMORY_MIB - ADVERTISED_MEMORY_HEADROOM_MIB;

/// Size of the ext4 image rebuilt from the live rootfs tree at snapshot time.
/// Matches the seed rootfs sizing; leaves headroom for sandbox writes.
const SNAPSHOT_ROOTFS_SIZE_MIB: u64 = 3072;

fn local_resolved_execution(
    requested: Option<pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<pplx_space_execution_common::proto::ExecutionMetadata, Status> {
    let execution = if let Some(requested) = requested {
        pplx_space_common_svc::core::types::ExecutionMetadata::try_from(&requested)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
    } else {
        let architecture = std::env::consts::ARCH
            .parse::<pplx_space_common_svc::core::types::Arch>()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        pplx_space_common_svc::core::types::ExecutionMetadata::initial_firecracker(
            architecture,
            vec![],
        )
    };
    Ok((&execution).into())
}

fn local_kernel_components()
-> Vec<pplx_space_execution_common::proto::ArtifactComponentInstallation> {
    use pplx_space_common_svc::core::types::{
        ArtifactComponentKind, ArtifactComponentMetadata, ArtifactComponentRole, ArtifactFormat,
        ArtifactVersion, Digest,
    };

    let kernel_path = [
        "/space/cache/firecracker/kernels/default/vmlinux.bin",
        "/space/cache/firecracker/assets/default/kernel/vmlinux.bin",
    ]
    .into_iter()
    .map(Path::new)
    .find(|path| path.is_file());
    let Some(kernel_path) = kernel_path else {
        return Vec::new();
    };

    let mut file = match std::fs::File::open(kernel_path) {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(
                path = %kernel_path.display(),
                %error,
                "failed to open local default kernel for execution inventory"
            );
            return Vec::new();
        }
    };
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = match file.read(&mut buffer) {
            Ok(read) => read,
            Err(error) => {
                tracing::warn!(
                    path = %kernel_path.display(),
                    %error,
                    "failed to hash local default kernel for execution inventory"
                );
                return Vec::new();
            }
        };
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = Digest::sha256(hasher.finalize().into());
    let version = std::fs::canonicalize(kernel_path)
        .ok()
        .and_then(|path| {
            path.parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                .filter(|name| name.starts_with("vmlinux-"))
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| format!("legacy-{}", &digest.hex_string()[..12]));
    let metadata = ArtifactComponentMetadata {
        kind: ArtifactComponentKind::Kernel,
        role: Some(ArtifactComponentRole::parse("boot").expect("boot component role is valid")),
        version: Some(ArtifactVersion::parse(version).expect("kernel version is valid")),
        digest: Some(digest),
        format: Some(ArtifactFormat::parse("vmlinux").expect("kernel format is valid")),
    };

    vec![
        pplx_space_execution_common::proto::ArtifactComponentInstallation {
            component: Some((&metadata).into()),
            default_for_new: true,
            legacy_default: true,
        },
    ]
}

/// Synthetic network-slot capacity advertised in `GetNetworkPoolStatus`.
/// `Allocate` fabricates a slot per sandbox on demand (there is no real IP
/// pool), but sandbox-manager's autostart / cold / snapshot create gate
/// (`reject_autostart_create_without_ready_slot`) rejects with
/// `ResourceExhausted` when `network_slots_ready == 0`. Advertise a non-zero
/// ready/total pool so those create paths admit; it's a soft ceiling, not a
/// hard limit (allocation never actually consults it).
const ADVERTISED_NETWORK_SLOTS: u32 = 256;

/// How long `LaunchSandbox` waits for the guest's spaced bridge socket to bind
/// (cold boot + systemd + spaced-guest startup) before returning.
const GUEST_BRIDGE_WAIT_TICKS: u32 = 180;
const GUEST_BRIDGE_WAIT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Filesystem root that `CleanupSandbox` removes when `keep_filesystem` is
/// false. It must be the sandbox cache root that sandbox-manager's cache
/// reconciler scans (each UUID-named child of `SANDBOX_CACHE_DIR` is treated as
/// a live sandbox), not a nested subdir like `instance/`; otherwise a deleted
/// sandbox's leftover root is rematerialized as an orphan behind quarantine.
pub(crate) fn cleanup_fs_root(sandbox_id: &str) -> std::path::PathBuf {
    paths::sandbox_cache_dir(sandbox_id)
}

/// How long `Launch` waits for spaced-host to bind its `--socket` control UDS
/// before reporting the guest bridge ready. spaced-host binds within ~1s; the
/// budget is generous so a slow first launch doesn't false-fail.
const SPACED_HOST_SOCKET_WAIT_TICKS: u32 = 60;

/// The `--socket` value spaced-host binds for its control RPC, parsed from the
/// Launch args. `Launch` waits for this UDS to appear before returning so it
/// never reports a not-yet-listening (or already-dead) spaced-host as ready.
pub(crate) fn spaced_host_control_socket(args: &[String]) -> Option<&str> {
    args.windows(2)
        .find(|w| w[0] == "--socket")
        .map(|w| w[1].as_str())
}

/// Terminate every helper process recorded for a sandbox — guest nspawn
/// supervisor, spaced-host, and the net-gateway placeholder — returning a log
/// line per signalled PID. Both cleanup paths call this so a dead-guest reap
/// doesn't leak the `sleep infinity` net-gateway or a still-running spaced-host.
pub(crate) async fn terminate_sidecars(entry: &SandboxEntry) -> Vec<String> {
    let mut actions = Vec::new();
    if let Some(pid) = entry.spaced_host_pid {
        process::terminate(pid).await;
        actions.push(format!("terminated spaced-host pid {pid}"));
    }
    if let Some(pid) = entry.netgtw_pid {
        process::terminate(pid).await;
        actions.push(format!("terminated net-gateway placeholder pid {pid}"));
    }
    if let Some(pid) = entry.nspawn_pid {
        process::terminate(pid).await;
        actions.push(format!("terminated guest pid {pid}"));
    }
    actions
}

#[derive(Clone)]
pub struct LocalNodeManager {
    execution_locks: Arc<dashmap::DashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    config: Arc<Config>,
    registry: Arc<Registry>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExecutionOperation {
    Bind,
    Current,
    Cleanup,
}

impl LocalNodeManager {
    pub fn new(config: Config) -> Self {
        Self {
            execution_locks: Arc::default(),
            config: Arc::new(config),
            registry: Arc::new(Registry::default()),
        }
    }

    fn run_dir(sandbox_id: &str) -> String {
        pplx_space_common_svc::core::host::linux::paths::sandbox_run_dir(sandbox_id)
    }

    async fn execution_guard(
        &self,
        sandbox_id: &str,
        wire: Option<&ExecutionFence>,
        operation: ExecutionOperation,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, Status> {
        use pplx_space_node_mgr_client::execution_fence::ExecutionFence as Fence;
        let canonical = sandbox_id
            .parse::<pplx_space_common_svc::core::types::SandboxId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if canonical.to_string() != sandbox_id {
            return Err(Status::invalid_argument(
                "sandbox_id must use canonical UUID spelling",
            ));
        }
        self.execution_locks
            .retain(|_, lock| lock.strong_count() > 0);
        let lock = {
            let mut entry = self
                .execution_locks
                .entry(sandbox_id.to_owned())
                .or_default();
            if let Some(lock) = entry.upgrade() {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                *entry = Arc::downgrade(&lock);
                lock
            }
        };
        let guard = lock.lock_owned().await;
        let requested = wire.map(Fence::try_from).transpose()?;
        let current = self.registry.execution(sandbox_id);
        let accepted = match (requested, current) {
            (Some(requested), Some(current)) if requested.current() == current.current() => true,
            (Some(requested), Some(current))
                if operation == ExecutionOperation::Cleanup
                    && requested.owns(current.current()) =>
            {
                true
            }
            (Some(_), None) if operation == ExecutionOperation::Cleanup => true,
            (Some(requested), current)
                if operation == ExecutionOperation::Bind
                    && current.map(Fence::current) == requested.previous()
                    && self.registry.get(sandbox_id).is_none() =>
            {
                self.registry.bind_execution(sandbox_id, requested);
                true
            }
            _ => false,
        };
        if !accepted {
            return Err(Status::failed_precondition(
                "local execution identity is missing or superseded",
            ));
        }
        Ok(guard)
    }

    /// Resolve the source rootfs image for a fresh prepare: the template build
    /// dir's `rootfs.ext4` when present, otherwise the baked seed image.
    fn source_rootfs(&self, template_dir: Option<&str>) -> std::path::PathBuf {
        if let Some(dir) = template_dir.filter(|d| !d.is_empty()) {
            let candidate = Path::new(dir).join("rootfs.ext4");
            if candidate.exists() {
                return candidate;
            }
        }
        self.config.seed_rootfs.clone()
    }

    async fn do_prepare_sandbox(
        &self,
        req: PrepareSandboxRequest,
    ) -> Result<PrepareSandboxResponse, Status> {
        let id = &req.sandbox_id;
        self.registry.ensure(id);
        self.registry.set_template(id, req.template.clone());

        let instance_dir = paths::instance_dir(id);
        std::fs::create_dir_all(&instance_dir).map_err(|e| internal("create instance dir", e))?;
        std::fs::create_dir_all(paths::bridge_dir(id))
            .map_err(|e| internal("create bridge dir", e))?;

        let rootfs = paths::instance_rootfs(id);
        let rootdir = paths::instance_rootdir(id);
        // Decide the clone source on the async thread (cheap path/stat logic).
        // `None` means the instance rootfs is already present — a prior step
        // (sandbox-manager's Full-codec restore reflink, or a retried prepare)
        // placed it, so keep it.
        let source = if rootfs.exists() {
            tracing::info!(sandbox_id = %id, "instance rootfs already present; keeping");
            None
        } else {
            Some(
                req.snapshot_dir
                    .as_deref()
                    .filter(|d| !d.is_empty())
                    .map(|d| Path::new(d).join("rootfs.ext4"))
                    .filter(|p| p.exists())
                    .unwrap_or_else(|| self.source_rootfs(req.template_dir.as_deref())),
            )
        };

        // Heavy, blocking I/O: a multi-GiB reflink/copy and a full `debugfs
        // rdump` extraction (nspawn boots `--directory`, so the ext4 is unpacked
        // to a tree — no loop device / udev). Run it on the blocking pool so a
        // several-second extraction can't stall the gRPC server's other RPCs and
        // health checks on a tokio worker thread. Re-extract from the current
        // ext4 each prepare so restore (which reflinks a fresh ext4) boots the
        // restored state.
        let rootfs_b = rootfs.clone();
        let rootdir_b = rootdir.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            if let Some(src) = source {
                snapshot::reflink_or_copy(&src, &rootfs_b)?;
            }
            let _ = std::fs::remove_dir_all(&rootdir_b);
            snapshot::extract_ext4_to_dir(&rootfs_b, &rootdir_b)
        })
        .await
        .map_err(|e| Status::internal(format!("prepare-sandbox blocking task join: {e}")))?
        .map_err(|e| internal("clone/extract rootfs", e))?;
        tracing::info!(sandbox_id = %id, rootdir = %rootdir.display(), "prepared instance rootfs + tree");

        let run_dir = Self::run_dir(id);
        Ok(PrepareSandboxResponse {
            cache_dir: instance_dir.to_string_lossy().into_owned(),
            run_dir: run_dir.clone(),
            rootfs_path: rootfs.to_string_lossy().into_owned(),
            config_path: format!("{run_dir}/vm-config.json"),
            log_path: format!("{run_dir}/{NSPAWN_LOG}"),
            resolved_execution: Some(local_resolved_execution(req.requested_execution)?),
        })
    }

    async fn do_launch_sandbox(
        &self,
        req: LaunchSandboxRequest,
    ) -> Result<LaunchSandboxResponse, Status> {
        let id = &req.sandbox_id;
        let entry = self.registry.ensure(id);
        let bridge_dir = paths::bridge_dir(id);
        std::fs::create_dir_all(&bridge_dir).map_err(|e| internal("create bridge dir", e))?;

        let rootdir = paths::instance_rootdir(id);
        if !rootdir.join("usr").is_dir() {
            return Err(Status::failed_precondition(format!(
                "instance rootfs tree missing for {id}: {}",
                rootdir.display()
            )));
        }

        let run_dir = Self::run_dir(id);
        std::fs::create_dir_all(&run_dir).map_err(|e| internal("create run dir", e))?;
        let log_path = Path::new(&run_dir).join(NSPAWN_LOG);

        let pid = process::spawn_nspawn(&entry.machine, &rootdir, &bridge_dir, &log_path)
            .map_err(|e| internal("boot guest via systemd-nspawn", e))?;
        self.registry.set_nspawn_pid(id, pid);
        tracing::info!(sandbox_id = %id, machine = %entry.machine, pid, "guest booted");

        // Wait for the guest's spaced bridge socket to be bound before returning.
        // A real Firecracker restore resumes instantly with the bridge already
        // up, so sandbox-manager's restore activation injects process env without
        // waiting; our guest cold-boots in a few seconds, so gate here to avoid a
        // "failed to connect to spaced bridge guest" race on restore.
        //
        // If the bridge never binds — the guest's spaced-svc crashed, or the
        // nspawn supervisor exited — fail the launch so sandbox-manager surfaces a
        // dead guest instead of recording a launched-but-unusable sandbox.
        let bridge_sock = paths::bridge_guest_socket(id);
        let mut bridge_up = false;
        for _ in 0..GUEST_BRIDGE_WAIT_TICKS {
            if bridge_sock.exists() {
                bridge_up = true;
                break;
            }
            if !process::is_alive(pid) {
                return Err(Status::unavailable(format!(
                    "guest {id} exited during boot (nspawn pid {pid} gone) before its spaced bridge bound"
                )));
            }
            tokio::time::sleep(GUEST_BRIDGE_WAIT_INTERVAL).await;
        }
        if !bridge_up {
            return Err(Status::unavailable(format!(
                "guest {id} spaced bridge did not bind at {} within the boot timeout",
                bridge_sock.display()
            )));
        }
        tracing::info!(sandbox_id = %id, "guest spaced bridge is up");

        Ok(LaunchSandboxResponse {
            pid,
            // Non-Firecracker runtime: there is no FC API socket. Lifecycle
            // (pause/resume/snapshot) goes through this gRPC, not a VMM socket.
            api_socket_path: String::new(),
            resolved_execution: Some(local_resolved_execution(req.requested_execution)?),
        })
    }

    async fn do_launch(&self, req: LaunchRequest) -> Result<LaunchResponse, Status> {
        let id = &req.sandbox_id;
        self.registry.ensure(id);
        let bridge_dir = paths::bridge_dir(id);
        std::fs::create_dir_all(&bridge_dir).map_err(|e| internal("create bridge dir", e))?;

        let run_dir = Self::run_dir(id);
        std::fs::create_dir_all(&run_dir).map_err(|e| internal("create run dir", e))?;

        if req.program.ends_with("space-spaced-host-svc") {
            let bridge_guest = paths::bridge_guest_socket(id);
            let bridge_host = paths::bridge_host_socket(id);
            let args = process::rewrite_spaced_host_args(&req.args, &bridge_guest, &bridge_host);
            let log_path = Path::new(&run_dir).join(SPACED_HOST_LOG);
            let control_socket = spaced_host_control_socket(&args).map(str::to_string);

            // Drop any stale control socket from a prior launch first. A
            // relaunch (suspend/restore, watchdog restart) reuses the run dir,
            // and cleanup only removes the bridge dir — so a leftover socket file
            // would make the readiness wait below pass immediately on the dead
            // socket, before the new spaced-host actually binds.
            if let Some(sock) = &control_socket {
                let _ = std::fs::remove_file(sock);
            }

            // A relaunch (suspend/restore, watchdog restart) reuses the run dir.
            // If a previous spaced-host for this sandbox is still alive, terminate
            // it before spawning + recording the new PID — otherwise the old
            // process leaks with no registry reference once we overwrite the PID.
            if let Some(prev) = self.registry.get(id).and_then(|e| e.spaced_host_pid)
                && process::is_alive(prev)
            {
                process::terminate(prev).await;
            }

            let pid = process::spawn_spaced_host(&req.program, &args, &req.env, &log_path)
                .map_err(|e| internal("launch spaced-host", e))?;
            self.registry.set_spaced_host_pid(id, pid);

            // Don't report success until spaced-host's control socket actually
            // accepts. Returning on spawn alone lets sandbox-manager record a
            // dead PID or race connection-refused on the control UDS; the
            // production node-manager likewise waits for the socket to bind.
            if let Some(sock) = &control_socket {
                let sock_path = Path::new(sock);
                let mut bound = false;
                for _ in 0..SPACED_HOST_SOCKET_WAIT_TICKS {
                    if sock_path.exists() {
                        bound = true;
                        break;
                    }
                    if !process::is_alive(pid) {
                        return Err(Status::unavailable(format!(
                            "spaced-host for {id} exited during startup (pid {pid} gone) before binding {sock}"
                        )));
                    }
                    tokio::time::sleep(GUEST_BRIDGE_WAIT_INTERVAL).await;
                }
                if !bound {
                    return Err(Status::unavailable(format!(
                        "spaced-host for {id} did not bind its control socket {sock} within the startup timeout"
                    )));
                }
            }
            tracing::info!(sandbox_id = %id, pid, "spaced-host launched in UDS-bridge mode");
            Ok(LaunchResponse { pid })
        } else {
            // The other program sandbox-manager launches via Launch is the
            // net-gateway (egress isolation), which is a non-goal here: the
            // guest is reached over the UDS bridge, not IP. Give sandbox-manager
            // a live PID to monitor via a no-op placeholder instead of running
            // (or failing to find) the real binary.
            let log_path = Path::new(&run_dir).join("net-gateway.log");
            let pid = process::spawn_placeholder(&log_path)
                .map_err(|e| internal("launch net-gateway placeholder", e))?;
            self.registry.set_netgtw_pid(id, pid);
            tracing::info!(sandbox_id = %id, program = %req.program, pid, "net-gateway launch is a no-op placeholder on the local runtime");
            Ok(LaunchResponse { pid })
        }
    }

    async fn do_cleanup_sandbox(
        &self,
        req: CleanupSandboxRequest,
    ) -> Result<CleanupSandboxResponse, Status> {
        let id = &req.sandbox_id;
        let mut actions = Vec::new();
        if let Some(entry) = self.registry.get(id) {
            actions.extend(terminate_sidecars(&entry).await);
        }
        // Bridge sockets are always disposable.
        let _ = std::fs::remove_dir_all(paths::bridge_dir(id));
        if !req.keep_filesystem && std::fs::remove_dir_all(cleanup_fs_root(id)).is_ok() {
            actions.push("removed sandbox cache root".to_string());
        }
        self.registry.remove(id);
        actions.push("released slot".to_string());
        let retained_execution_id = if req.keep_filesystem {
            self.registry
                .execution(id)
                .map(|fence| fence.current().to_string())
        } else {
            self.registry.retire_execution(id);
            None
        };
        Ok(CleanupSandboxResponse {
            actions,
            retained_execution_id,
        })
    }

    fn do_pause(&self, sandbox_id: &str, frozen: bool) {
        if let Some(entry) = self.registry.get(sandbox_id) {
            if let Some(pid) = entry.nspawn_pid {
                process::set_frozen(pid, frozen);
            }
            self.registry.set_paused(sandbox_id, frozen);
        } else {
            tracing::warn!(%sandbox_id, frozen, "pause/resume on unknown sandbox; no-op");
        }
    }

    async fn do_create_snapshot(&self, req: &CreateSnapshotRequest) -> Result<(), Status> {
        let id = &req.sandbox_id;
        let dest = req
            .dest_dir
            .as_deref()
            .filter(|d| !d.is_empty())
            .ok_or_else(|| Status::invalid_argument("create_snapshot requires dest_dir"))?
            .to_string();
        let rootdir = paths::instance_rootdir(id);
        if !rootdir.join("usr").is_dir() {
            return Err(Status::failed_precondition(format!(
                "instance rootfs tree missing for snapshot of {id}"
            )));
        }
        let vmstate = basename(&req.snapshot_path, "vmstate");
        let memory = basename(&req.mem_file_path, "memory");
        let rootfs = paths::instance_rootfs(id);
        // `mke2fs -d` rebuilds a multi-GiB ext4 from the live tree — blocking;
        // run it on the blocking pool so it doesn't stall the tokio worker.
        tokio::task::spawn_blocking(move || {
            snapshot::write_full_snapshot(
                &rootdir,
                &rootfs,
                Path::new(&dest),
                &vmstate,
                &memory,
                SNAPSHOT_ROOTFS_SIZE_MIB,
            )
        })
        .await
        .map_err(|e| Status::internal(format!("create-snapshot blocking task join: {e}")))?
        .map_err(|e| internal("write full snapshot", e))?;
        tracing::info!(sandbox_id = %id, "wrote Full-codec snapshot artifacts");
        Ok(())
    }
}

/// Map an IO error to an `Internal` status with a stable prefix.
fn internal(what: &str, error: std::io::Error) -> Status {
    Status::internal(format!("{what}: {error}"))
}

/// Last path component of a chroot-relative artifact path, or a fallback.
fn basename(path: &str, fallback: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|n| !n.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

/// Reject any path that does not name an entry strictly inside /space/cache.
fn validate_cache_artifact_path(local_path: &str) -> Result<(), Status> {
    let cache_root = pplx_space_common_svc::core::host::linux::paths::SPACE_CACHE_DIR;
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
    Ok(())
}

fn prune_empty_parents(start_path: &str, max_levels: usize) {
    let cache_root = Path::new(pplx_space_common_svc::core::host::linux::paths::SPACE_CACHE_DIR);
    let mut current = std::path::PathBuf::from(start_path);
    for _ in 0..max_levels {
        let Some(parent) = current.parent() else {
            return;
        };
        if parent == cache_root || !parent.starts_with(cache_root) {
            return;
        }
        let is_empty = match std::fs::read_dir(parent) {
            Ok(mut entries) => entries.next().is_none(),
            Err(error) => {
                tracing::debug!(
                    parent = %parent.display(),
                    %error,
                    "prune_empty_parents: read_dir failed; stopping walk",
                );
                return;
            }
        };
        if !is_empty {
            return;
        }
        match std::fs::remove_dir(parent) {
            Ok(()) => {
                tracing::debug!(
                    parent = %parent.display(),
                    "prune_empty_parents: removed empty parent dir",
                );
            }
            Err(error) => {
                tracing::debug!(
                    parent = %parent.display(),
                    kind = ?error.kind(),
                    %error,
                    "prune_empty_parents: rmdir failed; stopping walk",
                );
                return;
            }
        }
        current = parent.to_path_buf();
    }
}

#[tonic::async_trait]
impl NodeManager for LocalNodeManager {
    async fn validate_gateway_caller(
        &self,
        _request: Request<pplx_space_node_mgr_client::proto::ValidateGatewayCallerRequest>,
    ) -> Result<Response<pplx_space_node_mgr_client::proto::ValidateGatewayCallerResponse>, Status>
    {
        Err(Status::failed_precondition(
            "gateway launch attestation requires Linux runtime-supervisor",
        ))
    }

    async fn get_clone_capabilities(
        &self,
        _request: Request<CloneCapabilitiesRequest>,
    ) -> Result<Response<CloneCapabilitiesResponse>, Status> {
        Ok(Response::new(CloneCapabilitiesResponse {
            prepared_clone_disposal: false,
        }))
    }

    async fn get_execution_inventory(
        &self,
        _request: Request<GetExecutionInventoryRequest>,
    ) -> Result<Response<pplx_space_execution_common::proto::NodeExecutionInventory>, Status> {
        let execution_proto = local_resolved_execution(None)?;
        let execution =
            pplx_space_common_svc::core::types::ExecutionMetadata::try_from(&execution_proto)
                .map_err(|error| Status::internal(error.to_string()))?;
        let state_formats = execution
            .state_format()
            .into_iter()
            .map(
                |identity| pplx_space_execution_common::proto::StateFormatCapability {
                    identity: Some(identity.into()),
                    can_create: true,
                    can_load: true,
                },
            )
            .collect();
        Ok(Response::new(
            pplx_space_execution_common::proto::NodeExecutionInventory {
                runtimes: vec![pplx_space_execution_common::proto::RuntimeInstallation {
                    runtime: Some(execution.runtime().into()),
                    supported_guests: vec![execution.guest().into()],
                    state_formats,
                }],
                components: local_kernel_components(),
            },
        ))
    }

    async fn allocate(
        &self,
        request: Request<AllocateRequest>,
    ) -> Result<Response<AllocateResponse>, Status> {
        let req = request.into_inner();
        let _guard = self
            .execution_guard(
                &req.sandbox_id,
                req.execution_fence.as_ref(),
                ExecutionOperation::Bind,
            )
            .await?;
        let entry = self.registry.allocate(&req.sandbox_id);
        Ok(Response::new(AllocateResponse {
            slot: entry.slot,
            namespace: format!("sns-{}", paths::short_id(&req.sandbox_id)),
            namespace_path: String::new(),
            guest_ip: format!("169.254.{}.{}", entry.slot / 256, entry.slot % 256),
            guest_gateway: GUEST_GATEWAY.to_string(),
            guest_mac: synthetic_mac(entry.slot),
            tap_device: format!("tap{}", entry.slot),
            slot_generation: 1,
            slot_label: format!("local-{}", entry.slot),
            sandbox_uid: entry.uid,
        }))
    }

    async fn launch(
        &self,
        request: Request<LaunchRequest>,
    ) -> Result<Response<LaunchResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        Ok(Response::new(self.do_launch(request.into_inner()).await?))
    }

    async fn release(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let req = request.into_inner();
        let _guard = self
            .execution_guard(
                &req.sandbox_id,
                req.execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        if let Some(entry) = self.registry.get(&req.sandbox_id) {
            if let Some(pid) = entry.spaced_host_pid {
                process::terminate(pid).await;
            }
            if let Some(pid) = entry.netgtw_pid {
                process::terminate(pid).await;
            }
            if let Some(pid) = entry.nspawn_pid {
                process::terminate(pid).await;
            }
        }
        let _ = std::fs::remove_dir_all(paths::bridge_dir(&req.sandbox_id));
        self.registry.remove(&req.sandbox_id);
        Ok(Response::new(ReleaseResponse {}))
    }

    async fn get_node_resource_status(
        &self,
        _request: Request<NodeResourceStatusRequest>,
    ) -> Result<Response<NodeResourceStatusResponse>, Status> {
        Ok(Response::new(NodeResourceStatusResponse {
            cache_disk: Some(CacheDiskStatus {
                admission_state: CacheDiskAdmissionState::Ready as i32,
                admission_reason: "disk_admission_disabled".to_string(),
                admission_detail: "local node-manager does not manage /space/cache".to_string(),
                ..Default::default()
            }),
        }))
    }

    async fn get_network_pool_status(
        &self,
        _request: Request<NetworkPoolStatusRequest>,
    ) -> Result<Response<NetworkPoolStatusResponse>, Status> {
        Ok(Response::new(NetworkPoolStatusResponse {
            network_pool_status: HEALTHY,
            sandbox_cgroup_memory_mib: ADVERTISED_CGROUP_MEMORY_MIB,
            sandbox_guest_memory_max_mib: ADVERTISED_GUEST_MEMORY_MAX_MIB,
            sandbox_memory_headroom_mib: ADVERTISED_MEMORY_HEADROOM_MIB,
            sandbox_cgroup_total_memory_mib: ADVERTISED_CGROUP_MEMORY_MIB,
            // Non-zero so sandbox-manager's autostart/cold/snapshot ready-slot
            // gate admits; Allocate synthesizes the actual slot per sandbox.
            network_slots_total: ADVERTISED_NETWORK_SLOTS,
            network_slots_ready: ADVERTISED_NETWORK_SLOTS,
            ..Default::default()
        }))
    }

    async fn list_network_leases(
        &self,
        _request: Request<ListNetworkLeasesRequest>,
    ) -> Result<Response<ListNetworkLeasesResponse>, Status> {
        Ok(Response::new(ListNetworkLeasesResponse::default()))
    }

    async fn set_network_pool_drain(
        &self,
        request: Request<SetNetworkPoolDrainRequest>,
    ) -> Result<Response<SetNetworkPoolDrainResponse>, Status> {
        Ok(Response::new(SetNetworkPoolDrainResponse {
            draining: request.into_inner().draining,
        }))
    }

    async fn repair_network_pool(
        &self,
        _request: Request<RepairNetworkPoolRequest>,
    ) -> Result<Response<RepairNetworkPoolResponse>, Status> {
        Ok(Response::new(RepairNetworkPoolResponse::default()))
    }

    async fn set_network_pool_config(
        &self,
        _request: Request<SetNetworkPoolConfigRequest>,
    ) -> Result<Response<SetNetworkPoolConfigResponse>, Status> {
        Ok(Response::new(SetNetworkPoolConfigResponse::default()))
    }

    async fn stop_process(
        &self,
        request: Request<StopProcessRequest>,
    ) -> Result<Response<StopProcessResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        let req = request.into_inner();
        let success = match self
            .registry
            .get(&req.sandbox_id)
            .and_then(|e| e.nspawn_pid)
        {
            Some(pid) => nix::sys::signal::Signal::try_from(req.signal.cast_signed())
                .ok()
                .is_some_and(|signal| {
                    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid.cast_signed()), signal)
                        .is_ok()
                }),
            None => false,
        };
        Ok(Response::new(StopProcessResponse { success }))
    }

    async fn launch_sandbox(
        &self,
        request: Request<LaunchSandboxRequest>,
    ) -> Result<Response<LaunchSandboxResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        Ok(Response::new(
            self.do_launch_sandbox(request.into_inner()).await?,
        ))
    }

    async fn prepare_sandbox(
        &self,
        request: Request<PrepareSandboxRequest>,
    ) -> Result<Response<PrepareSandboxResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        Ok(Response::new(
            self.do_prepare_sandbox(request.into_inner()).await?,
        ))
    }

    async fn reserve_prepared_rootfs(
        &self,
        _request: Request<ReservePreparedRootfsRequest>,
    ) -> Result<Response<ReservePreparedRootfsResponse>, Status> {
        Err(Status::failed_precondition(
            "externally prepared rootfs files require Linux node-manager",
        ))
    }

    async fn complete_prepared_rootfs(
        &self,
        _request: Request<CompletePreparedRootfsRequest>,
    ) -> Result<Response<CompletePreparedRootfsResponse>, Status> {
        Err(Status::failed_precondition(
            "externally prepared rootfs files require Linux node-manager",
        ))
    }

    async fn prepare_sandbox_artifact_dir(
        &self,
        request: Request<PrepareSandboxArtifactDirRequest>,
    ) -> Result<Response<PrepareSandboxArtifactDirResponse>, Status> {
        let id = request.into_inner().sandbox_id;
        let dir = pplx_space_common_svc::core::host::linux::paths::sandbox_snapshots_dir(&id);
        std::fs::create_dir_all(&dir).map_err(|e| internal("create artifact dir", e))?;
        Ok(Response::new(PrepareSandboxArtifactDirResponse {}))
    }

    async fn cleanup_sandbox(
        &self,
        request: Request<CleanupSandboxRequest>,
    ) -> Result<Response<CleanupSandboxResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Cleanup,
            )
            .await?;
        Ok(Response::new(
            self.do_cleanup_sandbox(request.into_inner()).await?,
        ))
    }

    async fn inspect_sandbox(
        &self,
        request: Request<InspectSandboxRequest>,
    ) -> Result<Response<InspectSandboxResponse>, Status> {
        let req = request.into_inner();
        let entry = self.registry.get(&req.sandbox_id);
        if req.execution_only {
            let mut response = Response::new(InspectSandboxResponse {
                execution_fence: self.registry.execution(&req.sandbox_id).map(Into::into),
                sandbox_id: req.sandbox_id,
                ..Default::default()
            });
            #[allow(deprecated)]
            response.metadata_mut().insert(
                pplx_space_node_mgr_client::execution_fence::EXECUTION_AUTHORITY_CAPABILITY,
                tonic::metadata::MetadataValue::from_static("1"),
            );
            return Ok(response);
        }
        let nspawn = entry.as_ref().and_then(|e| e.nspawn_pid);
        let spaced = entry.as_ref().and_then(|e| e.spaced_host_pid);
        Ok(Response::new(InspectSandboxResponse {
            execution_fence: self.registry.execution(&req.sandbox_id).map(Into::into),
            sandbox_id: req.sandbox_id.clone(),
            slot: entry.as_ref().map(|e| e.slot),
            uid: entry.as_ref().map(|e| e.uid),
            fc_pid: nspawn,
            fc_alive: nspawn.is_some_and(process::is_alive),
            spaced_host_pid: spaced,
            spaced_host_alive: spaced.is_some_and(process::is_alive),
            cache_dir_exists: paths::instance_dir(&req.sandbox_id).exists(),
            rootfs_exists: paths::instance_rootfs(&req.sandbox_id).exists(),
            slot_allocated: entry.is_some(),
            template: entry.and_then(|e| e.template),
            ..Default::default()
        }))
    }

    async fn list_orphan_candidates(
        &self,
        _request: Request<ListOrphanCandidatesRequest>,
    ) -> Result<Response<ListOrphanCandidatesResponse>, Status> {
        Err(Status::unimplemented(
            "local simulator does not inspect host orphan resources",
        ))
    }

    async fn pause_sandbox(
        &self,
        request: Request<PauseSandboxRequest>,
    ) -> Result<Response<PauseSandboxResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        self.do_pause(&request.into_inner().sandbox_id, true);
        Ok(Response::new(PauseSandboxResponse {}))
    }

    async fn resume_sandbox(
        &self,
        request: Request<ResumeSandboxRequest>,
    ) -> Result<Response<ResumeSandboxResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        self.do_pause(&request.into_inner().sandbox_id, false);
        Ok(Response::new(ResumeSandboxResponse {}))
    }

    async fn get_firecracker_vm_state(
        &self,
        request: Request<GetFirecrackerVmStateRequest>,
    ) -> Result<Response<GetFirecrackerVmStateResponse>, Status> {
        let sandbox_id = request
            .into_inner()
            .sandbox_id
            .parse::<pplx_space_common_svc::core::types::SandboxId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let sandbox_id = sandbox_id.to_string();
        let entry = self
            .registry
            .get(&sandbox_id)
            .ok_or_else(|| Status::not_found("sandbox is not registered"))?;
        let state = if entry.paused {
            FirecrackerVmState::Paused
        } else if entry.nspawn_pid.is_some() {
            FirecrackerVmState::Running
        } else {
            FirecrackerVmState::NotStarted
        };
        Ok(Response::new(GetFirecrackerVmStateResponse {
            state: state.into(),
        }))
    }

    async fn capture_sandbox_endpoint_proof(
        &self,
        _request: Request<CaptureSandboxEndpointProofRequest>,
    ) -> Result<Response<SandboxEndpointProof>, Status> {
        Err(Status::unimplemented(
            "activation endpoint proof is available only for Firecracker runtimes",
        ))
    }

    async fn validate_sandbox_endpoint_proof(
        &self,
        _request: Request<ValidateSandboxEndpointProofRequest>,
    ) -> Result<Response<ValidateSandboxEndpointProofResponse>, Status> {
        Err(Status::unimplemented(
            "activation endpoint proof is available only for Firecracker runtimes",
        ))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        self.do_create_snapshot(&request.into_inner()).await?;
        Ok(Response::new(CreateSnapshotResponse {}))
    }

    async fn load_snapshot(
        &self,
        request: Request<LoadSnapshotRequest>,
    ) -> Result<Response<LoadSnapshotResponse>, Status> {
        let _guard = self
            .execution_guard(
                &request.get_ref().sandbox_id,
                request.get_ref().execution_fence.as_ref(),
                ExecutionOperation::Current,
            )
            .await?;
        // Filesystem-level restore: sandbox-manager has already reflinked the
        // pulled rootfs into the instance dir; the guest cold-boots from it in
        // LaunchSandbox. There is no VM memory image to load.
        Ok(Response::new(LoadSnapshotResponse {}))
    }

    async fn create_btrfs_snapshot(
        &self,
        _request: Request<CreateBtrfsSnapshotRequest>,
    ) -> Result<Response<CreateBtrfsSnapshotResponse>, Status> {
        Err(not_btrfs())
    }

    async fn delete_btrfs_snapshot(
        &self,
        _request: Request<DeleteBtrfsSnapshotRequest>,
    ) -> Result<Response<DeleteBtrfsSnapshotResponse>, Status> {
        Ok(Response::new(DeleteBtrfsSnapshotResponse {}))
    }

    async fn delete_cache_artifact(
        &self,
        request: Request<DeleteCacheArtifactRequest>,
    ) -> Result<Response<DeleteCacheArtifactResponse>, Status> {
        let req = request.into_inner();
        validate_cache_artifact_path(&req.local_path)?;

        if !Path::new(&req.local_path).exists() {
            return Ok(Response::new(DeleteCacheArtifactResponse {
                was_present: false,
            }));
        }

        std::fs::remove_dir_all(&req.local_path)
            .map_err(|error| internal("delete cache artifact", error))?;
        prune_empty_parents(&req.local_path, 2);
        Ok(Response::new(DeleteCacheArtifactResponse {
            was_present: true,
        }))
    }

    async fn list_btrfs_snapshots(
        &self,
        request: Request<ListBtrfsSnapshotsRequest>,
    ) -> Result<Response<ListBtrfsSnapshotsResponse>, Status> {
        let req = request.into_inner();
        if !req.page_token.is_empty() {
            return Err(Status::aborted("local snapshot cursor is no longer valid"));
        }
        let id = req
            .sandbox_id
            .parse::<pplx_space_common_svc::core::types::SandboxId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let directory =
            pplx_space_common_svc::core::host::linux::paths::sandbox_snapshots_dir(&id.to_string());
        Ok(Response::new(ListBtrfsSnapshotsResponse {
            snapshots: list_local_snapshots(Path::new(&directory)).await?,
            next_page_token: String::new(),
        }))
    }

    async fn rollback_btrfs_snapshot(
        &self,
        _request: Request<RollbackBtrfsSnapshotRequest>,
    ) -> Result<Response<RollbackBtrfsSnapshotResponse>, Status> {
        Err(not_btrfs())
    }

    async fn get_disk_usage(
        &self,
        request: Request<GetDiskUsageRequest>,
    ) -> Result<Response<GetDiskUsageResponse>, Status> {
        let req = request.into_inner();
        let mut usage_bytes = std::collections::HashMap::new();
        for id in req.sandbox_ids {
            let bytes = std::fs::metadata(paths::instance_rootfs(&id)).map_or(0, |m| m.len());
            usage_bytes.insert(id, bytes);
        }
        Ok(Response::new(GetDiskUsageResponse {
            usage_bytes,
            ..Default::default()
        }))
    }

    async fn sync_guest(
        &self,
        _request: Request<SyncGuestRequest>,
    ) -> Result<Response<SyncGuestResponse>, Status> {
        // Flush host page cache so a subsequent rootfs snapshot is consistent.
        nix_sync();
        Ok(Response::new(SyncGuestResponse {}))
    }

    async fn update_cgroup(
        &self,
        _request: Request<UpdateCgroupRequest>,
    ) -> Result<Response<UpdateCgroupResponse>, Status> {
        Ok(Response::new(UpdateCgroupResponse {}))
    }

    async fn cleanup_prepared_clone(
        &self,
        _request: Request<CleanupPreparedCloneRequest>,
    ) -> Result<Response<CleanupSandboxResponse>, Status> {
        Err(Status::unimplemented(
            "pool cloning is not supported on the local runtime",
        ))
    }

    async fn clone_sandbox_into_prepared_root(
        &self,
        _request: Request<CloneSandboxRequest>,
    ) -> Result<Response<CloneSandboxResponse>, Status> {
        Err(Status::unimplemented(
            "pool cloning is not supported on the local runtime",
        ))
    }

    async fn expose_port(
        &self,
        _request: Request<ExposePortRequest>,
    ) -> Result<Response<ExposePortResponse>, Status> {
        Ok(Response::new(ExposePortResponse {}))
    }

    async fn unexpose_port(
        &self,
        _request: Request<UnexposePortRequest>,
    ) -> Result<Response<UnexposePortResponse>, Status> {
        Ok(Response::new(UnexposePortResponse {}))
    }

    async fn allow_port(
        &self,
        _request: Request<AllowPortRequest>,
    ) -> Result<Response<AllowPortResponse>, Status> {
        Ok(Response::new(AllowPortResponse {}))
    }

    async fn deny_port(
        &self,
        _request: Request<DenyPortRequest>,
    ) -> Result<Response<DenyPortResponse>, Status> {
        Ok(Response::new(DenyPortResponse {}))
    }

    // The local node manager runs no runtime manager and has no node store.
    async fn publish_artifact(
        &self,
        _request: Request<PublishArtifactRequest>,
    ) -> Result<Response<PublishArtifactResponse>, Status> {
        Err(Status::unimplemented(
            "the local node manager has no artifact store",
        ))
    }

    async fn remove_artifact(
        &self,
        _request: Request<RemoveArtifactRequest>,
    ) -> Result<Response<RemoveArtifactResponse>, Status> {
        Err(Status::unimplemented(
            "the local node manager has no artifact store",
        ))
    }

    // ─── Btrfs delegation: force the Full ext4 codec ──────────────────────

    async fn btrfs_create_template_subvolume(
        &self,
        _request: Request<BtrfsCreateTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsCreateTemplateSubvolumeResponse>, Status> {
        Err(not_btrfs())
    }

    async fn btrfs_finalize_template_subvolume(
        &self,
        _request: Request<BtrfsFinalizeTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsFinalizeTemplateSubvolumeResponse>, Status> {
        Err(not_btrfs())
    }

    async fn btrfs_delete_template_subvolume(
        &self,
        _request: Request<BtrfsDeleteTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsDeleteTemplateSubvolumeResponse>, Status> {
        Ok(Response::new(BtrfsDeleteTemplateSubvolumeResponse {}))
    }

    async fn btrfs_encode_rootfs_send(
        &self,
        _request: Request<BtrfsEncodeRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeRootfsSendResponse>, Status> {
        Err(not_btrfs())
    }

    async fn btrfs_encode_snapshot_rootfs_send(
        &self,
        _request: Request<BtrfsEncodeSnapshotRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeSnapshotRootfsSendResponse>, Status> {
        Err(not_btrfs())
    }

    async fn btrfs_materialize_rootfs_send(
        &self,
        _request: Request<BtrfsMaterializeRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeRootfsSendResponse>, Status> {
        Err(not_btrfs())
    }

    async fn btrfs_inspect_send_source(
        &self,
        _request: Request<BtrfsInspectSendSourceRequest>,
    ) -> Result<Response<BtrfsInspectSendSourceResponse>, Status> {
        // This runtime never retains send sources, so there is nothing to
        // find. Unimplemented keeps callers from reading that as an absence.
        Err(Status::unimplemented(
            "local runtime does not retain btrfs send sources",
        ))
    }

    async fn get_btrfs_send_capabilities(
        &self,
        _request: Request<GetBtrfsSendCapabilitiesRequest>,
    ) -> Result<Response<GetBtrfsSendCapabilitiesResponse>, Status> {
        Ok(Response::new(GetBtrfsSendCapabilitiesResponse {
            supported: false,
            inspect_send_source: false,
        }))
    }

    type BtrfsReadSendSourceStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<BtrfsSendChunk, Status>> + Send>>;

    async fn btrfs_stage_send_source(
        &self,
        _request: Request<BtrfsStageSendSourceRequest>,
    ) -> Result<Response<BtrfsSendLease>, Status> {
        Err(not_btrfs())
    }
    async fn btrfs_read_send_source(
        &self,
        _request: Request<BtrfsSendLease>,
    ) -> Result<Response<Self::BtrfsReadSendSourceStream>, Status> {
        Err(not_btrfs())
    }
    async fn btrfs_delete_send_source(
        &self,
        _request: Request<BtrfsSendLease>,
    ) -> Result<Response<BtrfsDeleteSendSourceResponse>, Status> {
        Err(not_btrfs())
    }
    async fn btrfs_list_send_sources(
        &self,
        _request: Request<BtrfsListSendSourcesRequest>,
    ) -> Result<Response<BtrfsListSendSourcesResponse>, Status> {
        Err(not_btrfs())
    }

    async fn btrfs_receive_instance(
        &self,
        _request: Request<BtrfsReceiveInstanceRequest>,
    ) -> Result<Response<BtrfsReceiveInstanceResponse>, Status> {
        Err(not_btrfs())
    }

    async fn btrfs_delete_sandbox_instance(
        &self,
        _request: Request<BtrfsDeleteSandboxInstanceRequest>,
    ) -> Result<Response<BtrfsDeleteSandboxInstanceResponse>, Status> {
        Ok(Response::new(BtrfsDeleteSandboxInstanceResponse {}))
    }

    async fn prepare_encrypted_rootfs(
        &self,
        _request: Request<PrepareEncryptedRootfsRequest>,
    ) -> Result<Response<PrepareEncryptedRootfsResponse>, Status> {
        Err(no_device_mapper())
    }

    async fn prepare_encrypted_volume(
        &self,
        _request: Request<PrepareEncryptedVolumeRequest>,
    ) -> Result<Response<PrepareEncryptedVolumeResponse>, Status> {
        Err(no_device_mapper())
    }

    async fn prepare_snapshot_rootfs(
        &self,
        _request: Request<PrepareSnapshotRootfsRequest>,
    ) -> Result<Response<PrepareSnapshotRootfsResponse>, Status> {
        Err(no_device_mapper())
    }

    async fn release_encrypted_volume(
        &self,
        _request: Request<ReleaseEncryptedVolumeRequest>,
    ) -> Result<Response<ReleaseEncryptedVolumeResponse>, Status> {
        Err(no_device_mapper())
    }
}

/// The simulator returns one bounded page of its Full-codec snapshots. It has
/// no btrfs extent accounting; an unavailable measurement stays absent.
pub(crate) async fn list_local_snapshots(
    directory: &Path,
) -> Result<Vec<BtrfsSnapshotInfo>, Status> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut entries = match tokio::fs::read_dir(directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(Status::unavailable(format!(
                    "read local snapshots: {error}"
                )));
            }
        };
        let mut snapshots = Vec::new();
        for index in 0..=16_384 {
            let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?
            else {
                return Ok(snapshots);
            };
            if index == 16_384 {
                break;
            }
            let name = entry.file_name();
            let Some(rest) = name.to_str().and_then(|name| name.strip_prefix("f-")) else {
                continue;
            };
            if !entry
                .file_type()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?
                .is_dir()
            {
                continue;
            }
            let (tier, timestamp) = rest.split_once('-').unwrap_or(("", rest));
            snapshots.push(BtrfsSnapshotInfo {
                name: name.to_string_lossy().into_owned(),
                tier: tier.to_owned(),
                timestamp: timestamp.to_owned(),
                exclusive_bytes: None,
                referenced_bytes: None,
            });
        }
        Err(Status::resource_exhausted(
            "local snapshot inventory exceeds budget",
        ))
    })
    .await
    .map_err(|_| Status::resource_exhausted("local snapshot inventory timed out"))?
}

/// BYOK encrypted volumes need Linux device-mapper (dm-crypt/dm-snapshot),
/// which the local runtime does not provide. BYOK is never enabled for local
/// sandboxes, so this is a contract marker rather than a reachable path.
fn no_device_mapper() -> Status {
    Status::failed_precondition("local runtime has no device-mapper; BYOK volumes are unsupported")
}

/// The `FailedPrecondition` that pushes sandbox-manager onto the Full ext4
/// codec instead of `btrfs send`/`receive`.
fn not_btrfs() -> Status {
    Status::failed_precondition("local runtime is not btrfs-backed; use the Full ext4 codec")
}

/// MAC in the locally-administered range derived from the slot.
fn synthetic_mac(slot: u32) -> String {
    let b = slot.to_be_bytes();
    format!("02:00:{:02x}:{:02x}:{:02x}:{:02x}", b[0], b[1], b[2], b[3])
}

fn nix_sync() {
    nix::unistd::sync()
}
