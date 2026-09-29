//! SPACE Linux node host contracts.

pub mod gateway;

/// Signal the process pinned by a pidfd, including Linux real-time signals.
/// The kernel rejects descriptors that are not pidfds. The protocol accepts
/// numeric signals beyond nix's named variants; rustix also requires an unsafe
/// constructor for these values, with additional libc-reserved-signal constraints.
#[cfg(target_os = "linux")]
pub fn signal_pidfd(
    pidfd: impl std::os::fd::AsFd,
    signal: crate::types::ProcessSignal,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: the descriptor stays borrowed, ProcessSignal validates the
    // signal number, and a null siginfo is permitted with flags = 0.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_fd().as_raw_fd(),
            signal.as_i32(),
            std::ptr::null::<libc::siginfo_t>(),
            0u32,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// File-descriptor limit applied to Firecracker and sandbox sidecar processes.
pub const SANDBOX_PROCESS_NOFILE_LIMIT: u64 = 4096;

/// SPACE node Linux filesystem paths.
pub mod paths {
    use std::path::{Path, PathBuf};

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    pub use crate::types::node_fs::{
        FIRECRACKER_CACHE_DIR, FIRECRACKER_CACHE_PREFIX, FIRECRACKER_JAIL_DIR, FIRECRACKER_RUN_DIR,
        FORK_COPY_STAGING_DIR_NAME, SANDBOX_CACHE_DIR, SANDBOX_RUN_DIR, SPACE_CACHE_DIR,
        SPACE_RUN_DIR, SPACE_RUN_PREFIX, TEMPLATE_DIR,
    };

    /// Directory that contains SPACE binaries on a node.
    pub const SPACE_BIN_DIR: &str = "/space/bin";
    /// Root-owned parent of the gateway-only per-sandbox runtime directories.
    pub const NET_GATEWAY_RUN_DIR: &str = "/space/run/net-gateway";
    pub const TEMPLATE_CACHE_DIR_MODE: u32 = 0o2770;
    pub const TEMPLATE_CACHE_FILE_MODE: u32 = 0o660;

    pub const NODE_MANAGER_SOCKET: &str = "/space/run/node-manager/node-manager.sock";
    pub const NODE_MANAGER_STATE_DIR: &str = "/space/run/node-manager";
    pub const NODE_MANAGER_STATE_FILE: &str = "/space/run/node-manager/state.json";
    pub const NODE_MANAGER_DB: &str = "/space/run/node-manager/node-manager.db";
    pub const RUNTIME_SUPERVISOR_SOCKET: &str =
        "/space/run/runtime-supervisor/runtime-supervisor.sock";
    pub const RUNTIME_SUPERVISOR_STATE_DIR: &str = "/space/run/runtime-supervisor";
    pub const RUNTIME_MANAGER_SOCKET: &str = "/space/run/runtime-manager/runtime-manager.sock";
    /// The runtime manager's persistent state, on the reboot-persistent vendor
    /// volume rather than the recreatable cache or `/space/run`.
    pub const RUNTIME_MANAGER_STATE_DIR: &str = "/space/vendor/runtime-manager/state";
    /// The runtime manager's SQLite store, inside its state directory.
    pub const RUNTIME_MANAGER_DB: &str = "/space/vendor/runtime-manager/state/runtime-manager.db";
    /// The runtime manager's own kernel install root, `<version>/vmlinux.bin`.
    pub const RUNTIME_MANAGER_KERNELS_DIR: &str = "/space/vendor/runtime-manager/kernels";
    /// The runtime manager's own Firecracker release root,
    /// `<version>/bin/{firecracker,jailer}`.
    pub const RUNTIME_MANAGER_FIRECRACKER_DIR: &str = "/space/vendor/runtime-manager/firecracker";
    /// The root-owned manifest of what the node image pre-installed in the
    /// runtime manager's roots.
    pub const RUNTIME_MANAGER_SEED_MANIFEST: &str = "/space/vendor/runtime-manager/seed.json";
    /// Root-owned deployment configuration naming what the runtime manager may
    /// install, with the files of locally sourced entries.
    pub const RUNTIME_MANAGER_CATALOG_DIR: &str = "/space/etc/runtime-manager/catalog";
    pub const SANDBOX_MANAGER_SOCKET: &str = "/space/run/sandbox-manager/sandbox-manager.sock";
    pub const SANDBOX_MANAGER_DEPENDENCY_DRAIN_CAPABILITY: &str =
        "/space/run/dependency-drain/sandbox-manager/capability-v1.lock";
    pub const SANDBOX_MANAGER_DEPENDENCY_ADMISSION_GATE: &str =
        "/space/run/dependency-drain/sandbox-manager/admission-v1.lock";
    pub const SANDBOX_MANAGER_DEPENDENCY_WORK_GATE: &str =
        "/space/run/dependency-drain/sandbox-manager/work-v1.lock";
    pub const SANDBOX_HOST_CONTROL_SOCKET: &str =
        "/space/run/sandbox-host-control/host-control.sock";
    pub const TEMPLATE_MANAGER_SOCKET: &str = "/space/run/template-manager/template-manager.sock";
    pub const NODE_CTL_SOCKET: &str = "/space/run/node-ctl/node-ctl.sock";
    pub const BUILD_MANAGER_DB: &str = "/space/run/build-manager/build-manager.db";
    pub const CREDENTIAL_MANAGER_SOCKET: &str = "/space/run/credential-manager/credmgr.sock";
    pub const VOLUME_CREDENTIAL_MANAGER_SOCKET: &str = "/space/run/volume-credentials/credmgr.sock";
    pub const VOLUME_MANAGER_SOCKET: &str = "/space/run/volume-manager/volume-manager.sock";
    pub const VOLUME_MANAGER_DB: &str = "/space/run/volume-manager/volume-manager.db";
    /// Where vol-mgr reflinks each push source at submit time. Living under
    /// `/space/cache` (btrfs) keeps the reflink O(1) and on the same
    /// filesystem as the snapshot sources, so the staged file's bytes
    /// survive any later unlink of the original `local_path` — closing the
    /// race that produced suspend-time "entity not found" errors when a
    /// subsequent suspend cleared the prior snapshot dir before the upload
    /// dequeued.
    pub const VOLUME_MANAGER_STAGING_DIR: &str = "/space/cache/firecracker/.volmgr-staging";

    pub const SANDBOX_MANAGER_DB: &str = "/space/run/sandbox-manager/sbx-manager.db";
    pub const TEMPLATE_MANAGER_DB: &str = "/space/run/template-manager/template-manager.db";
    pub const NODE_CTL_DB: &str = "/space/run/node-ctl/node-ctl.db";

    pub const NET_GATEWAY_SVC_BIN: &str = "/space/bin/space-netgtw-svc";
    pub const SPACED_HOST_SVC_BIN: &str = "/space/bin/space-spaced-host-svc";
    pub const FIRECRACKER_VENDOR_DIR: &str = "/space/vendor/firecracker";
    pub const FIRECRACKER_ASSET_DIR: &str = "/space/cache/firecracker/assets";
    pub const FIRECRACKER_KERNEL_VENDOR_DIR: &str = "/space/vendor/kernels";
    pub const FIRECRACKER_KERNEL_ASSET_DIR: &str = "/space/cache/firecracker/kernels";
    pub const JAILER_BIN: &str = "/space/bin/jailer";
    pub const FIRECRACKER_BIN: &str = "/space/bin/firecracker";
    pub const DEFAULT_KERNEL: &str = "/space/cache/firecracker/kernels/default/vmlinux.bin";

    /// The two SPACE install bin-directory literals, usable in `concat!`
    /// (a `const` cannot feed `concat!`, so the macro is the single source
    /// and the named constants derive from it).
    macro_rules! space_bin_dir {
        (helm) => {
            "/opt/space/bin"
        };
        (systemd) => {
            "/space/bin"
        };
    }

    /// Helm-managed install bin directory used by Kubernetes pod commands.
    pub const SPACE_HELM_BIN_DIR: &str = space_bin_dir!(helm);
    /// systemd-managed install bin directory for operator-provisioned
    /// nodes. Entries here are symlinks into
    /// [`SPACE_SYSTEMD_APPS_DIR`], not the binaries themselves.
    pub const SPACE_SYSTEMD_BIN_DIR: &str = space_bin_dir!(systemd);
    /// Content-addressed install directory for systemd-managed service
    /// binaries, with one immutable
    /// `<name>-<sha256[:8]>` file per build, with the stable
    /// [`SPACE_SYSTEMD_BIN_DIR`] symlink repointed at the current one.
    /// An exec through the symlink makes `/proc/<pid>/exe` report the
    /// digest target, so peer binary checks must recognize this
    /// layout. Operator provisioning must preserve this layout.
    pub const SPACE_SYSTEMD_APPS_DIR: &str = "/space/apps";

    /// Build an allowlist of stable install paths for one or more SPACE
    /// service binaries, covering both deployment layouts
    /// ([`SPACE_HELM_BIN_DIR`] and [`SPACE_SYSTEMD_BIN_DIR`]) per name. A
    /// single binary serves either layout, so peer process checks
    /// must accept both. The Helm entry is the binary itself; the
    /// systemd entry is a symlink whose target the authorizer admits
    /// via check-time resolution and the [`SPACE_SYSTEMD_APPS_DIR`]
    /// content-addressed form. Keep this macro and the deployment
    /// scripts in lockstep when the install surface changes.
    ///
    /// Used to configure
    /// `pplx_space_common_svc::UdsPeerAccount::require_binary`.
    macro_rules! space_svc_binaries {
        ($($name:literal),+ $(,)?) => {
            &[$(
                concat!(space_bin_dir!(helm), "/", $name),
                concat!(space_bin_dir!(systemd), "/", $name),
            )+]
        };
    }

    // One allowlist per *uid* that peers a socket doing binary checks (the
    // account table is keyed by uid, so every binary that legitimately runs
    // as that uid must be listed). credmgr is the only server today, so
    // there is no `SPACE_CREDMGR_SVC_BINARIES`: nothing verifies credmgr's
    // own exe.
    //
    // Keep legacy volume-manager peers working while the new credential listener
    // rolls out. Remove this compatibility entry only after every volume-manager
    // has moved off the shared UID, including supported rollback versions.
    pub const SPACE_SBXMGR_UID_BINARIES: &[&str] =
        space_svc_binaries!("space-sbxmgr-svc", "space-volmgr-svc");
    pub const SPACE_VOLMGR_SVC_BINARIES: &[&str] = space_svc_binaries!("space-volmgr-svc");
    pub const SPACE_TMPLMGR_SVC_BINARIES: &[&str] = space_svc_binaries!("space-tmplmgr-svc");

    /// Marker the kernel appends to a `/proc/<pid>/exe` readlink target
    /// when the backing inode was unlinked while the process kept running.
    const EXE_DELETED_MARKER: &str = " (deleted)";

    /// Strip a trailing kernel ` (deleted)` marker from a `/proc/<pid>/exe`
    /// readlink target. The systemd deploy never unlinks a running inode's
    /// path (`deploy_binaries` uploads a fresh [`SPACE_SYSTEMD_APPS_DIR`]
    /// digest file and repoints the [`SPACE_SYSTEMD_BIN_DIR`] symlink); the
    /// marker's producers are in-place replacement under the Helm bin dir
    /// and any future GC of old digest installs. Conservative: only the
    /// literal trailing marker goes.
    pub fn strip_exe_deleted_marker(path: &Path) -> PathBuf {
        if let Some(s) = path.to_str()
            && let Some(stripped) = s.strip_suffix(EXE_DELETED_MARKER)
        {
            return PathBuf::from(stripped);
        }
        path.to_path_buf()
    }

    /// Whether a raw `/proc/<pid>/exe` readlink target carries the kernel's
    /// trailing ` (deleted)` marker — the backing inode was unlinked while
    /// the process kept running.
    pub fn exe_link_was_deleted(path: &Path) -> bool {
        path.to_str()
            .is_some_and(|s| s.ends_with(EXE_DELETED_MARKER))
    }

    /// True when `file_name` is `binary`'s content-addressed install name,
    /// `<binary>-<sha256[:8]>` (exactly 8 lowercase hex).
    pub fn is_content_addressed_binary_file_name(file_name: &str, binary: &str) -> bool {
        let Some(digest) = file_name
            .strip_prefix(binary)
            .and_then(|rest| rest.strip_prefix('-'))
        else {
            return false;
        };
        digest.len() == 8 && digest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
    }

    pub const SPACED_HOST_SOCKET_DIR: &str = SPACE_RUN_DIR;
    pub const SPACED_HOST_SOCKET_FILE: &str = "spaced-host.sock";

    /// Returns the default host-side UDS path for a socket file.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::host::linux::paths::default_host_uds_path;
    /// assert!(default_host_uds_path("spaced-host.sock").ends_with("spaced-host.sock"));
    /// ```
    pub fn default_host_uds_path(socket_file: &str) -> PathBuf {
        default_uds_path(SPACED_HOST_SOCKET_DIR, socket_file)
    }

    /// Returns a platform-appropriate default UDS path.
    pub fn default_uds_path(linux_socket_dir: &str, socket_file: &str) -> PathBuf {
        if cfg!(target_os = "linux") {
            PathBuf::from(linux_socket_dir)
        } else {
            std::env::temp_dir()
        }
        .join(socket_file)
    }

    /// Normalizes a sandbox id to the canonical on-disk form.
    ///
    /// SPACE accepts sandbox ids in two wire forms: the canonical
    /// hyphenated UUID (`Uuid::Display`, what `sbx-mgr` stores in its
    /// SQLite rows and what cluster-api stores in Postgres) and the
    /// compact 32-hex form produced by `SandboxSelector::parse` after it
    /// strips dashes for prefix matching. CLI invocations of
    /// `space-sbxmgr` parse user input through `SandboxSelector`, so the
    /// compact form propagates through `Command::*.sandbox_id.to_string()`
    /// onto the gRPC wire.
    ///
    /// Every per-sandbox path on disk is laid down using the canonical
    /// hyphenated form (sbx-mgr's `create` handler writes
    /// `Uuid::now_v7().to_string()` into its SQLite row, then path
    /// helpers stamp it onto `/space/cache/firecracker/sandboxes/<id>/`
    /// during suspend, snapshot, transfer, etc.). If a downstream handler
    /// receives the compact form from a `req.sandbox_id` and constructs
    /// a path from it without canonicalizing, it lands in a sibling
    /// directory that the rest of the system can't see — the bug that
    /// surfaced in cross-node snapshot restore.
    ///
    /// The helper is total: if the input parses as a UUID it returns the
    /// canonical hyphenated form; otherwise it returns the input as-is
    /// so test fixtures using non-UUID identifiers (`"sbx-1"` and friends)
    /// keep working. Idempotent: re-canonicalizing an already-canonical
    /// id is a no-op.
    fn canonical_sandbox_id(sandbox_id: &str) -> String {
        match uuid::Uuid::parse_str(sandbox_id) {
            Ok(uuid) => uuid.to_string(),
            Err(_) => sandbox_id.to_string(),
        }
    }

    /// Returns the sandbox runtime directory path.
    pub fn sandbox_run_dir(sandbox_id: &str) -> String {
        let id = canonical_sandbox_id(sandbox_id);
        format!("{SANDBOX_RUN_DIR}/{id}")
    }

    /// Returns the sandbox cache directory path.
    pub fn sandbox_cache_dir(sandbox_id: &str) -> String {
        let id = canonical_sandbox_id(sandbox_id);
        format!("{SANDBOX_CACHE_DIR}/{id}")
    }

    /// Returns the hidden cache directory owned by one restore transfer scope.
    pub fn sandbox_restore_staging_dir(sandbox_id: &str, transfer_scope_id: &str) -> String {
        sandbox_restore_staging_dir_in(Path::new(SANDBOX_CACHE_DIR), sandbox_id, transfer_scope_id)
            .to_string_lossy()
            .into_owned()
    }

    /// Returns the hidden restore directory under an injectable cache root.
    pub fn sandbox_restore_staging_dir_in(
        cache_dir: &Path,
        sandbox_id: &str,
        transfer_scope_id: &str,
    ) -> PathBuf {
        let id = canonical_sandbox_id(sandbox_id);
        cache_dir.join(format!(".{id}.restore-{transfer_scope_id}"))
    }

    /// Returns the sandbox instance directory path.
    pub fn sandbox_instance_dir(sandbox_id: &str) -> String {
        let id = canonical_sandbox_id(sandbox_id);
        format!("{SANDBOX_CACHE_DIR}/{id}/instance")
    }

    /// Returns the durable sandbox metadata sidecar path.
    pub fn sandbox_metadata_path(sandbox_id: &str) -> String {
        format!("{}/metadata.json", sandbox_cache_dir(sandbox_id))
    }

    /// Returns the sandbox rootfs path.
    pub fn sandbox_rootfs_path(sandbox_id: &str) -> String {
        format!("{}/rootfs.ext4", sandbox_instance_dir(sandbox_id))
    }

    /// Returns the sandbox snapshots directory path.
    pub fn sandbox_snapshots_dir(sandbox_id: &str) -> String {
        format!("{}/snapshots", sandbox_cache_dir(sandbox_id))
    }

    /// Returns the directory path for one sandbox snapshot.
    pub fn sandbox_snapshot_dir(sandbox_id: &str, snapshot_name: &str) -> String {
        format!("{}/{snapshot_name}", sandbox_snapshots_dir(sandbox_id))
    }

    /// Returns the metadata path for one sandbox snapshot.
    pub fn sandbox_snapshot_metadata_path(sandbox_id: &str, snapshot_name: &str) -> String {
        format!(
            "{}/snapshot.json",
            sandbox_snapshot_dir(sandbox_id, snapshot_name)
        )
    }

    /// Returns the spaced-host socket path for a sandbox.
    pub fn spaced_host_socket(sandbox_id: &str) -> String {
        let id = canonical_sandbox_id(sandbox_id);
        format!("{SANDBOX_RUN_DIR}/{id}/{SPACED_HOST_SOCKET_FILE}")
    }

    /// Returns the net-gateway control socket path for a sandbox.
    pub fn net_gateway_control_socket(sandbox_id: &str) -> String {
        let id = canonical_sandbox_id(sandbox_id);
        format!("{SANDBOX_RUN_DIR}/{id}/netgtw-control.sock")
    }

    /// Gateway-owned runtime directory, outside the VM control directory.
    pub fn isolated_net_gateway_run_dir(sandbox_id: &str) -> String {
        let id = canonical_sandbox_id(sandbox_id);
        format!("{NET_GATEWAY_RUN_DIR}/{id}")
    }

    /// Actual listener path for an isolated gateway. Node-manager publishes
    /// the existing control path as an alias for compatibility with clients.
    pub fn isolated_net_gateway_control_socket(sandbox_id: &str) -> String {
        format!("{}/control.sock", isolated_net_gateway_run_dir(sandbox_id))
    }

    /// Returns the build artifact directory for a template/content hash.
    pub fn template_build_dir(template: &str, content_hash: &str) -> String {
        format!("{TEMPLATE_DIR}/{template}/{content_hash}")
    }

    /// Returns the snapshot directory for a template/content hash and compute shape.
    pub fn template_snapshot_dir(
        template: &str,
        content_hash: &str,
        vcpu: u32,
        memory_mib: u32,
    ) -> String {
        format!("{TEMPLATE_DIR}/{template}/snapshots/{content_hash}/{vcpu}vcpu-{memory_mib}mib")
    }

    /// Returns the rootfs path for a template/content hash.
    pub fn template_rootfs_path(template: &str, content_hash: &str) -> String {
        format!("{}/rootfs.ext4", template_build_dir(template, content_hash))
    }

    /// Applies the shared SPACE template cache rwx bits to one path.
    ///
    /// Runtime services run with `RestrictSUIDSGID=true`, so they only manage
    /// normal rwx bits. Cache parent setgid bits come from provisioning, and
    /// on a freshly created child the kernel propagates that setgid bit from
    /// the parent inode (so the child also inherits the parent's group). We
    /// must preserve those inherited special bits when rewriting the rwx
    /// triplet — clobbering them strips the setgid from new staging dirs,
    /// which in turn breaks group inheritance for any file copied in
    /// afterward (the file gets the creator's primary group instead of
    /// `space-cache-rw`, leaving the sibling sandbox-manager service
    /// unable to read it).
    #[cfg(unix)]
    pub fn set_template_cache_permissions(
        path: impl AsRef<Path>,
        mode: u32,
    ) -> std::io::Result<()> {
        let path = path.as_ref();
        let metadata = std::fs::symlink_metadata(path)?;
        let current_mode = metadata.permissions().mode() & 0o7777;
        let desired_rwx = mode & 0o777;
        if current_mode & 0o777 == desired_rwx {
            return Ok(());
        }

        // Splice the new rwx triplet into the existing mode without touching
        // setuid/setgid/sticky bits.
        let desired_mode = (current_mode & !0o777) | desired_rwx;
        let mut permissions = metadata.permissions();
        permissions.set_mode(desired_mode);
        match std::fs::set_permissions(path, permissions) {
            Ok(()) => Ok(()),
            // Template artifacts can be owned by either space-tmplmgr or space-sbxmgr.
            // If the sibling service already left a safe superset of the requested mode,
            // do not fail just because this service is not the inode owner.
            Err(error)
                if error.kind() == std::io::ErrorKind::PermissionDenied
                    && current_mode & desired_rwx == desired_rwx =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Normalizes template cache directory/file rwx bits.
    ///
    /// Runtime services leave special bits such as setgid to provisioning or
    /// root-owned producers.
    #[cfg(unix)]
    pub fn normalize_template_cache_tree(path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        let metadata = std::fs::symlink_metadata(path)?;

        if metadata.is_dir() {
            set_template_cache_permissions(path, TEMPLATE_CACHE_DIR_MODE)?;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    normalize_template_cache_tree(entry.path())?;
                } else if file_type.is_file() {
                    set_template_cache_permissions(entry.path(), TEMPLATE_CACHE_FILE_MODE)?;
                }
            }
        } else if metadata.is_file() {
            set_template_cache_permissions(path, TEMPLATE_CACHE_FILE_MODE)?;
        }

        Ok(())
    }
}

/// SPACE node Linux users and groups.
///
/// The canonical source is `infra/space/identity/node-identities.sh`. This
/// module is generated at build time so Rust services and CLIs use the same
/// identity values as shell, Docker, Packer, and Helm deployment paths.
pub mod identities {
    include!(concat!(env!("OUT_DIR"), "/linux_identities.rs"));
}

#[cfg(test)]
mod tests;
