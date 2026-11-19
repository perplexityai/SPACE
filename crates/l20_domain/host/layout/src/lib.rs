//! SPACE's node layout as data: the node's well-known paths and sockets
//! ([`paths`]), its users and groups ([`identities`]), the slot-derived
//! sandbox and gateway uids ([`gateway`]) and the sandbox process limits. It
//! does no I/O; the Linux mechanisms that act on it live in
//! `pplx-space-host-linux`.

pub mod gateway;

/// File-descriptor limit applied to Firecracker and sandbox sidecar processes.
pub const SANDBOX_PROCESS_NOFILE_LIMIT: u64 = 4096;

/// SPACE node Linux filesystem paths.
pub mod paths {
    use std::path::{Path, PathBuf};

    /// Default root for runtime state on a node.
    pub const SPACE_RUN_DIR: &str = "/space/run";
    /// Prefix used when validating paths that must live below `/space/run`.
    pub const SPACE_RUN_PREFIX: &str = "/space/run/";
    /// Default root for cached node artifacts.
    pub const SPACE_CACHE_DIR: &str = "/space/cache";
    /// Firecracker runtime state root.
    pub const FIRECRACKER_RUN_DIR: &str = "/space/run/firecracker";
    /// Firecracker cache root.
    pub const FIRECRACKER_CACHE_DIR: &str = "/space/cache/firecracker";
    /// Prefix used when validating paths that must live below the Firecracker cache.
    pub const FIRECRACKER_CACHE_PREFIX: &str = "/space/cache/firecracker/";
    /// Per-sandbox Firecracker runtime directory root.
    pub const SANDBOX_RUN_DIR: &str = "/space/run/firecracker/sandboxes";
    /// Per-sandbox Firecracker cache directory root.
    pub const SANDBOX_CACHE_DIR: &str = "/space/cache/firecracker/sandboxes";
    /// Reserved staging directory for cross-sandbox durable artifact copies.
    pub const FORK_COPY_STAGING_DIR_NAME: &str = ".fork-copies";
    /// Firecracker jail directory root.
    pub const FIRECRACKER_JAIL_DIR: &str = "/space/cache/firecracker/jails";
    /// Firecracker's log file, in its jail root.
    pub const FIRECRACKER_LOG_FILE: &str = "fc.log";
    /// The FIFO Firecracker writes its metrics flushes to, in its jail root.
    pub const FIRECRACKER_METRICS_FIFO: &str = "fc-metrics.fifo";

    /// A sandbox's jail root, the chroot the jailer builds under
    /// [`FIRECRACKER_JAIL_DIR`] as `<chroot base>/<exec file name>/<id>/root`.
    pub fn jail_root(sandbox_id: &str) -> PathBuf {
        PathBuf::from(format!(
            "{FIRECRACKER_JAIL_DIR}/firecracker/{sandbox_id}/root"
        ))
    }
    /// Template artifact directory root.
    pub const TEMPLATE_DIR: &str = "/space/cache/firecracker/templates";

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
    /// The cluster execution policy node-manager last applied, kept across restarts.
    pub const NODE_MANAGER_EXECUTION_POLICY_FILE: &str =
        "/space/run/node-manager/execution-policy.json";
    pub const RUNTIME_SUPERVISOR_SOCKET: &str =
        "/space/run/runtime-supervisor/runtime-supervisor.sock";
    pub const RUNTIME_SUPERVISOR_STATE_DIR: &str = "/space/run/runtime-supervisor";
    pub const RUNTIME_MANAGER_SOCKET: &str = "/space/run/runtime-manager/runtime-manager.sock";
    /// The runtime manager's state: its SQLite store beside its socket, as
    /// for the other node services.
    pub const RUNTIME_MANAGER_STATE_DIR: &str = "/space/run/runtime-manager";
    /// The runtime manager's SQLite store, inside its state directory.
    pub const RUNTIME_MANAGER_DB: &str = "/space/run/runtime-manager/runtime-manager.db";
    /// Where the runtime manager downloads and unpacks what it installs,
    /// private to it, one namespace per artifact kind
    /// (`firecracker/releases`, `firecracker/kernels`); node-manager
    /// publishes from here into the node store and creates it at startup,
    /// since the cache is recreated at boot.
    pub const RUNTIME_MANAGER_STAGING_DIR: &str = "/space/cache/.rtmgr-staging";
    /// Root-owned catalog files adding release and kernel pins to the
    /// compiled ones.
    pub const RUNTIME_MANAGER_CATALOG_DIR: &str = "/space/etc/runtime-manager/catalog";
    /// The node's root-owned store of runtime releases and guest kernels,
    /// which only node-manager publishes into.
    pub const NODE_ARTIFACTS_DIR: &str = "/space/vendor/artifacts";
    /// Managed Firecracker releases. Execution never scans this root.
    pub const NODE_FIRECRACKER_DIR: &str = "/space/vendor/artifacts/firecracker";
    /// Managed Cloud Hypervisor releases, separate from execution caches.
    pub const NODE_CLOUD_HYPERVISOR_DIR: &str = "/space/vendor/artifacts/cloud-hypervisor";
    /// Managed guest kernels. Existing vendor/cache execution paths stay separate.
    pub const NODE_KERNELS_DIR: &str = "/space/vendor/artifacts/kernels";
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
    /// `pplx_space_uds_peer_auth::UdsPeerAccount::require_binary`, with
    /// [`SPACE_SYSTEMD_BIN_DIR`] and [`SPACE_SYSTEMD_APPS_DIR`] as the
    /// authorizer's `ContentAddressedInstalls`.
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
    pub const SPACE_SBXMGR_UID_BINARIES: &[&str] = space_svc_binaries!("space-sbxmgr-svc");
    pub const SPACE_VOLMGR_SVC_BINARIES: &[&str] = space_svc_binaries!("space-volmgr-svc");
    pub const SPACE_TMPLMGR_SVC_BINARIES: &[&str] = space_svc_binaries!("space-tmplmgr-svc");

    pub const SPACED_HOST_SOCKET_DIR: &str = SPACE_RUN_DIR;
    pub const SPACED_HOST_SOCKET_FILE: &str = "spaced-host.sock";

    /// Returns the host-side UDS path for a socket file in `socket_dir`:
    /// [`SPACED_HOST_SOCKET_DIR`] on a Linux node, or a directory the caller
    /// picks elsewhere.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_host_layout::paths::{SPACED_HOST_SOCKET_DIR, host_uds_path};
    /// assert_eq!(
    ///     host_uds_path(SPACED_HOST_SOCKET_DIR, "spaced-host.sock"),
    ///     std::path::Path::new("/space/run/spaced-host.sock"),
    /// );
    /// ```
    pub fn host_uds_path(socket_dir: impl AsRef<Path>, socket_file: &str) -> PathBuf {
        socket_dir.as_ref().join(socket_file)
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

    /// Prefix of sandbox-manager's internal operation snapshots under
    /// [`sandbox_snapshots_dir`]. Users can't list, delete or roll back to them.
    pub const INTERNAL_OPERATION_SNAPSHOT_PREFIX: &str = "d-operation-";

    /// Returns the directory path for one sandbox snapshot.
    pub fn sandbox_snapshot_dir(sandbox_id: &str, snapshot_name: &str) -> String {
        sandbox_snapshot_dir_in(SANDBOX_CACHE_DIR, sandbox_id, snapshot_name)
    }

    /// Returns the directory path for one sandbox snapshot under an injectable
    /// cache root.
    pub fn sandbox_snapshot_dir_in(
        cache_root: &str,
        sandbox_id: &str,
        snapshot_name: &str,
    ) -> String {
        let id = canonical_sandbox_id(sandbox_id);
        format!("{cache_root}/{id}/snapshots/{snapshot_name}")
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
