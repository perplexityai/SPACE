//! Node filesystem roots and directory names that are plain strings.

use std::path::PathBuf;

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
