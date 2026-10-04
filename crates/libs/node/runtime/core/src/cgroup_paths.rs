//! Where sandbox cgroups live, for readers that do not manage them.

use std::path::PathBuf;

pub(crate) const CGROUP_BASE: &str = "/sys/fs/cgroup/space";

/// The cgroup holding a sandbox's Firecracker process and vCPU threads, where
/// its CPU, memory and pids limits and usage live; `sbx-{id}` holds only the
/// sidecars.
pub fn jailer_cgroup_path(sandbox_id: &str) -> PathBuf {
    PathBuf::from(format!("{CGROUP_BASE}/{sandbox_id}"))
}
