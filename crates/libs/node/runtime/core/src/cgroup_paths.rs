//! Where sandbox cgroups live, for readers that do not manage them.

use std::path::{Path, PathBuf};

pub(crate) const CGROUP_BASE: &str = "/sys/fs/cgroup/space";

/// The cgroup holding a sandbox's Firecracker process and vCPU threads, where
/// its CPU, memory and pids limits and usage live; `sbx-{id}` holds only the
/// sidecars.
pub fn jailer_cgroup_path(sandbox_id: &str) -> PathBuf {
    jailer_cgroup_path_in(Path::new(CGROUP_BASE), sandbox_id)
}

/// [`jailer_cgroup_path`] for a reader that sees the node's sandbox cgroup
/// parent, [`CGROUP_BASE`], mounted at `parent`. A reader in a container with
/// its own cgroup namespace needs one: its `/sys/fs/cgroup` is the
/// container's own cgroup, with no `space` child.
pub fn jailer_cgroup_path_in(parent: &Path, sandbox_id: &str) -> PathBuf {
    parent.join(sandbox_id)
}
