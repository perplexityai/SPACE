//! Observational liveness from node-manager's runtime PID symlinks.

use std::path::Path;

use pplx_space_common_core::host::linux::paths::SANDBOX_RUN_DIR;

pub(crate) fn sidecar_alive(sandbox_id: &str, service_name: &str) -> bool {
    sidecar_live_pid(sandbox_id, service_name).is_some()
}

pub(crate) fn sidecar_live_pid(sandbox_id: &str, service_name: &str) -> Option<u32> {
    let path = format!("{SANDBOX_RUN_DIR}/{sandbox_id}/{service_name}.pid");
    live_pid_from_symlink(Path::new(&path))
}

fn live_pid_from_symlink(path: &Path) -> Option<u32> {
    // A regular file or dangling link is not a running sidecar. This is only
    // observational evidence; callers needing ownership still compare the PID
    // against the committed execution identity.
    if !path.exists() {
        return None;
    }
    std::fs::read_link(path)
        .ok()?
        .file_name()?
        .to_str()?
        .parse()
        .ok()
        .filter(|pid| *pid != 0)
}

#[cfg(test)]
mod tests;
