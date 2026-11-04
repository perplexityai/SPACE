// Runtime inventory is externalized as host/kernel state, not an in-memory map:
// /space/run/<sandbox_id>/*.pid symlinks are the primary process inventory, and
// Firecracker's jailer pidfile is the crash-recovery fallback before fc.pid is
// installed.
use std::fs;
use std::os::unix::fs as unix_fs;

use pplx_space_common_core::host::linux::paths::SANDBOX_RUN_DIR;
use pplx_space_common_core::types::SandboxId;
use pplx_space_node_runtime_core::cgroup;
use pplx_space_node_runtime_core::launch;
use tonic::Status;

pub(crate) fn live_runtime_pid(
    sandbox_id: &SandboxId,
    filename: &str,
) -> Result<Option<u32>, Status> {
    let path = runtime_pid_path(sandbox_id, filename);
    live_runtime_pid_at(sandbox_id, &path)
}

fn live_runtime_pid_at(sandbox_id: &SandboxId, path: &str) -> Result<Option<u32>, Status> {
    let Some(pid) = read_pid_symlink(path) else {
        return Ok(None);
    };
    let sandbox_id_str = sandbox_id.to_string();
    if launch::is_pid_alive(pid) && launch::pid_belongs_to_sandbox(pid, &sandbox_id_str) {
        return Ok(Some(pid));
    }
    fs::remove_file(path)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(|error| Status::internal(format!("remove stale pid link {path}: {error}")))?;
    Ok(None)
}

pub(crate) fn live_firecracker_pidfile(
    sandbox_id: &SandboxId,
    chroot_base_dir: &str,
) -> Result<Option<u32>, Status> {
    let path = format!("{chroot_base_dir}/firecracker/{sandbox_id}/root/firecracker.pid");
    let Some(pid) = fs::read_to_string(&path)
        .ok()
        .and_then(|contents| contents.trim().parse::<u32>().ok())
    else {
        return Ok(None);
    };
    if launch::is_pid_alive(pid) && launch::pid_belongs_to_sandbox(pid, &sandbox_id.to_string()) {
        Ok(Some(pid))
    } else {
        Ok(None)
    }
}

pub(crate) fn live_sidecar_pid_from_cgroup(
    sandbox_id: &SandboxId,
    program_filename: &str,
) -> Result<Option<u32>, Status> {
    let sandbox_id_str = sandbox_id.to_string();
    let pids = match cgroup::sidecar_cgroup_process_ids(&sandbox_id_str) {
        Ok(pids) => pids,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Status::unavailable(format!(
                "inspect sidecar cgroup: {error}"
            )));
        }
    };
    for pid in pids {
        let handle = match launch::ProcessHandle::open(pid) {
            Ok(handle) => handle,
            Err(error) if error.is_not_found() => continue,
            Err(error) => return Err(Status::unavailable(format!("pin sidecar: {error}"))),
        };
        if handle
            .program_matches(program_filename)
            .map_err(|error| Status::unavailable(format!("inspect sidecar executable: {error}")))?
            != Some(true)
        {
            continue;
        }
        match handle
            .sandbox_ownership(&sandbox_id_str)
            .map_err(|error| Status::unavailable(format!("inspect sidecar ownership: {error}")))?
        {
            launch::ProcessOwnership::Owned => return Ok(Some(pid)),
            launch::ProcessOwnership::Gone => continue,
            launch::ProcessOwnership::Foreign => {
                return Err(Status::failed_precondition(
                    "live sidecar ownership mismatch",
                ));
            }
        }
    }
    Ok(None)
}

pub(crate) fn write_pid_symlink(
    sandbox_id: &SandboxId,
    filename: &str,
    pid: u32,
) -> Result<(), Status> {
    let sandbox_id_str = sandbox_id.to_string();
    if !launch::is_pid_alive(pid) || !launch::pid_belongs_to_sandbox(pid, &sandbox_id_str) {
        return Err(Status::failed_precondition(format!(
            "pid {pid} does not belong to sandbox {sandbox_id}"
        )));
    }
    let run_dir = format!("{SANDBOX_RUN_DIR}/{sandbox_id}");
    fs::create_dir_all(&run_dir)
        .map_err(|error| Status::internal(format!("create runtime dir {run_dir}: {error}")))?;
    let pid_link = format!("{run_dir}/{filename}");
    let tmp_link = format!("{run_dir}/.{filename}.{pid}.tmp");
    let _ = fs::remove_file(&tmp_link);
    unix_fs::symlink(format!("/proc/{pid}"), &tmp_link)
        .map_err(|error| Status::internal(format!("create pid link {tmp_link}: {error}")))?;
    fs::rename(&tmp_link, &pid_link)
        .map_err(|error| Status::internal(format!("install pid link {pid_link}: {error}")))?;
    Ok(())
}

pub(crate) fn sandbox_runtime_pids(sandbox_id: &SandboxId) -> Vec<(&'static str, u32)> {
    [
        ("firecracker", "fc.pid"),
        ("spaced-host", "space-spaced-host-svc.pid"),
        ("net-gateway", "space-netgtw-svc.pid"),
    ]
    .into_iter()
    .filter_map(|(name, filename)| {
        read_pid_symlink(&runtime_pid_path(sandbox_id, filename)).map(|pid| (name, pid))
    })
    .collect()
}

fn runtime_pid_path(sandbox_id: &SandboxId, filename: &str) -> String {
    format!("{SANDBOX_RUN_DIR}/{sandbox_id}/{filename}")
}

fn read_pid_symlink(path: &str) -> Option<u32> {
    fs::read_link(path).ok().and_then(|target| {
        target
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<u32>().ok())
    })
}

#[cfg(test)]
mod tests;
