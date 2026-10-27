//! Registered process identity before launch and jail rebuild.

use super::*;

pub(in crate::service) fn registered_firecracker_pid(
    run_dir: &Path,
    sandbox_id: &str,
) -> Result<LinuxPid, Status> {
    let vm_processes = crate::cgroup::vm_cgroup_process_ids(sandbox_id).map_err(|error| {
        Status::unavailable(format!(
            "sandbox runtime cgroup membership is unavailable: {error}"
        ))
    })?;
    registered_firecracker_pid_with_vm_processes(run_dir, sandbox_id, &vm_processes)
}

/// Called under the launch/cleanup gate before touching a jail. Missing process
/// registration is not proof of absence when the VM cgroup is still populated.
pub(in crate::service) fn firecracker_before_jail_rebuild(
    sandbox_id: &str,
) -> Result<Option<LinuxPid>, Status> {
    let processes = match crate::cgroup::vm_cgroup_process_ids(sandbox_id) {
        Ok(processes) => processes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(Status::unavailable(format!(
                "observe VM ownership: {error}"
            )));
        }
    };
    if !processes.is_empty() {
        let pid = registered_firecracker_pid_with_vm_processes(
            Path::new(RUN_DIR),
            sandbox_id,
            &processes,
        )
        .map_err(|error| {
            Status::unavailable(format!("live VM ownership is unresolved: {error}"))
        })?;
        // Process ownership, not control-socket readiness, prevents rebuilding
        // a live runtime's filesystem. The adapter checks readiness on adoption.
        return Ok(Some(pid));
    }
    let pid_path = Path::new(RUN_DIR).join(sandbox_id).join("fc.pid");
    match std::fs::read_link(&pid_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Status::unavailable(format!(
            "observe runtime registration: {error}"
        ))),
        Ok(target) => {
            let pid = target
                .strip_prefix("/proc")
                .ok()
                .and_then(|tail| tail.to_str())
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or_else(|| Status::unavailable("malformed runtime registration"))?;
            match crate::launch::ProcessHandle::open(pid) {
                Err(error) if error.is_not_found() => Ok(None),
                Err(error) => Err(Status::unavailable(format!(
                    "observe registered runtime: {error}"
                ))),
                Ok(_) => Err(Status::unavailable(
                    "registered process remains outside observed VM cgroup; cleanup required",
                )),
            }
        }
    }
}

pub(in crate::service) fn registered_firecracker_pid_with_vm_processes(
    run_dir: &Path,
    sandbox_id: &str,
    vm_processes: &[u32],
) -> Result<LinuxPid, Status> {
    let pid_path = run_dir.join(sandbox_id).join("fc.pid");
    let pid = read_pid_from_symlink(&pid_path.to_string_lossy())
        .and_then(|pid| LinuxPid::try_from_u32(pid).ok())
        .ok_or_else(|| Status::unavailable("sandbox runtime pid is not registered"))?;
    if !vm_processes.contains(&pid.get().get()) {
        return Err(Status::permission_denied(
            "registered runtime pid is not in the sandbox VM cgroup",
        ));
    }
    Ok(pid)
}
