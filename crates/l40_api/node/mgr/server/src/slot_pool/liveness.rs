//! Reclamation requires agreement from independent runtime evidence. A stale
//! sidecar PID must not mask the VM, and a failed probe is not proof of death.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LeaseLiveness {
    Live,
    Gone,
    Unknown,
}

impl LeaseLiveness {
    fn observed(probe: std::io::Result<bool>) -> Self {
        match probe {
            Ok(true) => Self::Live,
            Ok(false) => Self::Gone,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Self::Gone,
            Err(_) => Self::Unknown,
        }
    }

    fn combine<const N: usize>(evidence: [Self; N]) -> Self {
        if evidence.contains(&Self::Live) {
            Self::Live
        } else if evidence.contains(&Self::Unknown) {
            Self::Unknown
        } else {
            Self::Gone
        }
    }
}

pub(super) fn lease_liveness(info: &SlotInfo) -> LeaseLiveness {
    let tracked = pid_liveness(info.pid, &info.sandbox_id);
    if info.sandbox_id.is_empty() {
        return tracked;
    }
    let [sidecar, vm] =
        crate::cgroup::sandbox_cgroup_occupancy(&info.sandbox_id).map(LeaseLiveness::observed);
    let pid_link = Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_RUN_DIR)
        .join(&info.sandbox_id)
        .join("fc.pid");
    let firecracker = match std::fs::read_link(pid_link) {
        Ok(target) => match target
            .strip_prefix("/proc")
            .ok()
            .and_then(|tail| tail.to_str())
            .and_then(|tail| tail.parse::<u32>().ok())
        {
            Some(pid) => pid_liveness(Some(pid), &info.sandbox_id),
            None => LeaseLiveness::Unknown,
        },
        Err(error) => LeaseLiveness::observed(Err(error)),
    };
    LeaseLiveness::combine([sidecar, vm, tracked, firecracker])
}

fn pid_liveness(pid: Option<u32>, sandbox_id: &str) -> LeaseLiveness {
    let Some(pid) = pid else {
        return LeaseLiveness::Gone;
    };
    if crate::launch::validate_process_pid(pid).is_err() {
        return LeaseLiveness::Unknown;
    }
    LeaseLiveness::observed(process_owns_sandbox(
        Path::new(&format!("/proc/{pid}")),
        sandbox_id,
    ))
}

fn process_owns_sandbox(proc_dir: &Path, sandbox_id: &str) -> std::io::Result<bool> {
    match read_process_ownership(proc_dir, sandbox_id) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if proc_dir.try_exists()? {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "process exists but ownership evidence is missing",
                ))
            } else {
                Ok(false)
            }
        }
        result => result,
    }
}

fn read_process_ownership(proc_dir: &Path, sandbox_id: &str) -> std::io::Result<bool> {
    let status = std::fs::read_to_string(proc_dir.join("status"))?;
    let state = status
        .lines()
        .find_map(|line| line.strip_prefix("State:"))
        .and_then(|value| value.split_whitespace().next());
    match state {
        Some("Z" | "X" | "x") => return Ok(false),
        Some("R" | "S" | "D" | "T" | "t" | "I" | "P") => {}
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing or invalid process state",
            ));
        }
    }
    if sandbox_id.is_empty() {
        return Ok(true);
    }
    let cgroups = std::fs::read_to_string(proc_dir.join("cgroup"))?;
    let sidecar = format!("sbx-{sandbox_id}");
    let mut parsed = false;
    for line in cgroups.lines() {
        let mut fields = line.splitn(3, ':');
        if fields.next().is_none() || fields.next().is_none() {
            continue;
        }
        if let Some(path) = fields.next().filter(|path| path.starts_with('/')) {
            parsed = true;
            let mut components = path.split('/');
            while let Some(component) = components.next() {
                if component == "space"
                    && components
                        .next()
                        .is_some_and(|id| id == sandbox_id || id == sidecar)
                {
                    return Ok(true);
                }
            }
        }
    }
    if !parsed {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid process cgroup membership",
        ));
    }
    // Retain the legacy launcher ownership marker, but match whole argv
    // tokens rather than substrings. Read failure remains unknown evidence.
    let cmdline = std::fs::read(proc_dir.join("cmdline"))?;
    Ok(cmdline
        .split(|byte| *byte == 0)
        .any(|arg| arg == sandbox_id.as_bytes()))
}

pub(super) fn lease_may_be_live(info: &SlotInfo) -> bool {
    lease_liveness(info) != LeaseLiveness::Gone
}

#[cfg(test)]
mod tests;
