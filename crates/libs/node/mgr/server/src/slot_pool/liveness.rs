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
    let [sidecar, vm, firecracker] = sandbox_runtime_evidence(&info.sandbox_id);
    LeaseLiveness::combine([sidecar, vm, tracked, firecracker])
}

/// Liveness of a released lease whose sandbox may already hold a newer lease
/// on another slot. The sandbox's cgroups and `fc.pid` are keyed by sandbox
/// ID, so once that successor runs they describe its runtime, not this slot's.
/// Only slot-scoped evidence can then hold the reset: the released lease's
/// own tracked PID, or any process still inside this slot's namespace.
pub(super) fn released_lease_liveness(
    info: &SlotInfo,
    successor: Option<&SlotInfo>,
) -> LeaseLiveness {
    let Some(successor) = successor.filter(|successor| successor.slot != info.slot) else {
        return lease_liveness(info);
    };
    // A recycled PID that now belongs to the successor is not this lease's.
    let tracked = match info.pid {
        Some(pid) if successor.pid == Some(pid) => LeaseLiveness::Gone,
        pid => pid_liveness(pid, &info.sandbox_id),
    };
    let namespace = netns_occupancy(
        Path::new("/proc"),
        Path::new(&NetId::from_slot(info.slot).ns_path()),
    );
    LeaseLiveness::combine([tracked, namespace])
}

/// Sidecar cgroup, VM cgroup and `fc.pid` evidence, all keyed by sandbox ID.
fn sandbox_runtime_evidence(sandbox_id: &str) -> [LeaseLiveness; 3] {
    #[cfg(test)]
    if let Some(evidence) = test_sandbox_runtime_evidence(sandbox_id) {
        return [evidence; 3];
    }
    let [sidecar, vm] =
        crate::cgroup::sandbox_cgroup_occupancy(sandbox_id).map(LeaseLiveness::observed);
    let pid_link = Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_RUN_DIR)
        .join(sandbox_id)
        .join("fc.pid");
    let firecracker = match std::fs::read_link(pid_link) {
        Ok(target) => match target
            .strip_prefix("/proc")
            .ok()
            .and_then(|tail| tail.to_str())
            .and_then(|tail| tail.parse::<u32>().ok())
        {
            Some(pid) => pid_liveness(Some(pid), sandbox_id),
            None => LeaseLiveness::Unknown,
        },
        Err(error) => LeaseLiveness::observed(Err(error)),
    };
    [sidecar, vm, firecracker]
}

#[cfg(test)]
static TEST_SANDBOX_RUNTIME_EVIDENCE: Mutex<Vec<(String, LeaseLiveness)>> = Mutex::new(Vec::new());

/// Stand in for host cgroups and `fc.pid`, which tests cannot create.
#[cfg(test)]
pub(super) fn set_test_sandbox_runtime_evidence(sandbox_id: &str, evidence: LeaseLiveness) {
    let mut overrides = TEST_SANDBOX_RUNTIME_EVIDENCE.lock().unwrap();
    overrides.retain(|(id, _)| id != sandbox_id);
    overrides.push((sandbox_id.to_string(), evidence));
}

#[cfg(test)]
fn test_sandbox_runtime_evidence(sandbox_id: &str) -> Option<LeaseLiveness> {
    TEST_SANDBOX_RUNTIME_EVIDENCE
        .lock()
        .unwrap()
        .iter()
        .find_map(|(id, evidence)| (id == sandbox_id).then_some(*evidence))
}

/// Whether any task is a member of the network namespace mounted at
/// `netns_path`. The VM and the sidecars of a lease run inside its slot's
/// namespace. An absent mount means no current slot namespace to occupy.
fn netns_occupancy(proc_root: &Path, netns_path: &Path) -> LeaseLiveness {
    use std::os::unix::fs::MetadataExt as _;
    let namespace = match std::fs::metadata(netns_path) {
        Ok(metadata) => (metadata.dev(), metadata.ino()),
        Err(error) => return LeaseLiveness::observed(Err(error)),
    };
    let membership = |task: &Path| {
        std::fs::metadata(task.join("ns/net"))
            .map(|metadata| (metadata.dev(), metadata.ino()) == namespace)
    };
    let Ok(entries) = std::fs::read_dir(proc_root) else {
        return LeaseLiveness::Unknown;
    };
    let mut evidence = LeaseLiveness::Gone;
    for entry in entries {
        let Ok(entry) = entry else {
            return LeaseLiveness::Unknown;
        };
        if !entry
            .file_name()
            .to_str()
            .is_some_and(|name| !name.is_empty() && name.bytes().all(|b| b.is_ascii_digit()))
        {
            continue;
        }
        let process = match membership(&entry.path()) {
            // An exited leader has no namespaces, but a VM's vCPU threads can
            // outlive it while they finish tearing the guest down.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::read_dir(entry.path().join("task")) {
                    Ok(tasks) => tasks
                        .map(|task| {
                            LeaseLiveness::observed(task.and_then(|task| membership(&task.path())))
                        })
                        .fold(LeaseLiveness::Gone, |left, right| {
                            LeaseLiveness::combine([left, right])
                        }),
                    Err(error) => LeaseLiveness::observed(Err(error)),
                }
            }
            result => LeaseLiveness::observed(result),
        };
        match process {
            LeaseLiveness::Live => return LeaseLiveness::Live,
            LeaseLiveness::Unknown => evidence = LeaseLiveness::Unknown,
            LeaseLiveness::Gone => {}
        }
    }
    evidence
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
