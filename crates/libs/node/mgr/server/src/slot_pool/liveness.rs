//! Reclamation requires agreement from independent runtime evidence. A stale
//! sidecar PID must not mask the VM, and a failed probe is not proof of death.

use super::*;
use std::os::unix::fs::MetadataExt as _;

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
}

/// Where the evidence that kept a lease from counting as gone came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EvidenceSource {
    SidecarCgroup,
    VmCgroup,
    FcPid,
    TrackedPid,
    /// A task inside the released slot's network namespace.
    NetnsMember,
    /// A process running as one of the released slot's UIDs.
    SlotUid,
    /// The slot's namespace mount could not be inspected.
    NetnsUnverifiable,
    /// The sandbox's cgroups hold tasks this PID namespace cannot see.
    PidNamespaceIncomplete,
    /// `/proc`, or one process in it, could not be read.
    ProcessUnreadable,
}

impl fmt::Display for EvidenceSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SidecarCgroup => "sidecar_cgroup",
            Self::VmCgroup => "vm_cgroup",
            Self::FcPid => "fc_pid",
            Self::TrackedPid => "tracked_pid",
            Self::NetnsMember => "netns_member",
            Self::SlotUid => "slot_uid",
            Self::NetnsUnverifiable => "netns_unverifiable",
            Self::PidNamespaceIncomplete => "pid_namespace_incomplete",
            Self::ProcessUnreadable => "process_unreadable",
        })
    }
}

/// The first evidence that a lease's runtime is `Live` or `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RuntimeBlocker {
    pub(super) liveness: LeaseLiveness,
    pub(super) source: EvidenceSource,
    pub(super) pid: Option<u32>,
}

impl RuntimeBlocker {
    /// `None` when `liveness` is `Gone`.
    pub(super) fn new(
        liveness: LeaseLiveness,
        source: EvidenceSource,
        pid: Option<u32>,
    ) -> Option<Self> {
        (liveness != LeaseLiveness::Gone).then_some(Self {
            liveness,
            source,
            pid,
        })
    }

    /// The first `Live` blocker, else the first `Unknown` one.
    fn first(evidence: impl IntoIterator<Item = Option<Self>>) -> Option<Self> {
        let mut unknown = None;
        for blocker in evidence.into_iter().flatten() {
            if blocker.liveness == LeaseLiveness::Live {
                return Some(blocker);
            }
            unknown = unknown.or(Some(blocker));
        }
        unknown
    }
}

impl fmt::Display for RuntimeBlocker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?} ({}", self.liveness, self.source)?;
        if let Some(pid) = self.pid {
            write!(formatter, " pid {pid}")?;
        }
        formatter.write_str(")")
    }
}

pub(super) fn lease_liveness(info: &SlotInfo) -> LeaseLiveness {
    lease_runtime(info).map_or(LeaseLiveness::Gone, |blocker| blocker.liveness)
}

fn lease_runtime(info: &SlotInfo) -> Option<RuntimeBlocker> {
    let tracked = tracked_pid_runtime(Path::new("/proc"), info);
    if info.sandbox_id.is_empty() {
        return tracked;
    }
    let [sidecar, vm, firecracker] = sandbox_runtime_evidence(&info.sandbox_id);
    RuntimeBlocker::first([sidecar, vm, tracked, firecracker])
}

fn tracked_pid_runtime(proc_root: &Path, info: &SlotInfo) -> Option<RuntimeBlocker> {
    let liveness = pid_liveness(proc_root, info.pid, &info.sandbox_id);
    RuntimeBlocker::new(liveness, EvidenceSource::TrackedPid, info.pid)
}

/// Runtime evidence for a released lease whose sandbox may already hold a
/// newer lease on another slot. The sandbox's cgroups and `fc.pid` are keyed
/// by sandbox ID, so once that successor runs they describe its runtime, not
/// this slot's. Only slot-scoped evidence can then hold the reset.
pub(super) fn released_lease_runtime(
    info: &SlotInfo,
    successor: Option<&SlotInfo>,
) -> Option<RuntimeBlocker> {
    let Some(successor) = successor.filter(|successor| successor.slot != info.slot) else {
        return lease_runtime(info);
    };
    #[cfg(test)]
    if let Some(evidence) = test_runtime_evidence(&info.sandbox_id) {
        return evidence.slot;
    }
    let proc_root = Path::new("/proc");
    let host = SlotHost {
        proc_root,
        namespace: mounted_namespace(proc_root, Path::new(&NetId::from_slot(info.slot).ns_path())),
        sandbox_cgroup_pids: sandbox_cgroup_pids(&info.sandbox_id),
    };
    released_lease_runtime_with(info, successor, &host)
}

/// The host state a released lease is judged by once its sandbox runs again
/// on a newer slot.
pub(super) struct SlotHost<'a> {
    pub(super) proc_root: &'a Path,
    /// Identity of the namespace mounted at the slot's path, or `None` when
    /// no namespace is mounted there.
    pub(super) namespace: std::io::Result<Option<NamespaceId>>,
    /// PIDs in the sandbox's VM and sidecar cgroups, as this PID namespace
    /// sees them. The kernel lists a task it cannot map here as `0`.
    pub(super) sandbox_cgroup_pids: std::io::Result<Vec<u32>>,
}

/// Device and inode that identify a namespace, shared by its bind mount and
/// every member's `/proc/<pid>/task/<tid>/ns/net`.
pub(super) type NamespaceId = (u64, u64);

/// Every task of the released lease runs under one of its slot's UIDs or,
/// for a jailer before it drops privileges, inside its slot's namespace.
/// Both are slot-scoped, so the successor's runtime matches neither.
///
/// The namespace mount is gone once an earlier attempt of this reset got
/// past this check and deleted it, then failed later in teardown. A process
/// cannot re-enter an unmounted namespace by path, so the slot's UIDs are
/// then the evidence that remains.
pub(super) fn released_lease_runtime_with(
    info: &SlotInfo,
    successor: &SlotInfo,
    host: &SlotHost<'_>,
) -> Option<RuntimeBlocker> {
    use EvidenceSource::{NetnsUnverifiable, PidNamespaceIncomplete};
    use LeaseLiveness::Unknown;
    // A recycled PID that now belongs to the successor is not this lease's.
    let tracked = if info.pid.is_some() && successor.pid == info.pid {
        None
    } else {
        tracked_pid_runtime(host.proc_root, info)
    };
    // A node-manager without the host's PID namespace sees none of the
    // runtime's tasks, and would find every slot clean.
    let pid_view = match &host.sandbox_cgroup_pids {
        Ok(pids) if !pids.contains(&0) => None,
        _ => RuntimeBlocker::new(Unknown, PidNamespaceIncomplete, None),
    };
    let (namespace, unverifiable) = match &host.namespace {
        Ok(namespace) => (*namespace, None),
        Err(_) => (None, RuntimeBlocker::new(Unknown, NetnsUnverifiable, None)),
    };
    let uids = [
        Some(info.sandbox_uid),
        pplx_space_common_core::host::linux::gateway::gateway_uid(
            pplx_space_common_core::types::LinuxUid::new(info.sandbox_uid),
        )
        .map(|uid| uid.get()),
    ];
    let processes = slot_processes(host.proc_root, namespace, &uids);
    RuntimeBlocker::first([tracked, pid_view, unverifiable, processes])
}

/// Identity of the namespace bind-mounted at `netns_path`. A path that is
/// not on nsfs, such as the placeholder left when an unmount succeeded but
/// the unlink did not, names no namespace.
fn mounted_namespace(proc_root: &Path, netns_path: &Path) -> std::io::Result<Option<NamespaceId>> {
    // Every namespace inode lives on the one nsfs superblock.
    let nsfs = std::fs::metadata(proc_root.join("self/ns/net"))?.dev();
    match std::fs::metadata(netns_path) {
        Ok(metadata) if metadata.dev() == nsfs => Ok(Some((metadata.dev(), metadata.ino()))),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn sandbox_cgroup_pids(sandbox_id: &str) -> std::io::Result<Vec<u32>> {
    let mut pids = Vec::new();
    for listed in [
        crate::cgroup::vm_cgroup_process_ids(sandbox_id),
        crate::cgroup::sidecar_cgroup_process_ids(sandbox_id),
    ] {
        match listed {
            Ok(listed) => pids.extend(listed),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(pids)
}

/// The first process running as one of `uids` or with any thread inside
/// `namespace`, else the first process that could not be read.
fn slot_processes(
    proc_root: &Path,
    namespace: Option<NamespaceId>,
    uids: &[Option<u32>],
) -> Option<RuntimeBlocker> {
    use EvidenceSource::{NetnsMember, ProcessUnreadable, SlotUid};
    use LeaseLiveness::{Live, Unknown};
    let Ok(entries) = std::fs::read_dir(proc_root) else {
        return RuntimeBlocker::new(Unknown, ProcessUnreadable, None);
    };
    let mut unreadable = None;
    for entry in entries {
        let Ok(entry) = entry else {
            return RuntimeBlocker::new(Unknown, ProcessUnreadable, None);
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .filter(|name| name.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let process = entry.path();
        let evidence = match process_uids(&process) {
            Ok(Some(process_uids)) if process_uids.iter().any(|uid| uids.contains(&Some(*uid))) => {
                Ok(Some(SlotUid))
            }
            Ok(Some(_)) => match namespace {
                Some(namespace) => tasks_in_namespace(&process, namespace)
                    .map(|member| member.then_some(NetnsMember)),
                None => Ok(None),
            },
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        };
        match evidence {
            Ok(Some(source)) => return RuntimeBlocker::new(Live, source, Some(pid)),
            Ok(None) => {}
            Err(_) => {
                unreadable = unreadable
                    .or_else(|| RuntimeBlocker::new(Unknown, ProcessUnreadable, Some(pid)));
            }
        }
    }
    unreadable
}

/// Real, effective, saved and filesystem UIDs, or `None` once it has exited.
fn process_uids(process: &Path) -> std::io::Result<Option<[u32; 4]>> {
    let status = match std::fs::read_to_string(process.join("status")) {
        Ok(status) => status,
        // ESRCH: it was reaped between the open and the read.
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let mut uids = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .into_iter()
        .flat_map(str::split_whitespace)
        .map(str::parse::<u32>);
    let mut parsed = [0; 4];
    for uid in &mut parsed {
        *uid = uids.next().and_then(Result::ok).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid Uid line")
        })?;
    }
    Ok(Some(parsed))
}

/// Whether any thread of `process` is in `namespace`. Namespaces are per
/// thread, and a VM's threads can outlive an exited leader.
fn tasks_in_namespace(process: &Path, namespace: NamespaceId) -> std::io::Result<bool> {
    let tasks = match std::fs::read_dir(process.join("task")) {
        Ok(tasks) => tasks,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for task in tasks {
        match std::fs::metadata(task?.path().join("ns/net")) {
            Ok(metadata) if (metadata.dev(), metadata.ino()) == namespace => return Ok(true),
            Ok(_) => {}
            // An exited thread, or a zombie leader, has no namespaces left.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

/// Sidecar cgroup, VM cgroup and `fc.pid` evidence, all keyed by sandbox ID.
fn sandbox_runtime_evidence(sandbox_id: &str) -> [Option<RuntimeBlocker>; 3] {
    use EvidenceSource::{FcPid, SidecarCgroup, VmCgroup};
    #[cfg(test)]
    if let Some(evidence) = test_runtime_evidence(sandbox_id) {
        return [SidecarCgroup, VmCgroup, FcPid]
            .map(|source| RuntimeBlocker::new(evidence.sandbox, source, None));
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
            Some(pid) => RuntimeBlocker::new(
                pid_liveness(Path::new("/proc"), Some(pid), sandbox_id),
                FcPid,
                Some(pid),
            ),
            None => RuntimeBlocker::new(LeaseLiveness::Unknown, FcPid, None),
        },
        Err(error) => RuntimeBlocker::new(LeaseLiveness::observed(Err(error)), FcPid, None),
    };
    [
        RuntimeBlocker::new(sidecar, SidecarCgroup, None),
        RuntimeBlocker::new(vm, VmCgroup, None),
        firecracker,
    ]
}

/// Stands in for host evidence that tests cannot create.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(super) struct TestRuntimeEvidence {
    /// The sandbox's cgroups and `fc.pid`.
    pub(super) sandbox: LeaseLiveness,
    /// The released slot's namespace, UIDs and PID view, once the sandbox
    /// holds a newer lease.
    pub(super) slot: Option<RuntimeBlocker>,
}

#[cfg(test)]
static TEST_RUNTIME_EVIDENCE: Mutex<Vec<(String, TestRuntimeEvidence)>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(super) fn set_test_runtime_evidence(sandbox_id: &str, evidence: TestRuntimeEvidence) {
    let mut overrides = TEST_RUNTIME_EVIDENCE.lock().unwrap();
    overrides.retain(|(id, _)| id != sandbox_id);
    overrides.push((sandbox_id.to_string(), evidence));
}

#[cfg(test)]
fn test_runtime_evidence(sandbox_id: &str) -> Option<TestRuntimeEvidence> {
    TEST_RUNTIME_EVIDENCE
        .lock()
        .unwrap()
        .iter()
        .find_map(|(id, evidence)| (id == sandbox_id).then_some(*evidence))
}

fn pid_liveness(proc_root: &Path, pid: Option<u32>, sandbox_id: &str) -> LeaseLiveness {
    let Some(pid) = pid else {
        return LeaseLiveness::Gone;
    };
    if crate::launch::validate_process_pid(pid).is_err() {
        return LeaseLiveness::Unknown;
    }
    LeaseLiveness::observed(process_owns_sandbox(
        &proc_root.join(pid.to_string()),
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
