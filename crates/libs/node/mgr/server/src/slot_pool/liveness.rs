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
    if successor.is_none_or(|successor| successor.slot == info.slot) {
        return lease_runtime(info);
    }
    #[cfg(test)]
    if let Some(evidence) = test_runtime_evidence(&info.sandbox_id) {
        return evidence.slot;
    }
    let proc_root = Path::new("/proc");
    let host = SlotHost {
        proc_root,
        namespace: mounted_namespace(proc_root, Path::new(&NetId::from_slot(info.slot).ns_path())),
        sandbox_cgroups: sandbox_cgroups(&info.sandbox_id),
    };
    released_lease_runtime_with(info, &host)
}

/// The host state a released lease is judged by once its sandbox runs again
/// on a newer slot.
pub(super) struct SlotHost<'a> {
    pub(super) proc_root: &'a Path,
    /// Identity of the namespace mounted at the slot's path, or `None` when
    /// no namespace is mounted there.
    pub(super) namespace: std::io::Result<Option<NamespaceId>>,
    /// The sandbox's VM and sidecar cgroups that exist.
    pub(super) sandbox_cgroups: std::io::Result<Vec<CgroupMembers>>,
}

/// One cgroup's occupancy, and the PIDs its subtree lists as this PID
/// namespace sees them. The kernel lists a task it cannot map here as `0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CgroupMembers {
    pub(super) populated: bool,
    pub(super) pids: Vec<u32>,
}

/// Device and inode that identify a namespace, shared by its bind mount and
/// every member's `/proc/<pid>/task/<tid>/ns/net`.
pub(super) type NamespaceId = (u64, u64);

/// What makes a process the released lease's rather than the successor's.
struct ReleasedSlot<'a> {
    sandbox_id: &'a str,
    namespace: Option<NamespaceId>,
    /// The released slot's VM and gateway UIDs.
    uids: [Option<u32>; 2],
}

/// The released lease's Firecracker and sidecars run as its slot's VM or
/// gateway UID, inside the sandbox's cgroups. A jailer runs as root outside
/// the slot's namespace until it calls `setns`, then inside it until it
/// drops to the VM UID. The successor's processes run as another slot's UID
/// in another namespace, so they match neither.
///
/// The namespace mount is gone once an earlier attempt of this reset got
/// past this check and deleted it, then failed later in teardown. A process
/// cannot re-enter an unmounted namespace by path, so the slot's UIDs are
/// then the evidence that remains.
pub(super) fn released_lease_runtime_with(
    info: &SlotInfo,
    host: &SlotHost<'_>,
) -> Option<RuntimeBlocker> {
    use EvidenceSource::{NetnsUnverifiable, PidNamespaceIncomplete};
    use LeaseLiveness::Unknown;
    // A node-manager without the host's PID namespace sees none of the
    // runtime's tasks, and would find every slot clean. A populated cgroup
    // that lists no task can hide one that is exiting.
    let pid_view = match &host.sandbox_cgroups {
        Ok(cgroups)
            if cgroups.iter().all(|cgroup| {
                !cgroup.pids.contains(&0) && (!cgroup.populated || !cgroup.pids.is_empty())
            }) =>
        {
            None
        }
        _ => RuntimeBlocker::new(Unknown, PidNamespaceIncomplete, None),
    };
    let (namespace, unverifiable) = match &host.namespace {
        Ok(namespace) => (*namespace, None),
        Err(_) => (None, RuntimeBlocker::new(Unknown, NetnsUnverifiable, None)),
    };
    let slot = ReleasedSlot {
        sandbox_id: &info.sandbox_id,
        namespace,
        uids: [
            Some(info.sandbox_uid),
            pplx_space_common_core::host::linux::gateway::gateway_uid(
                pplx_space_common_core::types::LinuxUid::new(info.sandbox_uid),
            )
            .map(|uid| uid.get()),
        ],
    };
    let processes = slot_processes(host.proc_root, &slot, info.pid);
    RuntimeBlocker::first([pid_view, unverifiable, processes])
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

fn sandbox_cgroups(sandbox_id: &str) -> std::io::Result<Vec<CgroupMembers>> {
    let typed = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
    let mut cgroups = Vec::new();
    for path in [
        pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path(sandbox_id),
        PathBuf::from(crate::cgroup::sidecar_cgroup_path(&typed)),
    ] {
        let events = match std::fs::read_to_string(path.join("cgroup.events")) {
            Ok(events) => events,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !path.exists() => {
                continue;
            }
            Err(error) => return Err(error),
        };
        let populated = match events
            .lines()
            .find_map(|line| line.strip_prefix("populated "))
        {
            Some("0") => false,
            Some("1") => true,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing or invalid cgroup populated field",
                ));
            }
        };
        let mut pids = Vec::new();
        subtree_pids(&path, &mut pids)?;
        cgroups.push(CgroupMembers { populated, pids });
    }
    Ok(cgroups)
}

/// `cgroup.procs` lists only a cgroup's own tasks; `populated` covers its
/// descendants too.
fn subtree_pids(cgroup: &Path, pids: &mut Vec<u32>) -> std::io::Result<()> {
    for line in std::fs::read_to_string(cgroup.join("cgroup.procs"))?.lines() {
        pids.push(line.trim().parse().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid cgroup.procs entry",
            )
        })?);
    }
    for entry in std::fs::read_dir(cgroup)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            match subtree_pids(&entry.path(), pids) {
                // A child cgroup removed while this walk read it.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                result => result?,
            }
        }
    }
    Ok(())
}

/// The first process of the released lease, starting with its tracked PID,
/// else the first process that could not be read.
fn slot_processes(
    proc_root: &Path,
    slot: &ReleasedSlot<'_>,
    tracked: Option<u32>,
) -> Option<RuntimeBlocker> {
    use EvidenceSource::{ProcessUnreadable, TrackedPid};
    use LeaseLiveness::{Live, Unknown};
    let mut unreadable = None;
    if let Some(pid) = tracked {
        match lease_process(&proc_root.join(pid.to_string()), slot) {
            Ok(Some(_)) => return RuntimeBlocker::new(Live, TrackedPid, Some(pid)),
            Ok(None) => {}
            Err(_) => unreadable = RuntimeBlocker::new(Unknown, ProcessUnreadable, Some(pid)),
        }
    }
    let Ok(entries) = std::fs::read_dir(proc_root) else {
        return RuntimeBlocker::new(Unknown, ProcessUnreadable, None);
    };
    for entry in entries {
        let Ok(entry) = entry else {
            return RuntimeBlocker::new(Unknown, ProcessUnreadable, None);
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .filter(|name| name.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|name| name.parse::<u32>().ok())
            .filter(|pid| Some(*pid) != tracked)
        else {
            continue;
        };
        match lease_process(&entry.path(), slot) {
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

/// Whether `process` belongs to the released lease, and by which evidence.
fn lease_process(
    process: &Path,
    slot: &ReleasedSlot<'_>,
) -> std::io::Result<Option<EvidenceSource>> {
    let Some(status) = process_status(process)? else {
        return Ok(None);
    };
    if status
        .uids
        .iter()
        .any(|uid| slot.uids.contains(&Some(*uid)))
        && in_sandbox_cgroup_unless_exited(process, slot.sandbox_id)?
        && (status.live || has_live_thread(process)?)
    {
        return Ok(Some(EvidenceSource::SlotUid));
    }
    // A task leaves its namespaces as it exits, so only live threads match.
    match slot.namespace {
        Some(namespace) if tasks_in_namespace(process, namespace)? => {
            Ok(Some(EvidenceSource::NetnsMember))
        }
        _ => Ok(None),
    }
}

/// A process's `status`, or `None` once it has been reaped.
struct ProcessStatus {
    /// Whether the thread-group leader has not exited.
    live: bool,
    /// Real, effective, saved and filesystem UIDs.
    uids: [u32; 4],
}

fn process_status(process: &Path) -> std::io::Result<Option<ProcessStatus>> {
    let Some(status) = read_unless_exited(&process.join("status"))? else {
        return Ok(None);
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
    Ok(Some(ProcessStatus {
        live: state_is_live(&status),
        uids: parsed,
    }))
}

fn state_is_live(status: &str) -> bool {
    !matches!(
        status
            .lines()
            .find_map(|line| line.strip_prefix("State:"))
            .and_then(|value| value.split_whitespace().next()),
        Some("Z" | "X" | "x")
    )
}

/// `None` when the task has exited: ENOENT once it is reaped, ESRCH when
/// that happened between the open and the read.
fn read_unless_exited(path: &Path) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn in_sandbox_cgroup_unless_exited(process: &Path, sandbox_id: &str) -> std::io::Result<bool> {
    match in_sandbox_cgroup(process, sandbox_id) {
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            Ok(false)
        }
        result => result,
    }
}

/// Whether any thread of an exited leader is still running, as a VM's vCPU
/// threads can while they tear the guest down.
fn has_live_thread(process: &Path) -> std::io::Result<bool> {
    let tasks = match std::fs::read_dir(process.join("task")) {
        Ok(tasks) => tasks,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for task in tasks {
        if read_unless_exited(&task?.path().join("status"))?
            .is_some_and(|status| state_is_live(&status))
        {
            return Ok(true);
        }
    }
    Ok(false)
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
    if in_sandbox_cgroup(proc_dir, sandbox_id)? {
        return Ok(true);
    }
    // Retain the legacy launcher ownership marker, but match whole argv
    // tokens rather than substrings. Read failure remains unknown evidence.
    let cmdline = std::fs::read(proc_dir.join("cmdline"))?;
    Ok(cmdline
        .split(|byte| *byte == 0)
        .any(|arg| arg == sandbox_id.as_bytes()))
}

/// Whether the process is in the sandbox's VM cgroup (`space/<id>`) or its
/// sidecar cgroup (`space/sbx-<id>`), at any depth below either.
fn in_sandbox_cgroup(proc_dir: &Path, sandbox_id: &str) -> std::io::Result<bool> {
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
    Ok(false)
}

pub(super) fn lease_may_be_live(info: &SlotInfo) -> bool {
    lease_liveness(info) != LeaseLiveness::Gone
}

#[cfg(test)]
mod tests;
