/// Cgroup v2 management for per-sandbox resource limits.
///
/// Node-manager launches sidecars in `/sys/fs/cgroup/space/sbx-{id}/`.
/// Firecracker jailer creates the VM cgroup at `/sys/fs/cgroup/space/{id}/`.
/// The parent `/sys/fs/cgroup/space/` is created at provisioning time
/// (06-sysctl.sh) with controllers already enabled.
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pplx_space_common_core::types::{SandboxId, VcpuCount};

use crate::cgroup_paths::{CGROUP_BASE, jailer_cgroup_path};
const REMOVE_ATTEMPTS: usize = 55;
const REMOVE_FAST_RETRY_ATTEMPTS: usize = 50;
const REMOVE_FAST_RETRY_DELAY: Duration = Duration::from_millis(5);
const REMOVE_RETRY_DELAY: Duration = Duration::from_millis(50);
pub const DEFAULT_VCPU_COUNT: u32 = 2;
pub const DEFAULT_SANDBOX_GUEST_MEMORY_MAX_MIB: u64 = 8192;
pub const DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB: u64 = 128;
pub const DEFAULT_MEMORY_MAX_MIB: u64 =
    DEFAULT_SANDBOX_GUEST_MEMORY_MAX_MIB + DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB;
pub const DEFAULT_PARENT_CPU_RESERVED_COUNT: u32 = 2;
const MIB_BYTES: u64 = 1024 * 1024;
const CGROUP_CPU_PERIOD_US: u64 = 100_000;

#[derive(Default)]
pub struct CgroupCleanupReport {
    pub paths_present: usize,
    pub paths_removed: usize,
    pub attempts: usize,
    pub cgroup_kill_requests: usize,
    pub killed_pids: usize,
    pub child_cgroups_removed: usize,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to remove cgroup {path} after {attempts} attempts: {source}")]
struct CgroupRemovalError {
    path: PathBuf,
    attempts: usize,
    #[source]
    source: std::io::Error,
}

#[derive(Default)]
struct CgroupPathCleanupReport {
    present: bool,
    removed: bool,
    attempts: usize,
    cgroup_kill_requests: usize,
    killed_pids: usize,
    child_cgroups_removed: usize,
}

pub struct CgroupCreateResult {
    pub created: bool,
}

pub struct ParentCpuLimit {
    pub reserved_count: u32,
    pub cpu_max: String,
}

pub struct ParentMemoryLimit {
    pub sandbox_cgroup_memory_mib: u64,
    pub max_slots: u32,
    pub memory_max: String,
    pub memory_swap_max: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CgroupUpdateError {
    #[error("cgroup not found: {0}")]
    NotFound(PathBuf),
    #[error("{0}")]
    Invalid(String),
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Default resource limits for a sandbox cgroup.
#[derive(Clone)]
pub struct CgroupLimits {
    /// cpu.max value, e.g. "200000 100000" for 2 vCPUs
    pub cpu_max: String,
    /// memory.max in bytes
    pub memory_max: u64,
    /// pids.max
    pub pids_max: u32,
}

impl Default for CgroupLimits {
    fn default() -> Self {
        Self {
            cpu_max: format!("{} 100000", u64::from(DEFAULT_VCPU_COUNT) * 100_000),
            memory_max: DEFAULT_MEMORY_MAX_MIB * MIB_BYTES,
            pids_max: 256,
        }
    }
}

impl CgroupLimits {
    /// Recover the integer CPU budget from the exact limits passed to jailer.
    /// This is a scheduling ceiling, not a statement about guest topology.
    pub fn cpu_budget(&self) -> Result<VcpuCount, CgroupUpdateError> {
        let invalid = || CgroupUpdateError::Invalid("invalid resolved launch CPU budget".into());
        let (quota, period) = self.cpu_max.split_once(' ').ok_or_else(invalid)?;
        let quota: u64 = quota.parse().map_err(|_| invalid())?;
        let period: u64 = period.parse().map_err(|_| invalid())?;
        if period == 0 || !quota.is_multiple_of(period) {
            return Err(invalid());
        }
        let count = u32::try_from(quota / period).map_err(|_| invalid())?;
        VcpuCount::try_from_u32(count).map_err(|_| invalid())
    }

    /// Build limits from LaunchJailer request fields.
    pub fn from_request(
        vcpu_count: Option<u32>,
        memory_mib: Option<u64>,
        pids_max: Option<u32>,
    ) -> Self {
        let defaults = Self::default();
        let cpu_max = if let Some(vcpu_count) = vcpu_count {
            format!("{} 100000", vcpu_count as u64 * 100_000)
        } else {
            defaults.cpu_max
        };
        let memory_max = if let Some(memory_mib) = memory_mib {
            memory_mib * MIB_BYTES
        } else {
            defaults.memory_max
        };
        let pids_max = if let Some(pids_max) = pids_max {
            pids_max
        } else {
            defaults.pids_max
        };
        Self {
            cpu_max,
            memory_max,
            pids_max,
        }
    }
}

pub fn default_parent_cpu_reserved_count() -> u32 {
    let available = parent_effective_cpu_count().unwrap_or(DEFAULT_PARENT_CPU_RESERVED_COUNT);
    if available >= 64 {
        DEFAULT_PARENT_CPU_RESERVED_COUNT
    } else if available >= 8 {
        1
    } else {
        0
    }
}

pub fn apply_parent_cpu_reservation(reserved_count: u32) -> Result<ParentCpuLimit, String> {
    let cpu_max = parent_cpu_max_value(reserved_count);
    write_cgroup_file(CGROUP_BASE, "cpu.max", &cpu_max)?;
    Ok(ParentCpuLimit {
        reserved_count,
        cpu_max,
    })
}

pub fn sandbox_guest_memory_max_mib(sandbox_cgroup_memory_mib: u64) -> Option<u64> {
    sandbox_cgroup_memory_mib.checked_sub(DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB)
}

pub fn sandbox_cgroup_total_memory_mib(
    sandbox_cgroup_memory_mib: u64,
    max_slots: u32,
) -> Option<u64> {
    sandbox_cgroup_memory_mib.checked_mul(u64::from(max_slots))
}

pub fn apply_parent_memory_limit(
    sandbox_cgroup_memory_mib: u64,
    max_slots: u32,
) -> Result<ParentMemoryLimit, String> {
    if sandbox_cgroup_memory_mib <= DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB {
        return Err(format!(
            "sandbox cgroup memory must be greater than {} MiB",
            DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB
        ));
    }

    let total_memory_mib =
        sandbox_cgroup_total_memory_mib(sandbox_cgroup_memory_mib, max_slots).ok_or_else(|| {
            format!(
                "sandbox parent memory overflow: {sandbox_cgroup_memory_mib} MiB * {max_slots} slots"
            )
        })?;
    let memory_max_bytes = total_memory_mib.checked_mul(MIB_BYTES).ok_or_else(|| {
        format!("sandbox parent memory overflow: {total_memory_mib} MiB to bytes")
    })?;
    let memory_max = memory_max_bytes.to_string();
    let memory_swap_max = "0".to_string();

    write_cgroup_file(CGROUP_BASE, "memory.max", &memory_max)?;
    write_cgroup_file(CGROUP_BASE, "memory.swap.max", &memory_swap_max)?;

    Ok(ParentMemoryLimit {
        sandbox_cgroup_memory_mib,
        max_slots,
        memory_max,
        memory_swap_max,
    })
}

fn parent_cpu_max_value(reserved_count: u32) -> String {
    if reserved_count == 0 {
        return format!("max {CGROUP_CPU_PERIOD_US}");
    }
    let available = parent_effective_cpu_count().unwrap_or(reserved_count.saturating_add(1));
    let sandbox_cpu_count = available.saturating_sub(reserved_count).max(1);
    format!(
        "{} {CGROUP_CPU_PERIOD_US}",
        u64::from(sandbox_cpu_count) * CGROUP_CPU_PERIOD_US
    )
}

fn parent_effective_cpu_count() -> Option<u32> {
    read_cpu_list_count(&format!("{CGROUP_BASE}/cpuset.cpus.effective"))
        .or_else(|| read_cpu_list_count("/sys/devices/system/cpu/online"))
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .and_then(|count| u32::try_from(count.get()).ok())
        })
}

fn read_cpu_list_count(path: &str) -> Option<u32> {
    let contents = fs::read_to_string(path).ok()?;
    parse_cpu_list_count(contents.trim())
}

fn parse_cpu_list_count(value: &str) -> Option<u32> {
    if value.is_empty() {
        return None;
    }

    let mut count = 0u32;
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        if let Some((start, end)) = part.split_once('-') {
            let start = start.trim().parse::<u32>().ok()?;
            let end = end.trim().parse::<u32>().ok()?;
            if end < start {
                return None;
            }
            let range_count = end.checked_sub(start)?.checked_add(1)?;
            count = count.checked_add(range_count)?;
        } else {
            part.parse::<u32>().ok()?;
            count = count.checked_add(1)?;
        }
    }

    Some(count)
}

/// Create a cgroup for a sandbox and set resource limits.
/// Returns whether this call created the cgroup.
pub fn create_cgroup(
    sandbox_id: &SandboxId,
    limits: &CgroupLimits,
) -> Result<CgroupCreateResult, String> {
    let cgroup_path = sidecar_cgroup_path(sandbox_id);

    let created = match fs::create_dir(&cgroup_path) {
        Ok(()) => true,
        Err(error)
            if error.kind() == std::io::ErrorKind::AlreadyExists
                && Path::new(&cgroup_path).is_dir() =>
        {
            false
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(&cgroup_path)
                .map_err(|e| format!("failed to create cgroup {cgroup_path}: {e}"))?;
            true
        }
        Err(error) => return Err(format!("failed to create cgroup {cgroup_path}: {error}")),
    };

    if !created {
        return Ok(CgroupCreateResult { created });
    }

    // Set resource limits
    write_cgroup_file(&cgroup_path, "cpu.max", &limits.cpu_max)?;
    write_cgroup_file(&cgroup_path, "memory.max", &limits.memory_max.to_string())?;
    write_cgroup_file(&cgroup_path, "memory.swap.max", "0")?; // Prevent swap data remanence
    write_cgroup_file(&cgroup_path, "pids.max", &limits.pids_max.to_string())?;

    Ok(CgroupCreateResult { created })
}

/// Place the current process into a cgroup.
/// Must be called from the child process after fork, before exec.
pub fn join_cgroup(cgroup_path: &str) -> Result<(), String> {
    let procs_path = format!("{cgroup_path}/cgroup.procs");
    fs::write(&procs_path, "0").map_err(|e| format!("failed to write to {procs_path}: {e}"))
}

/// Validate the entire update before any file is written. Kernel I/O failures
/// can still leave a partial update across the individual cgroup files.
pub fn validate_cgroup_update(
    vcpu_count: Option<u32>,
    memory_mib: Option<u64>,
    pids_max: Option<u32>,
) -> Result<(), CgroupUpdateError> {
    if vcpu_count == Some(0) {
        return Err(CgroupUpdateError::Invalid(
            "vcpu_count must be greater than 0".into(),
        ));
    }
    if memory_mib.is_some_and(|value| value == 0 || value.checked_mul(MIB_BYTES).is_none()) {
        return Err(CgroupUpdateError::Invalid(
            "memory_mib must be nonzero and representable in bytes".into(),
        ));
    }
    if pids_max == Some(0) {
        return Err(CgroupUpdateError::Invalid(
            "pids_max must be greater than 0".into(),
        ));
    }
    Ok(())
}

/// Legacy executions have no allocation evidence. The caller must serialize
/// this read with the subsequent update and execution replacement. A reduction
/// does not establish a budget: after lowering, raising again stays forbidden.
pub fn cpu_quota_is_nonincreasing(
    sandbox_id: &str,
    vcpu_count: u32,
) -> Result<bool, CgroupUpdateError> {
    cpu_quota_is_nonincreasing_at(&jailer_cgroup_path(sandbox_id), vcpu_count)
}

fn cpu_quota_is_nonincreasing_at(
    cgroup_path: &Path,
    vcpu_count: u32,
) -> Result<bool, CgroupUpdateError> {
    let path = cgroup_path.join("cpu.max");
    let current =
        fs::read_to_string(&path).map_err(|source| CgroupUpdateError::Read { path, source })?;
    let invalid = || CgroupUpdateError::Invalid("invalid current CPU quota".into());
    let mut fields = current.split_whitespace();
    let quota = fields.next().ok_or_else(invalid)?;
    let period: u64 = fields
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    if period == 0 || fields.next().is_some() {
        return Err(invalid());
    }
    if quota == "max" {
        return Ok(true);
    }
    let quota: u64 = quota.parse().map_err(|_| invalid())?;
    Ok(u128::from(vcpu_count) * u128::from(period) <= u128::from(quota))
}

pub fn update_firecracker_cgroup(
    sandbox_id: &str,
    vcpu_count: Option<u32>,
    memory_mib: Option<u64>,
    pids_max: Option<u32>,
) -> Result<(), CgroupUpdateError> {
    update_cgroup_at(
        &jailer_cgroup_path(sandbox_id),
        vcpu_count,
        memory_mib,
        pids_max,
    )
}

fn update_cgroup_at(
    cgroup_path: &Path,
    vcpu_count: Option<u32>,
    memory_mib: Option<u64>,
    pids_max: Option<u32>,
) -> Result<(), CgroupUpdateError> {
    validate_cgroup_update(vcpu_count, memory_mib, pids_max)?;
    if !cgroup_path.exists() {
        return Err(CgroupUpdateError::NotFound(cgroup_path.to_owned()));
    }

    if let Some(vcpu_count) = vcpu_count {
        let cpu_max = format!("{} 100000", vcpu_count as u64 * 100_000);
        write_cgroup_file_typed(cgroup_path, "cpu.max", &cpu_max)?;
    }

    if let Some(memory_mib) = memory_mib {
        let memory_bytes = memory_mib * MIB_BYTES;
        write_cgroup_file_typed(cgroup_path, "memory.max", &memory_bytes.to_string())?;
    }

    if let Some(pids_max) = pids_max {
        write_cgroup_file_typed(cgroup_path, "pids.max", &pids_max.to_string())?;
    }

    Ok(())
}

pub fn sandbox_cgroup_processes(sandbox_id: &str) -> Vec<(PathBuf, u32)> {
    sandbox_cgroup_paths(sandbox_id)
        .into_iter()
        .flat_map(|path| {
            cgroup_process_ids(&path)
                .into_iter()
                .map(move |pid| (path.clone(), pid))
        })
        .collect()
}

pub fn sidecar_cgroup_process_ids(sandbox_id: &str) -> Result<Vec<u32>, std::io::Error> {
    cgroup_process_ids_result(&sidecar_cgroup_path_buf(sandbox_id))
}

/// PIDs in the sandbox's VM (jailer) cgroup, preserving the read error.
///
/// Cleanup liveness has to tell "this sandbox has no VM" apart
/// from "this could not be read", because only the first may authorize
/// stopping a runtime.
pub fn vm_cgroup_process_ids(sandbox_id: &str) -> Result<Vec<u32>, std::io::Error> {
    let procs_path = jailer_cgroup_path(sandbox_id).join("cgroup.procs");
    Ok(fs::read_to_string(procs_path)?
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect())
}

/// Fallible occupancy evidence for reclamation, including non-leader threads
/// and descendants. Missing or malformed fields never mean an empty cgroup.
pub fn sandbox_cgroup_occupancy(sandbox_id: &str) -> [std::io::Result<bool>; 2] {
    sandbox_cgroup_paths(sandbox_id).map(|path| read_cgroup_populated(&path))
}

/// Absence is quiescent only when the directory itself is absent. A cgroup
/// with unreadable or missing occupancy evidence must not authorize mutation.
pub fn sandbox_is_quiescent(sandbox_id: &str) -> std::io::Result<bool> {
    for path in sandbox_cgroup_paths(sandbox_id) {
        if !cgroup_is_quiescent(&path)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// VM-only occupancy check; sidecars can start concurrently with Firecracker.
pub fn firecracker_is_quiescent(sandbox_id: &str) -> std::io::Result<bool> {
    cgroup_is_quiescent(&jailer_cgroup_path(sandbox_id))
}

fn cgroup_is_quiescent(path: &Path) -> std::io::Result<bool> {
    match read_cgroup_populated(path) {
        Ok(populated) => Ok(!populated),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                && matches!(fs::symlink_metadata(path), Err(error) if error.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

fn read_cgroup_populated(path: &Path) -> std::io::Result<bool> {
    let events = fs::read_to_string(path.join("cgroup.events"))?;
    for line in events.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        match fields.as_slice() {
            ["populated", "0"] => return Ok(false),
            ["populated", "1"] => return Ok(true),
            _ => {}
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "missing or invalid cgroup populated field",
    ))
}

/// Reads the cgroup-v2 `populated` flag from `cgroup.events`.
///
/// `cgroup.procs` lists only thread-group leaders, so a lingering non-leader
/// thread (e.g. a Firecracker vCPU thread finishing VM teardown after the
/// leader has exited) does not appear there even though it still holds the
/// cgroup. The kernel `populated` flag accounts for any live task in the
/// cgroup or its descendants, so it is the authoritative "is this cgroup still
/// occupied?" signal. A missing/unreadable `cgroup.events` is treated as not
/// populated.
fn cgroup_populated(cgroup_path: &Path) -> bool {
    let Ok(contents) = fs::read_to_string(cgroup_path.join("cgroup.events")) else {
        return false;
    };
    contents.lines().any(|line| {
        let mut fields = line.split_whitespace();
        fields.next() == Some("populated") && fields.next() == Some("1")
    })
}

/// Returns the sandbox cgroup paths that still report `populated 1`.
///
/// Used by cleanup quiescence waits so they do not declare a sandbox drained
/// while a non-leader thread still occupies the cgroup (which would make the
/// subsequent `rmdir` fail with `EBUSY`).
pub fn sandbox_cgroups_populated(sandbox_id: &str) -> Vec<PathBuf> {
    sandbox_cgroup_paths(sandbox_id)
        .into_iter()
        .filter(|path| cgroup_populated(path))
        .collect()
}

/// Outcome of attempting to reclaim a single orphaned sandbox cgroup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanReclaimOutcome {
    /// The cgroup was unpopulated and its directory tree was removed.
    Reclaimed,
    /// The cgroup still has live tasks; left untouched. A populated cgroup is
    /// either a racing create or a sandbox whose lease record was lost — it is
    /// handled by the per-sandbox cleanup path, never force-killed here.
    Populated,
    /// The cgroup directory was already gone.
    Absent,
}

/// Lists every per-sandbox cgroup directory under the base, as `(sandbox_id,
/// path)`. Handles both the sidecar (`sbx-<id>`) and jailer (`<id>`) naming;
/// non-sandbox entries (control files, the parent's own files) are ignored.
pub fn list_sandbox_cgroup_dirs() -> Vec<(String, PathBuf)> {
    list_sandbox_cgroup_dirs_in(Path::new(CGROUP_BASE))
}

fn list_sandbox_cgroup_dirs_in(base: &Path) -> Vec<(String, PathBuf)> {
    let mut dirs = Vec::new();
    let Ok(entries) = fs::read_dir(base) else {
        return dirs;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let id = name
            .strip_prefix("sbx-")
            .map(|rest| rest.to_string())
            .unwrap_or(name);
        // Only treat the directory as a sandbox cgroup if the (stripped) name
        // parses as a SandboxId, so the destructive reclaim sweep never matches
        // a non-sandbox cgroup under the SPACE parent (e.g. a future dashed
        // cgroup name) — keeping it within its safety contract.
        if id.parse::<SandboxId>().is_err() {
            continue;
        }
        dirs.push((id, entry.path()));
    }
    dirs
}

/// Reclaim an orphaned (untracked) sandbox cgroup directory, but only if it is
/// unpopulated.
///
/// This never sends a signal: an unpopulated cgroup has no live tasks, so even
/// if the orphan classification raced a live sandbox this cannot disturb it — a
/// populated cgroup is reported as [`OrphanReclaimOutcome::Populated`] and left
/// for the per-sandbox cleanup path. Child cgroups are removed bottom-up.
pub fn reclaim_unpopulated_cgroup(cgroup_path: &Path) -> Result<OrphanReclaimOutcome, String> {
    if !cgroup_path.exists() {
        return Ok(OrphanReclaimOutcome::Absent);
    }
    if cgroup_populated(cgroup_path) {
        return Ok(OrphanReclaimOutcome::Populated);
    }
    remove_child_cgroups(cgroup_path);
    match fs::remove_dir(cgroup_path) {
        Ok(()) => Ok(OrphanReclaimOutcome::Reclaimed),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(OrphanReclaimOutcome::Reclaimed)
        }
        Err(error) => Err(format!(
            "failed to reclaim orphan cgroup {}: {error}",
            cgroup_path.display()
        )),
    }
}

/// Remove all cgroups owned by a sandbox.
///
/// SPACE creates two cgroup families per Firecracker sandbox:
/// - `sbx-{sandbox_id}` for sidecar processes launched by node-manager.
/// - `{sandbox_id}` for the Firecracker/jailer cgroup created by jailer.
///
/// Process exit and cgroup unpopulation can lag behind SIGKILL under high delete
/// fanout, so removal is retried briefly instead of doing a single best-effort
/// `rmdir` that can leave empty cgroups behind forever.
pub fn remove_sandbox_cgroups(sandbox_id: &str) -> Result<(), Vec<String>> {
    remove_sandbox_cgroups_with_report(sandbox_id).map(|_| ())
}

pub fn remove_sandbox_cgroups_with_report(
    sandbox_id: &str,
) -> Result<CgroupCleanupReport, Vec<String>> {
    remove_sandbox_cgroups_with_report_attempts(sandbox_id, REMOVE_ATTEMPTS)
}

fn remove_sandbox_cgroups_with_report_attempts(
    sandbox_id: &str,
    remove_attempts: usize,
) -> Result<CgroupCleanupReport, Vec<String>> {
    let typed_sandbox_id = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| vec![format!("invalid sandbox id {sandbox_id:?}: {error}")])?;
    let mut errors = Vec::new();
    let mut report = CgroupCleanupReport::default();

    for cgroup_path in sandbox_cgroup_paths(sandbox_id) {
        match remove_cgroup_path(typed_sandbox_id, &cgroup_path, remove_attempts) {
            Ok(path_report) => {
                if path_report.present {
                    report.paths_present += 1;
                }
                if path_report.removed {
                    report.paths_removed += 1;
                }
                report.attempts += path_report.attempts;
                report.cgroup_kill_requests += path_report.cgroup_kill_requests;
                report.killed_pids += path_report.killed_pids;
                report.child_cgroups_removed += path_report.child_cgroups_removed;
            }
            Err(error) => {
                errors.push(error.to_string());
            }
        }
    }

    if errors.is_empty() {
        Ok(report)
    } else {
        Err(errors)
    }
}

/// Remove the sidecar cgroup for a sandbox.
///
/// Kept for callers that intentionally only manage node-manager-launched
/// sidecars. Most sandbox cleanup paths should use `remove_sandbox_cgroups`.
pub fn remove_cgroup(sandbox_id: SandboxId) {
    let cgroup_path = PathBuf::from(sidecar_cgroup_path(&sandbox_id));
    if let Err(error) = remove_cgroup_path(sandbox_id, &cgroup_path, REMOVE_ATTEMPTS) {
        tracing::warn!(%sandbox_id, %error, "failed to remove sandbox sidecar cgroup");
    }
}

fn sandbox_cgroup_paths(sandbox_id: &str) -> [PathBuf; 2] {
    [
        sidecar_cgroup_path_buf(sandbox_id),
        jailer_cgroup_path(sandbox_id),
    ]
}

/// Return the cgroup path that must be serialized into a sidecar launch plan.
/// The typed sandbox ID keeps path components validated at this boundary.
pub fn sidecar_cgroup_path(sandbox_id: &SandboxId) -> String {
    format!("{CGROUP_BASE}/sbx-{sandbox_id}")
}

fn sidecar_cgroup_path_str(sandbox_id: &str) -> String {
    format!("{CGROUP_BASE}/sbx-{sandbox_id}")
}

fn sidecar_cgroup_path_buf(sandbox_id: &str) -> PathBuf {
    PathBuf::from(sidecar_cgroup_path_str(sandbox_id))
}

fn remove_cgroup_path(
    sandbox_id: SandboxId,
    cgroup_path: &Path,
    remove_attempts: usize,
) -> Result<CgroupPathCleanupReport, CgroupRemovalError> {
    if !cgroup_path.exists() {
        return Ok(CgroupPathCleanupReport::default());
    }

    let mut last_error = None;
    let mut report = CgroupPathCleanupReport {
        present: true,
        ..Default::default()
    };

    let cgroup_kill_supported = request_cgroup_kill(cgroup_path);
    if cgroup_kill_supported {
        report.cgroup_kill_requests += 1;
    }

    for attempt in 0..remove_attempts {
        report.attempts += 1;
        let killed_pids = if cgroup_kill_supported {
            0
        } else {
            kill_cgroup_processes(cgroup_path)
        };
        report.killed_pids += killed_pids;

        // `cgroup.kill` reaps the subtree's processes, but the child cgroup
        // directories themselves (e.g. a jailer-created nested cgroup) are not
        // removed by it, and `rmdir` of a parent that still has descendants
        // returns EBUSY. Remove any descendant cgroups bottom-up first.
        let child_cgroups_removed = remove_child_cgroups(cgroup_path);
        report.child_cgroups_removed += child_cgroups_removed;

        match fs::remove_dir(cgroup_path) {
            Ok(()) => {
                report.removed = true;
                return Ok(report);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                report.removed = true;
                return Ok(report);
            }
            Err(error) => {
                if error.raw_os_error() == Some(libc::EBUSY) {
                    let retry_delay =
                        (attempt + 1 < remove_attempts).then(|| remove_retry_delay(attempt));
                    let populated = read_cgroup_populated(cgroup_path);
                    let direct_processes = cgroup_process_ids_result(cgroup_path);
                    let direct_threads = cgroup_task_ids_result(cgroup_path, "cgroup.threads");
                    let direct_children = fs::read_dir(cgroup_path).map(|entries| {
                        entries
                            .flatten()
                            .filter_map(|entry| entry.file_type().ok())
                            .filter(std::fs::FileType::is_dir)
                            .count()
                    });
                    tracing::info!(
                        %sandbox_id,
                        cgroup_path = %cgroup_path.display(),
                        cgroup_remove_attempt = attempt + 1,
                        cgroup_remove_max_attempts = remove_attempts,
                        cgroup_retry_will_continue = retry_delay.is_some(),
                        cgroup_retry_delay_ms = retry_delay.map_or(0, |delay| delay.as_millis() as u64),
                        cgroup_kill_requested = cgroup_kill_supported,
                        cgroup_populated = populated.as_ref().copied().unwrap_or(false),
                        cgroup_population_known = populated.is_ok(),
                        cgroup_direct_process_count = direct_processes.as_ref().map_or(0, Vec::len),
                        cgroup_direct_process_count_known = direct_processes.is_ok(),
                        cgroup_direct_thread_count = direct_threads.as_ref().map_or(0, Vec::len),
                        cgroup_direct_thread_count_known = direct_threads.is_ok(),
                        cgroup_direct_child_count = direct_children.as_ref().copied().unwrap_or(0),
                        cgroup_direct_child_count_known = direct_children.is_ok(),
                        cgroup_pids_killed_this_attempt = killed_pids,
                        cgroup_child_dirs_removed_this_attempt = child_cgroups_removed,
                        cgroup_population_error = ?populated.as_ref().err(),
                        cgroup_process_read_error = ?direct_processes.as_ref().err(),
                        cgroup_thread_read_error = ?direct_threads.as_ref().err(),
                        cgroup_children_read_error = ?direct_children.as_ref().err(),
                        %error,
                        "cgroup removal returned EBUSY"
                    );
                }
                last_error = Some(error);
            }
        }

        if attempt + 1 < remove_attempts {
            std::thread::sleep(remove_retry_delay(attempt));
        }
    }

    Err(CgroupRemovalError {
        path: cgroup_path.to_path_buf(),
        attempts: report.attempts,
        source: last_error
            .unwrap_or_else(|| std::io::Error::other("cgroup removal was not attempted")),
    })
}

fn request_cgroup_kill(cgroup_path: &Path) -> bool {
    let mut file = match fs::OpenOptions::new()
        .write(true)
        .open(cgroup_path.join("cgroup.kill"))
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
        Err(_) => return false,
    };
    file.write_all(b"1").is_ok()
}

fn remove_retry_delay(attempt: usize) -> Duration {
    if attempt < REMOVE_FAST_RETRY_ATTEMPTS {
        REMOVE_FAST_RETRY_DELAY
    } else {
        REMOVE_RETRY_DELAY
    }
}

/// Recursively removes descendant cgroup directories bottom-up, returning the
/// count removed.
///
/// Best-effort per call: a child that still has live tasks fails its `rmdir`
/// and is retried on the parent's next attempt. Control files inside a cgroup
/// are not directories and are reclaimed by the kernel when the cgroup `rmdir`
/// succeeds, so only subdirectories are descended into.
fn remove_child_cgroups(cgroup_path: &Path) -> usize {
    let mut removed = 0;
    let Ok(entries) = fs::read_dir(cgroup_path) else {
        return removed;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let child = entry.path();
        removed += remove_child_cgroups(&child);
        if fs::remove_dir(&child).is_ok() {
            removed += 1;
        }
    }
    removed
}

fn kill_cgroup_processes(cgroup_path: &Path) -> usize {
    let mut killed = 0;
    for pid in cgroup_process_ids(cgroup_path) {
        let Ok(raw_pid) = i32::try_from(pid) else {
            tracing::warn!(pid, "skipping cgroup process with pid outside i32 range");
            continue;
        };
        match crate::launch::signal_raw_process(raw_pid, libc::SIGKILL) {
            Ok(true) => killed += 1,
            Ok(false) => tracing::debug!(pid, "cgroup process was already gone before SIGKILL"),
            Err(error) => tracing::warn!(pid, %error, "failed to SIGKILL cgroup process"),
        }
    }
    killed
}

fn cgroup_process_ids(cgroup_path: &Path) -> Vec<u32> {
    cgroup_process_ids_result(cgroup_path).unwrap_or_default()
}

fn cgroup_process_ids_result(cgroup_path: &Path) -> Result<Vec<u32>, std::io::Error> {
    cgroup_task_ids_result(cgroup_path, "cgroup.procs")
}

fn cgroup_task_ids_result(cgroup_path: &Path, task_file: &str) -> Result<Vec<u32>, std::io::Error> {
    Ok(fs::read_to_string(cgroup_path.join(task_file))?
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect())
}

fn write_cgroup_file(cgroup_path: &str, file: &str, value: &str) -> Result<(), String> {
    let path = format!("{cgroup_path}/{file}");
    fs::write(&path, value).map_err(|e| format!("failed to write {path}: {e}"))
}

fn write_cgroup_file_typed(
    cgroup_path: &Path,
    file: &str,
    value: &str,
) -> Result<(), CgroupUpdateError> {
    let path = cgroup_path.join(file);
    fs::write(&path, value).map_err(|source| CgroupUpdateError::Write {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests;
