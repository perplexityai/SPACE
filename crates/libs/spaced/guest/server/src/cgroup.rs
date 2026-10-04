//! Guest-side cgroup v2 isolation for the spaced supervisor.
//!
//! Inside a Firecracker guest, systemd places spaced-svc and every user
//! workload it spawns in the single `spaced.service` cgroup. A runaway workload
//! (CPU spinner, memory hog, fork bomb) then competes with the supervisor for
//! the same CPU/memory/PID budget and can starve it, which makes host health
//! pings time out and the sandbox fail.
//!
//! We split the delegated `spaced.service` subtree into two leaves and apply
//! the minimal enforcement that protects the supervisor against each starvation
//! vector — none of which depends on the guest's RAM size:
//!   - CPU: `supervisor/cpu.weight` dominates, so spaced preempts a saturated
//!     workload the instant a ping arrives.
//!   - PIDs: `workloads/pids.max` caps workloads below the guest's effective
//!     task ceiling (`min(pid_max, threads-max)`), reserving task-creation
//!     headroom so a fork bomb can't starve the supervisor's own `fork`.
//!   - Memory: `workloads/memory.max` holds workloads below the guest's
//!     memory by a reserve, so a workload that runs out of memory is killed
//!     inside its own cgroup and the guest as a whole never runs out:
//!     spaced is not an OOM candidate and does not stall reclaiming for a
//!     workload. `supervisor/memory.min` keeps spaced's pages resident under
//!     the pressure that remains.
//!   - I/O: `supervisor/io.weight` dominates, so spaced's own disk I/O (reading
//!     `/proc`, fs metadata, writing logs) is serviced ahead of a workload
//!     saturating the device, rather than queuing behind it. This is the one
//!     vector that is **additive and best-effort**: `io` is not in
//!     `REQUIRED_CONTROLLERS`, so a guest that can't delegate it keeps the full
//!     cpu/memory/pids protection — we just skip the io step. (io.weight only
//!     bites under contention and only where the kernel's iocost model is
//!     active for the device.)
//!
//! `Delegate=yes` on the unit makes systemd enable the cpu/memory/pids (and,
//! where available, io) controllers for our subtree and stop managing it, which
//! is what lets us write `cgroup.subtree_control` and create the leaves.
//!
//! Setup is best-effort and never takes the daemon down. Enabling controllers
//! on `spaced.service` requires it to hold no processes, so the one
//! irreversible step is moving spaced-svc into `supervisor/` up front. That
//! move is gated behind a read of `cgroup.controllers`: the common failure — a
//! binary on a seed without `Delegate=yes` — is caught before any mutation and
//! leaves the original shared `spaced.service` cgroup untouched. A write that
//! fails *after* the move abandons the split with spaced — and the workloads
//! placed alongside it, since the placement hook never arms — left in
//! `supervisor/`: no isolation, the same unprotected state as no split, just
//! nested one level below the literal `spaced.service`. Either way spaced keeps
//! running.

use std::fs::File;
use std::sync::Arc;

/// Handle to the initialized guest cgroup split. Existence implies the
/// `workloads/` leaf is present and writable.
#[derive(Debug)]
pub(crate) struct GuestCgroups {
    /// Write handle to `workloads/cgroup.procs`, opened once at startup and held
    /// for the service's life so the per-spawn `pre_exec` placement is a single
    /// allocation-free `write`. Opening a path there instead would add syscalls
    /// (and a path buffer) in the post-`fork`/pre-`exec` window, where heap
    /// allocation can deadlock. It is `O_CLOEXEC` (Rust default), which matters
    /// twice: a forked child still inherits the fd (`fork` copies the fd table;
    /// `O_CLOEXEC` only fires on `exec`), so the placement write works — then
    /// `exec` closes it, so the spawned user program never inherits a writable
    /// handle to cgroup.procs.
    workloads_procs: Arc<File>,
}

impl GuestCgroups {
    /// Place spawned workloads by writing to `workloads_procs`, which a test
    /// can point at a full pipe to hold a child between fork and exec.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn with_workloads_procs(workloads_procs: File) -> Self {
        Self {
            workloads_procs: Arc::new(workloads_procs),
        }
    }

    /// Set up the supervisor/workloads split and return a handle used to place
    /// spawned workloads. Returns `None` — degrading to the prior shared-cgroup
    /// behavior — on any non-Linux target or setup failure. Never panics.
    pub(crate) fn initialize() -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            match linux::initialize() {
                Ok(cgroups) => {
                    tracing::info!("guest workload cgroups initialized");
                    Some(cgroups)
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "failed to initialize guest workload cgroups; \
                         supervisor and workloads will share a cgroup"
                    );
                    None
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }

    /// Register a `pre_exec` hook that moves the child into the `workloads/`
    /// cgroup before it can `exec` (and thus before it can fork), so the
    /// workload's descendants inherit the placement.
    ///
    /// Must be registered **before** the credential-drop hook: the move writes
    /// a root-owned `cgroup.procs`, and the child still holds root credentials
    /// here. Fails closed — a failed placement fails the spawn rather than
    /// letting a workload run unconfined in the supervisor cgroup.
    pub(crate) fn place_spawned_workload(&self, command: &mut tokio::process::Command) {
        let workloads_procs = Arc::clone(&self.workloads_procs);
        // SAFETY: the closure runs in the forked child after `fork` and before
        // `exec`. It issues a single async-signal-safe `write` of a fixed
        // buffer — no heap allocation and no locks a thread left behind by
        // `fork` in the multi-threaded parent could still hold.
        unsafe {
            command.pre_exec(move || place_in_workloads(&workloads_procs));
        }
    }
}

/// Move the calling (forked) process into the workloads cgroup behind `fd`.
/// Writing "0" to `cgroup.procs` moves the writing task — the kernel resolves
/// pid 0 to `current` — so we needn't format our own pid between fork and exec.
fn place_in_workloads(workloads_procs: &File) -> std::io::Result<()> {
    // cgroup.procs accepts the pid in a single async-signal-safe write.
    let written = nix::unistd::write(workloads_procs, b"0")?;
    if written != 1 {
        return Err(std::io::ErrorKind::WriteZero.into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{
        GuestCgroups, has_controller, missing_controllers, parse_unified_cgroup,
        workloads_memory_max_from, workloads_pids_max_from,
    };
    use std::path::{Path, PathBuf};

    const CGROUP_ROOT: &str = "/sys/fs/cgroup";

    // spaced-svc is near-idle, so a dominating weight costs workloads nothing in
    // practice (unused weight is not reserved) but guarantees the supervisor
    // preempts a saturated workload the instant a host ping arrives. cpu.weight
    // only bites under contention.
    const SUPERVISOR_CPU_WEIGHT: u32 = 10_000;
    // The I/O analog of the cpu.weight dominance: spaced is near-idle on disk,
    // so a dominating io.weight costs a busy workload nothing in practice (unused
    // weight is not reserved) but lets the supervisor's I/O preempt a workload
    // saturating the device. io.weight only takes effect under contention and
    // only where the kernel's iocost model is active. Range is 1..=10_000.
    const SUPERVISOR_IO_WEIGHT: u32 = 10_000;
    // Unreclaimable floor for the supervisor's resident set.
    const SUPERVISOR_MEMORY_MIN_BYTES: u64 = 64 * 1024 * 1024;
    #[derive(Debug, thiserror::Error)]
    pub(super) enum CgroupSetupError {
        #[error("reading {path}: {source}")]
        Read {
            path: PathBuf,
            #[source]
            source: std::io::Error,
        },
        #[error("writing {value:?} to {path}: {source}")]
        Write {
            path: PathBuf,
            value: String,
            #[source]
            source: std::io::Error,
        },
        #[error("creating cgroup {path}: {source}")]
        CreateDir {
            path: PathBuf,
            #[source]
            source: std::io::Error,
        },
        #[error("opening {path}: {source}")]
        Open {
            path: PathBuf,
            #[source]
            source: std::io::Error,
        },
        #[error("no unified cgroup v2 entry for this process in /proc/self/cgroup")]
        NoUnifiedCgroup,
        #[error("this process is in the cgroup root; refusing to manage it")]
        CgroupRoot,
        #[error(
            "required cgroup controllers not delegated (missing: {missing:?}; available: {available:?}); is Delegate=yes set on the unit?"
        )]
        ControllersUnavailable { missing: String, available: String },
    }

    /// The two leaf cgroups created under the delegated `spaced.service`
    /// subtree.
    #[derive(Clone, Copy)]
    enum Leaf {
        Supervisor,
        Workloads,
    }

    impl Leaf {
        fn as_str(self) -> &'static str {
            match self {
                Leaf::Supervisor => "supervisor",
                Leaf::Workloads => "workloads",
            }
        }
    }

    pub(super) fn initialize() -> Result<GuestCgroups, CgroupSetupError> {
        let base = own_cgroup_dir()?;

        // Pre-flight before any mutation. Moving spaced into `supervisor/`
        // (below) is irreversible, and the `subtree_control` write after it
        // fails if these controllers were never delegated — e.g. a new binary
        // on an older seed without `Delegate=yes`. That would strand spaced in
        // `supervisor/` with a half-built split. This read predicts that
        // failure with zero side effects, so we degrade cleanly instead.
        ensure_controllers_available(&base)?;

        let supervisor = base.join(Leaf::Supervisor.as_str());
        let workloads = base.join(Leaf::Workloads.as_str());

        create_dir(&supervisor)?;
        create_dir(&workloads)?;

        // Move spaced-svc into supervisor/ first (writing "0" moves the writing
        // process), so spaced.service is empty of processes and we may enable
        // controllers on its subtree (the "no internal processes" rule).
        write_cgroup(&supervisor.join("cgroup.procs"), "0")?;

        // Delegate=yes makes these controllers available; enabling them here is
        // what materializes cpu.weight/memory.min/pids.max in the leaves.
        write_cgroup(&base.join("cgroup.subtree_control"), "+cpu +memory +pids")?;

        // Per-leaf resource limits — one row per knob, applied now that the
        // controllers are delegated. Order-independent, so kept declarative.
        let mut limits = vec![
            (
                Leaf::Supervisor,
                "cpu.weight",
                SUPERVISOR_CPU_WEIGHT.to_string(),
            ),
            (
                Leaf::Supervisor,
                "memory.min",
                SUPERVISOR_MEMORY_MIN_BYTES.to_string(),
            ),
        ];
        if let Some(pids_max) = workloads_pids_max() {
            limits.push((Leaf::Workloads, "pids.max", pids_max.to_string()));
        }
        if let Some(memory_max) = workloads_memory_max() {
            limits.push((Leaf::Workloads, "memory.max", memory_max.to_string()));
        }
        for (leaf, file, value) in &limits {
            write_cgroup(&base.join(leaf.as_str()).join(file), value)?;
        }

        // Additive, best-effort I/O priority. Unlike cpu/memory/pids this is not
        // a precondition of the split: if `io` was never delegated, or any io
        // write fails, log and leave the (cpu/memory/pids) split intact rather
        // than failing the whole setup.
        if let Err(error) = apply_supervisor_io_weight(&base) {
            tracing::warn!(
                %error,
                "supervisor io.weight not applied; cpu/memory/pids protection intact"
            );
        }

        let procs_path = workloads.join("cgroup.procs");
        let workloads_procs = std::fs::OpenOptions::new()
            .write(true)
            .open(&procs_path)
            .map_err(|source| CgroupSetupError::Open {
                path: procs_path,
                source,
            })?;

        Ok(GuestCgroups {
            workloads_procs: workloads_procs.into(),
        })
    }

    /// Resolve this process's unified cgroup v2 directory under `/sys/fs/cgroup`
    /// from `/proc/self/cgroup`.
    fn own_cgroup_dir() -> Result<PathBuf, CgroupSetupError> {
        let path = Path::new("/proc/self/cgroup");
        let content = std::fs::read_to_string(path).map_err(|source| CgroupSetupError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let relative = parse_unified_cgroup(&content)
            .ok_or(CgroupSetupError::NoUnifiedCgroup)?
            .trim_start_matches('/');
        if relative.is_empty() {
            return Err(CgroupSetupError::CgroupRoot);
        }
        Ok(Path::new(CGROUP_ROOT).join(relative))
    }

    /// Verify the controllers the split needs are delegated to `base` before we
    /// mutate anything. A cgroup's `cgroup.controllers` lists the controllers
    /// available for use in it — the set `Delegate=yes` populates. Reading it is
    /// side-effect free, so a missing controller lets us bail before the
    /// irreversible move rather than after.
    fn ensure_controllers_available(base: &Path) -> Result<(), CgroupSetupError> {
        let path = base.join("cgroup.controllers");
        let available = std::fs::read_to_string(&path)
            .map_err(|source| CgroupSetupError::Read { path, source })?;
        let missing = missing_controllers(&available);
        if !missing.is_empty() {
            return Err(CgroupSetupError::ControllersUnavailable {
                missing: missing.join(" "),
                available: available.split_whitespace().collect::<Vec<_>>().join(" "),
            });
        }
        Ok(())
    }

    /// Best-effort: give the supervisor leaf a dominating `io.weight` so spaced's
    /// own disk I/O preempts a workload saturating the device. The `io`
    /// controller is intentionally *not* a precondition of the split (it is
    /// absent from `REQUIRED_CONTROLLERS`), so this runs after the leaves are
    /// built and is allowed to fail: a guest that can't delegate `io` keeps the
    /// cpu/memory/pids protection. Skips cleanly (no error) when `io` is not
    /// delegated; only an actual write failure is an error.
    fn apply_supervisor_io_weight(base: &Path) -> Result<(), CgroupSetupError> {
        let controllers_path = base.join("cgroup.controllers");
        let available = std::fs::read_to_string(&controllers_path).map_err(|source| {
            CgroupSetupError::Read {
                path: controllers_path,
                source,
            }
        })?;
        if !has_controller(&available, "io") {
            tracing::info!(
                "io controller not delegated; skipping supervisor io.weight \
                 (cpu/memory/pids protection intact)"
            );
            return Ok(());
        }
        // Enable io in the parent's subtree so the leaves expose io.weight, then
        // set the supervisor's weight. Distinct write from the required
        // `+cpu +memory +pids` above so an io failure can't regress that.
        write_cgroup(&base.join("cgroup.subtree_control"), "+io")?;
        write_cgroup(
            &base.join(Leaf::Supervisor.as_str()).join("io.weight"),
            &SUPERVISOR_IO_WEIGHT.to_string(),
        )?;
        tracing::info!(
            weight = SUPERVISOR_IO_WEIGHT,
            "applied supervisor io.weight"
        );
        Ok(())
    }

    fn create_dir(path: &Path) -> Result<(), CgroupSetupError> {
        match std::fs::create_dir(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(source) => Err(CgroupSetupError::CreateDir {
                path: path.to_path_buf(),
                source,
            }),
        }
    }

    fn write_cgroup(path: &Path, value: &str) -> Result<(), CgroupSetupError> {
        std::fs::write(path, value).map_err(|source| CgroupSetupError::Write {
            path: path.to_path_buf(),
            value: value.to_string(),
            source,
        })
    }

    /// `workloads.pids.max`, leaving the supervisor a task reserve. Returns
    /// `None` (leave workloads uncapped) if the kernel limits can't be
    /// read/parsed, or if the ceiling is so small that the reserve would claim
    /// half or more of it — in that degenerate case a cap would do more harm
    /// than the fork-bomb headroom is worth.
    fn workloads_pids_max() -> Option<u64> {
        let pid_max = read_kernel_u64("/proc/sys/kernel/pid_max")?;
        let threads_max = read_kernel_u64("/proc/sys/kernel/threads-max")?;
        workloads_pids_max_from(pid_max, threads_max)
    }

    fn read_kernel_u64(path: &str) -> Option<u64> {
        std::fs::read_to_string(path).ok()?.trim().parse().ok()
    }

    /// `workloads/memory.max` from the guest's `MemTotal`; `None` (leave
    /// workloads uncapped) if it can't be read.
    fn workloads_memory_max() -> Option<u64> {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let total_kib: u64 = meminfo
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))?
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse()
            .ok()?;
        workloads_memory_max_from(total_kib.checked_mul(1024)?)
    }
}

// Task slots held back from workloads and reserved for the supervisor and
// guest system daemons, so spaced can always fork/spawn — and answer host
// health pings — even while a workload fork bomb saturates its own leaf.
// Sized to cover spaced-svc's threads, the processes it spawns, and guest
// system daemons with comfortable headroom.
#[cfg(any(target_os = "linux", test))]
const WORKLOADS_PID_RESERVE: u64 = 256;

// Controllers the split needs delegated to the leaves: cpu (supervisor
// weight), memory (supervisor floor), pids (workloads cap). All three must
// be available before we mutate anything — see `ensure_controllers_available`.
#[cfg(any(target_os = "linux", test))]
const REQUIRED_CONTROLLERS: [&str; 3] = ["cpu", "memory", "pids"];

/// The `REQUIRED_CONTROLLERS` absent from a `cgroup.controllers` body
/// (whitespace-separated controller names). Empty means all are present.
#[cfg(any(target_os = "linux", test))]
fn missing_controllers(available: &str) -> Vec<&'static str> {
    REQUIRED_CONTROLLERS
        .into_iter()
        .filter(|required| !has_controller(available, required))
        .collect()
}

/// Whether `name` appears in a `cgroup.controllers` body (whitespace-separated).
#[cfg(any(target_os = "linux", test))]
fn has_controller(available: &str, name: &str) -> bool {
    available.split_whitespace().any(|have| have == name)
}

/// The effective task ceiling minus the supervisor reserve.
///
/// `kernel.pid_max` alone is the wrong basis: it is the maximum PID
/// *value*, which on a normal guest sits far above the number of tasks the
/// kernel will actually create. The memory-derived `kernel.threads-max` is
/// the limit a fork bomb hits first on a memory-constrained guest, and it
/// is global — not partitioned by the supervisor/workloads split — so a cap
/// based on `pid_max` never binds and reserves nothing for the supervisor.
/// Basing the cap on `min(pid_max, threads-max)` mirrors how systemd
/// derives `TasksMax`, so the cap binds below the real ceiling.
#[cfg(any(target_os = "linux", test))]
fn workloads_pids_max_from(pid_max: u64, threads_max: u64) -> Option<u64> {
    let ceiling = pid_max.min(threads_max);
    if ceiling <= WORKLOADS_PID_RESERVE.saturating_mul(2) {
        return None;
    }
    Some(ceiling - WORKLOADS_PID_RESERVE)
}

// Memory held back from workloads for spaced, the guest's system daemons and
// the kernel: an eighth of the guest's memory, at most this much.
#[cfg(any(target_os = "linux", test))]
const WORKLOADS_MEMORY_RESERVE_MAX_BYTES: u64 = 128 * 1024 * 1024;

/// The guest's memory minus the supervisor reserve, `min(total / 8, 128 MiB)`.
/// `None` (leave workloads uncapped) when the reserve would claim half or
/// more of the guest, where a cap would do more harm than the headroom is
/// worth.
#[cfg(any(target_os = "linux", test))]
fn workloads_memory_max_from(mem_total_bytes: u64) -> Option<u64> {
    let reserve = (mem_total_bytes / 8).min(WORKLOADS_MEMORY_RESERVE_MAX_BYTES);
    if reserve == 0 || reserve.saturating_mul(2) >= mem_total_bytes {
        return None;
    }
    Some(mem_total_bytes - reserve)
}

/// Extract the path of the unified (cgroup v2) hierarchy entry — the line of
/// the form `0::<path>` — from the contents of `/proc/self/cgroup`.
#[cfg(any(target_os = "linux", test))]
fn parse_unified_cgroup(content: &str) -> Option<&str> {
    content.lines().find_map(|line| line.strip_prefix("0::"))
}

#[cfg(test)]
mod tests;
