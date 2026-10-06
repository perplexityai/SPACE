//! Guest-side cgroup v2 isolation for the spaced supervisor.
//!
//! Inside a Firecracker guest, systemd places spaced-svc and every user
//! workload it spawns in the single `spaced.service` cgroup. A runaway workload
//! (CPU spinner, memory hog, fork bomb) then competes with the supervisor for
//! the same CPU/memory/PID budget and can starve it, which makes host health
//! pings time out and the sandbox fail.
//!
//! We split the delegated `spaced.service` subtree into three leaves:
//! `supervisor/` (spaced itself), `filesystem/` (the filesystem identity
//! workers it re-executes per user) and `workloads/` (commands started through
//! the API). SSH sessions and the template start command run outside spaced's
//! unit, in `space-sessions.slice`, so a spaced restart does not end them; spaced
//! applies that slice's limits from the same partition. We apply the minimal
//! enforcement that protects the supervisor against each starvation vector:
//!   - CPU: `supervisor/cpu.weight` dominates, so spaced preempts a saturated
//!     workload the instant a ping arrives.
//!   - PIDs: the guest's effective task ceiling (`min(pid_max, threads-max)`)
//!     is partitioned: a reserve for the supervisor, system daemons and
//!     recovery, a fixed budget for `filesystem/`, and the rest split between
//!     `workloads/` and the sessions slice. Workloads and sessions forking at
//!     once still leave spaced its tasks.
//!   - Memory: `workloads/memory.max` holds workloads below the guest's
//!     memory by a reserve, so a workload that runs out of memory is killed
//!     inside its own cgroup and the guest as a whole never runs out:
//!     spaced is not an OOM candidate and does not stall reclaiming for a
//!     workload. `supervisor/memory.min` keeps spaced's pages resident under
//!     the pressure that remains, as far as its ancestors' floors allow.
//!     Without swap only file pages are reclaimed, and spaced's text is
//!     locked (`crate::memlock`).
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
//! spaced places itself in `supervisor/` before it builds its runtime or
//! initializes telemetry: memory stays charged to the cgroup it was allocated
//! in, so anything allocated before the move would sit outside the leaf.
//!
//! Setup is best-effort and never takes the daemon down. It is idempotent and
//! always resolved against the delegated root, so a retry after a partial
//! setup resumes it instead of nesting a second split under `supervisor/`.
//! Transient failures are retried a few times; a configuration failure (no
//! delegation, as on a seed older than the split) degrades at once. Enabling controllers
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
    /// The same, for `filesystem/cgroup.procs` (identity workers are Linux-only).
    #[cfg_attr(
        not(target_os = "linux"),
        expect(dead_code, reason = "Linux-only workers")
    )]
    filesystem_procs: Arc<File>,
}

/// The process's split, set up once: [`GuestCgroups::initialize_for_process`]
/// runs it early in `run()`, and every later caller gets the same handle.
static PROCESS_CGROUPS: std::sync::OnceLock<Option<Arc<GuestCgroups>>> = std::sync::OnceLock::new();

impl GuestCgroups {
    /// Place spawned workloads by writing to `workloads_procs`, which a test
    /// can point at a full pipe to hold a child between fork and exec.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn with_workloads_procs(workloads_procs: File) -> Self {
        let filesystem_procs = workloads_procs.try_clone().expect("clone test descriptor");
        Self {
            workloads_procs: Arc::new(workloads_procs),
            filesystem_procs: Arc::new(filesystem_procs),
        }
    }

    /// Set up this process's split once and return it; later calls return
    /// the same result without touching the cgroup tree again.
    pub(crate) fn initialize_for_process() -> Option<Arc<Self>> {
        PROCESS_CGROUPS
            .get_or_init(|| Self::initialize().map(Arc::new))
            .clone()
    }

    /// The process's split, if it was set up.
    #[cfg(target_os = "linux")]
    pub(crate) fn for_process() -> Option<Arc<Self>> {
        PROCESS_CGROUPS.get().cloned().flatten()
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

    /// Register a `pre_exec` hook that moves a filesystem identity worker into
    /// `filesystem/` before it execs, as [`Self::place_spawned_workload`] does
    /// for workloads (before the worker drops its credentials, and failing the
    /// spawn if the move fails).
    #[cfg(target_os = "linux")]
    pub(crate) fn place_filesystem_worker(&self, command: &mut tokio::process::Command) {
        let filesystem_procs = Arc::clone(&self.filesystem_procs);
        // SAFETY: as in `place_spawned_workload`: one async-signal-safe `write`
        // of a fixed buffer in the forked child before `exec`.
        unsafe {
            command.pre_exec(move || place_in_workloads(&filesystem_procs));
        }
    }
}

/// Move the calling (forked) process into the leaf cgroup behind `fd`.
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
        GuestCgroups, TaskPartition, has_controller, missing_controllers, parse_unified_cgroup,
        task_partition_from, workloads_memory_max_from,
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
    // Filesystem workers do the user's file I/O: ahead of workloads, behind
    // the supervisor's control traffic.
    const FILESYSTEM_CPU_WEIGHT: u32 = 1_000;
    // Attempts for a setup that fails with a transient error.
    const SETUP_ATTEMPTS: u32 = 3;
    const SETUP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
    // Where SSH sessions and the template start command run; defined by the
    // seed, limited here.
    const SESSIONS_SLICE: &str = "space-sessions.slice";
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

    impl CgroupSetupError {
        /// A failure retrying cannot fix: where spaced runs, or what the unit
        /// delegates to it.
        fn is_configuration(&self) -> bool {
            matches!(
                self,
                Self::NoUnifiedCgroup | Self::CgroupRoot | Self::ControllersUnavailable { .. }
            )
        }
    }

    /// The two leaf cgroups created under the delegated `spaced.service`
    /// subtree.
    #[derive(Clone, Copy)]
    enum Leaf {
        Supervisor,
        Filesystem,
        Workloads,
    }

    impl Leaf {
        fn as_str(self) -> &'static str {
            match self {
                Leaf::Supervisor => "supervisor",
                Leaf::Filesystem => "filesystem",
                Leaf::Workloads => "workloads",
            }
        }
    }

    pub(super) fn initialize() -> Result<GuestCgroups, CgroupSetupError> {
        let base = delegated_root()?;
        let mut attempt = 1;
        loop {
            match initialize_at(&base) {
                Ok(cgroups) => return Ok(cgroups),
                Err(error) if error.is_configuration() || attempt >= SETUP_ATTEMPTS => {
                    return Err(error);
                }
                Err(error) => {
                    tracing::warn!(%error, attempt, "guest cgroup setup failed; retrying");
                    std::thread::sleep(SETUP_RETRY_DELAY * attempt);
                    attempt += 1;
                }
            }
        }
    }

    /// The delegated root (`spaced.service`), whether or not this process has
    /// already moved itself into `supervisor/` beneath it.
    fn delegated_root() -> Result<PathBuf, CgroupSetupError> {
        let own = own_cgroup_dir()?;
        let moved = own
            .file_name()
            .is_some_and(|name| name == Leaf::Supervisor.as_str())
            && own
                .parent()
                .is_some_and(|parent| parent.join(Leaf::Workloads.as_str()).is_dir());
        if moved {
            return own
                .parent()
                .map(Path::to_path_buf)
                .ok_or(CgroupSetupError::CgroupRoot);
        }
        Ok(own)
    }

    /// Every step is idempotent, so this resumes a setup an earlier attempt
    /// left partly applied.
    fn initialize_at(base: &Path) -> Result<GuestCgroups, CgroupSetupError> {
        // Pre-flight before any mutation. Moving spaced into `supervisor/`
        // (below) is irreversible, and the `subtree_control` write after it
        // fails if these controllers were never delegated — e.g. a new binary
        // on an older seed without `Delegate=yes`. That would strand spaced in
        // `supervisor/` with a half-built split. This read predicts that
        // failure with zero side effects, so we degrade cleanly instead.
        ensure_controllers_available(base)?;

        let supervisor = base.join(Leaf::Supervisor.as_str());
        let filesystem = base.join(Leaf::Filesystem.as_str());
        let workloads = base.join(Leaf::Workloads.as_str());

        create_dir(&supervisor)?;
        create_dir(&filesystem)?;
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
        limits.push((
            Leaf::Filesystem,
            "cpu.weight",
            FILESYSTEM_CPU_WEIGHT.to_string(),
        ));
        let tasks = task_partition();
        if let Some(tasks) = tasks {
            limits.push((Leaf::Filesystem, "pids.max", tasks.filesystem.to_string()));
            limits.push((Leaf::Workloads, "pids.max", tasks.workloads.to_string()));
        }
        let memory_max = workloads_memory_max();
        if let Some(memory_max) = memory_max {
            limits.push((Leaf::Workloads, "memory.max", memory_max.to_string()));
        }
        for (leaf, file, value) in &limits {
            write_cgroup(&base.join(leaf.as_str()).join(file), value)?;
        }

        // Additive, best-effort I/O priority. Unlike cpu/memory/pids this is not
        // a precondition of the split: if `io` was never delegated, or any io
        // write fails, log and leave the (cpu/memory/pids) split intact rather
        // than failing the whole setup.
        if let Err(error) = apply_supervisor_io_weight(base) {
            tracing::warn!(
                %error,
                "supervisor io.weight not applied; cpu/memory/pids protection intact"
            );
        }

        let workloads_procs = open_procs(&workloads)?;
        let filesystem_procs = open_procs(&filesystem)?;
        apply_sessions_limits(tasks, memory_max);

        Ok(GuestCgroups {
            workloads_procs: workloads_procs.into(),
            filesystem_procs: filesystem_procs.into(),
        })
    }

    fn open_procs(leaf: &Path) -> Result<std::fs::File, CgroupSetupError> {
        let path = leaf.join("cgroup.procs");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|source| CgroupSetupError::Open { path, source })
    }

    /// Limit the sessions slice from the same partition, through systemd (it
    /// owns that cgroup, and would reset values written to it directly). On a
    /// background thread, so it does not hold up readiness; best effort, and a
    /// no-op on a seed without the slice.
    fn apply_sessions_limits(tasks: Option<TaskPartition>, memory_max: Option<u64>) {
        if !Path::new("/etc/systemd/system")
            .join(SESSIONS_SLICE)
            .exists()
        {
            return;
        }
        let mut properties = Vec::new();
        if let Some(tasks) = tasks {
            properties.push(format!("TasksMax={}", tasks.sessions));
        }
        if let Some(memory_max) = memory_max {
            properties.push(format!("MemoryMax={memory_max}"));
        }
        if properties.is_empty() {
            return;
        }
        let spawned = std::thread::Builder::new()
            .name("sessions-limits".to_owned())
            .spawn(move || {
                let status = std::process::Command::new("systemctl")
                    .args(["set-property", "--runtime", SESSIONS_SLICE])
                    .args(&properties)
                    .status();
                match status {
                    Ok(status) if status.success() => {
                        tracing::info!(?properties, "applied sessions slice limits");
                    }
                    Ok(status) => {
                        tracing::warn!(%status, ?properties, "sessions slice limits not applied");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "sessions slice limits not applied");
                    }
                }
            });
        if let Err(error) = spawned {
            tracing::warn!(%error, "sessions slice limits not applied");
        }
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

    /// The task partition from the kernel's limits; `None` (leave everything
    /// uncapped) if they can't be read or the ceiling is too small to split.
    fn task_partition() -> Option<TaskPartition> {
        let pid_max = read_kernel_u64("/proc/sys/kernel/pid_max")?;
        let threads_max = read_kernel_u64("/proc/sys/kernel/threads-max")?;
        task_partition_from(pid_max, threads_max)
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

// Task slots held back from every capped group and reserved for the
// supervisor, guest system daemons and recovery (a restart), so spaced can
// always fork/spawn, and answer host health pings, even while workloads and
// sessions saturate their own limits. Sized to cover spaced-svc's threads and
// guest system daemons with comfortable headroom.
#[cfg(any(target_os = "linux", test))]
const SUPERVISION_TASK_RESERVE: u64 = 256;
// Filesystem identity workers: up to 16, each with a main thread, a
// current-thread runtime and up to 16 blocking threads.
#[cfg(any(target_os = "linux", test))]
const FILESYSTEM_TASKS_MAX: u64 = 512;

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

/// Task limits for the groups that run user or worker code.
#[cfg(any(target_os = "linux", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaskPartition {
    pub(crate) filesystem: u64,
    pub(crate) workloads: u64,
    pub(crate) sessions: u64,
}

/// The effective task ceiling, partitioned: the supervision reserve, the
/// filesystem budget, and the rest split three to one between workloads and
/// sessions, so all of them together stay below the ceiling.
///
/// `kernel.pid_max` alone is the wrong basis: it is the maximum PID
/// *value*, which on a normal guest sits far above the number of tasks the
/// kernel will actually create. The memory-derived `kernel.threads-max` is
/// the limit a fork bomb hits first on a memory-constrained guest, and it
/// is global — not partitioned by the supervisor/workloads split — so a cap
/// based on `pid_max` never binds and reserves nothing for the supervisor.
/// Basing the cap on `min(pid_max, threads-max)` mirrors how systemd
/// derives `TasksMax`, so the cap binds below the real ceiling.
///
/// `None` (leave everything uncapped) when the ceiling is too small for the
/// reserve and the filesystem budget to claim less than half of it, where caps
/// would do more harm than the headroom is worth.
#[cfg(any(target_os = "linux", test))]
fn task_partition_from(pid_max: u64, threads_max: u64) -> Option<TaskPartition> {
    let ceiling = pid_max.min(threads_max);
    let held_back = SUPERVISION_TASK_RESERVE + FILESYSTEM_TASKS_MAX;
    if ceiling <= held_back.saturating_mul(2) {
        return None;
    }
    let shared = ceiling - held_back;
    let workloads = shared / 4 * 3;
    Some(TaskPartition {
        filesystem: FILESYSTEM_TASKS_MAX,
        workloads,
        sessions: shared - workloads,
    })
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
