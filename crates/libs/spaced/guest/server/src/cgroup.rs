//! Guest-side cgroup v2 isolation for the spaced supervisor.
//!
//! Inside a Firecracker guest a runaway workload (CPU spinner, memory hog,
//! fork bomb) would otherwise compete with the supervisor for the same
//! CPU/memory/PID budget and can starve it, which makes host health pings
//! time out and the sandbox fail.
//!
//! The guest is split in two at the root. `system.slice` holds the guest's
//! infrastructure: `spaced.service`, which spaced divides into `supervisor/`
//! (spaced itself) and `filesystem/` (the filesystem identity workers it
//! re-executes per user), and the system daemons. `space.slice` holds all the
//! work users run: `space-workloads.service/workloads/` (commands started
//! through the API) and `space-sessions.slice` (SSH sessions and the template
//! start command). Both units are defined by the seed. Workloads end with
//! spaced: their unit is `PartOf=spaced.service`, so every stop or restart of
//! spaced, including the automatic one after a crash, restarts it and kills
//! what it holds. Sessions survive a spaced restart.
//!
//! spaced applies the minimal enforcement that protects the supervisor
//! against each starvation vector:
//!   - CPU: `supervisor/cpu.weight` dominates its unit, and `space.slice`
//!     runs behind `system.slice` (the seed's `CPUWeight=`), so spaced
//!     preempts saturated user work the instant a ping arrives.
//!   - PIDs: the guest's effective task ceiling (`min(pid_max, threads-max)`)
//!     is partitioned: a reserve for the supervisor, system daemons and
//!     recovery, a fixed budget for `filesystem/`, and the rest for
//!     `space.slice`. User work forking without end still leaves spaced its
//!     tasks. Workloads and sessions are each capped an eighth below the
//!     slice, so one kind filling it still lets the other start a process:
//!     an SSH login to clean up after an API fork bomb, or the reverse.
//!   - Memory: `space.slice` is capped below the guest's memory by a reserve,
//!     so workloads and sessions together cannot run the guest out of memory.
//!     One that does runs out inside the slice and is killed there: spaced,
//!     outside the cap, is not an OOM candidate and does no reclaim on its
//!     behalf. Either kind of user work can use the whole envelope.
//!     `supervisor/memory.min` keeps spaced's pages resident under the
//!     pressure that remains, as far as its ancestors' floors allow. Without
//!     swap only file pages are reclaimed, and spaced's text is locked
//!     (`crate::memlock`).
//!   - I/O: `supervisor/io.weight` dominates, so spaced's own disk I/O (reading
//!     `/proc`, fs metadata, writing logs) is serviced ahead of a workload
//!     saturating the device, rather than queuing behind it. This is the one
//!     vector that is **additive and best-effort**: `io` is not in
//!     `REQUIRED_CONTROLLERS`, so a guest that can't delegate it keeps the full
//!     cpu/memory/pids protection — we just skip the io step. (io.weight only
//!     bites under contention and only where the kernel's iocost model is
//!     active for the device.)
//!
//! A new binary can meet an older seed's units (OCI assembly installs the
//! current guest binaries over the selected seed's): without the workloads
//! unit, setup builds the previous layout instead, workloads in a capped
//! `workloads/` leaf inside `spaced.service`, rather than giving up isolation.
//!
//! `Delegate=yes` on both units makes systemd enable the cpu/memory/pids
//! (and, where available, io) controllers for their subtrees and stop
//! managing them, which is what lets us write `cgroup.subtree_control` and
//! create the leaves. The limits on `space.slice` are set through systemd,
//! which owns that cgroup, from the guest's size at startup.
//!
//! spaced places itself in `supervisor/` before it builds its runtime or
//! initializes telemetry: memory stays charged to the cgroup it was allocated
//! in, so anything allocated before the move would sit outside the leaf.
//!
//! Setup is best-effort and never takes the daemon down. It is idempotent and
//! always resolved against the delegated root, so a retry after a partial
//! setup resumes it instead of nesting a second split under `supervisor/`.
//! Transient failures are retried a few times; a configuration failure (no
//! delegation, or a seed without the workloads unit) degrades at once. Enabling controllers
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

/// What the process's setup did, kept until [`GuestCgroups::report_process_setup`]
/// logs it: setup runs before telemetry, when a log line would be dropped.
static PROCESS_SETUP_REPORT: std::sync::Mutex<Option<SetupReport>> = std::sync::Mutex::new(None);

/// The outcome of cgroup setup, recorded rather than logged (see
/// [`PROCESS_SETUP_REPORT`]).
#[derive(Debug, Default)]
pub(crate) struct SetupReport {
    /// The layout built, or why none was.
    outcome: Option<Result<&'static str, String>>,
    /// Transient failures a later attempt recovered from.
    retried: Vec<String>,
    /// Best-effort steps that did not apply; the split itself stands.
    warnings: Vec<String>,
    /// Limits already written to systemd-owned cgroups, to persist through
    /// systemd once telemetry is up (it would reset direct writes on reload).
    #[cfg_attr(
        not(target_os = "linux"),
        expect(dead_code, reason = "Linux-only persistence")
    )]
    unit_properties: Vec<(&'static str, Vec<String>)>,
}

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
            .get_or_init(|| {
                let (cgroups, report) = Self::initialize();
                *PROCESS_SETUP_REPORT
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
                cgroups.map(Arc::new)
            })
            .clone()
    }

    /// Log what the process's setup did, now that telemetry records it, and
    /// persist the user slice limits through systemd. Once; later calls do
    /// nothing.
    pub(crate) fn report_process_setup() {
        let report = PROCESS_SETUP_REPORT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(report) = report else {
            return;
        };
        for error in &report.retried {
            tracing::warn!(%error, "guest cgroup setup attempt failed; retried");
        }
        match &report.outcome {
            Some(Ok(layout)) => tracing::info!(layout, "guest workload cgroups initialized"),
            Some(Err(error)) => tracing::warn!(
                %error,
                "failed to initialize guest workload cgroups; \
                 supervisor and workloads share a cgroup"
            ),
            None => {}
        }
        for warning in &report.warnings {
            tracing::warn!(%warning, "guest cgroup setup step not applied; the split stands");
        }
        #[cfg(target_os = "linux")]
        linux::persist_unit_properties(report.unit_properties);
    }

    /// The process's split, if it was set up.
    #[cfg(target_os = "linux")]
    pub(crate) fn for_process() -> Option<Arc<Self>> {
        PROCESS_CGROUPS.get().cloned().flatten()
    }

    /// Set up the split and return a handle used to place spawned workloads,
    /// with a report of what setup did. The handle is `None` — degrading to the
    /// prior shared-cgroup behavior — on any non-Linux target or setup
    /// failure. Never panics.
    pub(crate) fn initialize() -> (Option<Self>, SetupReport) {
        #[cfg(target_os = "linux")]
        {
            let mut report = SetupReport::default();
            match linux::initialize(&mut report) {
                Ok((cgroups, layout)) => {
                    report.outcome = Some(Ok(layout));
                    (Some(cgroups), report)
                }
                Err(error) => {
                    report.outcome = Some(Err(error.to_string()));
                    (None, report)
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            (None, SetupReport::default())
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
        GuestCgroups, SetupReport, TaskPartition, has_controller, missing_controllers,
        parse_unified_cgroup, task_partition_from, user_memory_max_from,
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
    // All user work, and the unit holding API workloads within it; defined by
    // the seed. spaced limits the slice and builds the unit's leaf.
    const USER_SLICE: &str = "space.slice";
    const WORKLOADS_UNIT: &str = "space-workloads.service";
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

    /// The leaf cgroups spaced creates: two under `spaced.service`, one under
    /// the workloads unit.
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

    pub(super) fn initialize(
        report: &mut SetupReport,
    ) -> Result<(GuestCgroups, &'static str), CgroupSetupError> {
        let base = delegated_root()?;
        let mut attempt = 1;
        loop {
            match initialize_at(&base, report) {
                Ok(done) => return Ok(done),
                Err(error) if error.is_configuration() || attempt >= SETUP_ATTEMPTS => {
                    return Err(error);
                }
                Err(error) => {
                    report.retried.push(format!("attempt {attempt}: {error}"));
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
                .is_some_and(|parent| parent.join(Leaf::Filesystem.as_str()).is_dir());
        if moved {
            return own
                .parent()
                .map(Path::to_path_buf)
                .ok_or(CgroupSetupError::CgroupRoot);
        }
        Ok(own)
    }

    /// Every step is idempotent, so this resumes a setup an earlier attempt
    /// left partly applied. Returns the layout built: the envelope, or on a
    /// seed without the workloads unit (a new binary installed onto an older
    /// seed's units, as OCI assembly does) workloads inside spaced's own unit,
    /// with the same caps, as before the envelope.
    fn initialize_at(
        base: &Path,
        report: &mut SetupReport,
    ) -> Result<(GuestCgroups, &'static str), CgroupSetupError> {
        // Pre-flight before any mutation. Moving spaced into `supervisor/`
        // (below) is irreversible, and the `subtree_control` write after it
        // fails if these controllers were never delegated — e.g. a new binary
        // on an older seed without `Delegate=yes`. That would strand spaced in
        // `supervisor/` with a half-built split. This read predicts that
        // failure with zero side effects, so we degrade cleanly instead.
        ensure_controllers_available(base)?;
        let workloads_unit = Path::new(CGROUP_ROOT).join(USER_SLICE).join(WORKLOADS_UNIT);
        let envelope = workloads_unit.is_dir();
        if envelope {
            ensure_controllers_available(&workloads_unit)?;
        }

        let supervisor = base.join(Leaf::Supervisor.as_str());
        let filesystem = base.join(Leaf::Filesystem.as_str());
        let workloads = if envelope {
            workloads_unit.join(Leaf::Workloads.as_str())
        } else {
            base.join(Leaf::Workloads.as_str())
        };

        create_dir(&supervisor)?;
        create_dir(&filesystem)?;
        create_dir(&workloads)?;

        // Move spaced-svc into supervisor/ first (writing "0" moves the writing
        // process), so spaced.service is empty of processes and we may enable
        // controllers on its subtree (the "no internal processes" rule). The
        // workloads unit's own process, which keeps its cgroup alive, sits in
        // a subgroup of its own (`DelegateSubgroup=`) for the same reason.
        write_cgroup(&supervisor.join("cgroup.procs"), "0")?;

        // Delegate=yes makes these controllers available; enabling them here is
        // what materializes cpu.weight/memory.min/pids.max in the leaves, and
        // per-leaf accounting for workloads.
        write_cgroup(&base.join("cgroup.subtree_control"), "+cpu +memory +pids")?;
        if envelope {
            write_cgroup(
                &workloads_unit.join("cgroup.subtree_control"),
                "+cpu +memory +pids",
            )?;
        }

        // Per-leaf resource limits — one row per knob, applied now that the
        // controllers are delegated. Order-independent, so kept declarative.
        let mut limits = vec![
            (&supervisor, "cpu.weight", SUPERVISOR_CPU_WEIGHT.to_string()),
            (
                &supervisor,
                "memory.min",
                SUPERVISOR_MEMORY_MIN_BYTES.to_string(),
            ),
            (&filesystem, "cpu.weight", FILESYSTEM_CPU_WEIGHT.to_string()),
        ];
        // Workloads are capped here, synchronously, whatever the layout: in the
        // envelope by its share (the slice's own limits follow below), in a
        // legacy layout by the whole user share, as before the envelope.
        let tasks = task_partition();
        let memory_max = memory_max();
        if let Some(tasks) = tasks {
            limits.push((&filesystem, "pids.max", tasks.filesystem.to_string()));
            let workloads_tasks = if envelope {
                tasks.user_member
            } else {
                tasks.user
            };
            limits.push((&workloads, "pids.max", workloads_tasks.to_string()));
        }
        if let Some(memory_max) = memory_max {
            limits.push((&workloads, "memory.max", memory_max.to_string()));
        }
        for (leaf, file, value) in &limits {
            write_cgroup(&leaf.join(file), value)?;
        }

        // Additive, best-effort I/O priority. Unlike cpu/memory/pids this is not
        // a precondition of the split: if `io` was never delegated, or any io
        // write fails, log and leave the (cpu/memory/pids) split intact rather
        // than failing the whole setup.
        if let Err(error) = apply_supervisor_io_weight(base) {
            report
                .warnings
                .push(format!("supervisor io.weight: {error}"));
        }

        let workloads_procs = open_procs(&workloads)?;
        let filesystem_procs = open_procs(&filesystem)?;
        if envelope {
            report.unit_properties = cap_user_slices(tasks, memory_max, &mut report.warnings);
        }

        Ok((
            GuestCgroups {
                workloads_procs: workloads_procs.into(),
                filesystem_procs: filesystem_procs.into(),
            },
            if envelope {
                "envelope"
            } else {
                "legacy: seed without space-workloads.service, workloads in spaced.service"
            },
        ))
    }

    fn open_procs(leaf: &Path) -> Result<std::fs::File, CgroupSetupError> {
        let path = leaf.join("cgroup.procs");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|source| CgroupSetupError::Open { path, source })
    }

    /// Cap `space.slice`, and the sessions slice within it, from the same
    /// partition: written to their cgroups now, so the limits hold from the
    /// moment setup returns, and returned as unit properties for systemd to
    /// persist (it owns those cgroups and would reset direct writes on a
    /// reload). A direct write that fails is a warning: the workloads leaf is
    /// capped regardless, and systemd still applies the property.
    fn cap_user_slices(
        tasks: Option<TaskPartition>,
        memory_max: Option<u64>,
        warnings: &mut Vec<String>,
    ) -> Vec<(&'static str, Vec<String>)> {
        let slice = Path::new(CGROUP_ROOT).join(USER_SLICE);
        let sessions = slice.join(SESSIONS_SLICE);
        let mut direct = Vec::new();
        let mut user = Vec::new();
        let mut units = Vec::new();
        if let Some(tasks) = tasks {
            direct.push((slice.join("pids.max"), tasks.user.to_string()));
            direct.push((sessions.join("pids.max"), tasks.user_member.to_string()));
            user.push(format!("TasksMax={}", tasks.user));
            units.push((
                SESSIONS_SLICE,
                vec![format!("TasksMax={}", tasks.user_member)],
            ));
        }
        if let Some(memory_max) = memory_max {
            direct.push((slice.join("memory.max"), memory_max.to_string()));
            user.push(format!("MemoryMax={memory_max}"));
        }
        if !user.is_empty() {
            units.insert(0, (USER_SLICE, user));
        }
        for (path, value) in direct {
            if let Err(error) = write_cgroup(&path, &value) {
                warnings.push(error.to_string());
            }
        }
        units
    }

    // Attempts to persist each unit's limits, a second apart and then two.
    const UNIT_PROPERTY_ATTEMPTS: u32 = 3;

    /// Persist the user slice limits through systemd, on a background thread
    /// so readiness does not wait for it, retrying a failed `systemctl` a few
    /// times. Called once telemetry is up, so its outcome is recorded.
    pub(super) fn persist_unit_properties(units: Vec<(&'static str, Vec<String>)>) {
        if units.is_empty() {
            return;
        }
        let spawned = std::thread::Builder::new()
            .name("user-slice-limits".to_owned())
            .spawn(move || {
                for (unit, properties) in units {
                    let mut attempt = 1;
                    loop {
                        let outcome = std::process::Command::new("systemctl")
                            .args(["set-property", "--runtime", unit])
                            .args(&properties)
                            .status()
                            .map_err(|error| error.to_string())
                            .and_then(|status| {
                                status.success().then_some(()).ok_or(status.to_string())
                            });
                        match outcome {
                            Ok(()) => {
                                tracing::info!(unit, ?properties, "applied user slice limits");
                                break;
                            }
                            Err(error) if attempt < UNIT_PROPERTY_ATTEMPTS => {
                                tracing::warn!(unit, %error, attempt, "user slice limits not applied; retrying");
                                std::thread::sleep(std::time::Duration::from_secs(u64::from(attempt)));
                                attempt += 1;
                            }
                            Err(error) => {
                                tracing::warn!(
                                    unit,
                                    %error,
                                    ?properties,
                                    "user slice limits not persisted; the direct caps hold until systemd reloads"
                                );
                                break;
                            }
                        }
                    }
                }
            });
        if let Err(error) = spawned {
            tracing::warn!(%error, "user slice limits not persisted");
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

    /// `space.slice`'s `MemoryMax` from the guest's `MemTotal`; `None` (leave
    /// user work uncapped) if it can't be read.
    fn memory_max() -> Option<u64> {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let total_kib: u64 = meminfo
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))?
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse()
            .ok()?;
        user_memory_max_from(total_kib.checked_mul(1024)?)
    }
}

// Task slots held back from every capped group and reserved for the
// supervisor, guest system daemons and recovery (a restart), so spaced can
// always fork/spawn, and answer host health pings, even while user work
// saturates its limit. Sized to cover spaced-svc's threads and
// guest system daemons with comfortable headroom.
#[cfg(any(target_os = "linux", test))]
const SUPERVISION_TASK_RESERVE: u64 = 256;
// Filesystem identity workers: up to 16, each with a main thread, a
// current-thread runtime and up to 16 blocking threads.
#[cfg(any(target_os = "linux", test))]
const FILESYSTEM_TASKS_MAX: u64 = 512;

// Controllers the split needs delegated to the leaves: cpu (supervisor
// weight), memory (supervisor floor), pids (filesystem cap). All three must
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
    /// All user work: `space.slice`.
    pub(crate) user: u64,
    /// Each of workloads and sessions: an eighth below `user`, so either
    /// filling its cap leaves the other room to start a process.
    pub(crate) user_member: u64,
}

/// The effective task ceiling, partitioned: the supervision reserve, the
/// filesystem budget, and the rest for user work, so all of them together
/// stay below the ceiling.
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
    let user = ceiling - held_back;
    Some(TaskPartition {
        filesystem: FILESYSTEM_TASKS_MAX,
        user,
        user_member: user - user / 8,
    })
}

// Memory held back from user work for spaced, the guest's system daemons and
// the kernel: an eighth of the guest's memory, at most this much.
#[cfg(any(target_os = "linux", test))]
const USER_MEMORY_RESERVE_MAX_BYTES: u64 = 128 * 1024 * 1024;

/// The guest's memory minus the supervisor reserve, `min(total / 8, 128 MiB)`.
/// `None` (leave user work uncapped) when the reserve would claim half or
/// more of the guest, where a cap would do more harm than the headroom is
/// worth.
#[cfg(any(target_os = "linux", test))]
fn user_memory_max_from(mem_total_bytes: u64) -> Option<u64> {
    let reserve = (mem_total_bytes / 8).min(USER_MEMORY_RESERVE_MAX_BYTES);
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
