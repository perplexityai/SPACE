use std::fs::File;
use std::sync::Arc;

/// Already-open placement handles. Construction never changes host cgroups.
#[derive(Debug)]
pub struct ExecutionCgroups {
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

impl ExecutionCgroups {
    pub fn new(workloads_procs: File, filesystem_procs: File) -> Self {
        Self {
            workloads_procs: Arc::new(workloads_procs),
            filesystem_procs: Arc::new(filesystem_procs),
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

#[cfg(all(test, unix))]
mod tests;
