use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::Pid;
use pplx_space_common_core::host::linux::paths::{NET_GATEWAY_SVC_BIN, SPACED_HOST_SVC_BIN};
use pplx_space_common_core::types::SandboxId;
use pplx_space_node_runtime_core::launch;
use tonic::Status;

pub(crate) fn parse_sandbox_id(sandbox_id: &str) -> Result<SandboxId, Status> {
    SandboxId::from_str(sandbox_id).map_err(|error| Status::invalid_argument(error.to_string()))
}

pub(crate) fn validate_sidecar_program(program: &str) -> Result<(), Status> {
    if matches!(program, NET_GATEWAY_SVC_BIN | SPACED_HOST_SVC_BIN) {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "runtime supervisor cannot launch program {program}"
        )))
    }
}

pub(crate) fn sidecar_program_filename(program: &str) -> Result<String, Status> {
    let filename = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Status::invalid_argument(format!("invalid sidecar program {program}")))?;
    Ok(filename.to_string())
}

pub(crate) fn sidecar_pid_filename(program: &str) -> Result<String, Status> {
    let filename = sidecar_program_filename(program)?;
    Ok(format!("{filename}.pid"))
}

pub(crate) fn track_child(fork_children: &Arc<Mutex<HashSet<u32>>>, pid: u32) {
    let mut children = lock_child_tracker(fork_children);
    children.insert(pid);
    loop {
        match waitpid(Pid::from_raw(pid as i32), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => return,
            Err(nix::errno::Errno::EINTR) => {}
            Ok(_) | Err(_) => {
                children.remove(&pid);
                return;
            }
        }
    }
}

/// Best-effort signaling when pidfd creation fails. The nonblocking wait first
/// proves the numeric PID is still our unreaped direct child, so raw signaling
/// cannot target a reused PID. Only after signaling do we publish it to the
/// tracked-child reaper; tracking first could reap an early exit and make the
/// numeric PID reusable before cleanup.
pub(crate) fn kill_direct_child_without_pidfd(fork_children: &Arc<Mutex<HashSet<u32>>>, pid: u32) {
    loop {
        match waitpid(Pid::from_raw(pid as i32), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => {
                if let Err(error) = launch::signal_process(pid, libc::SIGKILL) {
                    tracing::warn!(pid, %error, "failed to kill untracked sidecar child");
                }
                track_child(fork_children, pid);
                return;
            }
            Ok(_) | Err(nix::errno::Errno::ECHILD) => return,
            Err(nix::errno::Errno::EINTR) => {}
            Err(error) => {
                tracing::warn!(pid, %error, "failed to inspect untracked sidecar child");
                // We cannot safely signal after losing proof that the numeric
                // PID is still our unreaped child, but publishing it still
                // gives the SIGCHLD reaper a chance to prevent a zombie.
                track_child(fork_children, pid);
                return;
            }
        }
    }
}

fn lock_child_tracker(fork_children: &Arc<Mutex<HashSet<u32>>>) -> MutexGuard<'_, HashSet<u32>> {
    match fork_children.lock() {
        Ok(children) => children,
        Err(poisoned) => {
            tracing::warn!("recovering poisoned runtime child tracker mutex");
            fork_children.clear_poison();
            poisoned.into_inner()
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn reap_tracked_children(fork_children: &Arc<Mutex<HashSet<u32>>>) {
    let mut children = lock_child_tracker(fork_children);
    children.retain(
        |&pid| match waitpid(Pid::from_raw(pid as i32), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) | Err(nix::errno::Errno::EINTR) => true,
            Ok(_) | Err(_) => false,
        },
    );
}

pub(crate) fn signal_sandbox_pids(
    sandbox_id: &SandboxId,
    pids: &[(&'static str, u32)],
    signal: i32,
    actions: &mut Vec<String>,
) {
    let sandbox_id_str = sandbox_id.to_string();
    for (name, pid) in pids {
        let handle = match launch::ProcessHandle::open(*pid) {
            Ok(handle) => handle,
            Err(error) if error.is_not_found() => continue,
            Err(error) => {
                actions.push(format!("Failed to pin {name} PID {pid}: {error}"));
                continue;
            }
        };
        match handle.signal_if_owned(&sandbox_id_str, signal) {
            Ok(true) => actions.push(format!("Sent signal {signal} to {name} PID {pid}")),
            Ok(false) => {}
            Err(error) => actions.push(format!("Failed to signal {name} PID {pid}: {error}")),
        }
    }
}
