use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use pplx_space_common_core::types::{LinuxPid, ProcessSignal};

/// Check if a PID is alive.
pub fn is_pid_alive(pid: u32) -> bool {
    if validate_process_pid(pid).is_err() {
        return false;
    }
    let proc_dir = format!("/proc/{pid}");
    if !std::path::Path::new(&proc_dir).exists() {
        return false;
    }

    !matches!(read_process_state(pid), Some('Z' | 'X' | 'x'))
}

fn read_process_state(pid: u32) -> Option<char> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    process_state_from_status(&status)
}

fn process_state_from_status(status: &str) -> Option<char> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("State:"))
        .and_then(|state| state.trim_start().chars().next())
}

/// Validate a host process PID before using it with kill(2).
///
/// POSIX gives pid 0 and negative values process-group semantics. Node-manager
/// only ever intends to address a single process, so reject values that could
/// signal more broadly than the sandbox process.
pub fn validate_process_pid(pid: u32) -> Result<libc::pid_t, String> {
    let pid = LinuxPid::try_from_u32(pid).map_err(|error| error.to_string())?;
    let raw = pid.get().get();
    if raw == std::process::id() {
        return Err(format!("refusing to signal node-manager pid {raw}"));
    }
    if raw > i32::MAX as u32 {
        return Err(format!("pid {raw} exceeds libc::pid_t range"));
    }
    Ok(raw as libc::pid_t)
}

pub fn validate_raw_process_pid(pid: i32) -> Result<libc::pid_t, String> {
    let pid = u32::try_from(pid)
        .map_err(|error| format!("refusing to signal invalid pid {pid}: {error}"))?;
    validate_process_pid(pid)
}

pub fn signal_process(pid: u32, signal: i32) -> Result<bool, String> {
    let pid = validate_process_pid(pid)?;
    let signal = ProcessSignal::try_from_i32(signal).map_err(|error| error.to_string())?;
    let signal = Signal::try_from(signal.as_i32()).map_err(|error| error.to_string())?;
    Ok(kill(Pid::from_raw(pid), signal).is_ok())
}

pub fn signal_raw_process(pid: i32, signal: i32) -> Result<bool, String> {
    let pid = validate_raw_process_pid(pid)?;
    let signal = ProcessSignal::try_from_i32(signal).map_err(|error| error.to_string())?;
    let signal = Signal::try_from(signal.as_i32()).map_err(|error| error.to_string())?;
    Ok(kill(Pid::from_raw(pid), signal).is_ok())
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessHandleError {
    #[error("invalid pid: {0}")]
    InvalidPid(String),
    #[error("process {pid} not found")]
    NotFound { pid: u32 },
    #[error("pidfd_open({pid}) failed: {source}")]
    Open {
        pid: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid process signal: {0}")]
    InvalidSignal(String),
    #[error("pidfd_send_signal({pid}) failed: {source}")]
    Signal {
        pid: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("signal process {pid} failed: {message}")]
    SignalFallback { pid: u32, message: String },
}

impl ProcessHandleError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }
}

pub struct ProcessHandle {
    pid: u32,
}

pub(super) fn pin_current_process() -> Result<ProcessHandle, ProcessHandleError> {
    Ok(ProcessHandle {
        pid: std::process::id(),
    })
}

impl ProcessHandle {
    pub fn open(pid: u32) -> Result<Self, ProcessHandleError> {
        validate_process_pid(pid).map_err(ProcessHandleError::InvalidPid)?;
        Ok(Self { pid })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Non-Linux hosts cannot prove process lifetime through a pinned pidfd.
    pub fn has_exited(&self) -> std::io::Result<bool> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "pinned process ownership requires Linux pidfds",
        ))
    }

    pub fn signal(&self, signal: i32) -> Result<bool, ProcessHandleError> {
        let signal = ProcessSignal::try_from_i32(signal)
            .map_err(|error| ProcessHandleError::InvalidSignal(error.to_string()))?;
        signal_process(self.pid, signal.as_i32()).map_err(|message| {
            ProcessHandleError::SignalFallback {
                pid: self.pid,
                message,
            }
        })
    }
}

/// Return true only if the current process at `pid` still appears to belong to `sandbox_id`.
///
/// This is intentionally checked immediately before signaling long-lived sandbox
/// processes. PIDs can be reaped and reused between a liveness check and a later
/// kill, so callers must not rely on an earlier ownership check.
pub fn pid_belongs_to_sandbox(pid: u32, sandbox_id: &str) -> bool {
    let cgroup_path = format!("/proc/{pid}/cgroup");
    if std::fs::read_to_string(&cgroup_path)
        .map(|contents| contents.contains(sandbox_id))
        .unwrap_or(false)
    {
        return true;
    }

    let cmdline_path = format!("/proc/{pid}/cmdline");
    std::fs::read(&cmdline_path)
        .map(|contents| {
            contents
                .split(|byte| *byte == 0)
                .any(|part| part == sandbox_id.as_bytes())
        })
        .unwrap_or(false)
}

/// Socket to create in the child process before dropping privileges.
/// Used for TPROXY sockets that require `CAP_NET_ADMIN` (IP_TRANSPARENT).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PreExecSocket {
    /// Address to bind (e.g., "0.0.0.0:15001")
    pub bind_addr: SocketAddr,
    /// TCP (true) or UDP (false)
    pub tcp: bool,
    /// Set IP_TRANSPARENT (required for TPROXY)
    pub transparent: bool,
    /// Set IP_RECVORIGDSTADDR (for recovering original destination)
    pub recvorigdstaddr: bool,
    /// CLI arg name to pass the FD number (e.g., "--tcp-fd")
    pub fd_arg: String,
}

#[derive(Deserialize, Serialize)]
pub struct SidecarLauncherConfig {
    pub ns_path: String,
    pub program: String,
    pub args: Vec<String>,
    pub env: std::collections::HashMap<String, String>,
    pub uid: u32,
    pub gid: u32,
    pub cgroup_path: String,
    pub pre_exec_sockets: Vec<PreExecSocket>,
    pub grant_cap_net_admin: bool,
}

pub struct PreparedSidecarLaunch;

pub struct JailerLaunch<'a> {
    pub sandbox_id: &'a str,
    pub jailer: crate::launch::JailerExecutable<'a>,
    pub firecracker_bin: &'a str,
    pub chroot_base_dir: &'a str,
    pub ns_path: &'a str,
    pub uid: u32,
    pub gid: u32,
    pub fc_config_path: &'a str,
    pub fc_log_path: &'a str,
    pub fc_metrics_path: &'a str,
    pub fc_console_path: &'a str,
    pub cgroup_limits: &'a crate::cgroup::CgroupLimits,
    /// How long the jailer may run before it is killed and the launch fails.
    pub jailer_start_timeout: std::time::Duration,
}

pub fn launch_via_jailer(_config: JailerLaunch<'_>) -> Result<u32, String> {
    Err("launch_via_jailer is only supported on Linux".to_string())
}

pub fn prepare_sidecar_launch(
    _config: SidecarLauncherConfig,
) -> Result<PreparedSidecarLaunch, String> {
    Err("sidecar launcher is only supported on Linux".to_string())
}

impl PreparedSidecarLaunch {
    pub fn exec(self) -> Result<(), String> {
        Err("sidecar launcher is only supported on Linux".to_string())
    }
}

#[cfg(test)]
mod tests;
