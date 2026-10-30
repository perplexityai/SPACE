use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::os::fd::{AsFd, AsRawFd, IntoRawFd};
use std::time::{Duration, Instant};

use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use nix::sys::resource::{Resource, setrlimit as nix_setrlimit};
use pplx_space_common_core::types::{LinuxPid, ProcessSignal};
use serde::{Deserialize, Serialize};

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
    Ok(kill_single_process(pid, signal))
}

pub fn signal_raw_process(pid: i32, signal: i32) -> Result<bool, String> {
    let pid = validate_raw_process_pid(pid)?;
    let signal = ProcessSignal::try_from_i32(signal).map_err(|error| error.to_string())?;
    Ok(kill_single_process(pid, signal))
}

fn kill_single_process(pid: libc::pid_t, signal: ProcessSignal) -> bool {
    // SAFETY: `pid` and `signal` were validated above and target a single
    // process, avoiding kill(2)'s process-group semantics.
    unsafe { libc::kill(pid, signal.as_i32()) == 0 }
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
}

impl ProcessHandleError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }
}

pub struct ProcessHandle {
    pid: u32,
    pidfd: OwnedFd,
}

impl ProcessHandle {
    pub fn open(pid: u32) -> Result<Self, ProcessHandleError> {
        let raw_pid = validate_process_pid(pid).map_err(ProcessHandleError::InvalidPid)?;
        open_process_handle(pid, raw_pid)
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Observe the pinned process, including an unreaped zombie; never a reused PID.
    pub fn has_exited(&self) -> std::io::Result<bool> {
        use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
        let mut descriptors = [PollFd::new(self.pidfd.as_fd(), PollFlags::POLLIN)];
        // poll(2) is never restarted after a signal handler, and the
        // supervisor handles SIGCHLD for every child it reaps. A zero-timeout
        // poll has no side effects, so retrying it is always safe.
        loop {
            match poll(&mut descriptors, PollTimeout::ZERO) {
                Ok(_) => break,
                Err(nix::errno::Errno::EINTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let [descriptor] = descriptors;
        let events = descriptor
            .revents()
            .ok_or_else(|| std::io::Error::other("unknown process poll events"))?;
        if events.intersects(PollFlags::POLLERR | PollFlags::POLLNVAL) {
            return Err(std::io::Error::other("cannot observe pinned process exit"));
        }
        Ok(events.intersects(PollFlags::POLLIN | PollFlags::POLLHUP))
    }

    pub fn signal(&self, signal: i32) -> Result<bool, ProcessHandleError> {
        let signal = ProcessSignal::try_from_i32(signal)
            .map_err(|error| ProcessHandleError::InvalidSignal(error.to_string()))?;
        signal_process_handle(self, signal)
    }
}

pub(super) fn pin_current_process() -> Result<ProcessHandle, ProcessHandleError> {
    let pid = nix::unistd::getpid().as_raw();
    open_process_handle(pid.cast_unsigned(), pid)
}

fn open_process_handle(
    pid: u32,
    raw_pid: libc::pid_t,
) -> Result<ProcessHandle, ProcessHandleError> {
    let process_pid = rustix::process::Pid::from_raw(raw_pid)
        .ok_or_else(|| ProcessHandleError::InvalidPid("pid must be positive".into()))?;
    match rustix::process::pidfd_open(process_pid, rustix::process::PidfdFlags::empty()) {
        Ok(pidfd) => Ok(ProcessHandle { pid, pidfd }),
        Err(rustix::io::Errno::SRCH) => Err(ProcessHandleError::NotFound { pid }),
        Err(source) => Err(ProcessHandleError::Open {
            pid,
            source: source.into(),
        }),
    }
}

fn signal_process_handle(
    handle: &ProcessHandle,
    signal: ProcessSignal,
) -> Result<bool, ProcessHandleError> {
    match pplx_space_common_core::host::linux::signal_pidfd(&handle.pidfd, signal) {
        Ok(()) => Ok(true),
        Err(source) if source.raw_os_error() == Some(libc::ESRCH) => Ok(false),
        Err(source) => Err(ProcessHandleError::Signal {
            pid: handle.pid,
            source,
        }),
    }
}

const FIRECRACKER_READINESS_POLL: Duration = Duration::from_millis(5);
const FIRECRACKER_LAUNCH_READINESS_POLL: Duration = Duration::from_millis(1);
const FIRECRACKER_READINESS_TIMEOUT: Duration = Duration::from_secs(5);
/// How much of each output stream a timed-out jailer left behind to report.
const JAILER_OUTPUT_TAIL_BYTES: usize = 1024;

fn read_firecracker_pidfile(pid_file: &str) -> Option<String> {
    std::fs::read_to_string(pid_file)
        .ok()
        .filter(|contents| !contents.trim().is_empty())
}

fn wait_for_firecracker_pid(sandbox_id: &str, pid_file: &str) -> Result<String, String> {
    let started = Instant::now();
    let mut attempts = 0u32;

    loop {
        attempts = attempts.saturating_add(1);

        if let Some(pid_str) = read_firecracker_pidfile(pid_file) {
            tracing::debug!(
                sandbox_id,
                strategy = "pidfile",
                poll_interval_ms = FIRECRACKER_LAUNCH_READINESS_POLL.as_millis() as u64,
                attempts,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "firecracker pidfile observed"
            );
            return Ok(pid_str);
        }

        if started.elapsed() >= FIRECRACKER_READINESS_TIMEOUT {
            return Err(format!(
                "jailer pidfile {pid_file} not found after {}ms",
                FIRECRACKER_READINESS_TIMEOUT.as_millis()
            ));
        }

        std::thread::sleep(FIRECRACKER_LAUNCH_READINESS_POLL);
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

pub struct PreparedSidecarLaunch {
    program: std::ffi::CString,
    args: Vec<std::ffi::CString>,
    env: Vec<(std::ffi::CString, std::ffi::CString)>,
}

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
    pub jailer_start_timeout: Duration,
}

fn clear_cloexec(fd: impl AsFd) -> Result<(), String> {
    let flags = FdFlag::from_bits_retain(
        fcntl(fd.as_fd(), FcntlArg::F_GETFD)
            .map_err(|error| format!("fcntl F_GETFD failed: {error}"))?,
    );
    fcntl(
        fd.as_fd(),
        FcntlArg::F_SETFD(flags.difference(FdFlag::FD_CLOEXEC)),
    )
    .map_err(|error| format!("fcntl F_SETFD failed: {error}"))?;
    Ok(())
}

/// Prepare the current process to become a sandbox sidecar.
///
/// `pre_exec_sockets`: Sockets to create after entering the namespace but before
/// dropping privileges. Their FD numbers are appended to args as `--{fd_arg} <fd>`.
/// This allows creating TPROXY sockets (which need CAP_NET_ADMIN) while the child
/// is still root, then dropping to an unprivileged UID before exec.
///
/// This function makes irreversible process-wide changes and must run only in
/// the dedicated, single-threaded launcher process.
pub fn prepare_sidecar_launch(
    config: SidecarLauncherConfig,
) -> Result<PreparedSidecarLaunch, String> {
    let SidecarLauncherConfig {
        ns_path,
        program,
        args,
        env,
        uid,
        gid,
        cgroup_path,
        pre_exec_sockets,
        grant_cap_net_admin,
    } = config;

    use nix::unistd::{Gid, Uid};
    use std::ffi::CString;

    let ns_file = std::fs::File::open(&ns_path)
        .map_err(|e| format!("failed to open namespace {ns_path}: {e}"))?;
    let c_program =
        CString::new(program.as_str()).map_err(|e| format!("invalid program path: {e}"))?;
    let mut full_args: Vec<String> = std::iter::once(program.clone()).chain(args).collect();
    let c_env: Vec<(CString, CString)> = env
        .into_iter()
        .map(|(key, value)| {
            if key.is_empty() || key.contains('=') {
                return Err("invalid env key: must be nonempty and contain no '='".into());
            }
            Ok((
                CString::new(key).map_err(|e| format!("invalid env key: {e}"))?,
                CString::new(value).map_err(|e| format!("invalid env value: {e}"))?,
            ))
        })
        .collect::<Result<_, String>>()?;

    nix::sched::setns(&ns_file, nix::sched::CloneFlags::CLONE_NEWNET)
        .map_err(|e| format!("setns failed: {e}"))?;
    drop(ns_file);

    crate::cgroup::join_cgroup(&cgroup_path)?;

    for sock_cfg in &pre_exec_sockets {
        let fd = create_pre_exec_socket(sock_cfg)
            .map_err(|e| format!("sidecar socket creation failed: {e}"))?;
        full_args.push(sock_cfg.fd_arg.clone());
        full_args.push(fd.to_string());
    }

    let c_args = full_args
        .into_iter()
        .map(CString::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("sidecar argument contains NUL byte: {error}"))?;

    if grant_cap_net_admin {
        nix::sys::prctl::set_keepcaps(true)
            .map_err(|error| format!("PR_SET_KEEPCAPS failed: {error}"))?;
    }

    nix::unistd::setgroups(&[]).map_err(|e| format!("setgroups([]) failed: {e}"))?;
    nix::unistd::setgid(Gid::from_raw(gid)).map_err(|e| format!("setgid failed: {e}"))?;
    nix::unistd::setuid(Uid::from_raw(uid)).map_err(|e| format!("setuid failed: {e}"))?;

    if grant_cap_net_admin {
        grant_ambient_cap_net_admin()?;
    }
    apply_child_hardening()?;

    Ok(PreparedSidecarLaunch {
        program: c_program,
        args: c_args,
        env: c_env,
    })
}

impl PreparedSidecarLaunch {
    /// Replace the launcher with the target sidecar. Returns only when exec fails.
    /// `Command::exec` gives the sidecar the default SIGPIPE disposition rather
    /// than inheriting the Rust launcher's ignored SIGPIPE. The signal mask stays
    /// unchanged, as with execvp.
    pub fn exec(self) -> Result<(), String> {
        use std::os::unix::{ffi::OsStrExt, process::CommandExt};
        let mut command =
            std::process::Command::new(std::ffi::OsStr::from_bytes(self.program.as_bytes()));
        command.args(
            self.args
                .iter()
                .skip(1)
                .map(|arg| std::ffi::OsStr::from_bytes(arg.as_bytes())),
        );
        command.envs(self.env.iter().map(|(key, value)| {
            (
                std::ffi::OsStr::from_bytes(key.as_bytes()),
                std::ffi::OsStr::from_bytes(value.as_bytes()),
            )
        }));
        Err(format!("exec sidecar failed: {}", command.exec()))
    }
}

fn apply_child_hardening() -> Result<(), String> {
    set_child_rlimit("RLIMIT_CORE", Resource::RLIMIT_CORE, 0, 0)?;
    set_child_rlimit(
        "RLIMIT_NOFILE",
        Resource::RLIMIT_NOFILE,
        pplx_space_common_core::host::linux::SANDBOX_PROCESS_NOFILE_LIMIT,
        pplx_space_common_core::host::linux::SANDBOX_PROCESS_NOFILE_LIMIT,
    )?;
    set_child_rlimit("RLIMIT_NPROC", Resource::RLIMIT_NPROC, 512, 512)?;

    nix::sys::prctl::set_no_new_privs()
        .map_err(|error| format!("prctl PR_SET_NO_NEW_PRIVS failed: {error}"))?;

    Ok(())
}

fn set_child_rlimit(
    resource_name: &'static str,
    resource: Resource,
    soft: libc::rlim_t,
    hard: libc::rlim_t,
) -> Result<(), String> {
    nix_setrlimit(resource, soft, hard)
        .map_err(|error| format!("setrlimit({resource_name}) failed: {error}"))
}

/// Grant CAP_NET_ADMIN + CAP_NET_BIND_SERVICE as ambient capabilities.
/// Called after setuid — PR_SET_KEEPCAPS preserved capabilities in the permitted set.
/// CAP_NET_ADMIN: required for IP_TRANSPARENT setsockopt
/// CAP_NET_BIND_SERVICE: required for binding to privileged ports (e.g., 53 for DNS response sockets)
fn grant_ambient_cap_net_admin() -> Result<(), String> {
    use rustix::thread::CapabilitySet;
    let cap_bits = CapabilitySet::NET_ADMIN | CapabilitySet::NET_BIND_SERVICE;
    let mut sets =
        rustix::thread::capabilities(None).map_err(|error| format!("capget failed: {error}"))?;
    sets.effective |= cap_bits;
    sets.permitted |= cap_bits;
    sets.inheritable |= cap_bits;
    rustix::thread::set_capabilities(None, sets)
        .map_err(|error| format!("capset failed: {error}"))?;
    for cap in [CapabilitySet::NET_ADMIN, CapabilitySet::NET_BIND_SERVICE] {
        rustix::thread::configure_capability_in_ambient_set(cap, true)
            .map_err(|error| format!("raise ambient capability {cap:?}: {error}"))?;
    }
    Ok(())
}

/// Create a socket with the specified options. Returns the raw FD.
/// Called in the child process after setns() but before setuid().
fn create_pre_exec_socket(cfg: &PreExecSocket) -> Result<i32, String> {
    if cfg.tcp {
        let s = std::net::TcpListener::bind(cfg.bind_addr)
            .map_err(|e| format!("tcp bind {}: {e}", cfg.bind_addr))?;
        s.set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking: {e}"))?;
        configure_pre_exec_socket(&s, cfg)?;
        Ok(s.into_raw_fd())
    } else {
        let s = std::net::UdpSocket::bind(cfg.bind_addr)
            .map_err(|e| format!("udp bind {}: {e}", cfg.bind_addr))?;
        s.set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking: {e}"))?;
        configure_pre_exec_socket(&s, cfg)?;
        Ok(s.into_raw_fd())
    }
}

fn configure_pre_exec_socket(
    socket: &(impl AsFd + AsRawFd),
    cfg: &PreExecSocket,
) -> Result<(), String> {
    if cfg.transparent {
        pplx_space_net_common::orig_dst::set_ip_transparent(socket)
            .map_err(|e| format!("IP_TRANSPARENT: {e}"))?;
    }
    if cfg.recvorigdstaddr {
        pplx_space_net_common::orig_dst::set_recvorigdstaddr(socket)
            .map_err(|e| format!("IP_RECVORIGDSTADDR: {e}"))?;
    }
    clear_cloexec(socket)
}

/// Launch Firecracker via jailer for production-grade isolation.
///
/// Jailer provides: chroot (pivot_root), mount namespace, PID namespace,
/// device node creation (/dev/kvm, /dev/net/tun, /dev/urandom), cgroup setup,
/// binary copy isolation, and privilege dropping.
///
/// The caller must have already:
/// - Created the network namespace (jailer only joins it)
/// - Placed kernel, rootfs, and config files in the chroot base dir
///   (jailer creates `{chroot_base}/{id}/root/` and the exec binary goes there)
///
/// Jailer runs as root and drops to the specified UID/GID.
/// Returns the jailer's child PID (which is the Firecracker PID).
/// The supervisor owns cleanup after a partial launch, outside this blocking
/// call, so waiting for cgroup drain does not occupy a blocking-work slot.
pub fn launch_via_jailer(config: JailerLaunch<'_>) -> Result<u32, String> {
    let JailerLaunch {
        sandbox_id,
        jailer,
        firecracker_bin,
        chroot_base_dir,
        ns_path,
        uid,
        gid,
        fc_config_path,
        fc_log_path,
        fc_metrics_path,
        fc_console_path,
        cgroup_limits,
        jailer_start_timeout,
    } = config;

    let pid_file = format!("{chroot_base_dir}/firecracker/{sandbox_id}/root/firecracker.pid");
    let jailer_args = jailer_args(JailerArgs {
        sandbox_id,
        firecracker_bin,
        chroot_base_dir,
        ns_path,
        uid,
        gid,
        fc_config_path,
        fc_log_path,
        fc_metrics_path,
        cgroup_limits,
        daemonize: true,
    });
    run_jailer_command(
        sandbox_id,
        jailer,
        fc_console_path,
        &jailer_args,
        jailer_start_timeout,
    )?;

    // Let jailer daemonize Firecracker, then read the pidfile ourselves. Jailer
    // intentionally moves Firecracker under its own delegated cgroup/chroot
    // path, and the pidfile is the concrete process identity that sbx-mgr
    // persists as fc.pid.
    let pid_str_result = wait_for_firecracker_pid(sandbox_id, &pid_file);
    let pid_result = pid_str_result.and_then(|pid_str| {
        let pid = pid_str
            .trim()
            .parse()
            .map_err(|e| format!("invalid Firecracker PID {pid_str:?}: {e}"))?;
        validate_process_pid(pid).map_err(|e| format!("invalid Firecracker PID {pid}: {e}"))?;
        Ok(pid)
    });

    let pid = pid_result?;
    wait_for_firecracker_api_socket(sandbox_id, chroot_base_dir)?;

    Ok(pid)
}

fn firecracker_api_socket_path(chroot_base_dir: &str, sandbox_id: &str) -> String {
    format!("{chroot_base_dir}/firecracker/{sandbox_id}/root/firecracker.sock")
}

fn wait_for_firecracker_api_socket(sandbox_id: &str, chroot_base_dir: &str) -> Result<(), String> {
    let socket_path = firecracker_api_socket_path(chroot_base_dir, sandbox_id);
    let started = Instant::now();
    let mut attempts = 0u32;

    loop {
        attempts = attempts.saturating_add(1);
        match std::os::unix::net::UnixStream::connect(&socket_path) {
            Ok(_) => {
                tracing::debug!(
                    sandbox_id,
                    socket_path,
                    attempts,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "firecracker API socket ready"
                );
                return Ok(());
            }
            Err(error) if started.elapsed() >= FIRECRACKER_READINESS_TIMEOUT => {
                return Err(format!(
                    "firecracker API socket {socket_path} not ready after {}ms: {error}",
                    FIRECRACKER_READINESS_TIMEOUT.as_millis()
                ));
            }
            Err(_) => {
                std::thread::sleep(FIRECRACKER_LAUNCH_READINESS_POLL);
            }
        }
    }
}

struct JailerArgs<'a> {
    sandbox_id: &'a str,
    firecracker_bin: &'a str,
    chroot_base_dir: &'a str,
    ns_path: &'a str,
    uid: u32,
    gid: u32,
    fc_config_path: &'a str,
    fc_log_path: &'a str,
    fc_metrics_path: &'a str,
    cgroup_limits: &'a crate::cgroup::CgroupLimits,
    daemonize: bool,
}

fn jailer_args(config: JailerArgs<'_>) -> Vec<String> {
    let JailerArgs {
        sandbox_id,
        firecracker_bin,
        chroot_base_dir,
        ns_path,
        uid,
        gid,
        fc_config_path,
        fc_log_path,
        fc_metrics_path,
        cgroup_limits,
        daemonize,
    } = config;

    let mut args = vec![
        "--id".to_string(),
        sandbox_id.to_string(),
        "--exec-file".to_string(),
        firecracker_bin.to_string(),
        "--uid".to_string(),
        uid.to_string(),
        "--gid".to_string(),
        gid.to_string(),
        "--chroot-base-dir".to_string(),
        chroot_base_dir.to_string(),
        "--netns".to_string(),
        ns_path.to_string(),
        "--cgroup-version".to_string(),
        "2".to_string(),
        "--parent-cgroup".to_string(),
        "space".to_string(),
        "--cgroup".to_string(),
        format!("cpu.max={}", cgroup_limits.cpu_max),
        "--cgroup".to_string(),
        format!("memory.max={}", cgroup_limits.memory_max),
        "--cgroup".to_string(),
        format!("pids.max={}", cgroup_limits.pids_max),
        "--resource-limit".to_string(),
        format!(
            "no-file={}",
            pplx_space_common_core::host::linux::SANDBOX_PROCESS_NOFILE_LIMIT
        ),
        "--new-pid-ns".to_string(),
    ];
    if daemonize {
        args.push("--daemonize".to_string());
    }
    args.extend([
        "--".to_string(),
        "--api-sock".to_string(),
        "firecracker.sock".to_string(),
    ]);
    if !fc_config_path.is_empty() {
        args.extend(["--config-file".to_string(), fc_config_path.to_string()]);
    }
    if !fc_log_path.is_empty() {
        // The level prefix lets sandbox-manager forward ERROR lines without
        // forwarding the WARN and INFO lines around them.
        args.extend([
            "--log-path".to_string(),
            fc_log_path.to_string(),
            "--level".to_string(),
            "Info".to_string(),
            "--show-level".to_string(),
        ]);
    }
    if !fc_metrics_path.is_empty() {
        args.extend(["--metrics-path".to_string(), fc_metrics_path.to_string()]);
    }
    args
}

const JAILER_PATH: &str = "/usr/sbin:/usr/bin:/sbin:/bin";

fn direct_jailer_command(
    jailer: crate::launch::JailerExecutable<'_>,
    jailer_args: &[String],
) -> std::io::Result<std::process::Command> {
    use crate::launch::JailerExecutable;
    let program = match jailer {
        JailerExecutable::LegacyPath(path) => path.to_owned(),
        JailerExecutable::Sealed(file) => format!("/proc/self/fd/{}", file.file().as_raw_fd()),
    };
    let mut cmd = std::process::Command::new(&program);
    cmd.env_clear().env("PATH", JAILER_PATH);
    cmd.args(jailer_args);
    if let JailerExecutable::Sealed(file) = jailer {
        bind_jailer_descriptor(&mut cmd, file.file().try_clone()?, &program, jailer_args)?;
    }
    Ok(cmd)
}

fn bind_jailer_descriptor(
    cmd: &mut std::process::Command,
    file: std::fs::File,
    program: &str,
    jailer_args: &[String],
) -> std::io::Result<()> {
    use std::os::unix::process::CommandExt;
    let arguments = std::iter::once(program)
        .chain(jailer_args.iter().map(String::as_str))
        .map(std::ffi::CString::new)
        .collect::<Result<Vec<_>, _>>()?;
    let environment = [std::ffi::CString::new(format!("PATH={JAILER_PATH}"))?];
    // Integer pointer representations keep the closure Send + Sync. The
    // captured CStrings own stable allocations until execveat consumes them.
    let argv: Vec<usize> = arguments
        .iter()
        .map(|arg| arg.as_ptr() as usize)
        .chain([0])
        .collect();
    let envp: Vec<usize> = environment
        .iter()
        .map(|arg| arg.as_ptr() as usize)
        .chain([0])
        .collect();
    let before_exec = move || {
        let _keep_alive = (&arguments, &environment);
        // SAFETY: argument pointers refer to captured, stable CStrings and
        // null-terminated arrays. The captured file pins the measured inode.
        unsafe {
            libc::syscall(
                libc::SYS_execveat,
                file.as_raw_fd(),
                c"".as_ptr(),
                argv.as_ptr().cast::<*const libc::c_char>(),
                envp.as_ptr().cast::<*const libc::c_char>(),
                libc::AT_EMPTY_PATH,
            );
        }
        Err(std::io::Error::last_os_error())
    };
    // SAFETY: the child callback performs only execveat and errno inspection;
    // all allocation and pointer-array construction happened in the parent.
    unsafe {
        cmd.pre_exec(before_exec);
    }

    Ok(())
}

/// Wait for a spawned jailer, killing it at `timeout`. `Ok(None)` means it was
/// still running at the limit and has been killed and reaped.
fn wait_for_jailer(
    child: &mut std::process::Child,
    label: &str,
    timeout: Duration,
    started: Instant,
) -> Result<Option<std::process::ExitStatus>, String> {
    let deadline = started + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(Some(status)),
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(FIRECRACKER_READINESS_POLL);
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Ok(None);
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{label}: {error}"));
            }
        }
    }
}

/// The last [`JAILER_OUTPUT_TAIL_BYTES`] a killed jailer wrote to `pipe`.
///
/// This reads only what is already buffered and never waits for end of file:
/// a process the jailer forked may still hold the write end open.
fn killed_jailer_output_tail(pipe: Option<impl std::io::Read + AsFd>) -> String {
    use nix::fcntl::OFlag;
    use std::io::Read as _;
    // A default pipe holds this much; the limit also stops a surviving writer
    // from keeping the read going.
    const READ_LIMIT: u64 = 64 * 1024;
    let Some(pipe) = pipe else {
        return String::new();
    };
    let nonblocking = fcntl(pipe.as_fd(), FcntlArg::F_GETFL).and_then(|flags| {
        fcntl(
            pipe.as_fd(),
            FcntlArg::F_SETFL(OFlag::from_bits_retain(flags) | OFlag::O_NONBLOCK),
        )
    });
    if let Err(error) = nonblocking {
        return format!("<unreadable: {error}>");
    }
    let mut output = Vec::new();
    // An empty pipe ends the read with WouldBlock; what was read before it
    // stays in `output`.
    let _ = pipe.take(READ_LIMIT).read_to_end(&mut output);
    let tail = output.split_off(output.len().saturating_sub(JAILER_OUTPUT_TAIL_BYTES));
    String::from_utf8_lossy(&tail).trim().to_owned()
}

fn command_output_text(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn run_jailer_command(
    sandbox_id: &str,
    jailer: crate::launch::JailerExecutable<'_>,
    console_path: &str,
    jailer_args: &[String],
    timeout: Duration,
) -> Result<(), String> {
    use pplx_space_metrics::runtime_supervisor::{JailerOutcome, jailer_finished};
    let label = format!("jailer for {sandbox_id}");
    let mut cmd = direct_jailer_command(jailer, jailer_args)
        .map_err(|error| format!("prepare jailer execution: {error}"))?;
    cmd.stdin(std::process::Stdio::null());

    if console_path.is_empty() {
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
    } else {
        let stdout = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(console_path)
            .map_err(|error| format!("open Firecracker console {console_path}: {error}"))?;
        let stderr = stdout
            .try_clone()
            .map_err(|error| format!("clone Firecracker console {console_path}: {error}"))?;
        cmd.stdout(std::process::Stdio::from(stdout));
        cmd.stderr(std::process::Stdio::from(stderr));
    }

    let started = Instant::now();
    let mut child = cmd.spawn().map_err(|error| format!("{label}: {error}"))?;
    let waited = wait_for_jailer(&mut child, &label, timeout, started);
    let elapsed = started.elapsed();
    let status = match waited {
        Ok(Some(status)) => {
            let outcome = if status.success() {
                JailerOutcome::Succeeded
            } else {
                JailerOutcome::Failed
            };
            jailer_finished(outcome, elapsed);
            status
        }
        Ok(None) => {
            jailer_finished(JailerOutcome::TimedOut, elapsed);
            return Err(jailer_timed_out(
                sandbox_id,
                &label,
                child,
                console_path,
                elapsed,
                timeout,
            ));
        }
        Err(error) => {
            jailer_finished(JailerOutcome::Failed, elapsed);
            return Err(error);
        }
    };

    if console_path.is_empty() {
        let output = child
            .wait_with_output()
            .map_err(|error| format!("collect output after {label} finished failed: {error}"))?;
        if status.success() {
            return Ok(());
        }
        return Err(format!(
            "{label} exited with {}: {}",
            status.code().unwrap_or(-1),
            command_output_text(&output).trim()
        ));
    }
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "{label} exited with {}; see console log {console_path}",
            status.code().unwrap_or(-1)
        ))
    }
}

/// Report a jailer killed at its start timeout with what it had written, so
/// the error says how far it got.
fn jailer_timed_out(
    sandbox_id: &str,
    label: &str,
    mut child: std::process::Child,
    console_path: &str,
    elapsed: Duration,
    timeout: Duration,
) -> String {
    let elapsed_ms = elapsed.as_millis() as u64;
    let timeout_ms = timeout.as_millis() as u64;
    let output = if console_path.is_empty() {
        let stdout = killed_jailer_output_tail(child.stdout.take());
        let stderr = killed_jailer_output_tail(child.stderr.take());
        tracing::warn!(
            sandbox_id,
            elapsed_ms,
            timeout_ms,
            stdout,
            stderr,
            "killed jailer at its start timeout"
        );
        format!("stdout tail {stdout:?}, stderr tail {stderr:?}")
    } else {
        tracing::warn!(
            sandbox_id,
            elapsed_ms,
            timeout_ms,
            console_path,
            "killed jailer at its start timeout"
        );
        format!("output in console log {console_path}")
    };
    format!("{label} timed out after {elapsed_ms}ms (limit {timeout_ms}ms); {output}")
}

#[cfg(test)]
mod tests;
