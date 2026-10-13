//! Process management for the local runtime: booting the guest via
//! `systemd-nspawn`, launching `spaced-host` in UDS-bridge mode, and
//! best-effort freeze/thaw + termination.
//!
//! Spawned children are reaped by a detached task so a container whose PID 1
//! does not reap does not accumulate zombies; lifecycle control
//! (freeze/kill) goes through the recorded PID via `libc`.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;

use crate::paths;

/// Binary that boots the guest userland as a container.
const NSPAWN_BIN: &str = "systemd-nspawn";

/// Rewrite the `spaced-host` argv that sandbox-manager hands to `Launch` so the
/// guest is reached over per-sandbox UDS bridge sockets instead of a Firecracker
/// vsock. We drop `--vsock-uds <path>` (which would force vsock mode) and append
/// `--bridge-guest-uds` / `--bridge-host-uds`, preserving every other flag
/// (`--socket`, `--sandbox-id`, `--host-control-socket`, `--boot-type`).
///
/// This is the seam that keeps sandbox-manager untouched: it still believes it
/// launched a vsock-backed spaced-host, but the local node-manager is the exec
/// boundary and substitutes the UDS wiring.
pub fn rewrite_spaced_host_args(
    args: &[String],
    bridge_guest: &Path,
    bridge_host: &Path,
) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len());
    let mut skip_next = false;
    for arg in args {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "--vsock-uds" {
            skip_next = true;
            continue;
        }
        out.push(arg.clone());
    }
    out.push("--bridge-guest-uds".to_string());
    out.push(bridge_guest.to_string_lossy().into_owned());
    out.push("--bridge-host-uds".to_string());
    out.push(bridge_host.to_string_lossy().into_owned());
    out
}

/// Spawn a long-running child, returning its PID. The child handle is moved into
/// a detached task that awaits it, so it is reaped when it exits.
fn spawn_reaped(mut command: Command, what: &'static str) -> std::io::Result<u32> {
    let mut child = command.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other(format!("{what} exited before reporting a pid")))?;
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) => tracing::info!(%pid, what, ?status, "child exited"),
            Err(error) => tracing::warn!(%pid, what, %error, "failed to await child"),
        }
    });
    Ok(pid)
}

/// Boot the guest as a `systemd-nspawn` container from a rootfs directory tree.
///
/// We boot `--directory` (not `--image`): `--image` requires udev + the host
/// `/run` hierarchy to attach a loop device, which kind worker pods don't
/// provide. `--register=no` avoids a `systemd-machined` dependency (the
/// container's PID 1 is not systemd), and the bridge dir is
/// bind-mounted to the well-known guest mount point so the spaced bridge sockets
/// are shared across the container boundary.
pub fn spawn_nspawn(
    machine: &str,
    rootdir: &Path,
    bridge_dir: &Path,
    log_path: &Path,
) -> std::io::Result<u32> {
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let log = std::fs::File::create(log_path)?;
    let log_err = log.try_clone()?;

    let mut command = Command::new(NSPAWN_BIN);
    command
        .arg("--quiet")
        .arg("--register=no")
        .arg("--keep-unit")
        .arg("--resolv-conf=off")
        .arg("--link-journal=no")
        .arg("--boot")
        .arg(format!("--machine={machine}"))
        .arg(format!("--directory={}", rootdir.display()))
        .arg(format!(
            "--bind={}:{}",
            bridge_dir.display(),
            paths::GUEST_BRIDGE_MOUNT
        ))
        .arg("--capability=all")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    command.kill_on_drop(false);
    spawn_reaped(command, "nspawn")
}

/// Spawn a long-lived no-op placeholder, returning its PID. Used for the
/// net-gateway launch the local runtime doesn't implement (synthetic
/// networking is a non-goal): sandbox-manager launches `space-netgtw-svc` via
/// `Launch` and monitors the returned PID, so we give it a live PID that does
/// nothing rather than failing the launch.
pub fn spawn_placeholder(log_path: &Path) -> std::io::Result<u32> {
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let log = std::fs::File::create(log_path)?;
    let mut command = Command::new("sleep");
    command
        .arg("infinity")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::null());
    command.kill_on_drop(false);
    spawn_reaped(command, "net-gateway-placeholder")
}

/// Launch `spaced-host` with the (already rewritten) UDS-bridge argv and env.
pub fn spawn_spaced_host(
    bin: &str,
    args: &[String],
    env: &HashMap<String, String>,
    log_path: &Path,
) -> std::io::Result<u32> {
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let log = std::fs::File::create(log_path)?;
    let log_err = log.try_clone()?;

    let mut command = Command::new(bin);
    command
        .args(args)
        .env_clear()
        .envs(inherit_base_env())
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    command.kill_on_drop(false);
    spawn_reaped(command, "spaced-host")
}

/// Minimal base environment for spawned services (PATH + RUST_LOG), on top of
/// which the per-launch env is layered.
fn inherit_base_env() -> HashMap<String, String> {
    const PASSTHROUGH: &[&str] = &[
        "PATH",
        "HOME",
        "RUST_LOG",
        "SPACE_INTERNAL_TOKEN",
        "SPACE_INTERNAL_CONTROL_TOKEN",
    ];
    let mut env = HashMap::new();
    for key in PASSTHROUGH {
        if let Ok(value) = std::env::var(key) {
            env.insert((*key).to_string(), value);
        }
    }
    if !env.contains_key("PATH") {
        env.insert(
            "PATH".to_string(),
            "/space/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
        );
    }
    env
}

/// Pause/resume the guest. Intentionally a **no-op** on the local runtime.
///
/// We launch nspawn with `--keep-unit` + `--register=no`, so the supervisor PID
/// lives in the node-manager pod's own cgroup rather than a dedicated guest scope.
/// Writing `cgroup.freeze` on that cgroup would freeze the node-manager handling
/// the RPC (and anything else in its pod), hanging the worker instead of
/// pausing just the guest. The design permits no-op pause/resume here:
/// sandbox-manager tracks the logical paused state, and pause/resume state
/// survival holds trivially because the guest keeps running. (A real
/// Firecracker pause freezes vCPUs via the VMM, which has no analogue here.)
pub fn set_frozen(nspawn_pid: u32, frozen: bool) {
    tracing::info!(
        %nspawn_pid,
        frozen,
        "pause/resume is a no-op on the local runtime (no guest-only freezer cgroup)"
    );
}

/// Send a signal to a PID. Returns whether the syscall reported success.
fn signal(pid: u32, sig: libc::c_int) -> bool {
    let pid = nix::unistd::Pid::from_raw(pid.cast_signed());
    if sig == 0 {
        return nix::sys::signal::kill(pid, None).is_ok();
    }
    nix::sys::signal::Signal::try_from(sig)
        .ok()
        .is_some_and(|signal| nix::sys::signal::kill(pid, signal).is_ok())
}

/// Whether a PID is currently alive (signal 0 probe).
pub fn is_alive(pid: u32) -> bool {
    signal(pid, 0)
}

/// Terminate a guest/spaced-host PID: SIGTERM, brief grace, then SIGKILL.
pub async fn terminate(pid: u32) {
    if !signal(pid, libc::SIGTERM) {
        return;
    }
    for _ in 0..50 {
        if !is_alive(pid) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    signal(pid, libc::SIGKILL);
}
