//! Net-gateway replacement that keeps the gateway's listening sockets.
//!
//! A replacement gateway normally binds fresh TPROXY sockets once its
//! predecessor has exited, and everything the guest sends in between is
//! dropped: no socket holds the port, so the sandbox firewall discards it. A
//! replacement launched with `replace_pid` instead runs on the predecessor's
//! own listening sockets, duplicated before it is stopped, so that traffic
//! waits in their queues for the replacement to read.

use std::os::fd::OwnedFd;
use std::time::Duration;

use pplx_space_node_runtime_core::launch;
use tonic::Status;

/// Bounded waits for the replaced gateway to exit after SIGTERM, then after
/// SIGKILL. They match sandbox-manager's own in-place cycle.
pub(super) const REPLACED_GATEWAY_TERM_GRACE: Duration = Duration::from_secs(3);
pub(super) const REPLACED_GATEWAY_KILL_GRACE: Duration = Duration::from_secs(2);
const REPLACED_GATEWAY_EXIT_POLL: Duration = Duration::from_millis(5);

/// Duplicate the socket each of `sockets` describes from `process`, in order.
/// Fails unless every one is found, so a replacement never runs on a mix of
/// inherited and fresh sockets.
pub(super) fn duplicate_listening_sockets(
    process: &launch::ProcessHandle,
    sockets: &[launch::PreExecSocket],
    ns_path: &str,
) -> Result<Vec<OwnedFd>, String> {
    let namespace = std::fs::File::open(ns_path)
        .map_err(|error| format!("open network namespace {ns_path}: {error}"))?;
    sockets
        .iter()
        .map(|socket| {
            process
                .duplicate_listening_socket(socket, &namespace)
                .map_err(|error| format!("duplicate {} socket: {error}", socket.bind_addr))?
                .ok_or_else(|| {
                    format!(
                        "pid {} holds no listening socket for {}",
                        process.pid(),
                        socket.bind_addr
                    )
                })
        })
        .collect()
}

/// Stop `process`, escalating from SIGTERM to SIGKILL once `term_grace`
/// passes, and return once it has exited.
pub(super) async fn terminate(
    process: &launch::ProcessHandle,
    term_grace: Duration,
    kill_grace: Duration,
) -> Result<(), Status> {
    for (signal, grace) in [(libc::SIGTERM, term_grace), (libc::SIGKILL, kill_grace)] {
        let signalled = process
            .signal(signal)
            .map_err(|error| Status::internal(format!("signal replaced net-gateway: {error}")))?;
        let deadline = tokio::time::Instant::now() + grace;
        loop {
            if !signalled
                || process.has_exited().map_err(|error| {
                    Status::unavailable(format!("observe replaced net-gateway exit: {error}"))
                })?
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(REPLACED_GATEWAY_EXIT_POLL).await;
        }
    }
    Err(Status::unavailable(format!(
        "replaced net-gateway pid {} did not exit after SIGKILL",
        process.pid()
    )))
}

#[cfg(test)]
mod tests;
