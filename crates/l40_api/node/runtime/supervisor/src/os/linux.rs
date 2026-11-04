use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::process::reap_tracked_children;

pub(crate) fn spawn_tracked_child_reaper(
    fork_children: Arc<Mutex<HashSet<u32>>>,
    shutdown: CancellationToken,
    tasks: &tokio_util::task::TaskTracker,
) -> std::io::Result<()> {
    let mut sigchld = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())?;
    tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                signal = sigchld.recv() => {
                    if signal.is_none() {
                        break;
                    }
                    reap_tracked_children(&fork_children);
                }
            }
        }
        // Accepted launches have drained. Reap exits racing the final signal;
        // surviving VMs/sidecars must not delay a supervisor restart.
        reap_tracked_children(&fork_children);
    });
    Ok(())
}

pub(crate) fn process_matches_binary(pid: u32, binary: &str) -> bool {
    pplx_space_node_runtime_core::launch::process_matches_program(pid, binary).unwrap_or(false)
}
