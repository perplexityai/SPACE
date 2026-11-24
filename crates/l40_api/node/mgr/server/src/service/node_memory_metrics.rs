//! Periodic memory gauges for the sandbox cgroup parent.

use std::path::{Path, PathBuf};
use std::time::Duration;

use pplx_space_node_runtime_core::cgroup_memory::read_cgroup_memory_state;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::metrics::node_memory;

const EMIT_INTERVAL: Duration = Duration::from_secs(15);

/// Emits the gauges of the cgroup at `cgroup_dir` every [`EMIT_INTERVAL`].
pub fn start_node_memory_metrics(
    cgroup_dir: PathBuf,
    shutdown_token: CancellationToken,
    task_tracker: &TaskTracker,
) {
    task_tracker.spawn(async move {
        let mut interval = tokio::time::interval(EMIT_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => return,
                _ = interval.tick() => {}
            }
            let dir = cgroup_dir.clone();
            if let Err(error) = tokio::task::spawn_blocking(move || emit_tick(&dir)).await {
                tracing::warn!(%error, "node memory metrics tick panicked");
            }
        }
    });
}

fn emit_tick(cgroup_dir: &Path) {
    match read_cgroup_memory_state(cgroup_dir) {
        Ok(state) => {
            node_memory::space_cgroup(&state);
            node_memory::space_cgroup_read_success(true);
        }
        Err(error) => {
            node_memory::space_cgroup_read_success(false);
            tracing::warn!(
                cgroup = %cgroup_dir.display(),
                %error,
                "cannot read sandbox cgroup parent memory state"
            );
        }
    }
}

#[cfg(test)]
mod tests;
