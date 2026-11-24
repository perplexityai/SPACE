//! Periodic memory gauges for the sandbox cgroup parent.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pplx_space_node_runtime_core::cgroup_memory::{CgroupMemoryEvents, read_cgroup_memory_state};
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
        let sampler = Arc::new(Mutex::new(Sampler::new(cgroup_dir)));
        let mut interval = tokio::time::interval(EMIT_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => return,
                _ = interval.tick() => {}
            }
            let sampler = sampler.clone();
            let tick = move || {
                sampler
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .tick();
            };
            if let Err(error) = tokio::task::spawn_blocking(tick).await {
                tracing::warn!(%error, "node memory metrics tick panicked");
            }
        }
    });
}

/// Reads the parent cgroup once per tick. It keeps the previous
/// `memory.events` reading to count new events, and logs a read failure
/// once and its recovery once; `read_success` carries the ongoing state.
struct Sampler {
    cgroup_dir: PathBuf,
    previous_events: Option<CgroupMemoryEvents>,
    failing: bool,
}

impl Sampler {
    fn new(cgroup_dir: PathBuf) -> Self {
        Self {
            cgroup_dir,
            previous_events: None,
            failing: false,
        }
    }

    fn tick(&mut self) {
        let state = match read_cgroup_memory_state(&self.cgroup_dir) {
            Ok(state) => state,
            Err(error) => {
                node_memory::space_cgroup_read_success(false);
                if !std::mem::replace(&mut self.failing, true) {
                    tracing::warn!(
                        cgroup = %self.cgroup_dir.display(),
                        %error,
                        "cannot read sandbox cgroup parent memory state"
                    );
                }
                return;
            }
        };
        if std::mem::replace(&mut self.failing, false) {
            tracing::info!(
                cgroup = %self.cgroup_dir.display(),
                "sandbox cgroup parent memory state readable again"
            );
        }
        node_memory::space_cgroup(&state);
        if let Some(previous) = self.previous_events.replace(state.events) {
            emit_new_events(&previous, &state.events);
        }
        node_memory::space_cgroup_read_success(true);
    }
}

/// Counts each event's growth since `previous`. A counter that went
/// backwards belongs to a recreated cgroup; its new value is the next
/// baseline, not a count.
fn emit_new_events(previous: &CgroupMemoryEvents, current: &CgroupMemoryEvents) {
    let pairs = |events: &CgroupMemoryEvents| {
        [
            ("low", events.low),
            ("high", events.high),
            ("max", events.max),
            ("oom", events.oom),
            ("oom_kill", events.oom_kill),
            ("oom_group_kill", events.oom_group_kill),
        ]
    };
    for ((event, before), (_, after)) in pairs(previous).into_iter().zip(pairs(current)) {
        if after > before {
            node_memory::space_cgroup_events(event, after - before);
        }
    }
}

#[cfg(test)]
mod tests;
