//! Periodic host memory observations of resident VM jailer cgroups.

use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use jiff::Timestamp;
use pplx_space_node_runtime_core::cgroup_memory::{CgroupMemoryUsage, read_cgroup_memory_usage};
use pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path_in;
use pplx_space_sandbox_model::SandboxId;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::metrics::sandbox::cgroup_memory::{self, AgeBucket, CgroupLimitBucket};

use super::SidecarInfo;
use super::diagnostics::{HostReads, keyed_u64, read_small, sandbox_cgroup_parent};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
const SWEEP_DEADLINE: Duration = Duration::from_secs(5);
// Limit contention for the shared host-read pool.
const SAMPLE_CONCURRENCY: usize = 2;

fn read_sample(cgroup: &Path) -> Option<CgroupMemoryUsage> {
    // A cgroup can retain charges after the VM exits. Don't sample it
    // unless the jailer cgroup still has a process.
    let events = read_small(&cgroup.join("cgroup.events")).ok()?;
    if keyed_u64(&events, "populated") != Some(1) {
        return None;
    }
    read_cgroup_memory_usage(cgroup).ok()
}

async fn sweep<T: Send, F: Future<Output = ()> + Send>(
    mut targets: Vec<T>,
    cursor: &mut usize,
    visit: impl FnMut(T) -> F + Send,
) -> bool {
    let len = targets.len();
    if len == 0 {
        return false;
    }
    let start = *cursor % len;
    targets.rotate_left(start);
    let completed = stream::iter(targets)
        .map(visit)
        .buffer_unordered(SAMPLE_CONCURRENCY)
        .take_until(tokio::time::sleep(SWEEP_DEADLINE))
        .count()
        .await;
    if completed < len {
        // Don't keep sampling only the same prefix when sweeps hit the deadline.
        // Advance by completions, not started reads that were dropped on timeout.
        *cursor = (start + completed.max(1)) % len;
        return true;
    }
    false
}

pub(super) async fn run(
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    reads: HostReads,
    shutdown: CancellationToken,
) {
    let parent = sandbox_cgroup_parent().await;
    let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut cursor = 0;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {}
        }
        let mut targets = {
            let sidecars = sidecars.lock().await;
            sidecars
                .values()
                .filter_map(|info| {
                    Some((info.sandbox_id.parse::<SandboxId>().ok()?, info.created_at))
                })
                .collect::<Vec<_>>()
        };
        targets.sort_unstable_by_key(|(sandbox_id, _)| *sandbox_id);
        let now = Timestamp::now().as_second();
        let sweep = sweep(targets, &mut cursor, |(sandbox_id, created_at)| {
            let cgroup = jailer_cgroup_path_in(&parent, &sandbox_id.to_string());
            let reads = &reads;
            async move {
                if let Some(sample) = reads.blocking(move || read_sample(&cgroup)).await.flatten() {
                    let age = AgeBucket::from_age_secs(
                        created_at.and_then(|created_at| now.checked_sub(created_at.get())),
                    );
                    cgroup_memory::emit(
                        sample.current_bytes,
                        sample.anon_bytes,
                        CgroupLimitBucket::from_memory_max(sample.limit),
                        age,
                    );
                }
            }
        });
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            truncated = sweep => {
                if truncated {
                    cgroup_memory::sweep_truncated();
                    tracing::warn!("VM cgroup memory sweep exceeded its deadline");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
