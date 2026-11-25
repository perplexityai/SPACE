//! node-manager's memory gauges for the sandbox cgroup parent
//! (`/sys/fs/cgroup/space`), which holds every sandbox's VM and sidecar
//! cgroups and carries the node's aggregate sandbox `memory.max`.

use pplx_space_metrics_emit::{counter, gauge, tag};
use pplx_space_node_runtime_core::cgroup_memory::CgroupMemoryState;

/// One reading of the parent cgroup. Byte gauges the kernel does not report
/// are skipped, as is `max_bytes` when the parent is unlimited.
pub fn space_cgroup(state: &CgroupMemoryState) {
    let bytes = |name: &str, value: Option<u64>| {
        if let Some(value) = value {
            gauge(name, value as f64, &[]);
        }
    };
    bytes("node_memory.space_cgroup.max_bytes", state.max_bytes);
    bytes(
        "node_memory.space_cgroup.current_bytes",
        Some(state.current_bytes),
    );
    let stat = &state.stat;
    bytes("node_memory.space_cgroup.anon_bytes", stat.anon);
    bytes("node_memory.space_cgroup.file_bytes", stat.file);
    bytes("node_memory.space_cgroup.kernel_bytes", stat.kernel);
    bytes("node_memory.space_cgroup.pagetables_bytes", stat.pagetables);
    bytes(
        "node_memory.space_cgroup.sec_pagetables_bytes",
        stat.sec_pagetables,
    );
    bytes("node_memory.space_cgroup.shmem_bytes", stat.shmem);
    if let Some(pressure) = state.pressure {
        gauge(
            "node_memory.space_cgroup.psi_some_avg10",
            pressure.some_avg10,
            &[],
        );
        gauge(
            "node_memory.space_cgroup.psi_full_avg10",
            pressure.full_avg10,
            &[],
        );
    }
}

/// New `memory.events` occurrences in the parent since the previous reading,
/// counted per `event` (`low`, `high`, `max`, `oom`, `oom_kill`,
/// `oom_group_kill`).
pub fn space_cgroup_events(event: &'static str, count: u64) {
    counter(
        "node_memory.space_cgroup.events",
        count,
        &[tag("event", event)],
    );
}

/// Whether the last parent cgroup read succeeded, so a stale byte gauge is
/// distinguishable from a node that stopped reporting.
pub fn space_cgroup_read_success(success: bool) {
    gauge(
        "node_memory.space_cgroup.read_success",
        f64::from(success),
        &[],
    );
}
