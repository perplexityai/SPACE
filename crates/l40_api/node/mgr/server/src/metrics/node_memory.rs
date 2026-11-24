//! node-manager's memory gauges for the sandbox cgroup parent
//! (`/sys/fs/cgroup/space`), which holds every sandbox's VM and sidecar
//! cgroups and carries the node's aggregate sandbox `memory.max`.

use pplx_space_metrics_emit::{gauge, tag};
use pplx_space_node_runtime_core::cgroup_memory::CgroupMemoryState;

/// One reading of the parent cgroup. Byte gauges the kernel does not report
/// are skipped, as is `max_bytes` when the parent is unlimited. `events` is
/// the cumulative `memory.events` count per `event`, a monotonic gauge.
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
    let events = &state.events;
    for (event, count) in [
        ("low", events.low),
        ("high", events.high),
        ("max", events.max),
        ("oom", events.oom),
        ("oom_kill", events.oom_kill),
        ("oom_group_kill", events.oom_group_kill),
    ] {
        gauge(
            "node_memory.space_cgroup.events",
            count as f64,
            &[tag("event", event)],
        );
    }
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

/// Whether the last parent cgroup read succeeded, so a stale byte gauge is
/// distinguishable from a node that stopped reporting.
pub fn space_cgroup_read_success(success: bool) {
    gauge(
        "node_memory.space_cgroup.read_success",
        f64::from(success),
        &[],
    );
}
