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

/// The parent's [`CgroupMemoryState::headroom_bytes`]. Skipped when the
/// parent is unlimited or its unreclaimable memory is unknown.
pub fn space_cgroup_headroom(state: &CgroupMemoryState) {
    if let Some(headroom) = state.headroom_bytes() {
        gauge(
            "node_memory.space_cgroup.headroom_bytes",
            headroom as f64,
            &[],
        );
    }
}

/// The growth memory admission booked for the node's existing leases in its
/// latest decision against `limit` (`host` or `parent_cgroup`): each VM's remaining room toward
/// its booking level, as that check counted it.
pub fn admission_booked_growth(limit: &'static str, booked_growth_mib: u64) {
    gauge(
        "node_memory.admission.booked_growth_bytes",
        booked_growth_mib.saturating_mul(1024 * 1024) as f64,
        &[tag("limit", limit)],
    );
}

/// The outcome of memory admission for one new lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryAdmissionOutcome {
    Accepted,
    InsufficientCapacity,
    RequirementOverflow,
    NodeMemoryUnobservable,
    ReservationsUnobservable,
    ParentCgroupInsufficientCapacity,
    ParentCgroupUnobservable,
}

impl MemoryAdmissionOutcome {
    const fn decision(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::InsufficientCapacity
            | Self::RequirementOverflow
            | Self::ParentCgroupInsufficientCapacity => "refused",
            Self::NodeMemoryUnobservable
            | Self::ReservationsUnobservable
            | Self::ParentCgroupUnobservable => "unavailable",
        }
    }

    pub(crate) const fn reason(self) -> &'static str {
        match self {
            Self::Accepted => "capacity_available",
            Self::InsufficientCapacity => "insufficient_capacity",
            Self::RequirementOverflow => "requirement_overflow",
            Self::NodeMemoryUnobservable => "node_memory_unobservable",
            Self::ReservationsUnobservable => "reservations_unobservable",
            Self::ParentCgroupInsufficientCapacity => "parent_cgroup_insufficient_capacity",
            Self::ParentCgroupUnobservable => "parent_cgroup_unobservable",
        }
    }
}

/// One memory-admission decision for a new lease, tagged `decision`
/// (`accepted`, `refused` or `unavailable`) and `reason`.
pub fn admission_decision(outcome: MemoryAdmissionOutcome) {
    counter(
        "node_memory.admission.decisions",
        1,
        &[
            tag("decision", outcome.decision()),
            tag("reason", outcome.reason()),
        ],
    );
}
