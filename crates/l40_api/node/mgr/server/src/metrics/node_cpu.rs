//! Per-node CPU utilization, pressure, and parent sandbox cgroup metrics
//! recorded by node-manager.

use pplx_space_metrics_emit::gauge;

/// Host CPU utilization in busy cores over the sample interval, derived from
/// `/proc/stat` deltas scaled by the number of online logical CPUs.
///
/// `busy` counts `user + nice + system + irq + softirq + steal`; `user` counts
/// `user + nice`; `system` counts `system + irq + softirq`; `iowait` counts
/// `iowait`.
pub fn host_cores(busy: f64, user: f64, system: f64, iowait: f64) {
    gauge("node_cpu.busy_cores", busy, &[]);
    gauge("node_cpu.user_cores", user, &[]);
    gauge("node_cpu.system_cores", system, &[]);
    gauge("node_cpu.iowait_cores", iowait, &[]);
}

/// Host CPU pressure (`some avg10`, 0..100%) from `/proc/pressure/cpu`.
pub fn pressure_some_avg10(avg10: f64) {
    gauge("node_cpu.pressure_some_avg10", avg10, &[]);
}

/// Parent sandbox cgroup CPU consumption rate in microseconds per second,
/// computed from `cpu.stat` `usage_usec` deltas (1,000,000 equals one busy
/// core).
pub fn space_cgroup_usage_usec_per_sec(rate: f64) {
    gauge("node_cpu.space_cgroup.usage_usec_per_sec", rate, &[]);
}

/// Cumulative number of throttled enforcement periods in the parent sandbox
/// cgroup (`cpu.stat` `nr_throttled`).
pub fn space_cgroup_nr_throttled(count: u64) {
    gauge("node_cpu.space_cgroup.nr_throttled", count as f64, &[]);
}

/// Cumulative throttled CPU time in microseconds in the parent sandbox cgroup
/// (`cpu.stat` `throttled_usec`).
pub fn space_cgroup_throttled_usec(usec: u64) {
    gauge("node_cpu.space_cgroup.throttled_usec", usec as f64, &[]);
}
