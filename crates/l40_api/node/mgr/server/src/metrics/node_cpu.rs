//! Per-node CPU pressure and parent sandbox cgroup throttling/usage gauges
//! recorded by node-manager.

use pplx_space_metrics_emit::gauge;

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

/// Parent sandbox cgroup throttled periods per second, computed from `cpu.stat`
/// `nr_throttled` deltas.
pub fn space_cgroup_nr_throttled_per_sec(rate: f64) {
    gauge("node_cpu.space_cgroup.nr_throttled_per_sec", rate, &[]);
}

/// Parent sandbox cgroup throttled CPU time in microseconds per second,
/// computed from `cpu.stat` `throttled_usec` deltas (1,000,000 equals one
/// core-second of throttling per wall-clock second).
pub fn space_cgroup_throttled_usec_per_sec(rate: f64) {
    gauge("node_cpu.space_cgroup.throttled_usec_per_sec", rate, &[]);
}
