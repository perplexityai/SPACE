//! Sandbox-admission view of the node's `/space/cache` disk.

use pplx_space_metrics_emit::{gauge, tag};

pub fn sandbox_admission_available_fraction(value: f64) {
    gauge("node.sandbox_admission.ssd_available_fraction", value, &[]);
}

pub fn sandbox_admission_available_bytes(bytes: u64) {
    gauge(
        "node.sandbox_admission.ssd_available_bytes",
        bytes as f64,
        &[],
    );
}

pub fn sandbox_admission_effective_available_bytes(bytes: u64) {
    gauge(
        "node.sandbox_admission.ssd_effective_available_bytes",
        bytes as f64,
        &[],
    );
}

pub fn sandbox_admission_pending_reservation_bytes(bytes: u64) {
    gauge(
        "node.sandbox_admission.ssd_pending_reservation_bytes",
        bytes as f64,
        &[],
    );
}

pub fn sandbox_admission_btrfs_allocation_pressure(pressure_type: &str, active: bool) {
    gauge(
        "node.sandbox_admission.btrfs_allocation_pressure",
        if active { 1.0 } else { 0.0 },
        &[tag("pressure_type", pressure_type)],
    );
}
