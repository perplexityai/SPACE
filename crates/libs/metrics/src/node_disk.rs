//! Node-local `/space/cache` disk occupancy as seen by the node-ctl
//! SSD-headroom tier reconcile (the signal behind the `ssd-headroom-tier`
//! Kubernetes node label that biases sandbox placement).
//! Also owns node-manager's aggregate I/O budget and per-VM IOPS ceiling signals.

use crate::emit::{counter, gauge, tag};

/// Per-VM operations ceiling, not an aggregate host budget. Zero maximum means
/// disabled. Emit every sweep, including when disabled, for gauge continuity.
pub fn disk_io_ceiling_config(maximum: Option<u64>) {
    gauge(
        "node_disk.io_ceiling.enabled",
        f64::from(maximum.is_some()),
        &[],
    );
    gauge(
        "node_disk.io_ceiling.maximum_iops",
        maximum.unwrap_or(0) as f64,
        &[],
    );
    if maximum.is_none() {
        gauge("node_disk.io_ceiling.inventory_success", 0.0, &[]);
        gauge("node_disk.io_ceiling.verified_vms", 0.0, &[]);
        gauge("node_disk.io_ceiling.pending_vms", 0.0, &[]);
        gauge(
            "node_disk.io_ceiling.last_inventory_timestamp_seconds",
            0.0,
            &[],
        );
    }
}

/// Current registered VMs verified by the sweep versus not yet verified. Pending
/// includes idle restore VMs and failed, busy or superseded targets; it does not
/// mean above-limit and can be nonzero during normal restores.
/// Counts require inventory_success=1 and a fresh inventory timestamp. Successful
/// verification is cached only for the same sandbox, slot generation and PID.
pub fn disk_io_ceiling_inventory(verified: u64, pending: u64, timestamp: f64) {
    gauge("node_disk.io_ceiling.verified_vms", verified as f64, &[]);
    gauge("node_disk.io_ceiling.pending_vms", pending as f64, &[]);
    gauge("node_disk.io_ceiling.inventory_success", 1.0, &[]);
    gauge(
        "node_disk.io_ceiling.last_inventory_timestamp_seconds",
        timestamp,
        &[],
    );
}

/// Publish failed inventory validity without resending counts or their timestamp.
/// Consumers must not assume that a telemetry sink retains those older samples.
pub fn disk_io_ceiling_inventory_failed() {
    gauge("node_disk.io_ceiling.inventory_success", 0.0, &[]);
    counter("node_disk.io_ceiling.inventory_failures", 1, &[]);
}

/// Live operations-bucket changes successfully verified by the sweep. Does not
/// count cold-boot config preparation or synchronous restore/adoption updates.
pub fn disk_io_ceiling_updated() {
    counter("node_disk.io_ceiling.updates", 1, &[]);
}

/// Failed sweep attempts, including lifecycle-lock contention and API failures.
/// Timeout tags are bounded; a timeout leaves enforcement unknown and retryable.
pub fn disk_io_ceiling_update_failed(timed_out: bool) {
    counter(
        "node_disk.io_ceiling.update_failures",
        1,
        &[tag("timeout", if timed_out { "true" } else { "false" })],
    );
}

/// Node-manager's five-minute census of canonical local rootfs files, including
/// retained copies. `kind`: template, instance, suspend_checkpoint, disk_snapshot.
/// `backing`: cow, nocow, non_btrfs, unknown. Counts are not running VM counts,
/// unique content counts, write traffic or checksum verification. All bounded
/// categories emit zero on a successful census; incomplete censuses retain the
/// previous counts. Cluster identity is supplied by the telemetry transport.
pub fn rootfs_backing_count(kind: &str, backing: &str, count: u64) {
    gauge(
        "node_disk.rootfs_backing_count",
        count as f64,
        &[tag("kind", kind), tag("backing", backing)],
    );
}

/// Whether the last directory census completed. Individual unreadable files
/// are counted as unknown, so success alone does not prove full classification.
pub fn rootfs_backing_inventory_success(success: bool) {
    gauge(
        "node_disk.rootfs_backing_inventory_success",
        f64::from(success),
        &[],
    );
}

/// Unix time at the beginning of the last successful backing census. Reject
/// stale nodes when calculating adoption; an absent series is not a zero count.
pub fn rootfs_backing_last_success(timestamp: f64) {
    gauge(
        "node_disk.rootfs_backing_last_success_timestamp_seconds",
        timestamp,
        &[],
    );
}

/// Free fraction of the `/space/cache` filesystem in `[0.0, 1.0]`,
/// measured once per node-ctl reconcile tick.
pub fn ssd_free_fraction(value: f64) {
    gauge("node.ssd_free_fraction", value, &[]);
}

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

/// Usage-keyed headroom tier (`0..=3`) advertised on the Kubernetes Node via
/// the `space.pplx.net/ssd-headroom-tier` label.
pub fn ssd_headroom_tier(tier: u8) {
    gauge("node.ssd_headroom_tier", f64::from(tier), &[]);
}

/// Btrfs-exclusive bytes held by locally cached template directories.
pub fn templates_bytes(bytes: u64) {
    gauge("node_disk.templates_bytes", bytes as f64, &[]);
}

/// Btrfs-exclusive bytes held by suspended sandbox snapshot directories.
pub fn snapshotted_sandbox_bytes(bytes: u64) {
    gauge("node_disk.snapshotted_sandbox_bytes", bytes as f64, &[]);
}

/// Btrfs-exclusive bytes held by active sandbox instance directories.
pub fn active_sandbox_bytes(bytes: u64) {
    gauge("node_disk.active_sandbox_bytes", bytes as f64, &[]);
}

/// Btrfs-exclusive bytes held by rolling/named sandbox btrfs snapshots.
pub fn btrfs_snapshot_bytes(bytes: u64) {
    gauge("node_disk.btrfs_snapshot_bytes", bytes as f64, &[]);
}

/// Count of locally cached template directories.
pub fn template_count(count: u64) {
    gauge("node_disk.template_count", count as f64, &[]);
}

/// Count of suspended sandboxes with a local snapshot directory.
pub fn snapshotted_sandbox_count(count: u64) {
    gauge("node_disk.snapshotted_sandbox_count", count as f64, &[]);
}

/// Count of rolling/named sandbox btrfs snapshot directories.
pub fn btrfs_snapshot_count(count: u64) {
    gauge("node_disk.btrfs_snapshot_count", count as f64, &[]);
}

/// Count of path measurements that succeeded during the latest node-manager
/// node disk metrics tick.
pub fn measurement_success_count(count: u64) {
    gauge("node_disk.measurement_success_count", count as f64, &[]);
}

/// Count of target paths that disappeared before measurement during the latest
/// node-manager node disk metrics tick.
pub fn measurement_missing_count(count: u64) {
    gauge("node_disk.measurement_missing_count", count as f64, &[]);
}

/// Count of path measurements that errored during the latest node-manager node
/// disk metrics tick.
pub fn measurement_error_count(count: u64) {
    gauge("node_disk.measurement_error_count", count as f64, &[]);
}

/// Bytes in the `/space/cache` btrfs unallocated pool — the only space
/// metadata chunk growth can draw from. Zero here with full metadata chunks
/// wedges the filesystem (subvolume deletes fail `EROFS`).
pub fn btrfs_unallocated_bytes(bytes: u64) {
    gauge("node_disk.btrfs_unallocated_bytes", bytes as f64, &[]);
}

/// Free bytes trapped inside allocated data block groups — what a data
/// balance could return to the unallocated pool.
pub fn btrfs_data_slack_bytes(bytes: u64) {
    gauge("node_disk.btrfs_data_slack_bytes", bytes as f64, &[]);
}

/// Free bytes inside allocated metadata block groups.
pub fn btrfs_metadata_available_bytes(bytes: u64) {
    gauge(
        "node_disk.btrfs_metadata_available_bytes",
        bytes as f64,
        &[],
    );
}

/// `1` while the balance reconciler's trigger predicate holds (unallocated
/// pool below the low watermark with reclaimable data-chunk slack), whether
/// or not balancing is armed on the node.
pub fn btrfs_balance_needed(needed: bool) {
    gauge(
        "node_disk.btrfs_balance_needed",
        if needed { 1.0 } else { 0.0 },
        &[],
    );
}

/// Balance stages started by the reconciler, tagged by the data-chunk
/// `usage` filter percentage.
pub fn btrfs_balance_stage_runs(dusage: u8) {
    counter(
        "node_disk.btrfs_balance_stage_runs",
        1,
        &[tag("dusage", dusage)],
    );
}

/// Unallocated bytes recovered by one completed balance reconcile pass.
pub fn btrfs_balance_reclaimed_bytes(bytes: u64) {
    counter("node_disk.btrfs_balance_reclaimed_bytes", bytes, &[]);
}

/// Balance passes that exhausted the stage ladder with the unallocated pool
/// still below the low watermark (the node needs eviction/drain, not
/// balance).
pub fn btrfs_balance_ineffective() {
    counter("node_disk.btrfs_balance_ineffective", 1, &[]);
}

/// Whether aggregate sandbox I/O reservation has explicit configuration.
pub fn sandbox_io_reservation_configured(configured: bool) {
    gauge(
        "node_disk.sandbox_io_reservation.configured",
        if configured { 1.0 } else { 0.0 },
        &[],
    );
}

/// Whether aggregate sandbox I/O throttling is currently enabled.
pub fn sandbox_io_reservation_enabled(enabled: bool) {
    gauge(
        "node_disk.sandbox_io_reservation.enabled",
        if enabled { 1.0 } else { 0.0 },
        &[],
    );
}

pub fn sandbox_io_reservation_device_count(count: u64) {
    gauge(
        "node_disk.sandbox_io_reservation.device_count",
        count as f64,
        &[],
    );
}

pub fn sandbox_io_read_iops_max(value: u64) {
    gauge(
        "node_disk.sandbox_io_reservation.read_iops_max",
        value as f64,
        &[],
    );
}

pub fn sandbox_io_write_iops_max(value: u64) {
    gauge(
        "node_disk.sandbox_io_reservation.write_iops_max",
        value as f64,
        &[],
    );
}

pub fn sandbox_io_read_bandwidth_mib_per_sec_max(value: u64) {
    gauge(
        "node_disk.sandbox_io_reservation.read_bandwidth_mib_per_sec_max",
        value as f64,
        &[],
    );
}

pub fn sandbox_io_write_bandwidth_mib_per_sec_max(value: u64) {
    gauge(
        "node_disk.sandbox_io_reservation.write_bandwidth_mib_per_sec_max",
        value as f64,
        &[],
    );
}
