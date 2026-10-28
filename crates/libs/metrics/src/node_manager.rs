use std::{fmt, time::Instant};

use crate::emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestUdp443Action {
    Observe,
    Reject,
}

impl GuestUdp443Action {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Reject => "reject",
        }
    }
}

impl fmt::Display for GuestUdp443Action {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Current kernel UDP/443 packet total across Leasing/Leased slots on this node.
/// `action` follows the installed verdict; there is no sandbox tag. Includes
/// conntrack-new retries, not unique flows. Slot reset/removal lowers the gauge,
/// so monotonic differencing/rate calculations can miss that interval.
pub fn guest_udp443_packets(packets: u64, action: GuestUdp443Action) {
    gauge(
        "node_mgr.guest_udp443_packets",
        packets as f64,
        &[tag("action", action)],
    );
}

/// Current number of guest VLAN frames dropped across active slots on this
/// node. The gauge intentionally has no sandbox or namespace tag.
pub fn guest_vlan_packets(packets: u64) {
    gauge("node_mgr.guest_vlan_packets", packets as f64, &[]);
}

/// Lifecycle admission uses the serving method name. Restore admission is tagged
/// `RestoreVmState`; include the former `LoadSnapshot` label while upgrading nodes.
/// The `LOAD_SNAPSHOT` configuration suffix remains stable independently of metrics.
pub fn lifecycle_rejected(method: &'static str, reason: &'static str) {
    counter(
        "node_mgr.lifecycle.rejected",
        1,
        &[tag("method", method), tag("reason", reason)],
    );
}

pub fn lifecycle_depth(method: &'static str, in_flight: usize, enqueued: usize) {
    let tags = [tag("method", method)];
    gauge("node_mgr.lifecycle.in_flight", in_flight as f64, &tags);
    gauge(
        "node_mgr.lifecycle.queue_depth",
        enqueued.saturating_sub(in_flight) as f64,
        &tags,
    );
}

pub fn lifecycle_acquired(method: &'static str, started: Instant) {
    timing_ms(
        "node_mgr.lifecycle.acquire_wait_ms",
        elapsed_ms(started),
        &[tag("method", method)],
    );
}

pub fn lifecycle_released(method: &'static str, started: Instant) {
    timing_ms(
        "node_mgr.lifecycle.handler_ms",
        elapsed_ms(started),
        &[tag("method", method)],
    );
}

/// Waiter outcomes are separate from mutation completion: abandoning a gRPC
/// request does not cancel tracked host work.
/// `method` names the serving RPC, including CaptureVmState/RestoreVmState for
/// snapshot capture and restore.
pub fn rpc_wait_finished(method: &'static str, outcome: &'static str, started: Instant) {
    let tags = [tag("method", method), tag("outcome", outcome)];
    counter("node_mgr.rpc_wait.outcome", 1, &tags);
    timing_ms("node_mgr.rpc_wait.duration_ms", elapsed_ms(started), &tags);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupOperationOutcome {
    Succeeded,
    Failed,
    WaiterTimedOut,
}

impl CleanupOperationOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::WaiterTimedOut => "waiter_timed_out",
        }
    }
}

impl fmt::Display for CleanupOperationOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn cleanup_operation_in_flight(value: usize) {
    gauge("node_mgr.cleanup_operation.in_flight", value as f64, &[]);
}

pub fn cleanup_operation_oldest_claim_age_seconds(value: f64) {
    gauge(
        "node_mgr.cleanup_operation.oldest_claim_age_seconds",
        value,
        &[],
    );
}

pub fn cleanup_operation_finished(outcome: CleanupOperationOutcome, started_at: Instant) {
    let tags = [tag("outcome", outcome.as_str())];
    counter("node_mgr.cleanup_operation.outcome", 1, &tags);
    timing_ms(
        "node_mgr.cleanup_operation.duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

pub fn cleanup_operation_waiter_timed_out(started_at: Instant) {
    cleanup_operation_finished(CleanupOperationOutcome::WaiterTimedOut, started_at);
}

/// Loop-device capacity ensured at node-manager startup and on a live
/// slot-budget increase. `present` is the device count the node has; `target` is
/// what it asked for.
///
/// A **deficit** (`present < target`) is the alert condition: the node could not
/// be given the devices it wants and is heading for creates that fail in
/// `losetup`. A surplus is normal and healthy — the node image pre-creates far
/// more devices than a typical derived target — so alerting on any divergence
/// would fire on every correctly-built node.
pub fn loop_device_capacity(present: u32, target: u32) {
    gauge("node_mgr.loop_devices.present", f64::from(present), &[]);
    gauge("node_mgr.loop_devices.target", f64::from(target), &[]);
}

/// Devices and `/dev` nodes node-manager had to create to reach the target.
/// Nonzero on a node whose loop module loaded before the image's modprobe
/// drop-in applied; steady-state zero once a node has been through startup
/// once, because the devices persist for the life of the boot.
pub fn loop_devices_created(kernel_devices: u32, device_nodes: u32) {
    counter(
        "node_mgr.loop_devices.created",
        u64::from(kernel_devices),
        &[tag("kind", "kernel_device")],
    );
    counter(
        "node_mgr.loop_devices.created",
        u64::from(device_nodes),
        &[tag("kind", "device_node")],
    );
}

/// An ensure that could not reach the target. Emitted alongside a best-effort
/// `loop_device_capacity` so a node that failed open at startup is visible
/// rather than simply absent from the capacity gauge.
pub fn loop_device_capacity_failed(phase: &str) {
    counter(
        "node_mgr.loop_devices.ensure_failed",
        1,
        &[tag("phase", phase)],
    );
}

metric_vocabulary!(
    /// How the free-page-hinting pass before a snapshot pause ended. The
    /// pause and snapshot proceed in every case.
    BalloonHintResult {
        /// The guest hinted every free range it found.
        Completed => "completed",
        /// The bounded wait ended first; the run was stopped.
        TimedOut => "timed_out",
        /// A Firecracker balloon API call failed.
        Failed => "failed",
    }
);

metric_vocabulary!(
    /// The balloon read before a snapshot pause that failed.
    BalloonSnapshotProbe {
        /// `GET /balloon`.
        Device => "device",
        /// `GET /balloon/statistics`.
        Statistics => "statistics",
    }
);

/// `node_mgr.balloon.hint.duration_ms`: one free-page-hinting pass before a
/// snapshot pause, from start to completion, timeout, or failure.
pub fn balloon_free_page_hint(result: BalloonHintResult, duration_ms: f64) {
    timing_ms(
        "node_mgr.balloon.hint.duration_ms",
        duration_ms.max(0.0),
        &[tag("result", result)],
    );
}

/// `node_mgr.balloon.probe_failed`: a balloon read before a snapshot pause
/// failed; the pause proceeded without it.
pub fn balloon_snapshot_probe_failed(probe: BalloonSnapshotProbe) {
    counter("node_mgr.balloon.probe_failed", 1, &[tag("probe", probe)]);
}

/// Balloon statistics sampled just before a snapshot pause, tagged with the
/// device's `mode` (`inflate`, `report`, or `hint`):
/// `node_mgr.balloon.actual_mib` is the memory the balloon holds and
/// `node_mgr.balloon.guest_free_mib` the guest's free memory, when the guest
/// has reported it.
pub fn balloon_snapshot_statistics(
    mode: pplx_space_common_core::types::BalloonMode,
    actual_mib: u64,
    guest_free_mib: Option<u64>,
) {
    let tags = [tag("mode", mode)];
    distribution("node_mgr.balloon.actual_mib", actual_mib as f64, &tags);
    if let Some(free_mib) = guest_free_mib {
        distribution("node_mgr.balloon.guest_free_mib", free_mib as f64, &tags);
    }
}

/// Successful unbound historical launches whose sealed runtime cannot claim a complete
/// compiled environment. No sandbox or environment identifiers are tagged.
pub fn legacy_execution_source_launch() {
    counter("node_mgr.execution.legacy_source_launch", 1, &[]);
}

metric_vocabulary!(
    /// The framing check that refused a btrfs send stream before node-manager
    /// relayed it to volume-manager.
    BtrfsSendStreamRejection {
        /// The stream does not start with the btrfs-stream header.
        BadMagic => "bad_magic",
        /// The stream header names a version node-manager does not read.
        Version => "version",
        /// The command is outside the protocol v1 command set SPACE emits.
        Command => "command",
        /// A command declares more payload than a send command can carry.
        CommandTooLarge => "command_too_large",
        /// A command's CRC does not match: the kernel restarted the send
        /// mid-pipe and appended a fresh stream to the partial one.
        Crc => "crc",
        /// Bytes follow the stream's END command.
        DataAfterEnd => "data_after_end",
        /// The stream ended inside an item, before its END command.
        Truncated => "truncated",
    }
);

/// `node_mgr.btrfs_send.stream_rejected`: node-manager refused a btrfs send
/// stream at its framing check, failing that upload attempt (`Aborted`, which
/// volume-manager retries with a fresh send) instead of forwarding a stream
/// `btrfs receive` would reject. Tagged with the check that failed; no lease
/// or sandbox tag.
pub fn btrfs_send_stream_rejected(reason: BtrfsSendStreamRejection) {
    counter(
        "node_mgr.btrfs_send.stream_rejected",
        1,
        &[tag("reason", reason)],
    );
}

/// `node_mgr.btrfs_receive.stream_salvaged`: a restore's `btrfs receive`
/// rejected the staged stream, which was a send the kernel restarted mid-pipe
/// (partial output, then a complete stream), and node-manager received its
/// complete stream instead. Streams stored before the upload framing check
/// existed take this path; no lease or sandbox tag.
pub fn btrfs_receive_stream_salvaged() {
    counter("node_mgr.btrfs_receive.stream_salvaged", 1, &[]);
}

/// `node_mgr.btrfs_receive.failed`: a restore's `btrfs receive` failed for a
/// reason other than cancellation, after node-manager tried to salvage a send
/// restarted mid-pipe from the staged stream. No lease or sandbox tag; the
/// matching warning carries the sandbox.
pub fn btrfs_receive_failed() {
    counter("node_mgr.btrfs_receive.failed", 1, &[]);
}

/// 1 while node-manager enforces the admit-nothing policy it falls back to
/// when it can't read the cluster execution policy it persisted, else 0. It
/// drops to 0 on the first inventory read after an application replaces that
/// policy, even if the application's own record write failed. Emitted on every execution inventory
/// read, which node-ctl makes each node-status tick.
pub fn execution_policy_record_unreadable(unreadable: bool) {
    gauge(
        "node_mgr.execution.policy_record_unreadable",
        f64::from(u8::from(unreadable)),
        &[],
    );
}

/// Successful resolver decisions selecting a consumer other than the recorded
/// source. Counts attempts (including retries), not completed launches; no
/// sandbox or environment identifiers are metric tags.
pub fn execution_consumer_fallback() {
    counter("node_mgr.execution.consumer_fallback", 1, &[]);
}
