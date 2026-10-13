//! Firecracker VMM and guest diagnostics, read on the node by sandbox-manager.
//!
//! No metric here carries a sandbox id; every tag is a closed vocabulary, so
//! each name has a fixed, small set of series per node.
//!
//! - `sandbox.firecracker.errors` (counter): error and fault events Firecracker
//!   reported in its metrics flushes, summed over the node's sandboxes. Tags
//!   `component` and `error`, from [`FirecrackerError`]. Firecracker flushes
//!   every 60 seconds, so a burst appears up to a minute late.
//! - `sandbox.firecracker.block.mean_latency_ms` (timing): one sample per
//!   sandbox and flush interval with block requests, the mean request latency
//!   over that interval. Tag `op` = `read` | `write`.
//! - `sandbox.firecracker.operation.duration_ms` (timing): Firecracker's own
//!   duration of a VM pause, resume, snapshot load or snapshot create, recorded
//!   once per operation that a later flush reports. Tag `operation`.
//! - `sandbox.firecracker.metrics.flushes` (counter): metrics flushes read.
//!   Tag `outcome` = `parsed` | `unparsable` | `oversized`.
//! - `sandbox.firecracker.telemetry_dropped` (counter): metrics flushes and log
//!   lines Firecracker could not write because the reader fell behind or the
//!   write failed. Firecracker never blocks on its outputs; it counts these.
//!   Tag `stream` = `metrics` | `log`.
//! - `sandbox.vm.diagnostics` (counter): WARN and ERROR diagnostics forwarded
//!   from Firecracker's log and from the guest's spaced. Tags `source`
//!   (`firecracker` | `guest`), `level` (`warn` | `error`) and `outcome`:
//!   `logged`, `rate_limited` (over the per-sandbox budget, counted but not
//!   logged), or `dropped_in_guest` (past the guest's callsite bound, so its
//!   level is reported as `warn`).
use crate::emit::{counter, tag, timing_ms};

metric_vocabulary!(
    /// A Firecracker metrics field SPACE exports, by device or subsystem.
    FirecrackerComponent {
        Block => "block",
        Net => "net",
        Vsock => "vsock",
        Vcpu => "vcpu",
        /// The VMM process: panics.
        Vmm => "vmm",
        /// A seccomp filter violation; Firecracker exits on it.
        Seccomp => "seccomp",
        /// A fatal or unexpected signal Firecracker received.
        Signal => "signal",
    }
);

/// One Firecracker error field: its component and its name in Firecracker's
/// metrics output, which is the `error` tag value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FirecrackerError {
    pub component: FirecrackerComponent,
    pub error: &'static str,
}

/// Every Firecracker field exported as `sandbox.firecracker.errors`. The
/// `error` tag can only take these values.
pub const FIRECRACKER_ERRORS: &[FirecrackerError] = &{
    use FirecrackerComponent::*;
    const fn e(component: FirecrackerComponent, error: &'static str) -> FirecrackerError {
        FirecrackerError { component, error }
    }
    [
        e(Block, "activate_fails"),
        e(Block, "cfg_fails"),
        e(Block, "event_fails"),
        e(Block, "execute_fails"),
        e(Block, "invalid_reqs_count"),
        // The guest posted no buffers: a guest that stopped servicing its queue.
        e(Block, "no_avail_buffer"),
        e(Block, "update_fails"),
        e(Net, "activate_fails"),
        e(Net, "cfg_fails"),
        e(Net, "event_fails"),
        e(Net, "rx_fails"),
        e(Net, "tx_fails"),
        e(Net, "tap_read_fails"),
        e(Net, "tap_write_fails"),
        e(Net, "tx_malformed_frames"),
        e(Net, "tx_spoofed_mac_count"),
        e(Net, "no_rx_avail_buffer"),
        e(Net, "no_tx_avail_buffer"),
        e(Vsock, "activate_fails"),
        e(Vsock, "cfg_fails"),
        e(Vsock, "conn_event_fails"),
        e(Vsock, "ev_queue_event_fails"),
        e(Vsock, "muxer_event_fails"),
        e(Vsock, "rx_queue_event_fails"),
        e(Vsock, "tx_queue_event_fails"),
        e(Vsock, "rx_read_fails"),
        e(Vsock, "tx_flush_fails"),
        e(Vsock, "tx_write_fails"),
        e(Vcpu, "failures"),
        e(Vcpu, "kvmclock_ctrl_fails"),
        e(Vmm, "panic_count"),
        e(Seccomp, "num_faults"),
        e(Signal, "sigbus"),
        e(Signal, "sigsegv"),
        e(Signal, "sigxfsz"),
        e(Signal, "sigxcpu"),
        e(Signal, "sigpipe"),
        e(Signal, "sighup"),
        e(Signal, "sigill"),
    ]
};

pub fn firecracker_errors(error: FirecrackerError, count: u64) {
    counter(
        "sandbox.firecracker.errors",
        count,
        &[tag("component", error.component), tag("error", error.error)],
    );
}

metric_vocabulary!(
    FirecrackerBlockOp {
        Read => "read",
        Write => "write",
    }
);

pub fn firecracker_block_mean_latency(op: FirecrackerBlockOp, mean_ms: f64) {
    timing_ms(
        "sandbox.firecracker.block.mean_latency_ms",
        mean_ms,
        &[tag("op", op)],
    );
}

metric_vocabulary!(
    /// A VMM operation whose duration Firecracker reports.
    FirecrackerOperation {
        PauseVm => "pause_vm",
        ResumeVm => "resume_vm",
        LoadSnapshot => "load_snapshot",
        FullCreateSnapshot => "full_create_snapshot",
        DiffCreateSnapshot => "diff_create_snapshot",
    }
);

pub fn firecracker_operation_duration(operation: FirecrackerOperation, duration_ms: f64) {
    timing_ms(
        "sandbox.firecracker.operation.duration_ms",
        duration_ms,
        &[tag("operation", operation)],
    );
}

metric_vocabulary!(
    FirecrackerFlushOutcome {
        Parsed => "parsed",
        /// The line was not Firecracker's metrics JSON.
        Unparsable => "unparsable",
        /// The line exceeded the reader's bound and was discarded unread.
        Oversized => "oversized",
    }
);

pub fn firecracker_metrics_flush(outcome: FirecrackerFlushOutcome) {
    counter(
        "sandbox.firecracker.metrics.flushes",
        1,
        &[tag("outcome", outcome)],
    );
}

metric_vocabulary!(
    FirecrackerTelemetryStream {
        Metrics => "metrics",
        Log => "log",
    }
);

pub fn firecracker_telemetry_dropped(stream: FirecrackerTelemetryStream, count: u64) {
    counter(
        "sandbox.firecracker.telemetry_dropped",
        count,
        &[tag("stream", stream)],
    );
}

metric_vocabulary!(
    VmDiagnosticSource {
        Firecracker => "firecracker",
        Guest => "guest",
    }
);

metric_vocabulary!(
    VmDiagnosticLevel {
        Warn => "warn",
        Error => "error",
    }
);

metric_vocabulary!(
    VmDiagnosticOutcome {
        Logged => "logged",
        RateLimited => "rate_limited",
        DroppedInGuest => "dropped_in_guest",
    }
);

pub fn vm_diagnostics(
    source: VmDiagnosticSource,
    level: VmDiagnosticLevel,
    outcome: VmDiagnosticOutcome,
    count: u64,
) {
    counter(
        "sandbox.vm.diagnostics",
        count,
        &[
            tag("source", source),
            tag("level", level),
            tag("outcome", outcome),
        ],
    );
}
