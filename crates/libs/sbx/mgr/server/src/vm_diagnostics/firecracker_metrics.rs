//! Firecracker's metrics FIFO: one JSON object per line, every 60 seconds.
//!
//! Counters (`SharedIncMetric`) report the change since the previous flush.
//! Stored values (`SharedStoreMetric`: fatal signals, panics, seccomp faults,
//! operation latencies) report the current value, so this reader remembers the
//! last one and emits only what changed.

use std::collections::HashMap;
use std::path::PathBuf;

use pplx_space_metrics::sandbox::vm::{
    FIRECRACKER_ERRORS, FirecrackerBlockOp, FirecrackerComponent, FirecrackerError,
    FirecrackerFlushOutcome, FirecrackerOperation, FirecrackerTelemetryStream,
};
use serde_json::Value;
use tokio::net::unix::pipe;

/// A flush is a few kilobytes per device; anything this long is not one.
const MAX_LINE_BYTES: usize = 256 * 1024;
const READ_CHUNK_BYTES: usize = 64 * 1024;

pub(super) struct MetricsReader {
    path: PathBuf,
    receiver: Option<pipe::Receiver>,
    line: Vec<u8>,
    /// Skipping the rest of an oversized line, up to its newline.
    discarding: bool,
    state: FlushState,
}

impl MetricsReader {
    pub(super) fn new(path: PathBuf) -> Self {
        let mut reader = Self {
            path,
            receiver: None,
            line: Vec::new(),
            discarding: false,
            state: FlushState::default(),
        };
        reader.reopen_if_closed();
        reader
    }

    /// Open the FIFO if it is not open. Read-only and non-blocking: the open
    /// never waits for Firecracker, and only a FIFO is accepted. A launch
    /// without one, or a sandbox whose jail is gone, leaves it closed.
    pub(super) fn reopen_if_closed(&mut self) {
        if self.receiver.is_none() {
            self.receiver = pipe::OpenOptions::new().open_receiver(&self.path).ok();
        }
    }

    /// Wait until the FIFO has data, then read and process what is there.
    /// Pending forever while it is closed; the caller's tick reopens it.
    /// Cancel-safe: nothing is consumed before the readiness wait completes.
    pub(super) async fn read_available(&mut self) {
        let mut chunk = vec![0; READ_CHUNK_BYTES];
        let read = {
            let Some(receiver) = self.receiver.as_ref() else {
                return std::future::pending().await;
            };
            match receiver.readable().await {
                Ok(()) => receiver.try_read(&mut chunk),
                Err(error) => Err(error),
            }
        };
        match read {
            // No writer: Firecracker has not opened it yet or has exited.
            Ok(0) => self.receiver = None,
            Ok(read) => self.accept(chunk.get(..read).unwrap_or_default()),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => self.receiver = None,
        }
    }

    fn accept(&mut self, mut bytes: &[u8]) {
        while let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            let (head, rest) = bytes.split_at(newline);
            bytes = rest.get(1..).unwrap_or_default();
            if self.discarding {
                self.discarding = false;
                continue;
            }
            self.line.extend_from_slice(head);
            let line = std::mem::take(&mut self.line);
            self.state.observe_line(&line);
        }
        if self.discarding {
            return;
        }
        self.line.extend_from_slice(bytes);
        if self.line.len() > MAX_LINE_BYTES {
            self.line = Vec::new();
            self.discarding = true;
            pplx_space_metrics::sandbox::vm::firecracker_metrics_flush(
                FirecrackerFlushOutcome::Oversized,
            );
        }
    }
}

#[derive(Default)]
struct FlushState {
    /// Last stored value per `group.field`, for the fields that are not deltas.
    stored: HashMap<(&'static str, &'static str), u64>,
}

impl FlushState {
    fn observe_line(&mut self, line: &[u8]) {
        match serde_json::from_slice::<Value>(line) {
            Ok(flush) if flush.is_object() => {
                pplx_space_metrics::sandbox::vm::firecracker_metrics_flush(
                    FirecrackerFlushOutcome::Parsed,
                );
                self.observe(&flush);
            }
            _ => pplx_space_metrics::sandbox::vm::firecracker_metrics_flush(
                FirecrackerFlushOutcome::Unparsable,
            ),
        }
    }

    fn observe(&mut self, flush: &Value) {
        for error in FIRECRACKER_ERRORS {
            let group = json_group(error.component);
            let Some(value) = field(flush, group, error.error) else {
                continue;
            };
            let count = if is_stored(*error) {
                self.stored_increase(group, error.error, value)
            } else {
                value
            };
            if count > 0 {
                pplx_space_metrics::sandbox::vm::firecracker_errors(*error, count);
            }
        }
        // `block` aggregates every drive; per-drive `block_<id>` entries are
        // not read, so no drive identity reaches a metric.
        for (op, count_field, latency_field) in [
            (FirecrackerBlockOp::Read, "read_count", "read_agg"),
            (FirecrackerBlockOp::Write, "write_count", "write_agg"),
        ] {
            let requests = field(flush, "block", count_field).unwrap_or(0);
            let total_us = flush
                .get("block")
                .and_then(|block| block.get(latency_field))
                .and_then(|latency| latency.get("sum_us"))
                .and_then(Value::as_u64);
            if let Some(total_us) = total_us
                && requests > 0
            {
                pplx_space_metrics::sandbox::vm::firecracker_block_mean_latency(
                    op,
                    total_us as f64 / requests as f64 / 1_000.0,
                );
            }
        }
        for (operation, name) in [
            (FirecrackerOperation::PauseVm, "pause_vm"),
            (FirecrackerOperation::ResumeVm, "resume_vm"),
            (FirecrackerOperation::LoadSnapshot, "load_snapshot"),
            (
                FirecrackerOperation::FullCreateSnapshot,
                "full_create_snapshot",
            ),
            (
                FirecrackerOperation::DiffCreateSnapshot,
                "diff_create_snapshot",
            ),
        ] {
            let Some(duration_us) = field(flush, "latencies_us", name) else {
                continue;
            };
            // Stored until the next operation of the same kind overwrites it.
            let previous = self.stored.insert(("latencies_us", name), duration_us);
            if duration_us > 0 && previous != Some(duration_us) {
                pplx_space_metrics::sandbox::vm::firecracker_operation_duration(
                    operation,
                    duration_us as f64 / 1_000.0,
                );
            }
        }
        let missed_metrics = field(flush, "logger", "missed_metrics_count").unwrap_or(0)
            + field(flush, "logger", "metrics_fails").unwrap_or(0);
        if missed_metrics > 0 {
            pplx_space_metrics::sandbox::vm::firecracker_telemetry_dropped(
                FirecrackerTelemetryStream::Metrics,
                missed_metrics,
            );
        }
        let missed_log = field(flush, "logger", "missed_log_count").unwrap_or(0);
        if missed_log > 0 {
            pplx_space_metrics::sandbox::vm::firecracker_telemetry_dropped(
                FirecrackerTelemetryStream::Log,
                missed_log,
            );
        }
    }

    fn stored_increase(&mut self, group: &'static str, name: &'static str, value: u64) -> u64 {
        let previous = self.stored.insert((group, name), value).unwrap_or(0);
        value.saturating_sub(previous)
    }
}

fn json_group(component: FirecrackerComponent) -> &'static str {
    match component {
        FirecrackerComponent::Block => "block",
        FirecrackerComponent::Net => "net",
        FirecrackerComponent::Vsock => "vsock",
        FirecrackerComponent::Vcpu => "vcpu",
        FirecrackerComponent::Vmm => "vmm",
        FirecrackerComponent::Seccomp => "seccomp",
        FirecrackerComponent::Signal => "signals",
    }
}

/// Fields Firecracker stores rather than counts per flush.
fn is_stored(error: FirecrackerError) -> bool {
    match error.component {
        // Every signal still exported ends the process, so it is stored.
        FirecrackerComponent::Vmm
        | FirecrackerComponent::Seccomp
        | FirecrackerComponent::Signal => true,
        FirecrackerComponent::Block
        | FirecrackerComponent::Net
        | FirecrackerComponent::Vsock
        | FirecrackerComponent::Vcpu => false,
    }
}

fn field(flush: &Value, group: &str, name: &str) -> Option<u64> {
    flush.get(group)?.get(name)?.as_u64()
}
