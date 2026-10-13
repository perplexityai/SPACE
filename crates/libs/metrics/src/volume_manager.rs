//! Volume-manager transfer measurements.
//!
//! Push sizing counters share `artifact`, `encoding` and `phase` tags:
//! - `volume_manager.push.payloads`: number of observed payloads.
//! - `volume_manager.push.logical_bytes`: source file lengths before transforms.
//! - `volume_manager.push.wire_bytes`: complete post-transform payload lengths.
//!
//! `prepared` is emitted after the upload file is successfully statted, before
//! calling the object store. It includes payloads whose upload subsequently
//! fails or is cancelled. `verified` is emitted only after HEAD confirms the
//! expected object length. Both are per-attempt observations: retries can count
//! the same artifact again, including after a sibling artifact fails. They are
//! not unique delivered bytes or network bytes including part retransmissions.
//! Partial/failed transforms, failed stats and process death before emission
//! are excluded. These counters must not be added across phases.
//!
//! With the file-backed pipeline, prepared wire bytes for non-raw encodings are
//! completed temporary-file bytes. They do not measure physical disk I/O. The
//! ratio of summed wire bytes to summed logical bytes gives a byte-weighted
//! encoding ratio within one phase and encoding. Raw uploads have no transform
//! temp file. `encoding` describes the volume-manager transform, not whether
//! the source is encrypted: LUKS snapshot artifacts use `zstd` (or `raw` when
//! compression is disabled), not `aead`. Their artifact tags are
//! `encrypted_snapshot_volume` and `encrypted_rootfs_cow`.

use std::time::Instant;

use crate::emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};
use pplx_space_common_core::types::{MaterializationLayout, MaterializationMode};

#[derive(Clone, Copy)]
pub enum PushPayloadEncoding {
    Raw,
    Zstd,
    Aead,
    ZstdAead,
}

impl PushPayloadEncoding {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Zstd => "zstd",
            Self::Aead => "aead",
            Self::ZstdAead => "zstd_aead",
        }
    }
}

#[derive(Clone, Copy)]
pub enum PushPayloadPhase {
    Prepared,
    Verified,
}

impl PushPayloadPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Verified => "verified",
        }
    }
}

pub fn push_payload(
    artifact: &str,
    encoding: PushPayloadEncoding,
    phase: PushPayloadPhase,
    logical_bytes: u64,
    wire_bytes: u64,
) {
    let tags = &[
        tag("artifact", artifact),
        tag("encoding", encoding.as_str()),
        tag("phase", phase.as_str()),
    ];
    counter("volume_manager.push.payloads", 1, tags);
    counter("volume_manager.push.logical_bytes", logical_bytes, tags);
    counter("volume_manager.push.wire_bytes", wire_bytes, tags);
}

/// Streaming uses separate counters so the existing file `prepared` wire
/// counter continues to size temporary files. `uploaded` means the producer
/// finalized and S3 accepted completion; `verified` additionally passed HEAD.
/// These are per-attempt lengths, excluding failed/partial uploads and part
/// retransmissions. Never sum phases or interpret them as disk/network I/O.
#[derive(Clone, Copy)]
pub enum StreamPushPhase {
    Uploaded,
    Verified,
}

impl StreamPushPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uploaded => "uploaded",
            Self::Verified => "verified",
        }
    }
}

pub fn stream_push_payload(
    artifact: &str,
    phase: StreamPushPhase,
    logical_bytes: u64,
    wire_bytes: u64,
) {
    let tags = &[tag("artifact", artifact), tag("phase", phase.as_str())];
    counter("volume_manager.push.stream.payloads", 1, tags);
    counter(
        "volume_manager.push.stream.logical_bytes",
        logical_bytes,
        tags,
    );
    counter("volume_manager.push.stream.wire_bytes", wire_bytes, tags);
}

/// Producer lifetime includes codec lending during backpressure. Pipe-write time
/// includes bridge, downstream and codec-reacquisition waits, not CPU time. Trailing uploads are outside
/// this duration. Both successful and failed producers contribute observations.
pub fn stream_push_producer(
    artifact: &str,
    started: Instant,
    pipe_wait: std::time::Duration,
    success: bool,
) {
    let tags = &[
        tag("artifact", artifact),
        tag("outcome", if success { "success" } else { "error" }),
    ];
    timing_ms(
        "volume_manager.push.stream.producer_ms",
        elapsed_ms(started),
        tags,
    );
    timing_ms(
        "volume_manager.push.stream.pipe_write_ms",
        pipe_wait.as_secs_f64() * 1000.0,
        tags,
    );
}

pub fn transfer_phase_duration(
    kind: &str,
    artifact: &str,
    phase: &str,
    mode: MaterializationMode,
    started_at: Instant,
) {
    timing_ms(
        "volume_manager.transfer.phase.duration_ms",
        elapsed_ms(started_at),
        &[
            tag("kind", kind),
            tag("artifact", artifact),
            tag("phase", phase),
            tag("mode", mode.as_str()),
        ],
    );
}

pub fn transfer_bytes(kind: &str, artifact: &str, form: &str, bytes: u64) {
    distribution(
        "volume_manager.transfer.bytes",
        bytes as f64,
        &[
            tag("kind", kind),
            tag("artifact", artifact),
            tag("form", form),
        ],
    );
}

pub fn transfer_inflation_ratio(kind: &str, artifact: &str, ratio: f64) {
    distribution(
        "volume_manager.transfer.inflation_ratio",
        ratio,
        &[tag("kind", kind), tag("artifact", artifact)],
    );
}

pub fn transfer_throughput_mib_s(
    kind: &str,
    artifact: &str,
    phase: &str,
    mode: MaterializationMode,
    mib_s: f64,
) {
    distribution(
        "volume_manager.transfer.throughput_mib_s",
        mib_s,
        &[
            tag("kind", kind),
            tag("artifact", artifact),
            tag("phase", phase),
            tag("mode", mode.as_str()),
        ],
    );
}

pub fn transfer_materialization_allocation(
    kind: &str,
    artifact: &str,
    mode: MaterializationMode,
    layout: MaterializationLayout,
    logical_bytes: u64,
    allocated_bytes: u64,
    candidate_hole_bytes: u64,
) {
    let tags = &[
        tag("kind", kind),
        tag("artifact", artifact),
        tag("mode", mode.as_str()),
        tag("layout", layout.as_str()),
    ];
    distribution(
        "volume_manager.transfer.materialization.logical_bytes",
        logical_bytes as f64,
        tags,
    );
    distribution(
        "volume_manager.transfer.materialization.allocated_bytes",
        allocated_bytes as f64,
        tags,
    );
    distribution(
        "volume_manager.transfer.materialization.candidate_hole_bytes",
        candidate_hole_bytes as f64,
        tags,
    );
    distribution(
        "volume_manager.transfer.materialization.projected_allocated_bytes",
        logical_bytes.saturating_sub(candidate_hole_bytes) as f64,
        tags,
    );
    if logical_bytes > 0 {
        distribution(
            "volume_manager.transfer.materialization.allocation_ratio",
            allocated_bytes as f64 / logical_bytes as f64,
            tags,
        );
    }
}

pub fn transfer_materialization_shadow_comparison(artifact: &str, outcome: &str) {
    counter(
        "volume_manager.transfer.materialization.shadow_comparison",
        1,
        &[tag("artifact", artifact), tag("outcome", outcome)],
    );
}

pub fn stream_writeback_outstanding_bytes(bytes: u64) {
    gauge(
        "volume_manager.stream_writeback.outstanding_bytes",
        bytes as f64,
        &[],
    );
}

pub fn stream_writeback_producer_blocked_ms(blocked_ms: f64, priority_class: &str) {
    distribution(
        "volume_manager.stream_writeback.producer_blocked_ms",
        blocked_ms,
        &[tag("priority_class", priority_class)],
    );
}

pub fn stream_writeback_failure(operation: &str) {
    counter(
        "volume_manager.stream_writeback.failure",
        1,
        &[tag("operation", operation)],
    );
}
