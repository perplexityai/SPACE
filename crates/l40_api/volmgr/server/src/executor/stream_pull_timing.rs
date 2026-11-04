//! Per-stage timing of a streamed pull and the summary it logs.

use super::*;

#[derive(Debug, Default)]
pub(super) struct StreamPullTiming {
    pub(super) compressed_input_bytes: std::sync::atomic::AtomicU64,
    pub(super) decoded_output_bytes: std::sync::atomic::AtomicU64,
    pub(super) decode_writer_us: std::sync::atomic::AtomicU64,
    pub(super) decode_writer_cpu_us: std::sync::atomic::AtomicU64,
    pub(super) output_writer_us: std::sync::atomic::AtomicU64,
    pub(super) output_writer_cpu_us: std::sync::atomic::AtomicU64,
    pub(super) writeback_reserve_us: std::sync::atomic::AtomicU64,
    pub(super) writeback_reserve_cpu_us: std::sync::atomic::AtomicU64,
    pub(super) writeback_start_us: std::sync::atomic::AtomicU64,
    pub(super) writeback_start_cpu_us: std::sync::atomic::AtomicU64,
    pub(super) writeback_wait_us: std::sync::atomic::AtomicU64,
    pub(super) writeback_start_calls: std::sync::atomic::AtomicU64,
    pub(super) writeback_submitted_logical_bytes: std::sync::atomic::AtomicU64,
    pub(super) writeback_submitted_dirty_bytes: std::sync::atomic::AtomicU64,
    pub(super) writeback_hole_bytes_released: std::sync::atomic::AtomicU64,
    pub(super) writeback_low_watermark_activations: std::sync::atomic::AtomicU64,
}

pub(super) fn current_thread_cpu_us() -> u64 {
    let Ok(value) = nix::time::clock_gettime(nix::time::ClockId::CLOCK_THREAD_CPUTIME_ID) else {
        return 0;
    };
    let seconds = u64::try_from(value.tv_sec()).unwrap_or(0);
    let nanoseconds = u64::try_from(value.tv_nsec()).unwrap_or(0);
    seconds
        .saturating_mul(1_000_000)
        .saturating_add(nanoseconds / 1_000)
}

impl StreamPullTiming {
    pub(super) fn add(counter: &std::sync::atomic::AtomicU64, value: u64) {
        match counter.try_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_add(value)),
        ) {
            Ok(_) | Err(_) => {}
        }
    }

    pub(super) fn add_elapsed(counter: &std::sync::atomic::AtomicU64, started_at: Instant) {
        Self::add(
            counter,
            u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
    }

    pub(super) fn add_wall_and_thread_cpu(
        wall_counter: &std::sync::atomic::AtomicU64,
        cpu_counter: &std::sync::atomic::AtomicU64,
        wall_started_at: Instant,
        cpu_started_at_us: u64,
    ) {
        let cpu_elapsed_us = current_thread_cpu_us().saturating_sub(cpu_started_at_us);
        let wall_elapsed_us =
            u64::try_from(wall_started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        Self::add(wall_counter, wall_elapsed_us);
        Self::add(cpu_counter, cpu_elapsed_us);
    }

    pub(super) fn load(counter: &std::sync::atomic::AtomicU64) -> u64 {
        counter.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub(super) fn log_streamed_pull_pipeline_timing(
    job: &Job,
    payload: &TransferPayload,
    timing: &StreamPullTiming,
    streamed_decoded: bool,
    download_wall_ms: u64,
) {
    let decode_writer_us = StreamPullTiming::load(&timing.decode_writer_us);
    let decode_writer_cpu_us = StreamPullTiming::load(&timing.decode_writer_cpu_us);
    let output_writer_us = StreamPullTiming::load(&timing.output_writer_us);
    let output_writer_cpu_us = StreamPullTiming::load(&timing.output_writer_cpu_us);
    let writeback_reserve_us = StreamPullTiming::load(&timing.writeback_reserve_us);
    let writeback_reserve_cpu_us = StreamPullTiming::load(&timing.writeback_reserve_cpu_us);
    tracing::info!(
        job_id = %job.job_id,
        subject_id = %payload.subject_id,
        s3_uri_len = payload.s3_uri.len(),
        local_path_len = payload.local_path.len(),
        streamed_decoded,
        download_decode_wall_ms = download_wall_ms,
        compressed_input_bytes = StreamPullTiming::load(&timing.compressed_input_bytes),
        decoded_output_bytes = StreamPullTiming::load(&timing.decoded_output_bytes),
        decode_writer_ms = decode_writer_us / 1_000,
        decode_stream_processing_ms = decode_writer_us
            .saturating_sub(output_writer_us)
            .saturating_sub(writeback_reserve_us)
            / 1_000,
        decode_stream_processing_cpu_ms = decode_writer_cpu_us
            .saturating_sub(output_writer_cpu_us)
            .saturating_sub(writeback_reserve_cpu_us)
            / 1_000,
        output_writer_ms = output_writer_us / 1_000,
        output_writer_cpu_ms = output_writer_cpu_us / 1_000,
        writeback_reserve_ms = writeback_reserve_us / 1_000,
        writeback_reserve_cpu_ms = writeback_reserve_cpu_us / 1_000,
        writeback_start_ms = StreamPullTiming::load(&timing.writeback_start_us) / 1_000,
        writeback_start_cpu_ms = StreamPullTiming::load(&timing.writeback_start_cpu_us) / 1_000,
        writeback_wait_observed_ms = StreamPullTiming::load(&timing.writeback_wait_us) / 1_000,
        writeback_start_calls = StreamPullTiming::load(&timing.writeback_start_calls),
        writeback_submitted_logical_bytes =
            StreamPullTiming::load(&timing.writeback_submitted_logical_bytes),
        writeback_submitted_dirty_bytes =
            StreamPullTiming::load(&timing.writeback_submitted_dirty_bytes),
        writeback_hole_bytes_released =
            StreamPullTiming::load(&timing.writeback_hole_bytes_released),
        writeback_low_watermark_activations =
            StreamPullTiming::load(&timing.writeback_low_watermark_activations),
        "streamed pull pipeline timing"
    );
}
