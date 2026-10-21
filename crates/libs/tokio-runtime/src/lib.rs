//! Builds the services' multi-thread Tokio runtime from the `WORKER_THREADS`
//! setting and samples the runtime's health into `pplx-space-metrics`.
//!
//! It lives apart from `pplx-space-common-svc` so a binary can build its
//! runtime without depending on the rest of the service host.

use std::env::VarError;
use std::num::{NonZeroUsize, ParseIntError};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use pplx_space_metrics::tokio_runtime::RuntimeHealth;
use thiserror::Error;
use tokio::runtime::{Builder, Handle, Runtime, RuntimeMetrics};

const WORKER_THREADS_ENV: &str = "WORKER_THREADS";
const MIN_WORKER_THREADS: usize = 4;
const MAX_WORKER_THREADS: usize = 8;
const METRICS_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const SCHEDULING_DELAY_TIMEOUT: Duration = METRICS_SAMPLE_INTERVAL;
const BLOCKED_WORKER_BUSY_RATIO: f64 = 0.9;

#[derive(Debug, Error)]
pub enum BuildMultiThreadRuntimeError {
    #[error("failed to read {WORKER_THREADS_ENV}")]
    ReadWorkerThreads(#[source] VarError),
    #[error("invalid {WORKER_THREADS_ENV} value {value:?}; expected a positive integer")]
    InvalidWorkerThreads {
        value: String,
        #[source]
        source: ParseIntError,
    },
    #[error("failed to determine available CPU parallelism")]
    AvailableParallelism(#[source] std::io::Error),
    #[error("failed to build the Tokio multi-thread runtime")]
    Build(#[source] std::io::Error),
}

/// Builds a Tokio multi-thread runtime with bounded worker parallelism.
///
/// `WORKER_THREADS`, when present, is interpreted as requested parallelism and
/// clamped to 4–8 workers. When it is absent, host parallelism supplies the
/// input to the same clamp.
pub fn build_multi_thread_runtime() -> Result<Runtime, BuildMultiThreadRuntimeError> {
    let requested = match std::env::var(WORKER_THREADS_ENV) {
        Ok(value) => parse_worker_threads(&value).map_err(|source| {
            BuildMultiThreadRuntimeError::InvalidWorkerThreads { value, source }
        })?,
        Err(VarError::NotPresent) => std::thread::available_parallelism()
            .map_err(BuildMultiThreadRuntimeError::AvailableParallelism)?,
        Err(source) => return Err(BuildMultiThreadRuntimeError::ReadWorkerThreads(source)),
    };

    let runtime = Builder::new_multi_thread()
        .worker_threads(bounded_worker_threads(requested))
        .enable_all()
        .build()
        .map_err(BuildMultiThreadRuntimeError::Build)?;
    start_metrics_sampler(runtime.handle().clone());
    Ok(runtime)
}

fn parse_worker_threads(value: &str) -> Result<NonZeroUsize, ParseIntError> {
    value.parse()
}

fn bounded_worker_threads(requested: NonZeroUsize) -> usize {
    requested
        .get()
        .clamp(MIN_WORKER_THREADS, MAX_WORKER_THREADS)
}

#[derive(Clone, Copy, Debug)]
struct WorkerSnapshot {
    total_busy_duration: Duration,
    park_count: u64,
}

#[derive(Debug)]
struct RuntimeSnapshot {
    sampled_at: Instant,
    num_alive_tasks: usize,
    global_queue_depth: usize,
    workers: Vec<WorkerSnapshot>,
}

impl RuntimeSnapshot {
    fn capture(metrics: &RuntimeMetrics) -> Self {
        let num_workers = metrics.num_workers();
        Self {
            sampled_at: Instant::now(),
            num_alive_tasks: metrics.num_alive_tasks(),
            global_queue_depth: metrics.global_queue_depth(),
            workers: (0..num_workers)
                .map(|worker| WorkerSnapshot {
                    total_busy_duration: metrics.worker_total_busy_duration(worker),
                    park_count: metrics.worker_park_count(worker),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct IntervalHealth {
    busy_ratio: f64,
    blocked_workers: usize,
}

fn interval_health(previous: &RuntimeSnapshot, current: &RuntimeSnapshot) -> IntervalHealth {
    debug_assert_eq!(previous.workers.len(), current.workers.len());
    let elapsed = current
        .sampled_at
        .saturating_duration_since(previous.sampled_at);
    if elapsed.is_zero() || current.workers.is_empty() {
        return IntervalHealth {
            busy_ratio: 0.0,
            blocked_workers: 0,
        };
    }

    let worker_deltas = previous
        .workers
        .iter()
        .zip(&current.workers)
        .map(|(previous, current)| {
            (
                current
                    .total_busy_duration
                    .saturating_sub(previous.total_busy_duration),
                previous.park_count == current.park_count,
            )
        })
        .collect::<Vec<_>>();
    let elapsed_seconds = elapsed.as_secs_f64();
    let total_busy_seconds = worker_deltas
        .iter()
        .map(|(busy_duration, _)| busy_duration.as_secs_f64())
        .sum::<f64>();
    let busy_ratio =
        (total_busy_seconds / (elapsed_seconds * current.workers.len() as f64)).clamp(0.0, 1.0);
    let blocked_workers = worker_deltas
        .iter()
        .filter(|(busy_duration, park_count_unchanged)| {
            *park_count_unchanged
                && busy_duration.as_secs_f64() / elapsed_seconds >= BLOCKED_WORKER_BUSY_RATIO
        })
        .count();

    IntervalHealth {
        busy_ratio,
        blocked_workers,
    }
}

fn start_metrics_sampler(handle: Handle) {
    if let Err(error) = std::thread::Builder::new()
        .name("space-tokio-metrics".to_string())
        .spawn(move || sample_metrics_forever(handle))
    {
        tracing::warn!(%error, "failed to start Tokio runtime metrics sampler");
    }
}

fn sample_metrics_forever(handle: Handle) {
    let metrics = handle.metrics();
    let mut previous = RuntimeSnapshot::capture(&metrics);
    let mut next_sample_at = Instant::now() + METRICS_SAMPLE_INTERVAL;

    loop {
        std::thread::sleep(next_sample_at.saturating_duration_since(Instant::now()));

        let current = RuntimeSnapshot::capture(&metrics);
        let interval = interval_health(&previous, &current);
        pplx_space_metrics::tokio_runtime::health(RuntimeHealth {
            num_workers: current.workers.len(),
            num_alive_tasks: current.num_alive_tasks,
            global_queue_depth: current.global_queue_depth,
            busy_ratio: interval.busy_ratio,
            blocked_workers: interval.blocked_workers,
        });
        previous = current;

        let scheduling_delay = measure_scheduling_delay(&handle, SCHEDULING_DELAY_TIMEOUT);
        pplx_space_metrics::tokio_runtime::scheduling_delay(scheduling_delay);

        next_sample_at = (next_sample_at + METRICS_SAMPLE_INTERVAL).max(Instant::now());
    }
}

fn measure_scheduling_delay(handle: &Handle, timeout: Duration) -> Duration {
    let started_at = Instant::now();
    let (sender, receiver) = mpsc::sync_channel(1);
    let _probe = handle.spawn(async move {
        if sender.try_send(()).is_err() {
            tracing::trace!("Tokio scheduling-delay probe receiver no longer waiting");
        }
    });

    match receiver.recv_timeout(timeout) {
        Ok(()) => started_at.elapsed().min(timeout),
        Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => timeout,
    }
}

#[cfg(test)]
mod tests;
