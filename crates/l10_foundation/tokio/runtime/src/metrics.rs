//! Process-level health of the shared Tokio runtime.

use std::time::Duration;

use pplx_space_metrics_emit::gauge;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RuntimeHealth {
    pub num_workers: usize,
    pub num_alive_tasks: usize,
    pub global_queue_depth: usize,
    pub busy_ratio: f64,
    pub blocked_workers: usize,
}

pub fn health(sample: RuntimeHealth) {
    gauge("tokio_runtime.num_workers", sample.num_workers as f64, &[]);
    gauge(
        "tokio_runtime.num_alive_tasks",
        sample.num_alive_tasks as f64,
        &[],
    );
    gauge(
        "tokio_runtime.global_queue_depth",
        sample.global_queue_depth as f64,
        &[],
    );
    gauge("tokio_runtime.busy_ratio", sample.busy_ratio, &[]);
    gauge(
        "tokio_runtime.blocked_workers",
        sample.blocked_workers as f64,
        &[],
    );
}

pub fn scheduling_delay(delay: Duration) {
    gauge(
        "tokio_runtime.scheduling_delay_ms",
        delay.as_secs_f64() * 1_000.0,
        &[],
    );
}
