use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::runtime::Builder;

use super::{
    RuntimeSnapshot, WorkerSnapshot, bounded_worker_threads, interval_health,
    measure_scheduling_delay, parse_worker_threads,
};

#[test]
fn worker_threads_stay_within_the_service_band() {
    for (requested, expected) in [(1, 4), (4, 4), (6, 6), (8, 8), (64, 8)] {
        assert_eq!(
            bounded_worker_threads(NonZeroUsize::new(requested).unwrap()),
            expected
        );
    }
}

#[test]
fn worker_threads_must_be_a_positive_integer() {
    for value in ["", "0", "1.5", "many"] {
        assert!(parse_worker_threads(value).is_err(), "accepted {value:?}");
    }
}

#[test]
fn interval_math_counts_only_continuously_busy_unparked_workers_as_blocked() {
    let sampled_at = Instant::now();
    let previous = RuntimeSnapshot {
        sampled_at,
        num_alive_tasks: 0,
        global_queue_depth: 0,
        workers: vec![
            WorkerSnapshot {
                total_busy_duration: Duration::from_secs(3),
                park_count: 4,
            },
            WorkerSnapshot {
                total_busy_duration: Duration::from_secs(5),
                park_count: 7,
            },
            WorkerSnapshot {
                total_busy_duration: Duration::from_secs(8),
                park_count: 11,
            },
        ],
    };
    let current = RuntimeSnapshot {
        sampled_at: sampled_at + Duration::from_secs(10),
        num_alive_tasks: 0,
        global_queue_depth: 0,
        workers: vec![
            WorkerSnapshot {
                total_busy_duration: Duration::from_secs(12),
                park_count: 4,
            },
            WorkerSnapshot {
                total_busy_duration: Duration::from_secs(15),
                park_count: 8,
            },
            WorkerSnapshot {
                total_busy_duration: Duration::from_secs(16),
                park_count: 11,
            },
        ],
    };

    let health = interval_health(&previous, &current);

    assert!((health.busy_ratio - 0.9).abs() < f64::EPSILON);
    assert_eq!(health.blocked_workers, 1);
}

#[test]
fn scheduling_delay_probe_times_out_while_the_runtime_worker_is_pinned() {
    const BLOCK_DURATION: Duration = Duration::from_millis(100);
    const PROBE_TIMEOUT: Duration = Duration::from_millis(20);

    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let blocker = runtime.spawn(async move {
        started_sender.send(()).unwrap();
        // Deliberately reproduce the failure mode this sampler detects.
        std::thread::sleep(BLOCK_DURATION);
    });
    started_receiver
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    assert_eq!(
        measure_scheduling_delay(runtime.handle(), PROBE_TIMEOUT),
        PROBE_TIMEOUT
    );

    runtime.block_on(blocker).unwrap();
    assert!(measure_scheduling_delay(runtime.handle(), PROBE_TIMEOUT) < PROBE_TIMEOUT);
}
