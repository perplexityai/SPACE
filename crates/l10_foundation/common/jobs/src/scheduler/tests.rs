use crate::fixture_job_id;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};

use super::*;

async fn wait_for_waiters(scheduler: &PriorityScheduler, expected: usize) {
    for _ in 0..100 {
        if scheduler
            .state
            .lock()
            .expect("priority scheduler mutex poisoned")
            .waiters
            .len()
            == expected
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("scheduler did not enqueue {expected} waiter(s)");
}

async fn poll_pending_once<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn spawn_waiter(
    scheduler: &Arc<PriorityScheduler>,
    job_id: &str,
    priority: i32,
    acquired: mpsc::UnboundedSender<String>,
    release: Arc<Semaphore>,
) {
    let scheduler = Arc::clone(scheduler);
    let job_id = job_id.to_string();
    tokio::spawn(async move {
        let _permit = scheduler
            .acquire(fixture_job_id(&job_id), JobPriority::new(priority))
            .await;
        acquired.send(job_id).unwrap();
        let _release = release.acquire().await.unwrap();
    });
}

fn band(min_priority: i32, concurrency: usize) -> SchedulerBand {
    SchedulerBand {
        min_priority: JobPriority::new(min_priority),
        concurrency,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn higher_numeric_priority_runs_first() {
    let scheduler = PriorityScheduler::new(1, Vec::new());
    let running = scheduler
        .acquire(fixture_job_id("running"), JobPriority::default())
        .await;
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));

    spawn_waiter(
        &scheduler,
        "priority-10",
        10,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(
        &scheduler,
        "priority-30",
        30,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(
        &scheduler,
        "priority-20",
        20,
        acquired_tx,
        Arc::clone(&release),
    );
    wait_for_waiters(&scheduler, 3).await;

    drop(running);

    assert_eq!(acquired_rx.recv().await.as_deref(), Some("priority-30"));
    release.add_permits(3);
}

#[tokio::test(flavor = "current_thread")]
async fn no_bands_uses_shared_priority_ordering() {
    let scheduler = PriorityScheduler::new(1, Vec::new());
    let running = scheduler
        .acquire(fixture_job_id("running"), JobPriority::default())
        .await;
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));

    spawn_waiter(
        &scheduler,
        "default",
        0,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(
        &scheduler,
        "priority",
        100,
        acquired_tx,
        Arc::clone(&release),
    );
    wait_for_waiters(&scheduler, 2).await;

    drop(running);

    assert_eq!(acquired_rx.recv().await.as_deref(), Some("priority"));
    release.add_permits(2);
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_capacity_runs_control_work_before_default_backlog() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1)]);
    let running_a = scheduler
        .acquire(fixture_job_id("running-a"), JobPriority::default())
        .await;
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    spawn_waiter(
        &scheduler,
        "default",
        90,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    wait_for_waiters(&scheduler, 1).await;
    spawn_waiter(
        &scheduler,
        "control",
        100,
        acquired_tx,
        Arc::clone(&release),
    );

    assert_eq!(acquired_rx.recv().await.as_deref(), Some("control"));
    drop(running_a);
    release.add_permits(2);
}

#[tokio::test(flavor = "current_thread")]
async fn reservation_keeps_default_work_from_starving() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1)]);
    let running_a = scheduler
        .acquire(fixture_job_id("running-a"), JobPriority::new(100))
        .await;
    let _running_b = scheduler
        .acquire(fixture_job_id("running-b"), JobPriority::new(100))
        .await;
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    spawn_waiter(
        &scheduler,
        "more-control",
        200,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(&scheduler, "default", 0, acquired_tx, Arc::clone(&release));
    wait_for_waiters(&scheduler, 2).await;

    drop(running_a);

    assert_eq!(acquired_rx.recv().await.as_deref(), Some("default"));
    release.add_permits(2);
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_reports_depth_by_priority_band() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1)]);
    let running_default = scheduler
        .acquire(fixture_job_id("running-default"), JobPriority::default())
        .await;
    let running_reserved = scheduler
        .acquire(fixture_job_id("running-reserved"), JobPriority::new(100))
        .await;
    let (acquired_tx, _acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    spawn_waiter(
        &scheduler,
        "waiting-default",
        0,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(
        &scheduler,
        "waiting-reserved",
        100,
        acquired_tx,
        Arc::clone(&release),
    );
    wait_for_waiters(&scheduler, 2).await;

    assert_eq!(
        scheduler.snapshot(),
        SchedulerSnapshot {
            bands: vec![
                SchedulerBandSnapshot {
                    min_priority: None,
                    running: 1,
                    waiting: 1,
                },
                SchedulerBandSnapshot {
                    min_priority: Some(JobPriority::new(100)),
                    running: 1,
                    waiting: 1,
                },
            ],
        }
    );

    drop(running_default);
    drop(running_reserved);
    release.add_permits(2);
}

#[tokio::test(flavor = "current_thread")]
async fn pending_job_can_be_promoted() {
    let scheduler = PriorityScheduler::new(1, vec![band(25, 1)]);
    let running = scheduler
        .acquire(fixture_job_id("running"), JobPriority::default())
        .await;
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    spawn_waiter(
        &scheduler,
        "promoted",
        10,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(
        &scheduler,
        "priority-20",
        20,
        acquired_tx,
        Arc::clone(&release),
    );
    wait_for_waiters(&scheduler, 2).await;

    scheduler.raise_waiting_priority(&fixture_job_id("promoted"), JobPriority::new(30));

    assert_eq!(acquired_rx.recv().await.as_deref(), Some("promoted"));
    drop(running);
    release.add_permits(2);
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_waiting_acquire_removes_waiter() {
    let scheduler = PriorityScheduler::new(1, Vec::new());
    let running = scheduler
        .acquire(fixture_job_id("running"), JobPriority::default())
        .await;
    let mut waiting =
        Box::pin(scheduler.acquire(fixture_job_id("waiting"), JobPriority::default()));
    poll_pending_once(waiting.as_mut()).await;
    assert_eq!(scheduler.snapshot().bands[0].waiting, 1);

    drop(waiting);

    assert_eq!(scheduler.snapshot().bands[0].waiting, 0);
    drop(running);
    let next = tokio::time::timeout(
        Duration::from_secs(1),
        scheduler.acquire(fixture_job_id("next"), JobPriority::default()),
    )
    .await
    .expect("cancelled waiter must not block the next acquisition");
    drop(next);
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_promoted_grant_releases_the_promoted_band() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1)]);
    let running_default = scheduler
        .acquire(fixture_job_id("running-default"), JobPriority::default())
        .await;
    let running_protected = scheduler
        .acquire(fixture_job_id("running-protected"), JobPriority::new(100))
        .await;
    let mut promoted =
        Box::pin(scheduler.acquire(fixture_job_id("promoted"), JobPriority::default()));
    poll_pending_once(promoted.as_mut()).await;

    scheduler.raise_waiting_priority(&fixture_job_id("promoted"), JobPriority::new(100));
    drop(running_protected);
    assert_eq!(
        scheduler
            .snapshot()
            .bands
            .iter()
            .map(|band| band.running)
            .collect::<Vec<_>>(),
        vec![1, 1]
    );

    drop(promoted);

    assert_eq!(
        scheduler
            .snapshot()
            .bands
            .iter()
            .map(|band| band.running)
            .collect::<Vec<_>>(),
        vec![1, 0]
    );
    drop(running_default);
    let next = tokio::time::timeout(
        Duration::from_secs(1),
        scheduler.acquire(fixture_job_id("next"), JobPriority::new(100)),
    )
    .await
    .expect("cancelled promoted grant must release protected capacity");
    drop(next);
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_bands_each_keep_protected_capacity() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1), band(1_000, 1)]);

    let critical = scheduler
        .acquire(fixture_job_id("critical"), JobPriority::new(1_000))
        .await;
    let control = scheduler
        .acquire(fixture_job_id("control"), JobPriority::new(100))
        .await;
    let default = scheduler
        .acquire(fixture_job_id("default"), JobPriority::default())
        .await;

    assert_eq!(
        scheduler.snapshot().bands,
        vec![
            SchedulerBandSnapshot {
                min_priority: None,
                running: 1,
                waiting: 0,
            },
            SchedulerBandSnapshot {
                min_priority: Some(JobPriority::new(100)),
                running: 1,
                waiting: 0,
            },
            SchedulerBandSnapshot {
                min_priority: Some(JobPriority::new(1_000)),
                running: 1,
                waiting: 0,
            },
        ]
    );

    drop(default);
    drop(control);
    drop(critical);
}

#[tokio::test(flavor = "current_thread")]
async fn lower_band_cannot_borrow_higher_band_capacity() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1), band(1_000, 1)]);
    let running = scheduler
        .acquire(fixture_job_id("running-default"), JobPriority::default())
        .await;
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));

    spawn_waiter(
        &scheduler,
        "waiting-default",
        0,
        acquired_tx,
        Arc::clone(&release),
    );
    wait_for_waiters(&scheduler, 1).await;

    acquired_rx.try_recv().unwrap_err();
    assert_eq!(scheduler.snapshot().bands[0].waiting, 1);

    drop(running);
    release.add_permits(1);
}

#[tokio::test(flavor = "current_thread")]
async fn higher_band_borrows_idle_lower_band_capacity() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1), band(1_000, 1)]);

    let first = scheduler
        .acquire(fixture_job_id("critical-1"), JobPriority::new(1_000))
        .await;
    let second = scheduler
        .acquire(fixture_job_id("critical-2"), JobPriority::new(1_000))
        .await;
    let third = scheduler
        .acquire(fixture_job_id("critical-3"), JobPriority::new(1_000))
        .await;

    assert_eq!(scheduler.snapshot().bands[2].running, 3);

    drop(first);
    drop(second);
    drop(third);
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_waiting_bands_reclaim_their_capacity_before_borrowing() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1), band(1_000, 1)]);
    let first = scheduler
        .acquire(fixture_job_id("borrowed-1"), JobPriority::new(1_000))
        .await;
    let second = scheduler
        .acquire(fixture_job_id("borrowed-2"), JobPriority::new(1_000))
        .await;
    let third = scheduler
        .acquire(fixture_job_id("borrowed-3"), JobPriority::new(1_000))
        .await;
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    spawn_waiter(
        &scheduler,
        "default",
        0,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(
        &scheduler,
        "control",
        100,
        acquired_tx.clone(),
        Arc::clone(&release),
    );
    spawn_waiter(
        &scheduler,
        "critical",
        1_000,
        acquired_tx,
        Arc::clone(&release),
    );
    wait_for_waiters(&scheduler, 3).await;

    drop(first);
    assert_eq!(acquired_rx.recv().await.as_deref(), Some("control"));
    drop(second);
    assert_eq!(acquired_rx.recv().await.as_deref(), Some("default"));
    drop(third);
    assert_eq!(acquired_rx.recv().await.as_deref(), Some("critical"));

    release.add_permits(3);
}

#[test]
fn spawn_budgets_preserve_capacity_for_due_lower_priority_work() {
    let scheduler = PriorityScheduler::new(1, vec![band(100, 1)]);

    assert_eq!(
        scheduler.spawn_budgets(&[
            JobPriority::new(100),
            JobPriority::new(100),
            JobPriority::default(),
        ]),
        vec![
            SchedulerBandBudget {
                min_priority: None,
                permits: 1,
            },
            SchedulerBandBudget {
                min_priority: Some(JobPriority::new(100)),
                permits: 1,
            },
        ]
    );
    assert_eq!(
        scheduler.spawn_budgets(&[JobPriority::new(100), JobPriority::new(100)]),
        vec![
            SchedulerBandBudget {
                min_priority: None,
                permits: 0,
            },
            SchedulerBandBudget {
                min_priority: Some(JobPriority::new(100)),
                permits: 2,
            },
        ]
    );
}
