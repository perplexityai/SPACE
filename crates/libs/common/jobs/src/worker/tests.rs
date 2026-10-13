use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use rusqlite::Connection;
use uuid::Uuid;

use super::*;

#[derive(Clone)]
struct TestExecutor {
    started: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
    should_block: Arc<AtomicBool>,
}

#[tokio::test]
async fn guarded_submission_refusal_neither_inserts_nor_reuses_work() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        TestExecutor::blocking(),
        WorkerConfig::new(1, Duration::from_secs(30), 1),
    );
    let existing = worker
        .submit("backup", "{}", Some("sandbox"), Some("artifact"))
        .unwrap();
    for key in ["artifact", "new-artifact"] {
        let error = worker
            .submit_json_guarded(
                "backup",
                &serde_json::json!({}),
                Some("sandbox"),
                Some(key),
                |_| Err(Status::aborted("generation retired")),
            )
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Aborted);
    }
    let conn = worker.lock_db_for_status().unwrap();
    let jobs = db::list_jobs(&conn, Some("sandbox"), None, 0).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, existing);
}

impl TestExecutor {
    fn blocking() -> Self {
        Self {
            started: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(tokio::sync::Notify::new()),
            should_block: Arc::new(AtomicBool::new(true)),
        }
    }

    fn immediate() -> Self {
        Self {
            started: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(tokio::sync::Notify::new()),
            should_block: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn wait_started(&self, count: usize) {
        for _ in 0..100 {
            if self.started.load(Ordering::SeqCst) >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("executor did not start {count} job(s)");
    }

    fn release(&self) {
        self.should_block.store(false, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

#[async_trait]
impl JobExecutor for TestExecutor {
    async fn execute(&self, _job: &Job, cancellation: CancellationToken) -> Result<i64, JobError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        while self.should_block.load(Ordering::SeqCst) {
            tokio::select! {
                _ = self.release.notified() => {}
                _ = cancellation.cancelled() => return Ok(0),
            }
        }
        Ok(1)
    }
}

#[derive(Clone)]
struct RestartYieldExecutor {
    started: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl JobExecutor for RestartYieldExecutor {
    async fn execute(&self, _job: &Job, cancellation: CancellationToken) -> Result<i64, JobError> {
        self.started.notify_one();
        cancellation.cancelled().await;
        Err(JobError::YieldedForRestart)
    }
}

#[tokio::test]
async fn restart_yield_returns_job_to_pending_without_consuming_attempt() {
    let path = test_db_path("restart-yield");
    let db = Arc::new(Mutex::new(open_test_db(&path)));
    let executor = RestartYieldExecutor {
        started: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = BackgroundWorker::new(
        Arc::clone(&db),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    let job_id = worker.submit("test", "{}", None, None).unwrap();
    executor.started.notified().await;
    let queued_job_id = worker.submit("test", "{}", None, None).unwrap();

    let yielded = worker.yield_for_restart(Duration::from_secs(1)).await;

    assert!(yielded);
    assert_eq!(worker.inflight(), 0);
    let job = worker.get_job(&job_id).unwrap().unwrap();
    assert_eq!(job.status, "pending");
    assert_eq!(job.attempts, 0);
    let queued_job = worker.get_job(&queued_job_id).unwrap().unwrap();
    assert_eq!(queued_job.status, "pending");
    assert_eq!(queued_job.attempts, 0);

    let successor = BackgroundWorker::new(
        db,
        TestExecutor::immediate(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    let mut recovered = successor.recover();
    recovered.sort();
    let mut expected = vec![job_id, queued_job_id];
    expected.sort();
    assert_eq!(recovered, expected);
    let recovered = successor
        .wait_for_job(&job_id, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(recovered.status, "completed");
    assert_eq!(recovered.attempts, 1);
    let queued = successor
        .wait_for_job(&queued_job_id, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(queued.status, "completed");
    assert_eq!(queued.attempts, 1);
    remove_test_db(path);
}

#[tokio::test]
async fn quiescent_drain_waits_for_executor_after_its_row_is_terminal() {
    let path = test_db_path("quiescent-drain");
    let executor = TestExecutor::blocking();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(open_test_db(&path))),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    let job_id = worker.submit("test", "{}", None, None).unwrap();
    executor.wait_started(1).await;
    {
        let conn = worker.db.lock().unwrap();
        db::complete_job(&conn, &job_id, 0).unwrap();
    }

    assert!(!worker.shutdown(Duration::ZERO).await);
    assert_eq!(worker.inflight(), 1);

    executor.release();
    assert!(worker.shutdown(Duration::from_secs(1)).await);
    remove_test_db(path);
}

#[derive(Clone)]
struct RetryOrderingExecutor {
    attempts: Arc<Mutex<HashMap<String, usize>>>,
    started: tokio::sync::mpsc::UnboundedSender<String>,
    release: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone)]
struct ExhaustionObserverExecutor {
    observed: Arc<tokio::sync::Notify>,
    message: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl JobExecutor for ExhaustionObserverExecutor {
    async fn execute(&self, _job: &Job, _cancellation: CancellationToken) -> Result<i64, JobError> {
        Err(JobError::retriable("vol-mgr unavailable"))
    }

    async fn retries_exhausted(&self, _job: &Job, message: &str) {
        *self.message.lock().unwrap() = Some(message.to_string());
        self.observed.notify_one();
    }
}

#[async_trait]
impl JobExecutor for RetryOrderingExecutor {
    async fn execute(&self, job: &Job, _cancellation: CancellationToken) -> Result<i64, JobError> {
        let attempt = {
            let mut attempts = self.attempts.lock().unwrap();
            let attempt = attempts.entry(job.payload.clone()).or_default();
            *attempt += 1;
            *attempt
        };
        if job.payload == "promoted" && attempt == 1 {
            self.started.send("promoted:first".to_string()).unwrap();
            return Err(JobError::Retriable("retry".to_string()));
        }

        self.started.send(job.payload.clone()).unwrap();
        let _permit = self.release.acquire().await.unwrap();
        Ok(1)
    }
}

async fn recv_started(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<String>) -> String {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("executor did not start a job")
        .expect("executor start channel closed")
}

#[tokio::test(flavor = "current_thread")]
async fn retriable_exhaustion_notifies_the_executor() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let executor = ExhaustionObserverExecutor {
        observed: Arc::new(tokio::sync::Notify::new()),
        message: Arc::new(Mutex::new(None)),
    };
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );

    let job_id = worker.submit("snapshot-upload", "{}", None, None).unwrap();
    tokio::time::timeout(Duration::from_secs(5), executor.observed.notified())
        .await
        .expect("executor was not notified of retry exhaustion");

    assert_eq!(
        executor.message.lock().unwrap().as_deref(),
        Some("vol-mgr unavailable")
    );
    assert_eq!(worker.get_job(&job_id).unwrap().unwrap().status, "failed");
}

fn test_db_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("space-common-jobs-{name}-{}.db", Uuid::now_v7()))
}

fn open_test_db(path: &PathBuf) -> Connection {
    let conn = Connection::open(path).unwrap();
    init_test_schema(&conn);
    conn
}

#[tokio::test(start_paused = true)]
async fn successful_execution_retains_ownership_until_completion_is_persisted_or_superseded() {
    for (leased, superseded) in [(false, false), (false, true), (true, false), (true, true)] {
        let conn = Connection::open_in_memory().unwrap();
        init_test_schema(&conn);
        conn.execute_batch(
            "CREATE TRIGGER fail_completion BEFORE UPDATE OF status ON jobs
             WHEN NEW.status = 'completed'
             BEGIN SELECT RAISE(FAIL, 'injected completion failure'); END;",
        )
        .unwrap();
        let db = Arc::new(Mutex::new(conn));
        let executor = TestExecutor::immediate();
        let config = WorkerConfig::new(1, Duration::from_secs(5), 1);
        let worker = BackgroundWorker::new(
            Arc::clone(&db),
            executor.clone(),
            if leased {
                config.with_lease(LeaseConfig::new("original"))
            } else {
                config
            },
        );
        let job_id = worker.submit("test", "{}", None, None).unwrap();
        executor.wait_started(1).await;
        assert_eq!(worker.get_job(&job_id).unwrap().unwrap().status, "running");
        assert_eq!(
            worker.inflight(),
            1,
            "successful execution must keep its persistence owner"
        );
        assert!(
            !worker.yield_for_restart(Duration::ZERO).await,
            "handoff cannot discard an uncommitted success"
        );
        {
            let conn = db.lock().unwrap();
            conn.execute_batch("DROP TRIGGER fail_completion").unwrap();
            if superseded {
                if leased {
                    conn.execute(
                        "UPDATE jobs SET lease_owner = 'successor' WHERE job_id = ?1",
                        [&job_id],
                    )
                    .unwrap();
                } else {
                    let recovered = db::recover_interrupted_jobs(&conn, &[]).unwrap();
                    assert_eq!(recovered.len(), 1);
                    assert_eq!(recovered[0].0, job_id);
                    assert!(db::mark_job_running(&conn, &job_id).unwrap());
                }
            }
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..100 {
            if worker.inflight() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(worker.inflight(), 0);
        assert_eq!(
            executor.started.load(Ordering::SeqCst),
            1,
            "persistence retry must not repeat remote execution"
        );
        let job = worker.get_job(&job_id).unwrap().unwrap();
        assert_eq!(job.attempts, if superseded && !leased { 2 } else { 1 });
        if superseded {
            assert_eq!(job.status, "running");
            assert_eq!(job.bytes_processed, 0);
        } else {
            assert_eq!(job.status, "completed");
            assert_eq!(job.bytes_processed, 1);
        }
    }
}

fn remove_test_db(path: PathBuf) {
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to remove test database {}: {error}", path.display()),
    }
}

fn init_test_schema(conn: &Connection) {
    if conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'jobs')",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap()
    {
        return;
    }
    for migration in sqlx::migrate!("../../../../db/common")
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
}

fn worker(
    conn: Connection,
    executor: TestExecutor,
    worker_id: &str,
) -> BackgroundWorker<TestExecutor> {
    BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor,
        WorkerConfig::new(1, Duration::from_secs(5), 1).with_lease(
            LeaseConfig::new(worker_id)
                .with_lease_ttl(Duration::from_secs(30))
                .with_heartbeat_interval(Duration::from_secs(30))
                .with_poll_interval(Duration::from_millis(20)),
        ),
    )
}

fn insert_pending(conn: &Connection, kind: &str) -> JobId {
    insert_pending_with_priority(conn, kind, "{}", JobPriority::default())
}

fn insert_pending_with_priority(
    conn: &Connection,
    kind: &str,
    payload: &str,
    priority: JobPriority,
) -> JobId {
    let job_id = JobId::new();
    db::insert_job(
        conn,
        db::InsertJob {
            job_id: &job_id,
            kind,
            payload,
            subject_id: None,
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority,
        },
    )
    .unwrap();
    job_id
}

#[tokio::test(flavor = "current_thread")]
async fn active_job_limit_rejects_unique_work_and_allows_dedup() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let executor = TestExecutor::blocking();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1)
            .with_kind_concurrency("pull", 1)
            .with_kind_active_job_limit("pull", 1),
    );
    let first = worker
        .submit("pull", "{}", Some("sandbox:first"), Some("first"))
        .unwrap();
    executor.wait_started(1).await;

    let duplicate = worker
        .submit("pull", "{}", Some("sandbox:first"), Some("first"))
        .unwrap();
    let rejected = worker
        .submit("pull", "{}", Some("sandbox:second"), Some("second"))
        .unwrap_err();

    assert_eq!(duplicate, first);
    assert_eq!(rejected.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        worker
            .list_jobs(None, None, 10)
            .unwrap()
            .into_iter()
            .filter(|job| !job.is_terminal())
            .count(),
        1,
    );

    executor.release();
    let completed = worker
        .wait_for_job(&first, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(completed.status, "completed");

    let accepted = worker.submit("pull", "{}", Some("sandbox:second"), Some("second"));
    accepted.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn completed_job_can_be_rejoined_across_submit_persistence_gap() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let db = Arc::new(Mutex::new(conn));
    let worker = BackgroundWorker::new(
        Arc::clone(&db),
        TestExecutor::immediate(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    let first = worker
        .submit_with_duplicate_policy(
            "push",
            "{}",
            Some("sandbox:first"),
            Some("artifact:first"),
            DuplicatePolicy::ReuseExistingOrCompleted,
        )
        .unwrap();
    {
        let conn = db.lock().unwrap();
        assert!(db::mark_job_running(&conn, &first).unwrap());
        assert!(db::complete_job(&conn, &first, 42).unwrap());
    }

    let recovered = worker
        .submit_with_duplicate_policy(
            "push",
            "{}",
            Some("sandbox:first"),
            Some("artifact:first"),
            DuplicatePolicy::ReuseExistingOrCompleted,
        )
        .unwrap();

    assert_eq!(recovered, first);
    assert_eq!(worker.list_jobs(None, None, 10).unwrap().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn failed_job_remains_retryable_with_completed_reuse_policy() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let db = Arc::new(Mutex::new(conn));
    let worker = BackgroundWorker::new(
        Arc::clone(&db),
        TestExecutor::immediate(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    let first = worker
        .submit_with_duplicate_policy(
            "push",
            "{}",
            Some("sandbox:first"),
            Some("artifact:first"),
            DuplicatePolicy::ReuseExistingOrCompleted,
        )
        .unwrap();
    db::fail_job_permanent(&db.lock().unwrap(), &first, "failed", None).unwrap();

    let retry = worker
        .submit_with_duplicate_policy(
            "push",
            "{}",
            Some("sandbox:first"),
            Some("artifact:first"),
            DuplicatePolicy::ReuseExistingOrCompleted,
        )
        .unwrap();

    assert_ne!(retry, first);
    assert_eq!(worker.list_jobs(None, None, 10).unwrap().len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_a_running_job_is_terminal_and_cannot_be_overwritten_by_completion() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let executor = TestExecutor::blocking();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    let job_id = worker.submit("pull", "{}", None, None).unwrap();
    executor.wait_started(1).await;

    let cancelled = worker
        .cancel_job_and_wait(&job_id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(cancelled.status, "cancelled");

    executor.release();
    tokio::task::yield_now().await;
    assert_eq!(
        worker.get_job(&job_id).unwrap().unwrap().status,
        "cancelled"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_before_token_registration_skips_executor() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let job_id = insert_pending(&conn, "pull");
    assert!(db::mark_job_running(&conn, &job_id).unwrap());
    let job = db::request_job_cancellation(&conn, &job_id)
        .unwrap()
        .expect("job");
    let executor = TestExecutor::immediate();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );

    worker.execute_claimed_job(job, None).await;

    assert_eq!(executor.started.load(Ordering::SeqCst), 0);
    assert_eq!(
        worker.get_job(&job_id).unwrap().unwrap().status,
        "cancelled"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn no_priority_bands_uses_shared_active_job_limit() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let executor = TestExecutor::blocking();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1)
            .with_kind_concurrency("pull", 1)
            .with_kind_active_job_limit("pull", 2),
    );

    worker
        .submit("pull", "{}", Some("sandbox:running"), Some("running"))
        .unwrap();
    executor.wait_started(1).await;
    worker
        .submit("pull", "{}", Some("sandbox:pending"), Some("pending"))
        .unwrap();

    let rejected = worker
        .submit_with_priority(
            "pull",
            "{}",
            Some("sandbox:priority"),
            Some("priority"),
            JobPriority::new(100),
        )
        .unwrap_err();

    assert_eq!(rejected.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        worker
            .list_jobs(None, None, 10)
            .unwrap()
            .into_iter()
            .filter(|job| !job.is_terminal())
            .count(),
        2,
    );
    executor.release();
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_priority_bands_protect_each_band() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let executor = TestExecutor::blocking();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1)
            .with_kind_concurrency("pull", 1)
            .with_kind_active_job_limit("pull", 1)
            .with_kind_priority_bands(
                "pull",
                [
                    PriorityBand::new(JobPriority::new(1_000), 1, 1),
                    PriorityBand::new(JobPriority::new(100), 1, 1),
                ],
            ),
    );

    worker
        .submit("pull", "bulk", Some("sandbox:bulk"), Some("bulk"))
        .unwrap();
    worker
        .submit_with_priority(
            "pull",
            "control",
            Some("sandbox:control"),
            Some("control"),
            JobPriority::new(100),
        )
        .unwrap();
    worker
        .submit_with_priority(
            "pull",
            "critical",
            Some("sandbox:critical"),
            Some("critical"),
            JobPriority::new(1_000),
        )
        .unwrap();

    executor.wait_started(3).await;

    let rejected_bulk = worker
        .submit("pull", "bulk-2", Some("sandbox:bulk-2"), Some("bulk-2"))
        .unwrap_err();
    let rejected_control = worker
        .submit_with_priority(
            "pull",
            "control-2",
            Some("sandbox:control-2"),
            Some("control-2"),
            JobPriority::new(100),
        )
        .unwrap_err();
    let rejected_critical = worker
        .submit_with_priority(
            "pull",
            "critical-2",
            Some("sandbox:critical-2"),
            Some("critical-2"),
            JobPriority::new(1_000),
        )
        .unwrap_err();

    assert_eq!(executor.started.load(Ordering::SeqCst), 3);
    assert_eq!(rejected_bulk.code(), tonic::Code::ResourceExhausted);
    assert_eq!(rejected_control.code(), tonic::Code::ResourceExhausted);
    assert_eq!(rejected_critical.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        worker
            .list_jobs(None, None, 10)
            .unwrap()
            .into_iter()
            .filter(|job| !job.is_terminal())
            .count(),
        3
    );
    executor.release();
}

#[tokio::test]
async fn dedup_promotion_during_backoff_changes_retry_scheduling_priority() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        RetryOrderingExecutor {
            attempts: Arc::new(Mutex::new(HashMap::new())),
            started: started_tx,
            release: Arc::clone(&release),
        },
        // The retry has to still be in backoff when the promotion lands and
        // has to leave it during the test: one second is long enough for the
        // promotion below (a single SQLite write) and short enough to wait
        // out. The delay's length is not what this test pins.
        WorkerConfig::new(1, Duration::from_secs(60), 2)
            .with_kind_concurrency("pull", 1)
            .with_retry_backoff_secs([1]),
    );

    let promoted = worker
        .submit_with_priority(
            "pull",
            "promoted",
            Some("sandbox:promoted"),
            Some("artifact:promoted"),
            JobPriority::default(),
        )
        .unwrap();
    assert_eq!(started_rx.recv().await.as_deref(), Some("promoted:first"));
    for _ in 0..100 {
        let is_pending = worker
            .list_jobs(Some("sandbox:promoted"), None, 1)
            .unwrap()
            .first()
            .is_some_and(|job| job.status == "pending");
        if is_pending {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        worker
            .list_jobs(Some("sandbox:promoted"), None, 1)
            .unwrap()
            .first()
            .is_some_and(|job| job.status == "pending")
    );
    assert_eq!(
        worker
            .submit_with_priority(
                "pull",
                "promoted",
                Some("sandbox:promoted"),
                Some("artifact:promoted"),
                JobPriority::new(100),
            )
            .unwrap(),
        promoted
    );

    worker
        .submit_with_priority(
            "pull",
            "blocker",
            Some("sandbox:blocker"),
            Some("artifact:blocker"),
            JobPriority::default(),
        )
        .unwrap();
    assert_eq!(started_rx.recv().await.as_deref(), Some("blocker"));
    // Wait for the retry to come out of backoff rather than sitting out the
    // schedule: the bound only has to outlast the delay above.
    for _ in 0..1_000 {
        if worker.scheduler_for("pull").snapshot().bands[0].waiting == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(worker.scheduler_for("pull").snapshot().bands[0].waiting, 1);

    worker
        .submit_with_priority(
            "pull",
            "competitor",
            Some("sandbox:competitor"),
            Some("artifact:competitor"),
            JobPriority::new(50),
        )
        .unwrap();
    for _ in 0..100 {
        if worker.scheduler_for("pull").snapshot().bands[0].waiting == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(worker.scheduler_for("pull").snapshot().bands[0].waiting, 2);

    release.add_permits(1);

    assert_eq!(started_rx.recv().await.as_deref(), Some("promoted"));
    release.add_permits(2);
}

#[tokio::test(flavor = "current_thread")]
async fn dedup_promotion_before_waiter_registration_uses_promoted_priority() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        RetryOrderingExecutor {
            attempts: Arc::new(Mutex::new(HashMap::new())),
            started: started_tx,
            release: Arc::clone(&release),
        },
        WorkerConfig::new(1, Duration::from_secs(5), 1).with_kind_concurrency("pull", 1),
    );
    worker
        .submit("pull", "blocker", Some("sandbox:blocker"), Some("blocker"))
        .unwrap();
    assert_eq!(recv_started(&mut started_rx).await, "blocker");

    let target = worker
        .submit_with_priority(
            "pull",
            "target",
            Some("sandbox:target"),
            Some("target"),
            JobPriority::default(),
        )
        .unwrap();
    assert_eq!(
        worker
            .submit_with_priority(
                "pull",
                "target",
                Some("sandbox:target"),
                Some("target"),
                JobPriority::new(100),
            )
            .unwrap(),
        target
    );
    worker
        .submit_with_priority(
            "pull",
            "competitor",
            Some("sandbox:competitor"),
            Some("competitor"),
            JobPriority::new(50),
        )
        .unwrap();
    for _ in 0..100 {
        if worker.scheduler_for("pull").snapshot().bands[0].waiting == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    release.add_permits(1);

    assert_eq!(recv_started(&mut started_rx).await, "target");
    release.add_permits(2);
}

#[tokio::test(flavor = "current_thread")]
async fn leased_workers_do_not_double_claim_same_pending_job() {
    let path = test_db_path("single-claim");
    let conn1 = open_test_db(&path);
    let job_id = insert_pending(&conn1, "kind");
    let conn2 = open_test_db(&path);
    let executor = TestExecutor::blocking();
    let worker1 = worker(conn1, executor.clone(), "worker-1");
    let worker2 = worker(conn2, executor.clone(), "worker-2");

    worker1.spawn_leased_job(job_id, "kind");
    worker2.spawn_leased_job(job_id, "kind");
    executor.wait_started(1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(executor.started.load(Ordering::SeqCst), 1);
    executor.release();
    let completed = worker1
        .wait_for_job(&job_id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(completed.status, "completed");

    remove_test_db(path);
}

#[tokio::test(flavor = "current_thread")]
async fn leased_worker_reclaims_expired_running_job() {
    let path = test_db_path("expired-lease-reclaim");
    let conn1 = open_test_db(&path);
    let job_id = insert_pending(&conn1, "kind");
    conn1
        .execute(
            "UPDATE jobs
             SET status = 'running',
                 lease_owner = 'dead-worker',
                 lease_expires_at = datetime('now', '-1 second')
             WHERE job_id = ?1",
            rusqlite::params![job_id],
        )
        .unwrap();
    let conn2 = open_test_db(&path);
    let executor = TestExecutor::immediate();
    let worker = worker(conn2, executor.clone(), "worker-1");

    worker.spawn_due_leased_jobs();
    executor.wait_started(1).await;
    let completed = worker
        .wait_for_job(&job_id, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(completed.status, "completed");
    remove_test_db(path);
}

#[tokio::test(flavor = "current_thread")]
async fn leased_completion_is_ignored_after_ownership_is_lost() {
    let path = test_db_path("lost-lease");
    let conn = open_test_db(&path);
    let job_id = insert_pending(&conn, "kind");
    let executor = TestExecutor::blocking();
    let worker = worker(conn, executor.clone(), "worker-1");

    worker.spawn_leased_job(job_id, "kind");
    executor.wait_started(1).await;
    {
        let conn = worker.db.lock().expect(DB_MUTEX_POISONED);
        conn.execute(
            "UPDATE jobs SET lease_owner = 'worker-2' WHERE job_id = ?1",
            rusqlite::params![job_id],
        )
        .unwrap();
    }

    executor.release();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let job = worker.get_job(&job_id).unwrap().unwrap();
    assert_eq!(job.status, "running");

    remove_test_db(path);
}

#[tokio::test(flavor = "current_thread")]
async fn wait_for_job_observes_completion_from_another_worker() {
    let path = test_db_path("cross-process-wait");
    let conn1 = open_test_db(&path);
    let job_id = insert_pending(&conn1, "kind");
    let conn2 = open_test_db(&path);
    let waiter = worker(conn1, TestExecutor::immediate(), "waiter");
    let runner = worker(conn2, TestExecutor::immediate(), "runner");

    let wait_worker = waiter.clone();
    let wait_job_id = job_id;
    let wait = tokio::spawn(async move {
        wait_worker
            .wait_for_job(&wait_job_id, Duration::from_secs(5))
            .await
    });
    runner.spawn_leased_job(job_id, "kind");
    let job = wait.await.unwrap().unwrap();
    assert_eq!(job.status, "completed");

    remove_test_db(path);
}

#[tokio::test(flavor = "current_thread")]
async fn non_leased_recover_still_requeues_running_jobs() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let job_id = insert_pending(&conn, "kind");
    db::mark_job_running(&conn, &job_id).unwrap();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        TestExecutor::immediate(),
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );

    let recovered = worker.recover();

    assert_eq!(recovered, vec![job_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn non_leased_recover_only_claims_owned_job_kinds() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let snapshot_job = insert_pending(&conn, "snapshot_upload");
    let teardown_job = insert_pending(&conn, "teardown_suspend");
    db::mark_job_running(&conn, &snapshot_job).unwrap();
    db::mark_job_running(&conn, &teardown_job).unwrap();
    let executor = TestExecutor::immediate();
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor.clone(),
        WorkerConfig::new(1, Duration::from_secs(5), 1).with_recovery_job_kind("snapshot_upload"),
    );

    let recovered = worker.recover();

    assert_eq!(recovered, vec![snapshot_job]);
    assert_eq!(
        worker
            .wait_for_job(&snapshot_job, Duration::from_secs(5))
            .await
            .unwrap()
            .status,
        "completed"
    );
    assert_eq!(executor.started.load(Ordering::SeqCst), 1);
    assert_eq!(
        worker.get_job(&teardown_job).unwrap().unwrap().status,
        "running"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn non_leased_recover_preserves_persisted_priority_order() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let low = insert_pending_with_priority(&conn, "pull", "low", JobPriority::default());
    db::mark_job_running(&conn, &low).unwrap();
    let high = insert_pending_with_priority(&conn, "pull", "high", JobPriority::new(100));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        RetryOrderingExecutor {
            attempts: Arc::new(Mutex::new(HashMap::new())),
            started: started_tx,
            release: Arc::clone(&release),
        },
        WorkerConfig::new(1, Duration::from_secs(5), 1).with_kind_concurrency("pull", 1),
    );

    let recovered = worker.recover();

    assert_eq!(recovered, vec![high, low]);
    assert_eq!(recv_started(&mut started_rx).await, "high");
    release.add_permits(1);
    assert_eq!(recv_started(&mut started_rx).await, "low");
    release.add_permits(1);
    assert_eq!(
        worker
            .wait_for_job(&high, Duration::from_secs(5))
            .await
            .unwrap()
            .status,
        "completed"
    );
    assert_eq!(
        worker
            .wait_for_job(&low, Duration::from_secs(5))
            .await
            .unwrap()
            .status,
        "completed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn leased_priority_bands_preserve_capacity_for_due_default_work() {
    let conn = Connection::open_in_memory().unwrap();
    init_test_schema(&conn);
    let normal = insert_pending_with_priority(&conn, "pull", "normal", JobPriority::default());
    let control_a = insert_pending_with_priority(&conn, "pull", "control-a", JobPriority::new(100));
    let control_b = insert_pending_with_priority(&conn, "pull", "control-b", JobPriority::new(100));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        RetryOrderingExecutor {
            attempts: Arc::new(Mutex::new(HashMap::new())),
            started: started_tx,
            release: Arc::clone(&release),
        },
        WorkerConfig::new(1, Duration::from_secs(5), 1)
            .with_kind_concurrency("pull", 1)
            .with_kind_active_job_limit("pull", 10)
            .with_kind_priority_bands("pull", [PriorityBand::new(JobPriority::new(100), 1, 10)])
            .with_lease(LeaseConfig::new("priority-worker")),
    );

    worker.spawn_due_leased_jobs();
    let first = recv_started(&mut started_rx).await;
    let second = recv_started(&mut started_rx).await;

    assert!(first == "normal" || second == "normal");
    let started_control = if first == "normal" { second } else { first };
    assert!(matches!(
        started_control.as_str(),
        "control-a" | "control-b"
    ));
    started_rx.try_recv().unwrap_err();

    release.add_permits(2);
    let (started_control_id, pending_control_id) = if started_control == "control-a" {
        (&control_a, &control_b)
    } else {
        (&control_b, &control_a)
    };
    assert_eq!(
        worker
            .wait_for_job(&normal, Duration::from_secs(5))
            .await
            .unwrap()
            .status,
        "completed"
    );
    assert_eq!(
        worker
            .wait_for_job(started_control_id, Duration::from_secs(5))
            .await
            .unwrap()
            .status,
        "completed"
    );

    worker.spawn_due_leased_jobs();
    assert_eq!(
        recv_started(&mut started_rx).await,
        if started_control == "control-a" {
            "control-b"
        } else {
            "control-a"
        }
    );
    release.add_permits(1);
    assert_eq!(
        worker
            .wait_for_job(pending_control_id, Duration::from_secs(5))
            .await
            .unwrap()
            .status,
        "completed"
    );
}

fn serialized_worker(
    conn: Connection,
    executor: TestExecutor,
    worker_id: &str,
    concurrency: usize,
) -> BackgroundWorker<TestExecutor> {
    BackgroundWorker::new(
        Arc::new(Mutex::new(conn)),
        executor,
        WorkerConfig::new(concurrency, Duration::from_secs(5), 1).with_lease(
            LeaseConfig::new(worker_id)
                .with_lease_ttl(Duration::from_secs(30))
                .with_heartbeat_interval(Duration::from_secs(30))
                .with_poll_interval(Duration::from_millis(20))
                .with_serialized_subjects(),
        ),
    )
}

fn insert_pending_with_subject(conn: &Connection, kind: &str, subject_id: &str) -> JobId {
    let job_id = JobId::new();
    db::insert_job(
        conn,
        db::InsertJob {
            job_id: &job_id,
            kind,
            payload: "{}",
            subject_id: Some(subject_id),
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: JobPriority::default(),
        },
    )
    .unwrap();
    job_id
}

#[tokio::test(flavor = "current_thread")]
async fn serialized_subjects_run_same_subject_jobs_one_at_a_time() {
    let path = test_db_path("serialized-same-subject");
    let conn = open_test_db(&path);
    let job1 = insert_pending_with_subject(&conn, "kind", "tpl-a");
    let job2 = insert_pending_with_subject(&conn, "kind", "tpl-a");
    let executor = TestExecutor::blocking();
    let worker = serialized_worker(conn, executor.clone(), "worker-1", 2);
    let shutdown = CancellationToken::new();
    let driver = tokio::spawn(worker.clone().run_queue_driver(shutdown.clone()));

    executor.wait_started(1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Concurrency allows 2, but the same subject serializes to 1.
    assert_eq!(executor.started.load(Ordering::SeqCst), 1);
    assert_eq!(
        worker.get_job(&job2).unwrap().unwrap().status,
        "pending",
        "second same-subject job must stay queued while the first runs"
    );

    executor.release();
    let first = worker.wait_for_job(&job1, Duration::from_secs(5)).await;
    let second = worker.wait_for_job(&job2, Duration::from_secs(5)).await;
    assert_eq!(first.unwrap().status, "completed");
    assert_eq!(second.unwrap().status, "completed");

    shutdown.cancel();
    driver.await.expect("queue driver task should not panic");
    remove_test_db(path);
}

#[tokio::test(flavor = "current_thread")]
async fn serialized_subjects_serialize_across_workers_sharing_db() {
    let path = test_db_path("serialized-cross-worker");
    let conn1 = open_test_db(&path);
    let job1 = insert_pending_with_subject(&conn1, "kind", "tpl-a");
    let conn2 = open_test_db(&path);
    let executor = TestExecutor::blocking();
    let worker1 = serialized_worker(conn1, executor.clone(), "worker-1", 2);
    let worker2 = serialized_worker(conn2, executor.clone(), "worker-2", 2);

    worker1.spawn_leased_job(job1, "kind");
    executor.wait_started(1).await;

    // Submitted on the sibling worker while worker-1 runs the first job.
    let job2 = {
        let conn = open_test_db(&path);
        insert_pending_with_subject(&conn, "kind", "tpl-a")
    };
    let shutdown = CancellationToken::new();
    let driver2 = tokio::spawn(worker2.clone().run_queue_driver(shutdown.clone()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(executor.started.load(Ordering::SeqCst), 1);
    assert_eq!(worker2.get_job(&job2).unwrap().unwrap().status, "pending");

    executor.release();
    let second = worker2.wait_for_job(&job2, Duration::from_secs(5)).await;
    assert_eq!(second.unwrap().status, "completed");
    let first = worker1.wait_for_job(&job1, Duration::from_secs(5)).await;
    assert_eq!(first.unwrap().status, "completed");

    shutdown.cancel();
    driver2.await.expect("queue driver task should not panic");
    remove_test_db(path);
}

#[tokio::test(flavor = "current_thread")]
async fn serialized_subjects_do_not_starve_other_subjects() {
    let path = test_db_path("serialized-no-starvation");
    let conn = open_test_db(&path);
    let job_a1 = insert_pending_with_subject(&conn, "kind", "tpl-a");
    let job_a2 = insert_pending_with_subject(&conn, "kind", "tpl-a");
    // Force a strict FIFO order between the same-second inserts so a2 is
    // unambiguously behind a1, and b1 is behind both in the due list.
    conn.execute(
        "UPDATE jobs SET created_at = '2020-01-01 00:00:00' WHERE job_id = ?1",
        rusqlite::params![job_a1],
    )
    .unwrap();
    conn.execute(
        "UPDATE jobs SET created_at = '2020-01-01 00:00:01' WHERE job_id = ?1",
        rusqlite::params![job_a2],
    )
    .unwrap();
    let job_b1 = insert_pending_with_subject(&conn, "kind", "tpl-b");
    let executor = TestExecutor::blocking();
    let worker = serialized_worker(conn, executor.clone(), "worker-1", 2);
    let shutdown = CancellationToken::new();
    let driver = tokio::spawn(worker.clone().run_queue_driver(shutdown.clone()));

    // a1 and b1 start; a2 must not eat the second permit while blocked
    // behind a1, or b1 would starve.
    executor.wait_started(2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(executor.started.load(Ordering::SeqCst), 2);
    assert_eq!(worker.get_job(&job_a2).unwrap().unwrap().status, "pending");
    assert_eq!(worker.get_job(&job_b1).unwrap().unwrap().status, "running");

    executor.release();
    for job_id in [&job_a1, &job_a2, &job_b1] {
        let job = worker.wait_for_job(job_id, Duration::from_secs(5)).await;
        assert_eq!(job.unwrap().status, "completed");
    }

    shutdown.cancel();
    driver.await.expect("queue driver task should not panic");
    remove_test_db(path);
}
