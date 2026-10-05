use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pplx_space_common_jobs::{BackgroundWorker, Job, JobError, JobExecutor, WorkerConfig};
use pplx_space_common_svc::{
    AcquireOutcome, LocalHealthLiveness, LocalHealthServer, ServiceMutex, UnixSocketOptions,
};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use super::{
    JobPriority, PriorityBand, PullPriorityBand, UNEXPECTED_SERVER_EXIT_CODE,
    acquire_service_mutex, build_static_credentials, stop_production_server_or_hard_exit,
    validate_pull_priority_bands, wait_for_shutdown_or_hard_exit,
};

const TEST_SERVICE_NAME: &str = "volume-manager-handoff-test";

#[derive(Clone)]
struct GatedExecutor {
    started: Arc<tokio::sync::Notify>,
    finish: Arc<tokio::sync::Notify>,
    executions: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobExecutor for GatedExecutor {
    async fn execute(&self, _job: &Job, _cancellation: CancellationToken) -> Result<i64, JobError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.finish.notified().await;
        Ok(1)
    }
}

fn create_jobs_db(path: &Path) {
    let mut conn = rusqlite::Connection::open(path).unwrap();
    pplx_space_common_svc::sql::sqlite::migrate_common_schema_up(
        &mut conn,
        &super::COMMON_SCHEMA_MIGRATOR,
    )
    .unwrap();
}

fn open_jobs_db(path: &Path) -> Arc<std::sync::Mutex<rusqlite::Connection>> {
    Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open(path).expect("open test jobs db"),
    ))
}

async fn health_status(socket: &Path, service: &str) -> ServingStatus {
    let channel = pplx_space_common_grpc::connect_uds_channel(TEST_SERVICE_NAME, socket)
        .await
        .expect("connect health UDS");
    let response = HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: service.to_string(),
        })
        .await
        .expect("check health")
        .into_inner();
    ServingStatus::try_from(response.status).expect("known health status")
}

async fn spawn_production_health(
    path: &Path,
    token: CancellationToken,
) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
    let listener = pplx_space_common_svc::bind_unix_socket(path, UnixSocketOptions::new(0o600))
        .expect("bind production health");
    let (_, service) = pplx_space_common_svc::grpc_health_service();
    tokio::spawn(
        pplx_space_common_svc::grpc_server_builder()
            .add_service(service)
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                token.cancelled().await
            }),
    )
}

async fn acquire_test_mutex(path: PathBuf) -> ServiceMutex {
    match ServiceMutex::acquire_with_timeout(
        path,
        &CancellationToken::new(),
        Duration::from_secs(2),
    )
    .await
    .expect("acquire test mutex")
    {
        AcquireOutcome::Acquired(mutex) => mutex,
        AcquireOutcome::PassiveExit => panic!("test mutex acquisition cancelled"),
    }
}

const UNEXPECTED_EXIT_CHILD_DIR_ENV: &str = "VOLMGR_UNEXPECTED_EXIT_CHILD_DIR";
const UNEXPECTED_EXIT_SCENARIO_ENV: &str = "VOLMGR_UNEXPECTED_EXIT_SCENARIO";

#[derive(Clone, Copy, Debug)]
enum UnexpectedExitScenario {
    ServerExit,
    SimultaneousError,
    SimultaneousPanic,
    FinalError,
    FinalPanic,
}

impl UnexpectedExitScenario {
    const ALL: [Self; 5] = [
        Self::ServerExit,
        Self::SimultaneousError,
        Self::SimultaneousPanic,
        Self::FinalError,
        Self::FinalPanic,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::ServerExit => "server-exit",
            Self::SimultaneousError => "simultaneous-error",
            Self::SimultaneousPanic => "simultaneous-panic",
            Self::FinalError => "final-error",
            Self::FinalPanic => "final-panic",
        }
    }

    fn parse(value: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|scenario| scenario.name() == value)
            .unwrap_or_else(|| panic!("unknown unexpected-exit scenario {value}"))
    }

    fn is_final(self) -> bool {
        matches!(self, Self::FinalError | Self::FinalPanic)
    }

    fn should_error(self) -> bool {
        matches!(self, Self::SimultaneousError | Self::FinalError)
    }

    fn should_panic(self) -> bool {
        matches!(self, Self::SimultaneousPanic | Self::FinalPanic)
    }
}

async fn wait_for_path(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("child process handshake timed out");
}

async fn wait_for_task_completion<T>(handle: &tokio::task::JoinHandle<T>) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server task completion timed out");
}

async fn run_unexpected_exit_child(dir: &Path, scenario: UnexpectedExitScenario) -> ! {
    let mut mutex = Some(acquire_test_mutex(dir.join("service.mutex")).await);
    let started = Arc::new(tokio::sync::Notify::new());
    let worker = BackgroundWorker::new(
        open_jobs_db(&dir.join("jobs.db")),
        GatedExecutor {
            started: Arc::clone(&started),
            finish: Arc::new(tokio::sync::Notify::new()),
            executions: Arc::new(AtomicUsize::new(0)),
        },
        WorkerConfig::new(1, Duration::from_secs(60), 1),
    );
    worker
        .submit("pull", "{}", Some("sandbox:test"), Some("restore"))
        .expect("submit blocked transfer");
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("blocked executor started");
    std::fs::write(dir.join("ready"), b"").expect("signal child ready");

    let server_gate = Arc::new(tokio::sync::Notify::new());
    let server_gate_for_task = Arc::clone(&server_gate);
    let mut server = tokio::spawn(async move {
        server_gate_for_task.notified().await;
        assert!(
            !scenario.should_panic(),
            "simulated production-server panic"
        );
        if scenario.should_error() {
            return Err(std::io::Error::other("simulated production-server failure"));
        }
        Ok(())
    });
    let shutdown = CancellationToken::new();
    wait_for_path(&dir.join("exit-now")).await;

    if scenario.is_final() {
        // Enter graceful shutdown while the server is still running, then
        // surface its error/panic only at the final join point.
        shutdown.cancel();
        wait_for_shutdown_or_hard_exit(&shutdown, &mut server, &mut mutex).await;
        server_gate.notify_one();
        stop_production_server_or_hard_exit(
            &CancellationToken::new(),
            &mut server,
            Duration::from_secs(1),
            &mut mutex,
        )
        .await;
    } else {
        server_gate.notify_one();
        wait_for_task_completion(&server).await;
        if !matches!(scenario, UnexpectedExitScenario::ServerExit) {
            // Both select arms are ready. The server result must win even
            // though process shutdown was requested at the same time.
            shutdown.cancel();
        }
        wait_for_shutdown_or_hard_exit(&shutdown, &mut server, &mut mutex).await;
    }
    panic!("unexpected production-server exit must terminate the process")
}

#[tokio::test]
async fn unexpected_server_exit_keeps_mutex_until_process_boundary() {
    if let Some(dir) = std::env::var_os(UNEXPECTED_EXIT_CHILD_DIR_ENV) {
        let dir = PathBuf::from(dir);
        create_jobs_db(&dir.join("jobs.db"));
        let scenario = UnexpectedExitScenario::parse(
            &std::env::var(UNEXPECTED_EXIT_SCENARIO_ENV).expect("child scenario"),
        );
        run_unexpected_exit_child(&dir, scenario).await;
    }

    for scenario in UnexpectedExitScenario::ALL {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .arg("--exact")
            .arg("run::tests::unexpected_server_exit_keeps_mutex_until_process_boundary")
            .arg("--nocapture")
            .env(UNEXPECTED_EXIT_CHILD_DIR_ENV, dir.path())
            .env(UNEXPECTED_EXIT_SCENARIO_ENV, scenario.name())
            .spawn()
            .expect("spawn predecessor process");
        wait_for_path(&dir.path().join("ready")).await;

        let mutex_path = dir.path().join("service.mutex");
        let successor = tokio::spawn(acquire_test_mutex(mutex_path));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !successor.is_finished(),
            "successor must not acquire while the {scenario:?} predecessor executor exists"
        );

        std::fs::write(dir.path().join("exit-now"), b"").expect("trigger hard exit");
        let status = tokio::task::spawn_blocking(move || child.wait())
            .await
            .expect("join child wait")
            .expect("wait for predecessor process");
        assert_eq!(
            status.code(),
            Some(UNEXPECTED_SERVER_EXIT_CODE),
            "{scenario:?} must hard-exit"
        );

        let successor_mutex = tokio::time::timeout(Duration::from_secs(1), successor)
            .await
            .expect("successor acquires after predecessor process exits")
            .expect("successor mutex task");
        drop(successor_mutex);
    }
}

#[tokio::test]
async fn overlapping_handoff_keeps_health_and_transfer_work_available_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mutex_path = dir.path().join("service.mutex");
    let production_socket = dir.path().join("volume-manager.sock");
    let successor_health_socket = dir.path().join("successor-health.sock");
    let predecessor_mutex = acquire_test_mutex(mutex_path.clone()).await;

    let production_shutdown = CancellationToken::new();
    let predecessor_server =
        spawn_production_health(&production_socket, production_shutdown.clone()).await;

    let successor_health_shutdown = CancellationToken::new();
    let successor_health = LocalHealthServer::spawn_with_liveness(
        &successor_health_socket,
        UnixSocketOptions::new(0o600),
        TEST_SERVICE_NAME,
        LocalHealthLiveness::ProductionWhenActive,
        successor_health_shutdown.clone(),
    )
    .expect("start successor health");
    assert_eq!(
        health_status(&successor_health_socket, "readiness").await,
        ServingStatus::Serving,
        "a passive successor must be Ready before predecessor termination"
    );
    assert_eq!(
        health_status(&successor_health_socket, "liveness").await,
        ServingStatus::Serving,
        "a passive successor must stay live"
    );

    let successor_waiter = tokio::spawn(acquire_test_mutex(mutex_path));
    let started = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Notify::new());
    let executions = Arc::new(AtomicUsize::new(0));
    let jobs_path = dir.path().join("jobs.db");
    create_jobs_db(&jobs_path);
    let predecessor_worker = BackgroundWorker::new(
        open_jobs_db(&jobs_path),
        GatedExecutor {
            started: Arc::clone(&started),
            finish: Arc::clone(&finish),
            executions: Arc::clone(&executions),
        },
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    let job_id = predecessor_worker
        .submit("pull", "{}", Some("sandbox:test"), Some("restore"))
        .expect("submit transfer");
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("transfer started");

    assert!(!successor_waiter.is_finished());
    assert_eq!(
        health_status(&production_socket, "").await,
        ServingStatus::Serving,
        "predecessor UDS must stay usable while transfer work drains"
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    finish.notify_one();
    let completed = predecessor_worker
        .wait_for_job(&job_id, Duration::from_secs(1))
        .await
        .expect("transfer completes");
    assert_eq!(completed.status, "completed");
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    production_shutdown.cancel();
    predecessor_server
        .await
        .expect("predecessor server task")
        .expect("predecessor server join");
    drop(predecessor_mutex);

    let successor_mutex = tokio::time::timeout(Duration::from_secs(1), successor_waiter)
        .await
        .expect("successor acquired after predecessor drain")
        .expect("successor mutex task");
    successor_health.mark_starting();
    assert_eq!(
        health_status(&successor_health_socket, "readiness").await,
        ServingStatus::NotServing
    );
    assert_eq!(
        health_status(&successor_health_socket, "liveness").await,
        ServingStatus::Serving,
        "active startup must stay live"
    );
    let successor_production_shutdown = CancellationToken::new();
    let retry_socket = production_socket.clone();
    let retry_client = tokio::spawn(async move {
        pplx_space_common_grpc::connect_uds_channel_with_retry(
            TEST_SERVICE_NAME,
            &retry_socket,
            pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
    });
    let successor_server =
        spawn_production_health(&production_socket, successor_production_shutdown.clone()).await;
    retry_client
        .await
        .expect("retry client task")
        .expect("retry client reaches successor without terminal connect failure");
    successor_health.activate(production_socket.clone());
    assert_eq!(
        health_status(&successor_health_socket, "readiness").await,
        ServingStatus::Serving
    );
    assert_eq!(
        health_status(&successor_health_socket, "liveness").await,
        ServingStatus::Serving,
        "active liveness must forward the production health result"
    );

    let successor_worker = BackgroundWorker::new(
        open_jobs_db(&jobs_path),
        GatedExecutor {
            started,
            finish,
            executions: Arc::clone(&executions),
        },
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );
    assert!(successor_worker.recover().is_empty());
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    successor_production_shutdown.cancel();
    successor_server
        .await
        .expect("successor server task")
        .expect("successor server join");
    assert_eq!(
        health_status(&successor_health_socket, "liveness").await,
        ServingStatus::NotServing,
        "active liveness must fail when the production socket is gone"
    );
    successor_health_shutdown.cancel();
    successor_health.join().await;
    drop(successor_mutex);
}

#[tokio::test]
async fn mutex_timeout_never_transfers_ownership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mutex_path = dir.path().join("service.mutex");
    let predecessor_mutex = acquire_test_mutex(mutex_path.clone()).await;

    let error = ServiceMutex::acquire_with_timeout(
        &mutex_path,
        &CancellationToken::new(),
        Duration::from_millis(30),
    )
    .await
    .expect_err("successor must time out while predecessor owns service");
    assert!(matches!(
        error,
        pplx_space_common_svc::ServiceMutexAcquireError::Timeout { .. }
    ));

    let still_blocked = ServiceMutex::acquire_with_timeout(
        mutex_path,
        &CancellationToken::new(),
        Duration::from_millis(30),
    )
    .await;
    assert!(matches!(
        still_blocked,
        Err(pplx_space_common_svc::ServiceMutexAcquireError::Timeout { .. })
    ));
    drop(predecessor_mutex);
}

#[tokio::test]
async fn mutex_timeout_closes_passive_readiness_and_keeps_waiting() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mutex_path = dir.path().join("service.mutex");
    let health_path = dir.path().join("health.sock");
    let predecessor_mutex = acquire_test_mutex(mutex_path.clone()).await;
    let shutdown = CancellationToken::new();
    let health = Arc::new(
        LocalHealthServer::spawn(
            &health_path,
            UnixSocketOptions::new(0o600),
            TEST_SERVICE_NAME,
            shutdown.clone(),
        )
        .expect("start local health"),
    );
    assert_eq!(
        health_status(&health_path, "readiness").await,
        ServingStatus::Serving
    );

    let waiter_health = Arc::clone(&health);
    let waiter_shutdown = shutdown.clone();
    let waiter = tokio::spawn(async move {
        acquire_service_mutex(
            &mutex_path,
            &waiter_shutdown,
            Duration::from_millis(30),
            &waiter_health,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(!waiter.is_finished());
    assert_eq!(
        health_status(&health_path, "readiness").await,
        ServingStatus::NotServing,
        "an exhausted passive budget must fail closed without restarting"
    );

    drop(predecessor_mutex);
    let successor_mutex = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("successor acquires after predecessor release")
        .expect("join successor waiter")
        .expect("successor mutex wait succeeds")
        .expect("successor remains active");
    drop(successor_mutex);
    shutdown.cancel();
    let health = match Arc::try_unwrap(health) {
        Ok(health) => health,
        Err(_) => panic!("test owns local health"),
    };
    health.join().await;
}

#[tokio::test]
async fn interrupted_transfer_is_recovered_once_by_successor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let jobs_path = dir.path().join("jobs.db");
    create_jobs_db(&jobs_path);
    let job_id = {
        let conn = open_jobs_db(&jobs_path);
        let conn = conn.lock().expect("lock predecessor jobs db");
        pplx_space_common_jobs::db::insert_job(
            &conn,
            pplx_space_common_jobs::db::InsertJob {
                job_id: &"019d84bd-280d-7331-99c6-b63377f3a3a4".parse().unwrap(),
                kind: "pull",
                payload: "{}",
                subject_id: Some("sandbox:test"),
                dedup_key: Some("restore"),
                max_attempts: 1,
                traceparent: None,
                priority: JobPriority::default(),
            },
        )
        .expect("persist interrupted transfer");
        assert!(
            pplx_space_common_jobs::db::mark_job_running(
                &conn,
                &"019d84bd-280d-7331-99c6-b63377f3a3a4".parse().unwrap()
            )
            .expect("mark predecessor transfer running")
        );
        "019d84bd-280d-7331-99c6-b63377f3a3a4"
            .parse::<pplx_space_common_jobs::JobId>()
            .unwrap()
    };

    let started = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Notify::new());
    let executions = Arc::new(AtomicUsize::new(0));
    let successor = BackgroundWorker::new(
        open_jobs_db(&jobs_path),
        GatedExecutor {
            started: Arc::clone(&started),
            finish: Arc::clone(&finish),
            executions: Arc::clone(&executions),
        },
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    );

    assert_eq!(successor.recover(), vec![job_id]);
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("successor executes recovered transfer");
    finish.notify_one();
    assert_eq!(
        successor
            .wait_for_job(&job_id, Duration::from_secs(1))
            .await
            .expect("recovered transfer completes")
            .status,
        "completed"
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert!(successor.recover().is_empty());
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn embedded_migrations_converge_volume_manager_service_and_common_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("volume-manager.db");
    super::migrate_volume_manager_db(&db_path)
        .await
        .expect("apply embedded migrations");

    let conn = rusqlite::Connection::open(&db_path).expect("open migrated database");
    for column in ["traceparent", "lease_owner", "priority", "failure_reason"] {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = ?1",
                [column],
                |row| row.get(0),
            )
            .expect("inspect jobs column");
        assert_eq!(count, 1, "embedded migration must install {column}");
    }
    let latest_common_version = super::COMMON_SCHEMA_MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .max()
        .expect("common migrations");
    let applied: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _space_common_schema_migrations WHERE version = ?1 AND success = TRUE",
                [latest_common_version],
                |row| row.get(0),
            )
            .expect("inspect common migration history");
    assert_eq!(applied, 1);
}

#[test]
fn static_credentials_present_with_session_token() {
    let creds = build_static_credentials(
        Some("ak-local".to_string()),
        Some("sk-local".to_string()),
        Some("sess".to_string()),
    )
    .expect("credentials present");
    assert_eq!(creds.access_key_id, "ak-local");
    assert_eq!(creds.secret_access_key, "sk-local");
    assert_eq!(creds.session_token.as_deref(), Some("sess"));
}

#[test]
fn static_credentials_empty_session_token_becomes_none() {
    let creds = build_static_credentials(
        Some("ak-local".to_string()),
        Some("sk-local".to_string()),
        Some(String::new()),
    )
    .expect("credentials present");
    assert_eq!(creds.session_token, None);
}

#[test]
fn static_credentials_missing_secret_is_none() {
    assert!(build_static_credentials(Some("ak-local".to_string()), None, None).is_none());
}

#[test]
fn static_credentials_empty_access_key_is_none() {
    assert!(
        build_static_credentials(Some(String::new()), Some("sk-local".to_string()), None).is_none()
    );
}

#[test]
fn accepts_multiple_pull_priority_bands() {
    let bands = ["100:4:8", "1000:2:4"]
        .into_iter()
        .map(|value| value.parse::<PullPriorityBand>().expect("parse band"))
        .collect::<Vec<_>>();

    assert_eq!(
        validate_pull_priority_bands(&bands).expect("distinct minimums"),
        vec![
            PriorityBand::new(JobPriority::new(100), 4, 8),
            PriorityBand::new(JobPriority::new(1_000), 2, 4),
        ]
    );
}

#[test]
fn rejects_invalid_pull_priority_bands() {
    for value in ["100:4", "control:4:8", "100:0:8", "100:4:0"] {
        let result = value.parse::<PullPriorityBand>();

        assert!(result.is_err(), "accepted invalid priority band {value}");
    }
}

#[test]
fn rejects_duplicate_pull_priority_band_minimums() {
    let bands = ["100:4:8", "100:2:4"]
        .into_iter()
        .map(|value| value.parse::<PullPriorityBand>().expect("parse band"))
        .collect::<Vec<_>>();

    let error = validate_pull_priority_bands(&bands).expect_err("duplicate minimum");

    assert_eq!(
        error.to_string(),
        "duplicate pull priority band minimum 100"
    );
}
