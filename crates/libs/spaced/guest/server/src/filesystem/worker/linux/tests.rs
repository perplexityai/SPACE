use super::*;

#[tokio::test]
async fn concurrent_invalidation_waits_for_reaping_after_first_caller_cancels() {
    let pool = WorkerPool::new(CancellationToken::new());
    let user = GuestUserName::parse("root").expect("user");
    let channel = tonic::transport::Endpoint::from_static("http://[::]:50051").connect_lazy();
    pool.insert_test_client(user.clone(), channel)
        .await
        .expect("insert");
    let worker = pool.client_for(user).await.expect("worker");
    let release_reaper = Arc::new(tokio::sync::Notify::new());
    let released = release_reaper.clone();
    *worker.supervisor.lock().await = Some(tokio::spawn(async move {
        released.notified().await;
    }));
    let first_pool = pool.clone();
    let first_worker = worker.clone();
    let first = tokio::spawn(async move { first_pool.invalidate(&first_worker).await });
    worker.stop_token.cancelled().await;
    first.abort();
    assert!(first.await.expect_err("first cancelled").is_cancelled());

    let mut second = Box::pin(pool.invalidate(&worker));
    assert!(
        futures_util::poll!(&mut second).is_pending(),
        "map removal and cancellation of a joiner must not imply reaping"
    );
    release_reaper.notify_one();
    second.await;
    assert!(worker.supervisor.lock().await.is_none());
}
use nix::errno::Errno;
use nix::sys::wait::{WaitPidFlag, waitpid};
use nix::unistd::Pid;

use tonic::transport::Endpoint;

#[tokio::test]
async fn active_client_lease_prevents_idle_worker_eviction() {
    let pool = WorkerPool::new(CancellationToken::new());
    let root = GuestUserName::parse("root").expect("root");
    let user = GuestUserName::parse("user").expect("user");
    let channel = Endpoint::from_static("http://[::]:50051").connect_lazy();
    pool.insert_test_client(root.clone(), channel.clone())
        .await
        .expect("root worker");
    pool.insert_test_client(user.clone(), channel)
        .await
        .expect("user worker");
    let lease = pool.client_for(root.clone()).await.expect("lease");
    {
        let mut workers = pool.workers.lock().await;
        let Some(WorkerEntry::Resident(worker)) = workers.get_mut(&root) else {
            panic!("resident");
        };
        worker.last_access = Instant::now() - IDENTITY_WORKER_IDLE_TTL - Duration::from_secs(1);
    }
    drop(pool.client_for(user.clone()).await.expect("another lookup"));
    assert!(
        pool.contains(&lease).await,
        "active execution cannot be idle-evicted"
    );
    drop(lease);
    drop(pool.client_for(user).await.expect("lookup after release"));
    assert!(
        !pool.workers.lock().await.contains_key(&root),
        "released idle worker can be evicted"
    );
}

#[tokio::test]
async fn identity_worker_command_does_not_inherit_parent_environment() {
    const PARENT_ONLY_ENV: &str = "SPACE_SPACED_TEST_PARENT_ONLY_SECRET";
    if !pplx_space_test_env::enter_test_process(&[(
        PARENT_ONLY_ENV,
        Some(std::ffi::OsStr::new("must-not-reach-worker")),
    )]) {
        return;
    }

    let user = GuestUserName::parse("user").expect("valid user");
    let output = super::worker_command(Path::new("/usr/bin/env"), &user, 17)
        .output()
        .await
        .expect("production worker command executes");
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(PARENT_ONLY_ENV));
    let configuration = [
        ("RUST_LOG", "space_spaced_worker=debug"),
        ("SPACE_ENV", "prod"),
        ("SPACE_VERSION", "neutral-revision"),
        (
            "OTEL_RESOURCE_ATTRIBUTES",
            "team=infra,deployment.environment.name=testing",
        ),
        ("OTEL_SERVICE_NAME", "guest-service"),
        ("DD_ENV", "testing"),
        ("DD_VERSION", "legacy-revision"),
        ("SPACE_METRICS_SINK", "otlp"),
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317"),
        ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
        ("SPACE_TRACE_FILTER", "warn"),
        ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4317"),
        ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc"),
        ("OTEL_TRACES_SAMPLER", "traceidratio"),
        ("OTEL_TRACES_SAMPLER_ARG", "0"),
        ("OTEL_BSP_SCHEDULE_DELAY", "50"),
        ("OTEL_BSP_MAX_QUEUE_SIZE", "64"),
        ("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "16"),
        ("OTEL_BSP_EXPORT_TIMEOUT", "100"),
    ];
    for exporter in [
        Some("off"),
        Some("otlp"),
        Some("datadog"),
        Some("auto"),
        None,
        Some(""),
    ] {
        let mut command = {
            let mut env = pplx_space_test_env::TestEnv::default();
            for (name, value) in configuration {
                env.set(name, value);
            }
            if let Some(exporter) = exporter {
                env.set("SPACE_TRACE_EXPORTER", exporter);
            }
            super::worker_command_from_environment(Path::new("/usr/bin/env"), &user, 17, &env)
        };
        let output = command
            .output()
            .await
            .expect("worker environment command executes");
        assert!(output.status.success());
        let environment = String::from_utf8(output.stdout).expect("UTF-8 environment");
        let environment: HashMap<_, _> = environment
            .lines()
            .map(|line| line.split_once('=').expect("environment entry"))
            .collect();
        assert!(!environment.contains_key(PARENT_ONLY_ENV));
        let expected: std::collections::BTreeSet<_> = configuration
            .iter()
            .map(|(key, _)| *key)
            .chain([
                super::WORKER_MODE_ENV,
                super::WORKER_USER_ENV,
                super::WORKER_FD_ENV,
            ])
            // The configuration below forwards an OTel service name, so the
            // worker's own name is pinned to outrank it.
            .chain([super::SPACE_SERVICE_NAME_ENV])
            .chain(exporter.map(|_| "SPACE_TRACE_EXPORTER"))
            .collect();
        assert_eq!(
            environment
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );

        assert_eq!(
            environment.get("SPACE_SPACED_FILESYSTEM_WORKER"),
            Some(&"1")
        );
        assert_eq!(
            environment.get("SPACE_SPACED_FILESYSTEM_WORKER_USER"),
            Some(&"user")
        );
        assert_eq!(
            environment.get("SPACE_SPACED_FILESYSTEM_WORKER_FD"),
            Some(&"17")
        );
        assert_eq!(environment.get("SPACE_TRACE_EXPORTER").copied(), exporter);
        for (name, value) in configuration {
            assert_eq!(environment.get(name), Some(&value), "worker lost {name}");
        }
    }

    for metrics_sink in ["auto", "datadog", "journald", "off", "otlp"] {
        let mut command = {
            let mut env = pplx_space_test_env::TestEnv::default();
            env.set("SPACE_METRICS_SINK", metrics_sink);
            super::worker_command_from_environment(Path::new("/usr/bin/env"), &user, 17, &env)
        };
        let output = command
            .output()
            .await
            .expect("worker metrics environment command executes");
        assert!(output.status.success());
        let environment = String::from_utf8(output.stdout).expect("UTF-8 environment");
        assert!(
            environment
                .lines()
                .any(|line| line == format!("SPACE_METRICS_SINK={metrics_sink}")),
            "unexpected inheritance for SPACE_METRICS_SINK={metrics_sink}:\n{environment}"
        );
    }
}

/// The worker names itself only when the forwarded environment would
/// otherwise rename it. A Datadog-only parent must be untouched.
#[tokio::test]
async fn worker_names_itself_only_when_it_would_inherit_its_launcher() {
    for (name, settings, pinned) in [
        (
            "datadog only",
            &[("DD_SERVICE", "space-spaced-guest"), ("DD_ENV", "prod")][..],
            false,
        ),
        (
            "otel service name",
            &[("OTEL_SERVICE_NAME", "parent")][..],
            true,
        ),
        (
            "otel resource service name",
            &[("OTEL_RESOURCE_ATTRIBUTES", "service.name=parent,team=space")][..],
            true,
        ),
        (
            "blank otel identity does not rename the worker",
            &[
                ("OTEL_SERVICE_NAME", "  "),
                ("OTEL_RESOURCE_ATTRIBUTES", "service.name= ,team=space"),
            ][..],
            false,
        ),
    ] {
        let user = GuestUserName::parse("user").expect("guest user name");
        let mut env = pplx_space_test_env::TestEnv::default();
        for (key, value) in settings {
            env.set(key, value);
        }
        let mut command =
            super::worker_command_from_environment(Path::new("/usr/bin/env"), &user, 17, &env);
        let output = command
            .output()
            .await
            .expect("worker environment command executes");
        assert!(output.status.success());
        let environment = String::from_utf8(output.stdout).expect("UTF-8 environment");
        let environment: HashMap<_, _> = environment
            .lines()
            .map(|line| line.split_once('=').expect("environment entry"))
            .collect();
        let actual = environment.get(super::SPACE_SERVICE_NAME_ENV).copied();
        if pinned {
            assert_eq!(
                actual,
                Some(crate::WORKER_SERVICE_NAME),
                "{name}: worker must name itself, not its launcher"
            );
        } else {
            assert_eq!(
                actual, None,
                "{name}: a worker that cannot be renamed must gain no setting"
            );
        }
        // The parent's own identity still crosses the boundary either way.
        for (key, value) in settings {
            if super::WORKER_ENV_ALLOWLIST.contains(key) {
                assert_eq!(environment.get(key).copied(), Some(*value), "{name}: {key}");
            }
        }
    }
}

#[tokio::test]
async fn expired_worker_shutdown_does_not_block_another_resident() {
    let pool = WorkerPool::new(CancellationToken::new());
    let expired_user = GuestUserName::parse("root").expect("valid user");
    let resident_user = GuestUserName::parse("user").expect("valid user");
    let release_expired_stop = Arc::new(tokio::sync::Notify::new());
    let expired_supervisor = {
        let release_expired_stop = Arc::clone(&release_expired_stop);
        tokio::spawn(async move {
            release_expired_stop.notified().await;
        })
    };
    let channel = Endpoint::from_static("http://[::]:50051").connect_lazy();
    let expired = WorkerClient {
        client: FilesystemClient::new(channel.clone()),
        user: expired_user.clone(),
        instance_id: uuid::Uuid::now_v7(),
        activity: Arc::new(()),
        ledger_update_slot: Arc::new(tokio::sync::Semaphore::new(1)),
        stop_token: CancellationToken::new(),
        supervisor: Arc::new(Mutex::new(Some(expired_supervisor))),
        pidfd: None,
    };
    let resident = WorkerClient {
        client: FilesystemClient::new(channel),
        user: resident_user.clone(),
        instance_id: uuid::Uuid::now_v7(),
        activity: Arc::new(()),
        ledger_update_slot: Arc::new(tokio::sync::Semaphore::new(1)),
        stop_token: CancellationToken::new(),
        supervisor: Arc::new(Mutex::new(None)),
        pidfd: None,
    };
    {
        let mut workers = pool.workers.lock().await;
        let mut expired_entry = ResidentWorker::new(expired);
        expired_entry.last_access =
            Instant::now() - IDENTITY_WORKER_IDLE_TTL - Duration::from_secs(1);
        workers.insert(expired_user, WorkerEntry::Resident(Box::new(expired_entry)));
        workers.insert(
            resident_user.clone(),
            WorkerEntry::Resident(Box::new(ResidentWorker::new(resident.clone()))),
        );
    }

    let pool_for_lookup = pool.clone();
    let lookup = tokio::spawn(async move { pool_for_lookup.client_for(resident_user).await });
    tokio::task::yield_now().await;
    let pool_accessible =
        tokio::time::timeout(Duration::from_millis(100), pool.contains(&resident))
            .await
            .is_ok();
    release_expired_stop.notify_one();
    lookup
        .await
        .expect("resident lookup task completes")
        .expect("resident lookup succeeds");

    assert!(
        pool_accessible,
        "shutting down an expired identity worker held the global pool lock"
    );
}

#[tokio::test]
async fn same_identity_waiters_share_an_in_progress_start() {
    let pool = WorkerPool::new(CancellationToken::new());
    let user = GuestUserName::parse("user").expect("valid user");
    let startup_id = uuid::Uuid::now_v7();
    let (result_tx, result_rx) = watch::channel(None);
    pool.workers.lock().await.insert(
        user.clone(),
        WorkerEntry::Starting(StartingWorker {
            startup_id,
            result: result_rx,
        }),
    );

    let first = {
        let pool = pool.clone();
        let user = user.clone();
        tokio::spawn(async move { pool.client_for(user).await })
    };
    let second = {
        let pool = pool.clone();
        let user = user.clone();
        tokio::spawn(async move { pool.client_for(user).await })
    };
    tokio::task::yield_now().await;

    let worker = WorkerClient {
        client: FilesystemClient::new(Endpoint::from_static("http://[::]:50051").connect_lazy()),
        user: user.clone(),
        instance_id: uuid::Uuid::now_v7(),
        activity: Arc::new(()),
        ledger_update_slot: Arc::new(tokio::sync::Semaphore::new(1)),
        stop_token: CancellationToken::new(),
        supervisor: Arc::new(Mutex::new(None)),
        pidfd: None,
    };
    let published = pool
        .publish_start(&user, startup_id, Ok(worker.clone()))
        .await;
    result_tx
        .send(Some(published))
        .expect("waiters retain startup receiver");

    for waiter in [first, second] {
        let routed = waiter
            .await
            .expect("waiter task completes")
            .expect("worker startup succeeds");
        assert_eq!(routed.instance_id, worker.instance_id);
    }
}

#[tokio::test]
async fn identity_worker_start_wait_has_a_deadline() {
    let (_result_tx, result_rx) = watch::channel(None);

    let error = match wait_for_start_with_timeout(result_rx, Duration::from_millis(10)).await {
        Ok(_) => panic!("worker startup should time out"),
        Err(error) => error,
    };

    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
}

#[tokio::test]
async fn failed_start_removes_its_capacity_reservation() {
    let pool = WorkerPool::new(CancellationToken::new());
    let user = GuestUserName::parse("user").expect("valid user");
    let startup_id = uuid::Uuid::now_v7();
    let (_result_tx, result_rx) = watch::channel(None);
    pool.workers.lock().await.insert(
        user.clone(),
        WorkerEntry::Starting(StartingWorker {
            startup_id,
            result: result_rx,
        }),
    );

    let result = pool
        .publish_start(
            &user,
            startup_id,
            Err(Status::unavailable("worker failed to start")),
        )
        .await;

    assert!(result.is_err());
    assert!(
        !pool.workers.lock().await.contains_key(&user),
        "failed start retained an identity worker capacity slot"
    );
}

#[tokio::test]
async fn invalidating_a_worker_cancels_its_supervisor() {
    let user = GuestUserName::parse("user").expect("valid user");
    let pool = WorkerPool::new(CancellationToken::new());
    let stop_token = CancellationToken::new();
    let worker = WorkerClient {
        client: FilesystemClient::new(Endpoint::from_static("http://[::]:50051").connect_lazy()),
        user: user.clone(),
        instance_id: uuid::Uuid::now_v7(),
        activity: Arc::new(()),
        ledger_update_slot: Arc::new(tokio::sync::Semaphore::new(1)),
        stop_token: stop_token.clone(),
        supervisor: Arc::new(Mutex::new(None)),
        pidfd: None,
    };
    pool.workers.lock().await.insert(
        user,
        WorkerEntry::Resident(Box::new(ResidentWorker::new(worker.clone()))),
    );

    pool.invalidate(&worker).await;

    assert!(stop_token.is_cancelled());
}

#[tokio::test]
async fn cancelled_worker_supervisor_reaps_its_child() {
    let child = tokio::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("child process");
    let pid = Pid::from_raw(child.id().expect("child pid") as i32);
    let stop_token = CancellationToken::new();
    let supervisor = supervise_worker(
        child,
        stop_token.clone(),
        GuestUserName::parse("user").expect("valid user"),
        Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .expect("process slot"),
    );

    stop_token.cancel();
    tokio::time::timeout(Duration::from_secs(2), supervisor)
        .await
        .expect("child supervisor completes after cancellation")
        .expect("child supervisor task completes");
    assert_eq!(
        waitpid(pid, Some(WaitPidFlag::WNOHANG)),
        Err(Errno::ECHILD),
        "child is reaped after worker cancellation"
    );
}

// An idle worker past its TTL is stopped by the reaper even if no further
// filesystem call ever arrives; a worker kept for the change ledger is not.
#[tokio::test]
async fn reaper_stops_an_expired_idle_worker_without_another_acquisition() {
    let pool = WorkerPool::new(CancellationToken::new());
    let idle = GuestUserName::parse("root").expect("root");
    let ledger = GuestUserName::parse("user").expect("user");
    let channel = Endpoint::from_static("http://[::]:50051").connect_lazy();
    pool.insert_test_client(idle.clone(), channel.clone())
        .await
        .expect("idle worker");
    pool.insert_test_client(ledger.clone(), channel)
        .await
        .expect("ledger worker");
    {
        let mut workers = pool.workers.lock().await;
        for (user, entry) in workers.iter_mut() {
            let WorkerEntry::Resident(worker) = entry else {
                panic!("resident");
            };
            worker.last_access = Instant::now() - IDENTITY_WORKER_IDLE_TTL - Duration::from_secs(1);
            worker.ledger_enabled = *user == ledger;
        }
    }
    pool.ensure_reaper();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.workers.lock().await.contains_key(&idle) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the reaper stops the expired idle worker");
    assert!(
        pool.workers.lock().await.contains_key(&ledger),
        "a worker kept for the change ledger does not expire"
    );
    // Nothing left that can expire, so the reaper exits rather than wake for
    // the ledger worker.
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool
            .reaper_running
            .load(std::sync::atomic::Ordering::Acquire)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the reaper exits once no worker can expire");
}

#[test]
fn next_expiry_is_the_earliest_idle_ttl_and_rechecks_busy_workers() {
    let channel = || Endpoint::from_static("http://[::]:50051").connect_lazy();
    let now = Instant::now();
    let worker = |user: &str, last_access: Instant, busy: bool| {
        let client = WorkerClient {
            client: FilesystemClient::new(channel()),
            user: GuestUserName::parse(user).expect("user"),
            instance_id: uuid::Uuid::now_v7(),
            activity: Arc::new(()),
            ledger_update_slot: Arc::new(tokio::sync::Semaphore::new(1)),
            stop_token: CancellationToken::new(),
            supervisor: Arc::new(Mutex::new(None)),
            pidfd: None,
        };
        let lease = busy.then(|| client.activity.clone());
        let mut resident = ResidentWorker::new(client);
        resident.last_access = last_access;
        (WorkerEntry::Resident(Box::new(resident)), lease)
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = runtime.enter();
    let mut workers = HashMap::new();
    assert_eq!(next_worker_expiry(&workers, now), None);

    let (recent, _) = worker("root", now - Duration::from_secs(60), false);
    workers.insert(GuestUserName::parse("root").unwrap(), recent);
    assert_eq!(
        next_worker_expiry(&workers, now),
        Some(now - Duration::from_secs(60) + IDENTITY_WORKER_IDLE_TTL)
    );

    // Past its TTL but still in use: looked at again later, not now.
    let (busy, _lease) = worker("user", now - IDENTITY_WORKER_IDLE_TTL * 2, true);
    let mut only_busy = HashMap::new();
    only_busy.insert(GuestUserName::parse("user").unwrap(), busy);
    assert_eq!(
        next_worker_expiry(&only_busy, now),
        Some(now + IDENTITY_WORKER_BUSY_RECHECK)
    );
    assert!(take_expired_workers(&mut only_busy, now).is_empty());
}
