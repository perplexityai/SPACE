use pplx_space_common_core::types::JobId;
use pplx_space_common_core::types::LocalArtifactId;

use super::*;

fn job_fixture(kind: &str, payload: String) -> Job {
    Job {
        job_id: JobId::new(),
        kind: kind.to_string(),
        status: "running".to_string(),
        payload,
        subject_id: None,
        dedup_key: None,
        failure: None,
        attempts: 1,
        max_attempts: 5,
        bytes_processed: 0,
        error: None,
        created_at: "now".to_string(),
        started_at: Some("now".to_string()),
        completed_at: None,
        traceparent: None,
        lease_owner: None,
        priority: Default::default(),
    }
}

#[tokio::test]
async fn upload_executor_converges_unstaged_metadata_failure() {
    let staging = tempfile::tempdir().unwrap();
    let metadata_dir = staging.path().join("metadata");
    std::fs::create_dir(&metadata_dir).unwrap();
    let mut payload =
        reclaim_fixture_payload("01939e3e-eeee-7000-8000-000000000020", "/unused".into());
    payload.includes_metadata_job = false;
    payload.metadata_staging_dir = Some(metadata_dir.clone());
    let executor = SnapshotUploadExecutor {
        db: crate::db::SharedConnection::new(open_backfill_test_db()),
        drain_state: crate::service::tests::closed_drain_for_test(),
        node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
        volume_manager_socket: absent_volume_manager_socket(),
        retained_source_inspector: std::sync::Arc::new(crate::retained_source::FakeInspector::new(
            crate::retained_source::RetainedSourceCheck::Absent,
        )),
    };
    let job = job_fixture(
        SNAPSHOT_UPLOAD_JOB_KIND,
        serde_json::to_string(&payload).unwrap(),
    );
    let error = executor
        .execute(&job, tokio_util::sync::CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(error, JobError::Permanent(_)));
    assert!(error.message().contains("staged metadata"));
    assert!(
        !metadata_dir.exists(),
        "terminal failure must run convergence cleanup"
    );
}

// The schedule `poll_artifact_job` probes on: soon after a completion, and
// at a bounded rate through a long transfer. The probing itself needs a
// volume-manager and is covered by the transfer tests.
#[test]
fn the_artifact_poll_schedule_notices_completion_promptly_and_bounds_probes() {
    // Probe times after submission, for the first minute of a transfer.
    let mut probes = vec![Duration::ZERO];
    let mut interval = ARTIFACT_POLL_INITIAL;
    while *probes.last().unwrap() < Duration::from_secs(60) {
        let next = *probes.last().unwrap() + interval;
        probes.push(next);
        interval = next_artifact_poll_interval(interval);
    }
    // A transfer that completes at any moment is seen by the next probe.
    let lateness = |done: Duration| *probes.iter().find(|probe| **probe >= done).unwrap() - done;
    assert!(lateness(Duration::from_millis(50)) <= Duration::from_millis(100));
    for ms in (0..60_000).step_by(37) {
        assert!(
            lateness(Duration::from_millis(ms)) <= ARTIFACT_POLL_MAX,
            "completion at {ms}ms is noticed within the probe cap"
        );
    }
    assert!(
        probes.len() <= 125,
        "a minute-long transfer is probed {} times",
        probes.len()
    );
}

#[tokio::test]
async fn cancelled_transfer_poll_yields_before_connecting() {
    let cancellation = tokio_util::sync::CancellationToken::new();
    cancellation.cancel();

    let error = wait_for_artifact_job(
        &absent_volume_manager_socket(),
        "sandbox",
        &JobId::new(),
        cancellation,
    )
    .await
    .unwrap_err();

    assert!(matches!(error, JobError::YieldedForRestart));
}

#[tokio::test(start_paused = true)]
async fn stalled_transfer_probe_is_bounded_without_cancelling_remote_work() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let channel = tonic::transport::Endpoint::from_shared(format!(
        "http://{}",
        listener.local_addr().unwrap(),
    ))
    .unwrap()
    .connect_lazy();
    let client = pplx_space_volmgr_client::VolumeManagerClient::new(
        pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
    );
    let cancellation = tokio_util::sync::CancellationToken::new();
    let probe = tokio::spawn(poll_artifact_job(
        "sandbox",
        JobId::new(),
        client,
        cancellation.clone(),
    ));
    // Accept the connection but never answer its HTTP/2 handshake or request.
    let (_connection, _) = listener.accept().await.unwrap();
    tokio::time::advance(TRANSFER_PROBE_TIMEOUT).await;
    assert!(matches!(probe.await.unwrap(), Err(JobError::Retriable(_))));
    assert!(
        !cancellation.is_cancelled(),
        "a failed observation must not cancel durable transfer work"
    );
}

#[test]
fn refund_interrupted_running_attempts_only_touches_running_snapshot_upload_jobs() {
    let conn = crate::db::Connection::open_in_memory();
    conn.execute_batch(
        "CREATE TABLE jobs (
             job_id          TEXT PRIMARY KEY,
             kind            TEXT NOT NULL,
             status          TEXT NOT NULL DEFAULT 'pending',
             payload         TEXT NOT NULL DEFAULT '',
             subject_id      TEXT,
             dedup_key       TEXT,
             error_kind      TEXT,
             attempts        INTEGER NOT NULL DEFAULT 0,
             max_attempts    INTEGER NOT NULL DEFAULT 5,
             bytes_processed INTEGER NOT NULL DEFAULT 0,
             error           TEXT,
             created_at      TEXT NOT NULL DEFAULT (datetime('now')),
             started_at      TEXT,
             completed_at    TEXT,
             next_attempt_at TEXT
         );",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, attempts) VALUES
         ('snapshot-running', 'snapshot_upload', 'running', 3),
         ('snapshot-pending', 'snapshot_upload', 'pending', 3),
         ('other-running', 'future_job', 'running', 3),
         ('snapshot-zero', 'snapshot_upload', 'running', 0)",
        [],
    )
    .unwrap();

    let refunded = refund_interrupted_running_attempts(&conn).unwrap();

    assert_eq!(refunded, 1);
    let attempts = |job_id: &str| -> i32 {
        conn.query_row(
            "SELECT attempts FROM jobs WHERE job_id = ?1",
            crate::db::fixture::params![job_id],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(attempts("snapshot-running"), 2);
    assert_eq!(attempts("snapshot-pending"), 3);
    assert_eq!(attempts("other-running"), 3);
    assert_eq!(attempts("snapshot-zero"), 0);
}

#[tokio::test]
async fn executor_rejects_non_snapshot_upload_job_kind() {
    let executor = SnapshotUploadExecutor {
        db: crate::db::SharedConnection::new(crate::db::Connection::open_in_memory()),
        drain_state: std::sync::Arc::new(crate::service::drain::DrainState::default()),
        node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
        volume_manager_socket: absent_volume_manager_socket(),
        retained_source_inspector: std::sync::Arc::new(crate::retained_source::FakeInspector::new(
            crate::retained_source::RetainedSourceCheck::Absent,
        )),
    };
    let job = job_fixture("future_sbx_mgr_job", "{}".to_string());

    let err = executor
        .execute(&job, tokio_util::sync::CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(
        err.kind(),
        pplx_space_common_jobs::JobFailureKind::Permanent
    );
    assert_eq!(
        err.message(),
        "unsupported snapshot-upload worker job kind: future_sbx_mgr_job"
    );
}

#[tokio::test]
async fn executor_requires_begin_before_volume_staging() {
    let endpoint_name = pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV;
    if !pplx_space_test_env::enter_test_process(&[(endpoint_name, None)]) {
        return;
    }
    let executor = SnapshotUploadExecutor {
        db: crate::db::SharedConnection::new(crate::db::Connection::open_in_memory()),
        drain_state: std::sync::Arc::new(crate::service::drain::DrainState::default()),
        node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
        volume_manager_socket: absent_volume_manager_socket(),
        retained_source_inspector: std::sync::Arc::new(crate::retained_source::FakeInspector::new(
            crate::retained_source::RetainedSourceCheck::Absent,
        )),
    };
    let artifact = "01939e3e-eeee-7000-8000-000000000022";
    let mut payload = reclaim_fixture_payload(artifact, "/space/cache/snapshot".into());
    payload.meta.execution = Some(
        pplx_space_common_core::types::SnapshotExecutionMetadata::new(
            pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
                std::env::consts::ARCH.parse().unwrap(),
                vec![],
            ),
        )
        .unwrap(),
    );
    let job = job_fixture(
        SNAPSHOT_UPLOAD_JOB_KIND,
        serde_json::to_string(&payload).unwrap(),
    );

    let result = executor
        .execute(&job, tokio_util::sync::CancellationToken::new())
        .await;

    let error = result.expect_err("missing ClusterApi must reject before volume staging");
    assert!(error.message().contains("cluster-api not configured"));
    assert!(!error.message().contains("vol-mgr"));
}

#[test]
fn sqlite_duration_separates_queue_and_transfer_timestamps() {
    // Arrange
    let created_at = "2026-08-14 12:00:00";
    let started_at = "2026-08-14 12:00:07";
    let completed_at = "2026-08-14 12:00:37";

    // Act
    let queue_ms = super::sqlite_duration_ms(created_at, started_at);
    let transfer_ms = super::sqlite_duration_ms(started_at, completed_at);

    // Assert
    assert_eq!(queue_ms, Some(7_000.0));
    assert_eq!(transfer_ms, Some(30_000.0));
}

#[test]
fn encrypted_delivery_metrics_follow_transfer_submission_order() {
    // Arrange
    let mut meta = reclaim_fixture_meta(None, None, None, None);
    meta.encrypted = true;

    // Act
    let artifact_tags = crate::snapshot_upload::upload_artifacts_for_meta(&meta)
        .iter()
        .map(|artifact| artifact.metric_tag())
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(artifact_tags, ["snapshot.luks", "cow.luks", "metadata"]);
}

#[test]
fn finalise_total_excludes_metadata_for_new_payloads() {
    // New payloads poll four jobs: three VM artifacts + the trailing
    // metadata document. The finalisation total (persisted as
    // snapshots.size_bytes, "total compressed bytes of all VM
    // artifacts") must sum only the three VM jobs and drop the
    // metadata bytes.
    let polled = [10u64, 20, 30, 5]; // vmstate, memory, rootfs, metadata
    assert_eq!(vm_artifact_total_bytes(&polled, 3), 60);
}

#[test]
fn finalise_total_is_all_jobs_for_legacy_payloads() {
    // Legacy payloads poll only the three VM artifact jobs; metadata
    // is uploaded inline and excluded from the total by construction
    // (it is never in `polled`). The same `take(vm_artifact_count)`
    // therefore sums every polled job.
    let polled = [10u64, 20, 30];
    assert_eq!(vm_artifact_total_bytes(&polled, 3), 60);
}

#[test]
fn finalise_total_saturates_instead_of_overflowing() {
    let polled = [u64::MAX, 1];
    assert_eq!(vm_artifact_total_bytes(&polled, 2), u64::MAX);
}

fn open_backfill_test_db() -> crate::db::Connection {
    crate::db::test_conn()
}

#[test]
fn ready_backup_keeps_completed_evidence_when_bookkeeping_misses_execution_deadline() {
    #[derive(Clone)]
    struct ReadyBackupExecutor {
        executor: SnapshotUploadExecutor,
        phase: std::sync::Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl JobExecutor for ReadyBackupExecutor {
        async fn execute(
            &self,
            job: &Job,
            _cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<i64, JobError> {
            let payload = job.payload_as()?;
            // Enter the production post-READY boundary near the final attempt's
            // deadline, without depending on external upload/finalize servers.
            self.phase.notify_one();
            tokio::time::sleep(Duration::from_secs(4)).await;
            self.phase.notify_one();
            Ok(self.executor.complete_ready_backup(&payload, 42))
        }
    }

    let artifact = "01939e3e-aaaa-7000-8000-000000000003";
    let conn = open_backfill_test_db();
    insert_artifact(&conn, artifact, None);
    let db = crate::db::SharedConnection::new(conn);
    let payload = reclaim_fixture_payload(artifact, "/unused".into());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (job, marker_missing, pruned, retained) = runtime.block_on(async {
        // Occupy the blocking pool while leaving SQLite available for the
        // worker's durable outcome. Bookkeeping cannot start before release.
        let (release, blocked) = std::sync::mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = blocked.recv();
        });
        ready.await.unwrap();
        let phase = std::sync::Arc::new(tokio::sync::Notify::new());
        let worker = db.background_worker(
            ReadyBackupExecutor {
                executor: SnapshotUploadExecutor {
                    db: db.clone(),
                    drain_state: std::sync::Arc::new(crate::service::drain::DrainState::default()),
                    node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
                    volume_manager_socket: absent_volume_manager_socket(),
                    retained_source_inspector: std::sync::Arc::new(
                        crate::retained_source::FakeInspector::new(
                            crate::retained_source::RetainedSourceCheck::Absent,
                        ),
                    ),
                },
                phase: std::sync::Arc::clone(&phase),
            },
            WorkerConfig::new(1, Duration::from_secs(5), 1),
        );
        let id = worker
            .submit(
                SNAPSHOT_UPLOAD_JOB_KIND,
                &serde_json::to_string(&payload).unwrap(),
                None,
                None,
            )
            .unwrap();
        phase.notified().await;
        tokio::time::advance(Duration::from_secs(4)).await;
        phase.notified().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        let job = worker
            .wait_for_job(&id, Duration::from_secs(10))
            .await
            .unwrap();
        let evidence = {
            let conn = db.lock().unwrap();
            let marker_missing = backup_marker(&conn, artifact).is_none();
            conn.execute(
                "UPDATE jobs SET completed_at = '2000-01-01 00:00:00' WHERE job_id = ?1",
                [&id],
            )
            .unwrap();
            let pruned = pplx_space_common_jobs::db::prune_terminal_jobs(
                conn.raw(),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap();
            let retained = pplx_space_common_jobs::db::get_job(conn.raw(), &id)
                .unwrap()
                .is_some();
            (job, marker_missing, pruned, retained)
        };
        release.send(()).unwrap();
        blocker.await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while backup_marker(&db.lock().unwrap(), artifact).is_none() && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        evidence
    });
    // Runtime teardown joins the started writer and guarantees cleanup before
    // any assertion can fail; the readiness wait lets queued work start first.
    drop(runtime);
    assert_eq!(job.attempts, 1);
    assert_eq!(job.status, "completed");
    assert_eq!(job.bytes_processed, 42);
    assert!(marker_missing);
    assert_eq!(pruned, 0);
    assert!(
        retained,
        "completed job must protect missing-marker evidence"
    );
    assert!(backup_marker(&db.lock().unwrap(), artifact).is_some());
}

#[tokio::test(start_paused = true)]
async fn maintenance_retries_failed_pruning_on_the_next_tick() {
    let conn = open_backfill_test_db();
    insert_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000002",
        "completed",
        &payload_for("absent"),
    );
    conn.execute_batch(
        "UPDATE jobs SET completed_at = '2000-01-01 00:00:00';
         CREATE TABLE prune_attempts (attempt INTEGER);
         CREATE TRIGGER fail_prune BEFORE DELETE ON jobs BEGIN
             INSERT INTO prune_attempts VALUES (1);
             SELECT RAISE(FAIL, 'injected pruning failure');
         END;",
    )
    .unwrap();
    let db = crate::db::SharedConnection::new(conn);
    let drain = std::sync::Arc::new(crate::service::drain::DrainState::default());
    let worker = build_worker(
        db.clone(),
        std::sync::Arc::clone(&drain),
        crate::node_manager_transport::fake::unbound_in_absent_dir(),
        absent_volume_manager_socket(),
    );
    let shutdown = tokio_util::sync::CancellationToken::new();
    let maintenance = tokio::spawn(run_maintenance(db.clone(), worker, drain, shutdown.clone()));

    // RAISE(FAIL) preserves the trigger's attempt witness, but not the delete.
    // Yield without advancing virtual time while the blocking pass finishes.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let attempts: i64 = db
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM prune_attempts", [], |row| row.get(0))
            .unwrap();
        if attempts > 0 {
            break;
        }
        assert!(Instant::now() < deadline, "first prune was not attempted");
        tokio::task::yield_now().await;
    }
    db.lock()
        .unwrap()
        .execute_batch("DROP TRIGGER fail_prune")
        .unwrap();
    tokio::time::advance(Duration::from_secs(60)).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    let deleted = loop {
        if pplx_space_common_jobs::db::get_job(
            db.lock().unwrap().raw(),
            &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap(),
        )
        .unwrap()
        .is_none()
        {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::task::yield_now().await;
    };
    shutdown.cancel();
    maintenance.await.unwrap();
    assert!(
        deleted,
        "a failed prune must retry next tick, not next hour"
    );
}

#[test]
fn pruning_retains_failed_backup_marker_evidence_until_repaired() {
    let conn = open_backfill_test_db();
    let artifact = "01939e3e-aaaa-7000-8000-000000000001";
    let without_evidence = "01939e3e-aaaa-7000-8000-000000000002";
    insert_artifact(&conn, artifact, None);
    insert_artifact(&conn, without_evidence, None);
    let payload = reclaim_fixture_payload(artifact, "/unused".into());
    conn.execute_batch(
        "CREATE TRIGGER fail_backup_marker BEFORE UPDATE OF backup_completed_at
         ON local_artifacts BEGIN SELECT RAISE(FAIL, 'injected marker failure'); END;",
    )
    .unwrap();
    // The remote finalize has succeeded, but its local bookkeeping fails.
    assert!(stamp_backup_completed_and_reclaim_stream(&conn, &payload, "attempt", None).is_err());
    insert_job(&conn, "completed", "completed", &payload_for(artifact));
    insert_job(&conn, "malformed", "completed", "not-json");
    insert_job(&conn, "legacy", "completed", r#"{"meta":{}}"#);
    insert_job(&conn, "gone", "completed", &payload_for("absent"));
    conn.execute("UPDATE jobs SET completed_at = '2000-01-01 00:00:00'", [])
        .unwrap();
    let prune = || {
        pplx_space_common_jobs::db::prune_terminal_jobs(
            conn.raw(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap()
    };
    assert_eq!(
        prune(),
        1,
        "only evidence for an absent artifact is disposable"
    );
    assert_eq!(
        backfill_backup_completed_markers(&conn, &mut BackupMarkerScan::default())
            .unwrap()
            .marker_write_failed,
        1
    );
    assert_eq!(
        prune(),
        0,
        "failed repair must not make evidence disposable"
    );
    conn.execute_batch("DROP TRIGGER fail_backup_marker")
        .unwrap();
    assert_eq!(
        backfill_backup_completed_markers(&conn, &mut BackupMarkerScan::default())
            .unwrap()
            .stamped,
        1
    );
    assert_eq!(
        prune(),
        1,
        "repair releases the completed job for ordinary retention"
    );
    assert!(
        backup_marker(&conn, without_evidence).is_none(),
        "missing historical evidence never proves remote durability"
    );
}

#[test]
fn backup_marker_repair_bounds_discovery_and_advances_past_retained_evidence() {
    let conn = open_backfill_test_db();
    let artifact = "01939e3e-aaaa-7000-8000-000000000001";
    let inserted_later = "01939e3e-aaaa-7000-8000-000000000002";
    insert_artifact(&conn, artifact, None);
    insert_artifact(&conn, inserted_later, None);
    for index in 0..BACKUP_MARKER_BATCH {
        insert_job(
            &conn,
            &format!("malformed-{index}"),
            "completed",
            "not-json",
        );
    }
    insert_job(&conn, "repair", "completed", &payload_for(artifact));
    let mut scan = BackupMarkerScan::default();
    let first = backfill_backup_completed_markers(&conn, &mut scan).unwrap();
    assert_eq!(first.examined, BACKUP_MARKER_BATCH);
    assert!(backup_marker(&conn, artifact).is_none());
    insert_job(
        &conn,
        "inserted-later",
        "completed",
        &payload_for(inserted_later),
    );
    let second = backfill_backup_completed_markers(&conn, &mut scan).unwrap();
    assert_eq!(second.examined, 1);
    assert_eq!(second.stamped, 1);
    assert!(
        backup_marker(&conn, inserted_later).is_none(),
        "fresh inserts cannot extend the active sweep"
    );
    backfill_backup_completed_markers(&conn, &mut scan).unwrap();
    assert_eq!(
        backfill_backup_completed_markers(&conn, &mut scan)
            .unwrap()
            .stamped,
        1
    );
    assert!(backup_marker(&conn, inserted_later).is_some());
}

fn insert_artifact(conn: &crate::db::Connection, id: &str, backup_completed_at: Option<&str>) {
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at
         ) VALUES (?1, ?1, 'full', 'suspend', NULL, ?1, '0', ?2)",
        crate::db::fixture::params![id, backup_completed_at],
    )
    .unwrap();
}

fn insert_job(conn: &crate::db::Connection, job_id: &str, status: &str, payload: &str) {
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, payload, subject_id)
         VALUES (?1, ?2, ?3, ?4, 'some-sandbox-id')",
        crate::db::fixture::params![job_id, SNAPSHOT_UPLOAD_JOB_KIND, status, payload],
    )
    .unwrap();
}

fn payload_for(artifact_id: &str) -> String {
    format!(r#"{{"meta":{{"local_artifact_id":"{artifact_id}"}}}}"#)
}

fn reclaim_fixture_meta(
    rootfs_diff_format: Option<&str>,
    artifact_kind: Option<pplx_space_common_core::types::ArtifactKind>,
    artifact_origin: Option<pplx_space_common_core::types::ArtifactOrigin>,
    local_artifact_id: Option<LocalArtifactId>,
) -> SnapshotMeta {
    SnapshotMeta {
        version: 1,
        execution: None,
        sandbox_id: "sbx-reclaim".to_string(),
        size_bytes: 0,
        rootfs_size_bytes: None,
        created_at: "2026-06-11T00:00:00Z".to_string(),
        source: crate::snapshot_upload::SnapshotSource {
            runtime: "fc".to_string(),
            runtime_version: None,
            state_format_version: None,
            template_id: "seed".to_string(),
            content_hash: None,
            rootfs_size_mib: None,
            rootfs_version: None,
            kernel_version: None,
            kernel_digest: None,
            disk_io_max_operations_per_sec: None,
            disk_io_max_bandwidth_mib_per_sec: None,
        },
        sandbox: None,
        rootfs_diff_format: rootfs_diff_format.map(str::to_string),
        rootfs_send_source_id: None,
        encrypted: false,
        rootfs_encryption_source: None,
        local_artifact_id,
        artifact_kind,
        artifact_origin,
        artifact_snapshot_name: None,
    }
}

/// Reclaim is scoped to suspend-origin full artifacts whose rootfs
/// rode the btrfs-send codec and that carry the artifact identity
/// the gate proves against. Everything else — Full codec (keeps
/// `rootfs.ext4` for reupload/promotion), named snapshots, stop
/// disk artifacts, legacy payloads without an artifact id — is out
/// of scope.
#[test]
fn stream_reclaim_applies_only_to_pinned_btrfs_send_suspend_artifacts() {
    use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin, LocalArtifactId};
    let id = LocalArtifactId::new();
    let btrfs = Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1);

    assert!(stream_reclaim_applies(&reclaim_fixture_meta(
        btrfs,
        Some(ArtifactKind::Full),
        Some(ArtifactOrigin::Suspend),
        Some(id),
    )));

    // Full codec: out of scope.
    assert!(!stream_reclaim_applies(&reclaim_fixture_meta(
        None,
        Some(ArtifactKind::Full),
        Some(ArtifactOrigin::Suspend),
        Some(id),
    )));
    // Named snapshot origin: orthogonal to the suspend pin.
    assert!(!stream_reclaim_applies(&reclaim_fixture_meta(
        btrfs,
        Some(ArtifactKind::Full),
        Some(ArtifactOrigin::Snapshot),
        Some(id),
    )));
    // Stop-origin disk artifact: never carries a btrfs-send stream.
    assert!(!stream_reclaim_applies(&reclaim_fixture_meta(
        btrfs,
        Some(ArtifactKind::Disk),
        Some(ArtifactOrigin::Stop),
        Some(id),
    )));
    // Legacy payload without artifact identity: nothing to prove
    // the on-disk file against.
    assert!(!stream_reclaim_applies(&reclaim_fixture_meta(
        btrfs,
        Some(ArtifactKind::Full),
        Some(ArtifactOrigin::Suspend),
        None,
    )));
}

fn open_reclaim_test_db() -> crate::db::Connection {
    crate::db::test_conn()
}

fn insert_reclaim_sandbox(conn: &crate::db::Connection, id: &str, state: &str, pin: &str) {
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         ) VALUES (?1, 'uuid8', ?2, 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}', ?3, 'full', 'suspend')",
        crate::db::fixture::params![id, state, pin],
    )
    .unwrap();
}

fn write_snapshot_dir_fixture(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("vmstate"), vec![0u8; 4]).unwrap();
    std::fs::write(dir.join("memory"), vec![0u8; 8]).unwrap();
    std::fs::write(dir.join("snapshot.json"), "{}").unwrap();
    std::fs::write(dir.join(ROOTFS_BTRFS_SEND_FILENAME), vec![0u8; 16]).unwrap();
}

pub(crate) fn reclaim_fixture_payload(
    artifact_id: &str,
    snapshot_dir: std::path::PathBuf,
) -> SnapshotUploadJobs {
    SnapshotUploadJobs {
        meta: reclaim_fixture_meta(
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1),
            Some(pplx_space_common_core::types::ArtifactKind::Full),
            Some(pplx_space_common_core::types::ArtifactOrigin::Suspend),
            Some(artifact_id.parse().unwrap()),
        ),
        snapshot_id: "01939e3e-ffff-7000-8000-000000000001".to_string(),
        snapshot_dir,
        config: crate::snapshot_upload::SnapshotUploadConfig {
            bucket: "b".to_string(),
            region: "r".to_string(),
            key_prefix: String::new(),
            endpoint: None,
            path_style: false,
        },
        storage_uri: "s3://b/x".to_string(),
        job_ids: Vec::new(),
        metadata_staging_dir: None,
        includes_metadata_job: true,
        deferred_metadata: false,
        execution_generation: None,
        failure_scope: Default::default(),
    }
}

#[test]
fn submitted_transfer_ids_replace_the_durable_prepared_intent() {
    let conn = open_backfill_test_db();
    let artifact = "01939e3e-eeee-7000-8000-000000000020";
    let mut payload = reclaim_fixture_payload(artifact, "/space/cache/snapshot".into());
    let prepared_json = serde_json::to_string(&payload).unwrap();
    insert_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000003",
        "pending",
        &prepared_json,
    );
    insert_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000004",
        "running",
        &prepared_json,
    );
    payload.deferred_metadata = true;
    payload.meta.rootfs_send_source_id = Some(uuid::Uuid::now_v7());
    payload.job_ids = vec![JobId::new(), JobId::new(), JobId::new()];
    let db = crate::db::SharedConnection::new(conn);

    for job_id in [
        "019d84bd-280d-7331-99c6-000000000003",
        "019d84bd-280d-7331-99c6-000000000004",
    ] {
        persist_submitted_transfer_ids(&db, &job_id.parse().unwrap(), &payload).unwrap();

        let persisted: String = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT payload FROM jobs WHERE job_id = ?1",
                [job_id],
                |row| row.get(0),
            )
            .unwrap();
        let recovered: SnapshotUploadJobs = serde_json::from_str(&persisted).unwrap();
        assert_eq!(recovered.job_ids, payload.job_ids);
        assert!(recovered.deferred_metadata);
        assert_eq!(
            recovered.meta.rootfs_send_source_id,
            payload.meta.rootfs_send_source_id
        );
        // The second durable checkpoint includes metadata; a restart must
        // poll this handle instead of recreating the sidecar after teardown.
        let mut finalized = recovered;
        finalized.job_ids.push(JobId::new());
        persist_submitted_transfer_ids(&db, &job_id.parse().unwrap(), &finalized).unwrap();
        let saved: String = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT payload FROM jobs WHERE job_id = ?1",
                [job_id],
                |row| row.get(0),
            )
            .unwrap();
        let restarted: SnapshotUploadJobs = serde_json::from_str(&saved).unwrap();
        assert_eq!(restarted.job_ids, finalized.job_ids);
    }
}

#[test]
fn only_exact_prepared_intents_begin_in_the_worker() {
    let artifact = "01939e3e-eeee-7000-8000-000000000021";
    let mut prepared = reclaim_fixture_payload(artifact, "/space/cache/snapshot".into());
    prepared.meta.execution = Some(
        pplx_space_common_core::types::SnapshotExecutionMetadata::new(
            pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
                std::env::consts::ARCH.parse().unwrap(),
                vec![],
            ),
        )
        .unwrap(),
    );
    assert!(prepared_upload_requires_begin(&prepared));

    let mut legacy = prepared;
    legacy.meta.local_artifact_id = None;
    legacy.job_ids = vec![JobId::new()];
    assert!(!prepared_upload_requires_begin(&legacy));
}

fn backup_marker(conn: &crate::db::Connection, artifact_id: &str) -> Option<String> {
    conn.query_row(
        "SELECT backup_completed_at FROM local_artifacts WHERE local_artifact_id = ?1",
        crate::db::fixture::params![artifact_id],
        |r| r.get(0),
    )
    .unwrap()
}

/// The combined bookkeeping step stamps the durability marker and
/// deletes exactly the uploaded stream, nothing else: `memory`,
/// `vmstate`, and the metadata sidecar — what same-node wake and
/// the recovery sniffers read — survive.
#[test]
fn bookkeeping_stamps_marker_and_deletes_only_the_stream() {
    let conn = open_reclaim_test_db();
    let artifact = "01939e3e-eeee-7000-8000-000000000001";
    insert_artifact(&conn, artifact, None);
    insert_reclaim_sandbox(&conn, "sbx-reclaim", "suspended", artifact);
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    write_snapshot_dir_fixture(&snapshot_dir);
    let payload = reclaim_fixture_payload(artifact, snapshot_dir.clone());

    stamp_backup_completed_and_reclaim_stream(&conn, &payload, "backup-1", None).unwrap();

    assert!(backup_marker(&conn, artifact).is_some(), "marker stamped");
    assert!(!snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME).exists());
    assert!(snapshot_dir.join("memory").exists());
    assert!(snapshot_dir.join("vmstate").exists());
    assert!(snapshot_dir.join("snapshot.json").exists());
}

/// If the pin moved (a newer suspend published) the file at the
/// published path belongs to the newer artifact — whose backup may
/// not be durable yet — and must be kept. The marker for the
/// uploaded artifact still gets stamped: its backup IS durable.
#[test]
fn bookkeeping_keeps_the_stream_when_pin_moved() {
    let conn = open_reclaim_test_db();
    let uploaded = "01939e3e-eeee-7000-8000-000000000001";
    let newer = "01939e3e-eeee-7000-8000-000000000002";
    insert_artifact(&conn, uploaded, None);
    insert_reclaim_sandbox(&conn, "sbx-reclaim", "suspended", newer);
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    write_snapshot_dir_fixture(&snapshot_dir);
    let payload = reclaim_fixture_payload(uploaded, snapshot_dir.clone());

    stamp_backup_completed_and_reclaim_stream(&conn, &payload, "backup-1", None).unwrap();

    assert!(backup_marker(&conn, uploaded).is_some(), "marker stamped");
    assert!(snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME).exists());
}

#[test]
fn disk_operation_backup_stamps_its_durability_marker() {
    let conn = open_reclaim_test_db();
    let artifact = "01939e3e-eeee-7000-8000-000000000010";
    insert_artifact(&conn, artifact, None);
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    write_snapshot_dir_fixture(&snapshot_dir);
    let mut payload = reclaim_fixture_payload(artifact, snapshot_dir.clone());
    payload.meta.artifact_kind = Some(pplx_space_common_core::types::ArtifactKind::Disk);

    stamp_backup_completed_and_reclaim_stream(&conn, &payload, "fork-backup-1", None).unwrap();

    assert!(backup_marker(&conn, artifact).is_some(), "marker stamped");
    assert!(snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME).exists());
}

/// Any non-`suspended` state means a lifecycle operation owns the
/// sandbox (an in-flight suspend holds `suspending` until it
/// atomically publishes its new pin) — never delete under it, even
/// with a matching pin.
#[test]
fn bookkeeping_keeps_the_stream_when_not_suspended() {
    let conn = open_reclaim_test_db();
    let artifact = "01939e3e-eeee-7000-8000-000000000003";
    insert_artifact(&conn, artifact, None);
    insert_reclaim_sandbox(&conn, "sbx-reclaim", "suspending", artifact);
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    write_snapshot_dir_fixture(&snapshot_dir);
    let payload = reclaim_fixture_payload(artifact, snapshot_dir.clone());

    stamp_backup_completed_and_reclaim_stream(&conn, &payload, "backup-1", None).unwrap();

    assert!(snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME).exists());
}

/// A gate-passing reclaim whose stream is already gone (prior
/// reclaim, manual cleanup) is benign, not an error — and the
/// marker still gets stamped.
#[test]
fn bookkeeping_tolerates_an_absent_stream() {
    let conn = open_reclaim_test_db();
    let artifact = "01939e3e-eeee-7000-8000-000000000004";
    insert_artifact(&conn, artifact, None);
    insert_reclaim_sandbox(&conn, "sbx-reclaim", "suspended", artifact);
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    let payload = reclaim_fixture_payload(artifact, snapshot_dir);

    stamp_backup_completed_and_reclaim_stream(&conn, &payload, "backup-1", None).unwrap();

    assert!(backup_marker(&conn, artifact).is_some(), "marker stamped");
}

/// The backfill must repair the two known gaps — artifacts
/// uploaded before `backup_completed_at` existed, and artifacts
/// whose best-effort stamp failed — while counting every other
/// payload shape into its own bucket and never touching
/// non-completed jobs.
#[test]
fn backfill_stamps_unmarked_artifacts_and_buckets_the_rest() {
    let conn = open_backfill_test_db();
    let unmarked = "01939e3e-aaaa-7000-8000-000000000001";
    let marked = "01939e3e-aaaa-7000-8000-000000000002";
    let pending_only = "01939e3e-aaaa-7000-8000-000000000003";
    let row_gone = "01939e3e-aaaa-7000-8000-000000000004";
    insert_artifact(&conn, unmarked, None);
    insert_artifact(&conn, marked, Some("123"));
    insert_artifact(&conn, pending_only, None);

    insert_job(&conn, "j-unmarked", "completed", &payload_for(unmarked));
    insert_job(&conn, "j-marked", "completed", &payload_for(marked));
    insert_job(&conn, "j-row-gone", "completed", &payload_for(row_gone));
    insert_job(&conn, "j-legacy", "completed", r#"{"meta":{}}"#);
    insert_job(&conn, "j-bad", "completed", "not-json");
    insert_job(&conn, "j-pending", "pending", &payload_for(pending_only));

    let report =
        backfill_backup_completed_markers(&conn, &mut BackupMarkerScan::default()).unwrap();
    assert_eq!(report.stamped, 1);
    assert_eq!(report.already_stamped, 1);
    assert_eq!(report.artifact_row_missing, 1);
    assert_eq!(report.payload_missing_artifact_id, 1);
    assert_eq!(report.malformed_payload, 1);
    assert_eq!(report.total_scanned(), 5);

    let stamped_at: Option<String> = conn
        .query_row(
            "SELECT backup_completed_at FROM local_artifacts WHERE local_artifact_id = ?1",
            crate::db::fixture::params![unmarked],
            |r| r.get(0),
        )
        .unwrap();
    assert!(stamped_at.is_some(), "completed upload must be stamped");

    // The pre-existing marker is preserved, not overwritten.
    let preserved: String = conn
        .query_row(
            "SELECT backup_completed_at FROM local_artifacts WHERE local_artifact_id = ?1",
            crate::db::fixture::params![marked],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(preserved, "123");

    // Pending job's artifact stays unmarked.
    let untouched: Option<String> = conn
        .query_row(
            "SELECT backup_completed_at FROM local_artifacts WHERE local_artifact_id = ?1",
            crate::db::fixture::params![pending_only],
            |r| r.get(0),
        )
        .unwrap();
    assert!(untouched.is_none(), "pending upload must not be stamped");

    // Idempotent: a second run stamps nothing new.
    let second =
        backfill_backup_completed_markers(&conn, &mut BackupMarkerScan::default()).unwrap();
    assert_eq!(second.stamped, 0);
    assert_eq!(second.already_stamped, 2);
}

/// A retained artifact counts its stream until reclaim. Reclaim first switches
/// the sidecar to the retained image, then removes the stream; without the
/// image length it keeps both.
#[test]
fn bookkeeping_switches_a_retained_sidecar_to_the_image_before_reclaiming() {
    let conn = open_reclaim_test_db();
    let artifact = "01939e3e-eeee-7000-8000-000000000001";
    insert_artifact(&conn, artifact, None);
    insert_reclaim_sandbox(&conn, "sbx-reclaim", "suspended", artifact);
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    write_snapshot_dir_fixture(&snapshot_dir);
    let stream_len = std::fs::metadata(snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME))
        .unwrap()
        .len();
    let mut payload = reclaim_fixture_payload(artifact, snapshot_dir.clone());
    payload.meta.rootfs_send_source_id = Some(uuid::Uuid::now_v7());
    let vm = crate::snapshot_upload::vm_artifacts_for_format(SuspendSnapshotFormat::BtrfsSend);
    let local =
        crate::snapshot_upload::write_snapshot_metadata(&payload.meta, &snapshot_dir, &vm, None)
            .unwrap();
    assert!(local.size_bytes >= stream_len);
    assert_eq!(local.rootfs_size_bytes, Some(stream_len));

    let sidecar = || -> SnapshotMeta {
        serde_json::from_slice(
            &std::fs::read(snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME))
                .unwrap(),
        )
        .unwrap()
    };

    stamp_backup_completed_and_reclaim_stream(&conn, &payload, "backup-1", None).unwrap();
    assert!(backup_marker(&conn, artifact).is_some(), "marker stamped");
    assert!(snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME).exists());

    // An earlier reclaim rewrote the sidecar and was interrupted before it
    // unlinked the stream.
    rewrite_sidecar_for_reclaim(&snapshot_dir, 5000).unwrap();
    let image_counting = sidecar().size_bytes;
    assert_eq!(image_counting, local.size_bytes - stream_len + 5000);

    stamp_backup_completed_and_reclaim_stream(&conn, &payload, "backup-1", Some(5000)).unwrap();
    assert!(!snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME).exists());
    let rewritten = sidecar();
    assert_eq!(
        rewritten.rootfs_size_bytes, None,
        "the retained image is not a published send payload"
    );
    assert_eq!(
        rewritten.size_bytes, image_counting,
        "repeating the rewrite must not count the image twice"
    );
    assert_eq!(
        rewritten.rootfs_send_source_id,
        payload.meta.rootfs_send_source_id
    );
    // After the unlink the rewrite is a no-op as well.
    rewrite_sidecar_for_reclaim(&snapshot_dir, 5000).unwrap();
    assert_eq!(sidecar().size_bytes, image_counting);
}

#[test]
fn ready_backup_returns_before_the_retained_source_is_inspected() {
    struct GatedInspector {
        gate: std::sync::Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl crate::retained_source::RetainedSourceInspector for GatedInspector {
        async fn inspect(
            &self,
            _source: &crate::retained_source::RetainedSource,
        ) -> crate::retained_source::RetainedSourceCheck {
            self.gate.notified().await;
            crate::retained_source::RetainedSourceCheck::Present { rootfs_bytes: 5000 }
        }
    }

    let conn = open_reclaim_test_db();
    let artifact = "01939e3e-eeee-7000-8000-000000000002";
    insert_artifact(&conn, artifact, None);
    insert_reclaim_sandbox(&conn, "sbx-reclaim", "suspended", artifact);
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    write_snapshot_dir_fixture(&snapshot_dir);
    let stream = snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME);
    let mut payload = reclaim_fixture_payload(artifact, snapshot_dir.clone());
    payload.meta.rootfs_send_source_id = Some(uuid::Uuid::now_v7());
    let vm = crate::snapshot_upload::vm_artifacts_for_format(SuspendSnapshotFormat::BtrfsSend);
    crate::snapshot_upload::write_snapshot_metadata(&payload.meta, &snapshot_dir, &vm, None)
        .unwrap();
    let db = crate::db::SharedConnection::new(conn);
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let executor = SnapshotUploadExecutor {
        db: db.clone(),
        drain_state: std::sync::Arc::new(crate::service::drain::DrainState::default()),
        node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
        volume_manager_socket: absent_volume_manager_socket(),
        retained_source_inspector: std::sync::Arc::new(GatedInspector {
            gate: std::sync::Arc::clone(&gate),
        }),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        // READY completes without awaiting node-manager: the inspection a
        // reclaim needs belongs to bookkeeping, after the job's outcome.
        assert_eq!(executor.complete_ready_backup(&payload, 42), 42);
        // The marker is stamped by a blocking task that waits on nothing but
        // the database; the stream waits for the gated inspection.
        let deadline = Instant::now() + Duration::from_secs(10);
        while backup_marker(&db.lock().unwrap(), artifact).is_none() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            backup_marker(&db.lock().unwrap(), artifact).is_some(),
            "the durability marker never waits for the inspection"
        );
        assert!(stream.exists(), "an unanswered inspection keeps the stream");
        gate.notify_one();
        let deadline = Instant::now() + Duration::from_secs(10);
        while stream.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!stream.exists(), "the verified image lets reclaim proceed");
        assert!(backup_marker(&db.lock().unwrap(), artifact).is_some());
    });
    drop(runtime);
}

/// The capture host CPU recorded in the artifact's metadata is what the cluster
/// API persists for restore placement, so the Begin request must carry it
/// verbatim, and carry nothing when the capture host was unknown.
#[test]
fn begin_request_carries_the_recorded_capture_host_cpu() {
    use pplx_space_common_core::types::{
        Arch, ArtifactKind, ArtifactOrigin, ExecutionMetadata, HostCpu, SnapshotExecutionMetadata,
    };

    let execution = SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
        Arch::X86_64,
        vec![],
    ))
    .unwrap();
    for host_cpu in [None, Some("genuineintel-6-85-7")] {
        let execution = execution
            .clone()
            .with_host_cpu(host_cpu.map(|value| HostCpu::parse(value).unwrap()));
        let request = begin_artifact_backup_request(&BeginArtifactBackup {
            sandbox_id: "01939e3e-eeee-7000-8000-000000000030",
            local_artifact_id: "01939e3e-eeee-7000-8000-000000000031",
            artifact_kind: ArtifactKind::Full,
            artifact_origin: ArtifactOrigin::Suspend,
            snapshot_name: None,
            backup_attempt_id: "01939e3e-eeee-7000-8000-000000000032",
            storage_uri: "s3://space-test/suspend-snapshots/attempt?region=us-east-1",
            execution: &execution,
            execution_generation: None,
            fence_verdict_only: false,
        });
        assert_eq!(request.capture_host_cpu.as_deref(), host_cpu);
    }
}

/// The teardown probe asks for the fence's verdict without registering, and
/// only a reply that doesn't echo the request, from a server that predates
/// verdict-only requests, left a row for the probe to close.
#[test]
fn verdict_only_begin_is_requested_and_its_echo_read() {
    use pplx_space_common_core::types::{
        Arch, ArtifactKind, ArtifactOrigin, ExecutionMetadata, SnapshotExecutionMetadata,
    };

    let execution = SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
        Arch::X86_64,
        vec![],
    ))
    .unwrap();
    for fence_verdict_only in [false, true] {
        let request = begin_artifact_backup_request(&BeginArtifactBackup {
            sandbox_id: "01939e3e-eeee-7000-8000-000000000040",
            local_artifact_id: "01939e3e-eeee-7000-8000-000000000041",
            artifact_kind: ArtifactKind::Full,
            artifact_origin: ArtifactOrigin::Suspend,
            snapshot_name: None,
            backup_attempt_id: "01939e3e-eeee-7000-8000-000000000042",
            storage_uri: "s3://space-test/suspend-snapshots/attempt?region=us-east-1",
            execution: &execution,
            execution_generation: None,
            fence_verdict_only,
        });
        assert_eq!(request.fence_verdict_only, fence_verdict_only);
    }
    let reply =
        |fence_verdict_only| pplx_space_node_api_client::proto::BeginArtifactBackupResponse {
            fence_verdict_only,
            ..Default::default()
        };
    assert!(!verdict_only_reply_registered(&reply(true)));
    assert!(verdict_only_reply_registered(&reply(false)));
}

/// A volume-manager socket in a fresh directory that nothing creates.
fn absent_volume_manager_socket() -> std::path::PathBuf {
    std::env::temp_dir()
        .join(format!("space-sbx-mgr-test-{}", uuid::Uuid::now_v7()))
        .join("absent-volume-manager.sock")
}

/// A suspend capture's ready backup reports the suspend uncommitted exactly
/// when the node may still hold the runtime that capture was meant to release:
/// in a state that holds one outside a suspend, or suspending or suspended
/// without this capture pinned. A quarantined row counts as one that may hold
/// a runtime. A capture of any other origin, a missing row, and the cleanup
/// states keep the projection.
#[test]
fn suspend_uncommitted_follows_the_row_state_and_its_pin() {
    use crate::state::SandboxState;
    use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin};

    let capture = LocalArtifactId::new();
    let pinned = capture.to_string();
    let other = LocalArtifactId::new().to_string();
    let cases = [
        // Rolled back to `paused`, which restores the previous pin.
        (SandboxState::Paused, Some(other.as_str()), true),
        (SandboxState::Started, Some(pinned.as_str()), true),
        (SandboxState::Restoring, None, true),
        // A node-local suspend before its cleanup completes, or one begun
        // after a rollback: neither has pinned this capture.
        (SandboxState::Suspending, None, true),
        (SandboxState::Suspending, Some(other.as_str()), true),
        // A teardown suspend that staged this capture.
        (SandboxState::Suspending, Some(pinned.as_str()), false),
        (SandboxState::Suspended, Some(pinned.as_str()), false),
        // A later suspend committed another capture.
        (SandboxState::Suspended, Some(other.as_str()), true),
        (SandboxState::CleanupRequired, Some(other.as_str()), false),
        (SandboxState::CleanupFailed, None, false),
        (SandboxState::Quarantined, None, true),
        (SandboxState::Stopped, None, false),
    ];
    let conn = crate::db::test_conn();
    for (index, (state, pin, _)) in cases.iter().enumerate() {
        conn.execute(
            "INSERT INTO sandboxes (
                 id, uuid8, state, template, user, runtime, created_at, updated_at,
                 process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
                 current_local_artifact_id, current_local_artifact_kind,
                 current_local_artifact_origin
             ) VALUES (?1, 'uuid8', ?2, 'seed', 'u', 'fc', '0', '0',
                       '{}', '{}', '{}', ?3, ?4, ?5)",
            crate::db::fixture::params![
                format!("sbx-{index}"),
                state.as_str(),
                pin,
                pin.map(|_| "full"),
                pin.map(|_| "suspend"),
            ],
        )
        .unwrap();
    }
    let db = crate::db::SharedConnection::new(conn);
    let meta = |sandbox_id: &str, origin: ArtifactOrigin| SnapshotMeta {
        sandbox_id: sandbox_id.to_owned(),
        ..reclaim_fixture_meta(None, Some(ArtifactKind::Full), Some(origin), Some(capture))
    };

    let answers: Vec<_> = (0..cases.len())
        .map(|index| {
            suspend_uncommitted(&db, &meta(&format!("sbx-{index}"), ArtifactOrigin::Suspend))
                .unwrap()
        })
        .collect();
    let expected: Vec<_> = cases.iter().map(|(_, _, expected)| *expected).collect();
    assert_eq!(answers, expected, "{cases:?}");

    assert!(!suspend_uncommitted(&db, &meta("sbx-absent", ArtifactOrigin::Suspend)).unwrap());
    assert!(!suspend_uncommitted(&db, &meta("sbx-0", ArtifactOrigin::Stop)).unwrap());
}
