//! A teardown suspend whose backup the cluster refuses because it retired the
//! execution — a reaped orphaned restore pod's VM — releases the runtime and
//! discards the capture instead of leaving the pod terminating.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExecutionGeneration, JobId, LocalArtifactId, SandboxId,
};
use pplx_space_sbx_mgr_client::proto::{
    SuspendSnapshotFormat, TeardownSuspendAction, TeardownSuspendOutcome,
};

use super::{OrgSnapshotConfigResolution, TeardownSuspendBackup};
use crate::service::SandboxManagerService;
use crate::service::activation_support::internal_request;
use crate::service::tests::service_tests::{
    insert_stranded_teardown_suspend_sandbox, prepare_teardown_suspend_request,
    service_with_runtime_probe,
};
use crate::snapshot_upload_jobs::RegistrationProbe;
use crate::state::SandboxState;

const GENERATION: i64 = 7;

fn generation(value: i64) -> ExecutionGeneration {
    ExecutionGeneration::new(value).unwrap()
}

fn insert_upload_job(
    conn: &rusqlite::Connection,
    sandbox_id: &str,
    staged: LocalArtifactId,
    payload: &str,
) -> JobId {
    let job_id = JobId::new();
    pplx_space_common_jobs::db::insert_job(
        conn,
        pplx_space_common_jobs::db::InsertJob {
            job_id: &job_id,
            kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            payload,
            subject_id: Some(sandbox_id),
            dedup_key: Some(&staged.to_string()),
            max_attempts: 1,
            traceparent: None,
            priority: pplx_space_common_jobs::JobPriority::default(),
        },
    )
    .unwrap();
    job_id
}

/// The stranded row a refused teardown backup leaves: `suspending`, its
/// capture staged and never backed up, and the upload job permanently
/// failed. Returns the service with a recording cleanup dispatcher, the error
/// the drive failed with, and the staged artifact.
struct RefusedTeardown {
    service: SandboxManagerService,
    sandbox_id: SandboxId,
    staged: LocalArtifactId,
    error: tonic::Status,
    dispatched: Dispatched,
}

async fn refused_teardown() -> RefusedTeardown {
    refused_teardown_with(|_| "{}".to_string()).await
}

/// [`refused_teardown`] whose failed upload job carries the payload `payload`
/// builds for the staged artifact.
async fn refused_teardown_with(payload: impl FnOnce(LocalArtifactId) -> String) -> RefusedTeardown {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let job_id = insert_upload_job(&conn, &sandbox_id_string, staged, &payload(staged));
    pplx_space_common_jobs::db::fail_job_permanent(
        &conn,
        &job_id,
        "cluster-api rejected begin_artifact_backup (FailedPrecondition): execution retired",
        None,
    )
    .unwrap();
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    let (service, dispatched) = service_recording_cleanup(conn);

    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id: staged,
        sandbox_id: sandbox_id_string.clone(),
        artifact_kind: ArtifactKind::Full,
        artifact_origin: ArtifactOrigin::Suspend,
        snapshot_name: None,
        local_path: format!("/var/lib/space/sandboxes/{sandbox_id_string}/suspend"),
    };
    let error = service
        .ensure_teardown_suspend_backup(
            &row,
            TeardownSuspendBackup {
                execution_generation: generation(GENERATION),
                artifact: &artifact,
                format: SuspendSnapshotFormat::Full,
                origin: super::TeardownBackupOrigin::Captured,
            },
        )
        .await
        .expect_err("a refused backup cannot make the suspend durable");
    RefusedTeardown {
        service,
        sandbox_id,
        staged,
        error,
        dispatched,
    }
}

type Dispatched = Arc<std::sync::Mutex<Vec<(String, bool)>>>;

/// A service whose cleanup dispatcher records each runtime teardown.
fn service_recording_cleanup(conn: rusqlite::Connection) -> (SandboxManagerService, Dispatched) {
    let service = service_with_runtime_probe(conn, false);
    let dispatched = Dispatched::default();
    let observed = Arc::clone(&dispatched);
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
        observed.lock().unwrap().push((id, kill));
        Box::pin(async { Ok(()) })
    }));
    (service, dispatched)
}

async fn poll_outcome(
    service: &SandboxManagerService,
    sandbox_id: SandboxId,
) -> TeardownSuspendOutcome {
    let response = service
        .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
            sandbox_id.to_string(),
            GENERATION,
            TeardownSuspendAction::BeginOrJoin,
        )))
        .await
        .unwrap()
        .into_inner();
    TeardownSuspendOutcome::try_from(response.outcome).unwrap()
}

async fn drain(service: &SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
        .await
        .expect("cleanup tasks must drain");
}

/// Settling with the release the cluster's verdict selects (injected here;
/// `retired_execution_off_its_node_still_releases` drives the verdict itself)
/// while the sandbox row still exists. The runtime is torn down, the capture
/// never becomes a pending backup, and the shim's next poll releases the pod.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_refused_for_the_retired_generation_releases_the_teardown() {
    with_cluster_api(RETIRING, async || {
        let refused = refused_teardown().await;
        let service = &refused.service;
        let sandbox_id_string = refused.sandbox_id.to_string();

        let capture = crate::service::tests::service_tests::SandboxEventCapture::default();
        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        {
            use tracing_subscriber::layer::SubscriberExt;
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(capture.clone()),
            );
            tokio::time::timeout(
                Duration::from_secs(5),
                service.settle_teardown_suspend_driver_failure(
                    refused.sandbox_id,
                    generation(GENERATION),
                    Err(refused.error.clone()),
                    None,
                    Some(refused.staged),
                ),
            )
            .await
            .expect("the release should complete")
            .expect("a retired execution's teardown must be released");
        }
        let emitted = counters.finish();
        assert!(
            emitted
                .iter()
                .any(|counter| counter.name == "sandbox.evict.evicted"
                    && counter
                        .tags
                        .iter()
                        .any(|tag| tag == "reason:execution_retired")),
            "the release counts as an execution_retired eviction: {emitted:?}"
        );
        let events = capture.events.lock().unwrap().clone();
        assert!(
            events.iter().any(|event| {
                event.get("event_type").map(String::as_str) == Some("local_eviction_completed")
                    && event.get("reason").map(String::as_str) == Some("execution_retired")
                    && event.get("sandbox_id") == Some(&sandbox_id_string)
            }),
            "the release emits its local_eviction_completed event: {events:?}"
        );

        assert_eq!(
            *refused.dispatched.lock().unwrap(),
            vec![(sandbox_id_string.clone(), false)],
            "the resident runtime must be torn down, not just forgotten"
        );
        {
            let db = service.db.lock().unwrap();
            assert!(
                crate::db::get_sandbox(&db, &sandbox_id_string)
                    .unwrap()
                    .is_none()
            );
        }
        let pending = service.local_artifacts().list_pending_backups().unwrap();
        assert!(
            pending
                .iter()
                .all(|backup| backup.local_artifact_id != refused.staged),
            "the discarded capture must not block a drain as a pending backup: {pending:?}"
        );
        assert_eq!(
            poll_outcome(service, refused.sandbox_id).await,
            TeardownSuspendOutcome::Deleted,
            "the shim must be released instead of polling forever"
        );
        drain(service).await;
    })
    .await;
}

/// Without a refusal for this execution the failure takes the existing
/// handling: the runtime stays and the row keeps waiting for its backup.
#[tokio::test]
async fn backup_refusal_without_a_release_verdict_releases_nothing() {
    let refused = refused_teardown().await;
    let service = &refused.service;
    let result = service
        .settle_teardown_suspend_driver_failure(
            refused.sandbox_id,
            generation(GENERATION),
            Err(refused.error.clone()),
            Some(&OrgSnapshotConfigResolution::Absent),
            None,
        )
        .await;
    assert!(result.is_err(), "the drive must stay failed");
    assert!(
        refused.dispatched.lock().unwrap().is_empty(),
        "the runtime must not be torn down"
    );
    let row = {
        let db = service.db.lock().unwrap();
        crate::db::get_sandbox(&db, &refused.sandbox_id.to_string())
            .unwrap()
            .unwrap()
    };
    assert_eq!(row.state, SandboxState::Suspending);
    drain(service).await;
}

/// A release for a row that moved on changes nothing and doesn't ask the
/// cluster whether the sandbox is restorable, so no hold is counted; the
/// drive's failure takes the ordinary handling: a quarantined row's teardown
/// job is closed rather than left running.
#[tokio::test]
async fn release_for_a_row_that_moved_on_records_the_drive_failure() {
    let refused = refused_teardown().await;
    let service = &refused.service;
    let sandbox_id_string = refused.sandbox_id.to_string();
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'quarantined' WHERE id = ?1",
            rusqlite::params![sandbox_id_string],
        )
        .unwrap();

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let result = service
        .settle_teardown_suspend_driver_failure(
            refused.sandbox_id,
            generation(GENERATION),
            Err(refused.error.clone()),
            Some(&OrgSnapshotConfigResolution::Absent),
            Some(refused.staged),
        )
        .await;
    let emitted = counters.finish();
    assert!(result.is_err(), "the drive must stay failed");
    assert!(refused.dispatched.lock().unwrap().is_empty());
    assert!(
        !emitted
            .iter()
            .any(|counter| counter.name == "sandbox.evict.skipped"),
        "a row that moved on isn't a held release: {emitted:?}"
    );
    let status = {
        let db = service.db.lock().unwrap();
        crate::db::latest_teardown_suspend_operation(&db, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .status
    };
    assert_eq!(status, crate::db::TeardownSuspendOperationStatus::Failed);
    drain(service).await;
}

/// A release held back by another artifact's pending backup is counted once,
/// as that hold, before the cluster is asked whether the sandbox is restorable
/// (the stub would answer no), and nothing is torn down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_release_counts_a_skip() {
    let no_backed_copy = RetiringClusterApi {
        backed_copy: false,
        ..RETIRING
    };
    with_cluster_api(no_backed_copy, async || {
        let refused = refused_teardown().await;
        let service = &refused.service;
        let sandbox_id_string = refused.sandbox_id.to_string();
        service
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO local_artifacts (
                     local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                     snapshot_name, local_path, created_at
                 ) VALUES (?1, ?2, 'full', 'snapshot', 'checkpoint', ?3, '0')",
                rusqlite::params![
                    LocalArtifactId::new().to_string(),
                    sandbox_id_string,
                    format!("/var/lib/space/sandboxes/{sandbox_id_string}/snapshots/checkpoint"),
                ],
            )
            .unwrap();

        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        let result = service
            .settle_teardown_suspend_driver_failure(
                refused.sandbox_id,
                generation(GENERATION),
                Err(refused.error.clone()),
                Some(&OrgSnapshotConfigResolution::Absent),
                Some(refused.staged),
            )
            .await;
        let emitted = counters.finish();
        assert!(result.is_err(), "a held release leaves the drive failed");
        assert!(refused.dispatched.lock().unwrap().is_empty());
        let skips: Vec<_> = emitted
            .iter()
            .filter(|counter| counter.name == "sandbox.evict.skipped")
            .collect();
        assert_eq!(skips.len(), 1, "one hold, counted once: {emitted:?}");
        assert!(
            skips[0]
                .tags
                .iter()
                .any(|tag| tag == "reason:local_artifact_awaiting_backup"),
            "the hold is counted as the pending artifact: {emitted:?}"
        );
        assert!(
            !emitted
                .iter()
                .any(|counter| counter.name == "sandbox.evict.evicted"),
            "nothing was evicted: {emitted:?}"
        );
        drain(service).await;
    })
    .await;
}

/// Without a ready snapshot of the sandbox to restore from, the refused capture
/// may be its only copy: the release is held, nothing is torn down or deleted,
/// the teardown stays running, and the capture still can't register.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_without_a_backed_copy_is_held() {
    with_cluster_api(
        RetiringClusterApi {
            backed_copy: false,
            ..RETIRING
        },
        async || {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let sandbox_id_string = refused.sandbox_id.to_string();
            let counters = pplx_space_metrics::test_capture::capture_counters_guard();
            let result = service
                .settle_teardown_suspend_drive(
                    refused.sandbox_id,
                    generation(GENERATION),
                    Err(refused.error.clone()),
                )
                .await;
            let emitted = counters.finish();
            assert!(result.is_err(), "a held release leaves the teardown failed");
            assert!(
                refused.dispatched.lock().unwrap().is_empty(),
                "the runtime stays"
            );
            assert!(
                emitted
                    .iter()
                    .any(|counter| counter.name == "sandbox.evict.skipped"
                        && counter
                            .tags
                            .iter()
                            .any(|tag| tag == "reason:no_backed_copy")),
                "the hold is counted: {emitted:?}"
            );
            assert!(
                !emitted
                    .iter()
                    .any(|counter| counter.name == "sandbox.evict.evicted"),
                "nothing was evicted: {emitted:?}"
            );
            {
                let db = service.db.lock().unwrap();
                let row = crate::db::get_sandbox(&db, &sandbox_id_string)
                    .unwrap()
                    .unwrap();
                assert_eq!(row.state, SandboxState::Suspending);
                assert_eq!(row.current_local_artifact_id, Some(refused.staged));
                assert!(
                    crate::db::get_local_artifact(&db, refused.staged)
                        .unwrap()
                        .is_some(),
                    "the capture is kept"
                );
                assert_eq!(
                    crate::db::latest_teardown_suspend_operation(&db, &sandbox_id_string)
                        .unwrap()
                        .unwrap()
                        .status,
                    crate::db::TeardownSuspendOperationStatus::Running,
                    "the next drive asks again"
                );
            }
            assert_eq!(
                crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(
                    &bound_upload_payload(refused.staged)
                )
                .await,
                RegistrationProbe::Superseded(generation(GENERATION)),
                "the held capture's upload stays bound to its generation"
            );
            drain(service).await;
        },
    )
    .await;
}

/// A winning claim can't stop an upload job that's already running, so it marks
/// the job `cancelling`, which the worker reads as cancelled rather than
/// completed once the job returns.
#[test]
fn release_claim_marks_a_running_upload_to_finish_cancelled() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let job_id = insert_upload_job(&conn, &sandbox_id_string, staged, "{}");
    assert!(pplx_space_common_jobs::db::mark_job_running(&conn, &job_id).unwrap());

    assert!(
        crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap()
    );
    let job = pplx_space_common_jobs::db::get_job(&conn, &job_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        job.status,
        pplx_space_common_jobs::JobStatus::Cancelling.as_str(),
        "a running upload is told to stop, not marked done"
    );
}

/// The claim re-asserts the execution the refusal was about: a different
/// generation or a re-staged artifact means the row moved on, and nothing
/// changes.
#[test]
fn release_claim_refuses_a_row_that_moved_on() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));

    for (claimed_generation, claimed_artifact) in [
        (generation(GENERATION + 1), staged),
        (generation(GENERATION - 1), staged),
        (generation(GENERATION), LocalArtifactId::new()),
    ] {
        assert!(
            !crate::db::claim_retired_teardown_execution_for_release(
                &conn,
                &sandbox_id_string,
                claimed_generation,
                claimed_artifact,
            )
            .unwrap()
        );
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Suspending);
        assert_eq!(
            crate::db::latest_teardown_suspend_operation(&conn, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Running
        );
    }

    assert!(
        crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap()
    );
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::Deleting);
    assert_ne!(
        crate::db::latest_teardown_suspend_operation(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .status,
        crate::db::TeardownSuspendOperationStatus::Running,
        "the claim closes the teardown job it took over"
    );
}

/// The probe re-sends only a terminal upload bound to the generation being
/// driven whose registration was never admitted: a live job still owns its
/// attempt, and a payload persisted before the fence, one bound to another
/// execution, or one that already has transfers carries no verdict about this
/// execution.
#[tokio::test]
async fn only_an_upload_bound_to_the_generation_is_probed() {
    let staged_id = "01939e3e-0000-7000-8000-0000000000aa";
    let staged: LocalArtifactId = staged_id.parse().unwrap();
    let mut payload = crate::snapshot_upload_jobs::tests::reclaim_fixture_payload(
        staged_id,
        std::path::PathBuf::from("/var/lib/space/sandboxes/s/suspend"),
    );
    let sandbox_id = payload.meta.sandbox_id.clone();

    for (bound_to, admitted, probed) in [
        (Some(generation(GENERATION)), false, true),
        (Some(generation(GENERATION)), true, false),
        (Some(generation(GENERATION + 1)), false, false),
        (None, false, false),
    ] {
        payload.execution_generation = bound_to;
        payload.job_ids = if admitted {
            vec![JobId::new()]
        } else {
            Vec::new()
        };
        let conn = crate::db::test_conn();
        let job_id = insert_upload_job(
            &conn,
            &sandbox_id,
            staged,
            &serde_json::to_string(&payload).unwrap(),
        );
        let service = SandboxManagerService::for_test(conn);
        // Only a payload that would be probed once terminal shows that
        // liveness alone holds the probe back.
        if probed {
            assert!(
                service
                    .teardown_backup_payload_bound_to(generation(GENERATION), staged)
                    .is_none(),
                "a live job still owns its attempt"
            );
        }
        pplx_space_common_jobs::db::fail_job_permanent(
            &service.db.lock().unwrap(),
            &job_id,
            "refused",
            None,
        )
        .unwrap();
        assert_eq!(
            service
                .teardown_backup_payload_bound_to(generation(GENERATION), staged)
                .is_some(),
            probed,
            "upload bound to {bound_to:?}, admitted {admitted}"
        );
    }

    let service = SandboxManagerService::for_test(crate::db::test_conn());
    assert!(
        service
            .teardown_backup_payload_bound_to(generation(GENERATION), staged)
            .is_none(),
        "no upload job, no probe"
    );
}

/// The release deletes every local artifact of the sandbox, so it waits while
/// another one is still awaiting its backup: those bytes may be the only copy.
#[test]
fn release_claim_waits_for_other_local_artifacts_to_back_up() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let staged =
        insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
    let named = LocalArtifactId::new();
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at
         ) VALUES (?1, ?2, 'full', 'snapshot', 'checkpoint', ?3, '0')",
        rusqlite::params![
            named.to_string(),
            sandbox_id_string,
            format!("/var/lib/space/sandboxes/{sandbox_id_string}/snapshots/checkpoint"),
        ],
    )
    .unwrap();

    assert!(
        !crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap(),
        "an unbacked named snapshot must keep the row"
    );
    assert!(
        crate::db::other_local_artifact_awaits_backup(&conn, &sandbox_id_string, staged).unwrap(),
        "the hold is observable"
    );
    let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::Suspending);

    crate::db::mark_local_artifact_backup_completed(&conn, &named.to_string()).unwrap();
    assert!(
        !crate::db::other_local_artifact_awaits_backup(&conn, &sandbox_id_string, staged).unwrap()
    );
    assert!(
        crate::db::claim_retired_teardown_execution_for_release(
            &conn,
            &sandbox_id_string,
            generation(GENERATION),
            staged,
        )
        .unwrap()
    );
}

/// Only this execution's own capture is fenced. A suspended row the teardown
/// merely binds registers its own attempt unfenced, since a refusal there
/// could only strand the pod.
#[test]
fn only_a_captured_teardown_artifact_carries_its_generation() {
    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id: LocalArtifactId::new(),
        sandbox_id: SandboxId::new().to_string(),
        artifact_kind: ArtifactKind::Full,
        artifact_origin: ArtifactOrigin::Suspend,
        snapshot_name: None,
        local_path: "/var/lib/space/sandboxes/s/suspend".to_string(),
    };
    let backup = |origin| TeardownSuspendBackup {
        execution_generation: generation(GENERATION),
        artifact: &artifact,
        format: SuspendSnapshotFormat::Full,
        origin,
    };
    assert_eq!(
        backup(super::TeardownBackupOrigin::Captured).registration_generation(),
        Some(generation(GENERATION))
    );
    assert_eq!(
        backup(super::TeardownBackupOrigin::PreviouslySuspended).registration_generation(),
        None
    );
}

/// A node-local suspend registers under the Kubernetes execution its runtime
/// belongs to, so an orphaned restore pod's VM can't publish by idling into
/// auto-suspend. An older route identity that names its runtime only by pod
/// UID is Kubernetes too. A runtime with no Kubernetes execution registers
/// unfenced, even when its route identity carries a generation.
#[test]
fn node_local_suspend_registers_under_its_kubernetes_execution() {
    let conn = crate::db::test_conn();
    let k8s_sandbox = SandboxId::new();
    insert_stranded_teardown_suspend_sandbox(&conn, k8s_sandbox, generation(GENERATION));
    let row = crate::db::get_sandbox(&conn, &k8s_sandbox.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(
        super::node_local_registration_generation(&row),
        Some(generation(GENERATION))
    );

    let mut pod_uid_only = row.clone();
    pod_uid_only.space_api_route_identity.substrate_kind = None;
    assert!(pod_uid_only.space_api_route_identity.pod_uid.is_some());
    assert_eq!(
        super::node_local_registration_generation(&pod_uid_only),
        Some(generation(GENERATION))
    );

    let mut space_runtime = row.clone();
    space_runtime.space_api_route_identity.substrate_kind =
        Some(pplx_space_common_core::types::NodeManagedBy::Space);
    assert!(space_runtime.space_api_route_identity.pod_uid.is_some());
    assert_eq!(
        super::node_local_registration_generation(&space_runtime),
        None
    );

    let unrouted = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &unrouted,
        SandboxState::Started,
    );
    let row = crate::db::get_sandbox(&conn, &unrouted).unwrap().unwrap();
    assert_eq!(super::node_local_registration_generation(&row), None);
}

/// A cluster-api stub. Its `BeginArtifactBackup` refuses every
/// generation-bound registration as superseded and admits the rest, or, when
/// `unavailable`, answers every registration `UNAVAILABLE`. Its
/// placement-filtered snapshot-storage lookup finds no sandbox, as it answers
/// a node the sandbox has moved off, unless `placed`, when it finds one with
/// no storage override. It has no ready backup of any artifact, and it shows
/// the sandbox has a ready snapshot to restore from exactly when
/// `backed_copy`.
#[derive(Clone, Copy)]
struct RetiringClusterApi {
    unavailable: bool,
    placed: bool,
    backed_copy: bool,
}

const RETIRING: RetiringClusterApi = RetiringClusterApi {
    unavailable: false,
    placed: false,
    backed_copy: true,
};

/// The stub's answer to a backup readiness query: whether the sandbox has a
/// ready snapshot to restore from, and no ready backup of any one artifact.
#[derive(Clone, Copy)]
struct BackedCopyLookup {
    backed_copy: bool,
}

impl
    tonic::server::UnaryService<pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest>
    for BackedCopyLookup
{
    type Response = pplx_space_node_api_client::proto::GetArtifactBackupLocationResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(
        &mut self,
        request: tonic::Request<
            pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest,
        >,
    ) -> Self::Future {
        use pplx_space_node_api_client::proto::backup_readiness_query::Artifact;
        let readiness = request
            .into_inner()
            .readiness
            .and_then(|query| query.artifact);
        std::future::ready(match readiness {
            Some(Artifact::DurablyRestorable(true)) => Ok(tonic::Response::new(Self::Response {
                ready: Some(self.backed_copy),
                ..Default::default()
            })),
            Some(_) => Ok(tonic::Response::new(Self::Response {
                ready: Some(false),
                ..Default::default()
            })),
            None => Err(tonic::Status::not_found("no ready backup of this artifact")),
        })
    }
}

/// The snapshot-storage lookup of a sandbox placed on the calling node, whose
/// organization has no storage override.
#[derive(Clone, Copy)]
struct PlacedStorageLookup;

impl
    tonic::server::UnaryService<
        pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriRequest,
    > for PlacedStorageLookup
{
    type Response = pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(
        &mut self,
        _: tonic::Request<pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriRequest>,
    ) -> Self::Future {
        std::future::ready(Ok(tonic::Response::new(Self::Response {
            storage_uri: None,
        })))
    }
}

impl tonic::server::NamedService for RetiringClusterApi {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl tonic::server::UnaryService<pplx_space_node_api_client::proto::BeginArtifactBackupRequest>
    for RetiringClusterApi
{
    type Response = pplx_space_node_api_client::proto::BeginArtifactBackupResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(
        &mut self,
        request: tonic::Request<pplx_space_node_api_client::proto::BeginArtifactBackupRequest>,
    ) -> Self::Future {
        let request = request.into_inner();
        if self.unavailable {
            return std::future::ready(Err(tonic::Status::unavailable(
                "cluster-api is restarting",
            )));
        }
        std::future::ready(match request.execution_generation {
            Some(generation) => Err(
                pplx_space_node_api_client::cluster::backup_execution_superseded_status(
                    "retired",
                    ExecutionGeneration::new(generation).unwrap(),
                ),
            ),
            None => Ok(tonic::Response::new(Self::Response {
                local_artifact_id: request.local_artifact_id,
                backup_attempt_id: request.backup_attempt_id,
            })),
        })
    }
}

impl<B> tower::Service<http::Request<B>> for RetiringClusterApi
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let stub = *self;
        Box::pin(async move {
            if path.ends_with("/BeginArtifactBackup") {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(stub, request).await)
            } else if path.ends_with("/GetSandboxSnapshotStorageUri") && stub.placed {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(PlacedStorageLookup, request).await)
            } else if path.ends_with("/GetSandboxSnapshotStorageUri") {
                Ok(tonic::Status::not_found("sandbox is not placed on this node").into_http())
            } else if path.ends_with("/GetArtifactBackupLocation") {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                let lookup = BackedCopyLookup {
                    backed_copy: stub.backed_copy,
                };
                Ok(grpc.unary(lookup, request).await)
            } else {
                Ok(tonic::Status::unimplemented(path).into_http())
            }
        })
    }
}

/// Serves `stub` and runs `test` in an isolated child process whose
/// cluster-api client wiring points at it, with a synthetic snapshot upload
/// destination configured.
async fn with_cluster_api(stub: RetiringClusterApi, test: impl AsyncFnOnce()) {
    use std::ffi::OsStr;

    // The parent serves while the isolated child exercises environment-based
    // client wiring. A second runtime thread keeps the server making progress.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let stop = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(stub)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
    );
    let token = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(token.path(), "synthetic-backup-test-token").unwrap();
    let child = pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("1"))),
        (
            pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV,
            Some(OsStr::new(&endpoint)),
        ),
        (
            pplx_space_common_svc::bearer_token::SPACE_CLUSTER_API_TOKEN_PATH_ENV,
            Some(token.path().as_os_str()),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(OsStr::new("synthetic-backup-test-token")),
        ),
        ("SPACE_SNAPSHOT_BUCKET", Some(OsStr::new("test-bucket"))),
        ("SPACE_SNAPSHOT_REGION", Some(OsStr::new("us-east-1"))),
    ]);
    if child {
        test().await;
    }
    stop.cancel();
    server.await.unwrap().unwrap();
}

/// A generation-bound suspend upload payload for `artifact`.
fn bound_upload_payload(artifact: LocalArtifactId) -> crate::snapshot_upload::SnapshotUploadJobs {
    let mut payload = crate::snapshot_upload_jobs::tests::reclaim_fixture_payload(
        &artifact.to_string(),
        std::path::PathBuf::from("/var/lib/space/sandboxes/s/suspend"),
    );
    payload.meta.execution = Some(
        pplx_space_common_core::types::SnapshotExecutionMetadata::new(
            pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
                std::env::consts::ARCH.parse().unwrap(),
                vec![],
            ),
        )
        .unwrap(),
    );
    payload.execution_generation = Some(generation(GENERATION));
    payload
}

/// A node the sandbox has moved off can't see the sandbox through the
/// placement-filtered storage lookup, but its retired execution's refused
/// backup still probes registration, learns it was superseded, and releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_execution_off_its_node_still_releases() {
    with_cluster_api(RETIRING, async || {
        let refused = refused_teardown_with(|staged| {
            serde_json::to_string(&bound_upload_payload(staged)).unwrap()
        })
        .await;
        let service = &refused.service;
        tokio::time::timeout(
            Duration::from_secs(5),
            service.settle_teardown_suspend_drive(
                refused.sandbox_id,
                generation(GENERATION),
                Err(refused.error.clone()),
            ),
        )
        .await
        .expect("the release should complete")
        .expect("a retired execution's teardown must be released");
        assert_eq!(
            *refused.dispatched.lock().unwrap(),
            vec![(refused.sandbox_id.to_string(), false)],
            "the resident runtime must be torn down"
        );
        assert_eq!(
            poll_outcome(service, refused.sandbox_id).await,
            TeardownSuspendOutcome::Deleted
        );
        drain(service).await;
    })
    .await;
}

/// The drive can meet the refusal itself, while the upload job it enqueued is
/// still live. On a node the sandbox has moved off, the refusal's own verdict
/// releases the runtime on that first drive, though the job can't be probed
/// yet and the storage lookup can't see the sandbox, and cancels the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_refusal_off_its_node_releases_on_the_first_drive() {
    with_cluster_api(RETIRING, async || {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let staged =
            insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, generation(GENERATION));
        let upload_job = insert_upload_job(
            &conn,
            &sandbox_id.to_string(),
            staged,
            &serde_json::to_string(&bound_upload_payload(staged)).unwrap(),
        );
        let (service, dispatched) = service_recording_cleanup(conn);
        let error = super::teardown_artifact_backup_status(
            super::ArtifactBackupSubmissionError::Begin {
                error: pplx_space_common_jobs::JobError::permanent(
                    "cluster-api rejected begin_artifact_backup (FailedPrecondition): retired",
                ),
                superseded_generation: Some(generation(GENERATION)),
            },
            staged,
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            service.settle_teardown_suspend_drive(sandbox_id, generation(GENERATION), Err(error)),
        )
        .await
        .expect("the release should complete")
        .expect("a retired execution's teardown must be released");
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec![(sandbox_id.to_string(), false)],
            "the resident runtime must be torn down"
        );
        let upload = {
            let db = service.db.lock().unwrap();
            pplx_space_common_jobs::db::get_job(&db, &upload_job)
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            upload.status,
            pplx_space_common_jobs::JobStatus::Cancelled.as_str(),
            "the discarded capture's upload must not register it later"
        );
        assert_eq!(
            poll_outcome(&service, sandbox_id).await,
            TeardownSuspendOutcome::Deleted
        );
        drain(&service).await;
    })
    .await;
}

/// The node-local suspend path end to end, up to the runtime it keeps: the
/// upload it persists carries the sandbox's Kubernetes execution generation,
/// the cluster's refusal of that generation comes back as a backup-fence
/// failure, and that failure rolls the suspend back to `paused` with the
/// runtime still resident.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_local_suspend_of_a_superseded_execution_rolls_back() {
    with_cluster_api(RETIRING, async || {
        use crate::service::tests::service_tests::{insert_test_sandbox, runtime_fields};
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        let mut fields = runtime_fields(generation(GENERATION), 7);
        fields.space_api_route_identity.substrate_kind =
            Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes);
        crate::db::update_runtime_fields(&conn, sandbox_id.parse().unwrap(), fields).unwrap();
        let service = service_with_runtime_probe(conn, true);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let snapshot_root = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let snapshot_dir = snapshot_root.path().join("snapshot");
        crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id,
            local_artifact_id,
            b"node-local-capture",
        );

        let refused = service
            .enqueue_node_local_suspend_backup(
                &sandbox,
                local_artifact_id,
                snapshot_dir.to_str().unwrap(),
                SuspendSnapshotFormat::Full,
            )
            .await
            .expect_err("the cluster refuses a superseded execution's capture");
        assert!(
            refused
                .metadata()
                .contains_key(super::AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA),
            "a refusal must disarm auto-lifecycle: {refused:?}"
        );
        let payload: String = service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT payload FROM jobs WHERE kind = ?1 AND subject_id = ?2",
                rusqlite::params![
                    crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                    sandbox_id
                ],
                |row| row.get(0),
            )
            .unwrap();
        let payload: crate::snapshot_upload::SnapshotUploadJobs =
            serde_json::from_str(&payload).unwrap();
        assert_eq!(payload.execution_generation, Some(generation(GENERATION)));

        service
            .require_node_local_backup_handoff(&sandbox_id, local_artifact_id, Err(refused))
            .expect_err("a refused handoff must stop before runtime finalization");
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Paused);
        assert_eq!(row.pid, Some(std::process::id()));
        drain(&service).await;
    })
    .await;
}

/// Re-requesting a refused capture's backup by its artifact id, as node-drain
/// promotion does, starts a fresh attempt under the generation the refused
/// one registered under, so the control plane refuses it again rather than
/// registering it unfenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_rerequested_by_artifact_keeps_its_execution_generation() {
    with_cluster_api(
        RetiringClusterApi {
            placed: true,
            ..RETIRING
        },
        async || {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let sandbox_id = refused.sandbox_id.to_string();
            let snapshot_root = tempfile::tempdir().unwrap();
            let snapshot_dir = snapshot_root.path().join("suspend");
            crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
                &snapshot_dir,
                &sandbox_id,
                refused.staged,
                b"refused-capture",
            );
            service
                .db
                .lock()
                .unwrap()
                .execute(
                    "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
                    rusqlite::params![snapshot_dir.to_str().unwrap(), refused.staged.to_string()],
                )
                .unwrap();

            let error = service
                .enqueue_artifact_backup(internal_request(
                    crate::proto::EnqueueArtifactBackupRequest {
                        sandbox_id: sandbox_id.clone(),
                        expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                        source: Some(
                            crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                                refused.staged.to_string(),
                            ),
                        ),
                    },
                ))
                .await
                .expect_err("the cluster refuses the retired execution's capture again");
            assert_eq!(
                pplx_space_node_api_client::cluster::backup_execution_superseded_generation(&error),
                Some(generation(GENERATION)),
                "the caller must see the superseded verdict: {error}"
            );
            let latest = service
                .snapshot_upload_worker
                .find_job_by_dedup_key(
                    crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                    &refused.staged.to_string(),
                )
                .unwrap()
                .unwrap();
            let payload = latest
                .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
                .unwrap();
            assert_ne!(
                payload.snapshot_id,
                bound_upload_payload(refused.staged).snapshot_id,
                "the re-request must start a fresh attempt: {error}"
            );
            assert_eq!(payload.execution_generation, Some(generation(GENERATION)));
            drain(service).await;
        },
    )
    .await;
}

/// A capture whose earlier upload was admitted stays bound to the execution
/// that took it: the re-request carries that generation, and the node passes
/// on whatever the control plane answers. (The control plane admits it, since
/// an earlier attempt registered these bytes; this stub refuses every
/// generation-bound registration.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rerequested_admitted_backup_keeps_its_execution_generation() {
    with_cluster_api(
        RetiringClusterApi {
            placed: true,
            ..RETIRING
        },
        async || {
            let refused = refused_teardown_with(|staged| {
                let mut admitted = bound_upload_payload(staged);
                admitted.job_ids = vec![JobId::new()];
                serde_json::to_string(&admitted).unwrap()
            })
            .await;
            let service = &refused.service;
            let sandbox_id = refused.sandbox_id.to_string();
            let snapshot_root = tempfile::tempdir().unwrap();
            let snapshot_dir = snapshot_root.path().join("suspend");
            crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
                &snapshot_dir,
                &sandbox_id,
                refused.staged,
                b"admitted-capture",
            );
            service
                .db
                .lock()
                .unwrap()
                .execute(
                    "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
                    rusqlite::params![snapshot_dir.to_str().unwrap(), refused.staged.to_string()],
                )
                .unwrap();

            let error = service
                .enqueue_artifact_backup(internal_request(
                    crate::proto::EnqueueArtifactBackupRequest {
                        sandbox_id: sandbox_id.clone(),
                        expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                        source: Some(
                            crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                                refused.staged.to_string(),
                            ),
                        ),
                    },
                ))
                .await
                .expect_err("the cluster refuses the superseded execution's capture");
            assert_eq!(
                pplx_space_node_api_client::cluster::backup_execution_superseded_generation(&error),
                Some(generation(GENERATION)),
                "the caller must see the superseded verdict: {error}"
            );
            let latest = service
                .snapshot_upload_worker
                .find_job_by_dedup_key(
                    crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                    &refused.staged.to_string(),
                )
                .unwrap()
                .unwrap();
            let payload = latest
                .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
                .unwrap();
            assert_ne!(
                payload.snapshot_id,
                bound_upload_payload(refused.staged).snapshot_id,
                "the re-request must start a fresh attempt"
            );
            assert_eq!(payload.execution_generation, Some(generation(GENERATION)));
            drain(service).await;
        },
    )
    .await;
}

/// A teardown capture requested by artifact id before its own upload job
/// exists registers under the running teardown's generation, not the route's,
/// while the row still pins it, including after a restart left it in cleanup.
/// A suspended row's artifact belongs to an earlier node-local suspend and
/// registers without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_request_of_a_staged_capture_carries_the_teardown_generation() {
    with_cluster_api(
        RetiringClusterApi {
            placed: true,
            ..RETIRING
        },
        async || {
            for (state, carried) in [
                ("suspending", Some(generation(GENERATION))),
                ("cleanup_required", Some(generation(GENERATION))),
                ("suspended", None),
            ] {
                let conn = crate::db::test_conn();
                let sandbox_id = SandboxId::new();
                let sandbox_id_string = sandbox_id.to_string();
                let staged = insert_stranded_teardown_suspend_sandbox(
                    &conn,
                    sandbox_id,
                    generation(GENERATION),
                );
                let snapshot_root = tempfile::tempdir().unwrap();
                let snapshot_dir = snapshot_root.path().join("suspend");
                crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
                    &snapshot_dir,
                    &sandbox_id_string,
                    staged,
                    b"staged-capture",
                );
                conn.execute(
                    "UPDATE local_artifacts SET local_path = ?1 WHERE local_artifact_id = ?2",
                    rusqlite::params![snapshot_dir.to_str().unwrap(), staged.to_string()],
                )
                .unwrap();
                // The route names another generation, so only the teardown's
                // own can be what the request carries.
                conn.execute(
                    "UPDATE sandboxes
                        SET state = ?1, space_api_route_execution_generation = ?2
                      WHERE id = ?3",
                    rusqlite::params![state, GENERATION + 92, sandbox_id_string],
                )
                .unwrap();
                let (service, _dispatched) = service_recording_cleanup(conn);

                let result = service
                    .enqueue_artifact_backup(internal_request(
                        crate::proto::EnqueueArtifactBackupRequest {
                            sandbox_id: sandbox_id_string.clone(),
                            expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                            source: Some(
                                crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                                    staged.to_string(),
                                ),
                            ),
                        },
                    ))
                    .await;
                let verdict = result.as_ref().err().and_then(|error| {
                    pplx_space_node_api_client::cluster::backup_execution_superseded_generation(error)
                });
                assert_eq!(verdict, carried, "{state}: {result:?}");
                drain(&service).await;
            }
        },
    )
    .await;
}

/// A probe cluster-api can't answer is no verdict, and the lookup that can't
/// see a sandbox placed on another node is no proof it's gone: the teardown
/// stays running, nothing is released, and the next drive asks again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unanswered_probe_off_its_node_keeps_the_teardown_running() {
    with_cluster_api(
        RetiringClusterApi {
            unavailable: true,
            ..RETIRING
        },
        async || {
            let refused = refused_teardown_with(|staged| {
                serde_json::to_string(&bound_upload_payload(staged)).unwrap()
            })
            .await;
            let service = &refused.service;
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                service.settle_teardown_suspend_drive(
                    refused.sandbox_id,
                    generation(GENERATION),
                    Err(refused.error.clone()),
                ),
            )
            .await
            .expect("the drive should settle");
            assert!(result.is_err(), "the drive must stay failed");
            assert!(refused.dispatched.lock().unwrap().is_empty());
            let status = {
                let db = service.db.lock().unwrap();
                crate::db::latest_teardown_suspend_operation(&db, &refused.sandbox_id.to_string())
                    .unwrap()
                    .unwrap()
                    .status
            };
            assert_eq!(
                status,
                crate::db::TeardownSuspendOperationStatus::Running,
                "an unanswered probe must not close the teardown"
            );
            drain(service).await;
        },
    )
    .await;
}

/// The re-sent registration carries the refused generation back across the
/// wire, to the probe and to a direct registration: a generation-bound upload
/// reads as superseded at exactly its own generation, and an unbound one,
/// which the cluster admits, reads as not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resent_registration_recovers_the_superseded_generation() {
    with_cluster_api(RETIRING, async || {
        let artifact: LocalArtifactId = "01939e3e-0000-7000-8000-0000000000bb".parse().unwrap();
        let mut payload = bound_upload_payload(artifact);
        assert_eq!(
            crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(&payload).await,
            RegistrationProbe::Superseded(generation(GENERATION))
        );
        let refusal =
            crate::snapshot_upload_jobs::begin_prepared_upload_naming_supersession(&payload)
                .await
                .expect_err("the cluster refuses a superseded registration");
        assert_eq!(refusal.superseded_generation, Some(generation(GENERATION)));
        assert_eq!(
            refusal.error.kind(),
            pplx_space_common_jobs::JobFailureKind::Permanent
        );
        payload.execution_generation = None;
        assert_eq!(
            crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(&payload).await,
            RegistrationProbe::NotSuperseded
        );
    })
    .await;
}
