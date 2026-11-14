use std::sync::Arc;
use std::time::Duration;

use pplx_space_accounts::{JobId, NodeId};
use pplx_space_common_core::types::{
    ExecutionGeneration, LocalArtifactId, NodeManagedBy, RouteGeneration,
    SandboxActivationAttemptId, SandboxId, SpaceApiRouteIdentity,
};
use pplx_space_k8s_ids::PodUid;
use tonic::Request;

use super::*;
use crate::proto::{DeleteRequest, TeardownSuspendAction, TeardownSuspendOutcome};
use crate::service::SandboxManagerService;
use crate::service::activation_support::internal_request;
use crate::service::tests::service_tests::{
    insert_platform_owned_sandbox, node_api_trusted_lifecycle_request,
    prepare_teardown_suspend_request,
};
use crate::state::{SandboxState, SandboxStatus};

const REASON: &str = "node lost mid-teardown; owner accepted losing unsaved work";

#[test]
fn a_reason_is_trimmed_and_bounded() {
    assert_eq!(
        DiscardReason::parse("  node lost before publish  ")
            .unwrap()
            .as_str(),
        "node lost before publish"
    );
    assert_eq!(DiscardReason::parse(" \t "), Err(DiscardReasonError::Empty));
    assert_eq!(
        DiscardReason::parse(&"x".repeat(MAX_DISCARD_REASON_CHARS)).map(|r| r.0.len()),
        Ok(MAX_DISCARD_REASON_CHARS)
    );
    assert_eq!(
        DiscardReason::parse(&"x".repeat(MAX_DISCARD_REASON_CHARS + 1)),
        Err(DiscardReasonError::TooLong(MAX_DISCARD_REASON_CHARS + 1))
    );
    for hidden in [
        "line one\nline two",
        "one\u{2028}two",
        "abc\u{202E}def",
        "\u{2066}x",
        "zero\u{200B}width",
        "no\u{FEFF}break",
        "soft\u{00AD}hyphen",
        "invisible\u{2062}times",
        "tag\u{E0041}text",
    ] {
        assert_eq!(
            DiscardReason::parse(hidden),
            Err(DiscardReasonError::ControlCharacter),
            "{hidden:?}"
        );
    }
}

fn discard_request(sandbox_id: &str, reason: &str) -> DeleteRequest {
    DeleteRequest {
        sandbox_id: sandbox_id.to_owned(),
        template_filter: None,
        blocking: true,
        discard_retained_filesystem_reason: Some(reason.to_owned()),
    }
}

/// How the stranded row came to be quarantined.
#[derive(Debug, Clone, Copy)]
enum Quarantine {
    /// Startup recovery found the runtime gone and no capture of its own.
    StartupRecovery,
    /// The suspend's cleanup completed without a published artifact, which
    /// also clears the row's route, so its execution generation survives only
    /// in the teardown operation.
    CleanupCompletion,
    /// As [`Quarantine::CleanupCompletion`], with the failed teardown job then
    /// pruned, as sbx-mgr does once its retention passes: the generation
    /// survives only as the row's retained route high-water mark.
    CleanupCompletionJobPruned,
    /// As [`Quarantine::CleanupCompletion`], on a row with no retained route
    /// high-water mark: the generation survives only in the teardown job.
    CleanupCompletionNoHighWater,
}

/// A Kubernetes teardown suspend of execution `generation` that the node lost
/// before any capture was staged, quarantined as `how` says: the strand a
/// discard delete exists for. Returns the service, which refuses the pod's
/// teardown until the row is gone.
fn quarantined_teardown(
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> (SandboxManagerService, tempfile::TempDir) {
    quarantined_teardown_by(sandbox_id, generation, Quarantine::StartupRecovery)
}

fn quarantined_teardown_by(
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    how: Quarantine,
) -> (SandboxManagerService, tempfile::TempDir) {
    let stranded = stranded_teardown(sandbox_id, generation, how, AttemptWindow::Passed);
    (stranded.service, stranded.cache)
}

/// Whether the quarantined row's suspend attempt is still inside the recovery
/// window in which its capture could converge.
#[derive(Debug, Clone, Copy)]
enum AttemptWindow {
    Open,
    Passed,
}

/// A quarantined teardown with the knobs the discard's capture guards read:
/// the attempt window and whether the runtime probes report the sandbox live
/// or, for the fallible probe, unreadable.
struct Stranded {
    service: SandboxManagerService,
    cache: tempfile::TempDir,
    runtime_live: Arc<std::sync::atomic::AtomicBool>,
    runtime_unreadable: Arc<std::sync::atomic::AtomicBool>,
}

fn stranded_teardown(
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    how: Quarantine,
    window: AttemptWindow,
) -> Stranded {
    let conn = crate::db::test_conn();
    let id = sandbox_id.to_string();
    insert_platform_owned_sandbox(&conn, &id, SandboxState::Paused, None);
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(generation.get().unsigned_abs()).unwrap()),
            current_node_id: Some(NodeId::new()),
            execution_generation: Some(generation),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            pod_uid: Some(PodUid::parse("pod-uid-discard").unwrap()),
            ..SpaceApiRouteIdentity::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();
    crate::db::begin_teardown_suspend(&conn, &id, generation, LocalArtifactId::new()).unwrap();
    if let Quarantine::CleanupCompletion
    | Quarantine::CleanupCompletionJobPruned
    | Quarantine::CleanupCompletionNoHighWater = how
    {
        crate::db::set_cleanup_intent(
            &conn,
            &id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();
        crate::db::complete_row_intent_cleanup(
            &conn,
            &id,
            crate::db::CleanupCompletion::MarkSuspended,
            None,
        )
        .unwrap();
        assert_eq!(
            crate::db::read::sandbox_view(&conn, &id)
                .runtime_execution_generation()
                .unwrap(),
            None,
            "the cleanup's quarantine clears the route"
        );
    }
    if let Quarantine::CleanupCompletionNoHighWater = how {
        let cleared = conn
            .execute(
                "UPDATE sandboxes SET last_observed_execution_generation = NULL WHERE id = ?1",
                crate::db::fixture::params![id],
            )
            .unwrap();
        assert_eq!(cleared, 1);
    }
    if let Quarantine::CleanupCompletionJobPruned = how {
        let pruned = conn
            .execute(
                "DELETE FROM jobs WHERE subject_id = ?1",
                crate::db::fixture::params![id],
            )
            .unwrap();
        assert!(pruned > 0, "the teardown job existed to prune");
    }
    let cache = tempfile::tempdir().unwrap();
    let runtime_live = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let runtime_unreadable = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let probe = Arc::clone(&runtime_live);
    let fallible_live = Arc::clone(&runtime_live);
    let fallible_unreadable = Arc::clone(&runtime_unreadable);
    let service = SandboxManagerService::new_with_options(
        conn,
        crate::service::SandboxManagerServiceOptions {
            sandbox_runtime_live: Arc::new(move |_| {
                probe.load(std::sync::atomic::Ordering::SeqCst)
            }),
            capture_runtime_probe: Some(Arc::new(move |_: &str| {
                if fallible_unreadable.load(std::sync::atomic::Ordering::SeqCst) {
                    Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
                } else {
                    Ok(fallible_live.load(std::sync::atomic::Ordering::SeqCst))
                }
            })),
            ..crate::service::SandboxManagerServiceOptions::for_test()
        },
    );
    service
        .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
        .unwrap();
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(row.state, SandboxStatus::from(SandboxState::Quarantined));
    assert_eq!(
        row.failure_reason.as_deref(),
        Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    );
    if let AttemptWindow::Passed = window {
        let window_secs = i64::try_from(
            crate::service::suspend_snapshot_publication::SUSPEND_ATTEMPT_RECOVERY_WINDOW.as_secs(),
        )
        .unwrap();
        let aged = service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes
                    SET suspend_attempt_started_at = suspend_attempt_started_at - ?2
                  WHERE id = ?1 AND suspend_attempt_started_at IS NOT NULL",
                crate::db::fixture::params![id, window_secs + 60],
            )
            .unwrap();
        assert_eq!(aged, 1, "the quarantined row keeps its suspend attempt");
    }
    *service.cleanup_executor.dispatch.lock().unwrap() =
        Some(Arc::new(|_, _| Box::pin(async { Ok(()) })));
    Stranded {
        service,
        cache,
        runtime_live,
        runtime_unreadable,
    }
}

async fn poll_teardown(
    service: &SandboxManagerService,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> Result<TeardownSuspendOutcome, tonic::Status> {
    service
        .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
            sandbox_id.to_string(),
            generation.get(),
            TeardownSuspendAction::BeginOrJoin,
        )))
        .await
        .map(|response| TeardownSuspendOutcome::try_from(response.into_inner().outcome).unwrap())
}

async fn drain(service: SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
        .await
        .expect("cleanup tasks drain");
}

/// The strand: the pod's teardown is refused while the row is quarantined,
/// and an ordinary delete refuses the only copy. A discard delete removes the
/// row and leaves the execution's delete receipt, so the next teardown poll
/// answers DELETED and the shim releases the pod.
#[tokio::test]
async fn a_discard_delete_releases_the_pod_of_a_quarantined_teardown() {
    for how in [
        Quarantine::StartupRecovery,
        Quarantine::CleanupCompletion,
        Quarantine::CleanupCompletionJobPruned,
        Quarantine::CleanupCompletionNoHighWater,
    ] {
        a_discard_delete_releases_the_pod(how).await;
    }
}

async fn a_discard_delete_releases_the_pod(how: Quarantine) {
    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(7).unwrap();
    let (service, _cache) = quarantined_teardown_by(sandbox_id, generation, how);
    let id = sandbox_id.to_string();

    let refused = poll_teardown(&service, sandbox_id, generation)
        .await
        .expect_err("a quarantined row's teardown is refused");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    let refused = service
        .delete(internal_request(DeleteRequest {
            sandbox_id: id.clone(),
            template_filter: None,
            blocking: true,
            discard_retained_filesystem_reason: None,
        }))
        .await
        .expect_err("an ordinary delete refuses the only copy");
    assert!(pplx_space_sbx_mgr_client::is_retained_filesystem_only_copy_status(&refused));

    let deleted = tokio::time::timeout(
        Duration::from_secs(5),
        service.delete(internal_request(discard_request(&id, REASON))),
    )
    .await
    .expect("the discard completes")
    .expect("the discard is admitted")
    .into_inner()
    .deleted_ids;
    assert_eq!(deleted, vec![id.clone()]);
    assert!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &id)
            .unwrap()
            .is_none()
    );

    assert_eq!(
        poll_teardown(&service, sandbox_id, generation)
            .await
            .expect("the teardown is answered"),
        TeardownSuspendOutcome::Deleted,
        "{how:?}: the shim releases the pod"
    );
    drain(service).await;
}

/// A discard erases only a filesystem held as the only copy: every other row
/// is refused and left as it was, as is a discard from a service peer, one
/// with a template filter, or one without a usable reason.
#[tokio::test]
async fn a_discard_delete_refuses_every_other_row_and_caller() {
    let conn = crate::db::test_conn();
    let ordinary = SandboxId::new().to_string();
    insert_platform_owned_sandbox(&conn, &ordinary, SandboxState::Stopped, None);
    // Quarantined for another reason: its filesystem isn't held as the only
    // copy, and an ordinary delete already handles it.
    let other_quarantine = SandboxId::new().to_string();
    insert_platform_owned_sandbox(&conn, &other_quarantine, SandboxState::Paused, None);
    conn.execute(
        "UPDATE sandboxes SET state = 'quarantined', failure_reason = 'pause_failed' WHERE id = ?1",
        crate::db::fixture::params![other_quarantine],
    )
    .unwrap();
    let service = SandboxManagerService::for_test(conn);

    for id in [&ordinary, &other_quarantine] {
        let before = service.lookup_sandbox(id).unwrap().state;
        let error = service
            .delete(internal_request(discard_request(id, REASON)))
            .await
            .expect_err("a discard of a row not held as its only copy is refused");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{id}");
        assert_eq!(service.lookup_sandbox(id).unwrap().state, before, "{id}");
    }
    let missing = SandboxId::new().to_string();
    assert_eq!(
        service
            .delete(internal_request(discard_request(&missing, REASON)))
            .await
            .expect_err("a discard names a row that is here")
            .code(),
        tonic::Code::NotFound
    );

    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(3).unwrap();
    let (stranded, _cache) = quarantined_teardown(sandbox_id, generation);
    let id = sandbox_id.to_string();
    let refusals: [(&str, Request<DeleteRequest>, tonic::Code); 4] = [
        (
            "a service peer",
            node_api_trusted_lifecycle_request(discard_request(&id, REASON)),
            tonic::Code::PermissionDenied,
        ),
        (
            "a template filter",
            internal_request(DeleteRequest {
                template_filter: Some("tmpl".to_owned()),
                ..discard_request(&id, REASON)
            }),
            tonic::Code::InvalidArgument,
        ),
        (
            "an empty reason",
            internal_request(discard_request(&id, "   ")),
            tonic::Code::InvalidArgument,
        ),
        (
            "a multi-line reason",
            internal_request(discard_request(&id, "one\ntwo")),
            tonic::Code::InvalidArgument,
        ),
    ];
    for (case, request, code) in refusals {
        let error = stranded
            .delete(request)
            .await
            .expect_err("the discard is refused");
        assert_eq!(error.code(), code, "{case}: {error:?}");
        assert_eq!(
            stranded.lookup_sandbox(&id).unwrap().state,
            SandboxStatus::from(SandboxState::Quarantined),
            "{case}"
        );
    }
    drain(stranded).await;
}

/// A discard can't race a capture that may still finish: it refuses while the
/// row's suspend attempt is inside its recovery window, while a backup upload
/// of its artifacts is pending or being admitted, while its runtime is live,
/// and while its liveness can't be read, and leaves the row quarantined. Past all of them
/// it's admitted.
#[tokio::test]
async fn a_discard_delete_refuses_while_a_capture_may_still_finish() {
    #[derive(Debug, Clone, Copy)]
    enum Open {
        AttemptWindow,
        BackupUpload,
        BackupAdmitting,
        RuntimeLive,
        RuntimeUnreadable,
    }
    for open in [
        Open::AttemptWindow,
        Open::BackupUpload,
        Open::BackupAdmitting,
        Open::RuntimeLive,
        Open::RuntimeUnreadable,
    ] {
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        let window = match open {
            Open::AttemptWindow => AttemptWindow::Open,
            Open::BackupUpload
            | Open::BackupAdmitting
            | Open::RuntimeLive
            | Open::RuntimeUnreadable => AttemptWindow::Passed,
        };
        let stranded = stranded_teardown(
            sandbox_id,
            ExecutionGeneration::new(5).unwrap(),
            Quarantine::StartupRecovery,
            window,
        );
        let mut _backup_admission = None;
        match open {
            Open::AttemptWindow => {}
            Open::BackupAdmitting => {
                _backup_admission = Some(
                    stranded
                        .service
                        .auto_lifecycle
                        .artifact_lock(&id)
                        .try_lock_owned()
                        .unwrap(),
                );
            }
            Open::BackupUpload => {
                let job_id = JobId::new();
                pplx_space_common_jobs::db::insert_job(
                    stranded.service.db.lock().unwrap().raw(),
                    pplx_space_common_jobs::db::InsertJob {
                        job_id: &job_id,
                        kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                        payload: "{}",
                        subject_id: Some(&id),
                        dedup_key: None,
                        max_attempts: 1,
                        traceparent: None,
                        priority: pplx_space_common_jobs::JobPriority::default(),
                    },
                )
                .unwrap();
            }
            Open::RuntimeLive => stranded
                .runtime_live
                .store(true, std::sync::atomic::Ordering::SeqCst),
            Open::RuntimeUnreadable => stranded
                .runtime_unreadable
                .store(true, std::sync::atomic::Ordering::SeqCst),
        }

        let error = stranded
            .service
            .delete(internal_request(discard_request(&id, REASON)))
            .await
            .expect_err("a discard waits for a capture that may still finish");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{open:?}");
        assert_eq!(
            stranded.service.lookup_sandbox(&id).unwrap().state,
            SandboxStatus::from(SandboxState::Quarantined),
            "{open:?}"
        );
        drain(stranded.service).await;
    }
}

/// A cluster delete can remove the control-plane row without reaching the
/// node-local delete, which is what refuses the only copy (a teardown of a pod
/// with no runtime address, or of a sandbox with no observed execution). The
/// node then hears the sandbox was deleted and node-ctl asks for its eviction.
/// The eviction refuses as the delete would: no cleanup runs and the row stays
/// quarantined, so an operator's discard still finds it and erases it.
#[tokio::test]
async fn a_cluster_deleted_eviction_refuses_the_only_copy_and_leaves_it_for_a_discard() {
    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(9).unwrap();
    let (service, _cache) = quarantined_teardown(sandbox_id, generation);
    let id = sandbox_id.to_string();
    // Past the cluster-deleted eviction's quiescence window.
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET updated_at = '0' WHERE id = ?1",
            crate::db::fixture::params![id],
        )
        .unwrap();
    let dispatched = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::clone(&dispatched);
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, _| {
        observed.lock().unwrap().push(id);
        Box::pin(async { Ok(()) })
    }));

    let mut eviction = Request::new(crate::proto::EvictClusterDeletedSandboxRequest {
        sandbox_id: id.clone(),
    });
    eviction
        .extensions_mut()
        .insert(pplx_space_uds_peer_auth::PeerIdentity::new(0, 0, None));
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        service.evict_cluster_deleted_sandbox(eviction),
    )
    .await
    .expect("the eviction answers")
    .expect_err("the eviction refuses the only copy");
    assert!(
        pplx_space_sbx_mgr_client::is_retained_filesystem_only_copy_status(&refused),
        "{refused:?}"
    );
    assert!(
        dispatched.lock().unwrap().is_empty(),
        "no cleanup erases the filesystem"
    );
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Quarantined)
    );

    let deleted = tokio::time::timeout(
        Duration::from_secs(5),
        service.delete(internal_request(discard_request(&id, REASON))),
    )
    .await
    .expect("the discard completes")
    .expect("the discard is admitted")
    .into_inner()
    .deleted_ids;
    assert_eq!(deleted, vec![id.clone()]);
    assert_eq!(*dispatched.lock().unwrap(), vec![id.clone()]);
    assert!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &id)
            .unwrap()
            .is_none()
    );
    drain(service).await;
}

/// The `DeleteRequest` an older release encodes and decodes: the fields
/// before `discard_retained_filesystem_reason`.
#[derive(prost::Message)]
struct DeleteRequestBeforeDiscard {
    #[prost(string, tag = "1")]
    sandbox_id: String,
    #[prost(string, optional, tag = "2")]
    template_filter: Option<String>,
    #[prost(bool, tag = "3")]
    blocking: bool,
}

/// Mixed versions on the wire, both directions. An older client's delete
/// carries no discard field, so this sandbox-manager runs the ordinary delete,
/// which refuses the only copy. A discard sent to an older sandbox-manager
/// loses the field it doesn't know, so that node also runs the ordinary
/// delete, and the same refusal holds; the row is untouched either way.
#[tokio::test]
async fn a_delete_across_versions_without_the_discard_field_still_refuses_the_only_copy() {
    use prost::Message;
    let sandbox_id = SandboxId::new();
    let (service, _cache) = quarantined_teardown(sandbox_id, ExecutionGeneration::new(3).unwrap());
    let id = sandbox_id.to_string();

    let from_older_client = DeleteRequestBeforeDiscard {
        sandbox_id: id.clone(),
        template_filter: None,
        blocking: true,
    }
    .encode_to_vec();
    let at_older_node =
        DeleteRequestBeforeDiscard::decode(discard_request(&id, REASON).encode_to_vec().as_slice())
            .unwrap()
            .encode_to_vec();
    for (direction, wire) in [
        ("older client, this node", from_older_client),
        ("this client, older node", at_older_node),
    ] {
        let request = DeleteRequest::decode(wire.as_slice()).unwrap();
        assert_eq!(
            request.discard_retained_filesystem_reason, None,
            "{direction}"
        );
        let refused = service
            .delete(internal_request(request))
            .await
            .expect_err("the ordinary delete refuses the only copy");
        assert!(
            pplx_space_sbx_mgr_client::is_retained_filesystem_only_copy_status(&refused),
            "{direction}: {refused:?}"
        );
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxStatus::from(SandboxState::Quarantined),
            "{direction}"
        );
    }
    drain(service).await;
}

/// The discard's window is the six-hour recovery window unless a test-hooks
/// build is given a shorter one; anything unparsable keeps the full window.
#[test]
fn a_discard_waits_out_the_recovery_window_unless_a_test_hook_shortens_it() {
    let full = crate::service::suspend_snapshot_publication::SUSPEND_ATTEMPT_RECOVERY_WINDOW;
    assert_eq!(discard_attempt_window_from(None), full);
    assert_eq!(discard_attempt_window_from(Some("not-a-number")), full);
    assert_eq!(
        discard_attempt_window_from(Some(" 60 ")),
        Duration::from_secs(60)
    );
    let window = Duration::from_secs(60);
    assert!(attempt_inside_window(1_000, 1_059, window));
    assert!(!attempt_inside_window(1_000, 1_060, window));
    assert!(
        attempt_inside_window(1_000, 900, window),
        "a backward clock stays inside"
    );
}
