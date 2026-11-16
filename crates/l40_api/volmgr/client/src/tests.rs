use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn verified_replica_provenance_is_a_hidden_sibling() {
    let local_path = Path::new("/space/snap/snapshot.luks");

    let provenance_path = verified_replica_provenance_path(local_path);

    assert_eq!(
        provenance_path,
        Path::new("/space/snap/.snapshot.luks.volmgr-replica.json")
    );
}

#[test]
fn verified_replica_provenance_wire_shape_is_pinned() {
    let provenance = VerifiedReplicaProvenance {
        version: VERIFIED_REPLICA_PROVENANCE_VERSION,
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        s3_uri: "s3://bucket/snapshots/a/memfile?region=us-east-2".to_string(),
        raw: false,
        file_mode: Some(0o600),
        snapshot_artifact: Some(pplx_space_common_core::types::SnapshotArtifact::Memory),
        object_size: 17,
        object_etag: Some("\"etag-a\"".to_string()),
        local_size: 4096,
    };

    insta::assert_snapshot!(serde_json::to_string_pretty(&provenance).unwrap());
}

#[test]
fn verified_replica_provenance_accepts_legacy_record_without_artifact() {
    let provenance: VerifiedReplicaProvenance = serde_json::from_str(
        r#"{
            "version": 1,
            "subject_id": "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2",
            "s3_uri": "s3://bucket/snapshots/a/memfile?region=us-east-2",
            "raw": false,
            "file_mode": null,
            "object_size": 17,
            "object_etag": null,
            "local_size": 4096
        }"#,
    )
    .unwrap();

    assert_eq!(provenance.snapshot_artifact, None);
}

#[tokio::test]
async fn ensure_file_replica_retries_volume_manager_deploy_handoff() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_submit = Arc::clone(&calls);
    let config = EnsureFileReplicaWaitConfig {
        max_total: Duration::from_secs(1),
    };

    let response = submit_pull_file_with_retry(
        PullFileRequest::default(),
        tokio::time::Instant::now(),
        config.max_total,
        move |_| {
            let attempt = calls_for_submit.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(tonic::Status::unavailable(
                        "volume-manager is handing off; retry transfer submission",
                    ))
                } else {
                    Ok(tonic::Response::new(SubmitTransferResponse {
                        job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
                    }))
                }
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(response.job_id, "019d84bd-280d-7331-99c6-b63377f3a3a2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn ensure_file_replica_does_not_retry_non_admission_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_submit = Arc::clone(&calls);

    let error = submit_pull_file_with_retry(
        PullFileRequest::default(),
        tokio::time::Instant::now(),
        Duration::from_secs(1),
        move |_| {
            calls_for_submit.fetch_add(1, Ordering::SeqCst);
            async { Err(tonic::Status::invalid_argument("invalid pull request")) }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ensure_file_replica_survives_unavailable_wait_during_rollout() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_wait = Arc::clone(&calls);

    let response = wait_transfer_job_with_retry(
        &"019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
        tokio::time::Instant::now(),
        Duration::from_secs(1),
        move |request| {
            assert_eq!(request.job_id, "019d84bd-280d-7331-99c6-b63377f3a3a2");
            let attempt = calls_for_wait.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(tonic::Status::unavailable("volume-manager restarting"))
                } else {
                    Ok(tonic::Response::new(GetTransferJobResponse {
                        job: Some(proto::TransferJob {
                            job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
                            status: TransferJobStatus::Completed as i32,
                            ..Default::default()
                        }),
                    }))
                }
            }
        },
    )
    .await
    .expect("wait should survive a transient volume-manager rollout gap");

    assert_eq!(
        response.job.unwrap().job_id,
        "019d84bd-280d-7331-99c6-b63377f3a3a2"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn ensure_file_replica_survives_cancelled_wait_during_rollout() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_wait = Arc::clone(&calls);

    let response = wait_transfer_job_with_retry(
        &"019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
        tokio::time::Instant::now(),
        Duration::from_secs(1),
        move |request| {
            assert_eq!(request.job_id, "019d84bd-280d-7331-99c6-b63377f3a3a2");
            let attempt = calls_for_wait.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(tonic::Status::cancelled("operation was canceled"))
                } else {
                    Ok(tonic::Response::new(GetTransferJobResponse {
                        job: Some(proto::TransferJob {
                            job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
                            status: TransferJobStatus::Completed as i32,
                            ..Default::default()
                        }),
                    }))
                }
            }
        },
    )
    .await
    .expect("wait should survive cancellation when volume-manager closes its transport");

    assert_eq!(
        response.job.unwrap().job_id,
        "019d84bd-280d-7331-99c6-b63377f3a3a2"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn ensure_file_replica_does_not_retry_permanent_wait_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_wait = Arc::clone(&calls);

    let error = wait_transfer_job_with_retry(
        &"019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
        tokio::time::Instant::now(),
        Duration::from_secs(1),
        move |_| {
            calls_for_wait.fetch_add(1, Ordering::SeqCst);
            async { Err(tonic::Status::invalid_argument("invalid job id")) }
        },
    )
    .await
    .unwrap_err();

    let WaitTransferJobError::Rpc(error) = error else {
        panic!("permanent RPC error should be preserved");
    };
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ensure_file_replica_stops_wait_retry_at_total_wait_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_wait = Arc::clone(&calls);

    let error = wait_transfer_job_with_retry(
        &"019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
        tokio::time::Instant::now(),
        Duration::from_millis(50),
        move |_| {
            calls_for_wait.fetch_add(1, Ordering::SeqCst);
            async { Err(tonic::Status::unavailable("volume-manager restarting")) }
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(error, WaitTransferJobError::DeadlineExceeded));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ensure_file_replica_stops_retrying_at_total_wait_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_submit = Arc::clone(&calls);

    let error = submit_pull_file_with_retry(
        PullFileRequest::default(),
        tokio::time::Instant::now(),
        Duration::from_millis(50),
        move |_| {
            calls_for_submit.fetch_add(1, Ordering::SeqCst);
            async {
                Err(tonic::Status::resource_exhausted(
                    "pull job admission limit reached",
                ))
            }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn ensure_file_replica_replays_a_submission_cut_by_a_restart() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_submit = Arc::clone(&calls);

    let response = submit_pull_file_with_retry(
        PullFileRequest::default(),
        tokio::time::Instant::now(),
        Duration::from_secs(10_800),
        move |_| {
            let attempt = calls_for_submit.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(tonic::Status::unknown("transport error"))
                } else {
                    Ok(tonic::Response::new(SubmitTransferResponse {
                        job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
                    }))
                }
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(response.job_id, "019d84bd-280d-7331-99c6-b63377f3a3a2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn ensure_file_replica_stops_waiting_for_an_unreachable_volume_manager() {
    let started = tokio::time::Instant::now();

    let error = submit_pull_file_with_retry(
        PullFileRequest::default(),
        started,
        Duration::from_secs(10_800),
        |_| async {
            Err(tonic::Status::unavailable(
                "Connection refused (os error 111)",
            ))
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(started.elapsed() >= VOLUME_MANAGER_RESTART_BUDGET);
    assert!(started.elapsed() < VOLUME_MANAGER_RESTART_BUDGET + Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn cancel_transfer_scope_waits_through_a_volume_manager_handoff() {
    let scope = pplx_space_common_core::types::TransferScopeId::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_cancel = Arc::clone(&calls);

    let jobs = cancel_transfer_scope_with(scope, move |request| {
        let attempt = calls_for_cancel.fetch_add(1, Ordering::SeqCst);
        async move {
            assert_eq!(
                request.selector,
                Some(proto::cancel_transfer_request::Selector::TransferScopeId(
                    scope.to_string()
                ))
            );
            if attempt < 2 {
                Err(tonic::Status::unavailable(
                    "volume-manager is handing off; retry transfer submission",
                ))
            } else {
                Ok(tonic::Response::new(proto::CancelTransferResponse {
                    jobs: vec![proto::TransferJob {
                        job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
                        status: TransferJobStatus::Cancelled as i32,
                        ..Default::default()
                    }],
                }))
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(jobs.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn ensure_file_replica_gives_up_on_a_volume_manager_that_never_answers() {
    let started = tokio::time::Instant::now();

    let error = tokio::time::timeout(
        VOLUME_MANAGER_RESTART_BUDGET + Duration::from_secs(60),
        submit_pull_file_with_retry(
            PullFileRequest::default(),
            started,
            Duration::from_secs(10_800),
            |_| std::future::pending(),
        ),
    )
    .await
    .expect("an unanswered submission is bounded by the restart budget")
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert!(started.elapsed() >= VOLUME_MANAGER_RESTART_BUDGET);
}

#[tokio::test(start_paused = true)]
async fn cancel_transfer_scope_gives_up_on_a_volume_manager_that_never_answers() {
    let started = tokio::time::Instant::now();

    let error = tokio::time::timeout(
        VOLUME_MANAGER_RESTART_BUDGET + Duration::from_secs(60),
        cancel_transfer_scope_with(
            pplx_space_common_core::types::TransferScopeId::new(),
            |_| std::future::pending(),
        ),
    )
    .await
    .expect("an unanswered cancellation is bounded by the restart budget")
    .unwrap_err();

    assert!(
        matches!(&error, CancelTransferScopeError::Rpc(status) if status.code() == tonic::Code::DeadlineExceeded),
        "{error:?}"
    );
    assert!(started.elapsed() >= VOLUME_MANAGER_RESTART_BUDGET);
}
