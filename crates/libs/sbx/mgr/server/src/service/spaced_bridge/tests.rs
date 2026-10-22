use super::*;
use futures_util::StreamExt as _;
use pplx_space_common_core::types::{
    ExecutionGeneration, LinuxPid, LinuxUid, SandboxActivationAttemptId, SandboxId,
    SpaceApiRouteIdentity, VsockCid,
};

fn endpoint_proof(start_time_ticks: u64) -> crate::endpoint_proof::SpacedEndpointProof {
    crate::endpoint_proof::SpacedEndpointProof {
        sandbox_id: SandboxId::new(),
        execution_generation: ExecutionGeneration::new(1).unwrap(),
        activation_attempt_id: SandboxActivationAttemptId::new(),
        spaced_host: crate::endpoint_proof::ProcessActivationIdentity {
            pid: LinuxPid::try_from_u32(1).unwrap(),
            start_time_ticks,
            executable_device: 1,
            executable_inode: 1,
        },
        runtime_process: crate::endpoint_proof::ProcessActivationIdentity {
            pid: LinuxPid::try_from_u32(2).unwrap(),
            start_time_ticks: 2,
            executable_device: 2,
            executable_inode: 2,
        },
        vsock: crate::endpoint_proof::VsockActivationIdentity {
            path: "/run/v.sock".into(),
            device: 3,
            inode: 3,
            owner_uid: LinuxUid::new(1000),
            owner_gid: 1000,
            guest_cid: VsockCid::try_from_u32(3).unwrap(),
            runtime_fd: 4,
            runtime_socket_inode: 5,
        },
    }
}

#[test]
fn endpoint_runtime_identity_requires_complete_activation_identity() {
    let sandbox_id = SandboxId::new().to_string();
    let complete = SpaceApiRouteIdentity {
        execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        ..SpaceApiRouteIdentity::default()
    };

    endpoint_runtime_identity_for_activation(&sandbox_id, &complete, 1, 2, 1000, 3, "/run/v.sock")
        .expect("complete activation identity should be accepted");

    for incomplete in [
        SpaceApiRouteIdentity {
            execution_generation: None,
            ..complete.clone()
        },
        SpaceApiRouteIdentity {
            activation_attempt_id: None,
            ..complete.clone()
        },
    ] {
        let error = endpoint_runtime_identity_for_activation(
            &sandbox_id,
            &incomplete,
            1,
            2,
            1000,
            3,
            "/run/v.sock",
        )
        .expect_err("incomplete activation identity must fail closed");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
}

#[test]
fn identity_free_pool_donor_defers_endpoint_proof_until_adoption() {
    assert_eq!(
        activation_endpoint_proof_requirement(&SpaceApiRouteIdentity::default(), Some("seed"),),
        ActivationEndpointProofRequirement::DeferredUntilPoolAdoption
    );
    assert_eq!(
        activation_endpoint_proof_requirement(&SpaceApiRouteIdentity::default(), None),
        ActivationEndpointProofRequirement::Required
    );
    assert_eq!(
        activation_endpoint_proof_requirement(
            &SpaceApiRouteIdentity {
                execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
                ..SpaceApiRouteIdentity::default()
            },
            Some("seed"),
        ),
        ActivationEndpointProofRequirement::Required,
        "only a wholly identity-free donor may defer proof capture"
    );
}

#[test]
fn malformed_endpoint_proof_is_an_internal_failure() {
    let status =
        endpoint_proof_error_status(crate::endpoint_proof::SpacedEndpointProofError::Invalid(
            "node returned a malformed proof".to_string(),
        ));
    assert_eq!(status.code(), tonic::Code::Internal);

    let status = node_endpoint_proof_status(tonic::Status::invalid_argument(
        "node returned malformed endpoint state",
    ));
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn missing_endpoint_proof_rpc_is_retryable_during_rollout() {
    let status = node_endpoint_proof_status(tonic::Status::unimplemented(
        "endpoint proof RPC is not deployed yet",
    ));
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

#[test]
fn activation_capture_retries_only_transient_failures_with_a_bound() {
    let unavailable = tonic::Status::unavailable("node-manager is starting");
    assert!(activation_capture_retry_delay(0, &unavailable).is_some());
    assert!(activation_capture_retry_delay(1, &unavailable).is_some());
    assert!(activation_capture_retry_delay(2, &unavailable).is_none());

    let rejected = tonic::Status::permission_denied("endpoint identity mismatch");
    assert!(activation_capture_retry_delay(0, &rejected).is_none());
}

#[tokio::test(start_paused = true)]
async fn endpoint_proof_rpc_survives_transient_node_manager_unavailability() {
    let attempts = std::sync::atomic::AtomicUsize::new(0);
    let started = tokio::time::Instant::now();

    let result = call_endpoint_proof_rpc("validate_sandbox_endpoint_proof", || {
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let elapsed = started.elapsed();
        async move {
            if elapsed < std::time::Duration::from_secs(12) {
                Err(tonic::Status::unavailable("node-manager is restarting"))
            } else {
                Ok(())
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "transient restart should be retried: {result:?}"
    );
    assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) > 5);
    assert!(started.elapsed() >= std::time::Duration::from_secs(12));
}

#[tokio::test(start_paused = true)]
async fn endpoint_proof_rpc_does_not_replay_identity_rejection() {
    let attempts = std::sync::atomic::AtomicUsize::new(0);

    let result = call_endpoint_proof_rpc("validate_sandbox_endpoint_proof", || {
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async {
            Err::<(), _>(tonic::Status::permission_denied(
                "endpoint identity mismatch",
            ))
        }
    })
    .await;

    assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn endpoint_proof_cas_loss_is_retryable() {
    let status = endpoint_proof_cas_lost_status();
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(status.message().contains("retry"));
}

#[test]
fn endpoint_proof_cas_winner_must_match_this_callers_capture() {
    let captured = endpoint_proof(1);
    assert_eq!(
        resolve_endpoint_proof_cas_winner(&captured, captured.clone()).unwrap(),
        captured
    );

    let mut different_winner = captured.clone();
    different_winner.spaced_host.start_time_ticks = 2;
    let error = resolve_endpoint_proof_cas_winner(&captured, different_winner)
        .expect_err("a different winning proof must retry the complete request");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("retry"));
}

#[tokio::test]
async fn endpoint_attestation_accepts_uds_owned_by_persisted_uid() {
    use std::os::unix::fs::MetadataExt as _;

    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().join("spaced.sock");
    let _listener = tokio::net::UnixListener::bind(&path).expect("bind Unix socket");
    let uid = std::fs::symlink_metadata(&path)
        .expect("inspect Unix socket")
        .uid();

    validate_spaced_endpoint_path(&path, uid)
        .await
        .expect("matching socket ownership should attest");
}

#[tokio::test]
async fn endpoint_attestation_rejects_non_socket_and_wrong_owner() {
    use std::os::unix::fs::MetadataExt as _;

    let dir = tempfile::tempdir().expect("create temp dir");
    let regular_file = dir.path().join("not-a-socket");
    std::fs::write(&regular_file, []).expect("write regular file");
    let error = validate_spaced_endpoint_path(&regular_file, 0)
        .await
        .expect_err("regular files must not attest");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert_eq!(error.message(), "spaced-host endpoint is not a Unix socket");

    let socket_path = dir.path().join("wrong-owner.sock");
    let _listener = tokio::net::UnixListener::bind(&socket_path).expect("bind Unix socket");
    let actual_uid = std::fs::symlink_metadata(&socket_path)
        .expect("inspect Unix socket")
        .uid();
    let mismatched_uid = actual_uid.wrapping_add(1);
    let error = validate_spaced_endpoint_path(&socket_path, mismatched_uid)
        .await
        .expect_err("wrong owner must not attest");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(error.message().contains("endpoint owner mismatch"));
}

#[test]
fn process_info_to_spaced_preserves_process_config() {
    let info = host_proto::ProcessInfo {
        process_id: "proc-1".to_string(),
        pid: 123,
        command: vec![
            "python".to_string(),
            "-m".to_string(),
            "http.server".to_string(),
        ],
        cwd: Some("/workspace".to_string()),
        env: std::collections::HashMap::from([("PORT".to_string(), "8000".to_string())]),
        user: Some("user".to_string()),
        tag: Some("server".to_string()),
        pty: Some(true),
    };

    let spaced = process_info_to_spaced(info);

    assert_eq!(spaced.process_id, "proc-1");
    assert_eq!(spaced.pid, 123);
    assert_eq!(spaced.command, ["python", "-m", "http.server"]);
    assert_eq!(spaced.cwd.as_deref(), Some("/workspace"));
    assert_eq!(spaced.env.get("PORT").map(String::as_str), Some("8000"));
    assert_eq!(spaced.user.as_deref(), Some("user"));
    assert_eq!(spaced.tag.as_deref(), Some("server"));
    assert_eq!(spaced.pty, Some(true));
}

/// The guest's report of where a reconnected stream starts reaches the data
/// plane, which relies on it to tell evicted output apart.
#[test]
fn process_event_to_spaced_keeps_the_connected_output_cursor() {
    let event = host_proto::ProcessEvent {
        process_id: "proc-1".to_string(),
        pid: Some(123),
        event: Some(host_proto::process_event::Event::Connected(
            host_proto::ProcessConnected {
                output_cursor: Some(9),
            },
        )),
    };

    let spaced = process_event_to_spaced(event).unwrap();

    assert_eq!(
        spaced.event,
        Some(crate::proto::spaced_process_event::Event::Connected(
            SpacedProcessConnected {
                output_cursor: Some(9)
            }
        ))
    );
}

#[tokio::test]
async fn identity_fence_aborts_an_idle_stream() {
    let fence = RouteStreamFence::new();
    let stream: BoxedStatusStream<()> = Box::pin(futures_util::stream::pending());
    let mut fenced = fence_status_stream(stream, fence.child());

    fence.cancel(pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged);

    let error = fenced
        .next()
        .await
        .expect("fence must close with a status")
        .expect_err("identity replacement must abort the stream");
    assert_eq!(error.code(), tonic::Code::Aborted);
}

#[tokio::test]
async fn identity_fence_already_cancelled_beats_ready_upstream_eof() {
    // Both select branches are ready before the forwarding task can record a
    // terminal state. Even if the unbiased select observes EOF first, the
    // producer must re-check the fence rather than report successful EOF.
    for _ in 0..32 {
        let fence = RouteStreamFence::new();
        let child = fence.child();
        fence.cancel(pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged);
        let stream: BoxedStatusStream<()> = Box::pin(futures_util::stream::empty());
        let mut response = fence_status_stream(stream, child);

        let error = response
            .next()
            .await
            .expect("cancelled stream must produce a status")
            .expect_err("identity cancellation must beat unrecorded EOF");
        assert_eq!(error.code(), tonic::Code::Aborted);
        assert!(response.next().await.is_none());
    }
}

#[tokio::test]
async fn identity_fence_releases_admission_while_response_is_backpressured() {
    let fence = RouteStreamFence::new();
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let admission = slots
        .clone()
        .try_acquire_owned()
        .expect("initial admission");
    let stream: BoxedStatusStream<()> = Box::pin(futures_util::stream::repeat_with(|| Ok(())));
    let mut response = fence_admitted_status_stream(stream, fence.child(), admission);

    // Let forwarding fill its bounded response channel and park on send.
    tokio::time::sleep(Duration::from_millis(10)).await;
    fence.cancel(pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged);

    let _replacement_admission =
        tokio::time::timeout(Duration::from_secs(1), slots.clone().acquire_owned())
            .await
            .expect("fence cancellation must release admission")
            .expect("semaphore remains open");
    let error = response
        .next()
        .await
        .expect("cancellation must remain visible after backpressure")
        .expect_err("identity replacement must not look like successful EOF");
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(response.next().await.is_none());
}

#[tokio::test]
async fn response_drop_releases_admission_while_upstream_is_idle() {
    let fence = RouteStreamFence::new();
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let admission = slots
        .clone()
        .try_acquire_owned()
        .expect("initial admission");
    let stream: BoxedStatusStream<()> = Box::pin(futures_util::stream::pending());
    let response = fence_admitted_status_stream(stream, fence.child(), admission);

    drop(response);
    assert_eq!(
        slots.available_permits(),
        1,
        "response drop releases synchronously"
    );

    let _replacement_admission =
        tokio::time::timeout(Duration::from_secs(1), slots.clone().acquire_owned())
            .await
            .expect("response drop must release admission")
            .expect("semaphore remains open");
}

struct CompletionSignal(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for CompletionSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

// The producer drops its upstream stream after publishing the terminal state.
// Admission has a different lifetime: buffered output still owns it afterward.
fn observe_producer_completion<T: Send + 'static>(
    mut stream: BoxedStatusStream<T>,
) -> (BoxedStatusStream<T>, tokio::sync::oneshot::Receiver<()>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let signal = CompletionSignal(Some(tx));
    let stream = futures_util::stream::poll_fn(move |cx| {
        let _keep_alive = &signal;
        stream.as_mut().poll_next(cx)
    });
    (Box::pin(stream), rx)
}

#[tokio::test]
async fn late_identity_change_does_not_replace_completed_stream_outcome() {
    let fence = RouteStreamFence::new();
    let stream: BoxedStatusStream<u8> = Box::pin(futures_util::stream::iter([Ok(7)]));
    let (stream, completed_rx) = observe_producer_completion(stream);
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let admission = slots.clone().try_acquire_owned().unwrap();
    let mut response = fence_admitted_status_stream(stream, fence.child(), admission);

    tokio::time::timeout(Duration::from_secs(1), completed_rx)
        .await
        .expect("producer must finish without downstream polling")
        .expect("producer completed");
    assert_eq!(
        slots.available_permits(),
        0,
        "buffered output retains admission"
    );
    fence.cancel(pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged);

    assert_eq!(response.next().await.expect("data item").unwrap(), 7);
    assert!(response.next().await.is_none());
    assert_eq!(
        slots.available_permits(),
        0,
        "the response still owns completion"
    );
    drop(response);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn late_identity_change_does_not_replace_upstream_error() {
    let fence = RouteStreamFence::new();
    let stream: BoxedStatusStream<u8> = Box::pin(futures_util::stream::iter([
        Ok(7),
        Err(Status::unavailable("upstream unavailable")),
    ]));
    let (stream, completed_rx) = observe_producer_completion(stream);
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let admission = slots.clone().try_acquire_owned().unwrap();
    let mut response = fence_admitted_status_stream(stream, fence.child(), admission);

    tokio::time::timeout(Duration::from_secs(1), completed_rx)
        .await
        .expect("producer must finish without downstream polling")
        .expect("producer observed upstream error");
    assert_eq!(
        slots.available_permits(),
        0,
        "buffered output retains admission"
    );
    fence.cancel(pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged);

    assert_eq!(response.next().await.expect("data item").unwrap(), 7);
    let error = response
        .next()
        .await
        .expect("upstream error")
        .expect_err("upstream failure must remain visible");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "upstream unavailable");
    assert!(response.next().await.is_none());
    assert_eq!(
        slots.available_permits(),
        0,
        "the response still owns completion"
    );
    drop(response);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn producer_panic_is_an_explicit_stream_error() {
    let fence = RouteStreamFence::new();
    let stream: BoxedStatusStream<()> = Box::pin(futures_util::stream::poll_fn(|_| {
        panic!("injected producer panic");
    }));
    let mut response = fence_status_stream(stream, fence.child());

    let error = tokio::time::timeout(Duration::from_secs(1), response.next())
        .await
        .expect("producer failure must terminate the response")
        .expect("producer failure must produce a status")
        .expect_err("producer panic must not look like successful EOF");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "route stream forwarding task failed");
    assert!(response.next().await.is_none());
}
