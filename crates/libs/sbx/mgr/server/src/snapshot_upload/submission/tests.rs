use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

struct Unary<F>(F);

impl<Req, Resp, F> tonic::server::UnaryService<Req> for Unary<F>
where
    F: Fn(Req) -> Result<Resp, Status> + Send + 'static,
    Resp: Send + 'static,
{
    type Response = Resp;
    type Future = std::future::Ready<Result<tonic::Response<Resp>, Status>>;

    fn call(&mut self, request: tonic::Request<Req>) -> Self::Future {
        std::future::ready((self.0)(request.into_inner()).map(tonic::Response::new))
    }
}

async fn answer<Req, Resp>(
    request: http::Request<tonic::body::Body>,
    handler: impl Fn(Req) -> Result<Resp, Status> + Send + 'static,
) -> http::Response<tonic::body::Body>
where
    Req: prost::Message + Default + Send + 'static,
    Resp: prost::Message + Send + 'static,
{
    tonic::server::Grpc::new(tonic_prost::ProstCodec::<Resp, Req>::default())
        .unary(Unary(handler), request)
        .await
}

#[derive(Default)]
struct AdmissionState {
    jobs: std::collections::BTreeMap<String, proto::TransferJob>,
    submissions: std::collections::BTreeMap<String, usize>,
    response: Option<Code>,
    conflicting_source: bool,
    revoke_owner_on_get: Option<Arc<AtomicBool>>,
}

#[tokio::test]
async fn replay_joins_ambiguous_and_partial_admission_without_duplicate_transfers() {
    for ambiguous in [Code::DeadlineExceeded, Code::AlreadyExists] {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("volume.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let state = Arc::new(Mutex::new(AdmissionState {
            response: Some(ambiguous),
            ..Default::default()
        }));
        let observed = state.clone();
        let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
            let method = request.uri().path().rsplit('/').next().unwrap().to_owned();
            let state = observed.clone();
            async move {
                Ok::<_, std::convert::Infallible>(match method.as_str() {
                    "ListTransferJobs" => {
                        answer(request, move |request: proto::ListTransferJobsRequest| {
                            Ok(proto::ListTransferJobsResponse {
                                exact_destination_lookup: true,
                                jobs: state
                                    .lock()
                                    .unwrap()
                                    .jobs
                                    .values()
                                    .filter(|job| Some(&job.s3_uri) == request.s3_uri.as_ref())
                                    .cloned()
                                    .collect(),
                            })
                        })
                        .await
                    }
                    "SubmitPushFile" => {
                        answer(request, move |request: proto::PushFileRequest| {
                            let mut state = state.lock().unwrap();
                            *state.submissions.entry(request.s3_uri.clone()).or_default() += 1;
                            let response = state.response.take();
                            if response == Some(Code::Unavailable) {
                                return Err(Status::unavailable("admission temporarily closed"));
                            }
                            let job_id = JobId::new().to_string();
                            let job = proto::TransferJob {
                                job_id: job_id.clone(),
                                subject_id: request.subject_id,
                                s3_uri: request.s3_uri.clone(),
                                local_path: if state.conflicting_source {
                                    "/test/different-capture".to_owned()
                                } else {
                                    request.local_path
                                },
                                kind: "push".to_owned(),
                                status: proto::TransferJobStatus::Running as i32,
                                ..Default::default()
                            };
                            state.jobs.insert(request.s3_uri, job);
                            match response {
                                Some(code) => {
                                    Err(Status::new(code, "response after accepting work"))
                                }
                                None => Ok(proto::SubmitTransferResponse { job_id }),
                            }
                        })
                        .await
                    }
                    "GetTransferJob" => {
                        answer(request, move |request: proto::GetTransferJobRequest| {
                            let state = state.lock().unwrap();
                            if let Some(owner) = &state.revoke_owner_on_get {
                                owner.store(false, Ordering::SeqCst);
                            }
                            Ok(proto::GetTransferJobResponse {
                                job: state
                                    .jobs
                                    .values()
                                    .find(|job| job.job_id == request.job_id)
                                    .cloned(),
                            })
                        })
                        .await
                    }
                    _ => Status::unimplemented(method).into_http(),
                })
            }
        });
        let stop = tokio_util::sync::CancellationToken::new();
        let server = tokio::spawn(
            tonic::transport::Server::builder().serve_with_incoming_shutdown(
                rpc,
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
        );
        let client = pplx_space_volmgr_client::lazy(&socket);
        let make_transfer = |name: &str| {
            PreparedTransfer::File(proto::PushFileRequest {
                subject_id: "sandbox:synthetic-handoff".to_owned(),
                local_path: format!("/test/capture/{name}"),
                s3_uri: format!("s3://test-bucket/attempt/{name}"),
            })
        };
        let first = make_transfer("memory");
        let second = make_transfer("state");
        let context = GrpcRetryContext::with_timeout(Duration::from_secs(5));
        let first_id = first
            .admit(
                &client,
                &context,
                HANDOFF_RETRY_POLICY,
                SnapshotArtifact::Memory,
                &|| Ok(()),
                &|_| {},
            )
            .await
            .unwrap();
        state.lock().unwrap().response = Some(Code::Unavailable);
        let single_attempt = GrpcRetryPolicy {
            max_retries: 0,
            ..HANDOFF_RETRY_POLICY
        };
        assert!(
            second
                .admit(
                    &client,
                    &context,
                    single_attempt,
                    SnapshotArtifact::VmState,
                    &|| Ok(()),
                    &|_| {}
                )
                .await
                .unwrap_err()
                .is_transient()
        );
        // A later durable attempt starts at the beginning but must only submit
        // the missing artifact, not duplicate the previously accepted one.
        assert_eq!(
            first
                .admit(
                    &client,
                    &context,
                    HANDOFF_RETRY_POLICY,
                    SnapshotArtifact::Memory,
                    &|| Ok(()),
                    &|_| {}
                )
                .await
                .unwrap(),
            first_id
        );
        second
            .admit(
                &client,
                &context,
                HANDOFF_RETRY_POLICY,
                SnapshotArtifact::VmState,
                &|| Ok(()),
                &|_| {},
            )
            .await
            .unwrap();
        {
            let mut state = state.lock().unwrap();
            assert_eq!(state.submissions["s3://test-bucket/attempt/memory"], 1);
            assert_eq!(state.submissions["s3://test-bucket/attempt/state"], 2);
            assert_eq!(state.jobs.len(), 2);
            state.response = Some(Code::AlreadyExists);
            state.conflicting_source = true;
        }
        let conflict = make_transfer("metadata")
            .admit(
                &client,
                &context,
                HANDOFF_RETRY_POLICY,
                SnapshotArtifact::Metadata,
                &|| Ok(()),
                &|_| {},
            )
            .await
            .unwrap_err();
        assert_eq!(conflict.status().unwrap().code(), Code::FailedPrecondition);
        let owner = Arc::new(AtomicBool::new(true));
        {
            let mut state = state.lock().unwrap();
            state.conflicting_source = false;
            state.revoke_owner_on_get = Some(owner.clone());
        }
        let superseded = make_transfer("owner-race")
            .admit(
                &client,
                &context,
                HANDOFF_RETRY_POLICY,
                SnapshotArtifact::Metadata,
                &|| {
                    if owner.load(Ordering::SeqCst) {
                        Ok(())
                    } else {
                        Err(Status::failed_precondition("execution owner changed"))
                    }
                },
                &|_| {},
            )
            .await
            .unwrap_err();
        assert_eq!(
            superseded.status().unwrap().code(),
            Code::FailedPrecondition
        );
        stop.cancel();
        server.await.unwrap().unwrap();
    }
}

#[test]
fn only_explicit_transient_causes_cross_the_handoff_boundary() {
    use pplx_space_common_grpc::{rate_limited_status, refused_before_start};
    for code in [
        Code::Ok,
        Code::Cancelled,
        Code::Unknown,
        Code::InvalidArgument,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::FailedPrecondition,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
        Code::Unauthenticated,
    ] {
        let status = Status::new(code, "connection refused; retry in 5 seconds");
        assert_eq!(
            transient_admission_status(&status),
            matches!(code, Code::Unavailable | Code::DeadlineExceeded),
            "{code:?}"
        );
    }
    assert!(transient_admission_status(&rate_limited_status(
        "temporary throttle",
        Some(Duration::from_secs(7))
    )));
    assert!(!transient_admission_status(&refused_before_start(
        Status::unavailable("caller refusal"),
        Duration::from_secs(1)
    )));
    for kind in [
        std::io::ErrorKind::ConnectionRefused,
        std::io::ErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied,
    ] {
        let mut status = Status::internal("transport");
        status.set_source(std::sync::Arc::new(std::io::Error::from(kind)));
        assert_eq!(
            transient_admission_status(&status),
            kind != std::io::ErrorKind::PermissionDenied
        );
    }
}

#[tokio::test(start_paused = true)]
async fn all_artifacts_spend_one_budget_and_terminal_causes_do_not_retry() {
    let start = tokio::time::Instant::now();
    let context = GrpcRetryContext::with_timeout(Duration::from_secs(120));
    let accepted = JobId::new();
    let mut calls = Vec::new();
    let first = retry_admission(
        &context,
        HANDOFF_RETRY_POLICY,
        SnapshotArtifact::Memory,
        || {
            let elapsed = start.elapsed();
            calls.push(elapsed);
            std::future::ready(if elapsed < Duration::from_secs(95) {
                Err(Status::unavailable("handoff"))
            } else {
                Ok(accepted)
            })
        },
    )
    .await
    .unwrap();
    assert_eq!(first, accepted);
    for (index, pair) in calls.windows(2).enumerate() {
        let nominal = (100_u64.saturating_mul(2_u64.pow(index.min(4) as u32))).min(1000);
        let delay = pair[1] - pair[0];
        assert!(delay >= Duration::from_millis(nominal * 8 / 10));
        assert!(delay <= Duration::from_millis(nominal * 12 / 10));
    }
    let exhausted = retry_admission(
        &context,
        HANDOFF_RETRY_POLICY,
        SnapshotArtifact::VmState,
        || std::future::ready(Err(Status::unavailable("still handing off"))),
    )
    .await
    .unwrap_err();
    assert!(exhausted.is_transient());
    assert!(start.elapsed() <= Duration::from_secs(120));
    assert!(start.elapsed() >= Duration::from_millis(118_700));
    let mut calls = 0;
    let fresh = GrpcRetryContext::with_timeout(Duration::from_secs(120));
    let error = retry_admission(
        &fresh,
        HANDOFF_RETRY_POLICY,
        SnapshotArtifact::VmState,
        || {
            calls += 1;
            std::future::ready(Err(Status::internal("connection refused")))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(error.status().unwrap().code(), Code::Internal);
    let refusal = retry_admission(
        &fresh,
        HANDOFF_RETRY_POLICY,
        SnapshotArtifact::Metadata,
        || {
            calls += 1;
            std::future::ready(Err(pplx_space_common_grpc::refused_before_start(
                Status::unavailable("caller refusal"),
                Duration::ZERO,
            )))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(calls, 2);
    assert!(!refusal.is_transient());
    let cancellation = retry_admission(
        &fresh,
        HANDOFF_RETRY_POLICY,
        SnapshotArtifact::Metadata,
        || {
            calls += 1;
            std::future::ready(Err(Status::cancelled("caller cancelled")))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(calls, 3, "caller cancellation must not be retried");
    assert_eq!(cancellation.status().unwrap().code(), Code::Cancelled);
}

#[test]
fn rejoin_requires_the_same_retained_source_not_just_a_destination() {
    let request = proto::PushBtrfsSendRequest {
        sandbox_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".to_owned(),
        source_id: "019e31d9-79a6-7132-b90f-007f1c62ddeb".to_owned(),
        artifact_dir: "/test/capture".to_owned(),
        s3_uri: "s3://test-bucket/attempt/rootfs".to_owned(),
    };
    let transfer = PreparedTransfer::Btrfs(request.clone());
    let (subject_id, local_path, uri, kind) = transfer.identity();
    let mut job = proto::TransferJob {
        job_id: JobId::new().to_string(),
        subject_id,
        local_path,
        s3_uri: uri.to_owned(),
        kind: kind.to_owned(),
        status: proto::TransferJobStatus::Completed as i32,
        dedup_key: Some(format!(
            "{}:{}:{}",
            request.sandbox_id, request.source_id, request.s3_uri
        )),
        ..Default::default()
    };
    transfer.verify(&job).unwrap();
    job.dedup_key = Some("different retained source".to_owned());
    assert_eq!(
        transfer.verify(&job).unwrap_err().code(),
        Code::FailedPrecondition
    );
}
