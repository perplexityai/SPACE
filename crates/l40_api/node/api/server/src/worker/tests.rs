use super::*;
use pplx_space_sbx_mgr_client::proto;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[test]
fn wait_ready_policy_requires_authenticated_authority_and_lifecycle_purpose() {
    for (authority, purpose, expected) in [
        (
            Some(WaitReadyLifecycleAuthority::Granted),
            WaitReadyCallPurpose::LifecycleProbe,
            SpaceApiBridgeCallerPolicy::TrustedLifecycle,
        ),
        (
            Some(WaitReadyLifecycleAuthority::Granted),
            WaitReadyCallPurpose::PublicBridge,
            SpaceApiBridgeCallerPolicy::RouteTokenRequired,
        ),
        (
            Some(WaitReadyLifecycleAuthority::Denied),
            WaitReadyCallPurpose::LifecycleProbe,
            SpaceApiBridgeCallerPolicy::RouteTokenRequired,
        ),
        (
            None,
            WaitReadyCallPurpose::LifecycleProbe,
            SpaceApiBridgeCallerPolicy::RouteTokenRequired,
        ),
    ] {
        assert_eq!(classify_wait_ready_policy(authority, purpose), expected);
    }
}

#[test]
fn wait_ready_forward_replaces_caller_supplied_policy() {
    let mut request = pplx_space_api_route::wait_ready_lifecycle_request(());
    set_space_api_bridge_caller_policy(
        request.metadata_mut(),
        SpaceApiBridgeCallerPolicy::TrustedLifecycle,
    );

    prepare_wait_ready_forward(&mut request);

    assert_eq!(
        pplx_space_api_route::space_api_bridge_caller_policy(request.metadata()).unwrap(),
        Some(SpaceApiBridgeCallerPolicy::RouteTokenRequired)
    );
    assert_eq!(
        wait_ready_call_purpose(request.metadata()),
        WaitReadyCallPurpose::PublicBridge
    );
}

#[test]
fn public_worker_api_rejects_explicit_netns_path() {
    let netns_path = Some("/run/netns/not-public".to_string());
    let error = reject_public_netns_path("StartSandbox", &netns_path).unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("internal-only"));
}

#[test]
fn public_worker_api_rejects_explicit_suspend_restore_storage_uri() {
    let storage_uri = Some("s3://space/snapshot?region=us-east-1".to_string());
    let error =
        reject_public_suspend_restore_context("StartSandbox", &storage_uri, &None).unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("internal-only"));
}

#[test]
fn public_worker_api_rejects_explicit_suspend_restore_source_node() {
    let source_node = Some("space-worker-a".to_string());
    assert!(reject_public_suspend_restore_context("StartSandbox", &None, &source_node).is_err());
}

#[test]
fn public_worker_api_accepts_absent_netns_path() {
    reject_public_netns_path("StartSandbox", &None).unwrap();
}

#[test]
fn public_worker_api_accepts_absent_suspend_restore_context() {
    reject_public_suspend_restore_context("StartSandbox", &None, &None).unwrap();
}

#[test]
fn public_worker_api_never_accepts_an_explicit_operation_owner() {
    for operation in [
        None,
        Some(String::new()),
        Some(uuid::Uuid::now_v7().to_string()),
    ] {
        let result = reject_public_operation_activation_attempt(&operation);
        match operation {
            None => assert!(result.is_ok()),
            Some(_) => assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument),
        }
    }
}

#[tokio::test]
async fn service_port_ingress_first_frame_wait_is_bounded() {
    let error = read_service_port_ingress_first_frame(
        std::future::pending::<Result<Option<ServicePortIngressFrame>, Status>>(),
        Duration::from_millis(1),
    )
    .await
    .expect_err("idle service-port ingress stream should time out");

    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert!(error.message().contains("timed out"));
}

#[tokio::test]
async fn service_port_ingress_guarded_response_holds_permit_until_stream_dropped() {
    let limiter = UdsPerSandboxLimiter::service_port_ingress_for_tests(1, 1);
    let sandbox_id = SandboxId::new().to_string();
    let permit = limiter
        .acquire("ServicePortIngress", &sandbox_id)
        .await
        .expect("first ingress stream acquires permit");
    let response_stream = tokio_stream::iter([Err::<ServicePortIngressFrame, Status>(
        Status::deadline_exceeded("service port ingress stream idle timed out"),
    )]);
    let mut guarded = guard_stream(response_stream, permit);

    let rejected_while_guarded = limiter.acquire("ServicePortIngress", &sandbox_id).await;
    let Err(error) = rejected_while_guarded else {
        panic!("guarded stream should hold permit");
    };
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    let error = guarded
        .next()
        .await
        .expect("guarded stream yields idle timeout")
        .expect_err("idle timeout propagates through guarded stream");
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    drop(guarded);

    let _permit = limiter
        .acquire("ServicePortIngress", &sandbox_id)
        .await
        .expect("dropping closed guarded stream releases permit");
}

#[test]
fn policy_mutation_requires_control_plane_authority() {
    // Only a caller node-api classified as control-plane (Granted) may reach the
    // sbx-mgr policy-mutation RPCs. A data-plane token (Denied) is refused; an
    // unauthenticated request (auth layer off, local dev) forwards as before.
    check_policy_mutation_authority(Some(NetworkPolicyMutationAuthority::Granted))
        .expect("granted caller may mutate policy");
    check_policy_mutation_authority(None).expect("no auth layer forwards as before");
    let denied = check_policy_mutation_authority(Some(NetworkPolicyMutationAuthority::Denied))
        .expect_err("denied caller is refused");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

/// A sandbox-manager that answers every call with `Unimplemented`, so a call
/// node-api forwarded is told apart from one it refused at admission.
async fn unimplemented_sandbox_manager() -> crate::clients::BackendClients {
    let (channel, _) = answering_sandbox_manager(tonic::Code::Unimplemented, "").await;
    crate::clients::BackendClients::with_channel(channel)
}

/// A sandbox-manager that answers every call with `code` and `message`, and
/// counts the calls that reach it.
async fn answering_sandbox_manager(
    code: tonic::Code,
    message: &'static str,
) -> (tonic::transport::Channel, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let backend = axum::Router::new().fallback(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        async move {
            http::Response::builder()
                .header("content-type", "application/grpc")
                .header("grpc-status", (code as i32).to_string())
                .header("grpc-message", message)
                .body(axum::body::Body::empty())
                .unwrap()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, backend).await.unwrap() });
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, calls)
}

/// A sandbox-manager that opens every response stream and never sends on it,
/// as an idle file watch or a long-running process does. A file read sends
/// one empty message first: node-api holds a read's open until its first item.
async fn silent_stream_sandbox_manager() -> crate::clients::BackendClients {
    let backend = axum::Router::new().fallback(|uri: http::Uri| async move {
        // An empty message: uncompressed, zero length.
        let first = uri
            .path()
            .ends_with("/SpacedRead")
            .then(|| Ok(bytes::Bytes::from_static(&[0, 0, 0, 0, 0])));
        http::Response::builder()
            .header("content-type", "application/grpc")
            .body(axum::body::Body::from_stream(
                futures_util::stream::iter(first).chain(futures_util::stream::pending::<
                    Result<bytes::Bytes, std::io::Error>,
                >()),
            ))
            .unwrap()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, backend).await.unwrap() });
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    crate::clients::BackendClients::with_channel(channel)
}

async fn assert_open<T: std::fmt::Debug>(method: &str, stream: &mut PinnedResponseStream<T>) {
    let next = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(next.is_err(), "{method} ended early: {next:?}");
}

async fn assert_ended_by_drain<T: std::fmt::Debug>(
    method: &str,
    stream: &mut PinnedResponseStream<T>,
) {
    let status = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap_or_else(|_| panic!("{method} stayed open after the drain"))
        .unwrap_or_else(|| panic!("{method} ended without the drain status"))
        .expect_err("the drain ends the stream with a status");
    assert!(
        pplx_space_node_api_client::is_node_api_draining_status(&status),
        "{method}: {status:?}"
    );
    assert!(
        stream.next().await.is_none(),
        "{method} continued after the drain status"
    );
}

// Watches and process event streams have no natural end, so one held open
// would keep node-api draining, refusing every new call on the node, until
// its whole shutdown budget ran out. Bounded streams such as reads keep that
// budget to finish.
#[tokio::test]
async fn open_ended_streams_end_with_the_drain_status_once_drained() {
    let drain = CancellationToken::new();
    let service = WorkerApiService::new(silent_stream_sandbox_manager().await)
        .with_open_stream_drain_token(drain.clone());
    let mut watch = service
        .spaced_watch(Request::new(proto::SpacedWatchRequest {
            sandbox_id: SandboxId::new().to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut start = service
        .spaced_start_process(Request::new(proto::SpacedStartProcessRequest {
            sandbox_id: SandboxId::new().to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut connect = service
        .spaced_connect_process(Request::new(proto::SpacedConnectProcessRequest {
            sandbox_id: SandboxId::new().to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut read = service
        .spaced_read(Request::new(proto::SpacedReadRequest {
            sandbox_id: SandboxId::new().to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    read.next()
        .await
        .expect("SpacedRead delivers its first message")
        .expect("the first message is not an error");
    assert_open("SpacedWatch", &mut watch).await;
    assert_open("SpacedStartProcess", &mut start).await;
    assert_open("SpacedConnectProcess", &mut connect).await;

    drain.cancel();

    assert_ended_by_drain("SpacedWatch", &mut watch).await;
    assert_ended_by_drain("SpacedStartProcess", &mut start).await;
    assert_ended_by_drain("SpacedConnectProcess", &mut connect).await;
    assert_open("SpacedRead", &mut read).await;
}

async fn service_with_limiter(limiter: UdsPerSandboxLimiter) -> WorkerApiService {
    WorkerApiService {
        clients: unimplemented_sandbox_manager().await,
        uds_per_sandbox_limiter: limiter,
        service_port_ingress_limiter: shared_service_port_ingress_limiter(),
        resource_status_reader: NodeResourceStatusReader::new(),
        shutdown_token: CancellationToken::new(),
        open_stream_drain_token: CancellationToken::new(),
    }
}

#[tokio::test]
async fn process_control_is_admitted_while_streams_hold_every_shared_permit() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_reserved(1, 1, 8);
    let service = service_with_limiter(limiter.clone()).await;
    let sandbox_id = SandboxId::new().to_string();
    let _stream = limiter
        .acquire("SpacedConnectProcess", &sandbox_id)
        .await
        .unwrap();

    let list = service
        .spaced_list_processes(Request::new(proto::SpacedListProcessesRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    let signal = service
        .spaced_send_signal(Request::new(proto::SpacedSendSignalRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    let kill = service
        .spaced_kill_process(Request::new(proto::SpacedKillProcessRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    for (method, code) in [
        ("SpacedListProcesses", list.unwrap_err().code()),
        ("SpacedSendSignal", signal.unwrap_err().code()),
        ("SpacedKillProcess", kill.unwrap_err().code()),
    ] {
        assert_eq!(
            code,
            tonic::Code::Unimplemented,
            "{method} was not forwarded"
        );
    }

    let stat = service
        .spaced_stat(Request::new(proto::SpacedStatRequest {
            sandbox_id,
            ..Default::default()
        }))
        .await;
    assert_eq!(stat.unwrap_err().code(), tonic::Code::ResourceExhausted);
}

// Sandbox kill stops the VM from the host and never waits on the guest, so it
// must not queue behind process calls a hung guest holds.
#[tokio::test]
async fn sandbox_kill_is_admitted_while_every_sandbox_permit_is_held() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_reserved(1, 1, 8);
    let service = service_with_limiter(limiter.clone()).await;
    let sandbox_id = SandboxId::new().to_string();
    let _held = [
        limiter
            .acquire("SpacedConnectProcess", &sandbox_id)
            .await
            .unwrap(),
        limiter
            .acquire_in(SandboxBudget::Reserved, "SpacedListProcesses", &sandbox_id)
            .await
            .unwrap(),
    ];

    let process_kill = service
        .spaced_kill_process(Request::new(proto::SpacedKillProcessRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    assert_eq!(
        process_kill.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );

    let sandbox_kill = service
        .kill_sandbox(Request::new(proto::KillRequest {
            sandbox_id,
            signal: 9,
            ..Default::default()
        }))
        .await;
    assert_eq!(sandbox_kill.unwrap_err().code(), tonic::Code::Unimplemented);
}

/// Sandbox-manager authorizes an upgrade by its caller, which is node-api, so
/// node-api's internal Wake forwards one only for the control plane.
#[tokio::test]
async fn the_internal_wake_forwards_a_runtime_upgrade_only_for_the_control_plane() {
    use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternal;
    let service =
        service_with_limiter(UdsPerSandboxLimiter::for_tests_with_reserved(1, 1, 8)).await;
    let wake = || {
        Request::new(proto::WakeRequest {
            sandbox_id: SandboxId::new().to_string(),
            blocking: true,
            runtime_upgrade: Some(proto::WakeRuntimeUpgrade::default()),
            ..Default::default()
        })
    };

    let mut unprivileged = wake();
    unprivileged
        .extensions_mut()
        .insert(pplx_space_rpc_auth::NodeApiAuthenticatedCaller::unprivileged_workload_for_test());
    let denied = WorkerApiInternal::wake_sandbox(&service, unprivileged)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied, "{denied}");

    // The fake sandbox-manager answers whatever reaches it with Unimplemented.
    let forwarded = WorkerApiInternal::wake_sandbox(&service, wake())
        .await
        .unwrap_err();
    assert_eq!(forwarded.code(), tonic::Code::Unimplemented, "{forwarded}");
}

/// The outcome of a runtime upgrade a pod's shim ran is the control plane's
/// to read, as the upgrade was its to request.
#[tokio::test]
async fn the_internal_runtime_upgrade_report_is_forwarded_only_for_the_control_plane() {
    use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternal;
    let service =
        service_with_limiter(UdsPerSandboxLimiter::for_tests_with_reserved(1, 1, 8)).await;
    let read = || {
        Request::new(proto::GetRuntimeUpgradeRequest {
            sandbox_id: SandboxId::new().to_string(),
            operation_id: uuid::Uuid::now_v7().to_string(),
            attempt_id: None,
        })
    };

    let mut unprivileged = read();
    unprivileged
        .extensions_mut()
        .insert(pplx_space_rpc_auth::NodeApiAuthenticatedCaller::unprivileged_workload_for_test());
    let denied = WorkerApiInternal::get_runtime_upgrade(&service, unprivileged)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied, "{denied}");

    let forwarded = WorkerApiInternal::get_runtime_upgrade(&service, read())
        .await
        .unwrap_err();
    assert_eq!(forwarded.code(), tonic::Code::Unimplemented, "{forwarded}");
}

/// Sandbox-manager keeps serving guest reads while it drains, then ends every
/// call still in flight when it hands its socket to the successor. The status
/// carries no drain marker, as the call may have run, so only a call that is
/// safe to run twice may be replayed on the successor.
#[tokio::test]
async fn guest_reads_interrupted_by_a_handoff_are_answered_by_the_successor() {
    let (predecessor, _) = answering_sandbox_manager(
        tonic::Code::Unavailable,
        "sandbox-manager handoff interrupted an in-flight RPC",
    )
    .await;
    let (successor, successor_calls) =
        answering_sandbox_manager(tonic::Code::NotFound, "answered by the successor").await;
    let mut service =
        service_with_limiter(UdsPerSandboxLimiter::for_tests_with_reserved(8, 8, 64)).await;
    service.clients =
        crate::clients::BackendClients::with_channel(predecessor).with_sbx_successor(successor);
    let sandbox_id = || SandboxId::new().to_string();

    let reads = [
        (
            "SpacedStat",
            service
                .spaced_stat(Request::new(proto::SpacedStatRequest {
                    sandbox_id: sandbox_id(),
                    ..Default::default()
                }))
                .await
                .map(drop),
        ),
        (
            "SpacedListProcesses",
            service
                .spaced_list_processes(Request::new(proto::SpacedListProcessesRequest {
                    sandbox_id: sandbox_id(),
                    ..Default::default()
                }))
                .await
                .map(drop),
        ),
        (
            "SpacedNormalizeWatchRoot",
            service
                .spaced_normalize_watch_root(Request::new(proto::SpacedNormalizeWatchRootRequest {
                    sandbox_id: sandbox_id(),
                    ..Default::default()
                }))
                .await
                .map(drop),
        ),
        (
            "SpacedGetModifiedFiles",
            service
                .spaced_get_modified_files(Request::new(proto::SpacedGetModifiedFilesRequest {
                    sandbox_id: sandbox_id(),
                    ..Default::default()
                }))
                .await
                .map(drop),
        ),
    ];
    for (method, result) in reads {
        let status = result.unwrap_err();
        assert_eq!(
            (status.code(), status.message()),
            (tonic::Code::NotFound, "answered by the successor"),
            "{method} was not replayed on the successor"
        );
    }
    assert_eq!(successor_calls.load(Ordering::SeqCst), 4);

    // Calls that change or consume guest state may already have run.
    let writes = [
        (
            "SpacedWriteBatch",
            service
                .spaced_write_batch(Request::new(proto::SpacedWriteBatchRequest {
                    sandbox_id: sandbox_id(),
                    ..Default::default()
                }))
                .await
                .map(drop),
        ),
        (
            "SpacedSendSignal",
            service
                .spaced_send_signal(Request::new(proto::SpacedSendSignalRequest {
                    sandbox_id: sandbox_id(),
                    ..Default::default()
                }))
                .await
                .map(drop),
        ),
        (
            "SpacedGetWatcherEvents",
            service
                .spaced_get_watcher_events(Request::new(proto::SpacedGetWatcherEventsRequest {
                    sandbox_id: sandbox_id(),
                    ..Default::default()
                }))
                .await
                .map(drop),
        ),
    ];
    for (method, result) in writes {
        let status = result.unwrap_err();
        assert_eq!(
            (status.code(), status.message()),
            (
                tonic::Code::Unavailable,
                "sandbox-manager handoff interrupted an in-flight RPC"
            ),
            "{method} must surface the interruption unchanged"
        );
    }
    assert_eq!(successor_calls.load(Ordering::SeqCst), 4);
}

/// A sandbox-manager whose streams send `items` empty messages and then end
/// with `code` and `message`, as its handoff ends a stream it already
/// answered.
async fn streaming_sandbox_manager(
    items: usize,
    code: tonic::Code,
    message: &'static str,
) -> tonic::transport::Channel {
    struct Frames(std::collections::VecDeque<http_body::Frame<bytes::Bytes>>);
    impl http_body::Body for Frames {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            std::task::Poll::Ready(self.get_mut().0.pop_front().map(Ok))
        }
    }
    let backend = axum::Router::new().fallback(move || async move {
        // An empty message: no compression, zero length.
        let mut frames: std::collections::VecDeque<_> = (0..items)
            .map(|_| http_body::Frame::data(bytes::Bytes::from_static(&[0; 5])))
            .collect();
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", (code as i32).to_string().parse().unwrap());
        trailers.insert("grpc-message", message.parse().unwrap());
        frames.push_back(http_body::Frame::trailers(trailers));
        http::Response::builder()
            .header("content-type", "application/grpc")
            .body(axum::body::Body::new(Frames(frames)))
            .unwrap()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, backend).await.unwrap() });
    tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// Sandbox-manager ends the streams it is serving when it hands its socket to
/// the successor, after it already sent their response headers. A listing or
/// a file read that has delivered nothing yet is replayed on the successor.
/// Streams whose first item may take long, or that have side effects, are not.
#[tokio::test]
async fn guest_read_streams_interrupted_before_their_first_item_reach_the_successor() {
    const INTERRUPTED: &str = "sandbox-manager handoff interrupted an in-flight RPC";
    let predecessor = streaming_sandbox_manager(0, tonic::Code::Unavailable, INTERRUPTED).await;
    let (successor, successor_calls) =
        answering_sandbox_manager(tonic::Code::NotFound, "answered by the successor").await;
    let mut service =
        service_with_limiter(UdsPerSandboxLimiter::for_tests_with_reserved(8, 8, 64)).await;
    service.clients =
        crate::clients::BackendClients::with_channel(predecessor).with_sbx_successor(successor);
    let sandbox_id = || SandboxId::new().to_string();

    let list = service
        .spaced_list(Request::new(proto::SpacedListRequest {
            sandbox_id: sandbox_id(),
            ..Default::default()
        }))
        .await;
    let read = service
        .spaced_read(Request::new(proto::SpacedReadRequest {
            sandbox_id: sandbox_id(),
            ..Default::default()
        }))
        .await;
    for (method, status) in [
        ("SpacedList", list.map(drop).unwrap_err()),
        ("SpacedRead", read.map(drop).unwrap_err()),
    ] {
        assert_eq!(
            (status.code(), status.message()),
            (tonic::Code::NotFound, "answered by the successor"),
            "{method} was not replayed on the successor"
        );
    }
    assert_eq!(successor_calls.load(Ordering::SeqCst), 2);

    let watch = service
        .spaced_watch(Request::new(proto::SpacedWatchRequest {
            sandbox_id: sandbox_id(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .next()
        .await;
    let connect = service
        .spaced_connect_process(Request::new(proto::SpacedConnectProcessRequest {
            sandbox_id: sandbox_id(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .next()
        .await;
    let start = service
        .spaced_start_process(Request::new(proto::SpacedStartProcessRequest {
            sandbox_id: sandbox_id(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .next()
        .await;
    for (method, first) in [
        ("SpacedWatch", watch.map(|item| item.map(drop))),
        ("SpacedConnectProcess", connect.map(|item| item.map(drop))),
        ("SpacedStartProcess", start.map(|item| item.map(drop))),
    ] {
        let status = first.unwrap().unwrap_err();
        assert_eq!(
            (status.code(), status.message()),
            (tonic::Code::Unavailable, INTERRUPTED),
            "{method} must surface the interruption unchanged"
        );
    }
    assert_eq!(successor_calls.load(Ordering::SeqCst), 2);
}

/// The guest read streams `rpc` names, on `service`, as streams of unit items.
async fn open_guest_read_stream(
    service: &WorkerApiService,
    rpc: &str,
) -> Result<PinnedResponseStream<()>, Status> {
    let sandbox_id = SandboxId::new().to_string();
    Ok(match rpc {
        "SpacedList" => pin_stream(
            service
                .spaced_list(Request::new(proto::SpacedListRequest {
                    sandbox_id,
                    ..Default::default()
                }))
                .await?
                .into_inner()
                .map(|item| item.map(drop)),
        ),
        "SpacedRead" => pin_stream(
            service
                .spaced_read(Request::new(proto::SpacedReadRequest {
                    sandbox_id,
                    ..Default::default()
                }))
                .await?
                .into_inner()
                .map(|item| item.map(drop)),
        ),
        _ => unreachable!("not a guest read stream: {rpc}"),
    })
}

/// A guest read stream whose predecessor sends `items` items and then ends
/// with `code`, behind a successor that answers every call `NotFound`.
async fn guest_read_stream_service(
    items: usize,
    code: tonic::Code,
    message: &'static str,
) -> (WorkerApiService, Arc<AtomicUsize>) {
    let predecessor = streaming_sandbox_manager(items, code, message).await;
    let (successor, successor_calls) =
        answering_sandbox_manager(tonic::Code::NotFound, "answered by the successor").await;
    let mut service =
        service_with_limiter(UdsPerSandboxLimiter::for_tests_with_reserved(8, 8, 64)).await;
    service.clients =
        crate::clients::BackendClients::with_channel(predecessor).with_sbx_successor(successor);
    (service, successor_calls)
}

/// Once a listing or a file read delivered an item, the successor would
/// deliver it again, so an interruption after it reaches the caller unchanged.
#[tokio::test]
async fn a_guest_read_stream_that_delivered_an_item_is_never_replayed() {
    const INTERRUPTED: &str = "sandbox-manager handoff interrupted an in-flight RPC";
    for rpc in ["SpacedList", "SpacedRead"] {
        let (service, successor_calls) =
            guest_read_stream_service(1, tonic::Code::Unavailable, INTERRUPTED).await;

        let mut items = open_guest_read_stream(&service, rpc).await.unwrap();
        items.next().await.unwrap().unwrap();
        let status = items.next().await.unwrap().unwrap_err();
        assert_eq!(
            (status.code(), status.message()),
            (tonic::Code::Unavailable, INTERRUPTED),
            "{rpc}"
        );
        assert!(items.next().await.is_none(), "{rpc}");
        assert_eq!(successor_calls.load(Ordering::SeqCst), 0, "{rpc}");
    }
}

/// A first item the read probe would not replay reaches the caller as the
/// stream's first item, and a stream that ends with no items stays empty.
#[tokio::test]
async fn guest_read_streams_pass_through_first_items_that_are_not_replayed() {
    for rpc in ["SpacedList", "SpacedRead"] {
        let (service, successor_calls) =
            guest_read_stream_service(0, tonic::Code::NotFound, "no such file").await;
        let mut items = open_guest_read_stream(&service, rpc).await.unwrap();
        let status = items.next().await.unwrap().unwrap_err();
        assert_eq!(
            (status.code(), status.message()),
            (tonic::Code::NotFound, "no such file"),
            "{rpc}"
        );
        assert_eq!(successor_calls.load(Ordering::SeqCst), 0, "{rpc}");

        let (service, successor_calls) = guest_read_stream_service(0, tonic::Code::Ok, "").await;
        let mut items = open_guest_read_stream(&service, rpc).await.unwrap();
        assert!(items.next().await.is_none(), "{rpc} is not empty");
        assert_eq!(successor_calls.load(Ordering::SeqCst), 0, "{rpc}");
    }
}
