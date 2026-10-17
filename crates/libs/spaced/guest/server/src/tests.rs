use super::*;
use tokio::sync::Notify;

#[tokio::test]
async fn listener_failures_preserve_drain_and_initial_error_priority() {
    use futures_util::FutureExt;
    use pplx_space_spaced_common::BindUdsError;
    use std::sync::atomic::{AtomicBool, Ordering};

    for initial_failure in [false, true] {
        for fail_local in [false, true] {
            let shutdown = CancellationToken::new();
            let service = SpacedGuestService::with_config(
                shutdown.clone(),
                SpacedGuestServiceConfig::default(),
            );
            if !initial_failure {
                shutdown.cancel();
            }
            let drained = AtomicBool::new(false);
            let listener = |fails_first: bool| {
                let shutdown = &shutdown;
                let drained = &drained;
                async move {
                    if !initial_failure || !fails_first {
                        shutdown.cancelled().await;
                        // Let shutdown win the initial select for drain failures.
                        tokio::task::yield_now().await;
                    }
                    if !fails_first {
                        drained.store(true, Ordering::Relaxed);
                    }
                    if fails_first || initial_failure {
                        Err(BindUdsError::Bind {
                            path: PathBuf::from("failed.sock"),
                            source: std::io::ErrorKind::BrokenPipe.into(),
                        })
                    } else {
                        Ok(())
                    }
                }
            };
            let result = service
                .serve_until_shutdown(
                    listener(fail_local).map(|r| r.map_err(RunLocalGuestServerError::Bind)),
                    listener(!fail_local).map(|r| r.map_err(RunBridgeGuestServerError::BindUds)),
                )
                .await;
            assert!(
                drained.load(Ordering::Relaxed),
                "sibling must finish draining"
            );
            assert!(match result {
                Err(RunGuestServerError::Local(_)) => fail_local,
                Err(RunGuestServerError::Bridge(_)) => !fail_local,
                Ok(()) => false,
            });
        }
    }
}

#[tokio::test(start_paused = true)]
async fn local_metadata_admission_bounds_rates_and_reserves_health() {
    use tonic::{body::Body, codegen::http};
    use tower::Service;
    let mut server = SpacedGuestService::with_config(
        CancellationToken::new(),
        SpacedGuestServiceConfig::default(),
    )
    .local_grpc_service();
    let request = |method| {
        http::Request::builder()
            .uri(format!("/space.spaced.guest.v1.Meta/{method}"))
            .header("content-type", "application/grpc")
            .body(Body::empty())
            .unwrap()
    };
    // Hold signal bodies before decoding, across rate windows. Health must
    // have separate admission even when forwarding work stops progressing.
    let mut pending = Vec::new();
    for _ in 0..7 {
        pending.push(server.call(request("SetActivityState")));
        tokio::time::advance(Duration::from_millis(250)).await;
    }
    let response = server.call(request("Ping")).await.unwrap();
    assert_ne!(response.headers()["grpc-status"], "8", "health reserved");
    let response = server.call(request("RequestShutdown")).await.unwrap();
    assert_eq!(response.headers()["grpc-status"], "8", "signal capacity");
    drop(pending);
    // Malformed requests spend rate budget before decoding, shared by clones.
    // Separate windows prove recovery without spawning a reset timer.
    for (methods, limit) in [
        (&["Ping"][..], 16),
        (
            &["SetActivityState", "RequestActiveFor", "RequestShutdown"][..],
            5,
        ),
    ] {
        tokio::time::advance(Duration::from_secs(1)).await;
        for index in 0..=limit {
            let response = server
                .clone()
                .call(request(methods[index % methods.len()]))
                .await
                .unwrap();
            assert_eq!(
                response.headers()["grpc-status"] == "8",
                index == limit,
                "{methods:?}: request {index} of {limit}"
            );
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        let response = server.call(request(methods[0])).await.unwrap();
        assert_ne!(response.headers()["grpc-status"], "8", "window recovered");
    }
}

#[tokio::test(start_paused = true)]
async fn disabled_listeners_observe_shutdown() {
    let shutdown = CancellationToken::new();
    let service = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            host_connector_type: HostConnectorType::Uds(PathBuf::from("unused.sock")),
            local_socket_path: None,
            bridge_socket_path: None,
            ..Default::default()
        },
    );
    shutdown.cancel();
    // Test the disabled listeners directly, without unrelated tests' shared
    // process-start gate advancing this runtime's paused clock.
    let (local, bridge) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            service.clone().run_local_server(),
            service.run_bridge_guest_server()
        )
    })
    .await
    .unwrap();
    local.unwrap();
    bridge.unwrap();
}

#[tokio::test]
async fn bridge_host_retries_preserve_generation_and_do_not_retry_overload() {
    let mut service = SpacedGuestService::with_config(
        CancellationToken::new(),
        SpacedGuestServiceConfig {
            host_connector_type: HostConnectorType::Uds(PathBuf::from("unused-test-host.sock")),
            ..Default::default()
        },
    );
    Arc::get_mut(&mut service.inner)
        .unwrap()
        .bridge_retry_policy
        .max_retry_count = 0;
    let make_client = || async {
        Ok::<_, Status>(BridgeHostClient::new(
            tonic::transport::Endpoint::from_static("http://[::]:50051").connect_lazy(),
        ))
    };
    let first = service
        .inner
        .bridge_host_client
        .get_or_connect_with_generation(make_client)
        .await
        .unwrap();
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let caller = service.clone();
    let called = entered.clone();
    let reply = resume.clone();
    let stale = tokio::spawn(async move {
        caller
            .call_bridge_host_response("stale-test", move |_| {
                let called = called.clone();
                let reply = reply.clone();
                async move {
                    called.notify_one();
                    reply.notified().await;
                    Err::<Response<()>, _>(Status::unavailable("old channel failed"))
                }
            })
            .await
    });
    entered.notified().await;
    service
        .inner
        .bridge_host_client
        .clear_if_generation(first.generation())
        .await;
    let replacement = service
        .inner
        .bridge_host_client
        .get_or_connect_with_generation(make_client)
        .await
        .unwrap();
    resume.notify_one();
    stale
        .await
        .unwrap()
        .expect_err("stale host generation fails");
    let current = service
        .inner
        .bridge_host_client
        .get_or_connect_with_generation(make_client)
        .await
        .unwrap();
    assert_eq!(current.generation(), replacement.generation());
    Arc::get_mut(&mut service.inner)
        .unwrap()
        .bridge_retry_policy = DEFAULT_COMMAND_RETRY_POLICY;
    let attempts = std::sync::atomic::AtomicUsize::new(0);
    let error = service
        .call_bridge_host_response("overload", |_| {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Err::<Response<()>, _>(Status::resource_exhausted("host busy")) }
        })
        .await
        .expect_err("host overload remains visible");
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test]
async fn process_adapter_requires_guest_readiness_and_observes_shutdown() {
    let shutdown = CancellationToken::new();
    let service = SpacedGuestService::new(shutdown.clone());
    let request = || Request::new(ListProcessesRequest {});
    assert_eq!(
        Process::list_processes(&service, request())
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition,
    );
    service.ready();
    let mut processes = Process::list_processes(&service, request())
        .await
        .unwrap()
        .into_inner();
    use futures_util::StreamExt;
    assert!(processes.next().await.is_none());
    shutdown.cancel();
    assert_eq!(
        Process::list_processes(&service, request())
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::Unavailable,
    );
}
