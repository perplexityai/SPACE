#![cfg(test)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pplx_space_common_grpc::{
    GrpcClient, GrpcRetryContext, GrpcRetryPolicy, RateLimited, SandboxServiceRouteNotReady,
    is_temporary_tcp_failure, rate_limited_status, retry_grpc_status_with_context,
    retry_grpc_status_with_context_if, sandbox_service_route_not_ready_status,
    should_reconnect_status, uds_per_sandbox_limit_exceeded_status,
};
use tonic::{Code, Status};

fn test_policy() -> GrpcRetryPolicy {
    GrpcRetryPolicy {
        max_retries: 4,
        initial_delay: Duration::ZERO,
        backoff_multiplier: 2.0,
        max_delay: Duration::ZERO,
        max_elapsed: Duration::from_secs(1),
        jitter_factor: 0.0,
        max_server_retry_after: Duration::from_millis(10),
    }
}

fn one_retry_policy() -> GrpcRetryPolicy {
    GrpcRetryPolicy {
        max_retries: 1,
        ..test_policy()
    }
}

#[test]
fn backoff_delay_is_deterministic_without_jitter() {
    let policy = GrpcRetryPolicy {
        initial_delay: Duration::from_millis(20),
        max_delay: Duration::from_millis(100),
        jitter_factor: 0.0,
        ..test_policy()
    };
    assert_eq!(policy.backoff_delay(0), Some(Duration::from_millis(20)));
    assert_eq!(policy.backoff_delay(1), Some(Duration::from_millis(40)));
    assert_eq!(policy.backoff_delay(2), Some(Duration::from_millis(80)));
    assert_eq!(policy.backoff_delay(3), Some(Duration::from_millis(100)));
    assert_eq!(policy.backoff_delay(4), None);
}

#[test]
fn parses_rate_limited_status_from_metadata() {
    let status = rate_limited_status("rate limited", Some(Duration::from_millis(25)));
    assert_eq!(
        RateLimited::from_status(&status),
        Some(RateLimited {
            try_again: Some(Duration::from_millis(25))
        })
    );
}

#[test]
fn parses_rate_limited_status_from_legacy_message() {
    let status =
        Status::resource_exhausted("UDS gRPC request-rate limit exceeded; retry_after_ms=42");
    assert_eq!(
        RateLimited::from_status(&status),
        Some(RateLimited {
            try_again: Some(Duration::from_millis(42))
        })
    );
}

#[test]
fn parses_sandbox_service_route_not_ready_status() {
    let status =
        sandbox_service_route_not_ready_status(Code::FailedPrecondition, "guest still converging");
    assert_eq!(
        SandboxServiceRouteNotReady::from_metadata(status.metadata()),
        Some(SandboxServiceRouteNotReady)
    );
    assert_eq!(
        SandboxServiceRouteNotReady::from_status(&status),
        Some(SandboxServiceRouteNotReady)
    );

    let legacy = Status::unavailable("sandbox service route port exposure is not applied");
    assert_eq!(
        SandboxServiceRouteNotReady::from_metadata(legacy.metadata()),
        None
    );
    assert_eq!(
        SandboxServiceRouteNotReady::from_status(&legacy),
        Some(SandboxServiceRouteNotReady)
    );

    assert_eq!(
        SandboxServiceRouteNotReady::from_status(&Status::failed_precondition("runtime missing")),
        None
    );

    let wrong_code =
        sandbox_service_route_not_ready_status(Code::Internal, "carries marker metadata");
    assert_eq!(SandboxServiceRouteNotReady::from_status(&wrong_code), None);
}

#[test]
fn reconnect_status_classifier_is_transport_shaped() {
    for code in [
        Code::Unavailable,
        Code::Unknown,
        Code::Cancelled,
        Code::DeadlineExceeded,
    ] {
        assert!(should_reconnect_status(&Status::new(code, "transport")));
    }

    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::PermissionDenied,
        Code::FailedPrecondition,
    ] {
        assert!(!should_reconnect_status(&Status::new(code, "application")));
    }
}

#[test]
fn temporary_tcp_classifier_matches_tonic_internal_transport_messages() {
    for message in [
        "transport error: connection closed before message completed",
        "h2 protocol error: http2 error",
        "error trying to connect: connection refused",
    ] {
        let status = Status::internal(message);
        assert!(is_temporary_tcp_failure(&status));
        assert!(should_reconnect_status(&status));
    }

    assert!(!is_temporary_tcp_failure(&Status::internal(
        "template manager returned internal error"
    )));
}

#[tokio::test]
async fn retries_unavailable_then_succeeds() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let result = retry_grpc_status_with_context(
        "test",
        "Call",
        test_policy(),
        GrpcRetryContext::none(),
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(Status::unavailable("restart"))
                } else {
                    Ok("ok")
                }
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(result, "ok");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retries_caller_classified_cancelled_status() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let result = retry_grpc_status_with_context_if(
        "test",
        "Call",
        test_policy(),
        GrpcRetryContext::none(),
        |status| status.code() == Code::Cancelled,
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(Status::cancelled("operation was canceled"))
                } else {
                    Ok("ok")
                }
            }
        },
    )
    .await;

    assert_eq!(result.unwrap(), "ok");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn caller_classified_cancelled_respects_deadline() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let status = retry_grpc_status_with_context_if(
        "test",
        "Process.Start",
        GrpcRetryPolicy {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(50),
            jitter_factor: 0.0,
            ..test_policy()
        },
        GrpcRetryContext::with_timeout(Duration::from_millis(1)),
        |status| status.code() == Code::Cancelled,
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(Status::cancelled("operation was canceled"))
            }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn caller_classified_cancelled_does_not_retry_uds_backpressure() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let status = retry_grpc_status_with_context_if(
        "test",
        "Process.Start",
        test_policy(),
        GrpcRetryContext::none(),
        |status| status.code() == Code::Cancelled,
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(uds_per_sandbox_limit_exceeded_status(
                    "UDS per-sandbox limiter exceeded",
                ))
            }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retries_internal_temporary_tcp_failure_then_succeeds() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let result = retry_grpc_status_with_context(
        "test",
        "Call",
        test_policy(),
        GrpcRetryContext::none(),
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(Status::internal("transport error: connection reset"))
                } else {
                    Ok("ok")
                }
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(result, "ok");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn does_not_retry_invalid_argument() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let status = retry_grpc_status_with_context(
        "test",
        "Call",
        test_policy(),
        GrpcRetryContext::none(),
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(Status::invalid_argument("bad request"))
            }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn does_not_retry_cancelled_or_deadline_exceeded() {
    for code in [Code::Cancelled, Code::DeadlineExceeded] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);
        let status = retry_grpc_status_with_context(
            "test",
            "Call",
            test_policy(),
            GrpcRetryContext::none(),
            move || {
                let attempts = Arc::clone(&attempts_for_call);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(Status::new(code, "caller stopped waiting"))
                }
            },
        )
        .await
        .unwrap_err();

        assert_eq!(status.code(), code);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn retries_rate_limit_with_retry_after() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let result = retry_grpc_status_with_context(
        "test",
        "Call",
        test_policy(),
        GrpcRetryContext::none(),
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(rate_limited_status(
                        "rate limited",
                        Some(Duration::from_millis(1)),
                    ))
                } else {
                    Ok("ok")
                }
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(result, "ok");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn grpc_client_reconnects_after_transport_like_status() {
    #[derive(Clone)]
    struct FakeClient {
        generation: usize,
    }

    let connects = Arc::new(AtomicUsize::new(0));
    let connects_for_client = Arc::clone(&connects);
    let mut client = GrpcClient::new("fake", move || {
        let connects = Arc::clone(&connects_for_client);
        async move {
            let generation = connects.fetch_add(1, Ordering::SeqCst) + 1;
            Ok::<_, &'static str>(FakeClient { generation })
        }
    })
    .with_retry_policy(test_policy());

    let result = client
        .call("Call", GrpcRetryContext::none(), |client| async move {
            if client.generation == 1 {
                Err(Status::unavailable("stale channel"))
            } else {
                Ok(client.generation)
            }
        })
        .await
        .unwrap();

    assert_eq!(result, 2);
    assert_eq!(connects.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn grpc_client_retries_initial_connect_failure() {
    #[derive(Clone)]
    struct FakeClient;

    let connects = Arc::new(AtomicUsize::new(0));
    let connects_for_client = Arc::clone(&connects);
    let mut client = GrpcClient::new("fake", move || {
        let connects = Arc::clone(&connects_for_client);
        async move {
            let attempt = connects.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Err("listener down")
            } else {
                Ok(FakeClient)
            }
        }
    })
    .with_retry_policy(test_policy());

    let result = client
        .call("Call", GrpcRetryContext::none(), |_client| async move {
            Ok::<_, Status>("ok")
        })
        .await
        .unwrap();

    assert_eq!(result, "ok");
    assert_eq!(connects.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn grpc_client_retries_connect_attempt_timeout_after_stale_channel() {
    #[derive(Clone)]
    struct FakeClient {
        generation: usize,
    }

    let connects = Arc::new(AtomicUsize::new(0));
    let connects_for_client = Arc::clone(&connects);
    let mut client = GrpcClient::new("fake", move || {
        let connects = Arc::clone(&connects_for_client);
        async move {
            let generation = connects.fetch_add(1, Ordering::SeqCst) + 1;
            if generation == 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<_, &'static str>(FakeClient { generation })
        }
    })
    .with_retry_policy(test_policy())
    .with_connect_attempt_timeout(Duration::from_millis(1));

    let first_result = client
        .call("Call", GrpcRetryContext::none(), |client| async move {
            Ok::<_, Status>(client.generation)
        })
        .await
        .unwrap();
    assert_eq!(first_result, 1);

    let rpc_attempts = Arc::new(AtomicUsize::new(0));
    let rpc_attempts_for_call = Arc::clone(&rpc_attempts);
    let result = client
        .call("Call", GrpcRetryContext::none(), move |client| {
            let rpc_attempts = Arc::clone(&rpc_attempts_for_call);
            async move {
                rpc_attempts.fetch_add(1, Ordering::SeqCst);
                if client.generation == 1 {
                    Err(Status::unavailable("stale channel"))
                } else {
                    Ok(client.generation)
                }
            }
        })
        .await
        .unwrap();

    assert_eq!(result, 3);
    assert_eq!(connects.load(Ordering::SeqCst), 3);
    assert_eq!(rpc_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn grpc_client_spends_one_budget_across_connect_and_rpc() {
    #[derive(Clone)]
    struct FakeClient;

    let connects = Arc::new(AtomicUsize::new(0));
    let connects_for_client = Arc::clone(&connects);
    let rpc_attempts = Arc::new(AtomicUsize::new(0));
    let rpc_attempts_for_call = Arc::clone(&rpc_attempts);
    let mut client = GrpcClient::new("fake", move || {
        let connects = Arc::clone(&connects_for_client);
        async move {
            let attempt = connects.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Err("listener down")
            } else {
                Ok(FakeClient)
            }
        }
    })
    .with_retry_policy(one_retry_policy());

    let status = client
        .call("Call", GrpcRetryContext::none(), move |_client| {
            let rpc_attempts = Arc::clone(&rpc_attempts_for_call);
            async move {
                rpc_attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(Status::unavailable("service restarted after connect"))
            }
        })
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(connects.load(Ordering::SeqCst), 2);
    assert_eq!(rpc_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn grpc_client_keeps_cached_client_after_application_status() {
    #[derive(Clone)]
    struct FakeClient {
        generation: usize,
    }

    let connects = Arc::new(AtomicUsize::new(0));
    let connects_for_client = Arc::clone(&connects);
    let mut client = GrpcClient::new("fake", move || {
        let connects = Arc::clone(&connects_for_client);
        async move {
            let generation = connects.fetch_add(1, Ordering::SeqCst) + 1;
            Ok::<_, &'static str>(FakeClient { generation })
        }
    })
    .with_retry_policy(test_policy());

    let status = client
        .call("Validate", GrpcRetryContext::none(), |_client| async move {
            Err::<(), _>(Status::invalid_argument("bad request"))
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let generation = client
        .call("Call", GrpcRetryContext::none(), |client| async move {
            Ok::<_, Status>(client.generation)
        })
        .await
        .unwrap();

    assert_eq!(generation, 1);
    assert_eq!(connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn grpc_client_clears_cached_client_after_reconnect_only_status() {
    #[derive(Clone)]
    struct FakeClient {
        generation: usize,
    }

    let connects = Arc::new(AtomicUsize::new(0));
    let connects_for_client = Arc::clone(&connects);
    let mut client = GrpcClient::new("fake", move || {
        let connects = Arc::clone(&connects_for_client);
        async move {
            let generation = connects.fetch_add(1, Ordering::SeqCst) + 1;
            Ok::<_, &'static str>(FakeClient { generation })
        }
    })
    .with_retry_policy(GrpcRetryPolicy::none());

    let status = client
        .call("Call", GrpcRetryContext::none(), |_client| async move {
            Err::<(), _>(Status::deadline_exceeded("caller deadline expired"))
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::DeadlineExceeded);

    let generation = client
        .call("Call", GrpcRetryContext::none(), |client| async move {
            Ok::<_, Status>(client.generation)
        })
        .await
        .unwrap();

    assert_eq!(generation, 2);
    assert_eq!(connects.load(Ordering::SeqCst), 2);
}

#[derive(Clone, Default)]
struct KeyedMacroClient {
    attempts: Arc<AtomicUsize>,
    observed_keys: Arc<Mutex<Vec<Option<String>>>>,
}

#[derive(Clone)]
struct KeyedMacroRequest {
    idempotency_key: Option<String>,
}

impl KeyedMacroClient {
    async fn effectful_call(
        &mut self,
        request: tonic::Request<KeyedMacroRequest>,
    ) -> Result<tonic::Response<&'static str>, Status> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        self.observed_keys
            .lock()
            .unwrap()
            .push(request.into_inner().idempotency_key);
        if attempt == 0 {
            Err(Status::unavailable("restart"))
        } else {
            Ok(tonic::Response::new("ok"))
        }
    }
}

#[tokio::test]
async fn fwd_retry_keyed_generates_one_key_for_all_attempts() {
    let client = KeyedMacroClient::default();
    let observed_keys = Arc::clone(&client.observed_keys);
    let response = pplx_space_common_grpc::fwd_retry_keyed!(
        "test",
        client,
        effectful_call,
        tonic::Request::new(KeyedMacroRequest {
            idempotency_key: None
        })
    )
    .unwrap();

    assert_eq!(response.into_inner(), "ok");
    let keys = observed_keys.lock().unwrap();
    assert_eq!(keys.len(), 2);
    let first = keys[0].as_deref().expect("first attempt has key");
    let second = keys[1].as_deref().expect("second attempt has key");
    assert_eq!(first, second);
    assert_eq!(uuid::Uuid::parse_str(first).unwrap().get_version_num(), 7);
}

#[tokio::test]
async fn fwd_retry_keyed_preserves_existing_key() {
    let client = KeyedMacroClient::default();
    let observed_keys = Arc::clone(&client.observed_keys);
    let response = pplx_space_common_grpc::fwd_retry_keyed!(
        "test",
        client,
        effectful_call,
        tonic::Request::new(KeyedMacroRequest {
            idempotency_key: Some("caller-key".to_string())
        })
    )
    .unwrap();

    assert_eq!(response.into_inner(), "ok");
    let keys = observed_keys.lock().unwrap();
    assert_eq!(
        keys.as_slice(),
        &[
            Some("caller-key".to_string()),
            Some("caller-key".to_string())
        ]
    );
}

#[tokio::test]
async fn does_not_retry_rate_limit_without_retry_after() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);
    let status = retry_grpc_status_with_context(
        "test",
        "Call",
        test_policy(),
        GrpcRetryContext::none(),
        move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(Status::resource_exhausted("rate limited"))
            }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn deadline_clamps_retries() {
    for (timeout, expected_attempts) in [(Duration::from_millis(1), 1), (Duration::MAX, 0)] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);
        let status = retry_grpc_status_with_context(
            "test",
            "Call",
            GrpcRetryPolicy {
                initial_delay: Duration::from_millis(50),
                max_delay: Duration::from_millis(50),
                jitter_factor: 0.0,
                ..test_policy()
            },
            GrpcRetryContext::with_timeout(timeout),
            move || {
                let attempts = Arc::clone(&attempts_for_call);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(Status::unavailable("restart"))
                }
            },
        )
        .await
        .unwrap_err();

        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(attempts.load(Ordering::SeqCst), expected_attempts);
    }
}

#[test]
fn client_connect_failure_requires_a_local_error_chain() {
    use pplx_space_common_grpc::is_client_connect_failure;

    // Wire-decoded statuses carry text but no local source chain: a server
    // echoing connect-failure wording must never classify.
    for message in [
        "error trying to connect: Connection refused (os error 61)",
        "connection refused",
        "No such file or directory (os error 2)",
    ] {
        assert!(!is_client_connect_failure(&Status::unavailable(message)));
    }
    assert!(!is_client_connect_failure(&Status::internal(
        "error trying to connect: Connection refused"
    )));
}

// A local `UNAVAILABLE` whose cause is a connection that broke, gave up or
// closed after the request went out proves nothing about transmission, as
// tonic reports a peer that stops answering keep-alives after reading the
// request: only a dial failure, or the connector's marker, is one.
#[test]
fn a_broken_connection_under_a_local_unavailable_is_not_a_client_connect_failure() {
    use pplx_space_common_grpc::is_client_connect_failure;

    for kind in [
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::ConnectionAborted,
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::TimedOut,
        std::io::ErrorKind::UnexpectedEof,
    ] {
        let mut status = Status::unavailable("transport error");
        status.set_source(Arc::new(std::io::Error::from(kind)));
        assert!(!is_client_connect_failure(&status), "{kind:?}");
    }
}

// A refusal made before the request started carries a retry hint for the
// caller. Neither retry path loops on it, even when the policy would accept
// the hint and the caller's own predicate would retry the code.
#[tokio::test]
async fn refusals_before_start_are_never_retried_by_either_retry_path() {
    let accepts_hint = GrpcRetryPolicy {
        max_server_retry_after: Duration::from_secs(2),
        ..test_policy()
    };
    for code in [Code::ResourceExhausted, Code::Unavailable] {
        let refused = || {
            pplx_space_common_grpc::refused_before_start(
                Status::new(code, "synthetic refusal"),
                Duration::from_millis(1),
            )
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);
        let status = retry_grpc_status_with_context_if(
            "test",
            "Process.Start",
            accepts_hint,
            GrpcRetryContext::none(),
            |_| true,
            move || {
                let attempts = Arc::clone(&attempts_for_call);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(refused())
                }
            },
        )
        .await
        .unwrap_err();
        assert_eq!(status.code(), code);
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "{code:?}");

        let calls = Arc::new(AtomicUsize::new(0));
        let mut client = GrpcClient::new("fake", || async { Ok::<_, &'static str>(()) })
            .with_retry_policy(accepts_hint);
        let calls_for_client = Arc::clone(&calls);
        let status = client
            .call("Call", GrpcRetryContext::none(), move |()| {
                let calls = Arc::clone(&calls_for_client);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(refused())
                }
            })
            .await
            .unwrap_err();
        assert_eq!(status.code(), code);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{code:?}");
    }
}
