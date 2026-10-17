use std::sync::{Arc, Mutex};

use tower::{Service, ServiceExt as _};
use tower_http::classify::ClassifiedResponse;

use super::*;

#[derive(Clone)]
struct MockInner {
    captured: Arc<Mutex<Option<(String, String)>>>,
}

impl Service<Request<tonic::body::Body>> for MockInner {
    type Response = Response<tonic::body::Body>;
    type Error = std::io::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<tonic::body::Body>) -> Self::Future {
        let caller = request
            .headers()
            .get(CLIENT_SERVICE_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        *self.captured.lock().unwrap() = Some((request.uri().path().to_string(), caller));
        Box::pin(async move { Ok(Response::new(tonic::body::Body::empty())) })
    }
}

#[test]
fn parse_grpc_path_splits_tonic_path() {
    assert_eq!(
        parse_grpc_path("/pplx.space.node.NodeManager/CreateSandbox"),
        ("pplx.space.node.NodeManager", "CreateSandbox"),
    );
}

#[test]
fn guest_application_statuses_are_expected_from_headers() {
    for (path, code) in [
        (
            "/space.node.api.v1.WorkerApi/SpacedMkdir",
            tonic::Code::PermissionDenied,
        ),
        (
            "/space.node.api.v1.WorkerApi/SpacedRead",
            tonic::Code::NotFound,
        ),
        (
            "/space.node.api.v1.WorkerApi/SpacedStartProcess",
            tonic::Code::InvalidArgument,
        ),
    ] {
        let request = Request::builder()
            .uri(format!("http://[::]{path}"))
            .body(())
            .unwrap();
        let classifier = GrpcClientMakeClassifier.make_classifier(&request);
        let response = Response::builder()
            .header("grpc-status", (code as i32).to_string())
            .body(())
            .unwrap();

        assert!(matches!(
            classifier.classify_response(&response),
            ClassifiedResponse::Ready(Ok(()))
        ));
    }
}

#[test]
fn guest_platform_statuses_remain_failures() {
    let request = Request::builder()
        .uri("http://[::]/space.node.api.v1.WorkerApi/SpacedMkdir")
        .body(())
        .unwrap();
    let classifier = GrpcClientMakeClassifier.make_classifier(&request);
    let response = Response::builder()
        .header("grpc-status", (tonic::Code::Unavailable as i32).to_string())
        .body(())
        .unwrap();

    assert!(matches!(
        classifier.classify_response(&response),
        ClassifiedResponse::Ready(Err(_))
    ));
}

#[test]
fn guest_application_statuses_are_expected_from_trailers() {
    let request = Request::builder()
        .uri("http://[::]/space.node.api.v1.WorkerApi/SpacedMkdir")
        .body(())
        .unwrap();
    let classifier = GrpcClientMakeClassifier.make_classifier(&request);
    let response = Response::builder().body(()).unwrap();
    let ClassifiedResponse::RequiresEos(eos) = classifier.classify_response(&response) else {
        panic!("missing grpc-status header should defer classification to trailers");
    };
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "grpc-status",
        HeaderValue::from_str(&(tonic::Code::PermissionDenied as i32).to_string()).unwrap(),
    );

    eos.classify_eos(Some(&trailers))
        .expect("application status should be classified as success");
}

#[tokio::test]
async fn trace_transport_stamps_the_caller_service() {
    let captured = Arc::new(Mutex::new(None));
    let inner = MockInner {
        captured: captured.clone(),
    };
    let request = Request::builder()
        .uri("http://[::]/pplx.space.SandboxManager/Create")
        .body(tonic::body::Body::empty())
        .unwrap();

    let _response = trace_grpc_client_transport(inner)
        .ready()
        .await
        .unwrap()
        .call(request)
        .await
        .unwrap();

    let captured = captured.lock().unwrap();
    let (path, caller) = captured.as_ref().expect("inner service was called");
    assert_eq!(path, "/pplx.space.SandboxManager/Create");
    assert_eq!(caller, current_service_name());
}

#[test]
fn client_eos_accepts_headers_and_missing_headers() {
    let span = tracing::Span::none();
    GrpcClientOnEos.on_eos(None, Duration::from_millis(5), &span);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    GrpcClientOnEos.on_eos(Some(&trailers), Duration::from_millis(5), &span);
}

#[test]
fn ingress_preconditions_require_the_guest_marker_in_the_status_block() {
    let mut wrong_marker =
        tonic::Status::failed_precondition("network policy is not ready").into_http::<()>();
    wrong_marker.headers_mut().insert(
        "space-guest-service-error",
        HeaderValue::from_static("unknown"),
    );
    let mut malformed_marker = wrong_marker.headers().clone();
    malformed_marker.insert(
        "space-guest-service-error",
        HeaderValue::from_bytes(b"\xff").unwrap(),
    );
    let mut unavailable_with_marker = crate::GuestServiceUnavailable
        .into_status()
        .into_http::<()>();
    unavailable_with_marker
        .headers_mut()
        .insert("grpc-status", HeaderValue::from_static("14"));
    let cases = [
        (
            "guest refusal",
            crate::GuestServiceUnavailable
                .into_status()
                .into_http::<()>()
                .headers()
                .clone(),
            true,
        ),
        (
            "policy not ready",
            tonic::Status::failed_precondition("network policy is not ready")
                .into_http::<()>()
                .headers()
                .clone(),
            false,
        ),
        (
            "runtime unreachable",
            tonic::Status::failed_precondition("guest bridge is unreachable")
                .into_http::<()>()
                .headers()
                .clone(),
            false,
        ),
        ("wrong marker", wrong_marker.headers().clone(), false),
        ("malformed marker", malformed_marker, false),
        (
            "unavailable",
            tonic::Status::unavailable("internal socket refused")
                .into_http::<()>()
                .headers()
                .clone(),
            false,
        ),
        (
            "wrong code with marker",
            unavailable_with_marker.headers().clone(),
            false,
        ),
    ];
    for path in [
        "/space.node.api.v1.WorkerApi/ServicePortIngress",
        "/space.sandbox.manager.v1.SandboxManager/ServicePortIngress",
        "/space.net.gateway.v1.NetGatewayControl/ServicePortIngress",
    ] {
        let request = Request::builder().uri(path).body(()).unwrap();
        for (name, headers, success) in &cases {
            let mut response = Response::new(());
            *response.headers_mut() = headers.clone();
            let classified = GrpcClientMakeClassifier
                .make_classifier(&request)
                .classify_response(&response);
            assert!(
                matches!(classified, ClassifiedResponse::Ready(ref result) if result.is_ok() == *success),
                "headers: {path}: {name}: {classified:?}"
            );
            if let ClassifiedResponse::Ready(Err(failure)) = classified {
                assert!(
                    failure.is_error,
                    "OnFailure must retain {name} as an internal error"
                );
                assert!(
                    matches!(failure.inner, GrpcFailureClass::Status(status) if status.code_raw() == grpc_status_from_headers(headers).unwrap())
                );
            }
            // A marker on initial headers cannot authorize an unmarked trailer status.
            let mut initial = Response::new(());
            initial.headers_mut().insert(
                "space-guest-service-error",
                HeaderValue::from_static("connection_refused"),
            );
            let ClassifiedResponse::RequiresEos(eos) = GrpcClientMakeClassifier
                .make_classifier(&request)
                .classify_response(&initial)
            else {
                panic!("a streaming response must wait for its status trailers");
            };
            let result = eos.classify_eos(Some(headers));
            assert_eq!(
                result.is_ok(),
                *success,
                "trailers: {path}: {name}: {result:?}"
            );
            if let Err(failure) = result {
                assert!(
                    failure.is_error,
                    "OnFailure must retain trailer {name} as an internal error"
                );
                assert!(
                    matches!(failure.inner, GrpcFailureClass::Status(status) if status.code_raw() == grpc_status_from_headers(headers).unwrap())
                );
            }
        }
    }
}

#[test]
fn ingress_transport_failures_are_not_guest_refusals() {
    let request = Request::builder()
        .uri("/space.node.api.v1.WorkerApi/ServicePortIngress")
        .body(())
        .unwrap();
    // The same kernel error at an internal transport boundary must remain an error.
    let error = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
    let failure = GrpcClientMakeClassifier
        .make_classifier(&request)
        .classify_error(&error);
    assert!(failure.is_error);
    assert!(matches!(failure.inner, GrpcFailureClass::Error(_)));
    let ClassifiedResponse::RequiresEos(eos) = GrpcClientMakeClassifier
        .make_classifier(&request)
        .classify_response(&Response::new(()))
    else {
        panic!("streaming response must defer classification");
    };
    let failure = eos.classify_error(&error);
    assert!(failure.is_error);
    assert!(matches!(failure.inner, GrpcFailureClass::Error(_)));
}

#[test]
fn non_ingress_precondition_conventions_are_unchanged() {
    for (path, expected_success) in [
        ("/space.node.api.v1.WorkerApi/SpacedRead", true),
        ("/space.node.api.v1.WorkerApi/CreateSandbox", false),
    ] {
        let request = Request::builder().uri(path).body(()).unwrap();
        for status in [
            tonic::Status::failed_precondition("runtime not ready"),
            crate::GuestServiceUnavailable.into_status(),
        ] {
            let response = status.into_http::<()>();
            let ClassifiedResponse::Ready(result) = GrpcClientMakeClassifier
                .make_classifier(&request)
                .classify_response(&response)
            else {
                panic!("status in headers must be classified immediately");
            };
            assert_eq!(result.is_ok(), expected_success);
            if let Err(failure) = result {
                assert!(
                    !failure.is_error,
                    "existing application-status logging policy is unchanged"
                );
            }
            let ClassifiedResponse::RequiresEos(eos) = GrpcClientMakeClassifier
                .make_classifier(&request)
                .classify_response(&Response::new(()))
            else {
                panic!("streaming response must defer classification");
            };
            let result = eos.classify_eos(Some(response.headers()));
            assert_eq!(result.is_ok(), expected_success);
            if let Err(failure) = result {
                assert!(!failure.is_error);
            }
        }
    }
}

/// A caller that marks a call's refusal as expected gets a success span for
/// that code, in headers and trailers alike. The same call unmarked, another
/// code, or a platform failure marked as expected all stay failures.
#[test]
fn caller_marked_status_is_expected_only_for_that_call() {
    let path = "/space.spaced.host.control.v1.HostControl/GetNetworkPolicy";
    let classify = |expected: Option<tonic::Code>, code: tonic::Code| {
        let mut request = Request::builder().uri(path).body(()).unwrap();
        if let Some(expected) = expected {
            request
                .extensions_mut()
                .insert(ExpectedGrpcStatus::new(expected));
        }
        let response = tonic::Status::new(code, "refused").into_http::<()>();
        let ClassifiedResponse::Ready(headers) = GrpcClientMakeClassifier
            .make_classifier(&request)
            .classify_response(&response)
        else {
            panic!("status in headers must be classified immediately");
        };
        let ClassifiedResponse::RequiresEos(eos) = GrpcClientMakeClassifier
            .make_classifier(&request)
            .classify_response(&Response::new(()))
        else {
            panic!("streaming response must defer classification");
        };
        let trailers = eos.classify_eos(Some(response.headers()));
        assert_eq!(headers.is_ok(), trailers.is_ok());
        headers.is_ok()
    };
    let denied = tonic::Code::PermissionDenied;

    assert!(classify(Some(denied), denied));
    assert!(!classify(None, denied));
    assert!(!classify(Some(denied), tonic::Code::FailedPrecondition));
    assert!(!classify(
        Some(tonic::Code::Unavailable),
        tonic::Code::Unavailable
    ));
}

/// tonic carries a request's extensions into the HTTP request the trace layer
/// classifies, which is how a caller's mark reaches it.
#[tokio::test]
async fn expected_status_mark_reaches_the_traced_transport() {
    #[derive(Clone, Default)]
    struct CaptureExpected(Arc<Mutex<Option<Option<tonic::Code>>>>);

    impl Service<Request<tonic::body::Body>> for CaptureExpected {
        type Response = Response<tonic::body::Body>;
        type Error = std::io::Error;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<tonic::body::Body>) -> Self::Future {
            *self.0.lock().unwrap() = Some(expected_status_for(&request));
            let response =
                tonic::Status::permission_denied("refused").into_http::<tonic::body::Body>();
            std::future::ready(Ok(response))
        }
    }

    let inner = CaptureExpected::default();
    let mut client = tonic_health::pb::health_client::HealthClient::new(
        trace_grpc_client_transport(inner.clone()),
    );
    let mut request = tonic::Request::new(tonic_health::pb::HealthCheckRequest::default());
    ExpectedGrpcStatus::new(tonic::Code::PermissionDenied).mark(&mut request);

    let status = client.check(request).await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        *inner.0.lock().unwrap(),
        Some(Some(tonic::Code::PermissionDenied))
    );
}
