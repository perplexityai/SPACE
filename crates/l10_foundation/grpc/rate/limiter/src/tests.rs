use std::convert::Infallible;
use std::future::{Ready, ready};

use super::*;

#[derive(Clone)]
struct OkService;

impl Service<Request<()>> for OkService {
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<()>) -> Self::Future {
        ready(Ok(Response::new(tonic::body::Body::empty())))
    }
}

#[derive(Clone)]
struct PendingBodyService;

impl Service<Request<()>> for PendingBodyService {
    type Response = Response<PendingBody>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<()>) -> Self::Future {
        ready(Ok(Response::new(PendingBody)))
    }
}

struct PendingBody;

impl HttpBody for PendingBody {
    type Data = bytes::Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Pending
    }
}

fn request_from(uid: u32, gid: u32) -> Request<()> {
    request_from_method(uid, gid, "/test.Service/Call")
}

fn request_from_method(uid: u32, gid: u32, method: &'static str) -> Request<()> {
    let mut req = Request::new(());
    *req.uri_mut() = method.parse().expect("test URI should parse");
    req.extensions_mut()
        .insert(PeerIdentity::new(uid, gid, Some(1234)));
    req
}

#[tokio::test]
async fn enforces_per_peer_request_rate() {
    let layer = GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce("test").with_default_limits(
            GrpcRateLimiterLimits::disabled()
                .with_max_requests_per_peer(1, Duration::from_secs(60)),
        ),
    );
    let mut svc = layer.layer(OkService);

    svc.call(request_from(100, 100)).await.unwrap();
    let err = svc.call(request_from(100, 100)).await.unwrap_err();
    let status = err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("retry_after_ms="));
    assert!(
        status
            .metadata()
            .contains_key(pplx_space_common_grpc::RETRY_AFTER_MS_METADATA)
    );
}

#[tokio::test]
async fn isolates_request_rate_by_peer() {
    let layer = GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce("test").with_default_limits(
            GrpcRateLimiterLimits::disabled()
                .with_max_requests_per_peer(1, Duration::from_secs(60)),
        ),
    );
    let mut svc = layer.layer(OkService);

    svc.call(request_from(100, 100)).await.unwrap();
    svc.call(request_from(101, 101)).await.unwrap();
}

#[tokio::test]
async fn rejects_missing_peer_identity_by_default() {
    let layer = GrpcRateLimiterLayer::new(GrpcRateLimiterConfig::enforce("test"));
    let mut svc = layer.layer(OkService);

    let err = svc.call(Request::new(())).await.unwrap_err();
    let status = err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn method_limits_use_method_specific_rate_buckets() {
    let layer = GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce("test").with_method_limits(
            "/test.Service/Slow",
            GrpcRateLimiterLimits::disabled()
                .with_max_requests_per_peer(1, Duration::from_secs(60)),
        ),
    );
    let mut svc = layer.layer(OkService);

    svc.call(request_from_method(100, 100, "/test.Service/Slow"))
        .await
        .unwrap();
    let err = svc
        .call(request_from_method(100, 100, "/test.Service/Slow"))
        .await
        .unwrap_err();
    let status = err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    svc.call(request_from_method(100, 100, "/test.Service/Fast"))
        .await
        .unwrap();
}

#[tokio::test]
async fn health_check_uses_independent_request_rate_bucket() {
    let layer = GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce("test")
            .with_default_limits(
                GrpcRateLimiterLimits::disabled()
                    .with_max_requests_per_peer(1, Duration::from_secs(60)),
            )
            .with_health_check_limits(
                GrpcRateLimiterLimits::disabled()
                    .with_max_requests_per_peer(2, Duration::from_secs(60)),
            ),
    );
    let mut svc = layer.layer(OkService);

    svc.call(request_from_method(100, 100, "/test.Service/Business"))
        .await
        .unwrap();
    let business_err = svc
        .call(request_from_method(100, 100, "/test.Service/Business"))
        .await
        .unwrap_err();
    let status = business_err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    svc.call(request_from_method(100, 100, GRPC_HEALTH_CHECK_METHOD))
        .await
        .unwrap();
    svc.call(request_from_method(100, 100, GRPC_HEALTH_CHECK_METHOD))
        .await
        .unwrap();
    let health_err = svc
        .call(request_from_method(100, 100, GRPC_HEALTH_CHECK_METHOD))
        .await
        .unwrap_err();
    let status = health_err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
async fn health_check_uses_independent_in_flight_bucket() {
    let layer = GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce("test")
            .with_default_limits(GrpcRateLimiterLimits::disabled().with_max_in_flight_per_peer(1))
            .with_health_check_limits(
                GrpcRateLimiterLimits::disabled().with_max_in_flight_per_peer(1),
            ),
    );
    let mut svc = layer.layer(PendingBodyService);

    let business_response = svc
        .call(request_from_method(100, 100, "/test.Service/Business"))
        .await
        .unwrap();
    let business_err = svc
        .call(request_from_method(100, 100, "/test.Service/Business"))
        .await
        .unwrap_err();
    let status = business_err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    let health_response = svc
        .call(request_from_method(100, 100, GRPC_HEALTH_CHECK_METHOD))
        .await
        .unwrap();
    let health_err = svc
        .call(request_from_method(100, 100, GRPC_HEALTH_CHECK_METHOD))
        .await
        .unwrap_err();
    let status = health_err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    drop(business_response);
    drop(health_response);
    svc.call(request_from_method(100, 100, "/test.Service/Business"))
        .await
        .unwrap();
    svc.call(request_from_method(100, 100, GRPC_HEALTH_CHECK_METHOD))
        .await
        .unwrap();
}

#[tokio::test]
async fn in_flight_guard_lives_until_response_body_drop() {
    let layer = GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce("test")
            .with_default_limits(GrpcRateLimiterLimits::disabled().with_max_global_in_flight(1)),
    );
    let mut svc = layer.layer(PendingBodyService);

    let first_response = svc.call(request_from(100, 100)).await.unwrap();
    let err = svc.call(request_from(101, 101)).await.unwrap_err();
    let status = err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    drop(first_response);
    svc.call(request_from(101, 101)).await.unwrap();
}

#[tokio::test]
async fn per_peer_in_flight_guard_lives_until_response_body_drop() {
    let layer = GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce("test")
            .with_default_limits(GrpcRateLimiterLimits::disabled().with_max_in_flight_per_peer(1)),
    );
    let mut svc = layer.layer(PendingBodyService);

    let first_response = svc.call(request_from(100, 100)).await.unwrap();
    let same_peer_err = svc.call(request_from(100, 100)).await.unwrap_err();
    let status = same_peer_err
        .downcast::<Status>()
        .expect("rejection should be a tonic status");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    svc.call(request_from(101, 101)).await.unwrap();
    drop(first_response);
    svc.call(request_from(100, 100)).await.unwrap();
}

#[test]
fn gcra_allows_configured_burst_then_smooths_refill() {
    let now = Instant::now();
    let rate = GcraRate {
        capacity: 2,
        window: Duration::from_millis(100),
    };
    let mut bucket = GcraBucket {
        theoretical_arrival_time: now,
    };

    check_gcra_bucket(&mut bucket, rate, now).unwrap();
    check_gcra_bucket(&mut bucket, rate, now).unwrap();
    assert!(check_gcra_bucket(&mut bucket, rate, now).is_err());

    check_gcra_bucket(&mut bucket, rate, now + Duration::from_millis(50)).unwrap();
    assert!(check_gcra_bucket(&mut bucket, rate, now + Duration::from_millis(50)).is_err());
}
