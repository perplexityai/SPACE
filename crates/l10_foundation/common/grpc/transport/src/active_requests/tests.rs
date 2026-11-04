use tower::Layer as _;
use tower::Service as _;

use super::*;

fn request() -> http::Request<tonic::body::Body> {
    http::Request::new(tonic::body::Body::empty())
}

async fn ok_response(
    _request: http::Request<tonic::body::Body>,
) -> Result<http::Response<tonic::body::Body>, std::convert::Infallible> {
    Ok(http::Response::new(tonic::body::Body::empty()))
}

#[tokio::test]
async fn middleware_holds_a_slot_from_dispatch_until_the_response_is_released() {
    let counter = InFlightRequestsCounter::new();
    let mut service = InFlightRequestsLayer::new(counter.clone())
        .layer(tower::service_fn(ok_response as fn(_) -> _));

    assert_eq!(counter.get(), 0);

    // Unconsumed responses stay counted.
    let first = service.call(request());
    let second = service.call(request());
    assert_eq!(counter.get(), 2);

    // Abandoned dispatches and dropped responses both release their slot.
    drop((first, second));
    assert_eq!(counter.get(), 0);
    let response = service.call(request()).await.unwrap();
    assert_eq!(counter.get(), 1);
    drop(response);
    assert_eq!(counter.get(), 0);
}

#[tokio::test]
async fn the_process_counter_is_shared_by_every_listener() {
    let counter = InFlightRequestsCounter::new();
    let mut first_listener = InFlightRequestsLayer::new(counter.clone())
        .layer(tower::service_fn(ok_response as fn(_) -> _));
    let mut second_listener = InFlightRequestsLayer::new(counter.clone())
        .layer(tower::service_fn(ok_response as fn(_) -> _));

    let on_first = first_listener.call(request());
    let on_second = second_listener.call(request());
    assert_eq!(counter.get(), 2);

    drop((on_first, on_second));
    assert_eq!(counter.get(), 0);
}
