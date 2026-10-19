use super::*;
use crate::{
    builder::BuilderApiService, proto::worker_api_server::WorkerApi, worker::WorkerApiService,
};
use pplx_space_common_core::types::NodeId;
use pplx_space_common_svc::bootstrap::wrap_grpc_client_channel;
use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tonic::{Request, Status};
use tower::Service;

struct SilentBody {
    dropped: Arc<AtomicBool>,
    polled: Arc<AtomicBool>,
}
impl Drop for SilentBody {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl http_body::Body for SilentBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.polled.store(true, Ordering::SeqCst);
        Poll::Pending
    }
}
struct NoMessages<T>(PhantomData<T>);
impl<T> tonic::codec::Decoder for NoMessages<T> {
    type Item = T;
    type Error = Status;
    fn decode(&mut self, _: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<T>, Status> {
        panic!("silent body cannot decode a message")
    }
}
fn silent_request<T: Send + 'static>(dropped: Arc<AtomicBool>) -> Request<tonic::Streaming<T>> {
    Request::new(tonic::Streaming::new_request(
        NoMessages(PhantomData),
        SilentBody {
            dropped,
            polled: Arc::default(),
        },
        None,
        None,
    ))
}
fn clients() -> BackendClients {
    clients_with_channel(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
    )
}
fn clients_with_channel(channel: tonic::transport::Channel) -> BackendClients {
    BackendClients {
        sbx: SandboxManagerGrpcClient::new(wrap_grpc_client_channel(channel.clone())),
        node: NodeManagerGrpcClient::new(wrap_grpc_client_channel(channel.clone())),
        tmpl: TemplateManagerGrpcClient::new(wrap_grpc_client_channel(channel.clone())),
        build_mgr: pplx_space_build_mgr_client::client_with_token(channel, "stream-test-token")
            .unwrap(),
    }
}
fn wire_request(method: &str, body: tonic::body::Body) -> http::Request<tonic::body::Body> {
    http::Request::builder()
        .uri(format!("/space.node.api.v1.BuilderApi/{method}"))
        .header("content-type", "application/grpc")
        .body(body)
        .unwrap()
}

fn response_code(response: &http::Response<tonic::body::Body>) -> tonic::Code {
    tonic::Code::from_i32(
        response.headers()["grpc-status"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap(),
    )
}

fn builder(clients: BackendClients) -> BuilderApiService {
    BuilderApiService::new(
        clients,
        "019d84bd-280d-7331-99c6-b63377f3a3a2"
            .parse::<NodeId>()
            .unwrap(),
    )
}

async fn assert_first_frame_bound(is_builder: bool) {
    let dropped = Arc::new(AtomicBool::new(false));
    let code = tokio::time::timeout(Duration::from_secs(6), async {
        if is_builder {
            let mut service =
                builder(clients()).into_grpc_service(Arc::new(tokio::sync::Semaphore::new(2)));
            let request = wire_request(
                "UploadBuildBlob",
                tonic::body::Body::new(SilentBody {
                    dropped: dropped.clone(),
                    polled: Arc::default(),
                }),
            );
            response_code(&service.call(request).await.unwrap())
        } else {
            WorkerApiService::new(clients())
                .spaced_write(silent_request(dropped.clone()))
                .await
                .unwrap_err()
                .code()
        }
    })
    .await
    .expect("handler must enforce a first-frame deadline");
    assert_eq!(code, tonic::Code::DeadlineExceeded);
    assert!(
        dropped.load(Ordering::SeqCst),
        "return must drop inbound body"
    );
}

#[tokio::test(start_paused = true)]
async fn spaced_write_bounds_first_frame_and_drops_body() {
    assert_first_frame_bound(false).await;
}
#[tokio::test(start_paused = true)]
async fn upload_blob_bounds_first_frame_and_drops_body() {
    assert_first_frame_bound(true).await;
}

#[tokio::test]
async fn blob_upload_admission_is_shared_upload_only_and_released_on_cancellation() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let mut first_listener = builder(clients()).into_grpc_service(slots.clone());
    let mut second_listener = builder(clients()).into_grpc_service(slots.clone());
    let mut clone = first_listener.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let mut first = Box::pin(first_listener.call(wire_request(
        "UploadBuildBlob",
        tonic::body::Body::new(SilentBody {
            dropped: dropped.clone(),
            polled: Arc::default(),
        }),
    )));
    assert!(futures_util::poll!(&mut first).is_pending());
    for service in [&mut second_listener, &mut clone] {
        let polled = Arc::new(AtomicBool::new(false));
        let rejected_body_dropped = Arc::new(AtomicBool::new(false));
        let response = service
            .call(wire_request(
                "UploadBuildBlob",
                tonic::body::Body::new(SilentBody {
                    dropped: rejected_body_dropped.clone(),
                    polled: polled.clone(),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response_code(&response), tonic::Code::ResourceExhausted);
        assert!(
            !polled.load(Ordering::SeqCst),
            "overload must precede decoding"
        );
        assert!(rejected_body_dropped.load(Ordering::SeqCst));
    }
    // This unary method must reach its decoder even with every upload slot in
    // use. Missing unary input is a decoder error, not an admission refusal.
    let response = second_listener
        .call(wire_request(
            "GetBuildTemplateStatus",
            tonic::body::Body::empty(),
        ))
        .await
        .unwrap();
    assert_ne!(response_code(&response), tonic::Code::ResourceExhausted);
    drop(first);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(slots.available_permits(), 1);
    let mut replacement = Box::pin(clone.call(wire_request(
        "UploadBuildBlob",
        tonic::body::Body::new(SilentBody {
            dropped: Arc::default(),
            polled: Arc::default(),
        }),
    )));
    assert!(futures_util::poll!(&mut replacement).is_pending());
}

#[tokio::test]
async fn empty_blob_upload_retains_admission_through_backend_response() {
    let input_ended = Arc::new(tokio::sync::Notify::new());
    let reply = Arc::new(tokio::sync::Notify::new());
    let backend_ended = input_ended.clone();
    let backend_reply = reply.clone();
    let backend = axum::Router::new().fallback(axum::routing::any(
        move |request: axum::extract::Request| {
            let input_ended = backend_ended.clone();
            let reply = backend_reply.clone();
            async move {
                let body = axum::body::to_bytes(request.into_body(), 1024)
                    .await
                    .unwrap();
                assert!(body.is_empty(), "empty upload must forward zero chunks");
                input_ended.notify_one();
                reply.notified().await;
                http::Response::builder()
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(axum::body::Body::from(vec![0u8; 5]))
                    .unwrap()
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move {
        axum::serve(listener, backend).await.unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let clients = clients_with_channel(channel);
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let mut service = builder(clients.clone()).into_grpc_service(slots.clone());
    let mut clone = service.clone();
    let mut upload =
        Box::pin(service.call(wire_request("UploadBuildBlob", tonic::body::Body::empty())));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = input_ended.notified() => {},
            result = &mut upload => panic!("response preceded backend processing: {result:?}"),
        }
    })
    .await
    .expect("backend must receive empty upload");
    assert_eq!(
        slots.available_permits(),
        0,
        "EOF must not release admission before response"
    );
    let response = clone
        .call(wire_request("UploadBuildBlob", tonic::body::Body::empty()))
        .await
        .unwrap();
    assert_eq!(response_code(&response), tonic::Code::ResourceExhausted);
    reply.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(2), upload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    assert!(
        response
            .headers()
            .get("grpc-status")
            .is_none_or(|code| code == "0"),
        "empty upload must succeed, not just return HTTP 200"
    );
    assert_eq!(slots.available_permits(), 1);
    let empty = Request::new(tonic::Streaming::new_request(
        NoMessages(PhantomData),
        axum::body::Body::empty(),
        None,
        None,
    ));
    let error = WorkerApiService::new(clients)
        .spaced_write(empty)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    servers.shutdown().await;
}

/// A body the caller has reset.
struct ResetBody;
impl http_body::Body for ResetBody {
    type Data = bytes::Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(Some(Err(Status::cancelled("peer reset"))))
    }
}

#[tokio::test]
async fn served_spaced_write_fails_a_reset_body_instead_of_reading_it_as_empty() {
    let mut service = WorkerApiService::new(clients()).into_grpc_service();
    let request = http::Request::builder()
        .uri("/space.node.api.v1.WorkerApi/SpacedWrite")
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::new(ResetBody))
        .unwrap();
    // Read as the end of the stream, the reset would be an empty upload,
    // which the handler refuses with InvalidArgument.
    assert_eq!(
        response_code(&service.call(request).await.unwrap()),
        tonic::Code::Aborted
    );
}
