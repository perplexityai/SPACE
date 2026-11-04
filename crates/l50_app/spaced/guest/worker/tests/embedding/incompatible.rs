//! An intentionally incompatible worker speaking only Ready over the inherited socket.
use std::convert::Infallible;
use std::future::{Ready, ready};
use std::os::fd::FromRawFd;
use std::task::{Context, Poll};

use pplx_space_spaced_guest_protocol::proto::worker::{ReadyRequest, ReadyResponse};
use tonic::body::Body;
use tonic::codegen::{BoxFuture, Service, http};

#[derive(Clone)]
struct Incompatible;

impl tonic::server::NamedService for Incompatible {
    const NAME: &'static str = "space.spaced.worker.v1.WorkerFilesystem";
}

impl tonic::server::UnaryService<ReadyRequest> for Incompatible {
    type Response = ReadyResponse;
    type Future = Ready<Result<tonic::Response<ReadyResponse>, tonic::Status>>;
    fn call(&mut self, _: tonic::Request<ReadyRequest>) -> Self::Future {
        ready(Ok(tonic::Response::new(ReadyResponse {
            protocol_version: u32::MAX,
        })))
    }
}

impl Service<http::Request<Body>> for Incompatible {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        Box::pin(async move {
            if request.uri().path() != "/space.spaced.worker.v1.WorkerFilesystem/Ready" {
                return Ok(tonic::Status::permission_denied(
                    "dispatch reached incompatible worker",
                )
                .into_http());
            }
            let codec = tonic_prost::ProstCodec::<ReadyResponse, ReadyRequest>::default();
            Ok(tonic::server::Grpc::new(codec)
                .unary(Incompatible, request)
                .await)
        })
    }
}

pub(super) fn run() {
    let fd = std::env::var("SPACE_SPACED_FILESYSTEM_WORKER_FD")
        .expect("worker fd")
        .parse()
        .expect("fd number");
    // SAFETY: this test subprocess takes exclusive ownership of the socket
    // inherited from the production launcher, before opening any descriptors.
    let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    stream.set_nonblocking(true).expect("nonblocking socket");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let stream = tokio::net::UnixStream::from_std(stream).expect("worker stream");
            tonic::transport::Server::builder()
                .add_service(Incompatible)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(stream)))
                .await
                .expect("incompatible server");
        });
}
