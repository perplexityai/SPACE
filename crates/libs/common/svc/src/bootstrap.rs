use std::borrow::Cow;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use axum::extract::MatchedPath;
use http::{HeaderMap, Method, Request as HttpRequest, Response as HttpResponse};
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tower_http::classify::{
    GrpcCode, GrpcErrorsAsFailures, GrpcFailureClass, ServerErrorsAsFailures,
    ServerErrorsFailureClass, SharedClassifier,
};
use tower_http::trace::{
    DefaultOnBodyChunk, DefaultOnEos, DefaultOnRequest, GrpcMakeClassifier, MakeSpan, OnEos,
    OnFailure, OnResponse, TraceLayer,
};
use uuid::Uuid;

pub use pplx_space_common_grpc::{
    CLIENT_SERVICE_METADATA_KEY, GrpcClientTrace, current_service_name, set_current_service_name,
    trace_grpc_client_transport as wrap_grpc_client_channel,
};

mod grpc_request_params;
mod grpc_trace;
mod http_trace;
mod shutdown;
mod unix_socket;

pub use grpc_request_params::*;
pub use grpc_trace::*;
pub use http_trace::*;
pub use shutdown::*;
pub use unix_socket::*;

pub const GRPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
pub const GRPC_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
pub const GRPC_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(60);

pub fn grpc_server_builder() -> pplx_space_common_grpc::GrpcServer {
    grpc_server_builder_with_request_timeout(GRPC_REQUEST_TIMEOUT)
}

pub fn grpc_server_builder_with_request_timeout(
    request_timeout: Duration,
) -> pplx_space_common_grpc::GrpcServer {
    pplx_space_common_grpc::grpc_server_builder()
        .timeout(request_timeout)
        .http2_keepalive_interval(Some(GRPC_HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(GRPC_HTTP2_KEEPALIVE_TIMEOUT))
}

pub use pplx_space_request_id::request_id_from_headers;

pub fn grpc_health_service() -> (
    tonic_health::server::HealthReporter,
    tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health>,
) {
    tonic_health::server::health_reporter()
}

#[cfg(test)]
mod tests;
