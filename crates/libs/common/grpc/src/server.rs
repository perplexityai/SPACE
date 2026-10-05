use tower::layer::util::{Identity, Stack};
use tower_http::metrics::InFlightRequestsLayer;

use crate::active_requests;

pub type GrpcServer = tonic::transport::Server<Stack<InFlightRequestsLayer, Identity>>;

/// The tonic server every SPACE gRPC listener starts from, wired for
/// active-request observation and nothing else.
///
/// Set no request timeout, keepalive, or connection age here: every listener
/// in the workspace is built from this, including the spaced bridges that
/// carry long-lived streams untimed. Shared timeout policy belongs in
/// `pplx_space_common_svc::grpc_server_builder`, which layers it on top.
pub fn grpc_server_builder() -> GrpcServer {
    tonic::transport::Server::builder()
        .max_frame_size(crate::BULK_HTTP2_MAX_FRAME_SIZE)
        .layer(active_requests::process_layer())
}
