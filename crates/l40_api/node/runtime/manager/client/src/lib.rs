//! Client for the node-local runtime manager, `space-rtmgr-svc`.

use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::transport::{Channel, Endpoint};

pub mod labels;

pub mod proto {
    tonic::include_proto!("space.node.runtime.manager.v1");
}

pub use pplx_space_common_core::host::linux::paths::RUNTIME_MANAGER_SOCKET;
/// The shared execution vocabulary the listing's runtime and component kinds use.
pub use pplx_space_execution_common::proto as execution;
pub use pplx_space_unix_socket_path::UnixSocketPath;

const LOCAL_ENDPOINT_URI: &str = "http://[::]:50051";

pub type RuntimeManagerGrpcClient =
    proto::runtime_manager_client::RuntimeManagerClient<GrpcClientTrace<Channel>>;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeManagerConnectError {
    #[error("runtime-manager transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Connect to the runtime manager on the node's shared socket.
pub async fn connect() -> Result<RuntimeManagerGrpcClient, RuntimeManagerConnectError> {
    connect_at(
        UnixSocketPath::parse(RUNTIME_MANAGER_SOCKET)
            .expect("runtime manager socket path is valid"),
    )
    .await
}

/// Connect to a runtime manager listening on `socket`.
pub async fn connect_at(
    socket: UnixSocketPath,
) -> Result<RuntimeManagerGrpcClient, RuntimeManagerConnectError> {
    let channel = Endpoint::from_static(LOCAL_ENDPOINT_URI)
        .connect_with_connector(tower::service_fn(move |_| {
            let socket = socket.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(socket.as_path()).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await?;
    Ok(proto::runtime_manager_client::RuntimeManagerClient::new(
        wrap_grpc_client_channel(channel),
    ))
}
