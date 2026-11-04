//! Node-local notifications without importing worker, builder, or cluster APIs.

use std::time::Duration;
use std::{future::Future, pin::Pin, sync::LazyLock};

use pplx_space_common_grpc::trace_grpc_client_transport as wrap_grpc_client_channel;
use pplx_space_common_grpc::{GrpcClient, GrpcRetryContext};
use tonic::transport::{Channel, Endpoint};

pub mod proto {
    tonic::include_proto!("space.node.api.v1");
}

type ConnectFuture = Pin<Box<dyn Future<Output = Result<Channel, tonic::transport::Error>> + Send>>;
type Client = GrpcClient<Channel, fn() -> ConnectFuture>;
static CLIENT: LazyLock<tokio::sync::Mutex<Client>> = LazyLock::new(|| {
    tokio::sync::Mutex::new(
        GrpcClient::new("node-ctl", connect as fn() -> ConnectFuture)
            .with_connect_attempt_timeout(Duration::from_secs(1)),
    )
});

/// Replay-safe dirty notification, bounded across connection and RPC retries.
/// Node-ctl's periodic reconciliation remains the backstop if the sender exits.
pub async fn mark_node_status_dirty() -> Result<(), tonic::Status> {
    // Share the reconnecting channel across coalesced notification attempts.
    // The outer budget includes contention for that single cached client.
    tokio::time::timeout(Duration::from_secs(30), async {
        CLIENT
            .lock()
            .await
            .call(
                "mark_facet_dirty",
                GrpcRetryContext::with_timeout(Duration::from_secs(30)),
                |channel| async move {
                    proto::node_ctl_client::NodeCtlClient::new(wrap_grpc_client_channel(channel))
                        .mark_facet_dirty(proto::MarkFacetDirtyRequest {
                            refs: vec![proto::FacetRef {
                                facet: proto::ReconcileFacet::NodeStatus as i32,
                                element: None,
                            }],
                        })
                        .await
                        .map(|_| ())
                },
            )
            .await
    })
    .await
    .map_err(|_| tonic::Status::deadline_exceeded("node-status notification timed out"))?
}

fn connect() -> ConnectFuture {
    Box::pin(async {
        Endpoint::from_static("http://[::]:50051")
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(30))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true)
            .connect_with_connector(tower::service_fn(|_| async {
                let stream = tokio::net::UnixStream::connect(
                    pplx_space_common_core::host::linux::paths::NODE_CTL_SOCKET,
                )
                .await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }))
            .await
    })
}
