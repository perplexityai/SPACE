pub mod boot;
#[cfg(feature = "host-runtime")]
pub mod cgroup;
pub mod cgroup_paths;
pub mod control;
pub mod fc_api;
#[cfg(feature = "host-runtime")]
pub mod gateway_launch;
#[cfg(feature = "host-runtime")]
pub mod launch;

#[cfg(feature = "artifact-store")]
pub mod artifact_store;
#[cfg(all(feature = "artifact-store", feature = "host-runtime"))]
pub mod authenticated_source;
pub mod catalog;
pub mod file_delta;

pub mod launch_admission;
pub mod runtime_inventory;
pub mod selection;
pub mod verity;

pub mod snapshot_files;

pub mod proto {
    tonic::include_proto!("space.node.runtime.v1");
}

use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::transport::{Channel, Endpoint};

pub const RUNTIME_SUPERVISOR_SOCKET: &str =
    pplx_space_common_core::host::linux::paths::RUNTIME_SUPERVISOR_SOCKET;
const LOCAL_ENDPOINT_URI: &str = "http://[::]:50051";

pub type RuntimeSupervisorGrpcClient =
    proto::runtime_supervisor_client::RuntimeSupervisorClient<GrpcClientTrace<Channel>>;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeSupervisorConnectError {
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("runtime-supervisor transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
}

fn endpoint() -> Result<Endpoint, tonic::transport::Error> {
    Endpoint::try_from(LOCAL_ENDPOINT_URI)
}

pub async fn connect_runtime_supervisor_channel() -> Result<Channel, RuntimeSupervisorConnectError>
{
    Ok(endpoint()?
        .connect_with_connector(tower::service_fn(|_| async {
            let stream = tokio::net::UnixStream::connect(RUNTIME_SUPERVISOR_SOCKET).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
        }))
        .await?)
}

pub fn runtime_supervisor_client(channel: Channel) -> RuntimeSupervisorGrpcClient {
    proto::runtime_supervisor_client::RuntimeSupervisorClient::new(wrap_grpc_client_channel(
        channel,
    ))
}

pub async fn connect_runtime_supervisor()
-> Result<RuntimeSupervisorGrpcClient, RuntimeSupervisorConnectError> {
    let channel = connect_runtime_supervisor_channel().await?;
    Ok(runtime_supervisor_client(channel))
}

/// Copy a file using btrfs reflink (instant CoW clone) on Linux.
/// Falls back to `clonefile` on macOS, then to regular copy.
pub fn reflink_copy(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        pplx_space_common_io::reflink::copy_file(src, dst).map(|_| ())
    }

    #[cfg(target_os = "macos")]
    {
        let status = std::process::Command::new("cp")
            .arg("-c")
            .arg(src)
            .arg(dst)
            .status()?;
        if status.success() {
            return Ok(());
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        std::fs::copy(src, dst)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

pub mod runtime_launch;

pub mod vm_state;

#[cfg(test)]
mod catalog_policy_tests;

pub mod artifact_layout;
