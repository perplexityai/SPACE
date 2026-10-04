use crate::RetryPolicy;

#[derive(Debug, thiserror::Error)]
pub enum VsockConnectError {
    #[error("failed to create gRPC endpoint: {0}")]
    Endpoint(#[source] tonic::transport::Error),
    #[error("failed to connect to {service_name} at vsock cid={cid} port={port}: {source}")]
    Transport {
        service_name: &'static str,
        cid: u32,
        port: u32,
        #[source]
        source: tonic::transport::Error,
    },
}

pub async fn connect_vsock_channel_with_retry(
    service_name: &'static str,
    cid: u32,
    port: u32,
    retry_policy: RetryPolicy,
) -> Result<tonic::transport::Channel, VsockConnectError> {
    crate::retry_policy::retry_connect(service_name, retry_policy, || {
        connect_vsock_channel(service_name, cid, port)
    })
    .await
}

pub async fn connect_vsock_channel(
    service_name: &'static str,
    cid: u32,
    port: u32,
) -> Result<tonic::transport::Channel, VsockConnectError> {
    let endpoint = tonic::transport::Endpoint::try_from("http://[::]:50051")
        .map(|endpoint| endpoint.max_frame_size(crate::BULK_HTTP2_MAX_FRAME_SIZE))
        .map_err(VsockConnectError::Endpoint)?;
    endpoint
        .connect_with_connector(tower::service_fn(move |_| async move {
            let stream =
                tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(cid, port)).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
        }))
        .await
        .map_err(|source| VsockConnectError::Transport {
            service_name,
            cid,
            port,
            source,
        })
}

pub fn bind_vsock(port: u32) -> std::io::Result<tokio_vsock::VsockListener> {
    tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
        tokio_vsock::VMADDR_CID_ANY,
        port,
    ))
}

pub const BRIDGE_HOST_VSOCK_PORT: u32 = 5001;
pub const BRIDGE_GUEST_VSOCK_PORT: u32 = 5002;

pub fn firecracker_vsock_port_path(
    vsock_uds_path: &std::path::Path,
    port: u32,
) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}_{}", vsock_uds_path.display(), port))
}

/// Connect to one authenticated Firecracker process without allowing the
/// channel to reconnect to a replacement process under the same socket path.
pub async fn connect_firecracker_vsock_channel_with_expected_peer(
    service_name: &'static str,
    vsock_uds_path: &std::path::Path,
    port: u32,
    expected_peer: crate::ExpectedUdsPeer,
) -> Result<tonic::transport::Channel, FirecrackerVsockConnectError> {
    let path = vsock_uds_path.to_path_buf();
    let endpoint = tonic::transport::Endpoint::try_from("http://[::]:50051")
        .map(|endpoint| endpoint.max_frame_size(crate::BULK_HTTP2_MAX_FRAME_SIZE))
        .map_err(FirecrackerVsockConnectError::Endpoint)?;
    let connection_claimed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    endpoint
        .connect_with_connector(tower::service_fn(move |_| {
            let path = path.clone();
            let connection_claimed = connection_claimed.clone();
            async move {
                use tokio::io::AsyncWriteExt;
                crate::uds::claim_single_connection(&connection_claimed)?;
                let mut stream = tokio::net::UnixStream::connect(&path).await?;
                crate::uds::validate_expected_uds_peer(&stream, expected_peer)?;
                stream
                    .write_all(format!("CONNECT {port}\n").as_bytes())
                    .await?;
                read_firecracker_ack(&mut stream).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|source| FirecrackerVsockConnectError::Transport {
            service_name,
            path: vsock_uds_path.to_path_buf(),
            port,
            source,
        })
}

async fn read_firecracker_ack(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt;
    // The ACK reports the assigned host-side port, not the requested guest port.
    // Do not buffer past the newline: subsequent bytes belong to HTTP/2.
    let mut ack = Vec::with_capacity(32);
    for _ in 0..32 {
        let byte = stream.read_u8().await?;
        if byte == b'\n' {
            let port = std::str::from_utf8(&ack)
                .ok()
                .and_then(|line| line.strip_prefix("OK "))
                .filter(|port| !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()))
                .and_then(|port| port.parse::<u32>().ok());
            return port.map(|_| ()).ok_or_else(|| {
                std::io::Error::other("invalid Firecracker CONNECT acknowledgement")
            });
        }
        ack.push(byte);
    }
    Err(std::io::Error::other(
        "Firecracker CONNECT acknowledgement too long",
    ))
}

#[derive(Debug, thiserror::Error)]
pub enum FirecrackerVsockConnectError {
    #[error("failed to create gRPC endpoint: {0}")]
    Endpoint(#[source] tonic::transport::Error),
    #[error("failed to connect to {service_name} at firecracker vsock {path}:{port}: {source}")]
    Transport {
        service_name: &'static str,
        path: std::path::PathBuf,
        port: u32,
        #[source]
        source: tonic::transport::Error,
    },
}

#[cfg(test)]
mod tests;
