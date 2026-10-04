use std::path::{Path, PathBuf};

use crate::RetryPolicy;

#[derive(Debug, thiserror::Error)]
pub enum UdsConnectError {
    #[error("failed to create gRPC endpoint: {0}")]
    Endpoint(#[source] tonic::transport::Error),
    #[error("failed to connect to {service_name} at {path}: {source}")]
    Transport {
        service_name: &'static str,
        path: PathBuf,
        #[source]
        source: tonic::transport::Error,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpectedUdsPeer {
    pub pid: i32,
    pub uid: u32,
}

impl ExpectedUdsPeer {
    pub const fn new(pid: i32, uid: u32) -> Self {
        Self { pid, uid }
    }
}

pub(crate) fn claim_single_connection(
    claimed: &std::sync::atomic::AtomicBool,
) -> std::io::Result<()> {
    if claimed.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "authenticated UDS channel cannot reconnect without re-attestation",
        ));
    }
    Ok(())
}

pub(crate) fn validate_expected_uds_peer(
    stream: &tokio::net::UnixStream,
    expected_peer: ExpectedUdsPeer,
) -> std::io::Result<()> {
    let credentials = stream.peer_cred()?;
    if credentials.pid() == Some(expected_peer.pid) && credentials.uid() == expected_peer.uid {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        format!(
            "UDS peer credential mismatch: expected pid={} uid={}, got pid={:?} uid={}",
            expected_peer.pid,
            expected_peer.uid,
            credentials.pid(),
            credentials.uid(),
        ),
    ))
}

pub async fn connect_uds_channel_with_retry(
    service_name: &'static str,
    socket_path: &Path,
    retry_policy: RetryPolicy,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    crate::retry_policy::retry_connect(service_name, retry_policy, || {
        connect_uds_channel(service_name, socket_path)
    })
    .await
}

/// Retry establishment of the initial authenticated UDS connection. Once
/// established, the returned channel does not reconnect to a replacement peer.
pub async fn connect_uds_channel_with_expected_peer_and_retry(
    service_name: &'static str,
    socket_path: &Path,
    expected_peer: ExpectedUdsPeer,
    retry_policy: RetryPolicy,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    crate::retry_policy::retry_connect(service_name, retry_policy, || {
        connect_uds_channel_with_expected_peer(service_name, socket_path, expected_peer)
    })
    .await
}

pub async fn connect_uds_channel(
    service_name: &'static str,
    socket_path: &Path,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    connect_uds_channel_inner(service_name, socket_path, None).await
}

/// Connect to one authenticated UDS peer without allowing transparent channel
/// reconnection. A caller that needs to reconnect must obtain and validate a
/// fresh peer identity before constructing another channel.
pub async fn connect_uds_channel_with_expected_peer(
    service_name: &'static str,
    socket_path: &Path,
    expected_peer: ExpectedUdsPeer,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    connect_uds_channel_inner(service_name, socket_path, Some(expected_peer)).await
}

async fn connect_uds_channel_inner(
    service_name: &'static str,
    socket_path: &Path,
    expected_peer: Option<ExpectedUdsPeer>,
) -> Result<tonic::transport::Channel, UdsConnectError> {
    let path = socket_path.to_path_buf();
    let endpoint = tonic::transport::Endpoint::try_from("http://[::]:50051")
        .map(|endpoint| endpoint.max_frame_size(crate::BULK_HTTP2_MAX_FRAME_SIZE))
        .map_err(UdsConnectError::Endpoint)?;
    // A channel authenticated only by a PID must not transparently reconnect:
    // the PID can be reused after the original peer exits. The caller can
    // validate stronger activation evidence after this initial connection;
    // any later reconnect has to restart that entire attestation sequence.
    let connection_claimed = expected_peer
        .is_some()
        .then(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)));
    endpoint
        .connect_with_connector(tower::service_fn(move |_| {
            let socket_path = path.clone();
            let connection_claimed = connection_claimed.clone();
            async move {
                if let Some(claimed) = connection_claimed {
                    claim_single_connection(&claimed)?;
                }
                let stream = tokio::net::UnixStream::connect(&socket_path).await?;
                if let Some(expected_peer) = expected_peer {
                    validate_expected_uds_peer(&stream, expected_peer)?;
                }
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|source| UdsConnectError::Transport {
            service_name,
            path: socket_path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests;

pub fn default_bridge_guest_uds_path() -> PathBuf {
    pplx_space_common_core::guest::linux::paths::default_guest_uds_path(
        pplx_space_common_core::guest::linux::paths::SPACED_BRIDGE_GUEST_SOCKET_FILE,
    )
}

pub fn default_bridge_host_uds_path() -> PathBuf {
    pplx_space_common_core::guest::linux::paths::default_guest_uds_path(
        pplx_space_common_core::guest::linux::paths::SPACED_BRIDGE_HOST_SOCKET_FILE,
    )
}
