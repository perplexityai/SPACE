use super::{ControlFuture, RuntimeControl, VmState};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request, StatusCode, body::Bytes};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const BOOT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Cloud Hypervisor API I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("Cloud Hypervisor API transport: {0}")]
    Transport(#[from] hyper::Error),
    #[error("Cloud Hypervisor API request: {0}")]
    Request(#[from] hyper::http::Error),
    #[error("Cloud Hypervisor API response exceeds its bound or cannot be read: {0}")]
    Body(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Cloud Hypervisor API returned HTTP {0}")]
    Status(StatusCode),
    #[error("Cloud Hypervisor API returned invalid VM information: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("control listener is not the registered runtime process")]
    PeerMismatch,
    #[error("Cloud Hypervisor API deadline exceeded")]
    Timeout,
}

pub(super) struct Client {
    socket: PathBuf,
    peer: crate::launch::ProcessObserver,
}

impl Client {
    pub(super) fn new(socket: PathBuf, peer: crate::launch::ProcessObserver) -> Self {
        Self { socket, peer }
    }

    /// One deadline covers connect, handshake, headers and a bounded body. A
    /// dropped caller aborts the connection task; control requests are not replayed.
    async fn info(&self) -> Result<Info, ApiError> {
        let body = self
            .request(Method::GET, "/api/v1/vm.info", Bytes::new())
            .await?;
        serde_json::from_slice(&body).map_err(ApiError::Decode)
    }

    async fn request(
        &self,
        method: Method,
        uri: &'static str,
        body: Bytes,
    ) -> Result<Bytes, ApiError> {
        let timeout = if uri == "/api/v1/vm.boot" {
            BOOT_TIMEOUT
        } else {
            REQUEST_TIMEOUT
        };
        tokio::time::timeout(timeout, async {
            let stream = tokio::net::UnixStream::connect(&self.socket).await?;
            // Authenticate the connection that carries the request, including
            // every reconnect. The pidfd rejects PID reuse while SO_PEERCRED
            // rejects a same-UID replacement listener at the expected path.
            let credentials = stream.peer_cred()?;
            if credentials.pid().and_then(|pid| u32::try_from(pid).ok()) != Some(self.peer.pid())
                || self.peer.has_exited()?
            {
                return Err(ApiError::PeerMismatch);
            }
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
            let task = ConnectionTask(tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::debug!(%error, "runtime control connection closed");
                }
            }));
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .header("Host", "localhost")
                .header("Content-Type", "application/json")
                .body(Full::new(body))?;
            let response = sender.send_request(request).await?;
            if !response.status().is_success() {
                return Err(ApiError::Status(response.status()));
            }
            let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(ApiError::Body)?
                .to_bytes();
            drop(task);
            Ok(body)
        })
        .await
        .map_err(|_| ApiError::Timeout)?
    }
}

struct ConnectionTask(tokio::task::JoinHandle<()>);
impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Deserialize)]
struct Info {
    state: State,
}

/// Upstream v54 VM states. Unknown values are an observation error, never a
/// guessed lifecycle state. Other response fields are owned by the CH adapter.
#[derive(Deserialize)]
enum State {
    Created,
    Running,
    Paused,
    Shutdown,
}

impl RuntimeControl for Client {
    fn state(&self) -> ControlFuture<'_, VmState> {
        Box::pin(async move {
            Ok(match self.info().await?.state {
                State::Created => VmState::NotStarted,
                State::Running => VmState::Running,
                State::Paused => VmState::Paused,
                State::Shutdown => VmState::Shutdown,
            })
        })
    }
    fn boot<'a>(&'a self, configuration: &'a crate::boot::StandaloneBoot) -> ControlFuture<'a, ()> {
        Box::pin(async move {
            self.request(
                Method::PUT,
                "/api/v1/vm.create",
                Bytes::from(configuration.encode().map_err(ApiError::Decode)?),
            )
            .await?;
            self.request(Method::PUT, "/api/v1/vm.boot", Bytes::new())
                .await?;
            Ok(())
        })
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
