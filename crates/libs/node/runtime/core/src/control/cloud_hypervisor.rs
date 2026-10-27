use super::{ControlError, ControlFuture, RuntimeControl, VmState};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request, StatusCode, body::Bytes};
use hyper_util::rt::TokioIo;
use pplx_space_common_core::types::RuntimeKind;
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
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
    #[error("Cloud Hypervisor API deadline exceeded")]
    Timeout,
}

pub(super) struct Client {
    socket: PathBuf,
}

impl Client {
    pub(super) fn new(socket: PathBuf) -> Self {
        Self { socket }
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
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let stream = tokio::net::UnixStream::connect(&self.socket).await?;
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
    fn boot<'a>(&'a self, configuration: &'a str) -> ControlFuture<'a, ()> {
        Box::pin(async move {
            self.request(
                Method::PUT,
                "/api/v1/vm.create",
                Bytes::copy_from_slice(configuration.as_bytes()),
            )
            .await?;
            self.request(Method::PUT, "/api/v1/vm.boot", Bytes::new())
                .await?;
            Ok(())
        })
    }
    fn request_guest_shutdown(&self) -> ControlFuture<'_, ()> {
        Box::pin(async move {
            self.request(Method::PUT, "/api/v1/vm.power-button", Bytes::new())
                .await?;
            Ok(())
        })
    }
    fn pause(&self) -> ControlFuture<'_, ()> {
        unsupported("pause")
    }
    fn resume(&self) -> ControlFuture<'_, ()> {
        unsupported("resume")
    }
}

fn unsupported(operation: &'static str) -> ControlFuture<'static, ()> {
    Box::pin(async move {
        Err(ControlError::UnsupportedOperation {
            runtime: RuntimeKind::CloudHypervisor,
            operation,
        })
    })
}

#[cfg(test)]
mod tests;
