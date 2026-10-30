use super::{ControlFuture, RuntimeControl, StateWorkspace, VmState};
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
    #[error("Cloud Hypervisor API returned HTTP {code}: {detail}")]
    Status { code: StatusCode, detail: String },
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
            .request(
                Method::GET,
                "/api/v1/vm.info",
                Bytes::new(),
                REQUEST_TIMEOUT,
            )
            .await?;
        serde_json::from_slice(&body).map_err(ApiError::Decode)
    }

    async fn request(
        &self,
        method: Method,
        uri: &'static str,
        body: Bytes,
        timeout: Duration,
    ) -> Result<Bytes, ApiError> {
        tokio::time::timeout(timeout, async {
            let stream = tokio::net::UnixStream::connect(&self.socket).await?;
            // Authenticate the connection that carries the request, including
            // every reconnect. The pidfd rejects PID reuse while SO_PEERCRED
            // rejects a same-UID replacement listener at the expected path. The
            // launcher binds as root before dropping credentials and execing.
            let credentials = stream.peer_cred()?;
            if credentials.uid() != 0
                || credentials.gid() != 0
                || credentials.pid().and_then(|pid| u32::try_from(pid).ok())
                    != Some(self.peer.pid())
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
            let code = response.status();
            let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(ApiError::Body)?
                .to_bytes();
            drop(task);
            if !code.is_success() {
                return Err(ApiError::Status {
                    code,
                    detail: String::from_utf8_lossy(&body).into_owned(),
                });
            }
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

/// Supported upstream v54 VM states (excluding guest-debug BreakPoint).
/// Unknown values are an observation error, never a
/// guessed lifecycle state. Other response fields are owned by the CH adapter.
#[derive(Deserialize)]
enum State {
    Created,
    Running,
    Paused,
    Shutdown,
}

impl RuntimeControl for Client {
    fn capture_state(&self, workspace: StateWorkspace, memory_bytes: u64) -> ControlFuture<'_, ()> {
        Box::pin(async move {
            self.request(
                Method::PUT,
                "/api/v1/vm.snapshot",
                Bytes::from_static(match workspace {
                    StateWorkspace::Plain => br#"{"destination_url":"file:///state/capture"}"#,
                    StateWorkspace::Encrypted => {
                        br#"{"destination_url":"file:///state/encsnap/capture"}"#
                    }
                }),
                super::memory_state_timeout(memory_bytes),
            )
            .await?;
            Ok(())
        })
    }
    fn restore_state(&self, workspace: StateWorkspace, memory_bytes: u64) -> ControlFuture<'_, ()> {
        Box::pin(async move {
            // Both restore workspaces stay with the execution. The encrypted
            // volume remains mounted until the VMM exits, so its private
            // memory clone can also back guest RAM without an eager copy.
            self.request(Method::PUT, "/api/v1/vm.restore", Bytes::from_static(match workspace {
                StateWorkspace::Plain => br#"{"source_url":"file:///state/restore","prefault":false,"memory_restore_mode":"CopyOnWrite","resume":false}"#,
                StateWorkspace::Encrypted => br#"{"source_url":"file:///state/encsnap/restore","prefault":false,"memory_restore_mode":"CopyOnWrite","resume":false}"#,
            }), super::memory_state_timeout(memory_bytes)).await?;
            Ok(())
        })
    }
    fn set_paused(&self, paused: bool) -> ControlFuture<'_, ()> {
        Box::pin(async move {
            let endpoint = if paused {
                "/api/v1/vm.pause"
            } else {
                "/api/v1/vm.resume"
            };
            self.request(Method::PUT, endpoint, Bytes::new(), Duration::from_secs(10))
                .await?;
            Ok(())
        })
    }
    fn state(&self) -> ControlFuture<'_, VmState> {
        Box::pin(async move {
            let info = match self.info().await {
                Ok(info) => info,
                // Upstream defines vm.info 404 as an idle VMM with no VM yet.
                Err(ApiError::Status {
                    code: StatusCode::NOT_FOUND,
                    ..
                }) => return Ok(VmState::NotStarted),
                Err(error) => return Err(error.into()),
            };
            Ok(match info.state {
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
                REQUEST_TIMEOUT,
            )
            .await?;
            self.request(Method::PUT, "/api/v1/vm.boot", Bytes::new(), BOOT_TIMEOUT)
                .await?;
            Ok(())
        })
    }
    fn request_guest_shutdown(&self) -> ControlFuture<'_, ()> {
        Box::pin(async move {
            self.request(
                Method::PUT,
                "/api/v1/vm.power-button",
                Bytes::new(),
                REQUEST_TIMEOUT,
            )
            .await?;
            Ok(())
        })
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
