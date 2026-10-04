use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use pplx_space_common_core::types::{RateLimiter, TokenBucket};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tokio::net::UnixStream;

pub struct FcApiClient {
    socket_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum FcApiError {
    #[error("Firecracker API I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Firecracker API HTTP error: {0}")]
    Http(#[from] hyper::http::Error),
    #[error("Firecracker API transport failed: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("Firecracker API JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Firecracker API path {field} is not valid UTF-8: {path:?}")]
    NonUtf8Path { field: &'static str, path: PathBuf },
    #[error("Firecracker API response field {field} is missing or invalid")]
    InvalidResponseField { field: &'static str },
    #[error("Firecracker API request body is not a JSON object")]
    RequestBodyNotObject,
    #[error(transparent)]
    InvalidVmState(#[from] FirecrackerVmStateParseError),
    #[error("Firecracker API {method} {path}: {status} - {body}")]
    Status {
        method: String,
        path: String,
        status: hyper::StatusCode,
        body: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FirecrackerVmState {
    NotStarted,
    Running,
    Paused,
}

impl FirecrackerVmState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "Not started",
            Self::Running => "Running",
            Self::Paused => "Paused",
        }
    }
}

impl fmt::Display for FirecrackerVmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown Firecracker VM state {0:?}")]
pub struct FirecrackerVmStateParseError(String);

impl FromStr for FirecrackerVmState {
    type Err = FirecrackerVmStateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Not started" => Ok(Self::NotStarted),
            "Running" => Ok(Self::Running),
            "Paused" => Ok(Self::Paused),
            other => Err(FirecrackerVmStateParseError(other.to_string())),
        }
    }
}

// SPACE intentionally uses Firecracker diff snapshots with dirty-page tracking.
// Full snapshots were measured to write the entire guest memory file on every
// suspend/checkpoint, while diff snapshots scale with the dirty set and do not
// regress restore latency.
const CREATE_SNAPSHOT_TYPE: &str = "Diff";
const LOAD_SNAPSHOT_RESUME_VM: bool = false;
const LOAD_SNAPSHOT_TRACK_DIRTY_PAGES: bool = true;

impl FcApiClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Pause the VM (freeze vCPUs).
    pub async fn pause(&self) -> Result<(), FcApiError> {
        self.patch_vm("Paused").await
    }

    /// Resume the VM (thaw vCPUs).
    pub async fn resume(&self) -> Result<(), FcApiError> {
        self.patch_vm("Resumed").await
    }

    /// Create a snapshot using SPACE's default Firecracker snapshot options.
    /// VM must be paused first.
    pub async fn create_snapshot(
        &self,
        snapshot_path: impl AsRef<Path>,
        mem_file_path: impl AsRef<Path>,
    ) -> Result<(), FcApiError> {
        let body = create_snapshot_request_body(snapshot_path, mem_file_path)?;
        self.put("/snapshot/create", &body).await
    }

    /// Load a snapshot. Call on a fresh Firecracker instance before resuming.
    pub async fn load_snapshot(
        &self,
        snapshot_path: impl AsRef<Path>,
        mem_file_path: impl AsRef<Path>,
    ) -> Result<(), FcApiError> {
        let body = load_snapshot_request_body(snapshot_path, mem_file_path)?;
        self.put("/snapshot/load", &body).await
    }

    /// Replace a drive's rate limiter. Valid once the VM exists, including
    /// right after a snapshot load while it is still paused.
    pub async fn update_drive_rate_limiter(
        &self,
        drive_id: &str,
        limiter: &RateLimiter,
    ) -> Result<(), FcApiError> {
        let body = drive_rate_limiter_request_body(drive_id, limiter);
        self.request("PATCH", &format!("/drives/{drive_id}"), &body)
            .await
    }

    /// Query the VM state.
    pub async fn get_vm_state(&self) -> Result<FirecrackerVmState, FcApiError> {
        let stream = UnixStream::connect(&self.socket_path).await?;
        let io = TokioIo::new(stream);

        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
        tokio::spawn(async move {
            if let Err(error) = conn.await {
                tracing::debug!(%error, "Firecracker API connection task failed");
            }
        });

        let req = hyper::Request::builder()
            .method("GET")
            .uri("/")
            .header("Accept", "application/json")
            .body(http_body_util::Empty::<Bytes>::new())?;

        let resp = sender.send_request(req).await?;
        let status = resp.status();

        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await?.to_bytes();
        if !status.is_success() {
            return Err(FcApiError::Status {
                method: "GET".to_string(),
                path: "/".to_string(),
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        let v: serde_json::Value = serde_json::from_slice(&body)?;
        let state = v
            .get("state")
            .and_then(serde_json::Value::as_str)
            .ok_or(FcApiError::InvalidResponseField { field: "state" })?;
        state.parse().map_err(Into::into)
    }

    async fn patch_vm(&self, state: &str) -> Result<(), FcApiError> {
        let body = serde_json::json!({"state": state});
        self.request("PATCH", "/vm", &body).await
    }

    async fn put(&self, path: &str, body: &serde_json::Value) -> Result<(), FcApiError> {
        self.request("PUT", path, body).await
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<(), FcApiError> {
        let stream = UnixStream::connect(&self.socket_path).await?;
        let io = TokioIo::new(stream);

        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
        tokio::spawn(async move {
            if let Err(error) = conn.await {
                tracing::debug!(%error, "Firecracker API connection task failed");
            }
        });

        let body_bytes = serde_json::to_vec(body)?;
        let req = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .body(http_body_util::Full::new(Bytes::from(body_bytes)))?;

        let resp = sender.send_request(req).await?;
        let status = resp.status();

        if !status.is_success() {
            use http_body_util::BodyExt;
            let body = resp.into_body().collect().await?.to_bytes();
            let msg = String::from_utf8_lossy(&body);
            return Err(FcApiError::Status {
                method: method.to_string(),
                path: path.to_string(),
                status,
                body: msg.to_string(),
            });
        }

        Ok(())
    }
}

fn firecracker_path_str<'a>(field: &'static str, path: &'a Path) -> Result<&'a str, FcApiError> {
    path.to_str().ok_or_else(|| FcApiError::NonUtf8Path {
        field,
        path: path.to_path_buf(),
    })
}

fn create_snapshot_request_body(
    snapshot_path: impl AsRef<Path>,
    mem_file_path: impl AsRef<Path>,
) -> Result<serde_json::Value, FcApiError> {
    let snapshot_path = firecracker_path_str("snapshot_path", snapshot_path.as_ref())?;
    let mem_file_path = firecracker_path_str("mem_file_path", mem_file_path.as_ref())?;
    Ok(serde_json::json!({
        "snapshot_path": snapshot_path,
        "mem_file_path": mem_file_path,
        "snapshot_type": CREATE_SNAPSHOT_TYPE
    }))
}

fn drive_rate_limiter_request_body(drive_id: &str, limiter: &RateLimiter) -> serde_json::Value {
    serde_json::json!({
        "drive_id": drive_id,
        "rate_limiter": rate_limiter_json(limiter),
    })
}

/// A Firecracker `rate_limiter` object, as a drive or network interface
/// takes it at boot or in a PATCH.
pub fn rate_limiter_json(limiter: &RateLimiter) -> serde_json::Value {
    let bucket = |bucket: TokenBucket| serde_json::json!({ "size": bucket.size, "refill_time": bucket.refill_time_ms });
    serde_json::json!({
        "bandwidth": bucket(limiter.bandwidth),
        "ops": bucket(limiter.operations)
    })
}

fn load_snapshot_request_body(
    snapshot_path: impl AsRef<Path>,
    mem_file_path: impl AsRef<Path>,
) -> Result<serde_json::Value, FcApiError> {
    load_snapshot_request_body_for_arch(snapshot_path, mem_file_path, std::env::consts::ARCH)
}

fn load_snapshot_request_body_for_arch(
    snapshot_path: impl AsRef<Path>,
    mem_file_path: impl AsRef<Path>,
    arch: &str,
) -> Result<serde_json::Value, FcApiError> {
    let snapshot_path = firecracker_path_str("snapshot_path", snapshot_path.as_ref())?;
    let backend_path = firecracker_path_str("mem_backend.backend_path", mem_file_path.as_ref())?;
    let mut body = serde_json::json!({
        "snapshot_path": snapshot_path,
        "mem_backend": {
            "backend_type": "File",
            "backend_path": backend_path
        },
        "resume_vm": LOAD_SNAPSHOT_RESUME_VM,
        "track_dirty_pages": LOAD_SNAPSHOT_TRACK_DIRTY_PAGES
    });
    if arch == "x86_64" {
        body.as_object_mut()
            .ok_or(FcApiError::RequestBodyNotObject)?
            .insert("clock_realtime".to_string(), true.into());
    }
    Ok(body)
}

#[cfg(test)]
mod tests;
