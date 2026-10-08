use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use pplx_space_common_core::types::{BalloonConfig, BalloonMode, RateLimiter, TokenBucket};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};
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

/// Firecracker's free-page-hinting command id meaning a run is over. With
/// `acknowledge_on_stop`, Firecracker sets it as soon as the guest finishes.
const FREE_PAGE_HINT_DONE: u32 = 1;

/// How long stopping an unfinished hinting run may take.
pub const FREE_PAGE_HINTING_STOP_TIMEOUT: Duration = Duration::from_secs(1);

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
        let v = self.get_json("/").await?;
        let state = v
            .get("state")
            .and_then(serde_json::Value::as_str)
            .ok_or(FcApiError::InvalidResponseField { field: "state" })?;
        state.parse().map_err(Into::into)
    }

    /// The balloon device the VM was configured with, or `None` when it has
    /// none. Firecracker answers 400 exactly when no device is configured.
    pub async fn balloon(&self) -> Result<Option<FirecrackerBalloon>, FcApiError> {
        match self.get_json("/balloon").await {
            Ok(body) => FirecrackerBalloon::from_json(&body).map(Some),
            Err(FcApiError::Status { status, .. }) if status == hyper::StatusCode::BAD_REQUEST => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// The balloon's latest statistics. Firecracker serves them only when
    /// the device was configured with a nonzero `stats_polling_interval_s`.
    pub async fn balloon_statistics(&self) -> Result<FirecrackerBalloonStatistics, FcApiError> {
        FirecrackerBalloonStatistics::from_json(&self.get_json("/balloon/statistics").await?)
    }

    /// Start a free-page-hinting run that Firecracker acknowledges itself
    /// once the guest reports it has hinted every free range.
    pub async fn start_free_page_hinting(&self) -> Result<(), FcApiError> {
        let body = serde_json::json!({ "acknowledge_on_stop": true });
        self.request("PATCH", "/balloon/hinting/start", &body).await
    }

    pub async fn free_page_hinting_status(&self) -> Result<FreePageHintingStatus, FcApiError> {
        FreePageHintingStatus::from_json(&self.get_json("/balloon/hinting/status").await?)
    }

    /// End a hinting run, letting the guest reuse the ranges it hinted.
    pub async fn stop_free_page_hinting(&self) -> Result<(), FcApiError> {
        self.request("PATCH", "/balloon/hinting/stop", &serde_json::json!({}))
            .await
    }

    /// Run one free-page-hinting pass on a running VM and wait, polling
    /// every `poll_interval`, until the guest completes it or `timeout`
    /// passes. A run that has not completed by then is stopped (within
    /// [`FREE_PAGE_HINTING_STOP_TIMEOUT`]) and reported as
    /// [`FreePageHintOutcome::TimedOut`]; the latency bound is
    /// `timeout + FREE_PAGE_HINTING_STOP_TIMEOUT`.
    pub async fn hint_free_pages(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<FreePageHintOutcome, FcApiError> {
        let started = Instant::now();
        let run = async {
            self.start_free_page_hinting().await?;
            loop {
                if self.free_page_hinting_status().await?.completed() {
                    return Ok(());
                }
                tokio::time::sleep(poll_interval).await;
            }
        };
        let result = tokio::time::timeout(timeout, run).await;
        if let Ok(Ok(())) = result {
            return Ok(FreePageHintOutcome::Completed {
                elapsed: started.elapsed(),
            });
        }
        match tokio::time::timeout(
            FREE_PAGE_HINTING_STOP_TIMEOUT,
            self.stop_free_page_hinting(),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "failed to stop unfinished Firecracker free-page hinting");
            }
            Err(_) => tracing::warn!(
                timeout_ms = FREE_PAGE_HINTING_STOP_TIMEOUT.as_millis(),
                "Firecracker did not stop unfinished free-page hinting in time"
            ),
        }
        match result {
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) | Err(_) => Ok(FreePageHintOutcome::TimedOut {
                elapsed: started.elapsed(),
            }),
        }
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, FcApiError> {
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
            .uri(path)
            .header("Accept", "application/json")
            .body(http_body_util::Empty::<Bytes>::new())?;

        let resp = sender.send_request(req).await?;
        let status = resp.status();

        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await?.to_bytes();
        if !status.is_success() {
            return Err(FcApiError::Status {
                method: "GET".to_string(),
                path: path.to_string(),
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        Ok(serde_json::from_slice(&body)?)
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

/// A Firecracker `balloon` device object, as the VM configuration takes it
/// at boot. Field names follow Firecracker's `BalloonDeviceConfig`, which
/// rejects unknown fields.
pub fn balloon_json(config: BalloonConfig) -> serde_json::Value {
    serde_json::json!({
        "amount_mib": config.amount_mib(),
        "deflate_on_oom": config.deflate_on_oom(),
        "stats_polling_interval_s": config.stats_polling_interval_s(),
        "free_page_hinting": config.free_page_hinting(),
        "free_page_reporting": config.free_page_reporting()
    })
}

/// A running VM's balloon device, as `GET /balloon` reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FirecrackerBalloon {
    pub amount_mib: u32,
    pub deflate_on_oom: bool,
    pub stats_polling_interval_s: u16,
    pub free_page_hinting: bool,
    pub free_page_reporting: bool,
}

impl FirecrackerBalloon {
    fn from_json(body: &serde_json::Value) -> Result<Self, FcApiError> {
        Ok(Self {
            amount_mib: required_u64(body, "amount_mib")?.try_into().map_err(|_| {
                FcApiError::InvalidResponseField {
                    field: "amount_mib",
                }
            })?,
            deflate_on_oom: body
                .get("deflate_on_oom")
                .and_then(serde_json::Value::as_bool)
                .ok_or(FcApiError::InvalidResponseField {
                    field: "deflate_on_oom",
                })?,
            stats_polling_interval_s: optional_u64(body, "stats_polling_interval_s")?
                .unwrap_or(0)
                .try_into()
                .map_err(|_| FcApiError::InvalidResponseField {
                    field: "stats_polling_interval_s",
                })?,
            free_page_hinting: optional_bool(body, "free_page_hinting")?,
            free_page_reporting: optional_bool(body, "free_page_reporting")?,
        })
    }

    /// The SPACE balloon mode that renders this device. SPACE enables one
    /// feature per device; a device with none is a plain inflation balloon.
    pub const fn mode(self) -> BalloonMode {
        if self.free_page_hinting {
            BalloonMode::FreePageHinting
        } else if self.free_page_reporting {
            BalloonMode::FreePageReporting
        } else {
            BalloonMode::Inflate
        }
    }
}

/// The subset of `GET /balloon/statistics` SPACE records. Guest-reported
/// fields are absent until the guest driver first reports them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FirecrackerBalloonStatistics {
    pub target_mib: u64,
    pub actual_mib: u64,
    pub free_memory_bytes: Option<u64>,
    pub available_memory_bytes: Option<u64>,
    pub total_memory_bytes: Option<u64>,
    pub disk_caches_bytes: Option<u64>,
}

impl FirecrackerBalloonStatistics {
    fn from_json(body: &serde_json::Value) -> Result<Self, FcApiError> {
        Ok(Self {
            target_mib: required_u64(body, "target_mib")?,
            actual_mib: required_u64(body, "actual_mib")?,
            free_memory_bytes: optional_u64(body, "free_memory")?,
            available_memory_bytes: optional_u64(body, "available_memory")?,
            total_memory_bytes: optional_u64(body, "total_memory")?,
            disk_caches_bytes: optional_u64(body, "disk_caches")?,
        })
    }
}

/// `GET /balloon/hinting/status`: the last command id each side sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FreePageHintingStatus {
    pub host_cmd: u32,
    pub guest_cmd: Option<u32>,
}

impl FreePageHintingStatus {
    fn from_json(body: &serde_json::Value) -> Result<Self, FcApiError> {
        let cmd = |field| {
            optional_u64(body, field)?
                .map(|value| {
                    u32::try_from(value).map_err(|_| FcApiError::InvalidResponseField { field })
                })
                .transpose()
        };
        Ok(Self {
            host_cmd: cmd("host_cmd")?
                .ok_or(FcApiError::InvalidResponseField { field: "host_cmd" })?,
            guest_cmd: cmd("guest_cmd")?,
        })
    }

    /// Whether the run started with `acknowledge_on_stop` has completed.
    pub const fn completed(self) -> bool {
        self.host_cmd == FREE_PAGE_HINT_DONE
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FreePageHintOutcome {
    Completed { elapsed: Duration },
    TimedOut { elapsed: Duration },
}

fn required_u64(body: &serde_json::Value, field: &'static str) -> Result<u64, FcApiError> {
    optional_u64(body, field)?.ok_or(FcApiError::InvalidResponseField { field })
}

fn optional_u64(body: &serde_json::Value, field: &'static str) -> Result<Option<u64>, FcApiError> {
    match body.get(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or(FcApiError::InvalidResponseField { field }),
    }
}

fn optional_bool(body: &serde_json::Value, field: &'static str) -> Result<bool, FcApiError> {
    match body.get(field) {
        None | Some(serde_json::Value::Null) => Ok(false),
        Some(value) => value
            .as_bool()
            .ok_or(FcApiError::InvalidResponseField { field }),
    }
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
