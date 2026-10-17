//! Process and filesystem service implementation for the SPACE guest daemon.
//!
//! The daemon uses the existing generated gRPC contract and owns listener
//! admission, readiness, worker entrypoint dispatch, and shutdown orchestration.
//! See the crate README for the guest process assumptions.

use pplx_space_common_core::types::{DEFAULT_GUEST_USER, EnvMap, GuestUserName};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::process_server::{
    Process, ProcessServer,
};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    CloseStdinRequest, CloseStdinResponse, ConnectProcessRequest, FileChunk, FileInfo,
    ListProcessesRequest, ProcessEvent, ProcessInfo, PtySize, ResizePtyRequest, ResizePtyResponse,
    SendInputRequest, SendInputResponse, SendSignalRequest, SendSignalResponse, StartRequest,
};
use pplx_space_spaced_guest_protocol::{
    invalid_argument_status, parse_env, proto_env, validate_optional_timeout,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

type GuestFileInfoStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileInfo, Status>> + Send>>;
type GuestFileChunkStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileChunk, Status>> + Send>>;
const IDEMPOTENCY_RECORD_TTL: Duration = Duration::from_secs(60);
const MAX_PROCESS_TAG_BYTES: usize = 4 * 1024;

mod execution;
mod filesystem;
mod guest_path;
mod idempotency;
mod identity;
mod placement;
mod process;
pub use execution::{ExecutionService, ExecutionServiceConfig};
use idempotency::IdempotencyStore;
pub use placement::ExecutionCgroups;
use process::ProcessRegistry;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemExecutionMode {
    IdentityWorkers,
    Direct,
}

/// Failure while starting the optional filesystem worker subprocess mode.
#[derive(Debug, thiserror::Error)]
#[error("filesystem worker startup failed: {source}")]
pub struct RunFilesystemWorkerError {
    #[source]
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl RunFilesystemWorkerError {
    #[cfg(target_os = "linux")]
    fn new(source: filesystem::FilesystemWorkerError) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

/// The service name the identity-isolated filesystem worker subprocess reports
/// to telemetry. Deliberately distinct from the daemon's own service name: the
/// worker emits service telemetry but does not own the systemd watchdog
/// contract.
///
/// The worker env builder pins this so an inherited OTel service name cannot
/// rename the child, so it resolves without telemetry enabled.
pub const WORKER_SERVICE_NAME: &str = "space-spaced-guest-worker";

pub fn run_filesystem_worker_from_environment() -> Result<bool, RunFilesystemWorkerError> {
    #[cfg(target_os = "linux")]
    {
        filesystem::run_worker_from_environment().map_err(RunFilesystemWorkerError::new)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(false)
    }
}

#[cfg(target_os = "linux")]
const FILESYSTEM_WORKER_MODE_ENV: &str = "SPACE_SPACED_FILESYSTEM_WORKER";

pub fn filesystem_worker_requested() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::env::var_os(FILESYSTEM_WORKER_MODE_ENV).is_some()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Mark an admission refusal before any execution work started.
pub(crate) fn refused(status: Status) -> Status {
    pplx_space_common_grpc::refused_before_start(status, Duration::from_secs(1))
}
