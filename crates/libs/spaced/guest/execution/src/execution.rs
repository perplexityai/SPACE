//! Process and filesystem state shared by the guest daemon adapters.
use super::*;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{WriteRequest, WriteResponse};

pub(crate) const DEFAULT_NATIVE_UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared admission check for process, filesystem and environment operations.
pub(crate) fn require_running(shutdown: &CancellationToken) -> Result<(), Status> {
    if shutdown.is_cancelled() {
        return Err(crate::refused(Status::unavailable("guest shutting down")));
    }
    Ok(())
}

/// Configuration for process execution and the filesystem backend.
///
/// # Defaults
///
/// Linux defaults to restricted identity workers. Callers must supply
/// [`Self::worker_executable`] and a root launcher with effective CAP_SETUID and
/// CAP_SETGID; otherwise construction returns
/// [`CreateExecutionServiceError::MissingWorkerExecutable`] or
/// [`CreateExecutionServiceError::RestrictedRequiresPrivilege`], respectively.
/// Other platforms default to Direct filesystem execution with the guest policy.
#[derive(Clone, Debug)]
pub struct ExecutionServiceConfig {
    pub default_operation_user: GuestUserName,
    /// Image WORKDIR for commands without an explicit cwd; None uses the account home.
    pub default_working_dir: Option<std::path::PathBuf>,
    pub filesystem_execution: FilesystemExecutionMode,
    pub identity_policy: IdentityPolicy,
    /// Required on Linux when using identity workers. Never inferred from the host.
    pub worker_executable: Option<crate::WorkerExecutable>,
    /// Grace for supervised filesystem work after request cancellation. Service
    /// cancellation can terminate identity workers immediately, without this grace.
    pub cancellation_grace: Duration,
    /// Maximum wait for each native upload message, including the initial `Open`.
    /// Defaults to 30 seconds. Applies only to native `write_file`; the gRPC
    /// stream retains its existing transport behavior. Does not bound cleanup,
    /// execution backpressure or the total upload duration. Zero gives pending
    /// input no grace period; it does not disable the timeout.
    pub native_upload_idle_timeout: Duration,
    /// Guest memory used to size retained process output; None uses the fallback budget.
    pub guest_memory_bytes: Option<u64>,
}

impl Default for ExecutionServiceConfig {
    fn default() -> Self {
        Self {
            default_working_dir: None,
            default_operation_user: GuestUserName::parse(DEFAULT_GUEST_USER)
                .expect("default guest user constant is valid"),
            filesystem_execution: if cfg!(target_os = "linux") {
                FilesystemExecutionMode::IdentityWorkers
            } else {
                FilesystemExecutionMode::Direct
            },
            cancellation_grace: filesystem::router::DEFAULT_CANCELLATION_GRACE,
            native_upload_idle_timeout: DEFAULT_NATIVE_UPLOAD_IDLE_TIMEOUT,
            guest_memory_bytes: None,
            worker_executable: None,
            identity_policy: if cfg!(target_os = "linux") {
                IdentityPolicy::Restricted
            } else {
                IdentityPolicy::Guest
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateExecutionServiceError {
    #[error("identity workers require an explicit worker executable")]
    MissingWorkerExecutable,
    #[error("identity workers require Linux")]
    UnsupportedIdentityWorkers,
    #[error("restricted identities require identity workers, not Direct filesystem execution")]
    RestrictedRequiresIdentityWorkers,
    #[error("restricted identities require a root launcher with CAP_SETUID and CAP_SETGID")]
    RestrictedRequiresPrivilege,
    #[error("cannot inspect launcher capabilities: {0}")]
    InspectPrivileges(#[source] std::io::Error),
}

#[derive(Clone)]
pub struct ExecutionService {
    pub(super) inner: Arc<ExecutionServiceInner>,
}

pub(super) struct ExecutionServiceInner {
    pub(super) shutdown: CancellationToken,
    pub(super) process_tasks: tokio_util::task::TaskTracker,
    // Orders process-start admission with shutdown, without holding the gate
    // across a spawn that could stall unrelated starts.
    pub(super) start_gate: std::sync::Mutex<()>,
    pub(super) process_payload_slots: Arc<Semaphore>,
    pub(super) default_operation_user: GuestUserName,
    pub(super) default_working_dir: Option<std::path::PathBuf>,
    pub(super) identity_policy: IdentityPolicy,
    pub(super) process_env: RwLock<EnvMap>,
    pub(super) processes: ProcessRegistry,
    pub(super) idempotency: IdempotencyStore,
    pub(super) filesystem: filesystem::FilesystemHandle,
    pub(super) guest_cgroups: Option<Arc<ExecutionCgroups>>,
}

impl ExecutionService {
    /// Construct a service with the caller's filesystem execution configuration.
    ///
    /// Caller cancellation closes the service. Closing or draining the service
    /// does not cancel the caller's token.
    ///
    /// # Errors
    ///
    /// Identity workers require an explicit executable on Linux. Selecting them
    /// on another platform returns an unsupported-mode error. No worker is
    /// launched during construction; protocol and account checks occur at worker startup.
    /// Restricted identities require identity workers and a root launcher with
    /// effective CAP_SETUID and CAP_SETGID. This checks the calling thread's
    /// current authority, not what the worker retains across exec: securebits,
    /// capability bounding sets and executable attributes can still cause worker
    /// startup to fail closed. Restricted requests must resolve to a nonzero UID.
    pub fn with_config(
        shutdown: CancellationToken,
        config: ExecutionServiceConfig,
    ) -> Result<Self, CreateExecutionServiceError> {
        Self::with_cgroups(shutdown, config, None)
    }

    /// Construct a service with optional caller-provided workload placement.
    ///
    /// Cancellation has the same scope as [`Self::with_config`].
    ///
    /// # Errors
    ///
    /// Returns the same configuration errors as [`Self::with_config`].
    pub fn with_cgroups(
        shutdown: CancellationToken,
        config: ExecutionServiceConfig,
        guest_cgroups: Option<Arc<ExecutionCgroups>>,
    ) -> Result<Self, CreateExecutionServiceError> {
        let shutdown = shutdown.child_token();
        let filesystem = filesystem::FilesystemHandle::new(
            config.filesystem_execution,
            config.worker_executable,
            config.identity_policy,
            config.default_operation_user.clone(),
            shutdown.clone(),
            config.cancellation_grace,
            guest_cgroups.clone(),
        )?
        .with_native_upload_idle_timeout(config.native_upload_idle_timeout);
        config
            .identity_policy
            .validate(config.filesystem_execution)?;
        Ok(Self {
            inner: Arc::new(ExecutionServiceInner {
                shutdown,
                process_tasks: tokio_util::task::TaskTracker::new(),
                start_gate: std::sync::Mutex::new(()),
                process_payload_slots: Arc::new(Semaphore::new(
                    pplx_space_spaced_guest_protocol::process_limits::PAYLOAD_SLOTS,
                )),
                default_operation_user: config.default_operation_user,
                default_working_dir: config.default_working_dir,
                identity_policy: config.identity_policy,
                process_env: RwLock::new(EnvMap::empty()),
                processes: ProcessRegistry::new(process::ProcessOutputBudget::for_guest_memory(
                    config.guest_memory_bytes,
                )),
                idempotency: IdempotencyStore::new("process"),
                filesystem,
                guest_cgroups,
            }),
        })
    }

    pub(super) fn require_running(&self) -> Result<(), Status> {
        require_running(&self.inner.shutdown)
    }

    /// Close operation admission and signal cancellation of owned work.
    ///
    /// Calls through every service clone and previously obtained filesystem handle
    /// are refused once admission is closed. Repeated calls are harmless. Work
    /// admitted earlier is signalled to cancel immediately. Service or caller-token
    /// cancellation can terminate identity workers immediately, leaving outcomes
    /// unknown. [`ExecutionServiceConfig::cancellation_grace`] applies to request
    /// cancellation, not service shutdown. Direct execution still waits for its
    /// outstanding I/O. This is abortive shutdown, not graceful admission-only
    /// closure. It neither waits
    /// for cleanup nor cancels the caller's token, and cannot roll back effects.
    pub fn close(&self) {
        let _start_gate = self
            .inner
            .start_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.shutdown.cancel();
        self.inner.process_tasks.close();
    }

    /// Drain guest-owned work during the daemon's cancellation and listener
    /// shutdown. The daemon owns the overall shutdown deadline.
    pub async fn drain(&self) {
        self.close();
        tokio::join!(
            self.inner.filesystem.drain(),
            self.inner.process_tasks.wait()
        );
    }

    /// Replace the environment defaults used by future process starts.
    ///
    /// [`EnvMap`] enforces the shared entry-count, key, value and aggregate byte
    /// limits at construction. Request environment entries override these defaults;
    /// `clean_env` skips them. Already-started processes keep their environment.
    /// Returns a refused `Unavailable` status when operation admission is closed.
    /// Uses `Status` deliberately to share the generated operation methods' error
    /// codes and `is_refused_before_start` classification, including in the daemon
    /// adapter; callers do not need a second shutdown-error representation.
    pub async fn configure_process_env(&self, env: EnvMap) -> Result<(), Status> {
        self.require_running()?;
        *self.inner.process_env.write().await = env;
        Ok(())
    }

    /// Owned, nameable filesystem handle for both native and generated-trait calls.
    pub fn filesystem_service(&self) -> crate::FilesystemHandle {
        self.inner.filesystem.clone()
    }

    /// Upload a file using the same messages and execution path as `Filesystem.Write`.
    ///
    /// The stream starts with `Open`, followed by `Data` messages. Payload capacity
    /// is acquired before polling input, each message has the filesystem size limit,
    /// and the relay applies backpressure. EOF completes the write; an input error or
    /// dropped future cancels it with the same partial-write and retry semantics as
    /// gRPC. Admission remains held until owned execution and cleanup settle.
    /// Caller-owned buffers and allocations before yielding input are not bounded
    /// by the service. The stream may borrow from the caller and need not be `Unpin`.
    /// Each wait for input has the configured native upload idle timeout (30
    /// seconds by default), including before `Open`. Timeout cancels the upload
    /// and returns `DeadlineExceeded`; cleanup can retain capacity. Backpressure
    /// from execution is not input idle time. Callers can also bound total upload
    /// duration, for example with `tokio::time::timeout`.
    /// After an upload reserves its idempotency key, a stalled upload also blocks
    /// retries for the same user and key. Each admitted retry holds its own payload
    /// slot while waiting, for up to the retention window.
    pub async fn write_file<S>(
        &self,
        request: Request<S>,
    ) -> Result<Response<WriteResponse>, Status>
    where
        S: futures_util::Stream<Item = Result<WriteRequest, Status>> + Send,
    {
        self.inner.filesystem.write_file(request).await
    }

    pub fn filesystem_grpc_service(
        &self,
    ) -> pplx_space_common_grpc::RequestAdmission<
        pplx_space_common_grpc::PreserveInputErrors<
            pplx_space_spaced_guest_protocol::proto::bridge_guest::filesystem_server::FilesystemServer<
                impl pplx_space_spaced_guest_protocol::proto::bridge_guest::filesystem_server::Filesystem
                + Clone,
            >,
        >,
    >{
        self.inner.filesystem.grpc_service()
    }
}

#[cfg(test)]
mod tests;
