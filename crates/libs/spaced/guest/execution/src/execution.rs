//! Process and filesystem state shared by the guest daemon adapters.
use super::*;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{WriteRequest, WriteResponse};

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
    pub cancellation_grace: Duration,
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
    pub(super) filesystem: filesystem::FilesystemRouter,
    pub(super) guest_cgroups: Option<Arc<ExecutionCgroups>>,
}

impl ExecutionService {
    /// Construct a service with the caller's filesystem execution configuration.
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
    /// # Errors
    ///
    /// Returns the same configuration errors as [`Self::with_config`].
    pub fn with_cgroups(
        shutdown: CancellationToken,
        config: ExecutionServiceConfig,
        guest_cgroups: Option<Arc<ExecutionCgroups>>,
    ) -> Result<Self, CreateExecutionServiceError> {
        let filesystem = filesystem::FilesystemRouter::new(
            config.filesystem_execution,
            config.worker_executable,
            config.identity_policy,
            config.default_operation_user.clone(),
            shutdown.clone(),
            config.cancellation_grace,
            guest_cgroups.clone(),
        )?;
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
        if self.inner.shutdown.is_cancelled() {
            return Err(crate::refused(Status::unavailable("guest shutting down")));
        }
        Ok(())
    }

    pub(super) fn close_process_admission(&self) {
        let _start_gate = self
            .inner
            .start_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.process_tasks.close();
    }

    /// Drain guest-owned work during the daemon's cancellation and listener
    /// shutdown. The daemon owns the overall shutdown deadline.
    pub async fn drain(&self) {
        self.close_process_admission();
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
    pub async fn configure_process_env(&self, env: EnvMap) {
        *self.inner.process_env.write().await = env;
    }

    /// Native calls use the same filesystem operations and payload budgets as gRPC.
    /// Use [`Self::write_file`] for uploads from a native stream.
    pub fn filesystem_service(
        &self,
    ) -> impl pplx_space_spaced_guest_protocol::proto::bridge_guest::filesystem_server::Filesystem
    + Clone
    + use<> {
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
    /// There is no idle timeout before `Open` or between `Data` messages. A stalled
    /// stream retains payload capacity and, after dispatch, execution capacity.
    /// Callers should bound the upload, for example with `tokio::time::timeout`;
    /// dropping its future cancels the upload but cleanup can retain capacity.
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
        self.inner.filesystem.write_from_stream(request).await
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
