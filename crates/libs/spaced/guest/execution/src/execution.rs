//! Process and filesystem state shared by the guest daemon adapters.
use super::*;

#[derive(Clone, Debug)]
pub struct ExecutionServiceConfig {
    pub default_operation_user: GuestUserName,
    pub filesystem_execution: FilesystemExecutionMode,
    pub cancellation_grace: Duration,
    /// Guest memory used to size retained process output; None uses the fallback budget.
    pub guest_memory_bytes: Option<u64>,
}

impl Default for ExecutionServiceConfig {
    fn default() -> Self {
        Self {
            default_operation_user: GuestUserName::parse(DEFAULT_GUEST_USER)
                .expect("default guest user constant is valid"),
            filesystem_execution: if cfg!(target_os = "linux") {
                FilesystemExecutionMode::IdentityWorkers
            } else {
                FilesystemExecutionMode::Direct
            },
            cancellation_grace: filesystem::router::DEFAULT_CANCELLATION_GRACE,
            guest_memory_bytes: None,
        }
    }
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
    pub(super) process_env: RwLock<EnvMap>,
    pub(super) processes: ProcessRegistry,
    pub(super) idempotency: IdempotencyStore,
    pub(super) filesystem: filesystem::FilesystemRouter,
    pub(super) guest_cgroups: Option<Arc<ExecutionCgroups>>,
}

impl ExecutionService {
    pub fn with_config(shutdown: CancellationToken, config: ExecutionServiceConfig) -> Self {
        Self::with_cgroups(shutdown, config, None)
    }

    pub fn with_cgroups(
        shutdown: CancellationToken,
        config: ExecutionServiceConfig,
        guest_cgroups: Option<Arc<ExecutionCgroups>>,
    ) -> Self {
        let filesystem = filesystem::FilesystemRouter::new(
            config.filesystem_execution,
            config.default_operation_user.clone(),
            shutdown.clone(),
            config.cancellation_grace,
            guest_cgroups.clone(),
        );
        Self {
            inner: Arc::new(ExecutionServiceInner {
                shutdown,
                process_tasks: tokio_util::task::TaskTracker::new(),
                start_gate: std::sync::Mutex::new(()),
                process_payload_slots: Arc::new(Semaphore::new(
                    pplx_space_spaced_guest_protocol::process_limits::PAYLOAD_SLOTS,
                )),
                default_operation_user: config.default_operation_user,
                process_env: RwLock::new(EnvMap::empty()),
                processes: ProcessRegistry::new(process::ProcessOutputBudget::for_guest_memory(
                    config.guest_memory_bytes,
                )),
                idempotency: IdempotencyStore::new("process"),
                filesystem,
                guest_cgroups,
            }),
        }
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

    pub async fn configure_process_env(&self, env: EnvMap) {
        *self.inner.process_env.write().await = env;
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
