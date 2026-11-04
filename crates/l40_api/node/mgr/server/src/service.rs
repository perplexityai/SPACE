mod filesystem_work;

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::core::host::linux::identities::{gids, group_names, user_names};
use pplx_space_common_svc::{LocalRole, UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal};
use pplx_space_node_runtime_core::runtime_inventory::{RuntimeInventory, RuntimeInventoryError};
use tonic::{Request, Response, Status};

use crate::disk_admission::DiskAdmission;
use crate::launch;
use crate::memory_admission::{
    MemoryAdmissionConfig, MemoryAdmissionError, MemoryAdmissionEvaluationError,
    MemoryAdmissionSources,
};
use crate::net::{self, GUEST_GATEWAY, GUEST_IP, NetId, SlotInfo, TAP_DEVICE};
use crate::proto::node_manager_server::NodeManager;
use crate::proto::{
    AllocateRequest, AllocateResponse, AllowPortRequest, AllowPortResponse,
    BtrfsCreateTemplateSubvolumeRequest, BtrfsCreateTemplateSubvolumeResponse,
    BtrfsDeleteSandboxInstanceRequest, BtrfsDeleteSandboxInstanceResponse,
    BtrfsDeleteTemplateSubvolumeRequest, BtrfsDeleteTemplateSubvolumeResponse,
    BtrfsEncodeRootfsSendRequest, BtrfsEncodeRootfsSendResponse,
    BtrfsEncodeSnapshotRootfsSendRequest, BtrfsEncodeSnapshotRootfsSendResponse,
    BtrfsFinalizeTemplateSubvolumeRequest, BtrfsFinalizeTemplateSubvolumeResponse,
    BtrfsReceiveInstanceRequest, BtrfsReceiveInstanceResponse, CaptureSandboxEndpointProofRequest,
    CaptureVmStateResponse, CleanupSandboxRequest, CleanupSandboxResponse, CloneSandboxRequest,
    CloneSandboxResponse, CompletePreparedRootfsRequest, CompletePreparedRootfsResponse,
    CreateBtrfsSnapshotRequest, CreateBtrfsSnapshotResponse, DeleteBtrfsSnapshotRequest,
    DeleteBtrfsSnapshotResponse, DeleteCacheArtifactRequest, DeleteCacheArtifactResponse,
    DenyPortRequest, DenyPortResponse, ExposePortRequest, ExposePortResponse, GetDiskUsageRequest,
    GetDiskUsageResponse, GetExecutionInventoryRequest, GetVmStateRequest, GetVmStateResponse,
    InspectSandboxRequest, InspectSandboxResponse, LaunchRequest, LaunchResponse,
    LaunchSandboxRequest, LaunchSandboxResponse, ListBtrfsSnapshotsRequest,
    ListBtrfsSnapshotsResponse, ListNetworkLeasesRequest, ListNetworkLeasesResponse,
    ListOrphanCandidatesRequest, ListOrphanCandidatesResponse, MissingDiskUsageId,
    MissingDiskUsageReason, NetworkLease, NetworkPoolConfigInfo, NetworkPoolStatusRequest,
    NetworkPoolStatusResponse, NetworkProtocol, NodeResourceStatusRequest,
    NodeResourceStatusResponse, PauseSandboxRequest, PauseSandboxResponse,
    PrepareEncryptedRootfsRequest, PrepareEncryptedRootfsResponse, PrepareEncryptedVolumeRequest,
    PrepareEncryptedVolumeResponse, PrepareSandboxArtifactDirRequest,
    PrepareSandboxArtifactDirResponse, PrepareSandboxRequest, PrepareSandboxResponse,
    PrepareSnapshotRootfsRequest, PrepareSnapshotRootfsResponse, PublishArtifactRequest,
    PublishArtifactResponse, ReleaseEncryptedVolumeRequest, ReleaseEncryptedVolumeResponse,
    ReleaseRequest, ReleaseResponse, RemoveArtifactRequest, RemoveArtifactResponse,
    RepairNetworkPoolRequest, RepairNetworkPoolResponse, ReservePreparedRootfsRequest,
    ReservePreparedRootfsResponse, RestoreInstanceSnapshotRequest, RestoreInstanceSnapshotResponse,
    RestoreVmStateResponse, ResumeSandboxRequest, ResumeSandboxResponse,
    RollbackBtrfsSnapshotRequest, RollbackBtrfsSnapshotResponse, SandboxEndpointProof,
    SetNetworkPoolConfigRequest, SetNetworkPoolConfigResponse, SetNetworkPoolDrainRequest,
    SetNetworkPoolDrainResponse, StopProcessRequest, StopProcessResponse, SyncGuestRequest,
    SyncGuestResponse, UnexposePortRequest, UnexposePortResponse, UpdateCgroupRequest,
    UpdateCgroupResponse, ValidateSandboxEndpointProofRequest,
    ValidateSandboxEndpointProofResponse,
};
use crate::slot_pool::{
    ActiveLeaseMemoryReservationError, AllocateSlotError, NETWORK_POOL_SCHEMA_VERSION, NetworkPool,
    NetworkPoolConfig, NetworkPoolConfigUpdate, NewLeaseMemoryReservation, PoolCounts, SlotRecord,
};

// Allow Firecracker's cgroup to quiesce after cleanup while bounding the wait
// for a persistently occupied cgroup.
const CLEANUP_QUIESCENCE_TIMEOUT: Duration = Duration::from_secs(3);
const CLEANUP_QUIESCENCE_POLL: Duration = Duration::from_millis(5);
const FIRECRACKER_API_SOCKET_POLL: Duration = Duration::from_millis(5);
const FIRECRACKER_API_SOCKET_TIMEOUT: Duration = Duration::from_secs(5);
const FIRECRACKER_VM_STATE_TIMEOUT: Duration = Duration::from_millis(250);
const FIRECRACKER_VSOCK_POLL: Duration = Duration::from_millis(50);
const FIRECRACKER_VSOCK_TIMEOUT: Duration = Duration::from_secs(1);
const RUN_DIR: &str = pplx_space_common_core::host::linux::paths::SANDBOX_RUN_DIR;
const CACHE_DIR: &str = pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR;
const TEMPLATE_DIR: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_DIR;
const JAIL_DIR: &str = pplx_space_common_core::host::linux::paths::FIRECRACKER_JAIL_DIR;

const ALLOW_NODE_STATUS: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];
const ALLOW_NODE_ADMIN: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
];
const ALLOW_NODE_ADMIN_OR_SBXMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];
const ALLOW_NODE_ADMIN_OR_NODEAPI: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

fn ensure_snapshot_export_permissions(path: &Path) -> Result<(), Status> {
    trusted_paths::export_permissions(path, Some(gids::SPACE_CACHE_RW))
}

#[cfg(test)]
fn set_snapshot_export_modes(path: &Path) -> Result<(), Status> {
    trusted_paths::export_permissions(path, None)
}

fn ensure_sandbox_artifact_dir_permissions(path: impl AsRef<Path>) -> Result<(), Status> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(());
    }

    let directory = trusted_paths::open_directory(path)?;
    trusted_paths::set_permissions(&directory, Some(gids::SPACE_CACHE_RW), 0o2770)
}

fn ensure_sandbox_instance_traversal(path: impl AsRef<Path>) -> Result<(), Status> {
    let path = path.as_ref();
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(Status::internal(format!(
                "inspect retained instance: {error}"
            )));
        }
        Ok(_) => {}
    }
    let directory = trusted_paths::open_directory(path)?;
    trusted_paths::set_permissions(&directory, Some(gids::SPACE_CACHE_RW), 0o2710)
}

fn ensure_sidecar_run_dir_permissions(path: &str) -> Result<(), Status> {
    cleanup_helpers::chgrp_path_blocking(path, gids::SPACE_SIDECARCLIENTS)?;
    cleanup_helpers::chmod_path_blocking(path, 0o2770)
}

fn validate_sandbox_artifact_dir_shape(path: &Path) -> Result<(), Status> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(Status::invalid_argument(
            format!("refusing sandbox artifact symlink {}", path.display()),
        )),
        Ok(metadata) if !metadata.is_dir() => Err(Status::failed_precondition(format!(
            "sandbox artifact path is not a directory: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Status::internal(format!(
            "stat sandbox artifact path {}: {error}",
            path.display()
        ))),
    }
}

fn ensure_sandbox_artifact_dir(path: impl AsRef<Path>) -> Result<(), Status> {
    let path = path.as_ref();
    let directory = trusted_paths::create_directory(path)?;
    trusted_paths::set_permissions(&directory, Some(gids::SPACE_CACHE_RW), 0o2770)
}

const RESERVED_EXPOSE_PORTS: &[(u16, &str)] = &[
    (22, "ssh"),
    (2379, "etcd-client"),
    (2380, "etcd-peer"),
    (6443, "kubernetes-api"),
    (8125, "datadog-agent"),
    (9100, "node-exporter"),
    (9800, "space-node-api"),
    (10248, "kubelet-healthz"),
    (10249, "kube-proxy-metrics"),
    (10250, "kubelet-api"),
    (10255, "kubelet-readonly"),
    (10256, "kube-proxy-healthz"),
];

const MAX_BLOCKING_PROBES: usize = 4;
/// Retained-source inspections keep their slot until a stuck filesystem call
/// returns, even after the caller's deadline. A separate pool keeps them from
/// exhausting the diagnostic lane that orphan evidence and size probes use.
const MAX_SEND_SOURCE_INSPECTIONS: usize = 4;
/// Concurrent cache artifact deletions. Deletion runs as blocking filesystem
/// work that outlives a cancelled caller, so the bound belongs here, held by
/// the closure that does the work, not with any one caller's admission.
const MAX_CACHE_ARTIFACT_DELETES: usize = 4;

#[derive(Clone)]
pub struct NodeManagerService {
    guest_networks: std::sync::Arc<guest_network::GuestNetworks>,
    guest_access: std::sync::Arc<guest_control::GuestAccessBudgets>,
    pool: std::sync::Arc<NetworkPool>,
    peer_authorizer: UdsPeerAuthorizer,
    sandbox_cgroup_config: SandboxCgroupConfig,
    memory_admission_config: MemoryAdmissionConfig,
    memory_admission_sources: MemoryAdmissionSources,
    memory_admission_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    sandbox_cgroup_gate_locks: SandboxCgroupGateLocks,
    sandbox_vm_cgroup_locks: SandboxCgroupFamilyLocks,
    sandbox_sidecar_cgroup_locks: SandboxCgroupFamilyLocks,
    sandbox_device_stack_locks: SandboxCgroupFamilyLocks,
    sandbox_artifact_locks: SandboxCgroupFamilyLocks,
    send_source_locks: SandboxCgroupFamilyLocks,
    send_source_slots: std::sync::Arc<tokio::sync::Semaphore>,
    artifact_work: Option<std::sync::Arc<filesystem_work::ArtifactWork>>,
    /// Shared drain ownership for detached mutations and lifecycle permits.
    device_op_tasks: tokio_util::task::TaskTracker,
    blocking_probe_slots: std::sync::Arc<tokio::sync::Semaphore>,
    send_source_inspection_slots: std::sync::Arc<tokio::sync::Semaphore>,
    cache_artifact_delete_slots: std::sync::Arc<tokio::sync::Semaphore>,
    /// One artifact-store change at a time takes a blocking thread; the
    /// store's own lock would otherwise leave queued calls holding threads
    /// the rest of node-manager needs.
    artifact_slots: std::sync::Arc<tokio::sync::Semaphore>,
    runtime_supervisor: crate::runtime_supervisor_client::RuntimeSupervisorLazyClient,
    runtime_inventory: std::sync::Arc<ArcSwapOption<RuntimeInventory>>,
    runtime_inventory_refresh_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    runtime_paths: crate::config::RuntimePaths,
    execution_policy: std::sync::Arc<execution_policy::ExecutionPolicyState>,
    guest_console_logs: bool,
    lifecycle_limiter: std::sync::Arc<crate::lifecycle_limiter::LifecycleLimiter>,
    disk_admission: std::sync::Arc<DiskAdmission>,
    prepared_rootfs_reservations: prepared_rootfs::PreparedRootfsReservations,
    disk_usage_cache: node_disk_metrics::SandboxDiskUsageCache,
    disk_io_ceiling: disk_io_ceiling::DiskIoCeiling,
    cleanup_operations: cleanup_inspect::CleanupOperationRegistry,
    orphan_inventory: orphan_inventory::OrphanInventory,
    snapshot_inventory: snapshot_inventory::SnapshotInventory,
}

type SandboxCgroupGateLock = std::sync::Arc<tokio::sync::RwLock<()>>;
type SandboxCgroupGateLocks = std::sync::Arc<
    tokio::sync::Mutex<std::collections::HashMap<String, std::sync::Weak<tokio::sync::RwLock<()>>>>,
>;
type SandboxCgroupFamilyLock = std::sync::Arc<tokio::sync::Mutex<()>>;
type SandboxCgroupFamilyLocks = std::sync::Arc<
    tokio::sync::Mutex<std::collections::HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
>;
type SandboxCgroupGuard = tokio::sync::OwnedRwLockWriteGuard<()>;

// Launches hold a shared operation gate plus one family lock so Firecracker
// and sidecars can start together, while teardown retains exclusive ordering.
struct SandboxCgroupLaunchGuards {
    _gate: tokio::sync::OwnedRwLockReadGuard<()>,
    _family: tokio::sync::OwnedMutexGuard<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxCgroupConfig {
    pub vcpu_count: Option<u32>,
    pub memory_mib: Option<u64>,
}

impl Default for SandboxCgroupConfig {
    fn default() -> Self {
        Self {
            vcpu_count: Some(crate::cgroup::DEFAULT_VCPU_COUNT),
            memory_mib: Some(crate::cgroup::DEFAULT_MEMORY_MAX_MIB),
        }
    }
}

impl SandboxCgroupConfig {
    pub fn memory_mib_or_default(self) -> u64 {
        self.memory_mib
            .unwrap_or(crate::cgroup::DEFAULT_MEMORY_MAX_MIB)
    }

    pub fn guest_memory_max_mib(self) -> u64 {
        crate::cgroup::sandbox_guest_memory_max_mib(self.memory_mib_or_default()).unwrap_or(0)
    }

    pub fn total_memory_mib(self, max_slots: u32) -> u64 {
        crate::cgroup::sandbox_cgroup_total_memory_mib(self.memory_mib_or_default(), max_slots)
            .unwrap_or(0)
    }

    fn clamp_requested_memory_mib(self, requested: u64) -> u64 {
        match self.memory_mib {
            Some(configured) => requested.min(configured),
            None => requested,
        }
    }

    fn launch_memory_mib(self, requested: Option<u64>) -> Option<u64> {
        requested
            .map(|requested| self.clamp_requested_memory_mib(requested))
            .or(self.memory_mib)
    }

    fn update_memory_mib(self, requested: Option<u64>) -> Option<u64> {
        requested.map(|requested| self.clamp_requested_memory_mib(requested))
    }

    fn allocation_memory_mib(self, requested: u64) -> Result<u64, Status> {
        match requested {
            0 => Err(Status::invalid_argument(
                "sandbox_cgroup_memory_mib is required for a new allocation",
            )),
            requested => Ok(self.clamp_requested_memory_mib(requested)),
        }
    }
}

fn slot_store_status(error: crate::slot_pool::SlotStoreError) -> Status {
    Status::unavailable(format!("network slot state unavailable: {error}"))
}

impl NodeManagerService {
    pub async fn get_execution_inventory(
        &self,
        request: Request<GetExecutionInventoryRequest>,
    ) -> Result<Response<pplx_space_execution_common::proto::NodeExecutionInventory>, Status> {
        self.authorize_peer(&request, "GetExecutionInventory", ALLOW_NODE_STATUS)?;
        // Before discovery, so a node that can't report inventory still reports this.
        pplx_space_metrics::node_manager::execution_policy_record_unreadable(
            self.execution_policy().cluster_policy_unreadable(),
        );
        let inventory = self.runtime_inventory().await?;
        let mut report = inventory.to_proto();
        // After discovery, so an application that landed meanwhile is the one published.
        let policy = execution_policy::observation(&self.execution_policy())
            .map_err(|error| Status::internal(error.to_string()))?;
        report.policy = Some((&policy).into());
        // Known is the whole compiled catalog, never the policy's E0-only
        // fallback; usable is judged against the startup ceiling discovery
        // used. When the catalog can't load, or the observation can't be
        // built from it, omit the observation (unknown) rather than the
        // inventory.
        report.environments = match node_artifacts::compiled_environments() {
            Ok(catalog) => {
                match inventory.environment_observation(catalog, self.execution_policy.startup()) {
                    Ok(observation) => Some((&observation).into()),
                    Err(error) => {
                        tracing::error!(%error, "compiled environment observation is invalid; omitting it");
                        None
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "compiled catalog unavailable; omitting environment observation");
                None
            }
        };
        Ok(Response::new(report))
    }

    pub(crate) async fn with_memory_admission_config_and_task_tracker(
        config: NetworkPoolConfig,
        sandbox_cgroup_config: SandboxCgroupConfig,
        memory_admission_config: MemoryAdmissionConfig,
        settings: crate::config::ServiceSettings,
        task_tracker: tokio_util::task::TaskTracker,
        shutdown_token: tokio_util::sync::CancellationToken,
        startup_ownership: std::sync::Arc<pplx_space_common_svc::service_mutex::ServiceMutex>,
    ) -> Result<Self, crate::slot_pool::PoolOpenError> {
        // Compiled pins are immutable for the lifetime of this process. Retain
        // unsupported-host errors for execution and mutation admission as well.
        if let Err(error) = node_artifacts::initial_environment() {
            tracing::warn!(%error, "initial execution environment unavailable");
        }
        let pool_tracker = task_tracker.clone();
        let memory_mib = sandbox_cgroup_config.memory_mib_or_default();
        let span = tracing::Span::current();
        let node_status_dirty = settings.node_status_dirty;
        // Startup host commands must not stall liveness. Track and await the
        // whole pass; shutdown cannot abandon namespace mutations under ownership.
        let pool = task_tracker
            .spawn_blocking(move || {
                let _ownership = startup_ownership;
                let _entered = span.enter();
                NetworkPool::open(
                    config,
                    memory_mib,
                    pool_tracker,
                    shutdown_token,
                    node_status_dirty,
                )
            })
            .await
            .map_err(crate::slot_pool::PoolOpenError::StartupTask)??;
        let disk_admission = std::sync::Arc::new(DiskAdmission::new(settings.disk_admission).await);
        let startup_policy = settings.execution_policy;
        let record_path = settings.execution_policy_record;
        let execution_policy = task_tracker
            .spawn_blocking(move || {
                execution_policy::ExecutionPolicyState::load(startup_policy, record_path)
            })
            .await
            .map_err(crate::slot_pool::PoolOpenError::StartupTask)?;
        let prepared_rootfs_reservations = prepared_rootfs::PreparedRootfsReservations::recover(
            &disk_admission,
            std::path::Path::new(CACHE_DIR),
        )
        .with_task_tracker(task_tracker.clone());
        Ok(Self {
            guest_networks: Default::default(),
            guest_access: Default::default(),
            pool,
            peer_authorizer: node_peer_authorizer(),
            sandbox_cgroup_config,
            memory_admission_config,
            memory_admission_sources: settings.memory_admission_sources,
            memory_admission_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            sandbox_cgroup_gate_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            sandbox_vm_cgroup_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            sandbox_sidecar_cgroup_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            sandbox_device_stack_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            device_op_tasks: task_tracker.clone(),
            sandbox_artifact_locks: Default::default(),
            send_source_locks: Default::default(),
            send_source_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
            artifact_work: None,
            blocking_probe_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_BLOCKING_PROBES,
            )),
            send_source_inspection_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_SEND_SOURCE_INSPECTIONS,
            )),
            cache_artifact_delete_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_CACHE_ARTIFACT_DELETES,
            )),
            artifact_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            runtime_supervisor: settings.runtime_supervisor,
            runtime_inventory: std::sync::Arc::default(),
            runtime_inventory_refresh_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            runtime_paths: settings.runtime_paths,
            execution_policy: std::sync::Arc::new(execution_policy),
            guest_console_logs: settings.guest_console_logs,
            lifecycle_limiter: std::sync::Arc::new(
                settings
                    .lifecycle_limiter
                    .with_task_tracker(task_tracker.clone()),
            ),
            disk_admission,
            prepared_rootfs_reservations,
            disk_usage_cache: node_disk_metrics::SandboxDiskUsageCache::default(),
            disk_io_ceiling: disk_io_ceiling::DiskIoCeiling::default(),
            cleanup_operations: cleanup_inspect::CleanupOperationRegistry::new(task_tracker),
            orphan_inventory: orphan_inventory::OrphanInventory::default(),
            snapshot_inventory: snapshot_inventory::SnapshotInventory::default(),
        })
    }

    /// Shared handle to the network slot pool, for background loops that need
    /// the set of currently-tracked sandbox ids (e.g. orphan cgroup reclaim).
    pub fn pool(&self) -> std::sync::Arc<NetworkPool> {
        self.pool.clone()
    }

    pub fn disk_admission(&self) -> std::sync::Arc<DiskAdmission> {
        self.disk_admission.clone()
    }

    pub fn disk_usage_cache(&self) -> node_disk_metrics::SandboxDiskUsageCache {
        self.disk_usage_cache.clone()
    }

    async fn runtime_inventory(&self) -> Result<std::sync::Arc<RuntimeInventory>, Status> {
        if let Some(inventory) = self.runtime_inventory.load_full() {
            let candidate = inventory.clone();
            // Read-only validation needs no sandbox mutation ownership. Keep
            // it off Tokio workers and tracked through waiter cancellation.
            let valid = self
                .device_op_tasks
                .spawn_blocking(move || candidate.check_files_unchanged())
                .await
                .map_err(|error| {
                    Status::internal(format!("validate execution inventory: {error}"))
                })?;
            if valid {
                return Ok(inventory);
            }
        }
        // Metadata checks and discovery are blocking. The owned lock stays in
        // the tracked closure even if the RPC waiter is cancelled.
        let guard = self
            .runtime_inventory_refresh_lock
            .clone()
            .lock_owned()
            .await;
        let snapshot = self.runtime_inventory.clone();
        let paths = self.runtime_paths.clone();
        // Discover against the startup ceiling: narrowing never needs a rescan.
        let policy = self.execution_policy.startup().clone();
        self.off_runtime("discover runtime inventory", move || {
            let _guard = guard;
            if let Some(inventory) = snapshot.load_full()
                && inventory.check_files_unchanged()
            {
                return Ok(inventory);
            }
            snapshot.store(None);
            let inventory =
                std::sync::Arc::new(node_artifacts::execution_inventory(&paths, &policy)?);
            snapshot.store(Some(inventory.clone()));
            Ok(inventory)
        })
        .await
    }

    fn launch_cgroup_limits(
        &self,
        req: &LaunchSandboxRequest,
    ) -> pplx_space_node_runtime_core::proto::CgroupLimits {
        pplx_space_node_runtime_core::proto::CgroupLimits {
            vcpu_count: req.vcpu_count.or(self.sandbox_cgroup_config.vcpu_count),
            memory_mib: self.sandbox_cgroup_config.launch_memory_mib(req.memory_mib),
            pids_max: req.pids_max,
        }
    }

    async fn sandbox_cgroup_gate_lock(&self, sandbox_id: &str) -> SandboxCgroupGateLock {
        let mut locks = self.sandbox_cgroup_gate_locks.lock().await;
        if let Some(lock) = locks.get(sandbox_id).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = std::sync::Arc::new(tokio::sync::RwLock::new(()));
        locks.insert(sandbox_id.to_string(), std::sync::Arc::downgrade(&lock));
        lock
    }

    async fn sandbox_cgroup_family_lock(
        locks: &SandboxCgroupFamilyLocks,
        sandbox_id: &str,
    ) -> SandboxCgroupFamilyLock {
        let mut locks = locks.lock().await;
        if let Some(lock) = locks.get(sandbox_id).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(sandbox_id.to_string(), std::sync::Arc::downgrade(&lock));
        lock
    }

    /// Per-sandbox serialization of rootfs and snapshot-volume device-stack
    /// mutations. Taking a typed ID makes alternate accepted UUID spellings
    /// converge on one lock key. Keyed and GC'd like the cgroup family locks.
    pub(crate) async fn sandbox_device_stack_lock(
        &self,
        sandbox_id: &SandboxId,
    ) -> SandboxCgroupFamilyLock {
        Self::sandbox_cgroup_family_lock(&self.sandbox_device_stack_locks, &sandbox_id.to_string())
            .await
    }

    async fn lock_sandbox_cgroups(&self, sandbox_id: &str) -> SandboxCgroupGuard {
        self.sandbox_cgroup_gate_lock(sandbox_id)
            .await
            .write_owned()
            .await
    }

    async fn lock_sandbox_vm_cgroup_launch(&self, sandbox_id: &str) -> SandboxCgroupLaunchGuards {
        let gate = self
            .sandbox_cgroup_gate_lock(sandbox_id)
            .await
            .read_owned()
            .await;
        let family = Self::sandbox_cgroup_family_lock(&self.sandbox_vm_cgroup_locks, sandbox_id)
            .await
            .lock_owned()
            .await;
        SandboxCgroupLaunchGuards {
            _gate: gate,
            _family: family,
        }
    }

    async fn lock_sandbox_sidecar_cgroup_launch(
        &self,
        sandbox_id: &str,
    ) -> SandboxCgroupLaunchGuards {
        let gate = self
            .sandbox_cgroup_gate_lock(sandbox_id)
            .await
            .read_owned()
            .await;
        let family =
            Self::sandbox_cgroup_family_lock(&self.sandbox_sidecar_cgroup_locks, sandbox_id)
                .await
                .lock_owned()
                .await;
        SandboxCgroupLaunchGuards {
            _gate: gate,
            _family: family,
        }
    }

    fn adopt_validated_lease(&self, sandbox_id: &str, expected: &SlotInfo) -> Result<(), Status> {
        if self.pool.shutdown_token.is_cancelled() {
            return Err(Status::unavailable("node-manager is shutting down"));
        }
        self.pool
            .adopt_lease(sandbox_id, expected)
            .map_err(slot_store_status)?
            .ok_or_else(|| {
                Status::unavailable("sandbox lease changed during validation; retry allocation")
            })?;
        Ok(())
    }

    fn claim_sandbox_mutation(
        &self,
        sandbox_id: &str,
    ) -> Result<std::sync::Arc<crate::slot_pool::SandboxMutationClaim>, Status> {
        self.pool
            .try_claim_sandbox_mutation(sandbox_id)
            .ok_or_else(|| {
                Status::unavailable("sandbox host mutation or reset is in progress; retry")
            })
    }

    /// Retain exclusion even when the RPC waiter disappears during host work.
    async fn run_owned_sandbox_mutation<T: Send + 'static>(
        &self,
        sandbox_id: &str,
        work: impl std::future::Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        let claim = self.claim_sandbox_mutation(sandbox_id)?;
        self.run_claimed_sandbox_work(claim, work).await
    }

    async fn run_owned_sandbox_work<T: Send + 'static>(
        &self,
        sandbox_id: &str,
        work: impl std::future::Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        let claim = self.wait_for_sandbox_work(sandbox_id).await?;
        self.run_claimed_sandbox_work(claim, work).await
    }

    fn claim_sandbox_work(
        &self,
        sandbox_id: &str,
    ) -> Result<std::sync::Arc<crate::slot_pool::SandboxMutationClaim>, Status> {
        self.pool
            .try_claim_sandbox_work(sandbox_id)
            .ok_or_else(|| Status::unavailable("sandbox cleanup or reset is in progress; retry"))
    }

    async fn wait_for_sandbox_work(
        &self,
        sandbox_id: &str,
    ) -> Result<std::sync::Arc<crate::slot_pool::SandboxMutationClaim>, Status> {
        // Allocate may grant a new slot before the previous slot finishes its
        // reset. Wait for exclusion to end before entering activation; the
        // inner operation still validates the current lease under its gate.
        cleanup_inspect::wait_for_cleanup_phase(
            &self.pool.shutdown_token,
            self.pool.claim_sandbox_work(sandbox_id),
            "timed out waiting for sandbox cleanup or reset; retry",
        )
        .await
    }

    async fn run_claimed_sandbox_work<T: Send + 'static>(
        &self,
        claim: std::sync::Arc<crate::slot_pool::SandboxMutationClaim>,
        work: impl std::future::Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        self.run_owned_operation("sandbox mutation", async move {
            let _claim = claim;
            work.await
        })
        .await
    }

    /// Detach the waiter, not ownership. The future captures each operation's
    /// concrete guards; cleanup's retry-coalescing registry remains its owner.
    async fn run_owned_operation<T: Send + 'static>(
        &self,
        operation: &'static str,
        work: impl std::future::Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        use tracing::Instrument as _;
        self.device_op_tasks
            .spawn(
                async move {
                    let result = work.await;
                    tracing::info!(
                        operation,
                        succeeded = result.is_ok(),
                        "owned node-manager operation completed"
                    );
                    result
                }
                .in_current_span(),
            )
            .await
            .map_err(|error| Status::internal(format!("{operation} task failed: {error}")))?
    }

    fn adopt_stopping_lease(&self, sandbox_id: &str, expected: &SlotInfo) -> Result<(), Status> {
        self.pool
            .adopt_stopping_lease(sandbox_id, expected)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::unavailable("sandbox lease changed before stop; retry"))?;
        Ok(())
    }

    fn authorize_peer<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
        allowed: &[UdsPeerPrincipal],
    ) -> Result<(), Status> {
        self.peer_authorizer
            .authorize_request(request, operation, allowed)?;
        if self.pool.shutdown_token.is_cancelled() {
            return Err(Status::unavailable("node-manager is shutting down"));
        }
        Ok(())
    }
}

/// Validate sandbox_id is a safe UUID-like string (prevents path traversal).
mod adapter;
mod artifact_fs;
pub mod btrfs_balance;
mod btrfs_delegation;
mod btrfs_send_source;
mod btrfs_send_stream;
mod btrfs_send_transport;
pub mod cgroup_reclaim;
#[cfg(test)]
mod claim_contention_tests;
mod cleanup_helpers;
mod cleanup_inspect;
mod control;
mod disk;
pub(crate) mod disk_io_ceiling;
mod encrypted_volumes;
mod endpoint_attestation;
mod execution_fence;
mod execution_policy;
mod gateway_isolation;
mod launch_prepare;
mod network_helpers;
pub mod network_pool_metrics;
mod node_artifacts;
pub mod node_disk_metrics;
mod orphan_inventory;
mod permission_tree;
mod prepared_rootfs;
mod runtime;
mod snapshot_inventory;
mod snapshots;
#[cfg(test)]
mod tests;
pub(crate) mod trusted_paths;
pub(crate) mod validation;

pub(crate) use cleanup_helpers::RetainedCacheCleanupError;
pub(crate) use cleanup_helpers::neutralize_retained_cache_owned_by_uid;
use network_helpers::*;

use crate::snapshot_request::{StateCapture, StateRestore};

mod runtime_control;

mod guest_control;

mod guest_network;
