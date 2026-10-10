use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use pplx_space_common_core::types::{SandboxId, SandboxIdParseError};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::memory_admission::ActiveLeaseMemoryReservation;
use crate::net::{self, NetId, SlotInfo};

pub const NETWORK_POOL_SCHEMA_VERSION: u32 = 4;
pub const DEFAULT_NETWORK_POOL_TARGET: u32 = 100;
const RESET_CONCURRENCY: usize = 10;
const REPAIR_CONCURRENCY: usize = 10;
const CREATE_CONCURRENCY: usize = 2;
const VALIDATION_CONCURRENCY: usize = 16;
const POOL_FILL_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_SLOT_CLEANUP_RESET_RETRIES: u32 = 5;
const MAX_SLOT_TRANSIENT_VALIDATION_RETRIES: u32 = 5;
const MAX_SLOT_TRANSIENT_SETUP_RETRIES: u32 = 5;
const MAX_SLOT_TRANSIENT_RESET_RETRIES: u32 = 5;

macro_rules! clear_cleanup_reset_retry_sql {
    () => {
        "cleanup_reset_attempts = 0, last_reset_retry_at = NULL"
    };
}

#[derive(Clone, Debug)]
pub struct NetworkPoolConfig {
    pub max_slots: u32,
    pub target_ready_slots: u32,
    pub retained_ready_slots: u32,
    pub min_ready_slots: u32,
    pub pod_netns_path: Option<String>,
    pub service_port_ingress_isolation: ServicePortIngressIsolation,
    pub sandbox_cache_dir: PathBuf,
}

impl NetworkPoolConfig {
    pub fn new(max_slots: u32) -> Self {
        Self {
            max_slots,
            target_ready_slots: max_slots.min(DEFAULT_NETWORK_POOL_TARGET),
            retained_ready_slots: max_slots,
            min_ready_slots: max_slots.min(32),
            pod_netns_path: None,
            service_port_ingress_isolation: ServicePortIngressIsolation::ExclusiveHost,
            sandbox_cache_dir: PathBuf::from(
                pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR,
            ),
        }
    }

    fn warm_pool_enabled(&self) -> bool {
        self.target_ready_slots > 0 || self.retained_ready_slots > 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePortIngressIsolation {
    SharedHost,
    ExclusiveHost,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported service-port ingress isolation '{0}'")]
pub struct ServicePortIngressIsolationParseError(String);

impl ServicePortIngressIsolation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SharedHost => "shared_host",
            Self::ExclusiveHost => "exclusive_host",
        }
    }

    pub fn permits_root_ingress(self) -> bool {
        self == Self::ExclusiveHost
    }
}

impl fmt::Display for ServicePortIngressIsolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ServicePortIngressIsolation {
    type Err = ServicePortIngressIsolationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "shared_host" | "shared-host" => Ok(Self::SharedHost),
            "exclusive_host" | "exclusive-host" => Ok(Self::ExclusiveHost),
            other => Err(ServicePortIngressIsolationParseError(other.to_string())),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct NetworkPoolConfigUpdate {
    pub max_slots: Option<u32>,
    pub target_ready_slots: Option<u32>,
    pub retained_ready_slots: Option<u32>,
    pub min_ready_slots: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotState {
    Missing,
    Creating,
    Ready,
    Leasing,
    Leased,
    Dirty,
    Resetting,
    Quarantined,
    Destroying,
}

impl SlotState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Leasing => "leasing",
            Self::Leased => "leased",
            Self::Dirty => "dirty",
            Self::Resetting => "resetting",
            Self::Quarantined => "quarantined",
            Self::Destroying => "destroying",
        }
    }

    pub fn proto(self) -> i32 {
        use crate::proto::NetworkSlotState;
        (match self {
            Self::Missing => NetworkSlotState::Missing,
            Self::Creating => NetworkSlotState::Creating,
            Self::Ready => NetworkSlotState::Ready,
            Self::Leasing => NetworkSlotState::Leasing,
            Self::Leased => NetworkSlotState::Leased,
            Self::Dirty => NetworkSlotState::Dirty,
            Self::Resetting => NetworkSlotState::Resetting,
            Self::Quarantined => NetworkSlotState::Quarantined,
            Self::Destroying => NetworkSlotState::Destroying,
        }) as i32
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotHealth {
    Unknown,
    Healthy,
    Degraded,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseKind {
    Pooled,
    Ephemeral,
}

impl LeaseKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pooled => "pooled",
            Self::Ephemeral => "ephemeral",
        }
    }
}

impl SlotHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Invalid => "invalid",
        }
    }

    pub fn proto(self) -> i32 {
        use crate::proto::NetworkSlotHealth;
        (match self {
            Self::Unknown => NetworkSlotHealth::Unknown,
            Self::Healthy => NetworkSlotHealth::Healthy,
            Self::Degraded => NetworkSlotHealth::Degraded,
            Self::Invalid => NetworkSlotHealth::Invalid,
        }) as i32
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid persisted {kind}: {value}")]
pub struct PoolValueParseError {
    kind: &'static str,
    value: String,
}

impl FromStr for SlotState {
    type Err = PoolValueParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "missing" => Ok(Self::Missing),
            "creating" => Ok(Self::Creating),
            "ready" => Ok(Self::Ready),
            "leasing" => Ok(Self::Leasing),
            "leased" => Ok(Self::Leased),
            "dirty" => Ok(Self::Dirty),
            "resetting" => Ok(Self::Resetting),
            "quarantined" => Ok(Self::Quarantined),
            "destroying" => Ok(Self::Destroying),
            _ => Err(PoolValueParseError {
                kind: "slot state",
                value: value.into(),
            }),
        }
    }
}

impl FromStr for SlotHealth {
    type Err = PoolValueParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unknown" => Ok(Self::Unknown),
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "invalid" => Ok(Self::Invalid),
            _ => Err(PoolValueParseError {
                kind: "slot health",
                value: value.into(),
            }),
        }
    }
}

impl FromStr for LeaseKind {
    type Err = PoolValueParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pooled" => Ok(Self::Pooled),
            "ephemeral" => Ok(Self::Ephemeral),
            _ => Err(PoolValueParseError {
                kind: "lease kind",
                value: value.into(),
            }),
        }
    }
}

impl fmt::Display for SlotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SlotHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for LeaseKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SlotStoreError {
    #[error("network slot database lock poisoned")]
    LockPoisoned,
    #[error("network lease setup or recovery for {0} is incomplete; retry allocation")]
    LeaseNotReady(String),
    #[error("network slot database: {0}")]
    Database(#[from] rusqlite::Error),
    #[error(transparent)]
    InvalidValue(#[from] PoolValueParseError),
    #[error(
        "cannot shrink max_slots to {capacity}: slot {slot_id} is {state}/{health} or retains ownership; restore the prior capacity, recover degraded slots, release affected leases and retire idle slots with min/target/retained ready counts set to zero before restarting"
    )]
    UnsafeShrink {
        slot_id: u32,
        capacity: u32,
        state: SlotState,
        health: SlotHealth,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum PoolOpenError {
    #[error("network pool startup was cancelled")]
    StartupCancelled,
    #[error("network pool startup task failed: {0}")]
    StartupTask(#[source] tokio::task::JoinError),
    #[error("invalid network pool configuration: {0}")]
    Configuration(String),
    #[error("cannot create network pool database directory {}: {source}", path.display())]
    Directory {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("cannot open network pool database: {0}")]
    Open(#[from] pplx_space_common_svc::sql::sqlite::SqliteError),
    #[error(transparent)]
    Store(#[from] SlotStoreError),
    #[error("cannot install guest L2 ingress policy in namespace {namespace}: {source}")]
    GuestL2Ingress {
        namespace: String,
        #[source]
        source: crate::firewall::FirewallValidationError,
    },
    #[error("cannot constrain direct inbound reply path in namespace {namespace}: {source}")]
    DirectInboundReplyPath {
        namespace: String,
        #[source]
        source: crate::firewall::FirewallError,
    },
    #[error("cannot inspect existing TAP containment boundary in namespace {namespace}: {source}")]
    ExistingTapProbe {
        namespace: String,
        #[source]
        source: crate::net::NetworkProbeError,
    },
}

#[derive(Clone, Debug)]
pub struct SlotRecord {
    pub info: SlotInfo,
    pub state: SlotState,
    pub health: SlotHealth,
    pub lease_kind: LeaseKind,
    pub last_error: Option<String>,
    pub memory_reservation_mib: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LeaseKey {
    slot_id: u32,
    generation: u64,
    sandbox_id: String,
}

impl LeaseKey {
    fn from_record(record: &SlotRecord) -> Option<Self> {
        (!record.info.sandbox_id.is_empty()).then(|| Self {
            slot_id: record.info.slot,
            generation: record.info.generation,
            sandbox_id: record.info.sandbox_id.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ResetKey {
    slot_id: u32,
    generation: u64,
    sandbox_id: Option<String>,
}

impl ResetKey {
    fn from_record(record: &SlotRecord) -> Self {
        Self {
            slot_id: record.info.slot,
            generation: record.info.generation,
            sandbox_id: (!record.info.sandbox_id.is_empty())
                .then(|| record.info.sandbox_id.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ValidationRetryDomain {
    StartupIdle,
    ReadyAdoption,
    LiveLease,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ValidationRetryKey {
    domain: ValidationRetryDomain,
    slot_id: u32,
    generation: u64,
    sandbox_id: Option<String>,
}

impl ValidationRetryKey {
    fn from_record(record: &SlotRecord, domain: ValidationRetryDomain) -> Self {
        Self {
            domain,
            slot_id: record.info.slot,
            generation: record.info.generation,
            sandbox_id: (!record.info.sandbox_id.is_empty())
                .then(|| record.info.sandbox_id.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SetupRetryDomain {
    Pooled,
    Ephemeral,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct SetupRetryKey {
    domain: SetupRetryDomain,
    slot_id: u32,
    generation: Option<u64>,
}

impl SetupRetryKey {
    fn pooled(slot_id: u32, generation: u64) -> Self {
        Self {
            domain: SetupRetryDomain::Pooled,
            slot_id,
            generation: Some(generation),
        }
    }

    fn ephemeral(slot_id: u32) -> Self {
        Self {
            domain: SetupRetryDomain::Ephemeral,
            slot_id,
            generation: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct EphemeralSlotClaim {
    info: SlotInfo,
    ownership: std::sync::Arc<ResetClaim>,
}

#[derive(Debug, thiserror::Error)]
enum NetworkHandoffError {
    #[error(transparent)]
    Store(#[from] SlotStoreError),
    #[error(transparent)]
    Teardown(#[from] net::NetworkTeardownError),
    #[error("network work for slot {slot} generation {generation} was superseded")]
    Superseded { slot: u32, generation: u64 },
}

#[derive(Debug)]
enum EphemeralSlotClaimResult {
    Existing(SlotInfo),
    Claimed(EphemeralSlotClaim),
}

#[derive(Debug)]
pub(crate) enum ClaimedSlotAllocation {
    Complete(SlotInfo),
    Ephemeral(EphemeralSlotClaim),
}

#[derive(Clone, Debug, Default)]
pub struct PoolCounts {
    pub total: u32,
    pub ready: u32,
    pub leased: u32,
    pub dirty: u32,
    pub resetting: u32,
    pub creating: u32,
    pub quarantined: u32,
    pub degraded: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PoolRepairResult {
    pub reset_enqueued: u32,
    pub topup_requested: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct NewLeaseMemoryReservation {
    pub memory_reservation_mib: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActiveLeaseMemoryReservationError {
    #[error("active lease memory reservation database lock is poisoned")]
    LockPoisoned,
    #[error("failed to query active lease memory reservations: {0}")]
    Query(#[from] rusqlite::Error),
    #[error("active lease contains invalid sandbox id {sandbox_id}: {source}")]
    InvalidSandboxId {
        sandbox_id: String,
        #[source]
        source: SandboxIdParseError,
    },
    #[error("active lease for sandbox {sandbox_id} has no persisted memory reservation")]
    MissingReservation { sandbox_id: SandboxId },
}

#[derive(Debug, thiserror::Error)]
enum EphemeralSetupError {
    #[error(transparent)]
    Handoff(#[from] NetworkHandoffError),
    #[error(transparent)]
    Setup(#[from] net::NetworkSetupError),
    #[error("dynamic slot setup task failed: {0}")]
    TaskJoin(String),
    #[error(transparent)]
    Validation(#[from] SlotValidationError),
}

impl EphemeralSetupError {
    fn is_transient(&self) -> bool {
        match self {
            Self::Handoff(_) => true,
            Self::Setup(error) => error.is_transient(),
            Self::TaskJoin(_) => false,
            Self::Validation(error) => error.is_transient(),
        }
    }
}

/// Shared by RPC host work and recovery, including cleanup with no active lease.
/// Entries exist only while a claim is alive; blocking work can retain a clone.
pub(crate) struct SandboxMutationClaim {
    active: std::sync::Arc<Mutex<HashMap<String, SandboxMutationOwnership>>>,
    released: std::sync::Arc<tokio::sync::Notify>,
    sandbox_id: String,
}

enum SandboxMutationOwnership {
    Exclusive,
    Work(usize),
}

impl Drop for SandboxMutationClaim {
    fn drop(&mut self) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(SandboxMutationOwnership::Work(count)) = active.get_mut(&self.sandbox_id)
            && *count > 1
        {
            *count -= 1;
        } else {
            active.remove(&self.sandbox_id);
            self.released.notify_waiters();
        }
    }
}

pub struct NetworkPool {
    pub(crate) task_tracker: tokio_util::task::TaskTracker,
    pub(crate) shutdown_token: tokio_util::sync::CancellationToken,
    db: Mutex<Connection>,
    // One coalesced wakeup; durable rows, never channel contents, own work.
    job_tx: mpsc::Sender<()>,
    maintenance_failed: AtomicBool,
    config: Mutex<NetworkPoolConfig>,
    pub(crate) config_update_lock: tokio::sync::Mutex<()>,
    startup_recovery_pending: AtomicBool,
    lease_health_recovery_pending: AtomicBool,
    // Captured before serving requests; never include a current setup owner.
    startup_leasing: Mutex<Vec<LeaseKey>>,
    // Lock after db: adoption and recovery must decide ownership atomically.
    startup_leases: Mutex<HashSet<LeaseKey>>,
    active_resets: Mutex<HashSet<u32>>,
    sandbox_mutations: std::sync::Arc<Mutex<HashMap<String, SandboxMutationOwnership>>>,
    sandbox_mutations_released: std::sync::Arc<tokio::sync::Notify>,
    cleanup_reset_db_failures: DashMap<ResetKey, u32>,
    transient_setup_failures: DashMap<SetupRetryKey, u32>,
    transient_reset_failures: DashMap<ResetKey, u32>,
    validation_transient_failures: DashMap<ValidationRetryKey, u32>,
}

impl NetworkPool {
    pub(crate) fn try_claim_sandbox_mutation(
        &self,
        sandbox_id: &str,
    ) -> Option<std::sync::Arc<SandboxMutationClaim>> {
        let mut active = self
            .sandbox_mutations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active.contains_key(sandbox_id) {
            return None;
        }
        active.insert(sandbox_id.to_string(), SandboxMutationOwnership::Exclusive);
        Some(std::sync::Arc::new(SandboxMutationClaim {
            active: self.sandbox_mutations.clone(),
            released: self.sandbox_mutations_released.clone(),
            sandbox_id: sandbox_id.to_string(),
        }))
    }

    /// Startup authority checks must wait for recovery without holding host locks.
    pub(crate) async fn claim_sandbox_mutation(
        &self,
        sandbox_id: &str,
    ) -> std::sync::Arc<SandboxMutationClaim> {
        loop {
            let released = self.sandbox_mutations_released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if let Some(claim) = self.try_claim_sandbox_mutation(sandbox_id) {
                return claim;
            }
            released.await;
        }
    }

    /// Non-destructive host work shares protection against cleanup and reset.
    /// Resource-family gates separately serialize conflicting VM/device work;
    /// sidecars may start while preparation or snapshot load is still running.
    pub(crate) fn try_claim_sandbox_work(
        &self,
        sandbox_id: &str,
    ) -> Option<std::sync::Arc<SandboxMutationClaim>> {
        let mut active = self
            .sandbox_mutations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match active.entry(sandbox_id.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                SandboxMutationOwnership::Exclusive => return None,
                SandboxMutationOwnership::Work(count) => *count += 1,
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(SandboxMutationOwnership::Work(1));
            }
        }
        Some(std::sync::Arc::new(SandboxMutationClaim {
            active: self.sandbox_mutations.clone(),
            released: self.sandbox_mutations_released.clone(),
            sandbox_id: sandbox_id.to_string(),
        }))
    }

    /// Wait without holding host locks or admission permits. Register before
    /// checking ownership so the final owner's drop cannot lose the wakeup.
    pub(crate) async fn claim_sandbox_work(
        &self,
        sandbox_id: &str,
    ) -> std::sync::Arc<SandboxMutationClaim> {
        loop {
            let released = self.sandbox_mutations_released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if let Some(claim) = self.try_claim_sandbox_work(sandbox_id) {
                return claim;
            }
            released.await;
        }
    }

    /// Check the migrated store before changing host limits. The caller must
    /// hold the service mutex through this check and activation; `open` repeats
    /// the check transactionally before repairing any rows.
    pub fn validate_startup(
        config: &NetworkPoolConfig,
        db_path: &Path,
    ) -> Result<(), PoolOpenError> {
        validate_config(config).map_err(PoolOpenError::Configuration)?;
        let conn = Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(SlotStoreError::from)?;
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(SlotStoreError::from)?;
        validate_startup_ownership(&conn, config)?;
        Ok(())
    }

    pub fn open(
        mut config: NetworkPoolConfig,
        legacy_active_memory_reservation_mib: u64,
        task_tracker: tokio_util::task::TaskTracker,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<std::sync::Arc<Self>, PoolOpenError> {
        config.pod_netns_path = config.pod_netns_path.as_deref().map(normalize_netns_name);
        validate_config(&config).map_err(PoolOpenError::Configuration)?;
        let db_path = pplx_space_common_core::host::linux::paths::NODE_MANAGER_DB;
        if let Some(parent) = std::path::Path::new(db_path).parent() {
            std::fs::create_dir_all(parent).map_err(|source| PoolOpenError::Directory {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let mut conn = pplx_space_common_svc::sql::sqlite::open_rusqlite(
            Path::new(db_path),
            pplx_space_common_svc::sql::sqlite::RusqliteOpenOptions {
                foreign_keys: true,
                ..Default::default()
            },
        )?;
        init_db(&mut conn, &config)?;
        reconcile_runtime_state(&mut conn, &config).map_err(SlotStoreError::from)?;
        // Existing durable TAP namespaces must carry both containments before
        // the node admits work: guest L2 ingress filtering and the constrained
        // direct inbound reply path. Probe each TAP once and apply both.
        let namespaces: Vec<_> = query_records(&conn, None)
            .map_err(SlotStoreError::from)?
            .into_iter()
            .map(|record| record.info.ns_name())
            .collect();
        startup_containment::contain_namespaces(
            &namespaces,
            startup_containment::CONCURRENCY,
            &shutdown_token,
            startup_containment::contain_namespace,
        )?;
        let startup_leasing = query_startup_leasing(&conn)?;
        let startup_leases = query_startup_leases(&conn)?;
        backfill_active_memory_reservations(&conn, legacy_active_memory_reservation_mib)
            .map_err(SlotStoreError::from)?;

        let (job_tx, job_rx) = mpsc::channel(1);
        let pool = std::sync::Arc::new(Self {
            task_tracker,
            shutdown_token,
            db: Mutex::new(conn),
            job_tx,
            maintenance_failed: AtomicBool::new(false),
            config: Mutex::new(config),
            config_update_lock: tokio::sync::Mutex::new(()),
            startup_recovery_pending: AtomicBool::new(true),
            lease_health_recovery_pending: AtomicBool::new(true),
            startup_leasing: Mutex::new(startup_leasing),
            startup_leases: Mutex::new(startup_leases),
            active_resets: Mutex::new(HashSet::new()),
            sandbox_mutations: Default::default(),
            sandbox_mutations_released: Default::default(),
            cleanup_reset_db_failures: DashMap::new(),
            transient_setup_failures: DashMap::new(),
            transient_reset_failures: DashMap::new(),
            validation_transient_failures: DashMap::new(),
        });
        pool.enqueue_pending_resets()?;
        spawn_worker(pool.clone(), job_rx);
        pool.nudge_topup();
        Ok(pool)
    }

    #[cfg(test)]
    pub(crate) fn open_in_memory_for_test(mut config: NetworkPoolConfig) -> std::sync::Arc<Self> {
        config.pod_netns_path = config.pod_netns_path.as_deref().map(normalize_netns_name);
        validate_config(&config).unwrap();
        let mut conn = Connection::open_in_memory().unwrap();
        configure_db(&conn).unwrap();
        init_in_memory_db(&mut conn, &config).unwrap();
        let (job_tx, _job_rx) = mpsc::channel(1);
        std::sync::Arc::new(Self {
            task_tracker: tokio_util::task::TaskTracker::new(),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            db: Mutex::new(conn),
            job_tx,
            maintenance_failed: AtomicBool::new(false),
            config: Mutex::new(config),
            config_update_lock: tokio::sync::Mutex::new(()),
            startup_recovery_pending: AtomicBool::new(false),
            lease_health_recovery_pending: AtomicBool::new(false),
            startup_leasing: Mutex::new(Vec::new()),
            startup_leases: Mutex::new(HashSet::new()),
            active_resets: Mutex::new(HashSet::new()),
            sandbox_mutations: Default::default(),
            sandbox_mutations_released: Default::default(),
            cleanup_reset_db_failures: DashMap::new(),
            transient_setup_failures: DashMap::new(),
            transient_reset_failures: DashMap::new(),
            validation_transient_failures: DashMap::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn set_all_slots_ready_for_test(&self) {
        self.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE network_slots
                 SET state = 'ready', health = 'healthy', pod_netns_path = NULL,
                     lease_kind = ?1",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();
    }

    pub fn config(&self) -> NetworkPoolConfig {
        self.config
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The config an update would produce, validated but not applied.
    ///
    /// Exposed separately so a caller can find out whether an update is
    /// publishable *before* doing work on its behalf. The device-capacity step
    /// in front of a live slot increase creates kernel loop devices and `/dev`
    /// nodes, and it never removes them — so doing that for an update that then
    /// fails validation would leave durable host state behind for a request
    /// that was rejected.
    pub fn candidate_config(
        &self,
        update: &NetworkPoolConfigUpdate,
    ) -> Result<NetworkPoolConfig, String> {
        let current = self.config.lock().map_err(|e| e.to_string())?.clone();
        Self::candidate_config_from(&current, update)
    }

    fn candidate_config_from(
        current: &NetworkPoolConfig,
        update: &NetworkPoolConfigUpdate,
    ) -> Result<NetworkPoolConfig, String> {
        let mut next = current.clone();
        if let Some(max_slots) = update.max_slots {
            next.max_slots = max_slots;
        }
        if let Some(target_ready_slots) = update.target_ready_slots {
            next.target_ready_slots = target_ready_slots;
        }
        if let Some(retained_ready_slots) = update.retained_ready_slots {
            next.retained_ready_slots = retained_ready_slots;
        }
        if let Some(min_ready_slots) = update.min_ready_slots {
            next.min_ready_slots = min_ready_slots;
        }
        validate_config(&next)?;

        if next.max_slots < current.max_slots {
            return Err(
                "live max_slots shrink is not supported; drain and release out-of-range leases before restarting with a smaller max_slots"
                    .to_string(),
            );
        }
        Ok(next)
    }

    pub fn update_config(
        &self,
        update: NetworkPoolConfigUpdate,
    ) -> Result<NetworkPoolConfig, String> {
        // Validation and publication are one critical section. The service also
        // serializes the preceding asynchronous host-capacity preparation.
        let mut config = self.config.lock().map_err(|e| e.to_string())?;
        let current = &*config;
        let next = Self::candidate_config_from(current, &update)?;

        if next.max_slots > current.max_slots {
            let conn = self.db.lock().map_err(|e| e.to_string())?;
            ensure_slot_rows(&conn, &next).map_err(|e| e.to_string())?;
        }

        let retention_shrunk = next.retained_ready_slots < current.retained_ready_slots;
        *config = next.clone();
        drop(config);
        if retention_shrunk {
            self.nudge_prune();
        }
        self.nudge_topup();
        crate::node_state_sync::mark_node_status_dirty();
        Ok(next)
    }

    pub async fn allocate(
        self: &std::sync::Arc<Self>,
        sandbox_id: &str,
        pod_netns_path: Option<&str>,
        memory_reservation: Option<NewLeaseMemoryReservation>,
    ) -> Result<SlotInfo, AllocateSlotError> {
        let allocation = self.claim_allocation(sandbox_id, pod_netns_path, memory_reservation)?;
        self.finish_allocation(allocation).await
    }

    pub(crate) fn claim_allocation(
        self: &std::sync::Arc<Self>,
        sandbox_id: &str,
        pod_netns_path: Option<&str>,
        memory_reservation: Option<NewLeaseMemoryReservation>,
    ) -> Result<ClaimedSlotAllocation, AllocateSlotError> {
        if self.shutdown_token.is_cancelled() {
            return Err(AllocateSlotError::InProgress(
                "node-manager is shutting down".into(),
            ));
        }
        if self.is_draining()? {
            return Err(AllocateSlotError::ResourceExhausted(
                "network slot pool is draining".to_string(),
            ));
        }
        let config = self.config();
        let requested_pod_netns = pod_netns_path.map(str::to_string);

        if config.warm_pool_enabled() && requested_pod_netns == config.pod_netns_path {
            return self
                .allocate_pooled_slot(sandbox_id, requested_pod_netns, memory_reservation)
                .map(ClaimedSlotAllocation::Complete);
        }

        self.claim_ephemeral_slot_for_setup(sandbox_id, requested_pod_netns, memory_reservation)
            .map(|claim| match claim {
                EphemeralSlotClaimResult::Existing(info) => ClaimedSlotAllocation::Complete(info),
                EphemeralSlotClaimResult::Claimed(claim) => ClaimedSlotAllocation::Ephemeral(claim),
            })
    }

    pub(crate) async fn finish_allocation(
        self: &std::sync::Arc<Self>,
        allocation: ClaimedSlotAllocation,
    ) -> Result<SlotInfo, AllocateSlotError> {
        match allocation {
            ClaimedSlotAllocation::Complete(info) => Ok(info),
            ClaimedSlotAllocation::Ephemeral(claim) => {
                self.finish_ephemeral_slot_allocation(claim).await
            }
        }
    }

    fn allocate_pooled_slot(
        &self,
        sandbox_id: &str,
        requested_pod_netns: Option<String>,
        memory_reservation: Option<NewLeaseMemoryReservation>,
    ) -> Result<SlotInfo, AllocateSlotError> {
        let mut conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let tx = conn.transaction().map_err(SlotStoreError::from)?;

        let stale_slot_to_reset = match query_current_lease_by_sandbox(&tx, sandbox_id)
            .map_err(SlotStoreError::from)?
        {
            Some(record) => match active_record_to_allocate_result(&record, &requested_pod_netns) {
                Ok(Some(info)) => {
                    self.adopt_record(&record)?;
                    return Ok(info);
                }
                Ok(None) => None,
                Err(error) => {
                    if !should_retire_stale_pod_namespace_lease(&record, &requested_pod_netns) {
                        return Err(error);
                    }
                    Some(retire_stale_pod_namespace_lease(
                        &tx,
                        &record,
                        &requested_pod_netns,
                    )?)
                }
            },
            None => None,
        };

        let row = tx
            .query_row(
                "SELECT slot_id, generation FROM network_slots
                 WHERE state = 'ready' AND health = 'healthy' AND sandbox_id IS NULL
                   AND ((pod_netns_path IS NULL AND ?1 IS NULL) OR pod_netns_path = ?1)
                 ORDER BY slot_id
                 LIMIT 1",
                params![requested_pod_netns],
                |row| Ok((row.get::<_, u32>(0)?, row.get::<_, SqliteU64>(1)?.0)),
            )
            .optional()
            .map_err(SlotStoreError::from)?;

        let Some((slot_id, generation)) = row else {
            tx.commit().map_err(SlotStoreError::from)?;
            drop(conn);
            if let Some(slot_id) = stale_slot_to_reset {
                self.enqueue_reset(slot_id);
                crate::node_state_sync::mark_node_status_dirty();
            }
            self.nudge_topup();
            return Err(AllocateSlotError::ResourceExhausted(
                "no ready network slots".to_string(),
            ));
        };

        let memory_reservation_mib = persist_new_lease_memory_reservation(memory_reservation)?;
        let lease_generation = generation + 1;
        let sandbox_uid = net::sandbox_uid_for_slot(slot_id);
        tx.execute(
            concat!(
                "UPDATE network_slots
                 SET generation = ?1, state = 'leased', health = 'healthy',
                     sandbox_id = ?2, sandbox_uid = ?3, pid = NULL,
                     lease_kind = ?4, memory_reservation_mib = ?5, last_error = NULL,
                     ",
                clear_cleanup_reset_retry_sql!(),
                ",
                     updated_at = ?6, reuse_count = reuse_count + 1
                 WHERE slot_id = ?7"
            ),
            params![
                SqliteU64(lease_generation),
                sandbox_id,
                sandbox_uid,
                LeaseKind::Pooled.as_str(),
                SqliteU64(memory_reservation_mib),
                SqliteU64(now_secs()),
                slot_id
            ],
        )
        .map_err(SlotStoreError::from)?;
        tx.commit().map_err(SlotStoreError::from)?;
        clear_stale_transient_setup_failures_for_slot(self, slot_id, None);

        if let Some(slot_id) = stale_slot_to_reset {
            self.enqueue_reset(slot_id);
        }
        crate::node_state_sync::mark_node_status_dirty();
        Ok(SlotInfo {
            sandbox_id: sandbox_id.to_string(),
            slot: slot_id,
            generation: lease_generation,
            sandbox_uid,
            pid: None,
            pod_netns: requested_pod_netns,
        })
    }

    async fn finish_ephemeral_slot_allocation(
        self: &std::sync::Arc<Self>,
        claim: EphemeralSlotClaim,
    ) -> Result<SlotInfo, AllocateSlotError> {
        let requested_pod_netns = claim.info.pod_netns.clone();
        let slot_id = claim.info.slot;
        let generation = claim.info.generation;
        let pool = self.clone();
        let setup_pod_netns = requested_pod_netns.clone();
        self.run_ephemeral_setup(claim, move || {
            let net = NetId::from_slot(slot_id);
            let ns = net.ns();
            pool.handoff_network_namespace(
                slot_id,
                generation,
                SlotState::Leasing,
                setup_pod_netns.as_deref(),
            )?;
            let result = net::setup_network(&ns, &net, setup_pod_netns.as_deref())
                .map_err(EphemeralSetupError::Setup)
                .and_then(|_| {
                    validate_slot_ready(slot_id, setup_pod_netns.as_deref())
                        .map_err(EphemeralSetupError::Validation)
                });
            if result.is_err()
                && let Err(error) = net::teardown_network(&ns, &net, setup_pod_netns.as_deref())
            {
                tracing::warn!(slot_id, %error, "partial setup cleanup remains pending");
            }
            result
        })
        .await
    }

    async fn run_ephemeral_setup(
        self: &std::sync::Arc<Self>,
        claim: EphemeralSlotClaim,
        setup: impl FnOnce() -> Result<(), EphemeralSetupError> + Send + 'static,
    ) -> Result<SlotInfo, AllocateSlotError> {
        let pool = self.clone();
        self.task_tracker
            .spawn(async move {
                let blocking_ownership = claim.ownership.clone();
                let result = pool
                    .task_tracker
                    .spawn_blocking(move || {
                        let _ownership = blocking_ownership;
                        setup()
                    })
                    .await
                    .map_err(|error| EphemeralSetupError::TaskJoin(error.to_string()))
                    .and_then(|result| result);
                pool.complete_ephemeral_setup(claim, result).await
            })
            .await
            .map_err(|error| {
                AllocateSlotError::Internal(format!("allocation task failed: {error}"))
            })?
    }

    async fn complete_ephemeral_setup(
        &self,
        claim: EphemeralSlotClaim,
        result: Result<(), EphemeralSetupError>,
    ) -> Result<SlotInfo, AllocateSlotError> {
        let slot_id = claim.info.slot;
        let setup_key = SetupRetryKey::ephemeral(slot_id);
        let requested_pod_netns = &claim.info.pod_netns;
        match result {
            Ok(()) => {
                clear_transient_setup_failure_for_key(self, setup_key);
                if let Err(error) = self.mark_ephemeral_slot_leased(&claim) {
                    self.recover_ephemeral_persistence_failure(&claim).await;
                    return Err(error);
                }
                claim.ownership.disarm();
                self.rebalance_ready_slots();
                Ok(claim.info)
            }
            Err(error) => {
                if let Err(store_error) =
                    self.persist_ephemeral_setup_failure(&claim, setup_key, &error)
                {
                    self.recover_ephemeral_persistence_failure(&claim).await;
                    return Err(store_error.into());
                }
                // A concurrent release can supersede the failed setup. Its
                // own reset wakeup survives this disarm.
                claim.ownership.disarm();
                self.rebalance_ready_slots();
                Err(AllocateSlotError::Internal(format!(
                    "failed to setup dynamic network slot {slot_id} for requested pod namespace {:?}: {error}",
                    requested_pod_netns
                )))
            }
        }
    }

    fn claim_ephemeral_slot_for_setup(
        self: &std::sync::Arc<Self>,
        sandbox_id: &str,
        requested_pod_netns: Option<String>,
        memory_reservation: Option<NewLeaseMemoryReservation>,
    ) -> Result<EphemeralSlotClaimResult, AllocateSlotError> {
        let config = self.config();
        let mut conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let tx = conn.transaction().map_err(SlotStoreError::from)?;

        let stale_slot_to_reset = match query_current_lease_by_sandbox(&tx, sandbox_id)
            .map_err(SlotStoreError::from)?
        {
            Some(record) => match active_record_to_allocate_result(&record, &requested_pod_netns) {
                Ok(Some(info)) => {
                    self.adopt_record(&record)?;
                    return Ok(EphemeralSlotClaimResult::Existing(info));
                }
                Ok(None) => None,
                Err(error) => {
                    if !should_retire_stale_pod_namespace_lease(&record, &requested_pod_netns) {
                        return Err(error);
                    }
                    Some(retire_stale_pod_namespace_lease(
                        &tx,
                        &record,
                        &requested_pod_netns,
                    )?)
                }
            },
            None => None,
        };

        let row = tx
            .query_row(
                "SELECT slot_id, generation, state, pod_netns_path FROM network_slots
                 WHERE slot_id < ?1 AND sandbox_id IS NULL
                   AND (state = 'missing' OR (state = 'ready' AND health = 'healthy'))
                 ORDER BY CASE state WHEN 'missing' THEN 0 ELSE 1 END, slot_id
                 LIMIT 1",
                params![config.max_slots],
                |row| {
                    Ok((
                        row.get::<_, u32>(0)?,
                        row.get::<_, SqliteU64>(1)?.0,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(SlotStoreError::from)?;

        let Some((slot_id, generation, previous_state, previous_pod_netns)) = row else {
            tx.commit().map_err(SlotStoreError::from)?;
            if let Some(slot_id) = stale_slot_to_reset {
                self.enqueue_reset(slot_id);
                crate::node_state_sync::mark_node_status_dirty();
            }
            return Err(AllocateSlotError::ResourceExhausted(
                "no idle network slots".to_string(),
            ));
        };

        let memory_reservation_mib = persist_new_lease_memory_reservation(memory_reservation)?;
        let ownership = ResetClaim::acquire(self, slot_id).ok_or_else(|| {
            AllocateSlotError::InProgress("slot mutation already in progress".into())
        })?;
        let lease_generation = generation + 1;
        let sandbox_uid = net::sandbox_uid_for_slot(slot_id);
        let committed = (|| -> Result<(), AllocateSlotError> {
            let changed = tx
                .execute(
                    concat!(
                        "UPDATE network_slots
                     SET generation = ?1, state = 'leasing', health = 'unknown',
                         sandbox_id = ?2, sandbox_uid = ?3, pid = NULL,
                         pod_netns_path = ?4, lease_kind = ?5, memory_reservation_mib = ?6,
                         last_error = NULL,
                         ",
                        clear_cleanup_reset_retry_sql!(),
                        ",
                         updated_at = ?7, reuse_count = reuse_count + 1
                     WHERE slot_id = ?8 AND generation = ?9 AND state = ?10 AND sandbox_id IS NULL"
                    ),
                    params![
                        SqliteU64(lease_generation),
                        sandbox_id,
                        sandbox_uid,
                        previous_pod_netns,
                        LeaseKind::Ephemeral.as_str(),
                        SqliteU64(memory_reservation_mib),
                        SqliteU64(now_secs()),
                        slot_id,
                        SqliteU64(generation),
                        previous_state,
                    ],
                )
                .map_err(SlotStoreError::from)?;
            if changed != 1 {
                return Err(AllocateSlotError::InProgress(
                    "network slot changed while claiming dynamic lease".to_string(),
                ));
            }
            tx.commit().map_err(SlotStoreError::from)?;
            Ok(())
        })();
        if let Err(error) = committed {
            // The transaction rolled back without handing any work to a
            // mutator. There is no new cleanup obligation to retry.
            ownership.disarm();
            return Err(error);
        }
        clear_stale_transient_setup_failures_for_slot(
            self,
            slot_id,
            Some(SetupRetryKey::ephemeral(slot_id)),
        );

        if let Some(slot_id) = stale_slot_to_reset {
            self.enqueue_reset(slot_id);
        }
        crate::node_state_sync::mark_node_status_dirty();
        Ok(EphemeralSlotClaimResult::Claimed(EphemeralSlotClaim {
            info: SlotInfo {
                sandbox_id: sandbox_id.to_string(),
                slot: slot_id,
                generation: lease_generation,
                sandbox_uid,
                pid: None,
                pod_netns: requested_pod_netns,
            },
            ownership,
        }))
    }

    /// The stored namespace always identifies the resources recovery must
    /// clean. Change it only after old teardown succeeds and before creating
    /// anything in the replacement namespace, while retaining the slot claim.
    fn handoff_network_namespace(
        &self,
        slot: u32,
        generation: u64,
        state: SlotState,
        target: Option<&str>,
    ) -> Result<(), NetworkHandoffError> {
        self.handoff_network_namespace_with(slot, generation, state, target, net::teardown_network)
    }

    fn handoff_network_namespace_with(
        &self,
        slot: u32,
        generation: u64,
        state: SlotState,
        target: Option<&str>,
        teardown: impl FnOnce(&str, &NetId, Option<&str>) -> Result<(), net::NetworkTeardownError>,
    ) -> Result<(), NetworkHandoffError> {
        let previous = {
            let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
            query_record_by_slot(&conn, slot)
                .map_err(SlotStoreError::from)?
                .filter(|record| record.info.generation == generation && record.state == state)
                .ok_or(NetworkHandoffError::Superseded { slot, generation })?
                .info
                .pod_netns
        };
        let net = NetId::from_slot(slot);
        teardown(&net.ns(), &net, previous.as_deref())?;
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let changed = conn
            .execute(
                "UPDATE network_slots SET pod_netns_path = ?1, updated_at = ?2
             WHERE slot_id = ?3 AND generation = ?4 AND state = ?5 AND pod_netns_path IS ?6",
                params![
                    target,
                    SqliteU64(now_secs()),
                    slot,
                    SqliteU64(generation),
                    state.as_str(),
                    previous
                ],
            )
            .map_err(SlotStoreError::from)?;
        if changed != 1 {
            return Err(NetworkHandoffError::Superseded { slot, generation });
        }
        Ok(())
    }

    fn mark_ephemeral_slot_leased(
        &self,
        claim: &EphemeralSlotClaim,
    ) -> Result<(), AllocateSlotError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let changed = conn
            .execute(
                concat!(
                    "UPDATE network_slots
                     SET state = 'leased', health = 'healthy', last_error = NULL,
                         ",
                    clear_cleanup_reset_retry_sql!(),
                    ",
                         updated_at = ?1
                     WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4
                       AND state = 'leasing' AND lease_kind = ?5"
                ),
                params![
                    SqliteU64(now_secs()),
                    claim.info.slot,
                    SqliteU64(claim.info.generation),
                    claim.info.sandbox_id,
                    LeaseKind::Ephemeral.as_str(),
                ],
            )
            .map_err(SlotStoreError::from)?;
        if changed == 1 {
            crate::node_state_sync::mark_node_status_dirty();
            Ok(())
        } else {
            Err(AllocateSlotError::Internal(format!(
                "dynamic network lease changed before ready transition for sandbox {}",
                claim.info.sandbox_id
            )))
        }
    }

    fn persist_ephemeral_setup_failure(
        &self,
        claim: &EphemeralSlotClaim,
        setup_key: SetupRetryKey,
        error: &EphemeralSetupError,
    ) -> Result<(), SlotStoreError> {
        let error_message = error.to_string();
        if error.is_transient() {
            let attempts = record_transient_setup_failure_for_key(self, setup_key);
            if attempts < MAX_SLOT_TRANSIENT_SETUP_RETRIES {
                tracing::warn!(
                    slot_id = claim.info.slot,
                    generation = claim.info.generation,
                    sandbox_id = %claim.info.sandbox_id,
                    error = %error_message,
                    setup_attempts = attempts,
                    max_setup_attempts = MAX_SLOT_TRANSIENT_SETUP_RETRIES,
                    "dynamic network slot setup hit transient host networking error"
                );
                return self.persist_ephemeral_setup_failure_state(
                    claim,
                    SlotState::Missing,
                    SlotHealth::Degraded,
                    &error_message,
                    false,
                );
            }

            tracing::error!(
                slot_id = claim.info.slot,
                generation = claim.info.generation,
                sandbox_id = %claim.info.sandbox_id,
                error = %error_message,
                setup_attempts = attempts,
                "dynamic network slot setup transient retries exhausted; quarantining slot"
            );
            clear_transient_setup_failure_for_key(self, setup_key);
            return self.persist_ephemeral_setup_failure_state(
                claim,
                SlotState::Quarantined,
                SlotHealth::Invalid,
                &format!(
                    "dynamic network slot setup exhausted transient retries after {attempts} attempts: {error_message}"
                ),
                true,
            );
        }

        clear_transient_setup_failure_for_key(self, setup_key);
        self.persist_ephemeral_setup_failure_state(
            claim,
            SlotState::Quarantined,
            SlotHealth::Invalid,
            &error_message,
            true,
        )
    }

    fn persist_ephemeral_setup_failure_state(
        &self,
        claim: &EphemeralSlotClaim,
        state: SlotState,
        health: SlotHealth,
        error: &str,
        increment_quarantine: bool,
    ) -> Result<(), SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let sql = if increment_quarantine {
            concat!(
                "UPDATE network_slots
                     SET state = ?1, health = ?2,
                         sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                         lease_kind = ?3, memory_reservation_mib = NULL,
                         ",
                clear_cleanup_reset_retry_sql!(),
                ",
                         last_error = ?4, updated_at = ?5,
                         quarantine_count = quarantine_count + 1
                     WHERE slot_id = ?6 AND generation = ?7 AND sandbox_id = ?8
                   AND state = 'leasing' AND lease_kind = ?9"
            )
        } else {
            concat!(
                "UPDATE network_slots
                     SET state = ?1, health = ?2,
                         sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                         lease_kind = ?3, memory_reservation_mib = NULL,
                         ",
                clear_cleanup_reset_retry_sql!(),
                ",
                         last_error = ?4, updated_at = ?5
                     WHERE slot_id = ?6 AND generation = ?7 AND sandbox_id = ?8
                   AND state = 'leasing' AND lease_kind = ?9"
            )
        };
        match conn.execute(
            sql,
            params![
                state.as_str(),
                health.as_str(),
                LeaseKind::Pooled.as_str(),
                error,
                SqliteU64(now_secs()),
                claim.info.slot,
                SqliteU64(claim.info.generation),
                claim.info.sandbox_id,
                LeaseKind::Ephemeral.as_str(),
            ],
        ) {
            Ok(changed) => {
                if changed > 0 {
                    crate::node_state_sync::mark_node_status_dirty();
                }
                Ok(())
            }
            Err(db_error) => {
                tracing::error!(
                    slot_id = claim.info.slot,
                    generation = claim.info.generation,
                    sandbox_id = %claim.info.sandbox_id,
                    error = %db_error,
                    "failed to persist dynamic network slot setup failure"
                );
                Err(db_error.into())
            }
        }
    }

    /// The tracked allocation remains the retry owner until it persists a
    /// cleanup obligation (or shutdown hands the durable lease to startup).
    /// Never abandon a leasing row after a failed success/failure write.
    async fn recover_ephemeral_persistence_failure(&self, claim: &EphemeralSlotClaim) {
        loop {
            let result = self.mark_ephemeral_for_cleanup(claim);
            match result {
                Ok(()) => return,
                Err(error) => {
                    tracing::error!(slot_id = claim.info.slot, %error, "ephemeral completion unavailable; retaining ownership and retrying")
                }
            }
            tokio::select! {
                _ = self.shutdown_token.cancelled() => return,
                _ = tokio::time::sleep(POOL_FILL_RETRY_DELAY) => {}
            }
        }
    }

    fn mark_ephemeral_for_cleanup(&self, claim: &EphemeralSlotClaim) -> Result<(), SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        conn.execute(
            "UPDATE network_slots SET state = 'dirty', health = 'degraded',
                 memory_reservation_mib = NULL,
                 last_error = 'ephemeral completion failed; cleanup required', updated_at = ?1
             WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4
               AND state = 'leasing' AND lease_kind = ?5",
            params![
                SqliteU64(now_secs()),
                claim.info.slot,
                SqliteU64(claim.info.generation),
                claim.info.sandbox_id,
                LeaseKind::Ephemeral.as_str()
            ],
        )?;
        crate::node_state_sync::mark_node_status_dirty();
        Ok(())
    }

    pub fn release(&self, sandbox_id: &str) -> Result<Option<SlotInfo>, String> {
        let (slot_id, info) = {
            let mut conn = self
                .db
                .lock()
                .map_err(|error| format!("slot pool db lock poisoned: {error}"))?;
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let Some(record) =
                query_current_lease_by_sandbox(&tx, sandbox_id).map_err(|e| e.to_string())?
            else {
                return Ok(None);
            };
            match record.state {
                SlotState::Dirty | SlotState::Resetting => return Ok(Some(record.info)),
                SlotState::Leased | SlotState::Leasing => {
                    let Some(lease) = LeaseKey::from_record(&record) else {
                        tx.execute(
                            "UPDATE network_slots
                             SET state = 'quarantined', health = 'invalid',
                                 last_error = 'active slot has no sandbox lease',
                                 updated_at = ?1,
                                 quarantine_count = quarantine_count + 1
                             WHERE slot_id = ?2",
                            params![SqliteU64(now_secs()), record.info.slot],
                        )
                        .map_err(|e| e.to_string())?;
                        tx.commit().map_err(|e| e.to_string())?;
                        crate::node_state_sync::mark_node_status_dirty();
                        return Ok(Some(record.info));
                    };
                    let changed = tx
                        .execute(
                            // Keep sandbox_id/sandbox_uid on dirty slots as previous-lease
                            // metadata. Active lease lookups only consider leasing/leased, so
                            // the same sandbox can acquire a new slot while reset still knows
                            // which retained cache UID must be scrubbed before reuse.
                            "UPDATE network_slots
                         SET state = 'dirty', health = 'unknown',
                             memory_reservation_mib = NULL, updated_at = ?1
                         WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4
                           AND state IN ('leasing', 'leased')",
                            params![
                                SqliteU64(now_secs()),
                                lease.slot_id,
                                SqliteU64(lease.generation),
                                lease.sandbox_id
                            ],
                        )
                        .map_err(|e| e.to_string())?;
                    if changed == 0 {
                        return Err(format!(
                            "network lease changed while releasing sandbox {sandbox_id}"
                        ));
                    }
                    tx.commit().map_err(|e| e.to_string())?;
                    (record.info.slot, record.info)
                }
                SlotState::Quarantined => return Ok(Some(record.info)),
                _ => return Ok(None),
            }
        };
        self.enqueue_reset(slot_id);
        crate::node_state_sync::mark_node_status_dirty();
        Ok(Some(info))
    }

    pub fn get(&self, sandbox_id: &str) -> Result<Option<SlotInfo>, SlotStoreError> {
        Ok(self.get_record(sandbox_id)?.map(|record| record.info))
    }

    pub fn get_record(&self, sandbox_id: &str) -> Result<Option<SlotRecord>, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        Ok(query_current_lease_by_sandbox(&conn, sandbox_id)?)
    }

    pub(crate) fn active_memory_reservations(
        &self,
    ) -> Result<Vec<ActiveLeaseMemoryReservation>, ActiveLeaseMemoryReservationError> {
        let conn = self
            .db
            .lock()
            .map_err(|_| ActiveLeaseMemoryReservationError::LockPoisoned)?;
        query_active_memory_reservations(&conn)?
            .into_iter()
            .map(
                |MemoryReservationRow {
                     slot_id,
                     sandbox_id,
                     generation,
                     memory_reservation_mib,
                 }| {
                    let parsed_sandbox_id = sandbox_id.parse::<SandboxId>().map_err(|source| {
                        ActiveLeaseMemoryReservationError::InvalidSandboxId {
                            sandbox_id: sandbox_id.clone(),
                            source,
                        }
                    })?;
                    let memory_reservation_mib = memory_reservation_mib.ok_or(
                        ActiveLeaseMemoryReservationError::MissingReservation {
                            sandbox_id: parsed_sandbox_id,
                        },
                    )?;
                    Ok(ActiveLeaseMemoryReservation {
                        slot_id,
                        sandbox_id: parsed_sandbox_id,
                        generation,
                        memory_reservation_mib,
                    })
                },
            )
            .collect()
    }

    pub fn dirty_infos_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<Vec<SlotInfo>, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let records = query_records(
            &conn,
            Some((
                "sandbox_id = ?1 AND state IN ('dirty', 'resetting')",
                sandbox_id,
            )),
        )?;
        Ok(records.into_iter().map(|record| record.info).collect())
    }

    /// Transfer a validated lease to the lifecycle owner immediately before host work.
    /// Recheck its identity under db so validation cannot adopt a replacement lease
    /// or revoke recovery when the request was rejected before reaching this point.
    pub(crate) fn adopt_lease(
        &self,
        sandbox_id: &str,
        expected: &SlotInfo,
    ) -> Result<Option<SlotInfo>, SlotStoreError> {
        self.adopt_lease_inner(sandbox_id, expected, false)
    }

    pub(crate) fn adopt_stopping_lease(
        &self,
        sandbox_id: &str,
        expected: &SlotInfo,
    ) -> Result<Option<SlotInfo>, SlotStoreError> {
        self.adopt_lease_inner(sandbox_id, expected, true)
    }

    fn adopt_lease_inner(
        &self,
        sandbox_id: &str,
        expected: &SlotInfo,
        allow_leasing: bool,
    ) -> Result<Option<SlotInfo>, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let Some(record) = query_current_lease_by_sandbox(&conn, sandbox_id)? else {
            return Ok(None);
        };
        if record.info.sandbox_id != sandbox_id
            || expected.sandbox_id != sandbox_id
            || (
                record.info.slot,
                record.info.generation,
                record.info.sandbox_uid,
            ) != (expected.slot, expected.generation, expected.sandbox_uid)
        {
            return Ok(None);
        }
        if record.state != SlotState::Leased
            && !(allow_leasing && record.state == SlotState::Leasing)
        {
            return Err(SlotStoreError::LeaseNotReady(sandbox_id.to_string()));
        }
        self.adopt_record(&record)?;
        Ok(Some(record.info))
    }

    // Caller holds db. Never acquire db while holding startup_leases.
    fn adopt_record(&self, record: &SlotRecord) -> Result<(), SlotStoreError> {
        if let Some(key) = LeaseKey::from_record(record) {
            self.startup_leases
                .lock()
                .map_err(|_| SlotStoreError::LockPoisoned)?
                .remove(&key);
        }
        Ok(())
    }

    pub fn set_pid(&self, sandbox_id: &str, pid: u32) -> Result<(), String> {
        crate::launch::validate_process_pid(pid)
            .map_err(|error| format!("refusing to persist invalid sandbox pid {pid}: {error}"))?;
        let conn = self.db.lock().map_err(|e| e.to_string())?;
        let record = record_lease_pid(&conn, sandbox_id, pid)
            .map_err(|error| format!("failed to persist sandbox pid {pid}: {error}"))?
            .ok_or_else(|| {
                format!("expected one exact active network lease for sandbox {sandbox_id}")
            })?;
        // Failed writes must not revoke startup recovery. Leasing rows retain
        // their interrupted-setup obligation; their re-probe fences on the PID.
        if record.state == SlotState::Leased {
            self.adopt_record(&record).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn all_records(&self) -> Result<Vec<SlotRecord>, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        Ok(query_records(&conn, None)?)
    }

    pub fn counts(&self) -> Result<PoolCounts, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let mut counts = PoolCounts::default();
        let mut stmt = conn
            .prepare("SELECT state, health, COUNT(*) FROM network_slots GROUP BY state, health")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u32>(2)?,
            ))
        })?;
        for row in rows {
            let (state, health, count) = row?;
            let parsed_state = state.parse::<SlotState>()?;
            let parsed_health = health.parse::<SlotHealth>()?;
            counts.total += count;
            match parsed_state {
                SlotState::Ready if parsed_health == SlotHealth::Healthy => {
                    counts.ready += count;
                }
                SlotState::Ready => {}
                SlotState::Leased | SlotState::Leasing => counts.leased += count,
                SlotState::Dirty => counts.dirty += count,
                SlotState::Resetting => counts.resetting += count,
                SlotState::Creating => counts.creating += count,
                SlotState::Quarantined => counts.quarantined += count,
                _ => {}
            }
            if parsed_health == SlotHealth::Degraded {
                counts.degraded += count;
            }
        }
        Ok(counts)
    }

    pub fn is_draining(&self) -> Result<bool, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        Ok(meta_bool(&conn, "draining")?)
    }

    pub fn set_draining(&self, draining: bool) -> Result<(), String> {
        let conn = self.db.lock().map_err(|e| e.to_string())?;
        set_meta_bool(&conn, "draining", draining).map_err(|e| e.to_string())?;
        if !draining {
            self.nudge_topup();
        }
        crate::node_state_sync::mark_node_status_dirty();
        Ok(())
    }

    pub fn repair(&self, rebuild_quarantined: bool) -> Result<PoolRepairResult, String> {
        if rebuild_quarantined {
            let conn = self.db.lock().map_err(|e| e.to_string())?;
            conn.execute(
                concat!(
                    "UPDATE network_slots
                     SET state = 'missing', health = 'degraded',
                         last_error = 'operator requested quarantined slot rebuild',
                         sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                         lease_kind = ?1, memory_reservation_mib = NULL,
                         ",
                    clear_cleanup_reset_retry_sql!(),
                    ",
                         updated_at = ?2
                     WHERE state = 'quarantined' AND sandbox_id IS NULL"
                ),
                params![LeaseKind::Pooled.as_str(), SqliteU64(now_secs())],
            )
            .map_err(|e| e.to_string())?;
        }

        let reset_enqueued = self
            .enqueue_pending_resets()
            .map_err(|error| error.to_string())?;
        self.nudge_topup();
        crate::node_state_sync::mark_node_status_dirty();
        Ok(PoolRepairResult {
            reset_enqueued,
            topup_requested: true,
        })
    }

    pub fn nudge_topup(&self) {
        if matches!(
            self.job_tx.try_send(()),
            Err(mpsc::error::TrySendError::Closed(()))
        ) {
            tracing::debug!("slot pool topup worker is not running");
        }
    }

    pub fn nudge_prune(&self) {
        self.nudge_topup();
    }

    pub fn enqueue_slot_reset(&self, slot_id: u32) {
        self.enqueue_reset(slot_id);
    }

    fn rebalance_ready_slots(&self) {
        let counts = match self.counts() {
            Ok(counts) => counts,
            Err(error) => {
                tracing::warn!(%error, "cannot rebalance network slots without database inventory");
                return;
            }
        };
        let config = self.config();
        if counts.ready > config.retained_ready_slots {
            self.nudge_prune();
        } else if counts.ready < config.target_ready_slots {
            self.nudge_topup();
        }
    }

    fn enqueue_reset(&self, _slot_id: u32) {
        self.nudge_topup();
    }

    fn enqueue_pending_resets(&self) -> Result<u32, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        let count = query_pending_reset_count(&conn)?;
        drop(conn);
        self.nudge_topup();
        Ok(count)
    }
}

fn normalize_netns_name(value: &str) -> String {
    std::path::Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(value)
        .to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum AllocateSlotError {
    #[error(transparent)]
    Store(#[from] SlotStoreError),
    #[error("{0}")]
    InvalidArgument(String),
    #[error("{0}")]
    ResourceExhausted(String),
    #[error("{0}")]
    FailedPrecondition(String),
    #[error("{0}")]
    InProgress(String),
    #[error("{0}")]
    Internal(String),
}

fn persist_new_lease_memory_reservation(
    memory_reservation: Option<NewLeaseMemoryReservation>,
) -> Result<u64, AllocateSlotError> {
    let memory_reservation = memory_reservation.ok_or_else(|| {
        AllocateSlotError::Internal(
            "new network lease is missing memory admission data".to_string(),
        )
    })?;
    Ok(memory_reservation.memory_reservation_mib)
}

fn active_record_to_allocate_result(
    record: &SlotRecord,
    requested_pod_netns: &Option<String>,
) -> Result<Option<SlotInfo>, AllocateSlotError> {
    match record.state {
        SlotState::Leased => {
            if &record.info.pod_netns == requested_pod_netns {
                Ok(Some(record.info.clone()))
            } else {
                Err(AllocateSlotError::FailedPrecondition(format!(
                    "existing network lease for sandbox {} uses pod namespace {:?}, requested {:?}",
                    record.info.sandbox_id, record.info.pod_netns, requested_pod_netns
                )))
            }
        }
        SlotState::Leasing => Err(AllocateSlotError::InProgress(
            "network lease is still in progress".to_string(),
        )),
        SlotState::Dirty | SlotState::Resetting => Err(AllocateSlotError::FailedPrecondition(
            "previous network lease is being reset".to_string(),
        )),
        SlotState::Quarantined => Err(AllocateSlotError::FailedPrecondition(format!(
            "previous network lease is quarantined: {}",
            record.last_error.clone().unwrap_or_default()
        ))),
        _ => Ok(None),
    }
}

fn should_retire_stale_pod_namespace_lease(
    record: &SlotRecord,
    requested_pod_netns: &Option<String>,
) -> bool {
    if record.state != SlotState::Leased
        || record.info.pod_netns.as_ref() == requested_pod_netns.as_ref()
    {
        return false;
    }
    let Some(existing_pod_netns) = record.info.pod_netns.as_deref() else {
        return false;
    };
    if requested_pod_netns.is_none() {
        return false;
    }
    if lease_may_be_live(&record.info) {
        return false;
    }
    !net::ns_exists(existing_pod_netns)
}

fn retire_stale_pod_namespace_lease(
    tx: &Transaction<'_>,
    record: &SlotRecord,
    requested_pod_netns: &Option<String>,
) -> Result<u32, AllocateSlotError> {
    let last_error = format!(
        "retired stale network lease for sandbox {} after pod namespace {:?} disappeared; requested {:?}",
        record.info.sandbox_id, record.info.pod_netns, requested_pod_netns
    );
    let changed = tx
        .execute(
            "UPDATE network_slots
             SET state = 'dirty', health = 'degraded',
                 last_error = ?1, updated_at = ?2
             WHERE slot_id = ?3 AND generation = ?4 AND sandbox_id = ?5
               AND state = 'leased'",
            params![
                last_error,
                SqliteU64(now_secs()),
                record.info.slot,
                SqliteU64(record.info.generation),
                record.info.sandbox_id,
            ],
        )
        .map_err(SlotStoreError::from)?;
    if changed != 1 {
        return Err(AllocateSlotError::InProgress(format!(
            "stale network lease changed while retiring sandbox {}",
            record.info.sandbox_id
        )));
    }
    tracing::warn!(
        slot_id = record.info.slot,
        generation = record.info.generation,
        sandbox_id = %record.info.sandbox_id,
        previous_pod_netns = ?record.info.pod_netns,
        requested_pod_netns = ?requested_pod_netns,
        "retired stale network lease after previous pod namespace disappeared"
    );
    Ok(record.info.slot)
}

#[cfg(test)]
mod claim_contention_tests;
mod db;
mod execution;
mod liveness;
mod reset;
mod scheduler;
mod startup_containment;
use liveness::{LeaseLiveness, lease_liveness, lease_may_be_live};
#[cfg(test)]
mod tests;
mod time;
mod worker;

use db::*;
use reset::*;
use scheduler::*;
use time::*;
use worker::*;
