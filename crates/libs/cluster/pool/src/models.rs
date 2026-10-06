use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::types::{
    ContentHash, DiskIoLimits, MemorySize, TemplateName, VcpuCount,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{PoolConfigOwner, PoolError, PoolSlotState, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolAssignment {
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: DiskIoLimits,
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub id: Uuid,
    pub template_release_id: Uuid,
    pub template_name: TemplateName,
    pub content_hash: ContentHash,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: DiskIoLimits,
    pub target_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
    pub namespace: String,
    pub enabled: bool,
    /// Members of an encryptable config serve only encrypted creates; the
    /// reconciler must not fill one until provisioning carries that shape.
    pub encryptable: bool,
    pub managed_by: PoolConfigOwner,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// A pool config joined with its live slot counts, as served by the
/// cluster-pool admin API. Numeric fields keep their storage types and
/// `template_name`/`content_hash` stay raw strings: this is a read model for
/// presentation, and parsing legacy rows into domain types here would turn a
/// malformed historical row into a failed list endpoint.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PoolConfigWithCounts {
    pub id: Uuid,
    pub template_release_id: Uuid,
    pub template_name: String,
    pub content_hash: String,
    pub vcpu: i32,
    pub memory_mib: i32,
    pub disk_mib: Option<i32>,
    pub disk_io_max_operations_per_sec: i64,
    pub disk_io_max_bandwidth_mib_per_sec: i64,
    pub target_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
    pub namespace: String,
    pub enabled: bool,
    pub encryptable: bool,
    pub managed_by: PoolConfigOwner,
    pub warming_slots: Option<i64>,
    pub ready_slots: Option<i64>,
    pub claimed_slots: Option<i64>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// Per-release pool configs with live slot counts, keyed for grouping by
/// `template_release_id` — the admin template-release listing's warm-pool
/// summary. Same read-model conventions as [`PoolConfigWithCounts`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReleasePoolConfigWithCounts {
    pub template_release_id: Uuid,
    pub id: Uuid,
    pub vcpu: i32,
    pub memory_mib: i32,
    pub disk_mib: Option<i32>,
    pub disk_io_max_operations_per_sec: i64,
    pub disk_io_max_bandwidth_mib_per_sec: i64,
    pub target_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
    pub namespace: String,
    pub enabled: bool,
    pub encryptable: bool,
    pub managed_by: PoolConfigOwner,
    pub warming_slots: Option<i64>,
    pub ready_slots: Option<i64>,
    pub claimed_slots: Option<i64>,
}

/// Immutable template release protected from node-local artifact eviction by
/// either an enabled pool config or an enabled policy's retained window.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProtectedTemplateRelease {
    pub template_name: String,
    pub content_hash: String,
}

/// The claimed pod's identity as read inside the bind transaction — the
/// column set the API copies onto the sandbox's execution rows. `pod_uid`
/// is normalized here (blank → NULL) so every consumer binds the same
/// value. Row-locked at read time; see [`crate::WarmPoolStore::bind_source`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClaimedSlotBindSource {
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: Option<String>,
    pub pod_ip: Option<String>,
    pub k8s_node_name: Option<String>,
}

/// Operator-requested (`managed_by = 'manual'`) pool config shape for
/// [`crate::WarmPoolStore::upsert_manual_config`]. Numeric fields are already
/// range-validated by the API boundary; they keep the storage width here.
/// Template release resolution is the caller's job (template-domain logic),
/// so the spec carries only pool-config fields.
#[derive(Debug, Clone, Copy)]
pub struct ManualPoolConfigSpec<'a> {
    pub vcpu: i32,
    pub memory_mib: i32,
    pub disk_mib: Option<i32>,
    pub reset_disk_to_template_default: bool,
    pub target_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
    pub namespace: &'a str,
    pub enabled: bool,
    /// A plaintext and an encryptable config for one (release, shape) are
    /// distinct rows; the upsert must never fold one into the other.
    pub encryptable: bool,
}

#[derive(Debug, Clone)]
pub struct PoolSlot {
    pub id: Uuid,
    pub config_id: Uuid,
    pub template_release_id: Uuid,
    pub reserved_sandbox_id: SandboxId,
    pub template_name: TemplateName,
    pub content_hash: ContentHash,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: DiskIoLimits,
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: Option<String>,
    pub pod_ip: Option<String>,
    pub k8s_node_name: Option<String>,
    pub state: PoolSlotState,
    pub claim_id: Option<Uuid>,
    pub ready_at: Option<OffsetDateTime>,
    pub claimed_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub enum OpenPoolSlotDeleteLock {
    Locked(Box<PoolSlot>),
    NotFound,
    NotOpen { state: PoolSlotState },
    Busy,
}

#[derive(Debug, Clone)]
pub struct ClaimedPoolSlot {
    pub id: Uuid,
    pub reserved_sandbox_id: SandboxId,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: DiskIoLimits,
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: Option<String>,
    pub pod_ip: Option<String>,
    pub k8s_node_name: Option<String>,
}

/// The slot reserving a given sandbox ID, read under a row lock for a create
/// that was admitted against that slot. `state` and `claim_id` are what the
/// caller checks before adopting: only a `claimed` slot carrying the admitted
/// claim id is the caller's to adopt.
#[derive(Debug, Clone)]
pub struct ReservedPoolSlot {
    pub slot: ClaimedPoolSlot,
    pub template_release_id: Uuid,
    pub content_hash: String,
    pub state: crate::PoolSlotState,
    pub claim_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct NewPoolSlot<'a> {
    pub config_id: Uuid,
    pub template_release_id: Uuid,
    pub reserved_sandbox_id: SandboxId,
    pub template_name: &'a TemplateName,
    pub content_hash: ContentHash,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub pod_name: &'a str,
    pub pod_namespace: &'a str,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolSlotCounts {
    pub warming: i64,
    pub ready: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct PoolAutoscaleCandidate {
    pub template_release_id: Uuid,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: DiskIoLimits,
    pub target_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct PoolPolicyRelease {
    pub template_release_id: Uuid,
    pub template_name: TemplateName,
    pub content_hash: String,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: DiskIoLimits,
    pub min_ready_slots: i32,
    pub max_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
    pub retain_releases: i32,
    pub retained: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PoolObservedDemand {
    pub template_release_id: Uuid,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub observed_sandboxes: i64,
}

impl PoolSlotCounts {
    pub fn open(self) -> i64 {
        self.warming + self.ready
    }
}

pub(crate) fn vcpu_to_i32(value: VcpuCount) -> i32 {
    i32::try_from(value.get().get()).expect("validated vcpu count fits i32")
}

pub(crate) fn memory_to_mib_i32(value: MemorySize) -> i32 {
    i32::try_from(value.as_mib_floor()).expect("validated memory size fits i32")
}

pub(crate) fn parse_vcpu(value: i32) -> Result<VcpuCount> {
    let value = u32::try_from(value).map_err(|_| PoolError::InvalidResource {
        field: "vcpu",
        value,
    })?;
    Ok(VcpuCount::try_from_u32(value)?)
}

pub(crate) fn parse_memory_mib(value: i32) -> Result<MemorySize> {
    let value = u64::try_from(value).map_err(|_| PoolError::InvalidResource {
        field: "memory_mib",
        value,
    })?;
    Ok(MemorySize::mib(value)?)
}

pub(crate) fn parse_disk_mib(value: Option<i32>) -> Result<Option<u32>> {
    value
        .map(|value| {
            u32::try_from(value)
                .ok()
                .filter(|value| *value > 0)
                .ok_or(PoolError::InvalidResource {
                    field: "disk_mib",
                    value,
                })
        })
        .transpose()
}

pub(crate) fn parse_disk_io_limits(
    max_operations_per_sec: i64,
    max_bandwidth_mib_per_sec: i64,
) -> Result<DiskIoLimits> {
    let operations =
        u64::try_from(max_operations_per_sec).map_err(|_| PoolError::InvalidDiskIo {
            field: "disk_io_max_operations_per_sec",
            value: max_operations_per_sec,
        })?;
    let bandwidth =
        u64::try_from(max_bandwidth_mib_per_sec).map_err(|_| PoolError::InvalidDiskIo {
            field: "disk_io_max_bandwidth_mib_per_sec",
            value: max_bandwidth_mib_per_sec,
        })?;
    Ok(DiskIoLimits::try_new(operations, bandwidth)?)
}
