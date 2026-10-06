use pplx_space_common_core::types::SandboxId;

use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    ClaimedPoolSlot, PoolConfig, PoolError, PoolSlot, PoolSlotCounts, ReservedPoolSlot, Result,
    models::{
        PoolObservedDemand, PoolPolicyRelease, parse_disk_io_limits, parse_disk_mib,
        parse_memory_mib, parse_vcpu,
    },
};

#[derive(sqlx::FromRow)]
pub(crate) struct PoolConfigRow {
    id: Uuid,
    template_release_id: Uuid,
    template_name: String,
    content_hash: String,
    vcpu: i32,
    memory_mib: i32,
    disk_mib: Option<i32>,
    disk_io_max_operations_per_sec: i64,
    disk_io_max_bandwidth_mib_per_sec: i64,
    target_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
    namespace: String,
    enabled: bool,
    encryptable: bool,
    managed_by: crate::PoolConfigOwner,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl TryFrom<PoolConfigRow> for PoolConfig {
    type Error = PoolError;

    fn try_from(row: PoolConfigRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            template_release_id: row.template_release_id,
            template_name: row.template_name.parse()?,
            content_hash: row.content_hash.parse()?,
            vcpu: parse_vcpu(row.vcpu)?,
            memory: parse_memory_mib(row.memory_mib)?,
            disk_mib: parse_disk_mib(row.disk_mib)?,
            disk_io_limits: parse_disk_io_limits(
                row.disk_io_max_operations_per_sec,
                row.disk_io_max_bandwidth_mib_per_sec,
            )?,
            target_ready_slots: row.target_ready_slots,
            refill_per_tick: row.refill_per_tick,
            max_warming_slots: row.max_warming_slots,
            namespace: row.namespace,
            enabled: row.enabled,
            encryptable: row.encryptable,
            managed_by: row.managed_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct PoolSlotCountsRow {
    warming: Option<i64>,
    ready: Option<i64>,
}

impl From<PoolSlotCountsRow> for PoolSlotCounts {
    fn from(row: PoolSlotCountsRow) -> Self {
        Self {
            warming: row.warming.unwrap_or_default(),
            ready: row.ready.unwrap_or_default(),
        }
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct PoolPolicyReleaseRow {
    template_release_id: Uuid,
    template_name: String,
    content_hash: String,
    vcpu: i32,
    memory_mib: i32,
    disk_mib: Option<i32>,
    disk_io_max_operations_per_sec: i64,
    disk_io_max_bandwidth_mib_per_sec: i64,
    min_ready_slots: i32,
    max_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
    retain_releases: i32,
    retained: bool,
}

impl TryFrom<PoolPolicyReleaseRow> for PoolPolicyRelease {
    type Error = PoolError;

    fn try_from(row: PoolPolicyReleaseRow) -> Result<Self> {
        Ok(Self {
            template_release_id: row.template_release_id,
            template_name: row.template_name.parse()?,
            content_hash: row.content_hash,
            vcpu: parse_vcpu(row.vcpu)?,
            memory: parse_memory_mib(row.memory_mib)?,
            disk_mib: parse_disk_mib(row.disk_mib)?,
            disk_io_limits: parse_disk_io_limits(
                row.disk_io_max_operations_per_sec,
                row.disk_io_max_bandwidth_mib_per_sec,
            )?,
            min_ready_slots: row.min_ready_slots,
            max_ready_slots: row.max_ready_slots,
            refill_per_tick: row.refill_per_tick,
            max_warming_slots: row.max_warming_slots,
            retain_releases: row.retain_releases,
            retained: row.retained,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct PoolObservedDemandRow {
    template_release_id: Uuid,
    vcpu: i32,
    memory_mib: i32,
    observed_sandboxes: i64,
}

impl TryFrom<PoolObservedDemandRow> for PoolObservedDemand {
    type Error = PoolError;

    fn try_from(row: PoolObservedDemandRow) -> Result<Self> {
        Ok(Self {
            template_release_id: row.template_release_id,
            vcpu: parse_vcpu(row.vcpu)?,
            memory: parse_memory_mib(row.memory_mib)?,
            observed_sandboxes: row.observed_sandboxes,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct PoolSlotRow {
    id: Uuid,
    config_id: Uuid,
    template_release_id: Uuid,
    reserved_sandbox_id: SandboxId,
    template_name: String,
    content_hash: String,
    vcpu: i32,
    memory_mib: i32,
    disk_mib: Option<i32>,
    disk_io_max_operations_per_sec: i64,
    disk_io_max_bandwidth_mib_per_sec: i64,
    pod_name: String,
    pod_namespace: String,
    pod_uid: Option<String>,
    pod_ip: Option<String>,
    k8s_node_name: Option<String>,
    state: String,
    claim_id: Option<Uuid>,
    ready_at: Option<OffsetDateTime>,
    claimed_at: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl TryFrom<PoolSlotRow> for PoolSlot {
    type Error = PoolError;

    fn try_from(row: PoolSlotRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            config_id: row.config_id,
            template_release_id: row.template_release_id,
            reserved_sandbox_id: row.reserved_sandbox_id,
            template_name: row.template_name.parse()?,
            content_hash: row.content_hash.parse()?,
            vcpu: parse_vcpu(row.vcpu)?,
            memory: parse_memory_mib(row.memory_mib)?,
            disk_mib: parse_disk_mib(row.disk_mib)?,
            disk_io_limits: parse_disk_io_limits(
                row.disk_io_max_operations_per_sec,
                row.disk_io_max_bandwidth_mib_per_sec,
            )?,
            pod_name: row.pod_name,
            pod_namespace: row.pod_namespace,
            pod_uid: row.pod_uid,
            pod_ip: row.pod_ip,
            k8s_node_name: row.k8s_node_name,
            state: row.state.parse()?,
            claim_id: row.claim_id,
            ready_at: row.ready_at,
            claimed_at: row.claimed_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct ClaimedPoolSlotRow {
    pub(crate) id: Uuid,
    reserved_sandbox_id: SandboxId,
    vcpu: i32,
    memory_mib: i32,
    disk_mib: Option<i32>,
    disk_io_max_operations_per_sec: i64,
    disk_io_max_bandwidth_mib_per_sec: i64,
    pod_name: String,
    pod_namespace: String,
    pod_uid: Option<String>,
    pod_ip: Option<String>,
    k8s_node_name: Option<String>,
}

impl TryFrom<ClaimedPoolSlotRow> for ClaimedPoolSlot {
    type Error = PoolError;

    fn try_from(row: ClaimedPoolSlotRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            reserved_sandbox_id: row.reserved_sandbox_id,
            vcpu: parse_vcpu(row.vcpu)?,
            memory: parse_memory_mib(row.memory_mib)?,
            disk_mib: parse_disk_mib(row.disk_mib)?,
            disk_io_limits: parse_disk_io_limits(
                row.disk_io_max_operations_per_sec,
                row.disk_io_max_bandwidth_mib_per_sec,
            )?,
            pod_name: row.pod_name,
            pod_namespace: row.pod_namespace,
            pod_uid: row.pod_uid,
            pod_ip: row.pod_ip,
            k8s_node_name: row.k8s_node_name,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct ReservedPoolSlotRow {
    #[sqlx(flatten)]
    slot: ClaimedPoolSlotRow,
    template_release_id: Uuid,
    content_hash: String,
    state: String,
    claim_id: Option<Uuid>,
}

impl TryFrom<ReservedPoolSlotRow> for ReservedPoolSlot {
    type Error = PoolError;

    fn try_from(row: ReservedPoolSlotRow) -> Result<Self> {
        Ok(Self {
            slot: row.slot.try_into()?,
            template_release_id: row.template_release_id,
            content_hash: row.content_hash,
            state: row.state.parse()?,
            claim_id: row.claim_id,
        })
    }
}
