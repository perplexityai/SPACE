use std::path::{Path, PathBuf};

use pplx_space_node_runtime_core::cgroup_memory::read_cgroup_memory_state;
use pplx_space_sandbox_model::SandboxId;

const PROC_MEMINFO_PATH: &str = "/proc/meminfo";
const FIRECRACKER_CGROUP_BASE: &str = "/sys/fs/cgroup/space";
const KIB_PER_MIB: u64 = 1024;
const BYTES_PER_MIB: u64 = 1024 * 1024;

pub const DEFAULT_MEMORY_ADMISSION_RESERVE_MIB: u64 = 1024;
pub const DEFAULT_MEMORY_ADMISSION_FILE_FLOOR_MIB: u64 = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryAdmissionConfig {
    pub reserve_mib: u64,
    /// When set, a new sandbox must also pass the sandbox parent cgroup check.
    pub parent_cgroup: Option<ParentCgroupAdmission>,
}

/// The sandbox parent cgroup check: the parent's `anon + kernel + shmem`,
/// each running VM booked at `min(active_level_mib, memory.max)` minus its
/// own `anon + kernel + shmem`, `reserve_mib`, `file_floor_mib` and the new
/// sandbox must fit in the parent `memory.max`, the limit the kernel
/// enforces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParentCgroupAdmission {
    pub active_level_mib: u64,
    pub file_floor_mib: u64,
}

/// The limit a refused admission check counted against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryAdmissionLimit {
    Host,
    ParentCgroup,
}

impl MemoryAdmissionLimit {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::ParentCgroup => "parent_cgroup",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeMemorySnapshot {
    pub total_mib: u64,
    pub available_mib: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ActiveLeaseMemoryReservation {
    pub slot_id: u32,
    pub sandbox_id: SandboxId,
    pub generation: u64,
    pub memory_reservation_mib: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemoryAdmissionObservation {
    node_memory: NodeMemorySnapshot,
    observed_leases: Vec<ActiveLeaseMemoryReservation>,
    existing_growth_reserve_mib: u64,
    parent_cgroup: Option<ParentCgroupObservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParentCgroupObservation {
    limit_mib: u64,
    used_mib: u64,
    booked_growth_mib: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryAdmissionSources {
    meminfo_path: PathBuf,
    firecracker_cgroup_base: PathBuf,
}

impl Default for MemoryAdmissionSources {
    fn default() -> Self {
        Self {
            meminfo_path: PathBuf::from(PROC_MEMINFO_PATH),
            firecracker_cgroup_base: PathBuf::from(FIRECRACKER_CGROUP_BASE),
        }
    }
}

impl MemoryAdmissionSources {
    #[cfg(test)]
    pub(crate) fn new(meminfo_path: PathBuf, firecracker_cgroup_base: PathBuf) -> Self {
        Self {
            meminfo_path,
            firecracker_cgroup_base,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MemoryAdmissionMeasurement {
    pub(crate) node_memory_total_mib: u64,
    pub(crate) node_used_mib: u64,
    pub(crate) existing_growth_reserve_mib: u64,
    pub(crate) sandbox_cgroup_memory_mib: u64,
    pub(crate) reserve_mib: u64,
    pub(crate) required_mib: u64,
    pub(crate) parent_cgroup: Option<ParentCgroupMeasurement>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ParentCgroupMeasurement {
    pub(crate) parent_cgroup_limit_mib: u64,
    pub(crate) parent_cgroup_used_mib: u64,
    pub(crate) parent_cgroup_booked_growth_mib: u64,
    pub(crate) file_floor_mib: u64,
    pub(crate) required_mib: u64,
}

impl Default for MemoryAdmissionConfig {
    fn default() -> Self {
        Self {
            reserve_mib: DEFAULT_MEMORY_ADMISSION_RESERVE_MIB,
            parent_cgroup: None,
        }
    }
}

impl MemoryAdmissionConfig {
    #[cfg(test)]
    pub(crate) async fn evaluate_new_allocation(
        self,
        sandbox_cgroup_memory_mib: u64,
        active_leases: &[ActiveLeaseMemoryReservation],
        sources: &MemoryAdmissionSources,
    ) -> Result<MemoryAdmissionMeasurement, MemoryAdmissionEvaluationError> {
        let observation = self.observe_active_leases(active_leases, sources).await?;
        self.check_new_allocation_at_commit(sandbox_cgroup_memory_mib, &observation, active_leases)
            .map_err(Into::into)
    }

    pub(crate) async fn observe_active_leases(
        self,
        active_leases: &[ActiveLeaseMemoryReservation],
        sources: &MemoryAdmissionSources,
    ) -> Result<MemoryAdmissionObservation, NodeMemoryCapacityError> {
        // VM cgroups are read before the totals that include them, so a VM
        // that grows between the reads is counted twice rather than missed.
        let (existing_growth_reserve_mib, parent_booked_growth_mib) = self
            .measure_existing_growth_reserve_mib(active_leases, sources)
            .await;
        let memory = read_node_memory_snapshot_from(&sources.meminfo_path).await?;
        let parent_cgroup = match self.parent_cgroup {
            None => None,
            Some(_) => {
                let (limit_mib, used_mib) =
                    read_parent_cgroup_usage(&sources.firecracker_cgroup_base).await?;
                Some(ParentCgroupObservation {
                    limit_mib,
                    used_mib,
                    booked_growth_mib: parent_booked_growth_mib,
                })
            }
        };
        Ok(MemoryAdmissionObservation {
            node_memory: memory,
            observed_leases: active_leases.to_vec(),
            existing_growth_reserve_mib,
            parent_cgroup,
        })
    }

    pub(crate) fn check_new_allocation_at_commit(
        self,
        sandbox_cgroup_memory_mib: u64,
        observation: &MemoryAdmissionObservation,
        active_leases: &[ActiveLeaseMemoryReservation],
    ) -> Result<MemoryAdmissionMeasurement, MemoryAdmissionError> {
        let committed_after_observation_mib = active_leases
            .iter()
            .map(|lease| {
                let previously_reserved = observation
                    .observed_leases
                    .iter()
                    .find(|observed| {
                        observed.slot_id == lease.slot_id
                            && observed.generation == lease.generation
                            && observed.sandbox_id == lease.sandbox_id
                    })
                    .map_or(0, |observed| observed.memory_reservation_mib);
                lease
                    .memory_reservation_mib
                    .saturating_sub(previously_reserved)
            })
            .fold(0u64, u64::saturating_add);
        let existing_growth_reserve_mib = observation
            .existing_growth_reserve_mib
            .saturating_add(committed_after_observation_mib);
        let mut measurement = self.check_measured_capacity(
            sandbox_cgroup_memory_mib,
            existing_growth_reserve_mib,
            observation.node_memory,
        )?;
        if let (Some(admission), Some(parent)) = (self.parent_cgroup, observation.parent_cgroup) {
            measurement.parent_cgroup = Some(
                self.check_parent_cgroup(
                    admission,
                    sandbox_cgroup_memory_mib,
                    ParentCgroupObservation {
                        booked_growth_mib: parent
                            .booked_growth_mib
                            .saturating_add(committed_after_observation_mib),
                        ..parent
                    },
                )?,
            );
        }
        Ok(measurement)
    }

    fn check_parent_cgroup(
        self,
        admission: ParentCgroupAdmission,
        sandbox_cgroup_memory_mib: u64,
        parent: ParentCgroupObservation,
    ) -> Result<ParentCgroupMeasurement, MemoryAdmissionError> {
        // Saturation can only exceed the limit, so it refuses.
        let required_mib = [
            parent.booked_growth_mib,
            sandbox_cgroup_memory_mib,
            self.reserve_mib,
            admission.file_floor_mib,
        ]
        .into_iter()
        .fold(parent.used_mib, u64::saturating_add);
        if required_mib > parent.limit_mib {
            return Err(MemoryAdmissionError::ParentCgroupInsufficientCapacity {
                parent_cgroup_limit_mib: parent.limit_mib,
                parent_cgroup_used_mib: parent.used_mib,
                parent_cgroup_booked_growth_mib: parent.booked_growth_mib,
                sandbox_cgroup_memory_mib,
                reserve_mib: self.reserve_mib,
                file_floor_mib: admission.file_floor_mib,
                required_mib,
            });
        }
        Ok(ParentCgroupMeasurement {
            parent_cgroup_limit_mib: parent.limit_mib,
            parent_cgroup_used_mib: parent.used_mib,
            parent_cgroup_booked_growth_mib: parent.booked_growth_mib,
            file_floor_mib: admission.file_floor_mib,
            required_mib,
        })
    }

    fn check_measured_capacity(
        self,
        sandbox_cgroup_memory_mib: u64,
        existing_growth_reserve_mib: u64,
        memory: NodeMemorySnapshot,
    ) -> Result<MemoryAdmissionMeasurement, MemoryAdmissionError> {
        let node_used_mib = memory.total_mib - memory.available_mib;
        let required_mib = node_used_mib
            .checked_add(existing_growth_reserve_mib)
            .and_then(|required_mib| required_mib.checked_add(sandbox_cgroup_memory_mib))
            .and_then(|required_mib| required_mib.checked_add(self.reserve_mib))
            .ok_or(MemoryAdmissionError::RequirementOverflow {
                node_used_mib,
                existing_growth_reserve_mib,
                sandbox_cgroup_memory_mib,
                reserve_mib: self.reserve_mib,
            })?;
        let measurement = MemoryAdmissionMeasurement {
            node_memory_total_mib: memory.total_mib,
            node_used_mib,
            existing_growth_reserve_mib,
            sandbox_cgroup_memory_mib,
            reserve_mib: self.reserve_mib,
            required_mib,
            parent_cgroup: None,
        };
        if required_mib > memory.total_mib {
            return Err(MemoryAdmissionError::InsufficientCapacity {
                node_memory_total_mib: measurement.node_memory_total_mib,
                node_used_mib: measurement.node_used_mib,
                existing_growth_reserve_mib: measurement.existing_growth_reserve_mib,
                sandbox_cgroup_memory_mib: measurement.sandbox_cgroup_memory_mib,
                reserve_mib: measurement.reserve_mib,
                required_mib: measurement.required_mib,
            });
        }
        Ok(measurement)
    }

    async fn measure_existing_growth_reserve_mib(
        self,
        active_leases: &[ActiveLeaseMemoryReservation],
        sources: &MemoryAdmissionSources,
    ) -> (u64, u64) {
        let (mut host_growth_mib, mut parent_growth_mib) = (0u64, 0u64);
        for lease in active_leases {
            let cgroup_path = sources
                .firecracker_cgroup_base
                .join(lease.sandbox_id.to_string());
            let host = self
                .read_host_growth_reserve_mib(&cgroup_path)
                .await
                .unwrap_or(lease.memory_reservation_mib);
            host_growth_mib = host_growth_mib.saturating_add(host);
            if let Some(admission) = self.parent_cgroup {
                let parent = read_parent_growth_reserve_mib(admission, cgroup_path)
                    .await
                    .unwrap_or(lease.memory_reservation_mib);
                parent_growth_mib = parent_growth_mib.saturating_add(parent);
            }
        }
        (host_growth_mib, parent_growth_mib)
    }

    /// A running VM's booked growth for the host check: up to its
    /// `memory.max`, capped at `reserve_mib`.
    async fn read_host_growth_reserve_mib(self, cgroup_path: &Path) -> Option<u64> {
        let current = read_cgroup_memory_bytes(&cgroup_path.join("memory.current")).await?;
        let maximum = read_cgroup_memory_bytes(&cgroup_path.join("memory.max")).await?;
        let reserve_bytes = self.reserve_mib.checked_mul(BYTES_PER_MIB)?;
        Some(
            maximum
                .saturating_sub(current)
                .min(reserve_bytes)
                .div_ceil(BYTES_PER_MIB),
        )
    }
}

/// A running VM's booked growth for the parent cgroup check: up to
/// `min(active_level_mib, memory.max)` above its `anon + kernel + shmem`,
/// the same measure the parent check counts as used. Other file pages, such
/// as a restored VM's memory file, are left out so copy-on-write growth
/// stays booked.
async fn read_parent_growth_reserve_mib(
    admission: ParentCgroupAdmission,
    cgroup_path: PathBuf,
) -> Option<u64> {
    let memory = tokio::task::spawn_blocking(move || read_cgroup_memory_state(&cgroup_path))
        .await
        .ok()?
        .ok()?;
    let level_bytes = admission
        .active_level_mib
        .checked_mul(BYTES_PER_MIB)?
        .min(memory.max_bytes?);
    Some(
        level_bytes
            .saturating_sub(memory.unreclaimable_bytes()?)
            .div_ceil(BYTES_PER_MIB),
    )
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum MemoryAdmissionError {
    #[error(
        "node uses {node_used_mib} MiB plus {existing_growth_reserve_mib} MiB existing Firecracker growth reserve plus new sandbox demand {sandbox_cgroup_memory_mib} MiB plus reserve {reserve_mib} MiB, requiring {required_mib} MiB above total memory {node_memory_total_mib} MiB"
    )]
    InsufficientCapacity {
        node_memory_total_mib: u64,
        node_used_mib: u64,
        existing_growth_reserve_mib: u64,
        sandbox_cgroup_memory_mib: u64,
        reserve_mib: u64,
        required_mib: u64,
    },
    #[error(
        "node use {node_used_mib} MiB plus existing Firecracker growth reserve {existing_growth_reserve_mib} MiB plus new sandbox demand {sandbox_cgroup_memory_mib} MiB plus reserve {reserve_mib} MiB overflows"
    )]
    RequirementOverflow {
        node_used_mib: u64,
        existing_growth_reserve_mib: u64,
        sandbox_cgroup_memory_mib: u64,
        reserve_mib: u64,
    },
    #[error(
        "sandbox parent cgroup uses {parent_cgroup_used_mib} MiB anon, kernel and shmem plus {parent_cgroup_booked_growth_mib} MiB booked Firecracker growth plus new sandbox demand {sandbox_cgroup_memory_mib} MiB plus reserve {reserve_mib} MiB plus file floor {file_floor_mib} MiB, requiring {required_mib} MiB above its memory.max {parent_cgroup_limit_mib} MiB"
    )]
    ParentCgroupInsufficientCapacity {
        parent_cgroup_limit_mib: u64,
        parent_cgroup_used_mib: u64,
        parent_cgroup_booked_growth_mib: u64,
        sandbox_cgroup_memory_mib: u64,
        reserve_mib: u64,
        file_floor_mib: u64,
        required_mib: u64,
    },
}

impl MemoryAdmissionError {
    pub fn limit(&self) -> MemoryAdmissionLimit {
        match self {
            Self::InsufficientCapacity { .. } | Self::RequirementOverflow { .. } => {
                MemoryAdmissionLimit::Host
            }
            Self::ParentCgroupInsufficientCapacity { .. } => MemoryAdmissionLimit::ParentCgroup,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MemoryAdmissionEvaluationError {
    #[error(transparent)]
    NodeMemory(#[from] NodeMemoryCapacityError),
    #[error(transparent)]
    Admission(#[from] MemoryAdmissionError),
}

#[derive(Debug, thiserror::Error)]
pub enum NodeMemoryCapacityError {
    #[error("failed to read {PROC_MEMINFO_PATH}: {source}")]
    Read {
        #[source]
        source: std::io::Error,
    },
    #[error("{PROC_MEMINFO_PATH} does not contain {field}")]
    Missing { field: &'static str },
    #[error("invalid {field} line in {PROC_MEMINFO_PATH}: {line}")]
    Invalid { field: &'static str, line: String },
    #[error(
        "invalid {PROC_MEMINFO_PATH}: MemAvailable {available_mib} MiB exceeds MemTotal {total_mib} MiB"
    )]
    AvailableExceedsTotal { total_mib: u64, available_mib: u64 },
    #[error("failed to read sandbox parent cgroup memory: {0}")]
    ParentCgroupRead(#[source] std::io::Error),
    #[error("sandbox parent cgroup memory.stat does not report anon, kernel and shmem")]
    ParentCgroupStatMissing,
    #[error("sandbox parent cgroup has no memory.max limit")]
    ParentCgroupUnlimited,
}

/// Fails when the sandbox parent cgroup cannot support the parent cgroup
/// check: unreadable, no `memory.max` limit, or no `anon`, `kernel` and
/// `shmem` in `memory.stat`. Without this probe such a node starts and then
/// refuses every create.
pub(crate) async fn probe_parent_cgroup(path: &Path) -> Result<(), NodeMemoryCapacityError> {
    read_parent_cgroup_usage(path).await.map(drop)
}

/// The parent cgroup `memory.max` and its `anon + kernel + shmem`, in MiB.
/// Other file pages are left out because the kernel reclaims them before it
/// OOM-kills; the check keeps a file floor instead.
async fn read_parent_cgroup_usage(path: &Path) -> Result<(u64, u64), NodeMemoryCapacityError> {
    let path = path.to_path_buf();
    let memory = tokio::task::spawn_blocking(move || read_cgroup_memory_state(&path))
        .await
        .map_err(|error| NodeMemoryCapacityError::ParentCgroupRead(std::io::Error::other(error)))?
        .map_err(NodeMemoryCapacityError::ParentCgroupRead)?;
    let limit_bytes = memory
        .max_bytes
        .ok_or(NodeMemoryCapacityError::ParentCgroupUnlimited)?;
    let unreclaimable_bytes = memory
        .unreclaimable_bytes()
        .ok_or(NodeMemoryCapacityError::ParentCgroupStatMissing)?;
    Ok((
        limit_bytes / BYTES_PER_MIB,
        unreclaimable_bytes.div_ceil(BYTES_PER_MIB),
    ))
}

async fn read_node_memory_snapshot_from(
    path: &Path,
) -> Result<NodeMemorySnapshot, NodeMemoryCapacityError> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| NodeMemoryCapacityError::Read { source })?;
    parse_meminfo(&contents)
}

pub(crate) fn parse_meminfo(contents: &str) -> Result<NodeMemorySnapshot, NodeMemoryCapacityError> {
    let snapshot = NodeMemorySnapshot {
        total_mib: parse_meminfo_field(contents, "MemTotal:")?,
        available_mib: parse_meminfo_field(contents, "MemAvailable:")?,
    };
    if snapshot.available_mib > snapshot.total_mib {
        return Err(NodeMemoryCapacityError::AvailableExceedsTotal {
            total_mib: snapshot.total_mib,
            available_mib: snapshot.available_mib,
        });
    }
    Ok(snapshot)
}

fn parse_meminfo_field(
    contents: &str,
    field: &'static str,
) -> Result<u64, NodeMemoryCapacityError> {
    let line = contents
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or(NodeMemoryCapacityError::Missing { field })?;
    let mut values = line.split_whitespace();
    let key = values.next();
    let value = values.next().and_then(|value| value.parse::<u64>().ok());
    let unit = values.next();
    if key != Some(field) || unit != Some("kB") || values.next().is_some() {
        return Err(NodeMemoryCapacityError::Invalid {
            field,
            line: line.to_string(),
        });
    }
    value
        .map(|value| value / KIB_PER_MIB)
        .ok_or_else(|| NodeMemoryCapacityError::Invalid {
            field,
            line: line.to_string(),
        })
}

async fn read_cgroup_memory_bytes(path: &Path) -> Option<u64> {
    tokio::fs::read_to_string(path)
        .await
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

#[cfg(test)]
mod tests;
