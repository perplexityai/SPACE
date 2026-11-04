use std::path::{Path, PathBuf};

use pplx_space_common_core::types::SandboxId;

const PROC_MEMINFO_PATH: &str = "/proc/meminfo";
const FIRECRACKER_CGROUP_BASE: &str = "/sys/fs/cgroup/space";
const KIB_PER_MIB: u64 = 1024;
const BYTES_PER_MIB: u64 = 1024 * 1024;

pub const DEFAULT_MEMORY_ADMISSION_RESERVE_MIB: u64 = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryAdmissionConfig {
    pub reserve_mib: u64,
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
}

impl Default for MemoryAdmissionConfig {
    fn default() -> Self {
        Self {
            reserve_mib: DEFAULT_MEMORY_ADMISSION_RESERVE_MIB,
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
        let memory = read_node_memory_snapshot_from(&sources.meminfo_path).await?;
        let existing_growth_reserve_mib = self
            .measure_existing_growth_reserve_mib(active_leases, sources)
            .await;
        Ok(MemoryAdmissionObservation {
            node_memory: memory,
            observed_leases: active_leases.to_vec(),
            existing_growth_reserve_mib,
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
        self.check_measured_capacity(
            sandbox_cgroup_memory_mib,
            existing_growth_reserve_mib,
            observation.node_memory,
        )
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
    ) -> u64 {
        let mut growth_reserve_mib = 0u64;
        for lease in active_leases {
            let lease_growth_mib = self
                .read_firecracker_growth_reserve_mib(lease, sources)
                .await
                .unwrap_or(lease.memory_reservation_mib);
            growth_reserve_mib = growth_reserve_mib.saturating_add(lease_growth_mib);
        }
        growth_reserve_mib
    }

    async fn read_firecracker_growth_reserve_mib(
        self,
        lease: &ActiveLeaseMemoryReservation,
        sources: &MemoryAdmissionSources,
    ) -> Option<u64> {
        let cgroup_path = sources
            .firecracker_cgroup_base
            .join(lease.sandbox_id.to_string());
        let current = read_cgroup_memory_bytes(&cgroup_path.join("memory.current")).await?;
        let maximum = read_cgroup_memory_bytes(&cgroup_path.join("memory.max")).await?;
        let reserve_bytes = self.reserve_mib.checked_mul(BYTES_PER_MIB)?;
        let remaining_bytes = maximum.saturating_sub(current);
        Some(remaining_bytes.min(reserve_bytes).div_ceil(BYTES_PER_MIB))
    }
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
