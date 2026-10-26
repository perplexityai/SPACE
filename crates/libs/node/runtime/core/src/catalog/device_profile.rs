//! Device qualification belongs to the compiled environment. A hardware profile
//! name on disk or in a request cannot authorize a different boot/driver bundle.
use pplx_space_common_core::types::Digest;
use pplx_space_execution_common::devices::DeviceExecution;
use pplx_space_execution_spec::devices::{DeviceProfile, DeviceRequirements};

/// Exact PCI function set for a dedicated allocation. Locators are deliberately
/// absent: model/board identity and guest-visible count belong to the profile,
/// while the host checks reset topology and continuity at the enrolled location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DedicatedPciHardware {
    functions: Vec<[u16; 4]>,
    guest_visible_devices: u32,
}
impl DedicatedPciHardware {
    /// Each tuple is vendor, device, subsystem vendor, subsystem device.
    pub fn new(
        mut functions: Vec<[u16; 4]>,
        guest_visible_devices: u32,
    ) -> Result<Self, DeviceProfileError> {
        if functions.is_empty()
            || functions.len() > 32
            || guest_visible_devices == 0
            || guest_visible_devices as usize > functions.len()
        {
            return Err(DeviceProfileError::InvalidHardware);
        }
        functions.sort_unstable();
        Ok(Self {
            functions,
            guest_visible_devices,
        })
    }
    pub fn guest_visible_devices(&self) -> u32 {
        self.guest_visible_devices
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExclusiveDeviceProfile {
    profile: DeviceProfile,
    max_allocations: u32,
    minimum_guest_memory_mib: u64,
    overhead_mib_per_allocation: u64,
    procedure: Digest,
    hardware: DedicatedPciHardware,
}

#[derive(Debug, thiserror::Error)]
pub enum DeviceProfileError {
    #[error("invalid compiled dedicated PCI function set or guest-visible device count")]
    InvalidHardware,
    #[error("device hardware or member count does not match profile {0}")]
    HardwareMismatch(DeviceProfile),
    #[error("device profile {0} is not qualified for this execution environment")]
    Unqualified(DeviceProfile),
    #[error("device profile {profile} exceeds its qualified allocation count {limit}")]
    AllocationCount { profile: DeviceProfile, limit: u32 },
    #[error("no profile in the device requirement permits {count} allocations")]
    ClauseCount { count: u32 },
    #[error("invalid compiled device memory or allocation limits")]
    InvalidLimits,
    #[error("device guest requires at least {minimum_mib} MiB, requested {requested_mib} MiB")]
    GuestMemory {
        minimum_mib: u64,
        requested_mib: u64,
    },
    #[error(
        "device execution requires {required_mib} MiB reserved host memory, has {reserved_mib} MiB"
    )]
    HostMemory {
        required_mib: u64,
        reserved_mib: u64,
    },
    #[error("device memory accounting overflow")]
    Overflow,
}

impl ExclusiveDeviceProfile {
    pub fn new(
        profile: DeviceProfile,
        max_allocations: u32,
        minimum_guest_memory_mib: u64,
        overhead_mib_per_allocation: u64,
        procedure: Digest,
        hardware: DedicatedPciHardware,
    ) -> Result<Self, DeviceProfileError> {
        if !(1..=16).contains(&max_allocations)
            || minimum_guest_memory_mib == 0
            || overhead_mib_per_allocation == 0
        {
            return Err(DeviceProfileError::InvalidLimits);
        }
        minimum_guest_memory_mib
            .checked_add(
                overhead_mib_per_allocation
                    .checked_mul(u64::from(max_allocations))
                    .ok_or(DeviceProfileError::Overflow)?,
            )
            .and_then(|mib| mib.checked_mul(1024 * 1024))
            .ok_or(DeviceProfileError::Overflow)?;
        Ok(Self {
            profile,
            max_allocations,
            minimum_guest_memory_mib,
            overhead_mib_per_allocation,
            procedure,
            hardware,
        })
    }
    pub fn profile(&self) -> &DeviceProfile {
        &self.profile
    }
    pub fn qualification_procedure(&self) -> Digest {
        self.procedure
    }
    pub fn hardware(&self) -> &DedicatedPciHardware {
        &self.hardware
    }
    pub fn check_pci_hardware(&self, observed: &[[u16; 4]]) -> Result<(), DeviceProfileError> {
        if observed.len() != self.hardware.functions.len() {
            return Err(DeviceProfileError::HardwareMismatch(self.profile.clone()));
        }
        let mut observed = observed.to_vec();
        observed.sort_unstable();
        if observed != self.hardware.functions {
            return Err(DeviceProfileError::HardwareMismatch(self.profile.clone()));
        }
        Ok(())
    }
    pub fn accepts_allocations(&self, count: u32) -> bool {
        (1..=self.max_allocations).contains(&count)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DeviceResourceLimits {
    minimum_guest_memory_mib: u64,
    overhead_mib: u64,
}

impl DeviceResourceLimits {
    #[cfg(test)]
    pub(crate) fn for_test(minimum_guest_memory_mib: u64, overhead_mib: u64) -> Self {
        Self {
            minimum_guest_memory_mib,
            overhead_mib,
        }
    }
    /// The caller reserves all guest memory plus the qualified device overhead;
    /// ballooning or a growth-based guest-memory discount cannot satisfy this.
    pub fn check_guest_memory(
        &self,
        guest_mib: u64,
        reserved_mib: u64,
    ) -> Result<(), DeviceProfileError> {
        let required_mib = self.required_host_memory_mib(guest_mib)?;
        if reserved_mib < required_mib {
            return Err(DeviceProfileError::HostMemory {
                required_mib,
                reserved_mib,
            });
        }
        Ok(())
    }

    pub fn required_host_memory_mib(&self, guest_mib: u64) -> Result<u64, DeviceProfileError> {
        if guest_mib < self.minimum_guest_memory_mib {
            return Err(DeviceProfileError::GuestMemory {
                minimum_mib: self.minimum_guest_memory_mib,
                requested_mib: guest_mib,
            });
        }
        guest_mib
            .checked_add(self.overhead_mib)
            .ok_or(DeviceProfileError::Overflow)
    }

    /// The launcher may lock no more than the finite host budget reserved by
    /// node-manager. Guest-size validation is additionally required at boot.
    pub fn locked_memory_bytes(&self, reserved_mib: u64) -> Result<u64, DeviceProfileError> {
        self.check_guest_memory(self.minimum_guest_memory_mib, reserved_mib)?;
        reserved_mib
            .checked_mul(1024 * 1024)
            .ok_or(DeviceProfileError::Overflow)
    }
}

/// Reserve before hardware selection, covering the most expensive alternative
/// in each clause. Every alternative must belong to the chosen environment;
/// allocation may not select a profile requiring different boot inputs.
pub(super) fn requirement_resources(
    profiles: &[ExclusiveDeviceProfile],
    requirements: &DeviceRequirements,
) -> Result<DeviceResourceLimits, DeviceProfileError> {
    let mut limits = DeviceResourceLimits {
        minimum_guest_memory_mib: 0,
        overhead_mib: 0,
    };
    for clause in requirements.entries() {
        let mut clause_overhead = None;
        for requested in clause.profiles() {
            let profile = profiles
                .iter()
                .find(|profile| &profile.profile == requested)
                .ok_or_else(|| DeviceProfileError::Unqualified(requested.clone()))?;
            if !profile.accepts_allocations(clause.count()) {
                continue;
            }
            limits.minimum_guest_memory_mib = limits
                .minimum_guest_memory_mib
                .max(profile.minimum_guest_memory_mib);
            let overhead = profile
                .overhead_mib_per_allocation
                .checked_mul(u64::from(clause.count()))
                .ok_or(DeviceProfileError::Overflow)?;
            clause_overhead =
                Some(clause_overhead.map_or(overhead, |previous: u64| previous.max(overhead)));
        }
        limits.overhead_mib = limits
            .overhead_mib
            .checked_add(clause_overhead.ok_or(DeviceProfileError::ClauseCount {
                count: clause.count(),
            })?)
            .ok_or(DeviceProfileError::Overflow)?;
    }
    Ok(limits)
}

pub(super) fn resources(
    profiles: &[ExclusiveDeviceProfile],
    execution: &DeviceExecution,
) -> Result<DeviceResourceLimits, DeviceProfileError> {
    let mut counts = std::collections::BTreeMap::new();
    let mut limits = DeviceResourceLimits {
        minimum_guest_memory_mib: 0,
        overhead_mib: 0,
    };
    for assignment in execution.assignments() {
        let profile = profiles
            .iter()
            .find(|p| &p.profile == assignment.profile())
            .ok_or_else(|| DeviceProfileError::Unqualified(assignment.profile().clone()))?;
        if assignment.host_device_ids().len() != profile.hardware.functions.len() {
            return Err(DeviceProfileError::HardwareMismatch(
                profile.profile.clone(),
            ));
        }
        let count = counts.entry(&profile.profile).or_insert(0_u32);
        *count += 1;
        if *count > profile.max_allocations {
            return Err(DeviceProfileError::AllocationCount {
                profile: profile.profile.clone(),
                limit: profile.max_allocations,
            });
        }
        limits.minimum_guest_memory_mib = limits
            .minimum_guest_memory_mib
            .max(profile.minimum_guest_memory_mib);
        limits.overhead_mib = limits
            .overhead_mib
            .checked_add(profile.overhead_mib_per_allocation)
            .ok_or(DeviceProfileError::Overflow)?;
    }
    Ok(limits)
}

#[cfg(test)]
mod tests;

/// This procedure accepts reset-only residual risk for an isolated development
/// run. It is not a sanitization qualification and is absent from normal builds.
#[cfg(feature = "gpu-development")]
pub fn development_t4_profile() -> Result<ExclusiveDeviceProfile, super::CatalogError> {
    use sha2::{Digest as _, Sha256};
    let invalid = |_| super::CatalogError::Compiled {
        entry: "development reset-only device".into(),
        field: "profile",
        value: "t4-whole-development",
    };
    let procedure = Digest::sha256(Sha256::digest(
        b"space.development.t4.reset-only.v1;single-pci-bus;host-boot-fenced;no-scrub;source-signed-guest"
    ).into());
    ExclusiveDeviceProfile::new(
        "t4-whole-development".parse().map_err(|_| invalid(()))?,
        1,
        4096,
        1024,
        procedure,
        DedicatedPciHardware::new(vec![[0x10de, 0x1eb8, 0x10de, 0x12a2]], 1)
            .map_err(|_| invalid(()))?,
    )
    .map_err(|_| invalid(()))
}
