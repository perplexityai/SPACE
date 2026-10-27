use super::*;
use crate::slot_pool::devices::{DeviceOwner, DeviceStoreError, HostBootId};
use pplx_space_execution_common::devices::DeviceExecution;
use pplx_space_node_mgr_client::execution_fence::ExecutionFence;

impl NodeManagerService {
    /// The caller retains sandbox launch exclusion through preparation/spawn.
    /// Wire assignments are evidence to compare, never authority to grant a lease.
    pub(super) async fn validate_devices_for_execution(
        &self,
        lease: &SlotInfo,
        fence: Option<ExecutionFence>,
        expected: Option<&DeviceExecution>,
    ) -> Result<(), Status> {
        let sandbox = lease.sandbox_id.parse::<SandboxId>().map_err(|error| {
            Status::failed_precondition(format!("invalid device owner sandbox: {error}"))
        })?;
        let fence =
            fence.ok_or_else(|| Status::failed_precondition("execution identity is missing"))?;
        let owner = DeviceOwner::new(sandbox, fence.current(), lease.generation);
        let pool = self.pool.clone();
        let expected = expected.cloned();
        let device_pool = self.device_pool.clone();
        self.off_runtime("validate device ownership", move || match &expected {
            None => pool
                .ensure_no_device_allocations(sandbox)
                .map_err(device_store_status),
            Some(expected) => {
                let boot = HostBootId::current().map_err(device_store_status)?;
                for assignment in expected.assignments() {
                    let parent = pool
                        .device_parent(assignment.parent_id())
                        .map_err(device_store_status)?;
                    device_pool.check(&parent, boot)?;
                }
                pool.validate_device_execution(owner, boot, expected)
                    .map_err(device_store_status)?;
                validate_live_device_identity(&pool, expected)
            }
        })
        .await
    }
}

#[cfg(target_os = "linux")]
fn validate_live_device_identity(
    pool: &crate::slot_pool::NetworkPool,
    expected: &DeviceExecution,
) -> Result<(), Status> {
    use pplx_space_node_runtime_core::vfio::{PciAddress, PinnedVfio};
    let pinned = PinnedVfio::inspect(expected).map_err(device_inspection_status)?;
    let profiles = expected
        .assignments()
        .iter()
        .map(|assignment| super::device_admin::compiled_profile(assignment.profile()))
        .collect::<Result<Vec<_>, _>>()?;
    pinned
        .check_profile_assignments(expected, &profiles)
        .map_err(device_inspection_status)?;
    validate_enrolled_devices(pool, expected, |locator| {
        let address =
            PciAddress::from_locator(locator.as_str()).map_err(device_inspection_status)?;
        let device = pinned
            .devices()
            .iter()
            .find(|device| device.address() == address)
            .ok_or_else(|| {
                Status::failed_precondition("enrolled device is absent from host inspection")
            })?;
        observe_device_identity(device)
    })
}

#[cfg(target_os = "linux")]
pub(super) fn observe_device_identity(
    pinned: &pplx_space_node_runtime_core::vfio::PinnedVfioDevice,
) -> Result<crate::slot_pool::devices::EnrollmentIdentity, Status> {
    let observation = pinned.observe().map_err(device_inspection_status)?;
    let [vendor, device, subsystem_vendor, subsystem_device] = observation.identifiers();
    let (sysfs_device, sysfs_inode) = observation.sysfs_identity();
    Ok(crate::slot_pool::devices::EnrollmentIdentity::LinuxPciV1 {
        locator: format!("pci:{}", pinned.address())
            .parse()
            .map_err(|error| Status::internal(format!("observed device locator: {error}")))?,
        vendor,
        device,
        subsystem_vendor,
        subsystem_device,
        topology_digest: observation.topology_digest(),
        sysfs_device,
        sysfs_inode,
    })
}

#[cfg(not(target_os = "linux"))]
fn validate_live_device_identity(
    _pool: &crate::slot_pool::NetworkPool,
    _expected: &DeviceExecution,
) -> Result<(), Status> {
    Err(Status::failed_precondition(
        "device admission requires the enrolled Linux host",
    ))
}

#[cfg(any(test, target_os = "linux"))]
fn validate_enrolled_devices(
    pool: &crate::slot_pool::NetworkPool,
    expected: &DeviceExecution,
    mut observe: impl FnMut(
        &pplx_space_execution_spec::devices::HostDeviceId,
    ) -> Result<crate::slot_pool::devices::EnrollmentIdentity, Status>,
) -> Result<(), Status> {
    for assignment in expected.assignments() {
        let parent = pool
            .device_parent(assignment.parent_id())
            .map_err(device_store_status)?;
        for enrolled in parent.enrolled_members() {
            if observe(enrolled.locator())? != *enrolled {
                return Err(Status::failed_precondition(format!(
                    "device {} changed since enrollment; parent {} requires requalification",
                    enrolled.locator(),
                    parent.id(),
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn device_store_status(error: DeviceStoreError) -> Status {
    use DeviceStoreError::*;
    let code = match &error {
        Database(rusqlite::Error::QueryReturnedNoRows) => tonic::Code::NotFound,
        AlreadyEnrolled(_) => tonic::Code::AlreadyExists,
        BootIo(_) | Lock | Database(_) => tonic::Code::Unavailable,
        CapacityBusy(_) => tonic::Code::ResourceExhausted,
        Contract(_) | Selection(_) => tonic::Code::InvalidArgument,
        BootIdentity(_) | CorruptState(_) | Evidence(_) | Ownership => tonic::Code::DataLoss,
        StaleOwner
        | CapacityRequiresOperator(_)
        | OutstandingAllocations
        | AssignmentMismatch
        | EnrollmentLimit => tonic::Code::FailedPrecondition,
    };
    Status::new(code, error.to_string())
}

/// The kernel group descriptors remain held through slot and execution retirement.
/// They are not passed to the VMM or used to reset/sanitize the hardware.
pub(super) struct DeviceCleanupExclusion {
    pool_gate: Option<tokio::sync::OwnedMutexGuard<()>>,
    changed: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(target_os = "linux")]
    _groups: Vec<pplx_space_node_runtime_core::vfio::VfioHolderExclusion>,
}

impl Drop for DeviceCleanupExclusion {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        self._groups.clear();
        self.pool_gate.take();
        self.changed.notify_one();
    }
}

impl NodeManagerService {
    /// Requires an acknowledged supervisor stop and sandbox launch exclusion.
    /// Release hands capacity to the independent pool. Only an explicitly
    /// authorized development profile queues reset-only reuse.
    pub(super) async fn cleanup_device_allocations(
        &self,
        sandbox: SandboxId,
        execution: Option<ExecutionFence>,
    ) -> Result<DeviceCleanupExclusion, Status> {
        let pool = self.pool.clone();
        let device_pool = self.device_pool.clone();
        let pool_gate = device_pool.gate.clone().lock_owned().await;
        self.off_runtime("release device ownership", move || {
            let allocations = pool
                .device_cleanup_allocations(sandbox, execution.map(|fence| fence.current()))
                .map_err(device_store_status)?;
            #[cfg(target_os = "linux")]
            {
                use pplx_space_node_runtime_core::vfio::PinnedVfio;
                let groups = if allocations.is_empty() {
                    Vec::new()
                } else {
                    let boot = HostBootId::current().map_err(device_store_status)?;
                    release_allocations(
                        &pool,
                        allocations,
                        boot,
                        |parent| {
                            if device_pool.check(parent, boot).is_ok() {
                                crate::slot_pool::devices::DeviceReleaseDisposition::QueueReset
                            } else {
                                crate::slot_pool::devices::DeviceReleaseDisposition::Quarantine
                            }
                        },
                        |devices| {
                            PinnedVfio::inspect(devices)
                                .map_err(device_inspection_status)?
                                .exclude_holders()
                                .map_err(device_inspection_status)
                        },
                    )?
                };
                pool.ensure_no_device_allocations(sandbox)
                    .map_err(device_store_status)?;
                Ok(DeviceCleanupExclusion {
                    _groups: groups,
                    pool_gate: Some(pool_gate),
                    changed: device_pool.changed.clone(),
                })
            }
            #[cfg(not(target_os = "linux"))]
            {
                if !allocations.is_empty() {
                    return Err(Status::failed_precondition(
                        "device cleanup requires the owning Linux host",
                    ));
                }
                Ok(DeviceCleanupExclusion {
                    pool_gate: Some(pool_gate),
                    changed: device_pool.changed.clone(),
                })
            }
        })
        .await
    }
}

#[cfg(target_os = "linux")]
pub(super) fn device_inspection_status(
    error: pplx_space_node_runtime_core::vfio::VfioError,
) -> Status {
    use pplx_space_node_runtime_core::vfio::VfioError;
    let code = match &error {
        VfioError::Io { source, .. }
            if source.raw_os_error() == Some(libc::EBUSY)
                || pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(
                    source,
                ) =>
        {
            tonic::Code::Unavailable
        }
        VfioError::Io { .. }
        | VfioError::Profile(_)
        | VfioError::Address(_)
        | VfioError::Invalid(_)
        | VfioError::Protection(_) => tonic::Code::FailedPrecondition,
    };
    Status::new(
        code,
        format!("device inspection retains ownership: {error}"),
    )
}

#[cfg(any(test, target_os = "linux"))]
fn release_allocations<Guard>(
    pool: &crate::slot_pool::NetworkPool,
    allocations: Vec<crate::slot_pool::devices::DeviceAllocation>,
    boot: HostBootId,
    disposition: impl Fn(
        &crate::slot_pool::devices::DeviceParent,
    ) -> crate::slot_pool::devices::DeviceReleaseDisposition,
    mut exclude: impl FnMut(&DeviceExecution) -> Result<Guard, Status>,
) -> Result<Vec<Guard>, Status> {
    use crate::slot_pool::devices::{AllocationState, DeviceHolder};
    let mut guards = Vec::new();
    for mut allocation in allocations {
        if allocation.state() != AllocationState::Releasing {
            if !pool
                .begin_device_release(&allocation)
                .map_err(device_store_status)?
            {
                return Err(device_store_status(DeviceStoreError::StaleOwner));
            }
            allocation = pool
                .device_allocation(allocation.assignment().lease_id())
                .map_err(device_store_status)?;
        }
        let DeviceHolder::Sandbox {
            requirements_digest,
            ..
        } = allocation.holder()
        else {
            return Err(device_store_status(DeviceStoreError::Ownership));
        };
        let devices =
            DeviceExecution::new(requirements_digest, vec![allocation.assignment().clone()])
                .map_err(|error| Status::data_loss(format!("stored device assignment: {error}")))?;
        let guard = exclude(&devices)?;
        let parent = pool
            .device_parent(allocation.assignment().parent_id())
            .map_err(device_store_status)?;
        pool.finish_device_release(&allocation, parent.revision(), boot, disposition(&parent))
            .map_err(device_store_status)?;
        guards.push(guard);
    }
    Ok(guards)
}

#[cfg(test)]
mod tests;

/// Local policy resolution already chose the environment. Device qualification
/// is checked against that same compiled definition, never a second selection.
pub(super) fn device_environment_resources(
    reference: Option<pplx_space_common_core::types::EnvironmentReference>,
    devices: Option<&DeviceExecution>,
) -> Result<Option<pplx_space_node_runtime_core::catalog::DeviceResourceLimits>, Status> {
    let Some(devices) = devices else {
        return Ok(None);
    };
    let reference = reference.ok_or_else(|| {
        Status::failed_precondition("device execution requires a compiled environment")
    })?;
    let arch = pplx_space_node_runtime_core::runtime_inventory::host_arch()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let catalog =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::cached_for_architecture(arch)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let environment = catalog
        .iter()
        .find(|environment| environment.definition().reference() == reference)
        .ok_or_else(|| Status::failed_precondition("unknown device execution environment"))?;
    environment
        .exclusive_device_resources(devices)
        .map(Some)
        .map_err(|error| Status::failed_precondition(error.to_string()))
}

impl NodeManagerService {
    /// Launch exclusion spans this intent, supervisor mutation and confirmation.
    pub(super) async fn transition_device_launch(
        &self,
        lease: &SlotInfo,
        fence: Option<ExecutionFence>,
        expected: &DeviceExecution,
        confirm: bool,
    ) -> Result<(), Status> {
        use crate::slot_pool::devices::AllocationState;
        let sandbox = lease
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::data_loss(error.to_string()))?;
        let fence =
            fence.ok_or_else(|| Status::failed_precondition("device execution is missing"))?;
        let owner = DeviceOwner::new(sandbox, fence.current(), lease.generation);
        let expected = expected.clone();
        let pool = self.pool.clone();
        self.off_runtime("transition device attachment", move || {
            let boot = HostBootId::current().map_err(device_store_status)?;
            pool.validate_device_execution(owner, boot, &expected)
                .map_err(device_store_status)?;
            if !confirm {
                validate_live_device_identity(&pool, &expected)?;
            }
            for assignment in expected.assignments() {
                let allocation = pool
                    .device_allocation(assignment.lease_id())
                    .map_err(device_store_status)?;
                let changed = match (confirm, allocation.state()) {
                    (false, AllocationState::Reserved) => {
                        pool.begin_device_attachment(&allocation, boot)
                    }
                    (true, AllocationState::Attaching) => {
                        pool.confirm_device_attachment(&allocation, boot)
                    }
                    (_, AllocationState::Attached) | (false, AllocationState::Attaching) => {
                        Ok(true)
                    }
                    _ => Ok(false),
                }
                .map_err(device_store_status)?;
                if !changed {
                    return Err(device_store_status(DeviceStoreError::StaleOwner));
                }
            }
            Ok(())
        })
        .await
    }
}

impl NodeManagerService {
    pub(super) async fn require_supervisor_devices(&self) -> Result<(), Status> {
        let capabilities = self
            .runtime_supervisor
            .get_execution_capabilities(
                pplx_space_node_runtime_core::proto::GetExecutionCapabilitiesRequest {},
            )
            .await?
            .into_inner();
        if !capabilities.exclusive_devices_v1 {
            return Err(Status::failed_precondition(
                "runtime supervisor does not support exclusive device assignments",
            ));
        }
        Ok(())
    }

    pub(super) fn device_allocation_preferences(
        &self,
        requirements: &pplx_space_execution_spec::devices::DeviceRequirements,
    ) -> Result<
        std::collections::BTreeMap<pplx_space_execution_spec::devices::DeviceProfile, u32>,
        Status,
    > {
        let architecture = pplx_space_node_runtime_core::runtime_inventory::host_arch()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let catalog =
            pplx_space_node_runtime_core::catalog::CompiledEnvironment::cached_for_architecture(
                architecture,
            )
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let policy = self.execution_policy();
        let mut preferences = std::collections::BTreeMap::new();
        for clause in requirements.entries() {
            let mut supported = false;
            for profile in catalog
                .iter()
                .filter(|entry| policy.allows(entry.definition().reference()))
                .flat_map(|entry| entry.exclusive_device_profiles())
            {
                if clause.profiles().contains(profile.profile())
                    && profile.accepts_allocations(clause.count())
                {
                    // Equal compiled priority refuses ambiguous alternatives;
                    // request order never selects a different hardware class.
                    preferences.insert(profile.profile().clone(), 0);
                    supported = true;
                }
            }
            if !supported {
                return Err(Status::failed_precondition(
                    "no requested device profile/count is qualified by an allowed environment",
                ));
            }
        }
        Ok(preferences)
    }
}

#[cfg(test)]
fn quarantine_allocations<Guard>(
    pool: &crate::slot_pool::NetworkPool,
    allocations: Vec<crate::slot_pool::devices::DeviceAllocation>,
    boot: HostBootId,
    exclude: impl FnMut(&DeviceExecution) -> Result<Guard, Status>,
) -> Result<Vec<Guard>, Status> {
    release_allocations(
        pool,
        allocations,
        boot,
        |_| crate::slot_pool::devices::DeviceReleaseDisposition::Quarantine,
        exclude,
    )
}
