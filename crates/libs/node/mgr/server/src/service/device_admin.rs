//! Operator enrollment observes hardware; only a qualified host procedure may
//! make it ready. Sandbox callers never supply host locations or clean evidence.
use super::device_admission::device_store_status;
use super::*;
use crate::proto::*;
use crate::slot_pool::devices::{DeviceParent, ParentState};
use pplx_space_execution_spec::devices::{DeviceParentId, DeviceProfile, HostDeviceId};
use pplx_space_node_runtime_core::catalog::{CompiledEnvironment, ExclusiveDeviceProfile};

fn parent_record(parent: &DeviceParent) -> DeviceParentRecord {
    DeviceParentRecord {
        parent_id: parent.id().to_string(),
        profile: parent.profile().to_string(),
        host_device_ids: parent.members().iter().map(ToString::to_string).collect(),
        state: match parent.state() {
            ParentState::Quarantined => DeviceParentState::Quarantined,
            ParentState::Ready => DeviceParentState::Ready,
            ParentState::Sanitizing => DeviceParentState::Sanitizing,
            ParentState::Retired => DeviceParentState::Retired,
        } as i32,
        revision: parent.revision(),
        qualification_digest: parent.qualification_procedure().to_string(),
        usage: None,
    }
}

fn allocation_record(
    allocation: &crate::slot_pool::devices::DeviceAllocation,
) -> Result<DeviceAllocationRecord, Status> {
    use crate::proto::device_allocation_record::Owner;
    use crate::slot_pool::devices::{AllocationState, DeviceHolder};
    let state = match allocation.state() {
        AllocationState::Reserved => DeviceAllocationState::Reserved,
        AllocationState::Attaching => DeviceAllocationState::Attaching,
        AllocationState::Attached => DeviceAllocationState::Attached,
        AllocationState::Releasing => DeviceAllocationState::Releasing,
        AllocationState::Quarantined => DeviceAllocationState::Quarantined,
        AllocationState::Released => {
            return Err(Status::data_loss(
                "released device allocation in active snapshot",
            ));
        }
    };
    let (execution, owner) = match allocation.holder() {
        DeviceHolder::Sandbox { owner, .. } => (
            owner.execution_id(),
            Owner::SandboxId(owner.sandbox_id().to_string()),
        ),
        DeviceHolder::Sanitizer(attempt) => (
            attempt.execution_id(),
            Owner::SanitizerProcedureDigest(attempt.procedure().to_string()),
        ),
    };
    Ok(DeviceAllocationRecord {
        assignment: Some(allocation.assignment().into()),
        state: state as i32,
        execution_id: execution.to_string(),
        owner: Some(owner),
    })
}

pub(super) fn compiled_profile(profile: &DeviceProfile) -> Result<ExclusiveDeviceProfile, Status> {
    let arch = pplx_space_node_runtime_core::runtime_inventory::host_arch()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let catalog = CompiledEnvironment::cached_for_architecture(arch)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let mut found = None;
    for candidate in catalog.iter().flat_map(|e| e.exclusive_device_profiles()) {
        if candidate.profile() == profile {
            if found.as_ref().is_some_and(|previous| previous != candidate) {
                return Err(Status::failed_precondition(
                    "compiled device profile has conflicting qualifications",
                ));
            }
            found = Some(candidate.clone());
        }
    }
    found.ok_or_else(|| {
        Status::failed_precondition(format!("unknown compiled device profile {profile}"))
    })
}

fn members(raw: Vec<String>) -> Result<Vec<HostDeviceId>, Status> {
    if raw.is_empty() || raw.len() > 32 {
        return Err(Status::invalid_argument(
            "enrollment requires 1–32 host device IDs",
        ));
    }
    let mut parsed = raw
        .into_iter()
        .map(HostDeviceId::parse)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    parsed.sort();
    if parsed
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left == right))
    {
        return Err(Status::invalid_argument("duplicate enrollment device ID"));
    }
    Ok(parsed)
}

impl NodeManagerService {
    pub(super) async fn list_device_parents(
        &self,
        request: Request<ListDeviceParentsRequest>,
    ) -> Result<Response<ListDeviceParentsResponse>, Status> {
        self.authorize_peer(&request, "ListDeviceParents", ALLOW_NODE_ADMIN)?;
        let service = self.clone();
        self.off_runtime("list device parents", move || {
            use crate::slot_pool::devices::HostBootId;
            use pplx_space_execution_spec::devices::{DeviceRequirement, DeviceRequirements};
            let boot = HostBootId::current().map_err(device_store_status)?;
            let snapshot = service
                .pool
                .device_capacity_snapshot()
                .map_err(device_store_status)?;
            let parents = snapshot
                .into_iter()
                .map(|entry| {
                    let parent = entry.parent();
                    let required = DeviceRequirements::new(vec![
                        DeviceRequirement::new(parent.profile().clone(), 1)
                            .map_err(|e| Status::data_loss(e.to_string()))?,
                    ])
                    .map_err(|e| Status::data_loss(e.to_string()))?;
                    // Reuse admission's compiled-policy resolution and host-scoped
                    // authorization. Request-specific fit and memory remain separate.
                    let enabled = service.device_allocation_preferences(&required).is_ok()
                        && service.device_pool.check(parent, boot).is_ok();
                    let mut record = parent_record(parent);
                    record.usage = Some(DeviceParentUsage {
                        allocatable: entry.allocatable(boot, enabled),
                        allocations: entry
                            .allocations()
                            .iter()
                            .map(allocation_record)
                            .collect::<Result<_, _>>()?,
                    });
                    Ok(record)
                })
                .collect::<Result<_, Status>>()?;
            Ok(Response::new(ListDeviceParentsResponse { parents }))
        })
        .await
    }

    pub(super) async fn enroll_device_parent(
        &self,
        request: Request<EnrollDeviceParentRequest>,
    ) -> Result<Response<EnrollDeviceParentResponse>, Status> {
        self.authorize_peer(&request, "EnrollDeviceParent", ALLOW_NODE_ADMIN)?;
        let request = request.into_inner();
        let profile = request
            .profile
            .parse()
            .map_err(|error| Status::invalid_argument(format!("device profile: {error}")))?;
        let profile = compiled_profile(&profile)?;
        let members = members(request.host_device_ids)?;
        #[cfg(target_os = "linux")]
        {
            use super::device_admission::{device_inspection_status, observe_device_identity};
            use crate::slot_pool::devices::HostBootId;
            use pplx_space_node_runtime_core::vfio::PinnedVfio;
            let pool = self.pool.clone();
            let device_pool = self.device_pool.clone();
            self.off_runtime("enroll device parent", move || {
                let boot = HostBootId::current().map_err(device_store_status)?;
                device_pool.check_members(&members, boot)?;
                let pinned =
                    PinnedVfio::inspect_parent(&members).map_err(device_inspection_status)?;
                let _exclusion = pinned.exclude_holders().map_err(device_inspection_status)?;
                let observations = pinned
                    .devices()
                    .iter()
                    .map(|device| {
                        let evidence = observe_device_identity(device)?;
                        Ok((evidence.locator().clone(), evidence))
                    })
                    .collect::<Result<Vec<_>, Status>>()?;
                profile
                    .check_pci_hardware(
                        &observations
                            .iter()
                            .map(|(_, identity)| identity.pci_identifiers())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                let id = pool
                    .enroll_device_parent(
                        profile.profile(),
                        boot,
                        profile.qualification_procedure(),
                        &observations,
                    )
                    .map_err(device_store_status)?;
                let parent = pool.device_parent(id).map_err(device_store_status)?;
                Ok(Response::new(EnrollDeviceParentResponse {
                    parent: Some(parent_record(&parent)),
                }))
            })
            .await
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (profile, members);
            Err(Status::failed_precondition(
                "device enrollment requires a qualified Linux host adapter",
            ))
        }
    }

    pub(super) async fn retire_device_parent(
        &self,
        request: Request<RetireDeviceParentRequest>,
    ) -> Result<Response<RetireDeviceParentResponse>, Status> {
        self.authorize_peer(&request, "RetireDeviceParent", ALLOW_NODE_ADMIN)?;
        let request = request.into_inner();
        let id: DeviceParentId = request
            .parent_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("device parent: {error}")))?;
        #[cfg(target_os = "linux")]
        {
            use super::device_admission::device_inspection_status;
            use pplx_space_node_runtime_core::vfio::PinnedVfio;
            let pool = self.pool.clone();
            self.off_runtime("retire device parent", move || {
                let parent = pool.device_parent(id).map_err(device_store_status)?;
                if parent.revision() != request.expected_revision {
                    return Err(Status::aborted("device parent revision changed"));
                }
                let pinned = PinnedVfio::inspect_parent(parent.members())
                    .map_err(device_inspection_status)?;
                let _exclusion = pinned.exclude_holders().map_err(device_inspection_status)?;
                if !pool
                    .retire_device_parent(id, request.expected_revision)
                    .map_err(device_store_status)?
                {
                    return Err(Status::aborted(
                        "device parent changed or has unresolved ownership",
                    ));
                }
                Ok(Response::new(RetireDeviceParentResponse {
                    parent_id: id.to_string(),
                    revision: pool
                        .device_parent(id)
                        .map_err(device_store_status)?
                        .revision(),
                }))
            })
            .await
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = id;
            Err(Status::failed_precondition(
                "device retirement requires the owning Linux host adapter",
            ))
        }
    }

    pub(super) async fn requalify_device_parent(
        &self,
        request: Request<RequalifyDeviceParentRequest>,
    ) -> Result<Response<RequalifyDeviceParentResponse>, Status> {
        self.authorize_peer(&request, "RequalifyDeviceParent", ALLOW_NODE_ADMIN)?;
        let request = request.into_inner();
        let id: DeviceParentId = request
            .parent_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("device parent: {error}")))?;
        let procedure: pplx_space_common_core::types::Digest = request
            .qualification_digest
            .parse()
            .map_err(|error| Status::invalid_argument(format!("qualification digest: {error}")))?;
        let pool = self.pool.clone();
        let device_pool = self.device_pool.clone();
        self.off_runtime("check device qualification", move || {
            let parent = pool.device_parent(id).map_err(device_store_status)?;
            if parent.revision() != request.expected_revision
                || parent.state() != ParentState::Quarantined
            {
                return Err(Status::aborted(
                    "qualification requires the observed quarantined parent revision",
                ));
            }
            let profile = compiled_profile(parent.profile())?;
            if procedure != parent.qualification_procedure()
                || procedure != profile.qualification_procedure()
            {
                return Err(Status::failed_precondition(
                    "qualification procedure does not match the enrolled profile",
                ));
            }
            let boot =
                crate::slot_pool::devices::HostBootId::current().map_err(device_store_status)?;
            device_pool.check(&parent, boot)?;
            pool.queue_device_reset(id, request.expected_revision, boot)
                .map_err(device_store_status)?;
            device_pool.changed.notify_one();
            Ok(Response::new(RequalifyDeviceParentResponse {
                parent_id: id.to_string(),
                revision: pool
                    .device_parent(id)
                    .map_err(device_store_status)?
                    .revision(),
            }))
        })
        .await
    }
}

#[cfg(test)]
mod tests;
