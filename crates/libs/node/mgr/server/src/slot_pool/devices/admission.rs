//! Admission validates the complete assignment set, including its absence.
//! A caller cannot drop one lease, change its members, or replay an old owner.
use super::*;
use pplx_space_execution_common::devices::DeviceExecution;

fn sandbox_allocations(
    db: &rusqlite::Connection,
    sandbox: SandboxId,
) -> Result<Vec<DeviceAllocation>, DeviceStoreError> {
    let leases = db
        .prepare(sql!("SELECT lease_id FROM device_allocations WHERE sandbox_id=?1 AND state!=?2 ORDER BY lease_id LIMIT 17"))?
        .query_map(params![sandbox.to_string(), AllocationState::Released.as_str()], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if leases.len() > 16 {
        return Err(DeviceStoreError::Ownership);
    }
    leases
        .into_iter()
        .map(|lease| read_allocation(db, lease.parse()?))
        .collect()
}

/// Checks the complete set under one database transaction before preparation.
/// Launch exclusion must remain held through use of the returned assignments.
impl NetworkPool {
    pub fn validate_device_execution(
        &self,
        owner: DeviceOwner,
        boot: HostBootId,
        expected: &DeviceExecution,
    ) -> Result<(), DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction()?;
        validate_execution(&tx, owner, boot, expected)?;
        tx.commit()?;
        Ok(())
    }

    /// Cleanup is also valid after losing the network slot, but never for a
    /// different execution. The service holds launch exclusion until release.
    pub fn device_cleanup_allocations(
        &self,
        sandbox: SandboxId,
        execution: Option<SandboxActivationAttemptId>,
    ) -> Result<Vec<DeviceAllocation>, DeviceStoreError> {
        let db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let allocations = sandbox_allocations(&db, sandbox)?;
        for allocation in &allocations {
            match allocation.holder {
                DeviceHolder::Sandbox { owner, .. } if Some(owner.execution_id) == execution => {}
                _ => return Err(DeviceStoreError::StaleOwner),
            }
        }
        Ok(allocations)
    }

    /// CPU-only admission must not erase a device-bearing execution by omission.
    pub fn ensure_no_device_allocations(&self, sandbox: SandboxId) -> Result<(), DeviceStoreError> {
        let db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        ensure_no_holders(&db, &sandbox.to_string())
    }
}

pub(in crate::slot_pool) fn ensure_no_holders(
    db: &rusqlite::Connection,
    sandbox: &str,
) -> Result<(), DeviceStoreError> {
    let held: bool = db.query_row(
        sql!("SELECT EXISTS(SELECT 1 FROM device_allocations WHERE sandbox_id=?1 AND state!=?2)"),
        params![sandbox, AllocationState::Released.as_str()],
        |row| row.get(0),
    )?;
    if held {
        return Err(DeviceStoreError::OutstandingAllocations);
    }
    Ok(())
}

fn validate_execution(
    db: &rusqlite::Connection,
    owner: DeviceOwner,
    boot: HostBootId,
    expected: &DeviceExecution,
) -> Result<Vec<DeviceAllocation>, DeviceStoreError> {
    check_owner(db, owner)?;
    let allocations = sandbox_allocations(db, owner.sandbox_id)?;
    if allocations.len() != expected.assignments().len() {
        return Err(DeviceStoreError::AssignmentMismatch);
    }
    for (allocation, assignment) in allocations.iter().zip(expected.assignments()) {
        if &allocation.assignment != assignment
            || allocation.holder
                != (DeviceHolder::Sandbox {
                    owner,
                    requirements_digest: expected.requirements_digest(),
                })
        {
            return Err(DeviceStoreError::AssignmentMismatch);
        }
        if !matches!(
            allocation.state,
            AllocationState::Reserved | AllocationState::Attaching | AllocationState::Attached
        ) {
            return Err(DeviceStoreError::StaleOwner);
        }
        let parent = read_parent(db, assignment.parent_id())?;
        if parent.host_boot_id != boot || parent.state != ParentState::Ready {
            return Err(DeviceStoreError::StaleOwner);
        }
    }
    Ok(allocations)
}
