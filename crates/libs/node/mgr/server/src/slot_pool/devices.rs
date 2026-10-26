//! Node-local device ownership. Parent eligibility is separate from every holder.
//!
//! Enrollment never grants clean capacity. Atomic member claims include sanitizer
//! executions, so cleanup cannot expose a free interval between tenant and scrub.

use super::{NetworkPool, db::SqliteU64};
use pplx_space_common_core::{
    sql,
    types::{Digest, SandboxActivationAttemptId, SandboxId},
};
use pplx_space_execution_spec::devices::{
    DeviceContractError, DeviceLeaseId, DeviceParentId, DeviceProfile, DeviceRequirements,
    ExclusiveDeviceAssignment, HostDeviceId,
};
use rusqlite::{TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, str::FromStr};
use uuid::Uuid;

pub(super) mod admission;
mod records;
pub use records::*;
mod reset;
mod transitions;

#[derive(Debug, thiserror::Error)]
pub enum DeviceStoreError {
    #[error("cannot read host boot identity: {0}")]
    BootIo(#[source] std::io::Error),
    #[error("invalid host boot identity: {0}")]
    BootIdentity(#[source] uuid::Error),
    #[error("device ownership database lock poisoned")]
    Lock,
    #[error("device ownership database: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("device contract: {0}")]
    Contract(#[from] DeviceContractError),
    #[error(transparent)]
    Selection(#[from] pplx_space_execution_spec::devices::DeviceSelectionError),
    #[error("invalid stored device state {0:?}")]
    CorruptState(String),
    #[error("invalid device identity evidence: {0}")]
    Evidence(#[from] serde_json::Error),
    #[error("stored device ownership is inconsistent")]
    Ownership,
    #[error("sandbox execution or network allocation changed")]
    StaleOwner,
    #[error("device capacity for {0} is currently allocated")]
    CapacityBusy(DeviceProfile),
    #[error("device capacity for {0} requires enrollment or operator requalification")]
    CapacityRequiresOperator(DeviceProfile),
    #[error("sandbox retains unresolved device allocations; finish device cleanup first")]
    OutstandingAllocations,
    #[error("device assignments differ from durable execution ownership")]
    AssignmentMismatch,
    #[error("device enrollment is limited to 256 active parents")]
    EnrollmentLimit,
    #[error("device {0} already belongs to an active enrolled parent")]
    AlreadyEnrolled(HostDeviceId),
}

fn check_owner(db: &rusqlite::Connection, owner: DeviceOwner) -> Result<(), DeviceStoreError> {
    let current = db.query_row(sql!("SELECT EXISTS (
        SELECT 1 FROM sandbox_executions e JOIN network_slots s ON s.sandbox_id = e.sandbox_id
        WHERE e.sandbox_id = ?1 AND e.execution_id = ?2 AND s.generation = ?3 AND s.state IN ('leasing', 'leased')
    )"), params![owner.sandbox_id.to_string(), owner.execution_id.to_string(), SqliteU64(owner.slot_generation)], |row| row.get::<_, bool>(0))?;
    if !current {
        return Err(DeviceStoreError::StaleOwner);
    }
    Ok(())
}

fn member_rows(
    db: &rusqlite::Connection,
    parent: DeviceParentId,
) -> Result<Vec<(String, EnrollmentIdentity)>, DeviceStoreError> {
    let rows = db.prepare(sql!("SELECT id, host_device_id, enrolled_identity FROM device_parent_members WHERE parent_id = ?1 ORDER BY host_device_id"))?
        .query_map([parent.to_string()], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(id, locator, evidence)| {
            let locator = HostDeviceId::parse(locator)?;
            let evidence: EnrollmentIdentity = serde_json::from_str(&evidence)?;
            if evidence.locator() != &locator {
                return Err(DeviceStoreError::Ownership);
            }
            Ok((id, evidence))
        })
        .collect()
}

fn read_parent(
    db: &rusqlite::Connection,
    id: DeviceParentId,
) -> Result<DeviceParent, DeviceStoreError> {
    let raw = db.query_row(sql!("SELECT profile, host_boot_id, state, revision, qualification_digest, qualification_evidence_json, mode FROM device_parents WHERE id = ?1"), [id.to_string()], |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, String>(2)?,r.get::<_, SqliteU64>(3)?.0,r.get::<_, String>(4)?,r.get::<_, Option<String>>(5)?,r.get::<_, String>(6)?)))?;
    let mode: ParentMode = raw.6.parse()?;
    let enrolled_members = member_rows(db, id)?
        .into_iter()
        .map(|(_, identity)| identity)
        .collect::<Vec<_>>();
    let parent = DeviceParent {
        id,
        profile: raw.0.parse()?,
        host_boot_id: raw.1.parse().map_err(DeviceStoreError::BootIdentity)?,
        state: raw.2.parse()?,
        revision: raw.3,
        procedure: raw.4.parse().map_err(|_| DeviceStoreError::Ownership)?,
        evidence: raw.5.map(|s| serde_json::from_str(&s)).transpose()?,
        mode,
        members: enrolled_members
            .iter()
            .map(|identity| identity.locator().clone())
            .collect(),
        enrolled_members,
    };
    if parent.members.is_empty() || parent.members.len() > 32 {
        return Err(DeviceStoreError::Ownership);
    }
    let active_count: u32 = db.query_row(sql!("SELECT COUNT(*) FROM device_active_members c JOIN device_parent_members m ON m.id=c.member_id AND m.host_device_id=c.host_device_id WHERE m.parent_id=?1"), [id.to_string()], |r| r.get(0))?;
    let expected_count = if parent.state == ParentState::Retired {
        0
    } else {
        parent.members.len()
    };
    if active_count as usize != expected_count {
        return Err(DeviceStoreError::Ownership);
    }
    if parent.state == ParentState::Ready
        && !parent.evidence.as_ref().is_some_and(|e| e.matches(&parent))
    {
        return Err(DeviceStoreError::Ownership);
    }
    Ok(parent)
}

fn read_allocation(
    db: &rusqlite::Connection,
    lease: DeviceLeaseId,
) -> Result<DeviceAllocation, DeviceStoreError> {
    let raw = db.query_row(sql!("SELECT parent_id, profile, holder_kind, execution_id, sandbox_id, slot_generation, requirements_digest, procedure_digest, state, revision, assignment_json FROM device_allocations WHERE lease_id = ?1"), [lease.to_string()], |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, String>(2)?,r.get::<_, String>(3)?,r.get::<_, Option<String>>(4)?,r.get::<_, Option<SqliteU64>>(5)?,r.get::<_, Option<String>>(6)?,r.get::<_, Option<String>>(7)?,r.get::<_, String>(8)?,r.get::<_, SqliteU64>(9)?.0,r.get::<_, Option<String>>(10)?)))?;
    let parent: DeviceParentId = raw.0.parse()?;
    let profile: DeviceProfile = raw.1.parse()?;
    let execution = raw.3.parse().map_err(|_| DeviceStoreError::Ownership)?;
    let holder = match (raw.2.parse::<HolderKind>()?, raw.4, raw.5, raw.6, raw.7) {
        (HolderKind::Sandbox, Some(sandbox), Some(generation), Some(digest), None) => {
            DeviceHolder::Sandbox {
                owner: DeviceOwner::new(
                    sandbox.parse().map_err(|_| DeviceStoreError::Ownership)?,
                    execution,
                    generation.0,
                ),
                requirements_digest: digest.parse().map_err(|_| DeviceStoreError::Ownership)?,
            }
        }
        (HolderKind::Sanitizer, None, None, None, Some(procedure)) => {
            DeviceHolder::Sanitizer(SanitizerAttempt {
                execution_id: execution,
                procedure: procedure.parse().map_err(|_| DeviceStoreError::Ownership)?,
            })
        }
        _ => return Err(DeviceStoreError::Ownership),
    };
    let state: AllocationState = raw.8.parse()?;
    let Some(encoded) = raw.10 else {
        return Err(DeviceStoreError::Ownership);
    };
    let AssignmentRecord::V1(assignment) = serde_json::from_str(&encoded)?;
    if assignment.parent_id() != parent
        || assignment.lease_id() != lease
        || assignment.profile() != &profile
    {
        return Err(DeviceStoreError::Ownership);
    }
    let held = db.prepare(sql!("SELECT m.host_device_id, h.holding_kind, h.capacity_units, c.lease_id FROM device_allocation_members h JOIN device_parent_members m ON m.id = h.member_id AND m.parent_id = h.parent_id LEFT JOIN device_exclusive_member_claims c ON c.member_id = h.member_id WHERE h.lease_id = ?1 ORDER BY m.host_device_id"))?
        .query_map([lease.to_string()], |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, u32>(2)?,r.get::<_, Option<String>>(3)?)))?
        .collect::<Result<Vec<_>,_>>()?;
    let mut members = Vec::new();
    for (locator, kind, units, claim) in held {
        let _: ExclusiveHolding = kind.parse()?;
        if units != 1
            || (state != AllocationState::Released && claim.as_deref() != Some(&lease.to_string()))
        {
            return Err(DeviceStoreError::Ownership);
        }
        members.push(HostDeviceId::parse(locator)?);
    }
    let enrolled = member_rows(db, parent)?
        .into_iter()
        .map(|(_, identity)| identity.locator().clone())
        .collect::<Vec<_>>();
    if members != assignment.host_device_ids() || members != enrolled {
        return Err(DeviceStoreError::Ownership);
    }
    Ok(DeviceAllocation {
        assignment,
        holder,
        state,
        revision: raw.9,
    })
}

fn held(db: &rusqlite::Connection, parent: DeviceParentId) -> Result<bool, DeviceStoreError> {
    Ok(db.query_row(sql!("SELECT EXISTS(SELECT 1 FROM device_allocations WHERE parent_id = ?1 AND state != ?2) OR EXISTS(SELECT 1 FROM device_exclusive_member_claims c JOIN device_parent_members m ON m.id = c.member_id WHERE m.parent_id = ?1)"), params![parent.to_string(),AllocationState::Released.as_str()], |r|r.get(0))?)
}

fn reserve(
    db: &rusqlite::Connection,
    parent: &DeviceParent,
    holder: DeviceHolder,
) -> Result<DeviceAllocation, DeviceStoreError> {
    if held(db, parent.id)? {
        return Err(DeviceStoreError::CapacityBusy(parent.profile.clone()));
    }
    let lease = DeviceLeaseId::new();
    let assignment = ExclusiveDeviceAssignment::new(
        lease,
        parent.id,
        parent.profile.clone(),
        parent.members.clone(),
    )?;
    let (kind, execution, sandbox, generation, requirements, procedure) = match holder {
        DeviceHolder::Sandbox {
            owner,
            requirements_digest,
        } => (
            HolderKind::Sandbox,
            owner.execution_id,
            Some(owner.sandbox_id.to_string()),
            Some(SqliteU64(owner.slot_generation)),
            Some(requirements_digest.to_string()),
            None,
        ),
        DeviceHolder::Sanitizer(attempt) => (
            HolderKind::Sanitizer,
            attempt.execution_id,
            None,
            None,
            None,
            Some(attempt.procedure.to_string()),
        ),
    };
    db.execute(sql!("INSERT INTO device_allocations (lease_id,parent_id,profile,holder_kind,execution_id,sandbox_id,slot_generation,requirements_digest,procedure_digest,state,revision,assignment_json,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,?11,unixepoch())"), params![lease.to_string(),parent.id.to_string(),parent.profile.as_str(),kind.as_str(),execution.to_string(),sandbox,generation,requirements,procedure,AllocationState::Reserved.as_str(),serde_json::to_string(&AssignmentRecord::V1(assignment))?])?;
    for (member, _) in member_rows(db, parent.id)? {
        db.execute(sql!("INSERT INTO device_allocation_members (lease_id,parent_id,member_id,holding_kind,capacity_units) VALUES (?1,?2,?3,?4,1)"),params![lease.to_string(),parent.id.to_string(),member,ExclusiveHolding::Exclusive.as_str()])?;
        db.execute(
            sql!("INSERT INTO device_exclusive_member_claims (member_id,lease_id) VALUES (?1,?2)"),
            params![member, lease.to_string()],
        )?;
    }
    read_allocation(db, lease)
}

fn read_parents(db: &rusqlite::Connection) -> Result<Vec<DeviceParent>, DeviceStoreError> {
    let ids = db
        .prepare(sql!(
            "SELECT id FROM device_parents WHERE state != ?1 ORDER BY id LIMIT 257"
        ))?
        .query_map([ParentState::Retired.as_str()], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if ids.len() > 256 {
        return Err(DeviceStoreError::EnrollmentLimit);
    }
    ids.into_iter()
        .map(|id| read_parent(db, id.parse()?))
        .collect()
}

fn read_parent_allocations(
    db: &rusqlite::Connection,
    parent: DeviceParentId,
) -> Result<Vec<DeviceAllocation>, DeviceStoreError> {
    let ids = db.prepare(sql!("SELECT lease_id FROM device_allocations WHERE parent_id=?1 AND state!=?2 ORDER BY lease_id LIMIT 257"))?
            .query_map(params![parent.to_string(), AllocationState::Released.as_str()], |r| r.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
    if ids.len() > 256 {
        return Err(DeviceStoreError::Ownership);
    }
    ids.into_iter()
        .map(|id| read_allocation(db, id.parse()?))
        .collect()
}

impl NetworkPool {
    pub(super) fn recover_device_boot(&self) -> Result<(), DeviceStoreError> {
        if self.device_parents()?.is_empty() {
            return Ok(());
        }
        let boot = HostBootId::current()?;
        self.quarantine_devices_from_other_boots(boot)?;
        Ok(())
    }

    pub fn enroll_device_parent(
        &self,
        profile: &DeviceProfile,
        boot: HostBootId,
        qualification: Digest,
        members: &[(HostDeviceId, EnrollmentIdentity)],
    ) -> Result<DeviceParentId, DeviceStoreError> {
        if members.is_empty()
            || members.len() > 32
            || members.iter().any(|(id, e)| e.locator() != id)
        {
            return Err(DeviceContractError::Members.into());
        }
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: u32 = tx.query_row(
            sql!("SELECT COUNT(*) FROM device_parents WHERE state != ?1"),
            [ParentState::Retired.as_str()],
            |r| r.get(0),
        )?;
        if count >= 256 {
            return Err(DeviceStoreError::EnrollmentLimit);
        }
        let id = DeviceParentId::new();
        tx.execute(sql!("INSERT INTO device_parents (id,profile,mode,host_boot_id,qualification_digest,state,revision,updated_at) VALUES (?1,?2,?3,?4,?5,?6,0,unixepoch())"),params![id.to_string(),profile.as_str(),ParentMode::Dedicated.as_str(),boot.to_string(),qualification.to_string(),ParentState::Quarantined.as_str()])?;
        for (locator, evidence) in members {
            let member = Uuid::now_v7().to_string();
            tx.execute(sql!("INSERT INTO device_parent_members (id,parent_id,host_device_id,enrolled_identity) VALUES (?1,?2,?3,?4)"),params![member,id.to_string(),locator.as_str(),serde_json::to_string(evidence)?])?;
            tx.execute(
                sql!("INSERT INTO device_active_members (host_device_id,member_id) VALUES (?1,?2)"),
                params![locator.as_str(), member],
            )
            .map_err(|error| match error {
                rusqlite::Error::SqliteFailure(ref failure, _)
                    if failure.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
                {
                    DeviceStoreError::AlreadyEnrolled(locator.clone())
                }
                other => DeviceStoreError::Database(other),
            })?;
        }
        tx.commit()?;
        Ok(id)
    }

    pub fn device_parents(&self) -> Result<Vec<DeviceParent>, DeviceStoreError> {
        let db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        read_parents(&db)
    }

    /// One read transaction prevents release/reset from splitting enrollment
    /// and its holder list into contradictory observations.
    pub fn device_capacity_snapshot(
        &self,
    ) -> Result<Vec<DeviceCapacitySnapshot>, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction()?;
        let result = read_parents(&tx)?
            .into_iter()
            .map(|parent| {
                let allocations = read_parent_allocations(&tx, parent.id())?;
                let held = held(&tx, parent.id())?;
                Ok(DeviceCapacitySnapshot {
                    parent,
                    allocations,
                    held,
                })
            })
            .collect::<Result<Vec<_>, DeviceStoreError>>()?;
        tx.commit()?;
        Ok(result)
    }

    pub fn device_parent(&self, id: DeviceParentId) -> Result<DeviceParent, DeviceStoreError> {
        let db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        read_parent(&db, id)
    }

    pub fn device_allocation(
        &self,
        lease: DeviceLeaseId,
    ) -> Result<DeviceAllocation, DeviceStoreError> {
        let db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        read_allocation(&db, lease)
    }

    /// Retains unresolved holders for startup and periodic reconciliation.
    pub fn device_parent_allocations(
        &self,
        parent: DeviceParentId,
    ) -> Result<Vec<DeviceAllocation>, DeviceStoreError> {
        let db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        read_parent_allocations(&db, parent)
    }

    pub fn reserve_device_parents(
        &self,
        owner: DeviceOwner,
        boot: HostBootId,
        requirements: &DeviceRequirements,
        preferences: &BTreeMap<DeviceProfile, u32>,
        enabled: impl Fn(&DeviceParent) -> bool,
    ) -> Result<Vec<DeviceAllocation>, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        check_owner(&tx, owner)?;
        let digest = pplx_space_execution_common::devices::requirements_digest(requirements);
        let held_leases=tx.prepare(sql!("SELECT lease_id FROM device_allocations WHERE sandbox_id = ?1 AND state != ?2 ORDER BY lease_id LIMIT 17"))?.query_map(params![owner.sandbox_id.to_string(),AllocationState::Released.as_str()],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
        if !held_leases.is_empty() {
            let allocations = held_leases
                .into_iter()
                .map(|id| read_allocation(&tx, id.parse()?))
                .collect::<Result<Vec<_>, _>>()?;
            if allocations.iter().any(|a| {
                a.holder
                    != DeviceHolder::Sandbox {
                        owner,
                        requirements_digest: digest,
                    }
                    || !matches!(
                        a.state,
                        AllocationState::Reserved
                            | AllocationState::Attaching
                            | AllocationState::Attached
                    )
            }) {
                return Err(DeviceStoreError::StaleOwner);
            }
            for a in &allocations {
                let p = read_parent(&tx, a.assignment.parent_id())?;
                if p.host_boot_id != boot
                    || p.state != ParentState::Ready
                    || !preferences.contains_key(a.assignment.profile())
                    || !enabled(&p)
                {
                    return Err(DeviceStoreError::StaleOwner);
                }
            }
            let ack = pplx_space_execution_common::proto::DeviceAcknowledgement {
                requirements_digest: digest.to_string(),
                assignments: allocations.iter().map(|a| (&a.assignment).into()).collect(),
            };
            pplx_space_execution_common::devices::validate_acknowledgement(
                requirements,
                Some(&ack),
                None,
            )
            .map_err(|_| DeviceStoreError::Ownership)?;
            tx.commit()?;
            return Ok(allocations);
        }
        let ids = tx
            .prepare(sql!(
                "SELECT id FROM device_parents WHERE state!=?1 ORDER BY id LIMIT 257"
            ))?
            .query_map([ParentState::Retired.as_str()], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if ids.len() > 256 {
            return Err(DeviceStoreError::EnrollmentLimit);
        }
        let mut candidates = BTreeMap::<DeviceProfile, Vec<DeviceParent>>::new();
        let mut potentially_available = BTreeMap::<DeviceProfile, u32>::new();
        for id in ids {
            let parent = read_parent(&tx, id.parse()?)?;
            if parent.host_boot_id != boot
                || !preferences.contains_key(&parent.profile)
                || !enabled(&parent)
                || !requirements
                    .entries()
                    .iter()
                    .any(|r| r.profiles().contains(&parent.profile))
            {
                continue;
            }
            let allocated = held(&tx, parent.id)?;
            if matches!(parent.state, ParentState::Ready | ParentState::Sanitizing) {
                *potentially_available
                    .entry(parent.profile.clone())
                    .or_default() += 1;
            }
            if parent.allocatable(boot, true, allocated) {
                candidates
                    .entry(parent.profile.clone())
                    .or_default()
                    .push(parent);
            }
        }
        let capacity = candidates
            .iter()
            .map(|(profile, parents)| (profile.clone(), parents.len() as u32))
            .collect();
        let Some(choices) = requirements.select_profiles(&capacity, preferences)? else {
            let profile = requirements
                .entries()
                .first()
                .and_then(|r| r.profiles().first())
                .ok_or(DeviceStoreError::Ownership)?
                .clone();
            return Err(if requirements.can_fit(&potentially_available)? {
                DeviceStoreError::CapacityBusy(profile)
            } else {
                DeviceStoreError::CapacityRequiresOperator(profile)
            });
        };
        let mut selected = Vec::new();
        for (requirement, choice) in requirements.entries().iter().zip(choices) {
            let parents = candidates
                .get_mut(&choice)
                .ok_or(DeviceStoreError::Ownership)?;
            for _ in 0..requirement.count() {
                let parent = parents.pop().ok_or(DeviceStoreError::Ownership)?;
                selected.push(reserve(
                    &tx,
                    &parent,
                    DeviceHolder::Sandbox {
                        owner,
                        requirements_digest: digest,
                    },
                )?);
                bump_parent(&tx, &parent, ParentState::Ready)?;
            }
        }
        tx.commit()?;
        Ok(selected)
    }
}

fn bump_parent(
    db: &rusqlite::Connection,
    parent: &DeviceParent,
    state: ParentState,
) -> Result<(), DeviceStoreError> {
    let changed=db.execute(sql!("UPDATE device_parents SET state=?1,revision=revision+1,updated_at=unixepoch() WHERE id=?2 AND revision=?3 AND revision < 9223372036854775807"),params![state.as_str(),parent.id.to_string(),SqliteU64(parent.revision)])?;
    if changed != 1 {
        return Err(DeviceStoreError::Ownership);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
