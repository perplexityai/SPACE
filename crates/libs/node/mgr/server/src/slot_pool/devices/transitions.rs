use super::*;

fn current(
    db: &rusqlite::Connection,
    expected: &DeviceAllocation,
) -> Result<bool, DeviceStoreError> {
    let exists: bool = db.query_row(
        sql!("SELECT EXISTS(SELECT 1 FROM device_allocations WHERE lease_id=?1)"),
        [expected.assignment.lease_id().to_string()],
        |r| r.get(0),
    )?;
    Ok(exists && read_allocation(db, expected.assignment.lease_id())? == *expected)
}
fn allocation_state(
    db: &rusqlite::Connection,
    a: &DeviceAllocation,
    to: AllocationState,
) -> Result<(), DeviceStoreError> {
    let changed=db.execute(sql!("UPDATE device_allocations SET state=?1, revision=revision+1, updated_at=unixepoch() WHERE lease_id=?2 AND revision=?3 AND revision < 9223372036854775807"),params![to.as_str(),a.assignment.lease_id().to_string(),SqliteU64(a.revision)])?;
    if changed != 1 {
        return Err(DeviceStoreError::Ownership);
    }
    Ok(())
}
fn release_claims(db: &rusqlite::Connection, a: &DeviceAllocation) -> Result<(), DeviceStoreError> {
    let deleted = db.execute(
        sql!("DELETE FROM device_exclusive_member_claims WHERE lease_id=?1"),
        [a.assignment.lease_id().to_string()],
    )?;
    if deleted != a.assignment.host_device_ids().len() {
        return Err(DeviceStoreError::Ownership);
    }
    allocation_state(db, a, AllocationState::Released)
}

impl NetworkPool {
    /// Persist intent before the supervisor receives any device handle.
    pub fn begin_device_attachment(
        &self,
        a: &DeviceAllocation,
        boot: HostBootId,
    ) -> Result<bool, DeviceStoreError> {
        self.transition_device_attachment(
            a,
            boot,
            AllocationState::Reserved,
            AllocationState::Attaching,
        )
    }
    pub fn confirm_device_attachment(
        &self,
        a: &DeviceAllocation,
        boot: HostBootId,
    ) -> Result<bool, DeviceStoreError> {
        self.transition_device_attachment(
            a,
            boot,
            AllocationState::Attaching,
            AllocationState::Attached,
        )
    }
    fn transition_device_attachment(
        &self,
        a: &DeviceAllocation,
        boot: HostBootId,
        from: AllocationState,
        to: AllocationState,
    ) -> Result<bool, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if a.state != from || !current(&tx, a)? {
            return Ok(false);
        }
        let parent = read_parent(&tx, a.assignment.parent_id())?;
        if parent.host_boot_id != boot {
            return Err(DeviceStoreError::StaleOwner);
        }
        match a.holder {
            DeviceHolder::Sandbox { owner, .. } => {
                check_owner(&tx, owner)?;
                if parent.state != ParentState::Ready {
                    return Err(DeviceStoreError::StaleOwner);
                }
            }
            DeviceHolder::Sanitizer(attempt) => {
                if parent.state != ParentState::Sanitizing || parent.procedure != attempt.procedure
                {
                    return Err(DeviceStoreError::StaleOwner);
                }
            }
        }
        allocation_state(&tx, a, to)?;
        tx.commit()?;
        Ok(true)
    }

    /// Release remains valid after the sandbox slot is gone, but never for a
    /// replacement allocation, execution, parent or revision.
    pub fn begin_device_release(&self, a: &DeviceAllocation) -> Result<bool, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !matches!(
            a.state,
            AllocationState::Reserved
                | AllocationState::Attaching
                | AllocationState::Attached
                | AllocationState::Quarantined
        ) || !current(&tx, a)?
        {
            return Ok(false);
        }
        allocation_state(&tx, a, AllocationState::Releasing)?;
        tx.commit()?;
        Ok(true)
    }

    /// The host cleanup reconciler must prove absence of the departing holder
    /// under launch exclusion before calling. None means quarantine; Some is
    /// the durably reserved sanitizer the reconciler must run/recover.
    pub fn finish_device_release(
        &self,
        a: &DeviceAllocation,
        parent_revision: u64,
        boot: HostBootId,
        disposition: DeviceReleaseDisposition,
    ) -> Result<Option<DeviceAllocation>, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if a.state != AllocationState::Releasing || !current(&tx, a)? {
            return Err(DeviceStoreError::StaleOwner);
        }
        let parent = read_parent(&tx, a.assignment.parent_id())?;
        if parent.revision != parent_revision || parent.state == ParentState::Retired {
            return Err(DeviceStoreError::StaleOwner);
        }
        // Quarantine cleanup can discharge an old-boot holder; reuse cannot.
        if let DeviceReleaseDisposition::Sanitize(attempt) = disposition
            && (parent.state != ParentState::Ready
                || parent.host_boot_id != boot
                || parent.procedure != attempt.procedure
                || !matches!(a.holder, DeviceHolder::Sandbox { .. }))
        {
            return Err(DeviceStoreError::StaleOwner);
        }
        if matches!(disposition, DeviceReleaseDisposition::QueueReset)
            && (parent.state != ParentState::Ready
                || parent.host_boot_id != boot
                || !matches!(a.holder, DeviceHolder::Sandbox { .. }))
        {
            return Err(DeviceStoreError::StaleOwner);
        }
        release_claims(&tx, a)?;
        let next = match disposition {
            DeviceReleaseDisposition::Quarantine => {
                bump_parent(&tx, &parent, ParentState::Quarantined)?;
                None
            }
            DeviceReleaseDisposition::QueueReset => {
                bump_parent(&tx, &parent, ParentState::Sanitizing)?;
                None
            }
            DeviceReleaseDisposition::Sanitize(attempt) => {
                bump_parent(&tx, &parent, ParentState::Sanitizing)?;
                Some(reserve(&tx, &parent, DeviceHolder::Sanitizer(attempt))?)
            }
        };
        tx.execute(
            sql!("UPDATE device_parents SET qualification_evidence_json=NULL WHERE id=?1"),
            [parent.id.to_string()],
        )?;
        tx.commit()?;
        Ok(next)
    }

    /// Operator qualification enters the same holder lifecycle as automatic
    /// cleanup. The host must validate a compiled procedure and enrollment;
    /// this storage boundary does not accept caller-authored success evidence.
    pub fn begin_device_requalification(
        &self,
        id: DeviceParentId,
        revision: u64,
        boot: HostBootId,
        attempt: SanitizerAttempt,
    ) -> Result<DeviceAllocation, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let parent = read_parent(&tx, id)?;
        if parent.state != ParentState::Quarantined
            || parent.revision != revision
            || parent.procedure != attempt.procedure
        {
            return Err(DeviceStoreError::StaleOwner);
        }
        if held(&tx, id)? {
            return Err(DeviceStoreError::CapacityBusy(parent.profile));
        }
        bump_parent(&tx, &parent, ParentState::Sanitizing)?;
        tx.execute(sql!("UPDATE device_parents SET host_boot_id=?1,qualification_evidence_json=NULL WHERE id=?2"),params![boot.to_string(),id.to_string()])?;
        let a = reserve(&tx, &parent, DeviceHolder::Sanitizer(attempt))?;
        tx.commit()?;
        Ok(a)
    }

    /// Only the qualified host procedure calls this after verifying cleanup,
    /// identity and firmware, and proving the sanitizer holder is gone. Guest
    /// reports and operator assertions must never invoke this completion path.
    pub fn finish_device_sanitization(
        &self,
        a: &DeviceAllocation,
        parent_revision: u64,
        boot: HostBootId,
        completed_at: jiff::Timestamp,
    ) -> Result<bool, DeviceStoreError> {
        let DeviceHolder::Sanitizer(attempt) = a.holder else {
            return Err(DeviceStoreError::Ownership);
        };
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if a.state != AllocationState::Releasing || !current(&tx, a)? {
            return Ok(false);
        }
        let parent = read_parent(&tx, a.assignment.parent_id())?;
        if parent.state != ParentState::Sanitizing
            || parent.revision != parent_revision
            || parent.host_boot_id != boot
            || parent.procedure != attempt.procedure
        {
            return Err(DeviceStoreError::StaleOwner);
        }
        release_claims(&tx, a)?;
        if held(&tx, parent.id)? {
            return Err(DeviceStoreError::Ownership);
        }
        let evidence = QualificationEvidence::CompletedV1 {
            execution_id: attempt.execution_id,
            profile: parent.profile.clone(),
            procedure: attempt.procedure,
            host_boot_id: boot,
            members: parent.members.clone(),
            completed_at,
        };
        tx.execute(sql!("UPDATE device_parents SET qualification_evidence_json=?1,last_error=NULL WHERE id=?2"),params![serde_json::to_string(&evidence)?,parent.id.to_string()])?;
        bump_parent(&tx, &parent, ParentState::Ready)?;
        tx.commit()?;
        Ok(true)
    }

    /// A failed/uncertain cleanup never drops claims. The retained holder must
    /// still be reconciled before repair, retirement or another sanitizer run.
    pub fn quarantine_device_allocation(
        &self,
        a: &DeviceAllocation,
        parent_revision: u64,
    ) -> Result<bool, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if a.state == AllocationState::Released || !current(&tx, a)? {
            return Ok(false);
        }
        let parent = read_parent(&tx, a.assignment.parent_id())?;
        if parent.state == ParentState::Retired || parent.revision != parent_revision {
            return Ok(false);
        }
        allocation_state(&tx, a, AllocationState::Quarantined)?;
        bump_parent(&tx, &parent, ParentState::Quarantined)?;
        tx.execute(sql!("UPDATE device_parents SET qualification_evidence_json=NULL,last_error='holder or cleanup outcome uncertain; reconciliation required' WHERE id=?1"),[parent.id.to_string()])?;
        tx.commit()?;
        Ok(true)
    }

    /// Preserve unknown holders and their claims; only their proven release may
    /// remove those claims. Also invalidates in-progress sanitizers on restart.
    pub fn quarantine_devices_from_other_boots(
        &self,
        boot: HostBootId,
    ) -> Result<usize, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ids=tx.prepare(sql!("SELECT id FROM device_parents WHERE state != ?1 AND state != ?2 AND (host_boot_id != ?3 OR state=?4)"))?.query_map(params![ParentState::Retired.as_str(),ParentState::Quarantined.as_str(),boot.to_string(),ParentState::Sanitizing.as_str()],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
        let mut quarantined = 0;
        for id in &ids {
            let p = read_parent(&tx, id.parse()?)?;
            // Queued reset work has no VMM holder and survives a same-boot
            // manager restart. A sanitizer allocation must instead reconcile.
            if p.host_boot_id == boot && p.state == ParentState::Sanitizing && !held(&tx, p.id)? {
                continue;
            }
            bump_parent(&tx, &p, ParentState::Quarantined)?;
            quarantined += 1;
            tx.execute(sql!("UPDATE device_parents SET qualification_evidence_json=NULL,last_error='host continuity lost or sanitizer interrupted; requalification required' WHERE id=?1"),[id])?;
            tx.execute(sql!("UPDATE device_allocations SET state=?1,revision=revision+1,updated_at=unixepoch() WHERE parent_id=?2 AND state!=?3 AND state!=?1 AND revision<9223372036854775807"),params![AllocationState::Quarantined.as_str(),id,AllocationState::Released.as_str()])?;
        }
        tx.commit()?;
        Ok(quarantined)
    }

    /// Host-side holder exclusion is required in addition to database checks.
    /// Retiring releases locator claims, but never erases membership history or
    /// transfers trust to a replacement at the same address.
    pub fn retire_device_parent(
        &self,
        id: DeviceParentId,
        revision: u64,
    ) -> Result<bool, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let p = read_parent(&tx, id)?;
        if p.revision != revision
            || matches!(p.state, ParentState::Retired | ParentState::Sanitizing)
        {
            return Ok(false);
        }
        if held(&tx, id)? {
            return Err(DeviceStoreError::CapacityBusy(p.profile));
        }
        bump_parent(&tx, &p, ParentState::Retired)?;
        tx.execute(sql!("DELETE FROM device_active_members WHERE member_id IN (SELECT id FROM device_parent_members WHERE parent_id=?1)"),[id.to_string()])?;
        tx.commit()?;
        Ok(true)
    }
}
