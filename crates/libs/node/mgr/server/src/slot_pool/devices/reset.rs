//! A reset-only pool attempt has no VM owner. Eligibility and parent revision
//! fence it while the service owns the physical exclusion for the actual reset.
use super::*;

impl NetworkPool {
    pub fn queue_device_reset(
        &self,
        id: DeviceParentId,
        revision: u64,
        boot: HostBootId,
    ) -> Result<(), DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let p = read_parent(&tx, id)?;
        if p.revision != revision
            || p.state != ParentState::Quarantined
            || p.host_boot_id != boot
            || held(&tx, id)?
        {
            return Err(DeviceStoreError::StaleOwner);
        }
        bump_parent(&tx, &p, ParentState::Sanitizing)?;
        tx.execute(sql!("UPDATE device_parents SET qualification_evidence_json=NULL,last_error=NULL WHERE id=?1"),[id.to_string()])?;
        tx.commit()?;
        Ok(())
    }

    /// Claim before effects. The owning service additionally serializes reset
    /// attempts so revision fencing cannot race a still-running physical reset.
    pub fn claim_device_reset(
        &self,
        id: DeviceParentId,
        revision: u64,
        boot: HostBootId,
    ) -> Result<DeviceParent, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let p = read_parent(&tx, id)?;
        if p.revision != revision
            || p.state != ParentState::Sanitizing
            || p.host_boot_id != boot
            || held(&tx, id)?
        {
            return Err(DeviceStoreError::StaleOwner);
        }
        bump_parent(&tx, &p, ParentState::Sanitizing)?;
        let claimed = read_parent(&tx, id)?;
        tx.commit()?;
        Ok(claimed)
    }

    /// Called only after all reset-owned VFIO handles have closed. Failure
    /// clears old evidence; neither failure nor stale completion grants reuse.
    pub fn finish_device_reset(
        &self,
        p: &DeviceParent,
        evidence: Result<QualificationEvidence, &str>,
    ) -> Result<bool, DeviceStoreError> {
        let mut db = self.db.lock().map_err(|_| DeviceStoreError::Lock)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read_parent(&tx, p.id)?;
        if current.revision != p.revision
            || current.state != ParentState::Sanitizing
            || current.host_boot_id != p.host_boot_id
            || held(&tx, p.id)?
        {
            return Ok(false);
        }
        let (evidence, error, state) = match evidence {
            Ok(evidence) if evidence.matches(&current) => (
                Some(serde_json::to_string(&evidence)?),
                None,
                ParentState::Ready,
            ),
            Ok(_) => return Err(DeviceStoreError::Ownership),
            Err(error) => (
                None,
                Some(error.chars().take(2048).collect::<String>()),
                ParentState::Quarantined,
            ),
        };
        tx.execute(
            sql!(
                "UPDATE device_parents SET qualification_evidence_json=?1,last_error=?2 WHERE id=?3"
            ),
            params![evidence, error, p.id.to_string()],
        )?;
        bump_parent(&tx, &current, state)?;
        tx.commit()?;
        Ok(true)
    }
}
