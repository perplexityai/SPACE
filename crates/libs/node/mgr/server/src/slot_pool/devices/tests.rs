use super::*;
use crate::slot_pool::{NetworkPoolConfig, NewLeaseMemoryReservation};
use pplx_space_execution_spec::devices::DeviceRequirement;

fn pool() -> std::sync::Arc<NetworkPool> {
    let pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(4));
    pool.set_all_slots_ready_for_test();
    pool
}
fn boot() -> HostBootId {
    "00000000-0000-4000-8000-000000000001".parse().unwrap()
}
fn owner(pool: &NetworkPool) -> DeviceOwner {
    let sandbox = SandboxId::new();
    let execution = SandboxActivationAttemptId::new();
    pool.bind_execution(
        &sandbox.to_string(),
        pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(execution, None),
    )
    .unwrap();
    let slot = pool
        .allocate_pooled_slot(
            &sandbox.to_string(),
            None,
            Some(NewLeaseMemoryReservation {
                memory_reservation_mib: 8192,
            }),
        )
        .unwrap();
    DeviceOwner::new(sandbox, execution, slot.generation)
}
fn attempt() -> SanitizerAttempt {
    SanitizerAttempt::new(SandboxActivationAttemptId::new(), Digest::sha256([1; 32]))
}
fn enroll(pool: &NetworkPool, profile: &str, member: &str) -> DeviceParentId {
    let id: HostDeviceId = member.parse().unwrap();
    pool.enroll_device_parent(
        &profile.parse().unwrap(),
        boot(),
        Digest::sha256([1; 32]),
        &[(
            id.clone(),
            EnrollmentIdentity::LinuxPciV1 {
                locator: id,
                vendor: 0x1234,
                device: 1,
                subsystem_vendor: 0x1234,
                subsystem_device: 2,
                topology_digest: Digest::sha256([2; 32]),
                sysfs_device: 1,
                sysfs_inode: 1,
            },
        )],
    )
    .unwrap()
}
fn parent(pool: &NetworkPool, id: DeviceParentId) -> DeviceParent {
    read_parent(&pool.db.lock().unwrap(), id).unwrap()
}
fn refresh(pool: &NetworkPool, a: &DeviceAllocation) -> DeviceAllocation {
    pool.device_allocation(a.assignment.lease_id()).unwrap()
}
fn requirements(entries: &[(&str, u32)]) -> DeviceRequirements {
    DeviceRequirements::new(
        entries
            .iter()
            .map(|(p, n)| DeviceRequirement::new(p.parse().unwrap(), *n).unwrap())
            .collect(),
    )
    .unwrap()
}
fn releasing(pool: &NetworkPool, a: &DeviceAllocation) -> DeviceAllocation {
    assert!(pool.begin_device_release(a).unwrap());
    refresh(pool, a)
}
// The store is exercised with synthetic host success, not a hardware qualifier.
fn complete_sanitizer(pool: &NetworkPool, a: &DeviceAllocation) {
    assert!(pool.begin_device_attachment(a, boot()).unwrap());
    let attached = refresh(pool, a);
    assert!(pool.confirm_device_attachment(&attached, boot()).unwrap());
    let a = releasing(pool, &refresh(pool, a));
    let p = parent(pool, a.assignment.parent_id());
    assert!(
        pool.finish_device_sanitization(
            &a,
            p.revision,
            boot(),
            "2026-01-01T00:00:00Z".parse().unwrap()
        )
        .unwrap()
    );
}
fn qualify(pool: &NetworkPool, id: DeviceParentId) {
    let p = parent(pool, id);
    let a = pool
        .begin_device_requalification(id, p.revision, boot(), attempt())
        .unwrap();
    complete_sanitizer(pool, &a);
}

#[test]
fn enrollment_retirement_and_rollback_never_manufacture_clean_capacity() {
    let pool = pool();
    let owner = owner(&pool);
    let id = enroll(&pool, "compute", "pci:example-a");
    assert_eq!(parent(&pool, id).state, ParentState::Quarantined);
    assert!(matches!(
        pool.reserve_for_test(owner, boot(), &requirements(&[("compute", 1)])),
        Err(DeviceStoreError::CapacityRequiresOperator(_))
    ));
    let evidence = EnrollmentIdentity::LinuxPciV1 {
        locator: "pci:example-a".parse().unwrap(),
        vendor: 0x1234,
        device: 1,
        subsystem_vendor: 0x1234,
        subsystem_device: 2,
        topology_digest: Digest::sha256([2; 32]),
        sysfs_device: 1,
        sysfs_inode: 1,
    };
    pool.enroll_device_parent(
        &"compute".parse().unwrap(),
        boot(),
        Digest::sha256([1; 32]),
        &[(evidence.locator().clone(), evidence)],
    )
    .unwrap_err();
    assert_eq!(pool.device_parents().unwrap().len(), 1);
    {
        let db = pool.db.lock().unwrap();
        db.execute_batch("SAVEPOINT rollback_test").unwrap();
        db.execute_batch(include_str!(
            "../../../../../../../../db/node-manager/20261025051000_device_ownership.down.sql"
        ))
        .unwrap_err();
        db.execute_batch("ROLLBACK TO rollback_test; RELEASE rollback_test")
            .unwrap();
        assert_eq!(
            read_parent(&db, id).unwrap().state,
            ParentState::Quarantined
        );
    }
    assert!(pool.retire_device_parent(id, 0).unwrap());
    let replacement = enroll(&pool, "compute", "pci:example-a");
    assert_ne!(replacement, id);
    assert_eq!(parent(&pool, id).state, ParentState::Retired);
    assert_eq!(parent(&pool, replacement).state, ParentState::Quarantined);
    pool.begin_device_requalification(id, 1, boot(), attempt())
        .unwrap_err();
}

#[test]
fn reservation_is_atomic_idempotent_and_execution_fenced() {
    let pool = pool();
    let owner = owner(&pool);
    let first = enroll(&pool, "a", "pci:example-a");
    qualify(&pool, first);
    let second = enroll(&pool, "b", "pci:example-b");
    let required = requirements(&[("a", 1), ("b", 1)]);
    assert_eq!(
        pool.active_memory_reservations().unwrap()[0].mode,
        crate::memory_admission::MemoryReservationMode::Elastic
    );
    pool.reserve_for_test(owner, boot(), &required).unwrap_err();
    assert!(!held(&pool.db.lock().unwrap(), first).unwrap());
    qualify(&pool, second);
    let initial = pool.reserve_for_test(owner, boot(), &required).unwrap();
    assert_eq!(
        pool.active_memory_reservations().unwrap()[0].mode,
        crate::memory_admission::MemoryReservationMode::FullyReserved
    );
    let replay = pool.reserve_for_test(owner, boot(), &required).unwrap();
    assert_eq!(initial, replay);
    pool.reserve_for_test(owner, boot(), &requirements(&[("a", 1)]))
        .unwrap_err();
    let a = &initial[0];
    let mut stale = a.clone();
    stale.revision += 1;
    assert!(!pool.begin_device_attachment(&stale, boot()).unwrap());
    assert!(pool.begin_device_attachment(a, boot()).unwrap());
    assert!(!pool.begin_device_attachment(a, boot()).unwrap());
    let attaching = refresh(&pool, a);
    assert!(pool.confirm_device_attachment(&attaching, boot()).unwrap());
    let releasing = releasing(&pool, &refresh(&pool, a));
    let p = parent(&pool, a.assignment.parent_id());
    assert!(
        pool.finish_device_release(
            &releasing,
            p.revision,
            boot(),
            DeviceReleaseDisposition::Quarantine
        )
        .unwrap()
        .is_none()
    );
    let released = refresh(&pool, a);
    assert_eq!(released.state, AllocationState::Released);
    assert_eq!(released.holder, a.holder);
    assert_eq!(parent(&pool, p.id).state, ParentState::Quarantined);
    assert!(!held(&pool.db.lock().unwrap(), p.id).unwrap());
    // Releasing one member must not make the still-device-bearing lease elastic.
    assert_eq!(
        pool.active_memory_reservations().unwrap()[0].mode,
        crate::memory_admission::MemoryReservationMode::FullyReserved
    );
    let other = self::releasing(&pool, &initial[1]);
    let other_parent = parent(&pool, other.assignment.parent_id());
    pool.finish_device_release(
        &other,
        other_parent.revision,
        boot(),
        DeviceReleaseDisposition::Quarantine,
    )
    .unwrap();
    assert_eq!(
        pool.active_memory_reservations().unwrap()[0].mode,
        crate::memory_admission::MemoryReservationMode::Elastic
    );
}

#[test]
fn cleanup_hands_exclusive_claims_to_sanitizer_without_ready_interval() {
    let pool = pool();
    let owner = owner(&pool);
    let other = owner_for(&pool);
    let id = enroll(&pool, "compute", "pci:example-a");
    qualify(&pool, id);
    let req = requirements(&[("compute", 1)]);
    let a = pool
        .reserve_for_test(owner, boot(), &req)
        .unwrap()
        .pop()
        .unwrap();
    let a = releasing(&pool, &a);
    let p = parent(&pool, id);
    let mut stale = a.clone();
    stale.revision += 1;
    pool.finish_device_release(
        &stale,
        p.revision,
        boot(),
        DeviceReleaseDisposition::Sanitize(attempt()),
    )
    .unwrap_err();
    assert_eq!(refresh(&pool, &a), a);
    let sanitizer = pool
        .finish_device_release(
            &a,
            p.revision,
            boot(),
            DeviceReleaseDisposition::Sanitize(attempt()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(parent(&pool, id).state, ParentState::Sanitizing);
    assert_eq!(refresh(&pool, &a).state, AllocationState::Released);
    assert!(matches!(
        pool.reserve_for_test(other, boot(), &req),
        Err(DeviceStoreError::CapacityBusy(_))
    ));
    let claimed: String = pool
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT lease_id FROM device_exclusive_member_claims",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(claimed, sanitizer.assignment.lease_id().to_string());
    pool.finish_device_release(
        &a,
        p.revision,
        boot(),
        DeviceReleaseDisposition::Sanitize(attempt()),
    )
    .unwrap_err();
    assert_eq!(refresh(&pool, &sanitizer), sanitizer);
    complete_sanitizer(&pool, &sanitizer);
    assert_eq!(parent(&pool, id).state, ParentState::Ready);
    let next = pool
        .reserve_for_test(other, boot(), &req)
        .unwrap()
        .pop()
        .unwrap();
    assert_ne!(next.assignment.lease_id(), a.assignment.lease_id());
}
fn owner_for(pool: &NetworkPool) -> DeviceOwner {
    owner(pool)
}

#[test]
fn interrupted_sanitizer_keeps_claims_and_cannot_publish_stale_success() {
    for interruption in 0..3 {
        let pool = pool();
        let id = enroll(&pool, "compute", "pci:example-a");
        let a = pool
            .begin_device_requalification(id, 0, boot(), attempt())
            .unwrap();
        assert!(pool.begin_device_attachment(&a, boot()).unwrap());
        let a = releasing(&pool, &refresh(&pool, &a));
        let old = parent(&pool, id);
        let observed = if interruption == 1 {
            "00000000-0000-4000-8000-000000000002".parse().unwrap()
        } else {
            boot()
        };
        if interruption == 2 {
            assert!(pool.quarantine_device_allocation(&a, old.revision).unwrap());
        } else {
            assert_eq!(
                pool.quarantine_devices_from_other_boots(observed).unwrap(),
                1
            );
        }
        let now = refresh(&pool, &a);
        assert_eq!(now.holder, a.holder);
        assert_eq!(now.state, AllocationState::Quarantined);
        assert!(
            !pool
                .finish_device_sanitization(&a, old.revision, boot(), jiff::Timestamp::now())
                .unwrap()
        );
        assert!(held(&pool.db.lock().unwrap(), id).unwrap());
        pool.retire_device_parent(id, parent(&pool, id).revision)
            .unwrap_err();
        pool.begin_device_requalification(id, parent(&pool, id).revision, observed, attempt())
            .unwrap_err();
        let releasing = releasing(&pool, &now);
        pool.finish_device_release(
            &releasing,
            parent(&pool, id).revision,
            observed,
            DeviceReleaseDisposition::Quarantine,
        )
        .unwrap();
        assert!(!held(&pool.db.lock().unwrap(), id).unwrap());
        assert_eq!(parent(&pool, id).state, ParentState::Quarantined);
    }
}

#[test]
fn database_rejects_cross_parent_holdings_and_duplicate_exclusive_claims() {
    let pool = pool();
    let a = enroll(&pool, "a", "pci:example-a");
    let b = enroll(&pool, "b", "pci:example-b");
    let first = pool
        .begin_device_requalification(a, 0, boot(), attempt())
        .unwrap();
    let second = pool
        .begin_device_requalification(b, 0, boot(), attempt())
        .unwrap();
    let db = pool.db.lock().unwrap();
    let member = member_rows(&db, a).unwrap()[0].0.clone();
    db.execute("INSERT INTO device_allocation_members (lease_id,parent_id,member_id,holding_kind,capacity_units) VALUES (?1,?2,?3,'exclusive',1)",params![second.assignment.lease_id().to_string(),b.to_string(),member]).unwrap_err();
    // Construct a second same-parent history row directly, then exercise the
    // database guard independently of the storage layer's conflict query.
    let duplicate = DeviceLeaseId::new().to_string();
    db.execute("INSERT INTO device_allocations (lease_id,parent_id,profile,holder_kind,execution_id,procedure_digest,state,revision,updated_at) SELECT ?1,parent_id,profile,holder_kind,execution_id,procedure_digest,state,revision,updated_at FROM device_allocations WHERE lease_id=?2",params![duplicate,first.assignment.lease_id().to_string()]).unwrap();
    db.execute("INSERT INTO device_allocation_members (lease_id,parent_id,member_id,holding_kind,capacity_units) VALUES (?1,?2,?3,'exclusive',1)",params![duplicate,a.to_string(),member]).unwrap();
    db.execute(
        "INSERT INTO device_exclusive_member_claims (member_id,lease_id) VALUES (?1,?2)",
        params![member, duplicate],
    )
    .unwrap_err();
    let actual: String = db
        .query_row(
            "SELECT lease_id FROM device_exclusive_member_claims WHERE member_id=?1",
            [member],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(actual, first.assignment.lease_id().to_string());
}

impl NetworkPool {
    fn reserve_for_test(
        &self,
        owner: DeviceOwner,
        boot: HostBootId,
        required: &DeviceRequirements,
    ) -> Result<Vec<DeviceAllocation>, DeviceStoreError> {
        let preferences = required
            .entries()
            .iter()
            .flat_map(|r| r.profiles())
            .map(|p| (p.clone(), 0))
            .collect();
        self.reserve_device_parents(owner, boot, required, &preferences, |_| true)
    }
}

#[test]
fn admission_checks_complete_assignments_and_cleanup_retains_the_slot() {
    use pplx_space_execution_common::devices::{DeviceExecution, requirements_digest};
    let pool = pool();
    let owner = owner(&pool);
    let required = requirements(&[("compute", 2)]);
    for member in ["pci:example-a", "pci:example-b"] {
        let parent = enroll(&pool, "compute", member);
        qualify(&pool, parent);
    }
    pool.ensure_no_device_allocations(owner.sandbox_id).unwrap();
    let allocations = pool.reserve_for_test(owner, boot(), &required).unwrap();
    for execution in [None, Some(SandboxActivationAttemptId::new())] {
        assert!(matches!(
            pool.device_cleanup_allocations(owner.sandbox_id, execution),
            Err(DeviceStoreError::StaleOwner)
        ));
    }
    assert_eq!(
        pool.device_cleanup_allocations(owner.sandbox_id, Some(owner.execution_id))
            .unwrap(),
        allocations
    );
    let expected = DeviceExecution::new(
        requirements_digest(&required),
        allocations.iter().map(|a| a.assignment.clone()).collect(),
    )
    .unwrap();
    pool.validate_device_execution(owner, boot(), &expected)
        .unwrap();
    assert!(matches!(
        pool.ensure_no_device_allocations(owner.sandbox_id),
        Err(DeviceStoreError::OutstandingAllocations)
    ));
    pool.release(&owner.sandbox_id.to_string()).unwrap_err();
    let fence =
        pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(owner.execution_id, None);
    pool.bind_execution(&owner.sandbox_id.to_string(), fence)
        .unwrap();
    pool.retire_execution(&owner.sandbox_id.to_string(), owner.execution_id)
        .unwrap_err();
    pool.bind_execution(
        &owner.sandbox_id.to_string(),
        pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
            SandboxActivationAttemptId::new(),
            Some(owner.execution_id),
        ),
    )
    .unwrap_err();
    // A stale retire remains an idempotent no-op, preserving the current fence.
    pool.retire_execution(
        &owner.sandbox_id.to_string(),
        SandboxActivationAttemptId::new(),
    )
    .unwrap();
    assert_eq!(
        pool.execution_fence(&owner.sandbox_id.to_string()).unwrap(),
        Some(fence)
    );
    assert_eq!(
        pool.get(&owner.sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .generation,
        owner.slot_generation
    );

    let missing = DeviceExecution::new(
        expected.requirements_digest(),
        expected.assignments()[..1].to_vec(),
    )
    .unwrap();
    let wrong_digest =
        DeviceExecution::new(Digest::sha256([9; 32]), expected.assignments().to_vec()).unwrap();
    let mut changed = expected.assignments().to_vec();
    let a = &changed[0];
    changed[0] = ExclusiveDeviceAssignment::new(
        a.lease_id(),
        a.parent_id(),
        a.profile().clone(),
        vec!["pci:example-other".parse().unwrap()],
    )
    .unwrap();
    let changed = DeviceExecution::new(expected.requirements_digest(), changed).unwrap();
    for bad in [missing, wrong_digest, changed] {
        assert!(matches!(
            pool.validate_device_execution(owner, boot(), &bad),
            Err(DeviceStoreError::AssignmentMismatch)
        ));
    }
    for stale in [
        DeviceOwner::new(
            owner.sandbox_id,
            SandboxActivationAttemptId::new(),
            owner.slot_generation,
        ),
        DeviceOwner::new(
            owner.sandbox_id,
            owner.execution_id,
            owner.slot_generation + 1,
        ),
    ] {
        assert!(matches!(
            pool.validate_device_execution(stale, boot(), &expected),
            Err(DeviceStoreError::StaleOwner)
        ));
    }
    assert!(matches!(
        pool.validate_device_execution(
            owner,
            "00000000-0000-4000-8000-000000000002".parse().unwrap(),
            &expected
        ),
        Err(DeviceStoreError::StaleOwner)
    ));
    for a in &allocations {
        assert!(pool.begin_device_attachment(a, boot()).unwrap());
    }
    pool.validate_device_execution(owner, boot(), &expected)
        .unwrap();
    for a in &allocations {
        assert!(
            pool.confirm_device_attachment(&refresh(&pool, a), boot())
                .unwrap()
        );
    }
    pool.validate_device_execution(owner, boot(), &expected)
        .unwrap();
    let first = &allocations[0];
    assert!(
        pool.quarantine_device_allocation(
            &refresh(&pool, first),
            parent(&pool, first.assignment.parent_id()).revision
        )
        .unwrap()
    );
    assert!(matches!(
        pool.validate_device_execution(owner, boot(), &expected),
        Err(DeviceStoreError::StaleOwner)
    ));
    for a in &allocations {
        let releasing = releasing(&pool, &refresh(&pool, a));
        pool.release(&owner.sandbox_id.to_string()).unwrap_err();
        let p = parent(&pool, a.assignment.parent_id());
        pool.finish_device_release(
            &releasing,
            p.revision,
            boot(),
            DeviceReleaseDisposition::Quarantine,
        )
        .unwrap();
    }
    pool.ensure_no_device_allocations(owner.sandbox_id).unwrap();
    assert!(
        pool.device_cleanup_allocations(owner.sandbox_id, Some(owner.execution_id))
            .unwrap()
            .is_empty()
    );
    assert!(
        pool.release(&owner.sandbox_id.to_string())
            .unwrap()
            .is_some()
    );
    pool.retire_execution(&owner.sandbox_id.to_string(), owner.execution_id)
        .unwrap();
    assert!(
        pool.execution_fence(&owner.sandbox_id.to_string())
            .unwrap()
            .is_none()
    );
    assert!(
        pool.device_parents()
            .unwrap()
            .iter()
            .all(|p| p.state() == ParentState::Quarantined)
    );
}

#[test]
fn reset_pool_keeps_release_unavailable_and_fences_stale_completions() {
    let pool = pool();
    let owner = owner(&pool);
    let id = enroll(&pool, "compute", "pci:example-a");
    qualify(&pool, id);
    let allocation = pool
        .reserve_for_test(owner, boot(), &requirements(&[("compute", 1)]))
        .unwrap()
        .remove(0);
    let allocation = releasing(&pool, &allocation);
    let p = parent(&pool, id);
    assert!(
        pool.finish_device_release(
            &allocation,
            p.revision,
            boot(),
            DeviceReleaseDisposition::QueueReset
        )
        .unwrap()
        .is_none()
    );
    pool.ensure_no_device_allocations(owner.sandbox_id).unwrap();
    assert!(matches!(
        pool.reserve_for_test(owner, boot(), &requirements(&[("compute", 1)])),
        Err(DeviceStoreError::CapacityBusy(_))
    ));
    pool.quarantine_devices_from_other_boots(boot()).unwrap();
    let queued = parent(&pool, id);
    assert_eq!(queued.state, ParentState::Sanitizing);
    let claimed = pool
        .claim_device_reset(id, queued.revision, boot())
        .unwrap();
    assert!(
        !pool
            .finish_device_reset(&queued, Err("stale worker"))
            .unwrap()
    );
    assert!(
        pool.finish_device_reset(&claimed, Err("reset failed"))
            .unwrap()
    );
    assert_eq!(parent(&pool, id).state, ParentState::Quarantined);
    assert!(matches!(
        pool.reserve_for_test(owner, boot(), &requirements(&[("compute", 1)])),
        Err(DeviceStoreError::CapacityRequiresOperator(_))
    ));
}

#[test]
fn reset_development_evidence_is_rejected_by_production_and_revoked_on_boot_change() {
    let pool = pool();
    let id = enroll(&pool, "compute", "pci:example-a");
    let p = parent(&pool, id);
    pool.queue_device_reset(id, p.revision, boot()).unwrap();
    let p = parent(&pool, id);
    let p = pool.claim_device_reset(id, p.revision, boot()).unwrap();
    let now: jiff::Timestamp = "2026-01-01T00:00:00Z".parse().unwrap();
    let evidence = QualificationEvidence::DevelopmentResetV1 {
        profile: p.profile.clone(),
        procedure: p.procedure,
        host_boot_id: boot(),
        members: p.members.clone(),
        reset_started_at: now,
        completed_at: now,
    };
    let result = pool.finish_device_reset(&p, Ok(evidence));
    if cfg!(feature = "gpu-development") {
        assert!(result.unwrap());
        assert_eq!(parent(&pool, id).state, ParentState::Ready);
        pool.quarantine_devices_from_other_boots(
            "00000000-0000-4000-8000-000000000099".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(parent(&pool, id).state, ParentState::Quarantined);
    } else {
        assert!(matches!(result, Err(DeviceStoreError::Ownership)));
        assert_eq!(parent(&pool, id).state, ParentState::Sanitizing);
    }
}

#[test]
fn capacity_snapshot_tracks_admission_and_retains_unresolved_owners() {
    let pool = pool();
    let id = enroll(&pool, "compute", "pci:example-capacity");
    let snapshot = pool.device_capacity_snapshot().unwrap();
    assert!(!snapshot[0].allocatable(boot(), true));
    qualify(&pool, id);
    let snapshot = pool.device_capacity_snapshot().unwrap();
    assert!(snapshot[0].allocatable(boot(), true));
    assert!(snapshot[0].allocations().is_empty()); // Released sanitizer history excluded.
    assert!(!snapshot[0].allocatable(boot(), false));
    let other_boot = "00000000-0000-4000-8000-000000000002".parse().unwrap();
    assert!(!snapshot[0].allocatable(other_boot, true));
    let owner = owner(&pool);
    let required = requirements(&[("compute", 1)]);
    let preferences = BTreeMap::from([("compute".parse().unwrap(), 0)]);
    pool.reserve_device_parents(owner, boot(), &required, &preferences, |_| false)
        .unwrap_err();
    let allocation = pool
        .reserve_for_test(owner, boot(), &required)
        .unwrap()
        .remove(0);
    for expected in [
        AllocationState::Reserved,
        AllocationState::Attaching,
        AllocationState::Attached,
        AllocationState::Releasing,
    ] {
        let current = refresh(&pool, &allocation);
        match expected {
            AllocationState::Attaching => {
                assert!(pool.begin_device_attachment(&current, boot()).unwrap());
            }
            AllocationState::Attached => {
                assert!(pool.confirm_device_attachment(&current, boot()).unwrap());
            }
            AllocationState::Releasing => {
                assert!(pool.begin_device_release(&current).unwrap());
            }
            _ => {}
        }
        let snapshot = pool.device_capacity_snapshot().unwrap();
        assert!(!snapshot[0].allocatable(boot(), true));
        assert_eq!(snapshot[0].allocations()[0].state(), expected);
        assert_eq!(
            snapshot[0].allocations()[0].assignment().lease_id(),
            allocation.assignment().lease_id()
        );
        assert!(
            matches!(snapshot[0].allocations()[0].holder(),DeviceHolder::Sandbox{owner:actual,..} if actual==owner)
        );
    }
    let current = refresh(&pool, &allocation);
    pool.finish_device_release(
        &current,
        parent(&pool, id).revision(),
        boot(),
        DeviceReleaseDisposition::QueueReset,
    )
    .unwrap();
    let snapshot = pool.device_capacity_snapshot().unwrap();
    assert_eq!(snapshot[0].parent().state(), ParentState::Sanitizing);
    assert!(!snapshot[0].allocatable(boot(), true));
    assert!(snapshot[0].allocations().is_empty());
}
