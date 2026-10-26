use super::*;
use crate::slot_pool::{NetworkPool, NetworkPoolConfig, NewLeaseMemoryReservation, devices::*};
use pplx_space_common_core::types::{Digest, SandboxActivationAttemptId};
use pplx_space_execution_spec::devices::{DeviceProfile, DeviceRequirement, DeviceRequirements};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
struct Exclusion(Arc<AtomicUsize>);
impl Drop for Exclusion {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[test]
fn device_admission_compares_live_observations_with_persisted_enrollment() {
    use pplx_space_execution_spec::devices::{
        DeviceLeaseId, ExclusiveDeviceAssignment, HostDeviceId,
    };
    let pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(1));
    let locator: HostDeviceId = "pci:0000:01:00.0".parse().unwrap();
    let identity = EnrollmentIdentity::LinuxPciV1 {
        locator: locator.clone(),
        vendor: 0x1234,
        device: 1,
        subsystem_vendor: 0x1234,
        subsystem_device: 2,
        topology_digest: Digest::sha256([2; 32]),
        sysfs_device: 1,
        sysfs_inode: 3,
    };
    let parent = pool
        .enroll_device_parent(
            &"compute".parse().unwrap(),
            "00000000-0000-4000-8000-000000000001".parse().unwrap(),
            Digest::sha256([1; 32]),
            &[(locator.clone(), identity.clone())],
        )
        .unwrap();
    let execution = DeviceExecution::new(
        Digest::sha256([3; 32]),
        vec![
            ExclusiveDeviceAssignment::new(
                DeviceLeaseId::new(),
                parent,
                "compute".parse().unwrap(),
                vec![locator.clone()],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    validate_enrolled_devices(&pool, &execution, |requested| {
        assert_eq!(requested, &locator);
        Ok(identity.clone())
    })
    .unwrap();
    for change in 0..8 {
        let mut observed = identity.clone();
        let EnrollmentIdentity::LinuxPciV1 {
            locator,
            vendor,
            device,
            subsystem_vendor,
            subsystem_device,
            topology_digest,
            sysfs_device,
            sysfs_inode,
        } = &mut observed;
        match change {
            0 => *locator = "pci:0000:02:00.0".parse().unwrap(),
            1 => *vendor += 1,
            2 => *device += 1,
            3 => *subsystem_vendor += 1,
            4 => *subsystem_device += 1,
            5 => *topology_digest = Digest::sha256([4; 32]),
            6 => *sysfs_device += 1,
            7 => *sysfs_inode += 1,
            _ => unreachable!(),
        }
        let error =
            validate_enrolled_devices(&pool, &execution, |_| Ok(observed.clone())).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
    let error = validate_enrolled_devices(&pool, &execution, |_| {
        Err(Status::unavailable("device read interrupted"))
    })
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(
        pool.device_parent(parent).unwrap().enrolled_members(),
        &[identity]
    );
}

#[tokio::test]
async fn interrupted_device_cleanup_keeps_claims_and_retries_without_releasing_clean_capacity() {
    let pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(4));
    pool.set_all_slots_ready_for_test();
    let sandbox = SandboxId::new();
    let execution = SandboxActivationAttemptId::new();
    pool.bind_execution(&sandbox.to_string(), ExecutionFence::new(execution, None))
        .unwrap();
    let slot = pool
        .allocate(
            &sandbox.to_string(),
            None,
            Some(NewLeaseMemoryReservation {
                memory_reservation_mib: 8192,
            }),
        )
        .await
        .unwrap();
    let boot: HostBootId = "00000000-0000-4000-8000-000000000001".parse().unwrap();
    let profile: DeviceProfile = "compute".parse().unwrap();
    let procedure = Digest::sha256([1; 32]);
    for locator in ["pci:0000:01:00.0", "pci:0000:02:00.0"] {
        let locator = locator.parse().unwrap();
        let identity = EnrollmentIdentity::LinuxPciV1 {
            locator,
            vendor: 0x1234,
            device: 1,
            subsystem_vendor: 0x1234,
            subsystem_device: 2,
            topology_digest: Digest::sha256([2; 32]),
            sysfs_device: 1,
            sysfs_inode: 1,
        };
        let parent = pool
            .enroll_device_parent(
                &profile,
                boot,
                procedure,
                &[(identity.locator().clone(), identity)],
            )
            .unwrap();
        // Synthetic host success exercises ownership only; this is not hardware qualification.
        let a = pool
            .begin_device_requalification(
                parent,
                pool.device_parent(parent).unwrap().revision(),
                boot,
                SanitizerAttempt::new(SandboxActivationAttemptId::new(), procedure),
            )
            .unwrap();
        assert!(pool.begin_device_attachment(&a, boot).unwrap());
        let a = pool.device_allocation(a.assignment().lease_id()).unwrap();
        assert!(pool.confirm_device_attachment(&a, boot).unwrap());
        let a = pool.device_allocation(a.assignment().lease_id()).unwrap();
        assert!(pool.begin_device_release(&a).unwrap());
        let a = pool.device_allocation(a.assignment().lease_id()).unwrap();
        assert!(
            pool.finish_device_sanitization(
                &a,
                pool.device_parent(parent).unwrap().revision(),
                boot,
                "2026-01-01T00:00:00Z".parse().unwrap()
            )
            .unwrap()
        );
    }
    let required =
        DeviceRequirements::new(vec![DeviceRequirement::new(profile.clone(), 2).unwrap()]).unwrap();
    let allocations = pool
        .reserve_device_parents(
            DeviceOwner::new(sandbox, execution, slot.generation),
            boot,
            &required,
            &BTreeMap::from([(profile, 0)]),
            |_| true,
        )
        .unwrap();
    let held = Arc::new(AtomicUsize::new(0));
    let mut observed = 0;
    let error = quarantine_allocations(&pool, allocations.clone(), boot, |devices| {
        let assignment = devices.assignments().first().unwrap();
        assert_eq!(
            pool.device_allocation(assignment.lease_id())
                .unwrap()
                .state(),
            AllocationState::Releasing
        );
        observed += 1;
        if observed == 2 {
            return Err(Status::unavailable("device still held"));
        }
        held.fetch_add(1, Ordering::SeqCst);
        Ok(Exclusion(held.clone()))
    })
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(held.load(Ordering::SeqCst), 0);
    let remaining = pool
        .device_cleanup_allocations(sandbox, Some(execution))
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(
        remaining.first().unwrap().state(),
        AllocationState::Releasing
    );
    pool.release(&sandbox.to_string()).unwrap_err();
    pool.retire_execution(&sandbox.to_string(), execution)
        .unwrap_err();
    let guards = quarantine_allocations(&pool, remaining, boot, |_| {
        held.fetch_add(1, Ordering::SeqCst);
        Ok(Exclusion(held.clone()))
    })
    .unwrap();
    for a in &allocations {
        assert_eq!(
            pool.device_allocation(a.assignment().lease_id())
                .unwrap()
                .state(),
            AllocationState::Released
        );
        assert_eq!(
            pool.device_parent(a.assignment().parent_id())
                .unwrap()
                .state(),
            ParentState::Quarantined
        );
    }
    pool.ensure_no_device_allocations(sandbox).unwrap();
    pool.release(&sandbox.to_string()).unwrap();
    pool.retire_execution(&sandbox.to_string(), execution)
        .unwrap();
    assert_eq!(
        held.load(Ordering::SeqCst),
        1,
        "exclusion survives ownership retirement"
    );
    drop(guards);
    assert_eq!(held.load(Ordering::SeqCst), 0);
}
