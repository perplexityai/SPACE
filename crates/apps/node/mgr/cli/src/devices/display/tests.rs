use super::*;

fn parent() -> DeviceParentRecord {
    DeviceParentRecord {
        parent_id: "parent".into(),
        profile: "compute".into(),
        host_device_ids: vec!["pci:example".into()],
        state: DeviceParentState::Ready as i32,
        usage: Some(DeviceParentUsage {
            allocatable: true,
            allocations: vec![],
        }),
        ..Default::default()
    }
}
fn allocation() -> DeviceAllocationRecord {
    DeviceAllocationRecord {
        assignment: Some(
            pplx_space_execution_common::proto::ExclusiveDeviceAssignment {
                lease_id: "lease".into(),
                parent_id: "parent".into(),
                profile: "compute".into(),
                host_device_ids: vec!["pci:example".into()],
            },
        ),
        state: DeviceAllocationState::Attached as i32,
        execution_id: "execution".into(),
        owner: Some(device_allocation_record::Owner::SandboxId("sandbox".into())),
    }
}
#[test]
fn available_capacity_requires_positive_complete_unheld_evidence() {
    let p = parent();
    assert_eq!(capacity(&p), Capacity::Available);
    let mut old = p.clone();
    old.usage = None;
    assert_eq!(capacity(&old), Capacity::Unknown);
    for (state, expected) in [
        (DeviceParentState::Ready, Capacity::Unavailable),
        (DeviceParentState::Sanitizing, Capacity::Cleaning),
        (DeviceParentState::Quarantined, Capacity::Quarantined),
        (DeviceParentState::Retired, Capacity::Unavailable),
        (DeviceParentState::Unspecified, Capacity::Unknown),
    ] {
        let mut p = p.clone();
        p.state = state as i32;
        p.usage.as_mut().unwrap().allocatable = false;
        assert_eq!(capacity(&p), expected);
    }
    let mut held = p.clone();
    held.usage.as_mut().unwrap().allocations.push(allocation());
    assert_eq!(capacity(&held), Capacity::Unknown); // Contradictory free claim.
    held.usage.as_mut().unwrap().allocatable = false;
    for (state, expected) in [
        (DeviceAllocationState::Reserved, Capacity::Allocated),
        (DeviceAllocationState::Attaching, Capacity::Allocated),
        (DeviceAllocationState::Attached, Capacity::Allocated),
        (DeviceAllocationState::Releasing, Capacity::Allocated),
        (DeviceAllocationState::Quarantined, Capacity::Quarantined),
        (DeviceAllocationState::Unspecified, Capacity::Unknown),
    ] {
        held.usage.as_mut().unwrap().allocations[0].state = state as i32;
        assert_eq!(capacity(&held), expected);
    }
    held.usage.as_mut().unwrap().allocations[0] = allocation();
    assert!(tables(&[held.clone()]).contains("sandbox:sandbox"));
    held.usage.as_mut().unwrap().allocations[0].owner = None;
    assert_eq!(capacity(&held), Capacity::Unknown);
    held.usage.as_mut().unwrap().allocations[0] = allocation();
    held.usage.as_mut().unwrap().allocations[0].state = 99;
    assert_eq!(capacity(&held), Capacity::Unknown);
    held.usage.as_mut().unwrap().allocations[0] = allocation();
    held.state = 99;
    assert_eq!(capacity(&held), Capacity::Unknown);
}
