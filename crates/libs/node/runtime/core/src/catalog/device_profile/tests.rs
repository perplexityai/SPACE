use super::*;
use pplx_space_common_core::types::Digest;
use pplx_space_execution_spec::devices::{
    DeviceLeaseId, DeviceParentId, DeviceRequirement, ExclusiveDeviceAssignment,
};

fn execution(profiles: &[&str]) -> DeviceExecution {
    DeviceExecution::new(
        Digest::sha256([1; 32]),
        profiles
            .iter()
            .enumerate()
            .map(|(i, p)| {
                ExclusiveDeviceAssignment::new(
                    DeviceLeaseId::new(),
                    DeviceParentId::new(),
                    p.parse().unwrap(),
                    vec![format!("pci:0000:01:{i:02x}.0").parse().unwrap()],
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

#[test]
fn environment_qualification_and_full_memory_budget_are_required_for_every_assignment() {
    let profiles = vec![
        ExclusiveDeviceProfile::new(
            "small".parse().unwrap(),
            2,
            1024,
            256,
            Digest::sha256([1; 32]),
            DedicatedPciHardware::new(vec![[0x1234, 1, 0x1234, 2]], 1).unwrap(),
        )
        .unwrap(),
        ExclusiveDeviceProfile::new(
            "large".parse().unwrap(),
            1,
            4096,
            512,
            Digest::sha256([2; 32]),
            DedicatedPciHardware::new(vec![[0x1234, 1, 0x1234, 2]], 1).unwrap(),
        )
        .unwrap(),
    ];
    assert!(matches!(
        resources(&[], &execution(&["small"])),
        Err(DeviceProfileError::Unqualified(_))
    ));
    assert!(matches!(
        resources(&profiles, &execution(&["small", "unknown"])),
        Err(DeviceProfileError::Unqualified(_))
    ));
    assert!(matches!(
        resources(&profiles, &execution(&["large", "large"])),
        Err(DeviceProfileError::AllocationCount { .. })
    ));
    let limits = resources(&profiles, &execution(&["small", "small", "large"])).unwrap();
    assert!(matches!(
        limits.check_guest_memory(2048, 16384),
        Err(DeviceProfileError::GuestMemory { .. })
    ));
    assert!(matches!(
        limits.check_guest_memory(8192, 8192),
        Err(DeviceProfileError::HostMemory { .. })
    ));
    limits.check_guest_memory(8192, 9216).unwrap();
    assert_eq!(
        limits.locked_memory_bytes(9216).unwrap(),
        9216 * 1024 * 1024
    );
    limits.locked_memory_bytes(4096).unwrap_err();
    limits.locked_memory_bytes(u64::MAX).unwrap_err();
    limits.check_guest_memory(u64::MAX, u64::MAX).unwrap_err();
    for (count, guest, overhead) in [
        (0, 1, 1),
        (17, 1, 1),
        (1, 0, 1),
        (1, 1, 0),
        (16, u64::MAX, 1),
    ] {
        ExclusiveDeviceProfile::new(
            "invalid".parse().unwrap(),
            count,
            guest,
            overhead,
            Digest::sha256([1; 32]),
            DedicatedPciHardware::new(vec![[0x1234, 1, 0x1234, 2]], 1).unwrap(),
        )
        .unwrap_err();
    }

    let requirements = DeviceRequirements::new(vec![
        DeviceRequirement::new("small".parse().unwrap(), 1).unwrap(),
        DeviceRequirement::compatible(vec!["small".parse().unwrap(), "large".parse().unwrap()], 1)
            .unwrap(),
    ])
    .unwrap();
    let reserved = requirement_resources(&profiles, &requirements).unwrap();
    assert_eq!(reserved.required_host_memory_mib(8192).unwrap(), 8960);
    for selection in [["small", "small"], ["small", "large"]] {
        resources(&profiles, &execution(&selection))
            .unwrap()
            .check_guest_memory(8192, 8960)
            .unwrap();
    }
    requirement_resources(&profiles[..1], &requirements).unwrap_err();
    let excessive = DeviceRequirements::new(vec![
        DeviceRequirement::new("large".parse().unwrap(), 2).unwrap(),
    ])
    .unwrap();
    assert!(matches!(
        requirement_resources(&profiles, &excessive),
        Err(DeviceProfileError::ClauseCount { count: 2 })
    ));
}

#[test]
fn dedicated_profiles_check_the_complete_function_set_and_guest_visible_count() {
    let compute = [0x1234, 1, 0x1234, 2];
    let auxiliary = [0x1234, 3, 0x1234, 4];
    let profile = ExclusiveDeviceProfile::new(
        "compute-with-aux".parse().unwrap(),
        1,
        1024,
        256,
        Digest::sha256([1; 32]),
        DedicatedPciHardware::new(vec![compute, auxiliary], 1).unwrap(),
    )
    .unwrap();
    assert_eq!(profile.hardware().guest_visible_devices(), 1);
    profile.check_pci_hardware(&[auxiliary, compute]).unwrap();
    for observed in [
        vec![],
        vec![compute],
        vec![compute, compute],
        vec![compute, auxiliary, compute],
        vec![[0xabcd, 1, 0x1234, 2], auxiliary],
    ] {
        assert!(matches!(
            profile.check_pci_hardware(&observed),
            Err(DeviceProfileError::HardwareMismatch(_))
        ));
    }
    for (functions, count) in [(vec![], 1), (vec![compute], 0), (vec![compute], 2)] {
        DedicatedPciHardware::new(functions, count).unwrap_err();
    }
    // One assignment is not permission to expose an arbitrary number of PCI
    // functions: the exact same profile is checked before any host preparation.
    assert!(matches!(
        resources(&[profile], &execution(&["compute-with-aux"])),
        Err(DeviceProfileError::HardwareMismatch(_))
    ));
}
