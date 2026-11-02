use super::*;

#[test]
fn explicit_capabilities_never_fall_back_to_version_defaults() {
    let mut descriptor = GuestPlatform::linux().wire();
    descriptor.operations = vec![9999];
    let platform = GuestPlatform::parse(descriptor, GuestOs::Linux).unwrap();
    assert!(!platform.supports(GuestOperation::Process));
    assert!(!platform.supports(GuestOperation::InteractiveProcessControl));
    assert!(!platform.supports(GuestOperation::Unspecified));
}

#[test]
fn readiness_requires_matching_platform_and_host_contract() {
    for os in [GuestOs::Linux, GuestOs::Windows] {
        for revision in [0, HOST_CONTRACT_REVISION, 99] {
            assert_eq!(
                require_host_contract(os, revision).is_ok(),
                os == GuestOs::Linux || revision >= 1
            );
        }
        assert_eq!(
            GuestPlatform::parse(GuestPlatform::linux().wire(), os).is_ok(),
            os == GuestOs::Linux
        );
    }
    assert!(require_host_contract(GuestOs::Unspecified, 99).is_err());
    let target = GuestPlatform::for_target().wire();
    assert_eq!(target.os, target_os() as i32);
    if target_os() != GuestOs::Linux {
        assert!(target.operations.is_empty());
    }
}
