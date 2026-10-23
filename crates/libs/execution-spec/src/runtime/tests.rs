use super::*;

#[test]
fn runtime_kind_parses_aliases_and_displays_full_names() {
    assert_eq!(RuntimeKind::parse("fc").unwrap(), RuntimeKind::Firecracker);
    assert_eq!(
        RuntimeKind::parse("firecracker").unwrap().to_string(),
        RUNTIME_FIRECRACKER
    );
    assert_eq!(RuntimeKind::Firecracker.alias(), RUNTIME_FIRECRACKER_ALIAS);

    assert_eq!(
        RuntimeKind::parse("cloud-hypervisor").unwrap(),
        RuntimeKind::CloudHypervisor
    );
    assert_eq!(
        RuntimeKind::parse("ch").unwrap(),
        RuntimeKind::CloudHypervisor
    );
    assert_eq!(
        RuntimeKind::CloudHypervisor.to_string(),
        RUNTIME_CLOUD_HYPERVISOR
    );
    assert_eq!(
        RuntimeKind::CloudHypervisor.alias(),
        RUNTIME_CLOUD_HYPERVISOR_ALIAS
    );
    RuntimeKind::parse("cloud_hypervisor").unwrap_err();

    assert_eq!(RuntimeKind::parse("gvisor").unwrap(), RuntimeKind::Gvisor);
    assert_eq!(RuntimeKind::parse("gv").unwrap(), RuntimeKind::Gvisor);
    assert_eq!(RuntimeKind::Gvisor.to_string(), RUNTIME_GVISOR);
    assert_eq!(RuntimeKind::Gvisor.alias(), RUNTIME_GVISOR_ALIAS);

    RuntimeKind::parse("unknown").unwrap_err();
}

#[test]
fn runtime_kind_exposes_baseline_capabilities() {
    assert!(RuntimeKind::Firecracker.supports_startup(StartupPolicy::RequireBaseSnapshot));
    assert!(RuntimeKind::CloudHypervisor.supports_startup(StartupPolicy::RequireWarmPool));
    assert!(!RuntimeKind::Gvisor.supports_startup(StartupPolicy::RequireBaseSnapshot));
    assert!(!RuntimeKind::Gvisor.supports_startup(StartupPolicy::RequireWarmPool));

    RuntimeKind::Firecracker
        .validate_startup(StartupPolicy::RequireBaseSnapshot)
        .unwrap();
    let err = RuntimeKind::Gvisor
        .validate_startup(StartupPolicy::RequireBaseSnapshot)
        .unwrap_err();
    assert_eq!(err.runtime(), RuntimeKind::Gvisor);
    assert_eq!(err.policy(), StartupPolicy::RequireBaseSnapshot);
}

#[test]
fn runtime_capabilities_match_required_capabilities() {
    let have = RuntimeCapabilities {
        snapshot: true,
        network_gateway: true,
        ..RuntimeCapabilities::default()
    };

    assert!(have.contains(StartupPolicy::RequireBaseSnapshot.required_capabilities()));
    assert!(!have.contains(StartupPolicy::RequireWarmPool.required_capabilities()));
}
