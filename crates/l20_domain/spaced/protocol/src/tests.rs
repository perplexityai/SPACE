use pplx_space_spaced_guest_protocol::spaced_compatibility_family_for_version;

use super::*;

#[test]
fn compatibility_family_follows_semver_stability() {
    assert_eq!(
        spaced_compatibility_family_for_version("0.0.3").unwrap(),
        "0.0.3"
    );
    assert_eq!(
        spaced_compatibility_family_for_version("0.1.0").unwrap(),
        "0.1"
    );
    assert_eq!(
        spaced_compatibility_family_for_version("0.1.9").unwrap(),
        "0.1"
    );
    assert_eq!(
        spaced_compatibility_family_for_version("0.2.0-alpha.1").unwrap(),
        "0.2"
    );
    assert_eq!(
        spaced_compatibility_family_for_version("1.2.3").unwrap(),
        "1"
    );
    assert_eq!(
        spaced_compatibility_family_for_version("1.9.0+build.5").unwrap(),
        "1"
    );
    assert_eq!(
        spaced_compatibility_family_for_version("2.0.0").unwrap(),
        "2"
    );
}

#[test]
// SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: retire with the version predicate.
fn interactive_process_control_requires_guest_version_with_bridge_rpcs() {
    assert!(!spaced_guest_supports_interactive_process_control("0.1.1").unwrap());
    assert!(!spaced_guest_supports_interactive_process_control("0.1.2-alpha.1").unwrap());
    assert!(spaced_guest_supports_interactive_process_control("0.1.2").unwrap());
    assert!(spaced_guest_supports_interactive_process_control("0.1.3").unwrap());
}

#[test]
fn lifecycle_qualification_agrees_with_the_guest_operation_allowlist() {
    use pplx_space_execution_spec::guest_image::QualifiedGuestAgent;
    use pplx_space_spaced_guest_protocol::proto::platform::GuestOperation;
    for agent in [
        QualifiedGuestAgent::Absent,
        QualifiedGuestAgent::LinuxV1,
        QualifiedGuestAgent::WindowsV1,
    ] {
        let allowed = allowed_guest_operations(agent);
        assert_eq!(
            agent.supports_process_environment(),
            allowed.contains(&GuestOperation::ProcessEnvironment)
        );
        assert_eq!(
            agent.supports_filesystem_flush(),
            allowed.contains(&GuestOperation::Flush)
        );
        assert_eq!(
            agent.supports_memory_lifecycle(),
            allowed.contains(&GuestOperation::Flush) && allowed.contains(&GuestOperation::Resume)
        );
    }
}
