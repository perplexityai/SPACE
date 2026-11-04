use super::*;

#[test]
fn retryable_status_codes_match_bridge_policy() {
    assert!(status_is_retryable(&tonic::Status::unavailable("retry")));
    assert!(status_is_retryable(&tonic::Status::unknown("retry")));
    assert!(status_is_retryable(&tonic::Status::deadline_exceeded(
        "retry"
    )));
    assert!(!status_is_retryable(&tonic::Status::invalid_argument("no")));
}

// A refusal the guest made before starting anything goes back to the caller,
// whatever its code: the hop does not retry it on the caller's behalf.
#[test]
fn refused_before_start_is_never_retried_by_a_hop() {
    let mut refused = tonic::Status::unavailable("guest shutting down");
    refused.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(
            pplx_space_common_core::spaced::REQUEST_REFUSED,
        ),
    );
    assert!(!status_is_retryable(&refused));
    assert!(status_is_retryable(&tonic::Status::unavailable(
        "transport closed"
    )));
}

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
    use pplx_space_common_core::types::guest_image::QualifiedGuestAgent;
    use proto::platform::GuestOperation;
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
