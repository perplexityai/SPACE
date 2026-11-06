use super::*;
use crate::controllers::substrate::ActivationRejectionKind;

#[test]
fn only_a_request_shape_is_not_defaulted() {
    assert!(!defaulted(CreateShapeSource::Request));
    for source in [
        CreateShapeSource::PoolConfig,
        CreateShapeSource::Policy,
        CreateShapeSource::Build,
        CreateShapeSource::Default,
    ] {
        assert!(defaulted(source), "{source:?}");
    }
}

#[test]
fn effective_size_needs_both_values_positive() {
    assert_eq!(effective(Some(2), Some(1024)), Some((2, 1024)));
    for (vcpu, memory_mib) in [
        (None, Some(1024)),
        (Some(2), None),
        (Some(0), Some(1024)),
        (Some(2), Some(0)),
        (Some(-2), Some(1024)),
        (Some(2), Some(-1024)),
    ] {
        assert_eq!(effective(vcpu, memory_mib), None, "{vcpu:?} {memory_mib:?}");
    }
}

/// A create refuses a size its row cannot store, after admission; that size
/// is not a requested one.
#[test]
fn create_size_is_limited_to_what_the_create_stores() {
    let resources = |vcpu, memory_mib| SandboxResources {
        vcpu: Some(vcpu),
        memory_mib: Some(memory_mib),
        disk_mib: None,
    };
    // i32::MAX, the largest size a sandbox row stores.
    let largest = 2_147_483_647_u32;
    assert_eq!(
        create_size(&resources(largest, largest)),
        Some((largest, largest))
    );
    assert_eq!(create_size(&resources(largest + 1, 1024)), None);
    assert_eq!(create_size(&resources(2, largest + 1)), None);
}

/// Which errors count as admission refusals is pinned where they are built
/// (`lifecycle::tests::admission_refusals_keep_their_dispatcher_retry_causes`).
#[test]
fn only_cluster_admission_refusals_are_rejected() {
    let ok: Result<(), ControllerError> = Ok(());
    assert_eq!(admission(&ok), Some(AdmissionDecision::Accepted));

    for kind in [
        ActivationRejectionKind::RateLimited,
        ActivationRejectionKind::EmptyCluster,
        ActivationRejectionKind::NoFreeSlot,
    ] {
        let error = kind.controller_error("refused".into(), None);
        assert_eq!(
            admission::<()>(&Err(error)),
            Some(AdmissionDecision::Rejected),
            "{kind:?}"
        );
    }

    // A 429 from anything but cluster admission, such as a per-client or
    // quota limiter, is not a rejected activation.
    for error in [
        ControllerError::rate_limited("client concurrency limit"),
        ControllerError::conflict("sandbox is deleting"),
        ControllerError::service_unavailable("runtime unreachable"),
    ] {
        assert_eq!(admission::<()>(&Err(error)), None);
    }
}
