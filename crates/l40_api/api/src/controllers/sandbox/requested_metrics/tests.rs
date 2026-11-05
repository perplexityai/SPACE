use super::*;

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
    assert_eq!(effective(Some(2_u32), Some(1024_u32)), Some((2, 1024)));
    assert_eq!(effective(Some(2_i32), Some(1024_i32)), Some((2, 1024)));
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

/// Which errors count as admission refusals is pinned where they are built
/// (`lifecycle::tests::admission_refusals_keep_their_dispatcher_retry_causes`).
#[test]
fn only_cluster_admission_refusals_are_rejected() {
    let ok: Result<(), ControllerError> = Ok(());
    assert_eq!(admission(&ok), Some(AdmissionDecision::Accepted));

    for rate_limited in [true, false] {
        let error = ControllerError::cluster_admission_rejected("refused", None, rate_limited);
        assert_eq!(
            admission::<()>(&Err(error)),
            Some(AdmissionDecision::Rejected),
            "rate_limited:{rate_limited}"
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
