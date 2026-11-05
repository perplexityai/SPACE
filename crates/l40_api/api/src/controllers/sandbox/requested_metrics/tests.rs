use pplx_space_metrics::test_capture::{CapturedValue, capture_values};

use super::*;

const MIB: f64 = 1_048_576.0;

fn resources(vcpu: Option<u32>, memory_mib: Option<u32>) -> SandboxResources {
    SandboxResources {
        vcpu,
        memory_mib,
        disk_mib: None,
    }
}

/// `(name, value, sorted tags)` per captured sample.
fn samples(captured: &[CapturedValue]) -> Vec<(&str, f64, Vec<&str>)> {
    captured
        .iter()
        .map(|value| {
            let mut tags: Vec<&str> = value.tags.iter().map(String::as_str).collect();
            tags.sort_unstable();
            (value.name.as_str(), value.value, tags)
        })
        .collect()
}

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

#[test]
fn create_records_its_resolved_size_and_shape_source() {
    let captured = capture_values(|| {
        record_create(
            SandboxApiSurface::Ecw,
            AdmissionDecision::Rejected,
            "base",
            &resources(Some(4), Some(2048)),
            Some(CreateShapeSource::Request),
        );
        record_create(
            SandboxApiSurface::Native,
            AdmissionDecision::Accepted,
            "base",
            &resources(Some(2), Some(1024)),
            Some(CreateShapeSource::Build),
        );
    });

    let tags = |admission: &'static str, defaulted: &'static str, surface: &'static str| {
        vec![
            admission,
            "client_id:unknown",
            defaulted,
            "kind:create",
            surface,
            "template:base",
        ]
    };
    assert_eq!(
        samples(&captured),
        [
            (
                "sandbox.api.requested.vcpu",
                4.0,
                tags("admission:rejected", "defaulted:false", "surface:ecw")
            ),
            (
                "sandbox.api.requested.memory_bytes",
                2048.0 * MIB,
                tags("admission:rejected", "defaulted:false", "surface:ecw")
            ),
            (
                "sandbox.api.requested.vcpu",
                2.0,
                tags("admission:accepted", "defaulted:true", "surface:native")
            ),
            (
                "sandbox.api.requested.memory_bytes",
                1024.0 * MIB,
                tags("admission:accepted", "defaulted:true", "surface:native")
            ),
        ]
    );
}

#[test]
fn create_without_a_resolved_shape_or_size_records_nothing() {
    let captured = capture_values(|| {
        // A direct create, fork or restore: the API resolved no shape.
        record_create(
            SandboxApiSurface::Native,
            AdmissionDecision::Accepted,
            "base",
            &resources(Some(2), Some(1024)),
            None,
        );
        // The platform default left the size to the node.
        record_create(
            SandboxApiSurface::Native,
            AdmissionDecision::Rejected,
            "base",
            &resources(None, None),
            Some(CreateShapeSource::Default),
        );
        record_create(
            SandboxApiSurface::Native,
            AdmissionDecision::Accepted,
            "base",
            &resources(Some(2), None),
            Some(CreateShapeSource::Build),
        );
    });

    assert!(captured.is_empty(), "{captured:?}");
}
