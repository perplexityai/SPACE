use super::*;
use crate::emit::test_capture::{CapturedValue, capture_values};
use crate::sandbox::api::{ClientIdTag, scope_request_client_id};

fn in_request_scope(client_id: ClientIdTag, emit: impl FnOnce()) -> Vec<CapturedValue> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(scope_request_client_id(client_id, async {
            capture_values(emit)
        }))
}

fn sorted_tags(value: &CapturedValue) -> Vec<&str> {
    let mut tags: Vec<&str> = value.tags.iter().map(String::as_str).collect();
    tags.sort_unstable();
    tags
}

fn sample(kind: RequestedKind, memory_mib: u32, defaulted: Option<bool>) -> RequestedSize<'static> {
    RequestedSize {
        kind,
        surface: SandboxApiSurface::Native,
        admission: AdmissionDecision::Accepted,
        template: "base",
        vcpu: 2,
        memory_mib,
        defaulted,
    }
}

#[test]
fn create_emits_both_sizes_with_defaulted_and_client() {
    let captured = in_request_scope(ClientIdTag::allowlisted("synthetic-client"), || {
        requested(RequestedSize {
            kind: RequestedKind::Create,
            surface: SandboxApiSurface::Ecw,
            admission: AdmissionDecision::Rejected,
            template: "base",
            vcpu: 4,
            memory_mib: 2048,
            defaulted: Some(true),
        });
    });

    let names: Vec<&str> = captured.iter().map(|value| value.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "sandbox.api.requested.vcpu",
            "sandbox.api.requested.memory_bytes"
        ]
    );
    assert_eq!(captured[0].value, 4.0);
    assert_eq!(captured[1].value, 2048.0 * 1_048_576.0);
    for value in &captured {
        assert_eq!(
            sorted_tags(value),
            [
                "admission:rejected",
                "client_id:synthetic-client",
                "defaulted:true",
                "kind:create",
                "surface:ecw",
                "template:base",
            ]
        );
    }
}

#[test]
fn activation_omits_defaulted() {
    let captured = capture_values(|| requested(sample(RequestedKind::Activate, 1024, None)));

    assert_eq!(captured.len(), 2, "{captured:?}");
    for value in &captured {
        assert_eq!(
            sorted_tags(value),
            [
                "admission:accepted",
                "client_id:unknown",
                "kind:activate",
                "surface:native",
                "template:base",
            ]
        );
    }
}

#[test]
fn memory_bytes_does_not_overflow_at_the_largest_size() {
    let captured = capture_values(|| {
        requested(sample(RequestedKind::Create, u32::MAX, Some(false)));
    });

    assert_eq!(captured[1].value, (u64::from(u32::MAX) * 1_048_576) as f64);
}
