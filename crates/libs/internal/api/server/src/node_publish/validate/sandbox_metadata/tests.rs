use super::*;
use crate::node_publish::validate::fields::execution_projection;
use pplx_space_common_core::types::{Arch, ExecutionMetadata};

#[test]
fn runtime_labels_stay_literal_and_legacy_labels_remain_open() {
    let execution = ExecutionMetadata::initial_firecracker(Arch::X86_64, Vec::new());
    let proto = (&execution).into();
    let projection = execution_projection(Some(&proto)).unwrap().unwrap();
    for (raw, expected) in [(" fc ", "fc"), (" firecracker ", "firecracker")] {
        let label = PublishedRuntimeLabel::try_from_proto(Some(raw), Some(&projection))
            .unwrap()
            .unwrap();
        assert_eq!(label.as_str(), expected);
    }
    assert!(
        PublishedRuntimeLabel::try_from_proto(Some(" \t"), Some(&projection))
            .unwrap()
            .is_none()
    );
    let label = PublishedRuntimeLabel::try_from_proto(Some(" future-runtime "), None)
        .unwrap()
        .unwrap();
    assert_eq!(label.as_str(), "future-runtime");
}

#[test]
fn runtime_and_authority_rejections_keep_the_existing_contract() {
    let execution = ExecutionMetadata::initial_firecracker(Arch::X86_64, Vec::new());
    let proto = (&execution).into();
    let projection = execution_projection(Some(&proto)).unwrap().unwrap();
    for (runtime, message) in [
        (
            "gvisor",
            "published sandbox runtime disagrees with execution metadata",
        ),
        ("future-runtime", "unsupported runtime 'future-runtime'"),
    ] {
        let error =
            PublishedRuntimeLabel::try_from_proto(Some(runtime), Some(&projection)).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), message);
    }
    let error = policy_authority(i32::MAX).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        error.message(),
        PolicyAuthority::from_optional_proto(i32::MAX)
            .unwrap_err()
            .to_string()
    );
    assert_eq!(policy_authority(0).unwrap(), None);
}
