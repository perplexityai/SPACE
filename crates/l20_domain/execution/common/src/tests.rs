use super::*;

#[test]
fn unspecified_enum_values_are_rejected() {
    let wire = proto::ExecutionMetadata {
        runtime: Some(proto::RuntimeIdentity {
            kind: proto::RuntimeKind::Unspecified as i32,
            version: "1.15.1".to_string(),
        }),
        guest: Some(proto::GuestPlatform {
            os: proto::GuestOperatingSystem::Linux as i32,
            architecture: proto::CpuArchitecture::Aarch64 as i32,
        }),
        state_format: None,
        components: vec![],
        environment: None,
    };

    assert!(matches!(
        ExecutionMetadata::try_from(&wire),
        Err(ExecutionProtoError::UnspecifiedRuntimeKind)
    ));
}
