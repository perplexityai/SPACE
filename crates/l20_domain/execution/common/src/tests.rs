use super::*;

#[test]
fn block_volume_binding_geometry_preserves_legacy_capacity_without_defaulting_invalid_sizes() {
    for (wire_size, expected) in [
        (0, Some(128)),
        (128, Some(128)),
        (256, Some(256)),
        (129, None),
    ] {
        let binding = proto::BlockVolumeBinding {
            size_gib: wire_size,
            ..Default::default()
        };
        assert_eq!(binding.capacity().ok().map(u32::from), expected);
    }
}

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
