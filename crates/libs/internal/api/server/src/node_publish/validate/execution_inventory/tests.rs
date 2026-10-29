use super::*;
use tonic::Code;

fn runtime() -> proto::RuntimeInstallation {
    proto::RuntimeInstallation {
        runtime: Some(proto::RuntimeIdentity {
            kind: proto::RuntimeKind::Firecracker as i32,
            version: "1.15.1".to_string(),
        }),
        ..proto::RuntimeInstallation::default()
    }
}

#[test]
fn missing_inventory_identities_keep_their_field_specific_errors() {
    let cases = [
        (
            proto::NodeExecutionInventory {
                runtimes: vec![proto::RuntimeInstallation::default()],
                ..proto::NodeExecutionInventory::default()
            },
            "runtime installation is missing identity",
        ),
        (
            proto::NodeExecutionInventory {
                runtimes: vec![proto::RuntimeInstallation {
                    state_formats: vec![proto::StateFormatCapability::default()],
                    ..runtime()
                }],
                ..proto::NodeExecutionInventory::default()
            },
            "state format capability is missing identity",
        ),
        (
            proto::NodeExecutionInventory {
                components: vec![proto::ArtifactComponentInstallation::default()],
                ..proto::NodeExecutionInventory::default()
            },
            "artifact component installation is missing component",
        ),
    ];
    for (inventory, message) in cases {
        let error = ValidatedExecutionInventory::try_from_proto(&inventory).unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(error.message(), message);
    }
}

#[test]
fn invalid_inventory_values_keep_the_domain_error_message() {
    let invalid_runtime = proto::RuntimeIdentity::default();
    let invalid_guest = proto::GuestPlatform::default();
    let invalid_format = proto::StateFormatIdentity::default();
    let invalid_component = proto::ArtifactComponent::default();
    let cases = [
        (
            proto::NodeExecutionInventory {
                runtimes: vec![proto::RuntimeInstallation {
                    runtime: Some(invalid_runtime.clone()),
                    ..runtime()
                }],
                ..proto::NodeExecutionInventory::default()
            },
            RuntimeIdentity::try_from(&invalid_runtime)
                .unwrap_err()
                .to_string(),
        ),
        (
            proto::NodeExecutionInventory {
                runtimes: vec![proto::RuntimeInstallation {
                    supported_guests: vec![invalid_guest],
                    ..runtime()
                }],
                ..proto::NodeExecutionInventory::default()
            },
            GuestPlatform::try_from(&invalid_guest)
                .unwrap_err()
                .to_string(),
        ),
        (
            proto::NodeExecutionInventory {
                runtimes: vec![proto::RuntimeInstallation {
                    state_formats: vec![proto::StateFormatCapability {
                        identity: Some(invalid_format.clone()),
                        ..proto::StateFormatCapability::default()
                    }],
                    ..runtime()
                }],
                ..proto::NodeExecutionInventory::default()
            },
            StateFormatIdentity::try_from(&invalid_format)
                .unwrap_err()
                .to_string(),
        ),
        (
            proto::NodeExecutionInventory {
                components: vec![proto::ArtifactComponentInstallation {
                    component: Some(invalid_component.clone()),
                    ..proto::ArtifactComponentInstallation::default()
                }],
                ..proto::NodeExecutionInventory::default()
            },
            ArtifactComponentMetadata::try_from(&invalid_component)
                .unwrap_err()
                .to_string(),
        ),
    ];
    for (inventory, message) in cases {
        let error = ValidatedExecutionInventory::try_from_proto(&inventory).unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(error.message(), message);
    }
}
