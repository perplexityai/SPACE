use std::str::FromStr;

use super::*;
use crate::types::Arch;

#[test]
fn execution_identity_is_nested_in_sandbox_metadata() {
    let metadata = SandboxArtifactMetadata::new(
        SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap(),
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    );

    let json = serde_json::to_value(metadata).unwrap();
    assert_eq!(json["schema_version"], 2);
    assert_eq!(json["execution"]["runtime"]["kind"], "firecracker");
    assert_eq!(json["execution"]["runtime"]["version"], "1.15.1");
    assert_eq!(json["execution"]["state_format"]["version"], "9.0.0");
}

#[test]
fn unsupported_schema_version_is_rejected() {
    let json = r#"{
            "schema_version": 1,
            "sandbox_id": "019d84bd-280d-7331-99c6-b63377f3a3a2",
            "execution": {
                "runtime": { "kind": "firecracker", "version": "1.15.1" },
                "guest": { "os": "linux", "architecture": "aarch64" },
                "components": []
            },
            "template": "seed"
        }"#;
    serde_json::from_str::<SandboxArtifactMetadata>(json).unwrap_err();
}

#[test]
fn sandbox_cpu_budget_round_trips_without_changing_legacy_shape() {
    let legacy = serde_json::to_value(SandboxArtifactMetadata::new(
        SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap(),
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    ))
    .unwrap();
    for budget in [None, Some(8), Some(0)] {
        let mut value = legacy.clone();
        if let Some(budget) = budget {
            value["cpu_budget"] = budget.into();
        }
        let parsed = serde_json::from_value::<SandboxArtifactMetadata>(value.clone());
        if budget == Some(0) {
            assert!(parsed.is_err(), "zero is not a CPU allocation");
        } else {
            assert_eq!(serde_json::to_value(parsed.unwrap()).unwrap(), value);
        }
    }
}

#[test]
fn device_metadata_cannot_be_downgraded_or_lose_requirements() {
    use pplx_space_execution_spec::devices::{DeviceRequirement, DeviceRequirements};
    let requirements = DeviceRequirements::new(vec![
        DeviceRequirement::new("compute".parse().unwrap(), 1).unwrap(),
    ])
    .unwrap();
    let metadata = SandboxArtifactMetadata::new(
        "019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    )
    .with_device_requirements(requirements.clone());
    let encoded = serde_json::to_value(&metadata).unwrap();
    assert_eq!(encoded["schema_version"], 3);
    let decoded: SandboxArtifactMetadata = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(decoded.device_requirements(), Some(&requirements));
    for (version, keep_requirements) in [(2, true), (3, false), (1, true), (4, true)] {
        let mut invalid = encoded.clone();
        invalid["schema_version"] = version.into();
        if !keep_requirements {
            invalid
                .as_object_mut()
                .unwrap()
                .remove("device_requirements");
        }
        serde_json::from_value::<SandboxArtifactMetadata>(invalid).unwrap_err();
    }
}

#[test]
fn firmware_contract_survives_disk_metadata_and_refuses_version_downgrade() {
    use pplx_space_execution_spec::guest_image::{
        GuestDiskLayout, GuestImageContract, GuestNetwork, GuestReadiness,
    };
    let image = GuestImageContract::new(
        GuestDiskLayout::OpaqueRawDisk,
        GuestReadiness::TcpService {
            port: std::num::NonZeroU16::new(3389).unwrap(),
        },
        GuestNetwork::SlotDhcpV1,
    );
    let metadata = SandboxArtifactMetadata::new(
        SandboxId::new(),
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        TemplateName::parse("opaque-image").unwrap(),
        None,
    )
    .with_guest_image(image);
    let value = serde_json::to_value(&metadata).unwrap();
    assert_eq!(
        serde_json::from_value::<SandboxArtifactMetadata>(value.clone())
            .unwrap()
            .guest_image(),
        image
    );
    for version in [1, 2, 3, 5] {
        let mut invalid = value.clone();
        invalid["schema_version"] = version.into();
        serde_json::from_value::<SandboxArtifactMetadata>(invalid).unwrap_err();
    }
    let mut missing = value;
    missing.as_object_mut().unwrap().remove("guest_image");
    serde_json::from_value::<SandboxArtifactMetadata>(missing).unwrap_err();
}
