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
fn scoped_enrollment_round_trips_and_cannot_hide_in_the_legacy_schema() {
    let metadata = SandboxArtifactMetadata::new(
        SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap(),
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    );
    assert!(
        !serde_json::to_value(&metadata)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("scoped_tls_key_id")
    );
    let scoped =
        metadata.with_scoped_tls_key_id(Some("mrk-0123456789abcdef0123456789abcdef".into()));
    let mut value = serde_json::to_value(&scoped).unwrap();
    assert_eq!(value["schema_version"], 4);
    assert_eq!(
        serde_json::from_value::<SandboxArtifactMetadata>(value.clone()).unwrap(),
        scoped
    );
    value["schema_version"] = 2.into();
    serde_json::from_value::<SandboxArtifactMetadata>(value.clone()).unwrap_err();
    value["schema_version"] = 3.into();
    serde_json::from_value::<SandboxArtifactMetadata>(value.clone()).unwrap_err();
    value["schema_version"] = 4.into();
    value.as_object_mut().unwrap().remove("scoped_tls_key_id");
    serde_json::from_value::<SandboxArtifactMetadata>(value).unwrap_err();
}
