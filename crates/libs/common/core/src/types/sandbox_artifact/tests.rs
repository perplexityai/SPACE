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
fn sandbox_allocation_round_trips_without_changing_legacy_shape() {
    let legacy = serde_json::to_value(SandboxArtifactMetadata::new(
        SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap(),
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    ))
    .unwrap();
    for (field, positive) in [("cpu_budget", 8u64), ("guest_memory", 4 * 1024 * 1024)] {
        for budget in [None, Some(positive), Some(0)] {
            let mut value = legacy.clone();
            if let Some(budget) = budget {
                value[field] = budget.into();
            }
            let parsed = serde_json::from_value::<SandboxArtifactMetadata>(value.clone());
            if budget == Some(0) {
                assert!(parsed.is_err(), "zero is not a valid allocation");
            } else {
                assert_eq!(serde_json::to_value(parsed.unwrap()).unwrap(), value);
            }
        }
    }
}
