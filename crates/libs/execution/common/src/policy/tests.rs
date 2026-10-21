use super::*;
use prost::Message;

#[test]
fn policy_observation_validates_shape_and_old_inventory_readers_omit_it() {
    let reference = format!("sha256:{}", "11".repeat(32));
    let wire = crate::proto::ExecutionPolicyObservation {
        schema_version: 1,
        legacy_unbound_allowed: true,
        allowed_environment_references: vec![reference.clone()],
        default_environment_reference: Some(reference.clone()),
    };
    let legacy_only = crate::proto::ExecutionPolicyObservation {
        schema_version: 1,
        legacy_unbound_allowed: true,
        allowed_environment_references: vec![],
        default_environment_reference: None,
    };
    let legacy_policy = ExecutionPolicyObservation::try_from(&legacy_only).unwrap();
    assert!(legacy_policy.legacy_unbound_allowed());
    assert_eq!(
        crate::proto::ExecutionPolicyObservation::from(&legacy_policy),
        legacy_only
    );
    let value = ExecutionPolicyObservation::try_from(&wire).unwrap();
    assert_eq!(crate::proto::ExecutionPolicyObservation::from(&value), wire);
    for bad in [
        crate::proto::ExecutionPolicyObservation {
            schema_version: 2,
            ..wire.clone()
        },
        crate::proto::ExecutionPolicyObservation {
            allowed_environment_references: vec![reference.clone(), reference],
            ..wire.clone()
        },
        crate::proto::ExecutionPolicyObservation {
            default_environment_reference: None,
            ..wire.clone()
        },
        crate::proto::ExecutionPolicyObservation {
            allowed_environment_references: vec![],
            ..wire.clone()
        },
    ] {
        ExecutionPolicyObservation::try_from(&bad).unwrap_err();
    }
    #[derive(Clone, PartialEq, Message)]
    struct OldInventory {
        #[prost(message, repeated, tag = "1")]
        runtimes: Vec<crate::proto::RuntimeInstallation>,
        #[prost(message, repeated, tag = "2")]
        components: Vec<crate::proto::ArtifactComponentInstallation>,
    }
    let inventory = crate::proto::NodeExecutionInventory {
        policy: Some(wire),
        ..Default::default()
    };
    let old = OldInventory::decode(inventory.encode_to_vec().as_slice()).unwrap();
    let forwarded =
        crate::proto::NodeExecutionInventory::decode(old.encode_to_vec().as_slice()).unwrap();
    assert!(forwarded.policy.is_none());
}
