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
        applied_generation: None,
        cluster_policy_unreadable: false,
    };
    let legacy_only = crate::proto::ExecutionPolicyObservation {
        schema_version: 1,
        legacy_unbound_allowed: true,
        allowed_environment_references: vec![],
        default_environment_reference: None,
        applied_generation: None,
        cluster_policy_unreadable: false,
    };
    let legacy_policy = ExecutionPolicyObservation::try_from(&legacy_only).unwrap();
    assert!(legacy_policy.legacy_unbound_allowed());
    assert_eq!(
        crate::proto::ExecutionPolicyObservation::from(&legacy_policy),
        legacy_only
    );
    let legacy_default = crate::proto::ExecutionPolicyObservation {
        default_environment_reference: None,
        ..wire.clone()
    };
    let parsed = ExecutionPolicyObservation::try_from(&legacy_default).unwrap();
    assert_eq!(
        crate::proto::ExecutionPolicyObservation::from(&parsed),
        legacy_default
    );
    let value = ExecutionPolicyObservation::try_from(&wire).unwrap();
    assert_eq!(crate::proto::ExecutionPolicyObservation::from(&value), wire);
    let applied = crate::proto::ExecutionPolicyObservation {
        schema_version: 2,
        default_environment_reference: None,
        applied_generation: Some(9),
        ..wire.clone()
    };
    let unreadable = crate::proto::ExecutionPolicyObservation {
        schema_version: 2,
        legacy_unbound_allowed: false,
        allowed_environment_references: vec![],
        default_environment_reference: None,
        applied_generation: None,
        cluster_policy_unreadable: true,
    };
    let unreadable_value = ExecutionPolicyObservation::try_from(&unreadable).unwrap();
    assert!(unreadable_value.cluster_policy_unreadable());
    assert_eq!(
        crate::proto::ExecutionPolicyObservation::from(&unreadable_value),
        unreadable
    );
    let applied_value = ExecutionPolicyObservation::try_from(&applied).unwrap();
    assert_eq!(applied_value.applied_generation(), Some(9));
    assert_eq!(
        crate::proto::ExecutionPolicyObservation::from(&applied_value),
        applied
    );
    for bad in [
        crate::proto::ExecutionPolicyObservation {
            schema_version: 2,
            ..wire.clone()
        },
        crate::proto::ExecutionPolicyObservation {
            applied_generation: Some(9),
            ..wire.clone()
        },
        crate::proto::ExecutionPolicyObservation {
            allowed_environment_references: vec![reference.clone(), reference],
            ..wire.clone()
        },
        crate::proto::ExecutionPolicyObservation {
            default_environment_reference: None,
            legacy_unbound_allowed: false,
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
