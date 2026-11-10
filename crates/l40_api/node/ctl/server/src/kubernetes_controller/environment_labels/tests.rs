use std::collections::BTreeMap;

use pplx_space_common_core::types::{
    Digest, ExecutionCompatibilityProfile, RuntimeBinding, StateFormatIdentity,
};
use pplx_space_execution_common::environment::{
    EnvironmentDefinition, FIRECRACKER_MACHINE_CONTRACT, KnownEnvironment,
};

use super::*;

fn e0() -> EnvironmentDefinition {
    EnvironmentDefinition::from_document(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../../proto/execution/v1/fixtures/e0.json"
    )))
    .unwrap()
    .into_runtime_neutral()
    .unwrap()
}

/// Another Firecracker release over E0's guest and kernel, as E1 is.
fn e1() -> EnvironmentDefinition {
    let mut spec = e0().descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.runtime.version = "1.17.0".parse().unwrap();
    release.digest = Digest::sha256([7; 32]);
    spec.compatibility = ExecutionCompatibilityProfile::RuntimeV1 {
        contract: FIRECRACKER_MACHINE_CONTRACT.parse().unwrap(),
        state_format: Some(StateFormatIdentity {
            format: "firecracker-snapshot".parse().unwrap(),
            version: "12.0.0".parse().unwrap(),
        }),
    };
    EnvironmentDefinition::new(spec.try_into().unwrap())
}

/// An inventory whose node knows E0 and E1 and can boot only E1.
fn inventory_usable_e1() -> NodeExecutionInventory {
    let observation = EnvironmentObservation::new(
        [
            KnownEnvironment::new(e0(), None, [], []).unwrap(),
            KnownEnvironment::new(e1(), None, [], []).unwrap(),
        ],
        [e1().reference()],
    )
    .unwrap();
    NodeExecutionInventory {
        environments: Some((&observation).into()),
        ..Default::default()
    }
}

fn metadata(labels: &[(&str, &str)]) -> node_watch::NodeMetadata {
    node_watch::NodeMetadata {
        resource_version: "1".to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        annotations: BTreeMap::new(),
        network_slots_capacity: None,
    }
}

#[test]
fn a_complete_observation_labels_exactly_its_usable_environments() {
    assert_eq!(
        desired_environment_labels(&inventory_usable_e1()),
        BTreeSet::from([environment_label_key(&e1().reference())])
    );
}

#[test]
fn an_inventory_without_a_valid_observation_labels_nothing() {
    assert_eq!(
        desired_environment_labels(&NodeExecutionInventory::default()),
        BTreeSet::new(),
        "a node-manager that predates observations"
    );
    let mut invalid = inventory_usable_e1();
    invalid.environments.as_mut().unwrap().schema_version += 1;
    assert_eq!(
        desired_environment_labels(&invalid),
        BTreeSet::new(),
        "an observation that doesn't decode"
    );
}

#[test]
fn labels_are_current_only_when_every_desired_label_is_set_and_none_is_stale() {
    let e0_key = environment_label_key(&e0().reference());
    let e1_key = environment_label_key(&e1().reference());
    let desired = BTreeSet::from([e1_key.clone()]);
    let other = ("node.kubernetes.io/instance-type", "metal");
    let template = ("space.pplx.net/template-build-seed", "true");

    assert!(environment_labels_current(
        &metadata(&[(&e1_key, ENVIRONMENT_USABLE_LABEL_VALUE), other, template]),
        &desired
    ));
    assert!(
        !environment_labels_current(&metadata(&[other]), &desired),
        "a missing label"
    );
    assert!(
        !environment_labels_current(&metadata(&[(&e1_key, "stale")]), &desired),
        "a label with another value"
    );
    assert!(
        !environment_labels_current(
            &metadata(&[
                (&e1_key, ENVIRONMENT_USABLE_LABEL_VALUE),
                (&e0_key, ENVIRONMENT_USABLE_LABEL_VALUE),
            ]),
            &desired
        ),
        "a label for an environment that's no longer usable"
    );
    assert!(
        environment_labels_current(&metadata(&[other, template]), &BTreeSet::new()),
        "no environment labels wanted, none present"
    );
}

#[test]
fn the_apply_sends_only_the_desired_environment_labels() {
    let e1_key = environment_label_key(&e1().reference());
    assert_eq!(
        environment_labels_apply("worker-1", &BTreeSet::from([e1_key.clone()])),
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "worker-1",
                "labels": { e1_key: ENVIRONMENT_USABLE_LABEL_VALUE },
            }
        })
    );
    // An empty apply still names `labels`, so server-side apply removes every
    // label this manager set before.
    assert_eq!(
        environment_labels_apply("worker-1", &BTreeSet::new())["metadata"]["labels"],
        serde_json::json!({})
    );
}
