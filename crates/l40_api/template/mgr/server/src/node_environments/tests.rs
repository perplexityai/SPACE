use pplx_space_cpu_arch::Arch;
use pplx_space_execution_catalog::catalog::CompiledEnvironment;
use pplx_space_execution_common::environment::{EnvironmentDefinition, KnownEnvironment};
use pplx_space_execution_spec::{
    EXECUTION_POLICY_APPLIED_SCHEMA_VERSION, EXECUTION_POLICY_SCHEMA_VERSION,
};

use super::*;

fn catalog_definition(name: &str) -> EnvironmentDefinition {
    CompiledEnvironment::for_architecture(Arch::X86_64)
        .unwrap()
        .into_iter()
        .find(|entry| entry.name() == name)
        .unwrap()
        .definition()
        .clone()
}

/// Two Firecracker environments of the node catalog, as an initial
/// environment and its upgrade are.
fn e0() -> EnvironmentDefinition {
    catalog_definition("fc-1.17.0-linux-6.1.155")
}

fn e1() -> EnvironmentDefinition {
    catalog_definition("fc-1.17.0-linux-6.18.44")
}

/// A node that knows and can boot E0 and E1, under `policy`.
fn inventory(policy: Option<ExecutionPolicyObservation>) -> NodeExecutionInventory {
    let observation = EnvironmentObservation::new(
        [
            KnownEnvironment::new(e0(), None, [], []).unwrap(),
            KnownEnvironment::new(e1(), None, [], []).unwrap(),
        ],
        [e0().reference(), e1().reference()],
    )
    .unwrap();
    NodeExecutionInventory {
        environments: Some((&observation).into()),
        policy: policy.as_ref().map(Into::into),
        ..Default::default()
    }
}

fn startup_policy(default: EnvironmentReference) -> ExecutionPolicyObservation {
    ExecutionPolicyObservation::new(
        EXECUTION_POLICY_SCHEMA_VERSION,
        vec![e0().reference(), e1().reference()],
        Some(default),
        false,
        None,
        false,
    )
    .unwrap()
}

#[test]
fn a_node_runs_its_usable_environments() {
    assert_eq!(
        NodeEnvironments::from_inventory(&inventory(Some(startup_policy(e1().reference())))),
        Some(NodeEnvironments {
            runnable: BTreeSet::from([e0().reference(), e1().reference()]),
        })
    );
}

#[test]
fn an_applied_cluster_policy_narrows_what_runs() {
    let narrowed = ExecutionPolicyObservation::new(
        EXECUTION_POLICY_APPLIED_SCHEMA_VERSION,
        vec![e0().reference()],
        None,
        false,
        Some(3),
        false,
    )
    .unwrap();
    assert_eq!(
        NodeEnvironments::from_inventory(&inventory(Some(narrowed))),
        Some(NodeEnvironments {
            runnable: BTreeSet::from([e0().reference()]),
        })
    );
}

#[test]
fn an_unreadable_cluster_policy_is_unknown_not_empty() {
    let unreadable = ExecutionPolicyObservation::new(
        EXECUTION_POLICY_APPLIED_SCHEMA_VERSION,
        Vec::new(),
        None,
        false,
        None,
        true,
    )
    .unwrap();
    assert_eq!(
        NodeEnvironments::from_inventory(&inventory(Some(unreadable))),
        None
    );
}

/// An older node-manager refuses template-manager or lacks the RPC; either
/// way the node's environments are unknown, so prefetch acts as before
/// environments rather than fail or treat the node as running none.
#[tokio::test]
async fn an_older_node_manager_reads_as_unknown() {
    for status in [
        tonic::Status::permission_denied("template-manager may not read node status"),
        tonic::Status::unimplemented("GetExecutionInventory"),
        tonic::Status::unavailable("node-manager is down"),
    ] {
        let code = status.code();
        assert_eq!(observe(async { Err(status) }).await, None, "{code:?}");
    }
    assert_eq!(
        observe(async { Ok(inventory(Some(startup_policy(e1().reference())))) })
            .await
            .map(|node| node.runnable),
        Some(BTreeSet::from([e0().reference(), e1().reference()]))
    );
}

#[tokio::test(start_paused = true)]
async fn a_hanging_node_manager_reads_as_unknown_within_the_budget() {
    let started = tokio::time::Instant::now();
    assert_eq!(observe(std::future::pending()).await, None);
    assert_eq!(started.elapsed(), READ_BUDGET);
}

#[test]
fn a_node_manager_without_observations_reports_nothing() {
    assert_eq!(
        NodeEnvironments::from_inventory(&NodeExecutionInventory::default()),
        None
    );
}

#[test]
fn an_observation_that_does_not_decode_reports_nothing() {
    let mut inventory = inventory(None);
    inventory
        .environments
        .as_mut()
        .unwrap()
        .usable_environments
        .push("not-a-reference".to_string());
    assert_eq!(NodeEnvironments::from_inventory(&inventory), None);
}

/// Environment awareness is off unless the switch says on.
#[test]
fn environment_awareness_is_off_by_default() {
    assert_eq!(
        EnvironmentAwareness::from_raw(None),
        EnvironmentAwareness::Off
    );
    for off in ["0", "false", "off", "", "maybe"] {
        assert_eq!(
            EnvironmentAwareness::from_raw(Some(off)),
            EnvironmentAwareness::Off,
            "{off:?}"
        );
    }
    for on in ["1", "true", "on", "yes", "enabled"] {
        assert_eq!(
            EnvironmentAwareness::from_raw(Some(on)),
            EnvironmentAwareness::On,
            "{on:?}"
        );
    }
}
