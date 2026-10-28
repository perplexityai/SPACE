use std::collections::BTreeSet;

use pplx_space_common_core::types::NodeId;
use uuid::Uuid;

use super::*;

fn enabled(canary_nodes: &[&str], percent: u8) -> RuntimeProvisioningCohort {
    RuntimeProvisioningCohort::Enabled {
        canary_nodes: canary_nodes.iter().map(|node| (*node).to_owned()).collect(),
        percent,
    }
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(Uuid::from_u128(value))
}

#[test]
fn off_unless_the_setting_is_truthy() {
    for raw in [None, Some(""), Some("0"), Some("false"), Some("yes please")] {
        assert_eq!(
            RuntimeProvisioningCohort::from_values(raw, Some("worker-a"), Some("100")),
            Ok(RuntimeProvisioningCohort::Disabled),
            "{raw:?}"
        );
    }
}

#[test]
fn cohort_values_are_ignored_while_off() {
    assert_eq!(
        RuntimeProvisioningCohort::from_values(None, None, Some("not a number")),
        Ok(RuntimeProvisioningCohort::Disabled)
    );
}

#[test]
fn enabled_reads_the_canary_list_and_percentage() {
    assert_eq!(
        RuntimeProvisioningCohort::from_values(
            Some("true"),
            Some(" worker-a, ,worker-b ,"),
            Some(" 10 ")
        ),
        Ok(enabled(&["worker-a", "worker-b"], 10))
    );
    assert_eq!(
        RuntimeProvisioningCohort::from_values(Some("1"), None, None),
        Ok(enabled(&[], 0))
    );
    assert_eq!(
        RuntimeProvisioningCohort::from_values(Some("1"), None, Some("100")),
        Ok(enabled(&[], 100))
    );
}

#[test]
fn an_invalid_percentage_is_refused() {
    for raw in ["101", "-1", "10%", "0.5", "ten", "256"] {
        assert_eq!(
            RuntimeProvisioningCohort::from_values(Some("true"), None, Some(raw)),
            Err(CohortConfigError::Percent(raw.to_owned())),
            "{raw:?}"
        );
    }
}

#[test]
fn disabled_admits_no_node() {
    let id = node(1);
    assert!(!RuntimeProvisioningCohort::Disabled.admits(&id.to_string(), id));
}

#[test]
fn canary_nodes_match_the_reported_name_or_the_node_id() {
    let id = node(7);
    let cohort = enabled(&["worker-a", &node(8).to_string()], 0);
    assert!(cohort.admits("worker-a", id));
    assert!(!cohort.admits("worker-b", id));
    assert!(cohort.admits("worker-b", node(8)));
}

#[test]
fn percentages_admit_a_growing_set() {
    let nodes: Vec<NodeId> = (0..2_000).map(node).collect();
    let admitted = |percent| -> BTreeSet<NodeId> {
        let cohort = enabled(&[], percent);
        nodes
            .iter()
            .copied()
            .filter(|id| cohort.admits("unlisted", *id))
            .collect()
    };
    assert!(admitted(0).is_empty());
    assert_eq!(admitted(100).len(), nodes.len());
    let mut previous = admitted(0);
    for percent in 1..=100 {
        let current = admitted(percent);
        assert!(
            previous.is_subset(&current),
            "{percent}% must keep every node a lower percentage admitted"
        );
        previous = current;
    }
    // The buckets spread: 10% of 2000 nodes lands well inside 100..300.
    let ten = admitted(10).len();
    assert!((100..300).contains(&ten), "{ten}");
}

/// Replicas and binaries must agree on every node's bucket, or a rollout
/// would admit different nodes on different calls. Changing the hash, its
/// domain or its input breaks this pin, and needs a deliberate decision about
/// which nodes a deployed percentage then admits.
#[test]
fn buckets_are_pinned() {
    let buckets: Vec<u8> = [0, 1, 0x0192_0000_0000_7000_8000_0000_0000_0001]
        .into_iter()
        .map(|value| hash_bucket(node(value)))
        .collect();
    assert_eq!(buckets, vec![52, 41, 51]);
}
