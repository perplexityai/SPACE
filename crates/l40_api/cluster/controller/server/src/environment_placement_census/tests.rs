use k8s_openapi::api::core::v1::{Pod, PodSpec, PodStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use pplx_space_common_core::kubernetes::{
    ENVIRONMENT_USABLE_LABEL_VALUE, SANDBOX_ENVIRONMENT_ANNOTATION, environment_label_key,
};

use super::*;

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn label(environment: &str) -> (String, String) {
    (
        environment_label_key(&environment.parse().unwrap()),
        ENVIRONMENT_USABLE_LABEL_VALUE.to_string(),
    )
}

/// A pod recording `environment`, on `node`, in `phase`.
fn pod(name: &str, environment: Option<&str>, node: Option<&str>, phase: &str) -> SandboxPodView {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            annotations: environment.map(|environment| {
                BTreeMap::from([(
                    SANDBOX_ENVIRONMENT_ANNOTATION.to_string(),
                    environment.to_string(),
                )])
            }),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: node.map(str::to_string),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            ..Default::default()
        }),
    }
    .into()
}

/// Every node of the fixture fleet, by name, with its labels.
fn fleet() -> BTreeMap<&'static str, BTreeMap<String, String>> {
    BTreeMap::from([
        (
            "labeled-e1",
            BTreeMap::from([
                label(E1),
                (
                    "kubernetes.io/hostname".to_string(),
                    "labeled-e1".to_string(),
                ),
            ]),
        ),
        ("labeled-e0", BTreeMap::from([label(E0)])),
        (
            "unlabeled",
            BTreeMap::from([(
                "space.pplx.net/template-build-seed".to_string(),
                "true".to_string(),
            )]),
        ),
    ])
}

/// The running counts over the whole fixture fleet.
fn counts(pods: &[SandboxPodView]) -> [(RunningPodPlacement, usize); 3] {
    let fleet = fleet();
    fleet_census(pods, &fleet.keys().copied().collect::<Vec<_>>()).running
}

/// The census over the fixture nodes named `names`.
fn fleet_census(pods: &[SandboxPodView], names: &[&str]) -> Census {
    let fleet = fleet();
    let nodes = fleet
        .iter()
        .filter(|(name, _)| names.contains(name))
        .map(|(name, labels)| (*name, labels))
        .collect();
    census(pods.iter(), &nodes)
}

/// A pod recording `environment` that the scheduler hasn't placed:
/// `requires` says whether it was built with the gate on, and
/// `unschedulable` whether the scheduler has ruled it so.
fn pending_pod(
    name: &str,
    environment: &str,
    requires: bool,
    unschedulable: bool,
) -> SandboxPodView {
    serde_json::from_value::<Pod>(serde_json::json!({
        "metadata": {
            "name": name,
            "annotations": {SANDBOX_ENVIRONMENT_ANNOTATION: environment},
        },
        "spec": {
            "containers": [],
            "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{"matchExpressions": [{
                    "key": if requires {
                        environment_label_key(&environment.parse().unwrap())
                    } else {
                        "space.pplx.net/node-pool".to_string()
                    },
                    "operator": "In",
                    "values": [ENVIRONMENT_USABLE_LABEL_VALUE],
                }]}],
            }}},
        },
        "status": {
            "phase": "Pending",
            "conditions": unschedulable.then(|| serde_json::json!([{
                "type": "PodScheduled",
                "status": "False",
                "reason": "Unschedulable",
                "message": "0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector.",
            }])),
        },
    }))
    .unwrap()
    .into()
}

#[test]
fn a_bound_pod_counts_by_what_its_node_is_labeled_with() {
    let pods = [
        pod(
            "on-its-environment",
            Some(E1),
            Some("labeled-e1"),
            "Running",
        ),
        pod(
            "on-another-environment",
            Some(E1),
            Some("labeled-e0"),
            "Running",
        ),
        pod(
            "on-an-unlabeled-node",
            Some(E1),
            Some("unlabeled"),
            "Pending",
        ),
        pod(
            "e0-on-an-unlabeled-node",
            Some(E0),
            Some("unlabeled"),
            "Running",
        ),
        pod(
            "e0-on-its-environment",
            Some(E0),
            Some("labeled-e0"),
            "Running",
        ),
    ];
    assert_eq!(
        counts(&pods),
        [
            (RunningPodPlacement::Admitted, 2),
            (RunningPodPlacement::NoEvidence, 2),
            (RunningPodPlacement::EnvironmentNotUsable, 1),
        ]
    );
}

#[test]
fn only_running_bound_pods_on_known_nodes_count() {
    let mut deleting: Pod = Pod {
        metadata: ObjectMeta {
            name: Some("deleting".to_string()),
            deletion_timestamp: Some(Time(jiff::Timestamp::UNIX_EPOCH)),
            annotations: Some(BTreeMap::from([(
                SANDBOX_ENVIRONMENT_ANNOTATION.to_string(),
                E1.to_string(),
            )])),
            ..Default::default()
        },
        ..Default::default()
    };
    deleting.spec = Some(PodSpec {
        node_name: Some("unlabeled".to_string()),
        ..Default::default()
    });
    let pods = [
        pod("unbound", None, Some("unlabeled"), "Running"),
        pod("unscheduled", Some(E1), None, "Pending"),
        pod("unknown-node", Some(E1), Some("gone"), "Running"),
        pod("finished", Some(E1), Some("unlabeled"), "Succeeded"),
        pod("failed", Some(E1), Some("unlabeled"), "Failed"),
        pod("unparseable", Some("E1"), Some("unlabeled"), "Running"),
        deleting.into(),
    ];
    assert_eq!(
        counts(&pods),
        PLACEMENTS.map(|placement| (placement, 0)),
        "every placement reports, at zero"
    );
}

/// Every node's labels gone at once, as on a new cluster or after a
/// node-manager rollout whose observation node-ctl can't decode. The pods that
/// already run, E0 included, count as `no_evidence`, and so does every pod the
/// gate, on, now holds back: no node anywhere can take them.
#[test]
fn a_fleet_wide_label_loss_counts_running_and_pending_pods_without_evidence() {
    let pods = [
        pod("e1-running", Some(E1), Some("unlabeled"), "Running"),
        pod("e0-running", Some(E0), Some("unlabeled"), "Running"),
        pod("unbound-running", None, Some("unlabeled"), "Running"),
        pending_pod("e1-held", E1, true, true),
        pending_pod("e0-held", E0, true, true),
        pending_pod("e0-also-held", E0, true, true),
    ];
    // Before the loss the fleet was labeled: the same pods split between
    // admitted placements and held work that some node could take.
    assert_eq!(
        fleet_census(&pods, &["labeled-e1", "labeled-e0"]).pending,
        PENDING_REASONS.map(|reason| (reason, 0)),
        "a pod whose label some node carries isn't held by its environment"
    );
    assert_eq!(
        fleet_census(&pods, &["unlabeled"]),
        Census {
            running: [
                (RunningPodPlacement::Admitted, 0),
                (RunningPodPlacement::NoEvidence, 2),
                (RunningPodPlacement::EnvironmentNotUsable, 0),
            ],
            pending: [
                (PendingPodReason::NoEvidence, 3),
                (PendingPodReason::EnvironmentNotUsable, 0),
            ],
        }
    );
}

/// A pending pod counts only when its environment is what holds it.
#[test]
fn a_pending_pod_counts_only_when_no_node_carries_the_label_it_requires() {
    let pods = [
        // Labeled with E0 only: E1 has no usable node.
        pending_pod("e1-held", E1, true, true),
        pending_pod("shadowed", E1, false, true),
        pending_pod("not-yet-ruled", E1, true, false),
        pending_pod("e0-placeable", E0, true, true),
    ];
    assert_eq!(
        fleet_census(&pods, &["labeled-e0", "unlabeled"]).pending,
        [
            (PendingPodReason::NoEvidence, 0),
            (PendingPodReason::EnvironmentNotUsable, 1),
        ]
    );
}
