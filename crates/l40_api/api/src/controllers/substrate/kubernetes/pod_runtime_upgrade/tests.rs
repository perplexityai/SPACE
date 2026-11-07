use k8s_openapi::api::core::v1::Pod;
use pplx_space_common_core::kubernetes::PodRuntimeUpgradeTargetState;
use pplx_space_sbx_mgr_client::proto::RuntimeUpgradeStatus;

use super::{NodeReport, Observation, PodUpgradeOutcome, observe, unlocated_observation};
use crate::controllers::substrate::kubernetes::sandbox_pod_view::SandboxPodView;

const THIS_POD: &str = "this-pod-uid";
const OTHER_POD: &str = "other-pod-uid";

fn pod(phase: &str, container: serde_json::Value) -> SandboxPodView {
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "space-sbx-a-g1", "namespace": "space", "uid": THIS_POD },
        "status": {
            "phase": phase,
            "containerStatuses": [{
                "name": "sandbox", "image": "sandbox", "imageID": "",
                "ready": false, "restartCount": 0, "state": container,
            }],
        },
    }))
    .expect("pod fixture parses");
    SandboxPodView::from(pod)
}

/// What each node report means for the pod's upgrade: an outcome only once
/// nothing of it can still run. A rollback or refusal settles only when it
/// names this pod, whatever its containers show (a failed create leaves the
/// pod pending in it forever): the node answers that pod's retries with the
/// same outcome. One naming another pod may be an earlier attempt's, and one
/// naming none comes from a node that predates the field, so neither
/// settles, even once the pod's container ended. With the pod gone, nothing
/// runs for it, but the report still settles only when it names the pod the
/// attempt knew. An operation the node hasn't recorded
/// settles as not started only after the pod's create, the one place a shim
/// calls the node, has ended. The report is the upgrade's outcome on the
/// node, not the pod's: a `started` target is reached only once the pod's
/// create returned and its container runs, and a pod whose create ended or
/// that's gone was refused even though the node has the sandbox on the
/// target. A `stopped` target's pod is a placeholder, and its caller checks
/// the sandbox is at rest from the node's publication.
#[test]
fn a_report_settles_the_upgrade_only_once_nothing_of_it_can_run() {
    let running = pod("Running", serde_json::json!({ "running": {} }));
    let creating = pod(
        "Pending",
        serde_json::json!({ "waiting": { "reason": "ContainerCreating" } }),
    );
    let ended = pod(
        "Running",
        serde_json::json!({ "terminated": { "exitCode": 128 } }),
    );
    let failed = pod("Failed", serde_json::json!({ "waiting": {} }));
    let failed_running = pod("Failed", serde_json::json!({ "running": {} }));
    let reached = |already_on_target| Some(PodUpgradeOutcome::Reached { already_on_target });
    let rolled_back = || {
        Some(PodUpgradeOutcome::RolledBack {
            failure: Some("boot failed".to_string()),
        })
    };
    let refused_early = || {
        Some(PodUpgradeOutcome::Refused {
            failure: Some("boot failed".to_string()),
        })
    };
    let refused = |already_on_target, activation_claimed| {
        Some(PodUpgradeOutcome::PodRefused {
            already_on_target,
            activation_claimed,
            failure: None,
        })
    };
    use PodRuntimeUpgradeTargetState::{Started, Stopped};
    use RuntimeUpgradeStatus as S;
    type Case<'a> = (
        S,
        bool,
        PodRuntimeUpgradeTargetState,
        Option<&'a SandboxPodView>,
        Option<PodUpgradeOutcome>,
    );
    // Rollbacks and refusals, by the pod the report names: settled for this
    // pod (`true`), or with no pod there (`false`).
    type Final<'a> = (
        S,
        Option<&'a str>,
        PodRuntimeUpgradeTargetState,
        Option<&'a SandboxPodView>,
        Option<(PodUpgradeOutcome, bool)>,
    );
    let for_pod = |outcome: Option<PodUpgradeOutcome>| outcome.map(|outcome| (outcome, true));
    let gone = |outcome: Option<PodUpgradeOutcome>| outcome.map(|outcome| (outcome, false));
    let finals: &[Final<'_>] = &[
        // Named for this pod: settled with the pod pending in its create, and
        // whatever else its containers show.
        (
            S::RolledBack,
            Some(THIS_POD),
            Started,
            Some(&creating),
            for_pod(rolled_back()),
        ),
        (
            S::RolledBack,
            Some(THIS_POD),
            Stopped,
            Some(&creating),
            for_pod(rolled_back()),
        ),
        (
            S::RolledBack,
            Some(THIS_POD),
            Stopped,
            Some(&running),
            for_pod(rolled_back()),
        ),
        (
            S::RolledBack,
            Some(THIS_POD),
            Started,
            Some(&ended),
            for_pod(rolled_back()),
        ),
        (
            S::Refused,
            Some(THIS_POD),
            Started,
            Some(&creating),
            for_pod(refused_early()),
        ),
        (
            S::Refused,
            Some(THIS_POD),
            Stopped,
            Some(&failed),
            for_pod(refused_early()),
        ),
        // Another pod's, or a node's that names none: never proof.
        (
            S::RolledBack,
            Some(OTHER_POD),
            Started,
            Some(&creating),
            None,
        ),
        (
            S::RolledBack,
            Some(OTHER_POD),
            Stopped,
            Some(&running),
            None,
        ),
        (S::RolledBack, Some(OTHER_POD), Started, Some(&ended), None),
        (S::RolledBack, None, Started, Some(&creating), None),
        (S::RolledBack, None, Stopped, Some(&ended), None),
        (S::RolledBack, None, Stopped, Some(&failed), None),
        (S::Refused, Some(OTHER_POD), Stopped, Some(&creating), None),
        (S::Refused, None, Started, Some(&ended), None),
        // No pod: nothing runs for it, but only a report naming the pod the
        // attempt knew is its own; another pod's, or none, may be an earlier
        // attempt's.
        (
            S::RolledBack,
            Some(THIS_POD),
            Started,
            None,
            gone(rolled_back()),
        ),
        (
            S::Refused,
            Some(THIS_POD),
            Stopped,
            None,
            gone(refused_early()),
        ),
        (S::RolledBack, None, Started, None, None),
        (S::RolledBack, Some(OTHER_POD), Stopped, None, None),
        (S::Refused, None, Started, None, None),
    ];
    for (status, named, target_state, pod, expected) in finals {
        let report = NodeReport {
            status: *status,
            failure: Some("boot failed".to_string()),
            activation_claimed: false,
            pod_uid: named.map(str::to_owned),
            refused_pod: None,
        };
        let observed = match observe(report, *target_state, *pod, Some(THIS_POD)) {
            Observation::Settled(outcome) => Some((outcome, false)),
            Observation::SettledForPod { outcome, pod_uid } => {
                assert_eq!(pod_uid, THIS_POD);
                Some((outcome, true))
            }
            Observation::Pending(_) => None,
        };
        assert_eq!(
            &observed,
            expected,
            "{status:?} naming {named:?} for {target_state} with {:?}",
            pod.map(SandboxPodView::phase)
        );
    }
    let cases: &[Case<'_>] = &[
        (S::Upgraded, false, Started, Some(&running), reached(false)),
        (S::Upgraded, false, Started, Some(&creating), None),
        (
            S::Upgraded,
            false,
            Started,
            Some(&ended),
            refused(false, false),
        ),
        (
            S::Upgraded,
            false,
            Started,
            Some(&failed),
            refused(false, false),
        ),
        (
            S::Upgraded,
            false,
            Started,
            Some(&failed_running),
            refused(false, false),
        ),
        (S::Upgraded, false, Started, None, refused(false, false)),
        (
            S::AlreadyOnTarget,
            true,
            Started,
            Some(&running),
            reached(true),
        ),
        (S::AlreadyOnTarget, true, Started, Some(&creating), None),
        (
            S::AlreadyOnTarget,
            true,
            Started,
            Some(&ended),
            refused(true, true),
        ),
        (
            S::AlreadyOnTarget,
            false,
            Started,
            Some(&ended),
            refused(true, false),
        ),
        (S::Upgraded, false, Stopped, Some(&running), reached(false)),
        (S::Upgraded, false, Stopped, Some(&creating), reached(false)),
        (S::Upgraded, false, Stopped, Some(&ended), reached(false)),
        (
            S::AlreadyOnTarget,
            false,
            Stopped,
            Some(&ended),
            reached(true),
        ),
        (
            S::Absent,
            false,
            Started,
            Some(&ended),
            Some(PodUpgradeOutcome::NotStarted),
        ),
        (
            S::Absent,
            false,
            Stopped,
            Some(&ended),
            Some(PodUpgradeOutcome::NotStarted),
        ),
        // A pod marked failed, its container never having terminated (an
        // eviction of a pending pod, say), proves nothing about its create's
        // call: never settled.
        (S::Absent, false, Stopped, Some(&failed), None),
        (S::Absent, false, Started, Some(&failed), None),
        // A pod that's gone may have been deleted while its create's call was
        // still on its way to the node: never settled, for either target.
        (S::Absent, false, Started, None, None),
        (S::Absent, false, Stopped, None, None),
        (S::Absent, false, Started, Some(&running), None),
        (S::Absent, false, Stopped, Some(&creating), None),
        (S::InProgress, false, Started, Some(&ended), None),
        (S::InProgress, false, Stopped, Some(&running), None),
        (S::Unspecified, false, Started, Some(&ended), None),
    ];
    for (status, activation_claimed, target_state, pod, expected) in cases {
        let report = NodeReport {
            status: *status,
            failure: None,
            activation_claimed: *activation_claimed,
            // Every report here names the pod: these outcomes don't depend
            // on it.
            pod_uid: Some(THIS_POD.to_string()),
            refused_pod: None,
        };
        let observed = match observe(report, *target_state, *pod, Some(THIS_POD)) {
            Observation::Settled(outcome) => Some(outcome),
            Observation::SettledForPod { .. } => panic!("{status:?} settled for the pod"),
            Observation::Pending(_) => None,
        };
        assert_eq!(
            &observed,
            expected,
            "{status:?} (claimed {activation_claimed}) for {target_state} with {:?}",
            pod.map(SandboxPodView::phase)
        );
    }
}

/// An upgraded or already-on-target operation whose node refused a pod's
/// create, for a `started` target: the pod stays pending in that create, with
/// no container ever ending, while kubelet's retries get the same refusal.
/// The refusal settles the upgrade, for the pod, only when it names the pod
/// at the name; one naming another pod (an earlier attempt's) or none (a
/// node that predates the field, or no refusal) leaves the pod's create
/// pending as before. The node records no refusal for a pod whose execution
/// the sandbox runs, so one naming this pod wins over a running container,
/// and a `stopped` target never reads them.
#[test]
fn a_refused_create_settles_only_for_the_pod_it_names() {
    let creating = pod(
        "Pending",
        serde_json::json!({ "waiting": { "reason": "ContainerCreating" } }),
    );
    let running = pod("Running", serde_json::json!({ "running": {} }));
    use PodRuntimeUpgradeTargetState::{Started, Stopped};
    use RuntimeUpgradeStatus as S;
    let refused = |already_on_target| PodUpgradeOutcome::PodRefused {
        already_on_target,
        activation_claimed: already_on_target,
        failure: Some("runs another pod's execution".to_string()),
    };
    // The report's status, the pod it names as refused, the target state,
    // the pod at the name, and the outcome expected with whether it settled
    // for that pod.
    type Case<'a> = (
        S,
        Option<&'a str>,
        PodRuntimeUpgradeTargetState,
        &'a SandboxPodView,
        Option<(PodUpgradeOutcome, bool)>,
    );
    let cases: &[Case<'_>] = &[
        (
            S::Upgraded,
            Some(THIS_POD),
            Started,
            &creating,
            Some((refused(false), true)),
        ),
        (
            S::AlreadyOnTarget,
            Some(THIS_POD),
            Started,
            &creating,
            Some((refused(true), true)),
        ),
        (S::Upgraded, Some(OTHER_POD), Started, &creating, None),
        (S::AlreadyOnTarget, None, Started, &creating, None),
        (
            S::Upgraded,
            Some(THIS_POD),
            Started,
            &running,
            Some((refused(false), true)),
        ),
        (
            S::Upgraded,
            Some(OTHER_POD),
            Started,
            &running,
            Some((
                PodUpgradeOutcome::Reached {
                    already_on_target: false,
                },
                false,
            )),
        ),
        (
            S::Upgraded,
            Some(THIS_POD),
            Stopped,
            &creating,
            Some((
                PodUpgradeOutcome::Reached {
                    already_on_target: false,
                },
                false,
            )),
        ),
    ];
    for (status, refused_pod, target_state, pod, expected) in cases {
        let report = NodeReport {
            status: *status,
            failure: None,
            activation_claimed: *status == S::AlreadyOnTarget,
            pod_uid: Some(THIS_POD.to_string()),
            refused_pod: refused_pod.map(|pod_uid| super::RefusedPod {
                pod_uid: pod_uid.to_string(),
                failure: Some("runs another pod's execution".to_string()),
            }),
        };
        let observed = match observe(report, *target_state, Some(pod), Some(THIS_POD)) {
            Observation::Settled(outcome) => Some((outcome, false)),
            Observation::SettledForPod { outcome, pod_uid } => {
                assert_eq!(pod_uid, THIS_POD);
                Some((outcome, true))
            }
            Observation::Pending(_) => None,
        };
        assert_eq!(
            &observed,
            expected,
            "{status:?} refusing {refused_pod:?} for {target_state} with {:?}",
            pod.phase()
        );
    }
}

/// With no node known for the pod, only a pod that never existed proves the
/// upgrade never began. A pod that existed and is gone may have begun its
/// create on a node nothing recorded, so that stays unsettled, as does a pod
/// still waiting to be scheduled.
#[test]
fn a_pod_with_no_known_node_settles_only_if_it_never_existed() {
    for (present, existed, settles) in [
        (true, true, false),
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ] {
        let observation = unlocated_observation(present, existed);
        assert_eq!(
            matches!(
                observation,
                Observation::Settled(PodUpgradeOutcome::NotStarted)
            ),
            settles,
            "present {present}, existed {existed}"
        );
    }
}
