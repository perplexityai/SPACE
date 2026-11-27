use k8s_openapi::api::core::v1::Pod;
use pplx_space_sandbox_pod::PodRuntimeUpgradeTargetState;
use pplx_space_sbx_mgr_client::proto::RuntimeUpgradeStatus;

use pplx_space_sandbox_model::SandboxId;

use super::{
    NodeReport, Observation, PodUpgradeOutcome, observe, outcome_poll_delay,
    settled_pod_is_recorded, unlocated_observation,
};
use crate::controllers::substrate::SubstrateError;
use crate::controllers::substrate::kubernetes::PodUid;
use crate::controllers::substrate::kubernetes::sandbox_pod_view::SandboxPodView;

const THIS_POD: &str = "this-pod-uid";
const OTHER_POD: &str = "other-pod-uid";

fn pod(phase: &str, container: serde_json::Value) -> SandboxPodView {
    pod_with_uid(THIS_POD, phase, container)
}

fn pod_with_uid(uid: &str, phase: &str, container: serde_json::Value) -> SandboxPodView {
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "space-sbx-a-g1", "namespace": "space", "uid": uid },
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
            abandoned: false,
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
            abandoned: false,
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
/// and a `stopped` target never reads them. An absent report carries the
/// refusal the node kept for a pod after the sandbox's row went, which
/// settles the same way, for the pod at the name or, with it gone, the one
/// the wait knew.
/// A rollback or refusal settles for the pod at the name when it names that
/// pod, even when the wait knew another pod's UID, and never on the known UID
/// while a different pod is there: the pod at the name is the one a delete
/// under its UID would remove and kubelet would retry.
#[test]
fn a_final_report_settles_for_the_pod_that_is_there() {
    let successor = pod_with_uid(
        OTHER_POD,
        "Pending",
        serde_json::json!({ "waiting": { "reason": "ContainerCreating" } }),
    );
    for status in [
        RuntimeUpgradeStatus::RolledBack,
        RuntimeUpgradeStatus::Refused,
    ] {
        for (named, settles) in [(OTHER_POD, true), (THIS_POD, false)] {
            let report = NodeReport {
                status,
                failure: Some("boot failed".to_string()),
                activation_claimed: false,
                pod_uid: Some(named.to_owned()),
                refused_pod: None,
                abandoned: false,
            };
            let observed = observe(
                report,
                PodRuntimeUpgradeTargetState::Started,
                Some(&successor),
                Some(THIS_POD),
            );
            match observed {
                Observation::SettledForPod { pod_uid, .. } if settles => {
                    assert_eq!(pod_uid, OTHER_POD, "{status:?} naming {named}");
                }
                Observation::Pending(_) if !settles => {}
                other => panic!("{status:?} naming {named}: {other:?}"),
            }
        }
    }
}

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
        (
            S::Absent,
            Some(THIS_POD),
            Started,
            &creating,
            Some((refused(false), true)),
        ),
        (S::Absent, Some(OTHER_POD), Started, &creating, None),
        (S::Absent, Some(THIS_POD), Stopped, &creating, None),
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
            abandoned: false,
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
    // With the pod gone, a kept refusal settles only for the pod the wait
    // knew.
    for (named, expected) in [(THIS_POD, Some(refused(false))), (OTHER_POD, None)] {
        let report = NodeReport {
            status: S::Absent,
            failure: None,
            activation_claimed: false,
            pod_uid: None,
            refused_pod: Some(super::RefusedPod {
                pod_uid: named.to_string(),
                failure: Some("runs another pod's execution".to_string()),
            }),
            abandoned: false,
        };
        let observed = match observe(report, Started, None, Some(THIS_POD)) {
            Observation::Settled(outcome) => Some(outcome),
            Observation::SettledForPod { .. } => panic!("settled for a pod that's gone"),
            Observation::Pending(_) => None,
        };
        assert_eq!(observed, expected, "absent, gone pod, refusing {named}");
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
        if settles {
            assert!(
                matches!(
                    observation,
                    Observation::Settled(PodUpgradeOutcome::NotStarted)
                ),
                "present {present}, existed {existed}: {observation:?}"
            );
        } else {
            assert!(
                matches!(observation, Observation::Pending(_)),
                "present {present}, existed {existed}: {observation:?}"
            );
        }
    }
}

/// An operation the node never recorded and has since abandoned never began
/// there and never will, so it settles as not started whatever the pod
/// shows, a pod that's gone or still in its create included. An abandoned
/// operation the node did record keeps its outcome for the pod it names, and
/// settles as not started for any other pod, whose call the node refuses
/// before recording anything.
#[test]
fn an_abandoned_operation_the_node_never_saw_settles_as_not_started() {
    let creating = pod(
        "Pending",
        serde_json::json!({ "waiting": { "reason": "ContainerCreating" } }),
    );
    for target_state in [
        PodRuntimeUpgradeTargetState::Started,
        PodRuntimeUpgradeTargetState::Stopped,
    ] {
        for shown in [None, Some(&creating)] {
            let report = NodeReport {
                status: RuntimeUpgradeStatus::Absent,
                failure: None,
                activation_claimed: false,
                pod_uid: None,
                refused_pod: None,
                abandoned: true,
            };
            assert!(
                matches!(
                    observe(report, target_state, shown, Some(THIS_POD)),
                    Observation::Settled(PodUpgradeOutcome::NotStarted)
                ),
                "{target_state} with {:?}",
                shown.map(SandboxPodView::phase)
            );
        }
    }
    // Abandoned after an earlier pod's rollback or refusal: the report still
    // names that pod, and this pod's call is refused before anything is
    // recorded for it.
    for status in [
        RuntimeUpgradeStatus::RolledBack,
        RuntimeUpgradeStatus::Refused,
    ] {
        for shown in [None, Some(&creating)] {
            let report = NodeReport {
                status,
                failure: Some("boot failed".to_string()),
                activation_claimed: false,
                pod_uid: Some(OTHER_POD.to_string()),
                refused_pod: None,
                abandoned: true,
            };
            assert!(
                matches!(
                    observe(
                        report,
                        PodRuntimeUpgradeTargetState::Stopped,
                        shown,
                        Some(THIS_POD)
                    ),
                    Observation::Settled(PodUpgradeOutcome::NotStarted)
                ),
                "{status:?} for an earlier pod with {:?}",
                shown.map(SandboxPodView::phase)
            );
        }
    }
    let report = NodeReport {
        status: RuntimeUpgradeStatus::InProgress,
        failure: None,
        activation_claimed: false,
        pod_uid: None,
        refused_pod: None,
        abandoned: true,
    };
    assert!(matches!(
        observe(
            report,
            PodRuntimeUpgradeTargetState::Stopped,
            None,
            Some(THIS_POD)
        ),
        Observation::Pending(_)
    ));
}

/// The wait between two looks doubles from 200 ms to its 2 s cap, and jitter
/// takes at most a fifth off it.
#[test]
fn the_outcome_wait_backs_off_to_its_cap() {
    use std::time::Duration;
    let millis = |looks, jitter| outcome_poll_delay(looks, jitter).as_millis();
    assert_eq!(
        [0, 1, 2, 3, 4, 5, 6, 50].map(|looks| millis(looks, 0.0)),
        [200, 200, 400, 800, 1600, 2000, 2000, 2000]
    );
    assert_eq!(millis(5, 0.1), 1800);
    assert_eq!(millis(5, 0.2), 1600);
    // A fraction past the cap's share is clamped to it.
    assert_eq!(millis(5, 0.9), 1600);
    assert!(outcome_poll_delay(u32::MAX, 0.0) <= Duration::from_secs(2));
}

/// The Wake follows an upgrade only to the pod its outcome was read for: a
/// re-drive that replaced the pod after the wait settled leaves a pod whose
/// outcome nobody read, so the activation answers retryably rather than
/// dial it.
#[test]
fn the_wake_follows_only_the_pod_whose_upgrade_settled() {
    let sandbox_id = SandboxId::new();
    let settled = "0f6d7f4e-1c2b-4a5d-9e8f-7a6b5c4d3e2f";
    let replacement = PodUid::parse("6a5b4c3d-2e1f-4a0b-9c8d-7e6f5a4b3c2d").expect("a pod UID");
    let same = PodUid::parse(settled).expect("a pod UID");

    settled_pod_is_recorded(sandbox_id, Some(settled), Some(&same)).expect("the Wake may follow");
    let error = settled_pod_is_recorded(sandbox_id, Some(settled), Some(&replacement))
        .expect_err("the Wake would dial a pod whose upgrade never settled");
    assert!(
        matches!(&error, SubstrateError::Unavailable { message } if message.contains("isn't read yet")),
        "{error:?}"
    );
    // With no pod seen or none recorded, the Wake's own check decides.
    settled_pod_is_recorded(sandbox_id, None, Some(&replacement)).expect("the Wake may follow");
    settled_pod_is_recorded(sandbox_id, Some(settled), None).expect("the Wake may follow");
}
