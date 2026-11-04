use pplx_space_common_core::types::SandboxId;
use std::time::Duration;

use super::*;
use pplx_space_api::controllers::substrate::RetireFence;

/// Every verdict, for exhaustive table-driven tests.
const PROBE_VERDICTS: &[ProbeVerdict] = &[
    ProbeVerdict::AliveOnNode,
    ProbeVerdict::DormantWithLocalArtifact,
    ProbeVerdict::DeadOnNode,
    ProbeVerdict::MissingOnNode,
    ProbeVerdict::Inconclusive,
];

fn candidate(
    lifecycle: SandboxLifecycle,
    ready_snapshot: bool,
    evacuation_restore_target: Option<SandboxLifecycle>,
) -> SweepCandidate {
    SweepCandidate {
        sandbox_id: SandboxId::new(),
        lifecycle,
        age: Duration::from_secs(900),
        evacuation_restore_target,
        ready_snapshot,
        fork_restore: false,
        node_name: Some("node-a".to_string()),
        routable_execution: true,
        activation_abandoned: false,
        episode_overdue: false,
        activation_attempt_held: true,
        pod_fence: RetireFence::NoExecution,
    }
}

fn unroutable_candidate(
    lifecycle: SandboxLifecycle,
    ready_snapshot: bool,
    evacuation_restore_target: Option<SandboxLifecycle>,
) -> SweepCandidate {
    SweepCandidate {
        routable_execution: false,
        ..candidate(lifecycle, ready_snapshot, evacuation_restore_target)
    }
}

/// A crashed create whose VM came up is failed and retired once its
/// activation is abandoned, unless a completion receipt can still finish it:
/// a routable execution that still holds its attempt defers to its node's
/// activation-completion receipt. One whose owner gave the activation up
/// holds no attempt, so it fails routable or not. An alive create without an
/// abandoned activation always defers. None is ever converged or hydrated.
#[test]
fn an_alive_abandoned_create_fails_unless_a_receipt_can_finish_it() {
    let defer = SweepAction::Defer {
        reason: DeferReason::AliveNotConvergeable,
    };
    for activation_abandoned in [true, false] {
        for routable_execution in [true, false] {
            for activation_attempt_held in [true, false] {
                let expected =
                    if activation_abandoned && !(routable_execution && activation_attempt_held) {
                        SweepAction::FailAndRetire
                    } else {
                        defer
                    };
                let create = SweepCandidate {
                    activation_abandoned,
                    routable_execution,
                    activation_attempt_held,
                    ..candidate(SandboxLifecycle::Creating, false, None)
                };
                assert_eq!(
                    decide(&create, ProbeVerdict::AliveOnNode),
                    expected,
                    "abandoned {activation_abandoned}, routable {routable_execution}, \
                     attempt held {activation_attempt_held}"
                );
            }
        }
    }
}

/// An abandoned activation fails an alive, never-routable row only when it's
/// `creating`. Every other lifecycle keeps its own alive-row decision:
/// `restoring` hydrates and converges to its wake target and the rest defer.
#[test]
fn an_abandoned_activation_fails_only_an_alive_create() {
    for lifecycle in super::super::SWEEPABLE_LIFECYCLES.iter().copied() {
        let candidate = SweepCandidate {
            activation_abandoned: true,
            routable_execution: false,
            ..candidate(lifecycle, false, None)
        };
        let expected = match lifecycle {
            SandboxLifecycle::Creating => SweepAction::FailAndRetire,
            SandboxLifecycle::Restoring => SweepAction::HydrateThenConverge {
                target: SandboxLifecycle::Started,
            },
            _ => SweepAction::Defer {
                reason: DeferReason::AliveNotConvergeable,
            },
        };
        assert_eq!(
            decide(&candidate, ProbeVerdict::AliveOnNode),
            expected,
            "lifecycle {lifecycle}"
        );
    }
}

#[test]
fn decision_table_covers_every_verdict() {
    use SandboxLifecycle::{Pausing, Restoring, Suspending};

    let cases: &[(SweepCandidate, ProbeVerdict, SweepAction)] = &[
        // Alive restoring converges to the wake target.
        (
            candidate(Restoring, false, None),
            ProbeVerdict::AliveOnNode,
            SweepAction::Converge {
                target: SandboxLifecycle::Started,
            },
        ),
        // An evacuation target, when present, is never downgraded.
        (
            candidate(Restoring, false, Some(SandboxLifecycle::Paused)),
            ProbeVerdict::AliveOnNode,
            SweepAction::Converge {
                target: SandboxLifecycle::Paused,
            },
        ),
        // An alive restoring row whose execution the DB reports
        // unroutable must be verified (and hydrated) before converge.
        (
            unroutable_candidate(Restoring, false, None),
            ProbeVerdict::AliveOnNode,
            SweepAction::HydrateThenConverge {
                target: SandboxLifecycle::Started,
            },
        ),
        // The evacuation target pass-through holds on the hydrate arm.
        (
            unroutable_candidate(Restoring, false, Some(SandboxLifecycle::Paused)),
            ProbeVerdict::AliveOnNode,
            SweepAction::HydrateThenConverge {
                target: SandboxLifecycle::Paused,
            },
        ),
        // Alive in any other lifecycle defers.
        (
            candidate(Pausing, true, None),
            ProbeVerdict::AliveOnNode,
            SweepAction::Defer {
                reason: DeferReason::AliveNotConvergeable,
            },
        ),
        // Dormant local artifact + canonical (restoring) snapshot reverts.
        (
            candidate(Restoring, true, None),
            ProbeVerdict::DormantWithLocalArtifact,
            SweepAction::RevertToSuspended,
        ),
        // A generic incomplete restore without a durable recovery source
        // follows the existing loud failure and cleanup path.
        (
            candidate(Restoring, false, None),
            ProbeVerdict::DormantWithLocalArtifact,
            SweepAction::FailAndRetire,
        ),
        // A fork target restores from another sandbox's durable artifact,
        // so it has no target-owned ready snapshot. Preserve a dormant
        // local copy instead of reverting to unrelated bytes.
        (
            SweepCandidate {
                fork_restore: true,
                ..candidate(Restoring, false, None)
            },
            ProbeVerdict::DormantWithLocalArtifact,
            SweepAction::Defer {
                reason: DeferReason::OnlyCopyOnNode,
            },
        ),
        // A dormant suspending row defers even with a "ready snapshot":
        // the matched backup is the previous cycle's bytes (the pointer
        // is never updated on wake), so the node holds the only current
        // copy and a revert would silently restore pre-wake state.
        (
            candidate(Suspending, true, None),
            ProbeVerdict::DormantWithLocalArtifact,
            SweepAction::Defer {
                reason: DeferReason::OnlyCopyOnNode,
            },
        ),
        // Dormant local artifact without a backup is the only copy.
        (
            candidate(Suspending, false, None),
            ProbeVerdict::DormantWithLocalArtifact,
            SweepAction::Defer {
                reason: DeferReason::OnlyCopyOnNode,
            },
        ),
        // Gone from the node + canonical (restoring) snapshot reverts.
        (
            candidate(Restoring, true, None),
            ProbeVerdict::DeadOnNode,
            SweepAction::RevertToSuspended,
        ),
        (
            candidate(Restoring, true, None),
            ProbeVerdict::MissingOnNode,
            SweepAction::RevertToSuspended,
        ),
        // A dead fork target has no target-owned snapshot to revert to.
        // Fail it loudly instead of leaving an immortal restoring row.
        (
            candidate(Restoring, false, None),
            ProbeVerdict::DeadOnNode,
            SweepAction::FailAndRetire,
        ),
        (
            candidate(Restoring, false, None),
            ProbeVerdict::MissingOnNode,
            SweepAction::FailAndRetire,
        ),
        // Gone from the node in a non-restoring lifecycle takes the old
        // loud fail+retire path even when a prior-cycle ready snapshot
        // exists — reverting to it would be silent data loss.
        (
            candidate(Suspending, true, None),
            ProbeVerdict::DeadOnNode,
            SweepAction::FailAndRetire,
        ),
        (
            candidate(Pausing, true, None),
            ProbeVerdict::MissingOnNode,
            SweepAction::FailAndRetire,
        ),
        // Gone from the node with nothing recoverable: old behavior.
        (
            candidate(Pausing, false, None),
            ProbeVerdict::DeadOnNode,
            SweepAction::FailAndRetire,
        ),
        (
            candidate(Pausing, false, None),
            ProbeVerdict::MissingOnNode,
            SweepAction::FailAndRetire,
        ),
        // Unreachable means defer.
        (
            candidate(Restoring, true, None),
            ProbeVerdict::Inconclusive,
            SweepAction::Defer {
                reason: DeferReason::ProbeInconclusive,
            },
        ),
    ];
    for (candidate, verdict, expected) in cases {
        assert_eq!(
            decide(candidate, *verdict),
            *expected,
            "verdict {verdict} on lifecycle {} (ready_snapshot={})",
            candidate.lifecycle,
            candidate.ready_snapshot,
        );
    }
}

/// The revert action trusts the durable snapshot as the sandbox's current
/// state, which only holds for `restoring` (the VM has not run since the
/// snapshot's artifact was pinned). Iterate the full lifecycle x verdict
/// space so adding a lifecycle or verdict can't silently widen revert
/// eligibility back to the stale-prior-cycle data-loss path.
#[test]
fn revert_to_suspended_is_restoring_only() {
    for lifecycle in super::super::SWEEPABLE_LIFECYCLES.iter().copied() {
        for verdict in PROBE_VERDICTS.iter().copied() {
            let action = decide(&candidate(lifecycle, true, None), verdict);
            if action == SweepAction::RevertToSuspended {
                assert_eq!(
                    lifecycle,
                    SandboxLifecycle::Restoring,
                    "revert chosen for {lifecycle} on verdict {verdict}; only restoring \
                     rows have a provably-current snapshot"
                );
            }
        }
    }
}

/// The failed-retire gate must defer exactly on a live VM and on an
/// unanswered probe — every verdict that affirmatively reports "no live
/// VM" permits the retire (which preserves node-local state).
#[test]
fn failed_retire_permitted_unless_alive_or_inconclusive() {
    for verdict in PROBE_VERDICTS {
        assert_eq!(
            verdict.permits_failed_retire(),
            !matches!(
                verdict,
                ProbeVerdict::AliveOnNode | ProbeVerdict::Inconclusive
            ),
            "verdict {verdict}"
        );
    }
}
