use std::collections::{BTreeMap, BTreeSet};

use super::planner::*;
use crate::db::activation_seal::{DrainEvacuation, DrainPrior};
use crate::service::*;
use pplx_space_snapshot_model::ArtifactKind;

const FULL_ARTIFACT: &str = "01939e3e-9001-7000-8000-000000000001";
const DISK_ARTIFACT: &str = "01939e3e-9002-7000-8000-000000000001";

fn row(id: &str, lifecycle: impl Into<SandboxStatus>) -> DrainRow {
    DrainRow {
        sandbox_id: id.to_owned(),
        lifecycle: lifecycle.into(),
        pool_member: false,
        volume_encryption: false,
        pin: None,
        suspend_refusal: None,
        snapshot_bytes: 0,
    }
}

fn evacuation(id: &str, prior: DrainPrior) -> (String, DrainEvacuation) {
    (
        id.to_owned(),
        DrainEvacuation {
            sandbox_id: id.to_owned(),
            prior,
        },
    )
}

fn pending(id: &str, artifact: &str) -> BTreeMap<String, BTreeSet<String>> {
    BTreeMap::from([(id.to_owned(), BTreeSet::from([artifact.to_owned()]))])
}

fn plan_one(
    row: DrainRow,
    pending_backups: &BTreeMap<String, BTreeSet<String>>,
    evacuations: &[(String, DrainEvacuation)],
    running: &[(&str, DrainAction)],
) -> DrainPlan {
    plan(
        &[row],
        &PlanInputs {
            pending_backups: pending_backups.clone(),
            evacuations: evacuations.iter().cloned().collect(),
            running: running
                .iter()
                .map(|(id, action)| ((*id).to_owned(), *action))
                .collect(),
            ..PlanInputs::default()
        },
    )
}

/// Every lifecycle, and each cleanup over each teardown, with no pin, record
/// or work in flight, beside a paused control row, for a forced and a gentle
/// drain: what the drain reports for it, the work it starts, and whether it
/// counts as a VM. The two differ only for a started row, which a gentle
/// drain holds, and in the lifecycles a suspend may start from.
#[test]
fn every_lifecycle_plans_as_the_drain_table_says() {
    use SandboxState as S;
    use SandboxStatus::{Owned, Unowned};
    for (gentle, state) in [false, true].into_iter().flat_map(|gentle| {
        SandboxStatus::all()
            .into_iter()
            .map(move |state| (gentle, state))
    }) {
        let drain_suspend = |id: &str| {
            if gentle {
                gentle_suspend(id)
            } else {
                suspend(id)
            }
        };
        let inputs = PlanInputs {
            gentle,
            ..PlanInputs::default()
        };
        let rows = [row("control", S::Paused), row("subject", state)];
        let plan = plan(&rows, &inputs);
        let state = format!("gentle={gentle} {state}");
        assert!(
            plan.actions.contains(&drain_suspend("control")),
            "{state}: the control is suspended"
        );
        let subject_pending: Vec<_> = plan
            .pending
            .iter()
            .filter(|(id, _)| id == "subject")
            .map(|(_, action)| action.as_str())
            .collect();
        let subject_blocked: Vec<_> = plan
            .blocked
            .iter()
            .filter(|(id, _)| id.as_deref() == Some("subject"))
            .map(|(_, reason)| reason.as_str())
            .collect();
        let subject_actions: Vec<_> = plan
            .actions
            .iter()
            .filter(|action| match action {
                PlannedAction::Suspend { sandbox_id, .. }
                | PlannedAction::PublishDisk { sandbox_id }
                | PlannedAction::PublishSnapshot { sandbox_id }
                | PlannedAction::Upload { sandbox_id, .. } => sandbox_id == "subject",
            })
            .cloned()
            .collect();
        let status = rows[1].lifecycle;
        let (expect_pending, expect_blocked, expect_actions): (Vec<String>, Vec<&str>, Vec<_>) =
            match status {
                Unowned(S::Started) if gentle => (vec![HOLD.into()], vec![], vec![]),
                Unowned(S::Started | S::Paused) => (
                    vec!["suspend".into()],
                    vec![],
                    vec![drain_suspend("subject")],
                ),
                Unowned(S::Stopped) => (
                    vec!["publish_disk".into()],
                    vec![],
                    vec![PlannedAction::PublishDisk {
                        sandbox_id: "subject".into(),
                    }],
                ),
                Unowned(S::Quarantined) => (vec![], vec!["quarantined"], vec![]),
                Owned {
                    cleanup: CleanupState::Failed,
                    ..
                } => (vec![], vec!["cleanup failed"], vec![]),
                // A suspended row that pins no artifact has nothing that
                // proves its snapshot durable: it is published, keeping its
                // lifecycle, and reported until it is.
                Unowned(S::Suspended) => (
                    vec!["publish_snapshot".into()],
                    vec![],
                    vec![PlannedAction::PublishSnapshot {
                        sandbox_id: "subject".into(),
                    }],
                ),
                // Nothing on the node to keep.
                Unowned(S::Failed) => (vec![], vec![], vec![]),
                other => (
                    vec![format!("settle:{}", other.legacy_label())],
                    vec![],
                    vec![],
                ),
            };
        assert_eq!(subject_pending, expect_pending, "{state}: pending");
        assert_eq!(subject_blocked, expect_blocked, "{state}: blocked");
        assert_eq!(subject_actions, expect_actions, "{state}: actions");

        // A warm-pool member in this status waits for its eviction while it
        // may still hold a VM, unless its cleanup failed, which blocks as a
        // resident's does.
        let member = DrainRow {
            pool_member: true,
            ..row("subject", status)
        };
        let member_plan = super::planner::plan(&[member], &inputs);
        let cleanup_failed = matches!(
            status,
            Owned {
                cleanup: CleanupState::Failed,
                ..
            }
        );
        let holds_vm = !matches!(status, Unowned(S::Stopped | S::Suspended | S::Failed));
        assert_eq!(
            member_plan.pending,
            if holds_vm && !cleanup_failed {
                vec![("subject".to_owned(), "warm_pool_eviction".to_owned())]
            } else {
                vec![]
            },
            "{state}: a warm-pool member's pending"
        );
        assert_eq!(
            member_plan.blocked,
            if cleanup_failed {
                vec![(Some("subject".to_owned()), "cleanup failed".to_owned())]
            } else {
                vec![]
            },
            "{state}: a warm-pool member's blocked"
        );
    }
}

/// A row with drain work in flight waits for it rather than starting it
/// again; a VM the node can't make durable blocks with the reason.
#[test]
fn a_vm_waits_for_its_suspend_or_blocks_with_why_it_cant() {
    let running = plan_one(
        row("a", SandboxState::Paused),
        &BTreeMap::new(),
        &[],
        &[("a", DrainAction::Suspend)],
    );
    assert_eq!(running.pending, [("a".into(), "suspend".into())]);
    assert!(running.actions.is_empty());

    let mut refused = row("a", SandboxState::Started);
    refused.suspend_refusal = Some("can't be made durable: no memory snapshots".into());
    let blocked = plan_one(refused, &BTreeMap::new(), &[], &[]);
    assert_eq!(
        blocked.blocked,
        [(
            Some("a".into()),
            "can't be made durable: no memory snapshots".into()
        )]
    );
    assert!(blocked.actions.is_empty() && blocked.pending.is_empty());
}

/// A stopped sandbox: an encrypted disk blocks; a durable pinned disk is
/// evacuated at once; a disk still uploading finishes its upload; no disk is
/// published.
#[test]
fn a_stopped_sandbox_is_published_once_and_reported_with_its_disk() {
    let mut encrypted = row("a", SandboxState::Stopped);
    encrypted.volume_encryption = true;
    let plan_encrypted = plan_one(encrypted, &BTreeMap::new(), &[], &[]);
    assert_eq!(plan_encrypted.blocked.len(), 1);
    assert!(plan_encrypted.actions.is_empty());

    let mut durable = row("a", SandboxState::Stopped);
    durable.pin = Some((DISK_ARTIFACT.into(), ArtifactKind::Disk));
    let plan_durable = plan_one(durable.clone(), &BTreeMap::new(), &[], &[]);
    assert_eq!(
        plan_durable.evacuated,
        [("a".into(), DrainPrior::Stopped, DISK_ARTIFACT.into())]
    );
    assert!(plan_durable.pending.is_empty() && plan_durable.blocked.is_empty());

    let plan_uploading = plan_one(durable, &pending("a", DISK_ARTIFACT), &[], &[]);
    assert_eq!(plan_uploading.pending, [("a".into(), "upload".into())]);
    assert!(plan_uploading.evacuated.is_empty());

    let mut full_pin = row("a", SandboxState::Stopped);
    full_pin.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let plan_full = plan_one(full_pin, &BTreeMap::new(), &[], &[]);
    assert_eq!(
        plan_full.actions,
        [PlannedAction::PublishDisk {
            sandbox_id: "a".into()
        }],
        "a stopped sandbox's disk is what it needs, not an older full snapshot"
    );
}

/// A sandbox the drain suspended is reported with the lifecycle it had once
/// its snapshot is durable, and waits for the upload until then. A
/// suspended sandbox the drain didn't suspend is already durable and is not
/// reported.
#[test]
fn a_suspended_evacuation_reports_its_prior_once_durable() {
    let mut suspended = row("a", SandboxState::Suspended);
    suspended.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let record = [evacuation("a", DrainPrior::Paused)];

    let durable = plan_one(suspended.clone(), &BTreeMap::new(), &record, &[]);
    assert_eq!(
        durable.evacuated,
        [("a".into(), DrainPrior::Paused, FULL_ARTIFACT.into())]
    );

    let uploading = plan_one(
        suspended.clone(),
        &pending("a", FULL_ARTIFACT),
        &record,
        &[],
    );
    assert_eq!(uploading.pending, [("a".into(), "upload".into())]);
    assert!(uploading.evacuated.is_empty());

    let not_ours = plan_one(suspended.clone(), &BTreeMap::new(), &[], &[]);
    assert_eq!(not_ours, DrainPlan::default());

    let mut lost = row("a", SandboxState::Suspended);
    lost.pin = None;
    let lost = plan_one(lost, &BTreeMap::new(), &record, &[]);
    assert_eq!(
        lost.blocked.len(),
        1,
        "a drain suspend that left no snapshot"
    );
}

/// A suspended sandbox whose row pins no artifact, one the drain didn't
/// suspend, is unproven: the drain publishes its snapshot under the window,
/// waits while that runs, finishes its upload once it is pinned, and reports
/// nothing once it is durable. It is never reported evacuated: the drain
/// gave it no prior lifecycle.
#[test]
fn an_unpinned_suspended_sandbox_is_published_before_the_drain_is_done() {
    let unpinned = row("a", SandboxState::Suspended);
    let publish = PlannedAction::PublishSnapshot {
        sandbox_id: "a".into(),
    };

    let first = plan_one(unpinned.clone(), &BTreeMap::new(), &[], &[]);
    assert_eq!(first.actions, [publish]);
    assert_eq!(first.pending, [("a".into(), "publish_snapshot".into())]);
    assert!(first.evacuated.is_empty() && first.blocked.is_empty());

    let running = plan_one(
        unpinned.clone(),
        &BTreeMap::new(),
        &[],
        &[("a", DrainAction::PublishSnapshot)],
    );
    assert!(running.actions.is_empty(), "{running:?}");
    assert_eq!(running.pending, [("a".into(), "publish_snapshot".into())]);
    let rows = [unpinned.clone()];
    let inputs = PlanInputs {
        running: BTreeMap::from([("a".to_owned(), DrainAction::PublishSnapshot)]),
        ..PlanInputs::default()
    };
    assert_eq!(
        inputs.window_in_flight(&rows),
        1,
        "a publication takes a slot"
    );

    let queued = plan(
        &[row("b", SandboxState::Started), unpinned.clone()],
        &PlanInputs {
            window: 1,
            ..PlanInputs::default()
        },
    );
    assert_eq!(queued.actions, [suspend("b")], "a VM goes first");
    assert!(queued.pending.contains(&("a".into(), "queued".into())));

    let failed = plan(
        &rows,
        &PlanInputs {
            failures: BTreeMap::from([(
                ("a".to_owned(), DrainAction::PublishSnapshot),
                (MAX_ACTION_FAILURES, "no snapshot".to_owned()),
            )]),
            ..PlanInputs::default()
        },
    );
    assert_eq!(
        failed.blocked,
        [(
            Some("a".into()),
            format!("publish_snapshot failed {MAX_ACTION_FAILURES} times: no snapshot")
        )]
    );

    let mut adopted = unpinned;
    adopted.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let uploading = plan_one(adopted.clone(), &pending("a", FULL_ARTIFACT), &[], &[]);
    assert_eq!(uploading.pending, [("a".into(), "upload".into())]);
    assert!(uploading.evacuated.is_empty());
    let durable = plan_one(adopted, &BTreeMap::new(), &[], &[]);
    assert_eq!(durable, DrainPlan::default());
}

/// An unclaimed warm-pool member is not a resident: nothing is started for
/// it, but its VM keeps the drain from being done, as a pending eviction
/// that blocks once it has waited past the settle bound; one with no VM
/// holds nothing.
#[test]
fn a_pool_member_is_left_to_the_pool_and_says_so() {
    let mut member = row("a", SandboxState::Started);
    member.pool_member = true;
    let waiting = plan(std::slice::from_ref(&member), &PlanInputs::default());
    assert!(waiting.actions.is_empty() && waiting.blocked.is_empty());
    assert_eq!(waiting.pending, [("a".into(), "warm_pool_eviction".into())]);

    let stuck = plan(
        std::slice::from_ref(&member),
        &PlanInputs {
            unchanged: BTreeSet::from(["a".to_owned()]),
            ..PlanInputs::default()
        },
    );
    assert!(stuck.pending.is_empty());
    assert_eq!(stuck.blocked.len(), 1, "{stuck:?}");

    member.lifecycle = SandboxState::Stopped.into();
    let idle = plan(&[member], &PlanInputs::default());
    assert!(idle.pending.is_empty() && idle.blocked.is_empty());
}

/// The inventory's process, cache and run directory findings, and those
/// scanners' failures, block, and so does a failure that names no scanner
/// this build knows; the other scanners' findings and failures are ignored.
#[test]
fn the_orphan_inventory_blocks_only_on_what_can_hold_a_resident() {
    let gateway_root = "/run/gateway";
    let page = pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse {
        orphaned_namespaces: vec!["ns-1".into()],
        orphaned_cgroups: vec!["cg-1".into()],
        orphaned_run_dirs: vec!["/run/sandboxes/x".into(), "/run/gateway/y".into()],
        orphaned_cache_dirs: vec!["/cache/z".into()],
        orphaned_nft_rules: vec!["sveth-1".into()],
        orphaned_processes: vec!["42:sbx".into()],
        warnings: vec![
            "Nft: chain missing".into(),
            "GatewayRunDirs: No such file or directory".into(),
            "Namespaces: denied".into(),
            "Cgroups: denied".into(),
            "RunDirs: denied".into(),
            "CacheDirs: denied".into(),
            "Processes: denied".into(),
            "Volumes: denied".into(),
            "unnamed failure".into(),
        ],
        ..Default::default()
    };
    let mut findings = OrphanFindings::default();
    classify_orphan_page(&page, gateway_root, &mut findings);
    assert_eq!(
        findings.dirs,
        [
            ("z".to_owned(), "orphan cache directory /cache/z".to_owned()),
            (
                "x".to_owned(),
                "orphan run directory /run/sandboxes/x".to_owned()
            ),
        ]
    );
    assert_eq!(
        findings.blocked,
        [
            "untracked runtime process 42:sbx",
            "orphan inventory scan failed: RunDirs: denied",
            "orphan inventory scan failed: CacheDirs: denied",
            "orphan inventory scan failed: Processes: denied",
            "orphan inventory scan failed: Volumes: denied",
            "orphan inventory scan failed: unnamed failure",
        ]
    );

    let mut clean = OrphanFindings::default();
    classify_orphan_page(&Default::default(), gateway_root, &mut clean);
    assert_eq!(clean, OrphanFindings::default());
}

/// Under a cancel, each sandbox the drain suspended is handed back: one
/// whose snapshot is durable is evacuated with its recorded prior, one still
/// uploading has its upload driven, one whose suspend runs is waited for, and
/// one the drain left without a durable snapshot blocks, as does one whose
/// cleanup failed. A record whose sandbox runs again, is stopped, failed or
/// gone is forgotten. Nothing is
/// suspended: a started row with no record, the control, gets no work.
#[test]
fn a_cancel_hands_back_what_the_drain_suspended() {
    use SandboxState as S;
    let pinned = |id: &str, lifecycle, artifact: &str| DrainRow {
        pin: Some((artifact.to_owned(), ArtifactKind::Full)),
        ..row(id, lifecycle)
    };
    let rows = [
        row("control", S::Started),
        pinned("durable", S::Suspended, FULL_ARTIFACT),
        pinned("uploading", S::Suspended, FULL_ARTIFACT),
        row("suspending", S::Started),
        row("resumed", S::Started),
        row("stopped", S::Stopped),
        row("failed", S::Failed),
        row("lost", S::Suspended),
        row("cleanup", SandboxStatus::CLEANUP_FAILED),
        row("left_paused", S::Paused),
        row("still_paused", S::Paused),
    ];
    let recorded = [
        "durable",
        "uploading",
        "suspending",
        "resumed",
        "stopped",
        "failed",
        "lost",
        "cleanup",
        "gone",
        "still_paused",
    ];
    let inputs = PlanInputs {
        pending_backups: pending("uploading", FULL_ARTIFACT),
        evacuations: recorded
            .iter()
            .map(|id| evacuation(id, DrainPrior::Paused))
            .chain([evacuation("left_paused", DrainPrior::Started)])
            .collect(),
        running: BTreeMap::from([("suspending".to_owned(), DrainAction::Suspend)]),
        ..PlanInputs::default()
    };

    let (plan, forget) = plan_cancel(&rows, &inputs, &[]);

    assert_eq!(
        plan.evacuated,
        [(
            "durable".to_owned(),
            DrainPrior::Paused,
            FULL_ARTIFACT.to_owned()
        )]
    );
    assert_eq!(
        plan.pending,
        [
            ("suspending".to_owned(), "suspend".to_owned()),
            ("uploading".to_owned(), "upload".to_owned()),
        ]
    );
    assert_eq!(
        plan.actions,
        [PlannedAction::Upload {
            sandbox_id: "uploading".into(),
            artifact_id: FULL_ARTIFACT.into(),
            kind: ArtifactKind::Full,
        }]
    );
    assert_eq!(
        plan.blocked,
        [
            (Some("cleanup".to_owned()), "cleanup failed".to_owned()),
            (
                Some("left_paused".to_owned()),
                "started before the drain, left paused by its suspend; kept until named in forget"
                    .to_owned()
            ),
            (
                Some("lost".to_owned()),
                "the drain's suspend left the sandbox suspended without a durable snapshot"
                    .to_owned()
            ),
        ]
    );
    assert_eq!(
        forget,
        ["failed", "gone", "resumed", "still_paused", "stopped"]
    );
    let (_, named) = plan_cancel(&rows, &inputs, &["left_paused".to_owned()]);
    assert!(
        named.contains(&"left_paused".to_owned()),
        "named in forget, it is forgotten"
    );
}

/// A cancel of a drain evacuating under its window starts no suspend, the
/// ones it queued included, however much room the window has; the suspend
/// still running finishes and is handed back once its snapshot is durable.
/// The control: the drain under the same window starts the queued suspend
/// once the running one is done.
#[test]
fn a_cancel_under_the_window_starts_no_suspend_and_hands_back_the_running_one() {
    use SandboxState as S;
    let durable = DrainRow {
        pin: Some((FULL_ARTIFACT.to_owned(), ArtifactKind::Full)),
        ..row("running", S::Suspended)
    };
    let before = [
        row("running", S::Started),
        row("queued", S::Started),
        row("stopped", S::Stopped),
    ];
    let after = [
        durable,
        row("queued", S::Started),
        row("stopped", S::Stopped),
    ];
    let inputs = |window: usize, running: bool| PlanInputs {
        window,
        evacuations: [evacuation("running", DrainPrior::Started)].into(),
        running: if running {
            BTreeMap::from([("running".to_owned(), DrainAction::Suspend)])
        } else {
            BTreeMap::new()
        },
        ..PlanInputs::default()
    };

    let drain = plan(&before, &inputs(1, true));
    assert!(drain.actions.is_empty(), "{drain:?}");
    assert_eq!(
        drain.pending,
        [
            ("running".to_owned(), "suspend".to_owned()),
            ("queued".to_owned(), "queued".to_owned()),
            ("stopped".to_owned(), "queued".to_owned()),
        ]
    );

    for window in [1, 9] {
        let (cancel, forget) = plan_cancel(&before, &inputs(window, true), &[]);
        assert!(cancel.actions.is_empty(), "window {window}: {cancel:?}");
        assert_eq!(
            cancel.pending,
            [("running".to_owned(), "suspend".to_owned())],
            "window {window}"
        );
        assert_eq!(forget, Vec::<String>::new(), "window {window}");

        let (cancel, forget) = plan_cancel(&after, &inputs(window, false), &[]);
        assert!(cancel.actions.is_empty(), "window {window}: {cancel:?}");
        assert_eq!(
            cancel.evacuated,
            [(
                "running".to_owned(),
                DrainPrior::Started,
                FULL_ARTIFACT.to_owned()
            )],
            "window {window}"
        );
        assert!(cancel.pending.is_empty(), "window {window}: {cancel:?}");
        assert_eq!(forget, Vec::<String>::new(), "window {window}");
    }

    let drain = plan(&after, &inputs(1, false));
    assert_eq!(drain.actions, [suspend("queued")]);
}

/// A cancel keeps a sandbox's record, and reports it, while the window
/// counts it in flight: a suspend or disk publication running, or its pinned
/// snapshot still uploading. That holds whatever the row's lifecycle, and
/// when the control plane names it to forget; a sandbox with nothing in
/// flight is forgotten when named, or when nothing is left to hand back. A
/// started sandbox (each record here is one) that the drain left paused has
/// something left: the lifecycle it had.
#[test]
fn a_cancel_forgets_a_record_only_once_the_window_counts_nothing_for_it() {
    use SandboxState as S;
    #[derive(Clone, Copy, Debug)]
    enum InFlight {
        Nothing,
        Running(DrainAction),
        Uploading,
        UploadBlocked,
    }
    let states = [
        InFlight::Nothing,
        InFlight::Running(DrainAction::Suspend),
        InFlight::Running(DrainAction::PublishDisk),
        InFlight::Running(DrainAction::Upload),
        InFlight::Uploading,
        InFlight::UploadBlocked,
    ];
    for lifecycle in [S::Started, S::Paused, S::Stopped, S::Failed, S::Suspended] {
        for in_flight in states {
            for named in [false, true] {
                let case = format!("{lifecycle} {in_flight:?} named={named}");
                let id = "sbx";
                let subject = DrainRow {
                    pin: Some((FULL_ARTIFACT.to_owned(), ArtifactKind::Full)),
                    ..row(id, lifecycle)
                };
                let mut inputs = PlanInputs {
                    evacuations: [
                        evacuation(id, DrainPrior::Started),
                        evacuation("control", DrainPrior::Started),
                    ]
                    .into(),
                    ..PlanInputs::default()
                };
                match in_flight {
                    InFlight::Nothing => {}
                    InFlight::Running(action) => {
                        inputs.running.insert(id.to_owned(), action);
                    }
                    InFlight::Uploading => {
                        inputs.pending_backups = pending(id, FULL_ARTIFACT);
                        inputs.active_uploads = BTreeSet::from([FULL_ARTIFACT.to_owned()]);
                    }
                    InFlight::UploadBlocked => {
                        inputs.pending_backups = pending(id, FULL_ARTIFACT);
                        inputs.active_uploads = BTreeSet::from([FULL_ARTIFACT.to_owned()]);
                        inputs.failures.insert(
                            (id.to_owned(), DrainAction::Upload),
                            (MAX_ACTION_FAILURES, "denied".to_owned()),
                        );
                    }
                }
                let rows = [subject, row("control", S::Started)];
                let counted = inputs.window_in_flight(&rows) > 0;
                let expect_counted = matches!(
                    in_flight,
                    InFlight::Running(DrainAction::Suspend | DrainAction::PublishDisk)
                        | InFlight::Uploading
                );
                assert_eq!(counted, expect_counted, "{case}: window_in_flight");
                let names = if named {
                    vec![id.to_owned()]
                } else {
                    Vec::new()
                };
                let (plan, forget) = plan_cancel(&rows, &inputs, &names);
                let forgotten = forget.contains(&id.to_owned());
                let reported_pending = plan.pending.iter().any(|(sandbox, _)| sandbox == id);
                if counted {
                    assert!(
                        !forgotten && reported_pending,
                        "{case}: {plan:?} {forget:?}"
                    );
                }
                let expect_forgotten = match in_flight {
                    InFlight::Nothing => named || !matches!(lifecycle, S::Suspended | S::Paused),
                    _ => false,
                };
                if matches!((lifecycle, in_flight), (S::Paused, InFlight::Nothing)) && !named {
                    assert_eq!(
                        plan.blocked
                            .iter()
                            .filter(|(sandbox, _)| sandbox.as_deref() == Some(id))
                            .count(),
                        1,
                        "{case}: a started sandbox left paused is blocked: {plan:?}"
                    );
                }
                assert_eq!(forgotten, expect_forgotten, "{case}: {plan:?} {forget:?}");
                assert!(forget.contains(&"control".to_owned()), "{case}");
            }
        }
    }
}

const NAMED_ARTIFACT: &str = "01939e3e-9003-7000-8000-000000000001";

/// A local-only artifact whose sandbox has no row blocks: nothing can
/// enqueue its upload.
#[test]
fn a_local_only_artifact_without_a_row_blocks() {
    let local_only = LocalOnlyArtifacts::from([(
        "gone".to_owned(),
        vec![(NAMED_ARTIFACT.to_owned(), ArtifactKind::Disk)],
    )]);
    let plan = plan(
        &[row("control", SandboxState::Stopped)],
        &PlanInputs {
            local_only,
            ..PlanInputs::default()
        },
    );
    assert_eq!(
        plan.blocked,
        [(
            Some("gone".to_owned()),
            format!("local-only artifact {NAMED_ARTIFACT} has no sandbox row")
        )]
    );
}

/// A sandbox whose suspend is running is reported once, pending or blocked,
/// though its own arm and each of its two other local-only artifacts wait
/// for that suspend; nothing else starts for it while the suspend runs. A
/// gentle drain plans a running suspend the same way.
#[test]
fn a_running_action_is_reported_once_per_sandbox() {
    const NAMED: &str = "01939e3e-9003-7000-8000-000000000001";
    const SUPERSEDED: &str = "01939e3e-9004-7000-8000-000000000001";
    for gentle in [false, true] {
        for (failures, pending, blocked) in
            [(MAX_ACTION_FAILURES - 1, 1, 0), (MAX_ACTION_FAILURES, 0, 1)]
        {
            let plan = plan(
                &[row("a", SandboxState::Started)],
                &PlanInputs {
                    local_only: BTreeMap::from([(
                        "a".to_owned(),
                        vec![
                            (NAMED.to_owned(), ArtifactKind::Full),
                            (SUPERSEDED.to_owned(), ArtifactKind::Full),
                        ],
                    )]),
                    running: BTreeMap::from([("a".to_owned(), DrainAction::Suspend)]),
                    failures: BTreeMap::from([(
                        ("a".to_owned(), DrainAction::Suspend),
                        (failures, "no space".to_owned()),
                    )]),
                    gentle,
                    ..PlanInputs::default()
                },
            );
            let case = format!("gentle={gentle} failures={failures}");
            assert_eq!(
                plan.pending,
                vec![("a".to_owned(), "suspend".to_owned()); pending],
                "{case}"
            );
            assert_eq!(
                plan.blocked,
                vec![
                    (
                        Some("a".to_owned()),
                        format!("suspend failed {failures} times: no space")
                    );
                    blocked
                ],
                "{case}"
            );
            assert!(plan.actions.is_empty(), "{case}: {:?}", plan.actions);
        }
    }
}

/// Work that has failed fewer than the bound times is pending; at the bound
/// it is blocked with the last failure, whether it is running or about to
/// start again, and it is still retried. The count is per kind of work: a
/// suspend's failures don't block the sandbox's upload.
#[test]
fn a_persistent_failure_blocks_with_its_reason_and_is_still_retried() {
    let plan_with = |failures: u32, running: &[(&str, DrainAction)]| {
        plan(
            &[row("a", SandboxState::Paused)],
            &PlanInputs {
                failures: [DrainAction::Suspend, DrainAction::Upload]
                    .into_iter()
                    .map(|action| {
                        (
                            ("a".to_owned(), action),
                            (failures, format!("no space for {}", action.as_str())),
                        )
                    })
                    .collect(),
                running: running
                    .iter()
                    .map(|(id, action)| ((*id).to_owned(), *action))
                    .collect(),
                ..PlanInputs::default()
            },
        )
    };
    let suspend = [suspend("a")];

    let below = plan_with(MAX_ACTION_FAILURES - 1, &[]);
    assert_eq!(below.pending, [("a".into(), "suspend".into())]);
    assert!(below.blocked.is_empty());
    assert_eq!(below.actions, suspend);

    let at = plan_with(MAX_ACTION_FAILURES, &[]);
    assert!(at.pending.is_empty());
    assert_eq!(
        at.blocked,
        [(
            Some("a".into()),
            format!("suspend failed {MAX_ACTION_FAILURES} times: no space for suspend")
        )]
    );
    assert_eq!(at.actions, suspend, "still retried");

    let running = plan_with(MAX_ACTION_FAILURES, &[("a", DrainAction::Upload)]);
    assert_eq!(
        running.blocked,
        [(
            Some("a".into()),
            format!("upload failed {MAX_ACTION_FAILURES} times: no space for upload")
        )]
    );
    assert!(running.actions.is_empty());

    // Only the suspend failed: the upload running now is pending.
    let other_kind = plan(
        &[row("a", SandboxState::Paused)],
        &PlanInputs {
            failures: BTreeMap::from([(
                ("a".to_owned(), DrainAction::Suspend),
                (MAX_ACTION_FAILURES, "no space".to_owned()),
            )]),
            running: BTreeMap::from([("a".to_owned(), DrainAction::Upload)]),
            ..PlanInputs::default()
        },
    );
    assert_eq!(other_kind.pending, [("a".into(), "upload".into())]);
    assert!(other_kind.blocked.is_empty());
}

/// A row in a transition the drain doesn't drive is pending until it has sat
/// there past the bound, then blocked with its state.
#[test]
fn a_row_stuck_in_a_transition_blocks_past_the_bound() {
    let plan_with = |unchanged: bool| {
        plan(
            &[row("a", SandboxState::Created)],
            &PlanInputs {
                unchanged: unchanged.then(|| "a".to_owned()).into_iter().collect(),
                ..PlanInputs::default()
            },
        )
    };
    assert_eq!(
        plan_with(false).pending,
        [("a".into(), "settle:created".into())]
    );
    let stuck = plan_with(true);
    assert!(stuck.pending.is_empty());
    assert_eq!(
        stuck.blocked,
        [(
            Some("a".into()),
            "created for over 15 minutes, a transition the drain doesn't drive".into()
        )]
    );
}

const MIB: u64 = 1024 * 1024;

/// A forced drain's suspend of `id`.
fn suspend(id: &str) -> PlannedAction {
    PlannedAction::Suspend {
        sandbox_id: id.into(),
        from: SuspendFrom::StartedOrPaused,
    }
}

/// The window starts paused VMs, then started ones, then stopped disks,
/// while the suspends and disk publications running and the uploads still
/// pending take fewer than its size; the rest are pending `queued`, or
/// blocked once they have failed the bound times.
#[test]
fn the_window_starts_paused_then_started_then_stopped_and_queues_the_rest() {
    let mut uploading = row("u", SandboxState::Suspended);
    uploading.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let rows = [
        row("s1", SandboxState::Started),
        row("d1", SandboxState::Stopped),
        row("p2", SandboxState::Paused),
        row("p1", SandboxState::Paused),
        row("s2", SandboxState::Started),
        row("r", SandboxState::Started),
        uploading,
    ];
    let inputs = |window: usize| PlanInputs {
        window,
        pending_backups: pending("u", FULL_ARTIFACT),
        active_uploads: BTreeSet::from([FULL_ARTIFACT.to_owned()]),
        running: BTreeMap::from([("r".to_owned(), DrainAction::Suspend)]),
        failures: BTreeMap::from([(
            ("s2".to_owned(), DrainAction::Suspend),
            (MAX_ACTION_FAILURES, "no space".to_owned()),
        )]),
        ..PlanInputs::default()
    };
    // The running suspend and the pending upload take two of five.
    let plan_five = plan(&rows, &inputs(5));
    assert_eq!(
        plan_five.actions,
        [suspend("p1"), suspend("p2"), suspend("s1")]
    );
    let pending_of = |plan: &DrainPlan, id: &str| -> Vec<String> {
        plan.pending
            .iter()
            .filter(|(sandbox_id, _)| sandbox_id == id)
            .map(|(_, reason)| reason.clone())
            .collect()
    };
    assert_eq!(pending_of(&plan_five, "s2"), Vec::<String>::new());
    assert_eq!(
        plan_five.blocked,
        [(
            Some("s2".into()),
            format!("suspend failed {MAX_ACTION_FAILURES} times: no space")
        )],
        "past the bound a queued row is blocked"
    );
    assert_eq!(pending_of(&plan_five, "d1"), ["queued"]);
    assert_eq!(pending_of(&plan_five, "r"), ["suspend"]);
    assert_eq!(pending_of(&plan_five, "u"), ["upload"]);

    // Room for everything: the stopped disk starts last.
    let plan_nine = plan(&rows, &inputs(9));
    assert_eq!(
        plan_nine.actions,
        [
            suspend("p1"),
            suspend("p2"),
            suspend("s1"),
            suspend("s2"),
            PlannedAction::PublishDisk {
                sandbox_id: "d1".into()
            },
        ]
    );
    assert!(
        plan_nine
            .pending
            .iter()
            .all(|(_, reason)| reason != "queued")
    );
}

/// A started or paused row's pending pin takes a window slot only while its
/// upload job is queued or running: the drain doesn't drive that upload, so
/// a capture a suspend rollback left pinned, its job given up, would hold the
/// slot for good. The paused rows here can't be suspended, so their own
/// suspends take no slot either.
#[test]
fn a_vm_pin_without_an_active_upload_takes_no_window_slot() {
    let paused: Vec<DrainRow> = (1..=8)
        .map(|n| {
            let mut paused = row(&format!("p{n}"), SandboxState::Paused);
            paused.pin = Some((format!("{FULL_ARTIFACT}-{n}"), ArtifactKind::Full));
            paused.suspend_refusal = Some("can't be made durable".into());
            paused
        })
        .collect();
    let rows: Vec<DrainRow> = paused
        .iter()
        .cloned()
        .chain([row("s", SandboxState::Started)])
        .collect();
    let pending_backups = paused
        .iter()
        .map(|row| {
            let artifact = row.pin.clone().unwrap().0;
            (row.sandbox_id.clone(), BTreeSet::from([artifact]))
        })
        .collect();
    let inputs = |active_uploads: BTreeSet<String>| PlanInputs {
        window: 8,
        pending_backups: BTreeMap::clone(&pending_backups),
        active_uploads,
        ..PlanInputs::default()
    };

    let given_up = plan(&rows, &inputs(BTreeSet::new()));
    assert_eq!(given_up.actions, [suspend("s")]);

    let uploading = plan(
        &rows,
        &inputs(
            paused
                .iter()
                .map(|row| row.pin.clone().unwrap().0)
                .collect(),
        ),
    );
    assert!(uploading.actions.is_empty(), "{:?}", uploading.actions);
    assert!(uploading.pending.contains(&("s".into(), "queued".into())));
}

/// A suspend starts only while the cache disk's headroom covers its snapshot
/// beside the snapshots of the suspends already running; otherwise it is
/// pending `disk`, and blocked once it has waited past the bound. A smaller
/// snapshot behind it still starts, and a node that doesn't measure its disk
/// starts every one.
#[test]
fn a_suspend_waits_for_disk_and_blocks_past_the_bound() {
    let sized = |id: &str, mib: u64| {
        let mut row = row(id, SandboxState::Started);
        row.snapshot_bytes = mib * MIB;
        row
    };
    let rows = [
        sized("a", 600),
        sized("b", 600),
        sized("c", 100),
        sized("r", 300),
    ];
    let inputs = |disk_headroom: Option<u64>, disk_waits: &[&str]| PlanInputs {
        disk_headroom,
        disk_waits: disk_waits.iter().map(|id| (*id).to_owned()).collect(),
        running: BTreeMap::from([("r".to_owned(), DrainAction::Suspend)]),
        ..PlanInputs::default()
    };

    let tight = plan(&rows, &inputs(Some(1024 * MIB), &[]));
    assert_eq!(tight.actions, [suspend("a"), suspend("c")]);
    assert!(tight.pending.contains(&("b".into(), "disk".into())));
    assert_eq!(tight.disk_waits, ["b"]);
    assert!(tight.blocked.is_empty());

    let past_bound = plan(&rows, &inputs(Some(1024 * MIB), &["b"]));
    assert_eq!(
        past_bound.blocked,
        [(
            Some("b".into()),
            "the cache disk hasn't had room for a 600 MiB snapshot for over 15 minutes".into()
        )]
    );
    assert_eq!(past_bound.disk_waits, ["b"], "it keeps waiting");

    let unmeasured = plan(&rows, &inputs(None, &[]));
    assert_eq!(
        unmeasured.actions,
        [suspend("a"), suspend("b"), suspend("c")]
    );
    assert!(unmeasured.disk_waits.is_empty());
}

/// An upload whose job is still queued or running is pending and not
/// enqueued again; once the job has ended, the drain enqueues it.
#[test]
fn an_upload_with_an_active_job_is_not_enqueued_again() {
    let mut suspended = row("a", SandboxState::Suspended);
    suspended.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let plan_with = |active: bool| {
        plan(
            std::slice::from_ref(&suspended),
            &PlanInputs {
                pending_backups: pending("a", FULL_ARTIFACT),
                evacuations: [evacuation("a", DrainPrior::Started)].into_iter().collect(),
                active_uploads: active
                    .then(|| FULL_ARTIFACT.to_owned())
                    .into_iter()
                    .collect(),
                ..PlanInputs::default()
            },
        )
    };
    let active = plan_with(true);
    assert_eq!(active.pending, [("a".into(), "upload".into())]);
    assert!(active.actions.is_empty());

    let ended = plan_with(false);
    assert_eq!(ended.pending, [("a".into(), "upload".into())]);
    assert_eq!(
        ended.actions,
        [PlannedAction::Upload {
            sandbox_id: "a".into(),
            artifact_id: FULL_ARTIFACT.into(),
            kind: ArtifactKind::Full,
        }]
    );
}

/// The disk's headroom is what is available above the floor disk admission
/// keeps free, and none once admission refuses allocations.
#[test]
fn the_disk_headroom_is_what_lies_above_the_admission_floor() {
    use pplx_space_host_linux::disk::DiskStatus;
    use pplx_space_node_disk_admission::{CacheDiskAdmissionConfig, CacheDiskMeasurement};
    const GIB: u64 = 1024 * MIB;
    let config = CacheDiskAdmissionConfig {
        min_available_bytes: GIB,
        ..CacheDiskAdmissionConfig::default()
    };
    let measured = |available_bytes: u64| CacheDiskMeasurement {
        status: DiskStatus {
            total_bytes: 40 * GIB,
            free_bytes: available_bytes,
            available_bytes,
        },
        btrfs: None,
    };
    // The floor is the larger of 5 % of the disk (2 GiB) and 1 GiB.
    assert_eq!(drain_disk_headroom(config, measured(10 * GIB)), 8 * GIB);
    assert_eq!(drain_disk_headroom(config, measured(GIB)), 0);
}

/// A gentle drain's suspend of `id`, which suspends it only from `paused`.
fn gentle_suspend(id: &str) -> PlannedAction {
    PlannedAction::Suspend {
        sandbox_id: id.into(),
        from: SuspendFrom::Paused,
    }
}

fn gentle(inputs: PlanInputs) -> PlanInputs {
    PlanInputs {
        gentle: true,
        ..inputs
    }
}

/// A gentle drain holds a started sandbox, reporting it pending with the
/// action `hold`, and plans every other row as a forced drain does: the
/// paused VM is suspended, the stopped disk published, the warm-pool member
/// left to its eviction, the VM it can't make durable blocked, and the
/// suspend already running waited for. A row in a transition settles into
/// the hold once it is started. Planned forced, the same started sandbox is
/// suspended.
#[test]
fn a_gentle_drain_holds_a_started_sandbox_and_plans_the_rest_as_forced() {
    let mut member = row("member", SandboxState::Started);
    member.pool_member = true;
    let mut refused = row("refused", SandboxState::Started);
    refused.suspend_refusal = Some("can't be made durable: no memory snapshots".into());
    let rows = [
        row("held", SandboxState::Started),
        row("paused", SandboxState::Paused),
        row("stopped", SandboxState::Stopped),
        row("resuming", SandboxState::Resuming),
        row("suspending", SandboxState::Started),
        member,
        refused,
    ];
    let inputs = PlanInputs {
        running: BTreeMap::from([("suspending".to_owned(), DrainAction::Suspend)]),
        ..PlanInputs::default()
    };
    let planned = plan(&rows, &gentle(inputs.clone()));
    let pending: BTreeMap<_, _> = planned.pending.iter().cloned().collect();
    assert_eq!(
        pending,
        BTreeMap::from([
            ("held".to_owned(), HOLD.to_owned()),
            ("paused".to_owned(), "suspend".to_owned()),
            ("stopped".to_owned(), "publish_disk".to_owned()),
            ("resuming".to_owned(), "settle:resuming".to_owned()),
            ("suspending".to_owned(), "suspend".to_owned()),
            ("member".to_owned(), "warm_pool_eviction".to_owned()),
        ]),
        "{planned:?}"
    );
    assert_eq!(
        planned.actions,
        [
            gentle_suspend("paused"),
            PlannedAction::PublishDisk {
                sandbox_id: "stopped".into(),
            },
        ]
    );
    assert_eq!(
        planned.blocked,
        [(
            Some("refused".into()),
            "can't be made durable: no memory snapshots".into()
        )]
    );

    let mut settled = rows.to_vec();
    settled[3].lifecycle = SandboxState::Started.into();
    let planned = plan(&settled, &gentle(inputs.clone()));
    assert!(
        planned
            .pending
            .contains(&("resuming".to_owned(), HOLD.to_owned())),
        "{planned:?}"
    );
    assert!(!planned.actions.contains(&gentle_suspend("resuming")));

    let forced = plan(&rows, &inputs);
    assert!(forced.actions.contains(&suspend("held")), "{forced:?}");
    assert!(!forced.pending.iter().any(|(_, action)| action == HOLD));
}

/// A started sandbox a gentle drain holds still has its pinned artifact's
/// upload finished: a pin without a durable copy is uploaded, and retried
/// after a failed attempt, while the row stays held; an upload whose job is
/// still active is left to it; one that failed too often blocks the drain.
/// The hold is reported throughout. Planned forced, the row is suspended
/// instead.
#[test]
fn a_held_started_sandbox_still_has_its_pending_pin_uploaded() {
    let mut held = row("held", SandboxState::Started);
    held.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let upload = PlannedAction::Upload {
        sandbox_id: "held".into(),
        artifact_id: FULL_ARTIFACT.into(),
        kind: ArtifactKind::Full,
    };
    let inputs = PlanInputs {
        pending_backups: pending("held", FULL_ARTIFACT),
        failures: BTreeMap::from([(
            ("held".to_owned(), DrainAction::Upload),
            (1, "connection reset".to_owned()),
        )]),
        ..PlanInputs::default()
    };
    let planned = plan(&[held.clone()], &gentle(inputs.clone()));
    assert_eq!(planned.actions, [upload], "{planned:?}");
    assert_eq!(
        planned.pending,
        [
            ("held".to_owned(), "upload".to_owned()),
            ("held".to_owned(), HOLD.to_owned())
        ]
    );
    assert!(planned.blocked.is_empty(), "{planned:?}");

    let active = plan(
        &[held.clone()],
        &gentle(PlanInputs {
            active_uploads: BTreeSet::from([FULL_ARTIFACT.to_owned()]),
            ..inputs.clone()
        }),
    );
    assert!(active.actions.is_empty(), "{active:?}");
    assert!(
        active
            .pending
            .contains(&("held".to_owned(), HOLD.to_owned()))
    );

    let given_up = plan(
        &[held.clone()],
        &gentle(PlanInputs {
            failures: BTreeMap::from([(
                ("held".to_owned(), DrainAction::Upload),
                (MAX_ACTION_FAILURES, "connection reset".to_owned()),
            )]),
            ..inputs.clone()
        }),
    );
    assert_eq!(given_up.pending, [("held".to_owned(), HOLD.to_owned())]);
    assert_eq!(
        given_up.blocked,
        [(
            Some("held".to_owned()),
            format!("upload failed {MAX_ACTION_FAILURES} times: connection reset")
        )]
    );

    let durable = plan(&[held.clone()], &gentle(PlanInputs::default()));
    assert!(durable.actions.is_empty(), "{durable:?}");
    assert_eq!(durable.pending, [("held".to_owned(), HOLD.to_owned())]);

    let forced = plan(&[held], &inputs);
    assert_eq!(forced.actions, [suspend("held")], "{forced:?}");
}

/// A sandbox that suspends on its own under a gentle drain has no drain
/// record: its snapshot's upload is finished, and once it is durable the
/// drain reports nothing for it, so the control plane restores it nowhere.
/// The one the drain suspended is reported evacuated.
#[test]
fn a_self_suspended_sandbox_is_uploaded_and_never_reported_evacuated() {
    let mut own = row("own", SandboxState::Suspended);
    own.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let mut drained = own.clone();
    drained.sandbox_id = "drained".into();
    let uploading = plan(
        &[own.clone()],
        &gentle(PlanInputs {
            pending_backups: pending("own", FULL_ARTIFACT),
            ..PlanInputs::default()
        }),
    );
    assert_eq!(uploading.pending, [("own".into(), "upload".into())]);
    assert_eq!(
        uploading.actions,
        [PlannedAction::Upload {
            sandbox_id: "own".into(),
            artifact_id: FULL_ARTIFACT.into(),
            kind: ArtifactKind::Full,
        }]
    );
    assert!(uploading.evacuated.is_empty());

    let durable = plan(
        &[own, drained],
        &gentle(PlanInputs {
            evacuations: [evacuation("drained", DrainPrior::Started)]
                .into_iter()
                .collect(),
            ..PlanInputs::default()
        }),
    );
    assert_eq!(
        durable.evacuated,
        [(
            "drained".to_owned(),
            DrainPrior::Started,
            FULL_ARTIFACT.to_owned()
        )],
        "{durable:?}"
    );
    assert!(durable.pending.is_empty() && durable.blocked.is_empty() && durable.actions.is_empty());
}
