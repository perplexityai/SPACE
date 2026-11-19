use std::collections::{BTreeMap, BTreeSet};

use super::planner::*;
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

fn pending(id: &str, artifact: &str) -> BTreeMap<String, BTreeSet<String>> {
    BTreeMap::from([(id.to_owned(), BTreeSet::from([artifact.to_owned()]))])
}

fn plan_one(
    row: DrainRow,
    pending_backups: &BTreeMap<String, BTreeSet<String>>,
    running: &[(&str, DrainAction)],
) -> DrainPlan {
    plan(
        &[row],
        &PlanInputs {
            pending_backups: pending_backups.clone(),
            running: running
                .iter()
                .map(|(id, action)| ((*id).to_owned(), *action))
                .collect(),
            ..PlanInputs::default()
        },
    )
}

/// Each pending row as `(sandbox, action)`.
fn waits(plan: &DrainPlan) -> Vec<(String, String)> {
    plan.pending
        .iter()
        .map(|wait| (wait.sandbox_id.clone(), wait.wait.label()))
        .collect()
}

/// `(sandbox, action)` pairs, for comparing with [`waits`].
fn expect(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(id, action)| ((*id).to_owned(), (*action).to_owned()))
        .collect()
}

/// The reason `id` is pending with `action`.
fn reason_of(plan: &DrainPlan, id: &str, action: &str) -> Option<String> {
    plan.pending
        .iter()
        .find(|wait| wait.sandbox_id == id && wait.wait.label() == action)
        .unwrap_or_else(|| panic!("{id} is not pending {action}: {plan:?}"))
        .reason
        .clone()
}

/// The drain's suspend of `id` while it evacuates.
fn suspend(id: &str) -> PlannedAction {
    PlannedAction::Suspend {
        sandbox_id: id.into(),
        from: SuspendFrom::StartedOrPaused,
    }
}

/// The drain's suspend of `id` while it holds, which suspends it only
/// from `paused`.
fn held_suspend(id: &str) -> PlannedAction {
    PlannedAction::Suspend {
        sandbox_id: id.into(),
        from: SuspendFrom::Paused,
    }
}

fn holding(inputs: PlanInputs) -> PlanInputs {
    PlanInputs {
        hold_running: true,
        ..inputs
    }
}

/// Every lifecycle, and each cleanup over each teardown, with no pin or work
/// in flight, beside a paused control row, holding and evacuating:
/// what the drain reports for it, the work it starts, and whether it counts
/// as a VM. The two differ only for a started row, which the drain holds
/// while holding, and in the lifecycles a suspend may start from. No
/// row is ever blocked: one the drain can't move is pending, with why.
#[test]
fn every_lifecycle_plans_as_the_drain_table_says() {
    use SandboxState as S;
    use SandboxStatus::{Owned, Unowned};
    for (hold_running, state) in [false, true].into_iter().flat_map(|hold_running| {
        SandboxStatus::all()
            .into_iter()
            .map(move |state| (hold_running, state))
    }) {
        let drain_suspend = |id: &str| {
            if hold_running {
                held_suspend(id)
            } else {
                suspend(id)
            }
        };
        let inputs = PlanInputs {
            hold_running,
            ..PlanInputs::default()
        };
        let rows = [row("control", S::Paused), row("subject", state)];
        let plan = plan(&rows, &inputs);
        let state = format!("hold_running={hold_running} {state}");
        assert!(
            plan.actions.contains(&drain_suspend("control")),
            "{state}: the control is suspended"
        );
        assert!(plan.blocked.is_empty(), "{state}: {plan:?}");
        let subject_pending: Vec<_> = waits(&plan)
            .into_iter()
            .filter(|(id, _)| id == "subject")
            .map(|(_, action)| action)
            .collect();
        let subject_actions: Vec<_> = plan
            .actions
            .iter()
            .filter(|action| action.target().0 == "subject")
            .cloned()
            .collect();
        let status = rows[1].lifecycle;
        let (expect_pending, expect_actions): (Vec<String>, Vec<_>) = match status {
            Unowned(S::Started) if hold_running => (vec!["hold".into()], vec![]),
            Unowned(S::Started | S::Paused) => {
                (vec!["suspend".into()], vec![drain_suspend("subject")])
            }
            Unowned(S::Stopped) => (
                vec!["publish_disk".into()],
                vec![PlannedAction::PublishDisk {
                    sandbox_id: "subject".into(),
                }],
            ),
            Unowned(S::Quarantined) => (vec!["settle:quarantined".into()], vec![]),
            Owned {
                cleanup: CleanupState::Failed,
                ..
            } => (vec!["settle:cleanup_failed".into()], vec![]),
            // A suspended row that pins no artifact has nothing that proves
            // its snapshot durable: it is published, keeping its lifecycle,
            // and reported until it is.
            Unowned(S::Suspended) => (
                vec!["publish_snapshot".into()],
                vec![PlannedAction::PublishSnapshot {
                    sandbox_id: "subject".into(),
                }],
            ),
            // Nothing on the node to keep.
            Unowned(S::Failed) => (vec![], vec![]),
            other => (vec![format!("settle:{}", other.legacy_label())], vec![]),
        };
        assert_eq!(subject_pending, expect_pending, "{state}: pending");
        assert_eq!(subject_actions, expect_actions, "{state}: actions");

        // A warm-pool member in this status waits for its eviction while it
        // may still hold a VM, and a failed cleanup waits as a resident's
        // does.
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
            waits(&member_plan),
            if cleanup_failed {
                expect(&[("subject", "settle:cleanup_failed")])
            } else if holds_vm {
                expect(&[("subject", "warm_pool_eviction")])
            } else {
                vec![]
            },
            "{state}: a warm-pool member's pending"
        );
        assert!(member_plan.blocked.is_empty(), "{state}: {member_plan:?}");
    }
}

/// A row with drain work in flight waits for it rather than starting it
/// again; a VM the node can't make durable is pending with the reason.
#[test]
fn a_vm_waits_for_its_suspend_or_says_why_it_cant() {
    let running = plan_one(
        row("a", SandboxState::Paused),
        &BTreeMap::new(),
        &[("a", DrainAction::Suspend)],
    );
    assert_eq!(waits(&running), expect(&[("a", "suspend")]));
    assert!(running.actions.is_empty());

    let mut refused = row("a", SandboxState::Started);
    refused.suspend_refusal = Some("can't be made durable: no memory snapshots".into());
    let waiting = plan_one(refused, &BTreeMap::new(), &[]);
    assert_eq!(waits(&waiting), expect(&[("a", "suspend")]));
    assert_eq!(
        reason_of(&waiting, "a", "suspend").as_deref(),
        Some("can't be made durable: no memory snapshots")
    );
    assert!(waiting.actions.is_empty() && waiting.blocked.is_empty());
}

/// A stopped sandbox: an encrypted disk can't be published and says so; a
/// durable pinned disk is evacuated at once; a disk still uploading
/// finishes its upload; no disk is published.
#[test]
fn a_stopped_sandbox_is_published_once_and_reported_with_its_disk() {
    let mut encrypted = row("a", SandboxState::Stopped);
    encrypted.volume_encryption = true;
    let plan_encrypted = plan_one(encrypted, &BTreeMap::new(), &[]);
    assert_eq!(
        reason_of(&plan_encrypted, "a", "publish_disk").as_deref(),
        Some("an encrypted stopped sandbox's disk can't be published")
    );
    assert!(plan_encrypted.actions.is_empty() && plan_encrypted.blocked.is_empty());

    let mut durable = row("a", SandboxState::Stopped);
    durable.pin = Some((DISK_ARTIFACT.into(), ArtifactKind::Disk));
    let plan_durable = plan_one(durable.clone(), &BTreeMap::new(), &[]);
    assert_eq!(
        plan_durable.evacuated,
        [("a".into(), ArtifactKind::Disk, DISK_ARTIFACT.into())]
    );
    assert!(plan_durable.pending.is_empty() && plan_durable.blocked.is_empty());

    let plan_uploading = plan_one(durable, &pending("a", DISK_ARTIFACT), &[]);
    assert_eq!(waits(&plan_uploading), expect(&[("a", "upload")]));
    assert!(plan_uploading.evacuated.is_empty());

    let mut full_pin = row("a", SandboxState::Stopped);
    full_pin.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let plan_full = plan_one(full_pin, &BTreeMap::new(), &[]);
    assert_eq!(
        plan_full.actions,
        [PlannedAction::PublishDisk {
            sandbox_id: "a".into()
        }],
        "a stopped sandbox's disk is what it needs, not an older full snapshot"
    );
}

/// A suspended sandbox is evacuated with its full snapshot once that is
/// durable, whatever suspended it, while holding too, and waits for
/// its upload until then. One that pins something other than a full
/// snapshot can't be evacuated and says so.
#[test]
fn a_suspended_sandbox_is_evacuated_once_its_snapshot_is_durable() {
    let mut suspended = row("a", SandboxState::Suspended);
    suspended.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));

    for hold_running in [false, true] {
        let durable = plan(
            std::slice::from_ref(&suspended),
            &PlanInputs {
                hold_running,
                ..PlanInputs::default()
            },
        );
        assert_eq!(
            durable.evacuated,
            [("a".into(), ArtifactKind::Full, FULL_ARTIFACT.into())],
            "hold_running={hold_running}"
        );
        assert!(durable.pending.is_empty() && durable.actions.is_empty());
    }

    let uploading = plan_one(suspended.clone(), &pending("a", FULL_ARTIFACT), &[]);
    assert_eq!(waits(&uploading), expect(&[("a", "upload")]));
    assert_eq!(
        uploading.actions,
        [PlannedAction::Upload {
            sandbox_id: "a".into(),
            artifact_id: FULL_ARTIFACT.into(),
            kind: ArtifactKind::Full,
        }]
    );
    assert!(uploading.evacuated.is_empty());

    let mut disk_pin = suspended;
    disk_pin.pin = Some((DISK_ARTIFACT.into(), ArtifactKind::Disk));
    let not_a_snapshot = plan_one(disk_pin, &BTreeMap::new(), &[]);
    assert_eq!(waits(&not_a_snapshot), expect(&[("a", "settle:suspended")]));
    assert!(not_a_snapshot.evacuated.is_empty() && not_a_snapshot.blocked.is_empty());
}

/// A suspended sandbox whose row pins no artifact is unproven: the drain
/// publishes its snapshot under the window, waits while that runs, finishes
/// its upload once it is pinned, and reports it evacuated once it is
/// durable. A publication that failed is retried, its failure the reason.
#[test]
fn an_unpinned_suspended_sandbox_is_published_before_the_drain_is_done() {
    let unpinned = row("a", SandboxState::Suspended);
    let publish = PlannedAction::PublishSnapshot {
        sandbox_id: "a".into(),
    };

    let first = plan_one(unpinned.clone(), &BTreeMap::new(), &[]);
    assert_eq!(first.actions, std::slice::from_ref(&publish));
    assert_eq!(waits(&first), expect(&[("a", "publish_snapshot")]));
    assert!(first.evacuated.is_empty() && first.blocked.is_empty());

    let running = plan_one(
        unpinned.clone(),
        &BTreeMap::new(),
        &[("a", DrainAction::PublishSnapshot)],
    );
    assert!(running.actions.is_empty(), "{running:?}");
    assert_eq!(waits(&running), expect(&[("a", "publish_snapshot")]));
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
    assert!(waits(&queued).contains(&("a".into(), "queued".into())));

    let failed = plan(
        &rows,
        &PlanInputs {
            failures: BTreeMap::from([(
                ("a".to_owned(), DrainAction::PublishSnapshot),
                "no snapshot".to_owned(),
            )]),
            ..PlanInputs::default()
        },
    );
    assert_eq!(failed.actions, [publish], "retried");
    assert_eq!(
        reason_of(&failed, "a", "publish_snapshot").as_deref(),
        Some("the last snapshot publication failed: no snapshot")
    );
    assert!(failed.blocked.is_empty());

    let mut adopted = unpinned;
    adopted.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let uploading = plan_one(adopted.clone(), &pending("a", FULL_ARTIFACT), &[]);
    assert_eq!(waits(&uploading), expect(&[("a", "upload")]));
    assert!(uploading.evacuated.is_empty());
    let durable = plan_one(adopted, &BTreeMap::new(), &[]);
    assert_eq!(
        durable.evacuated,
        [("a".into(), ArtifactKind::Full, FULL_ARTIFACT.into())]
    );
    assert!(durable.pending.is_empty() && durable.actions.is_empty());
}

/// An unclaimed warm-pool member is not a resident: nothing is started for
/// it, but its VM keeps the drain from being done, as a pending eviction
/// however long it waits; one with no VM holds nothing.
#[test]
fn a_pool_member_is_left_to_the_pool_and_says_so() {
    let mut member = row("a", SandboxState::Started);
    member.pool_member = true;
    let waiting = plan(std::slice::from_ref(&member), &PlanInputs::default());
    assert!(waiting.actions.is_empty() && waiting.blocked.is_empty());
    assert_eq!(waits(&waiting), expect(&[("a", "warm_pool_eviction")]));

    member.lifecycle = SandboxState::Stopped.into();
    let idle = plan(&[member], &PlanInputs::default());
    assert!(idle.pending.is_empty() && idle.blocked.is_empty());
}

const NAMED_ARTIFACT: &str = "01939e3e-9003-7000-8000-000000000001";

/// A local-only artifact whose sandbox has no row blocks the node, naming
/// both: nothing can enqueue its upload, and no row can carry it.
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
        [format!(
            "local-only artifact {NAMED_ARTIFACT} of sandbox gone has no sandbox row"
        )]
    );
}

/// A sandbox whose suspend is running is reported once, though its own arm
/// and each of its two other local-only artifacts wait for that suspend,
/// with the suspend's last failure as the reason; nothing else starts for it
/// while the suspend runs. While holding, a running suspend is planned
/// the same way.
#[test]
fn a_running_action_is_reported_once_per_sandbox() {
    const SUPERSEDED: &str = "01939e3e-9004-7000-8000-000000000001";
    for hold_running in [false, true] {
        for failure in [None, Some("no space")] {
            let plan = plan(
                &[row("a", SandboxState::Started)],
                &PlanInputs {
                    local_only: BTreeMap::from([(
                        "a".to_owned(),
                        vec![
                            (NAMED_ARTIFACT.to_owned(), ArtifactKind::Full),
                            (SUPERSEDED.to_owned(), ArtifactKind::Full),
                        ],
                    )]),
                    running: BTreeMap::from([("a".to_owned(), DrainAction::Suspend)]),
                    failures: failure
                        .map(|failure| (("a".to_owned(), DrainAction::Suspend), failure.to_owned()))
                        .into_iter()
                        .collect(),
                    hold_running,
                    ..PlanInputs::default()
                },
            );
            let case = format!("hold_running={hold_running} failure={failure:?}");
            assert_eq!(plan.pending.len(), 1, "{case}: {plan:?}");
            assert_eq!(waits(&plan), expect(&[("a", "suspend")]), "{case}");
            assert_eq!(
                reason_of(&plan, "a", "suspend"),
                failure.map(|failure| format!("the last suspend failed: {failure}")),
                "{case}"
            );
            assert!(plan.actions.is_empty(), "{case}: {:?}", plan.actions);
        }
    }
}

/// Work that failed is retried on every pass and stays pending, with its
/// last failure as the reason, whether it is running or about to start
/// again. A failure is per kind of work: a suspend's failure isn't the
/// reason of the sandbox's upload.
#[test]
fn a_failure_is_the_reason_of_its_work_which_is_retried() {
    let plan_with = |running: &[(&str, DrainAction)]| {
        plan(
            &[row("a", SandboxState::Paused)],
            &PlanInputs {
                failures: BTreeMap::from([(
                    ("a".to_owned(), DrainAction::Suspend),
                    "no space".to_owned(),
                )]),
                running: running
                    .iter()
                    .map(|(id, action)| ((*id).to_owned(), *action))
                    .collect(),
                ..PlanInputs::default()
            },
        )
    };

    let retried = plan_with(&[]);
    assert_eq!(retried.actions, [suspend("a")], "retried");
    assert_eq!(
        reason_of(&retried, "a", "suspend").as_deref(),
        Some("the last suspend failed: no space")
    );
    assert!(retried.blocked.is_empty());

    let running = plan_with(&[("a", DrainAction::Suspend)]);
    assert!(running.actions.is_empty());
    assert_eq!(
        reason_of(&running, "a", "suspend").as_deref(),
        Some("the last suspend failed: no space")
    );

    let other_kind = plan_with(&[("a", DrainAction::Upload)]);
    assert_eq!(waits(&other_kind), expect(&[("a", "upload")]));
    assert_eq!(reason_of(&other_kind, "a", "upload"), None);
}

/// A row in a transition the drain doesn't drive waits for it to settle,
/// pending with its state however long it sits there: the control plane
/// judges how long from the row's state stamp.
#[test]
fn a_row_in_a_transition_waits_for_it_to_settle() {
    let plan = plan(&[row("a", SandboxState::Created)], &PlanInputs::default());
    assert_eq!(waits(&plan), expect(&[("a", "settle:created")]));
    assert_eq!(reason_of(&plan, "a", "settle:created"), None);
    assert!(plan.blocked.is_empty() && plan.actions.is_empty());
}

const MIB: u64 = 1024 * 1024;

/// The window starts paused VMs, then started ones, then stopped disks,
/// while the suspends and disk publications running and the uploads still
/// active take fewer than its size; the rest are pending `queued`, a row
/// whose work failed with that failure as its reason.
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
            "no space".to_owned(),
        )]),
        ..PlanInputs::default()
    };
    // The running suspend and the active upload take two of five.
    let plan_five = plan(&rows, &inputs(5));
    assert_eq!(
        plan_five.actions,
        [suspend("p1"), suspend("p2"), suspend("s1")]
    );
    let pending_of = |plan: &DrainPlan, id: &str| -> Vec<String> {
        waits(plan)
            .into_iter()
            .filter(|(sandbox_id, _)| sandbox_id == id)
            .map(|(_, action)| action)
            .collect()
    };
    assert_eq!(pending_of(&plan_five, "s2"), ["queued"]);
    assert_eq!(
        reason_of(&plan_five, "s2", "queued").as_deref(),
        Some("the last suspend failed: no space")
    );
    assert_eq!(pending_of(&plan_five, "d1"), ["queued"]);
    assert_eq!(reason_of(&plan_five, "d1", "queued"), None);
    assert_eq!(pending_of(&plan_five, "r"), ["suspend"]);
    assert_eq!(pending_of(&plan_five, "u"), ["upload"]);
    assert!(plan_five.blocked.is_empty());

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
        waits(&plan_nine)
            .iter()
            .all(|(_, action)| action != "queued")
    );
}

/// A pinned artifact still without a durable copy takes a window slot only
/// while its upload job is queued or running. One whose job failed, or that
/// no job uploads yet, takes none, a VM's or a stopped disk's alike, so a
/// failed upload frees its slot. The paused rows here can't be suspended,
/// so their own suspends take no slot either.
#[test]
fn a_pin_takes_a_window_slot_only_while_its_upload_job_is_active() {
    let pinned: Vec<DrainRow> = (1..=8)
        .map(|n| {
            let mut pinned = if n % 2 == 0 {
                let mut paused = row(&format!("p{n}"), SandboxState::Paused);
                paused.suspend_refusal = Some("can't be made durable".into());
                paused
            } else {
                row(&format!("d{n}"), SandboxState::Stopped)
            };
            let kind = if n % 2 == 0 {
                ArtifactKind::Full
            } else {
                ArtifactKind::Disk
            };
            pinned.pin = Some((format!("{FULL_ARTIFACT}-{n}"), kind));
            pinned
        })
        .collect();
    let rows: Vec<DrainRow> = pinned
        .iter()
        .cloned()
        .chain([row("s", SandboxState::Started)])
        .collect();
    let artifacts: BTreeSet<String> = pinned
        .iter()
        .map(|row| row.pin.clone().unwrap().0)
        .collect();
    let pending_backups = pinned
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

    let failed = plan(&rows, &inputs(BTreeSet::new()));
    assert!(
        failed.actions.contains(&suspend("s")),
        "{:?}",
        failed.actions
    );

    let uploading = plan(&rows, &inputs(artifacts));
    assert!(
        !uploading.actions.contains(&suspend("s")),
        "{:?}",
        uploading.actions
    );
    assert!(waits(&uploading).contains(&("s".into(), "queued".into())));
}

/// A suspend starts only while the cache disk's headroom covers its snapshot
/// beside the snapshots of the suspends already running; otherwise it is
/// pending `disk`, with the snapshot's size, however long it waits. A
/// smaller snapshot behind it still starts, and a node that doesn't measure
/// its disk starts every one.
#[test]
fn a_suspend_waits_for_disk_room_for_its_snapshot() {
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
    let inputs = |disk_headroom: Option<u64>| PlanInputs {
        disk_headroom,
        running: BTreeMap::from([("r".to_owned(), DrainAction::Suspend)]),
        ..PlanInputs::default()
    };

    let tight = plan(&rows, &inputs(Some(1024 * MIB)));
    assert_eq!(tight.actions, [suspend("a"), suspend("c")]);
    assert_eq!(
        reason_of(&tight, "b", "disk").as_deref(),
        Some("the cache disk has no room yet for a 600 MiB snapshot")
    );
    assert!(tight.blocked.is_empty());

    let unmeasured = plan(&rows, &inputs(None));
    assert_eq!(
        unmeasured.actions,
        [suspend("a"), suspend("b"), suspend("c")]
    );
    assert!(
        !waits(&unmeasured)
            .iter()
            .any(|(_, action)| action == "disk")
    );
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
                active_uploads: active
                    .then(|| FULL_ARTIFACT.to_owned())
                    .into_iter()
                    .collect(),
                ..PlanInputs::default()
            },
        )
    };
    let active = plan_with(true);
    assert_eq!(waits(&active), expect(&[("a", "upload")]));
    assert!(active.actions.is_empty());

    let ended = plan_with(false);
    assert_eq!(waits(&ended), expect(&[("a", "upload")]));
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

/// While holding, the drain keeps a started sandbox running, reporting it
/// pending with the action `hold`, and plans every other row as it does
/// while evacuating: the paused VM is suspended, the stopped disk
/// published, the warm-pool member left to its eviction, the VM it can't
/// make durable pending with why, and the suspend already running waited
/// for. A row in a transition settles into the hold once it is started.
/// Evacuating, the same started sandbox is suspended.
#[test]
fn while_holding_the_drain_keeps_a_started_sandbox_running_and_plans_the_rest() {
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
    let planned = plan(&rows, &holding(inputs.clone()));
    let pending: BTreeMap<_, _> = waits(&planned).into_iter().collect();
    assert_eq!(
        pending,
        BTreeMap::from([
            ("held".to_owned(), "hold".to_owned()),
            ("paused".to_owned(), "suspend".to_owned()),
            ("stopped".to_owned(), "publish_disk".to_owned()),
            ("resuming".to_owned(), "settle:resuming".to_owned()),
            ("suspending".to_owned(), "suspend".to_owned()),
            ("member".to_owned(), "warm_pool_eviction".to_owned()),
            ("refused".to_owned(), "suspend".to_owned()),
        ]),
        "{planned:?}"
    );
    assert_eq!(reason_of(&planned, "held", "hold"), None);
    assert_eq!(
        planned
            .pending
            .iter()
            .find(|wait| wait.sandbox_id == "held")
            .map(DrainWait::reason)
            .as_deref(),
        Some(HOLD_REASON)
    );
    assert_eq!(
        planned.started,
        BTreeSet::from(["held", "suspending", "member", "refused"].map(str::to_owned)),
        "a started VM at the read is running, a paused one isn't"
    );
    assert_eq!(
        reason_of(&planned, "refused", "suspend").as_deref(),
        Some("can't be made durable: no memory snapshots")
    );
    assert_eq!(
        planned.actions,
        [
            held_suspend("paused"),
            PlannedAction::PublishDisk {
                sandbox_id: "stopped".into(),
            },
        ]
    );
    assert!(planned.blocked.is_empty());

    let mut settled = rows.to_vec();
    settled[3].lifecycle = SandboxState::Started.into();
    let planned = plan(&settled, &holding(inputs.clone()));
    assert!(
        waits(&planned).contains(&("resuming".to_owned(), "hold".to_owned())),
        "{planned:?}"
    );
    assert!(!planned.actions.contains(&held_suspend("resuming")));

    let evacuating = plan(&rows, &inputs);
    assert!(
        evacuating.actions.contains(&suspend("held")),
        "{evacuating:?}"
    );
    assert!(
        !waits(&evacuating)
            .iter()
            .any(|(_, action)| action == "hold")
    );
}

/// A started sandbox the drain holds still has its pinned artifact's upload
/// finished: a pin without a durable copy is uploaded, and retried after a
/// failed attempt, its failure the upload's reason, while the row stays
/// held; an upload whose job is still active is left to it. The hold is
/// reported throughout. Evacuating, the row is suspended instead.
/// A report's kind follows the planner's wait: a held sandbox is `HOLD`,
/// the drain's own work and its waits for a window slot or disk room are
/// `MOVING`, and what the drain doesn't drive is `EXTERNAL`. Each reason
/// says what the row waits for, then why when the wait alone doesn't.
#[test]
fn each_wait_reports_its_kind_and_says_what_it_waits_for() {
    use crate::proto::DrainWait as Kind;
    for (wait, kind) in [
        (Wait::Hold, Kind::Hold),
        (Wait::Work(DrainAction::Suspend), Kind::Moving),
        (Wait::Work(DrainAction::PublishDisk), Kind::Moving),
        (Wait::Work(DrainAction::PublishSnapshot), Kind::Moving),
        (Wait::Work(DrainAction::Upload), Kind::Moving),
        (Wait::Queued(DrainAction::Suspend), Kind::Moving),
        (Wait::Disk, Kind::Moving),
        (Wait::WarmPoolEviction, Kind::External),
        (Wait::Settle(SandboxState::Resuming.into()), Kind::External),
    ] {
        assert_eq!(wait.kind(), kind, "{wait:?}");
        let bare = DrainWait {
            sandbox_id: "a".into(),
            wait,
            reason: None,
        };
        assert!(!bare.reason().is_empty(), "{wait:?}");
        let why = DrainWait {
            reason: Some("why".into()),
            ..bare.clone()
        };
        assert_eq!(why.reason(), format!("{}: why", bare.reason()), "{wait:?}");
    }
}

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
            "connection reset".to_owned(),
        )]),
        ..PlanInputs::default()
    };
    let planned = plan(&[held.clone()], &holding(inputs.clone()));
    assert_eq!(planned.actions, [upload], "{planned:?}");
    assert_eq!(
        waits(&planned),
        expect(&[("held", "upload"), ("held", "hold")])
    );
    assert_eq!(
        reason_of(&planned, "held", "upload").as_deref(),
        Some("the last upload failed: connection reset")
    );
    assert!(planned.blocked.is_empty(), "{planned:?}");

    let active = plan(
        &[held.clone()],
        &holding(PlanInputs {
            active_uploads: BTreeSet::from([FULL_ARTIFACT.to_owned()]),
            ..inputs.clone()
        }),
    );
    assert!(active.actions.is_empty(), "{active:?}");
    assert!(waits(&active).contains(&("held".to_owned(), "hold".to_owned())));

    let durable = plan(&[held.clone()], &holding(PlanInputs::default()));
    assert!(durable.actions.is_empty(), "{durable:?}");
    assert_eq!(waits(&durable), expect(&[("held", "hold")]));

    let evacuating = plan(&[held], &inputs);
    assert_eq!(evacuating.actions, [suspend("held")], "{evacuating:?}");
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
