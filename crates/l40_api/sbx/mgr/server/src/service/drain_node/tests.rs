use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::actions::*;
use super::planner::*;
use super::rpc::*;
use crate::activation_seal::ActivationSealStatus;
use crate::proto::drain_node_request::{Cancel, Drain, Op};
use crate::proto::drain_node_response::Result as DrainResult;
use crate::proto::{
    DrainBlocked, DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPending, DrainReport,
    DrainWait as WireWait, PreviewDrainNodeRequest, PreviewDrainNodeResponse,
};
use crate::service::start_support::ActivationAdmissionKind;
use crate::service::*;
use pplx_space_snapshot_model::ArtifactKind;

const FULL_ARTIFACT: &str = "01939e3e-9001-7000-8000-000000000001";
const DISK_ARTIFACT: &str = "01939e3e-9002-7000-8000-000000000001";

fn row(id: &str, lifecycle: impl Into<SandboxStatus>) -> DrainRow {
    DrainRow {
        sandbox_id: id.to_owned(),
        lifecycle: lifecycle.into(),
        pool_member: false,
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
            // A v2 drain doesn't move a stopped sandbox.
            Unowned(S::Stopped) => (vec!["stopped".into()], vec![]),
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

/// A stopped sandbox is one pending entry, `EXTERNAL`, with the stopped
/// reason, whatever it pins or publishes: no pin, a durable stop disk, a
/// stop disk still uploading, an older full snapshot, a named snapshot
/// without a durable copy, or drain work still running for it. Nothing is
/// evacuated or started for it.
#[test]
fn a_stopped_sandbox_is_one_external_entry_whatever_it_pins() {
    let stopped = |pin: Option<(&str, ArtifactKind)>| DrainRow {
        pin: pin.map(|(artifact, kind)| (artifact.to_owned(), kind)),
        ..row("a", SandboxState::Stopped)
    };
    let durable_disk = Some((DISK_ARTIFACT, ArtifactKind::Disk));
    let cases = [
        ("no pin", plan_one(stopped(None), &BTreeMap::new(), &[])),
        (
            "a durable stop disk",
            plan_one(stopped(durable_disk), &BTreeMap::new(), &[]),
        ),
        (
            "a stop disk still uploading",
            plan_one(stopped(durable_disk), &pending("a", DISK_ARTIFACT), &[]),
        ),
        (
            "an older full snapshot",
            plan_one(
                stopped(Some((FULL_ARTIFACT, ArtifactKind::Full))),
                &BTreeMap::new(),
                &[],
            ),
        ),
        (
            "a named snapshot without a durable copy",
            plan(
                &[stopped(None)],
                &PlanInputs {
                    local_only: LocalOnlyArtifacts::from([(
                        "a".to_owned(),
                        vec![(NAMED_ARTIFACT.to_owned(), ArtifactKind::Disk)],
                    )]),
                    ..PlanInputs::default()
                },
            ),
        ),
        (
            "an upload running for it",
            plan_one(
                stopped(None),
                &BTreeMap::new(),
                &[("a", DrainAction::Upload)],
            ),
        ),
    ];
    for (case, plan) in cases {
        assert_eq!(waits(&plan), expect(&[("a", "stopped")]), "{case}");
        assert_eq!(
            plan.pending[0].wait.kind(),
            crate::proto::DrainWait::External,
            "{case}"
        );
        assert_eq!(plan.pending[0].reason(), STOPPED_REASON, "{case}");
        assert!(
            plan.evacuated.is_empty() && plan.actions.is_empty() && plan.blocked.is_empty(),
            "{case}: {plan:?}"
        );
    }
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

/// The window starts paused VMs, then started ones, while the suspends and
/// publications running and the uploads still active take fewer than its
/// size; the rest are pending `queued`, a row whose work failed with that
/// failure as its reason.
#[test]
fn the_window_starts_paused_then_started_and_queues_the_rest() {
    let mut uploading = row("u", SandboxState::Suspended);
    uploading.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let rows = [
        row("s1", SandboxState::Started),
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
    assert_eq!(pending_of(&plan_five, "r"), ["suspend"]);
    assert_eq!(pending_of(&plan_five, "u"), ["upload"]);
    assert!(plan_five.blocked.is_empty());

    // Room for everything.
    let plan_nine = plan(&rows, &inputs(9));
    assert_eq!(
        plan_nine.actions,
        [suspend("p1"), suspend("p2"), suspend("s1"), suspend("s2")]
    );
    assert!(
        waits(&plan_nine)
            .iter()
            .all(|(_, action)| action != "queued")
    );
}

/// A pinned artifact still without a durable copy takes a window slot only
/// while its upload job is queued or running. One whose job failed, or that
/// no job uploads yet, takes none, a VM's or a suspended snapshot's alike, so a
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
                row(&format!("d{n}"), SandboxState::Suspended)
            };
            pinned.pin = Some((format!("{FULL_ARTIFACT}-{n}"), ArtifactKind::Full));
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
/// while evacuating: the paused VM is suspended, the stopped sandbox left
/// pending, the warm-pool member left to its eviction, the VM it can't
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
            ("stopped".to_owned(), "stopped".to_owned()),
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
    assert_eq!(planned.actions, [held_suspend("paused")]);
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
        (Wait::Work(DrainAction::PublishSnapshot), Kind::Moving),
        (Wait::Work(DrainAction::Upload), Kind::Moving),
        (Wait::Queued(DrainAction::Suspend), Kind::Moving),
        (Wait::Disk, Kind::Moving),
        (Wait::WarmPoolEviction, Kind::External),
        (Wait::Settle(SandboxState::Resuming.into()), Kind::External),
        (Wait::Stopped, Kind::External),
        (Wait::Recheck, Kind::External),
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

/// A service on `conn` drained under `generation` by a call that says to
/// evacuate.
fn sealed_service_with(
    conn: crate::db::Connection,
    generation: u64,
) -> (SandboxManagerService, ActivationSealStatus) {
    let service = SandboxManagerService::for_test(conn);
    let first = service.begin_drain(generation, EVACUATE).unwrap();
    (service, first)
}

fn sealed_service(generation: u64) -> (SandboxManagerService, ActivationSealStatus) {
    sealed_service_with(crate::db::test_conn(), generation)
}

fn started_sandbox(conn: &crate::db::Connection) -> String {
    let id = pplx_space_sandbox_model::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(conn, &id, SandboxState::Started);
    id
}

/// Whether the drain `report` describes is finished: nothing pending or
/// blocked.
fn finished(report: &DrainReport) -> bool {
    report.pending.is_empty() && report.blocked.is_empty()
}

/// Each pending entry as `(sandbox, kind, running)`, what the tests compare
/// of an entry: its reason is display text.
fn kinds(pending: &[DrainPending]) -> Vec<(String, WireWait, bool)> {
    pending
        .iter()
        .map(|entry| (entry.sandbox_id.clone(), entry.wait(), entry.running))
        .collect()
}

/// An entry as [`kinds`] gives it.
fn kind(sandbox_id: &str, wait: WireWait, running: bool) -> (String, WireWait, bool) {
    (sandbox_id.to_owned(), wait, running)
}

/// Each pending entry says whether its row is a warm-pool member: a started
/// member, left to the pool, is reported running and a pool member, and a
/// user's started sandbox, which the drain holds, running and not one.
#[tokio::test]
async fn a_pending_entry_says_whether_it_is_a_warm_pool_member() {
    let conn = crate::db::test_conn();
    let member = started_sandbox(&conn);
    conn.execute(
        "UPDATE sandboxes SET pool_template = 'seed' WHERE id = ?1",
        [&member],
    )
    .unwrap();
    let user = started_sandbox(&conn);
    let service = SandboxManagerService::for_test(conn);
    let first = service.begin_drain(7, HOLD).unwrap();

    let report = service.finish_drain(7, &first, DrainScans::default());
    let mut entries: Vec<_> = report
        .pending
        .iter()
        .map(|entry| (entry.sandbox_id.clone(), entry.running, entry.pool_member))
        .collect();
    entries.sort();
    let mut expected = vec![(member, true, true), (user, true, false)];
    expected.sort();
    assert_eq!(entries, expected, "{report:?}");
}

/// An empty node sealed under the call's generation is finished; a scan's
/// failure makes it blocked, naming no sandbox, and not finished.
#[tokio::test]
async fn an_empty_sealed_node_is_finished_unless_a_scan_failed() {
    let (service, first) = sealed_service(7);
    let done = service.finish_drain(7, &first, DrainScans::default());
    assert!(finished(&done), "{done:?}");
    assert_eq!(service.activation_seal.status().sealed_generation, Some(7));

    let blocked = service.finish_drain(
        7,
        &first,
        DrainScans {
            blocked: vec!["can't scan the cache".into()],
            ..DrainScans::default()
        },
    );
    assert!(!finished(&blocked));
    assert_eq!(
        blocked.blocked,
        [DrainBlocked {
            reason: "can't scan the cache".into(),
        }]
    );
}

/// An activation admitted before the seal and still in flight at the walk's
/// first read is pending, waiting on work the drain doesn't drive, and keeps
/// the drain from finishing until it settles.
#[tokio::test]
async fn an_activation_in_flight_before_the_seal_is_pending() {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let token = service
        .admit_vm_activation(ActivationAdmissionKind::Restore, "sbx-a")
        .await
        .unwrap();
    service.begin_drain(7, EVACUATE).unwrap();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report));
    assert_eq!(
        kinds(&report.pending),
        [kind("sbx-a", WireWait::External, false)]
    );

    drop(token);
    let first = service.activation_seal.status();
    assert!(finished(&service.finish_drain(
        7,
        &first,
        DrainScans::default()
    )));
}

/// A seal that didn't hold under the call's generation from the walk's first
/// read to its end proves nothing, and the report says so: it is blocked,
/// naming both reads. One call runs at a time, so only something outside a
/// call, here the seal itself, can unseal the node during one.
#[tokio::test]
async fn a_seal_that_did_not_hold_for_the_whole_call_blocks() {
    let (service, first) = sealed_service(7);
    service.activation_seal.unseal(&service.db, 7).unwrap();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert_eq!(report.blocked.len(), 1, "{report:?}");
    let first = service.activation_seal.status();
    assert!(!finished(&service.finish_drain(
        7,
        &first,
        DrainScans::default()
    )));
}

/// Every condition under which the node isn't finished is an entry of the
/// report: a seal that didn't hold for the whole call, an activation in
/// flight at the walk's first read, a row that may hold a VM, an artifact
/// without a durable copy, a scan of the rows or one outside them that
/// failed. With none of them, nothing is pending or blocked.
#[tokio::test]
async fn the_report_has_an_entry_for_every_reason_the_node_isnt_finished() {
    let conn = crate::db::test_conn();
    let started = started_sandbox(&conn);
    let (service, sealed) = sealed_service_with(conn, 7);
    let report =
        |first: &ActivationSealStatus, scans: DrainScans| service.finish_drain(7, first, scans);
    // A started VM, held: pending, and running.
    service.drain_actions.stop_evacuating();
    let held = report(&sealed, DrainScans::default());
    assert_eq!(kinds(&held.pending), [kind(&started, WireWait::Hold, true)]);
    assert!(held.blocked.is_empty());
    service
        .db
        .lock()
        .unwrap()
        .execute("DELETE FROM sandboxes WHERE id = ?1", [&started])
        .unwrap();
    assert!(finished(&report(&sealed, DrainScans::default())));

    // An activation in flight at the first read.
    let refused = service
        .write_gate
        .check_activation(ActivationAdmissionKind::Cold, "sbx-a");
    assert!(refused.is_err(), "a sealed node admits no activation");
    let in_flight = ActivationSealStatus {
        in_flight: vec!["sbx-a".into()],
        ..sealed.clone()
    };
    assert!(!report(&in_flight, DrainScans::default()).pending.is_empty());

    // A scan outside the rows that failed.
    let scanned = report(
        &sealed,
        DrainScans {
            blocked: vec!["orphan inventory failed".into()],
            ..DrainScans::default()
        },
    );
    assert!(!scanned.blocked.is_empty());

    // An artifact without a durable copy.
    insert_artifact(
        &service.db.lock().unwrap(),
        NAMED_ARTIFACT,
        ("snapshot", Some("kept")),
        false,
        "ready",
    );
    assert!(!report(&sealed, DrainScans::default()).blocked.is_empty());
    service
        .db
        .lock()
        .unwrap()
        .execute("DELETE FROM local_artifacts", [])
        .unwrap();

    // A scan of the rows that failed.
    service
        .db
        .lock()
        .unwrap()
        .execute_batch("ALTER TABLE sandboxes RENAME TO sandboxes_hidden")
        .unwrap();
    assert!(!report(&sealed, DrainScans::default()).blocked.is_empty());
    service
        .db
        .lock()
        .unwrap()
        .execute_batch("ALTER TABLE sandboxes_hidden RENAME TO sandboxes")
        .unwrap();

    // A seal that didn't hold for the whole call.
    let unsealed = ActivationSealStatus {
        sealed_generation: None,
        ..sealed.clone()
    };
    assert!(!report(&unsealed, DrainScans::default()).blocked.is_empty());

    assert!(finished(&report(&sealed, DrainScans::default())));
}

/// A suspended sandbox whose pinned snapshot is durable is evacuated with
/// it, read from its row alone, whatever suspended it; the report and the
/// seal survive a restart of the service. A directory node-manager names
/// that no row accounts for blocks the node, naming no sandbox.
#[tokio::test]
async fn a_suspended_sandbox_is_reported_from_its_row_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manager.sqlite");
    crate::db::create_test_schema_on_disk(&path);
    let conn = crate::db::Connection::open_at(&path).unwrap();
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         ) VALUES ('sbx-a', 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}', ?1, 'full', 'suspend')",
        crate::db::fixture::params![FULL_ARTIFACT],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at
         ) VALUES (?1, 'sbx-a', 'full', 'suspend', NULL, '/tmp/a', '0', '1')",
        crate::db::fixture::params![FULL_ARTIFACT],
    )
    .unwrap();

    let (service, first) = sealed_service_with(conn, 7);
    let report = service.finish_drain(7, &first, DrainScans::default());
    let expected = [DrainEvacuated {
        sandbox_id: "sbx-a".into(),
        artifact_kind: crate::proto::ArtifactKind::Full as i32,
        artifact_id: FULL_ARTIFACT.into(),
    }];
    assert_eq!(report.evacuated, expected);
    assert!(finished(&report), "{report:?}");

    // node-manager knows only its slots, so it names the suspended row's
    // cache directory a candidate; the row accounts for it. A directory with
    // no row blocks.
    let report = service.finish_drain(
        7,
        &first,
        DrainScans {
            orphan_dirs: vec![
                ("sbx-a".into(), "orphan cache directory /cache/sbx-a".into()),
                (
                    "sbx-gone".into(),
                    "orphan cache directory /cache/sbx-gone".into(),
                ),
            ],
            ..DrainScans::default()
        },
    );
    assert!(!finished(&report));
    assert_eq!(
        report.blocked,
        [DrainBlocked {
            reason: "orphan cache directory /cache/sbx-gone".into(),
        }]
    );
    drop(service);

    let restarted = SandboxManagerService::for_test(crate::db::Connection::open_at(&path).unwrap());
    let first = restarted.activation_seal.status();
    assert_eq!(
        first.sealed_generation,
        Some(7),
        "the seal survives the restart"
    );
    let report = restarted.finish_drain(7, &first, DrainScans::default());
    assert_eq!(report.evacuated, expected);
}

/// A stopped sandbox is one pending entry, `EXTERNAL` and not running, with
/// the stopped reason, read from its row whatever it pins: no pin, a stop
/// disk with a durable copy, one still without, or an encrypted volume. None
/// is evacuated, the node isn't finished, and the call starts nothing.
#[tokio::test]
async fn a_stopped_sandbox_is_pending_from_its_row_whatever_it_pins() {
    const PENDING_DISK: &str = "01939e3e-9004-7000-8000-000000000001";
    let conn = crate::db::test_conn();
    for (id, uuid8, pin, encrypted) in [
        ("sbx-none", "uuid1", None, false),
        ("sbx-durable", "uuid2", Some(DISK_ARTIFACT), false),
        ("sbx-pending", "uuid3", Some(PENDING_DISK), false),
        ("sbx-encrypted", "uuid4", None, true),
    ] {
        conn.execute(
            "INSERT INTO sandboxes (
                 id, uuid8, state, template, user, runtime, created_at, updated_at,
                 process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
                 current_local_artifact_id, current_local_artifact_kind,
                 current_local_artifact_origin, volume_encryption
             ) VALUES (?1, ?2, 'stopped', 'seed', 'u', 'fc', '0', '0', '{}', '{}', '{}',
                       ?3, CASE WHEN ?3 IS NULL THEN NULL ELSE 'disk' END,
                       CASE WHEN ?3 IS NULL THEN NULL ELSE 'stop' END, ?4)",
            crate::db::fixture::params![id, uuid8, pin, encrypted],
        )
        .unwrap();
        if let Some(pin) = pin {
            conn.execute(
                "INSERT INTO local_artifacts (
                     local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                     local_path, created_at, backup_completed_at, state
                 ) VALUES (?1, ?2, 'disk', 'stop', '/tmp/a', '0', ?3, 'ready')",
                crate::db::fixture::params![pin, id, (pin == DISK_ARTIFACT).then_some("1")],
            )
            .unwrap();
        }
    }
    let (service, first) = sealed_service_with(conn, 7);
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(report.evacuated.is_empty(), "{report:?}");
    assert!(report.blocked.is_empty(), "{report:?}");
    let mut pending: Vec<_> = report
        .pending
        .iter()
        .map(|entry| {
            (
                entry.sandbox_id.as_str(),
                entry.wait(),
                entry.running,
                entry.reason.as_str(),
            )
        })
        .collect();
    pending.sort_unstable();
    let stopped = |id| {
        (
            id,
            WireWait::External,
            false,
            super::planner::STOPPED_REASON,
        )
    };
    assert_eq!(
        pending,
        [
            stopped("sbx-durable"),
            stopped("sbx-encrypted"),
            stopped("sbx-none"),
            stopped("sbx-pending"),
        ]
    );
    assert!(!finished(&report));
    // Nothing was started: no work ran, so none failed in this test
    // service, which can upload nothing.
    service.drain_actions.wait_idle().await;
    assert_eq!(service.drain_actions.failures(7), DrainFailures::new());
}

/// A suspended sandbox whose pinned full snapshot is durable, and a failed
/// one with nothing to keep, on `conn`.
fn insert_finished_rows(conn: &crate::db::Connection) {
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         ) VALUES ('sbx-a', 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}', ?1, 'full', 'suspend')",
        crate::db::fixture::params![FULL_ARTIFACT],
    )
    .unwrap();
    insert_artifact(conn, FULL_ARTIFACT, ("suspend", None), true, "ready");
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json
         ) VALUES ('sbx-f', 'uuid9', 'failed', 'seed', 'u', 'fc', '0', '0', '{}', '{}', '{}')",
        [],
    )
    .unwrap();
}

/// Each row the plan finds finished, evacuated or with nothing to keep, is
/// confirmed under its artifact lock: while a writer holds a row's lock the
/// row is pending, waiting on what the drain doesn't drive, and the
/// suspended one isn't reported evacuated; once the writer lets go, both
/// are finished.
#[tokio::test]
async fn a_writer_holding_a_rows_artifact_lock_keeps_the_row_pending() {
    let conn = crate::db::test_conn();
    insert_finished_rows(&conn);
    let (service, first) = sealed_service_with(conn, 7);
    let evacuated = [DrainEvacuated {
        sandbox_id: "sbx-a".into(),
        artifact_kind: crate::proto::ArtifactKind::Full as i32,
        artifact_id: FULL_ARTIFACT.into(),
    }];
    assert_eq!(
        service
            .finish_drain(7, &first, DrainScans::default())
            .evacuated,
        evacuated
    );
    for id in ["sbx-a", "sbx-f"] {
        let writer = service
            .write_gate
            .lock_for_read(id, crate::service::write_gate::Reader::Test)
            .await;
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert_eq!(
            kinds(&report.pending),
            [kind(id, WireWait::External, false)],
            "{id}: {report:?}"
        );
        assert!(
            report.pending[0]
                .reason
                .contains("another writer holds its artifacts"),
            "{report:?}"
        );
        if id == "sbx-a" {
            assert!(report.evacuated.is_empty(), "{report:?}");
        }
        drop(writer);
    }
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(finished(&report), "{report:?}");
    assert_eq!(report.evacuated, evacuated);
}

/// A row that changes between the drain's read and its artifact lock is
/// demoted: read again under the lock, a suspended row whose snapshot lost
/// its durable copy, and a failed row that gained an artifact without one,
/// are pending, not finished. A row that didn't change stays finished.
#[tokio::test]
async fn a_row_changed_between_the_read_and_its_lock_is_demoted() {
    let conn = crate::db::test_conn();
    insert_finished_rows(&conn);
    let service = SandboxManagerService::for_test(conn);
    let read = |service: &SandboxManagerService| service.read_plan_reads(None).unwrap();
    let unchanged = read(&service);
    let mut kept = plan(&unchanged.rows, &unchanged.inputs);
    service.confirm_finished_rows(&mut kept, &unchanged.rows, &unchanged.inputs, None);
    assert!(kept.pending.is_empty(), "{kept:?}");
    assert_eq!(kept.evacuated.len(), 1, "{kept:?}");

    let stale = read(&service);
    let mut demoted = plan(&stale.rows, &stale.inputs);
    {
        let db = service.db.lock().unwrap();
        db.execute(
            "UPDATE local_artifacts SET backup_completed_at = NULL WHERE local_artifact_id = ?1",
            [FULL_ARTIFACT],
        )
        .unwrap();
        db.execute(
            "INSERT INTO local_artifacts (
                 local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                 snapshot_name, local_path, created_at, backup_completed_at, state
             ) VALUES (?1, 'sbx-f', 'full', 'snapshot', 'late', '/tmp/f', '0', NULL, 'ready')",
            [NAMED_ARTIFACT],
        )
        .unwrap();
    }
    service.confirm_finished_rows(&mut demoted, &stale.rows, &stale.inputs, None);
    let mut pending: Vec<_> = demoted
        .pending
        .iter()
        .map(|wait| (wait.sandbox_id.as_str(), wait.wait.label(), wait.reason()))
        .collect();
    pending.sort();
    let why = "checked again on the next call: its row changed while the drain read it";
    assert_eq!(
        pending,
        [
            ("sbx-a", "recheck".to_owned(), why.to_owned()),
            ("sbx-f", "recheck".to_owned(), why.to_owned()),
        ]
    );
    assert!(demoted.evacuated.is_empty(), "{demoted:?}");
}

fn insert_artifact(
    conn: &crate::db::Connection,
    artifact: &str,
    origin_and_name: (&str, Option<&str>),
    backed_up: bool,
    state: &str,
) {
    let (origin, name) = origin_and_name;
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at, state
         ) VALUES (?1, 'sbx-a', 'full', ?2, ?3, '/tmp/a', '0', ?4, ?5)",
        crate::db::fixture::params![artifact, origin, name, backed_up.then_some("1"), state],
    )
    .unwrap();
}

/// A suspended sandbox whose current snapshot is durable still holds the
/// drain open while it keeps a named snapshot without a durable copy: the
/// drain finishes that upload and is finished once it is durable. A named
/// snapshot already durable, or one a delete has reserved, holds nothing.
#[tokio::test]
async fn a_local_only_named_snapshot_is_pending_until_it_is_durable() {
    let conn = crate::db::test_conn();
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         ) VALUES ('sbx-a', 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}', ?1, 'full', 'suspend')",
        crate::db::fixture::params![FULL_ARTIFACT],
    )
    .unwrap();
    insert_artifact(&conn, FULL_ARTIFACT, ("suspend", None), true, "ready");
    insert_artifact(
        &conn,
        NAMED_ARTIFACT,
        ("snapshot", Some("kept")),
        false,
        "ready",
    );
    insert_artifact(
        &conn,
        "01939e3e-9004-7000-8000-000000000001",
        ("snapshot", Some("durable")),
        true,
        "ready",
    );
    insert_artifact(
        &conn,
        "01939e3e-9005-7000-8000-000000000001",
        ("snapshot", Some("going")),
        false,
        "deleting",
    );
    let (service, first) = sealed_service_with(conn, 7);

    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert_eq!(
        kinds(&report.pending),
        [kind("sbx-a", WireWait::Moving, false)]
    );
    assert_eq!(report.evacuated.len(), 1, "{report:?}");

    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE local_artifacts SET backup_completed_at = '2' WHERE local_artifact_id = ?1",
            [NAMED_ARTIFACT],
        )
        .unwrap();
    service.drain_actions.wait_idle().await;
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(finished(&report), "{report:?}");
}

/// Failures are kept under the generation they happened in: a later drain
/// starts from none, and a failure of an action an earlier generation started
/// isn't kept for the new one.
#[test]
fn a_new_generation_forgets_the_old_failures() {
    let actions = DrainActions::default();
    assert!(actions.failures(7).is_empty());
    assert!(actions.start("a", DrainAction::Upload));
    actions.finish(7, "a", Some("no space".into()));
    assert_eq!(
        actions.failures(7),
        BTreeMap::from([(("a".to_owned(), DrainAction::Upload), "no space".to_owned())])
    );
    assert!(actions.start("a", DrainAction::Upload));
    assert!(
        actions.failures(8).is_empty(),
        "a new generation forgets the old failures"
    );
    actions.finish(7, "a", Some("late".into()));
    assert!(
        actions.failures(8).is_empty(),
        "the old generation's action isn't kept"
    );
}

fn service_with_node_manager(
    conn: crate::db::Connection,
    node_manager: &crate::node_manager_transport::fake::ServedNodeManager,
) -> SandboxManagerService {
    SandboxManagerService::new_with_options(
        conn,
        crate::service::SandboxManagerServiceOptions {
            node_manager: node_manager.connector(),
            ..crate::service::SandboxManagerServiceOptions::for_test()
        },
    )
}

fn orphan_inventory(
    page: pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse,
) -> crate::node_manager_transport::fake::ServedNodeManager {
    crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary(
            "ListOrphanCandidates",
            move |_: pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest| {
                Ok(page.clone())
            },
        )
        .serve()
}

/// A hold of `seconds`.
/// A drain call that suspends running sandboxes too.
const EVACUATE: bool = true;
/// A drain call that keeps running sandboxes running.
const HOLD: bool = false;

fn drain_request(generation: u64, evacuate: bool) -> Request<DrainNodeRequest> {
    crate::service::activation_support::internal_request(DrainNodeRequest {
        generation,
        op: Some(Op::Drain(Drain { evacuate })),
    })
}

fn cancel_request(generation: u64) -> Request<DrainNodeRequest> {
    crate::service::activation_support::internal_request(DrainNodeRequest {
        generation,
        op: Some(Op::Cancel(Cancel {})),
    })
}

/// The report of a drain call's answer, which names its `generation`.
#[track_caller]
fn drained(response: Response<DrainNodeResponse>, generation: u64) -> DrainReport {
    let response = response.into_inner();
    assert_eq!(response.generation, generation, "{response:?}");
    match response.result {
        Some(DrainResult::Drain(report)) => report,
        other => panic!("not a drain's answer: {other:?}"),
    }
}

/// The tombstone a cancel's answer names, which must say it is a cancel's.
#[track_caller]
fn cancelled(response: Response<DrainNodeResponse>) -> u64 {
    let response = response.into_inner();
    assert_eq!(
        response.result,
        Some(DrainResult::Cancel(crate::proto::CancelResult {})),
        "{response:?}"
    );
    response.generation
}

/// The whole call, through node-manager's inventory: a node with nothing on
/// it is sealed and finished; a run directory no row accounts for, or a failed
/// scan of run directories, blocks it; a cancel unseals it.
#[tokio::test]
async fn drain_node_seals_reads_the_inventory_and_reports() {
    let clean = orphan_inventory(Default::default());
    let service = service_with_node_manager(crate::db::test_conn(), &clean);
    let report = drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    assert!(finished(&report), "{report:?}");
    assert!(
        clean
            .calls()
            .iter()
            .any(|call| call.ends_with("ListOrphanCandidates")),
        "{:?}",
        clean.calls()
    );

    let leftovers = orphan_inventory(
        pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse {
            orphaned_run_dirs: vec!["/run/sandboxes/sbx-gone".into()],
            warnings: vec!["RunDirs: permission denied".into()],
            ..Default::default()
        },
    );
    let service = service_with_node_manager(crate::db::test_conn(), &leftovers);
    let report = drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    assert!(!finished(&report));
    assert_eq!(
        report.blocked,
        [
            DrainBlocked {
                reason: "orphan inventory scan failed: RunDirs: permission denied".into(),
            },
            DrainBlocked {
                reason: "orphan run directory /run/sandboxes/sbx-gone".into(),
            },
        ]
    );

    let cancelled = cancelled(service.drain_node(cancel_request(7)).await.unwrap());
    assert_eq!(cancelled, 7, "the cancel answers its tombstone");
    assert_eq!(service.activation_seal.status().sealed_generation, None);
    assert_eq!(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
        "a cancelled generation never seals again"
    );
}

/// A node-manager whose inventory never answers costs the call its deadline,
/// not the control plane's timeout, and blocks the drain.
#[tokio::test(start_paused = true)]
async fn an_inventory_that_never_answers_blocks_within_the_deadline() {
    let stalled = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary_async(
            "ListOrphanCandidates",
            |_: pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest| {
                std::future::pending::<
                    Result<pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse, Status>,
                >()
            },
        )
        .serve();
    let service = service_with_node_manager(crate::db::test_conn(), &stalled);
    let started = tokio::time::Instant::now();
    let report = drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    assert!(started.elapsed() < ORPHAN_INVENTORY_DEADLINE + Duration::from_secs(1));
    assert!(!finished(&report));
    assert_eq!(
        report
            .blocked
            .iter()
            .map(|entry| entry.reason.as_str())
            .collect::<Vec<_>>(),
        ["orphan inventory didn't finish within 20s"]
    );
}

/// Only root and node-api, which relays the control plane's drain, may
/// drain the node; node-ctl, which may read sandbox state, may not.
#[tokio::test]
async fn only_root_and_node_api_drain_the_node() {
    let clean = orphan_inventory(Default::default());
    let service = service_with_node_manager(crate::db::test_conn(), &clean);
    let request = DrainNodeRequest {
        generation: 7,
        op: Some(Op::Drain(Drain { evacuate: true })),
    };
    let refused = service
        .drain_node(crate::service::tests::service_tests::node_ctl_request(
            request,
        ))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        service.activation_seal.status().sealed_generation,
        None,
        "a refused call seals nothing"
    );
    let relayed = service
        .drain_node(
            crate::service::tests::service_tests::node_api_trusted_lifecycle_request(request),
        )
        .await
        .unwrap();
    drained(relayed, 7);
    drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
}

/// A node sealed under one drain's generation refuses a call under another,
/// later or earlier, with both generations named, and keeps its seal and
/// that drain's evacuating: no drain takes over another's.
#[tokio::test]
async fn a_drain_call_under_another_generation_is_refused() {
    let clean = orphan_inventory(Default::default());
    let service = service_with_node_manager(crate::db::test_conn(), &clean);
    drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    for other in [6, 8] {
        let refused = service
            .drain_node(drain_request(other, HOLD))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{other}");
        for generation in [7, other] {
            assert!(
                refused
                    .message()
                    .contains(&format!("generation {generation}")),
                "{refused:?}"
            );
        }
        assert_eq!(service.activation_seal.status().sealed_generation, Some(7));
        assert!(service.drain_actions.evacuating(7), "{other}");
    }
}

/// A node-manager whose orphan inventory answers only once `release` is
/// notified, and the number of inventory pages asked so far.
fn gated_inventory() -> (
    crate::node_manager_transport::fake::ServedNodeManager,
    std::sync::Arc<tokio::sync::Semaphore>,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    let release = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let served = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary_async("ListOrphanCandidates", {
            let release = std::sync::Arc::clone(&release);
            let asked = std::sync::Arc::clone(&asked);
            move |_: pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest| {
                let release = std::sync::Arc::clone(&release);
                asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    release.acquire().await.unwrap().forget();
                    Ok(pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse::default())
                }
            }
        })
        .serve();
    (served, release, asked)
}

/// Wait until `asked` counts `pages` inventory pages.
async fn until_asked(asked: &std::sync::atomic::AtomicUsize, pages: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while asked.load(std::sync::atomic::Ordering::SeqCst) < pages {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the call reaches node-manager's inventory");
}

#[track_caller]
fn assert_in_progress_refusal(refused: &Status) {
    assert_eq!(refused.code(), tonic::Code::Aborted, "{refused:?}");
    assert!(
        refused.message().contains(DRAIN_CALL_IN_PROGRESS_MARKER),
        "{refused:?}"
    );
}

/// One `DrainNode` call at a time: while a drain call is in progress, scans
/// included, another drain call, under its generation or another, and a
/// cancel each fail at once with `Aborted` and the in-progress marker, and
/// change nothing: the seal, its row, the evacuating and node-manager's
/// inventory stay as the first call left them. Once it answers, the next
/// call runs.
#[tokio::test]
async fn a_call_while_another_is_in_progress_is_aborted() {
    let (inventory, release, asked) = gated_inventory();
    let service = service_with_node_manager(crate::db::test_conn(), &inventory);
    let first = tokio::spawn({
        let service = service.clone();
        async move { service.drain_node(drain_request(7, HOLD)).await }
    });
    until_asked(&asked, 1).await;
    let row = crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap();
    for request in [
        drain_request(7, EVACUATE),
        drain_request(8, EVACUATE),
        cancel_request(7),
    ] {
        let refused = tokio::time::timeout(Duration::from_secs(2), service.drain_node(request))
            .await
            .expect("a refused call answers at once")
            .unwrap_err();
        assert_in_progress_refusal(&refused);
    }
    assert_eq!(service.activation_seal.status().sealed_generation, Some(7));
    assert!(
        !service.drain_actions.evacuating(7),
        "the refused call changed nothing"
    );
    assert_eq!(
        crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap(),
        row
    );
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1);

    release.add_permits(1);
    drained(first.await.unwrap().unwrap(), 7);
    release.add_permits(1);
    drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    assert!(service.drain_actions.evacuating(7));
    assert_eq!(
        cancelled(service.drain_node(cancel_request(7)).await.unwrap()),
        7
    );
}

/// A call bounded by the drain's call timeout: one whose scans run past it
/// answers `DeadlineExceeded` and frees the node, so the next call runs, and
/// so does one after a caller that went away mid-call.
#[tokio::test(start_paused = true)]
async fn a_call_that_runs_out_its_timeout_frees_the_node() {
    let (inventory, release, asked) = gated_inventory();
    let mut service = service_with_node_manager(crate::db::test_conn(), &inventory);
    let mut actions = DrainActions::default();
    actions.call_timeout = Duration::from_secs(5);
    assert!(actions.call_timeout < ORPHAN_INVENTORY_DEADLINE);
    service.drain_actions = std::sync::Arc::new(actions);

    let started = tokio::time::Instant::now();
    let timed_out = service
        .drain_node(drain_request(7, HOLD))
        .await
        .unwrap_err();
    assert_eq!(
        timed_out.code(),
        tonic::Code::DeadlineExceeded,
        "{timed_out:?}"
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(6),
        "{elapsed:?}"
    );
    assert_eq!(
        cancelled(service.drain_node(cancel_request(7)).await.unwrap()),
        7,
        "the timed-out call freed the node"
    );

    let gone = tokio::spawn({
        let service = service.clone();
        async move { service.drain_node(drain_request(8, HOLD)).await }
    });
    until_asked(&asked, 2).await;
    assert_in_progress_refusal(&service.drain_node(cancel_request(8)).await.unwrap_err());
    gone.abort();
    assert!(gone.await.unwrap_err().is_cancelled());
    release.add_permits(1);
    assert_eq!(
        cancelled(service.drain_node(cancel_request(8)).await.unwrap()),
        8,
        "a call its caller dropped freed the node"
    );
}

/// The reason of an upload the backup worker gave up on names it the last
/// upload's failure once: the failure the drain keeps is the worker's error
/// as the upload action answers it, and the report adds the prefix.
#[test]
fn a_failed_upload_is_named_the_last_uploads_failure_once() {
    let actions = DrainActions::default();
    assert!(actions.failures(7).is_empty());
    assert!(actions.start("a", DrainAction::Upload));
    let answered = upload_gave_up("the store refused it".to_owned());
    actions.finish(7, "a", Some(answered.message().to_owned()));
    let mut suspended = row("a", SandboxState::Suspended);
    suspended.pin = Some((FULL_ARTIFACT.into(), ArtifactKind::Full));
    let planned = plan(
        &[suspended],
        &PlanInputs {
            pending_backups: pending("a", FULL_ARTIFACT),
            failures: actions.failures(7),
            ..PlanInputs::default()
        },
    );
    let reasons: Vec<String> = planned.pending.iter().map(DrainWait::reason).collect();
    assert_eq!(reasons.len(), 1, "{planned:?}");
    assert_eq!(
        reasons[0].matches("the last upload failed").count(),
        1,
        "{reasons:?}"
    );
    assert!(reasons[0].ends_with("the store refused it"), "{reasons:?}");
}

/// A call without an op is refused before it seals anything or asks
/// node-manager anything, and so is one under generation zero.
#[tokio::test]
async fn a_drain_call_needs_an_op_and_a_generation() {
    let clean = orphan_inventory(Default::default());
    let service = service_with_node_manager(crate::db::test_conn(), &clean);
    let no_op = crate::service::activation_support::internal_request(DrainNodeRequest {
        generation: 7,
        op: None,
    });
    for refused in [
        service.drain_node(no_op).await.unwrap_err(),
        service
            .drain_node(drain_request(0, EVACUATE))
            .await
            .unwrap_err(),
        service.drain_node(cancel_request(0)).await.unwrap_err(),
    ] {
        assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused:?}");
        assert_eq!(service.activation_seal.status().sealed_generation, None);
    }
    assert_eq!(clean.calls(), Vec::<String>::new());
    assert_eq!(
        crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap(),
        None
    );
}

/// The started sandbox and the paused one of a node a holding drain call
/// drains, the paused one's suspend held back at its wake lock.
fn holding_node() -> (SandboxManagerService, String, String) {
    let conn = crate::db::test_conn();
    let started = started_sandbox(&conn);
    let paused = pplx_space_sandbox_model::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &paused, SandboxState::Paused);
    (SandboxManagerService::for_test(conn), started, paused)
}

/// A holding call keeps a running sandbox running, pending `HOLD` and
/// `running`, and starts nothing for it, while it suspends a paused one,
/// pending `MOVING` and not running. The whole call reports the hold, with
/// when the row entered its state.
#[tokio::test]
async fn a_hold_keeps_a_running_sandbox_running_while_a_paused_one_is_suspended() {
    let (service, started, paused) = holding_node();
    let wake = service.auto_lifecycle.wake_lock(&paused).lock_owned().await;
    let first = service.begin_drain(7, HOLD).unwrap();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert_eq!(
        service.drain_actions.running(),
        BTreeMap::from([(paused.clone(), DrainAction::Suspend)]),
        "{report:?}"
    );
    // The started VM is held and running; the paused one is being
    // suspended, and a paused VM isn't running.
    let mut entries = kinds(&report.pending);
    entries.sort();
    let mut expected = vec![
        kind(&started, WireWait::Hold, true),
        kind(&paused, WireWait::Moving, false),
    ];
    expected.sort();
    assert_eq!(entries, expected, "{report:?}");
    let held = report
        .pending
        .iter()
        .find(|entry| entry.sandbox_id == started)
        .expect("the started sandbox is pending");
    let entered = crate::db::scan::status_entered_at(&service.db.lock().unwrap()).unwrap()
        [&started]
        .expect("the fixture stamps its row");
    assert_eq!(
        held.since,
        Some(prost_types::Timestamp {
            seconds: i64::try_from(entered).unwrap(),
            nanos: 0,
        }),
        "since is when the row entered its state"
    );
    release_held_suspend(&service, 7, wake).await;
    assert_eq!(
        service.lookup_sandbox(&started).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
    );

    let node_manager = orphan_inventory(Default::default());
    let conn = crate::db::test_conn();
    let started = started_sandbox(&conn);
    let service = service_with_node_manager(conn, &node_manager);
    let report = drained(service.drain_node(drain_request(7, HOLD)).await.unwrap(), 7);
    assert_eq!(
        kinds(&report.pending),
        [kind(&started, WireWait::Hold, true)]
    );
    assert!(service.drain_actions.running().is_empty());
}

/// A call that says to evacuate suspends the running sandboxes, starting in
/// that call: the whole call plans the suspend (or a wait for cache disk,
/// which this host's disk decides), never a hold, and the entry stays
/// `running` until the suspend lands.
#[tokio::test]
async fn an_evacuating_call_suspends_running_sandboxes_in_that_call() {
    let (service, started, _) = holding_node();
    let wake = service
        .auto_lifecycle
        .wake_lock(&started)
        .lock_owned()
        .await;
    let first = service.begin_drain(7, EVACUATE).unwrap();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert_eq!(
        service.drain_actions.running().get(&started),
        Some(&DrainAction::Suspend),
        "{report:?}"
    );
    release_held_suspend(&service, 7, wake).await;

    let node_manager = orphan_inventory(Default::default());
    let conn = crate::db::test_conn();
    let started = started_sandbox(&conn);
    let service = service_with_node_manager(conn, &node_manager);
    let wake = service
        .auto_lifecycle
        .wake_lock(&started)
        .lock_owned()
        .await;
    let report = drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    assert!(
        report.pending.len() == 1
            && report.pending[0].wait() == WireWait::Moving
            && report.pending[0].running,
        "{report:?}"
    );
    release_held_suspend(&service, 7, wake).await;
}

/// Once a call under a generation has said to evacuate, the drain evacuates
/// under it: a later holding call under the same generation, a delayed or
/// stale one, doesn't return it to holding.
#[tokio::test]
async fn an_evacuating_drain_stays_evacuating_whatever_a_later_call_under_its_generation_says() {
    let (service, started, _) = holding_node();
    let wake = service
        .auto_lifecycle
        .wake_lock(&started)
        .lock_owned()
        .await;
    service.begin_drain(7, EVACUATE).unwrap();
    let first = service.begin_drain(7, HOLD).unwrap();
    assert!(service.drain_actions.evacuating(7));
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert_eq!(
        service.drain_actions.running().get(&started),
        Some(&DrainAction::Suspend),
        "{report:?}"
    );
    assert!(
        !report
            .pending
            .iter()
            .any(|entry| entry.wait() == WireWait::Hold),
        "{report:?}"
    );
    release_held_suspend(&service, 7, wake).await;
}

/// The drain keeps its evacuating in memory only: a process that starts
/// sealed holds the running sandboxes, under the seal it reads back, until a
/// call says to evacuate again.
#[tokio::test]
async fn a_restart_while_evacuating_holds_until_the_next_call() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manager.sqlite");
    crate::db::create_test_schema_on_disk(&path);
    let conn = crate::db::Connection::open_at(&path).unwrap();
    let started = started_sandbox(&conn);
    let before = SandboxManagerService::for_test(conn);
    before.begin_drain(7, EVACUATE).unwrap();
    assert!(before.drain_actions.evacuating(7));
    drop(before);

    let after = SandboxManagerService::for_test(crate::db::Connection::open_at(&path).unwrap());
    let first = after.activation_seal.status();
    assert_eq!(
        first.sealed_generation,
        Some(7),
        "the seal survives the restart"
    );
    assert!(!after.drain_actions.evacuating(7), "its evacuating doesn't");
    let report = after.finish_drain(7, &first, DrainScans::default());
    assert!(after.drain_actions.running().is_empty(), "{report:?}");
    assert_eq!(
        kinds(&report.pending),
        [kind(&started, WireWait::Hold, true)]
    );

    let wake = after.auto_lifecycle.wake_lock(&started).lock_owned().await;
    let first = after.begin_drain(7, EVACUATE).unwrap();
    after.finish_drain(7, &first, DrainScans::default());
    assert_eq!(
        after.drain_actions.running().get(&started),
        Some(&DrainAction::Suspend)
    );
    release_held_suspend(&after, 7, wake).await;
}

/// A cancel ends the evacuating, and a later generation holds until a call
/// under it says to evacuate.
#[tokio::test]
async fn a_cancel_ends_the_evacuating_and_a_later_generation_holds() {
    let (service, started, _) = holding_node();
    service.begin_drain(7, EVACUATE).unwrap();
    service.cancel_drain(7).unwrap();
    assert!(!service.drain_actions.evacuating(7));

    service.begin_drain(8, EVACUATE).unwrap();
    assert!(service.drain_actions.evacuating(8));
    assert_eq!(service.cancel_drain(8).unwrap(), 8);
    let first = service.begin_drain(9, HOLD).unwrap();
    assert!(!service.drain_actions.evacuating(9));
    let wake = service
        .auto_lifecycle
        .wake_lock(&started)
        .lock_owned()
        .await;
    let report = service.finish_drain(9, &first, DrainScans::default());
    assert!(
        !service.drain_actions.running().contains_key(&started),
        "{report:?}"
    );
    assert!(
        report
            .pending
            .iter()
            .any(|entry| entry.sandbox_id == started && entry.wait() == WireWait::Hold),
        "{report:?}"
    );
    release_held_suspend(&service, 9, wake).await;
}

/// A cancel unseals and clears the evacuating, and answers at once: a
/// cancel's result naming its tombstone, and no scan or work started,
/// whatever the node holds. A drain call under the cancelled generation that arrives after the
/// cancel, a delayed retry of the drain's own, seals nothing; a later generation
/// seals.
#[tokio::test]
async fn a_cancel_unseals_and_answers_at_once() {
    let conn = crate::db::test_conn();
    let id = started_sandbox(&conn);
    let node_manager = orphan_inventory(Default::default());
    let service = service_with_node_manager(conn, &node_manager);
    service.begin_drain(7, EVACUATE).unwrap();
    let cancelled = cancelled(service.drain_node(cancel_request(7)).await.unwrap());
    assert_eq!(cancelled, 7);
    assert!(service.drain_actions.running().is_empty());
    assert!(!service.drain_actions.evacuating(7));
    assert_eq!(node_manager.calls(), Vec::<String>::new());
    assert_eq!(
        crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap(),
        Some(crate::db::activation_seal::ActivationSealRecord {
            generation: 7,
            sealed: false,
        })
    );

    let delayed = service
        .drain_node(drain_request(7, EVACUATE))
        .await
        .unwrap_err();
    assert_eq!(delayed.code(), tonic::Code::FailedPrecondition);
    assert_eq!(service.activation_seal.status().sealed_generation, None);
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
    );
    drained(service.drain_node(drain_request(8, HOLD)).await.unwrap(), 8);
    assert_eq!(service.activation_seal.status().sealed_generation, Some(8));
}

/// Every drain action holds a state-writer lease while it runs, so a
/// restart handoff waits for it.
#[tokio::test]
async fn a_drain_action_runs_as_a_state_writer() {
    let conn = crate::db::test_conn();
    let id = started_sandbox(&conn);
    let (service, first) = sealed_service_with(conn, 7);
    // The suspend waits on the wake lock this test holds.
    let wake_lock = service.auto_lifecycle.wake_lock(&id);
    let held = wake_lock.lock_owned().await;
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert_eq!(report.pending.len(), 1, "{report:?}");
    assert!(
        service
            .active_state_transition_methods()
            .contains(&("NodeDrain", 1)),
        "{:?}",
        service.active_state_transition_methods()
    );
    // A cancel lands before the suspend gets the lock; it writes nothing.
    service.cancel_drain(7).unwrap();
    drop(held);
    service.drain_actions.wait_idle().await;
    assert!(service.active_state_transition_methods().is_empty());
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
    );
}

/// A drain's suspend of a row still `paused` when it runs suspends it,
/// holding as evacuating: the hold applies to a started row only.
#[tokio::test]
async fn a_suspend_while_holding_still_suspends_a_paused_row() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    for (evacuate, case) in [(HOLD, "holding"), (EVACUATE, "evacuating")] {
        let conn = crate::db::test_conn();
        let id = pplx_space_sandbox_model::SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Paused);
        let service = service_with_node_manager(conn, &node_manager);
        let first = service.begin_drain(7, evacuate).unwrap();
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert_eq!(
            kinds(&report.pending),
            [kind(&id, WireWait::Moving, false)],
            "{case}"
        );
        service.drain_actions.wait_idle().await;
        // The fake node-manager fails every suspend the drain attempts, so
        // an attempt shows as the suspend's failure.
        assert!(
            service
                .drain_actions
                .failures(7)
                .contains_key(&(id.clone(), DrainAction::Suspend)),
            "{case}: the suspend was attempted"
        );
    }
}

/// A suspend the drain planned for a paused VM waits for the wake lock, as
/// a resume that was already in flight at the seal holds it. When that
/// resume finishes `started` first, a suspend planned while holding leaves
/// the sandbox running, and the next call holds it; one planned while
/// evacuating still suspends it from `started`.
#[tokio::test]
async fn a_queued_suspend_while_holding_leaves_a_sandbox_a_resume_started() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    for (evacuate, holding) in [(HOLD, true), (EVACUATE, false)] {
        let conn = crate::db::test_conn();
        let id = pplx_space_sandbox_model::SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Paused);
        let service = service_with_node_manager(conn, &node_manager);
        let first = service.begin_drain(7, evacuate).unwrap();
        // The resume in flight holds the wake lock while the drain plans.
        let wake_lock = service.auto_lifecycle.wake_lock(&id);
        let resume = wake_lock.lock_owned().await;
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert_eq!(
            service.drain_actions.running().get(&id),
            Some(&DrainAction::Suspend),
            "holding: {holding}: {report:?}"
        );
        let resumed = crate::db::update_state_from_states(
            &service.db.lock().unwrap(),
            &id,
            crate::db::StateSet::of(&[SandboxState::Paused]),
            SandboxState::Started,
        )
        .unwrap();
        assert!(
            matches!(resumed, crate::db::StateWriteOutcome::Applied { .. }),
            "{resumed:?}"
        );
        drop(resume);
        service.drain_actions.wait_idle().await;

        // The fake node-manager fails every suspend the drain attempts, so
        // an attempt shows as the suspend's failure.
        let attempted = service
            .drain_actions
            .failures(7)
            .contains_key(&(id.clone(), DrainAction::Suspend));
        if holding {
            assert!(!attempted, "the suspend left it running");
            assert_eq!(
                service.lookup_sandbox(&id).unwrap().state,
                SandboxStatus::from(SandboxState::Started)
            );
            let next = service.finish_drain(7, &first, DrainScans::default());
            assert_eq!(kinds(&next.pending), [kind(&id, WireWait::Hold, true)]);
            assert!(service.drain_actions.running().is_empty());
        } else {
            assert!(attempted, "evacuating, it suspends it from started");
        }
    }
}

/// A drain suspend acts only on a row still started or paused, and only
/// under its own seal: a row something else suspended first, a node sealed
/// under another generation, or a started row while the drain holds gets no
/// suspend. One that fails leaves the row as it was, and it is planned
/// again.
#[tokio::test]
async fn a_drain_suspend_acts_only_on_its_own_resident_under_its_own_seal() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    for (state, generation, evacuate) in [
        (SandboxState::Suspended, 7, EVACUATE),
        (SandboxState::Started, 8, EVACUATE),
        (SandboxState::Started, 7, HOLD),
    ] {
        let conn = crate::db::test_conn();
        let id = pplx_space_sandbox_model::SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, state);
        let service = service_with_node_manager(conn, &node_manager);
        service.begin_drain(generation, evacuate).unwrap();
        service
            .drain_suspend(7, &id, SuspendFrom::StartedOrPaused)
            .await
            .unwrap();
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxStatus::from(state),
            "{state} under {generation}"
        );
    }
    assert_eq!(node_manager.calls(), Vec::<String>::new());

    let conn = crate::db::test_conn();
    let id = started_sandbox(&conn);
    let service = service_with_node_manager(conn, &node_manager);
    service.begin_drain(7, EVACUATE).unwrap();
    service
        .drain_suspend(7, &id, SuspendFrom::StartedOrPaused)
        .await
        .unwrap_err();
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
    );
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert_eq!(kinds(&report.pending), [kind(&id, WireWait::Moving, true)]);
}

/// A suspend planned under one generation that is still waiting for its locks
/// when that generation is cancelled and a later one seals, holding, suspends
/// nothing: it checks the seal once it holds both locks, and answers
/// without trying. The same suspend under its own seal, evacuating, is
/// attempted.
#[tokio::test]
async fn a_suspend_waiting_for_its_locks_suspends_nothing_under_a_later_seal() {
    for cancelled in [true, false] {
        let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
        let conn = crate::db::test_conn();
        let id = started_sandbox(&conn);
        let service = service_with_node_manager(conn, &node_manager);
        service.begin_drain(7, EVACUATE).unwrap();
        // A capture elsewhere holds the artifact lock.
        let capture = service
            .write_gate
            .lock_for_capture(&id, crate::service::write_gate::DrainCompatible::Suspend)
            .await;
        let suspend = tokio::spawn({
            let service = service.clone();
            let id = id.clone();
            async move {
                service
                    .drain_suspend(7, &id, SuspendFrom::StartedOrPaused)
                    .await
            }
        });
        // Once the suspend holds the wake lock, it waits for the capture.
        let wake_lock = service.auto_lifecycle.wake_lock(&id);
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Ok(free) = wake_lock.try_lock() {
                drop(free);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the suspend takes the wake lock");
        if cancelled {
            service.cancel_drain(7).unwrap();
            service.begin_drain(8, HOLD).unwrap();
        }
        drop(capture);
        let suspended = suspend.await.unwrap();
        if cancelled {
            suspended.unwrap();
            assert_eq!(node_manager.calls(), Vec::<String>::new());
            assert_eq!(
                service.lookup_sandbox(&id).unwrap().state,
                SandboxStatus::from(SandboxState::Started)
            );
        } else {
            // The attempt fails in this test service, which can't suspend a
            // VM.
            suspended.unwrap_err();
        }
    }
}

/// A drain action doesn't run once the node is no longer sealed under its
/// generation: an unpinned suspended sandbox's snapshot isn't published for
/// a drain a cancel ended.
#[tokio::test]
async fn a_drain_action_rechecks_its_seal() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    let conn = crate::db::test_conn();
    let id = pplx_space_sandbox_model::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Suspended);
    let service = service_with_node_manager(conn, &node_manager);
    service.begin_drain(7, EVACUATE).unwrap();
    service.activation_seal.unseal(&service.db, 7).unwrap();
    service
        .run_drain_action(7, PlannedAction::PublishSnapshot { sandbox_id: id })
        .await
        .unwrap();
    assert_eq!(node_manager.calls(), Vec::<String>::new());
}

/// A suspended sandbox whose snapshot is still uploading is pending; once
/// the upload completes it is evacuated with its snapshot and the node is
/// finished.
#[tokio::test]
async fn a_pending_upload_becomes_an_evacuation() {
    let conn = crate::db::test_conn();
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         ) VALUES ('sbx-a', 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}', ?1, 'full', 'suspend')",
        crate::db::fixture::params![FULL_ARTIFACT],
    )
    .unwrap();
    insert_artifact(&conn, FULL_ARTIFACT, ("suspend", None), false, "ready");
    let (service, first) = sealed_service_with(conn, 7);

    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report));
    assert!(report.evacuated.is_empty());
    assert_eq!(
        kinds(&report.pending),
        [kind("sbx-a", WireWait::Moving, false)]
    );
    service.drain_actions.wait_idle().await;

    service
        .local_artifacts()
        .mark_backup_completed(FULL_ARTIFACT.parse().unwrap())
        .unwrap();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert_eq!(
        report.evacuated,
        [DrainEvacuated {
            sandbox_id: "sbx-a".into(),
            artifact_kind: crate::proto::ArtifactKind::Full as i32,
            artifact_id: FULL_ARTIFACT.into(),
        }]
    );
    assert!(finished(&report), "{report:?}");
}

/// On a node with a suspended sandbox whose row pins no artifact, the drain
/// is not done: it publishes the sandbox's current instance as a full
/// artifact, which the backup adopts, and here, with no snapshot on disk,
/// fails past the backup's kind check, at its check of the snapshot's files.
/// The sandbox stays pending, its last failure the reason, and is never
/// reported evacuated or blocked.
#[tokio::test]
async fn an_unpinned_suspended_sandbox_keeps_the_drain_from_done() {
    // A backup needs the cluster-api wiring its upload registers with.
    if !crate::service::tests::enter_route_test_process(&[
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(std::ffi::OsStr::new("http://127.0.0.1:9")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("drain-publish-snapshot-test")),
        ),
    ]) {
        return;
    }
    let conn = crate::db::test_conn();
    let id = pplx_space_sandbox_model::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Suspended);
    let (service, first) = sealed_service_with(conn, 7);

    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert_eq!(kinds(&report.pending), [kind(&id, WireWait::Moving, false)]);
    service.drain_actions.wait_idle().await;

    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert!(report.evacuated.is_empty() && report.blocked.is_empty());
    assert_eq!(report.pending.len(), 1, "{report:?}");
    let failures = service.drain_actions.failures(7);
    let reason = failures
        .get(&(id.clone(), DrainAction::PublishSnapshot))
        .unwrap_or_else(|| panic!("the publication failed: {report:?}"));
    assert!(
        !reason.contains(
            pplx_space_rpc_contracts::artifact_backup_signals::CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER
        ) && !reason.contains("cluster-api wiring"),
        "the publication reaches the suspended instance's files: {reason}"
    );
    service.drain_actions.wait_idle().await;
}

/// An inventory busy with another caller is asked again within the
/// deadline: the call that met it still finishes.
#[tokio::test]
async fn a_busy_inventory_is_asked_again() {
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let busy_once = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary("ListOrphanCandidates", {
            let asked = std::sync::Arc::clone(&asked);
            move |_: pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest| {
                if asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    Err(tonic::Status::resource_exhausted(
                        "orphan inventory is busy; retry",
                    ))
                } else {
                    Ok(pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse::default())
                }
            }
        })
        .serve();
    let service = service_with_node_manager(crate::db::test_conn(), &busy_once);
    let report = drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    assert!(finished(&report), "{report:?}");
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 2);
}

/// The window is the positive number its variable sets, at most the
/// ceiling, or the default.
#[test]
fn the_drain_window_reads_a_positive_number_or_the_default() {
    for (raw, window) in [
        (None, DEFAULT_DRAIN_NODE_WINDOW),
        (Some("3"), 3),
        (Some(" 12 "), 12),
        (Some("13"), MAX_DRAIN_NODE_WINDOW),
        (Some("1000"), MAX_DRAIN_NODE_WINDOW),
        (Some("0"), DEFAULT_DRAIN_NODE_WINDOW),
        (Some("-1"), DEFAULT_DRAIN_NODE_WINDOW),
        (Some("eight"), DEFAULT_DRAIN_NODE_WINDOW),
    ] {
        assert_eq!(drain_node_window(raw), window, "{raw:?}");
    }
}

/// A failed drain suspend that left its capture pinned with a live upload is
/// remembered, and its upload ended once a later attempt's publication moves
/// the pin past it, whether that attempt fails or succeeds. The pin an
/// attempt leaves is never ended: a successful suspend's own capture keeps
/// its upload. A failure that pinned nothing new, or whose capture has no
/// live upload, remembers nothing.
#[test]
fn a_failed_drain_suspends_capture_is_ended_once_superseded() {
    let actions = DrainActions::default();
    // The first attempt publishes capture `c1` and fails its backup
    // handoff: `c1` stays pinned, its upload queued.
    assert!(
        actions
            .settle_captures("s", Some("c0"), Some("c1"), true, true)
            .is_empty()
    );
    // The second publishes `c2`, retiring `c1`, and fails before any upload.
    assert_eq!(
        actions.settle_captures("s", Some("c1"), Some("c2"), true, false),
        ["c1"]
    );
    // The third succeeds on `c3`: nothing left to end, and `c3` untouched.
    assert!(
        actions
            .settle_captures("s", Some("c2"), Some("c3"), false, true)
            .is_empty()
    );

    // A capture remembered across a success that superseded it is ended,
    // never the success's own.
    assert!(
        actions
            .settle_captures("t", Some("d0"), Some("d1"), true, true)
            .is_empty()
    );
    assert_eq!(
        actions.settle_captures("t", Some("d1"), Some("d2"), false, true),
        ["d1"]
    );
    assert!(
        actions
            .settle_captures("t", Some("d2"), Some("d3"), false, true)
            .is_empty()
    );

    // A failure that left the pin where it was, or a new pin with no live
    // upload, remembers nothing; a capture still pinned is kept.
    assert!(
        actions
            .settle_captures("u", Some("e0"), Some("e0"), true, true)
            .is_empty()
    );
    assert!(
        actions
            .settle_captures("u", Some("e0"), Some("e1"), true, false)
            .is_empty()
    );
    assert!(
        actions
            .settle_captures("u", Some("e1"), Some("e2"), false, true)
            .is_empty()
    );
    assert!(
        actions
            .settle_captures("v", Some("f0"), Some("f1"), true, true)
            .is_empty()
    );
    assert!(
        actions
            .settle_captures("v", Some("f1"), Some("f1"), true, true)
            .is_empty()
    );
    assert_eq!(
        actions.settle_captures("v", Some("f1"), Some("f2"), false, true),
        ["f1"]
    );
}

/// Ending a superseded capture's upload cancels its live job and no other:
/// the upload of the capture that superseded it, and any other sandbox's,
/// keep running.
#[tokio::test]
async fn ending_a_superseded_capture_cancels_only_its_upload() {
    use pplx_space_accounts::JobId;
    use pplx_space_snapshot_model::LocalArtifactId;
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let (superseded, kept, other) = (
        LocalArtifactId::new(),
        LocalArtifactId::new(),
        LocalArtifactId::new(),
    );
    let insert = |sandbox_id: &str, artifact: LocalArtifactId| {
        let job_id = JobId::new();
        let conn = service.db_conn().unwrap();
        pplx_space_common_jobs::db::insert_job(
            conn.raw(),
            pplx_space_common_jobs::db::InsertJob {
                job_id: &job_id,
                kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                payload: "{}",
                subject_id: Some(sandbox_id),
                dedup_key: Some(&crate::snapshot_upload_jobs::artifact_backup_dedup_key(
                    artifact,
                )),
                max_attempts: 5,
                traceparent: None,
                priority: pplx_space_common_jobs::JobPriority::default(),
            },
        )
        .unwrap();
        job_id
    };
    let ended = insert("sbx-a", superseded);
    let own = insert("sbx-a", kept);
    let elsewhere = insert("sbx-b", other);

    service.end_superseded_capture_upload("sbx-a", &superseded.to_string());
    service.end_superseded_capture_upload("sbx-a", &LocalArtifactId::new().to_string());

    let status = |job_id: &JobId| {
        let conn = service.db_conn().unwrap();
        pplx_space_common_jobs::db::get_job(conn.raw(), job_id)
            .unwrap()
            .unwrap()
            .status
    };
    assert_eq!(
        status(&ended),
        pplx_space_common_jobs::JobStatus::Cancelled.as_str()
    );
    assert_eq!(
        status(&own),
        pplx_space_common_jobs::JobStatus::Pending.as_str()
    );
    assert_eq!(
        status(&elsewhere),
        pplx_space_common_jobs::JobStatus::Pending.as_str()
    );
    assert!(!service.upload_job_active(&superseded.to_string()));
    assert!(service.upload_job_active(&kept.to_string()));
}

/// End `service`'s drain under `generation` and let a suspend `wake` held back
/// return without suspending.
async fn release_held_suspend(
    service: &SandboxManagerService,
    generation: u64,
    wake: pplx_space_sbx_mgr_store::auto_lifecycle::LifecycleMutexGuard,
) {
    service.cancel_drain(generation).unwrap();
    drop(wake);
    service.drain_actions.wait_idle().await;
}

/// While a restart handoff closes state-writer admission, a call starts no
/// drain action: the suspend it plans never starts, and a call to the next
/// process starts it. The same call on a node not handing off starts it,
/// held here at the sandbox's wake lock.
#[tokio::test]
async fn a_restart_handoff_starts_no_drain_action() {
    for handing_off in [false, true] {
        let conn = crate::db::test_conn();
        let id = started_sandbox(&conn);
        let service = SandboxManagerService::for_test(conn);
        let wake = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
        let first = service.begin_drain(7, EVACUATE).unwrap();
        if handing_off {
            service.begin_draining_for_restart();
        }
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert_eq!(
            service.drain_actions.running().get(&id),
            (!handing_off).then_some(&DrainAction::Suspend),
            "handing off: {handing_off}: {report:?}"
        );
        assert_eq!(
            service.active_state_transition_methods(),
            if handing_off {
                vec![]
            } else {
                vec![("NodeDrain", 1)]
            }
        );
        if handing_off {
            // The handoff refuses the cancel too; its successor takes it.
            drop(wake);
            service.drain_actions.wait_idle().await;
        } else {
            release_held_suspend(&service, 7, wake).await;
        }
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxStatus::from(SandboxState::Started)
        );
    }
}

/// A drain call's seal and a cancel each write the seal row under a
/// state-writer lease. Once a restart handoff closes state-writer admission,
/// either answers that it be retried on the successor and leaves the seal as
/// it was, in memory and in its row alike: the successor reads that row at
/// startup, so the predecessor can't answer a cancel done that leaves the
/// successor sealed, nor seal behind a successor that read it unsealed. A
/// refused cancel still stops this process evacuating, so the drain's
/// suspend of a started sandbox, queued on its wake lock, leaves it running.
#[tokio::test]
async fn a_restart_handoff_refuses_a_seal_or_cancel_and_changes_nothing() {
    for sealed_before in [false, true] {
        let conn = crate::db::test_conn();
        let id = started_sandbox(&conn);
        let service = SandboxManagerService::for_test(conn);
        let wake = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
        if sealed_before {
            let first = service.begin_drain(7, EVACUATE).unwrap();
            service.finish_drain(7, &first, DrainScans::default());
            assert_eq!(
                service.drain_actions.running().get(&id),
                Some(&DrainAction::Suspend)
            );
        }
        let seal = || {
            (
                service.activation_seal.status().sealed_generation,
                crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap(),
            )
        };
        let before = seal();
        service.begin_draining_for_restart();
        for request in [
            drain_request(7, EVACUATE),
            drain_request(8, HOLD),
            cancel_request(7),
        ] {
            let refused = service.drain_node(request).await.unwrap_err();
            assert!(
                pplx_space_sbx_mgr_client::is_restart_retry_on_successor(&refused),
                "sealed before: {sealed_before}: {refused:?}"
            );
            assert_eq!(seal(), before, "sealed before: {sealed_before}");
        }
        assert!(!service.drain_actions.evacuating(7));
        drop(wake);
        service.drain_actions.wait_idle().await;
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxStatus::from(SandboxState::Started),
            "sealed before: {sealed_before}"
        );
        assert_eq!(service.active_state_transition_count(), 0);
    }
}

/// `DrainNode`'s lease is admitted past a full state-writer capacity and a
/// closed dependency drain, which bound and gate every other writer: it
/// covers one write of the seal row in this process's database, one call at
/// a time. It never holds the dependency drain's work gate: a writer admitted
/// while it counts takes the gate as the first writer does, and the gate
/// goes when the last such writer ends, the seal-row write still counting.
/// A restart handoff refuses a new one and waits for one that counts.
#[tokio::test]
async fn drain_node_is_admitted_past_the_writer_cap_and_the_dependency_drain() {
    let locks = tempfile::tempdir().unwrap();
    let [capability, admission, work] =
        ["capability", "admission", "work"].map(|name| locks.path().join(name));
    for path in [&capability, &admission, &work] {
        std::fs::File::create(path).unwrap();
    }
    // A deployment's exclusive hold of a gate, if no writer shares it.
    let exclusive = |path: &std::path::Path| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .ok()
            .map(|()| file)
    };
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    service
        .enable_dependency_restart_drain(&capability, &admission, &work)
        .unwrap();

    let seal_write = service.state_writer_lease("DrainNode").unwrap();
    assert!(exclusive(&work).is_some(), "DrainNode holds no work gate");
    let writer = service.state_writer_lease("Test").unwrap();
    assert!(
        exclusive(&work).is_none(),
        "the writer shares the work gate"
    );
    drop((writer, seal_write));
    assert!(exclusive(&work).is_some());

    let writer = service.state_writer_lease("Test").unwrap();
    let seal_write = service.state_writer_lease("DrainNode").unwrap();
    assert!(
        exclusive(&work).is_none(),
        "the writer shares the work gate"
    );
    drop(writer);
    assert!(
        exclusive(&work).is_some(),
        "the work gate goes with the last writer that shares it"
    );
    let writer = service.state_writer_lease("Test").unwrap();
    assert!(
        exclusive(&work).is_none(),
        "a writer admitted beside the seal-row write takes the work gate"
    );
    drop((writer, seal_write));

    let writers: Vec<_> = (0..crate::service::drain::MAX_ACTIVE_STATE_WRITERS)
        .map(|_| service.state_writer_lease("Test").unwrap())
        .collect();
    assert!(matches!(
        service.state_writer_lease("Test"),
        Err(full) if full.code() == tonic::Code::ResourceExhausted
    ));
    let closed = exclusive(&admission).expect("no writer holds the admission gate");
    service.begin_drain(7, HOLD).unwrap();
    assert_eq!(service.activation_seal.status().sealed_generation, Some(7));
    assert_eq!(
        service.active_state_transition_count(),
        crate::service::drain::MAX_ACTIVE_STATE_WRITERS
    );
    drop((writers, closed));

    let seal_write = service.state_writer_lease("DrainNode").unwrap();
    service.begin_draining_for_restart();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            service.wait_for_state_transitions_drained()
        )
        .await
        .is_err(),
        "a restart handoff waits for the seal-row write"
    );
    drop(seal_write);
    tokio::time::timeout(
        Duration::from_secs(5),
        service.wait_for_state_transitions_drained(),
    )
    .await
    .expect("the handoff drains once the seal-row write ends");
    let refused = service.cancel_drain(7).unwrap_err();
    assert!(
        pplx_space_sbx_mgr_client::is_restart_retry_on_successor(&refused),
        "{refused:?}"
    );
    assert_eq!(service.activation_seal.status().sealed_generation, Some(7));
}

fn preview_request() -> Request<PreviewDrainNodeRequest> {
    crate::service::activation_support::internal_request(PreviewDrainNodeRequest {})
}

/// The report of a preview's answer, its only field.
#[track_caller]
fn previewed(response: Response<PreviewDrainNodeResponse>) -> DrainReport {
    response
        .into_inner()
        .report
        .expect("a preview answers a report")
}

/// Only root and node-api, which relays the control plane's preview, may
/// preview the node; node-ctl may not.
#[tokio::test]
async fn only_root_and_node_api_preview_the_node() {
    let inventory = orphan_inventory(Default::default());
    let service = service_with_node_manager(crate::db::test_conn(), &inventory);
    let refused = service
        .preview_drain_node(crate::service::tests::service_tests::node_ctl_request(
            PreviewDrainNodeRequest {},
        ))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        inventory.calls(),
        Vec::<String>::new(),
        "a refused call scans nothing"
    );
    let relayed = service
        .preview_drain_node(
            crate::service::tests::service_tests::node_api_trusted_lifecycle_request(
                PreviewDrainNodeRequest {},
            ),
        )
        .await
        .unwrap();
    assert!(finished(&previewed(relayed)));
    service.preview_drain_node(preview_request()).await.unwrap();
}

/// A preview can't see a seal it can't read: while the startup read of the
/// row has failed and no drain call has read it since, it answers
/// `Internal`; once a drain call has read it, and the node is unsealed, the
/// preview runs.
#[tokio::test]
async fn a_preview_refuses_while_the_seal_is_unreadable() {
    let conn = crate::db::test_conn();
    conn.execute_batch("ALTER TABLE activation_seal RENAME TO activation_seal_hidden")
        .unwrap();
    let inventory = orphan_inventory(Default::default());
    let service = service_with_node_manager(conn, &inventory);
    let refused = service
        .preview_drain_node(preview_request())
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Internal, "{refused:?}");
    assert!(
        refused
            .message()
            .starts_with("activation seal unreadable: ")
            && refused.message().contains("activation_seal"),
        "the refusal says why: {refused:?}"
    );

    service
        .db
        .lock()
        .unwrap()
        .execute_batch("ALTER TABLE activation_seal_hidden RENAME TO activation_seal")
        .unwrap();
    assert_eq!(service.cancel_drain(3).unwrap(), 3);
    assert!(finished(&previewed(
        service.preview_drain_node(preview_request()).await.unwrap()
    )));
}

/// A sealed node refuses a preview with `FailedPrecondition`, the sealed
/// marker and the generation it is sealed under, and the refusal changes
/// nothing: the seal, the rows and the drain's work stay as they were, and
/// node-manager is asked nothing. Once the drain is cancelled, the preview
/// runs.
#[tokio::test]
async fn a_preview_on_a_sealed_node_is_refused() {
    let conn = crate::db::test_conn();
    let id = started_sandbox(&conn);
    let inventory = orphan_inventory(Default::default());
    let service = service_with_node_manager(conn, &inventory);
    service.begin_drain(7, HOLD).unwrap();
    let before = row_witness(&service, &id);
    let row = crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap();
    let refused = service
        .preview_drain_node(preview_request())
        .await
        .unwrap_err();
    assert_eq!(
        refused.code(),
        tonic::Code::FailedPrecondition,
        "{refused:?}"
    );
    assert!(
        refused
            .message()
            .contains(pplx_space_rpc_contracts::wake_signals::ACTIVATIONS_SEALED_MARKER)
            && refused.message().contains("generation 7"),
        "{refused:?}"
    );
    assert_eq!(
        service
            .finish_preview(DrainScans::default())
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(service.activation_seal.status().sealed_generation, Some(7));
    assert_eq!(
        crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap(),
        row
    );
    assert_eq!(row_witness(&service, &id), before);
    assert!(service.drain_actions.running().is_empty());
    assert_eq!(inventory.calls(), Vec::<String>::new());

    service.cancel_drain(7).unwrap();
    let report = previewed(service.preview_drain_node(preview_request()).await.unwrap());
    assert_eq!(report.pending.len(), 1, "{report:?}");
}

/// One preview at a time: a preview that arrives while another is in
/// progress fails at once with `Aborted`, and once it ends, the next one
/// runs. A preview takes no drain call's slot: a drain call meanwhile runs.
#[tokio::test]
async fn a_preview_while_another_is_in_progress_is_aborted() {
    let inventory = orphan_inventory(Default::default());
    let service = service_with_node_manager(crate::db::test_conn(), &inventory);
    let held = service
        .drain_actions
        .enter_preview()
        .expect("no preview is in progress");
    let refused = tokio::time::timeout(
        Duration::from_secs(2),
        service.preview_drain_node(preview_request()),
    )
    .await
    .expect("a refused preview answers at once")
    .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Aborted, "{refused:?}");
    assert!(
        refused.message().contains(DRAIN_CALL_IN_PROGRESS_MARKER),
        "{refused:?}"
    );
    let call = service.drain_actions.enter_call();
    assert!(call.is_ok(), "a preview holds no drain call's slot");
    drop(call);
    drop(held);
    assert!(finished(&previewed(
        service.preview_drain_node(preview_request()).await.unwrap()
    )));
}

/// What a preview must leave as it found it on one row: its lifecycle, when
/// it entered it, and when the row was last written.
fn row_witness(
    service: &SandboxManagerService,
    id: &str,
) -> (SandboxStatus, String, Option<String>) {
    let row = service.lookup_sandbox(id).unwrap();
    let entered =
        crate::db::read::state_changed_at_witness(&service.db.lock().unwrap(), id).unwrap();
    (row.state, row.updated_at, entered)
}

/// A preview of a node with a started VM reports the suspend an evacuating
/// drain would start, `MOVING` and `running`, and seals nothing: the node
/// stays unsealed. It starts no suspend and writes nothing to the row.
/// Node-manager is asked nothing.
#[tokio::test]
async fn a_preview_seals_nothing_starts_nothing_and_writes_nothing() {
    let conn = crate::db::test_conn();
    let id = started_sandbox(&conn);
    let inventory = orphan_inventory(Default::default());
    let service = service_with_node_manager(conn, &inventory);
    let before = row_witness(&service, &id);
    // Through the call, the host's cache disk decides whether the suspend
    // waits for disk room; either is the drain's own work on the VM.
    let report = previewed(service.preview_drain_node(preview_request()).await.unwrap());
    assert_eq!(kinds(&report.pending), [kind(&id, WireWait::Moving, true)]);
    assert!(report.blocked.is_empty(), "{report:?}");
    assert_eq!(service.activation_seal.status().sealed_generation, None);
    assert_eq!(
        crate::db::read::activation_seal(&service.db.lock().unwrap()).unwrap(),
        None
    );

    // A suspend started now would wait here, in flight, for this lock.
    let wake_lock = service.auto_lifecycle.wake_lock(&id);
    let held = wake_lock.lock_owned().await;
    let report = service.finish_preview(DrainScans::default()).unwrap();
    assert_eq!(kinds(&report.pending), [kind(&id, WireWait::Moving, true)]);
    assert!(service.drain_actions.running().is_empty());
    assert!(
        service.active_state_transition_methods().is_empty(),
        "{:?}",
        service.active_state_transition_methods()
    );
    drop(held);

    assert_eq!(row_witness(&service, &id), before);
    assert_eq!(inventory.calls(), Vec::<String>::new());
}

/// A preview opens no scan of node-manager's orphan inventory, which a drain
/// call shares with every other caller on the node, and says so: a cache
/// directory no row accounts for, which blocks a drain call, doesn't show in
/// a preview. The drain call on the same node still reads it.
#[tokio::test]
async fn a_preview_opens_no_orphan_inventory_scan() {
    let inventory = orphan_inventory(
        pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse {
            orphaned_cache_dirs: vec!["/space/cache/sandboxes/sbx-orphan".into()],
            ..Default::default()
        },
    );
    let service = service_with_node_manager(crate::db::test_conn(), &inventory);
    for _ in 0..2 {
        let report = previewed(service.preview_drain_node(preview_request()).await.unwrap());
        assert!(finished(&report), "{report:?}");
        assert_eq!(inventory.calls(), Vec::<String>::new());
    }
    assert_eq!(service.activation_seal.status().sealed_generation, None);

    let report = drained(
        service
            .drain_node(drain_request(7, EVACUATE))
            .await
            .unwrap(),
        7,
    );
    assert_eq!(
        report.blocked,
        [DrainBlocked {
            reason: "orphan cache directory /space/cache/sandboxes/sbx-orphan".into(),
        }]
    );
    assert_eq!(inventory.calls(), ["ListOrphanCandidates"]);
}

/// A preview finds a drain finished only when it found nothing pending and
/// nothing blocked: on an empty node, and not once a scan blocks or a
/// stopped row, which the drain doesn't move, is pending.
#[tokio::test]
async fn a_preview_finds_a_drain_finished_only_with_nothing_pending_or_blocked() {
    let conn = crate::db::test_conn();
    let service = SandboxManagerService::for_test(conn);
    let empty = service.finish_preview(DrainScans::default()).unwrap();
    assert!(finished(&empty), "{empty:?}");
    let scan_failed = service
        .finish_preview(DrainScans {
            blocked: vec!["orphan inventory scan failed: CacheDirs: denied".into()],
            ..DrainScans::default()
        })
        .unwrap();
    assert!(
        !finished(&scan_failed) && scan_failed.pending.is_empty(),
        "{scan_failed:?}"
    );
    let stopped = insert_in_status(&service.db.lock().unwrap(), SandboxState::Stopped.into());
    let waiting = service.finish_preview(DrainScans::default()).unwrap();
    assert!(!finished(&waiting));
    assert_eq!(
        (kinds(&waiting.pending), waiting.blocked),
        (vec![kind(&stopped, WireWait::External, false)], Vec::new())
    );
    assert_eq!(waiting.pending[0].reason, super::planner::STOPPED_REASON);
}

/// A row in `status`, as the store would hold it: a cleanup's teardown
/// transition carries the completion that leads to it.
fn insert_in_status(conn: &crate::db::Connection, status: SandboxStatus) -> String {
    let id = pplx_space_sandbox_model::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(conn, &id, SandboxState::Started);
    match status {
        SandboxStatus::Unowned(lifecycle) => {
            crate::db::test_fixtures::state_write_fixtures::write_status(conn, &id, lifecycle);
        }
        SandboxStatus::Owned { teardown, cleanup } => {
            let completion = match teardown {
                crate::state::Teardown::Stopping => None,
                crate::state::Teardown::Deleting => Some("delete_row"),
                crate::state::Teardown::Suspending => Some("mark_suspended"),
            };
            conn.execute(
                "UPDATE sandboxes SET state = ?1, cleanup_state = ?2, cleanup_completion = ?3
                  WHERE id = ?4",
                crate::db::fixture::params![
                    teardown.lifecycle().as_str(),
                    cleanup.as_str(),
                    completion,
                    id
                ],
            )
            .unwrap();
        }
    }
    id
}

/// One item of a report: a blocked entry's reason, or a pending entry's
/// sandbox, kind and `running`.
type Item = (Option<String>, Option<String>, Option<(WireWait, bool)>);

fn items(blocked: &[DrainBlocked], pending: &[DrainPending]) -> Vec<Item> {
    let mut items: Vec<_> = blocked
        .iter()
        .map(|entry| (None, Some(entry.reason.clone()), None))
        .chain(pending.iter().map(|entry| {
            (
                Some(entry.sandbox_id.clone()),
                None,
                Some((entry.wait(), entry.running)),
            )
        }))
        .collect();
    items.sort();
    items
}

/// Preview `service`, then drain it under 7, evacuating, over the same rows
/// and scans, ending that drain before any work it
/// started runs; answer the preview's items once they are the drain call's,
/// sorted. The pending entries' reasons and stamps must match too.
async fn preview_then_drain(service: &SandboxManagerService, scans: DrainScans) -> Vec<Item> {
    let preview = service.finish_preview(scans.clone()).unwrap();
    service.begin_drain(7, EVACUATE).unwrap();
    let first = service.activation_seal.status();
    let drain = service.finish_drain(7, &first, scans);
    // The work the call started runs only once this test yields, and finds
    // the drain over.
    service.cancel_drain(7).unwrap();
    service.drain_actions.wait_idle().await;
    let by_sandbox = |pending: &[DrainPending]| {
        let mut pending = pending.to_vec();
        pending.sort_by(|left, right| left.sandbox_id.cmp(&right.sandbox_id));
        pending
    };
    assert_eq!(
        by_sandbox(&preview.pending),
        by_sandbox(&drain.pending),
        "a preview reports what the drain call does"
    );
    let preview = items(&preview.blocked, &preview.pending);
    assert_eq!(preview, items(&drain.blocked, &drain.pending));
    preview
}

/// Every status a row can be in, a row and a warm-pool member each, and
/// every arm that comes from elsewhere (an encrypted stopped sandbox, a
/// local-only artifact without a row, an orphan directory, a failed scan
/// and a refused suspend): a preview reports each item as a drain call over
/// the same rows and scans does, and as the planner's table says. Only the
/// node-wide items are blocked.
#[tokio::test]
async fn a_preview_reports_every_arm_as_a_drain_call_does() {
    use SandboxState as S;
    use SandboxStatus::{Owned, Unowned};
    let blocked = |reason: &str| -> Item { (None, Some(reason.to_owned()), None) };
    let pending = |id: &str, wait: WireWait, running: bool| -> Item {
        (Some(id.to_owned()), None, Some((wait, running)))
    };
    let conn = crate::db::test_conn();
    let mut expected = Vec::new();
    for status in SandboxStatus::all() {
        let id = insert_in_status(&conn, status);
        let running = status == Unowned(S::Started);
        expected.extend(match status {
            Unowned(S::Started | S::Paused) => Some(pending(&id, WireWait::Moving, running)),
            // A v2 drain doesn't move a stopped sandbox.
            Unowned(S::Stopped) => Some(pending(&id, WireWait::External, false)),
            Owned {
                cleanup: CleanupState::Failed,
                ..
            } => Some(pending(&id, WireWait::External, false)),
            // Its row pins no artifact, so nothing proves it durable.
            Unowned(S::Suspended) => Some(pending(&id, WireWait::Moving, false)),
            Unowned(S::Failed) => None,
            _ => Some(pending(&id, WireWait::External, false)),
        });
        let member = insert_in_status(&conn, status);
        conn.execute(
            "UPDATE sandboxes SET pool_template = 'seed' WHERE id = ?1",
            [&member],
        )
        .unwrap();
        if status.cleanup() == Some(CleanupState::Failed) {
            expected.push(pending(&member, WireWait::External, false));
        } else if may_hold_vm(status) {
            expected.push(pending(&member, WireWait::External, running));
        }
    }
    let encrypted = insert_in_status(&conn, S::Stopped.into());
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [&encrypted],
    )
    .unwrap();
    expected.push(pending(&encrypted, WireWait::External, false));
    let gone = pplx_space_sandbox_model::SandboxId::new().to_string();
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at, state
         ) VALUES (?1, ?2, 'disk', 'stop', NULL, '/tmp/a', '0', NULL, 'ready')",
        crate::db::fixture::params![NAMED_ARTIFACT, gone],
    )
    .unwrap();
    expected.push(blocked(&format!(
        "local-only artifact {NAMED_ARTIFACT} of sandbox {gone} has no sandbox row"
    )));
    let orphan = pplx_space_sandbox_model::SandboxId::new().to_string();
    let scan_failure = "orphan inventory scan failed: CacheDirs: denied";
    expected.push(blocked(&format!("orphan cache directory {orphan}")));
    expected.push(blocked(scan_failure));
    let scans = DrainScans {
        blocked: vec![scan_failure.into()],
        orphan_dirs: vec![(orphan.clone(), format!("orphan cache directory {orphan}"))],
        disk_headroom: None,
    };
    expected.sort();
    let service = SandboxManagerService::for_test(conn);
    assert_eq!(preview_then_drain(&service, scans).await, expected);

    // A node whose snapshots need file-backed memory refuses every suspend.
    let conn = crate::db::test_conn();
    let refused = insert_in_status(&conn, S::Started.into());
    let service = SandboxManagerService::new_with_config(
        conn,
        crate::service::SandboxManagerServiceOptions::for_test(),
        crate::config::SandboxConfig {
            hugepages: true,
            ..Default::default()
        },
    );
    assert_eq!(
        preview_then_drain(&service, DrainScans::default()).await,
        [pending(&refused, WireWait::Moving, true)]
    );
    let report = service.finish_preview(DrainScans::default()).unwrap();
    assert!(
        report.pending[0].reason.ends_with(
            "can't be made durable: snapshot restore requires file-backed memory; set \
             hugepages = false"
        ),
        "the refusal is the reason: {report:?}"
    );
}

/// A preview reads when each row entered its state from the store, as a
/// drain does, and judges nothing by it: a row in a transition the drain
/// doesn't drive, and a warm-pool member, both in their state for a long
/// time, are pending with that stamp, for the control plane to judge.
#[tokio::test]
async fn a_preview_reports_when_each_row_entered_its_state() {
    use pplx_space_sandbox_model::SandboxId;
    let conn = crate::db::test_conn();
    let stuck = SandboxId::new().to_string();
    let pool_member = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &stuck,
        SandboxState::Restoring,
    );
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &pool_member,
        SandboxState::Started,
    );
    let long_ago = crate::db::now_epoch_i64().unwrap() - 3600;
    conn.execute(
        "UPDATE sandboxes SET state_changed_at = ?1",
        [long_ago.to_string()],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET pool_template = 'seed' WHERE id = ?1",
        [&pool_member],
    )
    .unwrap();
    let service = SandboxManagerService::for_test(conn);
    let since = Some(prost_types::Timestamp {
        seconds: long_ago,
        nanos: 0,
    });
    let mut expected = vec![
        (stuck.clone(), WireWait::External, false, since),
        (pool_member.clone(), WireWait::External, true, since),
    ];
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    let stamped = |pending: Vec<DrainPending>| {
        let mut pending: Vec<_> = pending
            .into_iter()
            .map(|entry| {
                (
                    entry.sandbox_id.clone(),
                    entry.wait(),
                    entry.running,
                    entry.since,
                )
            })
            .collect();
        pending.sort_by(|left, right| left.0.cmp(&right.0));
        pending
    };

    let preview = service.finish_preview(DrainScans::default()).unwrap();
    assert!(preview.blocked.is_empty(), "{preview:?}");
    assert_eq!(stamped(preview.pending), expected);

    service.begin_drain(7, EVACUATE).unwrap();
    let first = service.activation_seal.status();
    let drain = service.finish_drain(7, &first, DrainScans::default());
    assert!(drain.blocked.is_empty(), "{drain:?}");
    assert_eq!(stamped(drain.pending), expected);
}
