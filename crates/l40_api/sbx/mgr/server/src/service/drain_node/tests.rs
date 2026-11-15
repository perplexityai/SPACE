use super::*;
use pplx_space_common_core::types::ArtifactKind;

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
/// or work in flight, beside a started control row: what the drain reports
/// for it, the work it starts, and whether it counts as a VM.
#[test]
fn every_lifecycle_plans_as_the_drain_table_says() {
    use SandboxState as S;
    use SandboxStatus::{Owned, Unowned};
    for state in SandboxStatus::all() {
        let rows = [row("control", S::Started), row("subject", state)];
        let plan = plan(&rows, &PlanInputs::default());
        assert!(
            plan.actions.contains(&suspend("control")),
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
                | PlannedAction::Upload { sandbox_id, .. } => sandbox_id == "subject",
            })
            .cloned()
            .collect();
        let (expect_pending, expect_blocked, expect_actions): (Vec<String>, Vec<&str>, Vec<_>) =
            match state {
                Unowned(S::Started | S::Paused) => {
                    (vec!["suspend".into()], vec![], vec![suspend("subject")])
                }
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
                // Durable already, or nothing on the node to keep.
                Unowned(S::Suspended | S::Failed) => (vec![], vec![], vec![]),
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
            ..row("subject", state)
        };
        let member_plan = super::plan(&[member], &PlanInputs::default());
        let cleanup_failed = matches!(
            state,
            Owned {
                cleanup: CleanupState::Failed,
                ..
            }
        );
        let holds_vm = !matches!(state, Unowned(S::Stopped | S::Suspended | S::Failed));
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

fn sealed_service(epoch: u64) -> (SandboxManagerService, ActivationSealStatus) {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    service.activation_seal.seal(&service.db, epoch).unwrap();
    let first = service.activation_seal.status();
    (service, first)
}

/// An empty node sealed under the call's epoch is done; a scan's failure
/// makes it blocked and not done.
#[tokio::test]
async fn an_empty_sealed_node_is_done_unless_a_scan_failed() {
    let (service, first) = sealed_service(7);
    let done = service.finish_drain(7, false, &first, DrainScans::default());
    assert!(done.done, "{done:?}");
    assert_eq!(done.sealed_epoch, Some(7));

    let blocked = service.finish_drain(
        7,
        false,
        &first,
        DrainScans {
            blocked: vec![(None, "can't scan the cache".into())],
            ..DrainScans::default()
        },
    );
    assert!(!blocked.done);
    assert_eq!(blocked.blocked.len(), 1);
}

/// An activation admitted before the seal and still in flight at the walk's
/// first read is pending, and keeps the drain from being done until it
/// settles.
#[tokio::test]
async fn an_activation_in_flight_before_the_seal_keeps_done_false() {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let token = service.activation_seal.admit("restore", "sbx-a").unwrap();
    service.activation_seal.seal(&service.db, 7).unwrap();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert!(!report.done);
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: "sbx-a".into(),
            action: "activation".into(),
            reason: String::new(),
        }]
    );

    drop(token);
    let first = service.activation_seal.status();
    assert!(
        service
            .finish_drain(7, false, &first, DrainScans::default())
            .done
    );
}

/// A seal that changed between the walk's first read and its end (another
/// call replaced the epoch) is not one continuous seal, and a seal under
/// another epoch is not the call's.
#[tokio::test]
async fn done_needs_one_continuous_seal_under_the_calls_epoch() {
    let (service, first) = sealed_service(7);
    service.activation_seal.seal(&service.db, 8).unwrap();
    assert!(
        !service
            .finish_drain(7, false, &first, DrainScans::default())
            .done
    );
    let first = service.activation_seal.status();
    assert!(
        !service
            .finish_drain(7, false, &first, DrainScans::default())
            .done
    );
    assert!(
        service
            .finish_drain(8, false, &first, DrainScans::default())
            .done
    );
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
    ];
    let inputs = PlanInputs {
        pending_backups: pending("uploading", FULL_ARTIFACT),
        evacuations: recorded
            .iter()
            .map(|id| evacuation(id, DrainPrior::Paused))
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
                Some("lost".to_owned()),
                "the drain's suspend left the sandbox suspended without a durable snapshot"
                    .to_owned()
            ),
        ]
    );
    assert_eq!(forget, ["failed", "gone", "resumed", "stopped"]);
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
/// flight is forgotten when named, or when nothing is left to hand back.
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
                    InFlight::Nothing => named || lifecycle != S::Suspended,
                    _ => false,
                };
                assert_eq!(forgotten, expect_forgotten, "{case}: {plan:?} {forget:?}");
                assert!(forget.contains(&"control".to_owned()), "{case}");
            }
        }
    }
}

/// A cancel keeps the drain's record and reports the sandbox evacuated, call
/// after call, until a cancel names it in `forget`; then nothing is left to
/// hand back and the cancel is done. The node stays unsealed throughout.
#[tokio::test]
async fn a_cancel_reports_an_evacuation_until_it_is_forgotten() {
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
    crate::db::activation_seal::record_drain_suspend(&conn, "sbx-a", DrainPrior::Started).unwrap();
    let clean = orphan_inventory(Default::default());
    let service = service_with_node_manager(conn, &clean);
    service.drain_node(drain_request(7, false)).await.unwrap();
    let cancel = |forget: Vec<String>| {
        crate::service::activation_support::internal_request(DrainNodeRequest {
            epoch: 7,
            cancel: true,
            forget,
            gentle_epoch: 0,
        })
    };
    let handed_back = |report: &DrainNodeResponse| -> Vec<(String, i32)> {
        report
            .evacuated
            .iter()
            .map(|entry| (entry.sandbox_id.clone(), entry.prior))
            .collect()
    };
    let started = vec![("sbx-a".to_owned(), crate::proto::DrainPrior::Started as i32)];

    for _ in 0..2 {
        let report = service
            .drain_node(cancel(Vec::new()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(report.sealed_epoch, None);
        assert_eq!(handed_back(&report), started);
        assert!(!report.done, "{report:?}");
    }
    let report = service
        .drain_node(cancel(vec!["sbx-a".into()]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(handed_back(&report), []);
    assert!(report.done, "{report:?}");
    assert_eq!(
        crate::db::read::drain_evacuations(&service.db_conn().unwrap()).unwrap(),
        []
    );
}

/// A suspended sandbox the drain suspended is reported with its recorded
/// prior lifecycle and its durable pin, and the seal and the record survive a
/// restart of the service.
#[tokio::test]
async fn a_recorded_evacuation_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manager.sqlite");
    let mut conn = crate::db::Connection::open_at(&path).unwrap();
    crate::db::create_test_schema(&mut conn);
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
    crate::db::activation_seal::record_drain_suspend(&conn, "sbx-a", DrainPrior::Paused).unwrap();

    let service = SandboxManagerService::for_test(conn);
    service.activation_seal.seal(&service.db, 7).unwrap();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, false, &first, DrainScans::default());
    let evacuated = |report: &DrainNodeResponse| -> Vec<(String, i32, String)> {
        report
            .evacuated
            .iter()
            .map(|entry| {
                (
                    entry.sandbox_id.clone(),
                    entry.prior,
                    entry.artifact_id.clone(),
                )
            })
            .collect()
    };
    let expected = vec![(
        "sbx-a".to_owned(),
        crate::proto::DrainPrior::Paused as i32,
        FULL_ARTIFACT.to_owned(),
    )];
    assert_eq!(evacuated(&report), expected);
    assert!(report.done, "{report:?}");

    // node-manager knows only its slots, so it names the suspended row's
    // cache directory a candidate; the row accounts for it. A directory with
    // no row blocks.
    let report = service.finish_drain(
        7,
        false,
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
    assert!(!report.done);
    let blocked: Vec<_> = report
        .blocked
        .iter()
        .map(|entry| (entry.sandbox_id.as_deref(), entry.reason.as_str()))
        .collect();
    assert_eq!(
        blocked,
        [(Some("sbx-gone"), "orphan cache directory /cache/sbx-gone")]
    );
    drop(service);

    let restarted = SandboxManagerService::for_test(crate::db::Connection::open_at(&path).unwrap());
    let first = restarted.activation_seal.status();
    assert_eq!(first.sealed_epoch, Some(7), "the seal survives the restart");
    let report = restarted.finish_drain(7, false, &first, DrainScans::default());
    assert_eq!(evacuated(&report), expected);
}

const NAMED_ARTIFACT: &str = "01939e3e-9003-7000-8000-000000000001";

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
/// drain finishes that upload and is done once it is durable. A named
/// snapshot already durable, or one a delete has reserved, holds nothing.
#[tokio::test]
async fn a_local_only_named_snapshot_holds_done_until_it_is_durable() {
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
    crate::db::activation_seal::record_drain_suspend(&conn, "sbx-a", DrainPrior::Started).unwrap();
    let (service, first) = {
        let service = SandboxManagerService::for_test(conn);
        service.activation_seal.seal(&service.db, 7).unwrap();
        let first = service.activation_seal.status();
        (service, first)
    };

    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert!(!report.done, "{report:?}");
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: "sbx-a".into(),
            action: "upload".into(),
            reason: String::new(),
        }]
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
    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert!(report.done, "{report:?}");
}

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

/// Failures are counted under the epoch they happened in: a later drain
/// starts from none, and a failure of an action an earlier epoch started
/// doesn't count toward the new one.
#[test]
fn a_new_epoch_starts_the_failure_count_again() {
    let actions = DrainActions::default();
    assert!(actions.failures(7).is_empty());
    for _ in 0..MAX_ACTION_FAILURES {
        assert!(actions.start("a", DrainAction::Upload));
        actions.finish(7, "a", Some("no space".into()));
    }
    assert_eq!(
        actions.failures(7),
        BTreeMap::from([(
            ("a".to_owned(), DrainAction::Upload),
            (MAX_ACTION_FAILURES, "no space".to_owned())
        )])
    );
    assert!(actions.start("a", DrainAction::Upload));
    assert!(
        actions.failures(8).is_empty(),
        "a new epoch forgets the old count"
    );
    actions.finish(7, "a", Some("late".into()));
    assert!(
        actions.failures(8).is_empty(),
        "the old epoch's action doesn't count"
    );
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

/// A row counts as unchanged once it has been in one lifecycle for the
/// bound under one epoch; a change of lifecycle, a call that doesn't see the
/// row, or a call under another epoch starts it over.
#[test]
fn a_row_is_unchanged_only_after_the_bound_in_one_lifecycle() {
    let actions = DrainActions::default();
    let start = tokio::time::Instant::now();
    let created = [("a".to_owned(), SandboxState::Created.into())];
    let restoring = [("a".to_owned(), SandboxState::Restoring.into())];
    let unchanged_under = |epoch: u64, rows: &[(String, SandboxStatus)], at: Duration| {
        actions.unchanged_since_bound(epoch, rows, start + at)
    };
    let unchanged = |rows: &[(String, SandboxStatus)], at: Duration| unchanged_under(7, rows, at);
    let a = BTreeSet::from(["a".to_owned()]);

    assert!(unchanged(&created, Duration::ZERO).is_empty());
    assert!(unchanged(&created, SETTLE_BOUND - Duration::from_secs(1)).is_empty());
    assert_eq!(unchanged(&created, SETTLE_BOUND), a);

    assert!(
        unchanged(&restoring, SETTLE_BOUND).is_empty(),
        "a new lifecycle"
    );
    assert_eq!(unchanged(&restoring, SETTLE_BOUND * 2), a);

    assert!(unchanged(&[], SETTLE_BOUND * 2).is_empty());
    assert!(
        unchanged(&restoring, SETTLE_BOUND * 3 - Duration::from_secs(1)).is_empty(),
        "a row the last call didn't see starts over"
    );
    assert_eq!(unchanged(&restoring, SETTLE_BOUND * 4), a);
    assert!(
        unchanged_under(8, &restoring, SETTLE_BOUND * 4).is_empty(),
        "a new epoch starts the clock again"
    );
}

/// A drain suspend that can't record the lifecycle it suspends from fails
/// before it suspends anything, and once it has failed the bound times in a
/// row the drain reports it blocked with why, instead of an evacuation it
/// couldn't remember.
#[tokio::test]
async fn a_failed_record_write_blocks_the_suspend() {
    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Started);
    // Reads of the record still work; every write of it fails.
    conn.execute_batch(
        "CREATE TRIGGER refuse_drain_records BEFORE INSERT ON drain_evacuations
         BEGIN SELECT RAISE(ABORT, 'refused by the test'); END;",
    )
    .unwrap();
    let (service, first) = {
        let service = SandboxManagerService::for_test(conn);
        service.activation_seal.seal(&service.db, 7).unwrap();
        let first = service.activation_seal.status();
        (service, first)
    };
    for _ in 0..MAX_ACTION_FAILURES {
        let report = service.finish_drain(7, false, &first, DrainScans::default());
        assert_eq!(
            report.pending,
            [DrainPending {
                sandbox_id: id.clone(),
                action: "suspend".into(),
                reason: String::new(),
            }],
            "{report:?}"
        );
        service.drain_actions.wait_idle().await;
    }
    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert!(!report.done);
    assert_eq!(report.blocked.len(), 1, "{report:?}");
    assert!(
        report.blocked[0]
            .reason
            .contains(&format!("record the drain suspend of {id}")),
        "{report:?}"
    );
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
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

fn drain_request(epoch: u64, cancel: bool) -> Request<DrainNodeRequest> {
    crate::service::activation_support::internal_request(DrainNodeRequest {
        epoch,
        cancel,
        forget: Vec::new(),
        gentle_epoch: 0,
    })
}

/// The whole call, through node-manager's inventory: a node with nothing on
/// it is sealed and done; a run directory no row accounts for, or a failed
/// scan of run directories, blocks it; a cancel unseals it.
#[tokio::test]
async fn drain_node_seals_reads_the_inventory_and_reports() {
    let clean = orphan_inventory(Default::default());
    let service = service_with_node_manager(crate::db::test_conn(), &clean);
    let report = service
        .drain_node(drain_request(7, false))
        .await
        .unwrap()
        .into_inner();
    assert!(report.done, "{report:?}");
    assert_eq!(report.sealed_epoch, Some(7));
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
    let report = service
        .drain_node(drain_request(7, false))
        .await
        .unwrap()
        .into_inner();
    assert!(!report.done);
    let blocked: Vec<_> = report
        .blocked
        .iter()
        .map(|entry| (entry.sandbox_id.as_deref(), entry.reason.as_str()))
        .collect();
    assert_eq!(
        blocked,
        [
            (
                None,
                "orphan inventory scan failed: RunDirs: permission denied"
            ),
            (
                Some("sbx-gone"),
                "orphan run directory /run/sandboxes/sbx-gone"
            ),
        ]
    );

    let cancelled = service
        .drain_node(drain_request(7, true))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cancelled.sealed_epoch, None);
    assert!(cancelled.done, "nothing to hand back: {cancelled:?}");
    assert_eq!(
        service
            .drain_node(drain_request(7, false))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
        "a cancelled epoch never seals again"
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
    let report = service
        .drain_node(drain_request(7, false))
        .await
        .unwrap()
        .into_inner();
    assert!(started.elapsed() < ORPHAN_INVENTORY_DEADLINE + Duration::from_secs(1));
    assert!(!report.done);
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
    let refused = service
        .drain_node(crate::service::tests::service_tests::node_ctl_request(
            DrainNodeRequest {
                epoch: 7,
                cancel: false,
                forget: Vec::new(),
                gentle_epoch: 0,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        service.activation_seal.status().sealed_epoch,
        None,
        "a refused call seals nothing"
    );
    let relayed = service
        .drain_node(
            crate::service::tests::service_tests::node_api_trusted_lifecycle_request(
                DrainNodeRequest {
                    epoch: 7,
                    cancel: false,
                    forget: Vec::new(),
                    gentle_epoch: 0,
                },
            ),
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(relayed.sealed_epoch, Some(7));
    service.drain_node(drain_request(8, false)).await.unwrap();
}

/// A preview answers what a drain would find on the whole node, so only
/// root and node-api, which relays the control plane's, may ask for one;
/// node-ctl, which may read sandbox state, may not.
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
        .unwrap()
        .into_inner();
    assert!(relayed.would_finish, "{relayed:?}");
    service.preview_drain_node(preview_request()).await.unwrap();
}

/// Every drain action holds a state-writer lease while it runs, so a
/// restart handoff waits for it.
#[tokio::test]
async fn a_drain_action_runs_as_a_state_writer() {
    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Started);
    let (service, first) = sealed_service_with(conn, 7);
    // The suspend waits on the wake lock this test holds.
    let wake_lock = service.auto_lifecycle.wake_lock(&id);
    let held = wake_lock.lock_owned().await;
    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert_eq!(report.pending.len(), 1, "{report:?}");
    assert!(
        service
            .active_state_transition_methods()
            .contains(&("NodeDrain", 1)),
        "{:?}",
        service.active_state_transition_methods()
    );
    // Another epoch takes over before the suspend gets the lock; it writes
    // nothing.
    service.activation_seal.seal(&service.db, 8).unwrap();
    drop(held);
    service.drain_actions.wait_idle().await;
    assert!(service.active_state_transition_methods().is_empty());
    assert!(
        crate::db::read::drain_evacuations(&service.db.lock().unwrap())
            .unwrap()
            .is_empty()
    );
}

/// A suspend the drain planned for a paused VM waits for the wake lock, as
/// a resume that was already in flight at the seal holds it. When that
/// resume finishes `started` first, a gentle drain's suspend leaves the
/// sandbox running, with no record, and the next gentle call holds it; a
/// forced drain's suspend still suspends it from `started`.
#[tokio::test]
async fn a_queued_gentle_suspend_leaves_a_sandbox_a_resume_started() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    for gentle in [true, false] {
        let conn = crate::db::test_conn();
        let id = pplx_space_common_core::types::SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Paused);
        let service = service_with_node_manager(conn, &node_manager);
        service.activation_seal.seal(&service.db, 7).unwrap();
        let first = service.activation_seal.status();
        // The resume in flight holds the wake lock while the drain plans.
        let wake_lock = service.auto_lifecycle.wake_lock(&id);
        let resume = wake_lock.lock_owned().await;
        let report = service.finish_drain(7, gentle, &first, DrainScans::default());
        assert_eq!(
            service.drain_actions.running().get(&id),
            Some(&DrainAction::Suspend),
            "gentle: {gentle}: {report:?}"
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
        let records = crate::db::read::drain_evacuations(&service.db.lock().unwrap()).unwrap();
        if gentle {
            assert!(!attempted, "the gentle suspend left it running");
            assert_eq!(records, []);
            assert_eq!(
                service.lookup_sandbox(&id).unwrap().state,
                SandboxStatus::from(SandboxState::Started)
            );
            let next = service.finish_drain(7, true, &first, DrainScans::default());
            assert_eq!(
                next.pending,
                [DrainPending {
                    sandbox_id: id,
                    action: HOLD.into(),
                    reason: HOLD_REASON.into(),
                }]
            );
            assert!(service.drain_actions.running().is_empty());
        } else {
            assert!(attempted, "the forced suspend suspends it from started");
        }
    }
}

fn sealed_service_with(
    conn: crate::db::Connection,
    epoch: u64,
) -> (SandboxManagerService, ActivationSealStatus) {
    let service = SandboxManagerService::for_test(conn);
    service.activation_seal.seal(&service.db, epoch).unwrap();
    let first = service.activation_seal.status();
    (service, first)
}

/// A drain suspend acts only on a row still started or paused, and only
/// under the call's own seal: a row something else suspended first, or a
/// seal another epoch replaced, gets no record and no suspend. One that
/// fails leaves no record behind, and the row is planned again.
#[tokio::test]
async fn a_drain_suspend_acts_only_on_its_own_resident_under_its_own_seal() {
    let records = |service: &SandboxManagerService| {
        crate::db::read::drain_evacuations(&service.db.lock().unwrap())
            .unwrap()
            .len()
    };
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    for (state, epoch) in [(SandboxState::Suspended, 7), (SandboxState::Started, 8)] {
        let conn = crate::db::test_conn();
        let id = pplx_space_common_core::types::SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, state);
        let service = service_with_node_manager(conn, &node_manager);
        service.activation_seal.seal(&service.db, epoch).unwrap();
        service
            .drain_suspend(7, &id, SuspendFrom::StartedOrPaused)
            .await
            .unwrap();
        assert_eq!(records(&service), 0, "{state} under {epoch}");
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxStatus::from(state)
        );
    }
    assert_eq!(node_manager.calls(), Vec::<String>::new());

    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Started);
    let service = service_with_node_manager(conn, &node_manager);
    service.activation_seal.seal(&service.db, 7).unwrap();
    service
        .drain_suspend(7, &id, SuspendFrom::StartedOrPaused)
        .await
        .unwrap_err();
    assert_eq!(records(&service), 0, "a failed suspend forgets its record");
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
    );
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: id,
            action: "suspend".into(),
            reason: String::new(),
        }]
    );
}

/// A drain suspend that failed after its pause leaves the row `paused`: the
/// record keeps the `started` it first recorded, through that failure and
/// the next attempt's, so the sandbox comes back started, not frozen.
#[tokio::test]
async fn a_drain_suspend_that_failed_after_its_pause_keeps_the_started_record() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Paused);
    // What the first attempt left: its record, from the started VM it paused.
    crate::db::activation_seal::record_drain_suspend(&conn, &id, DrainPrior::Started).unwrap();
    let service = service_with_node_manager(conn, &node_manager);
    service.activation_seal.seal(&service.db, 7).unwrap();
    service
        .drain_suspend(7, &id, SuspendFrom::StartedOrPaused)
        .await
        .unwrap_err();
    assert_ne!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Suspended)
    );
    assert_eq!(
        crate::db::read::drain_evacuations(&service.db.lock().unwrap()).unwrap(),
        [DrainEvacuation {
            sandbox_id: id,
            prior: DrainPrior::Started,
        }]
    );
}

/// A caller's suspend of a sandbox the drain holds a record for (an ECW
/// pause among them) takes it back: the record goes, so the drain reports
/// it suspended with no prior and nothing restores it started.
#[tokio::test]
async fn a_callers_suspend_takes_the_sandbox_back_from_the_drain() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Paused);
    crate::db::activation_seal::record_drain_suspend(&conn, &id, DrainPrior::Started).unwrap();
    let service = service_with_node_manager(conn, &node_manager);
    service.activation_seal.seal(&service.db, 7).unwrap();
    // Whether the suspend itself lands doesn't matter here.
    let _ = service
        .suspend(crate::service::activation_support::internal_request(
            crate::proto::SuspendRequest {
                sandbox_id: id.clone(),
                blocking: true,
                format: crate::proto::SuspendSnapshotFormat::Unspecified as i32,
            },
        ))
        .await;
    assert_eq!(
        crate::db::read::drain_evacuations(&service.db.lock().unwrap()).unwrap(),
        []
    );
}

/// A drain action doesn't run once the node is no longer sealed under its
/// epoch: a stopped sandbox's disk isn't published for a drain a cancel
/// ended.
#[tokio::test]
async fn a_drain_action_rechecks_its_seal() {
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let service = service_with_node_manager(conn, &node_manager);
    service.activation_seal.seal(&service.db, 7).unwrap();
    service.activation_seal.unseal(&service.db, 7).unwrap();
    service
        .run_drain_action(7, PlannedAction::PublishDisk { sandbox_id: id })
        .await
        .unwrap();
    assert_eq!(node_manager.calls(), Vec::<String>::new());
}

/// A suspended sandbox the drain suspended, whose snapshot is still
/// uploading, is pending; once the upload completes it is evacuated with its
/// prior lifecycle and the node is done.
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
    crate::db::activation_seal::record_drain_suspend(&conn, "sbx-a", DrainPrior::Paused).unwrap();
    let (service, first) = sealed_service_with(conn, 7);

    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert!(!report.done);
    assert!(report.evacuated.is_empty());
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: "sbx-a".into(),
            action: "upload".into(),
            reason: String::new(),
        }]
    );
    service.drain_actions.wait_idle().await;

    service
        .local_artifacts()
        .mark_backup_completed(FULL_ARTIFACT.parse().unwrap())
        .unwrap();
    let report = service.finish_drain(7, false, &first, DrainScans::default());
    assert_eq!(
        report.evacuated,
        [DrainEvacuated {
            sandbox_id: "sbx-a".into(),
            prior: crate::proto::DrainPrior::Paused as i32,
            artifact_id: FULL_ARTIFACT.into(),
        }]
    );
    assert!(report.done, "{report:?}");
}

/// An inventory busy with another caller is asked again within the
/// deadline: the call that met it still reports `done`.
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
    let report = service
        .drain_node(drain_request(7, false))
        .await
        .unwrap()
        .into_inner();
    assert!(report.done, "{report:?}");
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 2);
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

/// A row's wait for disk is clocked from the first call that saw it wait,
/// under one epoch; a call it no longer waits in, or another epoch, starts
/// it over.
#[test]
fn a_disk_wait_is_past_the_bound_only_after_waiting_that_long() {
    let actions = DrainActions::default();
    let start = tokio::time::Instant::now();
    let a = ["a".to_owned()];
    let past = |epoch: u64, at: Duration| actions.disk_waits_past_bound(epoch, start + at);

    actions.note_disk_waits(7, &a, start);
    actions.note_disk_waits(7, &a, start + SETTLE_BOUND / 2);
    assert!(past(7, SETTLE_BOUND - Duration::from_secs(1)).is_empty());
    assert_eq!(past(7, SETTLE_BOUND), BTreeSet::from(["a".to_owned()]));
    assert!(past(8, SETTLE_BOUND).is_empty(), "another epoch's wait");

    actions.note_disk_waits(7, &[], start + SETTLE_BOUND);
    actions.note_disk_waits(7, &a, start + SETTLE_BOUND);
    assert!(
        past(7, SETTLE_BOUND * 2 - Duration::from_secs(1)).is_empty(),
        "a row that stopped waiting starts over"
    );
}

/// The window is the positive number its variable sets, or the default.
#[test]
fn the_drain_window_reads_a_positive_number_or_the_default() {
    for (raw, window) in [
        (None, DEFAULT_DRAIN_NODE_WINDOW),
        (Some("3"), 3),
        (Some(" 12 "), 12),
        (Some("0"), DEFAULT_DRAIN_NODE_WINDOW),
        (Some("-1"), DEFAULT_DRAIN_NODE_WINDOW),
        (Some("eight"), DEFAULT_DRAIN_NODE_WINDOW),
    ] {
        assert_eq!(drain_node_window(raw), window, "{raw:?}");
    }
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

fn gentle_request(epoch: u64) -> Request<DrainNodeRequest> {
    crate::service::activation_support::internal_request(DrainNodeRequest {
        gentle_epoch: epoch,
        ..DrainNodeRequest::default()
    })
}

/// A gentle call seals the node under its `gentle_epoch`, leaves a started
/// sandbox running, reports it held with why, and echoes the epoch; a
/// forced call under the same epoch then suspends it, and echoes nothing.
#[tokio::test]
async fn a_gentle_call_seals_holds_and_says_so() {
    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Started);
    let node_manager = orphan_inventory(Default::default());
    let service = service_with_node_manager(conn, &node_manager);
    let report = service
        .drain_node(gentle_request(7))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (report.sealed_epoch, report.gentle_epoch, report.done),
        (Some(7), Some(7), false)
    );
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: id.clone(),
            action: HOLD.into(),
            reason: "running; waits for the sandbox to go idle".into(),
        }]
    );
    assert!(service.drain_actions.running().is_empty());
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
    );

    let forced = service
        .drain_node(drain_request(7, false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((forced.sealed_epoch, forced.gentle_epoch), (Some(7), None));
    // Its suspend starts, or waits for the cache disk to have room.
    assert!(
        matches!(forced.pending[0].action.as_str(), "suspend" | "disk"),
        "{forced:?}"
    );
    service.drain_actions.wait_idle().await;
}

/// The request as a node that predates gentle drains decodes it: fields 1
/// to 3 only.
#[derive(Clone, PartialEq, prost::Message)]
struct DrainNodeRequestBeforeGentle {
    #[prost(uint64, tag = "1")]
    epoch: u64,
    #[prost(bool, tag = "2")]
    cancel: bool,
    #[prost(string, repeated, tag = "3")]
    forget: Vec<String>,
}

/// A gentle request, read by a node or relay that predates gentle drains,
/// is a drain under epoch 0, which the node refuses before it seals or
/// suspends anything; so is a gentle request that names a forced epoch too,
/// or a cancel.
#[tokio::test]
async fn a_gentle_request_without_its_field_seals_and_suspends_nothing() {
    use prost::Message as _;
    let before = DrainNodeRequestBeforeGentle::decode(
        DrainNodeRequest {
            gentle_epoch: 7,
            ..DrainNodeRequest::default()
        }
        .encode_to_vec()
        .as_slice(),
    )
    .unwrap();
    assert_eq!(before.epoch, 0, "{before:?}");
    // What a relay that predates the field forwards.
    let relayed = DrainNodeRequest::decode(before.encode_to_vec().as_slice()).unwrap();
    for request in [
        relayed,
        DrainNodeRequest {
            epoch: 7,
            gentle_epoch: 7,
            ..DrainNodeRequest::default()
        },
        DrainNodeRequest {
            cancel: true,
            gentle_epoch: 7,
            ..DrainNodeRequest::default()
        },
    ] {
        let conn = crate::db::test_conn();
        let id = pplx_space_common_core::types::SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(
            &conn,
            &id,
            SandboxState::Started,
        );
        let node_manager = orphan_inventory(Default::default());
        let service = service_with_node_manager(conn, &node_manager);
        let refused = service
            .drain_node(crate::service::activation_support::internal_request(
                request.clone(),
            ))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{request:?}");
        assert_eq!(service.activation_seal.status().sealed_epoch, None);
        assert!(service.drain_actions.running().is_empty());
        assert_eq!(node_manager.calls(), Vec::<String>::new(), "{request:?}");
    }
}

fn preview_request() -> Request<PreviewDrainNodeRequest> {
    crate::service::activation_support::internal_request(PreviewDrainNodeRequest {})
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

/// A preview of a node with a started VM reports the suspend a drain would
/// start, and seals nothing: an unsealed node stays unsealed and a sealed
/// one stays sealed under its epoch, which the preview reports. It starts
/// no suspend, even under a seal whose drain would, and writes neither the
/// row nor an evacuation record. Only the orphan inventory is asked.
#[tokio::test]
async fn a_preview_seals_nothing_starts_nothing_and_writes_nothing() {
    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Started);
    let inventory = orphan_inventory(Default::default());
    let service = service_with_node_manager(conn, &inventory);
    let before = row_witness(&service, &id);
    // Through the call, the host's cache disk decides between `suspend` and
    // `disk`; either names only the VM.
    let preview = || async {
        let report = service
            .preview_drain_node(preview_request())
            .await
            .unwrap()
            .into_inner();
        assert!(
            report.pending.len() == 1 && report.pending[0].sandbox_id == id,
            "{report:?}"
        );
        assert!(
            report.blocked.is_empty() && !report.would_finish,
            "{report:?}"
        );
        report.sealed_epoch
    };

    assert_eq!(preview().await, None);
    assert_eq!(service.activation_seal.status().sealed_epoch, None);

    service.activation_seal.seal(&service.db, 7).unwrap();
    assert_eq!(preview().await, Some(7));
    assert_eq!(service.activation_seal.status().sealed_epoch, Some(7));
    // A suspend started now would wait here, in flight, for this lock.
    let wake_lock = service.auto_lifecycle.wake_lock(&id);
    let held = wake_lock.lock_owned().await;
    let report = service.finish_preview(DrainScans::default());
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: id.clone(),
            action: "suspend".into(),
            reason: String::new(),
        }]
    );
    assert!(service.drain_actions.running().is_empty());
    assert!(
        service.active_state_transition_methods().is_empty(),
        "{:?}",
        service.active_state_transition_methods()
    );
    drop(held);

    assert_eq!(row_witness(&service, &id), before);
    assert!(
        crate::db::read::drain_evacuations(&service.db.lock().unwrap())
            .unwrap()
            .is_empty()
    );
    assert!(
        inventory
            .calls()
            .iter()
            .all(|call| call.ends_with("ListOrphanCandidates")),
        "{:?}",
        inventory.calls()
    );
}

/// A preview reads node-manager's orphan inventory as a drain call does,
/// once per call: a cache directory no row accounts for blocks it.
#[tokio::test]
async fn a_preview_reads_the_orphan_inventory_once_and_reports_its_findings() {
    let inventory = orphan_inventory(
        pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse {
            orphaned_cache_dirs: vec!["/space/cache/sandboxes/sbx-orphan".into()],
            ..Default::default()
        },
    );
    let service = service_with_node_manager(crate::db::test_conn(), &inventory);
    for calls in 1..=2 {
        let report = service
            .preview_drain_node(preview_request())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            report.blocked,
            [DrainBlocked {
                sandbox_id: Some("sbx-orphan".into()),
                reason: "orphan cache directory /space/cache/sandboxes/sbx-orphan".into(),
            }]
        );
        assert!(!report.would_finish);
        assert_eq!(inventory.calls(), vec!["ListOrphanCandidates"; calls]);
    }
    assert_eq!(service.activation_seal.status().sealed_epoch, None);
}

/// A preview says a drain would finish only when it found nothing pending and
/// nothing blocked: on an empty node, and not once a scan blocks or a row
/// waits for work.
#[tokio::test]
async fn a_preview_would_finish_only_with_nothing_pending_or_blocked() {
    let conn = crate::db::test_conn();
    let service = SandboxManagerService::for_test(conn);
    let empty = service.finish_preview(DrainScans::default());
    assert!(
        empty.would_finish && empty.pending.is_empty() && empty.blocked.is_empty(),
        "{empty:?}"
    );
    let scan_failed = service.finish_preview(DrainScans {
        blocked: vec![(
            None,
            "orphan inventory scan failed: CacheDirs: denied".into(),
        )],
        ..DrainScans::default()
    });
    assert!(
        !scan_failed.would_finish && scan_failed.pending.is_empty(),
        "{scan_failed:?}"
    );
    let stopped = insert_in_status(&service.db.lock().unwrap(), SandboxState::Stopped.into());
    let waiting = service.finish_preview(DrainScans::default());
    assert_eq!(
        (waiting.would_finish, waiting.pending, waiting.blocked),
        (
            false,
            vec![DrainPending {
                sandbox_id: stopped,
                action: "publish_disk".into(),
                reason: String::new(),
            }],
            Vec::new()
        )
    );
}

/// Under a seal, a preview plans with the memory the drain's own calls
/// built: an action that failed the bound times, a transition the drain has
/// watched past the bound, and a wait for cache disk past it each block with
/// the drain's reason. A drain call after the preview reports the same
/// blockers, so the preview consumed none of that memory.
#[tokio::test(start_paused = true)]
async fn a_sealed_preview_reports_what_the_drains_memory_blocks_on() {
    let conn = crate::db::test_conn();
    let failing = pplx_space_common_core::types::SandboxId::new().to_string();
    let settling = pplx_space_common_core::types::SandboxId::new().to_string();
    let waiting = pplx_space_common_core::types::SandboxId::new().to_string();
    for (id, state) in [
        (&failing, SandboxState::Stopped),
        (&settling, SandboxState::Created),
        (&waiting, SandboxState::Started),
    ] {
        crate::service::tests::service_tests::insert_test_sandbox(&conn, id, state);
    }
    // A node-manager that answers nothing fails every disk publication.
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    let service = service_with_node_manager(conn, &node_manager);
    service.activation_seal.seal(&service.db, 7).unwrap();
    let no_disk = || DrainScans {
        disk_headroom: Some(0),
        ..DrainScans::default()
    };
    let blocked = |blocked: Vec<DrainBlocked>| {
        let mut blocked: Vec<_> = blocked
            .into_iter()
            .map(|entry| (entry.sandbox_id, entry.reason))
            .collect();
        blocked.sort();
        blocked
    };
    for _ in 0..MAX_ACTION_FAILURES {
        let first = service.activation_seal.status();
        service.finish_drain(7, false, &first, no_disk());
        service.drain_actions.wait_idle().await;
    }
    tokio::time::advance(SETTLE_BOUND).await;

    let preview = blocked(service.finish_preview(no_disk()).blocked);
    let first = service.activation_seal.status();
    let drain = blocked(service.finish_drain(7, false, &first, no_disk()).blocked);
    service.drain_actions.wait_idle().await;
    assert_eq!(preview, drain);
    let reasons: BTreeMap<_, _> = preview
        .into_iter()
        .map(|(id, reason)| (id.unwrap(), reason))
        .collect();
    assert!(
        reasons[&failing].starts_with(&format!(
            "publish_disk failed {MAX_ACTION_FAILURES} times: "
        )),
        "{reasons:?}"
    );
    assert_eq!(
        reasons[&settling],
        "created for over 15 minutes, a transition the drain doesn't drive"
    );
    assert!(
        reasons[&waiting].starts_with("the cache disk hasn't had room for a "),
        "{reasons:?}"
    );
    assert_eq!(reasons.len(), 3, "{reasons:?}");
}

/// A drain call after a preview plans with its own memory of earlier calls,
/// not one the preview started: a row the preview saw in a transition, and
/// one it saw waiting for cache disk, are still pending once the bound has
/// passed since the preview, because the drain saw neither before.
#[tokio::test(start_paused = true)]
async fn a_drain_after_a_preview_starts_its_memory_fresh() {
    let conn = crate::db::test_conn();
    let settling = pplx_space_common_core::types::SandboxId::new().to_string();
    let waiting = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &settling,
        SandboxState::Created,
    );
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &waiting,
        SandboxState::Started,
    );
    let (service, first) = sealed_service_with(conn, 7);
    let no_disk = || DrainScans {
        disk_headroom: Some(0),
        ..DrainScans::default()
    };
    let mut expected = vec![
        DrainPending {
            sandbox_id: settling.clone(),
            action: "settle:created".into(),
            reason: String::new(),
        },
        DrainPending {
            sandbox_id: waiting.clone(),
            action: "disk".into(),
            reason: String::new(),
        },
    ];
    expected.sort_by(|left, right| left.sandbox_id.cmp(&right.sandbox_id));
    let sorted = |mut pending: Vec<DrainPending>, blocked: Vec<DrainBlocked>| {
        pending.sort_by(|left, right| left.sandbox_id.cmp(&right.sandbox_id));
        (pending, blocked)
    };

    let preview = service.finish_preview(no_disk());
    assert_eq!(
        sorted(preview.pending, preview.blocked),
        (expected.clone(), Vec::new())
    );
    tokio::time::advance(SETTLE_BOUND).await;
    let drain = service.finish_drain(7, false, &first, no_disk());
    assert_eq!(sorted(drain.pending, drain.blocked), (expected, Vec::new()));
}

/// A row in `status`, as the store would hold it: a cleanup's teardown
/// transition carries the completion that leads to it.
fn insert_in_status(conn: &crate::db::Connection, status: SandboxStatus) -> String {
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
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

/// Seal `service` under 7 after a preview, make a drain call over the same
/// rows and scans, and answer the preview's items once they are the drain
/// call's: each blocker and each pending item, sorted.
async fn preview_then_drain(
    service: &SandboxManagerService,
    scans: DrainScans,
) -> Vec<(Option<String>, Option<String>, Option<String>)> {
    let items = |blocked: &[DrainBlocked], pending: &[DrainPending]| {
        let mut items: Vec<_> = blocked
            .iter()
            .map(|entry| (entry.sandbox_id.clone(), Some(entry.reason.clone()), None))
            .chain(pending.iter().map(|entry| {
                (
                    Some(entry.sandbox_id.clone()),
                    None,
                    Some(entry.action.clone()),
                )
            }))
            .collect();
        items.sort();
        items
    };
    let preview = service.finish_preview(scans.clone());
    let preview = items(&preview.blocked, &preview.pending);
    service.activation_seal.seal(&service.db, 7).unwrap();
    let first = service.activation_seal.status();
    let drain = service.finish_drain(7, false, &first, scans);
    let drain = items(&drain.blocked, &drain.pending);
    service.drain_actions.wait_idle().await;
    assert_eq!(preview, drain, "a preview reports what the drain call does");
    preview
}

/// Every status a row can be in, a row and a warm-pool member each, and
/// every arm that comes from elsewhere (an encrypted stopped sandbox, a
/// local-only artifact without a row, an orphan directory, a failed scan
/// and a refused suspend): a preview reports each item as a drain call over
/// the same rows and scans does, and as the planner's table says.
#[tokio::test]
async fn a_preview_reports_every_arm_as_a_drain_call_does() {
    use SandboxState as S;
    use SandboxStatus::{Owned, Unowned};
    type Item = (Option<String>, Option<String>, Option<String>);
    let blocked =
        |id: &str, reason: &str| -> Item { (Some(id.to_owned()), Some(reason.to_owned()), None) };
    let pending =
        |id: &str, action: &str| -> Item { (Some(id.to_owned()), None, Some(action.to_owned())) };
    let conn = crate::db::test_conn();
    let mut expected = Vec::new();
    for status in SandboxStatus::all() {
        let id = insert_in_status(&conn, status);
        expected.extend(match status {
            Unowned(S::Started | S::Paused) => Some(pending(&id, "suspend")),
            Unowned(S::Stopped) => Some(pending(&id, "publish_disk")),
            Unowned(S::Quarantined) => Some(blocked(&id, "quarantined")),
            Owned {
                cleanup: CleanupState::Failed,
                ..
            } => Some(blocked(&id, "cleanup failed")),
            Unowned(S::Suspended | S::Failed) => None,
            other => Some(pending(&id, &format!("settle:{}", other.legacy_label()))),
        });
        let member = insert_in_status(&conn, status);
        conn.execute(
            "UPDATE sandboxes SET pool_template = 'seed' WHERE id = ?1",
            [&member],
        )
        .unwrap();
        if status.cleanup() == Some(CleanupState::Failed) {
            expected.push(blocked(&member, "cleanup failed"));
        } else if may_hold_vm(status) {
            expected.push(pending(&member, "warm_pool_eviction"));
        }
    }
    let encrypted = insert_in_status(&conn, S::Stopped.into());
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [&encrypted],
    )
    .unwrap();
    expected.push(blocked(
        &encrypted,
        "an encrypted stopped sandbox's disk can't be published",
    ));
    let gone = pplx_space_common_core::types::SandboxId::new().to_string();
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at, state
         ) VALUES (?1, ?2, 'disk', 'stop', NULL, '/tmp/a', '0', NULL, 'ready')",
        crate::db::fixture::params![NAMED_ARTIFACT, gone],
    )
    .unwrap();
    expected.push(blocked(
        &gone,
        &format!("local-only artifact {NAMED_ARTIFACT} has no sandbox row"),
    ));
    let orphan = pplx_space_common_core::types::SandboxId::new().to_string();
    let scan_failure = "orphan inventory scan failed: CacheDirs: denied";
    expected.push(blocked(
        &orphan,
        &format!("orphan cache directory {orphan}"),
    ));
    expected.push((None, Some(scan_failure.to_owned()), None));
    let scans = DrainScans {
        blocked: vec![(None, scan_failure.into())],
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
        [blocked(
            &refused,
            "can't be made durable: snapshot restore requires file-backed memory; set \
             hugepages = false"
        )]
    );
}

/// A preview has watched no row, so it reads how long one has sat in its
/// lifecycle from the store: a row in a transition the drain doesn't drive
/// whose lifecycle last changed past the bound is blocked, as a drain that
/// had watched it that long would report it, while a drain's first call
/// still waits for it. A warm-pool member's bound is on its eviction, which
/// only a drain's cordon starts, so it stays pending however long it ran.
#[tokio::test]
async fn a_preview_judges_a_long_transition_by_when_the_row_entered_it() {
    use pplx_space_common_core::types::SandboxId;
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
    let long_ago =
        crate::db::now_epoch_i64().unwrap() - i64::try_from(SETTLE_BOUND.as_secs()).unwrap() - 60;
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

    let preview = service.finish_preview(DrainScans::default());
    assert_eq!(
        preview.blocked,
        [DrainBlocked {
            sandbox_id: Some(stuck.clone()),
            reason: "restoring for over 15 minutes, a transition the drain doesn't drive".into(),
        }]
    );
    assert_eq!(
        preview.pending,
        [DrainPending {
            sandbox_id: pool_member.clone(),
            action: "warm_pool_eviction".into(),
            reason: String::new(),
        }]
    );

    service.activation_seal.seal(&service.db, 7).unwrap();
    let first = service.activation_seal.status();
    let drain = service.finish_drain(7, false, &first, DrainScans::default());
    assert!(drain.blocked.is_empty(), "{drain:?}");
    assert_eq!(drain.pending.len(), 2, "{drain:?}");
}
