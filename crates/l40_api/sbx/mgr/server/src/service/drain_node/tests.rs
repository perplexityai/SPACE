use super::*;
use pplx_space_common_core::types::ArtifactKind;

const FULL_ARTIFACT: &str = "01939e3e-9001-7000-8000-000000000001";
const DISK_ARTIFACT: &str = "01939e3e-9002-7000-8000-000000000001";

fn row(id: &str, lifecycle: SandboxState) -> DrainRow {
    DrainRow {
        sandbox_id: id.to_owned(),
        lifecycle,
        pool_member: false,
        volume_encryption: false,
        pin: None,
        suspend_refusal: None,
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

/// Every lifecycle, with no pin, record or work in flight, beside a started
/// control row: what the drain reports for it, the work it starts, and
/// whether it counts as a VM.
#[test]
fn every_lifecycle_plans_as_the_drain_table_says() {
    use SandboxState as S;
    for state in [
        S::Creating,
        S::Created,
        S::Starting,
        S::Started,
        S::Stopping,
        S::Stopped,
        S::Deleting,
        S::Pausing,
        S::Paused,
        S::Resuming,
        S::Checkpointing,
        S::Suspending,
        S::Suspended,
        S::Restoring,
        S::CleanupRequired,
        S::CleanupFailed,
        S::Quarantined,
        S::Failed,
    ] {
        let rows = [row("control", S::Started), row("subject", state)];
        let plan = plan(&rows, &PlanInputs::default());
        assert!(
            plan.actions.contains(&PlannedAction::Suspend {
                sandbox_id: "control".into(),
            }),
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
                PlannedAction::Suspend { sandbox_id }
                | PlannedAction::PublishDisk { sandbox_id }
                | PlannedAction::Upload { sandbox_id, .. } => sandbox_id == "subject",
            })
            .cloned()
            .collect();
        let (expect_pending, expect_blocked, expect_actions): (Vec<String>, Vec<&str>, Vec<_>) =
            match state {
                S::Started | S::Paused => (
                    vec!["suspend".into()],
                    vec![],
                    vec![PlannedAction::Suspend {
                        sandbox_id: "subject".into(),
                    }],
                ),
                S::Stopped => (
                    vec!["publish_disk".into()],
                    vec![],
                    vec![PlannedAction::PublishDisk {
                        sandbox_id: "subject".into(),
                    }],
                ),
                S::Quarantined => (vec![], vec!["quarantined"], vec![]),
                S::CleanupFailed => (vec![], vec!["cleanup failed"], vec![]),
                // Durable already, or nothing on the node to keep.
                S::Suspended | S::Failed => (vec![], vec![], vec![]),
                other => (vec![format!("settle:{other}")], vec![], vec![]),
            };
        assert_eq!(subject_pending, expect_pending, "{state}: pending");
        assert_eq!(subject_blocked, expect_blocked, "{state}: blocked");
        assert_eq!(subject_actions, expect_actions, "{state}: actions");
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

    member.lifecycle = SandboxState::Stopped;
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
    let done = service.finish_drain(7, &first, DrainScans::default());
    assert!(done.done, "{done:?}");
    assert_eq!(done.sealed_epoch, Some(7));

    let blocked = service.finish_drain(
        7,
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
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!report.done);
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: "sbx-a".into(),
            action: "activation".into(),
        }]
    );

    drop(token);
    let first = service.activation_seal.status();
    assert!(service.finish_drain(7, &first, DrainScans::default()).done);
}

/// A seal that changed between the walk's first read and its end (another
/// call replaced the epoch) is not one continuous seal, and a seal under
/// another epoch is not the call's.
#[tokio::test]
async fn done_needs_one_continuous_seal_under_the_calls_epoch() {
    let (service, first) = sealed_service(7);
    service.activation_seal.seal(&service.db, 8).unwrap();
    assert!(!service.finish_drain(7, &first, DrainScans::default()).done);
    let first = service.activation_seal.status();
    assert!(!service.finish_drain(7, &first, DrainScans::default()).done);
    assert!(service.finish_drain(8, &first, DrainScans::default()).done);
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
    let report = service.finish_drain(7, &first, DrainScans::default());
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
    let report = restarted.finish_drain(7, &first, DrainScans::default());
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

    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!report.done, "{report:?}");
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: "sbx-a".into(),
            action: "upload".into(),
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
    let report = service.finish_drain(7, &first, DrainScans::default());
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
    let suspend = [PlannedAction::Suspend {
        sandbox_id: "a".into(),
    }];

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
    let created = [("a".to_owned(), SandboxState::Created)];
    let restoring = [("a".to_owned(), SandboxState::Restoring)];
    let unchanged_under = |epoch: u64, rows: &[(String, SandboxState)], at: Duration| {
        actions.unchanged_since_bound(epoch, rows, start + at)
    };
    let unchanged = |rows: &[(String, SandboxState)], at: Duration| unchanged_under(7, rows, at);
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
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert_eq!(
            report.pending,
            [DrainPending {
                sandbox_id: id.clone(),
                action: "suspend".into(),
            }],
            "{report:?}"
        );
        service.drain_actions.wait_idle().await;
    }
    let report = service.finish_drain(7, &first, DrainScans::default());
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
        SandboxState::Started
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
    crate::service::activation_support::internal_request(DrainNodeRequest { epoch, cancel })
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
    assert!(!cancelled.done);
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
                },
            ),
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(relayed.sealed_epoch, Some(7));
    service.drain_node(drain_request(8, false)).await.unwrap();
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
    let report = service.finish_drain(7, &first, DrainScans::default());
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
        service.drain_suspend(7, &id).await.unwrap();
        assert_eq!(records(&service), 0, "{state} under {epoch}");
        assert_eq!(service.lookup_sandbox(&id).unwrap().state, state);
    }
    assert_eq!(node_manager.calls(), Vec::<String>::new());

    let conn = crate::db::test_conn();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Started);
    let service = service_with_node_manager(conn, &node_manager);
    service.activation_seal.seal(&service.db, 7).unwrap();
    service.drain_suspend(7, &id).await.unwrap_err();
    assert_eq!(records(&service), 0, "a failed suspend forgets its record");
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Started
    );
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: id,
            action: "suspend".into(),
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
    service.drain_suspend(7, &id).await.unwrap_err();
    assert_ne!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Suspended
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

    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!report.done);
    assert!(report.evacuated.is_empty());
    assert_eq!(
        report.pending,
        [DrainPending {
            sandbox_id: "sbx-a".into(),
            action: "upload".into(),
        }]
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
