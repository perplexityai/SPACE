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
        pending_backups,
        &evacuations.iter().cloned().collect(),
        &running
            .iter()
            .map(|(id, action)| ((*id).to_owned(), *action))
            .collect(),
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
        let plan = plan(&rows, &BTreeMap::new(), &BTreeMap::new(), &BTreeMap::new());
        assert!(plan.vm, "{state}: the started control is a VM");
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
    assert!(running.vm);

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
    assert!(blocked.actions.is_empty() && blocked.pending.is_empty() && blocked.vm);
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
    assert!(plan_durable.pending.is_empty() && !plan_durable.vm);

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
/// it, but its VM keeps the drain from being done. A pending backup whose
/// sandbox has no row is still local-only.
#[test]
fn a_pool_member_and_a_rowless_backup() {
    let mut member = row("a", SandboxState::Started);
    member.pool_member = true;
    let plan_member = plan_one(member, &BTreeMap::new(), &[], &[]);
    assert!(plan_member.actions.is_empty() && plan_member.pending.is_empty());
    assert!(plan_member.vm);

    let rowless = plan(
        &[],
        &pending("gone", FULL_ARTIFACT),
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert_eq!(rowless.pending, [("gone".into(), "upload".into())]);
}

/// The inventory's process, cache and run directory findings, and those
/// scanners' failures, block; everything else is ignored.
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
async fn a_durable_evacuation_is_recorded_and_survives_a_restart() {
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
