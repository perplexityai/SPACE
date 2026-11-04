use pplx_space_common_core::types::{LocalArtifactId, SnapshotId};

use super::*;
use crate::service::recovery_cross_node::{
    insert_restoring_sandbox_row_from_snapshot, transition_restored_sandbox_to_suspended,
};

use pplx_space_common_core::types::{
    DEFAULT_GUEST_USER, SandboxOwner, SandboxOwnerKind, TEMPLATE_MANAGER_SANDBOX_OWNER,
};

fn snapshot_meta_with_sandbox(
    sandbox: Option<crate::snapshot_upload::SnapshotSandbox>,
) -> crate::snapshot_upload::SnapshotMeta {
    crate::snapshot_upload::SnapshotMeta {
        version: 1,
        execution: None,
        sandbox_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        size_bytes: 0,
        rootfs_size_bytes: None,
        created_at: "2026-05-21T00:00:00Z".to_string(),
        source: crate::snapshot_upload::SnapshotSource {
            runtime: pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.to_string(),
            runtime_version: None,
            state_format_version: None,
            template_id: "seed".to_string(),
            content_hash: Some(format!("sha256:{}", "ab".repeat(32))),
            rootfs_size_mib: None,
            rootfs_version: None,
            kernel_version: None,
            kernel_digest: None,
            disk_io_max_operations_per_sec: None,
            disk_io_max_bandwidth_mib_per_sec: None,
        },
        sandbox,
        rootfs_diff_format: None,
        rootfs_send_source_id: None,
        encrypted: false,
        rootfs_encryption_source: None,
        local_artifact_id: None,
        artifact_kind: None,
        artifact_origin: None,
        artifact_snapshot_name: None,
    }
}

fn restore_meta_with_process_env(
    process_env: std::collections::BTreeMap<String, String>,
) -> crate::snapshot_upload::SnapshotMeta {
    snapshot_meta_with_sandbox(Some(crate::snapshot_upload::SnapshotSandbox {
        owner: "user".to_string(),
        snapshots_enabled: true,
        process_env,
        auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
    }))
}

fn restore_source(sandbox_id: &str) -> pplx_space_common_core::types::ExactStorageUri {
    format!("s3://snapshots/suspend/{sandbox_id}/attempt-1?region=us-east-1")
        .parse()
        .unwrap()
}

pub(in crate::service) fn insert_suspended_test_sandbox(conn: &crate::db::Connection, id: &str) {
    let sandbox_id = SandboxId::from_str(id).unwrap();
    let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let user = SandboxOwner::parse("user").unwrap();
    let mut process_env = std::collections::BTreeMap::new();
    process_env.insert("ORIGINAL".to_string(), "sandbox".to_string());

    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id,
            uuid8: &uuid8,
            state: SandboxState::Suspended,
            template: "active-template",
            template_hash: Some("active-hash"),
            user: &user,
            runtime: RuntimeKind::Firecracker.alias(),
            snapshots_enabled: false,
            rootfs_size_mb: Some(2048),
            disk_io_limits: Default::default(),
            pool_template: Some("pool-template"),
            volume_encryption: false,
            process_env: &process_env,
            auto_lifecycle: &pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
}

#[test]
fn recovered_sandbox_row_fields_defaults_missing_owner_block_to_legacy_guest_user() {
    let missing = recovered_sandbox_row_fields(&snapshot_meta_with_sandbox(None)).unwrap();
    assert_eq!(missing.user.as_str(), DEFAULT_GUEST_USER);
    assert!(missing.snapshots_enabled);
    assert!(missing.process_env.is_empty());
    assert_eq!(
        missing.auto_lifecycle,
        pplx_space_common_core::types::AutoLifecyclePolicy::disabled()
    );
}

#[test]
fn recovered_sandbox_row_fields_rejects_invalid_owner() {
    let invalid = match recovered_sandbox_row_fields(&snapshot_meta_with_sandbox(Some(
        crate::snapshot_upload::SnapshotSandbox {
            owner: String::new(),
            snapshots_enabled: true,
            process_env: std::collections::BTreeMap::new(),
            auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
        },
    ))) {
        Ok(_) => panic!("empty sandbox owner must reject"),
        Err(error) => error,
    };
    assert!(
        invalid
            .to_string()
            .contains("invalid sandbox owner in snapshot metadata")
    );
}

#[test]
fn recovered_sandbox_row_fields_preserves_template_manager_owner() {
    let fields = recovered_sandbox_row_fields(&snapshot_meta_with_sandbox(Some(
        crate::snapshot_upload::SnapshotSandbox {
            owner: TEMPLATE_MANAGER_SANDBOX_OWNER.to_string(),
            snapshots_enabled: true,
            process_env: std::collections::BTreeMap::new(),
            auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
        },
    )))
    .unwrap();

    assert_eq!(fields.user.kind(), SandboxOwnerKind::TemplateManager);
}

#[test]
fn cross_node_restore_row_lifecycle_stays_restoring_until_rootfs_stage_succeeds() {
    let conn = crate::db::test_conn();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let restore_source = restore_source(sandbox_id);
    let mut process_env = std::collections::BTreeMap::new();
    process_env.insert("RESTORED_FROM".to_string(), "snapshot".to_string());
    let local_artifact_id = LocalArtifactId::new();
    let mut meta = restore_meta_with_process_env(process_env);
    meta.local_artifact_id = Some(local_artifact_id);
    meta.artifact_kind = Some(pplx_space_common_core::types::ArtifactKind::Full);
    meta.artifact_origin = Some(pplx_space_common_core::types::ArtifactOrigin::Suspend);

    insert_restoring_sandbox_row_from_snapshot(&conn, sandbox_id, &meta, &restore_source, None)
        .unwrap();
    let transfer_scope_id = pplx_space_common_core::types::TransferScopeId::new();
    crate::db::insert_restore_transfer_scope(
        &conn,
        sandbox_id,
        transfer_scope_id,
        None,
        None,
        "/space/cache/.restore-attempt",
    )
    .unwrap();
    assert!(
        !crate::db::retire_rowless_restore_transfer_scope(&conn, sandbox_id, transfer_scope_id)
            .unwrap(),
        "an active restoring row must fence orphan-scope retirement"
    );

    let row = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Restoring);
    let persisted_storage_uri = conn
        .query_row(
            "SELECT restore_storage_uri FROM sandboxes WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap();
    assert_eq!(
        persisted_storage_uri.as_deref(),
        Some(restore_source.as_wire())
    );
    let operation = row
        .restore_operation
        .as_ref()
        .expect("remote metadata staging ownership");
    assert_eq!(
        operation.origin(),
        crate::restore_operation::RestoreOrigin::Remote
    );
    assert_eq!(
        operation.phase(),
        crate::restore_operation::RestorePhase::MetadataStaging
    );
    assert_eq!(operation.remote_source(), Some(&restore_source));
    assert_eq!(row.template, meta.source.template_id);
    assert_eq!(row.template_hash, meta.source.content_hash);
    assert_eq!(row.runtime, meta.source.runtime);
    assert_eq!(
        row.process_env.get("RESTORED_FROM").map(String::as_str),
        Some("snapshot")
    );

    transition_restored_sandbox_to_suspended(&conn, sandbox_id, &meta).unwrap();

    let row = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Suspended);
    assert!(row.restore_operation.is_none());
    assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
    assert_eq!(
        row.current_local_artifact_kind,
        Some(pplx_space_common_core::types::ArtifactKind::Full)
    );
    assert!(
        crate::db::restore_transfer_scope(&conn, sandbox_id)
            .unwrap()
            .is_none(),
        "successful artifact publication must retire its transfer scope"
    );
}

#[test]
fn cross_node_restore_adopts_quarantined_synthetic_orphan_row() {
    let conn = crate::db::test_conn();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    insert_quarantined_orphan(&conn, sandbox_id);
    let mut process_env = std::collections::BTreeMap::new();
    process_env.insert("RESTORED_FROM".to_string(), "snapshot".to_string());
    let meta = restore_meta_with_process_env(process_env);

    insert_restoring_sandbox_row_from_snapshot(
        &conn,
        sandbox_id,
        &meta,
        &restore_source(sandbox_id),
        None,
    )
    .unwrap();

    let row = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Restoring);
    assert_eq!(row.template, meta.source.template_id);
    assert_eq!(row.template_hash, meta.source.content_hash);
    assert_eq!(row.runtime, meta.source.runtime);
    assert!(row.failure_reason.is_none());
    assert_eq!(
        row.process_env.get("RESTORED_FROM").map(String::as_str),
        Some("snapshot")
    );

    transition_restored_sandbox_to_suspended(&conn, sandbox_id, &meta).unwrap();

    let row = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Suspended);
}

#[test]
fn cross_node_restore_inherits_volume_encryption_from_snapshot_meta() {
    // insert_restoring_sandbox_row_from_snapshot maps `volume_encryption:
    // meta.encrypted` — the only carrier of the immutable flag across nodes.
    // A regression that hardcoded or inverted it would restore an encrypted
    // snapshot's sandbox as plaintext (or vice versa), which fails closed at
    // rootfs prep with no earlier signal. Guard both polarities through the
    // real orphan-adoption path.
    for encrypted in [true, false] {
        let conn = crate::db::test_conn();
        let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
        insert_quarantined_orphan(&conn, sandbox_id);
        let mut meta = restore_meta_with_process_env(std::collections::BTreeMap::new());
        meta.encrypted = encrypted;

        insert_restoring_sandbox_row_from_snapshot(
            &conn,
            sandbox_id,
            &meta,
            &restore_source(sandbox_id),
            None,
        )
        .unwrap();

        let row = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
        assert_eq!(
            row.volume_encryption, encrypted,
            "restoring row must inherit volume_encryption from meta.encrypted={encrypted}"
        );
    }
}

#[test]
fn restore_as_new_uses_target_disk_io_assignment() {
    let conn = crate::db::test_conn();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    insert_quarantined_orphan(&conn, sandbox_id);
    let meta = restore_meta_with_process_env(std::collections::BTreeMap::new());
    let target_limits = pplx_space_common_core::types::DiskIoLimits::try_new(5_000, 200).unwrap();

    insert_restoring_sandbox_row_from_snapshot(
        &conn,
        sandbox_id,
        &meta,
        &restore_source(sandbox_id),
        Some(target_limits),
    )
    .unwrap();

    let row = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
    assert_eq!(row.disk_io_limits, target_limits);
}

#[test]
fn cross_node_restore_rejects_existing_non_orphan_row_without_overwrite() {
    let conn = crate::db::test_conn();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    insert_suspended_test_sandbox(&conn, sandbox_id);
    let before = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
    let mut process_env = std::collections::BTreeMap::new();
    process_env.insert("RESTORED_FROM".to_string(), "snapshot".to_string());
    let meta = restore_meta_with_process_env(process_env);

    let error = insert_restoring_sandbox_row_from_snapshot(
        &conn,
        sandbox_id,
        &meta,
        &restore_source(sandbox_id),
        None,
    )
    .expect_err("restore must not overwrite a real sandbox row");

    assert!(error.to_string().contains("persist restoring sandbox row"));
    assert!(
        error
            .to_string()
            .contains("is not a recoverable synthetic orphan")
    );
    let after = crate::db::get_sandbox(&conn, sandbox_id).unwrap().unwrap();
    assert_eq!(after.state, SandboxState::Suspended);
    assert_eq!(after.template, before.template);
    assert_eq!(after.template_hash, before.template_hash);
    assert_eq!(after.runtime, before.runtime);
    assert_eq!(after.rootfs_size_mb, before.rootfs_size_mb);
    assert_eq!(
        after.process_env.get("ORIGINAL").map(String::as_str),
        Some("sandbox")
    );
    assert!(!after.process_env.contains_key("RESTORED_FROM"));
}

#[test]
fn cross_node_restore_rejects_snapshot_metadata_for_another_sandbox() {
    let conn = crate::db::test_conn();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let mut meta = restore_meta_with_process_env(std::collections::BTreeMap::new());
    meta.sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a3".to_string();

    let error = insert_restoring_sandbox_row_from_snapshot(
        &conn,
        sandbox_id,
        &meta,
        &restore_source(sandbox_id),
        None,
    )
    .expect_err("restore metadata for another sandbox must be rejected");

    assert!(error.to_string().contains("names sandbox"));
    assert!(crate::db::get_sandbox(&conn, sandbox_id).unwrap().is_none());
}

#[test]
fn orphan_restore_terminal_outcomes_are_reported_distinctly() {
    let sandbox_id = SandboxId::new();
    let cases = [
        (
            crate::snapshot_restore::RestoreError::QuarantineRecoveryTimedOut {
                sandbox_id,
                timeout: std::time::Duration::from_secs(8 * 60 * 60),
            },
            crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreTimedOut,
        ),
        (
            crate::snapshot_restore::RestoreError::QuarantineRecoveryClaimSuperseded { sandbox_id },
            crate::db::QuarantineRecoveryNoProgressReason::ClaimSuperseded,
        ),
        (
            crate::snapshot_restore::RestoreError::QuarantineRecoveryAbandonedAtShutdown {
                sandbox_id,
            },
            crate::db::QuarantineRecoveryNoProgressReason::AbandonedAtShutdown,
        ),
        // A spent renewal budget is a node-local database failure. Sharing the
        // deferral label with a failed transfer sends an operator to the snapshot's
        // storage while this node's SQLite is the thing refusing writes.
        (
            crate::snapshot_restore::RestoreError::RenewQuarantineRecoveryClaim {
                source: crate::db::Error::no_rows(),
            },
            crate::db::QuarantineRecoveryNoProgressReason::ClaimRenewalFailed,
        ),
        (
            crate::snapshot_restore::RestoreError::SandboxDatabaseMutexPoisoned,
            crate::db::QuarantineRecoveryNoProgressReason::NodeDatabaseFailure,
        ),
        // The deferral is now what is left after every other class is named, rather
        // than the default 24 of 28 variants fell into.
        (
            crate::snapshot_restore::RestoreError::Other("unclassified".to_owned()),
            crate::db::QuarantineRecoveryNoProgressReason::OrphanRestoreDeferred,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(
            quarantine_recovery_no_progress_reason(&error),
            expected,
            "the operation deadline and a superseded claim must be answerable \
             from the metric, not only from logs: {error}"
        );
    }
}

#[test]
fn a_lost_claim_is_reported_without_a_durable_retry_write() {
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    emit_quarantine_recovery_claim_not_acquired(
        crate::db::QuarantineRecoveryNoProgressReason::ClaimPreconditionsChanged,
    );
    emit_quarantine_recovery_claim_not_acquired(
        crate::db::QuarantineRecoveryNoProgressReason::ClaimAcquisitionFailed,
    );

    assert_eq!(
        counters
            .finish()
            .into_iter()
            .map(|counter| (counter.name, counter.tags))
            .collect::<Vec<_>>(),
        [
            (
                "sandbox.worker.quarantine_recovery.no_progress".to_string(),
                vec![
                    "reason:claim_preconditions_changed".to_string(),
                    "outcome:claim_not_acquired".to_string(),
                ],
            ),
            (
                "sandbox.worker.quarantine_recovery.no_progress".to_string(),
                vec![
                    "reason:claim_acquisition_failed".to_string(),
                    "outcome:claim_not_acquired".to_string(),
                ],
            ),
        ]
    );
}

// ---------------------------------------------------------------------------
// Orchestration pass
//
// These drive `adopt_quarantine_recovery_episodes` end to end with the plan
// lookup stubbed, which is the only step that needs a control plane. Before this
// existed the whole pass — claim outcomes, plan dispositions, the no-progress
// classifier — was reachable only from a live cluster, and coverage of the
// changed lines in this file was 11%.
// ---------------------------------------------------------------------------

pub(in crate::service) fn recovery_test_service(
    conn: crate::db::Connection,
) -> SandboxManagerService {
    crate::service::tests::service_tests::service_with_runtime_probe(conn, false)
}

/// Make the shipped orphan-restore driver return through its shutdown arm.
///
/// The service token is distinct from the pass token used by the test helpers, so
/// cancelling it leaves the pass free to claim the episode, reach `RestoreCompleted`,
/// and exercise the post-restore write without waiting for an external transfer.
fn cancel_orphan_restore_operation(service: &SandboxManagerService) {
    service.shutdown_token.cancel();
}

/// The state of one quarantine episode's retry bookkeeping.
fn retry_state(service: &SandboxManagerService, sandbox_id: SandboxId) -> (i64, Option<String>) {
    let db = service.db.lock().unwrap();
    db.query_row(
        "SELECT attempt_count, last_error FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
        crate::db::fixture::params![sandbox_id.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

fn stub_plan(
    resolution: crate::snapshot_restore::QuarantineRecoveryPlanResolution,
) -> impl Fn(
    String,
) -> std::pin::Pin<
    Box<
        dyn Future<
                Output = Result<
                    crate::snapshot_restore::QuarantineRecoveryPlanResolution,
                    crate::snapshot_restore::RestoreError,
                >,
            > + Send,
    >,
> {
    let resolution = std::sync::Arc::new(std::sync::Mutex::new(Some(resolution)));
    move |_| {
        let resolution = std::sync::Arc::clone(&resolution);
        Box::pin(async move {
            Ok(resolution
                .lock()
                .unwrap()
                .take()
                .expect("the stub plan is consumed once per pass"))
        })
    }
}

fn block_at_restore_completion() -> (
    impl crate::service::recovery::RecoveryPassProbe,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
) {
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let probe = move |window| {
        if window != crate::service::recovery::RecoveryPassWindow::RestoreCompleted {
            return;
        }
        entered_tx.send(()).expect("test still waits for restore");
        release_rx
            .lock()
            .expect("mutex poisoned")
            .recv()
            .expect("test releases restore completion");
    };
    (probe, entered_rx, release_tx)
}

// These tests synchronize with a detached Tokio task. Under the workspace-wide
// nextest run the process may not receive another scheduler slot within one
// second, even though the task is making progress normally.
const BACKGROUND_RESTORE_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The last value a gauge was set to during a captured window.
///
/// Gauges are last-write-wins, so the pass's final sample is the one an operator
/// would read; taking the last rather than the first is what makes an assertion
/// about "what this pass reported" correct.
fn gauge_value(
    captured: &[pplx_space_metrics::test_capture::CapturedValue],
    name: &str,
) -> Option<f64> {
    captured
        .iter()
        .rfind(|value| value.name == name)
        .map(|value| value.value)
}

/// Wait until the node-wide orphan-restore capacity is back to `target`.
///
/// Production never joins a dispatched restore, so a test must not make the pass
/// join one either — that shape releases the permit before the pass returns and
/// writes the outcome while the pass is still on the stack, the opposite of what
/// ships on all three counts. The permit is the observable edge instead: the
/// tracked task drops it after persisting its outcome.
///
/// `acquire_many` rather than a poll of `available_permits`: it waits on exactly
/// this condition without spinning a core, and its FIFO queue still completes if
/// the permit is released and immediately retaken — which a poll between two
/// samples would miss. `acquire_many(0)` returns immediately, which is the case
/// where a test deliberately held capacity across the whole pass.
pub(super) async fn wait_for_orphan_restore_capacity(
    service: &SandboxManagerService,
    target: usize,
) {
    let permits = u32::try_from(target).expect("test permit target fits in u32");
    if permits == 0 {
        // No permit was free before the pass, so the pass cannot have dispatched
        // against this semaphore and there is no release to wait for. Returning
        // here keeps that case visible to a reader rather than leaving it implicit
        // in `acquire_many(0)` resolving immediately, which reads as a wait that
        // happened and asserted something.
        return;
    }
    let acquired = tokio::time::timeout(
        BACKGROUND_RESTORE_TEST_TIMEOUT,
        service
            .quarantine_recovery_restore_sem
            .acquire_many(permits),
    )
    .await
    .expect("a dispatched orphan restore must release node-wide capacity");
    // A closed semaphore errors immediately, even for zero permits, so there is
    // nothing to wait for — no dispatch can acquire against it. Treated as drained
    // rather than as a failure, so a test that deliberately closes it can still use
    // this wrapper.
    drop(acquired);
}

#[tokio::test]
async fn a_plan_that_reports_no_progress_charges_the_attempt_and_backs_off() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);

    service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/space/cache/firecracker/sandboxes"),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                    crate::db::QuarantineRecoveryNoProgressReason::PlanIneligible,
                ),
            ),
        )
        .await;

    // The pass must leave durable evidence: the attempt is charged and the reason
    // recorded, which is what stops the next tick re-querying immediately.
    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 1);
    assert_eq!(last_error.as_deref(), Some("plan_ineligible"));
    assert!(
        crate::db::scan_quarantine_recovery_candidates(&service.db.lock().unwrap())
            .unwrap()
            .candidates
            .is_empty(),
        "a charged episode is not due again in the same breath"
    );
}

#[tokio::test]
async fn a_plan_for_a_different_reason_is_recorded_as_a_local_mismatch() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);

    let plan = crate::snapshot_restore::QuarantineRecoveryPlan {
        recovery_id: pplx_space_common_core::types::SandboxCleanupEpisodeId::new(),
        snapshot_id: SnapshotId::new(),
        storage_uri: "s3://snapshots/suspend/x?region=us-east-1".to_string(),
        local_artifact_id: None,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration::new(7).unwrap(),
        // The central plan disagrees with the local episode's reason code.
        reason_code:
            pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed,
    };

    service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/space/cache/firecracker/sandboxes"),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
        )
        .await;

    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 1);
    assert_eq!(last_error.as_deref(), Some("local_reason_mismatch"));
}

#[tokio::test]
async fn a_failed_plan_lookup_is_recorded_rather_than_retried_immediately() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);

    service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/space/cache/firecracker/sandboxes"),
            |_| {
                Box::pin(async {
                    Err(
                        crate::snapshot_restore::RestoreError::ClusterApiUnconfigured(
                            "no control plane in this test".to_string(),
                        ),
                    )
                })
            },
        )
        .await;

    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 1);
    assert_eq!(last_error.as_deref(), Some("plan_lookup_failed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_orphan_restore_runs_outside_the_inventory_pass_under_the_node_wide_bound() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = recovery_test_service(conn);
    // The dispatched restore's own vol-manager transfer is not what this asserts
    // — the claim, the permit and the pass's early return are — and against an
    // absent socket that transfer spends a whole ~9.5 s connect budget before
    // the probe below is reached. The shutdown arm returns it immediately.
    cancel_orphan_restore_operation(&service);
    let (probe, restore_completed, release_restore) = block_at_restore_completion();

    service
        .adopt_quarantine_recovery_episodes_with_shutdown(
            &mut crate::db::PeriodicQuarantineRecoveryScan::default(),
            std::path::Path::new("/nonexistent"),
            &tokio_util::sync::CancellationToken::new(),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan_for(
                    None,
                    reason_code,
                )),
            ),
            probe,
        )
        .await;
    restore_completed
        .recv_timeout(BACKGROUND_RESTORE_TEST_TIMEOUT)
        .expect("the detached restore reaches outcome persistence");

    assert_eq!(
        service.quarantine_recovery_restore_sem.available_permits(),
        0,
        "the inventory pass must return while the tracked restore still owns the sole node permit"
    );
    assert_eq!(
        retry_state(&service, sandbox_id),
        (1, None),
        "a dispatched operation keeps its current claim rather than being written up as deferred"
    );

    assert!(
        crate::db::scan_quarantine_recovery_candidates(&service.db.lock().unwrap())
            .unwrap()
            .candidates
            .is_empty(),
        "the live claim must keep the detached operation out of the next pass"
    );
    {
        let db = service.db.lock().unwrap();
        db.execute(
            "UPDATE sandbox_quarantine_state
                SET claim_expires_at = 0,
                    next_action_after = 0
              WHERE sandbox_id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
        )
        .unwrap();
    }
    let second_pass_lookups = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lookup_count = std::sync::Arc::clone(&second_pass_lookups);
    service
        .adopt_quarantine_recovery_episodes_with_shutdown(
            &mut crate::db::PeriodicQuarantineRecoveryScan::default(),
            std::path::Path::new("/nonexistent"),
            &tokio_util::sync::CancellationToken::new(),
            move |_| {
                lookup_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async {
                    Ok(
                        crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                            crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                        ),
                    )
                })
            },
            crate::service::recovery::no_pass_probe(),
        )
        .await;
    assert_eq!(
        second_pass_lookups.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the in-flight task's permit must stop a fresh claimant after lease expiry"
    );
    assert_eq!(retry_state(&service, sandbox_id), (1, None));

    release_restore.send(()).unwrap();
    wait_for_orphan_restore_capacity(&service, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_dispatch_defers_a_second_orphan_while_the_first_holds_capacity() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let first_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let second_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    conn.execute(
        "UPDATE sandbox_quarantine_state SET entered_at = '1' WHERE sandbox_id = ?1",
        crate::db::fixture::params![first_id.to_string()],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandbox_quarantine_state SET entered_at = '2' WHERE sandbox_id = ?1",
        crate::db::fixture::params![second_id.to_string()],
    )
    .unwrap();
    let service = recovery_test_service(conn);
    // Same reason as the sibling above: the first orphan's transfer is incidental
    // to the deferral this asserts, and running it against an absent vol-manager
    // socket costs the test the connect budget.
    cancel_orphan_restore_operation(&service);
    let (probe, restore_completed, release_restore) = block_at_restore_completion();
    let expected_first_id = first_id.to_string();
    let lookup_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lookups = Arc::clone(&lookup_count);

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    service
        .adopt_quarantine_recovery_episodes_with_shutdown(
            &mut crate::db::PeriodicQuarantineRecoveryScan::default(),
            Path::new("/nonexistent"),
            &tokio_util::sync::CancellationToken::new(),
            move |sandbox_id| {
                assert_eq!(sandbox_id, expected_first_id);
                lookups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move {
                    Ok(
                        crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(
                            plan_for(None, reason_code),
                        ),
                    )
                })
            },
            probe,
        )
        .await;
    restore_completed
        .recv_timeout(BACKGROUND_RESTORE_TEST_TIMEOUT)
        .expect("the first detached restore reaches outcome persistence");
    let emitted = counters.finish();

    assert_eq!(lookup_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(retry_state(&service, first_id), (1, None));
    assert_eq!(retry_state(&service, second_id), (0, None));
    assert_eq!(
        service.quarantine_recovery_restore_sem.available_permits(),
        0
    );
    assert_eq!(
        emitted
            .iter()
            .filter(|counter| {
                counter.name == "sandbox.worker.quarantine_recovery.no_progress"
                    && counter
                        .tags
                        .iter()
                        .any(|tag| tag.starts_with("reason:orphan_restore_capacity"))
            })
            .count(),
        0,
        "capacity is not an attempt that made no progress, so it must not reach the \
         attempt counter at all: {emitted:?}"
    );

    release_restore.send(()).unwrap();
    wait_for_orphan_restore_capacity(&service, 1).await;
}

#[tokio::test]
async fn a_detached_orphan_restore_task_panic_is_observed() {
    let conn = crate::db::test_conn();
    let service = recovery_test_service(conn);
    let sandbox_id = SandboxId::new();
    let local_quarantine_id = uuid::Uuid::now_v7();
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();

    let failed_task = service.task_tracker.spawn(async {
        panic!("deliberate detached restore panic");
    });
    service
        .supervise_quarantined_orphan_restore_task(sandbox_id, local_quarantine_id, failed_task)
        .await
        .expect("the supervisor itself must not fail");

    let emitted = counters.finish();
    assert!(emitted.iter().any(|counter| {
        counter.name == "sandbox.worker.quarantine_recovery.no_progress"
            && counter
                .tags
                .iter()
                .any(|tag| tag == "reason:orphan_restore_task_failed")
            && counter
                .tags
                .iter()
                .any(|tag| tag == "outcome:background_task_failed")
    }));
}

/// The dispatch path actually wires the supervisor to the restore task.
///
/// `a_detached_orphan_restore_task_panic_is_observed` calls the supervisor
/// directly, so it proves the supervisor works and nothing about whether anything
/// calls it — delete the `supervise_…` line from
/// `dispatch_quarantined_orphan_restore` and that test still passes while a real
/// panicking restore reports nothing at all. This drives the production dispatch
/// and panics the task from inside, at the probe.
///
/// Single-threaded on purpose: both capture seams are thread-local, so on a
/// multi-threaded runtime the supervisor lands on a worker that has neither and
/// the test fails for a harness reason rather than a production one.
#[tokio::test]
async fn the_dispatch_path_supervises_the_task_it_spawns() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    // One short of the bound, so the accepted plan below is all that keeps the
    // next rejection from completing a run that the plan interrupted.
    for _ in 1..crate::db::QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND {
        crate::db::test_fixtures::record_quarantine_recovery_attempt(
            &conn,
            sandbox_id,
            crate::db::QuarantineRecoveryNoProgressReason::PlanIneligible,
        );
    }
    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_after = NULL WHERE sandbox_id = ?1",
        crate::db::fixture::params![sandbox_id.to_string()],
    )
    .unwrap();
    let service = bound_test_service(conn);
    let cache = tempfile::tempdir().unwrap();
    // The panic this test injects fires at the probe, after the restore returns,
    // so how the restore returns is not part of the claim. Taking the shutdown
    // arm skips a ~9.5 s vol-manager connect budget that nothing here reads.
    cancel_orphan_restore_operation(&service);

    let capture = crate::service::tests::service_tests::SandboxEventCapture::default();
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    {
        use tracing_subscriber::layer::SubscriberExt;
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        // The production pass accepts the plan, claims, and dispatches; the
        // task panics before it can record an outcome. The pass waits for the
        // permit the panicking task held to come back.
        service
            .adopt_quarantine_recovery_episodes_with_probe(
                cache.path(),
                stub_plan(
                    crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan_for(
                        None,
                        reason_code,
                    )),
                ),
                |window| {
                    if window == crate::service::recovery::RecoveryPassWindow::RestoreCompleted {
                        panic!("deliberate panic inside the dispatched restore task");
                    }
                },
            )
            .await;
        tokio::time::timeout(BACKGROUND_RESTORE_TEST_TIMEOUT, async {
            while capture.types.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the supervisor must record the failed task");
    }

    let emitted = counters.finish();
    assert!(
        emitted.iter().any(|counter| {
            counter.name == "sandbox.worker.quarantine_recovery.no_progress"
                && counter
                    .tags
                    .iter()
                    .any(|tag| tag == "reason:orphan_restore_task_failed")
        }),
        "a panicking dispatched restore must be observed by the supervisor: {emitted:?}"
    );
    // The id reaches the supervisor separately from the claim, so assert the event
    // names this sandbox rather than merely that some event was emitted.
    let events = capture.events.lock().unwrap().clone();
    // Discriminated on `operation`, not just the type: the restore itself emits
    // `snapshot_restore_failed` from two other sites on this path, and the injected
    // panic fires after it has run, so the type alone does not identify the emitter.
    let restore_failed = events
        .iter()
        .find(|event| {
            event.get("event_type").map(String::as_str) == Some("snapshot_restore_failed")
                && event.get("operation").map(String::as_str)
                    == Some("quarantine_recovery_orphan_restore")
        })
        .expect(
            "the failed task must reach the sandbox timeline, or the episode simply \
             stops for a whole lease with nothing recording why",
        );
    assert_eq!(
        restore_failed.get("sandbox_id").map(String::as_str),
        Some(sandbox_id.to_string().as_str())
    );
    assert_eq!(
        restore_failed.get("reason").map(String::as_str),
        Some("orphan_restore_task_failed")
    );
    assert_eq!(
        restore_failed.get("result").map(String::as_str),
        Some("task_panicked")
    );

    // Nothing recorded the outcome, so the lease is what lapses. The claim
    // already ended the run, so the next rejection starts a new one.
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandbox_quarantine_state
                SET claim_expires_at = '1', next_action_after = NULL
              WHERE sandbox_id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
        )
        .unwrap();
    service
        .adopt_quarantine_recovery_episodes_with(
            cache.path(),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                    crate::db::QuarantineRecoveryNoProgressReason::PlanIneligible,
                ),
            ),
        )
        .await;
    assert_eq!(
        retry_state(&service, sandbox_id),
        (
            i64::from(crate::db::QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND) + 1,
            Some("plan_ineligible".to_string())
        ),
        "an accepted plan must break the run of rejections"
    );
}

/// A staging lock released while the attempt waits is acquired, not refused.
///
/// A refusal is expensive out of proportion to the collision: it charges the episode
/// an attempt and reschedules through `quarantine_recovery_backoff`, so five
/// sub-second collisions take an orphan from a millisecond wait to the 15-minute
/// cap. Momentary holders are ordinary — a concurrent `Start`, a cleanup runner, a
/// delete — so the admission absorbs them instead of escalating.
// Paused time makes the 150 ms hand-off precede the five-second admission
// deadline even when the workspace-wide test process is starved. With wall-clock
// time both timers can become ready in the same delayed scheduler turn, making
// this test observe the timeout branch instead of the ordering it constructed.
#[tokio::test(start_paused = true)]
async fn a_momentarily_busy_staging_tree_does_not_charge_the_episode() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    // After admission, stop at the live-runtime guard before touching the host
    // filesystem or calling volume-manager. Those operations use real I/O and
    // cannot finish reliably against the helper's auto-advancing virtual timeout.
    let service = SandboxManagerService::new_with_options(
        conn,
        crate::service::SandboxManagerServiceOptions {
            sandbox_runtime_live: Arc::new(|_| true),
            ..crate::service::SandboxManagerServiceOptions::for_test()
        },
    );
    let staging_lock = service.auto_lifecycle.staging_lock(&sandbox_id.to_string());
    let staging_guard = staging_lock.lock_owned().await;

    // Released well inside the admission's wait, the way a real hand-off would be.
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        drop(staging_guard);
    });

    service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/nonexistent"),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan_for(
                    None,
                    reason_code,
                )),
            ),
        )
        .await;
    releaser.await.unwrap();

    assert_eq!(
        retry_state(&service, sandbox_id),
        (1, Some("orphan_restore_deferred".to_string())),
        "the attempt must acquire the staging lock and reach the live-runtime guard"
    );
}

// Paused time: the holder never lets go, so the admission's `timeout` can only
// expire, and burning `ORPHAN_STAGING_ADMISSION_WAIT` of real wall clock to watch it
// buys nothing.
#[tokio::test(start_paused = true)]
async fn a_busy_staging_tree_releases_node_capacity_and_backs_off_the_episode() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = recovery_test_service(conn);
    let staging_lock = service.auto_lifecycle.staging_lock(&sandbox_id.to_string());
    let staging_guard = staging_lock.lock_owned().await;

    service
        .adopt_quarantine_recovery_episodes_with_shutdown(
            &mut crate::db::PeriodicQuarantineRecoveryScan::default(),
            Path::new("/nonexistent"),
            &tokio_util::sync::CancellationToken::new(),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan_for(
                    None,
                    reason_code,
                )),
            ),
            crate::service::recovery::no_pass_probe(),
        )
        .await;

    wait_for_orphan_restore_capacity(&service, 1).await;
    assert_eq!(
        retry_state(&service, sandbox_id),
        (1, Some("orphan_restore_local_contention".to_string()))
    );
    drop(staging_guard);
}

/// A closed semaphore is reported as closed, never as busy capacity.
///
/// Three orphans are seeded and all three are walked, so `deferred == 0` here is a
/// statement about classification rather than about an empty cohort: an
/// implementation that folded `Closed` into the busy tally would publish 3 deferred
/// beside 1 closed on a node where nothing was busy at all.
///
/// This deliberately does *not* pin where the closed signal is read from — the walk
/// reaches orphans, so a gauge driven off the loop tally would satisfy it too. That
/// property has its own fixture in
/// `closed_capacity_is_read_off_the_semaphore_not_the_walk`.
#[tokio::test]
async fn closed_capacity_is_not_reported_as_busy_capacity() {
    let artifact_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed;
    let orphan_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let artifact_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, artifact_reason);
    // Orphans behind the closed semaphore. Seeding none — as this test first did —
    // makes the `deferred == 0` assertion below true for the wrong reason: it cannot
    // tell "closed capacity is not reported as busy" from "there was nothing to
    // report either way", which is exactly the conflation it exists to catch.
    for _ in 0..3 {
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, orphan_reason);
    }
    let service = recovery_test_service(conn);
    service.quarantine_recovery_restore_sem.close();

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let values = pplx_space_metrics::test_capture::capture_values_guard();
    service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/nonexistent"),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                    crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                ),
            ),
        )
        .await;
    let counted = counters.finish();
    let emitted = values.finish();
    assert!(
        !counted.iter().any(|counter| {
            counter
                .tags
                .iter()
                .any(|tag| tag.starts_with("reason:orphan_restore_capacity"))
        }),
        "capacity never reaches the attempt counter: {counted:?}"
    );

    assert_eq!(
        gauge_value(
            &emitted,
            "sandbox.worker.quarantine_recovery.orphan_capacity_closed"
        ),
        Some(1.0),
        "a closed semaphore must be reported as closed: {emitted:?}"
    );
    assert_eq!(
        gauge_value(
            &emitted,
            "sandbox.worker.quarantine_recovery.orphan_capacity_deferred"
        ),
        Some(0.0),
        "and the three orphans behind it must not read as busy capacity: a closed \
         semaphore reported as 3 deferred beside 1 closed is the conflation the \
         separate gauge exists to remove: {emitted:?}"
    );
    assert_eq!(
        retry_state(&service, artifact_id),
        (1, Some("plan_not_found".to_string()))
    );

    // And the other direction: an open semaphore must say nothing under this reason.
    let open_conn = crate::db::test_conn();
    crate::db::test_fixtures::setup_quarantine_recovery_episode(&open_conn, artifact_reason);
    let open_service = recovery_test_service(open_conn);
    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    open_service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/nonexistent"),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                    crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                ),
            ),
        )
        .await;
    let emitted = counters.finish();
    assert!(
        !emitted.iter().any(|counter| {
            counter
                .tags
                .iter()
                .any(|tag| tag == "reason:orphan_restore_capacity_closed")
        }),
        "a healthy node must never emit the reason whose doc says it needs an \
         operator; losing the `is_closed()` guard is what makes the monitor \
         permanently firing and therefore useless: {emitted:?}"
    );
}

#[tokio::test]
async fn capacity_deferred_orphans_do_not_consume_attempts_or_admission_budget() {
    let orphan_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let artifact_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed;
    let conn = crate::db::test_conn();
    let orphan_ids = (0..QUARANTINE_RECOVERY_ADMISSIONS_PER_PASS + 1)
        .map(|_| crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, orphan_reason))
        .collect::<Vec<_>>();
    let artifact_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, artifact_reason);
    for sandbox_id in &orphan_ids {
        conn.execute(
            "UPDATE sandbox_quarantine_state SET entered_at = '1' WHERE sandbox_id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
        )
        .unwrap();
    }
    conn.execute(
        "UPDATE sandbox_quarantine_state SET entered_at = '2' WHERE sandbox_id = ?1",
        crate::db::fixture::params![artifact_id.to_string()],
    )
    .unwrap();
    let service = recovery_test_service(conn);
    let held_permit = Arc::clone(&service.quarantine_recovery_restore_sem)
        .try_acquire_owned()
        .unwrap();
    let lookups = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lookup_count = std::sync::Arc::clone(&lookups);
    let expected_artifact_id = artifact_id.to_string();

    let mut cursor = crate::db::PeriodicQuarantineRecoveryScan::default();
    for _ in 0..2 {
        service
            .adopt_quarantine_recovery_episodes_with_shutdown(
                &mut cursor,
                Path::new("/space/cache/firecracker/sandboxes"),
                &CancellationToken::new(),
                |sandbox_id| {
                    assert_eq!(sandbox_id, expected_artifact_id);
                    lookup_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Box::pin(async {
                        Ok(
                            crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                                crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                            ),
                        )
                    })
                },
                no_pass_probe(),
            )
            .await;
    }

    assert_eq!(
        lookups.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "capacity-skipped orphans must not consume the admission that reaches the artifact candidate"
    );
    for sandbox_id in orphan_ids {
        assert_eq!(
            retry_state(&service, sandbox_id),
            (0, None),
            "another sandbox's restore capacity is not an attempt by this orphan"
        );
    }
    assert_eq!(
        retry_state(&service, artifact_id),
        (1, Some("plan_not_found".to_string()))
    );
    drop(held_permit);
    assert_eq!(
        service.quarantine_recovery_restore_sem.available_permits(),
        1
    );
}

/// A refused capacity claim suppresses further capacity attempts in that batch.
/// The next sweep visits these insertion-ordered episodes again; changing a
/// runtime permit during a plan lookup does not change the current batch policy.
#[tokio::test]
async fn capacity_released_mid_pass_does_not_leapfrog_an_older_orphan() {
    let orphan_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let artifact_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed;
    let conn = crate::db::test_conn();
    let first_orphan_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, orphan_reason);
    let artifact_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, artifact_reason);
    let later_orphan_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, orphan_reason);
    for (sandbox_id, entered_at) in [
        (first_orphan_id, "1"),
        (artifact_id, "2"),
        (later_orphan_id, "3"),
    ] {
        conn.execute(
            "UPDATE sandbox_quarantine_state SET entered_at = ?1 WHERE sandbox_id = ?2",
            crate::db::fixture::params![entered_at, sandbox_id.to_string()],
        )
        .unwrap();
    }
    let service = recovery_test_service(conn);
    let held_permit = Arc::new(std::sync::Mutex::new(Some(
        Arc::clone(&service.quarantine_recovery_restore_sem)
            .try_acquire_owned()
            .unwrap(),
    )));
    let permit_to_release = Arc::clone(&held_permit);
    let expected_artifact_id = artifact_id.to_string();
    let lookups = Arc::new(std::sync::Mutex::new(Vec::new()));
    let lookup_log = Arc::clone(&lookups);

    service
        .adopt_quarantine_recovery_episodes_with(Path::new("/nonexistent"), move |sandbox_id| {
            lookup_log.lock().unwrap().push(sandbox_id.clone());
            // The artifact candidate frees node-wide capacity partway through the
            // pass, after the older orphan has already been refused it.
            if sandbox_id == expected_artifact_id {
                drop(permit_to_release.lock().unwrap().take());
            }
            Box::pin(async move {
                Ok(
                    crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                        crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                    ),
                )
            })
        })
        .await;

    assert_eq!(
        lookups.lock().unwrap().clone(),
        vec![artifact_id.to_string()],
        "only the artifact adoption may spend a lookup: the older orphan was refused \
         capacity, and the younger one must not be offered the permit that freed up \
         after that refusal"
    );
    assert_eq!(
        retry_state(&service, first_orphan_id),
        (0, None),
        "the older orphan is deferred, not charged"
    );
    assert_eq!(
        retry_state(&service, later_orphan_id),
        (0, None),
        "and the younger orphan did not take the freed capacity"
    );
    assert_eq!(
        retry_state(&service, artifact_id),
        (1, Some("plan_not_found".to_string()))
    );
    assert_eq!(
        service.quarantine_recovery_restore_sem.available_permits(),
        1,
        "the freed permit is still available for the next pass"
    );

    // Next pass: the oldest orphan is first in scan order and takes it.
    let second_pass_lookups = Arc::new(std::sync::Mutex::new(Vec::new()));
    let lookup_log = Arc::clone(&second_pass_lookups);
    service
        .adopt_quarantine_recovery_episodes_with(Path::new("/nonexistent"), move |sandbox_id| {
            lookup_log.lock().unwrap().push(sandbox_id.clone());
            Box::pin(async move {
                Ok(
                    crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                        crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                    ),
                )
            })
        })
        .await;
    let second = second_pass_lookups.lock().unwrap().clone();
    assert_eq!(
        second.first(),
        Some(&first_orphan_id.to_string()),
        "the next sweep visits the first episode again: {second:?}"
    );
    assert_eq!(
        retry_state(&service, first_orphan_id),
        (1, Some("plan_not_found".to_string())),
        "and it is the one that spent the attempt"
    );
}

#[tokio::test]
async fn orphan_capacity_is_released_when_plan_lookup_fails_before_spawn() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = recovery_test_service(conn);

    service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/space/cache/firecracker/sandboxes"),
            |_| {
                Box::pin(async {
                    Err(
                        crate::snapshot_restore::RestoreError::ClusterApiUnconfigured(
                            "no control plane in this test".to_string(),
                        ),
                    )
                })
            },
        )
        .await;

    assert_eq!(
        service.quarantine_recovery_restore_sem.available_permits(),
        1,
        "an iteration-local permit must drop on every pre-spawn exit"
    );
    assert_eq!(
        retry_state(&service, sandbox_id),
        (1, Some("plan_lookup_failed".to_string()))
    );
}

#[tokio::test]
async fn a_candidate_that_stops_being_admissible_is_reported_without_a_write() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);

    // The cleanup owner arrives after the scan and before the claim, which is the only
    // ordering that reaches the acquire CAS. Inserting it before the pass — as this
    // test used to — excludes the row from the scan instead, so the pass never claims
    // anything and every assertion below holds because nothing ran.
    let db_for_probe = service.db.clone();
    let probe = move |window: crate::service::recovery::RecoveryPassWindow| {
        if window != crate::service::recovery::RecoveryPassWindow::Scanned {
            return;
        }
        let db = db_for_probe.lock().expect("mutex poisoned");
        db.execute(
            "INSERT INTO sandbox_cleanup_episodes (
                 sandbox_id, cleanup_id, completion, keep_filesystem, phase,
                 activation_state, created_at
             ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)",
            crate::db::fixture::params![
                sandbox_id.to_string(),
                pplx_space_common_core::types::SandboxCleanupEpisodeId::new().to_string()
            ],
        )
        .unwrap();
    };

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    service
        .adopt_quarantine_recovery_episodes_with_probe(
            Path::new("/space/cache/firecracker/sandboxes"),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                    crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                ),
            ),
            probe,
        )
        .await;
    let emitted = counters.finish();

    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 0, "a refused claim must not charge an attempt");
    assert_eq!(
        last_error, None,
        "a claim the CAS refused must write nothing durable; the episode belongs to \
         the cleanup owner now, and resetting its retry state would move a row this \
         pass does not hold"
    );
    // The refusal is reported, because a pass that goes silent when every claim loses
    // looks identical to a node with nothing quarantined.
    assert!(
        emitted.iter().any(|counter| {
            counter.name.contains("quarantine_recovery.no_progress")
                && counter
                    .tags
                    .iter()
                    .any(|tag| tag == "outcome:claim_not_acquired")
        }),
        "the refused claim must be reported: {emitted:?}"
    );
}

/// The local artifact identity the seeded candidate carries.
fn candidate_artifact_id(service: &SandboxManagerService) -> LocalArtifactId {
    let db = service.db.lock().unwrap();
    let [candidate] = crate::db::scan_quarantine_recovery_candidates(&db)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    let crate::db::QuarantineRecoveryBranch::AdoptLocalArtifact(adoptable) = candidate.recovery
    else {
        panic!("the artifact-shaped fixture must admit the adoption branch");
    };
    adoptable.local_artifact_id()
}

fn plan_for(
    local_artifact_id: Option<LocalArtifactId>,
    reason_code: pplx_space_common_core::types::SandboxQuarantineReasonCode,
) -> crate::snapshot_restore::QuarantineRecoveryPlan {
    crate::snapshot_restore::QuarantineRecoveryPlan {
        recovery_id: pplx_space_common_core::types::SandboxCleanupEpisodeId::new(),
        snapshot_id: SnapshotId::new(),
        storage_uri: "s3://snapshots/suspend/sandbox/snap?region=us-east-1".to_string(),
        local_artifact_id,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration::new(7).unwrap(),
        reason_code,
    }
}

#[tokio::test]
async fn a_plan_naming_a_different_artifact_retains_the_quarantine() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);
    // The central plan points at an artifact this node no longer holds, so the
    // recovery must not act: adopting would fence the cleanup against evidence
    // that has already been replaced.
    let plan = plan_for(
        Some(LocalArtifactId::new()),
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );

    service
        .adopt_quarantine_recovery_episodes_with(
            Path::new("/space/cache/firecracker/sandboxes"),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
        )
        .await;

    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 1);
    assert_eq!(last_error.as_deref(), Some("plan_artifact_mismatch"));
}

#[tokio::test]
async fn an_unreadable_local_artifact_retains_the_quarantine() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);
    let artifact_id = candidate_artifact_id(&service);
    // The plan agrees with the local row, so the pass proceeds to inspect the
    // snapshot on disk — and there is none, because this cache directory is empty.
    // A recovery that cannot read its own evidence must retain the quarantine
    // rather than adopt on the strength of a database row alone.
    let empty_cache = tempfile::tempdir().unwrap();
    let plan = plan_for(
        Some(artifact_id),
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );

    service
        .adopt_quarantine_recovery_episodes_with(
            empty_cache.path(),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
        )
        .await;

    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 1);
    assert_eq!(
        last_error.as_deref(),
        Some("local_artifact_inspection_failed"),
        "an unreadable artifact is an inspection failure, not a silent adoption"
    );
}

/// Build a suspend snapshot on disk that the production validator accepts.
///
/// Not a mock: the real validator reads these files. It requires the metadata's
/// sandbox identity to match, the artifact to be a full suspend artifact, every
/// file named by the artifact set to exist as a regular file, and `size_bytes` to
/// equal their summed length — so the fixture writes real bytes and computes the
/// total rather than declaring one.
fn write_recoverable_suspend_snapshot(
    cache_dir: &Path,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
) {
    let snapshot_dir = cache_dir.join(sandbox_id.to_string()).join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();

    // The unencrypted full-format artifact set.
    let artifacts = [("vmstate", 64_usize), ("memory", 128), ("rootfs.ext4", 256)];
    let mut size_bytes = 0_u64;
    for (name, len) in artifacts {
        std::fs::write(snapshot_dir.join(name), vec![0_u8; len]).unwrap();
        size_bytes += len as u64;
    }

    let meta: crate::snapshot_upload::SnapshotMeta = serde_json::from_value(serde_json::json!({
        "version": 1,
        "sandbox_id": sandbox_id.to_string(),
        "size_bytes": size_bytes,
        "created_at": "2026-08-19T00:00:00Z",
        "artifact_kind": "full",
        "artifact_origin": "suspend",
        "local_artifact_id": local_artifact_id.to_string(),
        "source": {
            "runtime": "fc",
            "template_id": "seed"
        }
    }))
    .unwrap();
    std::fs::write(
        snapshot_dir.join("snapshot.json"),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn a_snapshot_on_disk_naming_a_different_artifact_retains_the_quarantine() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);
    let artifact_id = candidate_artifact_id(&service);

    // The plan agrees with the local row, so this is not the control plane
    // disagreeing with the node: the snapshot the node itself holds reports a
    // different artifact than the episode was quarantined against. It reads as a
    // separate failure and points an operator at the node's cache rather than at
    // the plan, so it must not share `plan_artifact_mismatch`'s reason code.
    let cache = tempfile::tempdir().unwrap();
    write_recoverable_suspend_snapshot(cache.path(), sandbox_id, LocalArtifactId::new());
    let plan = plan_for(
        Some(artifact_id),
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );

    service
        .adopt_quarantine_recovery_episodes_with(
            cache.path(),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
        )
        .await;

    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 1);
    assert_eq!(last_error.as_deref(), Some("local_artifact_mismatch"));
    // Nothing was adopted: the episode is retained, not handed to cleanup.
    let db = service.db.lock().unwrap();
    assert!(
        crate::db::cleanup_episode(&db, &sandbox_id.to_string())
            .unwrap()
            .is_none(),
        "a disk-side artifact mismatch must not create a cleanup episode"
    );
}

#[tokio::test]
async fn a_validated_artifact_is_adopted_into_durable_cleanup() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = recovery_test_service(conn);
    let artifact_id = candidate_artifact_id(&service);

    let cache = tempfile::tempdir().unwrap();
    write_recoverable_suspend_snapshot(cache.path(), sandbox_id, artifact_id);
    let plan = plan_for(
        Some(artifact_id),
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    service
        .adopt_quarantine_recovery_episodes_with(
            cache.path(),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
        )
        .await;
    let emitted = counters.finish();

    // This is the claim the whole feature exists to make, and nothing asserted it
    // before: a quarantined sandbox with valid evidence actually recovers.
    {
        let db = service.db.lock().unwrap();
        let state: String = db
            .query_row(
                "SELECT state FROM sandboxes WHERE id = ?1",
                crate::db::fixture::params![sandbox_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            state, "cleanup_required",
            "the recovered sandbox must be handed to durable cleanup"
        );
        let episode = crate::db::cleanup_episode(&db, &sandbox_id.to_string())
            .unwrap()
            .expect("adoption must create the durable cleanup episode");
        assert_eq!(
            episode
                .quarantine_recovery
                .map(|fence| fence.local_artifact_id),
            Some(artifact_id),
            "the cleanup episode must carry the artifact identity it was fenced against"
        );
        // The lease is released so no later pass acts under a spent claim.
        let (token, expires): (Option<String>, Option<String>) = db
            .query_row(
                "SELECT claim_token, claim_expires_at FROM sandbox_quarantine_state
                  WHERE sandbox_id = ?1",
                crate::db::fixture::params![sandbox_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((token, expires), (None, None));
        // The recovery is only visible to the control plane once the row is queued
        // for publication. An adoption that commits without this leaves the central
        // view stale indefinitely, which is exactly what the removed adoption
        // timeout could cause.
        let queued: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM node_dirty_refs WHERE element_value = ?1",
                crate::db::fixture::params![sandbox_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            queued >= 1,
            "the recovered sandbox must be queued for publication"
        );
    }

    // A success that emits no success counter leaves `no_progress` without a
    // denominator, which is how a recovery loop that never recovers looks healthy.
    assert!(
        emitted.iter().any(|counter| counter.name
            == "sandbox.worker.quarantine_recovery.succeeded"
            && counter
                .tags
                .iter()
                .any(|tag| tag == "outcome:cleanup_adopted")),
        "adoption must report a success: {emitted:?}"
    );
    assert!(
        !emitted
            .iter()
            .any(|counter| counter.name.contains("quarantine_recovery.no_progress")),
        "a successful adoption must not also report no progress: {emitted:?}"
    );
}

// Interleaving is covered deterministically, not by a race.
//
// Two nondeterministic race tests were written here and removed. The first
// staggered a re-quarantine against a full pass with `yield_now`; it passed with
// the live-claim bug deliberately reintroduced, because the pass almost always
// completes before another task can land inside it. The second tried to inject the
// re-quarantine at the plan lookup — but that lookup deliberately runs *before*
// the claim is taken, precisely so no lease is held across a control-plane call,
// so it is not inside the operation at all.
//
// Between the claim and the commit there is no seam a test controls, and adding
// one would be production surface existing only for a test. The class is instead
// covered by tests that construct the mid-operation state directly and assert the
// fence holds: `quarantine_recovery_new_episode_preserves_a_live_claim` and
// `..._preserves_a_foreign_owners_live_claim` in `db/tests.rs`. Each of those
// was red before its fix, which a race test never managed to be.

/// A deferred orphan restore must not put a storage URI in the log stream.
///
/// `RestoreError` embeds the raw URI in several variants — `InvalidRestoreSource`
/// carries `uri`, and the two artifact-mismatch variants carry `storage_uri`.
/// Those values are storage connection strings handed down by the control plane,
/// including an org-registerable override, so they can contain endpoint
/// credentials. Rendering the error's `Display` into a warning publishes them.
///
/// The oracle here is the emitted line itself, captured through a real
/// subscriber, rather than a helper's return value: the defect is what reaches
/// the log, so that is what the test reads.
#[tokio::test]
async fn a_deferred_orphan_restore_never_logs_the_storage_uri() {
    #[derive(Clone)]
    struct SharedWriter(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Ok(mut buffer) = self.0.lock() {
                buffer.extend_from_slice(bytes);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = SharedWriter;
        fn make_writer(&'a self) -> SharedWriter {
            self.clone()
        }
    }

    const SECRET: &str = "tOpSeCrEtCrEdEnTiAl";
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let candidate = crate::db::scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let claimed = crate::db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .expect("claiming a fresh episode should succeed");
    let service = recovery_test_service(conn);
    let plan = plan_for(None, reason_code);

    // A URI shaped like a connection string with inline credentials, which is
    // what makes this a disclosure rather than noise.
    let uri = format!("s3://AKIAEXAMPLEKEYID:{SECRET}@storage.example.com/snapshots/x");
    let source = pplx_space_common_core::types::ExactStorageUri::parse("::not-a-uri::")
        .expect_err("a malformed URI should not parse");
    let error = crate::snapshot_restore::RestoreError::InvalidRestoreSource {
        uri: uri.clone(),
        source,
    };
    assert!(
        error.to_string().contains(SECRET),
        "this test is only meaningful while the error's Display carries the URI; \
         if that changes, the hazard is gone and so is the reason for this test"
    );

    let logs = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(SharedWriter(Arc::clone(&logs)))
        .finish();
    {
        use tracing::instrument::WithSubscriber;
        service
            .finish_quarantined_orphan_restore(&claimed, &plan, Err(error))
            .with_subscriber(subscriber)
            .await;
    }

    let captured = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert!(
        captured.contains("orphaned restore staging recovery deferred"),
        "the deferral should still be logged; captured: {captured}"
    );
    assert!(
        !captured.contains(SECRET),
        "a storage-URI credential reached the log stream: {captured}"
    );
    assert!(
        !captured.contains("storage.example.com"),
        "the storage endpoint reached the log stream: {captured}"
    );

    // The variant production actually produces, not a hand-picked one. The
    // earlier version of this test constructed `InvalidRestoreSource` directly,
    // which is exactly the variant that cannot fire first on this path: a plan
    // URI is parsed by `SnapshotUploadConfig::from_uri` long before, and that
    // failure arrives as `Other`, wrapping a `StorageUriParseError` whose every
    // variant prints the URI it rejected. So the test passed while the real path
    // leaked.
    // Built the way production builds it, not hand-written. The premise — that a
    // storage-URI parse failure echoes the URI it rejected — is then asserted
    // against the real parser rather than against a literal this test chose, which
    // could not fail.
    let rejected = format!("s3://bucket/snap?endpoint=https://user:{SECRET}@minio.internal");
    let parse_error = pplx_space_common_core::types::StorageUri::parse(&rejected)
        .expect_err("a URI with an unsupported query param should not parse");
    assert!(
        parse_error.to_string().contains(SECRET),
        "premise: the parser's own error echoes the URI, so anything rendering it \
         discloses the credential. If this ever stops holding, the redaction below \
         is no longer load-bearing and this test should say so: {parse_error}"
    );
    let produced = crate::snapshot_restore::RestoreError::Other(format!(
        "parse storage_uri: invalid snapshot storage uri: {parse_error}"
    ));
    assert!(
        produced.redacted_detail().is_none(),
        "an opaque `Other` must never be rendered: the storage-URI parse errors \
         funnel into it, so it cannot be proven free of a credential"
    );

    // And the diagnosis that redaction cost is restored by a typed variant, so
    // the gRPC status survives without the server's message.
    let rpc = crate::snapshot_restore::RestoreError::QuarantineRecoveryPlanRpc {
        code: tonic::Code::Unavailable,
        message: format!("connection reset by {SECRET}"),
    };
    assert_eq!(
        rpc.redacted_detail().as_deref(),
        Some("plan_rpc_status=Unavailable"),
        "the status code is a bounded vocabulary and is what an operator needs"
    );
    assert!(
        !rpc.redacted_detail().unwrap().contains(SECRET),
        "while the server-controlled message stays out of the log"
    );
}

/// Shutdown must schedule the retry one backoff away, not one lease away.
///
/// Taking the claim schedules `next_action_after` at *claim expiry + backoff* —
/// ~16 minutes out — so that an operation which dies without recording anything
/// still paces itself once the lease lapses. Recording a no-progress result
/// rewrites that to *now + backoff* and releases the lease, which is what makes
/// the episode due on the next tick.
///
/// So the durable write is the fast path, and skipping it for shutdown — as an
/// earlier revision did, on the theory that it avoided charging an attempt —
/// left both gates closed for the full ~16 minutes. It also charged nothing:
/// this write never touches `attempt_count`, which the claim already charged.
///
/// The assertions below are on direction and magnitude against `now`, not on
/// equality between two opaque epochs. The earlier version compared epochs for
/// equality, which cannot tell "sooner" from "later" and so passed while
/// asserting the wrong one.
#[tokio::test]
async fn abandoning_at_shutdown_schedules_the_retry_one_backoff_away() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let candidate = crate::db::scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let claimed = crate::db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .expect("claiming a fresh episode should succeed");
    let (_, attempts_after_claim, scheduled_by_claim, claim_after_claim, _) =
        crate::db::test_fixtures::quarantine_claim_state(&conn, sandbox_id);
    assert_eq!(
        attempts_after_claim, 1,
        "the claim charges the first attempt"
    );
    assert!(claim_after_claim.is_some());

    let service = recovery_test_service(conn);
    let plan = plan_for(None, reason_code);
    service
        .finish_quarantined_orphan_restore(
            &claimed,
            &plan,
            Err(
                crate::snapshot_restore::RestoreError::QuarantineRecoveryAbandonedAtShutdown {
                    sandbox_id,
                },
            ),
        )
        .await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let db = service.db.lock().expect("mutex poisoned");
    let (_, attempts, scheduled, claim_token, claim_expiry) =
        crate::db::test_fixtures::quarantine_claim_state(&db, sandbox_id);
    let scheduled: i64 = scheduled
        .expect("a retry must be scheduled")
        .parse()
        .expect("the retry time is an epoch integer");

    assert!(
        claim_token.is_none() && claim_expiry.is_none(),
        "the lease must be released, or the claim-live gate keeps the episode \
         out of the candidate set for the rest of the lease"
    );
    let lease = crate::db::QUARANTINE_RECOVERY_CLAIM_LEASE.as_secs() as i64;
    let base = crate::db::QUARANTINE_RECOVERY_BACKOFF_BASE.as_secs() as i64;
    assert!(
        scheduled > now && scheduled <= now + 2 * base,
        "the retry must land about one backoff from now (now={now}, \
         scheduled={scheduled}, base={base}) — not one lease away"
    );
    let by_claim: i64 = scheduled_by_claim
        .expect("the claim schedules a retry too")
        .parse()
        .unwrap();
    assert!(
        scheduled < by_claim - lease / 2,
        "and it must be markedly sooner than the claim's expiry-based schedule \
         (claim scheduled {by_claim}, lease {lease}s); skipping this write is \
         what left the episode waiting out the whole lease"
    );
    assert_eq!(
        attempts, attempts_after_claim,
        "recording no progress must not charge a second attempt — the claim \
         already charged one and this statement never writes attempt_count"
    );
}

/// Cancelling mid-pass must stop the *next* candidate, not merely the first.
///
/// The check is per candidate, and only an oracle that cancels from inside the
/// pass can tell that apart from a single check above the loop. An earlier
/// version of this test cancelled before the pass and asserted zero lookups —
/// which a pre-loop `if cancelled { return; }` satisfies identically, leaving the
/// property the comment describes unasserted.
///
/// So: three admissible episodes, and the token is cancelled from within the
/// first plan lookup. Exactly one lookup must happen, and exactly one episode may
/// be claimed.
#[tokio::test]
async fn cancelling_shutdown_mid_pass_stops_the_next_candidate() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let mut episodes = Vec::new();
    for _ in 0..3 {
        episodes.push(crate::db::test_fixtures::setup_quarantine_recovery_episode(
            &conn,
            reason_code,
        ));
    }
    assert_eq!(
        crate::db::scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .len(),
        3,
        "all three episodes should be admissible"
    );

    let service = recovery_test_service(conn);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let looked_up = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&looked_up);
    let cancel_from_inside = shutdown.clone();
    service
        .adopt_quarantine_recovery_episodes_with_shutdown(
            &mut crate::db::PeriodicQuarantineRecoveryScan::default(),
            std::path::Path::new("/nonexistent"),
            &shutdown,
            move |_| {
                // The shutdown lands while the pass is already working, which is
                // the case the per-candidate check exists for.
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                cancel_from_inside.cancel();
                Box::pin(async {
                    Ok(
                        crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                            crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                        ),
                    )
                })
            },
            crate::service::recovery::no_pass_probe(),
        )
        .await;

    assert_eq!(
        looked_up.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "only the candidate whose lookup was in flight costs a lookup; the two behind \
         it must not be admitted. That candidate is then abandoned rather than \
         finished, which is what the zero below asserts — an earlier version of this \
         comment claimed it completed, contradicting its own assertion"
    );
    let db = service.db.lock().expect("mutex poisoned");
    let claimed = episodes
        .iter()
        .filter(|sandbox_id| {
            let (_, attempts, _, _, _) =
                crate::db::test_fixtures::quarantine_claim_state(&db, **sandbox_id);
            attempts > 0
        })
        .count();
    assert_eq!(
        claimed, 0,
        "a token cancelled during the plan lookup must be seen before the claim: \
         charging an attempt and taking a lease this process is about to stop \
         maintaining delays the episode by a lease plus a backoff, and the lookup \
         wrote nothing durable that a claim is needed to preserve"
    );
}

/// A fenced write that missed has to reach the sandbox event timeline.
///
/// The cleanup-contention path emits `lifecycle_transition_cas_lost` for the same
/// class of loss, and for a while this one emitted nothing — so a timeline showed
/// one loss and not the other, and the episode appeared to simply stop. Asserted
/// here rather than assumed: the capture layer exists, and claiming the emission was
/// untestable is what left it unverified.
#[tokio::test]
async fn a_stale_claim_reaches_the_sandbox_event_timeline() {
    use tracing_subscriber::layer::SubscriberExt;

    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let candidate = crate::db::scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let claimed = crate::db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .expect("claiming a fresh episode should succeed");
    // Another claimant takes the episode, so this claim's write will miss.
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_token = ?1 WHERE sandbox_id = ?2",
        crate::db::fixture::params![uuid::Uuid::now_v7().to_string(), sandbox_id.to_string()],
    )
    .unwrap();

    let service = recovery_test_service(conn);
    let capture = crate::service::tests::service_tests::SandboxEventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    service
        .record_quarantine_recovery_no_progress(
            &claimed,
            crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
        )
        .await;
    drop(guard);

    let types = capture.types.lock().unwrap().clone();
    assert!(
        types
            .iter()
            .any(|event_type| event_type == "lifecycle_transition_cas_lost"),
        "a stale claim must be visible on the timeline, not only in a counter: {types:?}"
    );
    let events = capture.events.lock().unwrap().clone();
    let event = events
        .iter()
        .find(|event| {
            event.get("event_type").map(String::as_str) == Some("lifecycle_transition_cas_lost")
        })
        .expect("the captured event must carry its fields");
    assert_eq!(
        event.get("operation").map(String::as_str),
        Some("quarantine_recovery_no_progress"),
        "the operation must distinguish this loss from the cleanup-contention one: {event:?}"
    );
}

/// What another actor does to the episode while the pass is mid-flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PassAdversary {
    /// Another claimant takes the episode.
    RotateClaimToken,
    /// A cleanup owner arrives.
    InsertCleanupEpisode,
    /// The sandbox leaves the states a recovery may act on.
    LeaveActivatableState,
    /// An operator resolves the quarantine outright.
    DeleteQuarantineRow,
    /// The episode is re-marked, advancing its attempt count.
    BumpAttemptCount,
}

impl PassAdversary {
    const ALL: [Self; 5] = [
        Self::RotateClaimToken,
        Self::InsertCleanupEpisode,
        Self::LeaveActivatableState,
        Self::DeleteQuarantineRow,
        Self::BumpAttemptCount,
    ];

    fn apply(self, conn: &crate::db::Connection, sandbox_id: SandboxId, token: uuid::Uuid) {
        let id = sandbox_id.to_string();
        match self {
            Self::RotateClaimToken => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state
                        SET claim_token = ?1,
                            claim_expires_at = CAST(strftime('%s','now') AS INTEGER) + 900
                      WHERE sandbox_id = ?2",
                    crate::db::fixture::params![token.to_string(), id],
                )
                .unwrap();
            }
            Self::InsertCleanupEpisode => {
                conn.execute(
                    "INSERT OR IGNORE INTO sandbox_cleanup_episodes (
                         sandbox_id, cleanup_id, completion, keep_filesystem, phase,
                         activation_state, created_at
                     ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)",
                    crate::db::fixture::params![
                        id,
                        pplx_space_common_core::types::SandboxCleanupEpisodeId::new().to_string()
                    ],
                )
                .unwrap();
            }
            Self::LeaveActivatableState => {
                conn.execute(
                    "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
                    crate::db::fixture::params![id],
                )
                .unwrap();
            }
            Self::DeleteQuarantineRow => {
                conn.execute(
                    "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                    crate::db::fixture::params![id],
                )
                .unwrap();
            }
            Self::BumpAttemptCount => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state
                        SET attempt_count = attempt_count + 7
                      WHERE sandbox_id = ?1",
                    crate::db::fixture::params![id],
                )
                .unwrap();
            }
        }
    }
}

/// Whether a cleanup owner arriving in this window must leave the episode released.
///
/// Once the claim is held and before the adoption commits, an arriving cleanup owner
/// makes the adoption CAS miss, and the pass has to record that through the relaxed
/// release — the fenced no-progress write requires the cleanup episode to be *absent*,
/// so it misses in exactly this situation and leaves the lease sitting until it lapses.
/// The two statements write byte-identical column sets and differ only in their three
/// relaxations, so which one ran is observable solely as write versus miss: asserting
/// the lease came back is what pins the routing choice the pass makes.
fn must_release_on_cleanup_owner(
    fixture: PassFixture,
    window: crate::service::recovery::RecoveryPassWindow,
) -> bool {
    // Both branches, now. This used to exclude the orphan fixture because that branch
    // recorded the outcome through the strict writer, which missed on a cleanup-owned
    // episode and left the lease standing — an asymmetry created by fixing the adoption
    // branch alone. There is one writer now, so the property holds for both.
    let _ = fixture;
    matches!(
        window,
        crate::service::recovery::RecoveryPassWindow::Claimed
            | crate::service::recovery::RecoveryPassWindow::CleanupFenceHeld
            | crate::service::recovery::RecoveryPassWindow::ArtifactInspected
    )
}

/// Whether an actor arriving in this window is still ahead of the pass's durable
/// commit for that candidate.
///
/// `AdoptionAttempted` and `RestoreCompleted` sit *after* the transaction that
/// commits the work, so an episode adopted before an adversary acted is a correct
/// outcome, not a write under a moved fence. Everything earlier precedes the commit,
/// and there nothing may be adopted once the identity has moved.
///
/// `OrphanDispatched` precedes the commit: the restore has only been handed to a
/// task at that point, and every durable write it makes is still fenced on the claim
/// the adversary may have moved.
fn precedes_commit(window: crate::service::recovery::RecoveryPassWindow) -> bool {
    match window {
        crate::service::recovery::RecoveryPassWindow::Scanned
        | crate::service::recovery::RecoveryPassWindow::PlanResolved
        | crate::service::recovery::RecoveryPassWindow::Claimed
        | crate::service::recovery::RecoveryPassWindow::CleanupFenceHeld
        | crate::service::recovery::RecoveryPassWindow::ArtifactInspected
        | crate::service::recovery::RecoveryPassWindow::OrphanDispatched => true,
        crate::service::recovery::RecoveryPassWindow::AdoptionAttempted
        | crate::service::recovery::RecoveryPassWindow::RestoreCompleted => false,
    }
}

/// The two shapes a pass can take, since they run different branches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PassFixture {
    /// A quarantine holding a valid suspend artifact, which the pass adopts into
    /// durable cleanup.
    Artifact,
    /// A rowless orphan, which the pass restores.
    Orphan,
}

impl PassFixture {
    const ALL: [Self; 2] = [Self::Artifact, Self::Orphan];

    fn reason_code(self) -> pplx_space_common_core::types::SandboxQuarantineReasonCode {
        match self {
            Self::Artifact => {
                pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed
            }
            Self::Orphan => {
                pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing
            }
        }
    }

    fn reaches_window(self, window: crate::service::recovery::RecoveryPassWindow) -> bool {
        use crate::service::recovery::RecoveryPassWindow;

        match (self, window) {
            (
                Self::Artifact,
                RecoveryPassWindow::Scanned
                | RecoveryPassWindow::PlanResolved
                | RecoveryPassWindow::Claimed
                | RecoveryPassWindow::CleanupFenceHeld
                | RecoveryPassWindow::ArtifactInspected
                | RecoveryPassWindow::AdoptionAttempted,
            )
            | (
                Self::Orphan,
                RecoveryPassWindow::Scanned
                | RecoveryPassWindow::PlanResolved
                | RecoveryPassWindow::Claimed
                | RecoveryPassWindow::RestoreCompleted
                | RecoveryPassWindow::OrphanDispatched,
            ) => true,
            (
                Self::Artifact,
                RecoveryPassWindow::RestoreCompleted | RecoveryPassWindow::OrphanDispatched,
            )
            | (
                Self::Orphan,
                RecoveryPassWindow::CleanupFenceHeld
                | RecoveryPassWindow::ArtifactInspected
                | RecoveryPassWindow::AdoptionAttempted,
            ) => false,
        }
    }
}

/// Every named window, crossed with every way another actor can move the episode,
/// on every branch of the pass that reaches it.
///
/// This is the answer to a class of finding that kept arriving one instance at a
/// time: a concurrent change in some window the tests could not reach. Rather than
/// arguing each interleaving on the diff, the pass names its windows and this drives
/// an adversary through all of them, asserting what must hold whatever happened.
///
/// The strong assertion is the row comparison. Each adversary that moves one of the
/// seven pinned identity conjuncts must leave the episode untouched from the moment
/// it acted, because every fenced write afterwards has to miss. A relaxation of any
/// conjunct shows up here as a write that landed.
#[tokio::test]
async fn no_interleaving_in_any_named_window_lets_the_pass_write_under_a_moved_fence() {
    let mut windows_exercised = std::collections::BTreeSet::new();
    for fixture in PassFixture::ALL {
        for window in crate::service::recovery::RecoveryPassWindow::ALL {
            if !fixture.reaches_window(window) {
                continue;
            }
            for adversary in PassAdversary::ALL {
                let reason_code = fixture.reason_code();
                let conn = crate::db::test_conn();
                let sandbox_id =
                    crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
                let service = recovery_test_service(conn);
                if fixture == PassFixture::Orphan {
                    cancel_orphan_restore_operation(&service);
                }
                let cache = tempfile::tempdir().unwrap();
                let plan = match fixture {
                    PassFixture::Artifact => {
                        let artifact_id = candidate_artifact_id(&service);
                        write_recoverable_suspend_snapshot(cache.path(), sandbox_id, artifact_id);
                        plan_for(Some(artifact_id), reason_code)
                    }
                    PassFixture::Orphan => plan_for(None, reason_code),
                };

                let foreign_token = uuid::Uuid::now_v7();
                let db_for_probe = service.db.clone();
                let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let fired_for_probe = std::sync::Arc::clone(&fired);
                // The row exactly as the adversary left it. Anything different at the
                // end is a write this pass landed after its fence had moved.
                let after_adversary: std::sync::Arc<std::sync::Mutex<Option<QuarantineRow>>> =
                    std::sync::Arc::new(std::sync::Mutex::new(None));
                let snapshot_slot = std::sync::Arc::clone(&after_adversary);
                // Captured separately, because deleting the quarantine row leaves the
                // join above with nothing to return while the fenced statements that
                // write only `sandboxes` can still land.
                let sandbox_side: std::sync::Arc<std::sync::Mutex<Option<SandboxSideRow>>> =
                    std::sync::Arc::new(std::sync::Mutex::new(None));
                let sandbox_slot = std::sync::Arc::clone(&sandbox_side);
                let probe = move |fired_window: crate::service::recovery::RecoveryPassWindow| {
                    if fired_window != window
                        || fired_for_probe.swap(true, std::sync::atomic::Ordering::SeqCst)
                    {
                        return;
                    }
                    let db = db_for_probe.lock().expect("mutex poisoned");
                    adversary.apply(&db, sandbox_id, foreign_token);
                    *snapshot_slot.lock().unwrap() = quarantine_row(&db, sandbox_id);
                    *sandbox_slot.lock().unwrap() = sandbox_side_row(&db, sandbox_id);
                };

                service
                    .adopt_quarantine_recovery_episodes_with_probe(
                        cache.path(),
                        stub_plan(
                            crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(
                                plan,
                            ),
                        ),
                        probe,
                    )
                    .await;

                assert!(
                    fired.load(std::sync::atomic::Ordering::SeqCst),
                    "{fixture:?} declared {window:?} reachable, but its probe never fired"
                );
                windows_exercised.insert(window);

                let case = format!("{fixture:?} + {window:?} + {adversary:?}");
                let db = service.db.lock().expect("mutex poisoned");
                let state: String = db
                    .query_row(
                        "SELECT state FROM sandboxes WHERE id = ?1",
                        crate::db::fixture::params![sandbox_id.to_string()],
                        |row| row.get(0),
                    )
                    .unwrap();
                let adopted = crate::db::cleanup_episode(&db, &sandbox_id.to_string())
                    .unwrap()
                    .is_some();
                let before = after_adversary.lock().unwrap().clone();
                let after = quarantine_row(&db, sandbox_id);
                match adversary {
                    PassAdversary::RotateClaimToken
                    | PassAdversary::BumpAttemptCount
                    | PassAdversary::DeleteQuarantineRow => {
                        assert_eq!(
                            after, before,
                            "{case}: a fenced write landed after the fence moved"
                        );
                        // The quarantine row can be gone, in which case the comparison
                        // above holds for any fence at all; the sandbox row is what
                        // still shows a write.
                        assert_eq!(
                            sandbox_side_row(&db, sandbox_id),
                            sandbox_side.lock().unwrap().clone(),
                            "{case}: a fenced write reached the sandbox row after the \
                             fence moved"
                        );
                        assert!(
                            !adopted || !precedes_commit(window),
                            "{case}: nothing may be adopted once the claim's identity moved"
                        );
                    }
                    PassAdversary::InsertCleanupEpisode => {
                        assert!(
                            state != "cleanup_required" || !precedes_commit(window),
                            "{case}: the cleanup owner already holds this sandbox"
                        );
                        if must_release_on_cleanup_owner(fixture, window) {
                            let row = after.as_ref().expect("the episode must still exist");
                            assert!(
                                row.claim_token.is_none() && row.claim_expires_at.is_none(),
                                "{case}: the lease must come back. A retained lease means \
                                 the pass recorded this through the fenced no-progress \
                                 write, which requires the cleanup episode to be absent — \
                                 so it missed, and the episode is invisible to the scan \
                                 for the rest of the lease instead of one backoff"
                            );
                            // The adoption branch's reason is pinned to the literal,
                            // because every call site now writes through one statement
                            // and passing a neighbour's reason would blame a busy
                            // cleanup fence for an adoption fence that moved. The
                            // orphan branch reaches the same write from its own
                            // classifier, which reports `claim_superseded` — the claim
                            // check it aborts on carries the strict environmental
                            // guards, so a cleanup owner reads there as a lost claim.
                            // Truthful about something, imprecise about which; that is
                            // a labelling matter and is tracked with the rest of the
                            // inline-restore work, so this asserts only that a reason
                            // was recorded.
                            match fixture {
                                PassFixture::Artifact => assert_eq!(
                                    row.last_error.as_deref(),
                                    Some("local_adoption_fence_changed"),
                                    "{case}: the recorded reason has to be the one this \
                                     call site passes"
                                ),
                                PassFixture::Orphan => assert!(
                                    row.last_error.is_some(),
                                    "{case}: some reason must be recorded"
                                ),
                            }
                        }
                    }
                    PassAdversary::LeaveActivatableState => {
                        assert!(
                            state == "started" || !precedes_commit(window),
                            "{case}: a sandbox that left an activatable state must not be \
                             moved (state={state})"
                        );
                        assert!(
                            !adopted || !precedes_commit(window),
                            "{case}: nothing may be adopted once the sandbox left the state"
                        );
                    }
                }
            }
        }
    }

    for window in crate::service::recovery::RecoveryPassWindow::ALL {
        assert!(
            windows_exercised.contains(&window),
            "{window:?} was never exercised by either fixture, so no interleaving in it \
             was tested: {windows_exercised:?}"
        );
    }
}

/// The row fields every fenced write touches.
#[derive(Clone, Debug, PartialEq, Eq)]
struct QuarantineRow {
    claim_token: Option<String>,
    claim_expires_at: Option<String>,
    attempt_count: i64,
    next_action_after: Option<String>,
    last_error: Option<String>,
    next_action_owner: Option<String>,
    /// Read from `sandboxes`, not the quarantine row.
    ///
    /// Two of the fenced statements write only here — the orphan recovery generation
    /// and the cross-node staging failure — so a snapshot of the quarantine row alone
    /// cannot see them. That also left the delete-the-row case asserting nothing: once
    /// the row is gone both snapshots are `None` and compare equal under any fence at
    /// all. A route generation stamped by a spent claimant fences out the generation
    /// its legitimate owner is activating on.
    route_generation: Option<i64>,
    failure_reason: Option<String>,
}

fn quarantine_row(conn: &crate::db::Connection, sandbox_id: SandboxId) -> Option<QuarantineRow> {
    conn.query_row(
        "SELECT qs.claim_token, qs.claim_expires_at, qs.attempt_count, qs.next_action_after,
                qs.last_error, qs.next_action_owner, s.space_api_route_execution_generation,
                s.failure_reason
           FROM sandbox_quarantine_state AS qs
           JOIN sandboxes AS s ON s.id = qs.sandbox_id
          WHERE qs.sandbox_id = ?1",
        crate::db::fixture::params![sandbox_id.to_string()],
        |row| {
            Ok(QuarantineRow {
                claim_token: row.get(0)?,
                claim_expires_at: row.get(1)?,
                attempt_count: row.get(2)?,
                next_action_after: row.get(3)?,
                last_error: row.get(4)?,
                next_action_owner: row.get(5)?,
                route_generation: row.get(6)?,
                failure_reason: row.get(7)?,
            })
        },
    )
    .ok()
}

/// The `sandboxes` columns a fenced statement can write when the quarantine row is
/// already gone, so the delete-the-row case has something to compare.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SandboxSideRow {
    state: String,
    route_generation: Option<i64>,
    failure_reason: Option<String>,
}

fn sandbox_side_row(conn: &crate::db::Connection, sandbox_id: SandboxId) -> Option<SandboxSideRow> {
    conn.query_row(
        "SELECT state, space_api_route_execution_generation, failure_reason
           FROM sandboxes WHERE id = ?1",
        crate::db::fixture::params![sandbox_id.to_string()],
        |row| {
            Ok(SandboxSideRow {
                state: row.get(0)?,
                route_generation: row.get(1)?,
                failure_reason: row.get(2)?,
            })
        },
    )
    .ok()
}

/// The control for the matrix above.
///
/// Every assertion there is of the form "this did not happen", and all of them hold
/// trivially if the pass never gets as far as adopting. This proves the same fixture
/// does adopt when nobody interferes, so the matrix is measuring interference rather
/// than a pass that never ran.
#[tokio::test]
async fn the_interleaving_matrix_fixture_adopts_when_nobody_interferes() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = recovery_test_service(conn);
    let artifact_id = candidate_artifact_id(&service);
    let cache = tempfile::tempdir().unwrap();
    write_recoverable_suspend_snapshot(cache.path(), sandbox_id, artifact_id);
    let plan = plan_for(Some(artifact_id), reason_code);

    // Records rather than interferes, so this also proves every named window is
    // reached by a pass that runs to completion.
    let recorder = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder_for_probe = std::sync::Arc::clone(&recorder);
    let probe = move |window: crate::service::recovery::RecoveryPassWindow| {
        recorder_for_probe.lock().unwrap().push(window);
    };
    service
        .adopt_quarantine_recovery_episodes_with_probe(
            cache.path(),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
            probe,
        )
        .await;
    let windows_seen = recorder.lock().unwrap().clone();

    let db = service.db.lock().expect("mutex poisoned");
    let state: String = db
        .query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "cleanup_required",
        "the matrix fixture must reach adoption when no adversary interferes, or every \
         \"this did not happen\" assertion in the matrix is vacuous"
    );
    // The adoption branch's windows, which is every window except the restore one —
    // this fixture adopts and never runs a restore. `the_orphan_branch_reaches_the_
    // restore_window` covers that one, and the matrix asserts coverage across both.
    for window in [
        crate::service::recovery::RecoveryPassWindow::Scanned,
        crate::service::recovery::RecoveryPassWindow::PlanResolved,
        crate::service::recovery::RecoveryPassWindow::Claimed,
        crate::service::recovery::RecoveryPassWindow::CleanupFenceHeld,
        crate::service::recovery::RecoveryPassWindow::ArtifactInspected,
    ] {
        assert!(
            windows_seen.contains(&window),
            "{window:?} is named but never reached on the branch that adopts, so the \
             matrix cannot exercise it there: {windows_seen:?}"
        );
    }
}

/// The orphan-restore branch reaches its window, so the matrix can drive into it.
///
/// The artifact-shaped fixture goes down the adoption branch and never runs a
/// restore, so without this the `RestoreCompleted` window would be named, hooked,
/// and never exercised.
#[tokio::test]
async fn the_orphan_branch_reaches_the_restore_window() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = recovery_test_service(conn);
    cancel_orphan_restore_operation(&service);
    let cache = tempfile::tempdir().unwrap();
    let plan = plan_for(None, reason_code);

    let recorder = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder_for_probe = std::sync::Arc::clone(&recorder);
    let probe = move |window: crate::service::recovery::RecoveryPassWindow| {
        recorder_for_probe.lock().unwrap().push(window);
    };
    service
        .adopt_quarantine_recovery_episodes_with_probe(
            cache.path(),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
            probe,
        )
        .await;

    let seen = recorder.lock().unwrap().clone();
    assert!(
        seen.contains(&crate::service::recovery::RecoveryPassWindow::RestoreCompleted),
        "the orphan branch must reach the restore window: {seen:?}"
    );
    let _ = sandbox_id;
}

/// Every await in the pass body is either an interleaving window or classified as not
/// one.
///
/// Scope, stated because the name reads wider than it is: this audits the bodies of
/// `adopt_quarantine_recovery_episodes_with_shutdown` in `recovery.rs` plus
/// `dispatch_quarantined_orphan_restore` and
/// `supervise_quarantined_orphan_restore_task` in `recovery/orphan_dispatch.rs` —
/// not the nested cross-node restore implementation. The orphan restore contains many more awaits — two three-hour
/// transfer waits, a staging-lock drop and reacquire, the rootfs pull — none of which
/// is a window in the inventory pass. What covers its fenced writes is the
/// per-conjunct matrix in `db/tests.rs`, which exercises every statement under every
/// conjunct regardless of which call path reaches it, plus
/// `a_claim_lost_during_the_orphan_restore_is_not_written_up` for the gap between the
/// restore returning and its outcome being recorded. An actor arriving *inside* the
/// restore is not covered by any of them.
///
/// The windows were enumerated by reading the pass, which is the same hand-kept-set
/// mistake as every other gap on this change: add an await, and the matrix keeps
/// passing while a new interleaving goes untested. So the set is derived from the
/// source instead. An await — or a statement that spawns a detached task, which
/// forks the pass without suspending it — is accounted for when a probe hook follows
/// it, or when it matches one of the classifications below. A new one is neither, so
/// it fails this test until it is classified.
#[test]
fn every_await_in_the_pass_is_a_window_or_classified_as_not_one() {
    /// An await that cannot be an interleaving window in the pass, and why.
    ///
    /// Each marker has to select the await it exempts and nothing else. A marker
    /// broad enough to match any statement this loop looks at — `.await` itself is
    /// the obvious one — silently exempts every future await, which is the exact
    /// mutation class the rest of this test exists to catch. The two assertions
    /// below are what make that fail loudly instead: a marker matching nothing is
    /// dead, and a marker that also matches a probe-hooked await is broad enough to
    /// swallow a real window.
    const NOT_A_PASS_WINDOW: [(&str, &str, &str); 5] = [
        // Terminal for the candidate being handled: the pass writes the outcome and
        // moves on, and the next candidate re-validates every conjunct when it
        // claims. An interleaving during one of these can only affect a later
        // candidate, which the acquire CAS already covers.
        (
            "",
            "record_quarantine_recovery_no_progress",
            "terminal write for this candidate",
        ),
        (
            "",
            "finish_quarantined_orphan_restore",
            "terminal write for this candidate",
        ),
        // The supervisor joins the tracked restore from a third task, after the pass
        // has already returned. There is no pass left to interleave with, and it
        // writes nothing durable by construction.
        (
            "supervise_quarantined_orphan_restore_task",
            "= task.await",
            "the detached-task join, which happens after the pass returned",
        ),
        // The two spawn sites are told apart by the function they are in, not by
        // their text: one line is a substring of the other, so a text-only marker
        // for the supervisor would silently also exempt the restore fork — which is
        // the one statement in this file that must never be exempt by accident.
        (
            "supervise_quarantined_orphan_restore_task",
            "task_tracker.spawn",
            "spawns the supervisor, which joins and logs and writes nothing durable",
        ),
        (
            "dispatch_quarantined_orphan_restore",
            "task_tracker.spawn",
            "the fork itself; its pass-side window is hooked as OrphanDispatched at \
             the call site, where the pass can actually be interleaved with",
        ),
    ];

    // One entry per audited function, named with the file it lives in. The orphan
    // dispatch is a sibling module, so a single `include_str!` would silently stop
    // covering two of the three.
    const AUDITED: [(&str, &str, &[&str]); 2] = [
        (
            "recovery.rs",
            include_str!("../recovery.rs"),
            &["async fn adopt_quarantine_recovery_episodes_with_shutdown"],
        ),
        (
            "recovery/orphan_dispatch.rs",
            include_str!("orphan_dispatch.rs"),
            &[
                "fn dispatch_quarantined_orphan_restore",
                "fn supervise_quarantined_orphan_restore_task",
            ],
        ),
    ];

    let mut unaccounted = Vec::new();
    let mut markers_fired = [false; NOT_A_PASS_WINDOW.len()];
    let mut markers_overbroad = [false; NOT_A_PASS_WINDOW.len()];
    let mut hooked_awaits = 0_usize;
    for (file, source, function_names) in AUDITED {
        let lines: Vec<&str> = source.lines().collect();
        for function_name in function_names {
            let start = lines
                .iter()
                .position(|line| line.contains(function_name))
                .unwrap_or_else(|| panic!("{function_name} must be findable by name in {file}"));
            let end = (start..lines.len())
                .find(|index| lines[*index] == "    }")
                .unwrap_or_else(|| {
                    panic!("{function_name} must end at the method's closing brace")
                });

            for index in start..end {
                // Awaits are not the only place another actor can interleave. A
                // statement that spawns a detached task forks the pass without
                // suspending it — the pass keeps running while the task holds a
                // claim, the node-wide permit, and a staging lock — so those sites
                // need a window too. Auditing only `.await` is what let the one
                // statement that makes this pass concurrent go unhooked and
                // unclassified.
                let forks = lines[index].contains("dispatch_quarantined_orphan_restore(")
                    || lines[index].contains("task_tracker.spawn(");
                if !lines[index].contains(".await") && !forks {
                    continue;
                }
                // The hook has to be on the very next line. A wider lookahead lets a newly
                // added await borrow the hook belonging to the one after it, which is exactly
                // how the first version of this guard passed a mutation that added one.
                let hooked = lines
                    .get(index + 1)
                    .is_some_and(|line| line.contains("probe(RecoveryPassWindow::"));
                // Scoped to this await's own statement, not a fixed number of lines back. A
                // twelve-line lookbehind let a new await inherit a neighbour's marker — in a
                // loop body where nearly every line is within twelve of one of these calls,
                // that is every position. The statement starts after the previous line that
                // closed one.
                let statement_start = (start..index)
                    .rev()
                    .find(|candidate| {
                        let trimmed = lines[*candidate].trim_end();
                        trimmed.ends_with(';') || trimmed.ends_with('{') || trimmed.ends_with('}')
                    })
                    .map_or(start, |boundary| boundary + 1);
                let statement = lines[statement_start..=index].join("\n");
                hooked_awaits += usize::from(hooked);
                let mut classified = false;
                for (position, (scope, marker, _reason)) in NOT_A_PASS_WINDOW.iter().enumerate() {
                    if !scope.is_empty() && !function_name.contains(scope) {
                        continue;
                    }
                    if statement.contains(marker) {
                        markers_fired[position] = true;
                        markers_overbroad[position] |= hooked;
                        classified = true;
                    }
                }
                if !hooked && !classified {
                    unaccounted.push(format!(
                        "{file}:{} ({function_name}): {}",
                        index + 1,
                        lines[index].trim()
                    ));
                }
            }
        }
    }
    assert!(
        unaccounted.is_empty(),
        "these awaits in the pass are neither followed by a probe hook nor classified \
         as not-a-window, so an interleaving at them is untested:\n{}",
        unaccounted.join("\n")
    );
    let stale = NOT_A_PASS_WINDOW
        .iter()
        .zip(markers_fired)
        .filter(|(_, fired)| !fired)
        .map(|((scope, marker, reason), _)| format!("{marker} in {scope:?} ({reason})"))
        .collect::<Vec<_>>();
    assert!(
        stale.is_empty(),
        "these classifications matched no await, so they are exempting nothing and \
         would silently start exempting the wrong thing if one moved:\n{}",
        stale.join("\n")
    );
    // The other half of that check, and the one the vacuous `.contains(\".await\")`
    // conjunct would have failed: a marker broad enough to also match an await that
    // already carries a probe hook is broad enough to exempt a real window.
    let overbroad = NOT_A_PASS_WINDOW
        .iter()
        .zip(markers_overbroad)
        .filter(|(_, overbroad)| *overbroad)
        .map(|((scope, marker, reason), _)| format!("{marker} in {scope:?} ({reason})"))
        .collect::<Vec<_>>();
    assert!(
        overbroad.is_empty(),
        "these classifications also match an await that has a probe hook, so they \
         would exempt a real interleaving window:\n{}",
        overbroad.join("\n")
    );
    // Not every window follows an await — several sit after a block that released
    // the database lock — so the count is only asserted as nonzero here;
    // `every_named_pass_window_has_a_hook_in_the_pass` is what pins the full set.
    assert!(
        hooked_awaits > 0,
        "no await in the pass carries a probe hook"
    );
}

/// The no-progress write reports a real loss and stays quiet on a healthy one.
///
/// Recording that a cleanup owner holds the sandbox is a healthy outcome, and
/// reporting it as a lost CAS leaves an operator unable to tell it from the real
/// refusal. There used to be a second statement here for that case; the distinction it
/// drew was between two identical writes, so it is gone and this asserts the property
/// against the one that remains.
#[tokio::test]
async fn the_cleanup_contention_release_reports_only_a_real_loss() {
    use tracing_subscriber::layer::SubscriberExt;

    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    for supersede in [false, true] {
        let conn = crate::db::test_conn();
        let sandbox_id =
            crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
        let candidate = crate::db::scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .into_iter()
            .find(|candidate| candidate.sandbox_id == sandbox_id)
            .expect("the seeded episode should be admissible");
        let claimed = crate::db::claim_quarantine_recovery_candidate(
            &conn,
            &candidate,
            crate::db::PlanRejectionRunEffect::Leaves,
        )
        .unwrap()
        .expect("claiming a fresh episode should succeed");
        if supersede {
            conn.execute(
                "UPDATE sandbox_quarantine_state SET claim_token = ?1 WHERE sandbox_id = ?2",
                crate::db::fixture::params![
                    uuid::Uuid::now_v7().to_string(),
                    sandbox_id.to_string()
                ],
            )
            .unwrap();
        }

        let service = recovery_test_service(conn);
        let capture = crate::service::tests::service_tests::SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        service
            .record_quarantine_recovery_no_progress(
                &claimed,
                crate::db::QuarantineRecoveryNoProgressReason::CleanupFenceBusy,
            )
            .await;
        let emitted = counters.finish();
        drop(guard);

        let types = capture.types.lock().unwrap().clone();
        let lost = types
            .iter()
            .any(|event_type| event_type == "lifecycle_transition_cas_lost");
        let outcome = if supersede {
            "stale_claim"
        } else {
            "retry_scheduled"
        };
        assert_eq!(
            lost, supersede,
            "supersede={supersede}: the event must mark a real loss and nothing else: \
             {types:?}"
        );
        assert!(
            emitted.iter().any(|counter| {
                counter.name.contains("quarantine_recovery.no_progress")
                    && counter
                        .tags
                        .iter()
                        .any(|tag| tag == &format!("outcome:{outcome}"))
            }),
            "supersede={supersede}: the release must report outcome:{outcome}: {emitted:?}"
        );
    }
}

/// Two actors, in two different windows of the same pass.
///
/// The single-adversary matrix leaves the case where the episode moves twice: a
/// claimant takes it, and then a re-mark advances it again while the pass is still
/// working. Each fenced write has to miss from the first move onwards, so the row must
/// match whatever the last actor left — a write that lands between the two moves is
/// invisible to the single-adversary matrix, because there the pass had only one
/// chance to be wrong.
#[tokio::test]
async fn two_actors_in_two_windows_still_leave_every_fenced_write_missing() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
    let windows = crate::service::recovery::RecoveryPassWindow::ALL;
    let mut pairs_exercised = 0_usize;
    for first in windows {
        for second in windows {
            if first == second {
                continue;
            }
            let conn = crate::db::test_conn();
            let sandbox_id =
                crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
            let service = recovery_test_service(conn);
            let artifact_id = candidate_artifact_id(&service);
            let cache = tempfile::tempdir().unwrap();
            write_recoverable_suspend_snapshot(cache.path(), sandbox_id, artifact_id);
            let plan = plan_for(Some(artifact_id), reason_code);

            let foreign_token = uuid::Uuid::now_v7();
            let db_for_probe = service.db.clone();
            // Whatever the most recent actor left behind. Every fenced write from the
            // first move onwards has to miss, so this is what the row must still be.
            let latest: std::sync::Arc<std::sync::Mutex<Option<QuarantineRow>>> =
                std::sync::Arc::new(std::sync::Mutex::new(None));
            let slot = std::sync::Arc::clone(&latest);
            let fired_first = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let fired_second = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let first_flag = std::sync::Arc::clone(&fired_first);
            let second_flag = std::sync::Arc::clone(&fired_second);
            let probe = move |window: crate::service::recovery::RecoveryPassWindow| {
                let adversary = if window == first
                    && !first_flag.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    PassAdversary::InsertCleanupEpisode
                } else if window == second
                    && first_flag.load(std::sync::atomic::Ordering::SeqCst)
                    && !second_flag.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    PassAdversary::BumpAttemptCount
                } else {
                    return;
                };
                let db = db_for_probe.lock().expect("mutex poisoned");
                adversary.apply(&db, sandbox_id, foreign_token);
                *slot.lock().unwrap() = quarantine_row(&db, sandbox_id);
            };

            service
                .adopt_quarantine_recovery_episodes_with_probe(
                    cache.path(),
                    stub_plan(
                        crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan),
                    ),
                    probe,
                )
                .await;

            // Both have to have fired. The first actor only inserts a cleanup episode,
            // which the relaxed release is allowed to write through — the strict rule
            // starts at the second actor, which moves a pinned conjunct.
            if !fired_second.load(std::sync::atomic::Ordering::SeqCst) {
                continue;
            }
            pairs_exercised += 1;
            let case = format!("{first:?} then {second:?}");
            let db = service.db.lock().expect("mutex poisoned");
            assert_eq!(
                quarantine_row(&db, sandbox_id),
                latest.lock().unwrap().clone(),
                "{case}: a fenced write landed after the episode moved"
            );
            let state: String = db
                .query_row(
                    "SELECT state FROM sandboxes WHERE id = ?1",
                    crate::db::fixture::params![sandbox_id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_ne!(
                state, "cleanup_required",
                "{case}: the sandbox must not be adopted once the episode moved"
            );
        }
    }
    assert!(
        pairs_exercised >= 4,
        "only {pairs_exercised} window pairs ran both actors, so this covers far less \
         than it reads as covering"
    );
}

/// The per-candidate check at the top of the loop is what stops the next candidate.
///
/// `cancelling_shutdown_mid_pass_stops_the_next_candidate` cancels from inside the
/// first plan lookup, so the check on the far side of that lookup breaks the loop on
/// the same iteration and the top-of-loop check is never what mattered — delete it
/// and that test still passes. Cancelling once the first candidate is already claimed
/// separates them: the second candidate must be refused before it costs a lookup.
#[tokio::test]
async fn a_token_cancelled_after_a_claim_stops_the_next_candidate_before_its_lookup() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    for _ in 0..3 {
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    }
    let service = recovery_test_service(conn);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let looked_up = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&looked_up);
    let plan = plan_for(None, reason_code);

    let cancel_at_claim = shutdown.clone();
    let probe = move |window: crate::service::recovery::RecoveryPassWindow| {
        if window == crate::service::recovery::RecoveryPassWindow::Claimed {
            cancel_at_claim.cancel();
        }
    };
    service
        .adopt_quarantine_recovery_episodes_with_shutdown(
            &mut crate::db::PeriodicQuarantineRecoveryScan::default(),
            std::path::Path::new("/nonexistent"),
            &shutdown,
            move |_| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let plan = plan.clone();
                Box::pin(async move {
                    Ok(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan))
                })
            },
            probe,
        )
        .await;

    assert_eq!(
        looked_up.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the second candidate must be refused at the top of the loop, before it spends \
         a plan lookup on a pass that is stopping"
    );
}

/// A claim lost during the orphan restore must not be written up afterwards.
///
/// The restore is the pass's long pole — up to the eight-hour operation deadline — so
/// this is the interleaving the lease renewal exists for. The matrix drives it through
/// `RestoreCompleted`, but that window is post-commit for the adoption branch, so this
/// spells the orphan case out: the restore returns, the claim has moved, and
/// `finish_quarantined_orphan_restore`'s write has to miss rather than stamp an outcome
/// on an episode someone else now holds.
#[tokio::test]
async fn a_claim_lost_during_the_orphan_restore_is_not_written_up() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = recovery_test_service(conn);
    cancel_orphan_restore_operation(&service);
    let cache = tempfile::tempdir().unwrap();
    let plan = plan_for(None, reason_code);

    let foreign_token = uuid::Uuid::now_v7();
    let db_for_probe = service.db.clone();
    let after_rotation: std::sync::Arc<std::sync::Mutex<Option<QuarantineRow>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let slot = std::sync::Arc::clone(&after_rotation);
    let probe = move |window: crate::service::recovery::RecoveryPassWindow| {
        if window != crate::service::recovery::RecoveryPassWindow::RestoreCompleted {
            return;
        }
        let db = db_for_probe.lock().expect("mutex poisoned");
        PassAdversary::RotateClaimToken.apply(&db, sandbox_id, foreign_token);
        *slot.lock().unwrap() = quarantine_row(&db, sandbox_id);
    };

    service
        .adopt_quarantine_recovery_episodes_with_probe(
            cache.path(),
            stub_plan(crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan)),
            probe,
        )
        .await;

    let snapshot = after_rotation
        .lock()
        .unwrap()
        .clone()
        .expect("the restore window must have been reached, or this proves nothing");
    let db = service.db.lock().expect("mutex poisoned");
    assert_eq!(
        quarantine_row(&db, sandbox_id),
        Some(snapshot),
        "the restore's outcome was written up under a claim that had moved"
    );
}

/// The closed signal comes off the semaphore, not off the loop's tally.
///
/// A pass whose admission budget goes entirely to artifact adoptions breaks before
/// reaching any orphan, so the loop counts zero closed refusals — and a node with a
/// closed semaphore and a queue behind it is exactly the one that must not go
/// silent. Seeding the admission cap in adoptions, all older than the single orphan,
/// is what makes the distinction observable: the orphan is never walked, so a gauge
/// driven off the tally would read 0 while the semaphore is closed.
#[tokio::test]
async fn closed_capacity_is_read_off_the_semaphore_not_the_walk() {
    let artifact_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed;
    let orphan_reason =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let conn = crate::db::test_conn();
    // Exactly the admission cap, so the walk breaks on the candidate after them.
    let adoptions: Vec<_> = (0..super::QUARANTINE_RECOVERY_ADMISSIONS_PER_PASS)
        .map(|_| {
            crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, artifact_reason)
        })
        .collect();
    let orphan_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, orphan_reason);
    // The orphan was inserted after the adoptions, so it falls on the next page.
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET entered_at = (SELECT MAX(entered_at) + 60 FROM sandbox_quarantine_state)
          WHERE sandbox_id = ?1",
        crate::db::fixture::params![orphan_id.to_string()],
    )
    .unwrap();
    let service = recovery_test_service(conn);
    service.quarantine_recovery_restore_sem.close();

    let values = pplx_space_metrics::test_capture::capture_values_guard();
    // `stub_plan` is one-shot on purpose, and this pass makes one lookup per
    // admitted adoption, so the resolution is rebuilt per call here.
    service
        .adopt_quarantine_recovery_episodes_with(Path::new("/nonexistent"), |_| {
            Box::pin(async {
                Ok(
                    crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
                        crate::db::QuarantineRecoveryNoProgressReason::PlanNotFound,
                    ),
                )
            })
        })
        .await;
    let emitted = values.finish();

    // The premise has to be forced arithmetically, not observed on the orphan: the
    // `Closed` arm writes nothing durable, so `retry_state(orphan) == (0, None)` holds
    // whether or not the orphan was walked. Charging every adoption is what proves the
    // budget was spent, and the pending count is what proves the orphan was still in
    // the cohort waiting — cap admitted out of cap + 1 due leaves exactly the orphan
    // unreached. Without both, a fixture drift that let the orphan through would
    // silently turn this into a copy of its sibling.
    for adoption_id in &adoptions {
        assert_eq!(
            retry_state(&service, *adoption_id),
            (1, Some("plan_not_found".to_string())),
            "every adoption must consume an admission, or the walk does not break \
             where this fixture needs it to"
        );
    }
    assert_eq!(
        crate::db::scan_quarantine_recovery_candidates(&service.db.lock().unwrap())
            .unwrap()
            .pending_count,
        u64::try_from(super::QUARANTINE_RECOVERY_ADMISSIONS_PER_PASS + 1).unwrap(),
        "the orphan must still be pending behind the spent admission budget",
    );
    assert_eq!(
        retry_state(&service, orphan_id),
        (0, None),
        "and it must be untouched — reached or not, the closed arm writes nothing, so \
         this is a corroborating check and not the premise"
    );
    assert_eq!(
        gauge_value(
            &emitted,
            "sandbox.worker.quarantine_recovery.orphan_capacity_closed"
        ),
        Some(1.0),
        "a pass that walks no orphan must still report the closed semaphore; a gauge \
         driven off the loop tally reads 0 here: {emitted:?}"
    );
}

fn insert_quarantined_orphan(conn: &crate::db::Connection, sandbox_id: &str) {
    insert_suspended_test_sandbox(conn, sandbox_id);
    conn.execute(
        "UPDATE sandboxes SET template = ?1, template_hash = NULL, user = ?2,
         pool_template = NULL, process_env_json = '{}' WHERE id = ?3",
        crate::db::fixture::params![
            pplx_space_common_core::types::ORPHANED_SANDBOX_TEMPLATE,
            pplx_space_common_core::types::ORPHANED_SANDBOX_OWNER.as_str(),
            sandbox_id
        ],
    )
    .unwrap();
    crate::db::mark_quarantined(
        conn,
        sandbox_id,
        pplx_space_common_core::types::SandboxQuarantineSource::SbxManager,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
        "unclassified canonical root",
        pplx_space_common_core::types::SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();
}

#[test]
fn cross_node_restore_leaves_failed_synthetic_orphan_untouched() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_quarantined_orphan(&conn, &sandbox_id);
    conn.execute(
        "UPDATE sandboxes SET state = 'failed' WHERE id = ?1",
        [&sandbox_id],
    )
    .unwrap();
    let before = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    let meta = restore_meta_with_process_env(std::collections::BTreeMap::new());
    assert!(
        insert_restoring_sandbox_row_from_snapshot(
            &conn,
            &sandbox_id,
            &meta,
            &restore_source(&sandbox_id),
            None,
        )
        .is_err()
    );
    let after = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    assert_eq!(after.state, SandboxState::Failed);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.template, before.template);
    assert!(after.restore_operation.is_none());
}

// ---------------------------------------------------------------------------
// Cleanup-target discovery.
// ---------------------------------------------------------------------------

fn discover_cleanup_targets(
    db: &crate::db::SharedConnection,
    authority: CleanupRecoveryAuthority,
) -> Vec<CleanupRecoveryTarget> {
    SandboxManagerService::recover_required_cleanup_targets(db, authority).unwrap()
}

/// An activation that was interrupted before it requested its own cleanup is
/// resumed by startup with the activation-specific quarantine reason.
#[test]
fn startup_requests_interrupted_activation_cleanup_with_the_activation_reason() {
    for (kind, entered_from, completion, reason_code) in [
        (
            crate::db::ActivationKind::ColdStart,
            SandboxState::Stopped,
            CleanupCompletion::MarkStopped,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
        ),
        (
            crate::db::ActivationKind::Restore,
            SandboxState::Suspended,
            CleanupCompletion::MarkSuspended,
            SandboxQuarantineReasonCode::RestoreActivationInterrupted,
        ),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        super::super::tests::service_tests::insert_test_sandbox(&conn, &id, entered_from);
        let episode = crate::db::begin_activation(&conn, &id, entered_from, kind, None, None)
            .unwrap()
            .expect("activation arms a cleanup episode");
        let db = crate::db::SharedConnection::new(conn);

        let targets = discover_cleanup_targets(&db, CleanupRecoveryAuthority::Startup);

        let [target] = targets.try_into().unwrap();
        assert_eq!(target.sandbox_id, sandbox_id, "{kind:?}");
        assert_eq!(target.cleanup_id, Some(episode.id), "{kind:?}");
        assert_eq!(target.completion, completion, "{kind:?}");
        assert!(target.keep_filesystem, "{kind:?}");
        let db = db.lock().unwrap();
        let requested = crate::db::cleanup_episode(&db, &id).unwrap().unwrap();
        assert_eq!(requested.id, episode.id, "{kind:?}");
        assert!(requested.requested_at.is_some(), "{kind:?}");
        assert_eq!(
            requested.quarantine_reason_code,
            Some(reason_code),
            "{kind:?}"
        );
        assert_eq!(
            crate::db::get_sandbox(&db, &id).unwrap().unwrap().state,
            SandboxState::CleanupRequired,
            "{kind:?}"
        );
    }
}

/// An armed activation cleanup whose row has already left the activation state
/// no longer describes the row; startup retires it instead of adopting it.
#[test]
fn startup_retires_an_armed_cleanup_whose_row_left_the_activation_state() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    super::super::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    crate::db::begin_activation(
        &conn,
        &id,
        SandboxState::Stopped,
        crate::db::ActivationKind::ColdStart,
        None,
        None,
    )
    .unwrap()
    .expect("activation arms a cleanup episode");
    // A writer that predates armed episodes settled the row without retiring it.
    conn.execute(
        "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
        [&id],
    )
    .unwrap();
    let db = crate::db::SharedConnection::new(conn);

    assert!(discover_cleanup_targets(&db, CleanupRecoveryAuthority::Startup).is_empty());

    let db = db.lock().unwrap();
    assert!(crate::db::cleanup_episode(&db, &id).unwrap().is_none());
    let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(row.cleanup_completion, None);
    assert_eq!(row.cleanup_keep_filesystem, None);
}

/// A start interrupted after it allocated runtime resources has no cleanup
/// intent of its own; startup infers one and persists it before scheduling, so
/// a second restart resumes the same cleanup.
#[test]
fn startup_persists_the_inferred_cleanup_intent_of_an_interrupted_start() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    super::super::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Starting);
    conn.execute(
        "UPDATE sandboxes SET uid = 10000, slot = 0 WHERE id = ?1",
        [&id],
    )
    .unwrap();
    let db = crate::db::SharedConnection::new(conn);

    let [target] = discover_cleanup_targets(&db, CleanupRecoveryAuthority::Startup)
        .try_into()
        .unwrap();

    assert_eq!(target.sandbox_id, sandbox_id);
    assert_eq!(target.completion, CleanupCompletion::MarkStopped);
    assert!(target.keep_filesystem);
    assert_eq!(target.cleanup_id, None);
    let row = crate::db::get_sandbox(&db.lock().unwrap(), &id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::CleanupRequired);
    assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
    assert_eq!(row.cleanup_keep_filesystem, Some(true));
    assert_eq!(
        row.failure_reason.as_deref(),
        Some("cleanup reconciliation required")
    );

    let [again] = discover_cleanup_targets(&db, CleanupRecoveryAuthority::Startup)
        .try_into()
        .unwrap();
    assert_eq!(again.completion, CleanupCompletion::MarkStopped);
    assert!(again.keep_filesystem);
}

/// Periodic discovery persists an inferred intent only if the row still has
/// none; an owner that recorded its own intent after the row was listed keeps it.
#[test]
fn requested_cleanup_discovery_yields_to_an_intent_recorded_after_listing() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    super::super::tests::service_tests::insert_test_sandbox(
        &conn,
        &id,
        SandboxState::CleanupFailed,
    );
    conn.execute(
        "UPDATE sandboxes SET uid = 10000, slot = 0 WHERE id = ?1",
        [&id],
    )
    .unwrap();
    let listed = vec![crate::db::get_sandbox(&conn, &id).unwrap().unwrap()];
    crate::db::set_cleanup_intent(&conn, &id, true, CleanupCompletion::MarkStopped).unwrap();
    let db = crate::db::SharedConnection::new(conn);

    let targets = SandboxManagerService::recover_required_cleanup_targets_from_rows(
        &db,
        listed,
        CleanupRecoveryAuthority::RequestedOnly,
        &|_| true,
    )
    .unwrap();

    assert!(targets.is_empty());
    let row = crate::db::get_sandbox(&db.lock().unwrap(), &id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::CleanupFailed);
    assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
    assert_eq!(row.cleanup_keep_filesystem, Some(true));
}

/// A legacy row whose id is not a sandbox id cannot be addressed by cleanup; it
/// is skipped without hiding the rows discovered after it.
#[test]
fn startup_cleanup_discovery_skips_rows_with_malformed_ids() {
    let conn = crate::db::test_conn();
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, user, created_at, updated_at)
         VALUES ('legacy-row', 'legacy01', 'stopping', 'seed', 'user', '1785456000', '1785456000')",
        [],
    )
    .unwrap();
    let sandbox_id = SandboxId::new();
    super::super::tests::service_tests::insert_test_sandbox(
        &conn,
        &sandbox_id.to_string(),
        SandboxState::Stopping,
    );
    // Discovery lists rows by `created_at`; pin both so the malformed row is
    // listed first regardless of the wall clock.
    conn.execute(
        "UPDATE sandboxes SET created_at = '1785456001' WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let db = crate::db::SharedConnection::new(conn);

    let [target] = discover_cleanup_targets(&db, CleanupRecoveryAuthority::Startup)
        .try_into()
        .unwrap();

    assert_eq!(target.sandbox_id, sandbox_id);
}

// ---------------------------------------------------------------------------
// Quarantine adoption: the cleanup-fence deferral.
// ---------------------------------------------------------------------------

/// A sandbox whose cleanup fence is held elsewhere is not adopted underneath its
/// owner; the attempt is charged so the retry backs off.
#[tokio::test]
async fn a_held_cleanup_fence_defers_adoption_and_charges_the_attempt() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = recovery_test_service(conn);
    let artifact_id = candidate_artifact_id(&service);
    let cache_dir = tempfile::tempdir().unwrap();
    let CleanupLockTryResult::Acquired(_fence) = service.try_cleanup_fence(&sandbox_id.to_string())
    else {
        panic!("the test must hold the cleanup fence");
    };

    service
        .adopt_quarantine_recovery_episodes_with(
            cache_dir.path(),
            stub_plan(
                crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan_for(
                    Some(artifact_id),
                    reason_code,
                )),
            ),
        )
        .await;

    let (attempts, last_error) = retry_state(&service, sandbox_id);
    assert_eq!(attempts, 1);
    assert_eq!(last_error.as_deref(), Some("cleanup_fence_busy"));
    let db = service.db.lock().unwrap();
    assert_eq!(
        crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Quarantined
    );
    assert!(
        crate::db::cleanup_episode(&db, &sandbox_id.to_string())
            .unwrap()
            .is_none(),
        "a deferred adoption must not open a cleanup episode"
    );
}

// ---------------------------------------------------------------------------
// The plan-rejection bound, driven through the production pass. Episodes are
// made due through the retry-time column, the same seam the other pass tests
// use, so no test waits on a backoff.
// ---------------------------------------------------------------------------

type PlanLookup = std::pin::Pin<
    Box<
        dyn Future<
                Output = Result<
                    crate::snapshot_restore::QuarantineRecoveryPlanResolution,
                    crate::snapshot_restore::RestoreError,
                >,
            > + Send,
    >,
>;

/// A plan lookup that records every sandbox it is asked about and answers
/// each with `answer`.
fn recording_plan(
    lookups: &Arc<std::sync::Mutex<Vec<String>>>,
    answer: impl Fn() -> crate::snapshot_restore::QuarantineRecoveryPlanResolution
    + Send
    + Sync
    + 'static,
) -> impl Fn(String) -> PlanLookup {
    let lookups = Arc::clone(lookups);
    let answer = Arc::new(answer);
    move |sandbox_id| {
        lookups.lock().unwrap().push(sandbox_id);
        let answer = Arc::clone(&answer);
        Box::pin(async move { Ok(answer()) })
    }
}

fn plan_rejected() -> crate::snapshot_restore::QuarantineRecoveryPlanResolution {
    crate::snapshot_restore::QuarantineRecoveryPlanResolution::NoProgress(
        crate::db::QuarantineRecoveryNoProgressReason::PlanIneligible,
    )
}

/// One reconciler pass with every episode's retry time reached.
async fn pass_with_every_episode_due(
    service: &SandboxManagerService,
    cache_dir: &Path,
    resolve_plan: impl Fn(String) -> PlanLookup,
) {
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandbox_quarantine_state SET next_action_after = NULL",
            [],
        )
        .unwrap();
    service
        .adopt_quarantine_recovery_episodes_with(cache_dir, resolve_plan)
        .await;
}

/// Every field explicit, so no fixture reads the host's node identity.
fn bound_test_service(conn: crate::db::Connection) -> SandboxManagerService {
    crate::service::tests::service_tests::service_with_runtime_probe(conn, false)
}

fn bound() -> i64 {
    i64::from(crate::db::QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND)
}

#[tokio::test]
async fn plan_rejections_stop_recovery_at_the_bound_and_later_passes_never_look_it_up() {
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let service = bound_test_service(conn);
    let cache = tempfile::tempdir().unwrap();
    let lookups = Arc::new(std::sync::Mutex::new(Vec::new()));

    // The literal bound, so changing the constant is a reviewed change here.
    for pass in 1..=19 {
        pass_with_every_episode_due(
            &service,
            cache.path(),
            recording_plan(&lookups, plan_rejected),
        )
        .await;
        let expected = pass.min(16);
        assert_eq!(
            i64::try_from(lookups.lock().unwrap().len()).unwrap(),
            expected,
            "pass {pass}"
        );
        assert_eq!(retry_state(&service, sandbox_id).0, expected, "pass {pass}");
    }
    assert_eq!(
        service
            .lookup_sandbox(&sandbox_id.to_string())
            .unwrap()
            .state,
        SandboxState::Quarantined,
        "stopping recovery hands the hold over; it does not release it"
    );
}

#[tokio::test]
async fn stopped_holds_take_no_admission_from_a_recoverable_hold() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
    let conn = crate::db::test_conn();
    // More stopped holds than a pass admits, all older than the recoverable one.
    let stopped = (0..QUARANTINE_RECOVERY_ADMISSIONS_PER_PASS + 1)
        .map(|_| crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code))
        .collect::<Vec<_>>();
    let service = bound_test_service(conn);
    let cache = tempfile::tempdir().unwrap();
    let lookups = Arc::new(std::sync::Mutex::new(Vec::new()));
    // A pass admits the eight oldest, so the ninth runs its bound after them.
    for _ in 0..2 * bound() {
        pass_with_every_episode_due(
            &service,
            cache.path(),
            recording_plan(&lookups, plan_rejected),
        )
        .await;
    }
    for sandbox_id in &stopped {
        assert_eq!(retry_state(&service, *sandbox_id).0, bound());
    }

    let recoverable = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &service.db.lock().unwrap(),
        reason_code,
    );
    let lookups = Arc::new(std::sync::Mutex::new(Vec::new()));
    pass_with_every_episode_due(
        &service,
        cache.path(),
        recording_plan(&lookups, plan_rejected),
    )
    .await;

    assert_eq!(*lookups.lock().unwrap(), vec![recoverable.to_string()]);
    assert_eq!(retry_state(&service, recoverable).0, 1);
    for sandbox_id in stopped {
        assert_eq!(retry_state(&service, sandbox_id).0, bound());
    }
}

#[tokio::test]
async fn a_restart_allows_a_stopped_hold_one_more_rejection_and_a_plan_answer_resumes_recovery() {
    let reason_code =
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
    let conn = crate::db::test_conn();
    let sandbox_id =
        crate::db::test_fixtures::setup_quarantine_recovery_episode(&conn, reason_code);
    let service = bound_test_service(conn);
    let cache = tempfile::tempdir().unwrap();
    let lookups = Arc::new(std::sync::Mutex::new(Vec::new()));
    let lookup_count = || i64::try_from(lookups.lock().unwrap().len()).unwrap();
    for _ in 0..bound() {
        pass_with_every_episode_due(
            &service,
            cache.path(),
            recording_plan(&lookups, plan_rejected),
        )
        .await;
    }
    assert_eq!(lookup_count(), bound());
    let mut stopped = bound();

    // Still rejected after the restart: one more rejection, then stopped again.
    assert_eq!(service.rearm_stopped_quarantine_recoveries().unwrap(), 1);
    for _ in 0..3 {
        pass_with_every_episode_due(
            &service,
            cache.path(),
            recording_plan(&lookups, plan_rejected),
        )
        .await;
    }
    stopped += 1;
    assert_eq!(lookup_count(), stopped);

    // The control plane answers with a plan after the next restart. That ends
    // the run, so ordinary recovery resumes with a fresh run of rejections.
    assert_eq!(service.rearm_stopped_quarantine_recoveries().unwrap(), 1);
    pass_with_every_episode_due(
        &service,
        cache.path(),
        recording_plan(&lookups, || {
            crate::snapshot_restore::QuarantineRecoveryPlanResolution::Available(plan_for(
                Some(LocalArtifactId::new()),
                pplx_space_common_core::types::SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
            ))
        }),
    )
    .await;
    assert_eq!(
        retry_state(&service, sandbox_id),
        (stopped + 1, Some("plan_artifact_mismatch".to_string()))
    );
    for _ in 1..bound() {
        pass_with_every_episode_due(
            &service,
            cache.path(),
            recording_plan(&lookups, plan_rejected),
        )
        .await;
    }
    assert_eq!(lookup_count(), stopped + bound());
    assert_eq!(
        service.rearm_stopped_quarantine_recoveries().unwrap(),
        0,
        "a run below the bound is still being retried, not stopped"
    );
}
