//! Service-layer tests for runtime-upgrade retention configuration, startup
//! grace, retry `last_seen_at` refreshes, and busy-lock pruning guards.

use std::time::Duration;

use pplx_space_execution_spec::ExecutionMetadata;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;

use super::{
    DEFAULT_RETENTION, RECOMMENDED_MIN_RETENTION, STARTUP_PRUNE_GRACE, parse_retention_env,
};
use crate::db::runtime_upgrade_retention::{
    RuntimeUpgradeRetentionPruner, backdate_retained_runtime_upgrade_for_test,
};
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::proto::{
    RuntimeUpgradeStageFrom, RuntimeUpgradeStatus, SandboxSubstrateKind, WakeRequest,
    WakeRuntimeUpgrade, WakeStartContext, WakeTargetState,
};
use crate::service::SandboxManagerService;
use crate::service::activation_support::internal_request;
use crate::service::runtime_upgrade::node_effects::tests::{certified_target, fixture};
use crate::service::runtime_upgrade::rpc::tests::{
    POD_UID, abandon, authorization, operation, pod_identity, pod_upgrade, report,
    report_for_attempt, upgrade,
};
use crate::state::SandboxState;

fn with_retention(
    service: &SandboxManagerService,
    retention: Option<Duration>,
) -> SandboxManagerService {
    SandboxManagerService {
        runtime_upgrade_retention: retention,
        ..service.clone()
    }
}

fn backdate(
    service: &SandboxManagerService,
    operation_id: crate::db::RuntimeUpgradeOperationId,
    secs: u64,
) {
    backdate_retained_runtime_upgrade_for_test(
        &service.db.lock().unwrap(),
        operation_id,
        secs,
        Some(secs),
    )
    .unwrap();
}

fn refused_suspended_request(
    sandbox_id: &str,
    target: &ExecutionMetadata,
    checkpoint: LocalArtifactId,
    operation_id: crate::db::RuntimeUpgradeOperationId,
) -> crate::proto::UpgradeRuntimeRequest {
    let mut req = pod_upgrade(sandbox_id, target, operation_id, WakeTargetState::Started);
    req.checkpoint_local_artifact_id = Some(checkpoint.to_string());
    req.authorization.as_mut().unwrap().memory_loss = false;
    req
}

#[test]
fn parse_retention_env_defaults_disables_and_fails_closed_on_invalid_values() {
    for unset in [None, Some(""), Some("   ")] {
        assert_eq!(parse_retention_env(unset), Some(DEFAULT_RETENTION));
    }
    for disabled in ["0", " 0 ", "off", "OFF", "false", "FALSE", "no", "disabled"] {
        assert_eq!(
            parse_retention_env(Some(disabled)),
            None,
            "{disabled:?} must disable pruning"
        );
    }
    assert_eq!(
        parse_retention_env(Some("3600")),
        Some(RECOMMENDED_MIN_RETENTION)
    );
    assert_eq!(
        parse_retention_env(Some("60")),
        Some(Duration::from_secs(60))
    );
    for invalid in ["7d", "-1", "forever", "true", "18446744073709551616"] {
        assert_eq!(
            parse_retention_env(Some(invalid)),
            None,
            "{invalid:?} must fail closed"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn delayed_attempt_retries_extend_idle_retention_while_reads_and_busy_locks_preserve_invariants()
 {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let checkpoint = f.adopt_checkpoint();
    let target = certified_target(&f.source);
    let service = with_retention(&f.service, Some(DEFAULT_RETENTION));

    let retried_op = operation();
    let read_only_op = operation();
    for op in [retried_op, read_only_op] {
        let err = upgrade(
            &service,
            refused_suspended_request(&f.sandbox_id, &target, checkpoint, op),
        )
        .await
        .expect_err("missing memory_loss authorization refuses the attempt");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err}");
    }

    crate::db::delete_sandbox(&service.db.lock().unwrap(), &f.sandbox_id).unwrap();

    let now = u64::try_from(crate::db::now_epoch_i64().unwrap()).unwrap();
    let ten_days_ago = now - 10 * 24 * 60 * 60;
    for op in [retried_op, read_only_op] {
        backdate(&service, op, ten_days_ago);
    }

    // `start_runtime_upgrade_recovery` holds pruning off for `STARTUP_PRUNE_GRACE`
    // so post-restart retries can check in before the first sweep.
    let shutdown = tokio_util::sync::CancellationToken::new();
    let tracker = tokio_util::task::TaskTracker::new();
    service.start_runtime_upgrade_recovery(shutdown.clone(), &tracker);
    tracker.close();

    tokio::time::advance(STARTUP_PRUNE_GRACE - Duration::from_secs(30)).await;
    assert_eq!(
        report_for_attempt(&service, &f.sandbox_id, read_only_op, POD_UID).status,
        RuntimeUpgradeStatus::Refused as i32
    );

    // Delayed retry of `retried_op` after sandbox deletion refuses without
    // staging and refreshes `last_seen_at`, while `GetRuntimeUpgrade` on
    // `read_only_op` stays read-only.
    let mut staged_retry =
        refused_suspended_request(&f.sandbox_id, &target, checkpoint, retried_op);
    staged_retry.stage_from = Some(RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: format!(
            "s3://space-test/suspend/{}/{checkpoint}?region=us-east-1",
            f.sandbox_id
        ),
        suspend_restore_source_node: None,
    });
    let retry_err = upgrade(&service, staged_retry)
        .await
        .expect_err("settled attempt refusal replays after sandbox deletion");
    assert_eq!(
        retry_err.code(),
        tonic::Code::FailedPrecondition,
        "{retry_err}"
    );
    assert_eq!(
        report_for_attempt(&service, &f.sandbox_id, read_only_op, POD_UID).status,
        RuntimeUpgradeStatus::Refused as i32
    );

    // Once `STARTUP_PRUNE_GRACE` elapses, the recovery task prunes `read_only_op`
    // while `retried_op` survives on its refreshed `last_seen_at`.
    tokio::time::advance(Duration::from_secs(90)).await;
    tokio::task::yield_now().await;
    shutdown.cancel();
    tracker.wait().await;
    assert_eq!(
        report_for_attempt(&service, &f.sandbox_id, read_only_op, POD_UID).status,
        RuntimeUpgradeStatus::Absent as i32
    );
    assert_eq!(
        report_for_attempt(&service, &f.sandbox_id, retried_op, POD_UID).status,
        RuntimeUpgradeStatus::Refused as i32
    );

    // Same-sandbox mismatch refreshes `last_seen_at`; cross-sandbox mismatch
    // gets `INVALID_ARGUMENT` without pinning another sandbox's record.
    let mut pruner = RuntimeUpgradeRetentionPruner::default();
    backdate(&service, retried_op, ten_days_ago);
    let same_sandbox_mismatch = upgrade(
        &service,
        pod_upgrade(&f.sandbox_id, &target, retried_op, WakeTargetState::Stopped),
    )
    .await
    .expect_err("reusing operation_id with different target_state is refused");
    assert_eq!(
        same_sandbox_mismatch.code(),
        tonic::Code::InvalidArgument,
        "{same_sandbox_mismatch}"
    );
    assert_eq!(
        service
            .prune_retained_runtime_upgrades(&mut pruner)
            .await
            .unwrap()
            .deleted_attempt_outcomes,
        0
    );

    backdate(&service, retried_op, ten_days_ago);
    let other_sandbox = SandboxId::new().to_string();
    let cross_sandbox_mismatch = upgrade(
        &service,
        pod_upgrade(
            &other_sandbox,
            &target,
            retried_op,
            WakeTargetState::Started,
        ),
    )
    .await
    .expect_err("reusing operation_id for another sandbox is refused");
    assert_eq!(
        cross_sandbox_mismatch.code(),
        tonic::Code::InvalidArgument,
        "{cross_sandbox_mismatch}"
    );

    // An in-flight per-sandbox upgrade lock holds pruning off until released.
    let lock = service.auto_lifecycle.runtime_upgrade_lock(&f.sandbox_id);
    let held = lock.lock_owned().await;
    assert_eq!(
        service
            .prune_retained_runtime_upgrades(&mut pruner)
            .await
            .unwrap()
            .deleted_attempt_outcomes,
        0
    );
    drop(held);

    let pruned = service
        .prune_retained_runtime_upgrades(&mut pruner)
        .await
        .unwrap();
    assert_eq!(pruned.deleted_attempt_outcomes, 1);
    assert_eq!(
        report_for_attempt(&service, &f.sandbox_id, retried_op, POD_UID).status,
        RuntimeUpgradeStatus::Absent as i32
    );

    // After pruning, a delayed retry without `stage_from` cannot run again
    // because the sandbox already left the node before pruning was eligible.
    let post_prune_retry = upgrade(
        &service,
        refused_suspended_request(&f.sandbox_id, &target, checkpoint, retried_op),
    )
    .await
    .expect_err("evicted sandbox without stage_from cannot run after prune");
    assert_eq!(
        post_prune_retry.code(),
        tonic::Code::NotFound,
        "{post_prune_retry}"
    );
    f.assert_no_node_calls();
}

#[tokio::test(start_paused = true)]
async fn late_upgrade_and_wake_calls_extend_abandoned_operation_retention_until_idle() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let service = with_retention(&f.service, Some(DEFAULT_RETENTION));
    let disabled_service = with_retention(&f.service, None);
    let mut pruner = RuntimeUpgradeRetentionPruner::default();

    let operation_id = operation();
    upgrade(
        &service,
        pod_upgrade(
            &f.sandbox_id,
            &f.source,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .expect("already-on-target outcome is recorded with no attempt_outcomes row");
    abandon(&service, &f.sandbox_id, operation_id)
        .await
        .expect("operation is abandoned");

    let now = u64::try_from(crate::db::now_epoch_i64().unwrap()).unwrap();
    let ten_days_ago = now - 10 * 24 * 60 * 60;
    backdate(&service, operation_id, ten_days_ago);

    let other_sandbox = SandboxId::new().to_string();
    let mismatch = upgrade(
        &service,
        pod_upgrade(
            &f.sandbox_id,
            &f.source,
            operation_id,
            WakeTargetState::Started,
        ),
    )
    .await
    .expect_err("parameter mismatch on same sandbox is refused");
    assert_eq!(mismatch.code(), tonic::Code::InvalidArgument, "{mismatch}");

    crate::db::delete_sandbox(&service.db.lock().unwrap(), &f.sandbox_id).unwrap();
    assert_eq!(
        service
            .prune_retained_runtime_upgrades(&mut pruner)
            .await
            .unwrap()
            .deleted_abandoned,
        0
    );

    backdate(&service, operation_id, ten_days_ago);
    let late_upgrade = upgrade(
        &service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .expect_err("abandoned operation refuses late UpgradeRuntime");
    assert_eq!(
        late_upgrade.code(),
        tonic::Code::FailedPrecondition,
        "{late_upgrade}"
    );
    assert_eq!(
        service
            .prune_retained_runtime_upgrades(&mut pruner)
            .await
            .unwrap()
            .deleted_abandoned,
        0
    );

    // Late `Wake` carrying `runtime_upgrade` refuses before staging and
    // refreshes `last_seen_at` as well.
    backdate(&service, operation_id, ten_days_ago);
    let checkpoint = LocalArtifactId::new();
    let mut direct = pod_identity(&f.sandbox_id, POD_UID, 4);
    direct.pod_uid = None;
    direct.substrate_kind = SandboxSubstrateKind::Direct as i32;
    let late_wake = service
        .wake(internal_request(WakeRequest {
            sandbox_id: f.sandbox_id.clone(),
            blocking: true,
            start_context: Some(WakeStartContext {
                route_identity: Some(direct),
                suspend_restore_storage_uri: Some(format!(
                    "s3://space-test/suspend/{}/{checkpoint}?region=us-east-1",
                    f.sandbox_id
                )),
                expected_local_artifact_id: Some(checkpoint.to_string()),
                ..Default::default()
            }),
            runtime_upgrade: Some(WakeRuntimeUpgrade {
                operation_id: operation_id.to_string(),
                target_environment: target.environment().unwrap().to_string(),
                authorization: Some(authorization()),
                checkpoint_local_artifact_id: Some(checkpoint.to_string()),
            }),
            ..Default::default()
        }))
        .await
        .expect_err("abandoned operation refuses late Wake before staging");
    assert_eq!(
        late_wake.code(),
        tonic::Code::FailedPrecondition,
        "{late_wake}"
    );
    assert!(late_wake.message().contains("abandoned"), "{late_wake}");
    assert_eq!(
        service
            .prune_retained_runtime_upgrades(&mut pruner)
            .await
            .unwrap()
            .deleted_abandoned,
        0
    );

    // Cross-sandbox calls do not refresh `last_seen_at`, and disabled retention
    // (`None`) is a no-op.
    backdate(&service, operation_id, ten_days_ago);
    assert_eq!(
        upgrade(
            &service,
            pod_upgrade(
                &other_sandbox,
                &target,
                operation_id,
                WakeTargetState::Stopped
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        abandon(&service, &other_sandbox, operation_id)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        disabled_service
            .prune_retained_runtime_upgrades(&mut pruner)
            .await,
        None
    );

    let pruned = service
        .prune_retained_runtime_upgrades(&mut pruner)
        .await
        .unwrap();
    assert_eq!(pruned.deleted_abandoned, 1);
    assert!(!report(&service, &f.sandbox_id, operation_id).abandoned);

    // After pruning, a late `UpgradeRuntime` without `stage_from` cannot run on
    // the node because the sandbox already left before pruning was eligible.
    let post_prune_call = upgrade(
        &service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .expect_err("evicted sandbox without stage_from cannot run after abandonment prune");
    assert_eq!(
        post_prune_call.code(),
        tonic::Code::NotFound,
        "{post_prune_call}"
    );

    // Restart drain refuses the writer lease and skips pruning.
    let draining_op = operation();
    abandon(&service, &f.sandbox_id, draining_op)
        .await
        .expect("operation is abandoned before restart drain");
    backdate(&service, draining_op, ten_days_ago);
    service.begin_draining_for_restart();
    assert_eq!(
        service.prune_retained_runtime_upgrades(&mut pruner).await,
        None
    );
    assert!(report(&service, &f.sandbox_id, draining_op).abandoned);
    f.assert_no_node_calls();
}
