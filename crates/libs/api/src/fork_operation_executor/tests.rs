use pplx_space_common_core::types::{NodeId, OrgId, SandboxId, SnapshotId, UserId};

use super::*;

#[test]
fn executor_invariants_are_not_reported_as_invalid_client_requests() {
    let ForkExecutionError::Terminal { code, .. } =
        ForkExecutionError::terminal("persisted workflow invariant is missing")
    else {
        panic!("expected a terminal error");
    };

    assert_eq!(code, PublicErrorCode::Internal);
}

#[test]
fn invalid_capture_leader_state_is_terminal() {
    let error = ForkExecutionError::from(
        crate::fork_operation_store::ForkOperationStoreError::InvalidLeaderState(
            "not-a-state".to_string(),
        ),
    );

    assert!(matches!(error, ForkExecutionError::Terminal { .. }));
}

fn target_config() -> ForkTargetConfig {
    ForkTargetConfig {
        egress_profiles: Default::default(),
        template: "seed:latest".parse().unwrap(),
        source_template_id: Some("seed".to_string()),
        runtime: None,
        env: Default::default(),
        metadata: Default::default(),
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
        network_policy: None,
    }
}

#[test]
fn create_request_applies_requested_fork_timeout() {
    let mut config = target_config();
    config.auto_lifecycle = crate::e2b::policy_for_timeout_update(&config.auto_lifecycle, 60);
    let request = create_request(&config, ForkSandboxLifetimeSeconds::try_from(42).unwrap());
    let policy = request.auto_lifecycle.unwrap();

    assert_eq!(
        policy.stages.first().map(|stage| stage.at),
        Some(42),
        "fork timeout must replace the source sandbox lifetime"
    );
}

#[test]
fn create_request_applies_timeout_to_disabled_source_lifecycle_policy() {
    let request = create_request(
        &target_config(),
        ForkSandboxLifetimeSeconds::try_from(42).unwrap(),
    );
    let policy = request.auto_lifecycle.unwrap();

    assert_eq!(
        policy.stages.first().map(|stage| stage.at),
        Some(42),
        "the explicit fork timeout must govern the child"
    );
}

#[test]
fn create_request_keeps_a_non_ecw_source_lifecycle_policy() {
    let mut config = target_config();
    config.auto_lifecycle = crate::test_support::command_idle_suspend_policy(600);
    let request = create_request(&config, ForkSandboxLifetimeSeconds::try_from(42).unwrap());

    assert_eq!(request.auto_lifecycle, Some(config.auto_lifecycle));
}

#[test]
fn persisted_sandbox_result_contains_only_minting_inputs() {
    let sandbox_id = SandboxId::new();
    let result = sandbox_result(
        sandbox_id,
        "seed".to_string(),
        Some("e2b.example.test"),
        EcwCredentialEpoch::new(1).unwrap(),
    );
    let persisted = serde_json::to_value(result).unwrap();

    assert_eq!(persisted["sandbox_id"], sandbox_id.to_string());
    assert_eq!(persisted["template_id"], "seed");
    assert_eq!(persisted["ecw_host_domain"], "e2b.example.test");
    assert!(persisted.get("envdAccessToken").is_none());
    assert!(persisted.get("trafficAccessToken").is_none());
}

#[test]
fn terminal_cleanup_uses_the_preallocated_target() {
    let payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        crate::fork_operations::ForkRequest::from_wire(Some(60), Some(1)).unwrap(),
        "e2b.example.test".to_string(),
    );
    let target = payload.target_sandbox_id();

    assert_eq!(terminal_cleanup_target_ids(&payload), [target]);
}

#[test]
fn fork_metrics_follow_the_persisted_workflow_phase() {
    let mut payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        crate::fork_operations::ForkRequest::from_wire(Some(60), Some(1)).unwrap(),
        "e2b.example.test".to_string(),
    );
    let dimensions = |payload: &ForkSandboxPayloadV1| {
        fork_metric_dimensions(
            &serde_json::to_value(crate::fork_operations::ForkSandboxPayload::v1(
                payload.clone(),
            ))
            .unwrap(),
        )
    };

    assert_eq!(
        dimensions(&payload),
        (
            pplx_space_metrics::sandbox_fork::ForkPhase::SourceFreeze,
            pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Unfrozen,
        )
    );
    payload.freeze_source(7, ForkSourceLifecycle::Paused, target_config(), None);
    assert_eq!(
        dimensions(&payload),
        (
            pplx_space_metrics::sandbox_fork::ForkPhase::ArtifactMaterialization,
            pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Paused,
        )
    );
    payload.resolve_fork_snapshot(SnapshotId::new(), uuid::Uuid::now_v7());
    assert_eq!(
        dimensions(&payload),
        (
            pplx_space_metrics::sandbox_fork::ForkPhase::TargetRestore,
            pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Paused,
        )
    );
}

#[test]
fn fork_retry_metrics_distinguish_transient_failures_from_workflow_progress() {
    assert_eq!(
        classify_fork_attempt(&retry("temporary failure")),
        (
            pplx_space_metrics::sandbox_fork::ForkAttemptOutcome::TransientRetry,
            Some(pplx_space_metrics::sandbox_fork::ForkRetryKind::TransientFailure),
        )
    );
    assert_eq!(
        classify_fork_attempt(&wait("artifact pending")),
        (
            pplx_space_metrics::sandbox_fork::ForkAttemptOutcome::WorkflowWait,
            Some(pplx_space_metrics::sandbox_fork::ForkRetryKind::WorkflowProgress),
        )
    );
}

#[test]
fn fork_response_preserves_untagged_source_template_id() {
    assert_eq!(fork_response_template_id(&target_config()), "seed");
}

#[test]
fn frozen_source_template_uses_the_resolved_content_hash() {
    let content_hash = "sha256:0000000000000000000000000000000000000000000000000000000000000001";

    let template = frozen_source_template_reference("seed", Some(content_hash)).unwrap();

    assert_eq!(template.to_string(), format!("seed@{content_hash}"));
}

#[test]
fn target_config_without_source_template_id_uses_the_normalized_template_reference() {
    let mut value = serde_json::to_value(target_config()).unwrap();
    value.as_object_mut().unwrap().remove("source_template_id");
    let config: ForkTargetConfig = serde_json::from_value(value).unwrap();

    assert_eq!(fork_response_template_id(&config), "seed:latest");
}

#[test]
fn sandbox_error_uses_typed_e2b_shape() {
    assert_eq!(
        sandbox_error(500, "restore failed"),
        ForkTargetOutcome::Failed {
            code: 500,
            message: "restore failed".to_string(),
        }
    );
}

#[test]
fn restore_activation_lease_preserves_the_existing_floor() {
    assert_eq!(
        fork_restore_activation_lease_seconds(Duration::from_secs(240)),
        600
    );
    assert_eq!(
        fork_restore_activation_lease_seconds(Duration::from_secs(900)),
        900
    );
}

#[test]
fn deadline_with_a_failed_target_is_not_reported_as_success() {
    let result = deadline_fork_result(vec![sandbox_error(
        504,
        "sandbox fork hard deadline exceeded",
    )]);

    assert!(matches!(
        result,
        Err(ForkExecutionError::DeadlineExceeded {
            code: PublicErrorCode::FailedPrecondition,
            ..
        })
    ));
}

#[test]
fn failed_targets_are_reported_as_a_failed_fork_outcome() {
    assert_eq!(
        fork_outcome(&[sandbox_error(500, "restore failed")]),
        pplx_space_metrics::sandbox_fork::ForkOutcome::Failed
    );
}

#[test]
fn executor_retry_uses_queued_state() {
    let result = retry("transient failure");

    assert_eq!(result.state, OperationState::Queued);
    assert_eq!(
        result.requeue_reason,
        PublicOperationRequeueReason::TransientFailure
    );
}

#[test]
fn executor_wait_preserves_retry_budget() {
    let result = wait("waiting for artifact upload");

    assert_eq!(result.state, OperationState::Queued);
    assert_eq!(
        result.requeue_reason,
        PublicOperationRequeueReason::WorkflowProgress
    );
}

#[test]
fn suspended_source_selects_the_durable_artifact_path() {
    assert_eq!(
        "suspended".parse::<ForkSourceLifecycle>().unwrap(),
        ForkSourceLifecycle::Suspended
    );
}

#[test]
fn snapshot_errors_have_explicit_workflow_dispositions() {
    assert_eq!(
        classify_snapshot_error(tonic::Code::FailedPrecondition),
        SnapshotErrorDisposition::Refreeze
    );
    assert_eq!(
        classify_snapshot_error(tonic::Code::Aborted),
        SnapshotErrorDisposition::Refreeze
    );
    assert_eq!(
        classify_snapshot_error(tonic::Code::InvalidArgument),
        SnapshotErrorDisposition::Terminal(PublicErrorCode::InvalidRequest)
    );
    assert_eq!(
        classify_snapshot_error(tonic::Code::NotFound),
        SnapshotErrorDisposition::Terminal(PublicErrorCode::NotFound)
    );
    assert_eq!(
        classify_snapshot_error(tonic::Code::PermissionDenied),
        SnapshotErrorDisposition::Terminal(PublicErrorCode::Forbidden)
    );
    assert_eq!(
        classify_snapshot_error(tonic::Code::Unavailable),
        SnapshotErrorDisposition::Retry
    );
}

#[test]
fn live_source_requires_a_positive_execution_generation() {
    frozen_execution_generation(None, ForkSourceLifecycle::Started).unwrap_err();
    frozen_execution_generation(Some(0), ForkSourceLifecycle::Paused).unwrap_err();
    assert_eq!(
        frozen_execution_generation(Some(7), ForkSourceLifecycle::Started).unwrap(),
        7
    );
    assert_eq!(
        frozen_execution_generation(None, ForkSourceLifecycle::Suspended).unwrap(),
        0
    );
}

#[test]
fn byok_organizations_are_rejected_before_capture() {
    assert!(validate_source_encryption(true, false).is_err());
    assert!(validate_source_encryption(false, true).is_err());
    validate_source_encryption(false, false).unwrap();
}

#[test]
fn retryable_restore_controller_error_requeues_the_operation() {
    let error = ControllerError::service_unavailable("restore admission unavailable");

    assert_eq!(
        classify_restore_controller_error(&error),
        RestoreControllerErrorDisposition::Retry
    );
}

#[test]
fn unrelated_restore_conflicts_are_terminal() {
    let error = ControllerError::conflict("restore lost its evacuation claim");

    assert_eq!(
        classify_restore_controller_error(&error),
        RestoreControllerErrorDisposition::Terminal
    );
}

#[test]
fn concurrent_target_creation_is_reobserved() {
    let error = ControllerError::sandbox_already_exists();

    assert_eq!(
        classify_restore_controller_error(&error),
        RestoreControllerErrorDisposition::Reobserve
    );
}

#[test]
fn suspended_target_replays_as_success_after_restore() {
    assert_eq!(
        classify_existing_target(SandboxLifecycle::Suspended, false),
        ExistingTargetDisposition::Succeeded
    );
    assert_eq!(
        classify_existing_target(SandboxLifecycle::Restoring, false),
        ExistingTargetDisposition::Wait
    );
    assert_eq!(
        classify_existing_target(SandboxLifecycle::Failed, false),
        ExistingTargetDisposition::Failed
    );
}

#[test]
fn command_pending_target_cannot_replay_as_success() {
    assert_eq!(
        classify_existing_target(SandboxLifecycle::Started, true),
        ExistingTargetDisposition::Wait
    );
    assert_eq!(
        classify_existing_target(SandboxLifecycle::Started, false),
        ExistingTargetDisposition::Succeeded
    );
}

#[tokio::test]
async fn external_work_has_an_overall_deadline() {
    let result =
        bounded_external_work(std::future::pending::<()>(), Duration::from_millis(10)).await;

    assert!(
        result.is_err(),
        "hung external work must stop renewing its claim"
    );
}
mod cold_copy_tests {

    use super::*;
    use pplx_space_common_core::types::SnapshotId;
    use pplx_space_db_test_fixtures::try_pg_test_db;

    #[test]
    fn cleanup_failure_does_not_overturn_a_successful_fork() {
        let result = successful_fork_result_after_retirement(
            vec![ForkTargetOutcome::Failed {
                code: 500,
                message: "restore failed".to_string(),
            }],
            Err(ForkExecutionError::Retry(
                "database unavailable".to_string(),
            )),
        )
        .expect("completed fork must stay successful");

        assert_eq!(result.state, OperationState::Succeeded);
    }

    #[test]
    fn cold_copy_destination_uses_snapshot_identity_and_preserves_connection_query() {
        let operation_id = pplx_space_public_api::OperationId::new();
        let source_sandbox_id = SandboxId::new();
        let fork_snapshot_id = deterministic_fork_uuid(operation_id.as_uuid(), b"snapshot");
        assert_eq!(
            fork_destination_uri(
                "s3://snapshots/suspend/source?region=us-east-1&endpoint=http://s3:8333&path_style=true",
                source_sandbox_id,
                SnapshotId::from_uuid(fork_snapshot_id),
            ),
            format!(
                "s3://snapshots/suspend/source/fork/{source_sandbox_id}/{fork_snapshot_id}?region=us-east-1&endpoint=http://s3:8333&path_style=true"
            )
        );
    }

    #[test]
    fn cold_copy_database_identities_are_stable_per_operation() {
        let operation_id = uuid::Uuid::now_v7();
        assert_eq!(
            deterministic_fork_uuid(operation_id, b"snapshot"),
            deterministic_fork_uuid(operation_id, b"snapshot")
        );
        assert_ne!(
            deterministic_fork_uuid(operation_id, b"snapshot"),
            deterministic_fork_uuid(operation_id, b"location")
        );
    }

    #[test]
    fn copied_snapshot_is_initially_owned_by_the_existing_source_sandbox() {
        let source = SandboxId::new();
        let payload = ForkSandboxPayloadV1::new(
            source,
            crate::fork_operations::ForkRequest::from_wire(Some(60), Some(1)).unwrap(),
            "e2b.example.test".to_string(),
        );

        assert_eq!(copied_snapshot_owner(&payload), source);
    }

    #[test]
    fn terminal_cold_copy_cleanup_recovers_ids_before_payload_publication() {
        let operation_id = pplx_space_public_api::OperationId::new();
        assert_eq!(
            terminal_copied_artifact_ids(operation_id),
            (
                SnapshotId::from_uuid(deterministic_fork_uuid(operation_id.as_uuid(), b"snapshot")),
                deterministic_fork_uuid(operation_id.as_uuid(), b"location"),
            )
        );
    }

    #[tokio::test]
    async fn direct_fork_copy_fallback_requires_a_fresh_ready_node_status() {
        let Some(db) = try_pg_test_db().await.unwrap() else {
            return;
        };
        let node_id = NodeId::new();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint)
             VALUES ($1, 'http://127.0.0.1:9802')"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await
        .unwrap();

        let substrate =
            crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone());
        substrate
            .select_ready_worker_node_api_target()
            .await
            .unwrap_err();

        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO node_status_snapshots (
                 node_id, observed_at, stale_after, reachable, role,
                 scheduling_status, services, updated_at
             ) VALUES (
                 $1, NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour',
                 TRUE, 'worker', 'ready', '[]'::jsonb, NOW()
             )"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await
        .unwrap();
        substrate
            .select_ready_worker_node_api_target()
            .await
            .unwrap_err();

        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE node_status_snapshots
             SET stale_after = NOW() + INTERVAL '1 hour', reachable = FALSE
             WHERE node_id = $1"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await
        .unwrap();
        substrate
            .select_ready_worker_node_api_target()
            .await
            .unwrap_err();

        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE node_status_snapshots
             SET reachable = TRUE, scheduling_status = 'draining'
             WHERE node_id = $1"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await
        .unwrap();
        substrate
            .select_ready_worker_node_api_target()
            .await
            .unwrap_err();

        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE node_status_snapshots
             SET scheduling_status = 'ready'
             WHERE node_id = $1"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await
        .unwrap();

        let target = substrate
            .select_ready_worker_node_api_target()
            .await
            .unwrap();

        assert_eq!(target.managed_by.as_str(), "space");
        assert_eq!(target.role.as_str(), "worker");
        assert_eq!(
            target.direct_endpoint.as_deref(),
            Some("http://127.0.0.1:9802")
        );
    }

    #[tokio::test]
    async fn terminal_cleanup_retires_a_ready_copied_artifact_consistently() {
        let Some(db) = try_pg_test_db().await.unwrap() else {
            return;
        };
        let org_id = OrgId::new();
        let user_id = UserId::new();
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        let storage_location_id = uuid::Uuid::now_v7();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)"
        ))
        .bind(org_id)
        .bind(format!("Fork copy retirement {org_id}"))
        .bind(format!(
            "fork-copy-retirement-{}",
            org_id.as_uuid().simple()
        ))
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
             VALUES ($1, $2, 'unused', 'Fork copy retirement', $3, 'member')"
        ))
        .bind(user_id)
        .bind(format!("fork-copy-retirement-{user_id}@example.com"))
        .bind(org_id)
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, placement, network_policy_generation
             ) VALUES ($1, $2, $3, 'suspended', 'unplaced', 1)"
        ))
        .bind(sandbox_id)
        .bind(user_id)
        .bind(org_id)
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, artifact_kind, artifact_origin, org_id
             ) VALUES ($1, $2, 'disk', 'ready', 'disk', 'fork', $3)"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(org_id)
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO snapshot_storage_locations (
                 id, snapshot_id, storage_connection_string
             ) VALUES ($1, $2, 's3://fork-copy-retirement/artifact')"
        ))
        .bind(storage_location_id)
        .bind(snapshot_id)
        .execute(&db.pool)
        .await
        .unwrap();

        ForkOperationStore::new(db.pool.clone())
            .retire_copied_artifact(snapshot_id, storage_location_id)
            .await
            .unwrap();

        let snapshot_exists: bool = sqlx::query_scalar(pplx_space_common_core::sql!(
            "SELECT EXISTS (SELECT 1 FROM snapshots WHERE id = $1)"
        ))
        .bind(snapshot_id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        let location_orphaned: bool = sqlx::query_scalar(pplx_space_common_core::sql!(
            "SELECT orphaned_at IS NOT NULL
             FROM snapshot_storage_locations WHERE id = $1"
        ))
        .bind(storage_location_id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(!snapshot_exists);
        assert!(location_orphaned);
    }

    #[test]
    fn node_operation_identity_uses_the_unprefixed_uuid() {
        let operation_id = pplx_space_public_api::OperationId::new();
        assert!(operation_id.to_string().starts_with("op_"));
        uuid::Uuid::parse_str(&operation_id.as_uuid().to_string()).unwrap();
    }
}

#[test]
fn kubernetes_copy_target_requires_a_substrate_node_id() {
    assert!(!active_copy_node_target_is_complete(
        NodeAddressing::SubstrateNodeId,
        true,
        true,
        true,
        false,
    ));
    assert!(active_copy_node_target_is_complete(
        NodeAddressing::SubstrateNodeId,
        true,
        true,
        true,
        true,
    ));
}

#[test]
fn direct_copy_target_accepts_a_direct_endpoint() {
    assert!(active_copy_node_target_is_complete(
        NodeAddressing::DirectEndpoint,
        true,
        true,
        true,
        false,
    ));
}
