use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};

use super::*;
use pplx_space_common_core::types::{
    ArtifactKind, Email, LocalArtifactId, NodeManagedBy, NodePoolName, OrgId, TemplateName, UserId,
};
use pplx_space_db_test_fixtures::{
    TestDb, pg_test_db_with_max_connections, try_pg_test_db_with_max_connections,
};
use pplx_space_node_api_sim::{SandboxRecord, SimState, SimWorkerHarness};
use pplx_space_placement::test_support::stored_lifecycle_matrix;
use sqlx::PgPool;

mod restore_lease_completion;
mod substrate_scope;

#[test]
fn direct_create_rejects_fork_disk_restore() {
    let error = ensure_direct_create_supported(true).unwrap_err();
    assert!(matches!(error, SubstrateError::Unsupported { .. }));
}

#[test]
fn direct_create_boot_mode_never_delegates_pool_adoption_to_sbxmgr() {
    assert_eq!(
        direct_create_boot_mode(CreateBootMode::Fallthrough).unwrap(),
        BootMode::BaseSnapshot as i32
    );
    assert_eq!(
        direct_create_boot_mode(CreateBootMode::Cold).unwrap(),
        BootMode::Cold as i32
    );
    assert_eq!(
        direct_create_boot_mode(CreateBootMode::BaseSnapshot).unwrap(),
        BootMode::BaseSnapshot as i32
    );
    direct_create_boot_mode(CreateBootMode::Pool).unwrap_err();
}

#[test]
fn one_shot_create_only_applies_to_ready_cold_or_base_snapshot_requests() {
    for disk_mib in [None, Some(4096)] {
        let resources = SandboxResources {
            vcpu: None,
            memory_mib: None,
            disk_mib,
        };
        assert!(use_one_shot_create(
            true,
            CreateBootMode::BaseSnapshot,
            &resources
        ));
        assert!(use_one_shot_create(true, CreateBootMode::Cold, &resources));
        assert!(!use_one_shot_create(
            true,
            CreateBootMode::Fallthrough,
            &resources
        ));
        assert!(!use_one_shot_create(true, CreateBootMode::Pool, &resources));
        assert!(!use_one_shot_create(
            false,
            CreateBootMode::BaseSnapshot,
            &resources
        ));
    }
}

#[test]
fn pull_on_create_requires_pinned_hash() {
    assert!(pull_on_create_allowed(
        Some("sha256:abc"),
        CreateBootMode::Fallthrough
    ));
    assert!(pull_on_create_allowed(
        Some("sha256:abc"),
        CreateBootMode::Cold
    ));
    assert!(!pull_on_create_allowed(
        Some("sha256:abc"),
        CreateBootMode::Pool
    ));
    assert!(!pull_on_create_allowed(None, CreateBootMode::Fallthrough));
}

#[test]
fn pull_on_create_does_not_handle_explicit_base_snapshot() {
    assert!(!pull_on_create_allowed(
        Some("sha256:abc"),
        CreateBootMode::BaseSnapshot
    ));
}

#[test]
fn pull_on_create_candidates_get_longer_request_timeout() {
    assert_eq!(
        WorkerCandidateSource::CachedTemplate.request_timeout(),
        DIRECT_NODE_RPC_TIMEOUT
    );
    assert_eq!(
        WorkerCandidateSource::PullOnCreate.request_timeout(),
        DIRECT_PULL_ON_CREATE_RPC_TIMEOUT
    );
    assert!(WorkerCandidateSource::PullOnCreate.request_timeout() > DIRECT_NODE_RPC_TIMEOUT);
}

#[test]
fn pool_adoption_retries_stale_and_transient_candidate_errors() {
    for code in [
        Code::ResourceExhausted,
        Code::NotFound,
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::Aborted,
        Code::Internal,
        Code::Unknown,
    ] {
        let status = tonic::Status::new(code, "candidate failed");
        assert!(
            should_try_next_worker_candidate(&status),
            "{code:?} should try the next pool candidate"
        );
    }

    for code in [
        Code::InvalidArgument,
        Code::PermissionDenied,
        Code::Unauthenticated,
        Code::FailedPrecondition,
    ] {
        let status = tonic::Status::new(code, "permanent failure");
        assert!(
            !should_try_next_worker_candidate(&status),
            "{code:?} should fail the pool claim"
        );
    }
}

#[test]
fn direct_create_stages_terminalize_invalid_key_precondition() {
    let evidence = "federated service grant is required";
    let status = invalid_key_reference_status(evidence);
    assert_eq!(cached_worker_failure_from_create_status(&status), None);

    for error in [
        direct_create_status_error(status),
        direct_create_start_status_error(invalid_key_reference_status(evidence)),
    ] {
        let controller_error = ControllerError::from(error);
        assert_eq!(
            controller_error.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
        assert!(controller_error.to_string().contains(evidence));
    }

    assert_eq!(
        cached_worker_failure_from_create_status(&tonic::Status::failed_precondition(
            "worker-local precondition"
        )),
        Some(CachedWorkerFailure::Degraded)
    );
    assert!(matches!(
        direct_create_start_status_error(tonic::Status::internal("worker-local failure")),
        SubstrateError::Failed { .. }
    ));
}

#[test]
fn template_policy_dependency_does_not_change_worker_health() {
    for (code, expected_worker_failure) in [
        (Code::Unavailable, CachedWorkerFailure::Unavailable),
        (Code::DeadlineExceeded, CachedWorkerFailure::Unavailable),
        (Code::Internal, CachedWorkerFailure::Degraded),
        (Code::FailedPrecondition, CachedWorkerFailure::Degraded),
    ] {
        let original = tonic::Status::new(code, "same diagnostic text");
        assert_eq!(
            cached_worker_failure_from_create_status(&original),
            Some(expected_worker_failure)
        );
        let dependency = pplx_space_sbx_mgr_client::template_policy_dependency_status(original);
        assert_eq!(cached_worker_failure_from_create_status(&dependency), None);
        assert_eq!(
            should_try_next_worker_candidate(&dependency),
            code != Code::FailedPrecondition,
            "conflicting hints are terminal; owner readiness errors retain failover"
        );
    }
}

#[test]
fn direct_delete_preserves_suspend_publication_refusal() {
    let sandbox_id = SandboxId::new();
    let pending = pplx_space_sbx_mgr_client::suspend_publication_pending_status(
        "suspend publication pending",
    );
    assert!(matches!(
        direct_delete_error(sandbox_id, pending),
        SubstrateError::SuspendInFlight { .. }
    ));
    assert!(matches!(
        direct_delete_error(
            sandbox_id,
            pplx_space_sbx_mgr_client::retained_filesystem_only_copy_status(
                "retained filesystem is the only copy",
            ),
        ),
        SubstrateError::RetainedFilesystemOnlyCopy { .. }
    ));
    assert!(matches!(
        direct_delete_error(sandbox_id, tonic::Status::aborted("other conflict")),
        SubstrateError::Failed { .. }
    ));
}

#[test]
fn pool_resource_value_rejects_negative_inventory_before_adoption() {
    assert_eq!(
        DirectSubstrate::pool_resource_value(None, "vcpu").unwrap(),
        None
    );
    assert_eq!(
        DirectSubstrate::pool_resource_value(Some(4), "vcpu").unwrap(),
        Some(4)
    );
    DirectSubstrate::pool_resource_value(Some(-1), "vcpu").unwrap_err();
}

#[test]
fn direct_pool_candidate_shape_pairs_inventory_dimensions() {
    let candidate = DirectPoolCandidate {
        id: NodeId::new(),
        node_api_endpoint: "http://127.0.0.1:9800".to_string(),
        content_hash: "sha256:pooled".to_string(),
        vcpu: Some(4),
        memory_mib: Some(8192),
    };

    assert_eq!(
        DirectSubstrate::pool_candidate_shape(&candidate).unwrap(),
        Some(PooledExecutionPoolShape::new(4, 8192, Default::default()))
    );
}

#[test]
fn direct_pool_candidate_shape_rejects_half_populated_inventory() {
    for (vcpu, memory_mib) in [(Some(4), None), (None, Some(8192))] {
        let candidate = DirectPoolCandidate {
            id: NodeId::new(),
            node_api_endpoint: "http://127.0.0.1:9800".to_string(),
            content_hash: "sha256:pooled".to_string(),
            vcpu,
            memory_mib,
        };

        DirectSubstrate::pool_candidate_shape(&candidate)
            .expect_err("incomplete shape inventory must be rejected");
    }
}

#[test]
fn restore_retries_node_local_candidate_errors() {
    for code in [
        Code::FailedPrecondition,
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Internal,
    ] {
        let status = tonic::Status::new(code, "candidate failed");
        assert!(
            should_try_next_restore_candidate(&status),
            "{code:?} should try the next restore candidate"
        );
    }

    for code in [
        Code::InvalidArgument,
        Code::PermissionDenied,
        Code::Unauthenticated,
        Code::NotFound,
        Code::Aborted,
        Code::Unknown,
    ] {
        let status = tonic::Status::new(code, "permanent failure");
        assert!(
            !should_try_next_restore_candidate(&status),
            "{code:?} should fail the restore"
        );
    }

    let invalid_key = invalid_key_reference_status("provider rejected key");
    assert!(!should_try_next_restore_candidate(&invalid_key));
    assert!(matches!(
        restore_status_error(&invalid_key),
        SubstrateError::FailedPrecondition { .. }
    ));
}

fn invalid_key_reference_status(message: &'static str) -> tonic::Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE
            .parse()
            .unwrap(),
    );
    tonic::Status::with_metadata(Code::FailedPrecondition, message, metadata)
}

#[test]
fn transient_route_generation_reservation_failure_is_unavailable() {
    let error = direct_route_generation_reservation_error(sqlx::Error::PoolTimedOut);
    assert!(matches!(error, SubstrateError::Unavailable { .. }));
}

#[test]
fn retained_generation_conflict_is_retryable_without_candidate_fallback() {
    let status = tonic::Status::aborted(
        "runtime activation identity generation is not newer than retained generation 7",
    );

    assert!(!should_try_next_restore_candidate(&status));
    assert!(matches!(
        restore_status_error(&status),
        SubstrateError::Unavailable { .. }
    ));
}

#[test]
fn cross_node_restore_request_carries_storage_uri_and_source_node() {
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let candidate = DirectRestoreCandidate {
        node: DirectNode {
            id: NodeId::new(),
            node_api_endpoint: "127.0.0.1:9802".to_string(),
        },
        storage_uri: Some("s3://bucket/snapshots/sandbox/snapshot".to_string()),
        source_node_id: Some(source_node_id),
        kind: DirectRestoreCandidateKind::RemoteSnapshot,
    };
    let request = direct_restore_start_request(
        sandbox_id,
        Some(8),
        true,
        Duration::from_millis(2500),
        &candidate,
        None,
    );

    assert_eq!(request.sandbox_id, sandbox_id.to_string());
    assert_eq!(request.checkpoint.as_deref(), Some("suspend"));
    assert_eq!(
        request.suspend_restore_storage_uri.as_deref(),
        Some("s3://bucket/snapshots/sandbox/snapshot")
    );
    assert_eq!(
        request.suspend_restore_source_node.as_deref(),
        Some(source_node_id.to_string().as_str())
    );
    assert_eq!(request.ready_timeout_secs, Some(3));
    assert_eq!(request.vcpu, Some(8));
}

#[test]
fn source_node_restore_request_preserves_local_fast_path() {
    let sandbox_id = SandboxId::new();
    let candidate = DirectRestoreCandidate {
        node: DirectNode {
            id: NodeId::new(),
            node_api_endpoint: "127.0.0.1:9802".to_string(),
        },
        storage_uri: None,
        source_node_id: Some(NodeId::new()),
        kind: DirectRestoreCandidateKind::SourceNode,
    };
    let request = direct_restore_start_request(
        sandbox_id,
        None,
        false,
        Duration::from_secs(9),
        &candidate,
        None,
    );

    assert_eq!(request.checkpoint.as_deref(), Some("suspend"));
    assert_eq!(request.vcpu, None);
    assert_eq!(request.suspend_restore_storage_uri, None);
    assert_eq!(request.suspend_restore_source_node, None);
}

#[test]
fn direct_restore_request_carries_control_plane_route_provenance() {
    let sandbox_id = SandboxId::new();
    let candidate = DirectRestoreCandidate {
        node: DirectNode {
            id: NodeId::new(),
            node_api_endpoint: "127.0.0.1:9802".to_string(),
        },
        storage_uri: Some("s3://bucket/snapshot".to_string()),
        source_node_id: Some(NodeId::new()),
        kind: DirectRestoreCandidateKind::RemoteSnapshot,
    };
    let route_identity = pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
        route_generation: Some(8),
        current_node_id: Some(candidate.node.id.to_string()),
        substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct as i32,
        execution_generation: Some(8),
        activation_attempt_id: Some(Uuid::now_v7().to_string()),
        ..Default::default()
    };

    let request = direct_restore_start_request(
        sandbox_id,
        Some(8),
        true,
        Duration::from_secs(9),
        &candidate,
        Some(route_identity.clone()),
    );

    assert_eq!(request.route_identity, Some(route_identity));
}

#[test]
fn direct_wake_request_delegates_route_identity_to_sandbox_manager() {
    let sandbox_id = SandboxId::new();

    for vcpu in [None, Some(8)] {
        let request = direct_wake_request(sandbox_id, vcpu, None, None);
        assert_eq!(
            request
                .start_context
                .as_ref()
                .and_then(|context| context.vcpu),
            vcpu
        );
        assert_eq!(
            request
                .start_context
                .and_then(|context| context.route_identity),
            None
        );
    }
}

#[test]
fn direct_route_receipt_checks_optional_sandbox_binding() {
    let sandbox_id = SandboxId::new();
    for (binding, accepted) in [
        (None, true),
        (Some(sandbox_id.to_string()), true),
        (Some(SandboxId::new().to_string()), false),
        (Some("invalid".to_string()), false),
    ] {
        let result = parse_direct_route_identity(
            sandbox_id,
            pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                sandbox_id: binding,
                route_generation: Some(1),
                current_node_id: Some(NodeId::new().to_string()),
                substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct
                    as i32,
                execution_generation: Some(1),
                activation_attempt_id: Some(Uuid::now_v7().to_string()),
                pod_uid: None,
            },
        );
        if accepted {
            assert_eq!(result.unwrap().sandbox_id(), sandbox_id);
        } else {
            assert!(matches!(
                result,
                Err(DirectRouteIdentityReceiptError::SandboxBinding)
            ));
        }
    }
}

#[test]
fn suspended_direct_wake_carries_control_plane_route_identity() {
    let sandbox_id = SandboxId::new();
    let route_identity = pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
        route_generation: Some(8),
        current_node_id: Some(Uuid::now_v7().to_string()),
        substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct as i32,
        execution_generation: Some(8),
        activation_attempt_id: Some(Uuid::now_v7().to_string()),
        ..Default::default()
    };

    let request = direct_wake_request(sandbox_id, Some(8), Some(route_identity.clone()), None);

    let context = request.start_context.expect("activation context");
    assert_eq!(context.vcpu, Some(8));
    assert_eq!(context.route_identity, Some(route_identity));
}

// Every node lifecycle state: only those a wake restores or starts reserve
// an identity first, matching sandbox-manager's wake transitions.
#[test]
fn only_a_resident_that_restores_or_starts_reserves_an_identity_before_waking() {
    use pplx_space_sbx_mgr_client::proto::{SandboxInfo, SandboxLifecycleState as State};
    let resident = |lifecycle: Option<i32>| SandboxInfo {
        lifecycle,
        ..SandboxInfo::default()
    };
    let mut seen = 0;
    for value in 0..=i32::from(u8::MAX) {
        let Ok(state) = State::try_from(value) else {
            assert!(!resident_is_dormant(&resident(Some(value))), "{value}");
            continue;
        };
        seen += 1;
        let dormant = match state {
            State::Suspended | State::Stopped | State::Created => true,
            State::Unspecified
            | State::Creating
            | State::Starting
            | State::Started
            | State::Stopping
            | State::Deleting
            | State::Pausing
            | State::Paused
            | State::Resuming
            | State::Checkpointing
            | State::Suspending
            | State::Restoring
            | State::CleanupRequired
            | State::CleanupFailed
            | State::Quarantined
            | State::Failed => false,
        };
        assert_eq!(
            resident_is_dormant(&resident(Some(value))),
            dormant,
            "{state:?}"
        );
    }
    assert_eq!(seen, 19, "every lifecycle state is covered");
    assert!(!resident_is_dormant(&resident(None)));
}

#[test]
fn wake_uses_restore_only_when_the_local_sandbox_row_is_missing() {
    assert!(wake_requires_restore(&tonic::Status::not_found(
        "sandbox not found"
    )));
    assert!(!wake_requires_restore(&tonic::Status::failed_precondition(
        "sandbox cannot wake"
    )));
}

#[test]
fn wake_missing_row_restore_fallback_requires_a_restore_eligible_lifecycle() {
    assert!(wake_lifecycle_allows_restore_fallback("suspended"));
    assert!(wake_lifecycle_allows_restore_fallback("restoring"));
    for lifecycle in ["created", "starting", "started", "paused", "failed"] {
        assert!(
            !wake_lifecycle_allows_restore_fallback(lifecycle),
            "{lifecycle} must preserve the original wake error"
        );
    }
}

#[test]
fn wake_retries_with_identity_when_activation_succeeds_without_a_receipt() {
    assert!(wake_response_requires_activation_identity(&WakeResponse {
        sandbox_id: Uuid::now_v7().to_string(),
        transition: WakeTransition::Restore as i32,
        route_identity: None,
    }));
    assert!(wake_response_requires_activation_identity(&WakeResponse {
        sandbox_id: Uuid::now_v7().to_string(),
        transition: WakeTransition::Start as i32,
        route_identity: None,
    }));
    assert!(!wake_response_requires_activation_identity(&WakeResponse {
        sandbox_id: Uuid::now_v7().to_string(),
        transition: WakeTransition::Noop as i32,
        route_identity: None,
    }));
}

#[test]
fn direct_restore_attempt_timeout_splits_aggregate_budget() {
    assert_eq!(
        split_restore_attempt_timeout(Duration::from_secs(21), 3),
        Duration::from_secs(7)
    );
    assert_eq!(
        split_restore_attempt_timeout(Duration::from_millis(5), 10),
        Duration::from_nanos(500_000)
    );
    assert_eq!(
        split_restore_attempt_timeout(Duration::from_secs(4), 0),
        Duration::from_secs(4)
    );
    assert_eq!(timeout_secs_ceil(Duration::from_millis(1)), 1);
    assert_eq!(timeout_secs_ceil(Duration::from_millis(2500)), 3);
}

use pplx_space_test_env::TestResult;

struct PostgresFixture {
    substrate: DirectSubstrate,
    pool: PgPool,
    _db: TestDb,
}

#[tokio::test(flavor = "current_thread")]
async fn deleting_snapshot_blocks_artifact_without_non_orphaned_ready_copy() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let artifact = super::super::PublishedLocalArtifact {
        id: LocalArtifactId::new(),
        kind: ArtifactKind::Full,
    };
    let snapshot_id = SnapshotId::new();
    insert_sandbox(&fixture.pool, sandbox_id, None).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES (
             $1, $2, 'full', 'deleting',
             '00000000-0000-0000-0000-000000000001',
             $3, 'full', 'suspend'
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id.as_uuid())
    .bind(artifact.id.as_uuid())
    .execute(&fixture.pool)
    .await?;

    let error = super::super::persist_optional_current_local_artifact(
        &fixture.pool,
        sandbox_id,
        Some(artifact),
        ArtifactOrigin::Suspend,
    )
    .await
    .expect_err("a deleting snapshot must not become the current artifact");

    assert!(matches!(error, SubstrateError::Unavailable { .. }));
    let current: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(current, None);

    let ready_snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES (
             $1, $2, 'full', 'ready',
             '00000000-0000-0000-0000-000000000001',
             $3, 'full', 'suspend'
         )"
    ))
    .bind(ready_snapshot_id)
    .bind(sandbox_id.as_uuid())
    .bind(artifact.id.as_uuid())
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             snapshot_id, storage_connection_string, org_id, orphaned_at
         )
         VALUES (
             $1, 's3://bucket/snapshots/sandbox/ready-copy',
             '00000000-0000-0000-0000-000000000001', NOW()
         )"
    ))
    .bind(ready_snapshot_id)
    .execute(&fixture.pool)
    .await?;

    let error = super::super::persist_optional_current_local_artifact(
        &fixture.pool,
        sandbox_id,
        Some(artifact),
        ArtifactOrigin::Suspend,
    )
    .await
    .expect_err("an orphaned ready copy must not override the deleting claim");
    assert!(matches!(error, SubstrateError::Unavailable { .. }));

    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations
            SET orphaned_at = NULL
          WHERE snapshot_id = $1"
    ))
    .bind(ready_snapshot_id)
    .execute(&fixture.pool)
    .await?;
    super::super::persist_optional_current_local_artifact(
        &fixture.pool,
        sandbox_id,
        Some(artifact),
        ArtifactOrigin::Suspend,
    )
    .await?;
    let current: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(current, Some(artifact.id.as_uuid()));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn non_restore_lifecycle_without_a_lease_reports_conflict() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started',
                restore_lease_expires_at = NULL,
                restore_lease_token = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&fixture.pool)
    .await?;

    let error = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await
        .expect_err("a started sandbox cannot claim the Direct restore lease");

    assert!(matches!(error, SubstrateError::Conflict { .. }));
    Ok(())
}

/// The Direct restore lease moves a row into and out of `restoring` only from
/// the lifecycles its restore owns.
///
/// - `claim`: an unleased row goes to `restoring` only out of `suspended` or
///   a lapsed `restoring` retry. The guard compares `LOWER(TRIM(lifecycle))`,
///   so a padded `suspended` is claimed too.
/// - `release`: a fresh claim returns to `suspended` only while the row is
///   still exactly `restoring`. The arrange step stores each lifecycle with
///   the lease trigger bypassed and checks the token survived, so a refusal
///   is the lifecycle guard's alone.
/// - `release_after_write`: the same release after a concurrent write stored
///   the lifecycle through the trigger, which drops the token outside
///   `restoring`: the ownership the token carries refuses it too.
///
/// `A`: the writer moves the row; `.`: it refuses and leaves the lifecycle.
#[tokio::test(flavor = "current_thread")]
async fn restore_lease_claim_and_release_move_only_restore_lifecycles() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              claim  release  release_after_write
        "created          .      .          .",
        "creating         .      .          .",
        "starting         .      .          .",
        "started          .      .          .",
        "restoring        A      A          A",
        "pausing          .      .          .",
        "paused           .      .          .",
        "resuming         .      .          .",
        "suspending       .      .          .",
        "suspended        A      .          .",
        "stopping         .      .          .",
        "stopped          .      .          .",
        "deleting         .      .          .",
        "checkpointing    .      .          .",
        "quarantined      .      .          .",
        "failed           .      .          .",
        "deleted          .      .          .",
        "cleanup_required .      .          .",
        "bogus            .      .          .",
        "_Suspended_      A      .          .",
    ];
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let substrate = DirectSubstrate::new(pool.clone());

    async fn lifecycle_and_token(
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) -> TestResult<(String, Option<Uuid>)> {
        Ok(sqlx::query_as(sql!(
            "SELECT lifecycle, restore_lease_token FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_one(pool)
        .await?)
    }

    /// Store `lifecycle` with `sandboxes_clear_restore_lease` disabled for
    /// the one statement, so the row keeps the lease it holds.
    async fn set_lifecycle_keeping_lease(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: &str,
    ) -> TestResult {
        let mut tx = pool.begin().await?;
        sqlx::query(sql!(
            "ALTER TABLE sandboxes DISABLE TRIGGER sandboxes_clear_restore_lease"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox_id.as_uuid())
            .bind(lifecycle)
            .execute(&mut *tx)
            .await?;
        sqlx::query(sql!(
            "ALTER TABLE sandboxes ENABLE TRIGGER sandboxes_clear_restore_lease"
        ))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum Writer {
        Claim,
        Release,
        ReleaseAfterWrite,
    }

    let mut mismatches = Vec::new();
    let writers = [Writer::Claim, Writer::Release, Writer::ReleaseAfterWrite];
    for (stored, cells) in stored_lifecycle_matrix(writers, EXPECTED) {
        for (writer, expect) in cells {
            let applies = expect.applies();
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, None).await?;
            let set_lifecycle =
                sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                    .bind(sandbox_id.as_uuid())
                    .bind(stored);
            let (applied, moved_to) = match writer {
                Writer::Claim => {
                    set_lifecycle.execute(pool).await?;
                    let claimed = substrate.claim_direct_restore_lease(sandbox_id).await;
                    (claimed.is_ok(), "restoring")
                }
                Writer::Release => {
                    let lease = substrate.claim_direct_restore_lease(sandbox_id).await?;
                    set_lifecycle_keeping_lease(pool, sandbox_id, stored).await?;
                    let (_, token) = lifecycle_and_token(pool, sandbox_id).await?;
                    assert_eq!(
                        token,
                        Some(lease.token),
                        "arrange must keep the token for {stored:?}"
                    );
                    let released = substrate
                        .release_direct_restore_lease(sandbox_id, lease)
                        .await;
                    (released.is_ok(), "suspended")
                }
                Writer::ReleaseAfterWrite => {
                    let lease = substrate.claim_direct_restore_lease(sandbox_id).await?;
                    set_lifecycle.execute(pool).await?;
                    let released = substrate
                        .release_direct_restore_lease(sandbox_id, lease)
                        .await;
                    (released.is_ok(), "suspended")
                }
            };
            let (lifecycle, _) = lifecycle_and_token(pool, sandbox_id).await?;
            let expected = if applies { moved_to } else { stored };
            if applied != applies || lifecycle != expected {
                mismatches.push(format!(
                    "{writer:?} from {stored:?}: applied {applied}, row now {lifecycle:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_attempt_renewal_extends_the_owned_lease() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, None).await?;
    let lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&fixture.pool)
    .await?;

    fixture
        .substrate
        .renew_direct_restore_lease(sandbox_id, lease)
        .await?;

    let remaining_seconds = sqlx::query_scalar::<_, f64>(sql!(
        "SELECT EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW()))::float8
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert!(remaining_seconds > DIRECT_NODE_RPC_TIMEOUT.as_secs_f64());
    Ok(())
}

async fn postgres_fixture() -> TestResult<Option<PostgresFixture>> {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(None);
    };
    let pool = db.pool.clone();

    Ok(Some(PostgresFixture {
        substrate: DirectSubstrate::new(pool.clone()),
        pool,
        _db: db,
    }))
}

#[tokio::test(flavor = "current_thread")]
async fn direct_create_rejects_node_pool_placement() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let actor = super::super::ActorRef::new(
        UserId::new(),
        OrgId::new(),
        Email::parse("direct-placement@example.com".to_string())?,
    );
    let template = TemplateName::parse("seed")?;
    let template_ref = super::super::TemplateRef {
        template: &template,
        content_hash: Some("hash-seed"),
        boot_mode: CreateBootMode::Cold,
        runtime: "fc",
    };
    let resources = super::super::SandboxResources {
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
    };
    let env = std::collections::BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let network_policy = NetworkPolicyV1::deny_all();
    let node_pool = NodePoolName::parse("isolated-test-pool")?;

    let result = fixture
        .substrate
        .create_execution(CreateExecutionRequest {
            metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                "create",
            ),
            sandbox_id,
            actor: &actor,
            template_ref: &template_ref,
            resources: &resources,
            env: &env,
            auto_lifecycle: &auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 1,
            template_network_policy: &network_policy,
            template_network_policy_generation: 1,
            disk_io_limits: Default::default(),
            restore_from: None,
            wait_until_ready: true,
            volume_encryption: false,
            node_pool: Some(&node_pool),
        })
        .await;
    let error = match result {
        Ok(_) => panic!("direct substrate must reject Kubernetes node-pool placement"),
        Err(error) => error,
    };

    assert!(matches!(error, SubstrateError::Unsupported { .. }));
    Ok(())
}

async fn insert_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, template_id, content_hash,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space',
             'suspended',
             'unplaced',
             NULL,
             $2,
             'seed',
             'hash-seed',
             1
         )"
    ))
    .bind(sandbox_id)
    .bind(source_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_ready_worker(
    pool: &PgPool,
    node_id: NodeId,
    endpoint: &str,
    state_format_version: &str,
) -> TestResult {
    insert_ready_worker_for_runtime(
        pool,
        node_id,
        endpoint,
        "fc",
        "1.15.1",
        "firecracker-snapshot",
        state_format_version,
    )
    .await
}

async fn insert_ready_worker_for_runtime(
    pool: &PgPool,
    node_id: NodeId,
    endpoint: &str,
    runtime: &str,
    version: &str,
    state_format: &str,
    state_format_version: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint)
         VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(endpoint)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, health_healthy, services, updated_at
         )
         VALUES (
             $1, NOW(), NOW() + INTERVAL '1 hour', TRUE, 'worker',
             'ready', TRUE, '[]'::JSONB, NOW()
         )"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_installations
             (node_id, runtime, runtime_version)
         VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(runtime)
    .bind(version)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_guests
             (node_id, runtime, runtime_version, guest_os, guest_arch)
         VALUES ($1, $2, $3, 'linux', 'aarch64')"
    ))
    .bind(node_id)
    .bind(runtime)
    .bind(version)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_state_formats (
             node_id, runtime, runtime_version, state_format_name,
             state_format_version, can_create, can_load
         )
         VALUES ($1, $2, $3, $4, $5, TRUE, TRUE)"
    ))
    .bind(node_id)
    .bind(runtime)
    .bind(version)
    .bind(state_format)
    .bind(state_format_version)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_template_inventory(
    pool: &PgPool,
    node_id: NodeId,
    template: &str,
    content_hash: &str,
    state_format_version: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, $2, NULL, '00000000-0000-0000-0000-000000000001')
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .bind(template)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime, arch,
             has_base_snapshot, vcpu, memory_mib, guest_os,
             state_format_name, state_format_version
         )
         VALUES (
             $1, $2, $3, 'fc', 'aarch64',
             TRUE, 1, 512, 'linux',
             'firecracker-snapshot', $4
         )"
    ))
    .bind(node_id)
    .bind(template)
    .bind(content_hash)
    .bind(state_format_version)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_installed_component(
    pool: &PgPool,
    node_id: NodeId,
    kind: &str,
    role_key: &str,
    digest: Option<&str>,
    version: Option<&str>,
    format: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_artifact_components (
             node_id, kind, role_key, version_key, digest_key, format_key,
             version, digest, format, default_for_new, legacy_default
         )
         VALUES (
             $1, $2, $3,
             COALESCE($4, ''), COALESCE($5, ''), COALESCE($6, ''),
             $4, $5, $6, TRUE, TRUE
         )"
    ))
    .bind(node_id)
    .bind(kind)
    .bind(role_key)
    .bind(version)
    .bind(digest)
    .bind(format)
    .execute(pool)
    .await?;
    Ok(())
}

// Seeds only the columns the picker still reads from the observed row
// (existence + placement); the compatibility facts live on `snapshots`.
async fn insert_observed_source(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, status, template, snapshots_enabled
         )
         VALUES ($1, $2, 'suspended', 'seed', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_direct_route_provenance(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance (
             sandbox_id,
             last_route_generation,
             last_execution_generation,
             updated_at
         )
         VALUES ($1, $2, $2, NOW())"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    state_format_version: &str,
    state: &str,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id,
             template_id, content_hash, runtime, runtime_version, guest_os,
             guest_arch, state_format_name, state_format_version
         )
         VALUES (
             $1, $2, 'full', $5, $3,
             '00000000-0000-0000-0000-000000000001',
             'seed', 'hash-seed', 'fc', '1.15.1', 'linux',
             'aarch64', 'firecracker-snapshot', $4
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(state_format_version)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

// A pre-execution-metadata snapshot row: no runtime/guest/state-format
// facts, no execution_metadata document.
async fn insert_legacy_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id,
             template_id, content_hash
         )
         VALUES (
             $1, $2, 'full', 'ready', $3,
             '00000000-0000-0000-0000-000000000001',
             'seed', 'hash-seed'
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

async fn insert_ready_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    state_format_version: &str,
    storage_uri: Option<&str>,
) -> TestResult<SnapshotId> {
    let snapshot_id = insert_full_snapshot(
        pool,
        sandbox_id,
        source_node_id,
        state_format_version,
        "ready",
    )
    .await?;
    if let Some(storage_uri) = storage_uri {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations
                 (snapshot_id, storage_connection_string, org_id)
             VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
        ))
        .bind(snapshot_id)
        .bind(storage_uri)
        .execute(pool)
        .await?;
    }
    Ok(snapshot_id)
}

async fn insert_snapshot_component(
    pool: &PgPool,
    snapshot_id: SnapshotId,
    kind: &str,
    role: &str,
    digest: Option<&str>,
    version: Option<&str>,
    format: Option<&str>,
) -> TestResult {
    // Append one component to the snapshot row's execution_metadata,
    // mirroring the shape `ExecutionMetadata` serializes: optional fields
    // are omitted keys (never JSON null), and the role is keyed `role` —
    // the candidate query derives the relational-era role_key from it.
    let mut component = serde_json::Map::new();
    component.insert("kind".to_string(), kind.into());
    if !role.is_empty() {
        component.insert("role".to_string(), role.into());
    }
    if let Some(digest) = digest {
        component.insert("digest".to_string(), digest.into());
    }
    if let Some(version) = version {
        component.insert("version".to_string(), version.into());
    }
    if let Some(format) = format {
        component.insert("format".to_string(), format.into());
    }
    sqlx::query(sql!(
        "UPDATE snapshots
         SET execution_metadata = jsonb_set(
                 COALESCE(execution_metadata, '{}'::jsonb),
                 '{components}',
                 COALESCE(execution_metadata->'components', '[]'::jsonb)
                     || $2::jsonb
             )
         WHERE id = $1"
    ))
    .bind(snapshot_id)
    .bind(serde_json::Value::Object(component).to_string())
    .execute(pool)
    .await?;
    Ok(())
}

fn creating_record(sandbox_id: SandboxId) -> SandboxRecord {
    SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".to_string(),
        "fc".to_string(),
        "root".to_string(),
        Some("hash-seed".to_string()),
        false,
    )
}

async fn start_sim_worker(node_id: NodeId) -> TestResult<(SimWorkerHarness, SimState)> {
    // Callers enter the simulator test environment before constructing clients,
    // whose bearer interceptor requires credentials even for an in-process worker.
    let harness = SimWorkerHarness::builder(node_id.to_string())
        .with_internal_api()
        .start()
        .await?;
    let state = harness.state().clone();
    Ok((harness, state))
}

#[tokio::test(flavor = "current_thread")]
async fn create_base_snapshot_candidates_prefilter_on_state_format_major() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let template = "seed";
    let content_hash = "hash-seed";
    let compatible_node_id = NodeId::new();
    let other_generation_node_id = NodeId::new();
    insert_ready_worker(&fixture.pool, compatible_node_id, "127.0.0.1:1", "10.1.0").await?;
    insert_ready_worker(
        &fixture.pool,
        other_generation_node_id,
        "127.0.0.1:2",
        "9.0.0",
    )
    .await?;
    insert_template_inventory(
        &fixture.pool,
        compatible_node_id,
        template,
        content_hash,
        "10.0.0",
    )
    .await?;
    insert_template_inventory(
        &fixture.pool,
        other_generation_node_id,
        template,
        content_hash,
        "10.0.0",
    )
    .await?;

    let candidates = fixture
        .substrate
        .cached_worker_create_candidates(
            template,
            Some(content_hash),
            "fc",
            CreateBootMode::BaseSnapshot,
        )
        .await?;

    let candidate_nodes = candidates
        .iter()
        .map(|candidate| candidate.id)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(candidate_nodes.len(), 1);
    assert!(candidate_nodes.contains(&compatible_node_id));
    assert!(!candidate_nodes.contains(&other_generation_node_id));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_select_source_node_first_without_storage_uri() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "9.0.0").await?;
    insert_installed_component(
        &fixture.pool,
        source_node_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    // Upload-failed snapshot: never a remote candidate (no storage
    // location, not 'ready'), but its execution facts still qualify the
    // source node, which restores from its local artifact.
    let snapshot_id =
        insert_full_snapshot(&fixture.pool, sandbox_id, source_node_id, "9.0.0", "failed").await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    let first = candidates.first().expect("source candidate selected");
    assert_eq!(first.kind, DirectRestoreCandidateKind::SourceNode);
    assert_eq!(first.node.id, source_node_id);
    assert_eq!(first.storage_uri, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn deleting_newest_snapshot_blocks_stale_remote_fallback() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "8.0.0").await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/latest"),
    )
    .await?;
    let claimed = insert_full_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        "deleting",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET timestamp = (SELECT timestamp + INTERVAL '1 second'
                               FROM snapshots
                              WHERE id = $2)
          WHERE id = $1"
    ))
    .bind(claimed)
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert!(
        candidates.is_empty(),
        "the claimed newest artifact must block fallback to an older remote snapshot"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_restore_claim_preserves_suspended_source_semantics() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    for (case, (source_shape, artifact_kind)) in [("markerless", None), ("disk", Some("disk"))]
        .into_iter()
        .enumerate()
    {
        let sandbox_id = SandboxId::new();
        let source_node_id = NodeId::new();
        let alternate_node_id = NodeId::new();
        insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
        let source_endpoint = format!("127.0.0.1:{}", case * 2 + 1);
        let alternate_endpoint = format!("127.0.0.1:{}", case * 2 + 2);
        insert_ready_worker(&fixture.pool, source_node_id, &source_endpoint, "8.0.0").await?;
        insert_ready_worker(
            &fixture.pool,
            alternate_node_id,
            &alternate_endpoint,
            "9.0.0",
        )
        .await?;
        let storage_uri = format!("s3://bucket/snapshots/sandbox/{source_shape}");
        let snapshot_id = insert_ready_snapshot(
            &fixture.pool,
            sandbox_id,
            source_node_id,
            "9.0.0",
            Some(&storage_uri),
        )
        .await?;
        if let Some(artifact_kind) = artifact_kind {
            let artifact_id = Uuid::now_v7();
            sqlx::query(sql!(
                "UPDATE snapshots
                    SET local_artifact_id = $2,
                        artifact_kind = $3,
                        artifact_origin = 'suspend'
                  WHERE id = $1"
            ))
            .bind(snapshot_id)
            .bind(artifact_id)
            .bind(artifact_kind)
            .execute(&fixture.pool)
            .await?;
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET current_local_artifact_id = $2,
                        current_local_artifact_kind = $3,
                        current_local_artifact_origin = 'suspend'
                  WHERE id = $1"
            ))
            .bind(sandbox_id)
            .bind(artifact_id)
            .bind(artifact_kind)
            .execute(&fixture.pool)
            .await?;
        }

        let lease = fixture
            .substrate
            .claim_direct_restore_lease(sandbox_id)
            .await?;
        assert!(
            !lease.restore_retry,
            "{source_shape} source is a fresh restore"
        );

        let candidates = fixture
            .substrate
            .direct_restore_candidates_or_unavailable(sandbox_id, lease)
            .await?;

        let candidate = candidates
            .iter()
            .find(|candidate| candidate.node.id == alternate_node_id)
            .unwrap_or_else(|| panic!("{source_shape} source was lost"));
        assert_eq!(candidate.kind, DirectRestoreCandidateKind::RemoteSnapshot);
        assert_eq!(candidate.storage_uri.as_deref(), Some(storage_uri.as_str()));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restoring_direct_selector_ignores_non_authoritative_newer_attempts() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "9.0.0").await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    let source_snapshot = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/source"),
    )
    .await?;
    let artifact_id = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE snapshots
            SET local_artifact_id = $2,
                artifact_kind = 'full',
                artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(source_snapshot)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        source_snapshot,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        source_node_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;

    let first_lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    let original_restore_boundary: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    let later_snapshot = insert_full_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "10.0.0",
        "failed",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET timestamp = CASE id
                WHEN $1 THEN (SELECT lifecycle_changed_at - INTERVAL '1 second'
                                FROM sandboxes WHERE id = $3)
                WHEN $2 THEN (SELECT lifecycle_changed_at + INTERVAL '1 second'
                                FROM sandboxes WHERE id = $3)
            END
          WHERE id IN ($1, $2)"
    ))
    .bind(source_snapshot)
    .bind(later_snapshot)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;

    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1
            AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(first_lease.token)
    .execute(&fixture.pool)
    .await?;
    let retry_lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    let retry_restore_boundary: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(retry_restore_boundary, original_restore_boundary);

    let candidates = fixture
        .substrate
        .direct_restore_retry_candidates(sandbox_id)
        .await?;
    let post_claim_source = candidates
        .iter()
        .find(|candidate| candidate.kind == DirectRestoreCandidateKind::RemoteSnapshot)
        .expect("pre-restore source candidate is present");
    assert_eq!(
        post_claim_source.storage_uri.as_deref(),
        Some("s3://bucket/snapshots/sandbox/source")
    );
    let source = candidates
        .iter()
        .find(|candidate| {
            candidate.node.id == source_node_id
                && candidate.kind == DirectRestoreCandidateKind::SourceNode
        })
        .expect("source node compatibility uses the pre-restore snapshot");
    assert_eq!(source.storage_uri, None);

    sqlx::query(sql!(
        "UPDATE snapshots
            SET state = 'deleting',
                state_format_version = '9.0.0',
                timestamp = (SELECT lifecycle_changed_at - INTERVAL '500 milliseconds'
                               FROM sandboxes WHERE id = $2)
          WHERE id = $1"
    ))
    .bind(later_snapshot)
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let candidates_with_newer_claim = fixture
        .substrate
        .direct_restore_retry_candidates(sandbox_id)
        .await?;
    assert!(
        candidates_with_newer_claim
            .iter()
            .all(|candidate| candidate.kind != DirectRestoreCandidateKind::RemoteSnapshot),
        "a claimed newer snapshot must block stale remote fallback until finalization"
    );
    assert!(
        candidates_with_newer_claim
            .iter()
            .any(|candidate| candidate.kind == DirectRestoreCandidateKind::SourceNode),
        "the claimed snapshot's local source remains a valid fast path"
    );

    sqlx::query(sql!(
        "UPDATE snapshots newer
            SET state = 'failed',
                local_artifact_id = source.local_artifact_id,
                artifact_kind = source.artifact_kind,
                artifact_origin = source.artifact_origin
           FROM snapshots source
          WHERE newer.id = $1
            AND source.id = $2"
    ))
    .bind(later_snapshot)
    .bind(source_snapshot)
    .execute(&fixture.pool)
    .await?;
    let candidates_with_failed_retry = fixture
        .substrate
        .direct_restore_retry_candidates(sandbox_id)
        .await?;
    let retry_source = candidates_with_failed_retry
        .iter()
        .find(|candidate| candidate.kind == DirectRestoreCandidateKind::RemoteSnapshot)
        .expect("ready copy remains usable after a failed retry of the same artifact");
    assert_eq!(
        retry_source.storage_uri.as_deref(),
        Some("s3://bucket/snapshots/sandbox/source")
    );
    fixture
        .substrate
        .release_direct_restore_lease(sandbox_id, retry_lease)
        .await?;
    let (released_lifecycle, released_boundary, released_expiry, released_token): (
        String,
        Option<time::OffsetDateTime>,
        Option<time::OffsetDateTime>,
        Option<Uuid>,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle, lifecycle_changed_at,
                restore_lease_expires_at, restore_lease_token
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(released_lifecycle, "restoring");
    assert_eq!(released_boundary, original_restore_boundary);
    assert_eq!(released_token, Some(retry_lease.token));
    assert!(
        released_expiry.is_some_and(|expiry| expiry > time::OffsetDateTime::now_utc()),
        "a failed retry release preserves its quiet window"
    );

    let error = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await
        .expect_err("the retry quiet window blocks an immediate claim");
    assert!(matches!(
        error,
        SubstrateError::RestoreInFlight {
            lease_remaining: Some(_),
            ..
        }
    ));
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let next_claim = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    assert!(next_claim.restore_retry);
    fixture
        .substrate
        .release_direct_restore_lease(sandbox_id, next_claim)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restoring_direct_selector_rejects_stale_ready_source() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "8.0.0").await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    let ready_snapshot = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/stale"),
    )
    .await?;
    let failed_snapshot =
        insert_full_snapshot(&fixture.pool, sandbox_id, source_node_id, "9.0.0", "failed").await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET timestamp = CASE id
                WHEN $1 THEN NOW() - INTERVAL '2 seconds'
                WHEN $2 THEN NOW() - INTERVAL '1 second'
            END
          WHERE id IN ($1, $2)"
    ))
    .bind(ready_snapshot)
    .bind(failed_snapshot)
    .execute(&fixture.pool)
    .await?;

    let lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;
    fixture
        .substrate
        .release_direct_restore_lease(sandbox_id, lease)
        .await?;

    assert!(
        candidates.is_empty(),
        "a newer pre-claim failed snapshot must not expose an older ready source"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_source_candidate_prefilters_on_state_format_major() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "10.1.0").await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    insert_full_snapshot(&fixture.pool, sandbox_id, source_node_id, "10.0.0", "ready").await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].kind, DirectRestoreCandidateKind::SourceNode);
    assert_eq!(candidates[0].node.id, source_node_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_source_candidate_requires_snapshot_execution_facts() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "9.0.0").await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    insert_legacy_full_snapshot(&fixture.pool, sandbox_id, source_node_id).await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert!(
        candidates.is_empty(),
        "a snapshot without execution facts qualifies no candidate"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_ignore_snapshot_boot_kernel_mismatch() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/kernel-mismatch"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "kernel",
        "boot",
        Some("sha256:old-kernel"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "kernel",
        "boot",
        Some("sha256:new-kernel"),
        None,
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].node.id, alternate_node_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_reject_non_kernel_component_mismatch() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/rootfs-mismatch"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        Some("sha256:old-rootfs"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "system",
        Some("sha256:new-rootfs"),
        None,
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert!(candidates.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_match_components_by_version_and_format_without_digest() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let matching_node_id = NodeId::new();
    let mismatched_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, matching_node_id, "127.0.0.1:2", "9.0.0").await?;
    insert_ready_worker(&fixture.pool, mismatched_node_id, "127.0.0.1:3", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/version-format"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        None,
        Some("2.1.0"),
        Some("erofs"),
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        matching_node_id,
        "rootfs",
        "system",
        Some("sha256:ignored-when-request-has-no-digest"),
        Some("2.1.0"),
        Some("erofs"),
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        mismatched_node_id,
        "rootfs",
        "system",
        None,
        Some("2.0.0"),
        Some("erofs"),
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].node.id, matching_node_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_treat_empty_component_list_as_compatible() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/empty-components"),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots
         SET execution_metadata = jsonb_build_object('components', '[]'::jsonb)
         WHERE id = $1"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].node.id, alternate_node_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_match_roleless_component_against_blank_role_key() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/roleless"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "",
        Some("sha256:roleless-rootfs"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "",
        Some("sha256:roleless-rootfs"),
        None,
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].node.id, alternate_node_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_exclude_node_when_component_has_no_kind() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/kindless"),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots
         SET execution_metadata = jsonb_set(
                 COALESCE(execution_metadata, '{}'::jsonb),
                 '{components}',
                 COALESCE(execution_metadata->'components', '[]'::jsonb)
                     || jsonb_build_array(jsonb_build_object(
                            'role', 'system',
                            'digest', 'sha256:no-kind'))
             )
         WHERE id = $1"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "system",
        Some("sha256:no-kind"),
        None,
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert!(
        candidates.is_empty(),
        "a component with no kind can never match an installed component, \
         so the node must be excluded (fail closed)"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_exclude_node_missing_requested_version() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/version-null-installed"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        None,
        Some("2.1.0"),
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "system",
        None,
        None,
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert!(
        candidates.is_empty(),
        "an installed component with no version cannot satisfy a versioned request"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_digest_match_wins_over_version_mismatch() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/digest-wins"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        Some("sha256:digest-wins"),
        Some("2.1.0"),
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "system",
        Some("sha256:digest-wins"),
        Some("9.9.9"),
        None,
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    assert_eq!(
        candidates.len(),
        1,
        "a digest match satisfies the component regardless of version"
    );
    assert_eq!(candidates[0].node.id, alternate_node_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_prefilter_on_state_format_major() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let exact_state_node_id = NodeId::new();
    let newer_state_node_id = NodeId::new();
    let other_generation_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, exact_state_node_id, "127.0.0.1:1", "9.1.0").await?;
    insert_ready_worker(&fixture.pool, newer_state_node_id, "127.0.0.1:2", "9.2.0").await?;
    insert_ready_worker(
        &fixture.pool,
        other_generation_node_id,
        "127.0.0.1:3",
        "10.2.0",
    )
    .await?;
    insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.1.0",
        Some("s3://bucket/snapshots/sandbox/semver"),
    )
    .await?;

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;

    let candidate_nodes = candidates
        .iter()
        .map(|candidate| candidate.node.id)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(candidate_nodes.len(), 2);
    assert!(candidate_nodes.contains(&exact_state_node_id));
    assert!(candidate_nodes.contains(&newer_state_node_id));
    assert!(!candidate_nodes.contains(&other_generation_node_id));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_preflight_reports_unexpired_owner_before_candidate_availability() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let lease_token = Uuid::now_v7();
    insert_sandbox(&fixture.pool, sandbox_id, None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring',
                restore_lease_expires_at = NOW() + INTERVAL '1 hour',
                restore_lease_token = $2
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(lease_token)
    .execute(&fixture.pool)
    .await?;

    let error = match fixture
        .substrate
        .restore_execution_with_candidates(
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
        )
        .await
    {
        Ok(_) => panic!("an unexpired owner must win over candidate availability"),
        Err(error) => error,
    };

    let SubstrateError::RestoreInFlight {
        lease_remaining: Some(lease_remaining),
        ..
    } = error
    else {
        panic!("expected the owner's lease-derived retry contract, got {error}");
    };
    assert!(lease_remaining > Duration::from_secs(55 * 60));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_without_source_or_ready_storage_uri_does_not_claim_a_lease() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "8.0.0").await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    insert_ready_snapshot(&fixture.pool, sandbox_id, source_node_id, "9.0.0", None).await?;
    let boundary_before: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;

    let error = match fixture
        .substrate
        .restore_execution_with_candidates(
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
        )
        .await
    {
        Ok(_) => panic!("restore without source compatibility or remote URI must be retryable"),
        Err(error) => error,
    };

    assert!(matches!(error, SubstrateError::Unavailable { .. }));
    assert!(
        error
            .to_string()
            .contains("no compatible direct restore candidates")
    );
    let (lifecycle, lease, boundary_after): (String, Option<Uuid>, Option<time::OffsetDateTime>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, restore_lease_token, lifecycle_changed_at
           FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(lease, None);
    assert_eq!(boundary_after, boundary_before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failed_restore_retry_preserves_the_restore_boundary() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "8.0.0").await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/retry"),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;

    let first_lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    let boundary_before: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1
            AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(first_lease.token)
    .execute(&fixture.pool)
    .await?;

    let error = match fixture
        .substrate
        .restore_execution_with_candidates(
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
        )
        .await
    {
        Ok(_) => panic!("an incompatible retry source must remain retryable"),
        Err(error) => error,
    };

    assert!(matches!(error, SubstrateError::Unavailable { .. }));
    let (lifecycle, lease, boundary_after): (String, Option<Uuid>, Option<time::OffsetDateTime>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, restore_lease_token, lifecycle_changed_at
           FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(lifecycle, "restoring");
    assert_eq!(lease, Some(first_lease.token));
    assert_eq!(boundary_after, boundary_before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_create_receipt_failure_deletes_node_local_sandbox() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let registered_node_id = NodeId::new();
    let (server, state) = start_sim_worker(registered_node_id).await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(registered_node_id)).await?;
    insert_ready_worker(
        &fixture.pool,
        registered_node_id,
        server.endpoint(),
        "9.0.0",
    )
    .await?;
    sqlx::query("DROP TABLE sandbox_direct_route_identities")
        .execute(&fixture.pool)
        .await?;
    insert_template_inventory(
        &fixture.pool,
        registered_node_id,
        "seed",
        "hash-seed",
        "9.0.0",
    )
    .await?;

    let actor = super::super::ActorRef::new(
        UserId::new(),
        OrgId::new(),
        Email::parse("direct-create@example.com".to_string())?,
    );
    let template = TemplateName::parse("seed")?;
    let template_ref = super::super::TemplateRef {
        template: &template,
        content_hash: Some("hash-seed"),
        boot_mode: CreateBootMode::Cold,
        runtime: "fc",
    };
    let resources = super::super::SandboxResources {
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
    };
    let env = std::collections::BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let network_policy = NetworkPolicyV1::deny_all();

    let result = fixture
        .substrate
        .create_execution(CreateExecutionRequest {
            metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                "create",
            ),
            sandbox_id,
            actor: &actor,
            template_ref: &template_ref,
            resources: &resources,
            env: &env,
            auto_lifecycle: &auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 1,
            template_network_policy: &network_policy,
            template_network_policy_generation: 1,
            disk_io_limits: Default::default(),
            restore_from: None,
            wait_until_ready: true,
            volume_encryption: false,
            node_pool: None,
        })
        .await;

    assert!(
        result.is_err(),
        "route identity persistence failure must fail the create"
    );

    assert!(
        state.get(&sandbox_id.to_string()).await.is_err(),
        "receipt failure must not orphan the created sandbox on the worker"
    );
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_create_identity_preparation_failure_deletes_node_local_sandbox() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    insert_template_inventory(&fixture.pool, node_id, "seed", "hash-seed", "9.0.0").await?;
    sqlx::query("DROP TABLE sandbox_direct_route_provenance")
        .execute(&fixture.pool)
        .await?;

    let actor = super::super::ActorRef::new(
        UserId::new(),
        OrgId::new(),
        Email::parse("direct-create-preparation@example.com".to_string())?,
    );
    let template = TemplateName::parse("seed")?;
    let template_ref = super::super::TemplateRef {
        template: &template,
        content_hash: Some("hash-seed"),
        boot_mode: CreateBootMode::Cold,
        runtime: "fc",
    };
    let resources = super::super::SandboxResources {
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
    };
    let env = std::collections::BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let network_policy = NetworkPolicyV1::deny_all();

    let result = fixture
        .substrate
        .create_execution(CreateExecutionRequest {
            metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                "create",
            ),
            sandbox_id,
            actor: &actor,
            template_ref: &template_ref,
            resources: &resources,
            env: &env,
            auto_lifecycle: &auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 1,
            template_network_policy: &network_policy,
            template_network_policy_generation: 1,
            disk_io_limits: Default::default(),
            restore_from: None,
            wait_until_ready: false,
            volume_encryption: false,
            node_pool: None,
        })
        .await;

    assert!(
        result.is_err(),
        "route identity preparation failure must fail the create"
    );
    assert!(
        state.get(&sandbox_id.to_string()).await.is_err(),
        "route identity preparation failure must not orphan the sandbox on the worker"
    );
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn two_step_direct_create_initializes_identity_for_initial_start() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, _state) = start_sim_worker(node_id).await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    insert_template_inventory(&fixture.pool, node_id, "seed", "hash-seed", "9.0.0").await?;

    let actor = super::super::ActorRef::new(
        UserId::new(),
        OrgId::new(),
        Email::parse("two-step-direct-create@example.com".to_string())?,
    );
    let template = TemplateName::parse("seed")?;
    let template_ref = super::super::TemplateRef {
        template: &template,
        content_hash: Some("hash-seed"),
        boot_mode: CreateBootMode::Cold,
        runtime: "fc",
    };
    let resources = super::super::SandboxResources {
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
    };
    let env = std::collections::BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let network_policy = NetworkPolicyV1::deny_all();

    let outcome = fixture
        .substrate
        .create_execution(CreateExecutionRequest {
            metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
                "create",
            ),
            sandbox_id,
            actor: &actor,
            template_ref: &template_ref,
            resources: &resources,
            env: &env,
            auto_lifecycle: &auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 1,
            template_network_policy: &network_policy,
            template_network_policy_generation: 1,
            disk_io_limits: Default::default(),
            restore_from: None,
            wait_until_ready: false,
            volume_encryption: false,
            node_pool: None,
        })
        .await?;

    assert_eq!(outcome.binding.unwrap().execution_generation, 2);
    let persisted_generation = sqlx::query_scalar::<_, i64>(
        "SELECT execution_generation
           FROM sandbox_direct_route_identities
          WHERE sandbox_id = $1",
    )
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted_generation, 2);
    drop(server);
    Ok(())
}

/// A Direct controller over one sim worker that has `seed` cached, so a create
/// the controller lets through reaches the worker.
struct DirectCreateFixture {
    postgres: PostgresFixture,
    controller: crate::controllers::sandbox::SandboxController,
    actor: crate::models::User,
    _server: SimWorkerHarness,
    worker: SimState,
}

async fn direct_create_fixture() -> TestResult<Option<DirectCreateFixture>> {
    let Some(postgres) = postgres_fixture().await? else {
        return Ok(None);
    };
    let node_id = NodeId::new();
    let (server, worker) = start_sim_worker(node_id).await?;
    insert_ready_worker(&postgres.pool, node_id, server.endpoint(), "9.0.0").await?;
    insert_template_inventory(&postgres.pool, node_id, "seed", "hash-seed", "9.0.0").await?;
    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'seed', 'fc', 'hash-seed')"
    ))
    .bind(release_id)
    .execute(&postgres.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'seed', 'latest')"
    ))
    .bind(release_id)
    .execute(&postgres.pool)
    .await?;
    // The baseline's seeded user, which `insert_sandbox` also owns rows as.
    let actor = sqlx::query_as::<_, crate::models::User>(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
           FROM users
          WHERE id = '00000000-0000-0000-0000-000000000010'"
    ))
    .fetch_one(&postgres.pool)
    .await?;
    let controller = crate::controllers::sandbox::SandboxController::new(
        super::super::Substrate::from_direct(DirectSubstrate::new(postgres.pool.clone())),
    );
    Ok(Some(DirectCreateFixture {
        postgres,
        controller,
        actor,
        _server: server,
        worker,
    }))
}

/// A create that takes Direct's one-shot path: it waits for readiness and
/// boots cold.
fn one_shot_create_request() -> TestResult<crate::models::CreateSandboxRequest> {
    Ok(crate::models::CreateSandboxRequest {
        name: None,
        template: Some("seed:latest".parse()?),
        runtime: Some("fc".to_string()),
        boot_mode: Some("cold".to_string()),
        env: Default::default(),
        metadata: Default::default(),
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        file_changes: None,
        auto_lifecycle: None,
        wait_until_ready: true,
        network_policy: None,
        node_pool: None,
    })
}

impl DirectCreateFixture {
    async fn create(
        &self,
        request: &crate::models::CreateSandboxRequest,
    ) -> Result<crate::models::SandboxResponse, ControllerError> {
        self.controller
            .create(
                &self.postgres.pool,
                &self.actor,
                request,
                pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("create"),
                crate::controllers::sandbox::FederatedSandboxCreateContext::new(None, None),
            )
            .await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_shot_direct_create_boots_a_default_shape_on_the_worker() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = direct_create_fixture().await? else {
        return Ok(());
    };

    let response = fixture.create(&one_shot_create_request()?).await?;

    assert_eq!(response.lifecycle.as_deref(), Some("started"));
    fixture.worker.get(&response.id.to_string()).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ready_direct_create_boots_custom_shapes_on_the_worker() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = direct_create_fixture().await? else {
        return Ok(());
    };
    for boot_mode in [None, Some("cold")] {
        for (vcpu, memory_mib) in [(Some(4), None), (None, Some(2048)), (Some(4), Some(2048))] {
            let mut request = one_shot_create_request()?;
            request.boot_mode = boot_mode.map(str::to_owned);
            request.vcpu = vcpu;
            request.memory_mib = memory_mib;

            let response = fixture.create(&request).await?;

            assert_eq!(response.lifecycle.as_deref(), Some("started"));
            assert_eq!(
                fixture.worker.get(&response.id.to_string()).await?.status,
                "running"
            );
            let stored = sqlx::query_as::<_, (Option<i32>, Option<i32>)>(sql!(
                "SELECT vcpu, memory_mib FROM sandboxes WHERE id = $1"
            ))
            .bind(response.id.as_uuid())
            .fetch_one(&fixture.postgres.pool)
            .await?;
            assert_eq!(
                stored,
                (vcpu.map(|v| v as i32), memory_mib.map(|v| v as i32))
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn two_call_direct_create_boots_a_custom_shape_on_the_worker() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = direct_create_fixture().await? else {
        return Ok(());
    };
    // The shape the native create operation sends (`controller_create_request`):
    // no boot mode and no readiness wait, so the create and start are two calls.
    let mut request = one_shot_create_request()?;
    request.boot_mode = None;
    request.wait_until_ready = false;
    request.vcpu = Some(4);
    request.memory_mib = Some(2048);

    let response = fixture.create(&request).await?;

    fixture.worker.get(&response.id.to_string()).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn one_shot_direct_create_accepts_a_custom_disk() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = direct_create_fixture().await? else {
        return Ok(());
    };
    // One-shot create forwards disk size (`rootfs_size_mb`).
    let mut request = one_shot_create_request()?;
    request.disk_mib = Some(4096);

    let response = fixture.create(&request).await?;

    fixture.worker.get(&response.id.to_string()).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn live_cpu_update_preserves_allocation_across_cold_restart() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    let mut record = creating_record(sandbox_id);
    record.status = "running".to_string();
    state.insert_new(record).await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $2, placement = 'on_node', lifecycle = 'started', vcpu = 8
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    let controller = crate::controllers::sandbox::SandboxController::new(
        super::super::Substrate::from_direct(DirectSubstrate::new(fixture.pool.clone())),
    );
    for quota in [2, 8] {
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
            .await?
            .expect("sandbox row");
        controller
            .update(
                &fixture.pool,
                &sandbox,
                sandbox_id,
                crate::models::UpdateSandboxRequest {
                    metadata: None,
                    vcpu: Some(quota),
                    memory_mib: None,
                    pids_max: None,
                    auto_lifecycle: None,
                    network_policy: None,
                },
                None,
            )
            .await?;
        // The simulator supplies RPC completion; this test owns persistence,
        // not real cgroup enforcement, which the supervisor tests cover.
        assert_eq!(
            fixture.substrate.activation_vcpu(sandbox_id).await?,
            Some(8)
        );
        if quota == 2 {
            fixture
                .substrate
                .retire_execution(RetireExecutionRequest {
                    sandbox_id,
                    fence: super::super::RetireFence::Unobserved,
                })
                .await?;
            fixture
                .substrate
                .start_execution(
                    pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
                    sandbox_id,
                    ActivationOptions {
                        blocking: true,
                        ready_timeout: Duration::from_secs(5),
                    },
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_start_persists_complete_route_identity_receipt() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    let mut record = creating_record(sandbox_id);
    record.status = "stopped".to_string();
    state
        .insert_new(record)
        .await
        .map_err(|error| format!("seed direct start sandbox: {error}"))?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    insert_direct_route_provenance(&fixture.pool, sandbox_id, 1).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $2,
             placement = 'on_node',
             lifecycle = 'stopped'
         WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;

    let current_policy = crate::e2b::auto_lifecycle_policy(60, true, true);
    sqlx::query(sql!(
        "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(&current_policy))
    .execute(&fixture.pool)
    .await?;

    fixture
        .substrate
        .start_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await?;

    let identity: (i64, NodeId, String, i64, Uuid, Option<String>) = sqlx::query_as(sql!(
        "SELECT route_generation,
                    current_node_id,
                    substrate_kind,
                    execution_generation,
                    activation_attempt_id,
                    pod_uid
             FROM sandbox_direct_route_identities
             WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(identity.0, 2);
    assert_eq!(identity.1, node_id);
    assert_eq!(identity.2, "space");
    assert_eq!(identity.3, 2);
    assert_ne!(identity.4, Uuid::nil());
    assert_eq!(identity.5, None);
    let installed = state.get(&sandbox_id.to_string()).await?;
    let installed =
        <pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy as prost::Message>::decode(
            installed.access_idle_policy.as_slice(),
        )?;
    assert_eq!(AutoLifecyclePolicy::try_from(installed)?, current_policy);

    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_direct_route_receipt_cannot_replace_newer_execution_identity() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let current_attempt_id = Uuid::now_v7();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $2,
             placement = 'on_node'
         WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id,
             route_generation,
             current_node_id,
             substrate_kind,
             execution_generation,
             activation_attempt_id
         )
         VALUES ($1, 2, $2, 'space', 2, $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .bind(current_attempt_id)
    .execute(&fixture.pool)
    .await?;
    fixture
        .substrate
        .persist_route_identity_receipt(
            sandbox_id,
            node_id,
            &sandbox_id.to_string(),
            Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                route_generation: Some(1),
                current_node_id: Some(node_id.to_string()),
                substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct
                    as i32,
                execution_generation: Some(1),
                activation_attempt_id: Some(current_attempt_id.to_string()),
                ..Default::default()
            }),
            DirectRouteIdentityOperation::Start,
        )
        .await
        .expect_err("a stale route receipt must not replace a newer execution identity");

    let persisted: (i64, i64, Uuid) = sqlx::query_as(sql!(
        "SELECT route_generation, execution_generation, activation_attempt_id
         FROM sandbox_direct_route_identities
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted, (2, 2, current_attempt_id));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn newer_direct_activation_attempt_replaces_equal_generation_identity() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let older_attempt_id = Uuid::parse_str("0198f000-0000-7000-8000-000000000001")?;
    let newer_attempt_id = Uuid::parse_str("0198f000-0000-7000-8000-000000000002")?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $2,
             placement = 'on_node'
         WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id,
             route_generation,
             current_node_id,
             substrate_kind,
             execution_generation,
             activation_attempt_id
         )
         VALUES ($1, 1, $2, 'space', 1, $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .bind(older_attempt_id)
    .execute(&fixture.pool)
    .await?;

    fixture
        .substrate
        .persist_route_identity_receipt(
            sandbox_id,
            node_id,
            &sandbox_id.to_string(),
            Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                route_generation: Some(1),
                current_node_id: Some(node_id.to_string()),
                substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct
                    as i32,
                execution_generation: Some(1),
                activation_attempt_id: Some(newer_attempt_id.to_string()),
                ..Default::default()
            }),
            DirectRouteIdentityOperation::Start,
        )
        .await?;

    let persisted_attempt_id: Uuid = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id
         FROM sandbox_direct_route_identities
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted_attempt_id, newer_attempt_id);

    fixture
        .substrate
        .persist_route_identity_receipt(
            sandbox_id,
            node_id,
            &sandbox_id.to_string(),
            Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                route_generation: Some(1),
                current_node_id: Some(node_id.to_string()),
                substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct
                    as i32,
                execution_generation: Some(1),
                activation_attempt_id: Some(older_attempt_id.to_string()),
                ..Default::default()
            }),
            DirectRouteIdentityOperation::Start,
        )
        .await
        .expect_err("an older equal-generation attempt must remain stale");
    let persisted_attempt_id: Uuid = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id
         FROM sandbox_direct_route_identities
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted_attempt_id, newer_attempt_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_route_provenance_survives_active_receipt_clear() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;

    fixture
        .substrate
        .persist_route_identity_receipt(
            sandbox_id,
            node_id,
            &sandbox_id.to_string(),
            Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                route_generation: Some(7),
                current_node_id: Some(node_id.to_string()),
                substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct
                    as i32,
                execution_generation: Some(7),
                activation_attempt_id: Some(Uuid::now_v7().to_string()),
                ..Default::default()
            }),
            DirectRouteIdentityOperation::Start,
        )
        .await?;
    fixture
        .substrate
        .clear_direct_route_identity(sandbox_id)
        .await?;

    let retained: (i64, i64) = sqlx::query_as(sql!(
        "SELECT last_route_generation, last_execution_generation
         FROM sandbox_direct_route_provenance
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(retained, (7, 7));

    let target_node_id = NodeId::new();
    let reserved = fixture
        .substrate
        .reserve_direct_activation_route_identity(sandbox_id, target_node_id)
        .await?;
    assert_eq!(reserved.route_generation, Some(8));
    assert_eq!(reserved.execution_generation, Some(8));
    assert_eq!(
        reserved.current_node_id.as_deref(),
        Some(target_node_id.to_string().as_str())
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_receipt_clear_backfills_missing_provenance_before_delete() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let activation_attempt_id = Uuid::now_v7();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id,
             route_generation,
             current_node_id,
             substrate_kind,
             execution_generation,
             activation_attempt_id
         )
         VALUES ($1, 7, $2, 'space', 9, $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .bind(activation_attempt_id)
    .execute(&fixture.pool)
    .await?;

    fixture
        .substrate
        .clear_direct_route_identity(sandbox_id)
        .await?;

    let active_receipt_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)
         FROM sandbox_direct_route_identities
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_receipt_count, 0);
    let retained: (i64, i64) = sqlx::query_as(sql!(
        "SELECT last_route_generation, last_execution_generation
         FROM sandbox_direct_route_provenance
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(retained, (7, 9));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wake_noop_without_receipt_preserves_current_direct_identity() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    let activation_attempt_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id,
             route_generation,
             current_node_id,
             substrate_kind,
             execution_generation,
             activation_attempt_id
         )
         VALUES ($1, 7, $2, 'space', 7, $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id.as_uuid())
    .bind(activation_attempt_id)
    .execute(&fixture.pool)
    .await?;

    let preserved = fixture
        .substrate
        .persist_wake_route_identity(
            sandbox_id,
            node_id,
            &pplx_space_sbx_mgr_client::proto::WakeResponse {
                sandbox_id: sandbox_id.to_string(),
                transition: pplx_space_sbx_mgr_client::proto::WakeTransition::Noop as i32,
                route_identity: None,
            },
        )
        .await?
        .expect("a no-op wake must retain the current identity");

    assert_eq!(preserved.route_generation().get(), 7);
    let persisted: i64 = sqlx::query_scalar(sql!(
        "SELECT route_generation
           FROM sandbox_direct_route_identities
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted, 7);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_reservation_recovers_authoritative_source_node_provenance() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let (server, state) = start_sim_worker(source_node_id).await?;
    state
        .insert_new(creating_record(sandbox_id).with_route_identity_provenance(7, 9, None))
        .await?;
    insert_ready_worker(&fixture.pool, source_node_id, server.endpoint(), "9.0.0").await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;

    let target_node_id = NodeId::new();
    let reserved = fixture
        .substrate
        .reserve_direct_activation_route_identity(sandbox_id, target_node_id)
        .await?;

    assert_eq!(reserved.route_generation, Some(10));
    assert_eq!(reserved.execution_generation, Some(10));
    assert_eq!(
        reserved.current_node_id.as_deref(),
        Some(target_node_id.to_string().as_str())
    );
    let retained: (i64, i64) = sqlx::query_as(sql!(
        "SELECT last_route_generation, last_execution_generation
         FROM sandbox_direct_route_provenance
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(retained, (10, 10));
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_reservation_reconciles_a_seeded_floor_with_source_node_provenance() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let (server, state) = start_sim_worker(source_node_id).await?;
    state
        .insert_new(creating_record(sandbox_id).with_route_identity_provenance(7, 9, None))
        .await?;
    insert_ready_worker(&fixture.pool, source_node_id, server.endpoint(), "9.0.0").await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance (
             sandbox_id,
             last_route_generation,
             last_execution_generation
         ) VALUES ($1, 1, 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&fixture.pool)
    .await?;

    let reserved = fixture
        .substrate
        .reserve_direct_activation_route_identity(sandbox_id, NodeId::new())
        .await?;

    assert_eq!(reserved.route_generation, Some(10));
    assert_eq!(reserved.execution_generation, Some(10));
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_reservation_rejects_kubernetes_source_node_provenance() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let (server, state) = start_sim_worker(source_node_id).await?;
    state
        .insert_new(creating_record(sandbox_id).with_route_identity_provenance(
            7,
            7,
            Some(NodeManagedBy::Kubernetes),
        ))
        .await?;
    insert_ready_worker(&fixture.pool, source_node_id, server.endpoint(), "9.0.0").await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;

    let error = fixture
        .substrate
        .reserve_direct_activation_route_identity(sandbox_id, NodeId::new())
        .await
        .expect_err("Kubernetes-owned provenance must remain fail-closed");

    assert!(
        error
            .to_string()
            .contains("reports Kubernetes route ownership")
    );
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_reservation_lazily_bootstraps_a_legacy_sandbox_without_a_source_node() -> TestResult
{
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(NodeId::new())).await?;

    let reserved = fixture
        .substrate
        .reserve_direct_activation_route_identity(sandbox_id, NodeId::new())
        .await?;

    assert_eq!(reserved.route_generation, Some(2));
    assert_eq!(reserved.execution_generation, Some(2));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_start_rejects_wrong_worker_receipt_and_clears_stale_identity() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let registered_node_id = NodeId::new();
    let reported_node_id = NodeId::new();
    let (server, state) = start_sim_worker(reported_node_id).await?;
    let mut record = creating_record(sandbox_id);
    record.status = "stopped".to_string();
    state
        .insert_new(record)
        .await
        .map_err(|error| format!("seed direct start sandbox: {error}"))?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(registered_node_id)).await?;
    insert_ready_worker(
        &fixture.pool,
        registered_node_id,
        server.endpoint(),
        "9.0.0",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $2,
             placement = 'on_node',
             lifecycle = 'stopped'
         WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(registered_node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id,
             route_generation,
             current_node_id,
             substrate_kind,
             execution_generation,
             activation_attempt_id
         )
         VALUES ($1, 1, $2, 'space', 1, $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(registered_node_id)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    insert_direct_route_provenance(&fixture.pool, sandbox_id, 1).await?;

    fixture
        .substrate
        .start_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await
        .expect_err("a receipt for another worker must fail closed");

    let identity_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM sandbox_direct_route_identities
             WHERE sandbox_id = $1
         )"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert!(
        !identity_exists,
        "invalid receipt must clear stale authority"
    );
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_claim_rollback_clears_direct_route_identity() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    state
        .insert_new(creating_record(sandbox_id))
        .await
        .map_err(|error| format!("seed pooled direct sandbox: {error}"))?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id,
             route_generation,
             current_node_id,
             substrate_kind,
             execution_generation,
             activation_attempt_id
         )
         VALUES ($1, 1, $2, 'space', 1, $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;

    fixture
        .substrate
        .rollback_pooled_execution_claim(&PooledExecutionClaim {
            claim_id: Uuid::now_v7(),
            pool_slot_id: None,
            sandbox_id,
            content_hash: "hash-seed".to_string(),
            pod_name: sandbox_id.to_string(),
            pod_uid: None,
            execution_generation: 1,
            pod_ip: String::new(),
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
            disk_io_limits: Default::default(),
            active_node_id: Some(node_id),
            route_identity: None,
        })
        .await?;

    let identity_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM sandbox_direct_route_identities
             WHERE sandbox_id = $1
         )"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!identity_exists, "rollback must clear the strict receipt");
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_claim_persists_route_receipt_after_api_sandbox_insert() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    let route_identity = parse_direct_route_identity(
        sandbox_id,
        pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
            route_generation: Some(3),
            current_node_id: Some(node_id.to_string()),
            substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct as i32,
            execution_generation: Some(3),
            activation_attempt_id: Some(Uuid::now_v7().to_string()),
            ..Default::default()
        },
    )?;
    let claim = PooledExecutionClaim {
        claim_id: Uuid::now_v7(),
        pool_slot_id: None,
        sandbox_id,
        content_hash: "hash-seed".to_string(),
        pod_name: sandbox_id.to_string(),
        pod_uid: None,
        execution_generation: 3,
        pod_ip: String::new(),
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        disk_io_limits: Default::default(),
        active_node_id: Some(node_id),
        route_identity: Some(route_identity.clone()),
    };
    let actor = super::super::ActorRef::new(
        UserId::new(),
        OrgId::new(),
        Email::parse("pooled-receipt@example.com".to_string())?,
    );
    let env = std::collections::BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let policy = NetworkPolicyV1::deny_all();
    let request = PooledExecutionClaimRequest {
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
            "create",
        ),
        actor: &actor,
        template: "seed",
        content_hash: Some("hash-seed"),
        template_release_id: None,
        runtime: "fc",
        vcpu: None,
        memory_mib: None,
        env: &env,
        auto_lifecycle: &auto_lifecycle,
        network_policy_override: None,
        network_policy_generation: 1,
        template_network_policy: &policy,
        template_network_policy_generation: 1,
        volume_encryption: false,
    };
    let mut transaction = fixture.pool.begin().await?;

    fixture
        .substrate
        .persist_pooled_execution_claim(&mut transaction, &request, &claim)
        .await?;
    transaction.commit().await?;

    let persisted: (i64, i64) = sqlx::query_as(sql!(
        "SELECT route_generation, execution_generation
           FROM sandbox_direct_route_identities
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(persisted, (3, 3));
    Ok(())
}

#[rstest::rstest]
#[case::source_start_refused(true, true)]
#[case::source_unsupported(false, true)]
#[case::all_unsupported(false, false)]
#[tokio::test(flavor = "current_thread")]
async fn cross_node_restore_handles_candidate_failures(
    #[case] source_support: bool,
    #[case] alternate_support: bool,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    let mut source_builder =
        SimWorkerHarness::builder(source_node_id.to_string()).with_internal_api();
    if !source_support {
        source_builder = source_builder.without_activation_policy_support();
    }
    let source_server = source_builder.start().await?;
    let source_state = source_server.state().clone();
    let mut source_record = creating_record(sandbox_id);
    source_record.status = "running".to_string();
    source_state
        .insert_new(source_record)
        .await
        .map_err(|err| format!("seed source sim sandbox: {err}"))?;
    let mut alternate_builder =
        SimWorkerHarness::builder(alternate_node_id.to_string()).with_internal_api();
    if !alternate_support {
        alternate_builder = alternate_builder.without_activation_policy_support();
    }
    let alternate_server = alternate_builder.start().await?;
    let alternate_state = alternate_server.state().clone();
    alternate_state
        .insert_new(creating_record(sandbox_id))
        .await
        .map_err(|err| format!("seed alternate sim sandbox: {err}"))?;

    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_direct_route_provenance(&fixture.pool, sandbox_id, 1).await?;
    insert_ready_worker(
        &fixture.pool,
        source_node_id,
        source_server.endpoint(),
        "9.0.0",
    )
    .await?;
    insert_ready_worker(
        &fixture.pool,
        alternate_node_id,
        alternate_server.endpoint(),
        "9.0.0",
    )
    .await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/flow"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        source_node_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    insert_installed_component(
        &fixture.pool,
        alternate_node_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;

    let result = fixture
        .substrate
        .restore_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("restore"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await;
    if !alternate_support {
        let error = result.expect_err("no candidate supports activation policy");
        assert!(
            error
                .to_string()
                .contains("no direct restore candidates succeeded")
        );
        let ownership: (String, Option<Uuid>) = sqlx::query_as(sql!(
            "SELECT lifecycle, restore_lease_token FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(ownership, ("suspended".to_owned(), None));
        assert_eq!(
            source_state.get(&sandbox_id.to_string()).await?.status,
            "running"
        );
        assert_eq!(
            alternate_state.get(&sandbox_id.to_string()).await?.status,
            "creating"
        );
        return Ok(());
    }
    result?;

    let (active_node_id, target_node_id, placement, lifecycle): (
        Option<NodeId>,
        Option<NodeId>,
        String,
        String,
    ) = sqlx::query_as(sql!(
        "SELECT active_node_id, target_node_id, placement, lifecycle
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_node_id, Some(alternate_node_id));
    assert_eq!(target_node_id, Some(alternate_node_id));
    assert_eq!(placement, "on_node");
    assert_eq!(lifecycle, "started");
    let persisted_identity: Option<(i64, NodeId, String, i64, Uuid, Option<String>)> =
        sqlx::query_as(sql!(
            "SELECT route_generation,
                    current_node_id,
                    substrate_kind,
                    execution_generation,
                    activation_attempt_id,
                    pod_uid
             FROM sandbox_direct_route_identities
             WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&fixture.pool)
        .await?;
    let persisted_identity =
        persisted_identity.expect("successful direct restore persists route identity");
    assert_eq!(persisted_identity.0, if source_support { 3 } else { 2 });
    assert_eq!(persisted_identity.1, alternate_node_id);
    assert_eq!(persisted_identity.2, "space");
    assert_eq!(persisted_identity.3, if source_support { 3 } else { 2 });
    assert_ne!(persisted_identity.4, Uuid::nil());
    assert_eq!(persisted_identity.5, None);
    drop(source_server);
    drop(alternate_server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cross_node_restore_retains_source_after_receipt_persistence_failure() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    let mismatched_receipt_node_id = NodeId::new();
    let (source_server, source_state) = start_sim_worker(source_node_id).await?;
    let mut source_record = creating_record(sandbox_id);
    source_record.status = "running".to_string();
    source_state
        .insert_new(source_record)
        .await
        .map_err(|err| format!("seed source sim sandbox: {err}"))?;
    let (alternate_server, alternate_state) = start_sim_worker(mismatched_receipt_node_id).await?;
    alternate_state
        .insert_new(creating_record(sandbox_id))
        .await
        .map_err(|err| format!("seed alternate sim sandbox: {err}"))?;

    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_direct_route_provenance(&fixture.pool, sandbox_id, 1).await?;
    insert_ready_worker(
        &fixture.pool,
        source_node_id,
        source_server.endpoint(),
        "9.0.0",
    )
    .await?;
    insert_ready_worker(
        &fixture.pool,
        alternate_node_id,
        alternate_server.endpoint(),
        "9.0.0",
    )
    .await?;
    insert_observed_source(&fixture.pool, sandbox_id, source_node_id).await?;
    let snapshot_id = insert_ready_snapshot(
        &fixture.pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/bad-receipt"),
    )
    .await?;
    insert_snapshot_component(
        &fixture.pool,
        snapshot_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    for node_id in [source_node_id, alternate_node_id] {
        insert_installed_component(
            &fixture.pool,
            node_id,
            "rootfs",
            "system",
            Some("sha256:rootfs"),
            None,
            None,
        )
        .await?;
    }

    let error = fixture
        .substrate
        .restore_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("restore"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await
        .expect_err("mismatched destination receipt must fail closed");

    assert!(
        error.to_string().contains("route identity targets node"),
        "unexpected restore error: {error}"
    );
    assert!(
        source_state.get(&sandbox_id.to_string()).await.is_ok(),
        "receipt persistence failure must retain the last known-good source"
    );
    assert!(
        alternate_state.get(&sandbox_id.to_string()).await.is_err(),
        "receipt persistence failure must clean the untracked destination"
    );
    let (active_node_id, target_node_id, placement): (Option<NodeId>, Option<NodeId>, String) =
        sqlx::query_as(sql!(
            "SELECT active_node_id, target_node_id, placement
             FROM sandboxes
             WHERE id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, Some(source_node_id));
    assert_eq!(placement, "unplaced");
    drop(source_server);
    drop(alternate_server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_cross_node_restore_receipt_cannot_overwrite_newer_placement() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let stale_attempt_node_id = NodeId::new();
    let newer_attempt_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;

    let stale_lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    let stale_identity = pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
        route_generation: Some(2),
        current_node_id: Some(stale_attempt_node_id.to_string()),
        substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct as i32,
        execution_generation: Some(2),
        activation_attempt_id: Some(Uuid::now_v7().to_string()),
        ..Default::default()
    };
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&fixture.pool)
    .await?;
    let newer_lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2,
                target_node_id = $2,
                placement = 'on_node'
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(newer_attempt_node_id)
    .execute(&fixture.pool)
    .await?;
    let stale_response = pplx_space_sbx_mgr_client::proto::StartResponse {
        sandbox_id: sandbox_id.to_string(),
        route_identity: Some(stale_identity.clone()),
        ..Default::default()
    };
    let error = fixture
        .substrate
        .persist_owned_restore_route_identity_receipt(
            sandbox_id,
            stale_attempt_node_id,
            &stale_response,
            stale_identity,
            stale_lease,
        )
        .await
        .expect_err("an expired restore owner must not publish placement or provenance");
    assert!(matches!(error, SubstrateError::RestoreInFlight { .. }));

    let (active_node_id, target_node_id): (Option<NodeId>, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT active_node_id, target_node_id
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_node_id, Some(newer_attempt_node_id));
    assert_eq!(target_node_id, Some(newer_attempt_node_id));
    fixture
        .substrate
        .release_direct_restore_lease(sandbox_id, newer_lease)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn owned_restore_persists_the_supplied_identity_when_legacy_manager_omits_receipt()
-> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let destination_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    let lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    let supplied_identity = pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
        route_generation: Some(2),
        current_node_id: Some(destination_node_id.to_string()),
        substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct as i32,
        execution_generation: Some(2),
        activation_attempt_id: Some(Uuid::now_v7().to_string()),
        ..Default::default()
    };

    let persisted = fixture
        .substrate
        .persist_owned_restore_route_identity_receipt(
            sandbox_id,
            destination_node_id,
            &pplx_space_sbx_mgr_client::proto::StartResponse {
                sandbox_id: sandbox_id.to_string(),
                route_identity: None,
                ..Default::default()
            },
            supplied_identity,
            lease,
        )
        .await?
        .expect("the supplied identity must be retained during rolling skew");

    assert_eq!(persisted.current_node_id(), destination_node_id);
    let (active_node_id, target_node_id): (Option<NodeId>, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT active_node_id, target_node_id
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(active_node_id, Some(destination_node_id));
    assert_eq!(target_node_id, Some(destination_node_id));
    fixture
        .substrate
        .release_direct_restore_lease(sandbox_id, lease)
        .await?;
    Ok(())
}

/// The compensating-delete outcomes that count as "nothing left behind".
///
/// This predicate now gates a demotion, not just a log line: the two
/// route-identity cleanup arms in this file keep a contention abort retryable
/// when it answers `true` and demote it to a 500 when it answers `false`. A
/// wrong `false` costs a safe retry; a wrong `true` advertises a retry while a
/// sandbox is still live on the worker. So both directions are asserted, and
/// the `FailedPrecondition` pair is asserted on both sides of its substring
/// test rather than only the matching one.
#[test]
fn idempotent_rollback_delete_accepts_only_already_gone_or_going() {
    use tonic::{Code, Status};

    assert!(is_idempotent_pool_rollback_delete(&Status::new(
        Code::NotFound,
        "sandbox not found"
    )));
    assert!(is_idempotent_pool_rollback_delete(&Status::new(
        Code::FailedPrecondition,
        "sandbox is already deleting"
    )));
    // Same code, different reason — a precondition that is not a delete in
    // progress must not be read as one.
    assert!(!is_idempotent_pool_rollback_delete(&Status::new(
        Code::FailedPrecondition,
        "sandbox is suspending"
    )));
    // The codes that mean the node never confirmed anything.
    for code in [Code::Unavailable, Code::DeadlineExceeded, Code::Internal] {
        assert!(
            !is_idempotent_pool_rollback_delete(&Status::new(code, "sandbox is already deleting")),
            "{code:?} must not be accepted even when its message mentions deleting"
        );
    }
}

async fn place_suspended_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    cluster_policy: &AutoLifecyclePolicy,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'on_node',
                active_node_id = $2,
                auto_lifecycle_json = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(sqlx::types::Json(cluster_policy))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn implicit_access_wake_honors_the_workers_effective_policy() -> TestResult {
    use prost::Message;

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    let wake_on_access = crate::e2b::auto_lifecycle_policy(300, true, true);
    let no_wake = crate::e2b::auto_lifecycle_policy(300, true, false);

    // The cluster policy allows an Access wake for every case below. The
    // worker's effective policy, which may carry a local override, decides
    // whether the placed sandbox actually wakes.
    for (label, worker_policy, expect_wake) in [
        ("worker denies", Some(no_wake.clone()), false),
        ("worker allows", Some(wake_on_access.clone()), true),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
        place_suspended_sandbox(&fixture.pool, sandbox_id, node_id, &wake_on_access).await?;
        let mut record = creating_record(sandbox_id);
        record.auto_lifecycle_authority = PolicyAuthority::Local;
        state.insert_new(record).await?;
        state.set_status(&sandbox_id.to_string(), "running").await?;
        state
            .apply_access_deadline(
                &sandbox_id.to_string(),
                None,
                worker_policy.map(|policy| {
                    crate::controllers::auto_lifecycle::policy_to_proto(&policy).encode_to_vec()
                }),
            )
            .await?;
        state.suspend_to_shared_dir(&sandbox_id.to_string()).await?;
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
            .await?
            .expect("sandbox row");

        let result = fixture
            .substrate
            .access_runtime_client(
                pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("ingress"),
                &sandbox,
                ExecutionActivityKind::Access,
                Duration::from_secs(5),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await;

        let status = state.get(&sandbox_id.to_string()).await?.status;
        if expect_wake {
            result.unwrap_or_else(|error| panic!("{label}: {error}"));
            assert_eq!(status, "running", "{label}");
        } else {
            let error = match result {
                Ok(_) => panic!("{label}: a denied implicit wake must not succeed"),
                Err(error) => error,
            };
            assert_eq!(error.status(), axum::http::StatusCode::CONFLICT, "{label}");
            assert_eq!(status, "suspended", "{label}: the worker must not wake");
        }
    }
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn implicit_access_wake_refuses_when_the_cluster_policy_denies_it() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(crate::e2b::auto_lifecycle_policy(
        300, true, false,
    )))
    .execute(&fixture.pool)
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");

    // Unplaced and cluster-denied: refused before any restore lease or worker.
    let error = match fixture
        .substrate
        .access_runtime_client(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("ingress"),
            &sandbox,
            ExecutionActivityKind::Access,
            Duration::from_secs(5),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await
    {
        Ok(_) => panic!("a denied implicit wake must not succeed"),
        Err(error) => error,
    };
    assert_eq!(error.status(), axum::http::StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_claim_checks_capability_before_adopting_and_retains_its_generation() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let actor = super::super::ActorRef::new(
        UserId::new(),
        OrgId::new(),
        Email::parse("pool@example.com".to_owned())?,
    );
    let env = std::collections::BTreeMap::new();
    let policy = crate::e2b::auto_lifecycle_policy(60, true, true);
    let network = NetworkPolicyV1::deny_all();
    let request = PooledExecutionClaimRequest {
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native(
            "create",
        ),
        actor: &actor,
        template: "seed",
        content_hash: Some("hash-seed"),
        template_release_id: None,
        runtime: "fc",
        vcpu: None,
        memory_mib: None,
        env: &env,
        auto_lifecycle: &policy,
        network_policy_override: None,
        network_policy_generation: 1,
        template_network_policy: &network,
        template_network_policy_generation: 1,
        volume_encryption: false,
    };
    for supported in [false, true] {
        let node = NodeId::new();
        let id = SandboxId::new();
        let builder = SimWorkerHarness::builder(node.to_string()).with_internal_api();
        let worker = if supported {
            builder
        } else {
            builder.without_activation_policy_support()
        }
        .start()
        .await?;
        insert_ready_worker(&fixture.pool, node, worker.endpoint(), "9.0.0").await?;
        insert_template_inventory(&fixture.pool, node, "seed", "hash-seed", "9.0.0").await?;
        sqlx::query(sql!(
            "UPDATE node_template_inventory SET pool_current = 1 WHERE node_id = $1"
        ))
        .bind(node)
        .execute(&fixture.pool)
        .await?;
        let mut donor = creating_record(id).with_pool_template(Some("seed".to_owned()));
        donor.status = "paused".to_owned();
        donor.content_hash = Some("hash-seed".to_owned());
        let owner = donor.user.clone();
        worker.state().insert_new(donor).await?;
        let result = fixture.substrate.claim_pooled_execution(&request).await?;
        let resident = worker.state().get(&id.to_string()).await?;
        if supported {
            let claim = result.claim.expect("supported donor should be adopted");
            let identity = claim
                .route_identity
                .expect("Direct adoption must return an identity");
            assert_eq!(
                resident.execution_generation,
                Some(identity.execution_generation().get())
            );
            assert_eq!(resident.pool_template, None);
            assert_eq!(resident.status, "paused");
        } else {
            assert!(result.claim.is_none());
            assert_eq!(resident.pool_template.as_deref(), Some("seed"));
            assert_eq!(resident.user, owner);
            assert_eq!(resident.status, "paused");
        }
        // This worker is gone before the next case; keep its inventory out of selection.
        sqlx::query(sql!(
            "UPDATE node_template_inventory SET pool_current = 0 WHERE node_id = $1"
        ))
        .bind(node)
        .execute(&fixture.pool)
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wake_reserves_fresh_identity_when_observed_resident_retires() -> TestResult {
    use prost::Message;

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    let policy = crate::e2b::auto_lifecycle_policy(60, true, true);
    place_suspended_sandbox(&fixture.pool, sandbox_id, node_id, &policy).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities
         (sandbox_id, route_generation, current_node_id, substrate_kind,
          execution_generation, activation_attempt_id)
         VALUES ($1, 1, $2, 'space', 1, $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(SandboxActivationAttemptId::new())
    .execute(&fixture.pool)
    .await?;
    let mut resident = creating_record(sandbox_id).with_route_identity_provenance(
        1,
        1,
        Some(NodeManagedBy::Space),
    );
    resident.status = "paused".to_owned();
    state.insert_new(resident).await?;
    // Reproduce the old request shape: a stale resident generation presented
    // as fresh activation authority is rejected after the resident retires.
    state.set_status(&sandbox_id.to_string(), "stopped").await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");
    let runtime = connect_worker(DirectSubstrate::endpoint(server.endpoint())?).await?;
    let error = runtime
        .internal_client()
        .wake_sandbox(
            fixture
                .substrate
                .wake_request_with_policy(
                    &sandbox,
                    Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                        execution_generation: Some(1),
                        ..Default::default()
                    }),
                    Some(1),
                    None,
                )
                .await?,
        )
        .await
        .expect_err("a retired generation cannot authorize activation");
    assert_eq!(error.code(), Code::Aborted);
    state.set_status(&sandbox_id.to_string(), "paused").await?;
    state
        .transition_after_next_state_read(sandbox_id.to_string(), "stopped")
        .await;
    fixture
        .substrate
        .wake_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            &sandbox,
            Duration::from_secs(5),
            None,
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await?;
    let running = state.get(&sandbox_id.to_string()).await?;
    assert_eq!(running.status, "running");
    assert_eq!(running.execution_generation, Some(2));
    assert_eq!(
        running.access_idle_policy,
        policy_to_proto(&policy).encode_to_vec()
    );
    let receipt: (i64,) = sqlx::query_as(sql!(
        "SELECT execution_generation FROM sandbox_direct_route_identities WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(receipt.0, 2);
    Ok(())
}

#[rstest::rstest]
#[case::pausing("pausing")]
#[case::checkpointing("checkpointing")]
#[tokio::test(flavor = "current_thread")]
async fn wake_keeps_generation_observed_during_resident_transition(
    #[case] observed: &str,
) -> TestResult {
    use prost::Message;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    let policy = crate::e2b::auto_lifecycle_policy(60, true, true);
    place_suspended_sandbox(&fixture.pool, sandbox_id, node_id, &policy).await?;
    let mut resident = creating_record(sandbox_id).with_route_identity_provenance(
        7,
        7,
        Some(NodeManagedBy::Space),
    );
    resident.status = observed.to_owned();
    state.insert_new(resident).await?;
    state
        .transition_after_next_state_read(sandbox_id.to_string(), "paused")
        .await;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");
    fixture
        .substrate
        .wake_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            &sandbox,
            Duration::from_secs(5),
            Some(ExecutionActivityKind::Access),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await?;
    let resumed = state.get(&sandbox_id.to_string()).await?;
    assert_eq!(resumed.status, "running");
    assert_eq!(resumed.execution_generation, Some(7));
    assert_eq!(
        resumed.access_idle_policy,
        policy_to_proto(&policy).encode_to_vec()
    );
    Ok(())
}

mod runtime_evidence;
