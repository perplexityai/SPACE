use pplx_space_common_core::types::OrgId;
use pplx_space_common_core::types::{SnapshotId, UserId};

use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::types::Json;

use super::*;

use pplx_space_test_env::TestResult;

fn workflow_phase(payload: &ForkSandboxPayloadV1) -> TestResult<String> {
    let value = serde_json::to_value(ForkSandboxPayload::v1(payload.clone()))?;
    value
        .pointer("/payload/workflow/phase")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "fork payload is missing its workflow phase".into())
}

#[test]
fn request_defaults_and_bounds_are_typed() -> TestResult {
    let request = ForkRequest::from_wire(None, None)?;
    assert_eq!(request.timeout().get(), DEFAULT_FORK_TIMEOUT_SECONDS);
    assert_eq!(u32::from(request.count().get()), DEFAULT_FORK_COUNT);

    assert_eq!(
        ForkRequest::from_wire(Some(0), Some(1)),
        Err(ForkRequestError::ZeroTimeout)
    );
    assert_eq!(
        ForkRequest::from_wire(Some(300), Some(0)),
        Err(ForkRequestError::UnsupportedCount { value: 0 })
    );
    assert_eq!(
        ForkRequest::from_wire(Some(300), Some(2)),
        Err(ForkRequestError::UnsupportedCount { value: 2 })
    );
    Ok(())
}

#[test]
fn payload_preallocates_one_target_id() -> TestResult {
    let request = ForkRequest::from_wire(Some(60), Some(1))?;
    let payload =
        ForkSandboxPayloadV1::new(SandboxId::new(), request, "e2b.example.test".to_string());
    assert_eq!(payload.target_sandbox_id().as_uuid().get_version_num(), 7);
    Ok(())
}

#[test]
fn payload_wire_version_and_validated_newtypes_round_trip() -> TestResult {
    let source_sandbox_id = SandboxId::new();
    let payload = ForkSandboxPayload::v1(ForkSandboxPayloadV1::new(
        source_sandbox_id,
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
    ));
    let value = serde_json::to_value(&payload)?;
    assert_eq!(value["version"], "1");
    assert_eq!(
        value.pointer("/payload/workflow/phase"),
        Some(&serde_json::json!("awaiting_source"))
    );
    assert_eq!(
        serde_json::from_value::<ForkSandboxPayload>(value)?,
        payload
    );
    serde_json::from_value::<ForkCount>(serde_json::json!(0)).unwrap_err();
    serde_json::from_value::<ForkSandboxLifetimeSeconds>(serde_json::json!(0)).unwrap_err();
    Ok(())
}

#[test]
fn new_admission_persists_the_explicit_v1_workflow() -> TestResult {
    let idempotency_key = IdempotencyKey::parse("explicit-v1-admission")?;
    let input = AdmitForkOperation {
        org_id: OrgId::new(),
        actor_user_id: Some(UserId::new()),
        source_sandbox_id: SandboxId::new(),
        request: ForkRequest::from_wire(Some(300), Some(1))?,
        ecw_host_domain: "e2b.example.test",
        hard_deadline: Duration::from_secs(240),
        federated_owner: None,
        federated_service_grant: None,
        idempotency_key: &idempotency_key,
    };

    let prepared = prepare_fork_operation(&input)?;

    assert_eq!(prepared.payload_json["version"], "1");
    assert_eq!(
        prepared.payload_json.pointer("/payload/workflow/phase"),
        Some(&serde_json::json!("awaiting_source"))
    );
    Ok(())
}

#[test]
fn payload_persists_one_explicit_workflow_phase() -> TestResult {
    let mut payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
    );
    assert_eq!(workflow_phase(&payload)?, "awaiting_source");

    payload.freeze_source(7, ForkSourceLifecycle::Paused, test_target_config(), None);
    assert_eq!(workflow_phase(&payload)?, "materializing_artifact");

    payload.resolve_fork_snapshot(SnapshotId::new(), Uuid::now_v7());
    assert_eq!(workflow_phase(&payload)?, "restoring_target");

    let target_id = payload.target_sandbox_id();
    payload.record_target_result(
        target_id,
        ForkTargetOutcome::Failed {
            code: 500,
            message: "restore failed".to_string(),
        },
    );
    assert_eq!(workflow_phase(&payload)?, "target_completed");

    let encoded = serde_json::to_value(ForkSandboxPayload::v1(payload))?;
    assert_eq!(
        encoded.pointer("/payload/workflow/phase"),
        Some(&serde_json::json!("target_completed"))
    );
    Ok(())
}

#[test]
fn federated_subject_is_part_of_the_idempotency_fingerprint() -> TestResult {
    let source = SandboxId::new();
    let request = ForkRequest::from_wire(Some(300), Some(1))?;
    let first = UpstreamSubjectId::parse("subject-one")?;
    let second = UpstreamSubjectId::parse("subject-two")?;

    assert_ne!(
        fork_request_fingerprint(source, request, Some(&first))?,
        fork_request_fingerprint(source, request, Some(&second))?
    );
    Ok(())
}

#[test]
fn payload_round_trips_and_scrubs_federated_create_credentials() -> TestResult {
    let owner = crate::federated_identity::FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse("upstream-org")?,
        subject_id: UpstreamSubjectId::parse("upstream-user")?,
    };
    // The expiry and issued-at ride along with the blob, so the equality
    // assertion below also covers them surviving the round trip.
    let service_grant = crate::federated_identity::FederatedServiceGrant::from_unix_expiry(
        b"opaque-grant".to_vec(),
        1_797_120_000,
    )?
    .with_issued_at_unix(1_796_515_200)?;
    let mut payload = ForkSandboxPayloadV1::new_with_deadline(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
        Duration::from_secs(240),
    );
    payload.set_federated_create(&owner, &service_grant);

    let value = serde_json::to_value(ForkSandboxPayload::v1(payload))?;
    let mut restored = decode_fork_payload(value)?;
    let (restored_owner, restored_grant) = restored
        .federated_create()
        .ok_or("federated create context was not persisted")?
        .restore()?;
    assert_eq!(restored_owner, owner);
    assert_eq!(restored_grant, service_grant);

    assert!(restored.scrub_federated_service_grant());
    let scrubbed = serde_json::to_value(ForkSandboxPayload::v1(restored))?;
    assert_eq!(
        scrubbed.pointer("/payload/federated_create/service_grant_base64"),
        None
    );
    Ok(())
}

/// A payload queued before the issued-at field existed has no
/// `service_grant_issued_at_unix` key at all: it must still deserialize and
/// restore, yielding an unstamped grant.
#[test]
fn old_fork_payloads_without_issued_at_still_restore() -> TestResult {
    let owner = crate::federated_identity::FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse("upstream-org")?,
        subject_id: UpstreamSubjectId::parse("upstream-user")?,
    };
    let service_grant = crate::federated_identity::FederatedServiceGrant::from_unix_expiry(
        b"opaque-grant".to_vec(),
        1_797_120_000,
    )?
    .with_issued_at_unix(1_796_515_200)?;
    let mut payload = ForkSandboxPayloadV1::new_with_deadline(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
        Duration::from_secs(240),
    );
    payload.set_federated_create(&owner, &service_grant);

    let mut value = serde_json::to_value(ForkSandboxPayload::v1(payload))?;
    value
        .pointer_mut("/payload/federated_create")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("federated create context was not persisted")?
        .remove("service_grant_issued_at_unix")
        .ok_or("expected the stamped payload to carry issued-at")?;

    let restored = decode_fork_payload(value)?;
    let (_, restored_grant) = restored
        .federated_create()
        .ok_or("federated create context was not persisted")?
        .restore()?;
    assert_eq!(restored_grant.as_bytes(), b"opaque-grant");
    assert_eq!(restored_grant.expires_at_unix(), 1_797_120_000);
    assert!(restored_grant.issued_at().is_none());
    Ok(())
}

#[test]
fn payload_records_terminal_target_results_for_replay() -> TestResult {
    let mut payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
    );
    let target_id = payload.target_sandbox_id();
    let result = ForkTargetOutcome::Succeeded {
        sandbox_id: target_id,
        template_id: "seed".to_string(),
        ecw_host_domain: Some("e2b.example.test".to_string()),
        credential_epoch: EcwCredentialEpoch::new(1)?,
    };

    payload.freeze_source(7, ForkSourceLifecycle::Started, test_target_config(), None);
    payload.resolve_fork_snapshot(SnapshotId::new(), Uuid::now_v7());
    payload.record_target_result(target_id, result.clone());
    let encoded = serde_json::to_value(ForkSandboxPayload::v1(payload))?;
    let decoded = decode_fork_payload(encoded)?;

    assert_eq!(decoded.target_result(target_id), Some(&result));
    Ok(())
}

#[test]
fn completed_target_results_follow_admission_order() -> TestResult {
    let mut payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
    );
    let target_id = payload.target_sandbox_id();
    assert_eq!(payload.completed_target_results(), None);

    let result = ForkTargetOutcome::Failed {
        code: 500,
        message: "restore failed".to_string(),
    };
    payload.freeze_source(7, ForkSourceLifecycle::Started, test_target_config(), None);
    payload.resolve_fork_snapshot(SnapshotId::new(), Uuid::now_v7());
    payload.record_target_result(target_id, result.clone());
    assert_eq!(payload.completed_target_results(), Some(vec![result]));
    Ok(())
}

#[test]
fn payload_capture_progress_prevents_duplicate_side_effects() -> TestResult {
    for encrypted in [false, true] {
        let mut payload = ForkSandboxPayloadV1::new(
            SandboxId::new(),
            ForkRequest::from_wire(Some(60), Some(1))?,
            "e2b.example.test".to_string(),
        );
        assert_eq!(payload.capture_progress(), ForkCaptureProgress::Capture);
        let mut config = test_target_config();
        config.volume_encryption = encrypted;
        payload.freeze_source(7, ForkSourceLifecycle::Started, config, None);

        let local_artifact_id = LocalArtifactId::new();
        payload.record_local_artifact(local_artifact_id);
        assert_eq!(
            payload.capture_progress(),
            ForkCaptureProgress::EnqueueBackup(local_artifact_id)
        );

        let attempt_id = BackupAttemptId::new();
        payload.record_backup_attempt(attempt_id);
        assert_eq!(
            payload.capture_progress(),
            ForkCaptureProgress::ObserveBackup {
                local_artifact_id,
                attempt_id,
            }
        );
        let encoded = serde_json::to_value(ForkSandboxPayload::v1(payload))?;
        let restored = decode_fork_payload(encoded)?;
        assert_eq!(
            restored.target_config().unwrap().volume_encryption,
            encrypted
        );
        assert_eq!(
            restored.capture_progress(),
            ForkCaptureProgress::ObserveBackup {
                local_artifact_id,
                attempt_id
            }
        );
    }
    Ok(())
}

#[test]
fn failed_backup_attempt_retries_the_same_local_artifact() -> TestResult {
    let mut payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
    );
    payload.freeze_source(7, ForkSourceLifecycle::Started, test_target_config(), None);
    let local_artifact_id = LocalArtifactId::new();
    payload.record_local_artifact(local_artifact_id);
    payload.record_backup_attempt(BackupAttemptId::new());

    payload.retry_backup();

    assert_eq!(payload.backup_attempt_count(), 1);
    assert_eq!(
        payload.capture_progress(),
        ForkCaptureProgress::EnqueueBackup(local_artifact_id)
    );
    Ok(())
}

#[test]
fn source_refreeze_is_bounded_and_clears_capture_progress() -> TestResult {
    let mut payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
    );
    payload.freeze_source(7, ForkSourceLifecycle::Started, test_target_config(), None);
    payload.record_local_artifact(LocalArtifactId::new());
    payload.record_backup_attempt(BackupAttemptId::new());

    payload.reset_source_for_refreeze();

    assert_eq!(payload.source_refreeze_count(), 1);
    assert!(payload.source_execution_generation().is_none());
    assert!(payload.target_config().is_none());
    assert_eq!(payload.capture_progress(), ForkCaptureProgress::Capture);
    Ok(())
}

#[test]
fn fork_deadline_must_be_strictly_shorter_than_both_graces() {
    let config = ForkAvailabilityConfig::validate(600, 601, 900).unwrap();
    assert_eq!(config.hard_deadline(), std::time::Duration::from_secs(600));
    assert_eq!(
        ForkAvailabilityConfig::validate(600, 600, 900),
        Err(ForkAvailabilityError::GraceTooShort {
            grace: ForkArtifactGrace::Retention,
        })
    );
    assert_eq!(
        ForkAvailabilityConfig::validate(600, 900, 600),
        Err(ForkAvailabilityError::GraceTooShort {
            grace: ForkArtifactGrace::Storage,
        })
    );
    let configured = ForkAvailabilityConfig::validate(
        DEFAULT_FORK_HARD_DEADLINE_SECONDS,
        SANDBOX_FORK_SNAPSHOT_RETENTION_DEFAULT_GRACE_SECONDS,
        SNAPSHOT_STORAGE_SWEEPER_DEFAULT_MIN_AGE_SECONDS,
    )
    .expect("both sweeper graces outlive the restore activation lease");
    assert_eq!(
        configured.hard_deadline(),
        std::time::Duration::from_secs(DEFAULT_FORK_HARD_DEADLINE_SECONDS)
    );
}

#[test]
fn fork_availability_defaults_on_and_honors_the_env_switch() {
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(
        ForkAvailabilityConfig::from_environment(&env)
            .expect("fork admission is enabled when the env switch is unset")
            .hard_deadline(),
        Duration::from_secs(DEFAULT_FORK_HARD_DEADLINE_SECONDS),
    );

    env.set(FORK_ENABLED_ENV, "false");
    assert_eq!(
        ForkAvailabilityConfig::from_environment(&env),
        Err(ForkAvailabilityError::Disabled),
    );

    env.set(FORK_ENABLED_ENV, "true");
    ForkAvailabilityConfig::from_environment(&env)
        .expect("the env switch can enable fork admission");
}

/// The restore activation lease follows the hard deadline, and both artifact
/// graces must outlive it.
#[test]
fn fork_availability_checks_safety_timings() {
    let lease = u64::from(fork_restore_activation_lease_seconds(Duration::from_secs(
        DEFAULT_FORK_HARD_DEADLINE_SECONDS,
    )));
    assert_eq!(
        ForkAvailabilityConfig::validate(DEFAULT_FORK_HARD_DEADLINE_SECONDS, lease, lease + 1),
        Err(ForkAvailabilityError::GraceTooShort {
            grace: ForkArtifactGrace::Retention,
        }),
    );
    assert_eq!(
        ForkAvailabilityConfig::validate(DEFAULT_FORK_HARD_DEADLINE_SECONDS, lease + 1, lease),
        Err(ForkAvailabilityError::GraceTooShort {
            grace: ForkArtifactGrace::Storage,
        }),
    );
    assert_eq!(
        ForkAvailabilityConfig::validate(0, lease + 1, lease + 1),
        Err(ForkAvailabilityError::ZeroDeadline),
    );
    ForkAvailabilityConfig::validate(DEFAULT_FORK_HARD_DEADLINE_SECONDS, lease + 1, lease + 1)
        .expect("graces that outlive the lease are accepted");
}

#[test]
fn payload_preserves_the_admitted_hard_deadline() -> TestResult {
    let payload = ForkSandboxPayloadV1::new_with_deadline(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
        std::time::Duration::from_secs(777),
    );
    let encoded = serde_json::to_value(ForkSandboxPayload::v1(payload))?;
    let decoded = decode_fork_payload(encoded)?;

    assert_eq!(decoded.hard_deadline(), std::time::Duration::from_secs(777));
    Ok(())
}

#[test]
fn source_refreeze_clears_only_source_derived_state() -> TestResult {
    let mut payload = ForkSandboxPayloadV1::new(
        SandboxId::new(),
        ForkRequest::from_wire(Some(60), Some(1))?,
        "e2b.example.test".to_string(),
    );
    let target_id = payload.target_sandbox_id();
    payload.freeze_source(7, ForkSourceLifecycle::Started, test_target_config(), None);
    payload.record_local_artifact(LocalArtifactId::new());
    payload.set_capture_operation_id(OperationId::new());

    payload.reset_source_for_refreeze();

    assert_eq!(payload.source_refreeze_count(), 1);
    assert_eq!(payload.target_sandbox_id(), target_id);
    assert_eq!(payload.source_execution_generation(), None);
    assert_eq!(payload.source_lifecycle(), None);
    assert_eq!(payload.capture_id(), None);
    assert_eq!(payload.local_artifact_id(), None);
    assert_eq!(payload.capture_operation_id(), None);
    Ok(())
}

#[tokio::test]
async fn fork_admission_waits_for_source_lifecycle_guard() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_user_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let source_sandbox_id = SandboxId::new();
    let request = ForkRequest::from_wire(Some(300), Some(1))?;
    let idempotency_key = IdempotencyKey::parse("fork-delete-serialization")?;
    let store = PublicOperationStore::new(db.pool.clone());

    let mut delete_guard = db.pool.begin().await?;
    sqlx::query(pplx_space_common_core::sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1::text, $2))"
    ))
    .bind(source_sandbox_id.as_uuid())
    .bind(pplx_space_sandbox_store::fork_source_lifecycle::FORK_SOURCE_LIFECYCLE_LOCK_SEED)
    .execute(&mut *delete_guard)
    .await?;

    let admission = begin_fork_operation_admission(
        &store,
        OrgId::from_uuid(org_id),
        source_sandbox_id,
        request,
        None,
        &idempotency_key,
    )
    .await?;
    let blocked = tokio::time::timeout(
        std::time::Duration::from_millis(150),
        admit_locked_fork_operation(
            admission,
            AdmitForkOperation {
                org_id: OrgId::from_uuid(org_id),
                actor_user_id: Some(UserId::from_uuid(actor_user_id)),
                source_sandbox_id,
                request,
                ecw_host_domain: "e2b.example.test",
                hard_deadline: std::time::Duration::from_secs(240),
                federated_owner: None,
                federated_service_grant: None,
                idempotency_key: &idempotency_key,
            },
        ),
    )
    .await;
    assert!(
        blocked.is_err(),
        "admission must wait while DELETE owns the source guard"
    );
    delete_guard.rollback().await?;

    let missing_key = IdempotencyKey::parse("fork-after-delete")?;
    let missing_admission = begin_fork_operation_admission(
        &store,
        OrgId::from_uuid(org_id),
        source_sandbox_id,
        request,
        None,
        &missing_key,
    )
    .await?;
    let outcome = admit_locked_fork_operation(
        missing_admission,
        AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_user_id)),
            source_sandbox_id,
            request,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &missing_key,
        },
    )
    .await?;
    assert_eq!(outcome, CreatePublicOperationResult::AdmissionDenied);
    Ok(())
}

fn test_target_config() -> ForkTargetConfig {
    ForkTargetConfig {
        volume_encryption: false,
        egress_profiles: Default::default(),
        template: "seed:latest".parse().unwrap(),
        source_template_id: Some("seed".to_string()),
        runtime: None,
        env: Default::default(),
        metadata: Default::default(),
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        auto_lifecycle: AutoLifecyclePolicy::disabled(),
        network_policy: None,
    }
}

#[tokio::test]
async fn admission_reuses_matching_request_and_preserves_original_targets() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_user_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let source_sandbox_id = SandboxId::new();
    let request = ForkRequest::from_wire(Some(300), Some(1))?;
    let idempotency_key = IdempotencyKey::parse("fork-admission-replay")?;
    let store = PublicOperationStore::new(db.pool.clone());

    let first = admit_fork_operation(
        &store,
        AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_user_id)),
            source_sandbox_id,
            request,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &idempotency_key,
        },
    )
    .await?;
    let CreatePublicOperationResult::Created(operation_id) = first else {
        return Err("first admission did not create the operation".into());
    };

    let second = admit_fork_operation(
        &store,
        AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_user_id)),
            source_sandbox_id,
            request,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &idempotency_key,
        },
    )
    .await?;
    assert_eq!(second, CreatePublicOperationResult::Existing(operation_id));

    let stored: Json<ForkSandboxPayload> =
        sqlx::query_scalar("SELECT payload_json FROM public_operations WHERE operation_id = $1")
            .bind(operation_id.as_uuid())
            .fetch_one(&db.pool)
            .await?;
    let stored = stored.0.into_v1();
    assert_eq!(stored.source_sandbox_id(), source_sandbox_id);
    assert_eq!(stored.requested_count().get(), 1);
    assert_eq!(stored.ecw_host_domain(), Some("e2b.example.test"));
    Ok(())
}

#[tokio::test]
async fn admitted_replay_survives_a_missing_source_sandbox() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_user_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let source_sandbox_id = SandboxId::new();
    let request = ForkRequest::from_wire(Some(300), Some(1))?;
    let idempotency_key = IdempotencyKey::parse("fork-admission-missing-source-replay")?;
    let store = PublicOperationStore::new(db.pool.clone());
    let created = admit_fork_operation(
        &store,
        AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_user_id)),
            source_sandbox_id,
            request,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &idempotency_key,
        },
    )
    .await?;
    let CreatePublicOperationResult::Created(operation_id) = created else {
        return Err("first admission did not create the operation".into());
    };

    let admission = begin_fork_operation_admission(
        &store,
        OrgId::from_uuid(org_id),
        source_sandbox_id,
        request,
        None,
        &idempotency_key,
    )
    .await?;
    let replay = admit_locked_fork_operation(
        admission,
        AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_user_id)),
            source_sandbox_id,
            request,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &idempotency_key,
        },
    )
    .await?;

    assert_eq!(replay, CreatePublicOperationResult::Existing(operation_id));
    Ok(())
}

#[tokio::test]
async fn admission_rejects_same_key_with_different_request() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_user_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let source_sandbox_id = SandboxId::new();
    let idempotency_key = IdempotencyKey::parse("fork-admission-conflict")?;
    let store = PublicOperationStore::new(db.pool.clone());

    let first = admit_fork_operation(
        &store,
        AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_user_id)),
            source_sandbox_id,
            request: ForkRequest::from_wire(Some(300), Some(1))?,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &idempotency_key,
        },
    )
    .await?;
    let CreatePublicOperationResult::Created(first_id) = first else {
        return Err("first admission did not create the operation".into());
    };

    let conflict = admit_fork_operation(
        &store,
        AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_user_id)),
            source_sandbox_id,
            request: ForkRequest::from_wire(Some(301), Some(1))?,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &idempotency_key,
        },
    )
    .await?;
    assert!(matches!(
        conflict,
        CreatePublicOperationResult::IdempotencyConflict {
            operation_id: Some(operation_id),
            ..
        } if operation_id == first_id
    ));
    Ok(())
}

async fn seed_org(pool: &sqlx::PgPool) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO organizations (id, name, slug, byok_enabled)
         VALUES ($1, $2, $3, false)",
    )
    .bind(id)
    .bind(format!("Fork Admission Org {id}"))
    .bind(format!("fork-admission-{}", id.simple()))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_user(pool: &sqlx::PgPool, org_id: OrgId) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role
         )
         VALUES ($1, $2, 'hash', 'Fork Admission Test', $3, 'member')",
    )
    .bind(id)
    .bind(format!("fork-admission-{id}@example.com"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}
