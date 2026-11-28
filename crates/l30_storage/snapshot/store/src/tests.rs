use super::{
    GetSnapshotError, InsertOutcome, InsertSnapshotError, NewSnapshot, RecordArtifactDigestsError,
    SnapshotArtifact, SnapshotArtifactDigest, artifact_captured_after, artifact_digests,
    artifact_registered_by_another_attempt, delete_snapshot_catalog, get, insert,
    is_latest_ready_suspend, record_artifact_digests,
};
use jiff::Timestamp;
use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_artifact_model::{ExactStorageUri, SnapshotArtifact as SnapshotArtifactFile};
use pplx_space_content_digest::Digest;
use pplx_space_cpu_arch::Arch;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEEDED_ORG_ID, SEED_USER_ID as SEEDED_USER_ID, try_pg_test_db,
    try_pg_test_db_with_max_connections,
};
use pplx_space_execution_spec::{ExecutionMetadata, HostCpu, SnapshotExecutionMetadata};
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId};
use pplx_space_snapshot_model::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, SnapshotContentHash, SnapshotId, SnapshotKind,
    SnapshotName, SnapshotState,
};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{TemplateArtifactDigest, TemplateName};
use pplx_space_test_env::TestResult;
use sqlx::types::Json;
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn snapshot_catalog_store_contract() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .execute(&db.pool)
    .await?;

    let snapshot_id = SnapshotId::new();
    let source_node_id = NodeId::new();
    let local_artifact_id = LocalArtifactId::new();
    let location_id = Uuid::now_v7();
    let execution_metadata = SnapshotExecutionMetadata::new(
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
    )?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, source_node_id, label,
             retention_tier, vcpu, memory_mib, size_bytes, template_id,
             content_hash, org_id, failure_reason, last_attempt_at,
             local_artifact_id, artifact_kind, artifact_origin,
             artifact_snapshot_name, runtime, runtime_version, guest_os,
             guest_arch, state_format_name, state_format_version,
             execution_metadata, generation
         ) VALUES (
             $1, $2, 'full', 'failed', '2026-08-01T02:03:04.123456Z', $3,
             'nightly', '10m', 8, 16384, 987654321, 'seed',
             'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
             $4, 'upload failed', '2026-08-01T02:04:05.654321Z', $5,
             'full', 'snapshot', 'checkpoint', 'ignored-runtime',
             'ignored-version', 'ignored-os', 'ignored-arch',
             'ignored-format', 'ignored-format-version', $6, 7
         )"
    ))
    .bind(snapshot_id.as_uuid())
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(SEEDED_ORG_ID)
    .bind(local_artifact_id.as_uuid())
    .bind(Json(&execution_metadata))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string, created_at, org_id,
             orphaned_at
         ) VALUES (
             $1, $2, 's3://test-bucket/snapshots/catalog?region=us-east-1',
             '2026-08-01T02:05:06.123456Z', $3, NULL
         )"
    ))
    .bind(location_id)
    .bind(snapshot_id.as_uuid())
    .bind(SEEDED_ORG_ID)
    .execute(&db.pool)
    .await?;

    assert_eq!(
        crate::attempt::status(&db.pool, snapshot_id).await?,
        Some(crate::attempt::AttemptStatus {
            state: "failed".to_string(),
            failure_reason: Some("upload failed".to_string()),
        })
    );
    assert_eq!(
        crate::attempt::state(&db.pool, snapshot_id)
            .await?
            .as_deref(),
        Some("failed")
    );
    assert_eq!(
        crate::storage_location::live_connection_string(&db.pool, snapshot_id, location_id)
            .await?
            .as_deref(),
        Some("s3://test-bucket/snapshots/catalog?region=us-east-1")
    );
    let listed = crate::listing::stored_for_sandbox(&db.pool, sandbox_id).await?;
    assert_eq!(
        listed
            .iter()
            .map(|row| (row.id, row.r#type.as_str(), row.state.as_str()))
            .collect::<Vec<_>>(),
        vec![(snapshot_id, "full", "failed")]
    );

    let record = get(&db.pool, snapshot_id)
        .await?
        .expect("populated snapshot exists");
    assert_eq!(record.id(), snapshot_id);
    assert_eq!(record.sandbox_id(), sandbox_id);
    assert_eq!(record.kind(), SnapshotKind::Full);
    assert_eq!(record.state(), SnapshotState::Failed);
    assert_eq!(
        record.timestamp(),
        "2026-08-01T02:03:04.123456Z".parse::<Timestamp>()?
    );
    assert_eq!(record.source_node_id(), Some(source_node_id));
    assert_eq!(record.label(), Some("nightly"));
    assert_eq!(record.retention_tier(), Some("10m"));
    assert_eq!(record.vcpu(), Some(8));
    assert_eq!(record.memory_mib(), Some(16384));
    assert_eq!(record.size_bytes(), Some(987654321));
    let expected_template_name = TemplateName::parse("seed")?;
    assert_eq!(record.template_id(), Some(&expected_template_name));
    let expected_content_hash =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .parse::<TemplateArtifactDigest>()?;
    assert_eq!(
        record.content_hash(),
        Some(&SnapshotContentHash::from(expected_content_hash))
    );
    assert_eq!(record.org_id(), Some(OrgId::from_uuid(SEEDED_ORG_ID)));
    assert_eq!(record.failure_reason(), Some("upload failed"));
    assert_eq!(
        record.last_attempt_at(),
        Some("2026-08-01T02:04:05.654321Z".parse::<Timestamp>()?)
    );
    assert_eq!(record.execution_metadata(), Some(&execution_metadata));
    assert_eq!(record.generation(), Some(ExecutionGeneration::new(7)?));
    assert!(!record.is_suspend_snapshot());
    let artifact = record.artifact().expect("artifact identity");
    assert_eq!(artifact.local_artifact_id(), Some(local_artifact_id));
    assert_eq!(artifact.kind(), ArtifactKind::Full);
    assert_eq!(artifact.origin(), ArtifactOrigin::Snapshot);
    let expected_snapshot_name = SnapshotName::parse("checkpoint")?;
    assert_eq!(artifact.snapshot_name(), Some(&expected_snapshot_name));
    let location = record.storage_location().expect("linked storage location");
    assert_eq!(location.id(), location_id);
    assert_eq!(location.snapshot_id(), snapshot_id);
    assert_eq!(
        location.storage_connection_string(),
        "s3://test-bucket/snapshots/catalog?region=us-east-1"
    );
    assert_eq!(
        location.created_at(),
        "2026-08-01T02:05:06.123456Z".parse::<Timestamp>()?
    );
    assert_eq!(location.org_id(), Some(OrgId::from_uuid(SEEDED_ORG_ID)));
    assert_eq!(location.orphaned_at(), None);

    let legacy_snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state)
         VALUES ($1, $2, 'full', 'creating')"
    ))
    .bind(legacy_snapshot_id.as_uuid())
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let legacy = get(&db.pool, legacy_snapshot_id)
        .await?
        .expect("legacy snapshot exists");
    assert!(legacy.is_suspend_snapshot());
    assert_eq!(legacy.state(), SnapshotState::Creating);
    assert_eq!(legacy.source_node_id(), None);
    assert_eq!(legacy.label(), None);
    assert_eq!(legacy.retention_tier(), None);
    assert_eq!(legacy.vcpu(), None);
    assert_eq!(legacy.memory_mib(), None);
    assert_eq!(legacy.size_bytes(), None);
    assert_eq!(legacy.template_id(), None);
    assert_eq!(legacy.content_hash(), None);
    assert_eq!(legacy.org_id(), None);
    assert_eq!(legacy.failure_reason(), None);
    assert_eq!(legacy.last_attempt_at(), None);
    assert_eq!(legacy.artifact(), None);
    assert_eq!(legacy.execution_metadata(), None);
    assert_eq!(legacy.generation(), None);
    assert_eq!(legacy.storage_location(), None);

    let expected_uri =
        ExactStorageUri::parse("s3://test-bucket/snapshots/legacy?region=us-east-1")?;
    let different_uri =
        ExactStorageUri::parse("s3://test-bucket/snapshots/different?region=us-east-1")?;
    let inserted_snapshot_id = SnapshotId::new();
    let inserted_execution_metadata = SnapshotExecutionMetadata::new(
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
    )?;
    let inserted_content_hash =
        "sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .parse::<TemplateArtifactDigest>()?;
    let make_snapshot = |storage_uri: ExactStorageUri| {
        NewSnapshot::new(
            inserted_snapshot_id,
            sandbox_id,
            SnapshotKind::Full,
            SnapshotState::Creating,
            OrgId::from_uuid(SEEDED_ORG_ID),
            storage_uri,
        )
        .with_source_node_id(Some(source_node_id))
        .with_resources(Some(4), Some(8192))
        .with_template(
            Some(TemplateName::parse("seed").expect("valid template name")),
            Some(SnapshotContentHash::from(inserted_content_hash)),
        )
        .with_execution_metadata(inserted_execution_metadata.clone())
        .with_generation(Some(ExecutionGeneration::new(7).expect("valid generation")))
    };
    let snapshot = make_snapshot(expected_uri.clone());
    let mut insert_tx = db.pool.begin().await?;
    let (inserted, outcome) = insert(&mut *insert_tx, &snapshot).await?;
    assert_eq!(outcome, InsertOutcome::Created);
    assert_eq!(inserted.id(), inserted_snapshot_id);
    // The capture's generation is recorded with the attempt.
    assert_eq!(inserted.generation(), Some(ExecutionGeneration::new(7)?));
    assert_eq!(
        inserted
            .storage_location()
            .expect("insert returns complete aggregate")
            .storage_connection_string(),
        expected_uri.as_wire()
    );
    insert_tx.commit().await?;

    let mut retry_tx = db.pool.begin().await?;
    let (retried, outcome) = insert(&mut *retry_tx, &snapshot).await?;
    assert_eq!(outcome, InsertOutcome::AlreadyRegistered);
    assert_eq!(retried, inserted);
    retry_tx.commit().await?;

    // A replay that now carries the capture host CPU (the first registration
    // went through a cluster API that dropped it) is the same attempt, and the
    // persisted row is returned unchanged.
    assert_eq!(
        inserted
            .execution_metadata()
            .and_then(SnapshotExecutionMetadata::host_cpu),
        None
    );
    let replay_with_host_cpu = make_snapshot(expected_uri.clone()).with_execution_metadata(
        inserted_execution_metadata
            .clone()
            .with_host_cpu(Some(HostCpu::parse("genuineintel-6-85-7")?)),
    );
    let mut host_cpu_retry_tx = db.pool.begin().await?;
    let (retried, outcome) = insert(&mut *host_cpu_retry_tx, &replay_with_host_cpu).await?;
    assert_eq!(outcome, InsertOutcome::AlreadyRegistered);
    assert_eq!(retried, inserted);
    host_cpu_retry_tx.commit().await?;

    let mut location_conflict_tx = db.pool.begin().await?;
    let error = insert(
        &mut *location_conflict_tx,
        &make_snapshot(different_uri.clone()),
    )
    .await
    .expect_err("one snapshot ID cannot identify two storage locations");
    assert!(matches!(
        error,
        InsertSnapshotError::StorageLocationConflict { snapshot_id }
            if snapshot_id == inserted_snapshot_id
    ));
    location_conflict_tx.rollback().await?;

    let conflicting_artifact = SnapshotArtifact::new(
        Some(LocalArtifactId::new()),
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
    )?;
    let conflicting_snapshot =
        make_snapshot(expected_uri.clone()).with_artifact(conflicting_artifact);
    let mut identity_conflict_tx = db.pool.begin().await?;
    let error = insert(&mut *identity_conflict_tx, &conflicting_snapshot)
        .await
        .expect_err("one snapshot ID cannot identify two snapshots");
    assert!(matches!(
        error,
        InsertSnapshotError::IdentityConflict { snapshot_id }
            if snapshot_id == inserted_snapshot_id
    ));
    identity_conflict_tx.rollback().await?;

    assert_eq!(
        get(&db.pool, inserted_snapshot_id)
            .await?
            .expect("inserted snapshot exists")
            .storage_location()
            .expect("location inserted")
            .storage_connection_string(),
        expected_uri.as_wire()
    );

    let malformed_snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, execution_metadata)
         VALUES ($1, $2, 'full', 'creating', $3)"
    ))
    .bind(malformed_snapshot_id.as_uuid())
    .bind(sandbox_id)
    .bind(Json(serde_json::json!({ "runtime": {} })))
    .execute(&db.pool)
    .await?;
    let error = get(&db.pool, malformed_snapshot_id)
        .await
        .expect_err("malformed non-null execution metadata must fail");
    assert!(matches!(
        error,
        GetSnapshotError::InvalidExecutionMetadata { snapshot_id, .. }
            if snapshot_id == malformed_snapshot_id
    ));

    let unknown_retention_tier_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, retention_tier)
         VALUES ($1, $2, 'full', 'creating', 'manual')"
    ))
    .bind(unknown_retention_tier_id.as_uuid())
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let unknown_retention_tier = get(&db.pool, unknown_retention_tier_id)
        .await?
        .expect("snapshot with unknown retention tier exists");
    assert_eq!(unknown_retention_tier.retention_tier(), Some("manual"));

    let invalid_template_name_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, template_id)
         VALUES ($1, $2, 'full', 'creating', '../seed')"
    ))
    .bind(invalid_template_name_id.as_uuid())
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let error = get(&db.pool, invalid_template_name_id)
        .await
        .expect_err("path-unsafe template name must fail");
    assert!(matches!(
        error,
        GetSnapshotError::InvalidTemplateName { snapshot_id, .. }
            if snapshot_id == invalid_template_name_id
    ));

    let legacy_content_hash_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, content_hash)
         VALUES ($1, $2, 'full', 'creating', 'legacy-hash')"
    ))
    .bind(legacy_content_hash_id.as_uuid())
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let legacy = get(&db.pool, legacy_content_hash_id)
        .await?
        .expect("legacy snapshot exists");
    assert_eq!(
        legacy.content_hash().map(SnapshotContentHash::as_stored),
        Some("legacy-hash")
    );

    let invalid_snapshot_name_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, local_artifact_id, artifact_kind,
             artifact_origin, artifact_snapshot_name
         ) VALUES ($1, $2, 'full', 'creating', $3, 'full', 'snapshot', '../checkpoint')"
    ))
    .bind(invalid_snapshot_name_id.as_uuid())
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let error = get(&db.pool, invalid_snapshot_name_id)
        .await
        .expect_err("path-unsafe artifact snapshot name must fail");
    assert!(matches!(
        error,
        GetSnapshotError::InvalidArtifactSnapshotName { snapshot_id, .. }
            if snapshot_id == invalid_snapshot_name_id
    ));

    let mut delete_tx = db.pool.begin().await?;
    assert!(delete_snapshot_catalog(&mut *delete_tx, snapshot_id).await?);
    assert!(!delete_snapshot_catalog(&mut *delete_tx, snapshot_id).await?);
    assert!(get(&mut *delete_tx, snapshot_id).await?.is_none());
    delete_tx.rollback().await?;
    let restored = get(&db.pool, snapshot_id)
        .await?
        .expect("rollback restores the aggregate");
    assert_eq!(
        restored.storage_location().map(|location| location.id()),
        Some(location_id)
    );
    Ok(())
}

/// Digests recorded at finalize are read back for restore, a replay cannot
/// rewrite them, and they leave the catalog with their snapshot.
#[tokio::test(flavor = "current_thread")]
async fn artifact_digests_follow_their_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state)
         VALUES ($1, $2, 'full', 'creating')"
    ))
    .bind(snapshot_id.as_uuid())
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let digests = [
        SnapshotArtifactDigest {
            artifact: SnapshotArtifactFile::Memory,
            digest: Digest::sha256([0x22; 32]),
            size_bytes: 1 << 30,
        },
        SnapshotArtifactDigest {
            artifact: SnapshotArtifactFile::VmState,
            digest: Digest::sha256([0x11; 32]),
            size_bytes: 4096,
        },
    ];
    let mut tx = db.pool.begin().await?;
    record_artifact_digests(&mut *tx, snapshot_id, &digests).await?;
    tx.commit().await?;

    let mut expected = digests.to_vec();
    expected.sort_by_key(|digest| digest.artifact.file_name());
    assert_eq!(artifact_digests(&db.pool, snapshot_id).await?, expected);

    // An identical replay is a no-op; a conflicting one is refused and
    // leaves the recorded digests untouched.
    let mut tx = db.pool.begin().await?;
    record_artifact_digests(&mut *tx, snapshot_id, &digests[..1]).await?;
    tx.commit().await?;
    let mut tx = db.pool.begin().await?;
    let conflict = record_artifact_digests(
        &mut *tx,
        snapshot_id,
        &[SnapshotArtifactDigest {
            artifact: SnapshotArtifactFile::VmState,
            digest: Digest::sha256([0x33; 32]),
            size_bytes: 4096,
        }],
    )
    .await;
    assert!(matches!(
        conflict,
        Err(RecordArtifactDigestsError::Conflict { artifact, .. })
            if artifact == SnapshotArtifactFile::VmState
    ));
    tx.rollback().await?;
    assert_eq!(artifact_digests(&db.pool, snapshot_id).await?, expected);

    let mut tx = db.pool.begin().await?;
    assert!(delete_snapshot_catalog(&mut *tx, snapshot_id).await?);
    tx.commit().await?;
    assert!(artifact_digests(&db.pool, snapshot_id).await?.is_empty());
    Ok(())
}

/// A registration of the same bytes by any other attempt counts, whatever its
/// state; the asking attempt itself and other artifacts of the sandbox don't.
#[tokio::test(flavor = "current_thread")]
async fn another_attempts_registration_of_an_artifact_is_found() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let artifact = LocalArtifactId::new();
    let other_artifact = LocalArtifactId::new();
    let earlier = SnapshotId::new();
    let asking = SnapshotId::new();
    for (id, local_artifact_id, state) in [
        (earlier, artifact, "failed"),
        (asking, artifact, "creating"),
        (SnapshotId::new(), other_artifact, "ready"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, local_artifact_id, artifact_kind, artifact_origin
             )
             VALUES ($1, $2, 'full', $3, $4, 'full', 'suspend')"
        ))
        .bind(id.as_uuid())
        .bind(sandbox_id)
        .bind(state)
        .bind(local_artifact_id.as_uuid())
        .execute(&db.pool)
        .await?;
    }

    assert!(artifact_registered_by_another_attempt(&db.pool, sandbox_id, artifact, asking).await?);
    assert!(
        artifact_registered_by_another_attempt(&db.pool, sandbox_id, artifact, earlier).await?,
        "from the earlier attempt's side, the later one counts"
    );
    sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
        .bind(earlier.as_uuid())
        .execute(&db.pool)
        .await?;
    assert!(
        !artifact_registered_by_another_attempt(&db.pool, sandbox_id, artifact, asking).await?,
        "neither the asking attempt nor another artifact's row counts"
    );
    Ok(())
}

/// Captures are ordered by the generation of the execution that took them,
/// not by when their backups registered: an older capture backed up again
/// after a newer one still orders before it. A capture whose backups record
/// no generation, or whose only ready backup lost its location, orders against
/// nothing.
#[tokio::test(flavor = "current_thread")]
async fn captures_order_by_the_generation_that_took_them() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let (older, newer, unrecorded, orphaned) = (
        LocalArtifactId::new(),
        LocalArtifactId::new(),
        LocalArtifactId::new(),
        LocalArtifactId::new(),
    );
    // (artifact, its capture generation, minutes since its backup registered,
    // whether its location is orphaned)
    for (artifact, generation, minutes_ago, orphan) in [
        (older, Some(4), 30, false),
        (newer, Some(5), 20, false),
        // The older capture, backed up again after the newer one.
        (older, Some(4), 1, false),
        (unrecorded, None, 10, false),
        (orphaned, Some(9), 5, true),
    ] {
        let snapshot_id = insert_snapshot(
            &db.pool,
            sandbox_id,
            "ready",
            artifact,
            None,
            Some(orphan),
            minutes_ago,
        )
        .await?;
        sqlx::query(sql!("UPDATE snapshots SET generation = $2 WHERE id = $1"))
            .bind(snapshot_id.as_uuid())
            .bind(generation)
            .execute(&db.pool)
            .await?;
    }
    let full = |artifact| (artifact, ArtifactKind::Full);
    let after =
        |later, earlier| artifact_captured_after(&db.pool, sandbox_id, full(later), full(earlier));

    assert!(after(newer, older).await?);
    assert!(!after(older, newer).await?);
    assert!(!after(older, older).await?, "a capture isn't after itself");
    assert!(!after(unrecorded, older).await?);
    assert!(!after(newer, unrecorded).await?);
    assert!(
        !after(orphaned, newer).await?,
        "an orphaned backup orders nothing"
    );
    Ok(())
}

/// Every backup attempt of one artifact records the capture generation its
/// first attempt with a generation recorded: a re-upload the node can no longer vouch
/// for inherits it, and one that vouches for another generation can't move
/// it. An artifact that never recorded one takes the first that's given.
#[tokio::test(flavor = "current_thread")]
async fn backup_attempts_of_an_artifact_keep_its_capture_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_placed_sandbox(&db.pool, sandbox_id, "suspended", None, None, None).await?;
    let generation = |value| ExecutionGeneration::new(value).expect("valid generation");
    let register = |artifact: LocalArtifactId, recorded: Option<i64>| {
        let pool = db.pool.clone();
        async move {
            let snapshot_id = SnapshotId::new();
            let snapshot = NewSnapshot::new(
                snapshot_id,
                sandbox_id,
                SnapshotKind::Full,
                SnapshotState::Creating,
                OrgId::from_uuid(SEEDED_ORG_ID),
                ExactStorageUri::parse(&format!(
                    "s3://test-bucket/snapshots/{sandbox_id}/{snapshot_id}?region=us-east-1"
                ))?,
            )
            .with_artifact(SnapshotArtifact::new(
                Some(artifact),
                ArtifactKind::Full,
                ArtifactOrigin::Suspend,
                None,
            )?)
            .with_generation(recorded.map(generation));
            let mut tx = pool.begin().await?;
            let (inserted, outcome) = insert(&mut *tx, &snapshot).await?;
            tx.commit().await?;
            assert_eq!(outcome, InsertOutcome::Created);
            TestResult::Ok(inserted.generation())
        }
    };

    let with_generation = LocalArtifactId::new();
    assert_eq!(
        register(with_generation, Some(4)).await?,
        Some(generation(4))
    );
    assert_eq!(
        register(with_generation, None).await?,
        Some(generation(4)),
        "an unvouched re-upload inherits the capture's generation"
    );
    assert_eq!(
        register(with_generation, Some(9)).await?,
        Some(generation(4)),
        "a later registration can't move the capture's generation"
    );

    let without_generation = LocalArtifactId::new();
    assert_eq!(register(without_generation, None).await?, None);
    assert_eq!(
        register(without_generation, Some(6)).await?,
        Some(generation(6)),
        "an artifact that never recorded a generation takes the first given"
    );
    assert_eq!(
        register(LocalArtifactId::new(), None).await?,
        None,
        "another artifact's generation isn't inherited"
    );
    Ok(())
}

/// A catalog snapshot is in the object store only when it is `ready` with a
/// live location, and node-local only when it is the current local artifact
/// of a sandbox settled in `suspended` or `stopped` on the node it uploaded
/// from. During a move, and after a failed cross-node restore rolls the row
/// back with the placement still on the destination, no node-local copy is
/// reported.
#[tokio::test(flavor = "current_thread")]
async fn storage_placements_report_object_store_and_node_copies() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let source = NodeId::new();
    let destination = NodeId::new();
    for node in [source, destination] {
        pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, node).await?;
    }
    let unsnapshotted = SandboxId::new();
    insert_placed_sandbox(
        &db.pool,
        unsnapshotted,
        "suspended",
        Some(source),
        None,
        None,
    )
    .await?;
    assert!(
        super::storage_placements(&db.pool, unsnapshotted)
            .await?
            .is_empty()
    );

    // (case, lifecycle, target, active, uploaded from) -> the node a current
    // artifact is local to.
    for (case, lifecycle, target, active, uploaded_from, expected_node) in [
        (
            "suspended",
            "suspended",
            Some(source),
            None,
            Some(source),
            Some(source),
        ),
        (
            "stopped",
            "stopped",
            None,
            Some(source),
            Some(source),
            Some(source),
        ),
        (
            "restore placed on the destination",
            "restoring",
            Some(destination),
            Some(destination),
            Some(source),
            None,
        ),
        // The same restore failed and rolled back: settled again, but both
        // placement columns still name the destination.
        (
            "rolled-back restore",
            "suspended",
            Some(destination),
            Some(destination),
            Some(source),
            None,
        ),
        (
            "rolled-back restore, active cleared",
            "suspended",
            Some(destination),
            None,
            Some(source),
            None,
        ),
        (
            "no recorded source node",
            "suspended",
            Some(source),
            None,
            None,
            None,
        ),
        (
            "started",
            "started",
            Some(source),
            Some(source),
            Some(source),
            None,
        ),
    ] {
        let current = LocalArtifactId::new();
        let sandbox_id = SandboxId::new();
        insert_placed_sandbox(
            &db.pool,
            sandbox_id,
            lifecycle,
            target,
            active,
            Some(current),
        )
        .await?;
        // (state, artifact, location orphaned?) newest first.
        let rows = [
            ("creating", current, Some(false)),
            ("ready", current, Some(false)),
            ("failed", LocalArtifactId::new(), Some(false)),
            ("ready", LocalArtifactId::new(), Some(false)),
            ("ready", LocalArtifactId::new(), Some(true)),
            ("ready", LocalArtifactId::new(), None),
        ];
        let mut ids = Vec::new();
        for (age, (state, artifact, orphaned)) in rows.into_iter().enumerate() {
            ids.push(
                insert_snapshot(
                    &db.pool,
                    sandbox_id,
                    state,
                    artifact,
                    uploaded_from,
                    orphaned,
                    age,
                )
                .await?,
            );
        }
        let current_node = expected_node;
        assert_eq!(
            super::storage_placements(&db.pool, sandbox_id)
                .await?
                .iter()
                .map(|p| (p.snapshot_id, p.in_object_store, p.node_local_node_id))
                .collect::<Vec<_>>(),
            vec![
                // Uploading: a live location does not make it stored yet.
                (ids[0], false, current_node),
                (ids[1], true, current_node),
                (ids[2], false, None),
                (ids[3], true, None),
                (ids[4], false, None),
                (ids[5], false, None),
            ],
            "{case}"
        );
    }
    Ok(())
}

async fn insert_placed_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    target: Option<NodeId>,
    active: Option<NodeId>,
    artifact: Option<LocalArtifactId>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation, lifecycle,
                                placement, target_node_id, active_node_id,
                                current_local_artifact_id, current_local_artifact_kind,
                                current_local_artifact_origin)
         VALUES ($1, $2::UUID, $3::UUID, 1, $4, 'placed', $5, $6, $7,
                 CASE WHEN $7::UUID IS NULL THEN NULL ELSE 'full' END,
                 CASE WHEN $7::UUID IS NULL THEN NULL ELSE 'suspend' END)"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .bind(lifecycle)
    .bind(target)
    .bind(active)
    .bind(artifact.map(LocalArtifactId::as_uuid))
    .execute(pool)
    .await?;
    Ok(())
}

/// A full suspend snapshot `minutes_ago` old, with a storage location unless
/// `orphaned` is `None`.
async fn insert_snapshot(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    state: &str,
    artifact: LocalArtifactId,
    source_node: Option<NodeId>,
    orphaned: Option<bool>,
    minutes_ago: usize,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                                local_artifact_id, artifact_kind, artifact_origin,
                                source_node_id)
         VALUES ($1, $2, 'full', $3, NOW() - make_interval(mins => $4), $5::UUID, $6,
                 'full', 'suspend', $7)"
    ))
    .bind(snapshot_id.as_uuid())
    .bind(sandbox_id)
    .bind(state)
    .bind(i32::try_from(minutes_ago)?)
    .bind(SEEDED_ORG_ID)
    .bind(artifact.as_uuid())
    .bind(source_node)
    .execute(pool)
    .await?;
    if let Some(orphaned) = orphaned {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations
                 (snapshot_id, storage_connection_string, org_id, orphaned_at)
             VALUES ($1, 's3://synthetic-bucket/snapshots/x', $2::UUID,
                     CASE WHEN $3 THEN NOW() END)"
        ))
        .bind(snapshot_id.as_uuid())
        .bind(SEEDED_ORG_ID)
        .bind(orphaned)
        .execute(pool)
        .await?;
    }
    Ok(snapshot_id)
}

/// The unpinned eviction proof refuses a sandbox whose history is unordered,
/// a ready capture without a generation registered after one with a
/// generation, even for its newest ready attempt: its restore refuses off its
/// node, so the node's copy may be its only restore path. Ordered, or
/// pinned, the newest ready attempt proves it.
#[tokio::test(flavor = "current_thread")]
async fn the_unpinned_eviction_proof_refuses_an_unordered_history() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // (case, generation of the newest attempt, pinned, proven)
    for (case, newest_generation, pinned, proven) in [
        ("unordered", None, false, false),
        ("ordered", Some(6_i64), false, true),
        ("unordered, pinned", None, true, true),
    ] {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
             VALUES ($1, $2::UUID, $3::UUID, 'suspended', 1)"
        ))
        .bind(sandbox_id)
        .bind(SEEDED_USER_ID)
        .bind(SEEDED_ORG_ID)
        .execute(pool)
        .await?;
        let mut newest = SnapshotId::new();
        for (generation, minutes_ago) in [(Some(5_i64), 20), (newest_generation, 10)] {
            let snapshot_id = SnapshotId::new();
            newest = snapshot_id;
            sqlx::query(sql!(
                "INSERT INTO snapshots (
                     id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
                     artifact_origin, generation, org_id
                 )
                 VALUES ($1, $2, 'full', 'ready', NOW() - make_interval(mins => $3::INT), $4,
                         'full', 'suspend', $5, $6)"
            ))
            .bind(snapshot_id)
            .bind(sandbox_id)
            .bind(minutes_ago)
            .bind(Uuid::now_v7())
            .bind(generation)
            .bind(SEEDED_ORG_ID)
            .execute(pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
                 VALUES ($1, 's3://test/latest-ready?region=us-east-1', $2)"
            ))
            .bind(snapshot_id)
            .bind(SEEDED_ORG_ID)
            .execute(pool)
            .await?;
        }
        if pinned {
            sqlx::query(sql!(
                "UPDATE sandboxes s
                    SET current_local_artifact_id = sn.local_artifact_id,
                        current_local_artifact_kind = 'full',
                        current_local_artifact_origin = 'suspend'
                   FROM snapshots sn
                  WHERE s.id = $1 AND sn.id = $2"
            ))
            .bind(sandbox_id)
            .bind(newest)
            .execute(pool)
            .await?;
        }
        assert_eq!(
            is_latest_ready_suspend(pool, sandbox_id, newest).await?,
            proven,
            "{case}"
        );
    }
    Ok(())
}

/// A backup transaction whose `NOW()` predates a concurrent restore claim must
/// still stamp its snapshot at or after `sandboxes.lifecycle_changed_at` when
/// the sandbox is `restoring`, whether the claim committed before `INSERT INTO
/// snapshots` ran or held the sandbox row `FOR UPDATE` while `INSERT` waited.
#[tokio::test(flavor = "current_thread")]
async fn insert_stamps_a_restoring_sandbox_snapshot_at_or_after_its_claim() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let pool = &db.pool;

    // Interleaving 1: `backup_tx` starts and pins its transaction clock before
    // the restore claim commits `lifecycle = 'restoring'`, then inserts.
    let sandbox_after_claim = SandboxId::new();
    insert_placed_sandbox(pool, sandbox_after_claim, "suspended", None, None, None).await?;
    let mut backup_tx = pool.begin().await?;
    sqlx::query(sql!("SELECT 1"))
        .execute(&mut *backup_tx)
        .await?;

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(sandbox_after_claim)
    .execute(pool)
    .await?;
    // The claim's cutoff ahead of every clock the insert reads, as a claim
    // that moved it to the database clock past a later backup leaves it, so
    // only the cutoff itself can bring the stamp up to it. The lifecycle
    // trigger doesn't touch it, since the lifecycle stays.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = clock_timestamp() + INTERVAL '1 hour'
          WHERE id = $1"
    ))
    .bind(sandbox_after_claim)
    .execute(pool)
    .await?;

    let post_claim_id = SnapshotId::new();
    let post_claim_snapshot = NewSnapshot::new(
        post_claim_id,
        sandbox_after_claim,
        SnapshotKind::Full,
        SnapshotState::Creating,
        OrgId::from_uuid(SEEDED_ORG_ID),
        ExactStorageUri::parse(&format!(
            "s3://test-bucket/snapshots/{sandbox_after_claim}/{post_claim_id}?region=us-east-1"
        ))?,
    )
    .with_artifact(SnapshotArtifact::new(
        Some(LocalArtifactId::new()),
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
    )?);
    let (_, outcome) = insert(&mut *backup_tx, &post_claim_snapshot).await?;
    backup_tx.commit().await?;
    assert_eq!(outcome, InsertOutcome::Created);

    let at_or_after_claim: bool = sqlx::query_scalar(sql!(
        "SELECT sn.timestamp = s.lifecycle_changed_at
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
          WHERE sn.id = $1"
    ))
    .bind(post_claim_id)
    .fetch_one(pool)
    .await?;
    assert!(
        at_or_after_claim,
        "snapshot inserted after restore claim committed must not have timestamp < lifecycle_changed_at"
    );

    // Interleaving 2: the restore claim holds `sandboxes FOR UPDATE` while
    // `INSERT INTO snapshots` waits on the parent row lock, then commits.
    let sandbox_while_locked = SandboxId::new();
    insert_placed_sandbox(pool, sandbox_while_locked, "suspended", None, None, None).await?;
    let mut locked_backup_tx = pool.begin().await?;
    let backup_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *locked_backup_tx)
        .await?;

    let mut claim_tx = pool.begin().await?;
    let claim_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *claim_tx)
        .await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_while_locked)
        .execute(&mut *claim_tx)
        .await?;

    let blocked_id = SnapshotId::new();
    let blocked_snapshot = NewSnapshot::new(
        blocked_id,
        sandbox_while_locked,
        SnapshotKind::Full,
        SnapshotState::Creating,
        OrgId::from_uuid(SEEDED_ORG_ID),
        ExactStorageUri::parse(&format!(
            "s3://test-bucket/snapshots/{sandbox_while_locked}/{blocked_id}?region=us-east-1"
        ))?,
    )
    .with_artifact(SnapshotArtifact::new(
        Some(LocalArtifactId::new()),
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
    )?);

    let insert_fut = async {
        let (_, outcome) = insert(&mut *locked_backup_tx, &blocked_snapshot).await?;
        locked_backup_tx.commit().await?;
        TestResult::Ok(outcome)
    };
    let commit_claim_fut = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let blocked_by_claim: bool =
                sqlx::query_scalar(sql!("SELECT $1 = ANY(pg_blocking_pids($2))"))
                    .bind(claim_pid)
                    .bind(backup_pid)
                    .fetch_one(pool)
                    .await?;
            if blocked_by_claim {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err("INSERT INTO snapshots never waited on sandboxes FOR UPDATE".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
        ))
        .bind(sandbox_while_locked)
        .execute(&mut *claim_tx)
        .await?;
        // A separate statement: the lifecycle trigger would overwrite a
        // cutoff set beside the lifecycle change.
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle_changed_at = clock_timestamp() + INTERVAL '1 hour'
              WHERE id = $1"
        ))
        .bind(sandbox_while_locked)
        .execute(&mut *claim_tx)
        .await?;
        claim_tx.commit().await?;
        TestResult::Ok(())
    };
    let (insert_res, commit_res) = tokio::join!(insert_fut, commit_claim_fut);
    commit_res?;
    assert_eq!(insert_res?, InsertOutcome::Created);

    let blocked_at_or_after_claim: bool = sqlx::query_scalar(sql!(
        "SELECT sn.timestamp = s.lifecycle_changed_at
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
          WHERE sn.id = $1"
    ))
    .bind(blocked_id)
    .fetch_one(pool)
    .await?;
    assert!(
        blocked_at_or_after_claim,
        "snapshot insert unblocked after restore claim committed must stamp timestamp >= lifecycle_changed_at"
    );

    // Interleaving 3: a concurrent `FOR NO KEY UPDATE` writer on `sandboxes`
    // (such as `ingest::release_completed_activations`) mustn't block `insert`,
    // preserving the foreign-key `FOR KEY SHARE` lock level that
    // `BeginArtifactBackup`'s sandbox-then-execution lock order relies on.
    let sandbox_no_key_update = SandboxId::new();
    insert_placed_sandbox(pool, sandbox_no_key_update, "started", None, None, None).await?;
    let mut no_key_tx = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR NO KEY UPDATE"
    ))
    .bind(sandbox_no_key_update)
    .execute(&mut *no_key_tx)
    .await?;

    let unblocked_id = SnapshotId::new();
    let unblocked_snapshot = NewSnapshot::new(
        unblocked_id,
        sandbox_no_key_update,
        SnapshotKind::Full,
        SnapshotState::Creating,
        OrgId::from_uuid(SEEDED_ORG_ID),
        ExactStorageUri::parse(&format!(
            "s3://test-bucket/snapshots/{sandbox_no_key_update}/{unblocked_id}?region=us-east-1"
        ))?,
    )
    .with_artifact(SnapshotArtifact::new(
        Some(LocalArtifactId::new()),
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
    )?);
    let mut unblocked_backup_tx = pool.begin().await?;
    let (_, outcome) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        insert(&mut *unblocked_backup_tx, &unblocked_snapshot),
    )
    .await
    .map_err(|_| "insert blocked on concurrent FOR NO KEY UPDATE")??;
    unblocked_backup_tx.commit().await?;
    no_key_tx.rollback().await?;
    assert_eq!(outcome, InsertOutcome::Created);

    // Interleaving 4: two backup attempts for the same artifact where attempt 2
    // opens its transaction before attempt 1, then waits on attempt 1's
    // per-artifact advisory lock while the sandbox isn't `restoring`. Stamping
    // `clock_timestamp()` (rather than pre-wait `NOW()`) gives attempt 2 a
    // strictly newer `"timestamp"` than attempt 1.
    let sandbox_advisory_order = SandboxId::new();
    insert_placed_sandbox(pool, sandbox_advisory_order, "suspended", None, None, None).await?;
    let shared_artifact = SnapshotArtifact::new(
        Some(LocalArtifactId::new()),
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
    )?;
    let mut second_attempt_tx = pool.begin().await?;
    let second_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *second_attempt_tx)
        .await?;

    let mut first_attempt_tx = pool.begin().await?;
    let first_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *first_attempt_tx)
        .await?;
    let first_id = SnapshotId::new();
    let first_snapshot = NewSnapshot::new(
        first_id,
        sandbox_advisory_order,
        SnapshotKind::Full,
        SnapshotState::Creating,
        OrgId::from_uuid(SEEDED_ORG_ID),
        ExactStorageUri::parse(&format!(
            "s3://test-bucket/snapshots/{sandbox_advisory_order}/{first_id}?region=us-east-1"
        ))?,
    )
    .with_artifact(shared_artifact.clone());
    let (_, first_outcome) = insert(&mut *first_attempt_tx, &first_snapshot).await?;
    assert_eq!(first_outcome, InsertOutcome::Created);

    let second_id = SnapshotId::new();
    let second_snapshot = NewSnapshot::new(
        second_id,
        sandbox_advisory_order,
        SnapshotKind::Full,
        SnapshotState::Creating,
        OrgId::from_uuid(SEEDED_ORG_ID),
        ExactStorageUri::parse(&format!(
            "s3://test-bucket/snapshots/{sandbox_advisory_order}/{second_id}?region=us-east-1"
        ))?,
    )
    .with_artifact(shared_artifact);

    let second_insert_fut = async {
        let (_, outcome) = insert(&mut *second_attempt_tx, &second_snapshot).await?;
        second_attempt_tx.commit().await?;
        TestResult::Ok(outcome)
    };
    let release_first_fut = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let blocked_by_first: bool =
                sqlx::query_scalar(sql!("SELECT $1 = ANY(pg_blocking_pids($2))"))
                    .bind(first_pid)
                    .bind(second_pid)
                    .fetch_one(pool)
                    .await?;
            if blocked_by_first {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err(
                    "second attempt never waited on first attempt's artifact advisory lock".into(),
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        first_attempt_tx.commit().await?;
        TestResult::Ok(())
    };
    let (second_res, release_res) = tokio::join!(second_insert_fut, release_first_fut);
    release_res?;
    assert_eq!(second_res?, InsertOutcome::Created);

    let strictly_ordered: bool = sqlx::query_scalar(sql!(
        "SELECT second_row.timestamp > first_row.timestamp
           FROM snapshots first_row
           JOIN snapshots second_row ON second_row.id = $2
          WHERE first_row.id = $1"
    ))
    .bind(first_id)
    .bind(second_id)
    .fetch_one(pool)
    .await?;
    assert!(
        strictly_ordered,
        "attempt unblocked from artifact advisory lock must stamp a strictly newer timestamp than the prior holder"
    );

    // Interleaving 5: a verified `BeginArtifactBackup` replay racing a `READY`
    // finalization (`UPDATE snapshots SET state = 'ready'` followed by
    // `FOR UPDATE` on `sandboxes`) must not lock `sandboxes FOR KEY SHARE`
    // before checking the existing `snapshots` row, or the two transactions
    // deadlock (`40P01`).
    let sandbox_replay_finalize = SandboxId::new();
    insert_placed_sandbox(
        pool,
        sandbox_replay_finalize,
        "suspending",
        None,
        None,
        None,
    )
    .await?;
    let replay_id = SnapshotId::new();
    let replay_snapshot = NewSnapshot::new(
        replay_id,
        sandbox_replay_finalize,
        SnapshotKind::Full,
        SnapshotState::Creating,
        OrgId::from_uuid(SEEDED_ORG_ID),
        ExactStorageUri::parse(&format!(
            "s3://test-bucket/snapshots/{sandbox_replay_finalize}/{replay_id}?region=us-east-1"
        ))?,
    )
    .with_artifact(SnapshotArtifact::new(
        Some(LocalArtifactId::new()),
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
    )?);
    let mut initial_tx = pool.begin().await?;
    let (_, initial_outcome) = insert(&mut *initial_tx, &replay_snapshot).await?;
    initial_tx.commit().await?;
    assert_eq!(initial_outcome, InsertOutcome::Created);

    let mut finalize_tx = pool.begin().await?;
    let finalize_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *finalize_tx)
        .await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET state = 'ready'
          WHERE id = $1
            AND state IN ('creating', 'ready')"
    ))
    .bind(replay_id)
    .execute(&mut *finalize_tx)
    .await?;

    let mut replay_tx = pool.begin().await?;
    let replay_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *replay_tx)
        .await?;
    let (replay_inserted_tx, replay_inserted_rx) = tokio::sync::oneshot::channel();
    let (release_replay_tx, release_replay_rx) = tokio::sync::oneshot::channel();

    let replay_fut = async {
        let (_, outcome) = insert(&mut *replay_tx, &replay_snapshot).await?;
        let _ = replay_inserted_tx.send(());
        let _ = release_replay_rx.await;
        replay_tx.commit().await?;
        TestResult::Ok(outcome)
    };
    let finalize_fut = async {
        let wait_blocked = async {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let blocked_by_finalize: bool =
                    sqlx::query_scalar(sql!("SELECT $1 = ANY(pg_blocking_pids($2))"))
                        .bind(finalize_pid)
                        .bind(replay_pid)
                        .fetch_one(pool)
                        .await?;
                if blocked_by_finalize {
                    return TestResult::Ok(());
                }
                if tokio::time::Instant::now() > deadline {
                    return Err("timed out waiting for replay to finish or block".into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            _ = replay_inserted_rx => {}
            blocked = wait_blocked => {
                blocked?;
            }
        }
        sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_replay_finalize)
            .execute(&mut *finalize_tx)
            .await?;
        finalize_tx.commit().await?;
        let _ = release_replay_tx.send(());
        TestResult::Ok(())
    };
    let (replay_res, finalize_res) =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(replay_fut, finalize_fut)
        })
        .await
        .map_err(|_| "idempotent replay and finalize deadlocked or timed out")?;
    finalize_res?;
    assert_eq!(replay_res?, InsertOutcome::AlreadyRegistered);

    // Interleaving 6: if an earlier registration of the same artifact recorded a
    // `snapshots."timestamp"` slightly ahead of a later new-id registration's
    // `clock_timestamp()` (for instance after a backward DB wall-clock jump),
    // the later row floors its `timestamp` at `prior."timestamp" + 1us` so
    // `capture_registered_at` (`MIN("timestamp")`) stays anchored to the first
    // registration and `timestamp` stays strictly monotonic across attempts.
    let sandbox_clock_jump = SandboxId::new();
    insert_placed_sandbox(pool, sandbox_clock_jump, "suspended", None, None, None).await?;
    let clock_jump_artifact_id = LocalArtifactId::new();
    let clock_jump_artifact = SnapshotArtifact::new(
        Some(clock_jump_artifact_id),
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
    )?;
    let prior_jump_id = SnapshotId::new();
    let prior_jump_ts: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, \"timestamp\",
             local_artifact_id, artifact_kind, artifact_origin, org_id
         )
         VALUES (
             $1, $2, 'full', 'failed', clock_timestamp() + INTERVAL '5 seconds',
             $3, 'full', 'suspend', $4
         )
         RETURNING \"timestamp\""
    ))
    .bind(prior_jump_id)
    .bind(sandbox_clock_jump)
    .bind(clock_jump_artifact_id)
    .bind(SEEDED_ORG_ID)
    .fetch_one(pool)
    .await?;

    let later_jump_id = SnapshotId::new();
    let later_jump_snapshot = NewSnapshot::new(
        later_jump_id,
        sandbox_clock_jump,
        SnapshotKind::Full,
        SnapshotState::Ready,
        OrgId::from_uuid(SEEDED_ORG_ID),
        ExactStorageUri::parse(&format!(
            "s3://test-bucket/snapshots/{sandbox_clock_jump}/{later_jump_id}?region=us-east-1"
        ))?,
    )
    .with_artifact(clock_jump_artifact);
    let mut jump_tx = pool.begin().await?;
    let (_, jump_outcome) = insert(&mut *jump_tx, &later_jump_snapshot).await?;
    jump_tx.commit().await?;
    assert_eq!(jump_outcome, InsertOutcome::Created);

    let (later_jump_ts, min_jump_ts): (time::OffsetDateTime, time::OffsetDateTime) =
        sqlx::query_as(sql!(
            "SELECT
                 (SELECT \"timestamp\" FROM snapshots WHERE id = $1),
                 (SELECT MIN(\"timestamp\")
                    FROM snapshots
                   WHERE sandbox_id = $2
                     AND local_artifact_id = $3)"
        ))
        .bind(later_jump_id)
        .bind(sandbox_clock_jump)
        .bind(clock_jump_artifact_id)
        .fetch_one(pool)
        .await?;
    assert!(
        later_jump_ts > prior_jump_ts,
        "later registration timestamp ({later_jump_ts}) must stay strictly after prior artifact row ({prior_jump_ts})"
    );
    assert_eq!(
        min_jump_ts, prior_jump_ts,
        "capture_registered_at (MIN(timestamp)) must not move backward across re-registrations"
    );

    Ok(())
}
