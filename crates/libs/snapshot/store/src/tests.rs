use super::{
    GetSnapshotError, InsertSnapshotError, NewSnapshot, RecordArtifactDigestsError,
    SnapshotArtifact, SnapshotArtifactDigest, artifact_digests, delete_snapshot_catalog, get,
    insert, record_artifact_digests,
};
use jiff::Timestamp;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    Arch, ArtifactKind, ArtifactOrigin, Digest, ExactStorageUri, ExecutionGeneration,
    ExecutionMetadata, LocalArtifactId, NodeId, OrgId, SandboxId,
    SnapshotArtifact as SnapshotArtifactFile, SnapshotContentHash, SnapshotExecutionMetadata,
    SnapshotId, SnapshotKind, SnapshotName, SnapshotState, TemplateArtifactDigest, TemplateName,
};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEEDED_ORG_ID, SEED_USER_ID as SEEDED_USER_ID, try_pg_test_db,
};
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
    };
    let snapshot = make_snapshot(expected_uri.clone());
    let mut insert_tx = db.pool.begin().await?;
    let inserted = insert(&mut insert_tx, &snapshot).await?;
    assert_eq!(inserted.id(), inserted_snapshot_id);
    assert_eq!(
        inserted
            .storage_location()
            .expect("insert returns complete aggregate")
            .storage_connection_string(),
        expected_uri.as_wire()
    );
    insert_tx.commit().await?;

    let mut retry_tx = db.pool.begin().await?;
    let retried = insert(&mut retry_tx, &snapshot).await?;
    assert_eq!(retried, inserted);
    retry_tx.commit().await?;

    let mut location_conflict_tx = db.pool.begin().await?;
    let error = insert(
        &mut location_conflict_tx,
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
    let error = insert(&mut identity_conflict_tx, &conflicting_snapshot)
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
    assert!(delete_snapshot_catalog(&mut delete_tx, snapshot_id).await?);
    assert!(!delete_snapshot_catalog(&mut delete_tx, snapshot_id).await?);
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
    record_artifact_digests(&mut tx, snapshot_id, &digests).await?;
    tx.commit().await?;

    let mut expected = digests.to_vec();
    expected.sort_by_key(|digest| digest.artifact.file_name());
    assert_eq!(artifact_digests(&db.pool, snapshot_id).await?, expected);

    // An identical replay is a no-op; a conflicting one is refused and
    // leaves the recorded digests untouched.
    let mut tx = db.pool.begin().await?;
    record_artifact_digests(&mut tx, snapshot_id, &digests[..1]).await?;
    tx.commit().await?;
    let mut tx = db.pool.begin().await?;
    let conflict = record_artifact_digests(
        &mut tx,
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
    assert!(delete_snapshot_catalog(&mut tx, snapshot_id).await?);
    tx.commit().await?;
    assert!(artifact_digests(&db.pool, snapshot_id).await?.is_empty());
    Ok(())
}
