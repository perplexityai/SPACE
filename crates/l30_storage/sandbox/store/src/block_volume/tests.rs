use super::*;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_execution_spec::{
    BlockVolumeBackend, BlockVolumePermissions, BlockVolumeSizeGiB, GuestBlockMountPath,
};
use pplx_space_test_env::TestResult;

const ZONE: &str = "test-region-1a";
const INSTANCE: &str = "i-00000000000000001";
const DEVICE: &str = "/dev/sdf";
const CLOUD_ID: &str = "vol-00000000000000001";

#[tokio::test]
async fn block_volume_authority_outlives_the_temporary_activation_claim() -> TestResult {
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
    let db = pg_test_db().await?;
    let sandbox = SandboxId::new();
    let route = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id,user_id,org_id,network_policy_generation)
         VALUES ($1,$2,$3,1)"
    ))
    .bind(sandbox)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
         (sandbox_id,generation,pod_name,pod_namespace,state,active,
          activation_attempt_id,route_activation_id)
         VALUES ($1,7,'execution-test','test','running',TRUE,$2,$3)"
    ))
    .bind(sandbox)
    .bind(attempt)
    .bind(route)
    .execute(&db.pool)
    .await?;
    for temporary_claim in [Some(attempt), None] {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET activation_attempt_id = $2
             WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox)
        .bind(temporary_claim)
        .execute(&db.pool)
        .await?;
        let authority = current_execution(&db.pool, sandbox).await?.unwrap();
        let lifecycle = crate::auto_lifecycle_execution(&db.pool, sandbox)
            .await?
            .unwrap();
        assert_eq!(authority.execution_generation, Some(7));
        assert_eq!(authority.execution_id, Some(route));
        assert_eq!(lifecycle.activation_attempt_id, temporary_claim);
    }
    Ok(())
}

#[tokio::test]
async fn independent_reservation_survives_detach_and_releases_only_with_deleted_owner() -> TestResult
{
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
    let db = pg_test_db().await?;
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let volume = Uuid::now_v7();
    let a = SandboxId::new();
    let b = SandboxId::new();
    for sandbox in [a, b] {
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id,user_id,org_id,lifecycle,network_policy_generation)
             VALUES ($1,$2,$3,'stopped',1)"
        ))
        .bind(sandbox)
        .bind(SEED_USER_ID)
        .bind(org)
        .execute(&db.pool)
        .await?;
    }
    let mut tx = db.pool.begin().await?;
    resource::reserve(
        &mut tx,
        resource::NewResource {
            id: volume,
            org_id: org,
            managed_by: "test",
            availability_zone: ZONE,
            resource_limit: 1,
        },
    )
    .await?;
    tx.commit().await?;
    let row = resource::get(&db.pool, org, volume).await?.unwrap();
    assert!(row.sandbox_id.is_none() && row.mount_spec.is_none());
    assert!(
        resource::get(&db.pool, OrgId::from_uuid(Uuid::now_v7()), volume)
            .await?
            .is_none()
    );
    assert!(resource::observe(&db.pool, &row, VolumePhase::Available, Some(CLOUD_ID), None).await?);
    let mut tx = db.pool.begin().await?;
    // A pre-upgrade reconciler must never deserialize an unbound resource.
    assert!(lock_next(&mut tx, "test").await?.is_none());
    assert!(matches!(
        resource::reserve(
            &mut tx,
            resource::NewResource {
                id: Uuid::now_v7(),
                org_id: org,
                managed_by: "test",
                availability_zone: ZONE,
                resource_limit: 1,
            }
        )
        .await,
        Err(VolumeStoreError::Capacity)
    ));
    tx.rollback().await?;
    let mount: BlockVolumeMount = serde_json::from_value(serde_json::json!({
        "type":"ebs", "size_gib":128, "permissions":"read_write", "mount":"/data"
    }))?;
    let mut tx = db.pool.begin().await?;
    assert!(matches!(
        resource::bind(&mut tx, OrgId::new(), volume, a, "test", ZONE, &mount).await,
        Err(VolumeStoreError::Conflict)
    ));
    resource::bind(&mut tx, org, volume, a, "test", ZONE, &mount).await?;
    tx.commit().await?;
    let mut tx = db.pool.begin().await?;
    assert!(matches!(
        resource::bind(&mut tx, org, volume, b, "test", ZONE, &mount).await,
        Err(VolumeStoreError::Conflict)
    ));
    assert!(!resource::request_delete(&mut tx, org, volume, Uuid::now_v7()).await?);
    tx.commit().await?;
    let execution_a = Uuid::now_v7();
    let bound_a = attach(&db.pool, a, execution_a).await?;
    let initialization = Uuid::now_v7();
    assert!(claim_initialization(&db.pool, volume, execution_a, 1, initialization).await?);
    assert!(
        complete_initialization(
            &db.pool,
            volume,
            execution_a,
            1,
            initialization,
            bound_a.filesystem_uuid,
        )
        .await?
    );
    assert!(activate(&db.pool, volume, execution_a, 1, Uuid::now_v7(), None).await?);
    let checkpoint = Uuid::now_v7();
    assert!(publish_checkpoint(&db.pool, volume, execution_a, 1, 1, checkpoint).await?);
    assert!(acknowledge_release(&db.pool, volume, execution_a, 1).await?);
    let detaching = get(&db.pool, a).await?.unwrap();
    assert!(finish_detach(&db.pool, &detaching).await?);
    assert!(!resource::release_deleted_owner(&db.pool, volume).await?);
    // Physical availability with a suspended dependency is not free storage.
    assert_eq!(
        resource::get(&db.pool, org, volume)
            .await?
            .unwrap()
            .sandbox_id,
        Some(a)
    );
    let mut old_writer = db.pool.begin().await?;
    let error = sqlx::query(sql!(
        "UPDATE sandbox_block_volumes SET delete_requested = TRUE WHERE id = $1"
    ))
    .bind(volume)
    .execute(&mut *old_writer)
    .await
    .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23514")
    );
    old_writer.rollback().await?;
    request_delete(&db.pool, a).await?;
    assert!(!get(&db.pool, a).await?.unwrap().delete_requested);
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(a)
        .execute(&db.pool)
        .await?;
    assert!(!request_orphan_deletion(&db.pool, volume).await?);
    assert!(resource::release_deleted_owner(&db.pool, volume).await?);
    let readonly = BlockVolumeMount {
        permissions: BlockVolumePermissions::ReadOnly,
        ..mount
    };
    let mut tx = db.pool.begin().await?;
    resource::bind(&mut tx, org, volume, b, "test", ZONE, &readonly).await?;
    tx.commit().await?;
    assert_eq!(
        get(&db.pool, b).await?.unwrap().mount_spec.permissions,
        BlockVolumePermissions::ReadOnly
    );
    let execution_b = Uuid::now_v7();
    let bound_b = attach(&db.pool, b, execution_b).await?;
    assert_eq!(bound_b.filesystem_uuid, bound_a.filesystem_uuid);
    assert!(!claim_initialization(&db.pool, volume, execution_b, 2, Uuid::now_v7()).await?);
    assert!(
        activate_with_source(
            &db.pool,
            volume,
            execution_b,
            2,
            Uuid::now_v7(),
            ActivationSource::RetainedColdStart,
        )
        .await?
    );
    assert_eq!(get(&db.pool, b).await?.unwrap().disk_generation, 2);
    assert!(!acknowledge_release(&db.pool, volume, execution_a, 1).await?);
    let node = bound_b.node_id.unwrap();
    let mut tx = db.pool.begin().await?;
    assert!(!acknowledge_deleted_owner_release(&mut tx, b, node).await?);
    tx.commit().await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(b)
        .execute(&db.pool)
        .await?;
    let mut tx = db.pool.begin().await?;
    assert!(!acknowledge_deleted_owner_release(&mut tx, b, NodeId::new()).await?);
    assert!(acknowledge_deleted_owner_release(&mut tx, b, node).await?);
    tx.commit().await?;
    let detaching = get(&db.pool, b).await?.unwrap();
    assert_eq!(detaching.released_epoch, Some(2));
    assert!(finish_detach(&db.pool, &detaching).await?);
    let mut tx = db.pool.begin().await?;
    assert!(acknowledge_deleted_owner_release(&mut tx, b, node).await?);
    tx.commit().await?;
    assert!(resource::release_deleted_owner(&db.pool, volume).await?);
    let mut tx = db.pool.begin().await?;
    assert!(resource::request_delete(&mut tx, org, volume, Uuid::now_v7()).await?);
    assert!(
        resource::bind(&mut tx, org, volume, a, "test", ZONE, &readonly)
            .await
            .is_err()
    );
    tx.commit().await?;
    assert!(
        resource::get(&db.pool, org, volume)
            .await?
            .unwrap()
            .delete_requested
    );
    Ok(())
}

#[tokio::test]
async fn client_delete_requires_stopped_detached_checkpoint_free_owner_and_fences_attach()
-> TestResult {
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
    let db = pg_test_db().await?;
    let sandbox = SandboxId::new();
    let org = OrgId::from_uuid(SEED_ORG_ID);
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id,user_id,org_id,lifecycle,network_policy_generation)
        VALUES ($1,$2,$3,'stopped',1)"
    ))
    .bind(sandbox)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let volume = reserve_volume(&db.pool, sandbox, 2).await?;
    let row = get(&db.pool, sandbox).await?.unwrap();
    assert!(observe(&db.pool, &row, VolumePhase::Available, Some(CLOUD_ID), None).await?);
    // Activation's sandbox-row fence must serialize even while the volume
    // still looks detached. Exercise a real competing PostgreSQL transaction.
    let mut activation = db.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox)
        .execute(&mut *activation)
        .await?;
    let mut deletion = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '50ms'"))
        .execute(&mut *deletion)
        .await?;
    let error = request_detached_delete(&mut deletion, org, volume)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    deletion.rollback().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'starting' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&mut *activation)
    .await?;
    activation.commit().await?;
    let mut deletion = db.pool.begin().await?;
    assert!(!request_detached_delete(&mut deletion, org, volume).await?);
    deletion.commit().await?;
    for lifecycle in [
        "started",
        "paused",
        "suspended",
        "starting",
        "stopping",
        "suspending",
    ] {
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox)
            .bind(lifecycle)
            .execute(&db.pool)
            .await?;
        let mut tx = db.pool.begin().await?;
        assert!(
            !request_detached_delete(&mut tx, org, volume).await?,
            "{lifecycle}"
        );
        tx.commit().await?;
        assert!(!get(&db.pool, sandbox).await?.unwrap().delete_requested);
    }
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
        SET checkpoint_id = $2, checkpoint_generation = disk_generation WHERE id = $1"
    ))
    .bind(volume)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let mut tx = db.pool.begin().await?;
    assert!(!request_detached_delete(&mut tx, org, volume).await?);
    tx.commit().await?;
    sqlx::query(sql!(
        "UPDATE sandbox_block_volumes
        SET checkpoint_id = NULL, checkpoint_generation = NULL WHERE id = $1"
    ))
    .bind(volume)
    .execute(&db.pool)
    .await?;
    let mut tx = db.pool.begin().await?;
    assert!(!request_detached_delete(&mut tx, OrgId::new(), volume).await?);
    assert!(request_detached_delete(&mut tx, org, volume).await?);
    tx.commit().await?;
    let mut tx = db.pool.begin().await?;
    assert!(
        !reserve_attachment(
            &mut tx,
            sandbox,
            AttachmentTarget {
                node_id: NodeId::new(),
                instance_id: INSTANCE,
                device_name: DEVICE,
                execution_id: Uuid::now_v7(),
                availability_zone: ZONE,
                host_attachment_limit: 1,
            }
        )
        .await?
    );
    assert!(request_detached_delete(&mut tx, org, volume).await?);
    tx.commit().await?;
    assert_eq!(
        get(&db.pool, sandbox).await?.unwrap().phase,
        VolumePhase::Available
    );
    assert!(matches!(
        reserve_volume(&db.pool, SandboxId::new(), 1).await,
        Err(VolumeStoreError::Capacity)
    ));
    Ok(())
}

fn mount_spec() -> BlockVolumeMount {
    BlockVolumeMount {
        backend: BlockVolumeBackend::Ebs,
        size_gib: BlockVolumeSizeGiB::try_from(128).unwrap(),
        permissions: BlockVolumePermissions::ReadWrite,
        mount: GuestBlockMountPath::try_from("/data".to_owned()).unwrap(),
    }
}

async fn reserve_volume(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    limit: u32,
) -> Result<Uuid, VolumeStoreError> {
    let mut tx = pool.begin().await?;
    let id = reserve(
        &mut tx,
        NewBlockVolume {
            sandbox_id,
            org_id: OrgId::from_uuid(Uuid::from_u128(1)),
            managed_by: "test",
            mount_spec: &mount_spec(),
            availability_zone: ZONE,
            resource_limit: limit,
        },
    )
    .await?;
    tx.commit().await?;
    Ok(id)
}

async fn attach(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    execution_id: Uuid,
) -> TestResult<BlockVolume> {
    let mut tx = pool.begin().await?;
    assert!(
        reserve_attachment(
            &mut tx,
            sandbox_id,
            AttachmentTarget {
                node_id: NodeId::from_uuid(Uuid::from_u128(2)),
                instance_id: INSTANCE,
                device_name: DEVICE,
                execution_id,
                availability_zone: ZONE,
                host_attachment_limit: 1,
            },
        )
        .await?
    );
    tx.commit().await?;
    let attaching = get(pool, sandbox_id).await?.unwrap();
    assert!(observe(pool, &attaching, VolumePhase::Attached, None, None).await?);
    Ok(get(pool, sandbox_id).await?.unwrap())
}

#[tokio::test]
async fn admissions_serialize_and_count_unresolved_and_retained_resources() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let id = reserve_volume(&db.pool, sandbox_id, 2).await?;
    assert_eq!(reserve_volume(&db.pool, sandbox_id, 2).await?, id);
    let visible = statuses(
        &db.pool,
        OrgId::from_uuid(Uuid::from_u128(1)),
        &[sandbox_id],
    )
    .await?;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, id);
    assert_eq!(visible[0].mount_spec.0, mount_spec());
    assert_eq!(visible[0].phase, VolumePhase::Requested);
    assert!(
        statuses(&db.pool, OrgId::new(), &[sandbox_id])
            .await?
            .is_empty()
    );
    assert!(
        statuses(
            &db.pool,
            OrgId::from_uuid(Uuid::from_u128(1)),
            &[SandboxId::new()]
        )
        .await?
        .is_empty()
    );

    let (a, b) = tokio::join!(
        reserve_volume(&db.pool, SandboxId::new(), 2),
        reserve_volume(&db.pool, SandboxId::new(), 2)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let refused = if a.is_err() { a } else { b };
    assert!(matches!(refused, Err(VolumeStoreError::Capacity)));
    request_delete(&db.pool, sandbox_id).await?;
    assert!(matches!(
        reserve_volume(&db.pool, SandboxId::new(), 2).await,
        Err(VolumeStoreError::Capacity)
    ));
    // Cleanup intent cannot turn a used resource into reusable inventory.
    assert!(matches!(
        reserve_volume(&db.pool, sandbox_id, 2).await,
        Err(VolumeStoreError::Conflict)
    ));
    Ok(())
}

#[tokio::test]
async fn retained_checkpoint_is_consumed_once_and_old_epochs_cannot_release() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    let id = reserve_volume(pool, sandbox_id, 2).await?;
    let requested = get(pool, sandbox_id).await?.unwrap();
    assert!(observe(pool, &requested, VolumePhase::Provisioning, None, None).await?);
    // A stale reconciler cannot overwrite the newer observation.
    assert!(!observe(pool, &requested, VolumePhase::Deleted, None, None).await?);
    let provisioning = get(pool, sandbox_id).await?.unwrap();
    assert!(
        observe(
            pool,
            &provisioning,
            VolumePhase::Available,
            Some(CLOUD_ID),
            None
        )
        .await?
    );
    let execution_a = Uuid::now_v7();
    let bound_a = attach(pool, sandbox_id, execution_a).await?;
    let claim = Uuid::now_v7();
    assert!(claim_initialization(pool, id, execution_a, 1, claim).await?);
    assert!(!claim_initialization(pool, id, execution_a, 1, Uuid::now_v7()).await?);
    assert!(!complete_initialization(pool, id, execution_a, 1, claim, Uuid::now_v7()).await?);
    assert!(
        complete_initialization(pool, id, execution_a, 1, claim, bound_a.filesystem_uuid).await?
    );
    let activation_a = Uuid::now_v7();
    assert!(activate(pool, id, execution_a, 1, activation_a, None).await?);
    assert!(activate(pool, id, execution_a, 1, activation_a, None).await?);
    assert_eq!(get(pool, sandbox_id).await?.unwrap().disk_generation, 1);
    assert!(!activate(pool, id, execution_a, 1, Uuid::now_v7(), None).await?);
    let checkpoint_a = Uuid::now_v7();
    assert!(!publish_checkpoint(pool, id, execution_a, 1, 0, checkpoint_a).await?);
    assert!(publish_checkpoint(pool, id, execution_a, 1, 1, checkpoint_a).await?);
    assert!(!activate(pool, id, execution_a, 1, Uuid::now_v7(), Some(checkpoint_a)).await?);
    // Failed suspend may resume only its existing owner. Withdraw the
    // published pairing before writes resume, without consuming a new epoch.
    assert!(activate(pool, id, execution_a, 1, activation_a, None).await?);
    let resumed = get(pool, sandbox_id).await?.unwrap();
    assert_eq!(resumed.phase, VolumePhase::Attached);
    assert_eq!(resumed.checkpoint_id, None);
    assert_eq!(resumed.disk_generation, 1);
    assert!(publish_checkpoint(pool, id, execution_a, 1, 1, checkpoint_a).await?);
    let releasing = get(pool, sandbox_id).await?.unwrap();
    assert!(!finish_detach(pool, &releasing).await?);
    assert!(acknowledge_release(pool, id, execution_a, 1).await?);
    assert!(!activate(pool, id, execution_a, 1, activation_a, None).await?);
    let detaching = get(pool, sandbox_id).await?.unwrap();
    assert!(finish_detach(pool, &detaching).await?);
    let execution_b = Uuid::now_v7();
    let bound_b = attach(pool, sandbox_id, execution_b).await?;
    assert_eq!(bound_b.aws_volume_id.as_deref(), Some(CLOUD_ID));
    assert_eq!(bound_b.filesystem_uuid, bound_a.filesystem_uuid);
    assert_eq!(bound_b.attachment_epoch, 2);
    assert!(!acknowledge_release(pool, id, execution_a, 1).await?);
    // Reusing an old activation ID on a new epoch is not an idempotent retry.
    assert!(!activate(pool, id, execution_b, 2, activation_a, None).await?);
    // Cold boot is never a substitute for a still-valid RAM/disk pairing.
    assert!(
        !activate_with_source(
            pool,
            id,
            execution_b,
            2,
            Uuid::now_v7(),
            ActivationSource::RetainedColdStart,
        )
        .await?
    );
    assert!(
        !activate(
            pool,
            id,
            execution_b,
            2,
            Uuid::now_v7(),
            Some(Uuid::now_v7())
        )
        .await?
    );
    let activation_b = Uuid::now_v7();
    assert!(activate(pool, id, execution_b, 2, activation_b, Some(checkpoint_a)).await?);
    assert!(!activate(pool, id, execution_b, 2, Uuid::now_v7(), Some(checkpoint_a)).await?);
    let activated = get(pool, sandbox_id).await?.unwrap();
    assert_eq!(activated.disk_generation, 2);
    assert_eq!(activated.checkpoint_id, None);
    assert!(!claim_initialization(pool, id, execution_b, 2, Uuid::now_v7()).await?);

    // A stop without checkpointing permits only a fresh, explicitly qualified
    // cold activation after observed detach, not replay of saved RAM.
    assert!(acknowledge_release(pool, id, execution_b, 2).await?);
    let detaching = get(pool, sandbox_id).await?.unwrap();
    assert!(finish_detach(pool, &detaching).await?);
    let execution_c = Uuid::now_v7();
    let bound_c = attach(pool, sandbox_id, execution_c).await?;
    let activation_c = Uuid::now_v7();
    assert_eq!(bound_c.filesystem_uuid, bound_a.filesystem_uuid);
    assert_eq!(bound_c.aws_volume_id.as_deref(), Some(CLOUD_ID));
    assert!(!activate(pool, id, execution_c, 3, activation_c, None).await?);
    assert!(!activate(pool, id, execution_c, 3, activation_c, Some(checkpoint_a)).await?);
    assert!(
        !activate_with_source(
            pool,
            id,
            execution_c,
            3,
            activation_b,
            ActivationSource::RetainedColdStart,
        )
        .await?
    );
    for _ in 0..2 {
        assert!(
            activate_with_source(
                pool,
                id,
                execution_c,
                3,
                activation_c,
                ActivationSource::RetainedColdStart,
            )
            .await?
        );
        assert_eq!(get(pool, sandbox_id).await?.unwrap().disk_generation, 3);
    }
    assert!(
        !activate_with_source(
            pool,
            id,
            execution_c,
            3,
            Uuid::now_v7(),
            ActivationSource::RetainedColdStart,
        )
        .await?
    );
    assert!(!claim_initialization(pool, id, execution_c, 3, Uuid::now_v7()).await?);
    request_delete(pool, sandbox_id).await?;
    assert!(
        !activate_with_source(
            pool,
            id,
            execution_c,
            3,
            activation_c,
            ActivationSource::RetainedColdStart,
        )
        .await?
    );
    Ok(())
}
