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
    request_delete(pool, sandbox_id).await?;
    assert!(!activate(pool, id, execution_b, 2, activation_b, None).await?);
    Ok(())
}
