use pplx_space_common_core::types::SandboxId;

use super::*;

mod postgres_tests {

    use pplx_space_common_core::types::SnapshotId;
    use pplx_space_db_test_fixtures::{
        insert_pool_slot_receipt, pg_test_db, pool_slot_receipt_state,
    };
    use uuid::Uuid;

    use super::*;

    use pplx_space_test_env::TestResult;

    async fn insert_sandbox(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, placement, managed_by,
                 network_policy_override_json, network_policy_generation
             )
             VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 $2,
                 'unplaced',
                 'k8s',
                 $3,
                 1
             )"
        ))
        .bind(sandbox_id)
        .bind(lifecycle)
        .bind(sqlx::types::Json(
            &pplx_space_net_common::NetworkPolicyV1::default(),
        ))
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_snapshot_with_storage_location(
        pool: &PgPool,
        sandbox_id: SandboxId,
        snapshot_id: SnapshotId,
        storage_location_id: Uuid,
    ) -> TestResult {
        sqlx::query(sql!(
            "INSERT INTO snapshots (id, sandbox_id, type, state)
             VALUES ($1, $2, 'full', 'ready')"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (
                 id, snapshot_id, storage_connection_string
             )
             VALUES ($1, $2, $3)"
        ))
        .bind(storage_location_id)
        .bind(snapshot_id)
        .bind(format!("s3://test-bucket/snapshots/{snapshot_id}"))
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn sandbox_exists(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
        let count: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
        Ok(count > 0)
    }

    async fn snapshot_rows_for_sandbox(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<i64> {
        let count: i64 =
            sqlx::query_scalar(sql!("SELECT COUNT(*) FROM snapshots WHERE sandbox_id = $1"))
                .bind(sandbox_id)
                .fetch_one(pool)
                .await?;
        Ok(count)
    }

    async fn storage_location_rows_for_snapshot(
        pool: &PgPool,
        snapshot_id: SnapshotId,
    ) -> TestResult<i64> {
        let count: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM snapshot_storage_locations WHERE snapshot_id = $1"
        ))
        .bind(snapshot_id)
        .fetch_one(pool)
        .await?;
        Ok(count)
    }

    async fn storage_location_orphan_row(
        pool: &PgPool,
        storage_location_id: Uuid,
    ) -> TestResult<Option<(Option<Uuid>, Option<time::OffsetDateTime>)>> {
        Ok(
            sqlx::query_as::<_, (Option<Uuid>, Option<time::OffsetDateTime>)>(sql!(
                "SELECT snapshot_id, orphaned_at
             FROM snapshot_storage_locations
             WHERE id = $1"
            ))
            .bind(storage_location_id)
            .fetch_optional(pool)
            .await?,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finalize_sandbox_db_delete_orphans_suspended_snapshot_storage_locations() -> TestResult
    {
        let db = pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        let storage_location_id = Uuid::now_v7();

        insert_sandbox(&db.pool, sandbox_id, "suspended").await?;
        insert_snapshot_with_storage_location(
            &db.pool,
            sandbox_id,
            snapshot_id,
            storage_location_id,
        )
        .await?;

        finalize_sandbox_db_delete(&db.pool, sandbox_id).await?;

        assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
        assert_eq!(snapshot_rows_for_sandbox(&db.pool, sandbox_id).await?, 0);
        // Storage-location row survives (detached from snapshot, stamped
        // orphaned_at) for the S3 sweeper to reap.
        assert_eq!(
            storage_location_rows_for_snapshot(&db.pool, snapshot_id).await?,
            0
        );
        let orphan = storage_location_orphan_row(&db.pool, storage_location_id)
            .await?
            .expect("storage_location row should survive sandbox delete");
        assert!(
            orphan.0.is_none(),
            "snapshot_id should be NULL after orphan"
        );
        assert!(orphan.1.is_some(), "orphaned_at should be stamped");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finalize_sandbox_db_delete_retires_warm_pool_receipt() -> TestResult {
        let db = pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        insert_sandbox(&db.pool, sandbox_id, "stopped").await?;
        insert_pool_slot_receipt(&db.pool, sandbox_id, "bound").await?;

        finalize_sandbox_db_delete(&db.pool, sandbox_id).await?;

        assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
        assert_eq!(pool_slot_receipt_state(&db.pool, sandbox_id).await?, None);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finalize_sandbox_db_delete_succeeds_without_snapshots() -> TestResult {
        let db = pg_test_db().await?;
        let sandbox_id = SandboxId::new();

        insert_sandbox(&db.pool, sandbox_id, "started").await?;

        finalize_sandbox_db_delete(&db.pool, sandbox_id).await?;

        assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
        Ok(())
    }

    async fn insert_snapshot_with_state(
        pool: &PgPool,
        sandbox_id: SandboxId,
        snapshot_id: SnapshotId,
        state: &str,
    ) -> TestResult {
        sqlx::query(sql!(
            "INSERT INTO snapshots (id, sandbox_id, type, state)
             VALUES ($1, $2, 'full', $3)"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(state)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// User-facing DELETE must refuse to orphan storage-location
    /// rows while a suspend-snapshot upload is still in flight
    /// (`snapshots.state = 'creating'`). Otherwise the orphan-on-
    /// delete path would race vol-mgr writing to the same S3
    /// prefix and leak objects with no DB anchor for the sweeper.
    #[tokio::test(flavor = "current_thread")]
    async fn finalize_sandbox_db_delete_user_initiated_refuses_in_flight_upload() -> TestResult {
        let db = pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let creating_snapshot_id = SnapshotId::new();

        insert_sandbox(&db.pool, sandbox_id, "suspending").await?;
        insert_snapshot_with_state(&db.pool, sandbox_id, creating_snapshot_id, "creating").await?;

        let result = finalize_sandbox_db_delete_user_initiated(&db.pool, sandbox_id).await;
        assert!(
            matches!(
                result,
                Err(FinalizeSandboxDbDeleteError::InFlightSuspendUpload { .. })
            ),
            "expected InFlightSuspendUpload, got {result:?}"
        );

        // Sandbox row must survive — nothing was deleted.
        assert!(sandbox_exists(&db.pool, sandbox_id).await?);
        assert_eq!(
            snapshot_rows_for_sandbox(&db.pool, sandbox_id).await?,
            1,
            "creating snapshot row must remain after a refused DELETE"
        );
        Ok(())
    }

    /// `in_flight_snapshot_upload_idle_for` reports an upload iff
    /// there is at least one snapshot for the sandbox in
    /// `state='creating'`. Used by the HTTP DELETE pre-flight to
    /// refuse 409 before the runtime is torn down. Covers the
    /// happy path (creating exists), the negative (only ready),
    /// and the empty case (no snapshots at all).
    #[tokio::test(flavor = "current_thread")]
    async fn in_flight_snapshot_upload_idle_for_classifies_states() -> TestResult {
        let db = pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        insert_sandbox(&db.pool, sandbox_id, "suspending").await?;

        assert!(
            in_flight_snapshot_upload_idle_for(&db.pool, sandbox_id)
                .await?
                .is_none(),
            "no snapshots → not in flight"
        );

        let ready_snap = SnapshotId::new();
        insert_snapshot_with_state(&db.pool, sandbox_id, ready_snap, "ready").await?;
        assert!(
            in_flight_snapshot_upload_idle_for(&db.pool, sandbox_id)
                .await?
                .is_none(),
            "only 'ready' snapshots → not in flight"
        );

        let creating_snap = SnapshotId::new();
        insert_snapshot_with_state(&db.pool, sandbox_id, creating_snap, "creating").await?;
        assert!(
            in_flight_snapshot_upload_idle_for(&db.pool, sandbox_id)
                .await?
                .is_some(),
            "one 'creating' snapshot → in flight"
        );
        Ok(())
    }

    /// Companion to the refusal test: when every snapshot is
    /// already `ready` (upload done), the user-initiated DELETE
    /// proceeds exactly like the unguarded path — orphan
    /// stamped, sandbox + snapshots deleted.
    #[tokio::test(flavor = "current_thread")]
    async fn finalize_sandbox_db_delete_user_initiated_proceeds_when_ready() -> TestResult {
        let db = pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let snapshot_id = SnapshotId::new();
        let storage_location_id = Uuid::now_v7();

        insert_sandbox(&db.pool, sandbox_id, "suspended").await?;
        insert_snapshot_with_storage_location(
            &db.pool,
            sandbox_id,
            snapshot_id,
            storage_location_id,
        )
        .await?;

        finalize_sandbox_db_delete_user_initiated(&db.pool, sandbox_id).await?;

        assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
        assert_eq!(snapshot_rows_for_sandbox(&db.pool, sandbox_id).await?, 0);
        let orphan = storage_location_orphan_row(&db.pool, storage_location_id)
            .await?
            .expect("storage_location row should survive sandbox delete");
        assert!(
            orphan.0.is_none(),
            "snapshot_id should be NULL after orphan"
        );
        assert!(orphan.1.is_some(), "orphaned_at should be stamped");
        Ok(())
    }
}

#[test]
fn sandbox_reconcile_cleanup_refs_cover_entity_scoped_facets_only() {
    let sandbox_id = SandboxId::from_uuid("019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap());
    let refs = sandbox_reconcile_cleanup_refs(sandbox_id);

    assert_eq!(
        refs,
        vec![
            ReconcileElementRef::network_policy(NetworkPolicyTarget::sandbox(sandbox_id)),
            ReconcileElementRef::sandbox_desired_state(sandbox_id),
            ReconcileElementRef::sandbox_inventory(sandbox_id),
        ]
    );
    assert!(
        refs.iter()
            .all(|reference| !reference.element_string().is_empty())
    );
}
