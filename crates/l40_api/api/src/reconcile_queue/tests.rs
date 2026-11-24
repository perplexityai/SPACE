use pplx_space_sandbox_model::SandboxId;

use super::*;

mod postgres_tests {

    use pplx_space_db_test_fixtures::{
        insert_pool_slot_receipt, insert_ready_worker_node, pg_test_db,
        pg_test_db_with_max_connections, pool_slot_receipt_state,
    };
    use pplx_space_internal_api_server::{
        ClusterApiService,
        proto::{PublishNodeStateRequest, PublishedSandboxState, cluster_api_server::ClusterApi},
        publish_sandbox_states,
    };
    use pplx_space_snapshot_model::SnapshotId;
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

    #[rstest::rstest]
    #[case::present(Some(true))]
    #[case::absent(Some(false))]
    #[case::omitted(None)]
    #[tokio::test(flavor = "current_thread")]
    async fn user_delete_finalization_racing_publication_does_not_deadlock(
        #[case] present: Option<bool>,
    ) -> TestResult {
        let db = pg_test_db_with_max_connections(4).await?;
        let node_id = NodeId::new();
        let sandbox_id = SandboxId::new();
        insert_ready_worker_node(&db.pool, node_id).await?;
        let lifecycle = if present == Some(true) {
            "started"
        } else {
            "stopped"
        };
        insert_sandbox(&db.pool, sandbox_id, lifecycle).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
             SET placement = 'on_node', active_node_id = $2, target_node_id = $2, managed_by = $3
             WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        .bind(if present == Some(true) {
            "k8s"
        } else {
            "space"
        })
        .execute(&db.pool)
        .await?;

        let mut state = PublishedSandboxState {
            sandbox_id: sandbox_id.to_string(),
            status: Some(lifecycle.to_owned()),
            present: true,
            ..Default::default()
        };
        let mut tx = db.pool.begin().await?;
        publish_sandbox_states(&mut tx, node_id, std::slice::from_ref(&state)).await?;
        tx.commit().await?;
        if present.is_none() {
            // Give the prune's DELETE a snapshot inventory row to visit.
            // Otherwise PostgreSQL can drain its two write CTEs in either
            // order without scanning their results.
            sqlx::query(sql!(
                "INSERT INTO sandbox_snapshot_inventory (sandbox_id, snapshot_id, type, state)
                 VALUES ($1, 'observed-snapshot', 'disk', 'ready')"
            ))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
        }
        if present == Some(true) {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
        }

        // Pause finalization at the fork-operation check, after it locks the
        // sandbox row. The publication then either updates observed state and
        // activity, removes an explicitly absent row, or prunes an omitted
        // sandbox in a full census. Wait for both writers to
        // block before releasing the gate so their overlap is deterministic.
        let mut gate = db.pool.begin().await?;
        sqlx::query(sql!(
            "LOCK TABLE public_operations IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(&mut *gate)
        .await?;
        let wait_for_writers = async |expected| {
            tokio::time::timeout(Duration::from_secs(5), async {
                while sqlx::query_scalar::<_, i64>(sql!(
                    "SELECT COUNT(*) FROM pg_stat_activity
                     WHERE datname = current_database() AND wait_event_type = 'Lock'"
                ))
                .fetch_one(&db.pool)
                .await?
                    < expected
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                TestResult::Ok(())
            })
            .await?
        };

        let pool = db.pool.clone();
        let delete = tokio::spawn(async move {
            finalize_sandbox_db_delete_user_initiated(&pool, sandbox_id).await
        });
        wait_for_writers(1).await?;

        if present == Some(true) {
            state.uptime = Some("1m".to_owned());
            state.last_access_at = Some(prost_types::Timestamp {
                seconds: 1_900_000_000,
                nanos: 0,
            });
        }
        let pool = db.pool.clone();
        let publish = tokio::spawn(async move {
            if let Some(present) = present {
                state.present = present;
                let mut tx = pool.begin().await?;
                publish_sandbox_states(&mut tx, node_id, &[state]).await?;
                tx.commit().await?;
            } else {
                let mut request = tonic::Request::new(PublishNodeStateRequest {
                    node_id: node_id.to_string(),
                    sandbox_inventory_complete: true,
                    ..Default::default()
                });
                request
                    .extensions_mut()
                    .insert(tonic::transport::server::TcpConnectInfo {
                        local_addr: None,
                        remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
                    });
                ClusterApiService::new(pool)
                    .publish_node_state(request)
                    .await?;
            }
            TestResult::Ok(())
        });
        wait_for_writers(2).await?;
        gate.commit().await?;

        let (deleted, published) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(delete, publish)
        })
        .await?;
        let (deleted, published) = (deleted?, published?);
        assert!(
            deleted.is_ok() && published.is_ok(),
            "delete: {deleted:?}; publication: {published:?}"
        );
        assert!(!sandbox_exists(&db.pool, sandbox_id).await?);
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
