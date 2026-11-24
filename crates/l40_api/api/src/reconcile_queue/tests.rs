use pplx_space_sandbox_model::SandboxId;

use super::*;

mod postgres_tests {

    use pplx_space_db_test_fixtures::{
        insert_pool_slot_receipt, pg_test_db, pool_slot_receipt_state,
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

    #[tokio::test]
    async fn port_enqueue_and_pending_backup_publication_do_not_deadlock() -> TestResult {
        race_port_reconciliation_with_pending_backup(PortOperation::Enqueue).await
    }

    #[tokio::test]
    async fn port_open_and_pending_backup_publication_do_not_deadlock() -> TestResult {
        race_port_reconciliation_with_pending_backup(PortOperation::Open).await
    }

    #[tokio::test]
    async fn port_close_and_pending_backup_publication_do_not_deadlock() -> TestResult {
        race_port_reconciliation_with_pending_backup(PortOperation::Close).await
    }

    #[derive(Clone, Copy, Debug)]
    enum PortOperation {
        Enqueue,
        Open,
        Close,
    }

    async fn race_port_reconciliation_with_pending_backup(operation: PortOperation) -> TestResult {
        use pplx_space_internal_api_server::{
            ClusterApiService,
            proto::{
                PendingLocalBackupReport, PublishNodeStateRequest, PublishedSandboxState,
                cluster_api_server::ClusterApi,
            },
        };
        use pplx_space_sbx_mgr_client::proto::{ArtifactKind, PendingLocalBackup};

        use crate::controllers::{
            sandbox::{SandboxController, fetch_sandbox},
            substrate::{Substrate, direct::DirectSubstrate},
        };

        async fn waiter(pool: &PgPool, blocker_pid: i32) -> TestResult<i32> {
            Ok(tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(pid) = sqlx::query_scalar(sql!(
                        "SELECT pid FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND $1 = ANY(pg_blocking_pids(pid))"
                    ))
                    .bind(blocker_pid)
                    .fetch_optional(pool)
                    .await?
                    {
                        return Ok::<_, sqlx::Error>(pid);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??)
        }

        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
        let pool = &db.pool;
        let node_id = NodeId::new();
        pplx_space_db_test_fixtures::insert_ready_worker_node(pool, node_id).await?;
        let sandbox_id = SandboxId::new();
        let arriving_sandbox_id = SandboxId::new();
        for id in [sandbox_id, arriving_sandbox_id] {
            insert_sandbox(pool, id, "started").await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET active_node_id = $2, placement = 'on_node' WHERE id = $1"
            ))
            .bind(id)
            .bind(node_id)
            .execute(pool)
            .await?;
        }
        let sandbox = fetch_sandbox(pool, sandbox_id)
            .await?
            .ok_or("missing sandbox fixture")?;
        // The port handlers only write database intent; the substrate is not called.
        let controller =
            SandboxController::new(Substrate::from_direct(DirectSubstrate::new(pool.clone())));

        let server = ClusterApiService::new(pool.clone());
        let census = |ids: &[SandboxId], pending_local_backups| {
            let mut request = tonic::Request::new(PublishNodeStateRequest {
                node_id: node_id.to_string(),
                sandbox_inventory_complete: true,
                sandboxes: ids
                    .iter()
                    .map(|id| PublishedSandboxState {
                        sandbox_id: id.to_string(),
                        present: true,
                        status: Some("started".to_owned()),
                        ..Default::default()
                    })
                    .collect(),
                pending_local_backups,
                ..Default::default()
            });
            request
                .extensions_mut()
                .insert(tonic::transport::server::TcpConnectInfo {
                    local_addr: None,
                    remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
                });
            request
        };
        ClusterApi::publish_node_state(&server, census(&[sandbox_id], None)).await?;

        // Pause the census after it has locked the trigger for a first-seen
        // sandbox, but before it mirrors the other sandbox's pending backup.
        sqlx::raw_sql(
            "CREATE FUNCTION pause_census_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_advisory_xact_lock(14702);
                 RETURN NEW;
             END $$;
             CREATE TRIGGER pause_census_trigger AFTER UPDATE ON node_reconcile_triggers
               FOR EACH ROW EXECUTE FUNCTION pause_census_trigger();",
        )
        .execute(pool)
        .await?;
        let mut gate = pool.begin().await?;
        let gate_pid = sqlx::query_scalar(sql!(
            "SELECT pg_backend_pid() FROM pg_advisory_xact_lock(14702)"
        ))
        .fetch_one(&mut *gate)
        .await?;
        let artifact = Uuid::now_v7();
        let publish = ClusterApi::publish_node_state(
            &server,
            census(
                &[sandbox_id, arriving_sandbox_id],
                Some(PendingLocalBackupReport {
                    pending: vec![PendingLocalBackup {
                        sandbox_id: sandbox_id.to_string(),
                        local_artifact_id: artifact.to_string(),
                        artifact_kind: ArtifactKind::Full as i32,
                    }],
                }),
            ),
        );
        let race = async {
            let publisher_pid = waiter(pool, gate_pid).await?;
            let enqueue = async {
                match operation {
                    PortOperation::Enqueue => enqueue_sandbox_reconcile_and_trigger(
                        pool,
                        sandbox_id,
                        &[ReconcileRef::sandbox_desired_state(sandbox_id)],
                        ReconcileReason::SandboxPortExposureUpdated,
                    )
                    .await
                    .map_err(|error| format!("{error:?}"))?
                    .map(|_| ())
                    .ok_or_else(|| "sandbox has no placement".to_owned()),
                    PortOperation::Open => controller
                        .open_sandbox_port(pool, &sandbox, 8080, None, None, "api")
                        .await
                        .map(|_| ())
                        .map_err(|error| format!("{error:?}")),
                    PortOperation::Close => controller
                        .close_sandbox_port(pool, &sandbox, 8080, None)
                        .await
                        .map(|_| ())
                        .map_err(|error| format!("{error:?}")),
                }
            };
            let resume = async {
                waiter(pool, publisher_pid).await?;
                gate.rollback().await?;
                TestResult::Ok(())
            };
            let (enqueue, resume) = tokio::join!(enqueue, resume);
            resume?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(enqueue)
        };
        let (publish, enqueue) = tokio::time::timeout(Duration::from_secs(12), async {
            tokio::join!(publish, race)
        })
        .await?;
        let enqueue = enqueue?;
        assert!(
            publish.is_ok() && enqueue.is_ok(),
            "publish={publish:?}, port {operation:?}={enqueue:?}"
        );
        let mirrored = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT artifact_id FROM node_local_backup_pending WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(mirrored, artifact);
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
