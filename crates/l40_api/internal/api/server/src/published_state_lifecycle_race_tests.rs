//! The `failed` node-report projection, the delete of a stopped sandbox
//! reported `present: false`, and node-state publication against orphan
//! recovery, racing on the same control-plane Postgres `public.sandboxes` row.
//! Each case holds the other writer's lock, proves the peer waits on it
//! (`pg_blocking_pids`), commits the write, and asserts the persisted state.
//! The node-local sbx-mgr SQLite store isn't involved.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db_with_max_connections};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Transaction};

use super::{project_node_published_failed_sandboxes, publish_sandbox_states};
use crate::proto::PublishedSandboxState;

/// Bounds both the wait for the projection to block and its completion.
const RACE_BUDGET: Duration = Duration::from_secs(10);

/// The persisted Postgres `public.sandboxes.lifecycle` and `active_node_id`.
async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<(String, Option<NodeId>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// A `k8s` row placed on `node_id` in `lifecycle`.
async fn insert_k8s_sandbox(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_override_json,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'on_node', $3, $3, 'k8s', $4, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(node_id)
    .bind(Json(pplx_space_net_common::NetworkPolicyV1::default()))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Opens the conflicting writer's transaction and returns it with its backend
/// pid; the caller's statements on it hold their row locks until it ends.
async fn begin_holder(pool: &PgPool) -> TestResult<(Transaction<'static, Postgres>, i32)> {
    let mut holder = pool.begin().await?;
    let pid = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *holder)
        .await?;
    Ok((holder, pid))
}

/// Whether backend `waiter` is waiting on a lock `holder` holds.
async fn is_blocked_by(pool: &PgPool, waiter: i32, holder: i32) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar(sql!("SELECT $1 = ANY(pg_blocking_pids($2))"))
            .bind(holder)
            .bind(waiter)
            .fetch_one(pool)
            .await?,
    )
}

async fn wait_until_blocked(pool: &PgPool, waiter: i32, holder: i32) -> TestResult {
    tokio::time::timeout(RACE_BUDGET, async {
        while !is_blocked_by(pool, waiter, holder).await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        TestResult::Ok(())
    })
    .await
    .map_err(|_| format!("backend {waiter} never waited on backend {holder}"))?
}

/// The concurrent writer that commits while the `failed` projection waits.
#[derive(Clone, Copy, Debug)]
enum FailedRace {
    /// A user DELETE: lock the row, then persist `deleting` intent.
    Delete,
    /// A quarantine hold committed for the row.
    Quarantine,
    /// A suspend that finalized and unplaced the row.
    SuspendUnplaced,
    /// A pause: the row still qualifies for `failed`.
    Pause,
}

impl FailedRace {
    async fn write(
        self,
        holder: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> TestResult {
        if matches!(self, Self::Delete) {
            sqlx::query(sql!(
                "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
            ))
            .bind(sandbox_id)
            .execute(&mut **holder)
            .await?;
        }
        match self {
            Self::Delete => sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
            )),
            Self::Quarantine => sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'quarantined' WHERE id = $1"
            )),
            Self::SuspendUnplaced => sqlx::query(sql!(
                "UPDATE sandboxes
                    SET lifecycle = 'suspended', placement = 'unplaced', active_node_id = NULL
                  WHERE id = $1"
            )),
            Self::Pause => sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1"
            )),
        }
        .bind(sandbox_id)
        .execute(&mut **holder)
        .await?;
        Ok(())
    }
}

/// A `failed` report for a `started` row reaches the row while another writer
/// holds it. The projection decides on what that writer commits: a `deleting`,
/// a `quarantined` or a row unplaced as `suspended` stands, and a `paused` row
/// still becomes `failed`, reported as leaving `paused`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_projection_decides_on_the_row_committed_while_it_waits() -> TestResult {
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;

    let cases = [
        (FailedRace::Delete, "deleting", Some(node_id), None),
        (FailedRace::Quarantine, "quarantined", Some(node_id), None),
        (FailedRace::SuspendUnplaced, "suspended", None, None),
        (FailedRace::Pause, "failed", Some(node_id), Some("paused")),
    ];
    let mut mismatches = Vec::new();
    for (race, expected_lifecycle, expected_node, expected_from) in cases {
        let sandbox_id = insert_k8s_sandbox(pool, node_id, "started").await?;
        let (mut holder, holder_pid) = begin_holder(pool).await?;
        race.write(&mut holder, sandbox_id).await?;

        let mut projection_tx = pool.begin().await?;
        let waiter_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *projection_tx)
            .await?;
        let rows_json = serde_json::json!([
            { "sandbox_id": sandbox_id.to_string(), "published_lifecycle": "failed" }
        ]);
        let projection = tokio::spawn(async move {
            let moved =
                project_node_published_failed_sandboxes(&mut projection_tx, node_id, rows_json)
                    .await?;
            projection_tx.commit().await?;
            TestResult::<Vec<_>>::Ok(moved)
        });
        wait_until_blocked(pool, waiter_pid, holder_pid).await?;
        holder.commit().await?;
        let moved = tokio::time::timeout(RACE_BUDGET, projection)
            .await
            .map_err(|_| format!("{race:?}: projection did not finish"))???;

        let expected_moved = expected_from
            .map(|from| vec![(sandbox_id, from.to_owned())])
            .unwrap_or_default();
        let (lifecycle, active_node_id) = row(pool, sandbox_id).await?;
        let actual = (lifecycle.as_str(), active_node_id, moved);
        let expected = (expected_lifecycle, expected_node, expected_moved);
        if actual != expected {
            mismatches.push(format!(
                "{race:?}: (lifecycle, active node, reported moves) {actual:?}, expected {expected:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The concurrent writer that commits while the `present: false` delete waits.
#[derive(Clone, Copy, Debug)]
enum AbsentStoppedRace {
    /// A start claimed the row: it is no longer `stopped`.
    Start,
    /// The row was released from its node: the stop pin naming its
    /// already-durable disk is committed.
    Release,
    /// The release commits a new catalog row and its pin together, in the
    /// transaction the delete waits on.
    ReleaseWithCatalog,
    /// An unrelated write: the row still qualifies for the delete.
    Touch,
}

impl AbsentStoppedRace {
    async fn write(
        self,
        holder: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> TestResult {
        match self {
            Self::Start => sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'starting' WHERE id = $1"
            )),
            Self::Release => sqlx::query(sql!(
                "UPDATE sandboxes s
                    SET current_local_artifact_id = sn.local_artifact_id,
                        current_local_artifact_kind = sn.artifact_kind,
                        current_local_artifact_origin = sn.artifact_origin
                   FROM snapshots sn
                  WHERE s.id = $1
                    AND sn.sandbox_id = s.id"
            )),
            Self::ReleaseWithCatalog => sqlx::query(sql!(
                "WITH snapshot AS (
                     INSERT INTO snapshots (
                         id, sandbox_id, type, state, org_id,
                         local_artifact_id, artifact_kind, artifact_origin
                     )
                     VALUES (
                         gen_random_uuid(), $1, 'disk', 'ready',
                         '00000000-0000-0000-0000-000000000001',
                         gen_random_uuid(), 'disk', 'stop'
                     )
                     RETURNING id, local_artifact_id
                 ),
                 location AS (
                     INSERT INTO snapshot_storage_locations (
                         snapshot_id, storage_connection_string, org_id
                     )
                     SELECT id, 's3://space-test/' || id, '00000000-0000-0000-0000-000000000001'
                       FROM snapshot
                 )
                 UPDATE sandboxes s
                    SET current_local_artifact_id = snapshot.local_artifact_id,
                        current_local_artifact_kind = 'disk',
                        current_local_artifact_origin = 'stop'
                   FROM snapshot
                  WHERE s.id = $1"
            )),
            Self::Touch => sqlx::query(sql!(
                "UPDATE sandboxes SET updated_at = NOW() WHERE id = $1"
            )),
        }
        .bind(sandbox_id)
        .execute(&mut **holder)
        .await?;
        Ok(())
    }
}

/// A stopped Direct row with no pin, and one `ready` stop-origin disk snapshot
/// on a live storage location: the catalog a release commits before its pin.
async fn insert_stopped_space_sandbox_with_snapshot(
    pool: &PgPool,
    node_id: NodeId,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'stopped', 'on_node', $2, $2, 'space', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    let snapshot_id = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES (
             $1, $2, 'disk', 'ready', '00000000-0000-0000-0000-000000000001',
             $3, 'disk', 'stop'
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(uuid::Uuid::now_v7())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://space-test/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Whether the row exists, and how many of its snapshots keep a live location.
async fn absent_delete_outcome(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<(bool, i64)> {
    Ok(sqlx::query_as(sql!(
        "SELECT EXISTS (SELECT 1 FROM sandboxes WHERE id = $1),
                (SELECT COUNT(*)
                   FROM snapshot_storage_locations ssl
                   JOIN snapshots sn ON sn.id = ssl.snapshot_id
                  WHERE sn.sandbox_id = $1
                    AND ssl.orphaned_at IS NULL)"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// A `present: false` report for a stopped row reaches it while another writer
/// holds it. The delete decides on what that writer commits: a row a start
/// claimed survives with its storage location live, a row whose stop pin was
/// committed over an already-durable disk is spared the same way, and a row
/// that is still `stopped` and unpinned is deleted and its location detached.
/// A pin committed together with its catalog rows isn't spared, but the
/// delete fails rather than removing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_stopped_delete_decides_on_the_row_committed_while_it_waits() -> TestResult {
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;

    let cases = [
        (AbsentStoppedRace::Start, (true, true, 1)),
        (AbsentStoppedRace::Release, (true, true, 1)),
        // The delete can't see the new snapshot's location, so its cascade
        // into `snapshots` trips that location's `RESTRICT` foreign key and
        // the whole publication fails, keeping both catalog rows' locations.
        (AbsentStoppedRace::ReleaseWithCatalog, (false, true, 2)),
        (AbsentStoppedRace::Touch, (true, false, 0)),
    ];
    let mut mismatches = Vec::new();
    for (race, expected) in cases {
        let sandbox_id = insert_stopped_space_sandbox_with_snapshot(pool, node_id).await?;
        let (mut holder, holder_pid) = begin_holder(pool).await?;
        race.write(&mut holder, sandbox_id).await?;

        let mut publish_tx = pool.begin().await?;
        let waiter_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *publish_tx)
            .await?;
        let absent = PublishedSandboxState {
            sandbox_id: sandbox_id.to_string(),
            present: false,
            ..PublishedSandboxState::default()
        };
        // `Ok(None)` when the publication committed, `Ok(Some(message))` when
        // `publish_sandbox_states` refused it.
        let publish = tokio::spawn(async move {
            if let Err(status) = publish_sandbox_states(&mut publish_tx, node_id, &[absent]).await {
                return TestResult::Ok(Some(status.message().to_owned()));
            }
            publish_tx.commit().await?;
            TestResult::Ok(None)
        });
        wait_until_blocked(pool, waiter_pid, holder_pid).await?;
        holder.commit().await?;
        let refusal = tokio::time::timeout(RACE_BUDGET, publish)
            .await
            .map_err(|_| format!("{race:?}: publish did not finish"))???;
        // A refusal counts only if it is the foreign key the case names, so
        // an unrelated failure can't stand in for it.
        let published = match refusal {
            None => true,
            Some(message)
                if matches!(race, AbsentStoppedRace::ReleaseWithCatalog)
                    && message.contains("snapshot_storage_locations_snapshot_id_fkey") =>
            {
                false
            }
            Some(message) => {
                mismatches.push(format!("{race:?}: publication refused: {message}"));
                continue;
            }
        };

        let (exists, live_locations) = absent_delete_outcome(pool, sandbox_id).await?;
        let actual = (published, exists, live_locations);
        if actual != expected {
            mismatches.push(format!(
                "{race:?}: (published, row exists, live locations) {actual:?}, expected {expected:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// A `paused` Kubernetes row on `node_id` with an active running execution and
/// a newer ready full snapshot: eligible for `recover_orphan_to_suspended`
/// until `node_id` publishes an observed copy.
async fn insert_recoverable_orphan_k8s_sandbox(
    pool: &PgPool,
    node_id: NodeId,
) -> TestResult<(SandboxId, String)> {
    let sandbox_id = insert_k8s_sandbox(pool, node_id, "paused").await?;
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, state, active, created_at
         )
         VALUES ($1, 1, $2, 'sandboxes', 'running', TRUE, NOW() - INTERVAL '1 hour')"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(pool)
    .await?;
    let snapshot_id = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, org_id, type, state, timestamp
         )
         VALUES (
             $1, $2, '00000000-0000-0000-0000-000000000001',
             'full', 'ready', NOW() + INTERVAL '1 minute'
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://synthetic-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok((sandbox_id, pod_name))
}

fn published_present_paused(sandbox_id: SandboxId, state_degraded: bool) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some("paused".to_owned()),
        state_degraded,
        ..PublishedSandboxState::default()
    }
}

async fn has_observed_state(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM sandbox_observed_states WHERE sandbox_id = $1)"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// An uncommitted node publication (both full and `state_degraded` state-only)
/// blocks `recover_orphan_to_suspended` until the publication commits, and the
/// recovery then sees the committed `sandbox_observed_states` row and refuses
/// to revert. Conversely, an uncommitted recovery blocks a starting publication
/// so that once the revert clears `active_node_id`, the publication omits the
/// unplaced row instead of leaving a dangling observed-state record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncommitted_node_publish_and_orphan_revert_serialize() -> TestResult {
    use pplx_space_sandbox_store::orphan::{OrphanGuard, recover_orphan_to_suspended};

    const STOP_LIFECYCLES: &[&str] = &["deleted", "quarantined"];
    const LIVE_POD_LIFECYCLES: &[&str] = &["started", "paused", "resuming", "pausing", "stopping"];
    const PENDING_POD_LIFECYCLES: &[&str] = &["creating", "starting"];

    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;

    // Baseline: without a node publication, the seeded row recovers to `suspended`.
    {
        let (sandbox_id, pod_name) = insert_recoverable_orphan_k8s_sandbox(pool, node_id).await?;
        let mut tx = pool.begin().await?;
        let guard = OrphanGuard::PodName {
            name: &pod_name,
            uid: None,
            live_pod_required_lifecycles: LIVE_POD_LIFECYCLES,
            pending_pod_lifecycles: PENDING_POD_LIFECYCLES,
            create_age_threshold_secs: 600,
        };
        let recovered =
            recover_orphan_to_suspended(&mut *tx, sandbox_id, STOP_LIFECYCLES, &guard).await?;
        tx.commit().await?;
        assert!(
            recovered.is_some(),
            "fixture must be eligible without a publish"
        );
        assert_eq!(row(pool, sandbox_id).await?, ("suspended".to_owned(), None));
    }

    for state_degraded in [false, true] {
        // Direction 1: publish writes `sandbox_observed_states` first and hasn't
        // committed yet; `recover_orphan_to_suspended` waits, then sees the
        // committed observation and refuses to revert.
        let (sandbox_id, pod_name) = insert_recoverable_orphan_k8s_sandbox(pool, node_id).await?;
        let (mut publish_holder, holder_pid) = begin_holder(pool).await?;
        publish_sandbox_states(
            &mut publish_holder,
            node_id,
            &[published_present_paused(sandbox_id, state_degraded)],
        )
        .await
        .map_err(|status| format!("publish_sandbox_states: {status}"))?;

        let mut recover_tx = pool.begin().await?;
        let waiter_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *recover_tx)
            .await?;
        let recover = tokio::spawn(async move {
            let guard = OrphanGuard::PodName {
                name: &pod_name,
                uid: None,
                live_pod_required_lifecycles: LIVE_POD_LIFECYCLES,
                pending_pod_lifecycles: PENDING_POD_LIFECYCLES,
                create_age_threshold_secs: 600,
            };
            let outcome =
                recover_orphan_to_suspended(&mut *recover_tx, sandbox_id, STOP_LIFECYCLES, &guard)
                    .await?;
            recover_tx.commit().await?;
            TestResult::Ok(outcome.is_some())
        });

        wait_until_blocked(pool, waiter_pid, holder_pid).await?;
        publish_holder.commit().await?;
        let reverted = tokio::time::timeout(RACE_BUDGET, recover)
            .await
            .map_err(|_| format!("state_degraded={state_degraded}: recover did not finish"))???;
        assert!(
            !reverted,
            "state_degraded={state_degraded}: committed publish must defeat orphan revert"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            ("paused".to_owned(), Some(node_id))
        );
        assert!(has_observed_state(pool, sandbox_id).await?);

        // Direction 2: `recover_orphan_to_suspended` runs first and hasn't
        // committed yet; a concurrent publish waits, then sees `active_node_id`
        // cleared and omits the unplaced row.
        let (sandbox_id, pod_name) = insert_recoverable_orphan_k8s_sandbox(pool, node_id).await?;
        let (mut recover_holder, holder_pid) = begin_holder(pool).await?;
        let guard = OrphanGuard::PodName {
            name: &pod_name,
            uid: None,
            live_pod_required_lifecycles: LIVE_POD_LIFECYCLES,
            pending_pod_lifecycles: PENDING_POD_LIFECYCLES,
            create_age_threshold_secs: 600,
        };
        let outcome =
            recover_orphan_to_suspended(&mut *recover_holder, sandbox_id, STOP_LIFECYCLES, &guard)
                .await?;
        assert!(outcome.is_some());

        let mut publish_tx = pool.begin().await?;
        let waiter_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *publish_tx)
            .await?;
        let publish = tokio::spawn(async move {
            publish_sandbox_states(
                &mut publish_tx,
                node_id,
                &[published_present_paused(sandbox_id, state_degraded)],
            )
            .await
            .map_err(|status| format!("publish_sandbox_states: {status}"))?;
            publish_tx.commit().await?;
            TestResult::Ok(())
        });

        wait_until_blocked(pool, waiter_pid, holder_pid).await?;
        recover_holder.commit().await?;
        tokio::time::timeout(RACE_BUDGET, publish)
            .await
            .map_err(|_| format!("state_degraded={state_degraded}: publish did not finish"))???;
        assert_eq!(row(pool, sandbox_id).await?, ("suspended".to_owned(), None));
        assert!(
            !has_observed_state(pool, sandbox_id).await?,
            "state_degraded={state_degraded}: publish after revert must not write observed state"
        );
    }
    Ok(())
}
