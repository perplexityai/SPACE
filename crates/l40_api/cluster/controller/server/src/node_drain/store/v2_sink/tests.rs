//! The v2 loop's statements against Postgres: the evacuation record, the
//! report, the `drained` edge and the cancel.

use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

use super::*;

const ORG: &str = "00000000-0000-0000-0000-000000000001";

async fn v2_node(
    pool: &PgPool,
    phase: Option<&str>,
    status: serde_json::Value,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode,
                            drain_started_at, drain_engine, drain_status)
         VALUES ($1, '10.0.0.70', 'worker', 'k8s', 'registered', $2, 'forced', NOW(), 'v2', $3)"
    ))
    .bind(node_id)
    .bind(phase)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn sandbox(pool: &PgPool, lifecycle: &str, node: Option<NodeId>) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, network_policy_generation
         ) VALUES ($1, '00000000-0000-0000-0000-000000000010', $2::UUID, $3,
                   CASE WHEN $4::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                   'k8s', $4, $4, 1)"
    ))
    .bind(id)
    .bind(ORG)
    .bind(lifecycle)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A catalog snapshot of `kind` for `artifact`, in `state`, stored.
async fn catalog(
    pool: &PgPool,
    id: SandboxId,
    artifact: Uuid,
    kind: &str,
    state: &str,
) -> TestResult<Uuid> {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, local_artifact_id,
                                artifact_kind, artifact_origin, org_id)
         VALUES ($1, $2, $3, $4, NOW(), $5, $3,
                 CASE WHEN $3 = 'disk' THEN 'stop' ELSE 'suspend' END, $6::UUID)"
    ))
    .bind(snapshot)
    .bind(id)
    .bind(kind)
    .bind(state)
    .bind(artifact)
    .bind(ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (id, snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, 's3://test-bucket/drain-v2?region=us-east-1', $3::UUID)"
    ))
    .bind(Uuid::now_v7())
    .bind(snapshot)
    .bind(ORG)
    .execute(pool)
    .await?;
    Ok(snapshot)
}

/// The drain restores of `id`, as `(target, snapshot)`.
async fn restores(pool: &PgPool, id: SandboxId) -> TestResult<Vec<(String, String)>> {
    let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
    Ok(sqlx::query_as(sql!(
        "SELECT payload_json->>'target', payload_json->>'snapshot_id'
           FROM public_operations
          WHERE kind = 'drain_restore_sandbox' AND target_id = $1
          ORDER BY created_at"
    ))
    .bind(target_id)
    .fetch_all(pool)
    .await?)
}

async fn pin(pool: &PgPool, id: SandboxId) -> TestResult<(Option<Uuid>, Option<String>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT current_local_artifact_id, current_local_artifact_kind
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// A suspended evacuation enqueues its restore from the catalog's ready
/// snapshot of the reported artifact, once: another tick, or a controller
/// that restarted and reads the same report, finds it.
#[tokio::test(flavor = "current_thread")]
async fn a_suspended_evacuation_enqueues_its_restore_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(pool, Some("draining"), serde_json::json!({"epoch": 5})).await?;
    let id = sandbox(pool, "suspended", None).await?;
    let artifact = Uuid::now_v7();
    let s = catalog(pool, id, artifact, "full", "ready").await?;
    for _ in 0..2 {
        assert_eq!(
            record_v2_evacuation(pool, node, id, EvacuationRestoreTarget::Paused, artifact).await?,
            EvacuationRecord::Recorded
        );
    }
    assert_eq!(
        restores(pool, id).await?,
        [("paused".to_owned(), s.to_string())]
    );
    Ok(())
}

/// A stopped evacuation pins the disk the node published and releases the
/// node, enqueuing nothing: the row stays `stopped`, unplaced, for its next
/// start. A row that pins another disk is not yet recorded, never settled,
/// and keeps its pin and its home.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_evacuation_pins_its_disk_and_releases_the_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(pool, Some("draining"), serde_json::json!({"epoch": 5})).await?;
    let name = format!("node-{node}");
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node)
    .bind(&name)
    .execute(pool)
    .await?;
    let stopped_at_home = |pool: PgPool| {
        let name = name.clone();
        async move {
            let id = sandbox(&pool, "stopped", Some(node)).await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour'
                  WHERE id = $1"
            ))
            .bind(id)
            .execute(&pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO sandbox_k8s_executions
                     (sandbox_id, generation, pod_name, pod_namespace, state, k8s_node_name,
                      created_at)
                 VALUES ($1, 1, $2, 'space', 'running', $3, NOW() - INTERVAL '2 hours')"
            ))
            .bind(id)
            .bind(format!("pod-{id}"))
            .bind(&name)
            .execute(&pool)
            .await?;
            TestResult::Ok(id)
        }
    };

    let id = stopped_at_home(pool.clone()).await?;
    let disk = Uuid::now_v7();
    catalog(pool, id, disk, "disk", "ready").await?;
    assert_eq!(
        record_v2_evacuation(pool, node, id, EvacuationRestoreTarget::Stopped, disk).await?,
        EvacuationRecord::Recorded
    );
    assert_eq!(pin(pool, id).await?, (Some(disk), Some("disk".to_owned())));
    let home: Option<NodeId> =
        sqlx::query_scalar(sql!("SELECT active_node_id FROM sandboxes WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?;
    assert_eq!(home, None);
    assert!(restores(pool, id).await?.is_empty());

    let other = stopped_at_home(pool.clone()).await?;
    let disk = Uuid::now_v7();
    catalog(pool, other, disk, "disk", "ready").await?;
    let newer = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes SET current_local_artifact_id = $2, current_local_artifact_kind = 'disk',
                              current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(other)
    .bind(newer)
    .execute(pool)
    .await?;
    assert!(matches!(
        record_v2_evacuation(pool, node, other, EvacuationRestoreTarget::Stopped, disk).await?,
        EvacuationRecord::NotYet(_)
    ));
    assert_eq!(
        pin(pool, other).await?,
        (Some(newer), Some("disk".to_owned()))
    );
    let home: Option<NodeId> =
        sqlx::query_scalar(sql!("SELECT active_node_id FROM sandboxes WHERE id = $1"))
            .bind(other)
            .fetch_one(pool)
            .await?;
    assert_eq!(home, Some(node));
    Ok(())
}

/// An evacuation waits while the catalog doesn't hold its artifact ready, or
/// while the node's suspend isn't projected onto a row still on the node, or
/// while a running row has no execution elsewhere; a sandbox a user woke on
/// another node, or deleted, needs no restore.
#[tokio::test(flavor = "current_thread")]
async fn an_evacuation_waits_for_its_catalog_and_projection_and_skips_what_moved_on() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(pool, Some("draining"), serde_json::json!({"epoch": 5})).await?;
    let elsewhere = NodeId::new();
    let artifact = Uuid::now_v7();

    let uploading = sandbox(pool, "suspended", None).await?;
    catalog(pool, uploading, artifact, "full", "creating").await?;
    let unprojected = sandbox(pool, "started", Some(node)).await?;
    catalog(pool, unprojected, artifact, "full", "ready").await?;
    let woken = sandbox(pool, "started", Some(elsewhere)).await?;
    catalog(pool, woken, artifact, "full", "ready").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, k8s_node_name)
         VALUES ($1, 1, $2, 'space', 'running', 'elsewhere')"
    ))
    .bind(woken)
    .bind(format!("pod-{woken}"))
    .execute(pool)
    .await?;
    let stale = sandbox(pool, "started", Some(elsewhere)).await?;
    catalog(pool, stale, artifact, "full", "ready").await?;
    let control = sandbox(pool, "suspended", None).await?;
    catalog(pool, control, artifact, "full", "ready").await?;
    for (case, id, expected) in [
        ("uploading", uploading, "not yet"),
        ("unprojected", unprojected, "not yet"),
        ("woken elsewhere", woken, "recorded"),
        ("placed elsewhere, running nowhere", stale, "not yet"),
        ("deleted", SandboxId::new(), "recorded"),
        ("control", control, "recorded"),
    ] {
        let record =
            record_v2_evacuation(pool, node, id, EvacuationRestoreTarget::Started, artifact)
                .await?;
        let read = match record {
            EvacuationRecord::Recorded => "recorded",
            EvacuationRecord::NotYet(_) => "not yet",
        };
        assert_eq!(read, expected, "{case}");
        assert_eq!(
            restores(pool, id).await?.len(),
            usize::from(case == "control"),
            "{case}"
        );
    }
    Ok(())
}

/// The report is stored only under the row's own epoch, and `drained` needs
/// a stored report that is `done` under that same epoch: a stale report
/// from an earlier seal doesn't advance a redrain.
#[tokio::test(flavor = "current_thread")]
async fn the_report_and_the_drained_edge_need_the_rows_own_epoch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(pool, Some("draining"), serde_json::json!({"epoch": 5})).await?;
    let report =
        |sealed: i64, done: bool| serde_json::json!({"sealed_epoch": sealed, "done": done});

    assert!(
        !record_v2_report(pool, node, 4, report(4, true)).await?,
        "another epoch's report"
    );
    assert!(record_v2_report(pool, node, 5, report(4, true)).await?);
    assert!(
        !advance_v2_drained(pool, node, 5).await?,
        "a stale seal's report"
    );
    assert!(record_v2_report(pool, node, 5, report(5, false)).await?);
    assert!(!advance_v2_drained(pool, node, 5).await?, "not done");
    assert!(record_v2_report(pool, node, 5, report(5, true)).await?);
    assert!(
        !advance_v2_drained(pool, node, 4).await?,
        "another epoch's edge"
    );
    assert!(advance_v2_drained(pool, node, 5).await?);
    let (phase, status): (Option<String>, serde_json::Value) = sqlx::query_as(sql!(
        "SELECT drain_phase, drain_status FROM nodes WHERE id = $1"
    ))
    .bind(node)
    .fetch_one(pool)
    .await?;
    assert_eq!(phase.as_deref(), Some("drained"));
    assert_eq!(status["epoch"], 5, "the epoch stays with the report");
    Ok(())
}

/// The cancel clears only an undrained row marked for it, under its epoch.
#[tokio::test(flavor = "current_thread")]
async fn the_cancel_clears_only_an_undrained_row_of_its_epoch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let marked = serde_json::json!({"epoch": 5, "cancel": true});
    let undrained = v2_node(pool, None, marked.clone()).await?;
    let still_draining = v2_node(pool, Some("draining"), marked).await?;
    let unmarked = v2_node(pool, None, serde_json::json!({"epoch": 5})).await?;
    assert!(!finish_v2_cancel(pool, undrained, 4).await?);
    assert!(!finish_v2_cancel(pool, still_draining, 5).await?);
    assert!(!finish_v2_cancel(pool, unmarked, 5).await?);
    assert!(finish_v2_cancel(pool, undrained, 5).await?);
    let listed: Vec<NodeId> = list_v2_nodes(pool)
        .await?
        .into_iter()
        .map(|node| node.id)
        .collect();
    assert!(
        !listed.contains(&undrained) && listed.contains(&still_draining),
        "{listed:?}"
    );
    Ok(())
}

/// The termination is flagged on a terminating node of the row's epoch, so
/// the loop doesn't terminate it again; any other row is left as it was.
#[tokio::test(flavor = "current_thread")]
async fn the_termination_is_flagged_on_its_epochs_terminating_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let status = serde_json::json!({"epoch": 7});
    for (case, phase, epoch, flagged) in [
        ("flagged", Some("terminating"), 7, true),
        ("another epoch", Some("terminating"), 8, false),
        ("still drained", Some("drained"), 7, false),
    ] {
        let node_id = v2_node(pool, phase, status.clone()).await?;
        assert_eq!(
            record_v2_terminated(pool, node_id, epoch).await?,
            flagged,
            "{case}"
        );
        let stored: serde_json::Value =
            sqlx::query_scalar(sql!("SELECT drain_status FROM nodes WHERE id = $1"))
                .bind(node_id)
                .fetch_one(pool)
                .await?;
        let listed = list_v2_nodes(pool)
            .await?
            .into_iter()
            .find(|node| node.id == node_id)
            .expect("listed");
        assert_eq!(listed.terminated, flagged, "{case}");
        if flagged {
            assert_eq!(stored, serde_json::json!({"epoch": 7, "terminated": true}));
        } else {
            assert_eq!(stored, status, "{case}: wrote");
        }
    }
    Ok(())
}
