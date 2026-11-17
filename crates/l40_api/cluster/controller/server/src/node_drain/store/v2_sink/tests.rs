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

const INSTANCE: &str = "i-0000000000000d0d0";

/// The node's Kubernetes identity: name, uid `uid`, and [`INSTANCE`] unless
/// `instance` says otherwise.
async fn identity(pool: &PgPool, node_id: NodeId, instance: Option<&str>) -> TestResult<String> {
    let name = format!("worker-{}.synthetic.internal", node_id.as_uuid().simple());
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
         VALUES ($1, $2, 'uid', $3)"
    ))
    .bind(node_id)
    .bind(&name)
    .bind(instance)
    .execute(pool)
    .await?;
    Ok(name)
}

async fn heartbeat(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots
            (node_id, observed_at, stale_after, reachable, services, updated_at)
         VALUES ($1, NOW(), NOW() + INTERVAL '1 minute', TRUE, '[]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A done report for `epoch`, and anything else in `extra`.
fn done(epoch: i64, extra: serde_json::Value) -> serde_json::Value {
    let mut status = serde_json::json!({
        "epoch": epoch,
        "report": {"sealed_epoch": epoch, "done": true},
    });
    if let (Some(status), Some(extra)) = (status.as_object_mut(), extra.as_object()) {
        status.extend(extra.clone());
    }
    status
}

/// A durable full snapshot of the row, taken now.
async fn durable(pool: &PgPool, id: SandboxId) -> TestResult {
    catalog(pool, id, Uuid::now_v7(), "full", "ready").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn columns(pool: &PgPool, node_id: NodeId) -> TestResult<NodeColumns> {
    Ok(sqlx::query_as(sql!(
        "SELECT drain_phase, drain_engine, drain_status, drain_last_error
           FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

/// `(drain_phase, drain_engine, drain_status, drain_last_error)`.
type NodeColumns = (
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
    Option<String>,
);

async fn listed(pool: &PgPool, node_id: NodeId) -> TestResult<V2Node> {
    Ok(list_v2_nodes(pool)
        .await?
        .into_iter()
        .find(|node| node.id == node_id)
        .ok_or("listed")?)
}

/// The release runs only on a `drained` node of its epoch whose report is
/// `done`, and releases a durably suspended row naming it.
#[tokio::test(flavor = "current_thread")]
async fn the_release_runs_only_on_its_epochs_drained_done_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for (case, phase, status, epoch, releases) in [
        (
            "drained",
            "drained",
            done(7, serde_json::json!({})),
            7,
            true,
        ),
        (
            "another epoch",
            "drained",
            done(7, serde_json::json!({})),
            8,
            false,
        ),
        (
            "not done",
            "drained",
            serde_json::json!({"epoch": 7, "report": {"sealed_epoch": 7, "done": false}}),
            7,
            false,
        ),
        (
            "draining",
            "draining",
            done(7, serde_json::json!({})),
            7,
            false,
        ),
    ] {
        let node_id = v2_node(pool, Some(phase), status).await?;
        identity(pool, node_id, Some(INSTANCE)).await?;
        let id = sandbox(pool, "suspended", Some(node_id)).await?;
        durable(pool, id).await?;
        let released = release_v2_suspended(pool, node_id, epoch).await?;
        assert_eq!(released == [id], releases, "{case}: {released:?}");
    }
    Ok(())
}

/// The terminate transaction fences only its epoch's unfenced
/// `terminating` node with a `done` report and a recorded instance, after
/// releasing the durably suspended rows naming it; a row or execution that
/// still names it holds the fence, recorded in `drain_last_error`, while the
/// release it ran commits.
#[tokio::test(flavor = "current_thread")]
async fn the_terminate_transaction_fences_only_an_empty_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for case in [
        "fenced",
        "released then fenced",
        "another epoch",
        "not done",
        "already fenced",
        "drained",
        "no instance",
        "held by a row",
        "held by an execution",
    ] {
        let (phase, status) = match case {
            "not done" => (
                "terminating",
                serde_json::json!({"epoch": 7, "report": {"sealed_epoch": 7, "done": false}}),
            ),
            "already fenced" => (
                "terminating",
                done(
                    7,
                    serde_json::json!({"terminate_instance": "i-0000000000000e0e0"}),
                ),
            ),
            "drained" => ("drained", done(7, serde_json::json!({}))),
            _ => ("terminating", done(7, serde_json::json!({}))),
        };
        let node_id = v2_node(pool, Some(phase), status.clone()).await?;
        let name = identity(pool, node_id, (case != "no instance").then_some(INSTANCE)).await?;
        let epoch = if case == "another epoch" { 8 } else { 7 };
        let mut releasable = None;
        let mut holder = None;
        match case {
            "released then fenced" | "held by a row" => {
                let id = sandbox(pool, "suspended", Some(node_id)).await?;
                durable(pool, id).await?;
                releasable = Some(id);
                if case == "held by a row" {
                    holder = Some(sandbox(pool, "suspended", Some(node_id)).await?);
                }
            }
            "held by an execution" => {
                let id = sandbox(pool, "started", None).await?;
                sqlx::query(sql!(
                    "INSERT INTO sandbox_k8s_executions
                        (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state,
                         active)
                     VALUES ($1, 1, 'space-sbx-synthetic', 'sandboxes', $2, 'running', TRUE)"
                ))
                .bind(id)
                .bind(&name)
                .execute(pool)
                .await?;
                holder = Some(id);
            }
            _ => {}
        }
        let fence = fence_v2_node(pool, node_id, epoch).await?;
        let (_, _, stored, last_error) = columns(pool, node_id).await?;
        let fenced = stored
            .as_ref()
            .and_then(|status| status.get("terminate_instance"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        match case {
            "fenced" | "released then fenced" => {
                assert_eq!(fence, Fence::Fenced(INSTANCE.to_owned()), "{case}");
                assert_eq!(fenced.as_deref(), Some(INSTANCE), "{case}");
                assert_eq!(
                    listed(pool, node_id).await?.fence.as_deref(),
                    Some(INSTANCE)
                );
            }
            "no instance" => {
                assert_eq!(fence, Fence::NoInstance, "{case}");
                assert_eq!(stored.as_ref(), Some(&status), "{case}");
            }
            "held by a row" | "held by an execution" => {
                let Fence::Held(held) = fence else {
                    panic!("{case}: {fence:?}");
                };
                let holder = holder.ok_or("holder")?;
                assert!(held.contains(&holder.to_string()), "{case}: {held}");
                assert_eq!(last_error.as_deref(), Some(held.as_str()), "{case}");
                assert_eq!(fenced, None, "{case}");
            }
            _ => {
                assert_eq!(fence, Fence::Moved, "{case}");
                assert_eq!(stored.as_ref(), Some(&status), "{case}");
            }
        }
        if let Some(id) = releasable {
            let (active, target): (Option<NodeId>, Option<NodeId>) = sqlx::query_as(sql!(
                "SELECT active_node_id, target_node_id FROM sandboxes WHERE id = $1"
            ))
            .bind(id)
            .fetch_one(pool)
            .await?;
            assert_eq!((active, target), (None, None), "{case}: released");
        }
    }
    Ok(())
}

/// The dispatch is recorded once, on the fenced instance of its epoch's
/// `terminating` node, by the database clock; listing reads it back.
#[tokio::test(flavor = "current_thread")]
async fn the_dispatch_is_recorded_once_on_the_fenced_instance() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = v2_node(
        pool,
        Some("terminating"),
        done(7, serde_json::json!({"terminate_instance": INSTANCE})),
    )
    .await?;
    identity(pool, node_id, Some(INSTANCE)).await?;
    assert!(!record_v2_dispatched(pool, node_id, 7, "i-0000000000000e0e0").await?);
    assert!(!record_v2_dispatched(pool, node_id, 8, INSTANCE).await?);
    assert!(record_v2_dispatched(pool, node_id, 7, INSTANCE).await?);
    assert!(!record_v2_dispatched(pool, node_id, 7, INSTANCE).await?);
    let node = listed(pool, node_id).await?;
    assert_eq!(node.fence.as_deref(), Some(INSTANCE));
    assert!(node.dispatched);
    assert!(node.dispatched_at.is_some_and(|at| at > 0));

    let unfenced = v2_node(pool, Some("terminating"), done(7, serde_json::json!({}))).await?;
    assert!(!record_v2_dispatched(pool, unfenced, 7, INSTANCE).await?);
    let node = listed(pool, unfenced).await?;
    assert_eq!((node.fence, node.dispatched), (None, false));
    Ok(())
}

/// A row an earlier build's terminator acted on carries
/// `drain_status.terminated` and no fence: it is listed fenced on its
/// recorded instance and dispatched, with no dispatch time, and the
/// terminate transaction doesn't fence it again. A `false` marker is none.
#[tokio::test(flavor = "current_thread")]
async fn an_earlier_builds_terminated_marker_reads_as_a_dispatched_fence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for (case, marker, fenced) in [("marked", true, true), ("false", false, false)] {
        let node_id = v2_node(
            pool,
            Some("terminating"),
            done(7, serde_json::json!({"terminated": marker})),
        )
        .await?;
        identity(pool, node_id, Some(INSTANCE)).await?;
        let node = listed(pool, node_id).await?;
        assert_eq!(
            (node.fence.as_deref(), node.dispatched, node.dispatched_at),
            (fenced.then_some(INSTANCE), fenced, None),
            "{case}"
        );
        let fence = fence_v2_node(pool, node_id, 7).await?;
        if fenced {
            assert_eq!(fence, Fence::Moved, "{case}");
        } else {
            assert_eq!(fence, Fence::Fenced(INSTANCE.to_owned()), "{case}");
        }
    }
    Ok(())
}

/// The first time EC2 lists the fenced instance `terminated` is recorded by
/// the database clock, on its epoch's `terminating` node fenced on that
/// instance, an earlier build's marker included; a second record keeps the
/// first time, and any other row is left as it was.
#[tokio::test(flavor = "current_thread")]
async fn the_first_terminated_is_recorded_on_the_fenced_instance() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for (case, phase, extra, epoch, instance, records) in [
        (
            "fenced",
            "terminating",
            serde_json::json!({"terminate_instance": INSTANCE}),
            7,
            INSTANCE,
            true,
        ),
        (
            "an earlier build's marker",
            "terminating",
            serde_json::json!({"terminated": true}),
            7,
            INSTANCE,
            true,
        ),
        (
            "another instance",
            "terminating",
            serde_json::json!({"terminate_instance": INSTANCE}),
            7,
            "i-0000000000000e0e0",
            false,
        ),
        (
            "another epoch",
            "terminating",
            serde_json::json!({"terminate_instance": INSTANCE}),
            8,
            INSTANCE,
            false,
        ),
        (
            "unfenced",
            "terminating",
            serde_json::json!({}),
            7,
            INSTANCE,
            false,
        ),
        (
            "drained",
            "drained",
            serde_json::json!({"terminate_instance": INSTANCE}),
            7,
            INSTANCE,
            false,
        ),
    ] {
        let status = done(7, extra);
        let node_id = v2_node(pool, Some(phase), status.clone()).await?;
        identity(pool, node_id, Some(INSTANCE)).await?;
        assert_eq!(
            record_v2_terminated_observed(pool, node_id, epoch, instance).await?,
            records,
            "{case}"
        );
        let stored = columns(pool, node_id).await?.2.ok_or("a status")?;
        if !records {
            assert_eq!(stored, status, "{case}");
            continue;
        }
        let first = stored["terminated_observed_at"]
            .as_i64()
            .ok_or("an observation time")?;
        assert!(first > 0, "{case}");
        sqlx::query(sql!(
            "UPDATE nodes
                SET drain_status = drain_status || '{\"terminated_observed_at\": 1}'::JSONB
              WHERE id = $1"
        ))
        .bind(node_id)
        .execute(pool)
        .await?;
        assert!(record_v2_terminated_observed(pool, node_id, epoch, instance).await?);
        assert_eq!(
            listed(pool, node_id).await?.terminated_observed_at,
            Some(1),
            "{case}"
        );
    }
    Ok(())
}

/// The delete of a terminated node's row needs the row to still carry the
/// fence with a dispatch, the identity the tick read, a stale heartbeat and
/// nothing naming the node; it leaves no tombstone. A row a registration
/// took has no fence left; a fresh heartbeat or a moved identity waits; a
/// row that names the node lets it go instead, with the trace, writing no
/// sandbox row.
#[tokio::test(flavor = "current_thread")]
async fn the_terminated_delete_holds_its_guards_and_lets_go_on_a_reference() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let dispatched = || {
        done(
            7,
            serde_json::json!({
                "terminate_instance": INSTANCE,
                "dispatched_at": 1,
                "terminated_observed_at": 2,
            }),
        )
    };
    for case in [
        "deleted",
        "an earlier build's marker",
        "unfenced",
        "not dispatched",
        "terminated not recorded",
        "heartbeat fresh",
        "uid moved",
        "instance moved",
        "referenced",
    ] {
        let status = match case {
            "unfenced" => done(7, serde_json::json!({})),
            "not dispatched" => done(
                7,
                serde_json::json!({"terminate_instance": INSTANCE, "terminated_observed_at": 2}),
            ),
            "terminated not recorded" => done(
                7,
                serde_json::json!({"terminate_instance": INSTANCE, "dispatched_at": 1}),
            ),
            "an earlier build's marker" => done(
                7,
                serde_json::json!({"terminated": true, "terminated_observed_at": 2}),
            ),
            _ => dispatched(),
        };
        let node_id = v2_node(pool, Some("terminating"), status.clone()).await?;
        let recorded = if case == "instance moved" {
            "i-0000000000000e0e0"
        } else {
            INSTANCE
        };
        identity(pool, node_id, Some(recorded)).await?;
        if case == "heartbeat fresh" {
            heartbeat(pool, node_id).await?;
        }
        let holder = if case == "referenced" {
            Some(sandbox(pool, "suspended", Some(node_id)).await?)
        } else {
            None
        };
        let mut node = listed(pool, node_id).await?;
        if case == "uid moved" {
            node.k8s_node_uid = Some("uid-before".to_owned());
        }
        let outcome = delete_v2_terminated(pool, &node, 7, INSTANCE).await?;
        let exists: bool =
            sqlx::query_scalar(sql!("SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $1)"))
                .bind(node_id)
                .fetch_one(pool)
                .await?;
        match case {
            "deleted" | "an earlier build's marker" => {
                assert_eq!(outcome, TerminatedDelete::Deleted, "{case}");
                assert!(!exists, "{case}");
                let tombstoned: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1)"
                ))
                .bind(node_id)
                .fetch_one(pool)
                .await?;
                assert!(!tombstoned);
            }
            "unfenced" => {
                assert_eq!(outcome, TerminatedDelete::Unfenced);
                assert!(exists);
            }
            "referenced" => {
                let holder = holder.ok_or("holder")?;
                let trace = format!(
                    "v2 drain let go after terminating {INSTANCE}: target_node_id held by {holder}; \
                     active_node_id held by {holder}"
                );
                let TerminatedDelete::LetGo(got) = &outcome else {
                    panic!("{case}: {outcome:?}");
                };
                assert!(
                    got.starts_with(&format!("v2 drain let go after terminating {INSTANCE}: "))
                        && got.contains(&format!("held by {holder}")),
                    "{got} vs {trace}"
                );
                assert_eq!(
                    columns(pool, node_id).await?,
                    (None, None, None, Some(got.clone()))
                );
                let (active, target): (Option<NodeId>, Option<NodeId>) = sqlx::query_as(sql!(
                    "SELECT active_node_id, target_node_id FROM sandboxes WHERE id = $1"
                ))
                .bind(holder)
                .fetch_one(pool)
                .await?;
                assert_eq!((active, target), (Some(node_id), Some(node_id)));
            }
            _ => {
                assert!(
                    matches!(outcome, TerminatedDelete::Wait(_)),
                    "{case}: {outcome:?}"
                );
                assert!(exists, "{case}");
                assert_eq!(columns(pool, node_id).await?.2, Some(status), "{case}");
            }
        }
    }
    Ok(())
}

/// The let-go clears the phase and the v2 columns of the node in the epoch,
/// phase and fence the tick read, records the trace, and writes no sandbox
/// row; a row that moved on is left as it was.
#[tokio::test(flavor = "current_thread")]
async fn the_let_go_clears_only_the_row_the_tick_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    const OTHER: &str = "i-0000000000000e0e0";
    for (case, phase, fence, epoch, read_phase, read_fence, clears) in [
        (
            "drained",
            Some("drained"),
            None,
            7,
            Some("drained"),
            None,
            true,
        ),
        ("cancel pending", None, None, 7, None, None, true),
        (
            "fenced",
            Some("terminating"),
            Some(INSTANCE),
            7,
            Some("terminating"),
            Some(INSTANCE),
            true,
        ),
        (
            "an earlier build's marker",
            Some("terminating"),
            Some("terminated"),
            7,
            Some("terminating"),
            Some(INSTANCE),
            true,
        ),
        (
            "another epoch",
            Some("drained"),
            None,
            8,
            Some("drained"),
            None,
            false,
        ),
        (
            "phase moved",
            Some("terminating"),
            None,
            7,
            Some("drained"),
            None,
            false,
        ),
        (
            "another fence",
            Some("terminating"),
            Some(OTHER),
            7,
            Some("terminating"),
            Some(INSTANCE),
            false,
        ),
        (
            "fenced since the tick read it",
            Some("terminating"),
            Some(INSTANCE),
            7,
            Some("terminating"),
            None,
            false,
        ),
    ] {
        let mut status = done(7, serde_json::json!({}));
        match fence {
            Some("terminated") => status["terminated"] = serde_json::json!(true),
            Some(fence) => status["terminate_instance"] = serde_json::json!(fence),
            None => {}
        }
        if phase.is_none() {
            status["cancel"] = serde_json::json!(true);
        }
        let node_id = v2_node(pool, phase, status.clone()).await?;
        identity(pool, node_id, Some(INSTANCE)).await?;
        let pinned = sandbox(pool, "started", Some(node_id)).await?;
        let cleared = let_go_v2(pool, node_id, epoch, read_phase, read_fence, "the trace").await?;
        assert_eq!(cleared, clears, "{case}");
        let (stored_phase, engine, stored, last_error) = columns(pool, node_id).await?;
        if clears {
            assert_eq!(
                (stored_phase, engine, stored, last_error),
                (None, None, None, Some("the trace".to_owned())),
                "{case}"
            );
            let mode: Option<String> =
                sqlx::query_scalar(sql!("SELECT drain_mode FROM nodes WHERE id = $1"))
                    .bind(node_id)
                    .fetch_one(pool)
                    .await?;
            assert_eq!(mode, None, "{case}");
        } else {
            assert_eq!(stored, Some(status), "{case}");
        }
        let lifecycle: (String, Option<NodeId>) = sqlx::query_as(sql!(
            "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
        ))
        .bind(pinned)
        .fetch_one(pool)
        .await?;
        assert_eq!(lifecycle, ("started".to_owned(), Some(node_id)), "{case}");
    }
    Ok(())
}

/// A drain the admin route left waiting is listed waiting, and admitted
/// once, under its own epoch and only while it is `draining`: the row then
/// reads `waiting: false`. A row without the key isn't waiting.
#[tokio::test(flavor = "current_thread")]
async fn a_waiting_drain_is_admitted_once_under_its_epoch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let waiting = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"epoch": 5, "waiting": true}),
    )
    .await?;
    let sealed = v2_node(pool, Some("draining"), serde_json::json!({"epoch": 6})).await?;
    let drained = v2_node(
        pool,
        Some("drained"),
        serde_json::json!({"epoch": 7, "waiting": true}),
    )
    .await?;
    let listed = |nodes: &[V2Node], id: NodeId| {
        nodes
            .iter()
            .find(|node| node.id == id)
            .map(|node| node.waiting)
    };
    let nodes = list_v2_nodes(pool).await?;
    assert_eq!(listed(&nodes, waiting), Some(true));
    assert_eq!(listed(&nodes, sealed), Some(false));
    assert_eq!(listed(&nodes, drained), Some(true));

    assert!(
        !admit_v2_evacuation(pool, waiting, 4).await?,
        "another epoch"
    );
    assert!(!admit_v2_evacuation(pool, sealed, 6).await?, "not waiting");
    assert!(
        !admit_v2_evacuation(pool, drained, 7).await?,
        "not draining"
    );
    assert!(admit_v2_evacuation(pool, waiting, 5).await?);
    assert!(!admit_v2_evacuation(pool, waiting, 5).await?, "once");

    let nodes = list_v2_nodes(pool).await?;
    assert_eq!(listed(&nodes, waiting), Some(false));
    let status: serde_json::Value =
        sqlx::query_scalar(sql!("SELECT drain_status FROM nodes WHERE id = $1"))
            .bind(waiting)
            .fetch_one(pool)
            .await?;
    assert_eq!(status, serde_json::json!({"epoch": 5, "waiting": false}));
    Ok(())
}
