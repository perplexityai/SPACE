//! The v2 loop's statements against Postgres: the evacuation record, the
//! report, the `drained` edge, the cancel, the terminate transaction and
//! the listing.

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
         VALUES ($1, '10.0.0.70', 'worker', 'k8s', 'registered', $2, NULL, NOW(), 'v2', $3)"
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

/// Move the row to `node`, active and target, or off every node.
async fn home(pool: &PgPool, id: SandboxId, node: Option<NodeId>) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, target_node_id = $2,
                placement = CASE WHEN $2::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END
          WHERE id = $1"
    ))
    .bind(id)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Pin `artifact` as the row's current full artifact, as a suspend does.
async fn pin_full(pool: &PgPool, id: SandboxId, artifact: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2, current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(artifact)
    .execute(pool)
    .await?;
    Ok(())
}

async fn read(
    pool: &PgPool,
    node: NodeId,
    id: SandboxId,
    artifact: Uuid,
) -> TestResult<&'static str> {
    Ok(
        match record_v2_evacuation(pool, node, id, artifact).await? {
            EvacuationRecord::Recorded => "recorded",
            EvacuationRecord::NotYet(_) => "not yet",
        },
    )
}

/// A full-snapshot evacuation writes nothing: it is recorded once the row,
/// pinning the reported artifact, reads `suspended` and the catalog holds
/// that artifact ready and stored, and waits before; the row stays
/// `suspended` for its next call. A row not yet projected, or a snapshot
/// not yet ready, is not yet recorded.
#[tokio::test(flavor = "current_thread")]
async fn a_full_evacuation_is_recorded_once_durable_and_writes_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 5, "deadline_at": 0}),
    )
    .await?;
    let artifact = Uuid::now_v7();
    let durable = sandbox(pool, "suspended", Some(node)).await?;
    pin_full(pool, durable, artifact).await?;
    catalog(pool, durable, artifact, "full", "ready").await?;
    let uploading = sandbox(pool, "suspended", Some(node)).await?;
    pin_full(pool, uploading, artifact).await?;
    catalog(pool, uploading, artifact, "full", "creating").await?;
    let unprojected = sandbox(pool, "paused", Some(node)).await?;
    catalog(pool, unprojected, artifact, "full", "ready").await?;
    // A ready, stored snapshot of type full whose artifact's kind isn't full:
    // nothing in the schema ties the two, and durable is the artifact's.
    let other_kind = sandbox(pool, "suspended", Some(node)).await?;
    pin_full(pool, other_kind, artifact).await?;
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, local_artifact_id,
                                artifact_kind, artifact_origin, org_id)
         VALUES ($1, $2, 'full', 'ready', NOW(), $3, 'disk', 'stop', $4::UUID)"
    ))
    .bind(snapshot)
    .bind(other_kind)
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
    for (case, id, expected) in [
        ("durable", durable, "recorded"),
        ("uploading", uploading, "not yet"),
        ("unprojected", unprojected, "not yet"),
        ("another artifact kind", other_kind, "not yet"),
    ] {
        let before: String = sqlx::query_scalar(sql!(
            "SELECT row_to_json(s)::TEXT FROM sandboxes s WHERE s.id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await?;
        for _ in 0..2 {
            assert_eq!(read(pool, node, id, artifact).await?, expected, "{case}");
        }
        let after: String = sqlx::query_scalar(sql!(
            "SELECT row_to_json(s)::TEXT FROM sandboxes s WHERE s.id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await?;
        assert_eq!(after, before, "{case}: nothing written");
    }
    Ok(())
}

/// A node keeps a suspended sandbox's cache after the sandbox was restored
/// elsewhere, so it can report an artifact the row no longer pins: a
/// sandbox suspended on A, restored on B and suspended again on B is
/// reported by A with A's old artifact, which the catalog may hold retired.
/// That evacuation is recorded, so A's drain can finish: the row is homed
/// on B, or, released from every node, pins B's artifact, durable. A's own
/// fresh suspend, whose artifact the row still pins, waits for it, and so
/// does a row that pins an artifact not yet durable on no other node.
#[tokio::test(flavor = "current_thread")]
async fn a_row_that_moved_on_from_the_reported_artifact_is_recorded() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let a = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 5, "deadline_at": 0}),
    )
    .await?;
    let b = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.71', 'worker', 'k8s', 'registered')"
    ))
    .bind(b)
    .execute(pool)
    .await?;
    let on_a = Uuid::now_v7();
    let on_b = Uuid::now_v7();

    let homed_on_b = sandbox(pool, "suspended", Some(b)).await?;
    catalog(pool, homed_on_b, on_a, "full", "failed").await?;
    pin_full(pool, homed_on_b, on_b).await?;
    assert_eq!(
        read(pool, a, homed_on_b, on_a).await?,
        "recorded",
        "homed on B"
    );

    let released = sandbox(pool, "suspended", None).await?;
    catalog(pool, released, on_a, "full", "failed").await?;
    pin_full(pool, released, on_b).await?;
    catalog(pool, released, on_b, "full", "ready").await?;
    assert_eq!(
        read(pool, a, released, on_a).await?,
        "recorded",
        "released, B's artifact durable"
    );

    let not_yet_durable = sandbox(pool, "suspended", None).await?;
    pin_full(pool, not_yet_durable, on_b).await?;
    catalog(pool, not_yet_durable, on_b, "full", "creating").await?;
    assert_eq!(
        read(pool, a, not_yet_durable, on_a).await?,
        "not yet",
        "another artifact, not yet durable, on no other node"
    );

    let fresh_on_a = sandbox(pool, "suspended", Some(a)).await?;
    pin_full(pool, fresh_on_a, on_a).await?;
    catalog(pool, fresh_on_a, on_a, "full", "creating").await?;
    assert_eq!(
        read(pool, a, fresh_on_a, on_a).await?,
        "not yet",
        "A's own suspend, uploading"
    );
    home(pool, fresh_on_a, Some(b)).await?;
    assert_eq!(
        read(pool, a, fresh_on_a, on_a).await?,
        "not yet",
        "a row still pinning A's artifact waits for it wherever it is homed"
    );
    Ok(())
}

/// The whole row as JSON text: equal before and after iff nothing wrote it.
async fn row_image(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT to_jsonb(s)::TEXT FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// A v2 drain doesn't move a stopped sandbox: a stopped sandbox's disk the
/// node reports evacuated is never recorded, even with the disk ready and
/// stored since the stop, and the row isn't written. Stored unrecorded, it
/// keeps the drain from `drained`. A full snapshot reported for a row that
/// reads stopped and is still homed on the node isn't recorded either, with
/// its own reason; homed on another node, it has moved on and is.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_disk_evacuation_is_never_recorded() -> TestResult {
    use pplx_space_node_registration_store::drain_v2::StoredV2Unrecorded;
    use pplx_space_sbx_mgr_client::proto::ArtifactKind;
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 5, "deadline_at": 0}),
    )
    .await?;
    let id = sandbox(pool, "stopped", Some(node)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    let disk = Uuid::now_v7();
    catalog(pool, id, disk, "disk", "ready").await?;
    let before = row_image(pool, id).await?;
    let entry = |kind: ArtifactKind| DrainEvacuated {
        sandbox_id: id.to_string(),
        artifact_kind: kind as i32,
        artifact_id: disk.to_string(),
    };
    assert_eq!(
        record_v2_evacuated(pool, node, &entry(ArtifactKind::Disk)).await?,
        EvacuationRecord::NotYet(DISK_NOT_RECORDED.to_owned())
    );
    assert_eq!(row_image(pool, id).await?, before, "the row isn't written");
    // A stopped row reported with a full snapshot stays on the node.
    assert_eq!(
        record_v2_evacuated(pool, node, &entry(ArtifactKind::Full)).await?,
        EvacuationRecord::NotYet(STOPPED_NOT_RECORDED.to_owned())
    );
    let elsewhere = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 6, "deadline_at": 0}),
    )
    .await?;
    home(pool, id, Some(elsewhere)).await?;
    assert_eq!(
        record_v2_evacuated(pool, node, &entry(ArtifactKind::Full)).await?,
        EvacuationRecord::Recorded,
        "homed on another node, it has moved on"
    );
    home(pool, id, Some(node)).await?;
    record_v2_evacuated(pool, node, &entry(ArtifactKind::Unspecified))
        .await
        .expect_err("an unknown artifact kind is refused");

    let unrecorded = StoredV2Report {
        reported_at: 1,
        evacuated_grew_at: 1,
        unrecorded: vec![StoredV2Unrecorded {
            sandbox_id: id.to_string(),
            reason: DISK_NOT_RECORDED.to_owned(),
            since: 1,
        }],
        ..StoredV2Report::default()
    };
    assert!(record_v2_report(pool, node, 5, &unrecorded, &[], &[]).await?);
    assert!(
        !advance_v2_drained(pool, node, 5).await?,
        "an unrecorded disk keeps the drain unfinished"
    );
    Ok(())
}

/// An evacuation waits while the catalog doesn't hold its artifact ready, or
/// while the node's suspend isn't projected onto a row still on the node, or
/// while a running row has no execution elsewhere; a sandbox a user woke on
/// another node, or deleted, has moved on.
#[tokio::test(flavor = "current_thread")]
async fn an_evacuation_waits_for_its_catalog_and_projection_and_skips_what_moved_on() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 5, "deadline_at": 0}),
    )
    .await?;
    let elsewhere = NodeId::new();
    let artifact = Uuid::now_v7();

    let uploading = sandbox(pool, "suspended", None).await?;
    pin_full(pool, uploading, artifact).await?;
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
    pin_full(pool, control, artifact).await?;
    catalog(pool, control, artifact, "full", "ready").await?;
    for (case, id, expected) in [
        ("uploading", uploading, "not yet"),
        ("unprojected", unprojected, "not yet"),
        ("woken elsewhere", woken, "recorded"),
        ("placed elsewhere, running nowhere", stale, "not yet"),
        ("deleted", SandboxId::new(), "recorded"),
        ("control", control, "recorded"),
    ] {
        assert_eq!(read(pool, node, id, artifact).await?, expected, "{case}");
    }
    Ok(())
}

/// The report is stored only under the row's own generation, and `drained`
/// needs a stored report under that same generation with nothing pending,
/// blocked or unrecorded.
#[tokio::test(flavor = "current_thread")]
async fn the_report_and_the_drained_edge_need_the_rows_own_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 5, "deadline_at": 0}),
    )
    .await?;
    let report = |pending: &[&str]| StoredV2Report {
        reported_at: 1,
        evacuated_grew_at: 1,
        pending: pending
            .iter()
            .map(
                |id| pplx_space_node_registration_store::drain_v2::StoredV2Pending {
                    sandbox_id: (*id).to_owned(),
                    wait: pplx_space_node_registration_store::drain_v2::StoredV2Wait::Moving,
                    running: false,
                    reason: String::new(),
                    since: None,
                    pool_member: false,
                },
            )
            .collect(),
        ..StoredV2Report::default()
    };

    assert!(
        !record_v2_report(pool, node, 4, &report(&[]), &[], &[]).await?,
        "another generation's report"
    );
    assert!(!advance_v2_drained(pool, node, 5).await?, "no report");
    assert!(record_v2_report(pool, node, 5, &report(&["p"]), &[], &[]).await?);
    assert!(!advance_v2_drained(pool, node, 5).await?, "a pending entry");
    assert!(record_v2_report(pool, node, 5, &report(&[]), &[], &[]).await?);
    assert!(
        !advance_v2_drained(pool, node, 4).await?,
        "another generation's edge"
    );
    assert!(advance_v2_drained(pool, node, 5).await?);
    let (phase, status): (Option<String>, serde_json::Value) = sqlx::query_as(sql!(
        "SELECT drain_phase, drain_status FROM nodes WHERE id = $1"
    ))
    .bind(node)
    .fetch_one(pool)
    .await?;
    assert_eq!(phase.as_deref(), Some("drained"));
    assert_eq!(
        status["generation"], 5,
        "the generation stays with the report"
    );
    Ok(())
}

/// The cancel's finish, its uncordon and its clear, runs only on an
/// undrained row of the cancel's generation (a v2 engine with no phase),
/// under its row lock: the uncordon is asked for with the Node's name and
/// recorded uid, before the clear, and one that fails clears nothing. A row
/// re-drained since the tick listed it, still draining, or of another
/// generation is neither uncordoned nor cleared.
#[tokio::test(flavor = "current_thread")]
async fn the_cancel_clears_and_uncordons_only_an_undrained_row_of_its_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let status = serde_json::json!({"generation": 5, "deadline_at": 0});
    let uncordoned = std::sync::Mutex::new(Vec::new());
    let uncordon = |name: String, uid: Option<String>| {
        uncordoned.lock().unwrap().push((name, uid));
        async { Ok::<_, String>(()) }
    };
    let cleared = |pool: PgPool, node: NodeId| async move {
        let engine: Option<String> =
            sqlx::query_scalar(sql!("SELECT drain_engine FROM nodes WHERE id = $1"))
                .bind(node)
                .fetch_one(&pool)
                .await?;
        TestResult::Ok(engine.is_none())
    };

    let undrained = v2_node(pool, None, status.clone()).await?;
    let name = identity(pool, undrained, Some(INSTANCE)).await?;
    let still_draining = v2_node(pool, Some("draining"), status.clone()).await?;
    // Re-drained after the tick listed it: a new generation, a phase.
    let redrained = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 6, "deadline_at": 0}),
    )
    .await?;
    for (case, node, generation) in [
        ("another generation", undrained, 4),
        ("still draining", still_draining, 5),
        ("re-drained", redrained, 5),
    ] {
        assert!(
            !finish_v2_cancel_locked(pool, node, generation, uncordon).await?,
            "{case}"
        );
        assert!(!cleared(pool.clone(), node).await?, "{case}");
    }
    assert!(uncordoned.lock().unwrap().is_empty());

    let failing = |_: String, _: Option<String>| async { Err::<(), _>("refused".to_owned()) };
    assert_eq!(
        finish_v2_cancel_locked(pool, undrained, 5, failing).await,
        Err("refused".to_owned())
    );
    assert!(
        !cleared(pool.clone(), undrained).await?,
        "a failed uncordon"
    );
    assert!(finish_v2_cancel_locked(pool, undrained, 5, uncordon).await?);
    assert!(cleared(pool.clone(), undrained).await?);
    assert_eq!(
        *uncordoned.lock().unwrap(),
        [(name, Some("uid".to_owned()))]
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

/// A drain under `generation` with a finished report, and anything else in
/// `extra`.
fn done(generation: i64, extra: serde_json::Value) -> serde_json::Value {
    let mut status = serde_json::json!({
        "generation": generation,
        "deadline_at": 0,
        "report": {"reported_at": 1, "evacuated_grew_at": 1},
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

/// The terminate transaction fences only its generation's unfenced
/// `terminating` node with a recorded instance, after its reference
/// cleanup: the durably suspended rows naming it released, their executions
/// retired. A row or execution that still names it holds the fence,
/// recorded in `drain_last_error`, while the cleanup it ran commits.
#[tokio::test(flavor = "current_thread")]
async fn the_terminate_transaction_fences_only_an_empty_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for case in [
        "fenced",
        "released then fenced",
        "another generation",
        "already fenced",
        "drained",
        "no instance",
        "held by a row",
        "held by a stopped row",
        "held by an execution",
    ] {
        let (phase, status) = match case {
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
        let generation = if case == "another generation" { 8 } else { 7 };
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
            "held by a stopped row" => {
                holder = Some(sandbox(pool, "stopped", Some(node_id)).await?);
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
        let fence = fence_v2_node(pool, node_id, generation).await?;
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
            "held by a row" | "held by a stopped row" | "held by an execution" => {
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

/// The dispatch is recorded once, on the fenced instance of its generation's
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
    assert!(node.dispatched_at.is_some_and(|at| at > 0));

    let unfenced = v2_node(pool, Some("terminating"), done(7, serde_json::json!({}))).await?;
    assert!(!record_v2_dispatched(pool, unfenced, 7, INSTANCE).await?);
    let node = listed(pool, unfenced).await?;
    assert_eq!((node.fence, node.dispatched_at), (None, None));
    Ok(())
}

/// The first time EC2 lists the drain's instance `terminated` is recorded
/// by the database clock, on any v2 row of its generation asking about that
/// instance: the fenced one once written, else the recorded one. A second
/// record keeps the first time, and any other row is left as it was.
#[tokio::test(flavor = "current_thread")]
async fn the_first_terminated_is_recorded_on_the_asked_instance() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    const OTHER: &str = "i-0000000000000e0e0";
    for (case, phase, extra, generation, instance, records) in [
        (
            "fenced",
            Some("terminating"),
            serde_json::json!({"terminate_instance": INSTANCE}),
            7,
            INSTANCE,
            true,
        ),
        (
            "fenced, asked about another",
            Some("terminating"),
            serde_json::json!({"terminate_instance": OTHER}),
            7,
            INSTANCE,
            false,
        ),
        (
            "another instance",
            Some("terminating"),
            serde_json::json!({"terminate_instance": INSTANCE}),
            7,
            OTHER,
            false,
        ),
        (
            "another generation",
            Some("terminating"),
            serde_json::json!({"terminate_instance": INSTANCE}),
            8,
            INSTANCE,
            false,
        ),
        (
            "unfenced terminating",
            Some("terminating"),
            serde_json::json!({}),
            7,
            INSTANCE,
            true,
        ),
        (
            "draining",
            Some("draining"),
            serde_json::json!({}),
            7,
            INSTANCE,
            true,
        ),
        (
            "drained",
            Some("drained"),
            serde_json::json!({}),
            7,
            INSTANCE,
            true,
        ),
        (
            "a cancel pending",
            None,
            serde_json::json!({}),
            7,
            INSTANCE,
            true,
        ),
    ] {
        let status = done(7, extra);
        let node_id = v2_node(pool, phase, status.clone()).await?;
        identity(pool, node_id, Some(INSTANCE)).await?;
        assert_eq!(
            record_v2_terminated_observed(pool, node_id, generation, instance).await?,
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
        assert!(record_v2_terminated_observed(pool, node_id, generation, instance).await?);
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
            "deleted" => {
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

/// The let-go clears the phase and the v2 columns of the node in the generation,
/// phase and fence the tick read, records the trace, and writes no sandbox
/// row; a row that moved on is left as it was.
#[tokio::test(flavor = "current_thread")]
async fn the_let_go_clears_only_the_row_the_tick_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    const OTHER: &str = "i-0000000000000e0e0";
    for (case, phase, fence, generation, read_phase, read_fence, clears) in [
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
            "another generation",
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
        (
            "draining",
            Some("draining"),
            None,
            7,
            Some("draining"),
            None,
            true,
        ),
        (
            "draining, another generation",
            Some("draining"),
            None,
            8,
            Some("draining"),
            None,
            false,
        ),
    ] {
        let mut status = done(7, serde_json::json!({}));
        if let Some(fence) = fence {
            status["terminate_instance"] = serde_json::json!(fence);
        }
        let node_id = v2_node(pool, phase, status.clone()).await?;
        identity(pool, node_id, Some(INSTANCE)).await?;
        let pinned = sandbox(pool, "started", Some(node_id)).await?;
        let cleared = let_go_v2(
            pool,
            node_id,
            generation,
            read_phase,
            read_fence,
            "the trace",
        )
        .await?;
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

/// The listing reads whether each drain evacuates, from its deadline by
/// the database clock, and the database clock itself; and it carries the
/// stored report, typed: the unrecorded clocks and the sandboxes it lists
/// evacuated with the stall clock. A report that doesn't read is none.
#[tokio::test(flavor = "current_thread")]
async fn the_listing_reads_the_deadline_by_the_database_clock_and_the_report() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let db_now: i64 = sqlx::query_scalar(sql!("SELECT FLOOR(EXTRACT(EPOCH FROM NOW()))::BIGINT"))
        .fetch_one(pool)
        .await?;
    for (case, deadline_at, evacuate) in [
        ("ahead", Some(db_now + 600), false),
        ("passed", Some(db_now - 60), true),
        ("none", None, true),
    ] {
        let mut status = serde_json::json!({"generation": 5});
        if let Some(deadline_at) = deadline_at {
            status["deadline_at"] = serde_json::json!(deadline_at);
        }
        let node = v2_node(pool, Some("draining"), status).await?;
        let listed_node = listed(pool, node).await?;
        assert_eq!(listed_node.deadline_at, deadline_at, "{case}");
        assert_eq!(listed_node.evacuate, evacuate, "{case}");
        assert_eq!(listed_node.generation, Some(5), "{case}");
        assert!(
            (db_now..=db_now + 5).contains(&listed_node.db_now),
            "{case}"
        );
    }
    let report = serde_json::json!({
        "reported_at": 350,
        "evacuated_grew_at": 300,
        "evacuated": [{"sandbox_id": "e", "artifact_kind": "full", "artifact_id": "x"}],
        "pending": [{"sandbox_id": "p", "wait": "moving", "running": false, "since": 200}],
        "unrecorded": [{"sandbox_id": "a", "reason": "not projected", "since": 100}],
    });
    let node = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 5, "deadline_at": 0, "report": report}),
    )
    .await?;
    let stored = listed(pool, node).await?.report.ok_or("a report")?;
    assert_eq!(stored.evacuated_grew_at, 300);
    assert_eq!(stored.unrecorded[0].since, 100);
    assert_eq!(stored.evacuated[0].sandbox_id, "e");
    let unreadable = v2_node(
        pool,
        Some("draining"),
        serde_json::json!({"generation": 5, "deadline_at": 0, "report": {"pending": 3}}),
    )
    .await?;
    assert_eq!(listed(pool, unreadable).await?.report, None);
    Ok(())
}
