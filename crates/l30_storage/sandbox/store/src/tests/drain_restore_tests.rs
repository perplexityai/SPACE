//! The v2 drain's stopped handoff ([`crate::drain_restore`]).
//!
//! A refused or moved-on handoff must leave the row exactly as it was: each
//! one compares the whole row, and its executions, before and after.

use super::*;
use crate::drain_restore::{
    StoppedHandoff, claim_handed_off_stopped, hand_off_stopped, lapse_handed_off_stopped_claim,
    project_stopped_disk_pins, stopped_handoff_state,
};

/// When the fixtures' sandboxes stopped.
const STOPPED_AT: &str = "2026-01-01T00:00:00Z";
const NEWER: &str = "2026-01-02T00:00:00Z";
const OLDER: &str = "2025-12-01T00:00:00Z";

/// A `disk` snapshot of `sandbox` in `state`, taken at `taken`, carrying
/// `artifact`, stored at a location that is orphaned when asked.
async fn disk(
    pool: &PgPool,
    sandbox: SandboxId,
    artifact: Uuid,
    state: &str,
    taken: &str,
    orphaned: bool,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, org_id
         )
         VALUES ($1, $2, 'disk', $3, $4::TIMESTAMPTZ, $5, 'disk', 'stop', $6)"
    ))
    .bind(id)
    .bind(sandbox)
    .bind(state)
    .bind(taken)
    .bind(artifact)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string, org_id, orphaned_at
         ) VALUES (
             $1, $2, 's3://test-bucket/snapshots/drained?region=us-east-1', $3,
             CASE WHEN $4 THEN NOW() END
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(id)
    .bind(SEED_ORG)
    .bind(orphaned)
    .execute(pool)
    .await?;
    Ok(id)
}

/// The whole row as JSON: equal before and after a statement iff it wrote
/// nothing.
async fn row_json(pool: &PgPool, id: SandboxId) -> TestResult<serde_json::Value> {
    Ok(
        sqlx::query_scalar(sql!("SELECT to_jsonb(s) FROM sandboxes s WHERE s.id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?,
    )
}

/// Every execution of `id` as JSON text, beside [`row_json`].
async fn executions_image(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.generation)::TEXT, '[]')
           FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// The columns whose values differ between two row images, sorted.
fn changed_columns(before: &serde_json::Value, after: &serde_json::Value) -> Vec<String> {
    let (Some(before), Some(after)) = (before.as_object(), after.as_object()) else {
        return vec!["<not a row>".to_owned()];
    };
    let mut changed: Vec<String> = after
        .iter()
        .filter(|(column, value)| before.get(*column) != Some(*value))
        .map(|(column, _)| column.clone())
        .collect();
    changed.sort();
    changed
}

/// A node's drain engine and phase.
type Drain = (Option<&'static str>, Option<&'static str>);

/// The drained node's Kubernetes name in the stopped fixtures.
const NODE_NAME: &str = "worker-drained.synthetic.internal";

/// A worker node named [`NODE_NAME`] under drain `engine` in `phase`.
async fn node(pool: &PgPool, engine: Option<&str>, phase: Option<&str>) -> TestResult<NodeId> {
    let id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_engine, drain_phase)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'registered', $2, $3)"
    ))
    .bind(id)
    .bind(engine)
    .bind(phase)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(id)
    .bind(NODE_NAME)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A node whose v2 drain is `drained`.
async fn drained_node(pool: &PgPool) -> TestResult<NodeId> {
    node(pool, Some("v2"), Some("drained")).await
}

/// A stopped sandbox at home on `node`: stopped at [`STOPPED_AT`], its
/// pre-stop execution on [`NODE_NAME`] created before that, and a ready,
/// stored disk of `artifact` published at `published_at`, which it doesn't
/// pin.
async fn stopped_at_home(
    pool: &PgPool,
    node: NodeId,
    artifact: Uuid,
    published_at: &str,
) -> TestResult<SandboxId> {
    let id = SandboxId::from_uuid(insert_sandbox(pool, None, "stopped").await?);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, placement = 'on_node',
                lifecycle_changed_at = $3::TIMESTAMPTZ,
                placement_changed_at = $3::TIMESTAMPTZ
          WHERE id = $1"
    ))
    .bind(id)
    .bind(node)
    .bind(STOPPED_AT)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, state, k8s_node_name, created_at,
             pod_uid
         )
         VALUES ($1, 1, $2, 'space', 'running', $3, $4::TIMESTAMPTZ, 'pod-uid-' || $1::TEXT)"
    ))
    .bind(id)
    .bind(format!("pod-{id}-g1"))
    .bind(NODE_NAME)
    .bind(OLDER)
    .execute(pool)
    .await?;
    disk(pool, id, artifact, "ready", published_at, false).await?;
    Ok(id)
}

/// Pin `artifact` on `id`, as the ingest's projection does.
async fn pin(pool: &PgPool, id: SandboxId, artifact: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2, current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(artifact)
    .execute(pool)
    .await?;
    Ok(())
}

async fn hand_off(pool: &PgPool, id: SandboxId, node: NodeId) -> TestResult<StoppedHandoff> {
    let mut tx = pool.begin().await?;
    let done = hand_off_stopped(&mut tx, id, node).await?;
    tx.commit().await?;
    Ok(done)
}

/// The handoff writes exactly the pin, v1's `stopped` marker and the
/// unplacing, stamping the placement clock, and retires the execution on
/// the node: every other column of the row is as it was. A second call
/// answers handed off and writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_handoff_writes_exactly_the_pin_the_marker_the_unplacing_and_the_retirement()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = drained_node(pool).await?;
    let artifact = Uuid::now_v7();
    let id = stopped_at_home(pool, node, artifact, NEWER).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET command_ready_pending = TRUE, target_node_id = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(node)
    .execute(pool)
    .await?;
    let before = row_json(pool, id).await?;

    assert_eq!(
        hand_off(pool, id, node).await?,
        StoppedHandoff::HandedOff(artifact)
    );
    let after = row_json(pool, id).await?;
    assert_eq!(
        changed_columns(&before, &after),
        [
            "active_node_id",
            "command_ready_pending",
            "current_local_artifact_id",
            "current_local_artifact_kind",
            "current_local_artifact_origin",
            "evacuation_restore_target",
            "placement",
            "placement_changed_at",
            "target_node_id",
            "updated_at",
        ]
    );
    assert_eq!(after["lifecycle"], "stopped");
    assert_eq!(after["active_node_id"], serde_json::Value::Null);
    assert_eq!(after["target_node_id"], serde_json::Value::Null);
    assert_eq!(after["placement"], "unplaced");
    assert_eq!(after["command_ready_pending"], false);
    assert_eq!(after["current_local_artifact_id"], artifact.to_string());
    assert_eq!(after["current_local_artifact_kind"], "disk");
    assert_eq!(after["current_local_artifact_origin"], "stop");
    assert_eq!(after["evacuation_restore_target"], "stopped");
    let execution: (bool, String) = sqlx::query_as(sql!(
        "SELECT active, state FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    assert_eq!(execution, (false, "forgotten".to_owned()));

    let executions = executions_image(pool, id).await?;
    assert_eq!(
        hand_off(pool, id, node).await?,
        StoppedHandoff::HandedOff(artifact),
        "handed off again"
    );
    assert_eq!(row_json(pool, id).await?, after, "handed off twice");
    assert_eq!(executions_image(pool, id).await?, executions);
    Ok(())
}

/// A row that moved on is left alone, and so is one the handoff refuses: a
/// disk from before the stop, a disk not ready or not stored, a pin of
/// another kind, an execution since the stop or on another node or held by
/// an activation, or a node whose drain isn't a drained or terminating v2
/// drain.
#[tokio::test(flavor = "current_thread")]
async fn a_row_that_moved_on_or_is_refused_is_left_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let artifact = Uuid::now_v7();

    type Setup = fn(SandboxId) -> String;
    let none: Setup = |_| String::new();
    // (case, node drain, disk published at, setup, moved on)
    type Case = (&'static str, Drain, &'static str, Setup, bool);
    let cases: [Case; 15] = [
        (
            "a snapshot of the disk is being deleted",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind, artifact_origin, org_id) SELECT gen_random_uuid(), sandbox_id, 'disk', 'deleting', NOW(), local_artifact_id, 'disk', 'stop', org_id FROM snapshots WHERE sandbox_id = '{id}'"
                )
            },
            false,
        ),
        (
            "started again",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| format!("UPDATE sandboxes SET lifecycle = 'started' WHERE id = '{id}'"),
            true,
        ),
        (
            "home elsewhere",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!("UPDATE sandboxes SET active_node_id = gen_random_uuid() WHERE id = '{id}'")
            },
            true,
        ),
        (
            "gone",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "DELETE FROM sandbox_k8s_executions WHERE sandbox_id = '{id}'; DELETE FROM snapshot_storage_locations WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = '{id}'); DELETE FROM snapshots WHERE sandbox_id = '{id}'; DELETE FROM sandboxes WHERE id = '{id}'"
                )
            },
            true,
        ),
        (
            "disk from before the stop",
            (Some("v2"), Some("drained")),
            OLDER,
            none,
            false,
        ),
        (
            "disk not ready",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| format!("UPDATE snapshots SET state = 'creating' WHERE sandbox_id = '{id}'"),
            false,
        ),
        (
            "disk not stored",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = '{id}')"
                )
            },
            false,
        ),
        (
            "pins a full snapshot",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "UPDATE sandboxes SET current_local_artifact_id = gen_random_uuid(), current_local_artifact_kind = 'full', current_local_artifact_origin = 'suspend' WHERE id = '{id}'"
                )
            },
            false,
        ),
        (
            "pins another disk",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "UPDATE sandboxes SET current_local_artifact_id = gen_random_uuid(), current_local_artifact_kind = 'disk', current_local_artifact_origin = 'stop' WHERE id = '{id}'"
                )
            },
            false,
        ),
        (
            "execution since the stop",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "UPDATE sandbox_k8s_executions SET created_at = NOW() WHERE sandbox_id = '{id}'"
                )
            },
            false,
        ),
        (
            "execution on another node",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "UPDATE sandbox_k8s_executions SET k8s_node_name = 'elsewhere' WHERE sandbox_id = '{id}'"
                )
            },
            false,
        ),
        (
            "an activation holds the execution",
            (Some("v2"), Some("drained")),
            NEWER,
            |id| {
                format!(
                    "UPDATE sandbox_k8s_executions SET activation_attempt_id = gen_random_uuid(), activation_attempt_expires_at = NOW() + INTERVAL '1 hour', activation_attempt_target_state = 'started' WHERE sandbox_id = '{id}'"
                )
            },
            false,
        ),
        (
            "a draining v2 node",
            (Some("v2"), Some("draining")),
            NEWER,
            none,
            false,
        ),
        (
            "a v1 drained node",
            (None, Some("drained")),
            NEWER,
            none,
            false,
        ),
        (
            "a v2 cancel pending",
            (Some("v2"), None),
            NEWER,
            none,
            false,
        ),
    ];
    for (case, (engine, phase), published_at, setup, moved_on) in cases {
        let node = node(pool, engine, phase).await?;
        let id = stopped_at_home(pool, node, artifact, published_at).await?;
        let statement = setup(id);
        if !statement.is_empty() {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                .execute(pool)
                .await?;
        }
        let gone = case == "gone";
        let before = if gone {
            None
        } else {
            Some((row_json(pool, id).await?, executions_image(pool, id).await?))
        };
        let done = hand_off(pool, id, node).await?;
        if moved_on {
            assert_eq!(done, StoppedHandoff::MovedOn, "{case}");
        } else {
            assert!(
                matches!(done, StoppedHandoff::Refused(_)),
                "{case}: {done:?}"
            );
        }
        if let Some(before) = before {
            assert_eq!(
                (row_json(pool, id).await?, executions_image(pool, id).await?),
                before,
                "{case}: wrote"
            );
        }
    }
    Ok(())
}

/// Freshness: a disk taken before the stop is refused whether or not the
/// row pins it, since only the catalog's clock places a disk after the stop.
/// A terminating node hands off, and a v1 `stopped` marker an undrain
/// abandoned on a row at home is taken over.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_pin_is_refused_and_a_terminating_node_and_an_abandoned_v1_marker_hand_off()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_drained = drained_node(pool).await?;
    let artifact = Uuid::now_v7();

    let pinned = stopped_at_home(pool, node_drained, artifact, OLDER).await?;
    pin(pool, pinned, artifact).await?;
    let before = row_json(pool, pinned).await?;
    assert!(matches!(
        hand_off(pool, pinned, node_drained).await?,
        StoppedHandoff::Refused(_)
    ));
    assert_eq!(row_json(pool, pinned).await?, before);
    let pinned_fresh = stopped_at_home(pool, node_drained, artifact, NEWER).await?;
    pin(pool, pinned_fresh, artifact).await?;
    assert_eq!(
        hand_off(pool, pinned_fresh, node_drained).await?,
        StoppedHandoff::HandedOff(artifact)
    );
    let unpinned = stopped_at_home(pool, node_drained, artifact, OLDER).await?;
    assert!(matches!(
        hand_off(pool, unpinned, node_drained).await?,
        StoppedHandoff::Refused(_)
    ));

    let terminating = node(pool, Some("v2"), Some("terminating")).await?;
    let id = stopped_at_home(pool, terminating, artifact, NEWER).await?;
    assert_eq!(
        hand_off(pool, id, terminating).await?,
        StoppedHandoff::HandedOff(artifact)
    );

    let abandoned = stopped_at_home(pool, node_drained, artifact, NEWER).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'stopped' WHERE id = $1"
    ))
    .bind(abandoned)
    .execute(pool)
    .await?;
    assert_eq!(
        hand_off(pool, abandoned, node_drained).await?,
        StoppedHandoff::HandedOff(artifact)
    );
    Ok(())
}

/// The handoff decides under the row lock: while another transaction holds
/// the row it waits, and once that transaction commits a start of the row
/// the handoff finds it moved on and writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_handoff_decides_under_the_row_lock() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = drained_node(pool).await?;
    let artifact = Uuid::now_v7();
    let id = stopped_at_home(pool, node, artifact, NEWER).await?;

    let mut holder = pool.begin().await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(id)
        .execute(&mut *holder)
        .await?;
    let handoff = tokio::spawn({
        let pool = pool.clone();
        async move {
            let mut tx = pool.begin().await?;
            let done = hand_off_stopped(&mut tx, id, node).await?;
            tx.commit().await?;
            TestResult::Ok(done)
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!handoff.is_finished(), "the handoff waits for the row lock");
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(id)
    .execute(&mut *holder)
    .await?;
    let started = row_json_in(&mut holder, id).await?;
    holder.commit().await?;
    assert_eq!(handoff.await??, StoppedHandoff::MovedOn);
    assert_eq!(row_json(pool, id).await?, started);
    Ok(())
}

async fn row_json_in(
    conn: &mut sqlx::PgConnection,
    id: SandboxId,
) -> TestResult<serde_json::Value> {
    Ok(
        sqlx::query_scalar(sql!("SELECT to_jsonb(s) FROM sandboxes s WHERE s.id = $1"))
            .bind(id)
            .fetch_one(conn)
            .await?,
    )
}

/// One published row, as the internal API's recordset carries it.
fn published(
    id: SandboxId,
    lifecycle: &str,
    artifact: Uuid,
    kind: &str,
    origin: &str,
) -> serde_json::Value {
    serde_json::json!({
        "sandbox_id": id.to_string(),
        "published_lifecycle": lifecycle,
        "current_local_artifact_id": artifact,
        "current_local_artifact_kind": kind,
        "current_local_artifact_origin": origin,
        "execution_generation": 1,
        "pod_uid": format!("pod-uid-{id}"),
    })
}

/// `published` with `field` set to `value`.
fn published_with(
    id: SandboxId,
    artifact: Uuid,
    field: &str,
    value: serde_json::Value,
) -> serde_json::Value {
    let mut row = published(id, "stopped", artifact, "disk", "stop");
    row[field] = value;
    row
}

async fn project(
    pool: &PgPool,
    node: NodeId,
    rows: Vec<serde_json::Value>,
) -> TestResult<Vec<SandboxId>> {
    Ok(project_stopped_disk_pins(
        pool,
        node,
        sqlx::types::Json(serde_json::Value::Array(rows)),
    )
    .await?)
}

/// The ingest's projection pins a stopped row's published disk only for a
/// node whose drain is v2, and only when fenced: still stopped at home with
/// no pin and no marker, the same stop, and a ready, stored disk of the
/// published artifact taken since the stop (freshness), none of it being
/// deleted. Anything else writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_projection_pins_a_stopped_disk_only_for_a_v2_node_and_only_when_fenced() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for phase in [Some("draining"), Some("drained"), None] {
        let node = node(pool, Some("v2"), phase).await?;
        let artifact = Uuid::now_v7();
        let id = stopped_at_home(pool, node, artifact, NEWER).await?;
        let before = row_json(pool, id).await?;
        assert_eq!(
            project(
                pool,
                node,
                vec![published(id, "stopped", artifact, "disk", "stop")]
            )
            .await?,
            [id],
            "{phase:?}"
        );
        let after = row_json(pool, id).await?;
        assert_eq!(
            changed_columns(&before, &after),
            [
                "current_local_artifact_id",
                "current_local_artifact_kind",
                "current_local_artifact_origin",
                "updated_at",
            ]
        );
        assert_eq!(after["current_local_artifact_id"], artifact.to_string());
        assert!(
            project(
                pool,
                node,
                vec![published(id, "stopped", artifact, "disk", "stop")]
            )
            .await?
            .is_empty(),
            "pinned already"
        );
    }

    type Setup = fn(SandboxId) -> String;
    let none: Setup = |_| String::new();
    // (case, node drain, disk published at, setup, published lifecycle, kind, origin)
    type Case = (
        &'static str,
        Drain,
        &'static str,
        Setup,
        &'static str,
        &'static str,
        &'static str,
    );
    let cases: [Case; 12] = [
        (
            "no drain",
            (None, None),
            NEWER,
            none,
            "stopped",
            "disk",
            "stop",
        ),
        (
            "a v1 drain",
            (None, Some("draining")),
            NEWER,
            none,
            "stopped",
            "disk",
            "stop",
        ),
        (
            "published started",
            (Some("v2"), Some("draining")),
            NEWER,
            none,
            "started",
            "disk",
            "stop",
        ),
        (
            "a full artifact",
            (Some("v2"), Some("draining")),
            NEWER,
            none,
            "stopped",
            "full",
            "suspend",
        ),
        (
            "not this stop's disk",
            (Some("v2"), Some("draining")),
            OLDER,
            none,
            "stopped",
            "disk",
            "stop",
        ),
        (
            "disk not ready",
            (Some("v2"), Some("draining")),
            NEWER,
            |id| format!("UPDATE snapshots SET state = 'creating' WHERE sandbox_id = '{id}'"),
            "stopped",
            "disk",
            "stop",
        ),
        (
            "disk not stored",
            (Some("v2"), Some("draining")),
            NEWER,
            |id| {
                format!(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = '{id}')"
                )
            },
            "stopped",
            "disk",
            "stop",
        ),
        (
            "a snapshot of the artifact is being deleted",
            (Some("v2"), Some("draining")),
            NEWER,
            |id| {
                format!(
                    "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind, artifact_origin, org_id) SELECT gen_random_uuid(), sandbox_id, 'disk', 'deleting', NOW(), local_artifact_id, 'disk', 'stop', org_id FROM snapshots WHERE sandbox_id = '{id}'"
                )
            },
            "stopped",
            "disk",
            "stop",
        ),
        (
            "home elsewhere",
            (Some("v2"), Some("draining")),
            NEWER,
            |id| {
                format!("UPDATE sandboxes SET active_node_id = gen_random_uuid() WHERE id = '{id}'")
            },
            "stopped",
            "disk",
            "stop",
        ),
        (
            "still started",
            (Some("v2"), Some("draining")),
            NEWER,
            |id| format!("UPDATE sandboxes SET lifecycle = 'started' WHERE id = '{id}'"),
            "stopped",
            "disk",
            "stop",
        ),
        (
            "an execution since the stop",
            (Some("v2"), Some("draining")),
            NEWER,
            |id| {
                format!(
                    "UPDATE sandbox_k8s_executions SET created_at = NOW() WHERE sandbox_id = '{id}'"
                )
            },
            "stopped",
            "disk",
            "stop",
        ),
        (
            "marked",
            (Some("v2"), Some("draining")),
            NEWER,
            |id| {
                format!(
                    "UPDATE sandboxes SET evacuation_restore_target = 'stopped' WHERE id = '{id}'"
                )
            },
            "stopped",
            "disk",
            "stop",
        ),
    ];
    for (case, (engine, phase), published_at, setup, lifecycle, kind, origin) in cases {
        let node = node(pool, engine, phase).await?;
        let artifact = Uuid::now_v7();
        let id = stopped_at_home(pool, node, artifact, published_at).await?;
        let statement = setup(id);
        if !statement.is_empty() {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                .execute(pool)
                .await?;
        }
        let before = row_json(pool, id).await?;
        assert!(
            project(
                pool,
                node,
                vec![published(id, lifecycle, artifact, kind, origin)]
            )
            .await?
            .is_empty(),
            "{case}"
        );
        assert_eq!(row_json(pool, id).await?, before, "{case}: wrote");
    }
    Ok(())
}

async fn lease(pool: &PgPool, id: SandboxId) -> TestResult<(String, Option<Uuid>, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, restore_lease_token,
                COALESCE(restore_lease_expires_at > NOW(), FALSE)
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn v1_claim(pool: &PgPool, id: SandboxId) -> TestResult<Option<String>> {
    Ok(crate::restore_lease::claim_evacuation_restore(
        pool,
        id,
        120,
        120,
        Some(SandboxLifecycle::Stopped),
    )
    .await?
    .map(|row| row.lifecycle))
}

/// The start's claim and v1's restore sweep's claim exclude each other: the
/// start claims a handed-off row under a live lease, which v1's claim and a
/// second start leave alone; a start that lapses its lease is resumed by the
/// next start; and a lapsed lease v1's claim takes over is cleared, so a
/// start leaves the sweep's re-placement alone.
#[tokio::test(flavor = "current_thread")]
async fn the_starts_claim_and_v1s_sweep_never_both_own_a_handed_off_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = drained_node(pool).await?;
    let artifact = Uuid::now_v7();
    let id = stopped_at_home(pool, node, artifact, NEWER).await?;
    assert_eq!(
        hand_off(pool, id, node).await?,
        StoppedHandoff::HandedOff(artifact)
    );

    let first = Uuid::now_v7();
    assert_eq!(
        claim_handed_off_stopped(pool, id, 120, first)
            .await?
            .as_deref(),
        Some("stopped")
    );
    assert_eq!(
        lease(pool, id).await?,
        ("restoring".to_owned(), Some(first), true)
    );
    assert_eq!(
        v1_claim(pool, id).await?,
        None,
        "v1 leaves a live lease alone"
    );
    assert_eq!(
        claim_handed_off_stopped(pool, id, 120, Uuid::now_v7()).await?,
        None,
        "a second start leaves a live lease alone"
    );
    assert_eq!(
        lease(pool, id).await?,
        ("restoring".to_owned(), Some(first), true)
    );

    // The start failed after its claim: it lapses its lease, and the next
    // start resumes the claim.
    assert!(lapse_handed_off_stopped_claim(pool, id, first).await?);
    let second = Uuid::now_v7();
    assert_eq!(
        claim_handed_off_stopped(pool, id, 120, second)
            .await?
            .as_deref(),
        Some("restoring")
    );
    assert_eq!(
        lease(pool, id).await?,
        ("restoring".to_owned(), Some(second), true)
    );
    assert!(
        !lapse_handed_off_stopped_claim(pool, id, first).await?,
        "another start's lease"
    );

    // Lapsed again, v1's sweep takes the re-placement over and clears the
    // lease; the start then leaves the sweep's re-placement alone.
    assert!(lapse_handed_off_stopped_claim(pool, id, second).await?);
    assert_eq!(v1_claim(pool, id).await?.as_deref(), Some("restoring"));
    assert_eq!(
        lease(pool, id).await?,
        ("restoring".to_owned(), None, false)
    );
    assert_eq!(
        claim_handed_off_stopped(pool, id, 120, Uuid::now_v7()).await?,
        None
    );

    // An unclaimed handed-off row the sweep claims first is the sweep's.
    let swept = stopped_at_home(pool, node, artifact, NEWER).await?;
    assert_eq!(
        hand_off(pool, swept, node).await?,
        StoppedHandoff::HandedOff(artifact)
    );
    assert_eq!(v1_claim(pool, swept).await?.as_deref(), Some("stopped"));
    assert_eq!(
        claim_handed_off_stopped(pool, swept, 120, Uuid::now_v7()).await?,
        None
    );

    // A start never claims a row that isn't handed off.
    let at_home = stopped_at_home(pool, node, artifact, NEWER).await?;
    assert_eq!(
        claim_handed_off_stopped(pool, at_home, 120, Uuid::now_v7()).await?,
        None
    );
    assert_eq!(
        lease(pool, at_home).await?,
        ("stopped".to_owned(), None, false)
    );
    Ok(())
}

/// The start's one read: the evidence for a handoff holds for a stopped row
/// at home on a drained or terminating v2 node with its disk durable since
/// the stop, and for nothing else; a handed-off row, claimed or not, is
/// pending.
#[tokio::test(flavor = "current_thread")]
async fn the_starts_read_names_the_evidence_and_a_pending_handoff() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let state = |pool: PgPool, id: SandboxId| async move {
        let state = stopped_handoff_state(&pool, id)
            .await?
            .expect("the row exists");
        TestResult::Ok((state.hand_off_from, state.pending))
    };
    let artifact = Uuid::now_v7();
    for (engine, phase, evidence) in [
        (Some("v2"), Some("drained"), true),
        (Some("v2"), Some("terminating"), true),
        (Some("v2"), Some("draining"), false),
        (Some("v2"), None, false),
        (None, Some("drained"), false),
        (None, None, false),
    ] {
        let node = node(pool, engine, phase).await?;
        let id = stopped_at_home(pool, node, artifact, NEWER).await?;
        assert_eq!(
            state(pool.clone(), id).await?,
            (evidence.then_some(node), false),
            "{engine:?} {phase:?}"
        );
    }
    let node = drained_node(pool).await?;
    let not_durable = stopped_at_home(pool, node, artifact, OLDER).await?;
    assert_eq!(state(pool.clone(), not_durable).await?, (None, false));
    let pinned = stopped_at_home(pool, node, artifact, OLDER).await?;
    pin(pool, pinned, artifact).await?;
    assert_eq!(
        state(pool.clone(), pinned).await?,
        (None, false),
        "a pinned disk from before the stop isn't this stop's"
    );
    let pinned = stopped_at_home(pool, node, artifact, NEWER).await?;
    pin(pool, pinned, artifact).await?;
    assert_eq!(state(pool.clone(), pinned).await?, (Some(node), false));

    let id = stopped_at_home(pool, node, artifact, NEWER).await?;
    hand_off(pool, id, node).await?;
    assert_eq!(state(pool.clone(), id).await?, (None, true));
    claim_handed_off_stopped(pool, id, 120, Uuid::now_v7()).await?;
    assert_eq!(state(pool.clone(), id).await?, (None, true));
    assert!(
        stopped_handoff_state(pool, SandboxId::new())
            .await?
            .is_none()
    );
    Ok(())
}

/// The row's version: changes iff Postgres wrote the row.
async fn xmin(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT xmin::TEXT FROM sandboxes WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?,
    )
}

/// The projection's identity, pin and write fences: a publication from
/// another execution generation or pod is refused, and one with no identity,
/// a stopped sandbox's, is pinned on the same-stop fences; a pin is never replaced
/// by another, and a suspend's pin is never written, whatever the node
/// publishes; a `(disk, stop)` pin the node no longer publishes is cleared,
/// and the next publication pins the disk it does publish; and republishing
/// what the row already pins writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_projection_keeps_to_the_execution_never_replaces_a_pin_and_writes_only_changes()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = node(pool, Some("v2"), Some("draining")).await?;

    // Another execution: a generation or a pod the row's isn't.
    for (field, value) in [
        ("execution_generation", serde_json::json!(2)),
        ("pod_uid", serde_json::json!("another-pod")),
        ("pod_uid", serde_json::Value::Null),
    ] {
        let artifact = Uuid::now_v7();
        let id = stopped_at_home(pool, node, artifact, NEWER).await?;
        let before = row_json(pool, id).await?;
        assert!(
            project(
                pool,
                node,
                vec![published_with(id, artifact, field, value.clone())]
            )
            .await?
            .is_empty(),
            "{field}={value}"
        );
        assert_eq!(row_json(pool, id).await?, before, "{field}={value}: wrote");
    }

    // A stopped sandbox's publication carries no execution identity (the
    // node clears its route identity at the stop): the same-stop fences
    // stand for it, and its disk is pinned.
    let artifact = Uuid::now_v7();
    let anonymous = stopped_at_home(pool, node, artifact, NEWER).await?;
    let mut row = published(anonymous, "stopped", artifact, "disk", "stop");
    row["execution_generation"] = serde_json::Value::Null;
    row["pod_uid"] = serde_json::Value::Null;
    assert_eq!(project(pool, node, vec![row]).await?, [anonymous]);

    // A suspend's pin is never wiped or replaced: not by a published disk,
    // nor by a publication with no artifact.
    let artifact = Uuid::now_v7();
    let suspended_pin = stopped_at_home(pool, node, artifact, NEWER).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = gen_random_uuid(),
                current_local_artifact_kind = 'full', current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(suspended_pin)
    .execute(pool)
    .await?;
    let before = row_json(pool, suspended_pin).await?;
    for row in [
        published(suspended_pin, "stopped", artifact, "disk", "stop"),
        published_with(
            suspended_pin,
            artifact,
            "current_local_artifact_id",
            serde_json::Value::Null,
        ),
    ] {
        assert!(project(pool, node, vec![row]).await?.is_empty());
        assert_eq!(
            row_json(pool, suspended_pin).await?,
            before,
            "a suspend's pin was written"
        );
    }

    // A stop pin the node no longer publishes is cleared, never replaced:
    // the next publication pins what the node publishes.
    let published_disk = Uuid::now_v7();
    let stale = stopped_at_home(pool, node, published_disk, NEWER).await?;
    let stale_pin = Uuid::now_v7();
    pin(pool, stale, stale_pin).await?;
    assert_eq!(
        project(
            pool,
            node,
            vec![published(stale, "stopped", published_disk, "disk", "stop")]
        )
        .await?,
        [stale]
    );
    let cleared = row_json(pool, stale).await?;
    assert_eq!(
        cleared["current_local_artifact_id"],
        serde_json::Value::Null
    );
    assert_eq!(
        cleared["current_local_artifact_kind"],
        serde_json::Value::Null
    );
    assert_eq!(
        project(
            pool,
            node,
            vec![published(stale, "stopped", published_disk, "disk", "stop")]
        )
        .await?,
        [stale]
    );
    assert_eq!(
        row_json(pool, stale).await?["current_local_artifact_id"],
        published_disk.to_string()
    );
    // A publication with no artifact clears it too, under the same fences;
    // one from another execution doesn't.
    let pinned = row_json(pool, stale).await?;
    assert!(
        project(
            pool,
            node,
            vec![published_with(
                stale,
                published_disk,
                "current_local_artifact_id",
                serde_json::Value::Null
            ),]
            .into_iter()
            .map(|mut row| {
                row["execution_generation"] = serde_json::json!(2);
                row
            })
            .collect()
        )
        .await?
        .is_empty()
    );
    assert_eq!(row_json(pool, stale).await?, pinned);
    assert_eq!(
        project(
            pool,
            node,
            vec![published_with(
                stale,
                published_disk,
                "current_local_artifact_id",
                serde_json::Value::Null
            )]
        )
        .await?,
        [stale]
    );
    assert_eq!(
        row_json(pool, stale).await?["current_local_artifact_id"],
        serde_json::Value::Null
    );

    // The census republishing what the row already pins writes nothing.
    let unchanged = stopped_at_home(pool, node, artifact, NEWER).await?;
    pin(pool, unchanged, artifact).await?;
    let version = xmin(pool, unchanged).await?;
    for _ in 0..2 {
        assert!(
            project(
                pool,
                node,
                vec![published(unchanged, "stopped", artifact, "disk", "stop")]
            )
            .await?
            .is_empty()
        );
    }
    assert_eq!(
        xmin(pool, unchanged).await?,
        version,
        "an unchanged republish wrote"
    );
    Ok(())
}
