//! The v2 drain's release of a stopped sandbox ([`crate::drain_restore`]).
//!
//! A refused release must leave the row exactly as it was: each refusal
//! compares the whole row before and after.

use super::*;
use crate::drain_restore::{V2StoppedEvacuation, evacuate_v2_stopped};

/// When the fixtures' sandboxes stopped.
const STOPPED_AT: &str = "2026-01-01T00:00:00Z";
const NEWER: &str = "2026-01-02T00:00:00Z";
const OLDER: &str = "2025-12-01T00:00:00Z";

/// A full snapshot of `sandbox` in `state`, taken at `taken`, carrying
/// `artifact` when given, stored at a location that is orphaned when asked.
async fn snapshot(
    pool: &PgPool,
    sandbox: SandboxId,
    artifact: Option<Uuid>,
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
         VALUES ($1, $2, 'full', $3, $4::TIMESTAMPTZ, $5,
                 CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'full' END,
                 CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'suspend' END, $6)"
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

/// The whole row as JSON text: equal before and after a statement iff it
/// wrote nothing.
async fn row_image(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT row_to_json(s)::TEXT FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Every execution of `id` as JSON text, beside [`row_image`].
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

/// The drained node's Kubernetes name in the stopped fixtures.
const NODE_NAME: &str = "worker-drained.synthetic.internal";

/// A stopped sandbox at home on `node`: stopped at `stopped_at`, its
/// pre-stop execution on [`NODE_NAME`] created before that, and a ready,
/// stored disk of `artifact` published at `published_at`.
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
                lifecycle_changed_at = $3::TIMESTAMPTZ
          WHERE id = $1"
    ))
    .bind(id)
    .bind(node)
    .bind(STOPPED_AT)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, k8s_node_name, created_at)
         VALUES ($1, 1, $2, 'space', 'running', $3, $4::TIMESTAMPTZ)"
    ))
    .bind(id)
    .bind(format!("pod-{id}-g1"))
    .bind(NODE_NAME)
    .bind(OLDER)
    .execute(pool)
    .await?;
    let disk = snapshot(pool, id, Some(artifact), "ready", published_at, false).await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET type = 'disk', artifact_kind = 'disk', artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(disk)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn evacuate(
    pool: &PgPool,
    id: SandboxId,
    node: NodeId,
    artifact: Uuid,
) -> TestResult<V2StoppedEvacuation> {
    let mut tx = pool.begin().await?;
    let done = evacuate_v2_stopped(&mut tx, id, node, NODE_NAME, artifact).await?;
    tx.commit().await?;
    Ok(done)
}

/// A stopped sandbox the node reported evacuated is pinned to the reported
/// disk and released from its home, once, and answers released again after.
/// A report it can't take, from another stop or with another pin, changes
/// nothing and is refused, never settled; a row that left the node moved on.
/// A v1 marker an undrain abandoned is taken over.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_sandbox_is_released_only_on_its_homes_report_of_this_stops_disk() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let artifact = Uuid::now_v7();

    let id = stopped_at_home(pool, node, artifact, NEWER).await?;
    assert_eq!(
        evacuate(pool, id, node, artifact).await?,
        V2StoppedEvacuation::Released
    );
    let released: (
        String,
        Option<NodeId>,
        String,
        Option<Uuid>,
        Option<String>,
        bool,
    ) = sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.active_node_id, s.placement, s.current_local_artifact_id,
                    s.current_local_artifact_kind,
                    EXISTS (SELECT 1 FROM sandbox_k8s_executions e
                             WHERE e.sandbox_id = s.id AND e.active)
               FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        released,
        (
            "stopped".to_owned(),
            None,
            "unplaced".to_owned(),
            Some(artifact),
            Some("disk".to_owned()),
            false
        )
    );
    let after = row_image(pool, id).await?;
    assert_eq!(
        evacuate(pool, id, node, artifact).await?,
        V2StoppedEvacuation::Released
    );
    assert_eq!(row_image(pool, id).await?, after, "released twice");

    type Setup = fn(SandboxId, NodeId) -> String;
    let cases: [(&str, &str, Setup, bool); 7] = [
        (
            "disk from before the stop",
            OLDER,
            |_, _| String::new(),
            true,
        ),
        (
            "pins another disk",
            NEWER,
            |id, _| {
                format!(
                    "UPDATE sandboxes SET current_local_artifact_id = gen_random_uuid(), current_local_artifact_kind = 'disk', current_local_artifact_origin = 'stop' WHERE id = '{id}'"
                )
            },
            true,
        ),
        (
            "execution since the stop",
            NEWER,
            |id, _| {
                format!(
                    "UPDATE sandbox_k8s_executions SET created_at = NOW() WHERE sandbox_id = '{id}'"
                )
            },
            true,
        ),
        (
            "execution on another node",
            NEWER,
            |id, _| {
                format!(
                    "UPDATE sandbox_k8s_executions SET k8s_node_name = 'elsewhere' WHERE sandbox_id = '{id}'"
                )
            },
            true,
        ),
        (
            "disk not ready",
            NEWER,
            |id, _| format!("UPDATE snapshots SET state = 'creating' WHERE sandbox_id = '{id}'"),
            true,
        ),
        (
            "home elsewhere",
            NEWER,
            |id, _| {
                format!("UPDATE sandboxes SET active_node_id = gen_random_uuid() WHERE id = '{id}'")
            },
            false,
        ),
        (
            "started again",
            NEWER,
            |id, _| format!("UPDATE sandboxes SET lifecycle = 'started' WHERE id = '{id}'"),
            false,
        ),
    ];
    for (case, published_at, setup, refused) in cases {
        let id = stopped_at_home(pool, node, artifact, published_at).await?;
        let statement = setup(id, node);
        if !statement.is_empty() {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(pool)
                .await?;
        }
        let before = (
            row_image(pool, id).await?,
            executions_image(pool, id).await?,
        );
        let done = evacuate(pool, id, node, artifact).await?;
        assert_eq!(
            matches!(done, V2StoppedEvacuation::Refused(_)),
            refused,
            "{case}: {done:?}"
        );
        assert_eq!(
            matches!(done, V2StoppedEvacuation::MovedOn),
            !refused,
            "{case}: {done:?}"
        );
        assert_eq!(
            (
                row_image(pool, id).await?,
                executions_image(pool, id).await?
            ),
            before,
            "{case}: wrote"
        );
    }
    assert_eq!(
        evacuate(pool, SandboxId::new(), node, artifact).await?,
        V2StoppedEvacuation::MovedOn
    );

    // A v1 stopped evacuation an undrain abandoned before its claim is taken
    // over: released, its marker cleared.
    let abandoned = stopped_at_home(pool, node, artifact, NEWER).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'stopped' WHERE id = $1"
    ))
    .bind(abandoned)
    .execute(pool)
    .await?;
    assert_eq!(
        evacuate(pool, abandoned, node, artifact).await?,
        V2StoppedEvacuation::Released
    );
    let marker: Option<String> = sqlx::query_scalar(sql!(
        "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(abandoned)
    .fetch_one(pool)
    .await?;
    assert_eq!(marker, None);
    Ok(())
}

/// A stopped sandbox a re-placement landed on the node, still pinning the
/// disk it was materialized from, is released on that disk's durability,
/// though the disk was published before the landing; unpinned, the same
/// disk is refused as another stop's.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placed_stopped_sandbox_is_released_on_its_pinned_disk() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let artifact = Uuid::now_v7();
    for (pinned, expected) in [
        (true, V2StoppedEvacuation::Released),
        (
            false,
            V2StoppedEvacuation::Refused("the disk isn't a ready, stored disk of this stop"),
        ),
    ] {
        let id = stopped_at_home(pool, node, artifact, OLDER).await?;
        if pinned {
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
        }
        assert_eq!(
            evacuate(pool, id, node, artifact).await?,
            expected,
            "pinned={pinned}"
        );
    }
    Ok(())
}

/// A row main's own paths leave in the released shape isn't one a v2 drain
/// released: a v1 re-placement landed it, its pod bind stamping the placement
/// clock, pinning its stored disk, and a pod teardown then unplaced it. It
/// isn't materializable, a start can't mark it, and the drain doesn't take
/// its report of the same disk as a release of its own. The same row
/// released by the v2 drain, which leaves the clock unset, is materializable.
#[tokio::test(flavor = "current_thread")]
async fn only_a_v2_release_leaves_an_unplaced_stopped_row_materializable() -> TestResult {
    use crate::drain_restore::{
        mark_unplaced_stopped_for_materialization, materializable_unplaced_stopped,
    };
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let materializable = |pool: PgPool, id: SandboxId| async move {
        let predicate = materializable_unplaced_stopped("s");
        let statement = format!("SELECT {predicate} FROM sandboxes s WHERE s.id = $1");
        let materializable: bool =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement.as_str())))
                .bind(id)
                .fetch_one(&pool)
                .await?;
        TestResult::Ok(materializable)
    };
    // Landed on `node` by a pod bind, which stamps the placement clock, and
    // pinning its stored disk, as v1's stopped re-placement leaves a row.
    let landed = |pool: PgPool, artifact: Uuid| async move {
        let id = stopped_at_home(&pool, node, artifact, NEWER).await?;
        crate::wake::record_pod_node_placement(&pool, id, node).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2, current_local_artifact_kind = 'disk',
                    current_local_artifact_origin = 'stop'
              WHERE id = $1"
        ))
        .bind(id)
        .bind(artifact)
        .execute(&pool)
        .await?;
        TestResult::Ok(id)
    };

    // Its pod went: the pod teardown retires the execution and unplaces it.
    let artifact = Uuid::now_v7();
    let main_shaped = landed(pool.clone(), artifact).await?;
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'forgotten'
          WHERE sandbox_id = $1"
    ))
    .bind(main_shaped)
    .execute(&mut *tx)
    .await?;
    crate::pod_tracking::unplace_after_pod_teardown(&mut tx, main_shaped).await?;
    tx.commit().await?;
    let before = row_image(pool, main_shaped).await?;
    assert!(!materializable(pool.clone(), main_shaped).await?);
    assert!(!mark_unplaced_stopped_for_materialization(pool, main_shaped).await?);
    assert_eq!(
        evacuate(pool, main_shaped, node, artifact).await?,
        V2StoppedEvacuation::MovedOn,
        "a row main unplaced isn't one this drain released"
    );
    assert_eq!(row_image(pool, main_shaped).await?, before);

    let artifact = Uuid::now_v7();
    let released = landed(pool.clone(), artifact).await?;
    assert_eq!(
        evacuate(pool, released, node, artifact).await?,
        V2StoppedEvacuation::Released
    );
    assert!(materializable(pool.clone(), released).await?);
    assert_eq!(
        evacuate(pool, released, node, artifact).await?,
        V2StoppedEvacuation::Released,
        "a release this drain made answers released again"
    );
    assert!(mark_unplaced_stopped_for_materialization(pool, released).await?);
    Ok(())
}

/// A start materializes only an unplaced stopped row whose pinned disk is
/// durable and that runs nowhere: the row a drain's release leaves is marked
/// once, and taken back; a placed row, one with an active execution, or one
/// whose disk isn't ready is not.
#[tokio::test(flavor = "current_thread")]
async fn only_an_unplaced_stopped_row_with_a_durable_pin_is_marked_for_materialization()
-> TestResult {
    use crate::drain_restore::{
        mark_unplaced_stopped_for_materialization, unmark_unplaced_stopped,
    };
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let released = |pool: PgPool| async move {
        let artifact = Uuid::now_v7();
        let id = stopped_at_home(&pool, node, artifact, NEWER).await?;
        assert_eq!(
            evacuate(&pool, id, node, artifact).await?,
            V2StoppedEvacuation::Released
        );
        TestResult::Ok(id)
    };
    let marker = |pool: PgPool, id: SandboxId| async move {
        let marker: Option<String> = sqlx::query_scalar(sql!(
            "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(&pool)
        .await?;
        TestResult::Ok(marker)
    };

    let id = released(pool.clone()).await?;
    assert!(mark_unplaced_stopped_for_materialization(pool, id).await?);
    assert_eq!(marker(pool.clone(), id).await?.as_deref(), Some("stopped"));
    assert!(
        !mark_unplaced_stopped_for_materialization(pool, id).await?,
        "marked twice"
    );
    assert!(unmark_unplaced_stopped(pool, id).await?);
    assert_eq!(marker(pool.clone(), id).await?, None);

    for (case, statement) in [
        (
            "placed",
            "UPDATE sandboxes SET active_node_id = gen_random_uuid() WHERE id = $1",
        ),
        (
            "an active execution",
            "UPDATE sandbox_k8s_executions SET active = TRUE WHERE sandbox_id = $1",
        ),
        (
            "disk not ready",
            "UPDATE snapshots SET state = 'creating' WHERE sandbox_id = $1",
        ),
    ] {
        let id = released(pool.clone()).await?;
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .bind(id)
            .execute(pool)
            .await?;
        assert!(
            !mark_unplaced_stopped_for_materialization(pool, id).await?,
            "{case}"
        );
        assert_eq!(marker(pool.clone(), id).await?, None, "{case}");
    }
    Ok(())
}
