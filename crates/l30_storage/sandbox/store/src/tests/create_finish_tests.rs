//! Characterization of the statements that end a create:
//! [`create::delete_failed_create`] removes the row and its cascading
//! dependents whatever the lifecycle, [`create::record_created_placement`]
//! writes the node and `placement_changed_at` only while the row is in a
//! lifecycle the create owns, [`create::finish_created`] records a Kubernetes
//! create's node and lifecycle only while its execution is still the active
//! one, under the sandbox row lock, and [`create::finalize_created`] records
//! `command_ready_pending` as the inverse of the reported command readiness on
//! a `creating` row. A superseded create's marker clears only on the active
//! execution's activation receipt.

use super::*;
use crate::create;
use crate::test_support::stored_lifecycle_matrix;

/// `active_node_id`, `placement_changed_at IS NOT NULL`, `lifecycle`,
/// `placement` and `command_ready_pending`, as stored.
type Placement = (Option<NodeId>, bool, String, String, bool);

async fn read_placement(pool: &PgPool, id: Uuid) -> TestResult<Placement> {
    Ok(sqlx::query_as::<_, Placement>(sql!(
        "SELECT active_node_id, placement_changed_at IS NOT NULL, lifecycle, placement,
                command_ready_pending
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn row_exists(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let (exists,) = sqlx::query_as::<_, (bool,)>(sql!(
        "SELECT EXISTS (SELECT 1 FROM sandboxes WHERE id = $1)"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

async fn dependent_exists(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let (exists,) = sqlx::query_as::<_, (bool,)>(sql!(
        "SELECT EXISTS (SELECT 1 FROM sandbox_ecw_credential_epochs WHERE sandbox_id = $1)"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

#[tokio::test(flavor = "current_thread")]
async fn delete_failed_create_removes_the_row_whatever_its_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let unrelated = insert_sandbox(&db.pool, None, "creating").await?;

    for lifecycle in ["creating", "restoring", "started", "failed", "deleting"] {
        let id = insert_sandbox(&db.pool, None, lifecycle).await?;
        // A dependent whose foreign key to `sandboxes` cascades on delete.
        sqlx::query(sql!(
            "INSERT INTO sandbox_ecw_credential_epochs (sandbox_id) VALUES ($1)"
        ))
        .bind(id)
        .execute(&db.pool)
        .await?;

        create::delete_failed_create(&db.pool, SandboxId::from_uuid(id)).await?;

        assert!(!row_exists(&db.pool, id).await?, "{lifecycle} row deleted");
        assert!(
            !dependent_exists(&db.pool, id).await?,
            "{lifecycle} row's cascading dependent deleted"
        );
    }

    assert!(
        row_exists(&db.pool, unrelated).await?,
        "only the named row is deleted"
    );
    // An id with no row is not an error.
    create::delete_failed_create(&db.pool, SandboxId::new()).await?;
    Ok(())
}

/// A create records its placement only while its row is still in a
/// lifecycle the create owns: the `creating` row it inserted, or the
/// `started` row an activation-completion receipt converged while it waited
/// for readiness. A row deleted, failed, quarantined, suspended or in any
/// other lifecycle meanwhile keeps its node. `A`: recorded; `.`: refused, row
/// unchanged.
#[tokio::test(flavor = "current_thread")]
async fn record_created_placement_records_only_while_the_create_owns_the_row() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              placement
        "created            .",
        "creating           A",
        "starting           .",
        "started            A",
        "restoring          .",
        "pausing            .",
        "paused             .",
        "resuming           .",
        "suspending         .",
        "suspended          .",
        "stopping           .",
        "stopped            .",
        "deleting           .",
        "checkpointing      .",
        "quarantined        .",
        "failed             .",
        "deleted            .",
        "bogus              .",
        "_Suspended_        .",
    ];
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let mut mismatches = Vec::new();
    for (stored, [(_, expect)]) in stored_lifecycle_matrix(["placement"], EXPECTED) {
        let id = insert_sandbox(&db.pool, None, stored).await?;
        let node = NodeId::new();

        let recorded =
            create::record_created_placement(&db.pool, SandboxId::from_uuid(id), node).await?;

        let (active_node_id, changed_at_set, stored_after, placement, _) =
            read_placement(&db.pool, id).await?;
        let applied = active_node_id == Some(node) && changed_at_set;
        let untouched = active_node_id.is_none() && !changed_at_set;
        if recorded != expect.applies()
            || (expect.applies() && !applied)
            || (!expect.applies() && !untouched)
            || stored_after != stored
            || placement != "unplaced"
        {
            mismatches.push(format!(
                "{stored:?}: expected {expect:?}, got recorded={recorded} node={active_node_id:?} \
                 changed_at={changed_at_set} lifecycle={stored_after:?} placement={placement:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    Ok(())
}

/// `active_node_id`, `placement_changed_at` (as text, compared exactly),
/// `lifecycle`, `placement` and `command_ready_pending`, as stored.
type StoredFinish = (Option<NodeId>, Option<String>, String, String, bool);

async fn read_finish(pool: &PgPool, id: Uuid) -> TestResult<StoredFinish> {
    Ok(sqlx::query_as::<_, StoredFinish>(sql!(
        "SELECT active_node_id, placement_changed_at::TEXT, lifecycle, placement,
                command_ready_pending
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Place the row as an execution's own placement writer does: on `node`, at
/// a fixed `placement_changed_at` a later write would move.
async fn place_row(pool: &PgPool, id: Uuid, node: NodeId) -> TestResult<StoredFinish> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2,
                target_node_id = $2,
                placement = 'on_node',
                placement_changed_at = TIMESTAMPTZ '2026-01-02 03:04:05+00'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(node)
    .execute(pool)
    .await?;
    read_finish(pool, id).await
}

/// A Kubernetes create that reported `node` for generation 2, ready.
fn created_on(
    id: Uuid,
    node: NodeId,
    generation_reported: Option<i64>,
) -> create::CreatedOutcome<'static> {
    create::CreatedOutcome {
        sandbox_id: SandboxId::from_uuid(id),
        lifecycle: "started",
        placement: "on_node",
        active_node_id: Some(node),
        command_ready: true,
        restore_operation_id: None,
        execution_generation: generation_reported.map(generation),
    }
}

async fn finish(
    pool: &PgPool,
    outcome: create::CreatedOutcome<'_>,
) -> TestResult<create::CreatedFinish> {
    let mut tx = pool.begin().await?;
    let finish = create::finish_created(&mut tx, outcome).await?;
    tx.commit().await?;
    Ok(finish)
}

/// A create records its node and lifecycle only while the Kubernetes
/// execution it started is still the sandbox's active one. When another
/// execution replaced it, on a row still `creating` or already `started`, the
/// create is superseded: the successor's node and placement stay, a
/// `creating` row moves to `started` with `command_ready_pending` set, and a
/// `started` row is not written at all. A row in a lifecycle the create does
/// not own loses, whatever runs. A Direct create, which reports no
/// generation, is finalized by its lifecycle alone.
#[tokio::test(flavor = "current_thread")]
async fn finish_created_records_only_the_creates_own_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let create_node = NodeId::new();
    let successor_node = NodeId::new();

    // The create's own execution is active: its node and lifecycle land.
    let id = insert_sandbox(&db.pool, None, "creating").await?;
    insert_k8s_execution(&db.pool, id, 2, true).await?;
    assert_eq!(
        finish(&db.pool, created_on(id, create_node, Some(2))).await?,
        create::CreatedFinish::Finalized
    );
    let (node, changed_at, lifecycle, placement, pending) = read_finish(&db.pool, id).await?;
    assert_eq!(
        (
            node,
            changed_at.is_some(),
            lifecycle.as_str(),
            placement.as_str(),
            pending
        ),
        (Some(create_node), true, "started", "on_node", false)
    );

    // A receipt converged the row to `started` on the create's node, then a
    // started recovery placed generation 3 on another node: superseded, and
    // the row is exactly as the recovery left it.
    let id = insert_sandbox(&db.pool, None, "started").await?;
    insert_k8s_execution(&db.pool, id, 2, false).await?;
    insert_k8s_execution(&db.pool, id, 3, true).await?;
    let recovered = place_row(&db.pool, id, successor_node).await?;
    assert_eq!(
        finish(&db.pool, created_on(id, create_node, Some(2))).await?,
        create::CreatedFinish::Superseded {
            active_generation: Some(generation(3))
        }
    );
    assert_eq!(read_finish(&db.pool, id).await?, recovered);

    // Still `creating` when the successor landed: its node stays, and the
    // row is published `started`, pending command readiness.
    let id = insert_sandbox(&db.pool, None, "creating").await?;
    insert_k8s_execution(&db.pool, id, 2, false).await?;
    insert_k8s_execution(&db.pool, id, 3, true).await?;
    let (_, placed_at, ..) = place_row(&db.pool, id, successor_node).await?;
    assert_eq!(
        finish(&db.pool, created_on(id, create_node, Some(2))).await?,
        create::CreatedFinish::Superseded {
            active_generation: Some(generation(3))
        }
    );
    assert_eq!(
        read_finish(&db.pool, id).await?,
        (
            Some(successor_node),
            placed_at,
            "started".to_owned(),
            "on_node".to_owned(),
            true
        )
    );

    // Retired with nothing active yet: no node is written.
    let id = insert_sandbox(&db.pool, None, "started").await?;
    insert_k8s_execution(&db.pool, id, 2, false).await?;
    let before = read_finish(&db.pool, id).await?;
    assert_eq!(
        finish(&db.pool, created_on(id, create_node, Some(2))).await?,
        create::CreatedFinish::Superseded {
            active_generation: None
        }
    );
    assert_eq!(read_finish(&db.pool, id).await?, before);

    // Failed meanwhile: lost, whatever execution is active.
    let id = insert_sandbox(&db.pool, None, "failed").await?;
    insert_k8s_execution(&db.pool, id, 2, false).await?;
    insert_k8s_execution(&db.pool, id, 3, true).await?;
    let before = read_finish(&db.pool, id).await?;
    assert_eq!(
        finish(&db.pool, created_on(id, create_node, Some(2))).await?,
        create::CreatedFinish::Lost
    );
    assert_eq!(read_finish(&db.pool, id).await?, before);

    // Direct: no generation, finalized by lifecycle alone.
    let id = insert_sandbox(&db.pool, None, "creating").await?;
    assert_eq!(
        finish(&db.pool, created_on(id, create_node, None)).await?,
        create::CreatedFinish::Finalized
    );
    let (node, _, lifecycle, ..) = read_finish(&db.pool, id).await?;
    assert_eq!((node, lifecycle.as_str()), (Some(create_node), "started"));
    Ok(())
}

/// Hold an activation attempt for `started` on `sandbox_id`'s execution at
/// `generation`, with a pod IP and route, as the activation that started it
/// left them.
async fn hold_started_attempt(
    pool: &PgPool,
    sandbox_id: Uuid,
    generation: i64,
    attempt: Uuid,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running',
                pod_ip = '10.0.0.3',
                route_activation_id = $3,
                activation_attempt_id = $3,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = 'started'
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(attempt)
    .execute(pool)
    .await?;
    Ok(())
}

/// `node`'s activation-completed receipt for `started`, naming `generation`
/// and `attempt`.
async fn publish_started_receipt(
    pool: &PgPool,
    node: NodeId,
    sandbox_id: Uuid,
    generation: i64,
    attempt: Uuid,
) -> TestResult<Vec<SandboxId>> {
    Ok(crate::ingest::release_completed_activations(
        pool,
        node,
        sqlx::types::Json(serde_json::json!([{
            "sandbox_id": sandbox_id,
            "published_lifecycle": "started",
            "transition_receipt_id": Uuid::now_v7(),
            "transition_receipt_kind": "activation_completed",
            "transition_receipt_target_state": "started",
            "transition_receipt_execution_generation": generation,
            "transition_receipt_activation_attempt_id": attempt,
            "execution_generation": generation,
        }])),
    )
    .await?)
}

/// A superseded create's `command_ready_pending` marker waits for evidence
/// about the successor, as a slow create's does. The activation-completion
/// receipt of the create's own, replaced, execution is not that evidence, so
/// it releases nothing and leaves the marker, even from the row's node; the
/// receipt that completes the active execution's attempt clears it.
#[tokio::test(flavor = "current_thread")]
async fn a_superseded_creates_marker_clears_only_on_the_active_executions_receipt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let create_node = NodeId::new();
    let successor_node = NodeId::new();
    let id = insert_sandbox(&db.pool, None, "creating").await?;
    insert_k8s_execution(&db.pool, id, 2, false).await?;
    insert_k8s_execution(&db.pool, id, 3, true).await?;
    let (create_attempt, successor_attempt) = (Uuid::now_v7(), Uuid::now_v7());
    hold_started_attempt(&db.pool, id, 2, create_attempt).await?;
    hold_started_attempt(&db.pool, id, 3, successor_attempt).await?;
    place_row(&db.pool, id, successor_node).await?;
    assert_eq!(
        finish(&db.pool, created_on(id, create_node, Some(2))).await?,
        create::CreatedFinish::Superseded {
            active_generation: Some(generation(3))
        }
    );
    let (.., lifecycle, _, pending) = read_finish(&db.pool, id).await?;
    assert_eq!((lifecycle.as_str(), pending), ("started", true));

    for node in [create_node, successor_node] {
        assert!(
            publish_started_receipt(&db.pool, node, id, 2, create_attempt)
                .await?
                .is_empty()
        );
        let (.., pending) = read_finish(&db.pool, id).await?;
        assert!(pending, "the replaced execution's receipt from {node}");
    }

    assert_eq!(
        publish_started_receipt(&db.pool, successor_node, id, 3, successor_attempt).await?,
        vec![SandboxId::from_uuid(id)]
    );
    let (node, _, lifecycle, placement, pending) = read_finish(&db.pool, id).await?;
    assert_eq!(
        (node, lifecycle.as_str(), placement.as_str(), pending),
        (Some(successor_node), "started", "on_node", false)
    );
    Ok(())
}

/// The create's check of its execution waits for the sandbox row lock and
/// reads after it: a replacement that holds the lock while the create's
/// finish waits, and retires the create's execution before it commits, is
/// what the finish sees. The create is superseded and leaves the successor's
/// node, though its execution was still active when the finish began.
#[tokio::test(flavor = "current_thread")]
async fn finish_created_sees_a_replacement_that_held_the_row_lock() -> TestResult {
    // The replacement's connection, the finish's, and one to read the
    // sandbox afterwards.
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(3).await?
    else {
        return Ok(());
    };
    let create_node = NodeId::new();
    let successor_node = NodeId::new();
    let id = insert_sandbox(&db.pool, None, "started").await?;
    insert_k8s_execution(&db.pool, id, 2, true).await?;

    // The replacement, as a started recovery's handoff runs it: the sandbox
    // row first, then the execution rows and the successor's placement.
    let mut replacement = db.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(id)
        .execute(&mut *replacement)
        .await?;

    let pool = db.pool.clone();
    let finishing = tokio::spawn(async move {
        finish(&pool, created_on(id, create_node, Some(2)))
            .await
            .map_err(|error| error.to_string())
    });
    // Wait until the finish is blocked on the row lock: a session the
    // replacement's own blocks.
    let mut blocked = false;
    for _ in 0..200 {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE pg_backend_pid() = ANY (pg_blocking_pids(pid))
             )
               FROM pg_stat_clear_snapshot()"
        ))
        .fetch_one(&mut *replacement)
        .await?;
        if waiting {
            blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(blocked, "the finish waits for the sandbox row lock");

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'replaced', terminated_at = NOW()
          WHERE sandbox_id = $1 AND generation = 2"
    ))
    .bind(id)
    .execute(&mut *replacement)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 3, $2, 'space-test', 'recovering', TRUE)"
    ))
    .bind(id)
    .bind(format!("space-sbx-{}-g3", id.simple()))
    .execute(&mut *replacement)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, target_node_id = $2, placement = 'on_node'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(successor_node)
    .execute(&mut *replacement)
    .await?;
    replacement.commit().await?;

    let finished = finishing.await??;
    assert_eq!(
        finished,
        create::CreatedFinish::Superseded {
            active_generation: Some(generation(3))
        }
    );
    let (node, ..) = read_finish(&db.pool, id).await?;
    assert_eq!(node, Some(successor_node), "the successor's node stays");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_created_records_command_readiness_on_a_creating_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    for command_ready in [false, true] {
        let id = insert_sandbox(&db.pool, None, "creating").await?;
        let node = NodeId::new();

        let matched = create::finalize_created(
            &db.pool,
            SandboxId::from_uuid(id),
            "started",
            "on_node",
            Some(node),
            command_ready,
            None,
        )
        .await?;

        assert!(
            matched,
            "command_ready = {command_ready}: a creating row matches"
        );
        let (active_node_id, _, lifecycle, placement, command_ready_pending) =
            read_placement(&db.pool, id).await?;
        assert_eq!(
            (active_node_id, lifecycle.as_str(), placement.as_str()),
            (Some(node), "started", "on_node"),
            "command_ready = {command_ready}: the reported state"
        );
        assert_eq!(
            command_ready_pending, !command_ready,
            "command_ready = {command_ready}: pending is its inverse"
        );
    }
    Ok(())
}
