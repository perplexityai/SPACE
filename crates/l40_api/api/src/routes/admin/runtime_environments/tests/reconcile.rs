//! A committed policy write queues a `runtime-provisioning` reconcile on
//! every worker and builder node in its own transaction, due at once even
//! where a ref is backing off; a refused write, or one whose enqueue fails,
//! queues nothing.

use super::*;

/// Seeds three Kubernetes workers, two builders and a Direct worker with a
/// live registration, which a write reconciles, plus a cluster-role node and
/// Direct workers with a deleted registration and with none, which it
/// doesn't. Returns the ones it reconciles, sorted.
async fn seed_nodes(pool: &PgPool) -> TestResult<Vec<NodeId>> {
    // The Direct registration: `None` for no `node_direct` row, else whether
    // it's live.
    let mut targets = Vec::new();
    for (index, (role, managed_by, registration, target)) in [
        (NodeRole::Worker, "k8s", None, true),
        (NodeRole::Worker, "k8s", None, true),
        (NodeRole::Worker, "k8s", None, true),
        (NodeRole::Builder, "k8s", None, true),
        (NodeRole::Builder, "k8s", None, true),
        (NodeRole::Cluster, "k8s", None, false),
        (NodeRole::Worker, "space", Some(true), true),
        (NodeRole::Worker, "space", Some(false), false),
        (NodeRole::Worker, "space", None, false),
    ]
    .into_iter()
    .enumerate()
    {
        let node_id = NodeId::new();
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.90'::INET, $2, $3, 'ready')"
        ))
        .bind(node_id)
        .bind(role.as_str())
        .bind(managed_by)
        .execute(pool)
        .await?;
        if let Some(live) = registration {
            sqlx::query(sql!(
                "INSERT INTO node_direct (node_id, name, node_api_endpoint, deleted_at)
                 VALUES ($1, $2, 'http://127.0.0.1:1', CASE WHEN $3 THEN NULL ELSE NOW() END)"
            ))
            .bind(node_id)
            .bind(format!("direct-{index}"))
            .bind(live)
            .execute(pool)
            .await?;
        }
        if target {
            targets.push(node_id);
        }
    }
    targets.sort();
    Ok(targets)
}

/// Every queued trigger's node and reasons, by node: the whole table, which
/// the crate's `queued_triggers` (by node id) can't show.
async fn all_queued_triggers(pool: &PgPool) -> TestResult<Vec<(NodeId, Vec<String>)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT node_id, reasons::TEXT[] FROM node_reconcile_triggers ORDER BY node_id"
    ))
    .fetch_all(pool)
    .await?)
}

/// Every queued ref's node, facet and element, by node.
async fn all_queued_refs(pool: &PgPool) -> TestResult<Vec<(NodeId, String, String)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT node_id, facet, element FROM node_reconcile_queue ORDER BY node_id, facet, element"
    ))
    .fetch_all(pool)
    .await?)
}

/// Empties both queues, as a drained controller and node-ctl would leave them.
async fn drain_queues(pool: &PgPool) -> TestResult {
    sqlx::query(sql!("DELETE FROM node_reconcile_triggers"))
        .execute(pool)
        .await?;
    sqlx::query(sql!("DELETE FROM node_reconcile_queue"))
        .execute(pool)
        .await?;
    Ok(())
}

async fn assert_nothing_queued(pool: &PgPool) -> TestResult {
    assert_eq!(all_queued_triggers(pool).await?, []);
    assert_eq!(all_queued_refs(pool).await?, []);
    Ok(())
}

/// One `node_updated` trigger and one whole-facet `runtime-provisioning` ref
/// on each target, and nothing anywhere else.
async fn assert_each_target_queued(pool: &PgPool, targets: &[NodeId]) -> TestResult {
    assert_eq!(
        all_queued_triggers(pool).await?,
        targets
            .iter()
            .map(|node_id| (*node_id, vec!["node_updated".to_owned()]))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        all_queued_refs(pool).await?,
        targets
            .iter()
            .map(|node_id| (*node_id, "runtime-provisioning".to_owned(), String::new()))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn each_committed_write_reconciles_every_worker_and_builder() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let targets = seed_nodes(&h.db.pool).await?;
    let (e0, e1) = (environment(0), environment(1));

    let (status, body) = h.set_default(e0).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_each_target_queued(&h.db.pool, &targets).await?;

    // With the queues drained, the state write queues the same again.
    drain_queues(&h.db.pool).await?;
    let (status, body) = h
        .set_state(e1, json!({"state": "deprecated", "reason": "superseded"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_each_target_queued(&h.db.pool, &targets).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_write_makes_a_backing_off_ref_due_at_once() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let targets = seed_nodes(&h.db.pool).await?;
    // A ref that failed three times to apply the previous policy and won't
    // be due for an hour.
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue
             (node_id, facet, element, attempts, last_error, next_attempt_at)
         VALUES ($1, 'runtime-provisioning', '', 3, 'synthetic failure',
                 NOW() + INTERVAL '1 hour')"
    ))
    .bind(targets[0])
    .execute(&h.db.pool)
    .await?;

    let (status, body) = h.set_default(environment(0)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_each_target_queued(&h.db.pool, &targets).await?;
    let (attempts, last_error, due): (i32, String, bool) = sqlx::query_as(sql!(
        "SELECT attempts, last_error, next_attempt_at <= NOW()
         FROM node_reconcile_queue
         WHERE node_id = $1 AND facet = 'runtime-provisioning' AND element = ''"
    ))
    .bind(targets[0])
    .fetch_one(&h.db.pool)
    .await?;
    assert_eq!((attempts, last_error.as_str(), due), (0, "", true));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_refused_write_queues_nothing() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    seed_nodes(&h.db.pool).await?;
    let (e0, e1) = (environment(0), environment(1));

    // The store refuses it: no default to block under.
    let (status, body) = h
        .set_state(
            e1,
            json!({"state": "blocked", "reason": "0-day", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_nothing_queued(&h.db.pool).await?;

    // A stale If-Match.
    let (status, body) = h
        .put_if_match(
            "/admin/api/v1/runtime-environments/default",
            json!({"environment_ref": e1.to_string(), "reason": "stale"}),
            "5",
        )
        .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_nothing_queued(&h.db.pool).await?;

    // The strand check: E0 has work and no state, so the first default
    // would stop it.
    let worker = NodeId::new();
    insert_ready_worker_node(&h.db.pool, worker).await?;
    let sandbox = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, sandbox, worker, Some(metadata(Some(e0)))).await?;
    let (status, body) = h.set_default(e1).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "strand: true");
    assert_nothing_queued(&h.db.pool).await?;
    assert_eq!(h.generation().await?, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_refused_state_write_queues_nothing() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    seed_nodes(&h.db.pool).await?;
    let (e0, e1) = (environment(0), environment(1));
    // E1 is the default and E0 is allowed, with a sandbox bound to E0, so
    // blocking E0 needs strand.
    let (status, body) = h.set_default(e1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = h
        .set_state(e0, json!({"state": "allowed", "reason": "in use"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let worker = NodeId::new();
    insert_ready_worker_node(&h.db.pool, worker).await?;
    let sandbox = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, sandbox, worker, Some(metadata(Some(e0)))).await?;
    drain_queues(&h.db.pool).await?;
    let generation = h.generation().await?;

    // The strand check.
    let (status, body) = h
        .set_state(e0, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "strand: true");
    assert_nothing_queued(&h.db.pool).await?;

    // A stale If-Match.
    let (status, body) = h
        .put_if_match(
            &state_path(e0),
            json!({"state": "deprecated", "reason": "stale"}),
            &(generation - 1).to_string(),
        )
        .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_nothing_queued(&h.db.pool).await?;
    assert_eq!(h.generation().await?, generation);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_enqueue_rolls_the_write_back() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let targets = seed_nodes(&h.db.pool).await?;
    // Fail the trigger insert, after the ref insert has run in the same
    // transaction.
    sqlx::query(sql!(
        "CREATE FUNCTION test_refuse_trigger_insert() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             RAISE EXCEPTION 'synthetic enqueue failure';
         END;
         $$"
    ))
    .execute(&h.db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER test_refuse_trigger_insert
         BEFORE INSERT ON node_reconcile_triggers
         FOR EACH ROW EXECUTE FUNCTION test_refuse_trigger_insert()"
    ))
    .execute(&h.db.pool)
    .await?;

    let (status, body) = h.set_default(environment(0)).await;
    assert!(status.is_server_error(), "{status}: {body}");
    assert_eq!(h.generation().await?, 0);
    assert_eq!(
        policy_store::get(&h.db.pool).await?.default_environment,
        None
    );
    assert_nothing_queued(&h.db.pool).await?;

    let (status, body) = h
        .set_state(
            environment(1),
            json!({"state": "deprecated", "reason": "superseded"}),
        )
        .await;
    assert!(status.is_server_error(), "{status}: {body}");
    assert_eq!(h.generation().await?, 0);
    assert_nothing_queued(&h.db.pool).await?;

    // The synthetic failure is what refused them: without it, the same write
    // commits and queues.
    sqlx::query(sql!(
        "DROP TRIGGER test_refuse_trigger_insert ON node_reconcile_triggers"
    ))
    .execute(&h.db.pool)
    .await?;
    let (status, body) = h.set_default(environment(0)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_each_target_queued(&h.db.pool, &targets).await?;
    Ok(())
}
