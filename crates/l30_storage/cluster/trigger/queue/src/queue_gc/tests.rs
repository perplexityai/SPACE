use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const RETENTION_SECS: i64 = 60;
const CLAIM_GRACE_SECS: i64 = 30;
const OLD_SECS: i64 = 120;

/// A Kubernetes worker in `state`, registered with `uid`.
async fn insert_k8s_node(pool: &PgPool, state: &str, uid: &str) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'k8s', $2)"
    ))
    .bind(node_id)
    .bind(state)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("ip-{node_id}.ec2.internal"))
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// One ref and one trigger on `node_id`, requested `age_secs` ago, dead as
/// long when `dead`.
async fn insert_debt(pool: &PgPool, node_id: NodeId, age_secs: i64, dead: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id, facet, element, requested_at, updated_at, dead_at
         ) VALUES (
             $1, 'sandbox-desired-state', $2,
             NOW() - ($3 * INTERVAL '1 second'),
             NOW() - ($3 * INTERVAL '1 second'),
             CASE WHEN $4 THEN NOW() - ($3 * INTERVAL '1 second') ELSE NULL END
         )"
    ))
    .bind(node_id)
    .bind(format!("sandbox:{age_secs}"))
    .bind(age_secs)
    .bind(dead)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_triggers (
             node_id, reason, requested_at, updated_at, dead_at
         ) VALUES (
             $1, 'test',
             NOW() - ($2 * INTERVAL '1 second'),
             NOW() - ($2 * INTERVAL '1 second'),
             CASE WHEN $3 THEN NOW() - ($2 * INTERVAL '1 second') ELSE NULL END
         )"
    ))
    .bind(node_id)
    .bind(age_secs)
    .bind(dead)
    .execute(pool)
    .await?;
    Ok(())
}

/// The node's queued refs and triggers, dead or alive.
async fn debt(pool: &PgPool, node_id: NodeId) -> TestResult<(i64, i64)> {
    Ok(sqlx::query_as(sql!(
        "SELECT (SELECT COUNT(*)::BIGINT FROM node_reconcile_queue WHERE node_id = $1),
                (SELECT COUNT(*)::BIGINT FROM node_reconcile_triggers WHERE node_id = $1)"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn prune_dead_deletes_only_debt_dead_past_retention() -> TestResult {
    let db = pg_test_db().await?;
    let old = insert_k8s_node(&db.pool, "ready", "uid-old").await?;
    insert_debt(&db.pool, old, OLD_SECS, true).await?;
    let recent = insert_k8s_node(&db.pool, "ready", "uid-recent").await?;
    insert_debt(&db.pool, recent, RETENTION_SECS / 2, true).await?;
    let live = insert_k8s_node(&db.pool, "ready", "uid-live").await?;
    insert_debt(&db.pool, live, OLD_SECS, false).await?;

    let pruned = prune_dead(&db.pool, RETENTION_SECS, 100).await?;

    assert_eq!(
        pruned,
        DeadPruneCounts {
            queue: 1,
            triggers: 1,
        }
    );
    assert_eq!(debt(&db.pool, old).await?, (0, 0));
    assert_eq!(debt(&db.pool, recent).await?, (1, 1));
    assert_eq!(debt(&db.pool, live).await?, (1, 1));

    // One limit covers refs and triggers together.
    let shared = insert_k8s_node(&db.pool, "ready", "uid-shared").await?;
    insert_debt(&db.pool, shared, OLD_SECS, true).await?;
    let pruned = prune_dead(&db.pool, RETENTION_SECS, 1).await?;
    assert_eq!(pruned.queue + pruned.triggers, 1);
    let (queue, triggers) = debt(&db.pool, shared).await?;
    assert_eq!(queue + triggers, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn gone_node_debt_is_counted_on_a_dry_run_and_deleted_live() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = insert_k8s_node(&db.pool, "offline", "uid-gone").await?;
    insert_debt(&db.pool, node_id, OLD_SECS, false).await?;
    // A sandbox still placed on the node: the prune leaves its desired state.
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, active_node_id, network_policy_generation
         ) VALUES ($1, $2::UUID, $3::UUID, 'started', $4, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    // A ref requested within retention is neither counted nor deleted.
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (node_id, facet, element, requested_at, updated_at)
         VALUES ($1, 'sandbox-desired-state', 'sandbox:recent', NOW(), NOW())"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let expected = GoneNodeDebt {
        eligible_queue: 1,
        eligible_triggers: 1,
        deleted_queue: 0,
        deleted_triggers: 0,
    };

    let dry_run = prune_gone_node_debt(
        &db.pool,
        node_id,
        Some("uid-gone"),
        RETENTION_SECS,
        100,
        CLAIM_GRACE_SECS,
        false,
    )
    .await?;
    assert_eq!(dry_run, expected);
    assert_eq!(debt(&db.pool, node_id).await?, (2, 1));

    let live = prune_gone_node_debt(
        &db.pool,
        node_id,
        Some("uid-gone"),
        RETENTION_SECS,
        100,
        CLAIM_GRACE_SECS,
        true,
    )
    .await?;
    assert_eq!(
        live,
        GoneNodeDebt {
            deleted_queue: 1,
            deleted_triggers: 1,
            ..expected
        }
    );
    assert_eq!(debt(&db.pool, node_id).await?, (1, 0));
    let node_kept: bool =
        sqlx::query_scalar(sql!("SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $1)"))
            .bind(node_id)
            .fetch_one(&db.pool)
            .await?;
    assert!(node_kept, "the node row is never deleted");
    let desired: (String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(desired, ("started".to_owned(), Some(node_id)));

    // A node holds one trigger, so a trigger requested within retention
    // needs a gone node of its own.
    let recent = insert_k8s_node(&db.pool, "offline", "uid-recent").await?;
    insert_debt(&db.pool, recent, RETENTION_SECS / 2, false).await?;
    let pruned = prune_gone_node_debt(
        &db.pool,
        recent,
        Some("uid-recent"),
        RETENTION_SECS,
        100,
        CLAIM_GRACE_SECS,
        true,
    )
    .await?;
    assert_eq!(pruned, GoneNodeDebt::default());
    assert_eq!(debt(&db.pool, recent).await?, (1, 1));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn gone_node_debt_survives_a_node_that_is_ready_draining_replaced_or_direct() -> TestResult {
    let db = pg_test_db().await?;
    let ready = insert_k8s_node(&db.pool, "ready", "uid-ready").await?;
    let draining = insert_k8s_node(&db.pool, "offline", "uid-draining").await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_started_at = NOW() WHERE id = $1"
    ))
    .bind(draining)
    .execute(&db.pool)
    .await?;
    let replaced = insert_k8s_node(&db.pool, "offline", "uid-new").await?;
    // A Kubernetes registration too, so only the substrate check can exclude
    // it.
    let direct = insert_k8s_node(&db.pool, "offline", "uid-direct").await?;
    sqlx::query(sql!("UPDATE nodes SET managed_by = 'space' WHERE id = $1"))
        .bind(direct)
        .execute(&db.pool)
        .await?;
    for (node_id, expected_uid) in [
        (ready, "uid-ready"),
        (draining, "uid-draining"),
        (replaced, "uid-old"),
        (direct, "uid-direct"),
    ] {
        insert_debt(&db.pool, node_id, OLD_SECS, false).await?;

        let pruned = prune_gone_node_debt(
            &db.pool,
            node_id,
            Some(expected_uid),
            RETENTION_SECS,
            100,
            CLAIM_GRACE_SECS,
            true,
        )
        .await?;

        assert_eq!(pruned, GoneNodeDebt::default(), "{expected_uid}");
        assert_eq!(debt(&db.pool, node_id).await?, (1, 1), "{expected_uid}");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_ref_claim_vetoes_the_whole_gone_node() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = insert_k8s_node(&db.pool, "offline", "uid-claimed").await?;
    insert_debt(&db.pool, node_id, OLD_SECS, false).await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue SET claim_token = 'node-ctl', claimed_at = NOW()
         WHERE node_id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    // Siblings that would each be pruned on their own: one unclaimed, one
    // whose claim is older than the grace period.
    for (element, claim_token, claimed_ago_secs) in [
        ("sandbox:unclaimed", "", None),
        (
            "sandbox:stale-claim",
            "node-ctl",
            Some(CLAIM_GRACE_SECS * 2),
        ),
    ] {
        sqlx::query(sql!(
            "INSERT INTO node_reconcile_queue (
                 node_id, facet, element, requested_at, updated_at, claim_token, claimed_at
             ) VALUES (
                 $1, 'sandbox-desired-state', $2,
                 NOW() - ($3 * INTERVAL '1 second'),
                 NOW() - ($3 * INTERVAL '1 second'),
                 $4,
                 NOW() - ($5 * INTERVAL '1 second')
             )"
        ))
        .bind(node_id)
        .bind(element)
        .bind(OLD_SECS)
        .bind(claim_token)
        .bind(claimed_ago_secs)
        .execute(&db.pool)
        .await?;
    }

    for delete in [false, true] {
        let pruned = prune_gone_node_debt(
            &db.pool,
            node_id,
            Some("uid-claimed"),
            RETENTION_SECS,
            100,
            CLAIM_GRACE_SECS,
            delete,
        )
        .await?;

        assert_eq!(pruned, GoneNodeDebt::default(), "delete: {delete}");
        assert_eq!(debt(&db.pool, node_id).await?, (3, 1), "delete: {delete}");
    }
    Ok(())
}
