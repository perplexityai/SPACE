//! The orphaned-nodes sweep against Postgres: which rows the scan lists,
//! and that the delete refuses each row the guards hold, writing nothing.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use sqlx::PgPool;

use super::*;

/// A Kubernetes node row registered under uid `uid`, under `engine` with no
/// phase.
async fn node(pool: &PgPool, engine: Option<&str>) -> TestDbResult<NodeId> {
    node_managed_by(pool, engine, SubstrateKind::Kubernetes).await
}

/// A node row under `managed_by` registered under uid `uid`, under `engine`
/// with no phase.
async fn node_managed_by(
    pool: &PgPool,
    engine: Option<&str>,
    managed_by: SubstrateKind,
) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_engine, drain_status)
         VALUES ($1, '10.0.0.83', 'worker', $3, 'ready', $2,
                 CASE WHEN $2::TEXT IS NOT NULL
                      THEN '{\"generation\": 1, \"deadline_at\": 0}'::JSONB END)"
    ))
    .bind(node_id)
    .bind(engine)
    .bind(managed_by)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, 'uid')"
    ))
    .bind(node_id)
    .bind(format!(
        "worker-{}.synthetic.internal",
        node_id.as_uuid().simple()
    ))
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A sandbox row in `lifecycle` naming `node_id` through `active_node_id`
/// (`active`) or `target_node_id`.
async fn sandbox(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
    active: bool,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, network_policy_generation
         )
         VALUES (space_uuid_v7(), $1, $2, $3, 'on_node', 'k8s',
                 CASE WHEN $5 THEN $4::UUID END,
                 CASE WHEN $5 THEN NULL ELSE $4::UUID END, 1)"
    ))
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle)
    .bind(node_id)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

async fn exists(pool: &PgPool, node_id: NodeId) -> TestDbResult<bool> {
    Ok(
        sqlx::query_scalar(sql!("SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $1)"))
            .bind(node_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn listed(pool: &PgPool) -> TestDbResult<Vec<NodeId>> {
    Ok(candidates(pool, None, 100)
        .await?
        .into_iter()
        .map(|candidate| candidate.node_id)
        .collect())
}

/// A row under an engine is never an orphan candidate and its DELETE
/// refuses it, with no phase: a v2 drain's cancel pending, or a drain under
/// an engine this build doesn't know. A row with no drain is both.
#[tokio::test]
async fn a_row_under_an_engine_is_never_an_orphan() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    for (engine, orphan) in [(None, true), (Some("v2"), false), (Some("v9"), false)] {
        let node_id = node(&db.pool, engine).await?;
        let candidates = listed(&db.pool).await?;
        assert_eq!(
            candidates.contains(&node_id),
            orphan,
            "{engine:?}: {candidates:?}"
        );
        assert_eq!(
            delete_if_orphan(&db.pool, node_id, Some("uid")).await?,
            orphan,
            "{engine:?}"
        );
        assert_eq!(exists(&db.pool, node_id).await?, !orphan, "{engine:?}");
    }
    Ok(())
}

/// A non-terminal sandbox naming the node, through either column, keeps it
/// off the list and refuses its delete; a `failed` or `deleted` one doesn't.
/// A moved uid refuses too, and so do a v1 drain's row in a phase and a
/// Direct row, neither of which the scan lists. A refused delete leaves the
/// row.
#[tokio::test]
async fn the_delete_refuses_a_referenced_node_or_a_moved_uid() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for active in [true, false] {
        let held = node(pool, None).await?;
        sandbox(pool, held, "suspended", active).await?;
        assert!(!listed(pool).await?.contains(&held), "active={active}");
        assert!(
            !delete_if_orphan(pool, held, Some("uid")).await?,
            "active={active}"
        );
        assert!(exists(pool, held).await?, "active={active}");
    }

    let moved = node(pool, None).await?;
    assert!(!delete_if_orphan(pool, moved, Some("uid-replaced")).await?);
    assert!(!delete_if_orphan(pool, moved, None).await?);
    assert!(exists(pool, moved).await?);

    let draining = node(pool, None).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_started_at = NOW() WHERE id = $1"
    ))
    .bind(draining)
    .execute(pool)
    .await?;
    assert!(!listed(pool).await?.contains(&draining));
    assert!(!delete_if_orphan(pool, draining, Some("uid")).await?);
    assert!(exists(pool, draining).await?);

    let direct = node_managed_by(pool, None, SubstrateKind::Space).await?;
    assert!(!listed(pool).await?.contains(&direct));
    assert!(!delete_if_orphan(pool, direct, Some("uid")).await?);
    assert!(exists(pool, direct).await?);

    let terminal = node(pool, None).await?;
    sandbox(pool, terminal, "failed", true).await?;
    sandbox(pool, terminal, "deleted", false).await?;
    assert!(listed(pool).await?.contains(&terminal));
    assert!(delete_if_orphan(pool, terminal, Some("uid")).await?);
    assert!(!exists(pool, terminal).await?);
    Ok(())
}
