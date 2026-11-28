//! The drained-node terminator's guarded writes against Postgres: each
//! guard refuses and writes nothing, and the write goes through once
//! nothing holds the node.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use sqlx::PgPool;

use super::*;

/// A `drained` Kubernetes node registered under uid `uid-<id>`.
async fn drained_node(pool: &PgPool) -> TestDbResult<NodeId> {
    drained_node_managed_by(pool, SubstrateKind::Kubernetes).await
}

/// A `drained` node under `managed_by`, registered under uid `uid-<id>`.
async fn drained_node_managed_by(pool: &PgPool, managed_by: SubstrateKind) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.5'::INET, 'worker', $2, 'registered', 'drained', NOW())"
    ))
    .bind(node_id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!(
        "worker-{}.synthetic.internal",
        node_id.as_uuid().simple()
    ))
    .bind(uid(node_id))
    .execute(pool)
    .await?;
    Ok(node_id)
}

fn uid(node_id: NodeId) -> String {
    format!("uid-{node_id}")
}

/// A sandbox row naming `node_id`: a `started` resident through
/// `active_node_id`, or a `suspended` row with no snapshot at all through
/// `target_node_id`, whose only copy is on the node. Returns its id.
async fn sandbox(pool: &PgPool, node_id: NodeId, resident: bool) -> TestDbResult<String> {
    let id = if resident {
        sqlx::query_scalar(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, placement, managed_by,
                 active_node_id, placement_changed_at, network_policy_generation
             )
             VALUES (space_uuid_v7(), $1, $2, 'started', 'on_node', 'k8s',
                     $3, NOW() - INTERVAL '1 hour', 1)
             RETURNING id::TEXT"
        ))
    } else {
        sqlx::query_scalar(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, placement, managed_by,
                 target_node_id, placement_changed_at, network_policy_generation
             )
             VALUES (space_uuid_v7(), $1, $2, 'suspended', 'unplaced', 'k8s',
                     $3, NOW() - INTERVAL '1 hour', 1)
             RETURNING id::TEXT"
        ))
    }
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

async fn delete_sandbox(pool: &PgPool, id: &str) -> TestDbResult<()> {
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1::UUID"))
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// The node's drain phase; `None` once the row is gone.
async fn phase(pool: &PgPool, node_id: NodeId) -> TestDbResult<Option<String>> {
    Ok(
        sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT drain_phase FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_optional(pool)
        .await?
        .flatten(),
    )
}

/// The flip refuses while the snapshot guard or the resident guard holds
/// the node, and on a v2 drain's row, each leaving the row `drained`; once
/// nothing holds it, the row becomes `terminating`.
#[tokio::test]
async fn mark_terminating_refuses_while_a_guard_holds_the_node() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for residency in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        for resident in [false, true] {
            let case = format!("{residency:?} resident={resident}");
            let node_id = drained_node(pool).await?;
            let id = sandbox(pool, node_id, resident).await?;
            assert!(!mark_terminating(pool, node_id, residency).await?, "{case}");
            assert_eq!(
                phase(pool, node_id).await?.as_deref(),
                Some("drained"),
                "{case}"
            );
            delete_sandbox(pool, &id).await?;
            assert!(mark_terminating(pool, node_id, residency).await?, "{case}");
            assert_eq!(
                phase(pool, node_id).await?.as_deref(),
                Some("terminating"),
                "{case}"
            );
            assert!(
                !mark_terminating(pool, node_id, residency).await?,
                "{case}: no longer drained"
            );
        }
    }

    let v2 = drained_node(pool).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_engine = 'v2',
                          drain_status = '{\"generation\": 1, \"deadline_at\": 0}'::JSONB
          WHERE id = $1"
    ))
    .bind(v2)
    .execute(pool)
    .await?;
    assert!(!mark_terminating(pool, v2, ResidencyEvidence::default()).await?);
    assert_eq!(phase(pool, v2).await?.as_deref(), Some("drained"));
    Ok(())
}

/// The reap refuses while the snapshot guard or the resident guard holds
/// the node, or its uid is not the expected one, each leaving the row in
/// place; once nothing holds it, the row is deleted. A v2 drain's row, a
/// `terminating` row and a Direct row are refused with nothing holding
/// them.
#[tokio::test]
async fn delete_if_orphan_refuses_while_a_guard_holds_the_node() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for residency in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        for resident in [false, true] {
            let case = format!("{residency:?} resident={resident}");
            let node_id = drained_node(pool).await?;
            let expected = uid(node_id);
            let id = sandbox(pool, node_id, resident).await?;
            assert!(
                !delete_if_orphan(pool, node_id, Some(&expected), residency).await?,
                "{case}"
            );
            assert_eq!(
                phase(pool, node_id).await?.as_deref(),
                Some("drained"),
                "{case}"
            );
            delete_sandbox(pool, &id).await?;
            assert!(
                !delete_if_orphan(pool, node_id, Some("uid-replaced"), residency).await?,
                "{case}: the uid moved"
            );
            assert!(
                !delete_if_orphan(pool, node_id, None, residency).await?,
                "{case}: the uid moved"
            );
            assert_eq!(
                phase(pool, node_id).await?.as_deref(),
                Some("drained"),
                "{case}"
            );
            assert!(
                delete_if_orphan(pool, node_id, Some(&expected), residency).await?,
                "{case}"
            );
            assert_eq!(phase(pool, node_id).await?, None, "{case}");
        }
    }

    let v2 = drained_node(pool).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_engine = 'v2',
                          drain_status = '{\"generation\": 1, \"deadline_at\": 0}'::JSONB
          WHERE id = $1"
    ))
    .bind(v2)
    .execute(pool)
    .await?;
    let terminating = drained_node(pool).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'terminating' WHERE id = $1"
    ))
    .bind(terminating)
    .execute(pool)
    .await?;
    let direct = drained_node_managed_by(pool, SubstrateKind::Space).await?;
    for (case, node_id, expected_phase) in [
        ("v2", v2, "drained"),
        ("terminating", terminating, "terminating"),
        ("direct", direct, "drained"),
    ] {
        assert!(
            !delete_if_orphan(
                pool,
                node_id,
                Some(&uid(node_id)),
                ResidencyEvidence::default()
            )
            .await?,
            "{case}"
        );
        assert_eq!(
            phase(pool, node_id).await?.as_deref(),
            Some(expected_phase),
            "{case}"
        );
    }
    Ok(())
}
