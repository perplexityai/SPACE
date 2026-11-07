use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::collections::BTreeMap;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.5'::INET, 'worker', 'k8s')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

fn report<'a>(node_id: NodeId, verdict: &'a str, lost: u64) -> DeadNodeReportWrite<'a> {
    DeadNodeReportWrite {
        node_id,
        verdict,
        reason: "node_deleted",
        rows_truncated: false,
        counts: BTreeMap::from([("lost", lost)]),
        samples: BTreeMap::from([(
            "lost",
            vec![DeadNodeSample {
                sandbox_id: SandboxId::new(),
                lifecycle: "started".to_owned(),
            }],
        )]),
    }
}

/// One report per node: a later write replaces every column, a deleted node
/// takes its report with it, and a node that no longer exists gets none.
#[tokio::test(flavor = "current_thread")]
async fn one_report_per_node_lives_and_dies_with_the_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let kept = insert_node(pool).await?;
    let deleted = insert_node(pool).await?;

    upsert_report(pool, &report(kept, "unknown", 1)).await?;
    let first_observed = reports(pool, 10).await?[0].observed_at;
    let latest = DeadNodeReportWrite {
        reason: "instance_and_node_gone",
        rows_truncated: true,
        ..report(kept, "lost", 2)
    };
    upsert_report(pool, &latest).await?;
    upsert_report(pool, &report(deleted, "lost", 3)).await?;
    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
        .bind(deleted)
        .execute(pool)
        .await?;
    upsert_report(pool, &report(NodeId::new(), "lost", 4))
        .await
        .expect_err("a report for a missing node is refused");

    let stored = reports(pool, 10).await?;
    let summary: Vec<_> = stored
        .iter()
        .map(|r| {
            (
                r.node_id,
                r.verdict.as_str(),
                r.counts.clone(),
                r.samples.clone(),
            )
        })
        .collect();
    let expected_counts: BTreeMap<String, u64> = BTreeMap::from([("lost".to_owned(), 2)]);
    let expected_samples: BTreeMap<String, Vec<DeadNodeSample>> = latest
        .samples
        .iter()
        .map(|(outcome, rows)| ((*outcome).to_owned(), rows.clone()))
        .collect();
    assert_eq!(
        summary,
        vec![(kept, "lost", expected_counts, expected_samples)]
    );
    assert_eq!(
        (stored[0].reason.as_str(), stored[0].rows_truncated),
        ("instance_and_node_gone", true)
    );
    assert!(
        stored[0].observed_at > first_observed,
        "{} then {first_observed}",
        stored[0].observed_at
    );
    Ok(())
}

/// The row read stops at its limit, in id order.
#[tokio::test(flavor = "current_thread")]
async fn rows_on_node_stops_at_its_limit_in_id_order() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = insert_node(pool).await?;
    let mut ids = Vec::new();
    for _ in 0..3 {
        let id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation)
             VALUES ($1, $2::UUID, $3::UUID, $4, 'started', 'on_node', $5, $5, 1)"
        ))
        .bind(id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(SubstrateKind::Kubernetes)
        .bind(node_id)
        .execute(pool)
        .await?;
        ids.push(id);
    }
    ids.sort();
    let read: Vec<SandboxId> = rows_on_node(
        pool,
        node_id,
        None,
        SubstrateKind::Kubernetes,
        crate::lost::DEAD_UPLOAD_SETTLE,
        2,
    )
    .await?
    .into_iter()
    .map(|row| row.sandbox_id)
    .collect();
    assert_eq!(read, ids[..2]);
    Ok(())
}
