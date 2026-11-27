//! The api's observed-state reads against Postgres, with a Direct node's
//! `node_direct` row seeded.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_net_common::NetworkPolicyCondition;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::{observed_only, observed_states};

const NETWORK_POLICY_FACET: &str = "network-policy";

async fn insert_direct_worker(pool: &PgPool, node_id: NodeId, name: &str) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint)
         VALUES ($1, $2, 'http://10.0.0.1:9800')"
    ))
    .bind(node_id)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

async fn sandbox(pool: &PgPool) -> TestDbResult<SandboxId> {
    insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await
}

/// Records `sandbox_id` as observed on `node_id`, `age_secs` ago.
async fn observe(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    age_secs: i64,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, observed_at)
         VALUES ($1, $2, 'started', NOW() - make_interval(secs => $3))"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(age_secs as f64)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn observed_states_name_the_direct_node_and_its_conditions() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_direct_worker(pool, node_id, "worker-a").await?;
    let asked = sandbox(pool).await?;
    let other = sandbox(pool).await?;
    observe(pool, asked, node_id, 0).await?;
    observe(pool, other, node_id, 0).await?;
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_generation, applied_source,
             applied_authority, observed_at
         )
         VALUES ($1, $2, 'sandbox:' || $3::text, 2, 'template', 'cluster', NOW())"
    ))
    .bind(node_id)
    .bind(NETWORK_POLICY_FACET)
    .bind(asked)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_facet_conditions (
             node_id, facet, element, condition_type, condition_status, reason,
             last_transition_at
         )
         VALUES ($1, $2, 'sandbox:' || $3::text, 'Applied', 'True', 'ok', NOW())"
    ))
    .bind(node_id)
    .bind(NETWORK_POLICY_FACET)
    .bind(asked)
    .execute(pool)
    .await?;

    let rows = observed_states(pool, &[asked], NETWORK_POLICY_FACET).await?;
    let [row] = rows.as_slice() else {
        panic!("one observed state for one id, got {rows:?}");
    };
    assert_eq!(row.sandbox_id, asked);
    assert_eq!(row.node_id, Some(node_id));
    assert_eq!(row.node_name.as_deref(), Some("worker-a"));
    assert_eq!(row.node_role.as_deref(), Some("worker"));
    assert_eq!(row.node_managed_by.as_deref(), Some("space"));
    assert_eq!(
        row.node_api_endpoint.as_deref(),
        Some("http://10.0.0.1:9800")
    );
    assert_eq!(row.substrate_node_id, None);
    assert_eq!(row.status.as_deref(), Some("started"));
    assert!(row.release_tags.is_empty());
    assert!(row.observed_network_policy.is_none());
    assert_eq!(row.observed_network_policy_generation, Some(2));
    assert_eq!(
        row.observed_network_policy_source.as_deref(),
        Some("template")
    );
    assert_eq!(
        row.observed_network_policy_authority.as_deref(),
        Some("cluster")
    );
    assert!(row.observed_network_policy_at.is_some());
    let conditions = &row.network_policy_conditions;
    assert_eq!(conditions.len(), 1);
    assert_eq!(
        NetworkPolicyCondition {
            last_transition_at: None,
            ..conditions[0].clone()
        },
        NetworkPolicyCondition {
            condition_type: "Applied".to_owned(),
            status: "True".to_owned(),
            reason: Some("ok".to_owned()),
            message: None,
            last_transition_at: None,
        }
    );
    assert!(conditions[0].last_transition_at.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn observed_only_lists_rowless_unknown_observations_newest_first() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_direct_worker(pool, node_id, "worker-b").await?;
    let with_row = sandbox(pool).await?;
    let (older, newer, known) = (SandboxId::new(), SandboxId::new(), SandboxId::new());
    observe(pool, with_row, node_id, 0).await?;
    observe(pool, older, node_id, 20).await?;
    observe(pool, newer, node_id, 10).await?;
    observe(pool, known, node_id, 0).await?;

    let rows = observed_only(pool, &[known], NETWORK_POLICY_FACET).await?;
    assert_eq!(
        rows.iter().map(|row| row.sandbox_id).collect::<Vec<_>>(),
        vec![newer, older],
        "a row with a sandboxes row, or a known id, is not observed-only"
    );
    assert!(
        rows.iter()
            .all(|row| row.node_name.as_deref() == Some("worker-b")
                && row.network_policy_conditions.is_empty())
    );
    Ok(())
}
