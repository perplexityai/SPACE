//! The Direct-aware inventory reads against Postgres.

use std::collections::HashSet;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;
use sqlx::PgPool;
use uuid::Uuid;

use super::template_holder_ids;

const TEMPLATE: &str = "held-template";

/// A node holding `template`, with a `node_direct` row when `direct` is
/// `Some(deleted)`.
async fn holder(
    pool: &PgPool,
    managed_by: &str,
    template: &str,
    direct: Option<bool>,
) -> TestDbResult<NodeId> {
    let id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'worker', $2, 'registered')"
    ))
    .bind(id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    if let Some(deleted) = direct {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, 'http://holder:7070', CASE WHEN $2 THEN NOW() END)"
        ))
        .bind(id)
        .bind(deleted)
        .execute(pool)
        .await?;
    }
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .bind(template)
    .execute(pool)
    .await?;
    for content_hash in ["sha256:aaaa", "sha256:bbbb"] {
        sqlx::query(sql!(
            "INSERT INTO node_template_inventory (node_id, template_name, content_hash)
             VALUES ($1, $2, $3)"
        ))
        .bind(id)
        .bind(template)
        .bind(content_hash)
        .execute(pool)
        .await?;
    }
    Ok(id)
}

#[tokio::test(flavor = "current_thread")]
async fn template_holders_skip_deleted_or_unregistered_direct_nodes() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let k8s = holder(pool, "k8s", TEMPLATE, None).await?;
    let live_direct = holder(pool, "space", TEMPLATE, Some(false)).await?;
    holder(pool, "space", TEMPLATE, Some(true)).await?;
    holder(pool, "space", TEMPLATE, None).await?;
    holder(pool, "k8s", "other-template", None).await?;

    let template = TemplateName::parse(TEMPLATE)?;
    let holders = template_holder_ids(pool, &template, SubstrateKind::Space).await?;
    assert_eq!(holders.len(), 2, "each node once: {holders:?}");
    assert_eq!(
        holders.into_iter().collect::<HashSet<_>>(),
        HashSet::from([k8s, live_direct])
    );
    Ok(())
}
