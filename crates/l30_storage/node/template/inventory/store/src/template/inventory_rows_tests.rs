//! [`template_inventory_rows`] against Postgres, the count behind the admin
//! visibility flip's narrowing guard.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_db_test_fixtures::{TestDbResult, try_pg_test_db};
use sqlx::PgPool;
use uuid::Uuid;

use super::template_inventory_rows;

async fn insert_template(
    pool: &PgPool,
    name: &str,
    visibility: &str,
    org_id: Option<OrgId>,
) -> TestDbResult<()> {
    sqlx::query("INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)")
        .bind(Uuid::now_v7())
        .bind(name)
        .bind(org_id)
        .bind(visibility)
        .execute(pool)
        .await?;
    Ok(())
}

/// The narrowing guard sees any cached rootfs row.
#[tokio::test(flavor = "current_thread")]
async fn cached_rootfs_counts_inventory_rows() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    insert_template(pool, "cached", "generic", None).await?;
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'ready')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO node_template_inventory (node_id, template_name) VALUES ($1, $2)")
        .bind(node_id)
        .bind("cached")
        .execute(pool)
        .await?;

    let mut tx = pool.begin().await?;
    assert_eq!(template_inventory_rows(&mut *tx, "cached").await?, 1);
    assert_eq!(template_inventory_rows(&mut *tx, "uncached").await?, 0);
    tx.commit().await?;
    Ok(())
}
