//! [`prune_cross_org_template_compat`] against Postgres, the allowlist prune
//! the admin visibility flip runs when it narrows a template.

use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::{TestDbResult, try_pg_test_db};
use sqlx::PgPool;
use uuid::Uuid;

use super::prune_cross_org_template_compat;

/// Org 0x1 is seeded by the baseline migration, org 0x3 by the test
/// fixtures' template database.
const ORG_A: Uuid = Uuid::from_u128(0x1);
const ORG_B: Uuid = Uuid::from_u128(0x3);

/// The ids of the one template the test inserts, version 7 for the
/// `templates_id_uuid_v7` CHECK, and of its two groups.
const TEMPLATE_ID: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0001);
const GROUP_A_ID: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_00a0);
const GROUP_B_ID: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_00b0);

async fn insert_template(
    pool: &PgPool,
    name: &str,
    visibility: &str,
    org_id: Option<OrgId>,
) -> TestDbResult<()> {
    sqlx::query("INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)")
        .bind(TEMPLATE_ID)
        .bind(name)
        .bind(org_id)
        .bind(visibility)
        .execute(pool)
        .await?;
    Ok(())
}

/// A narrowing flip prunes the template's allowlist rows from every
/// group not owned by the new owner, and keeps the owner's own rows.
#[tokio::test(flavor = "current_thread")]
async fn narrowing_prunes_cross_org_compat_rows() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    insert_template(pool, "narrowed", "generic", Some(OrgId::from_uuid(ORG_A))).await?;
    let group_a = GROUP_A_ID;
    let group_b = GROUP_B_ID;
    sqlx::query(
        "INSERT INTO node_groups (id, name, org_id, template_policy)
         VALUES ($1, 'org-a-restricted', $2, 'restricted'),
                ($3, 'org-b-restricted', $4, 'restricted')",
    )
    .bind(group_a)
    .bind(ORG_A)
    .bind(group_b)
    .bind(ORG_B)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_group_template_compat (node_group_id, template_name)
         VALUES ($1, 'narrowed'), ($2, 'narrowed')",
    )
    .bind(group_a)
    .bind(group_b)
    .execute(pool)
    .await?;

    let mut tx = pool.begin().await?;
    let pruned =
        prune_cross_org_template_compat(&mut *tx, "narrowed", OrgId::from_uuid(ORG_A)).await?;
    tx.commit().await?;
    assert_eq!(pruned, 1, "only the other org's row is pruned");

    let remaining: Vec<Uuid> = sqlx::query_scalar(
        "SELECT node_group_id FROM node_group_template_compat WHERE template_name = $1",
    )
    .bind("narrowed")
    .fetch_all(pool)
    .await?;
    assert_eq!(remaining, vec![group_a], "the owner's row survives");
    Ok(())
}
