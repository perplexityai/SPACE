//! [`update_visibility`] and [`lock_visibility`] against Postgres: the admin
//! visibility flip's write, and the locked read it takes before writing.

use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_template_model::TemplateVisibility;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{lock_visibility, update_visibility};

/// Orgs seeded by the baseline migration's fixture data.
const ORG_A: Uuid = Uuid::from_u128(0x1);

/// The id of the one template the test inserts, version 7 for the
/// `templates_id_uuid_v7` CHECK.
const TEMPLATE_ID: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0001);

async fn insert_template(
    pool: &PgPool,
    name: &str,
    visibility: &str,
    org_id: Option<OrgId>,
) -> TestResult {
    sqlx::query("INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)")
        .bind(TEMPLATE_ID)
        .bind(name)
        .bind(org_id)
        .bind(visibility)
        .execute(pool)
        .await?;
    Ok(())
}

/// The visibility write assigns an owner in the same statement when one
/// is supplied and otherwise leaves ownership untouched; the
/// `org_specific_requires_owner` CHECK backstops ownerless narrows.
#[tokio::test(flavor = "current_thread")]
async fn visibility_write_and_owner_check() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    insert_template(pool, "ownerless", "generic", None).await?;

    // Ownerless narrow without an owner in the same write → CHECK.
    let mut tx = pool.begin().await?;
    let err = update_visibility(&mut *tx, "ownerless", TemplateVisibility::OrgSpecific, None)
        .await
        .unwrap_err();
    drop(tx);
    let constraint = match &err {
        sqlx::Error::Database(db) => db.constraint().map(str::to_string),
        _ => None,
    };
    assert_eq!(constraint.as_deref(), Some("org_specific_requires_owner"));

    // Narrow with the owner supplied in the same write → row carries
    // both the new visibility and the owner.
    let mut tx = pool.begin().await?;
    update_visibility(
        &mut *tx,
        "ownerless",
        TemplateVisibility::OrgSpecific,
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    let row = lock_visibility(&mut tx, "ownerless").await?.expect("row");
    tx.commit().await?;
    assert_eq!(row.visibility, TemplateVisibility::OrgSpecific);
    assert_eq!(row.org_id, Some(OrgId::from_uuid(ORG_A)));

    // Widening back leaves the owner in place (org_id is don't-care for
    // generic templates).
    let mut tx = pool.begin().await?;
    update_visibility(&mut *tx, "ownerless", TemplateVisibility::Generic, None).await?;
    let row = lock_visibility(&mut tx, "ownerless").await?.expect("row");
    tx.commit().await?;
    assert_eq!(row.visibility, TemplateVisibility::Generic);
    assert_eq!(row.org_id, Some(OrgId::from_uuid(ORG_A)));
    Ok(())
}
