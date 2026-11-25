//! [`template_sandboxes_in_use`] against Postgres, the count behind the admin
//! visibility flip's in-use guard, which both flip directions run.

use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::template_sandboxes_in_use;

/// Orgs seeded by the baseline migration's fixture data.
const ORG_A: Uuid = Uuid::from_u128(0x1);
const SEEDED_USER: Uuid = Uuid::from_u128(0x10);

async fn insert_template(
    pool: &PgPool,
    name: &str,
    visibility: &str,
    org_id: Option<OrgId>,
) -> TestResult {
    sqlx::query("INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)")
        .bind(Uuid::now_v7())
        .bind(name)
        .bind(org_id)
        .bind(visibility)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_sandbox(pool: &PgPool, template: &str, lifecycle: &str) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             template_id, network_policy_generation
         )
         VALUES ($1, $2, $3, $4, 'unplaced', 'k8s', $5, 1)",
    )
    .bind(Uuid::now_v7())
    .bind(SEEDED_USER)
    .bind(ORG_A)
    .bind(lifecycle)
    .bind(template)
    .execute(pool)
    .await?;
    Ok(())
}

/// The in-use guard counts live and restorable rows: `started`,
/// `suspended` (restorable), and `failed` rows that carry a durable,
/// repair-eligible suspend snapshot (the failed-restorable repair loop can
/// revert those to `suspended`). Rows that are genuinely gone —
/// `deleted`, and a `failed` row with no restorable
/// snapshot — do not count. The `failed` rows seeded here have no snapshot,
/// so this case pins the exclusion of unrecoverable-failed; the positive
/// failed+snapshot path is covered by the shared
/// `failed_repair_revert_eligibility` predicate's own unit tests, seeding a
/// durable snapshot fixture at this layer being a heavier follow-up.
#[tokio::test(flavor = "current_thread")]
async fn in_use_counts_live_and_restorable_only() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    insert_template(pool, "flip-target", "generic", None).await?;
    insert_sandbox(pool, "flip-target", "started").await?;
    insert_sandbox(pool, "flip-target", "suspended").await?;
    // `failed` with no restorable snapshot → not recoverable → excluded.
    insert_sandbox(pool, "flip-target", "failed").await?;
    insert_sandbox(pool, "flip-target", "deleted").await?;

    let mut tx = pool.begin().await?;
    assert_eq!(template_sandboxes_in_use(&mut *tx, "flip-target").await?, 2);
    assert_eq!(template_sandboxes_in_use(&mut *tx, "other").await?, 0);
    tx.commit().await?;
    Ok(())
}
