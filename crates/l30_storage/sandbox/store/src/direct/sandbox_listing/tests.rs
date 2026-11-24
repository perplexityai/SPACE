use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;

use super::*;

/// Bytes a row adds to the walk apart from its metadata, env and name. Large
/// beside those, so a budget in rows of it stops where the test says.
const ROW_BYTES: i64 = 1_000;

/// Four sandboxes of the seed org's local user, a minute apart, returned
/// newest first: the list's order.
async fn seed_four(pool: &PgPool) -> TestResult<Vec<SandboxId>> {
    let base = OffsetDateTime::from_unix_timestamp(1_790_000_000)?;
    let mut ids = Vec::new();
    for minute in 0..4 {
        let id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, network_policy_generation,
                 created_at)
             VALUES ($1, $2, $3, 'k8s', 'started', 1, $4)"
        ))
        .bind(id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(base + time::Duration::minutes(minute))
        .execute(pool)
        .await?;
        ids.push(id);
    }
    ids.reverse();
    Ok(ids)
}

async fn walk(
    pool: &PgPool,
    after: Option<(OffsetDateTime, SandboxId)>,
    max_rows: i64,
    max_bytes: i64,
) -> TestResult<Vec<(SandboxId, bool)>> {
    let rows = local_org_page(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        None,
        &PageWalk {
            after,
            max_rows,
            max_bytes,
            row_overhead_bytes: ROW_BYTES,
        },
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.sandbox.id, row.has_next))
        .collect())
}

/// The walk keeps rows while the running size is within the budget, always
/// the first, and says whether a candidate follows the last row it kept,
/// whether the budget or the row bound stopped it.
#[tokio::test(flavor = "current_thread")]
async fn a_page_stops_at_its_byte_budget_and_says_whether_more_follow() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let ids = seed_four(pool).await?;
    let unbounded = i64::MAX / 2;

    // Two and a half rows of budget: the third row passes it, so the page
    // keeps two and the second is followed.
    let page = walk(pool, None, 5, ROW_BYTES * 5 / 2).await?;
    assert_eq!(page, vec![(ids[0], true), (ids[1], true)]);

    // Less than a row: the first row is kept so the cursor advances.
    let page = walk(pool, None, 5, 1).await?;
    assert_eq!(page, vec![(ids[0], true)]);

    // The row bound's lookahead: three candidates walked, none after them.
    let page = walk(pool, None, 3, unbounded).await?;
    assert_eq!(page, vec![(ids[0], true), (ids[1], true), (ids[2], false)]);

    // After the second row, the rest of the list, with nothing to follow.
    let second = sqlx::query_scalar::<_, OffsetDateTime>(sql!(
        "SELECT created_at FROM sandboxes WHERE id = $1"
    ))
    .bind(ids[1])
    .fetch_one(pool)
    .await?;
    let page = walk(pool, Some((second, ids[1])), 5, unbounded).await?;
    assert_eq!(page, vec![(ids[2], true), (ids[3], false)]);

    // The page rows are the response rows `by_id` reads.
    let one = by_id(pool, ids[2]).await?.ok_or("seeded sandbox")?;
    let paged = local_org_page(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        None,
        &PageWalk {
            after: Some((second, ids[1])),
            max_rows: 1,
            max_bytes: unbounded,
            row_overhead_bytes: ROW_BYTES,
        },
    )
    .await?;
    assert_eq!(paged.len(), 1);
    assert_eq!(paged[0].sandbox.id, one.id);
    assert_eq!(paged[0].sandbox.created_at, one.created_at);
    assert_eq!(paged[0].sandbox.owner_source, one.owner_source);
    db.pool.close().await;
    Ok(())
}
