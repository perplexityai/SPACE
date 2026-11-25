//! Query-plan bounds for the warm-pool release reads.

use pplx_space_sql_locator::sql;

use super::{TestResult, insert_template, postgres_fixture, upsert_warm_pool_policy};
use crate::store::RETAINED_POLICY_RELEASES;

/// A policy retains its template's newest-published releases, and a
/// release's publish time is its digest's newest tag under either spelling.
/// Matching the spellings by normalizing the tagged column (`replace(tagged.hash,
/// …) = …`) can't use the `(template_name, hash, …)` index, so each release's
/// publish time scans every release of the template, and `ORDER BY … LIMIT`
/// needs every publish time first: one template's release count squared. Node
/// GC reads this on every tick, so it must stay linear.
#[tokio::test(flavor = "current_thread")]
async fn retained_releases_look_up_publish_times_by_index() -> TestResult {
    const RELEASES: i64 = 1_500;
    const RETAIN: i32 = 3;
    let fixture = postgres_fixture().await?;
    let pool = &fixture.pool;
    insert_template(pool, "busy").await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, created_at)
         SELECT gen_random_uuid(), 'busy', 'fc',
                'sha256:' || lpad(to_hex(g), 64, '0'),
                NOW() - make_interval(days => 1, secs => ($1 - g)::double precision)
           FROM generate_series(1, $1) g"
    ))
    .bind(RELEASES)
    .execute(pool)
    .await?;
    // Every release published once, in creation order.
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (
             template_release_id, template_name, tag, created_at, modified_at
         )
         SELECT id, 'busy', 'v' || row_number() OVER (ORDER BY created_at),
                created_at, created_at
           FROM template_releases
          WHERE template_name = 'busy'"
    ))
    .execute(pool)
    .await?;
    upsert_warm_pool_policy(pool, "busy", 1, 2, RETAIN).await?;
    sqlx::query(sql!("ANALYZE template_releases, template_release_tags"))
        .execute(pool)
        .await?;

    let mut protected = fixture
        .store
        .protected_template_releases()
        .await?
        .into_iter()
        .map(|release| release.content_hash)
        .collect::<Vec<_>>();
    protected.sort();
    let newest = (RELEASES - i64::from(RETAIN) + 1..=RELEASES)
        .map(|g| format!("sha256:{g:064x}"))
        .collect::<Vec<_>>();
    assert_eq!(protected, newest);

    let plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {}",
        RETAINED_POLICY_RELEASES.as_str()
    )))
    .bind(0_i64)
    .bind(0_i64)
    .bind(None::<&str>)
    .fetch_all(pool)
    .await?;
    let shown = plan.join("\n");
    let examined = rows_examined(&plan);
    // A plan with no parsed node lines would pass any upper bound.
    assert!(
        (1..RELEASES * 20).contains(&examined),
        "examined {examined} rows for {RELEASES} releases:\n{shown}"
    );
    Ok(())
}

/// Rows an `EXPLAIN (ANALYZE)` text plan's nodes returned or filtered out,
/// over all their loops. The plan reports both per loop, as fractional
/// averages on newer servers.
fn rows_examined(plan: &[String]) -> i64 {
    let count = |text: &str| text.parse::<f64>().expect("plan row count");
    let mut loops = 0.0;
    let mut examined = 0.0;
    for line in plan {
        if let Some((_, actual)) = line.split_once("(actual rows=") {
            let (rows, rest) = actual.split_once(" loops=").expect("loops");
            loops = count(rest.trim_end_matches(')'));
            examined += count(rows) * loops;
        } else if let Some((_, removed)) = line.split_once("Rows Removed by ") {
            let (_, removed) = removed.split_once(": ").expect("removed count");
            examined += count(removed.trim()) * loops;
        }
    }
    examined.round() as i64
}
