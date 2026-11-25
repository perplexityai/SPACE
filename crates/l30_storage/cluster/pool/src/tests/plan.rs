//! Plan-cost regression for the warm-pool release reads,
//! [`RETAINED_POLICY_RELEASES`] and [`DEMANDED_POLICY_RELEASES`].
//!
//! A policy retains its template's newest-published releases, and a
//! release's publish time is its digest's newest tag under either spelling.
//! Each release finds its digest's tagged releases through the
//! `(template_name, hash, environment)` index. Matching the spellings by
//! normalizing the tagged column (`replace(tagged.hash, …) = …`) can't use it,
//! so each release's publish time scans every release of the template, and
//! `ORDER BY … LIMIT` needs every publish time first: one template's release
//! count squared. Node GC reads the retained releases on every tick, so they
//! must stay linear.
//!
//! [`warm_pool_release_reads_look_up_publish_times_by_index`] seeds one
//! template with [`RELEASES`] tagged releases among [`OTHER_TEMPLATES`]
//! others, checks the policy retains the newest, and bounds the per-row work
//! ([`PlanNode::repeated_rows_handled`]) of each read under both plan modes by
//! [`MAX_ROWS_PER_RELEASE`] per release it reads. An index scan can also walk
//! the template's index range per release and discard entries inside the
//! index, which no row count sees, so the test seeds a second fixture
//! [`SCALE`] times the size and bounds how much more each plan touches
//! ([`PlanNode::blocks_touched`]): by [`MAX_GROWTH`], linear in the template,
//! for a read of every release, and by [`MAX_LOOKUP_GROWTH`], independent of
//! it, for the demanded read of one.
//!
//! [`PlanNode::repeated_rows_handled`]: pplx_space_db_test_fixtures::plan::PlanNode::repeated_rows_handled
//! [`PlanNode::blocks_touched`]: pplx_space_db_test_fixtures::plan::PlanNode::blocks_touched

use pplx_space_db_test_fixtures::plan::{PLAN_CACHE_MODES, explain_prepared};
use pplx_space_sql_locator::sql;

use super::{TestResult, insert_template, postgres_fixture, upsert_warm_pool_policy};
use crate::store::{DEMANDED_POLICY_RELEASES, RETAINED_POLICY_RELEASES};

const TEMPLATE: &str = "busy";

/// The template's releases in the smaller fixture, each tagged once, in
/// creation order.
const RELEASES: i64 = 150;

/// How many times larger the second fixture is.
const SCALE: i64 = 8;

/// The policy's retained releases.
const RETAIN: i32 = 3;

/// Other templates, each with [`RELEASES`] tagged releases at both sizes, so
/// the tables are many times the smaller template's size, as in a cluster
/// with many templates. On tables of one template's size the planner rightly
/// rescans small tables rather than probe their indexes, and the bounds would
/// measure that choice; the larger template is large enough on its own.
const OTHER_TEMPLATES: i64 = 10;

/// The per-row work a read may handle per release it reads: its digest's
/// tagged releases under both spellings, their tags, and its original check,
/// about 7. A plan that rescans the template per release handles the
/// template's release count per release.
const MAX_ROWS_PER_RELEASE: f64 = 20.0;

/// How many times more blocks a read's plan may touch in the larger fixture:
/// at most [`SCALE`] for a plan linear in the template (about 8 in practice),
/// with room for deeper indexes. A plan that walks the template's index range
/// per release touches 23 to 29 times more, toward [`SCALE`] squared.
const MAX_GROWTH: f64 = 12.0;

/// How many times more blocks the demanded read of one release may touch in
/// the larger fixture: about once, as its work doesn't depend on the
/// template's history, with room for deeper indexes. A read that computes
/// publish times for releases it wasn't asked for touches about [`SCALE`]
/// times more.
const MAX_LOOKUP_GROWTH: f64 = 1.5;

/// One read under test: `PREPARE`, with each parameter typed as the store
/// binds it, the `EXECUTE` to explain, and how many of the template's
/// releases it reads.
struct Case {
    label: &'static str,
    prepare: String,
    execute: String,
    /// The template's releases the read is asked about, which its row bound
    /// is per release of.
    reads: Reads,
}

/// How many of the template's releases a case reads.
#[derive(Clone, Copy)]
enum Reads {
    /// Every one, so its work grows with the template.
    All,
    /// One, so its work doesn't.
    One,
}

impl Reads {
    #[expect(
        clippy::cast_precision_loss,
        reason = "a few thousand releases are exact in an f64"
    )]
    fn count(self, releases: i64) -> f64 {
        match self {
            Reads::All => releases as f64,
            Reads::One => 1.0,
        }
    }

    fn max_growth(self) -> f64 {
        match self {
            Reads::All => MAX_GROWTH,
            Reads::One => MAX_LOOKUP_GROWTH,
        }
    }
}

/// One case's plan under one plan mode.
struct Measured {
    label: &'static str,
    plan_cache_mode: &'static str,
    max_growth: f64,
    blocks: f64,
    outline: String,
}

#[tokio::test(flavor = "current_thread")]
async fn warm_pool_release_reads_look_up_publish_times_by_index() -> TestResult {
    let small = measure(RELEASES).await?;
    let large = measure(RELEASES * SCALE).await?;
    for (small, large) in small.iter().zip(&large) {
        let growth = large.blocks / small.blocks;
        assert!(
            growth <= small.max_growth,
            "{} ({}): touched {} blocks at {RELEASES} releases and {} at {SCALE} times that, \
             {growth} times more, over {}. Rows each node handled per loop at the larger \
             size:{}",
            small.label,
            small.plan_cache_mode,
            small.blocks,
            large.blocks,
            small.max_growth,
            large.outline
        );
    }
    Ok(())
}

/// Seed a fixture with `releases` releases of the template in a database of
/// its own, check the policy retains the newest and each read stays within
/// its row bound under both plan modes, and return the blocks each plan
/// touched.
async fn measure(releases: i64) -> TestResult<Vec<Measured>> {
    let fixture = postgres_fixture().await?;
    let pool = &fixture.pool;
    seed(pool, releases).await?;

    let mut protected = fixture
        .store
        .protected_template_releases()
        .await?
        .into_iter()
        .map(|release| release.content_hash)
        .collect::<Vec<_>>();
    protected.sort();
    let newest = (releases - i64::from(RETAIN) + 1..=releases)
        .map(|g| format!("sha256:{g:064x}"))
        .collect::<Vec<_>>();
    assert_eq!(protected, newest);

    let release_ids: Vec<uuid::Uuid> = sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE template_name = $1 ORDER BY created_at"
    ))
    .bind(TEMPLATE)
    .fetch_all(pool)
    .await?;

    let mut measured = Vec::new();
    let middle = *release_ids
        .get(release_ids.len() / 2)
        .ok_or("the template has releases")?;
    for case in cases(&release_ids, middle) {
        for plan_cache_mode in PLAN_CACHE_MODES {
            let plan =
                explain_prepared(pool, &case.prepare, &case.execute, plan_cache_mode).await?;
            // A plan whose rows didn't parse would pass any upper bound.
            assert!(plan.rows_handled() > 0.0, "{}: no rows parsed", case.label);
            let handled = plan.repeated_rows_handled();
            let max_rows = MAX_ROWS_PER_RELEASE * case.reads.count(releases);
            assert!(
                handled <= max_rows,
                "{} ({plan_cache_mode}, {releases} releases): handled {handled} repeated rows, \
                 over {max_rows}. Rows each node handled per loop:{}",
                case.label,
                plan.outline()
            );
            measured.push(Measured {
                label: case.label,
                plan_cache_mode,
                max_growth: case.reads.max_growth(),
                blocks: plan.blocks_touched(),
                outline: plan.outline(),
            });
        }
    }
    Ok(measured)
}

/// The retained read as node GC runs it (no initial environment) and as the
/// autoscaler does, which also resolves each retained release's twin, and
/// the demanded read for every release of the template and for one in the
/// middle of its history.
fn cases(release_ids: &[uuid::Uuid], middle: uuid::Uuid) -> Vec<Case> {
    let retained = |label, initial_environment: &str| Case {
        label,
        prepare: format!(
            "PREPARE plan_under_test(bigint, bigint, text) AS {}",
            RETAINED_POLICY_RELEASES.as_str()
        ),
        execute: format!("EXECUTE plan_under_test(1, 1, {initial_environment})"),
        reads: Reads::All,
    };
    let demanded = |label, ids: &[uuid::Uuid], reads| Case {
        label,
        prepare: format!(
            "PREPARE plan_under_test(uuid[], bigint, bigint) AS {DEMANDED_POLICY_RELEASES}"
        ),
        execute: format!(
            "EXECUTE plan_under_test(ARRAY[{}]::uuid[], 1, 1)",
            ids.iter()
                .map(|id| format!("'{id}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        reads,
    };
    vec![
        retained("retained releases, for node GC", "NULL"),
        retained(
            "retained releases, for the autoscaler",
            &format!("'sha256:{}'", "0".repeat(64)),
        ),
        demanded(
            "demanded releases, every release of the template",
            release_ids,
            Reads::All,
        ),
        demanded(
            "demanded releases, one release of the template",
            &[middle],
            Reads::One,
        ),
    ]
}

/// The template's `releases` releases, published once each in creation
/// order, its policy, the [`OTHER_TEMPLATES`] others, and statistics for both
/// plan modes, vacuumed too so index-only scans skip the same heap pages at
/// both sizes.
async fn seed(pool: &sqlx::PgPool, releases: i64) -> TestResult {
    insert_template(pool, TEMPLATE).await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name)
         SELECT space_uuid_v7(), $1 || '-other-' || t FROM generate_series(1, $2) t"
    ))
    .bind(TEMPLATE)
    .bind(OTHER_TEMPLATES)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, created_at)
         SELECT space_uuid_v7(), $1, 'fc', 'sha256:' || lpad(to_hex(g), 64, '0'),
                NOW() - make_interval(days => 1, secs => ($2 - g)::double precision)
           FROM generate_series(1, $2) g
         UNION ALL
         SELECT space_uuid_v7(), $1 || '-other-' || t, 'fc',
                'sha256:' || lpad(to_hex(g), 64, '0'),
                NOW() - make_interval(days => 1, secs => ($2 - g)::double precision)
           FROM generate_series(1, $3) t, generate_series(1, $4) g"
    ))
    .bind(TEMPLATE)
    .bind(releases)
    .bind(OTHER_TEMPLATES)
    .bind(RELEASES)
    .execute(pool)
    .await?;
    // Every release published once, in creation order.
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (
             template_release_id, template_name, tag, created_at, modified_at
         )
         SELECT id, template_name,
                'v' || row_number() OVER (PARTITION BY template_name ORDER BY created_at),
                created_at, created_at
           FROM template_releases"
    ))
    .execute(pool)
    .await?;
    upsert_warm_pool_policy(pool, TEMPLATE, 1, 2, RETAIN).await?;
    sqlx::query(sql!(
        "VACUUM (ANALYZE) templates, template_releases, template_release_tags, warm_pool_policies"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
