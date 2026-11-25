//! Plan-cost regression for the hot release reads: [`list_latest`]'s two
//! statements and the twin resolution [`resolved_release_sql`] defines.
//!
//! Which releases they answer with is covered by [`super::tests`]; this
//! covers how many rows they read for a template with many releases, through
//! [`pplx_space_db_test_fixtures::plan`]. Each release is matched to the other
//! releases of its digest, under either spelling (`<hex>` and
//! `sha256:<hex>`), through the `(template_name, hash, environment)` index. A
//! predicate that wraps the probed column instead, such as
//! `replace(o.hash, 'sha256:', '') = …`, can't use it, nor can a generic plan
//! use it through an `OR` whose second branch only a custom plan can drop, so
//! every release rescans the template's releases.
//!
//! [`hot_release_reads_handle_rows_linear_in_the_releases`] seeds one template
//! with [`DIGESTS`] digests among [`OTHER_TEMPLATES`] others and bounds each
//! statement's plan under both plan modes. [`list_latest`] reads every release
//! of the template before its `LIMIT`, so it may handle
//! [`MAX_ROWS_PER_RELEASE`] rows per release in nodes that run more than once
//! ([`Measure::Repeated`]): per-row work, which a rescan multiplies and a
//! single pass over a table never adds to. A resolution looks up one digest,
//! so it may handle [`MAX_RESOLUTION_ROWS`] rows in all ([`Measure::All`])
//! whatever the template's size. An index scan can also walk the template's
//! index range per row and discard entries inside the index, which no row
//! count sees, so the test seeds a second fixture [`SCALE`] times the size and
//! bounds how much more the plans touch ([`PlanNode::blocks_touched`]): a
//! listing at most [`MAX_LISTING_GROWTH`] times, linear in the template, and a
//! resolution at most [`MAX_RESOLUTION_GROWTH`] times, independent of it.
//!
//! [`PlanNode::blocks_touched`]: pplx_space_db_test_fixtures::plan::PlanNode::blocks_touched

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::plan::{PLAN_CACHE_MODES, PlanNode, explain_prepared};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, try_pg_test_db,
};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use crate::catalog::{LIST_LATEST_NODE_SCOPED, LIST_LATEST_UNSCOPED, list_latest};
use crate::resolution::RESOLVED_RELEASE;
use crate::tests::{catalog, environment, insert_template, metadata, set_default};
use crate::{PartitionSelector, resolved_release};

const TEMPLATE: &str = "plan-scale";

/// Digests of the fixture template: each has an original stored as
/// `sha256:<hex>`; every [`TWIN_EVERY`]th also a twin bound to the twin
/// environment, and every [`BARE_EVERY`]th also an original stored bare.
const DIGESTS: i32 = 150;
const TWIN_EVERY: i32 = 5;
const BARE_EVERY: i32 = 10;
// So every digest with a bare original also has a twin.
const _: () = assert!(BARE_EVERY % TWIN_EVERY == 0);

/// Other templates, each with [`DIGESTS`] releases at both sizes and a storage
/// location on each, so the tables are many times the smaller template's
/// size, as in a cluster with many templates. On tables of one template's size
/// the planner rightly rescans small tables rather than probe their indexes,
/// and the bounds would measure that choice instead of the probes; the larger
/// template is large enough on its own.
const OTHER_TEMPLATES: i32 = 10;

/// The per-row work a [`list_latest`] plan may handle per release it reads:
/// probing the releases of its digest under both spellings for its version's
/// age and its original, and its storage locations. A plan that rescans the
/// template per release it reads handles the template's release count (195
/// in the smaller fixture) per release, whether it reads them all or only
/// the twins.
const MAX_ROWS_PER_RELEASE: f64 = 10.0;

/// The rows a resolution may handle: one digest's releases, their storage
/// locations and environments, about 80. A plan that reads the template's
/// releases even once handles its release count more, 195 in the smaller
/// fixture.
const MAX_RESOLUTION_ROWS: f64 = 200.0;

/// How many times larger the second fixture is.
const SCALE: i32 = 8;

/// How many times more blocks a listing's plan may touch in the larger
/// fixture: at most [`SCALE`] for a plan linear in the template (5 to 8 in
/// practice, as the other templates don't grow), with room for deeper
/// indexes. A plan that walks the template's index range per release touches
/// 20 to 33 times more, toward [`SCALE`] squared.
const MAX_LISTING_GROWTH: f64 = 12.0;

/// How many times more blocks a resolution's plan may touch in the larger
/// fixture: about once, as it looks up one digest, with room for deeper
/// indexes. A plan that walks the template's index range touches about twice
/// as much and more, toward [`SCALE`].
const MAX_RESOLUTION_GROWTH: f64 = 1.5;

/// One statement under test: `PREPARE`, with each parameter typed as the
/// store binds it, the `EXECUTE` to explain, and the rows of its plan, by
/// `measure`, it may handle.
struct Case {
    label: &'static str,
    prepare: String,
    execute: String,
    max_rows: f64,
    /// What `max_rows` bounds.
    measure: Measure,
    /// How many times more blocks its plan may touch in the larger fixture.
    max_growth: f64,
}

/// The rows of a plan a case bounds.
#[derive(Debug, Clone, Copy)]
enum Measure {
    /// Rows handled by nodes that ran more than once: per-row work, for a
    /// statement that reads every release of the template, where a single
    /// pass over a table is linear whatever its size.
    Repeated,
    /// Every row handled, for a lookup of one digest, where reading the
    /// template's releases at all is the regression.
    All,
}

impl Measure {
    fn rows(self, plan: &PlanNode) -> f64 {
        match self {
            Measure::Repeated => plan.repeated_rows_handled(),
            Measure::All => plan.rows_handled(),
        }
    }
}

/// What [`seed`] leaves for the cases to bind and bound.
struct Fixture {
    /// The template's releases.
    releases: f64,
    /// Its twins, the releases a listing that selects their environment reads.
    twins: f64,
    /// A worker running one of the template's sandboxes.
    node: NodeId,
    /// The bare spelling of a digest with a twin and both originals.
    resolved_hash: String,
    /// That digest's twin, which its resolution answers.
    resolved_twin: Uuid,
}

#[tokio::test(flavor = "current_thread")]
async fn hot_release_reads_handle_rows_linear_in_the_releases() -> TestResult {
    let Some(small) = measure(DIGESTS).await? else {
        return Ok(());
    };
    let Some(large) = measure(DIGESTS * SCALE).await? else {
        return Ok(());
    };
    for (small, large) in small.iter().zip(&large) {
        let growth = large.blocks / small.blocks;
        assert!(
            growth <= small.max_growth,
            "{} ({}): touched {} blocks at {DIGESTS} digests and {} at {SCALE} times that, \
             {growth} times more, over {}. Rows each node handled per loop at the larger size:{}",
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

/// One case's plan under one plan mode.
struct Measured {
    label: &'static str,
    plan_cache_mode: &'static str,
    max_growth: f64,
    blocks: f64,
    outline: String,
}

/// Seed a fixture of `digests` digests in a database of its own, check that
/// each case takes the path its label names and stays within its row bound
/// under both plan modes, and return the blocks each plan touched.
async fn measure(digests: i32) -> TestResult<Option<Vec<Measured>>> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(None);
    };
    let pool = &db.pool;
    let fixture = seed(pool, digests).await?;
    // Each case takes the path its label names, so its bound covers that
    // path's plan: the resolution answers the digest's twin, the worker sees
    // the template through its sandbox, and the twins' environment selects
    // its twins.
    let resolved = resolved_release(pool, TEMPLATE, &fixture.resolved_hash, environment(0))
        .await?
        .map(|(release, _)| release);
    assert_eq!(resolved, Some(fixture.resolved_twin));
    let listed = list_latest(pool, TEMPLATE, "fc", 10, Some(&[fixture.node]), None).await?;
    assert_eq!(listed.len(), 10, "the worker sees the template's releases");
    let selected = PartitionSelector {
        environment: twin_environment(),
        includes_unbound: false,
    };
    let twins = list_latest(pool, TEMPLATE, "fc", 10, None, Some(selected)).await?;
    assert_eq!(twins.len(), 10, "the twins' environment selects its twins");
    assert!(
        twins
            .iter()
            .all(|release| release.environment == Some(twin_environment().to_string())),
        "the twins' environment selects only its twins"
    );

    let mut measured = Vec::new();
    for case in cases(&fixture) {
        for plan_cache_mode in PLAN_CACHE_MODES {
            let plan =
                explain_prepared(pool, &case.prepare, &case.execute, plan_cache_mode).await?;
            // A plan whose rows didn't parse would pass any upper bound.
            assert!(plan.rows_handled() > 0.0, "{}: no rows parsed", case.label);
            let handled = case.measure.rows(&plan);
            assert!(
                handled <= case.max_rows,
                "{} ({plan_cache_mode}, {digests} digests): handled {handled} {:?} rows, over \
                 {}. Rows each node handled per loop:{}",
                case.label,
                case.measure,
                case.max_rows,
                plan.outline()
            );
            measured.push(Measured {
                label: case.label,
                plan_cache_mode,
                max_growth: case.max_growth,
                blocks: plan.blocks_touched(),
                outline: plan.outline(),
            });
        }
    }
    Ok(Some(measured))
}

fn cases(fixture: &Fixture) -> Vec<Case> {
    let list_latest_rows = MAX_ROWS_PER_RELEASE * fixture.releases;
    // A listing that selects the twins' environment reads only the twins.
    let selected_rows = MAX_ROWS_PER_RELEASE * fixture.twins;
    let unscoped = |label, environment: &str, max_rows| Case {
        label,
        prepare: format!(
            "PREPARE plan_under_test(text, text, bigint, text, boolean) AS {}",
            LIST_LATEST_UNSCOPED.as_str()
        ),
        execute: format!("EXECUTE plan_under_test('{TEMPLATE}', 'fc', 10, {environment}, FALSE)"),
        max_rows,
        measure: Measure::Repeated,
        max_growth: MAX_LISTING_GROWTH,
    };
    vec![
        unscoped(
            "list_latest, selecting the twin environment",
            &format!("'{}'", twin_environment()),
            selected_rows,
        ),
        unscoped("list_latest, unselected", "NULL", list_latest_rows),
        Case {
            label: "list_latest, node-scoped to a worker running one of the template's sandboxes",
            prepare: format!(
                "PREPARE plan_under_test(text, text, bigint, uuid[], text, boolean) AS {}",
                LIST_LATEST_NODE_SCOPED.as_str()
            ),
            execute: format!(
                "EXECUTE plan_under_test('{TEMPLATE}', 'fc', 10, ARRAY['{}']::uuid[], NULL, \
                 FALSE)",
                fixture.node
            ),
            max_rows: list_latest_rows,
            measure: Measure::Repeated,
            max_growth: MAX_LISTING_GROWTH,
        },
        Case {
            label: "resolved_release, for a digest with a twin and both spellings",
            prepare: format!(
                "PREPARE plan_under_test(text, text, text) AS {}",
                RESOLVED_RELEASE.as_str()
            ),
            // The initial environment, which unbound releases count as.
            execute: format!(
                "EXECUTE plan_under_test('{TEMPLATE}', '{}', '{}')",
                fixture.resolved_hash,
                environment(0)
            ),
            max_rows: MAX_RESOLUTION_ROWS,
            measure: Measure::All,
            max_growth: MAX_RESOLUTION_GROWTH,
        },
    ]
}

/// The environment the fixture's twins are bound to: allowed, catalogued and
/// the cluster default, so resolutions pick its twins.
fn twin_environment() -> EnvironmentReference {
    environment(1)
}

/// One org-specific template, which a node sees through a sandbox it runs,
/// with `digests` digests registered a minute apart: a `sha256:` original
/// each, then a bare original every [`BARE_EVERY`]th and a twin every
/// [`TWIN_EVERY`]th; a storage
/// location on each `sha256:` release, twins included, as a twin copies its
/// original's; [`OTHER_TEMPLATES`] more templates; and statistics for both
/// plan modes, vacuumed too so index-only scans skip the same heap pages at
/// both sizes.
async fn seed(pool: &PgPool, digests: i32) -> TestResult<Fixture> {
    insert_template(pool, TEMPLATE).await?;
    sqlx::query(sql!(
        "UPDATE templates SET visibility = 'org_specific', org_id = $2 WHERE name = $1"
    ))
    .bind(TEMPLATE)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
         SELECT $1, 'fc', 'sha256:' || md5(g::text) || md5(g::text), NULL::JSONB,
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + g * INTERVAL '1 minute'
           FROM generate_series(1, $2) g
         UNION ALL
         SELECT $1, 'fc', md5(g::text) || md5(g::text), NULL::JSONB,
                TIMESTAMPTZ '2026-02-01 00:00:00+00' + g * INTERVAL '1 minute'
           FROM generate_series(1, $2) g
          WHERE g % $3 = 0
         UNION ALL
         SELECT $1, 'fc', 'sha256:' || md5(g::text) || md5(g::text), $5,
                TIMESTAMPTZ '2026-03-01 00:00:00+00' + g * INTERVAL '1 minute'
           FROM generate_series(1, $2) g
          WHERE g % $4 = 0"
    ))
    .bind(TEMPLATE)
    .bind(digests)
    .bind(BARE_EVERY)
    .bind(TWIN_EVERY)
    .bind(metadata(twin_environment()))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         SELECT id, 's3://plan-scale/' || hash
           FROM template_releases
          WHERE template_name = $1
            AND hash LIKE 'sha256:%'"
    ))
    .bind(TEMPLATE)
    .execute(pool)
    .await?;
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, user_id, org_id, template_id, active_node_id, managed_by, lifecycle,
              network_policy_generation)
         VALUES ($1, $2, $3, $4, $5, 'k8s', 'started', 1)"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(TEMPLATE)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name)
         SELECT space_uuid_v7(), $1 || '-other-' || t FROM generate_series(1, $2) t"
    ))
    .bind(TEMPLATE)
    .bind(OTHER_TEMPLATES)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "WITH other AS (
             INSERT INTO template_releases (template_name, type, hash, created_at)
             SELECT $1 || '-other-' || t, 'fc', 'sha256:' || md5(t || '/' || g) || md5(g::text),
                    TIMESTAMPTZ '2026-01-01 00:00:00+00' + g * INTERVAL '1 minute'
               FROM generate_series(1, $2) t, generate_series(1, $3) g
             RETURNING id, hash
         )
         INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         SELECT id, 's3://plan-scale/' || hash FROM other"
    ))
    .bind(TEMPLATE)
    .bind(OTHER_TEMPLATES)
    .bind(DIGESTS)
    .execute(pool)
    .await?;
    set_default(pool, twin_environment()).await?;
    catalog(pool, twin_environment(), &[2]).await?;
    sqlx::query(sql!(
        "VACUUM (ANALYZE) templates, template_releases, template_release_storage_locations, sandboxes,
                 runtime_environment_states, runtime_environments"
    ))
    .execute(pool)
    .await?;

    // A digest with a twin and a bare original, near the middle of the
    // template's history, looked up by its bare spelling.
    let resolved_digest = digests / 2 / BARE_EVERY * BARE_EVERY;
    let (releases, twins, resolved_hash, resolved_twin) = sqlx::query_as(sql!(
        "SELECT (SELECT COUNT(*) FROM template_releases WHERE template_name = $1)::FLOAT8,
                (SELECT COUNT(*) FROM template_releases
                  WHERE template_name = $1 AND execution_metadata IS NOT NULL)::FLOAT8,
                md5($2::TEXT) || md5($2::TEXT),
                (SELECT id FROM template_releases
                  WHERE template_name = $1
                    AND hash = 'sha256:' || md5($2::TEXT) || md5($2::TEXT)
                    AND execution_metadata IS NOT NULL)"
    ))
    .bind(TEMPLATE)
    .bind(resolved_digest)
    .fetch_one(pool)
    .await?;
    Ok(Fixture {
        releases,
        twins,
        node,
        resolved_hash,
        resolved_twin,
    })
}
