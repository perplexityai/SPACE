//! Plan-cost regression for the [`SandboxSearch`] statements.
//!
//! Which rows a search lists is covered in `tests`; this covers how many it
//! reads. A search usually matches a few sandboxes of a large table, so the
//! statement has to find them through the indexes on the lowercased name and
//! the reversed ID rather than walk the table in list order. A cached
//! statement can switch to a generic plan, which knows neither the text nor
//! its ID range, so the bound has to hold under both plan modes.
//!
//! [`a_search_reads_only_the_rows_it_matches`] explains the page statement
//! and the unpaged one for a name prefix in another case, at full length and
//! at the three characters a search needs, an ID label and a text nothing
//! matches, under both modes, and bounds the rows every plan
//! node handles by [`MAX_ROWS_HANDLED`]. The fixture is many times larger, so
//! a plan that reads the table fails. To show that the bound is what the
//! indexes buy, it then drops both and requires the generic plan of each
//! statement to exceed it. It reads rows off the plan through
//! [`crate::public_operations::plan_explain`].

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;
use crate::public_operations::plan_explain::{PLAN_CACHE_MODES, PlanNode, explain_prepared};

/// Sandboxes in the fixture.
const SANDBOXES: i32 = 20_000;
/// The rows a search for a few sandboxes may handle across its plan: the
/// index entries and heap rows of its matches, their sort, and the joins of
/// the page's full rows.
const MAX_ROWS_HANDLED: f64 = 500.0;

/// The fixture's sandboxes: [`SANDBOXES`] with names sharing one prefix, of
/// which three are named `Needle-…` and one has a known ID.
struct Fixture {
    /// The console's label for `labelled`: its ID's last eight hex digits.
    label: String,
}

#[tokio::test(flavor = "current_thread")]
async fn a_search_reads_only_the_rows_it_matches() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fixture = seed(pool).await?;
    let searches = [
        ("a name prefix in another case", "nEEDLE".to_string()),
        ("the shortest name prefix, three letters", "NEE".to_string()),
        ("an ID label", fixture.label.clone()),
        ("a text nothing matches", "no-such-sandbox".to_string()),
    ];
    let statements = [
        ("page", Statement::Page),
        ("unpaged list", Statement::Unpaged),
    ];
    for (statement_label, statement) in statements {
        for (search_label, text) in &searches {
            for plan_cache_mode in PLAN_CACHE_MODES {
                let plan = explain(pool, statement, text, plan_cache_mode).await?;
                let handled = plan.rows_handled();
                assert!(
                    handled <= MAX_ROWS_HANDLED,
                    "{statement_label}, {search_label} ({plan_cache_mode}): handled {handled} \
                     rows, over {MAX_ROWS_HANDLED}. Rows handled by node:{}",
                    plan.outline()
                );
            }
        }
    }

    sqlx::query(sql!(
        "DROP INDEX idx_sandboxes_id_suffix, idx_sandboxes_name_lower"
    ))
    .execute(pool)
    .await?;
    for (statement_label, statement) in statements {
        let plan = explain(pool, statement, &fixture.label, "force_generic_plan").await?;
        let handled = plan.rows_handled();
        assert!(
            handled > MAX_ROWS_HANDLED,
            "{statement_label} without the indexes: handled {handled} rows, within the bound \
             the indexes are to buy. Rows handled by node:{}",
            plan.outline()
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Statement {
    Page,
    Unpaged,
}

/// `EXPLAIN (ANALYZE)` a search for `text` with no other filter, as the
/// first page of 100 or the unpaged list of 100, under a forced
/// `plan_cache_mode`.
async fn explain(
    pool: &PgPool,
    statement: Statement,
    text: &str,
    plan_cache_mode: &str,
) -> TestResult<PlanNode> {
    let search = SandboxSearch::new(text)?.ok_or("an empty search")?;
    let binds = SearchBinds::new(pool, &search).await?;
    let literal = |value: Option<String>| {
        value.map_or("NULL".to_string(), |value| {
            format!("'{}'", value.replace('\'', "''"))
        })
    };
    let search_values = [
        literal(Some(binds.text.to_string())),
        literal(Some(binds.name_key.0.clone())),
        literal(Some(binds.name_key.1.clone())),
        literal(binds.id_suffix.map(|(low, _)| low.clone())),
        literal(binds.id_suffix.map(|(_, high)| high.clone())),
        literal(binds.id_prefix.map(|(low, _)| low.to_string())),
        literal(binds.id_prefix.map(|(_, high)| high.to_string())),
    ]
    .join(", ");
    let search_types = "text, text, text, text, text, uuid, uuid";
    let filters = "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL";
    let filter_types =
        "uuid, text[], text, uuid, timestamptz, timestamptz, text, uuid, uuid, uuid, boolean";
    let (prepare, execute) = match statement {
        Statement::Page => (
            format!(
                "PREPARE search({filter_types}, {search_types}, timestamptz, uuid, bigint, \
                 bigint, bigint) AS {}",
                ADMIN_SEARCH_PAGE_SQL.as_str()
            ),
            format!(
                "EXECUTE search({filters}, {search_values}, NULL, NULL, 101, {}, {})",
                super::super::MAX_SANDBOX_LIST_PAGE_BYTES,
                SANDBOX_LIST_ROW_OVERHEAD_BYTES
            ),
        ),
        Statement::Unpaged => (
            format!(
                "PREPARE search({filter_types}, {search_types}, bigint) AS {}",
                ADMIN_SEARCH_LIST_SQL.as_str()
            ),
            format!("EXECUTE search({filters}, {search_values}, 100)"),
        ),
    };
    explain_prepared(pool, &prepare, &execute, plan_cache_mode).await
}

/// [`SANDBOXES`] sandboxes of one owner, analyzed so both plan modes see
/// their statistics.
async fn seed(pool: &PgPool) -> TestResult<Fixture> {
    let owner = super::tests::seed_owner(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, name, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT gen_random_uuid(),
                CASE WHEN g <= 3 THEN 'Needle-' || g ELSE 'sandbox-' || g END,
                $1, $2, 'k8s', 'started', 1
           FROM generate_series(1, $3) g"
    ))
    .bind(owner.user_id)
    .bind(owner.org_id)
    .bind(SANDBOXES)
    .execute(pool)
    .await?;
    let labelled =
        super::tests::seed_sandbox(pool, owner, super::tests::SeedSandbox::default()).await?;
    sqlx::query(sql!("ANALYZE sandboxes")).execute(pool).await?;
    let compact = labelled.as_uuid().simple().to_string();
    Ok(Fixture {
        label: compact.chars().skip(24).collect(),
    })
}
