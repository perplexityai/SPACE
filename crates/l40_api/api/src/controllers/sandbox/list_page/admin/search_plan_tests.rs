//! Plan-cost regression for the [`SandboxSearch`] statements.
//!
//! Which rows a search lists is covered in `tests`; this covers how many it
//! reads. A search usually matches a few sandboxes of a large table, so the
//! statement has to find them through the index on the reversed ID and the
//! primary key rather than walk the table in list order. A cached statement
//! can switch to a generic plan, which knows none of its ID ranges, so the
//! bound has to hold under both plan modes.
//!
//! [`a_search_reads_only_the_rows_it_matches`] explains the page statement
//! and the unpaged one for an ID label, the four digits of the shortest ID
//! end, an eight-digit ID start of UUIDv7-shaped IDs spread over time, and
//! digits no ID starts or ends with, under both modes, and bounds the rows
//! every plan node handles by [`MAX_ROWS_HANDLED`]. The fixture is many times
//! larger, so a plan that reads the table fails. To show that the bound is
//! what the ID-end index buys, it then drops it and requires the generic plan
//! of each statement to exceed the bound for the label. It reads rows off the
//! plan through [`crate::public_operations::plan_explain`].

use futures_util::future::try_join_all;
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;
use crate::public_operations::plan_explain::{PLAN_CACHE_MODES, PlanNode, explain_prepared};

/// Sandboxes in the fixture.
const SANDBOXES: i32 = 5_000;
/// The rows a search for a few sandboxes may handle across its plan: the
/// index entries and heap rows of its matches, their sort, and the joins of
/// the page's full rows.
const MAX_ROWS_HANDLED: f64 = 500.0;

/// The first fixture sandbox's creation time, in Unix milliseconds, and the
/// time between two sandboxes: a UUIDv7 starts with this, so an eight-digit
/// ID start, which spans 65.5 s, matches six or seven of them.
const FIRST_CREATED_MS: i64 = 1_790_000_000_000;
const CREATED_EVERY_MS: i64 = 10_000;

/// The fixture's sandboxes: [`SANDBOXES`] with UUIDv7-shaped IDs, and one
/// more with a known ID.
struct Fixture {
    /// The console's label for `labelled`: its ID's last eight hex digits.
    label: String,
    /// The first eight hex digits of a fixture sandbox's ID.
    id_start: String,
}

#[tokio::test(flavor = "current_thread")]
async fn a_search_reads_only_the_rows_it_matches() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let fixture = seed(pool).await?;
    let searches = [
        ("an ID label", fixture.label.clone()),
        (
            "the shortest ID end, four digits",
            fixture.label.chars().skip(4).collect(),
        ),
        ("an eight-digit ID start", fixture.id_start.clone()),
        ("digits no ID starts or ends with", "00000000".to_string()),
    ];
    let statements = [
        ("page", Statement::Page),
        ("unpaged list", Statement::Unpaged),
    ];
    let plans = try_join_all(statements.iter().flat_map(|&(statement_label, statement)| {
        searches.iter().flat_map(move |(search_label, text)| {
            PLAN_CACHE_MODES
                .into_iter()
                .map(move |plan_cache_mode| async move {
                    let plan = explain(pool, statement, text, plan_cache_mode).await?;
                    TestResult::Ok((statement_label, search_label, plan_cache_mode, plan))
                })
        })
    }))
    .await?;
    for (statement_label, search_label, plan_cache_mode, plan) in plans {
        let handled = plan.rows_handled();
        assert!(
            handled <= MAX_ROWS_HANDLED,
            "{statement_label}, {search_label} ({plan_cache_mode}): handled {handled} \
             rows, over {MAX_ROWS_HANDLED}. Rows handled by node:{}",
            plan.outline()
        );
    }

    sqlx::query(sql!("DROP INDEX idx_sandboxes_id_suffix"))
        .execute(pool)
        .await?;
    for (statement_label, statement) in statements {
        let plan = explain(pool, statement, &fixture.label, "force_generic_plan").await?;
        let handled = plan.rows_handled();
        assert!(
            handled > MAX_ROWS_HANDLED,
            "{statement_label} without the ID-end index: handled {handled} rows, within the \
             bound the index is to buy. Rows handled by node:{}",
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
    // Hex digits, `:`, `g` and a UUID's text: nothing to quote.
    let literal =
        |value: Option<String>| value.map_or("NULL".to_string(), |value| format!("'{value}'"));
    let (suffix_low, suffix_high) = &search.id_suffix;
    let search_values = [
        literal(Some(suffix_low.clone())),
        literal(Some(suffix_high.clone())),
        literal(search.id_prefix.map(|(low, _)| low.to_string())),
        literal(search.id_prefix.map(|(_, high)| high.to_string())),
    ]
    .join(", ");
    let search_types = "text, text, uuid, uuid";
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
         SELECT (lpad(to_hex($4 + g * $5), 12, '0') || '7' || substr(md5(g::text), 1, 3)
                 || '8' || substr(md5(g::text), 4, 15))::UUID,
                'sandbox-' || g,
                $1, $2, 'k8s', 'started', 1
           FROM generate_series(1, $3) g"
    ))
    .bind(owner.user_id)
    .bind(owner.org_id)
    .bind(SANDBOXES)
    .bind(FIRST_CREATED_MS)
    .bind(CREATED_EVERY_MS)
    .execute(pool)
    .await?;
    let labelled =
        super::tests::seed_sandbox(pool, owner, super::tests::SeedSandbox::default()).await?;
    sqlx::query(sql!("ANALYZE sandboxes")).execute(pool).await?;
    let compact = labelled.as_uuid().simple().to_string();
    let middle = format!(
        "{:012x}",
        FIRST_CREATED_MS + i64::from(SANDBOXES / 2) * CREATED_EVERY_MS
    );
    Ok(Fixture {
        label: compact.chars().skip(24).collect(),
        id_start: middle.chars().take(8).collect(),
    })
}
