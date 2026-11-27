//! Plan-cost regression for the ID search statements, [`SEARCH_PAGE_SQL`]
//! and [`SEARCH_LIST_SQL`].
//!
//! Which rows a search lists is covered by the api's admin list tests; this
//! covers how many it reads. A search usually matches a few sandboxes of a
//! large table, so the statement has to find them through the index on the
//! reversed ID and the primary key rather than walk the table in list order.
//! A cached statement can switch to a generic plan, which knows none of its
//! ID ranges, so the bound has to hold under both plan modes.
//!
//! [`a_search_reads_only_the_rows_it_matches`] explains the page statement
//! and the unpaged one for an ID label, the four digits of the shortest ID
//! end, an eight-digit ID start of UUIDv7-shaped IDs spread over time, and
//! digits no ID starts or ends with, under both modes, and bounds the rows
//! every plan node handles by [`MAX_ROWS_HANDLED`]. The fixture is many times
//! larger, so a plan that reads the table fails. To show that the bound is
//! what the ID-end index buys, it then drops it and requires the generic plan
//! of each statement to exceed the bound for the label. It reads rows off the
//! plan rather than naming an index or timing the statement: which linear
//! plan the planner takes is its business. Each search binds the ranges
//! [`AdminIdSearch::from_id_digits`] builds, as the api's searches do, so a
//! change to how they are built is explained here too.
//!
//! The explains are dealt to [`CONNECTIONS`] connections opened while the
//! fixture is seeded, and each connection runs its share in one round trip
//! ([`explain`]). On a loaded server, a connection per explain, which
//! [`explain_prepared`] opens, and a round trip per statement cost more than
//! the plans do.
//!
//! [`explain_prepared`]: pplx_space_db_test_fixtures::plan::explain_prepared

use pplx_space_db_test_fixtures::plan::{PLAN_CACHE_MODES, PlanNode};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::{Connection as _, PgConnection, PgPool, Row as _};
use tokio::task::JoinSet;
use uuid::Uuid;

use super::*;

/// Sandboxes in the fixture.
const SANDBOXES: i32 = 5_000;
/// The rows a search for a few sandboxes may handle across its plan: the
/// index entries and heap rows of its matches, their sort, and the joins of
/// the page's full rows.
const MAX_ROWS_HANDLED: f64 = 500.0;
/// The connections the explains are dealt to and run on at once.
const CONNECTIONS: usize = 4;

/// The first fixture sandbox's creation time, in Unix milliseconds, and the
/// time between two sandboxes: a UUIDv7 starts with this, so an eight-digit
/// ID start, which spans 65.5 s, matches six or seven of them.
const FIRST_CREATED_MS: i64 = 1_790_000_000_000;
const CREATED_EVERY_MS: i64 = 10_000;

/// The budget and row overhead the page statement is explained with: the
/// api's full list budget and its estimate of a row
/// (`MAX_SANDBOX_LIST_PAGE_BYTES` and `SANDBOX_LIST_ROW_OVERHEAD_BYTES`),
/// which these copy.
const MAX_PAGE_BYTES: i64 = 8 * 1024 * 1024;
const ROW_OVERHEAD_BYTES: i64 = 2 * 1024;

/// The fixture's sandboxes: [`SANDBOXES`] with UUIDv7-shaped IDs, and one
/// more with a known ID.
struct Fixture {
    /// The console's label for the known sandbox: its ID's last eight hex
    /// digits.
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
    let (fixture, mut conns) = tokio::try_join!(seed(pool), connect(pool))?;
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
    let mut cases = Vec::new();
    for (statement_label, statement) in statements {
        for (search_label, digits) in &searches {
            for plan_cache_mode in PLAN_CACHE_MODES {
                cases.push(Case {
                    statement_label,
                    search_label,
                    statement,
                    digits: digits.clone(),
                    plan_cache_mode,
                });
            }
        }
    }
    for (case, plan) in cases.iter().zip(explain_all(&mut conns, &cases).await?) {
        let handled = plan.rows_handled();
        assert!(
            handled <= MAX_ROWS_HANDLED,
            "{}, {} ({}): handled {handled} rows, over {MAX_ROWS_HANDLED}. Rows each node \
             handled per loop:{}",
            case.statement_label,
            case.search_label,
            case.plan_cache_mode,
            plan.outline()
        );
    }

    sqlx::query(sql!("DROP INDEX idx_sandboxes_id_suffix"))
        .execute(pool)
        .await?;
    let controls = statements.map(|(statement_label, statement)| Case {
        statement_label,
        search_label: "an ID label",
        statement,
        digits: fixture.label.clone(),
        plan_cache_mode: "force_generic_plan",
    });
    for (case, plan) in controls
        .iter()
        .zip(explain_all(&mut conns, &controls).await?)
    {
        let handled = plan.rows_handled();
        assert!(
            handled > MAX_ROWS_HANDLED,
            "{} without the ID-end index: handled {handled} rows, within the bound the index \
             is to buy. Rows each node handled per loop:{}",
            case.statement_label,
            plan.outline()
        );
    }
    for conn in conns {
        conn.close().await?;
    }
    Ok(())
}

/// One explain: a statement, the digits it searches for, and the plan mode
/// it is forced to, with the labels a failure names it by.
#[derive(Debug, Clone)]
struct Case {
    statement_label: &'static str,
    search_label: &'static str,
    statement: Statement,
    digits: String,
    plan_cache_mode: &'static str,
}

/// Explain `cases` on `conns` at once, each connection the cases dealt to it
/// by position in turn, and return their plans in the order of `cases`.
async fn explain_all(conns: &mut Vec<PgConnection>, cases: &[Case]) -> TestResult<Vec<PlanNode>> {
    let count = conns.len();
    let mut tasks = JoinSet::new();
    for (shard, mut conn) in conns.drain(..).enumerate() {
        let dealt: Vec<(usize, Case)> = cases
            .iter()
            .cloned()
            .enumerate()
            .skip(shard)
            .step_by(count)
            .collect();
        tasks.spawn(async move {
            let (indexes, shard_cases): (Vec<usize>, Vec<Case>) = dealt.into_iter().unzip();
            let plans = explain(&mut conn, &shard_cases).await;
            (conn, indexes, plans)
        });
    }
    let mut plans: Vec<Option<PlanNode>> = cases.iter().map(|_| None).collect();
    while let Some(joined) = tasks.join_next().await {
        let (conn, indexes, shard_plans) = joined?;
        conns.push(conn);
        for (index, plan) in indexes.into_iter().zip(shard_plans?) {
            plans[index] = Some(plan);
        }
    }
    plans
        .into_iter()
        .map(|plan| plan.ok_or_else(|| "a case was not explained".into()))
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum Statement {
    Page,
    Unpaged,
}

/// [`CONNECTIONS`] connections to `pool`'s database, not the pool's, so the
/// statements and modes the explains leave on them go nowhere else.
async fn connect(pool: &PgPool) -> TestResult<Vec<PgConnection>> {
    let mut conns = Vec::with_capacity(CONNECTIONS);
    for _ in 0..CONNECTIONS {
        conns.push(PgConnection::connect_with(&pool.connect_options()).await?);
    }
    Ok(conns)
}

/// `EXPLAIN (ANALYZE)` each case on `conn`, in one round trip, and return
/// their plans in order.
///
/// As `explain_prepared` does, each case prepares its statement with the
/// types its production caller binds, forces its mode, and explains its
/// `EXECUTE`, but all on one connection, in one simple query. That runs its
/// statements in one implicit transaction, and a prepared statement and a
/// mode stay with the session, so each case first deallocates every prepared
/// statement on the connection and sets its own mode: no statement or mode an
/// earlier case left, here or in an earlier batch, reaches it, and its
/// generic plan is built afresh. A simple query prepares no protocol-level
/// statement, so `DEALLOCATE ALL` drops only the cases' own.
async fn explain(conn: &mut PgConnection, cases: &[Case]) -> TestResult<Vec<PlanNode>> {
    if cases.is_empty() {
        return Ok(Vec::new());
    }
    let mut batch = String::new();
    for case in cases {
        let (prepare, execute) = search_sql(case.statement, &case.digits)?;
        // Each `;` on a line of its own, so no trailing comment swallows it.
        batch.push_str(&format!(
            "DEALLOCATE ALL\n;\nSET plan_cache_mode = {}\n;\n{prepare}\n;\n\
             EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) {execute}\n;\n",
            case.plan_cache_mode
        ));
    }
    let rows = sqlx::raw_sql(sqlx::AssertSqlSafe(batch))
        .fetch_all(conn)
        .await?;
    if rows.len() != cases.len() {
        return Err(format!("{} explains returned {} rows", cases.len(), rows.len()).into());
    }
    rows.iter()
        .map(|row| {
            let mut explained: serde_json::Value = row.try_get(0)?;
            Ok(serde_json::from_value(explained[0]["Plan"].take())?)
        })
        .collect()
}

/// The `PREPARE` and the `EXECUTE` of a search for `digits` with no other
/// filter, as the first page of 100 or the unpaged list of 100.
fn search_sql(statement: Statement, digits: &str) -> TestResult<(String, String)> {
    // The ranges the api binds for the same digits.
    let search = AdminIdSearch::from_id_digits(digits).ok_or("an ID search")?;
    let (suffix_low, suffix_high) = search.id_suffix();
    let id_prefix = search.id_prefix();
    // Hex digits, `:`, `g` and a UUID's text: nothing to quote.
    let literal =
        |value: Option<String>| value.map_or("NULL".to_string(), |value| format!("'{value}'"));
    let search_values = [
        literal(Some(suffix_low.to_string())),
        literal(Some(suffix_high.to_string())),
        literal(id_prefix.map(|(low, _)| low.to_string())),
        literal(id_prefix.map(|(_, high)| high.to_string())),
    ]
    .join(", ");
    let search_types = "text, text, uuid, uuid";
    let filters = "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL";
    let filter_types =
        "uuid, text[], text, uuid, timestamptz, timestamptz, text, uuid, uuid, uuid, boolean";
    Ok(match statement {
        Statement::Page => (
            format!(
                "PREPARE search({filter_types}, {search_types}, timestamptz, uuid, bigint, \
                 bigint, bigint) AS {}",
                SEARCH_PAGE_SQL.as_str()
            ),
            format!(
                "EXECUTE search({filters}, {search_values}, NULL, NULL, 101, {MAX_PAGE_BYTES}, \
                 {ROW_OVERHEAD_BYTES})"
            ),
        ),
        Statement::Unpaged => (
            format!(
                "PREPARE search({filter_types}, {search_types}, bigint) AS {}",
                SEARCH_LIST_SQL.as_str()
            ),
            format!("EXECUTE search({filters}, {search_values}, 100)"),
        ),
    })
}

/// [`SANDBOXES`] sandboxes of the seed user, and one more with a fresh
/// UUIDv7, in one statement, analyzed so both plan modes see their
/// statistics.
///
/// The insert runs with `session_replication_role = replica`, which turns
/// off its two foreign-key checks, a lookup each per row and nearly half the
/// insert's cost. Every row references the seed user and org, so there is
/// nothing for a check to catch, and `sandboxes` has no insert trigger of its
/// own that the setting would also skip.
async fn seed(pool: &PgPool) -> TestResult<Fixture> {
    let labelled = Uuid::now_v7();
    let mut insert = pool.begin().await?;
    sqlx::query(sql!("SET LOCAL session_replication_role = replica"))
        .execute(&mut *insert)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, name, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT (lpad(to_hex($4 + g * $5), 12, '0') || '7' || substr(md5(g::text), 1, 3)
                 || '8' || substr(md5(g::text), 4, 15))::UUID,
                'sandbox-' || g,
                $1, $2, 'k8s', 'started', 1
           FROM generate_series(1, $3) g
         UNION ALL
         SELECT $6, NULL, $1, $2, 'k8s', 'started', 1"
    ))
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SANDBOXES)
    .bind(FIRST_CREATED_MS)
    .bind(CREATED_EVERY_MS)
    .bind(labelled)
    .execute(&mut *insert)
    .await?;
    insert.commit().await?;
    sqlx::query(sql!("ANALYZE sandboxes")).execute(pool).await?;
    let compact = labelled.simple().to_string();
    let middle = format!(
        "{:012x}",
        FIRST_CREATED_MS + i64::from(SANDBOXES / 2) * CREATED_EVERY_MS
    );
    Ok(Fixture {
        label: compact.chars().skip(24).collect(),
        id_start: middle.chars().take(8).collect(),
    })
}
