use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

async fn seed_sandbox(
    pool: &PgPool,
    active_node_id: Option<NodeId>,
    route_node_id: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation,
             active_node_id)
         VALUES ($1, $2, $3, $4, 'started', 1, $5)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Space)
    .bind(active_node_id)
    .execute(pool)
    .await?;
    if let Some(node_id) = route_node_id {
        sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_identities (
                 sandbox_id, route_generation, current_node_id, substrate_kind,
                 execution_generation, activation_attempt_id
             )
             VALUES ($1, 1, $2, $3, 1, $4)"
        ))
        .bind(id)
        .bind(node_id)
        .bind(SubstrateKind::Space)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
    }
    Ok(id)
}

/// The predicate a statement writes in the first `(...)` group after
/// `marker`, parentheses balanced; the statements' text has no parenthesis in
/// a literal.
fn predicate_after(statement: &str, marker: &str) -> String {
    let (_, rest) = statement
        .split_once(marker)
        .expect("the statement tests the filter");
    let mut depth = 0;
    let mut predicate = String::new();
    for character in rest.chars().skip_while(|character| *character != '(') {
        predicate.push(character);
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return predicate;
                }
            }
            _ => {}
        }
    }
    panic!("an unbalanced predicate after {marker}");
}

/// The node predicate ($10) and the assigned predicate ($11) each of the
/// four admin statements writes, by statement.
fn reported_node_predicates() -> [(&'static str, String, String); 4] {
    [
        ("page", &PAGE_SQL),
        ("unpaged list", &LIST_SQL),
        ("search page", &SEARCH_PAGE_SQL),
        ("search list", &SEARCH_LIST_SQL),
    ]
    .map(|(statement, text)| {
        (
            statement,
            predicate_after(text, "($10::UUID IS NULL OR "),
            predicate_after(text, "($11::BOOLEAN IS NULL OR "),
        )
    })
}

/// Neither predicate is ever NULL, so negated they keep exactly the rows
/// they leave out, including the sandboxes on no node. Each admin
/// statement's own pair runs negated.
#[tokio::test(flavor = "current_thread")]
async fn the_reported_node_predicates_are_never_null() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (row_node, route_node) = (NodeId::new(), NodeId::new());
    let on_row_node = seed_sandbox(pool, Some(row_node), None).await?;
    let on_route_node = seed_sandbox(pool, None, Some(route_node)).await?;
    let on_no_node = seed_sandbox(pool, None, None).await?;
    let rerouted = seed_sandbox(pool, Some(row_node), Some(route_node)).await?;

    let mut expected_off_route_node = vec![on_row_node, on_no_node];
    expected_off_route_node.sort();
    let mut expected_off_row_node = vec![on_route_node, on_no_node, rerouted];
    expected_off_row_node.sort();

    for (statement, on_node, on_a_node) in reported_node_predicates() {
        let not_on_node = format!(
            "SELECT s.id FROM sandboxes s WHERE s.org_id = $1 AND NOT {} ORDER BY s.id",
            on_node.replace("$10", "$2")
        );
        let off_route_node: Vec<SandboxId> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(not_on_node.as_str())))
                .bind(SEED_ORG_ID)
                .bind(route_node)
                .fetch_all(pool)
                .await?;
        assert_eq!(
            off_route_node, expected_off_route_node,
            "{statement}: off the route node"
        );

        let off_row_node: Vec<SandboxId> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(not_on_node.as_str())))
                .bind(SEED_ORG_ID)
                .bind(row_node)
                .fetch_all(pool)
                .await?;
        assert_eq!(
            off_row_node, expected_off_row_node,
            "{statement}: off the row node"
        );

        let not_on_a_node =
            format!("SELECT s.id FROM sandboxes s WHERE s.org_id = $1 AND NOT {on_a_node}");
        let unassigned: Vec<SandboxId> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(not_on_a_node.as_str())))
                .bind(SEED_ORG_ID)
                .fetch_all(pool)
                .await?;
        assert_eq!(unassigned, vec![on_no_node], "{statement}: on no node");
    }
    db.pool.close().await;
    Ok(())
}
