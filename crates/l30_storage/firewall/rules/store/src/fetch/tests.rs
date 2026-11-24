use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_firewall_rules_protocol::decode_revision;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

/// Two rules out of name order, the first with two comparisons.
const ORDERED: &str = concat!(
    r#"[{"id":"web","all_of":["#,
    r#"{"left":{"payload":{"protocol":"tcp","field":"sport"}},"op":"==","right":1024},"#,
    r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]},"#,
    r#"{"id":"dns","all_of":["#,
    r#"{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}]"#,
);

fn issued_at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(1_790_000_000_123_456_000).unwrap()
}

async fn assign(
    pool: &PgPool,
    node: NodeId,
    generation: i64,
    rules: &str,
) -> TestResult<RevisionId> {
    let revision_id = RevisionId(uuid::Uuid::now_v7());
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, 1, $2::jsonb)"
    ))
    .bind(revision_id)
    .bind(rules)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_assignments (node_id, assignment_generation, revision_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(node)
    .bind(generation)
    .bind(revision_id)
    .execute(pool)
    .await?;
    Ok(revision_id)
}

/// An observation row with the given pending round, with the generation it
/// asks about, and accepted round.
async fn observe(
    pool: &PgPool,
    node: NodeId,
    pending: Option<(i64, i64)>,
    accepted: Option<i64>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_observations (
             node_id, manager_epoch, pending_round, pending_round_issued_at,
             pending_assignment_generation,
             accepted_round, accepted_round_issued_at, accepted_report
         )
         VALUES ($1, 1, $2, CASE WHEN $2 IS NOT NULL THEN $4 END, $5,
                 $3, CASE WHEN $3 IS NOT NULL THEN $4 END,
                 CASE WHEN $3 IS NOT NULL THEN '{}'::jsonb END)"
    ))
    .bind(node)
    .bind(pending.map(|(round, _)| round))
    .bind(accepted)
    .bind(issued_at())
    .bind(pending.map(|(_, generation)| generation))
    .execute(pool)
    .await?;
    Ok(())
}

/// Each part is read for this node only, from its own row: a round without an
/// assignment survives, an empty revision is an assignment, rule order and
/// the round's issue time are kept, the round's generation is its own (zero
/// included) rather than the assignment's, and accepted evidence is no
/// pending round.
#[tokio::test]
async fn fetch_returns_exactly_the_nodes_own_parts() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // Another node's work, which no case may see.
    let other = NodeId::new();
    assign(pool, other, 9, "[]").await?;
    observe(pool, other, Some((99, 9)), None).await?;

    for (case, assignment, pending, accepted) in [
        ("neither", None, None, None),
        ("empty revision", Some((0, "[]")), None, None),
        ("round only, for generation zero", None, Some((7, 0)), None),
        (
            "both, the round for an earlier generation",
            Some((13, ORDERED)),
            Some((7, 12)),
            None,
        ),
        ("accepted evidence only", None, None, Some(6)),
    ] {
        let node = NodeId::new();
        let assignment = match assignment {
            Some((generation, rules)) => Some(Assignment {
                generation: generation.try_into()?,
                revision_id: assign(pool, node, generation, rules).await?,
                revision: decode_revision(&format!(r#"{{"schema_version":1,"rules":{rules}}}"#))?,
            }),
            None => None,
        };
        if pending.is_some() || accepted.is_some() {
            observe(pool, node, pending, accepted).await?;
        }
        let expected = NodeInstructions {
            assignment,
            pending_round: pending.map(|(round, generation)| PendingRound {
                round: round.try_into().unwrap(),
                issued_at: Timestamp::from_nanosecond(issued_at().unix_timestamp_nanos()).unwrap(),
                assignment_generation: generation.try_into().unwrap(),
            }),
        };
        let fetched = fetch_instructions(&mut *pool.acquire().await?, node).await?;
        assert_eq!(fetched, expected, "{case}");
    }
    Ok(())
}

/// A stored revision the strict decoder refuses is an error, not no work.
#[tokio::test]
async fn an_undecodable_revision_is_an_error() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let revision = assign(pool, node, 1, r#"[{"id":"r","all_of":[],"extra":1}]"#).await?;
    let error = fetch_instructions(&mut *pool.acquire().await?, node)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            StoredStateError::StoredRevision { id, .. } if id == revision
        ),
        "{error}"
    );
    Ok(())
}
