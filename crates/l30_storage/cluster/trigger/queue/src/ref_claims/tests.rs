use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db};
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;
use crate::{QueuedRefConflict, queue_node_reconcile_refs};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FACET: &str = "network-policy";
const STALE_SECS: i64 = 60;

fn element() -> String {
    format!("sandbox:{}", Uuid::now_v7())
}

/// The claimed elements, sorted: `RETURNING` hands rows back in no set order.
fn claimed_elements(claimed: &[ClaimedReconcileRef]) -> Vec<Option<String>> {
    let mut elements = claimed
        .iter()
        .map(|claimed| claimed.element.clone())
        .collect::<Vec<_>>();
    elements.sort();
    elements
}

/// Every column a claim transition writes.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
struct QueuedRef {
    generation: i64,
    claim_token: String,
    claim_generation: i64,
    claimed_at: Option<OffsetDateTime>,
    next_attempt_at: OffsetDateTime,
    attempts: i32,
    last_error: String,
    dead_at: Option<OffsetDateTime>,
    dead_reason: String,
    updated_at: OffsetDateTime,
}

async fn queued_ref(
    pool: &PgPool,
    node_id: NodeId,
    element: &str,
) -> Result<Option<QueuedRef>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT generation, claim_token, claim_generation, claimed_at, next_attempt_at,
                attempts, last_error, dead_at, dead_reason, updated_at
         FROM node_reconcile_queue
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET)
    .bind(element)
    .fetch_optional(pool)
    .await
}

async fn queue(pool: &PgPool, node_id: NodeId, element: &str) -> TestResult {
    queue_node_reconcile_refs(
        pool,
        &[node_id],
        &[(FACET.to_owned(), element.to_owned())],
        QueuedRefConflict::KeepProgress,
    )
    .await?;
    Ok(())
}

/// A node with one ref queued and claimed by the returned token.
async fn claimed_ref(pool: &PgPool) -> TestResult<(NodeId, String, String)> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    let element = element();
    queue(pool, node_id, &element).await?;
    let claim_token = format!("{node_id}-{}", Uuid::now_v7());
    let claimed = claim_node_refs(pool, node_id, 16, STALE_SECS, &claim_token).await?;
    assert_eq!(claimed.len(), 1);
    Ok((node_id, element, claim_token))
}

/// The ways a completing transition can lose: another claim's token, and a
/// refresh after the claim moved `generation` past `claim_generation`.
#[derive(Debug, Clone, Copy)]
enum Loser {
    WrongToken,
    Refreshed,
}

/// A claimed ref set up to lose as `loser` does, with the token the
/// transition is to run with.
async fn losing_ref(pool: &PgPool, loser: Loser) -> TestResult<(NodeId, String, String)> {
    let (node_id, element, claim_token) = claimed_ref(pool).await?;
    match loser {
        Loser::WrongToken => Ok((node_id, element, format!("{node_id}-{}", Uuid::now_v7()))),
        Loser::Refreshed => {
            queue(pool, node_id, &element).await?;
            let refreshed = queued_ref(pool, node_id, &element)
                .await?
                .ok_or("refreshed ref is gone")?;
            assert!(refreshed.generation > refreshed.claim_generation);
            Ok((node_id, element, claim_token))
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn claim_takes_due_unclaimed_and_stale_refs_oldest_first() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let [oldest, held, stale, whole_facet] = ["a", "b", "c", ""].map(|suffix| {
        if suffix.is_empty() {
            String::new()
        } else {
            format!("sandbox:{suffix}-{}", Uuid::now_v7())
        }
    });
    for (element, age_secs) in [(&oldest, 40), (&held, 30), (&stale, 20), (&whole_facet, 10)] {
        sqlx::query(sql!(
            "INSERT INTO node_reconcile_queue (node_id, facet, element, requested_at, updated_at)
             VALUES ($1, $2, $3, NOW() - ($4 * INTERVAL '1 second'), NOW())"
        ))
        .bind(node_id)
        .bind(FACET)
        .bind(element)
        .bind(age_secs)
        .execute(&db.pool)
        .await?;
    }
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET claim_token = 'held', claimed_at = NOW() - ($3 * INTERVAL '1 second')
         WHERE node_id = $1 AND element = ANY($2)"
    ))
    .bind(node_id)
    .bind(vec![held.clone(), stale.clone()])
    .bind(0_i64)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET claimed_at = NOW() - ($3 * INTERVAL '1 second')
         WHERE node_id = $1 AND element = $2"
    ))
    .bind(node_id)
    .bind(&stale)
    .bind(STALE_SECS + 1)
    .execute(&db.pool)
    .await?;

    // A limit below the three due refs shows which the claim takes first.
    let first = claim_node_refs(&db.pool, node_id, 2, STALE_SECS, "mine").await?;
    let rest = claim_node_refs(&db.pool, node_id, 16, STALE_SECS, "mine").await?;

    assert_eq!(
        claimed_elements(&first),
        [Some(oldest.clone()), Some(stale.clone())]
    );
    assert_eq!(claimed_elements(&rest), [None::<String>]);
    for element in [&oldest, &stale, &whole_facet] {
        let row = queued_ref(&db.pool, node_id, element)
            .await?
            .ok_or("claimed ref is gone")?;
        assert_eq!(row.claim_token, "mine");
        assert_eq!(row.claim_generation, row.generation);
    }
    let held = queued_ref(&db.pool, node_id, &held)
        .await?
        .ok_or("held ref is gone")?;
    assert_eq!(held.claim_token, "held");

    let limited = claim_node_refs(&db.pool, node_id, 1, STALE_SECS, "again").await?;
    assert!(limited.is_empty(), "every due ref is freshly claimed");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn complete_deletes_the_ref_its_claim_holds() -> TestResult {
    let db = pg_test_db().await?;
    let (node_id, element, claim_token) = claimed_ref(&db.pool).await?;

    let completed =
        complete_claimed_ref(&db.pool, node_id, FACET, Some(&element), &claim_token).await?;

    assert_eq!(completed, 1);
    assert_eq!(queued_ref(&db.pool, node_id, &element).await?, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn complete_loses_to_another_claim_or_a_refresh() -> TestResult {
    let db = pg_test_db().await?;
    for loser in [Loser::WrongToken, Loser::Refreshed] {
        let (node_id, element, claim_token) = losing_ref(&db.pool, loser).await?;
        let before = queued_ref(&db.pool, node_id, &element).await?;

        let completed =
            complete_claimed_ref(&db.pool, node_id, FACET, Some(&element), &claim_token).await?;

        assert_eq!(completed, 0, "{loser:?}");
        assert_eq!(
            queued_ref(&db.pool, node_id, &element).await?,
            before,
            "{loser:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn release_frees_a_refreshed_ref_but_not_another_claim() -> TestResult {
    let db = pg_test_db().await?;
    let (node_id, element, claim_token) = losing_ref(&db.pool, Loser::Refreshed).await?;

    let released =
        release_claimed_ref(&db.pool, node_id, FACET, Some(&element), &claim_token).await?;

    assert_eq!(released, 1);
    let row = queued_ref(&db.pool, node_id, &element)
        .await?
        .ok_or("released ref is gone")?;
    assert_eq!(row.claim_token, "");
    assert_eq!(row.claim_generation, 0);
    assert_eq!(row.claimed_at, None);

    let (node_id, element, wrong_token) = losing_ref(&db.pool, Loser::WrongToken).await?;
    let before = queued_ref(&db.pool, node_id, &element).await?;
    let released =
        release_claimed_ref(&db.pool, node_id, FACET, Some(&element), &wrong_token).await?;
    assert_eq!(released, 0);
    assert_eq!(queued_ref(&db.pool, node_id, &element).await?, before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dead_letter_records_the_failure_and_kills_the_ref() -> TestResult {
    let db = pg_test_db().await?;
    let (node_id, element, claim_token) = claimed_ref(&db.pool).await?;

    let dead = dead_letter_claimed_ref(
        &db.pool,
        node_id,
        FACET,
        Some(&element),
        &claim_token,
        "apply failed",
    )
    .await?;

    assert_eq!(dead, 1);
    let row = queued_ref(&db.pool, node_id, &element)
        .await?
        .ok_or("dead ref is gone")?;
    assert_eq!(row.attempts, 1);
    assert_eq!(row.last_error, "apply failed");
    assert_eq!(row.dead_reason, "apply failed");
    assert!(row.dead_at.is_some());
    assert_eq!(row.claim_token, "");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dead_letter_loses_to_another_claim_or_a_refresh() -> TestResult {
    let db = pg_test_db().await?;
    for loser in [Loser::WrongToken, Loser::Refreshed] {
        let (node_id, element, claim_token) = losing_ref(&db.pool, loser).await?;
        let before = queued_ref(&db.pool, node_id, &element).await?;

        let dead = dead_letter_claimed_ref(
            &db.pool,
            node_id,
            FACET,
            Some(&element),
            &claim_token,
            "apply failed",
        )
        .await?;

        assert_eq!(dead, 0, "{loser:?}");
        assert_eq!(
            queued_ref(&db.pool, node_id, &element).await?,
            before,
            "{loser:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retry_records_the_failure_and_backs_off() -> TestResult {
    let db = pg_test_db().await?;
    let (node_id, element, claim_token) = claimed_ref(&db.pool).await?;

    let retried = retry_claimed_ref(
        &db.pool,
        node_id,
        FACET,
        Some(&element),
        &claim_token,
        "apply failed",
        3_600,
    )
    .await?;

    assert_eq!(retried, 1);
    let row = queued_ref(&db.pool, node_id, &element)
        .await?
        .ok_or("retried ref is gone")?;
    assert_eq!(row.attempts, 1);
    assert_eq!(row.last_error, "apply failed");
    assert_eq!(row.dead_at, None);
    assert_eq!(row.claim_token, "");
    assert!(row.next_attempt_at > row.updated_at);
    let claimed = claim_node_refs(&db.pool, node_id, 16, STALE_SECS, "later").await?;
    assert!(claimed.is_empty(), "the ref is backing off");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retry_loses_to_another_claim_or_a_refresh() -> TestResult {
    let db = pg_test_db().await?;
    for loser in [Loser::WrongToken, Loser::Refreshed] {
        let (node_id, element, claim_token) = losing_ref(&db.pool, loser).await?;
        let before = queued_ref(&db.pool, node_id, &element).await?;

        let retried = retry_claimed_ref(
            &db.pool,
            node_id,
            FACET,
            Some(&element),
            &claim_token,
            "apply failed",
            3_600,
        )
        .await?;

        assert_eq!(retried, 0, "{loser:?}");
        assert_eq!(
            queued_ref(&db.pool, node_id, &element).await?,
            before,
            "{loser:?}"
        );
    }
    Ok(())
}
