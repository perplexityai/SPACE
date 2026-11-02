use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db};
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CONFLICTS: [QueuedRefConflict; 2] =
    [QueuedRefConflict::KeepProgress, QueuedRefConflict::RetryNow];

fn element() -> String {
    format!("sandbox:{}", Uuid::now_v7())
}

async fn saved_traceparent(
    pool: &PgPool,
    node_id: NodeId,
    element: &str,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT traceparent FROM node_reconcile_queue
         WHERE node_id = $1 AND facet = 'network-policy' AND element = $2"
    ))
    .bind(node_id)
    .bind(element)
    .fetch_one(pool)
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn queued_ref_keeps_the_latest_writer_traceparent_unless_it_drops_a_sampled_one() -> TestResult
{
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let unsampled = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    let later_unsampled = "00-5bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    let sampled = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let later_sampled = "00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let node_ids = [node_id];
    for conflict in CONFLICTS {
        let element = element();
        let elements = [element.as_str()];
        let queue = |traceparent: Option<&'static str>| {
            upsert_node_reconcile_refs(
                &db.pool,
                &node_ids,
                &["network-policy"],
                &elements,
                traceparent,
                conflict,
            )
        };
        let saved = || saved_traceparent(&db.pool, node_id, &element);

        queue(Some(unsampled)).await?;
        assert_eq!(saved().await?.as_deref(), Some(unsampled), "{conflict:?}");
        queue(Some(later_unsampled)).await?;
        assert_eq!(
            saved().await?.as_deref(),
            Some(later_unsampled),
            "{conflict:?}"
        );
        queue(Some(sampled)).await?;
        assert_eq!(saved().await?.as_deref(), Some(sampled), "{conflict:?}");
        // A writer without trace context coalesces without erasing it.
        queue(None).await?;
        assert_eq!(saved().await?.as_deref(), Some(sampled), "{conflict:?}");
        // An unsampled writer does not replace the sampled request's context.
        queue(Some(later_unsampled)).await?;
        assert_eq!(saved().await?.as_deref(), Some(sampled), "{conflict:?}");
        queue(Some(later_sampled)).await?;
        assert_eq!(
            saved().await?.as_deref(),
            Some(later_sampled),
            "{conflict:?}"
        );
    }
    Ok(())
}

/// A queued ref's state before an enqueue lands on it.
#[derive(Debug, Clone, Copy)]
enum Queued {
    /// Waiting for its first claim.
    Pending,
    /// Claimed by a reconciler, never failed.
    Claimed,
    /// Failed twice, backing off.
    Retrying,
    /// Gave up after too many failures.
    Dead,
}

/// The ref after the enqueue, by the columns the conflict modes treat differently.
#[derive(Debug, PartialEq, Eq)]
struct Progress {
    attempts: i32,
    last_error: &'static str,
    claim_token: &'static str,
    /// The enqueue restamped `requested_at`.
    requested_now: bool,
    /// The enqueue made the ref due at once.
    due_now: bool,
}

async fn queue_ref_in_state(
    pool: &PgPool,
    node_id: NodeId,
    element: &str,
    state: Queued,
) -> TestResult {
    let (attempts, last_error, claim_token, next_attempt_in, dead) = match state {
        Queued::Pending => (0, "", "", "-1 hour", false),
        Queued::Claimed => (0, "", "claimant", "-1 hour", false),
        Queued::Retrying => (2, "synthetic failure", "", "10 minutes", false),
        Queued::Dead => (8, "synthetic failure", "", "-1 hour", true),
    };
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id, facet, element, generation, attempts, last_error,
             claim_token, claim_generation, claimed_at, next_attempt_at,
             requested_at, updated_at, dead_at, dead_reason
         )
         VALUES (
             $1, 'network-policy', $2, 3, $3, $4,
             $5, 3, CASE WHEN $5 = '' THEN NULL ELSE NOW() END,
             NOW() + $6::INTERVAL, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
             CASE WHEN $7 THEN NOW() END, CASE WHEN $7 THEN 'synthetic failure' ELSE '' END
         )"
    ))
    .bind(node_id)
    .bind(element)
    .bind(attempts)
    .bind(last_error)
    .bind(claim_token)
    .bind(next_attempt_in)
    .bind(dead)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn each_conflict_mode_moves_a_queued_ref_as_documented() -> TestResult {
    use Queued::*;
    use QueuedRefConflict::*;

    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let progress = |attempts, last_error, claim_token, requested_now, due_now| Progress {
        attempts,
        last_error,
        claim_token,
        requested_now,
        due_now,
    };
    let retried = |claim_token| progress(0, "", claim_token, true, true);
    for (conflict, state, expected) in [
        (KeepProgress, Pending, progress(0, "", "", false, true)),
        (
            KeepProgress,
            Claimed,
            progress(0, "", "claimant", false, false),
        ),
        (
            KeepProgress,
            Retrying,
            progress(2, "synthetic failure", "", false, false),
        ),
        (KeepProgress, Dead, retried("")),
        (RetryNow, Pending, retried("")),
        (RetryNow, Claimed, retried("claimant")),
        (RetryNow, Retrying, retried("")),
        (RetryNow, Dead, retried("")),
    ] {
        let element = element();
        queue_ref_in_state(&db.pool, node_id, &element, state).await?;
        let queued = upsert_node_reconcile_refs(
            &db.pool,
            &[node_id],
            &["network-policy"],
            &[element.as_str()],
            None,
            conflict,
        )
        .await?;
        assert_eq!(
            queued,
            [QueuedReconcileRefs {
                node_id,
                facet: "network-policy".to_string(),
                queued: 1,
            }],
            "{conflict:?} {state:?}"
        );
        let (generation, dead, attempts, last_error, claim_token, requested_now, due_now): (
            i64,
            bool,
            i32,
            String,
            String,
            bool,
            bool,
        ) = sqlx::query_as(sql!(
            "SELECT generation, dead_at IS NOT NULL, attempts, last_error, claim_token,
                    requested_at = updated_at, next_attempt_at = updated_at
             FROM node_reconcile_queue
             WHERE node_id = $1 AND facet = 'network-policy' AND element = $2"
        ))
        .bind(node_id)
        .bind(&element)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!((generation, dead), (4, false), "{conflict:?} {state:?}");
        assert_eq!(
            (
                attempts,
                last_error.as_str(),
                claim_token.as_str(),
                requested_now,
                due_now
            ),
            (
                expected.attempts,
                expected.last_error,
                expected.claim_token,
                expected.requested_now,
                expected.due_now,
            ),
            "{conflict:?} {state:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn queueing_saves_the_current_span_trace_context() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let caller = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let caller_trace = "00-0af7651916cd43dd8448eb211c80319c-";
    for conflict in CONFLICTS {
        let element = element();
        let refs = [("network-policy".to_string(), element.clone())];
        pplx_space_observability::test_trace::in_remote_parent_span(
            caller,
            queue_node_reconcile_refs(&db.pool, &[node_id], &refs, conflict),
        )
        .await?;
        let saved = saved_traceparent(&db.pool, node_id, &element).await?;
        assert!(
            saved
                .as_deref()
                .is_some_and(|saved| saved.starts_with(caller_trace)),
            "{conflict:?}: {saved:?}"
        );
    }
    Ok(())
}
