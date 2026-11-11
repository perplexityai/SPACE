//! The `node_reconcile_queue` enqueue: the refs a node's reconciler applies
//! after the trigger for that node fires, each with the trace context of the
//! writer that asked for it.

use pplx_space_common_core::{sql, types::NodeId};

/// What an enqueue does to a ref that is already queued on the node.
///
/// Either way the ref's `generation` moves, so a reconciler holding a claim
/// on it keeps the claim, misses the `generation = claim_generation` guard
/// when it completes, and releases the ref to run again. A dead ref is
/// revived.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QueuedRefConflict {
    /// Coalesce onto the queued ref. A ref nothing has claimed or failed
    /// becomes due at once; a claimed or retrying ref keeps its attempts,
    /// last error and backoff; every live ref keeps its `requested_at`, which
    /// orders the node's claims and feeds the reconciler's queue-lag metrics.
    KeepProgress,
    /// Queue the ref as a new request: no attempts or last error, and a fresh
    /// `requested_at`, which puts it behind the refs already queued on the
    /// node. It is due at once even while backing off; a claimed ref keeps
    /// its claim and runs again once the claimant releases it or the claim
    /// goes stale. Refs for inventory a node publishes for the first time
    /// use it.
    RetryNow,
}

/// How many refs one enqueue queued on a node for a facet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct QueuedReconcileRefs {
    pub node_id: NodeId,
    pub facet: String,
    pub queued: u64,
}

/// Intent: queue each `(facet, element)` ref on each node, as stored (an
/// empty element is a whole-facet ref), with `conflict` deciding what
/// happens to a ref already queued.
///
/// The current span's trace context is saved with each ref for the pass
/// that applies it to link to. A writer without one keeps the context a ref
/// already has, so a background write that coalesces onto a request's
/// pending ref does not erase the request's context, and an unsampled
/// writer's context never replaces a sampled one, so the pass that applies
/// the ref still follows the sampled request.
///
/// No guard of its own: it does not check that a node can run the refs; the
/// `node_id` foreign key only requires the node row to exist. Most callers
/// then enqueue the node's trigger (`enqueue_triggers*`), which fails for a
/// node that is not a worker or builder; that undoes these refs only when
/// both run in one transaction. Not every caller queues a trigger
/// (`enqueue_sandbox_reconcile` does not).
pub async fn queue_node_reconcile_refs<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    node_ids: &[NodeId],
    refs: &[(String, String)],
    conflict: QueuedRefConflict,
) -> Result<Vec<QueuedReconcileRefs>, sqlx::Error> {
    if node_ids.is_empty() || refs.is_empty() {
        return Ok(Vec::new());
    }
    let (facets, elements): (Vec<&str>, Vec<&str>) = refs
        .iter()
        .map(|(facet, element)| (facet.as_str(), element.as_str()))
        .unzip();
    upsert_node_reconcile_refs(
        executor,
        node_ids,
        &facets,
        &elements,
        current_storable_traceparent().as_deref(),
        conflict,
    )
    .await
}

/// The writer's trace context, for the pass that applies the refs to link
/// to. Refs queued outside any traced span save none.
fn current_storable_traceparent() -> Option<String> {
    pplx_space_observability::trace_propagation::current_traceparent()
        .filter(|traceparent| pplx_space_observability::is_storable_traceparent(traceparent))
}

async fn upsert_node_reconcile_refs<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    node_ids: &[NodeId],
    facets: &[&str],
    elements: &[&str],
    traceparent: Option<&str>,
    conflict: QueuedRefConflict,
) -> Result<Vec<QueuedReconcileRefs>, sqlx::Error> {
    let query = match conflict {
        QueuedRefConflict::KeepProgress => sqlx::query_as::<_, (NodeId, String, i64)>(sql!(
            "WITH nodes AS (
                 SELECT DISTINCT node_id
                 FROM UNNEST($1::UUID[]) AS nodes(node_id)
             ),
             refs AS (
                 SELECT DISTINCT facet, element
                 FROM UNNEST($2::TEXT[], $3::TEXT[]) AS refs(facet, element)
             ),
             upserted AS (
                 INSERT INTO node_reconcile_queue (
                     node_id, facet, element, requested_at, updated_at, traceparent
                 )
                 SELECT nodes.node_id, refs.facet, refs.element, NOW(), NOW(), $4
                 FROM nodes
                 CROSS JOIN refs
                 ON CONFLICT (node_id, facet, element) DO UPDATE
                 SET generation = node_reconcile_queue.generation + 1,
                     requested_at = CASE
                         WHEN node_reconcile_queue.dead_at IS NOT NULL
                         THEN EXCLUDED.requested_at
                         ELSE node_reconcile_queue.requested_at
                     END,
                     next_attempt_at = CASE
                         WHEN node_reconcile_queue.dead_at IS NOT NULL
                         THEN NOW()
                         WHEN node_reconcile_queue.claim_token = ''
                              AND node_reconcile_queue.attempts = 0
                         THEN NOW()
                         ELSE node_reconcile_queue.next_attempt_at
                     END,
                     attempts = CASE
                         WHEN node_reconcile_queue.dead_at IS NOT NULL THEN 0
                         ELSE node_reconcile_queue.attempts
                     END,
                     last_error = CASE
                         WHEN node_reconcile_queue.dead_at IS NOT NULL
                              OR node_reconcile_queue.attempts = 0
                         THEN ''
                         ELSE node_reconcile_queue.last_error
                     END,
                     dead_at = NULL,
                     dead_reason = '',
                     updated_at = NOW(),
                     traceparent = CASE
                         WHEN EXCLUDED.traceparent IS NULL
                         THEN node_reconcile_queue.traceparent
                         WHEN get_byte(decode(right(node_reconcile_queue.traceparent, 2), 'hex'), 0) & 1 = 1
                              AND get_byte(decode(right(EXCLUDED.traceparent, 2), 'hex'), 0) & 1 = 0
                         THEN node_reconcile_queue.traceparent
                         ELSE EXCLUDED.traceparent
                     END
                 RETURNING node_id, facet
             )
             SELECT node_id, facet, COUNT(*)::BIGINT AS queued
             FROM upserted
             GROUP BY node_id, facet
             ORDER BY node_id, facet"
        )),
        QueuedRefConflict::RetryNow => sqlx::query_as::<_, (NodeId, String, i64)>(sql!(
            "WITH nodes AS (
                 SELECT DISTINCT node_id
                 FROM UNNEST($1::UUID[]) AS nodes(node_id)
             ),
             refs AS (
                 SELECT DISTINCT facet, element
                 FROM UNNEST($2::TEXT[], $3::TEXT[]) AS refs(facet, element)
             ),
             upserted AS (
                 INSERT INTO node_reconcile_queue (
                     node_id, facet, element, requested_at, updated_at, traceparent
                 )
                 SELECT nodes.node_id, refs.facet, refs.element, NOW(), NOW(), $4
                 FROM nodes
                 CROSS JOIN refs
                 ON CONFLICT (node_id, facet, element) DO UPDATE
                 SET generation = node_reconcile_queue.generation + 1,
                     requested_at = EXCLUDED.requested_at,
                     next_attempt_at = NOW(),
                     attempts = 0,
                     last_error = '',
                     dead_at = NULL,
                     dead_reason = '',
                     updated_at = NOW(),
                     traceparent = CASE
                         WHEN EXCLUDED.traceparent IS NULL
                         THEN node_reconcile_queue.traceparent
                         WHEN get_byte(decode(right(node_reconcile_queue.traceparent, 2), 'hex'), 0) & 1 = 1
                              AND get_byte(decode(right(EXCLUDED.traceparent, 2), 'hex'), 0) & 1 = 0
                         THEN node_reconcile_queue.traceparent
                         ELSE EXCLUDED.traceparent
                     END
                 RETURNING node_id, facet
             )
             SELECT node_id, facet, COUNT(*)::BIGINT AS queued
             FROM upserted
             GROUP BY node_id, facet
             ORDER BY node_id, facet"
        )),
    };
    let rows = query
        .bind(node_ids)
        .bind(facets)
        .bind(elements)
        .bind(traceparent)
        .fetch_all(executor)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(node_id, facet, queued)| QueuedReconcileRefs {
            node_id,
            facet,
            queued: queued.cast_unsigned(),
        })
        .collect())
}

#[cfg(test)]
mod tests;
