use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::{sql, types::SandboxId};
use std::collections::{BTreeMap, BTreeSet};

use sqlx::PgPool;
use uuid::Uuid;

use pplx_space_sandbox_store::fork_source_lifecycle::{
    active_fork_source_operation_exists, lock_fork_source_lifecycle,
};

pub use pplx_space_common_svc::types::{
    NetworkPolicyTarget, ReconcileElementRef, ReconcileFacet, ReconcileReason, ReconcileRef,
};
pub use pplx_space_sandbox_teardown::{
    delete_reconcile_state_for_elements, finalize_sandbox_db_delete_tx,
    sandbox_reconcile_cleanup_refs,
};

use crate::reconcile_trigger::{enqueue_triggers, enqueue_triggers_tx};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct EnqueuedNodeReconcile {
    pub node_id: NodeId,
    pub queued: u64,
}

/// Outcome of a user-initiated sandbox DB delete that has to be careful
/// about in-flight suspend-snapshot uploads.
///
/// The `BeginArtifactBackup` / `FinalizeArtifactBackup` two-call flow
/// (`crates/libs/node/api/server/src/cluster.rs`) INSERTs the `snapshots`
/// row with `state='creating'` **before** the S3 upload starts, and flips
/// to `state='ready'` only after the upload completes. If a user DELETEs
/// the sandbox in that window, [`finalize_sandbox_db_delete_tx`]'s
/// CASCADE drops the `snapshots` row and the storage-location row's
/// `snapshot_id` becomes NULL — but the S3 upload is still actively
/// writing under the prefix. The orphan-storage-location sweeper
/// in `space-cluster-controller` would then reap a half-written
/// prefix, and the upload's later writes land as permanent S3
/// orphans with no DB row to anchor them.
///
/// User-facing DELETEs must check this and refuse mid-upload. The
/// orphaned-sandbox reconciler also defers while the row is `creating`;
/// its cluster-wide `stuck_snapshot_reaper` backstop uses the upload
/// heartbeat to mark a provably abandoned upload `failed` after the
/// worker retry window. A later orphan sweep can then finalize deletion
/// without racing live object-store writes.
#[derive(Debug, thiserror::Error)]
pub enum FinalizeSandboxDbDeleteError {
    /// At least one `snapshots` row for the sandbox is in `state='creating'` —
    /// a suspend-snapshot upload is in flight. Caller (HTTP DELETE) maps
    /// this to a 409 Conflict; user retries after the upload finishes.
    #[error(
        "sandbox has a suspend-snapshot upload in flight; \
         retry the DELETE after the snapshot finalizes"
    )]
    InFlightSuspendUpload,
    #[error("sandbox has a fork operation in progress; retry the DELETE after the fork completes")]
    ForkInProgress,
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

/// Best-effort, no-lock check for "any snapshot for this sandbox is
/// currently in `state='creating'`". Used by HTTP DELETE handlers as
/// a pre-flight gate so a 409 can be returned **before** the runtime
/// (pod/VM) is torn down. The authoritative race-safe gate is inside
/// [`finalize_sandbox_db_delete_user_initiated`] — that one holds a
/// `FOR UPDATE` lock on the parent `sandboxes` row to serialize
/// against `BeginArtifactBackup`'s INSERT (which takes the implicit
/// `FOR KEY SHARE` lock on the same row via the snapshots → sandboxes
/// foreign key). This helper just catches the common case where the
/// suspend was already in flight when the DELETE arrived; the narrow
/// window between this pre-check and the locked finalize is covered
/// by the locked finalize's own check.
pub async fn any_creating_snapshot_for_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    let row: Option<(Uuid,)> = sqlx::query_as(sql!(
        "SELECT id FROM snapshots
         WHERE sandbox_id = $1
           AND state = 'creating'
           AND artifact_origin IS DISTINCT FROM 'fork'
         LIMIT 1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// Variant of [`finalize_sandbox_db_delete`] safe for user-facing
/// DELETEs: refuses to orphan storage-location rows while any
/// `snapshots` row for the sandbox is in `state='creating'`. See
/// [`FinalizeSandboxDbDeleteError::InFlightSuspendUpload`] for why.
///
/// ## Concurrency
///
/// Takes a `FOR UPDATE` lock on the parent `sandboxes` row first.
/// PostgreSQL's foreign-key enforcement makes a concurrent
/// `INSERT INTO snapshots (sandbox_id, …)` take an implicit
/// `FOR KEY SHARE` lock on the same row; `FOR UPDATE` conflicts with
/// `FOR KEY SHARE`, so a concurrent `BeginArtifactBackup` is forced
/// to serialize:
///
/// - If begin-suspend acquired its lock first, our `FOR UPDATE`
///   blocks until it commits. We then read the new `creating` row in
///   the snapshot scan below and return
///   [`FinalizeSandboxDbDeleteError::InFlightSuspendUpload`].
/// - If we acquired first, begin-suspend's INSERT blocks behind us.
///   We either commit (deleting the sandbox row, so begin-suspend's
///   FK INSERT fails) or roll back (sandbox survives, begin-suspend
///   proceeds normally after we release).
///
/// Without the parent-row lock, a `BeginArtifactBackup` arriving
/// between the snapshot scan and `finalize_sandbox_db_delete_tx`
/// would slip a fresh `state='creating'` row past the gate.
pub async fn finalize_sandbox_db_delete_user_initiated(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<(), FinalizeSandboxDbDeleteError> {
    let mut tx = pool.begin().await?;
    lock_fork_source_lifecycle(&mut tx, sandbox_id).await?;
    // Lock the parent sandbox row to serialize against any
    // concurrent BeginArtifactBackup INSERT (which acquires the
    // implicit FK FOR KEY SHARE on this row). Result is discarded —
    // if the sandbox doesn't exist (already deleted), the
    // subsequent finalize_sandbox_db_delete_tx is a no-op.
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_optional(&mut *tx)
        .await?;
    // Under the parent-row lock above, no new `creating` row can
    // appear between this SELECT and the orphan UPDATE inside
    // `finalize_sandbox_db_delete_tx`. A plain SELECT (no row lock)
    // is enough.
    let in_flight: Option<(Uuid,)> = sqlx::query_as(sql!(
        "SELECT id FROM snapshots
         WHERE sandbox_id = $1
           AND state = 'creating'
           AND artifact_origin IS DISTINCT FROM 'fork'
         LIMIT 1"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut *tx)
    .await?;
    if in_flight.is_some() {
        // Drop tx without commit; abandoning the transaction
        // releases the FOR UPDATE lock immediately. Don't issue
        // ROLLBACK explicitly — sqlx will roll back on drop.
        return Err(FinalizeSandboxDbDeleteError::InFlightSuspendUpload);
    }
    if active_fork_source_operation_exists(&mut tx, sandbox_id).await? {
        return Err(FinalizeSandboxDbDeleteError::ForkInProgress);
    }
    finalize_sandbox_db_delete_tx(&mut tx, sandbox_id).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn finalize_sandbox_db_delete(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    finalize_sandbox_db_delete_tx(&mut tx, sandbox_id).await?;
    tx.commit().await
}

pub async fn enqueue_node_reconcile_refs(
    pool: &PgPool,
    node_id: NodeId,
    refs: &[ReconcileRef],
) -> Result<EnqueuedNodeReconcile, sqlx::Error> {
    enqueue_node_reconcile_refs_for_nodes(pool, [node_id], refs)
        .await
        .map(|mut enqueued| {
            enqueued
                .pop()
                .unwrap_or(EnqueuedNodeReconcile { node_id, queued: 0 })
        })
}

pub async fn enqueue_node_reconcile_refs_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    refs: &[ReconcileRef],
) -> Result<EnqueuedNodeReconcile, sqlx::Error> {
    enqueue_node_reconcile_refs_for_nodes_tx(tx, [node_id], refs)
        .await
        .map(|mut enqueued| {
            enqueued
                .pop()
                .unwrap_or(EnqueuedNodeReconcile { node_id, queued: 0 })
        })
}

pub async fn enqueue_node_reconcile_refs_for_nodes(
    pool: &PgPool,
    node_ids: impl IntoIterator<Item = NodeId>,
    refs: &[ReconcileRef],
) -> Result<Vec<EnqueuedNodeReconcile>, sqlx::Error> {
    let node_ids = normalized_node_ids(node_ids);
    let refs = normalized_refs(refs);
    if node_ids.is_empty() || refs.is_empty() {
        return Ok(Vec::new());
    }
    enqueue_node_reconcile_refs_for_nodes_on(pool, &node_ids, &refs).await
}

pub async fn enqueue_node_reconcile_refs_for_nodes_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_ids: impl IntoIterator<Item = NodeId>,
    refs: &[ReconcileRef],
) -> Result<Vec<EnqueuedNodeReconcile>, sqlx::Error> {
    let node_ids = normalized_node_ids(node_ids);
    let refs = normalized_refs(refs);
    if node_ids.is_empty() || refs.is_empty() {
        return Ok(Vec::new());
    }
    enqueue_node_reconcile_refs_for_nodes_on(&mut **tx, &node_ids, &refs).await
}

fn normalized_node_ids(node_ids: impl IntoIterator<Item = NodeId>) -> Vec<NodeId> {
    node_ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalized_refs(refs: &[ReconcileRef]) -> Vec<(String, String)> {
    refs.iter()
        .map(|reference| {
            (
                reference.facet_str().to_string(),
                reference.element_string().unwrap_or_default(),
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn enqueue_node_reconcile_refs_for_nodes_on<'e, E>(
    executor: E,
    node_ids: &[NodeId],
    refs: &[(String, String)],
) -> Result<Vec<EnqueuedNodeReconcile>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let facets = refs
        .iter()
        .map(|(facet, _)| facet.clone())
        .collect::<Vec<_>>();
    let elements = refs
        .iter()
        .map(|(_, element)| element.clone())
        .collect::<Vec<_>>();
    let rows = upsert_node_reconcile_refs(
        executor,
        node_ids,
        &facets,
        &elements,
        current_storable_traceparent().as_deref(),
    )
    .await;
    match rows {
        Ok(rows) => {
            let mut by_node = BTreeMap::<NodeId, u64>::new();
            for (node_id, facet, queued) in rows {
                let queued = u64::try_from(queued).unwrap_or(0);
                if queued > 0 {
                    pplx_space_metrics::reconcile::queued_count(&facet, "ok", queued);
                }
                by_node
                    .entry(node_id)
                    .and_modify(|count| *count = count.saturating_add(queued))
                    .or_insert(queued);
            }
            Ok(by_node
                .into_iter()
                .map(|(node_id, queued)| EnqueuedNodeReconcile { node_id, queued })
                .collect())
        }
        Err(error) => {
            for _node_id in node_ids {
                for (facet, _) in refs {
                    pplx_space_metrics::reconcile::queued(facet, "error");
                }
            }
            Err(error)
        }
    }
}

/// The writer's trace context, for the pass that applies the refs to link
/// to. Refs queued outside any traced span save none.
fn current_storable_traceparent() -> Option<String> {
    pplx_space_observability::trace_propagation::current_traceparent()
        .filter(|traceparent| pplx_space_observability::is_storable_traceparent(traceparent))
}

/// Queue each ref on each node, reviving a dead row and keeping a live one's
/// place, and return how many rows each node and facet queued.
///
/// `traceparent` is saved with each row. A writer without one keeps the
/// context a row already has, so a background write that coalesces onto a
/// request's pending ref does not erase the request's context.
async fn upsert_node_reconcile_refs<'e, E>(
    executor: E,
    node_ids: &[NodeId],
    facets: &[String],
    elements: &[String],
    traceparent: Option<&str>,
) -> Result<Vec<(NodeId, String, i64)>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_as::<_, (NodeId, String, i64)>(sql!(
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
                 traceparent = COALESCE(
                     EXCLUDED.traceparent,
                     node_reconcile_queue.traceparent
                 )
             RETURNING node_id, facet
         )
         SELECT node_id, facet, COUNT(*)::BIGINT AS queued
         FROM upserted
         GROUP BY node_id, facet
         ORDER BY node_id, facet"
    ))
    .bind(node_ids)
    .bind(facets)
    .bind(elements)
    .bind(traceparent)
    .fetch_all(executor)
    .await
}

pub async fn enqueue_node_reconcile_refs_and_trigger(
    pool: &PgPool,
    node_id: NodeId,
    refs: &[ReconcileRef],
    reason: ReconcileReason,
) -> Result<EnqueuedNodeReconcile, sqlx::Error> {
    let enqueued = enqueue_node_reconcile_refs(pool, node_id, refs).await?;
    enqueue_triggers(pool, [enqueued.node_id], reason).await?;
    Ok(enqueued)
}

pub async fn enqueue_node_reconcile_refs_and_trigger_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    refs: &[ReconcileRef],
    reason: ReconcileReason,
) -> Result<EnqueuedNodeReconcile, sqlx::Error> {
    let enqueued = enqueue_node_reconcile_refs_tx(tx, node_id, refs).await?;
    enqueue_triggers_tx(tx, [enqueued.node_id], reason).await?;
    Ok(enqueued)
}

pub async fn enqueue_sandbox_reconcile(
    pool: &PgPool,
    sandbox_id: SandboxId,
    refs: &[ReconcileRef],
) -> Result<Option<EnqueuedNodeReconcile>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let enqueued = enqueue_sandbox_reconcile_tx(&mut tx, sandbox_id, refs).await?;
    tx.commit().await?;
    Ok(enqueued)
}

pub async fn enqueue_sandbox_reconcile_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    refs: &[ReconcileRef],
) -> Result<Option<EnqueuedNodeReconcile>, sqlx::Error> {
    let node_id: Option<NodeId> = sqlx::query_scalar(sql!(
        "SELECT active_node_id FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut **tx)
    .await?
    .flatten();
    let Some(node_id) = node_id else {
        return Ok(None);
    };
    enqueue_node_reconcile_refs_tx(tx, node_id, refs)
        .await
        .map(Some)
}

pub async fn enqueue_sandbox_reconcile_and_trigger(
    pool: &PgPool,
    sandbox_id: SandboxId,
    refs: &[ReconcileRef],
    reason: ReconcileReason,
) -> Result<Option<EnqueuedNodeReconcile>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let enqueued =
        enqueue_sandbox_reconcile_and_trigger_tx(&mut tx, sandbox_id, refs, reason).await?;
    tx.commit().await?;
    Ok(enqueued)
}

pub async fn enqueue_sandbox_reconcile_and_trigger_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    refs: &[ReconcileRef],
    reason: ReconcileReason,
) -> Result<Option<EnqueuedNodeReconcile>, sqlx::Error> {
    let enqueued = enqueue_sandbox_reconcile_tx(tx, sandbox_id, refs).await?;
    if let Some(enqueued) = enqueued {
        enqueue_triggers_tx(tx, [enqueued.node_id], reason).await?;
        return Ok(Some(enqueued));
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
