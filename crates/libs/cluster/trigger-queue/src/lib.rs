use pplx_space_common_core::sql;
use std::collections::{BTreeSet, HashSet};

use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::{NodeManagedBy, NodeRole as SpaceNodeRole, SubstrateKind};
use pplx_space_common_svc::types::{
    RECONCILE_REASON_MAX_CHARS, ReconcileReason,
    normalize_reconcile_reason as normalize_shared_reconcile_reason,
};
use sqlx::{FromRow, PgPool};

const TRIGGER_CLAIM_STALE_SECS: i64 = 60;
const TRIGGER_MAX_ATTEMPTS: i32 = 16;
const TRIGGER_FAILURE_BACKOFF_MAX_SECS: i32 = 60;

#[derive(Debug, Clone, FromRow)]
struct ClaimedStateReconciliationTriggerRow {
    node_id: NodeId,
    reasons: Vec<ReconcileReason>,
    diagnostic_reason: String,
}

#[derive(Debug, Clone)]
pub struct ClaimedStateReconciliationTrigger {
    pub node_id: NodeId,
    pub reasons: Vec<ReconcileReason>,
    pub diagnostic_reason: String,
}

#[derive(Debug, Clone)]
pub struct StateReconciliationTriggerClaim {
    pub claim_token: String,
    pub triggers: Vec<ClaimedStateReconciliationTrigger>,
}

#[derive(Debug, Clone, FromRow)]
pub struct TriggerTargetRow {
    pub id: NodeId,
    pub role: SpaceNodeRole,
    pub managed_by: NodeManagedBy,
    pub direct_endpoint: Option<String>,
    pub node_name: Option<String>,
}

pub struct TriggerTargets {
    pub triggerable: Vec<TriggerTargetRow>,
    pub permanently_unavailable: Vec<NodeId>,
}

pub async fn enqueue_triggers(
    pool: &PgPool,
    node_ids: impl IntoIterator<Item = NodeId>,
    reason: ReconcileReason,
) -> Result<u64, sqlx::Error> {
    let node_ids = normalized_node_ids(node_ids);
    if node_ids.is_empty() {
        return Ok(0);
    }
    enqueue_triggers_on(pool, &node_ids, reason).await
}

pub async fn enqueue_triggers_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_ids: impl IntoIterator<Item = NodeId>,
    reason: ReconcileReason,
) -> Result<u64, sqlx::Error> {
    let node_ids = normalized_node_ids(node_ids);
    if node_ids.is_empty() {
        return Ok(0);
    }
    enqueue_triggers_on(&mut **tx, &node_ids, reason).await
}

pub async fn enqueue_triggers_conn(
    conn: &mut sqlx::PgConnection,
    node_ids: impl IntoIterator<Item = NodeId>,
    reason: ReconcileReason,
) -> Result<u64, sqlx::Error> {
    let node_ids = normalized_node_ids(node_ids);
    if node_ids.is_empty() {
        return Ok(0);
    }
    enqueue_triggers_on(&mut *conn, &node_ids, reason).await
}

fn normalized_node_ids(node_ids: impl IntoIterator<Item = NodeId>) -> Vec<NodeId> {
    let node_ids = node_ids.into_iter().collect::<BTreeSet<_>>();
    node_ids.into_iter().collect::<Vec<_>>()
}

async fn enqueue_triggers_on<'e, E>(
    executor: E,
    node_ids: &[NodeId],
    reason: ReconcileReason,
) -> Result<u64, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let diagnostic_reason = reason.as_str();
    let (queued, rejected) = sqlx::query_as::<_, (i64, i64)>(sql!(
        "WITH input AS (
             SELECT DISTINCT node_id
             FROM UNNEST($1::UUID[]) AS input(node_id)
         ),
         eligible AS (
             SELECT input.node_id
             FROM input
             JOIN nodes n ON n.id = input.node_id
             WHERE n.role IN ($5, $6)
         ),
         counts AS (
             SELECT
                 (SELECT COUNT(*)::BIGINT FROM input) AS requested,
                 (SELECT COUNT(*)::BIGINT FROM eligible) AS eligible
         ),
         upserted AS (
             INSERT INTO node_reconcile_triggers (
                 node_id, reason, reasons, requested_at, updated_at
             )
             SELECT eligible.node_id, $2, ARRAY[$3::state_reconciliation_reason], NOW(), NOW()
             FROM eligible
             CROSS JOIN counts
             WHERE counts.requested = counts.eligible
             ON CONFLICT (node_id) DO UPDATE
             SET generation = node_reconcile_triggers.generation + 1,
                 reasons = CASE
                     WHEN $3::state_reconciliation_reason = ANY(node_reconcile_triggers.reasons)
                     THEN node_reconcile_triggers.reasons
                     ELSE ARRAY[$3::state_reconciliation_reason] || node_reconcile_triggers.reasons
                 END,
                 reason = CASE
                     WHEN node_reconcile_triggers.reason = ''
                          OR node_reconcile_triggers.reason = EXCLUDED.reason
                     THEN EXCLUDED.reason
                     ELSE LEFT(EXCLUDED.reason || ',' || node_reconcile_triggers.reason, $4)
                 END,
                 requested_at = CASE
                     WHEN node_reconcile_triggers.claim_token = '' THEN NOW()
                     ELSE node_reconcile_triggers.requested_at
                 END,
                 next_attempt_at = CASE
                     WHEN node_reconcile_triggers.claim_token = '' THEN NOW()
                     ELSE node_reconcile_triggers.next_attempt_at
                 END,
                 attempts = CASE
                     WHEN node_reconcile_triggers.claim_token = '' THEN 0
                     ELSE node_reconcile_triggers.attempts
                 END,
                 last_error = CASE
                     WHEN node_reconcile_triggers.claim_token = '' THEN ''
                     ELSE node_reconcile_triggers.last_error
                 END,
                 dead_at = NULL,
                 dead_reason = '',
                 updated_at = NOW()
             RETURNING node_id
         )
         SELECT
             (SELECT COUNT(*)::BIGINT FROM upserted) AS queued,
             (SELECT requested - eligible FROM counts) AS rejected"
    ))
    .bind(node_ids)
    .bind(diagnostic_reason)
    .bind(reason)
    .bind(RECONCILE_REASON_MAX_CHARS as i32)
    .bind(SpaceNodeRole::Worker)
    .bind(SpaceNodeRole::Builder)
    .fetch_one(executor)
    .await?;
    if rejected > 0 {
        return Err(sqlx::Error::Protocol(format!(
            "state reconciliation triggers can only target worker or builder nodes; rejected {rejected} node(s)"
        )));
    }
    Ok(queued.cast_unsigned())
}

pub async fn claim_triggers(
    pool: &PgPool,
    limit: u32,
) -> Result<StateReconciliationTriggerClaim, sqlx::Error> {
    let claim_token = format!("state-reconcile-trigger-{}", uuid::Uuid::now_v7());
    let rows = sqlx::query_as::<_, ClaimedStateReconciliationTriggerRow>(sql!(
        "WITH picked AS (
             SELECT node_id
             FROM node_reconcile_triggers
             WHERE next_attempt_at <= NOW()
               AND dead_at IS NULL
               AND (
                   claim_token = ''
                   OR claimed_at <= NOW() - ($2 * INTERVAL '1 second')
               )
             ORDER BY requested_at ASC, node_id ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE node_reconcile_triggers t
         SET claim_token = $3,
             claim_generation = t.generation,
             claimed_at = NOW(),
             updated_at = NOW()
         FROM picked
         WHERE t.node_id = picked.node_id
         RETURNING t.node_id,
                   t.reasons AS reasons,
                   t.reason AS diagnostic_reason"
    ))
    .bind(i64::from(limit))
    .bind(TRIGGER_CLAIM_STALE_SECS)
    .bind(&claim_token)
    .fetch_all(pool)
    .await?;
    let triggers = rows
        .into_iter()
        .map(|row| ClaimedStateReconciliationTrigger {
            node_id: row.node_id,
            reasons: coalesced_reasons(row.reasons, &row.diagnostic_reason),
            diagnostic_reason: normalize_trigger_reason(&row.diagnostic_reason),
        })
        .collect();

    Ok(StateReconciliationTriggerClaim {
        claim_token,
        triggers,
    })
}

pub async fn fetch_trigger_targets(
    pool: &PgPool,
    node_ids: &[NodeId],
    live_node_names: Option<&HashSet<String>>,
) -> Result<TriggerTargets, sqlx::Error> {
    let targets = sqlx::query_as::<_, TriggerTargetRow>(sql!(
        "SELECT n.id,
                n.role,
                n.managed_by,
                nd.node_api_endpoint AS direct_endpoint,
                nk.k8s_node_name AS node_name
         FROM nodes n
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         WHERE n.id = ANY($1)
           AND n.role IN ($2, $3)
           AND (n.managed_by != $4 OR (nd.node_id IS NOT NULL AND nd.deleted_at IS NULL))"
    ))
    .bind(node_ids)
    .bind(SpaceNodeRole::Worker)
    .bind(SpaceNodeRole::Builder)
    .bind(SubstrateKind::Space)
    .fetch_all(pool)
    .await?;
    // An empty warm snapshot means membership is unknown, not that every node
    // left: a real SPACE cluster always has nodes, so empty almost certainly
    // means the reflector froze, and trusting it would dead-letter every
    // Kubernetes target in the claim. Fail open like the cold (`None`) case —
    // the same rule the reconcile-queue GC's kube-presence detector applies to
    // this snapshot.
    let Some(live_node_names) = live_node_names.filter(|names| !names.is_empty()) else {
        return Ok(TriggerTargets {
            triggerable: targets,
            permanently_unavailable: Vec::new(),
        });
    };
    // A node of a substrate listed in that inventory but missing from it is
    // gone for good. Other nodes never appear there, so absence says nothing.
    let (permanently_unavailable, triggerable): (Vec<_>, Vec<_>) =
        targets.into_iter().partition(|target| {
            target.managed_by.listed_in_live_node_inventory()
                && !target
                    .node_name
                    .as_ref()
                    .is_some_and(|node_name| live_node_names.contains(node_name))
        });
    Ok(TriggerTargets {
        triggerable,
        permanently_unavailable: permanently_unavailable
            .into_iter()
            .map(|target| target.id)
            .collect(),
    })
}

pub async fn complete_claim(
    pool: &PgPool,
    claim_token: &str,
    succeeded: &[NodeId],
    failed: &[(NodeId, String)],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let mut release_node_ids = Vec::with_capacity(succeeded.len() + failed.len());
    release_node_ids.extend_from_slice(succeeded);
    release_node_ids.extend(failed.iter().map(|(node_id, _)| *node_id));

    if !succeeded.is_empty() {
        sqlx::query(sql!(
            "DELETE FROM node_reconcile_triggers
             WHERE node_id = ANY($1)
               AND claim_token = $2
               AND generation = claim_generation"
        ))
        .bind(succeeded)
        .bind(claim_token)
        .execute(&mut *tx)
        .await?;
    }

    let dead_lettered = if failed.is_empty() {
        0_i64
    } else {
        let failed_node_ids = failed
            .iter()
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        let failed_errors = failed
            .iter()
            .map(|(_, error)| error.clone())
            .collect::<Vec<_>>();
        sqlx::query_scalar::<_, i64>(sql!(
            "WITH failed(node_id, error) AS (
                 SELECT * FROM UNNEST($1::UUID[], $2::TEXT[])
             ),
             updated AS (
                 UPDATE node_reconcile_triggers t
                 SET attempts = t.attempts + 1,
                     last_error = failed.error,
                     claim_token = '',
                     claim_generation = 0,
                     claimed_at = NULL,
                     dead_at = CASE
                         WHEN t.attempts + 1 >= $5 THEN NOW()
                         ELSE NULL
                     END,
                     dead_reason = CASE
                         WHEN t.attempts + 1 >= $5 THEN failed.error
                         ELSE ''
                     END,
                     next_attempt_at = CASE
                         WHEN t.attempts + 1 >= $5 THEN t.next_attempt_at
                         ELSE NOW()
                             + (LEAST($4::INTEGER, POWER(2::NUMERIC, LEAST(t.attempts, 6))::INTEGER)
                                * INTERVAL '1 second')
                     END,
                     updated_at = NOW()
                 FROM failed
                 WHERE t.node_id = failed.node_id
                   AND t.claim_token = $3
                   AND t.generation = t.claim_generation
                 RETURNING t.dead_at IS NOT NULL AS dead_lettered
             )
             SELECT COUNT(*) FILTER (WHERE dead_lettered) FROM updated"
        ))
        .bind(&failed_node_ids)
        .bind(&failed_errors)
        .bind(claim_token)
        .bind(TRIGGER_FAILURE_BACKOFF_MAX_SECS)
        .bind(TRIGGER_MAX_ATTEMPTS)
        .fetch_one(&mut *tx)
        .await?
    };

    release_refreshed_trigger_claims(&mut tx, &release_node_ids, claim_token).await?;
    tx.commit().await?;
    for _ in 0..u64::try_from(dead_lettered).unwrap_or(0) {
        pplx_space_metrics::reconcile::trigger_dead_letter();
    }
    Ok(())
}

pub async fn dead_letter_claim(
    pool: &PgPool,
    claim_token: &str,
    failed: &[(NodeId, String)],
) -> Result<(), sqlx::Error> {
    if failed.is_empty() {
        return Ok(());
    }

    let failed_node_ids = failed
        .iter()
        .map(|(node_id, _)| *node_id)
        .collect::<Vec<_>>();
    let failed_errors = failed
        .iter()
        .map(|(_, error)| error.clone())
        .collect::<Vec<_>>();
    let dead_lettered = sqlx::query_scalar::<_, i64>(sql!(
        "WITH failed(node_id, error) AS (
             SELECT * FROM UNNEST($1::UUID[], $2::TEXT[])
         ),
         updated AS (
             UPDATE node_reconcile_triggers t
             SET attempts = $4,
                 last_error = failed.error,
                 claim_token = '',
                 claim_generation = 0,
                 claimed_at = NULL,
                 dead_at = NOW(),
                 dead_reason = failed.error,
                 updated_at = NOW()
             FROM failed
             WHERE t.node_id = failed.node_id
               AND t.claim_token = $3
               AND t.generation = t.claim_generation
               AND t.dead_at IS NULL
             RETURNING 1
         )
         SELECT COUNT(*)::BIGINT FROM updated"
    ))
    .bind(&failed_node_ids)
    .bind(&failed_errors)
    .bind(claim_token)
    .bind(TRIGGER_MAX_ATTEMPTS)
    .fetch_one(pool)
    .await?;

    for _ in 0..u64::try_from(dead_lettered).unwrap_or(0) {
        pplx_space_metrics::reconcile::trigger_dead_letter();
    }
    Ok(())
}

pub async fn sweep_dead_triggers(
    pool: &PgPool,
    older_than_secs: i64,
    limit: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "WITH picked AS (
             SELECT node_id
             FROM node_reconcile_triggers
             WHERE dead_at IS NOT NULL
               AND dead_at < NOW() - ($1 * INTERVAL '1 second')
             ORDER BY dead_at ASC, node_id ASC
             LIMIT $2
         )
         DELETE FROM node_reconcile_triggers t
         USING picked
         WHERE t.node_id = picked.node_id"
    ))
    .bind(older_than_secs)
    .bind(limit)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

async fn release_refreshed_trigger_claims(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_ids: &[NodeId],
    claim_token: &str,
) -> Result<(), sqlx::Error> {
    if node_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(sql!(
        "UPDATE node_reconcile_triggers
         SET claim_token = '',
             claim_generation = 0,
             claimed_at = NULL,
             next_attempt_at = NOW(),
             updated_at = NOW()
         WHERE node_id = ANY($1)
           AND claim_token = $2"
    ))
    .bind(node_ids)
    .bind(claim_token)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub fn normalize_trigger_reason(reason: &str) -> String {
    normalize_shared_reconcile_reason(reason)
}

fn coalesced_reasons(
    raw_reasons: Vec<ReconcileReason>,
    diagnostic_reason: &str,
) -> Vec<ReconcileReason> {
    let mut seen = BTreeSet::new();
    let mut reasons = raw_reasons
        .into_iter()
        .filter(|reason| seen.insert(*reason))
        .collect::<Vec<_>>();
    if reasons.is_empty() {
        reasons.extend(
            diagnostic_reason
                .split(',')
                .filter_map(|reason| ReconcileReason::parse(reason.trim()))
                .filter(|reason| seen.insert(*reason)),
        );
    }
    reasons
}

#[cfg(test)]
mod tests;
