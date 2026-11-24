//! The dispatcher's claim of due `public_operations`: which queued
//! operations, and which running ones under a stale claim, a claim takes, one
//! statement that picks, locks and claims them. The rows it locks stay locked
//! until the caller's transaction ends, so the claim takes the caller's
//! connection. The claim lock that serializes keyed claims
//! ([`try_lock_claims`]) is the caller's to take, in the transaction the
//! claim runs in.

use pplx_space_public_api::{OperationKind, OperationState, OperationTargetType};
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, Row};
use uuid::Uuid;

use crate::operations::{ClaimedPublicOperationRow, kind_labels};

/// Try to take the transaction-level advisory lock `key` that serializes
/// keyed claims, without waiting, and turn off JIT for the rest of the
/// transaction in the same round trip. `false` means another transaction
/// holds the lock. A lock taken lasts until the caller's transaction ends.
pub async fn try_lock_claims(conn: &mut sqlx::PgConnection, key: i64) -> Result<bool, sqlx::Error> {
    let (claim_lock_held, _jit) = sqlx::query_as::<_, (bool, String)>(sql!(
        "SELECT pg_try_advisory_xact_lock($1), set_config('jit', 'off', true)"
    ))
    .bind(key)
    .fetch_one(&mut *conn)
    .await?;
    Ok(claim_lock_held)
}

/// What [`claim_due`] claims, and how many.
#[derive(Debug)]
pub struct DueClaim<'a> {
    /// `$1`: the most operations the claim takes.
    pub total: i64,
    /// `$2`: how long a claim may go unrenewed before another claim takes it
    /// over.
    pub claim_stale_after_secs: i64,
    /// `$3`: the token the claimed operations are claimed under.
    pub claim_token: &'a str,
    /// `$6`: the kinds the claim may take.
    pub claimable_kinds: &'a [OperationKind],
    /// `$8`: the kinds outside a sandbox's queue, which the key rule ignores.
    pub outside_queue_kinds: &'a [OperationKind],
    /// `$11`: whether the key rule lets at most one operation per sandbox
    /// start. Overlapping keyed claims can break that rule unless each runs
    /// in a transaction that holds [`try_lock_claims`].
    pub keyed: bool,
    /// `$12`: the most operations other than teardowns the claim takes.
    pub non_teardown: i64,
    /// `$13`: the most teardowns the claim takes while other work is due.
    pub teardown_while_other_due: i64,
    /// `$14`: the teardown kinds.
    pub teardown_kinds: &'a [OperationKind],
    /// `$15`: each organization's limit on operations other than teardowns,
    /// or 0 for none.
    pub per_org_non_teardown: i64,
    /// `$16`: the free slots kept for work another organization submits
    /// later.
    pub per_org_headroom: i64,
    /// `$17`: how the claim token of an operation claimed as it was created
    /// begins.
    pub admission_claim_token_prefix: &'a str,
}

/// An operation [`claim_due`] claimed.
#[derive(Debug, FromRow)]
pub struct DueClaimRow {
    #[sqlx(flatten)]
    pub operation: ClaimedPublicOperationRow,
    /// Milliseconds since it became claimable, by the database clock.
    pub queue_wait_ms: f64,
    /// Whether it was taken over from a stale claim.
    pub reclaimed: bool,
}

/// What [`claim_due`] returns.
#[derive(Debug)]
pub struct DueClaimed {
    /// The claimed operations, each row decoded into its own result, so
    /// the caller meets a row that fails to decode in its turn.
    pub operations: Vec<Result<DueClaimRow, sqlx::Error>>,
    /// With nothing claimed, whether other work was due and the
    /// organization limit held all of it back.
    pub org_limit_held_back: bool,
}

/// Claim due operations as `claim` says, picking them in claim order. The
/// claimed rows come back in no particular order. The rows it claims, and
/// any it locks but leaves unclaimed, stay locked until the caller's
/// transaction ends.
///
/// Across keys, claim in order of when each operation became claimable,
/// so a requeued retry waits behind work queued before its backoff
/// expired. With `$11` (keyed) set, `blocked` lets at most one operation
/// per sandbox start: none while one runs under a live claim; otherwise a
/// stale claim first, so its token is rotated before anything else on the
/// sandbox can start beside its old execution; otherwise the
/// earliest-requested one if it is due; otherwise the earliest due delete
/// with no create requested before it, so a delete is not stuck behind
/// work in retry backoff but never overtakes a create. Only sandboxes with
/// more than one active operation can hold anything back, so the window
/// pass reads just those, in one sort. Kinds outside a sandbox's queue
/// (`$8`, `outside_queue_kinds`) stay outside the key.
/// With `$11` unset, `blocked` is empty. Teardowns
/// (`$14`, `teardown_kinds`) and other work are picked apart. At most
/// `$12` of the claim are other work, which keeps
/// the slots reserved for teardowns free of it. While other work is due, at
/// most `$13` are teardowns, which keeps the slots reserved for other work
/// free of them; with none due, teardowns may take any free slot. The claim
/// then takes the first `$1` of both, in the same order. Both picks lock
/// their rows before that last `LIMIT`, so up to `$12 + $13 - $1` rows stay
/// locked but unclaimed until the caller's transaction ends. A concurrent
/// unkeyed claim skips them meanwhile, and a keyed one does not run while
/// the caller holds the claim lock. With `$15` above zero, it limits each
/// organization's operations other than teardowns: `org_slots` counts the
/// ones it runs under a live claim, except one claimed as it was created
/// (`$17`), which the process that created it runs, and, in claim order,
/// the ones this claim would add. An organization is waiting while it has
/// such work due and holds fewer than `$15`. While another one is
/// waiting, an organization gets no more than `$15`. While none is, it may
/// go past `$15`, but operations past their organization's limit are taken
/// only among the first `$12 - $16` admissible ones in claim order, so
/// that `$16` of the free slots open to other work stay free for work
/// another organization submits later. Organizations all at their limit
/// share that one budget.
/// No more than `2 * $12` of an organization's due operations can be
/// admitted, so the rank reads only those: `due_other` reads every due
/// one once, an organization with more gets a top-N pass of its own, and
/// the others' go to the window whole, which keeps one organization's
/// backlog out of the sort.
/// That rank needs a window, which cannot share a level with `FOR
/// UPDATE`, so `first_other` admits up to `2 * $12` and `picked_other`
/// locks the first `$12` of them it can by key, re-checking that each is
/// still claimable. Admitting more than it takes keeps a row another
/// transaction has locked from shrinking the claim, unless more than `$12`
/// of the admitted ones are. A locked row still counts toward its
/// organization's limit and the headroom budget in this claim, since
/// finding it locked takes the lock, so work admitted only past it waits
/// for the next claim. When it claims nothing, the statement returns
/// one row with no operation, whose `org_limit_held_back` says whether
/// other work was due and the organization limit held all of it back.
pub async fn claim_due(
    conn: &mut sqlx::PgConnection,
    claim: DueClaim<'_>,
) -> Result<DueClaimed, sqlx::Error> {
    let rows = sqlx::query(sql!(
        "WITH shared AS (
             SELECT org_id, target_id
             FROM public_operations
             WHERE $11::BOOLEAN
               AND state IN ($4, $5)
               AND target_type = $7
               AND target_id IS NOT NULL
               AND kind = ANY($6)
               AND kind <> ALL($8)
             GROUP BY org_id, target_id
             HAVING COUNT(*) > 1
         ),
         active AS (
             SELECT operation_id,
                    op.org_id,
                    op.target_id,
                    requested_at,
                    kind,
                    next_attempt_at <= NOW() AS due,
                    state = $5 AND claim_token <> ''
                        AND claimed_at > NOW() - make_interval(secs => $2::BIGINT) AS live,
                    state = $5 AND claim_token <> ''
                        AND claimed_at <= NOW() - make_interval(secs => $2::BIGINT) AS stale
             FROM shared
             JOIN public_operations op
               ON op.org_id = shared.org_id
              AND op.target_type = $7
              AND op.target_id = shared.target_id
             WHERE op.state IN ($4, $5)
               AND op.kind = ANY($6)
               AND op.kind <> ALL($8)
         ),
         ranked AS (
             SELECT operation_id,
                    ROW_NUMBER() OVER queue AS position,
                    FIRST_VALUE(due) OVER queue AS head_due,
                    BOOL_OR(live) OVER whole AS sandbox_live,
                    BOOL_OR(stale) OVER whole AS sandbox_stale,
                    stale AND NOT COALESCE(BOOL_OR(stale) OVER earlier, FALSE) AS first_stale,
                    kind = $9
                        AND due
                        AND NOT COALESCE(BOOL_OR(kind = $10) OVER earlier, FALSE)
                        AND NOT COALESCE(BOOL_OR(kind = $9 AND due) OVER earlier, FALSE)
                        AS first_free_delete
             FROM active
             WINDOW queue AS (
                        PARTITION BY org_id, target_id ORDER BY requested_at, operation_id
                    ),
                    whole AS (queue ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                    earlier AS (queue ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
         ),
         blocked AS (
             SELECT operation_id
             FROM ranked
             WHERE sandbox_live
                OR (sandbox_stale AND NOT first_stale)
                OR (NOT sandbox_stale AND head_due AND position > 1)
                OR (NOT sandbox_stale AND NOT head_due AND NOT first_free_delete)
         ),
         org_live AS (
             SELECT org_id, COUNT(*) AS live
             FROM public_operations
             WHERE $15::BIGINT > 0
               AND state = $5
               AND claim_token <> ''
               AND NOT starts_with(claim_token, $17)
               AND claimed_at > NOW() - make_interval(secs => $2::BIGINT)
               AND kind = ANY($6)
               AND kind <> ALL($14)
             GROUP BY org_id
         ),
         due_other AS MATERIALIZED (
             SELECT candidate.operation_id,
                    candidate.org_id,
                    candidate.next_attempt_at,
                    candidate.requested_at,
                    GREATEST(candidate.requested_at, candidate.next_attempt_at) AS claimable_at,
                    candidate.claim_token <> '' AS reclaimed,
                    candidate.started_at IS NULL AS first_start
             FROM public_operations candidate
             WHERE candidate.state IN ($4, $5)
               AND candidate.kind = ANY($6)
               AND candidate.kind <> ALL($14)
               AND candidate.next_attempt_at <= NOW()
               AND (
                   candidate.claim_token = ''
                   OR candidate.claimed_at <= NOW() - make_interval(secs => $2::BIGINT)
               )
               AND candidate.operation_id NOT IN (SELECT operation_id FROM blocked)
         ),
         due_orgs AS (
             SELECT org_id, COUNT(*) AS due
             FROM due_other
             GROUP BY org_id
         ),
         org_first AS (
             SELECT due_other.*
             FROM due_other
             JOIN due_orgs ON due_orgs.org_id = due_other.org_id
             WHERE due_orgs.due <= 2 * $12
             UNION ALL
             SELECT first.*
             FROM due_orgs
             CROSS JOIN LATERAL (
                 SELECT *
                 FROM due_other
                 WHERE due_other.org_id = due_orgs.org_id
                 ORDER BY next_attempt_at, requested_at, operation_id
                 LIMIT 2 * $12
             ) first
             WHERE due_orgs.due > 2 * $12
         ),
         other_candidates AS (
             SELECT org_first.*,
                    COALESCE(org_live.live, 0) AS org_held,
                    COALESCE(org_live.live, 0) + ROW_NUMBER() OVER (
                        PARTITION BY org_first.org_id
                        ORDER BY org_first.next_attempt_at, org_first.requested_at, org_first.operation_id
                    ) AS org_slots
             FROM org_first
             LEFT JOIN org_live ON org_live.org_id = org_first.org_id
         ),
         waiting AS (
             SELECT COUNT(*) AS orgs
             FROM due_orgs
             LEFT JOIN org_live ON org_live.org_id = due_orgs.org_id
             WHERE COALESCE(org_live.live, 0) < $15
         ),
         admissible AS (
             SELECT other.*,
                    ROW_NUMBER() OVER (
                        ORDER BY other.next_attempt_at, other.requested_at, other.operation_id
                    ) AS admissible_rank
             FROM other_candidates other
             WHERE $15::BIGINT = 0
                OR other.org_slots <= $15
                -- Past its limit only while no other organization waits.
                OR (SELECT orgs FROM waiting) = CASE WHEN other.org_held < $15 THEN 1 ELSE 0 END
         ),
         first_other AS (
             SELECT operation_id, next_attempt_at, requested_at, claimable_at, reclaimed, first_start
             FROM admissible
             WHERE $15::BIGINT = 0
                OR org_slots <= $15
                OR admissible_rank <= $12 - $16
             ORDER BY next_attempt_at ASC, requested_at ASC, operation_id ASC
             LIMIT 2 * $12
         ),
         picked_other AS (
             SELECT candidate.operation_id,
                    first_other.next_attempt_at,
                    first_other.requested_at,
                    first_other.claimable_at,
                    first_other.reclaimed,
                    first_other.first_start
             FROM first_other
             JOIN public_operations candidate
               ON candidate.operation_id = first_other.operation_id
             WHERE candidate.state IN ($4, $5)
               AND candidate.next_attempt_at <= NOW()
               AND (
                   candidate.claim_token = ''
                   OR candidate.claimed_at <= NOW() - make_interval(secs => $2::BIGINT)
               )
             ORDER BY first_other.next_attempt_at ASC,
                      first_other.requested_at ASC,
                      first_other.operation_id ASC
             LIMIT $12
             FOR UPDATE OF candidate SKIP LOCKED
         ),
         picked_teardown AS (
             SELECT candidate.operation_id,
                    candidate.next_attempt_at,
                    candidate.requested_at,
                    GREATEST(candidate.requested_at, candidate.next_attempt_at) AS claimable_at,
                    candidate.claim_token <> '' AS reclaimed,
                    candidate.started_at IS NULL AS first_start
             FROM public_operations candidate
             WHERE candidate.state IN ($4, $5)
               AND candidate.kind = ANY($6)
               AND candidate.kind = ANY($14)
               AND candidate.next_attempt_at <= NOW()
               AND (
                   candidate.claim_token = ''
                   OR candidate.claimed_at <= NOW() - make_interval(secs => $2::BIGINT)
               )
               AND candidate.operation_id NOT IN (SELECT operation_id FROM blocked)
             ORDER BY candidate.next_attempt_at ASC, candidate.requested_at ASC, candidate.operation_id ASC
             LIMIT CASE WHEN EXISTS (SELECT 1 FROM picked_other) THEN $13 ELSE $1 END
             FOR UPDATE OF candidate SKIP LOCKED
         ),
         picked AS (
             SELECT operation_id, claimable_at, reclaimed, first_start
             FROM (
                 SELECT * FROM picked_other
                 UNION ALL
                 SELECT * FROM picked_teardown
             ) claimable
             ORDER BY next_attempt_at ASC, requested_at ASC, operation_id ASC
             LIMIT $1
         ),
         claimed AS (
             UPDATE public_operations o
             SET state = $5,
                 attempts = o.attempts + 1,
                 claim_token = $3,
                 claim_generation = o.claim_generation + 1,
                 claimed_at = NOW(),
                 started_at = COALESCE(o.started_at, NOW()),
                 progress_message = CASE
                     WHEN o.progress_message = '' THEN 'operation claimed'
                     ELSE o.progress_message
                 END,
                 updated_at = NOW()
             FROM picked
             WHERE o.operation_id = picked.operation_id
             RETURNING o.operation_id,
                       o.org_id,
                       o.actor_user_id,
                       o.api_product,
                       o.kind,
                       o.target_type,
                       o.target_id,
                       o.payload_json,
                       o.attempts,
                       o.claim_generation,
                       o.traceparent,
                       CASE WHEN picked.first_start
                           THEN FLOOR(EXTRACT(EPOCH FROM NOW() - o.created_at))::BIGINT
                       END AS age_at_first_claim_secs,
                       (EXTRACT(EPOCH FROM NOW() - picked.claimable_at) * 1000)::DOUBLE PRECISION
                           AS queue_wait_ms,
                       picked.reclaimed
         )
         SELECT claimed.*, summary.org_limit_held_back
         FROM (
             SELECT $12::BIGINT > 0
                    AND NOT EXISTS (SELECT 1 FROM first_other)
                    AND EXISTS (SELECT 1 FROM other_candidates) AS org_limit_held_back
         ) summary
         LEFT JOIN claimed ON TRUE"
    ))
    .bind(claim.total)
    .bind(claim.claim_stale_after_secs)
    .bind(claim.claim_token)
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .bind(kind_labels(claim.claimable_kinds))
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(kind_labels(claim.outside_queue_kinds))
    .bind(OperationKind::DeleteSandbox.as_str())
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(claim.keyed)
    .bind(claim.non_teardown)
    .bind(claim.teardown_while_other_due)
    .bind(kind_labels(claim.teardown_kinds))
    .bind(claim.per_org_non_teardown)
    .bind(claim.per_org_headroom)
    .bind(claim.admission_claim_token_prefix)
    .fetch_all(&mut *conn)
    .await?;

    let mut operations = Vec::with_capacity(rows.len());
    let mut org_limit_held_back = false;
    for row in rows {
        match row.try_get::<Option<Uuid>, _>("operation_id") {
            // The row with no operation comes back only when nothing was
            // claimed, so it is the only row.
            Ok(None) => org_limit_held_back = row.try_get("org_limit_held_back")?,
            Ok(Some(_)) => operations.push(DueClaimRow::from_row(&row)),
            Err(error) => operations.push(Err(error)),
        }
    }
    Ok(DueClaimed {
        operations,
        org_limit_held_back,
    })
}
