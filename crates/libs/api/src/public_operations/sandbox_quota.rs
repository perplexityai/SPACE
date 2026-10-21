//! Live-sandbox quotas on native creates, checked in the create's admission
//! transaction.
//!
//! A create is held to its organization's limit, which counts every live
//! sandbox of the org whoever owns it, and, for a federated-identity caller,
//! to a per-subject limit that counts the subject's own. "Live" excludes the
//! terminal set the lifecycle guards use (`failed`, `quarantined`, `deleted`,
//! `cleanup_required`); queued or running native creates that have no row
//! yet count too. A create targets its public sandbox id, the row's id with
//! the public prefix, so one past its row insert is counted once.
//!
//! # Concurrency
//!
//! Each limit has an advisory transaction lock: per org for the org limit,
//! per subject for the subject limit. [`PublicOperationAdmission::plan_sandbox_quota`]
//! counts without a lock and picks a lock mode for each limit;
//! [`PublicOperationAdmission::enforce_sandbox_quota`] takes the locks in that
//! mode and counts again before the caller records the create in the same
//! transaction.
//!
//! * Well under a limit (below `limit - 2 * headroom`, see [`headroom`]) a
//!   create takes the lock shared, so such creates do not wait for one
//!   another. It is admitted only if its count under the lock is below
//!   `limit - headroom`.
//! * Otherwise it takes the lock exclusively, which waits for every create
//!   holding it shared, and is admitted only if its count is below `limit`.
//!
//! Every create holds its lock from its count through its commit, and the
//! count is a new statement after the lock is granted, so under READ
//! COMMITTED it sees every create that held the lock before it. An exclusive
//! holder therefore sees all creates that could still commit, and never
//! admits past the limit. Creates holding the lock shared at the same time do
//! not see one another; each counted under `limit - headroom`, so together
//! they exceed the limit only when more than `headroom` of them hold it at
//! once, and by at most that excess. Each holder is an open transaction on
//! some API replica, so the excess is below the API's total database
//! connections across all replicas less the headroom. A limit of
//! `2 * MIN_LOCK_HEADROOM` or less is always checked exclusively and is exact.
//!
//! A shared count that lands between `limit - headroom` and `limit` (the
//! count grew while the create waited for the lock) cannot be upgraded to
//! exclusive without risking a deadlock against another such create, so the
//! create is answered [`SandboxQuotaOutcome::Contended`] and the caller
//! retries; the retry counts near the limit and takes the lock exclusively.
//!
//! Locks are taken subject first, then org, after the idempotency-key lock,
//! on every create, so they cannot deadlock. Deletes take none; one landing
//! concurrently only lowers a count.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, UserId};
use pplx_space_public_api::OperationKind;
use sqlx::{Postgres, Transaction};

use super::{ApiProduct, PublicOperationAdmission, PublicOperationStoreError};

/// Stable namespace for the per-org quota lock.
const ORG_LOCK_SEED: i64 = 0x6f72_675f_7162;
/// Stable namespace for the per-subject quota lock.
const SUBJECT_LOCK_SEED: i64 = 0x7375_625f_7162;
/// The smallest headroom; see [`headroom`].
const MIN_LOCK_HEADROOM: i64 = 64;

/// The limits a native create is held to. Build one only when at least one
/// is set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxQuota {
    /// The org's limit over every live sandbox it holds.
    pub org_limit: Option<u32>,
    /// A federated-identity caller's own limit.
    pub subject: Option<SubjectSandboxQuota>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubjectSandboxQuota {
    pub user_id: UserId,
    pub limit: u32,
}

/// Which limit refused a create.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxQuotaRefusal {
    Org { limit: u32 },
    Subject { limit: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxQuotaOutcome {
    Admit,
    Refuse(SandboxQuotaRefusal),
    /// The count moved while the create waited for its lock; retry.
    Contended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LockMode {
    Shared,
    Exclusive,
}

/// The lock mode each limit of one create takes, from a lock-free count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxQuotaPlan {
    org: Option<(u32, LockMode)>,
    subject: Option<(UserId, u32, LockMode)>,
}

/// The margin below a limit within which creates are serialized: a tenth of
/// the limit, never less than [`MIN_LOCK_HEADROOM`].
fn headroom(limit: u32) -> i64 {
    (i64::from(limit) / 10).max(MIN_LOCK_HEADROOM)
}

/// From a lock-free count: refuse at or over the limit, else the lock mode.
fn plan_limit(held: i64, limit: u32) -> Result<LockMode, ()> {
    let limit_count = i64::from(limit);
    if held >= limit_count {
        Err(())
    } else if held < limit_count - 2 * headroom(limit) {
        Ok(LockMode::Shared)
    } else {
        Ok(LockMode::Exclusive)
    }
}

/// From a count under the lock taken in `mode`.
fn enforce_limit(held: i64, limit: u32, mode: LockMode) -> Verdict {
    let limit_count = i64::from(limit);
    match mode {
        _ if held >= limit_count => Verdict::Refuse,
        LockMode::Exclusive => Verdict::Admit,
        LockMode::Shared if held < limit_count - headroom(limit) => Verdict::Admit,
        LockMode::Shared => Verdict::Contended,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Admit,
    Refuse,
    Contended,
}

impl PublicOperationAdmission {
    /// Count the caller's sandboxes without a lock. A create at or over a
    /// limit is refused here, before the caller does any other work in the
    /// transaction (an admission-time warm-pool claim); otherwise the plan
    /// says how [`Self::enforce_sandbox_quota`] locks. Call only for a
    /// genuinely new create: a replay or a key conflict is not counted.
    pub(crate) async fn plan_sandbox_quota(
        &mut self,
        org_id: OrgId,
        quota: SandboxQuota,
    ) -> Result<Result<SandboxQuotaPlan, SandboxQuotaRefusal>, PublicOperationStoreError> {
        let subject_id = quota.subject.map(|subject| subject.user_id);
        let held = sandboxes_held_tx(&mut self.tx, org_id, subject_id).await?;
        let org = match quota.org_limit {
            None => None,
            Some(limit) => match plan_limit(held.org, limit) {
                Ok(mode) => Some((limit, mode)),
                Err(()) => return Ok(Err(SandboxQuotaRefusal::Org { limit })),
            },
        };
        let subject = match quota.subject {
            None => None,
            Some(SubjectSandboxQuota { user_id, limit }) => match plan_limit(held.subject, limit) {
                Ok(mode) => Some((user_id, limit, mode)),
                Err(()) => return Ok(Err(SandboxQuotaRefusal::Subject { limit })),
            },
        };
        Ok(Ok(SandboxQuotaPlan { org, subject }))
    }

    /// Take the plan's locks, held until the transaction ends, and count
    /// again. Record the create in this transaction only on
    /// [`SandboxQuotaOutcome::Admit`].
    pub(crate) async fn enforce_sandbox_quota(
        &mut self,
        org_id: OrgId,
        plan: SandboxQuotaPlan,
    ) -> Result<SandboxQuotaOutcome, PublicOperationStoreError> {
        if let Some((user_id, _, mode)) = plan.subject {
            lock_tx(&mut self.tx, user_id.as_uuid(), SUBJECT_LOCK_SEED, mode).await?;
        }
        if let Some((_, mode)) = plan.org {
            lock_tx(&mut self.tx, org_id.as_uuid(), ORG_LOCK_SEED, mode).await?;
        }
        let subject_id = plan.subject.map(|(user_id, _, _)| user_id);
        let held = sandboxes_held_tx(&mut self.tx, org_id, subject_id).await?;
        let org = plan.org.map(|(limit, mode)| {
            (
                enforce_limit(held.org, limit, mode),
                SandboxQuotaRefusal::Org { limit },
            )
        });
        let subject = plan.subject.map(|(_, limit, mode)| {
            (
                enforce_limit(held.subject, limit, mode),
                SandboxQuotaRefusal::Subject { limit },
            )
        });
        let verdicts = [org, subject].into_iter().flatten();
        let mut outcome = SandboxQuotaOutcome::Admit;
        for (verdict, refusal) in verdicts {
            match verdict {
                Verdict::Refuse => return Ok(SandboxQuotaOutcome::Refuse(refusal)),
                Verdict::Contended => outcome = SandboxQuotaOutcome::Contended,
                Verdict::Admit => {}
            }
        }
        Ok(outcome)
    }
}

async fn lock_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: uuid::Uuid,
    seed: i64,
    mode: LockMode,
) -> Result<(), sqlx::Error> {
    let statement = match mode {
        LockMode::Shared => sqlx::query(sql!(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1::text, $2))"
        )),
        LockMode::Exclusive => sqlx::query(sql!(
            "SELECT pg_advisory_xact_lock(hashtextextended($1::text, $2))"
        )),
    };
    statement.bind(id).bind(seed).execute(&mut **tx).await?;
    Ok(())
}

struct SandboxesHeld {
    org: i64,
    subject: i64,
}

/// The org's live sandboxes plus its in-flight native creates without a row,
/// and the share of those `subject_id` owns or requested (zero without one).
///
/// Cost: only a custom plan takes `idx_public_operations_state_next_attempt`
/// alone, over the cluster's active operations; the generic plan a cached
/// statement can switch to intersects it with
/// `idx_public_operations_tenant_created` over the org's whole operation
/// history. Both halves read the org's rows through `idx_sandboxes_org_id`,
/// `deleted` tombstones included, so the count grows with an org's retained
/// tombstones.
async fn sandboxes_held_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    subject_id: Option<UserId>,
) -> Result<SandboxesHeld, sqlx::Error> {
    let (org, subject) = sqlx::query_as::<_, (i64, i64)>(sql!(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE held.user_id = $2)
           FROM (
                SELECT s.user_id
                  FROM sandboxes s
                 WHERE s.org_id = $1
                   AND s.lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')
                UNION ALL
                SELECT op.actor_user_id
                  FROM public_operations op
                 WHERE op.state IN ('queued', 'running')
                   AND op.org_id = $1
                   AND op.api_product = $3
                   AND op.kind = $4
                   AND NOT EXISTS (
                       SELECT 1
                         FROM sandboxes s
                        WHERE s.org_id = $1
                          AND $5 || s.id::text = op.target_id
                   )
           ) held"
    ))
    .bind(org_id)
    .bind(subject_id)
    .bind(ApiProduct::Native.as_str())
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX)
    .fetch_one(&mut **tx)
    .await?;
    Ok(SandboxesHeld { org, subject })
}

#[cfg(test)]
mod tests;
