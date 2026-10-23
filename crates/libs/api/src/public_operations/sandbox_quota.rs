//! Live-sandbox quotas on native creates, checked in the create's admission
//! transaction.
//!
//! A create is held to its organization's limit, which counts every live
//! sandbox of the org whoever owns it, and, for a federated-identity caller,
//! to a per-subject limit that counts the subject's own. "Live" excludes the
//! terminal set the lifecycle guards use (`failed`, `quarantined`, `deleted`,
//! `cleanup_required`); queued or running native creates without a live row
//! count too. A create targets its public sandbox id, the row's id with the
//! public prefix, so one past its row insert is counted once.
//!
//! # Concurrency
//!
//! Each limit has an advisory transaction lock: per org for the org limit,
//! per subject for the subject limit. [`PublicOperationAdmission::admit_new_create`]
//! counts without a lock and picks a lock mode for each limit, then takes the
//! locks in that mode and counts again before the caller records the create
//! in the same transaction.
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
//! holder therefore sees all native creates that could still commit, and
//! admits none past the limit. That holds only among native creates on
//! replicas running this check with the same limits: ECW, fork and admin
//! creates, the failed-sandbox repair that returns a `failed` row to
//! `suspended`, and a replica without this check take no quota lock, so the
//! count sees their sandboxes only once committed. Creates holding the lock
//! shared at the same time do not see one another; each counted under
//! `limit - headroom`, so together they exceed the limit only when more than
//! `headroom` of them hold it at once, and by at most that excess. Each
//! holder is an open transaction on some API replica, so the excess is below
//! the API's total database connections across all replicas less the
//! headroom. A limit of `2 * MIN_LOCK_HEADROOM` or less is always checked
//! exclusively and is exact.
//!
//! A shared count that lands between `limit - headroom` and `limit` (the
//! count grew while the create waited for the lock) cannot be upgraded to
//! exclusive without risking a deadlock against another such create, so the
//! create is answered [`SandboxQuotaOutcome::Contended`] and the caller
//! retries; the retry counts near the limit and takes the lock exclusively.
//!
//! Each lock statement's wait is bounded by [`QUOTA_LOCK_TIMEOUT`]. Near a
//! limit creates queue on the exclusive lock, each holding its pooled
//! connection while it waits; past the bound the wait ends as
//! [`SandboxQuotaOutcome::Contended`] too. A create takes one lock per limit
//! that applies, so a federated caller with both an org and a per-user limit
//! can wait up to twice the bound, and a burst at one org parks connections
//! for at most that long. Afterwards the transaction's earlier `lock_timeout`
//! is restored, not reset to the role's default.
//!
//! Locks are taken subject first, then org, after the idempotency-key lock,
//! on every create, so they cannot deadlock. Deletes take none; one landing
//! concurrently only lowers a count.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, UserId};
use pplx_space_common_svc::sql::contention;
use pplx_space_metrics::operations as metrics;
use pplx_space_public_api::OperationKind;
use sqlx::{Postgres, Transaction};

use super::{
    ApiProduct, ObservePublicOperationIdempotency, PublicOperationAdmission,
    PublicOperationStoreError,
};

/// Stable namespace for the per-org quota lock.
const ORG_LOCK_SEED: i64 = 0x6f72_675f_7162;
/// Stable namespace for the per-subject quota lock.
const SUBJECT_LOCK_SEED: i64 = 0x7375_625f_7162;
/// The smallest headroom; see [`headroom`].
const MIN_LOCK_HEADROOM: i64 = 64;
/// The longest a create waits for one quota lock before it answers
/// [`SandboxQuotaOutcome::Contended`]. Set transaction-locally around the
/// lock statements only, so the rest of the admission keeps the role's own
/// `lock_timeout`, and never raises a shorter one.
const QUOTA_LOCK_TIMEOUT: Duration = Duration::from_secs(2);

/// The limits one native create in `org_id` is held to. Build one only when
/// at least one is set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxQuota {
    pub org_id: OrgId,
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
    /// The count moved while the create waited for its lock, or the wait
    /// timed out; retry.
    Contended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum LockMode {
    Shared,
    Exclusive,
}

/// The lock mode each limit of one create takes, from a lock-free count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SandboxQuotaPlan {
    org_id: OrgId,
    org: Option<(u32, LockMode)>,
    subject: Option<(UserId, u32, LockMode)>,
}

impl SandboxQuotaPlan {
    fn strongest_mode(&self) -> metrics::SandboxQuotaLockMode {
        let org = self.org.map(|(_, mode)| mode);
        let subject = self.subject.map(|(_, _, mode)| mode);
        match org.max(subject) {
            Some(LockMode::Exclusive) => metrics::SandboxQuotaLockMode::Exclusive,
            Some(LockMode::Shared) => metrics::SandboxQuotaLockMode::Shared,
            None => metrics::SandboxQuotaLockMode::None,
        }
    }
}

/// The margin below a limit within which a create taking the lock shared is
/// not admitted: a tenth of the limit, never less than [`MIN_LOCK_HEADROOM`].
/// Creates are serialized within twice this of the limit.
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
    /// Hold a genuinely new create to `quota`. A replay or a key conflict is
    /// not counted and is answered [`SandboxQuotaOutcome::Admit`], so the
    /// admission answers it as before. Call before any other work in the
    /// transaction (an admission-time warm-pool claim), so a refused create,
    /// or one waiting for its lock, holds no slot. On `Admit` the transaction
    /// holds the quota locks until it ends; record the create in it next.
    /// Otherwise drop it.
    pub(crate) async fn admit_new_create(
        &mut self,
        quota: SandboxQuota,
    ) -> Result<SandboxQuotaOutcome, PublicOperationStoreError> {
        if !matches!(
            self.observation(),
            ObservePublicOperationIdempotency::Missing
        ) {
            return Ok(SandboxQuotaOutcome::Admit);
        }
        let (outcome, lock_mode) = match self.plan_sandbox_quota(quota).await? {
            Err(refusal) => (
                SandboxQuotaOutcome::Refuse(refusal),
                metrics::SandboxQuotaLockMode::None,
            ),
            Ok(plan) => (
                self.enforce_sandbox_quota(plan).await?,
                plan.strongest_mode(),
            ),
        };
        let label = match outcome {
            SandboxQuotaOutcome::Admit => metrics::SandboxQuotaOutcome::Admit,
            SandboxQuotaOutcome::Refuse(SandboxQuotaRefusal::Org { .. }) => {
                metrics::SandboxQuotaOutcome::RefuseOrg
            }
            SandboxQuotaOutcome::Refuse(SandboxQuotaRefusal::Subject { .. }) => {
                metrics::SandboxQuotaOutcome::RefuseSubject
            }
            SandboxQuotaOutcome::Contended => metrics::SandboxQuotaOutcome::Contended,
        };
        metrics::sandbox_quota(label, lock_mode);
        Ok(outcome)
    }

    /// Count the caller's sandboxes without a lock: a create at or over a
    /// limit is refused here; otherwise the plan says how
    /// [`Self::enforce_sandbox_quota`] locks.
    async fn plan_sandbox_quota(
        &mut self,
        quota: SandboxQuota,
    ) -> Result<Result<SandboxQuotaPlan, SandboxQuotaRefusal>, PublicOperationStoreError> {
        let subject_id = quota.subject.map(|subject| subject.user_id);
        let held = sandboxes_held_tx(&mut self.tx, quota.org_id, subject_id).await?;
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
        Ok(Ok(SandboxQuotaPlan {
            org_id: quota.org_id,
            org,
            subject,
        }))
    }

    /// Take the plan's locks, held until the transaction ends, and count
    /// again. A lock wait that ends in `lock_timeout` or `statement_timeout`
    /// is [`SandboxQuotaOutcome::Contended`] and leaves the transaction
    /// aborted.
    async fn enforce_sandbox_quota(
        &mut self,
        plan: SandboxQuotaPlan,
    ) -> Result<SandboxQuotaOutcome, PublicOperationStoreError> {
        let locks = [
            plan.subject
                .map(|(user_id, _, mode)| (user_id.as_uuid(), SUBJECT_LOCK_SEED, mode)),
            plan.org
                .map(|(_, mode)| (plan.org_id.as_uuid(), ORG_LOCK_SEED, mode)),
        ];
        let lock_timeout = bound_lock_timeout_tx(&mut self.tx, QUOTA_LOCK_TIMEOUT).await?;
        for (id, seed, mode) in locks.into_iter().flatten() {
            match lock_tx(&mut self.tx, id, seed, mode).await {
                Ok(()) => {}
                Err(error)
                    if contention::is_lock_contention(&error)
                        || contention::is_query_canceled(&error) =>
                {
                    tracing::warn!(
                        error = %error,
                        org_id = %plan.org_id,
                        "native create gave up waiting for its sandbox quota lock; retryable"
                    );
                    return Ok(SandboxQuotaOutcome::Contended);
                }
                Err(error) => return Err(error.into()),
            }
        }
        restore_lock_timeout_tx(&mut self.tx, &lock_timeout).await?;
        let subject_id = plan.subject.map(|(user_id, _, _)| user_id);
        let held = sandboxes_held_tx(&mut self.tx, plan.org_id, subject_id).await?;
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

/// Bound this transaction's lock waits by `timeout` unless a shorter bound
/// is already set. Returns the `lock_timeout` in force before the call, for
/// [`restore_lock_timeout_tx`].
async fn bound_lock_timeout_tx(
    tx: &mut Transaction<'_, Postgres>,
    timeout: Duration,
) -> Result<String, sqlx::Error> {
    let previous: String = sqlx::query_scalar(sql!("SELECT current_setting('lock_timeout')"))
        .fetch_one(&mut **tx)
        .await?;
    // `set_config(..., is_local => true)` is `SET LOCAL` with a bind
    // parameter. `0` is no bound.
    sqlx::query(sql!(
        "SELECT set_config('lock_timeout', $1::text, TRUE)
          WHERE current_setting('lock_timeout')::interval = interval '0'
             OR current_setting('lock_timeout')::interval > $1::text::interval"
    ))
    .bind(format!("{}ms", timeout.as_millis()))
    .execute(&mut **tx)
    .await?;
    Ok(previous)
}

/// Return this transaction's `lock_timeout` to `previous`, as read by
/// [`bound_lock_timeout_tx`], keeping any value the caller had set for the
/// transaction or the session.
async fn restore_lock_timeout_tx(
    tx: &mut Transaction<'_, Postgres>,
    previous: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(previous)
        .execute(&mut **tx)
        .await?;
    Ok(())
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

/// The org's live sandboxes plus its in-flight native creates without a
/// live row, and the share of those `subject_id` owns or requested (zero
/// without one). An in-flight create whose row went terminal (a drain sweep
/// failed it while the create ran) still counts: it can retry and succeed.
///
/// One statement on purpose: split into a row count and an operation count,
/// two READ COMMITTED snapshots could miss a create whose row lands between
/// them, counted by neither half. It lives here, beside the
/// `public_operations` half this module owns and inside the admission
/// transaction, rather than as a composed verb of `pplx-space-sandbox-store`,
/// which owns `sandboxes`; moving it there has to keep it one statement. The
/// terminal set repeats the lifecycle CAS guards' list.
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
                          AND s.lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')
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
