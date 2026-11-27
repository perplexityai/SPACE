//! The locks a writer under a lease takes before it judges its item, in the
//! lease verbs' order: the rollout row, the lease handoff lock (shared), then
//! the item row. Issuance and a round's opening take all three and read them
//! with [`lock_leased_item`]; a renewal takes the last two with
//! [`share_handoff`] and [`lock_item`], under the shared queue's own rollout
//! check.

use pplx_space_accounts::NodeId;
use pplx_space_rollout_queue::HeaderPhase;
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;
use time::OffsetDateTime;

use crate::lease::{HANDOFF_LOCK, stamped_work};
use crate::{ItemPhase, LeasedTarget, RolloutId, Work, rollout_phase};

/// How [`lock_leased_item`] holds the rollout row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeaderLock {
    /// A pause or abort waits; other shared holders don't.
    Share,
    /// Other writers under the rollout's leases wait too, as a canary's
    /// issuance needs.
    Update,
}

/// What [`lock_leased_item`] read under its locks.
pub(crate) struct LeasedItem {
    /// The rollout is in canary or rolling.
    pub(crate) working: bool,
    pub(crate) canary: bool,
    pub(crate) revision: uuid::Uuid,
    /// The item is in flight under the caller's token, and its lease is live
    /// on a clock read after any wait for the item row.
    pub(crate) leased: bool,
    generation: Option<i64>,
    issued_at: Option<OffsetDateTime>,
    pub(crate) next_check_at: Option<OffsetDateTime>,
}

impl LeasedItem {
    /// The work the item's issuance stamp leaves: the stamp, not the lease's
    /// hint, says what was issued.
    pub(crate) fn work(&self, target: &LeasedTarget) -> Result<Work, sqlx::Error> {
        stamped_work(
            target.rollout_id,
            target.node_id,
            self.generation,
            self.issued_at,
        )
    }
}

/// Lock `target`'s rollout row as `header` says, the lease handoff lock and
/// the item row, and read them. `None` when the rollout or the item doesn't
/// exist; the locks taken stay held either way, until the caller's
/// transaction ends.
pub(crate) async fn lock_leased_item(
    conn: &mut PgConnection,
    target: &LeasedTarget,
    header: HeaderLock,
) -> Result<Option<LeasedItem>, sqlx::Error> {
    let header = match header {
        HeaderLock::Share => sqlx::query_as::<_, (bool, bool, uuid::Uuid)>(sql!(
            "SELECT phase IN ($2, $3), phase = $2, to_revision_id
             FROM firewall_rules_rollouts
             WHERE id = $1
             FOR SHARE"
        )),
        HeaderLock::Update => sqlx::query_as::<_, (bool, bool, uuid::Uuid)>(sql!(
            "SELECT phase IN ($2, $3), phase = $2, to_revision_id
             FROM firewall_rules_rollouts
             WHERE id = $1
             FOR UPDATE"
        )),
    }
    .bind(target.rollout_id)
    .bind(rollout_phase(HeaderPhase::Canary))
    .bind(rollout_phase(HeaderPhase::Rolling))
    .fetch_optional(&mut *conn)
    .await?;
    let Some((working, canary, revision)) = header else {
        return Ok(None);
    };
    // Without it, a claim on another rollout could find this lease expired on
    // its clock while the caller writes under it on an earlier one, and both
    // commit. No test catches its removal.
    share_handoff(conn).await?;
    // Locked before it's read, so the lease is judged on a clock read after
    // any wait for the row.
    lock_item(conn, target.rollout_id, target.node_id).await?;
    let item = sqlx::query_as::<
        _,
        (
            bool,
            Option<i64>,
            Option<OffsetDateTime>,
            Option<OffsetDateTime>,
        ),
    >(sql!(
        "SELECT phase = $4 AND lease_token = $3 AND lease_expires_at > clock_timestamp(),
                issued_assignment_generation,
                issued_at,
                next_check_at
         FROM firewall_rules_rollout_items
         WHERE rollout_id = $1
           AND node_id = $2"
    ))
    .bind(target.rollout_id)
    .bind(target.node_id)
    .bind(target.lease_token)
    .bind(ItemPhase::InFlight.as_str())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(item.map(
        |(leased, generation, issued_at, next_check_at)| LeasedItem {
            working,
            canary,
            revision,
            leased,
            generation,
            issued_at,
            next_check_at,
        },
    ))
}

/// Take the lease handoff lock shared, as every writer under a lease does; a
/// claim takes it exclusively.
pub(crate) async fn share_handoff(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))"
    ))
    .bind(HANDOFF_LOCK)
    .execute(conn)
    .await?;
    Ok(())
}

/// Lock the item row, if there is one.
pub(crate) async fn lock_item(
    conn: &mut PgConnection,
    rollout_id: RolloutId,
    node_id: NodeId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "SELECT 1
         FROM firewall_rules_rollout_items
         WHERE rollout_id = $1
           AND node_id = $2
         FOR UPDATE"
    ))
    .bind(rollout_id)
    .bind(node_id)
    .fetch_optional(conn)
    .await?;
    Ok(())
}
