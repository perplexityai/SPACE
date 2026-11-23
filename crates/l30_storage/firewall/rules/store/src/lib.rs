//! Owning store for firewall-rules rollouts in control-plane Postgres: the
//! rollout header, its per-node work items, and their supporting records.
//!
//! * `firewall_rules_rollouts`: the operator's rollout header.
//! * `firewall_rules_rollout_items`: the fixed set of nodes a rollout targets,
//!   and their work state.
//! * `firewall_rules_revisions`: immutable rulesets.
//! * `firewall_rules_control`: the default revision for newly enrolled nodes.
//! * `firewall_rules_node_assignments`: each node's current intent.
//! * `firewall_rules_node_observations`: each node's pending round and
//!   accepted observations.
//!
//! [`read_adopted`] reads the default a candidate is reviewed against, and
//! [`start_rollout`] saves a reviewed revision and the nodes it targets
//! without adopting it.
//!
//! [`lease_targets`] leases a rollout's saved targets to a controller under
//! the shared queue's rules ([`pplx_space_rollout_queue`]); [`renew_lease`],
//! [`requeue`] and [`redrive_expired`] keep or end the lease.
//! [`issue_first_assignment`] issues a leased target's first assignment with
//! its node's wakeup. [`lease_first_canary`] and [`issue_canary_and_release`]
//! do both for a canary's one target, leaving it pending once issued.
//!
//! [`open_round`] asks an issued target's node to verify that assignment,
//! with its wakeup. [`fetch_instructions`] reads one node's assignment and
//! pending round. [`accept_report`] records the node's answer to that round.

use std::fmt;

use pplx_space_rollout_queue::HeaderPhase;

mod fetch;
mod issue;
mod lease;
mod leased_item;
mod read;
mod report;
mod round;
mod start;

pub use fetch::{Assignment, NodeInstructions, PendingRound, fetch_instructions};
pub use issue::{
    Issuance, Refusal, Stamp, Wakeup, issue_canary_and_release, issue_first_assignment,
};
pub use lease::{
    LeasedTarget, Requeue, Work, lease_first_canary, lease_targets, redrive_expired, renew_lease,
    requeue,
};
pub use pplx_space_rollout_queue::{MAX_LEASE, RedriveReport, clamp_lease};
pub use read::{StoredStateError, read_adopted};
pub use report::{
    Acceptance, Census, CensusGap, Failure, InvalidReport, Report, ReportRefusal, SlotEvidence,
    SlotOutcome, UnknownReason, accept_report,
};
pub use round::{OpenRefusal, Opening, open_round};
pub use start::{
    AdoptedRevision, REASON_MAX_CHARS, Review, StartError, StartRollout, StartedRollout,
    start_rollout,
};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
#[cfg(test)]
mod tests;

/// A rollout's id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct RolloutId(pub uuid::Uuid);

impl fmt::Display for RolloutId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A revision's id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct RevisionId(pub uuid::Uuid);

impl fmt::Display for RevisionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// `firewall_rules_rollouts.phase` for each phase of the shared queue.
const fn rollout_phase(phase: HeaderPhase) -> &'static str {
    match phase {
        HeaderPhase::Canary => "canary",
        HeaderPhase::Rolling => "rolling",
        HeaderPhase::Paused => "paused",
        HeaderPhase::Completed => "completed",
        HeaderPhase::Aborted => "aborted",
    }
}

/// `firewall_rules_rollout_items.phase`. The schema leaves the value set to
/// this enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ItemPhase {
    Pending,
    InFlight,
    Done,
    Failed,
}

impl ItemPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

/// Which verbs a statement serves: all of them, or only
/// [`lease_first_canary`] and [`issue_canary_and_release`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scope {
    Any,
    FirstCanary,
}

/// Commit on `Ok`, roll back otherwise, so a verb's locks end with it rather
/// than when the connection is next used.
async fn end_transaction<T, E: From<sqlx::Error>>(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    result: Result<T, E>,
) -> Result<T, E> {
    match result {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(error) => {
            roll_back(tx).await;
            Err(error)
        }
    }
}

async fn roll_back(tx: sqlx::Transaction<'_, sqlx::Postgres>) {
    if let Err(rollback) = tx.rollback().await {
        tracing::warn!(
            error = %rollback,
            "couldn't roll back a firewall rules store verb's transaction"
        );
    }
}
