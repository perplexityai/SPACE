//! Owning store for firewall-rules rollouts in control-plane Postgres: the
//! rollout header, its per-node work items, and their supporting records.
//!
//! * `firewall_rules_rollouts`: the operator's rollout header.
//! * `firewall_rules_rollout_items`: the fixed set of nodes a rollout targets,
//!   and their work state.
//! * `firewall_rules_revisions`: immutable rulesets.
//! * `firewall_rules_control`: the default revision for newly enrolled nodes.
//! * `firewall_rules_node_assignments`: each node's current intent.
//! * `firewall_rules_node_observations`: each node's accepted observations.
//! * `firewall_rules_mutation_receipts`: idempotent responses to operator
//!   requests.
//!
//! [`lease_targets`] leases a rollout's saved targets to a controller under
//! the shared queue's rules ([`pplx_space_rollout_queue`]); [`renew_lease`],
//! [`requeue`] and [`redrive_expired`] keep or end the lease.

use std::fmt;

use pplx_space_rollout_queue::HeaderPhase;

mod lease;

pub use lease::{
    LeasedTarget, Requeue, Work, lease_targets, redrive_expired, renew_lease, requeue,
};
pub use pplx_space_rollout_queue::{MAX_LEASE, RedriveReport, clamp_lease};

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

/// Commit on `Ok`, roll back otherwise, so a verb's locks end with it rather
/// than when the connection is next used.
async fn end_transaction<T>(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    result: Result<T, sqlx::Error>,
) -> Result<T, sqlx::Error> {
    match result {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(error) => {
            if let Err(rollback) = tx.rollback().await {
                tracing::warn!(
                    error = %rollback,
                    "couldn't roll back a firewall rules store verb's transaction"
                );
            }
            Err(error)
        }
    }
}
