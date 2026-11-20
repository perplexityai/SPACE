//! Issuing a leased firewall rules target's first assignment with its node's
//! `FirewallRulesChanged` wakeup, both in the store's issuing transaction
//! ([`pplx_space_firewall_rules_store::issue_first_assignment`]). No worker
//! is called.
//!
//! Nothing runs it yet: no loop calls it, the frozen targets come from a
//! targeting step that isn't enabled, and there is no production first-check
//! delay. A deployment may enqueue the reason only once no controller it could
//! roll back to decodes reasons as the enum
//! ([`ReconcileReason::FirewallRulesChanged`]).

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_cluster_reconciler::enqueue_triggers_tx;
use pplx_space_firewall_rules_store::{Issuance, LeasedTarget, Wakeup};
use pplx_space_state_reconciliation::ReconcileReason;
use sqlx::PgConnection;

#[cfg(test)]
mod tests;

/// Queues the node's reconcile trigger with `FirewallRulesChanged`, coalescing
/// with any it already has.
pub struct FirewallWakeup;

impl Wakeup for FirewallWakeup {
    async fn wake(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        node: NodeId,
    ) -> Result<u64, sqlx::Error> {
        enqueue_triggers_tx(tx, [node], ReconcileReason::FirewallRulesChanged).await
    }
}

/// Issue `target`'s first assignment and wake its node, its first check due
/// `first_check_in` after issuance. The caller then ends the lease with
/// [`Requeue::Release`](pplx_space_firewall_rules_store::Requeue::Release).
pub async fn issue_first_assignment(
    conn: &mut PgConnection,
    target: &LeasedTarget,
    first_check_in: Duration,
) -> Result<Issuance, sqlx::Error> {
    pplx_space_firewall_rules_store::issue_first_assignment(
        conn,
        target,
        first_check_in,
        &mut FirewallWakeup,
    )
    .await
}
