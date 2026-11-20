//! Issuing a leased firewall rules target's first assignment with its node's
//! `FirewallRulesChanged` wakeup, both in the store's issuing transaction
//! ([`pplx_space_firewall_rules_store::issue_first_assignment`]). No worker
//! is called.
//!
//! [`run`] issues a canary's one target each tick and then leaves it alone,
//! due check included; checks, rolling and completion belong to a later
//! driver. It's off in every deployment: there is no production lease or
//! first-check delay, and the targets come from a start that isn't enabled.
//! Before anything enables it, every reader and rollback target of the queue
//! must decode the reason ([`ReconcileReason::FirewallRulesChanged`]), and
//! the control plane must be disposable.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_cluster_reconciler::enqueue_triggers_tx;
use pplx_space_firewall_rules_store::{
    Issuance, LeasedTarget, Wakeup, issue_canary_and_release, lease_first_canary,
};
use pplx_space_state_reconciliation::ReconcileReason;
use sqlx::{PgConnection, PgPool};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

/// The shortest tick [`run`] keeps, so a small one can't spin.
const MIN_TICK: Duration = Duration::from_secs(1);
/// Eligible targets considered per claim.
const LEASE_SCAN: u32 = 16;

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

/// What [`run`] runs with; only tests have one.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Config {
    /// Raised to [`MIN_TICK`].
    pub(crate) tick: Duration,
    /// How long a claim holds the target before it issues.
    pub(crate) lease: Duration,
    /// When the issued target's first check falls due.
    pub(crate) first_check_in: Duration,
}

/// Lease the canary's target if none is issued yet, and issue it. `None`
/// when nothing was leased.
pub(crate) async fn tick(pool: &PgPool, config: &Config) -> Result<Option<Issuance>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let Some(target) = lease_first_canary(&mut conn, config.lease, LEASE_SCAN).await? else {
        return Ok(None);
    };
    // A claim that ends here unissued lapses, and a later tick takes it back.
    issue_canary_and_release(
        &mut conn,
        &target,
        config.first_check_in,
        &mut FirewallWakeup,
    )
    .await
    .map(Some)
}

/// Tick under `config` until `shutdown`. With no config, wait for shutdown
/// without touching the database.
pub(crate) async fn run(config: Option<Config>, pool: PgPool, shutdown: CancellationToken) {
    let Some(config) = config else {
        tracing::info!("firewall rules canary issuance is off; waiting for shutdown");
        shutdown.cancelled().await;
        return;
    };
    let mut interval = tokio::time::interval(config.tick.max(MIN_TICK));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            () = shutdown.cancelled() => return,
        }
        // Raced against shutdown: a dropped transaction rolls back.
        let outcome = tokio::select! {
            outcome = tick(&pool, &config) => outcome,
            () = shutdown.cancelled() => return,
        };
        match outcome {
            Ok(Some(issuance)) => tracing::info!(?issuance, "firewall rules canary issuance"),
            Ok(None) => tracing::debug!("firewall rules canary: nothing to issue"),
            Err(error) => {
                tracing::warn!(%error, "firewall rules canary issuance failed; retrying next tick");
            }
        }
    }
}
