//! Driving one leased item: reading the sandbox, asking the dispatcher to
//! move it, and recording the outcome under the lease.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_metrics::runtime_upgrade::{self as metrics, CampaignItemOutcome};
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, CampaignId, CampaignKind, ItemError, ItemOutcome, LeasedItem,
};
use sqlx::PgPool;
use tokio::time::Instant;

use super::MAX_ATTEMPTS;

/// How the node-side upgrade of a stopped sandbox ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoppedUpgrade {
    /// Booted under the cluster default and stopped again.
    Upgraded,
    /// Already on the cluster default; nothing was booted.
    AlreadyOnTarget,
    /// The upgrade's own decision moved nothing, for the reason given: the
    /// policy or catalog changed since the item was found, for example.
    NotMoved(String),
}

/// A dispatch that didn't finish.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DispatchError {
    /// Worth another lease: the sandbox is busy, the node is unavailable, or
    /// a deadline passed. The sandbox is left as it was.
    #[error("{0}")]
    Retryable(String),
    /// The upgrade ran and failed; counts against the failure budget. The
    /// sandbox is left stopped on its old environment.
    #[error("{0}")]
    Failed(String),
}

/// What the reconciler asks of the substrate for one sandbox. Every call is
/// idempotent and safe to retry after any failure, including one that left
/// the caller unsure whether it ran.
pub trait SandboxUpgradeDispatcher: Send + Sync + 'static {
    /// Upgrade a stopped sandbox to the cluster default, leaving it stopped.
    fn upgrade_stopped(
        &self,
        sandbox_id: SandboxId,
    ) -> impl Future<Output = Result<StoppedUpgrade, DispatchError>> + Send;

    /// Stop a started or paused sandbox, so it can take the stopped path.
    fn stop(&self, sandbox_id: SandboxId)
    -> impl Future<Output = Result<(), DispatchError>> + Send;
}

/// What to do with a sandbox in `lifecycle`.
enum Step {
    /// Upgrade it as it is: stopped.
    Upgrade,
    /// Stop it, then upgrade it.
    StopThenUpgrade,
    /// Leave the item for a later lease, without spending an attempt.
    Release(String),
    /// Not this campaign's to move.
    Skip(String),
}

fn step(lifecycle: &str, takes_running: bool) -> Step {
    let Ok(parsed) = lifecycle.parse::<SandboxLifecycle>() else {
        return Step::Skip(format!(
            "the sandbox's lifecycle {lifecycle:?} isn't upgradable"
        ));
    };
    match parsed {
        SandboxLifecycle::Stopped => Step::Upgrade,
        SandboxLifecycle::Started | SandboxLifecycle::Paused if takes_running => {
            Step::StopThenUpgrade
        }
        SandboxLifecycle::Started | SandboxLifecycle::Paused => Step::Skip(format!(
            "the sandbox is {lifecycle}, and this campaign moves only stopped sandboxes"
        )),
        SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Suspending
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Checkpointing => Step::Release(format!(
            "the sandbox is {lifecycle}; retrying once it settles"
        )),
        SandboxLifecycle::Created
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Quarantined
        | SandboxLifecycle::Failed => Step::Skip(format!(
            "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
        )),
    }
}

/// The outcome a dispatch error records on lease `attempts`: a retryable
/// error fails the item once its attempts are used up.
fn error_outcome(error: DispatchError, attempts: u64) -> ItemOutcome {
    match error {
        DispatchError::Retryable(message) if attempts >= MAX_ATTEMPTS => ItemOutcome::Failed(
            ItemError::new(format!("{message} (gave up after {attempts} attempts)")),
        ),
        DispatchError::Retryable(message) => ItemOutcome::Retry(ItemError::new(message)),
        DispatchError::Failed(message) => ItemOutcome::Failed(ItemError::new(message)),
    }
}

const fn outcome_metric(outcome: &ItemOutcome) -> CampaignItemOutcome {
    match outcome {
        ItemOutcome::Done => CampaignItemOutcome::Done,
        ItemOutcome::Skipped(_) => CampaignItemOutcome::Skipped,
        ItemOutcome::Failed(_) => CampaignItemOutcome::Failed,
        ItemOutcome::Retry(_) => CampaignItemOutcome::Retry,
        ItemOutcome::Release(_) => CampaignItemOutcome::Released,
    }
}

/// What a dispatch task needs, and nothing else from the reconciler.
pub(super) struct Worker<D> {
    pool: PgPool,
    dispatcher: Arc<D>,
    lease: Duration,
}

impl<D> Clone for Worker<D> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            dispatcher: Arc::clone(&self.dispatcher),
            lease: self.lease,
        }
    }
}

impl<D: SandboxUpgradeDispatcher> Worker<D> {
    pub(super) const fn new(pool: PgPool, dispatcher: Arc<D>, lease: Duration) -> Self {
        Self {
            pool,
            dispatcher,
            lease,
        }
    }

    /// Move one leased item and record its outcome, renewing the lease while
    /// the dispatcher works. A lost lease drops the work and records
    /// nothing: whoever holds the item now finishes it. `leased_at` is when
    /// the lease was requested.
    pub(super) async fn drive_item(
        self,
        campaign_id: CampaignId,
        kind: CampaignKind,
        item: LeasedItem,
        takes_running: bool,
        leased_at: Instant,
    ) {
        let lease_lost = |reason: &str| {
            metrics::campaign_item_outcome(kind.as_str(), CampaignItemOutcome::LeaseLost);
            tracing::warn!(
                campaign_id = %campaign_id,
                sandbox_id = %item.sandbox_id,
                "runtime_upgrade: {reason}; leaving the item to its new holder"
            );
        };
        let outcome = tokio::select! {
            outcome = self.upgrade_item(&item, takes_running) => outcome,
            () = self.hold_lease(&item, leased_at) => {
                return lease_lost("lost the item's lease mid-upgrade");
            }
        };
        match store::finish_item(&self.pool, &item, &outcome).await {
            Ok(true) => {
                metrics::campaign_item_outcome(kind.as_str(), outcome_metric(&outcome));
                tracing::info!(
                    campaign_id = %campaign_id,
                    sandbox_id = %item.sandbox_id,
                    attempts = item.attempts,
                    outcome = outcome_metric(&outcome).as_str(),
                    error = ?outcome,
                    "runtime_upgrade: item finished"
                );
            }
            Ok(false) => lease_lost("the lease lapsed before the outcome was recorded"),
            Err(error) => tracing::warn!(
                campaign_id = %campaign_id,
                sandbox_id = %item.sandbox_id,
                %error,
                "runtime_upgrade: recording the outcome failed; the lease will lapse and the \
                 item will be re-driven"
            ),
        }
    }

    async fn upgrade_item(&self, item: &LeasedItem, takes_running: bool) -> ItemOutcome {
        let sandbox = match pplx_space_sandbox_store::get(&self.pool, item.sandbox_id).await {
            Ok(Some(sandbox)) => sandbox,
            Ok(None) => return ItemOutcome::Skipped(ItemError::new("the sandbox was deleted")),
            Err(error) => {
                return error_outcome(
                    DispatchError::Retryable(format!("reading the sandbox failed: {error}")),
                    item.attempts,
                );
            }
        };
        match step(&sandbox.lifecycle, takes_running) {
            Step::Upgrade => {}
            Step::StopThenUpgrade => {
                if let Err(error) = self.dispatcher.stop(item.sandbox_id).await {
                    return error_outcome(error, item.attempts);
                }
            }
            Step::Release(reason) => return ItemOutcome::Release(ItemError::new(reason)),
            Step::Skip(reason) => return ItemOutcome::Skipped(ItemError::new(reason)),
        }
        match self.dispatcher.upgrade_stopped(item.sandbox_id).await {
            Ok(StoppedUpgrade::Upgraded | StoppedUpgrade::AlreadyOnTarget) => ItemOutcome::Done,
            Ok(StoppedUpgrade::NotMoved(reason)) => ItemOutcome::Skipped(ItemError::new(reason)),
            Err(error) => error_outcome(error, item.attempts),
        }
    }

    /// Renew `item`'s lease at a third of its length until it's lost:
    /// another holder took it, or renewals kept failing until it lapsed.
    /// `leased_at` is when the lease was requested.
    async fn hold_lease(&self, item: &LeasedItem, leased_at: Instant) {
        // Clamped as the store clamps it, so this deadline is the database's
        // or earlier.
        let lease = self.lease.clamp(Duration::from_secs(1), store::MAX_LEASE);
        let every = pplx_space_api::public_operation_driver::claim_renewal_interval(lease);
        // The lease runs on the database clock from its own write. Measuring
        // from just before each write keeps this deadline no later than the
        // database's. Nothing waits past it: not a sleep between renewals, and
        // not a renewal still pending when it comes.
        let mut held_until = leased_at + lease;
        loop {
            tokio::time::sleep_until((Instant::now() + every).min(held_until)).await;
            if Instant::now() >= held_until {
                return;
            }
            let attempt = Instant::now();
            match tokio::time::timeout_at(held_until, store::renew_lease(&self.pool, item, lease))
                .await
            {
                Ok(Ok(true)) => held_until = attempt + lease,
                Ok(Ok(false)) | Err(_) => return,
                Ok(Err(error)) => tracing::warn!(
                    sandbox_id = %item.sandbox_id,
                    %error,
                    "runtime_upgrade: renewing the lease failed"
                ),
            }
        }
    }
}
