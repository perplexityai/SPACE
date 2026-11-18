//! Driving one leased item: asking the kind's driver to move it, holding the
//! lease while it works, and recording the outcome under the lease.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_runtime_upgrade_campaign_store::{
    self as store, Campaign, ItemError, ItemOutcome, LeasedItem,
};
use sqlx::PgPool;
use tokio::time::Instant;

use super::{CampaignDriver, MAX_ATTEMPTS};
use crate::metrics::runtime_upgrade::{self as metrics, CampaignItemOutcome};

/// The outcome to record for `outcome` on lease `attempts`: a retry on the
/// last allowed lease fails the item.
pub(super) fn recorded_outcome(outcome: ItemOutcome, attempts: u64) -> ItemOutcome {
    match outcome {
        ItemOutcome::Retry(error) if attempts >= MAX_ATTEMPTS => {
            let message = error.as_deref().unwrap_or("retry");
            ItemOutcome::Failed(ItemError::new(format!(
                "{message} (gave up after {attempts} attempts)"
            )))
        }
        outcome => outcome,
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

/// How long a worker treats a `lease` as its own from the write that
/// granted or renewed it: a tenth short of the lease, so it stops before the
/// database's lease lapses and another campaign may take the subject, with
/// room for the gap between the two clocks.
fn held_for(lease: Duration) -> Duration {
    lease - lease / 10
}

/// What a dispatch task needs, and nothing else from the reconciler.
#[derive(Clone)]
pub(super) struct Worker {
    pool: PgPool,
    lease: Duration,
    deadline: Duration,
}

impl Worker {
    pub(super) const fn new(pool: PgPool, lease: Duration, deadline: Duration) -> Self {
        Self {
            pool,
            lease,
            deadline,
        }
    }

    /// Move one leased item and record its outcome, renewing the lease while
    /// the driver works. A lost lease drops the work and records nothing:
    /// whoever holds the item now finishes it. `leased_at` is when the lease
    /// was requested.
    pub(super) async fn drive_item(
        self,
        driver: Arc<dyn CampaignDriver>,
        campaign: Campaign,
        item: LeasedItem,
        leased_at: Instant,
    ) {
        let (campaign_id, kind) = (campaign.id, campaign.kind);
        let lease_lost = |reason: &str| {
            metrics::campaign_item_outcome(kind.as_str(), CampaignItemOutcome::LeaseLost);
            tracing::warn!(
                campaign_id = %campaign_id,
                subject_id = %item.subject_id,
                "runtime_upgrade: {reason}; leaving the item to its new holder"
            );
        };
        // A dispatch that starts late, after its lease's local deadline,
        // holds nothing: another campaign may lease the subject once the
        // lease lapses, so the upgrade mustn't start at all.
        let lease = store::clamp_lease(self.lease);
        if Instant::now() >= leased_at + held_for(lease) {
            return lease_lost("the lease lapsed before the upgrade started");
        }
        // Renew once before touching the subject: a refusal means the lease
        // is no longer this worker's, or its campaign was aborted since the
        // lease, and the upgrade mustn't start. A failed renewal keeps the
        // lease's original deadline.
        let attempt = Instant::now();
        let held_from = match tokio::time::timeout_at(
            leased_at + held_for(lease),
            store::renew_lease(&self.pool, &item, lease),
        )
        .await
        {
            Ok(Ok(true)) => attempt,
            Ok(Ok(false)) => {
                return lease_lost(
                    "the lease was lost or its campaign finished before the upgrade started",
                );
            }
            Err(_) => return lease_lost("the lease lapsed before the upgrade started"),
            Ok(Err(error)) => {
                tracing::warn!(
                    subject_id = %item.subject_id,
                    %error,
                    "runtime_upgrade: renewing the lease before the upgrade failed"
                );
                leased_at
            }
        };
        let outcome = tokio::select! {
            // Lease loss first: when it and the upgrade are both ready, the
            // lease decides, and a lapsed lease never lets an upgrade run on.
            biased;
            () = self.hold_lease(&item, held_from) => {
                return lease_lost("lost the item's lease mid-upgrade");
            }
            outcome = driver.upgrade(&campaign, &item) => recorded_outcome(outcome, item.attempts),
            // Measured from the lease's request, so the whole dispatch,
            // however it's spread across renewals, fits in it.
            () = tokio::time::sleep_until(leased_at + self.deadline) => ItemOutcome::Failed(
                ItemError::new(format!(
                    "the upgrade didn't finish within {}s",
                    self.deadline.as_secs()
                )),
            ),
        };
        match store::finish_item(&self.pool, &item, &outcome).await {
            Ok(true) => {
                metrics::campaign_item_outcome(kind.as_str(), outcome_metric(&outcome));
                tracing::info!(
                    campaign_id = %campaign_id,
                    subject_id = %item.subject_id,
                    attempts = item.attempts,
                    outcome = outcome_metric(&outcome).as_str(),
                    error = ?outcome,
                    "runtime_upgrade: item finished"
                );
            }
            Ok(false) => lease_lost("the lease lapsed before the outcome was recorded"),
            Err(error) => tracing::warn!(
                campaign_id = %campaign_id,
                subject_id = %item.subject_id,
                %error,
                "runtime_upgrade: recording the outcome failed; the lease will lapse and the \
                 item will be re-driven"
            ),
        }
    }

    /// Renew `item`'s lease at a third of its length until it's lost:
    /// another holder took it, its campaign finished, or renewals kept
    /// failing until it lapsed. `held_from` is when the lease's latest write
    /// was requested: its grant, or the renewal before the upgrade.
    async fn hold_lease(&self, item: &LeasedItem, held_from: Instant) {
        // Clamped as the store clamps it, so this deadline is the database's
        // or earlier.
        let lease = store::clamp_lease(self.lease);
        let every = pplx_space_api::public_operation_driver::claim_renewal_interval(lease);
        // The lease runs on the database clock from its own write. Measuring
        // from just before each write, and stopping a margin short of the
        // lease, keeps this deadline before the database's. Nothing waits past
        // it: not a sleep between renewals, and not a renewal still pending
        // when it comes.
        let mut held_until = held_from + held_for(lease);
        loop {
            tokio::time::sleep_until((Instant::now() + every).min(held_until)).await;
            if Instant::now() >= held_until {
                return;
            }
            let attempt = Instant::now();
            match tokio::time::timeout_at(held_until, store::renew_lease(&self.pool, item, lease))
                .await
            {
                Ok(Ok(true)) => held_until = attempt + held_for(lease),
                Ok(Ok(false)) | Err(_) => return,
                Ok(Err(error)) => tracing::warn!(
                    subject_id = %item.subject_id,
                    %error,
                    "runtime_upgrade: renewing the lease failed"
                ),
            }
        }
    }
}
