//! Runtime upgrade campaigns, driven by the `runtime_upgrade` reconciler.
//!
//! * `runtime_upgrade.campaigns` (gauge): unfinished campaigns, every tick of
//!   the `runtime_upgrade` reconciler. Tags: `kind` (`templates`, `sandboxes`,
//!   `sandbox_force_restarts`, or `sandbox_memory_drops`) and `phase`
//!   (`canary`, `rolling` or `paused`); every combination is emitted, zero
//!   included, so a campaign that changes phase leaves no stale series.
//! * `runtime_upgrade.campaign.items` (gauge): one campaign's items by phase,
//!   every tick. Tags: `campaign_id`, `kind`, and `phase` (`pending`,
//!   `in_flight`, `done`, `failed` or `skipped`), zero included.
//! * `runtime_upgrade.campaign.stranded` (gauge): subjects on a campaign's
//!   source that its driver says it can't move (for a sandbox campaign, no
//!   certified upgrade edge reaches the target), reported at the end of each
//!   whole pass: the campaign's pending items, which a stranded pass records
//!   but doesn't lease. `0` once a pass ends that moves. Tag: `campaign_id`.
//!   Like `campaign.items`, it keeps its last value once the campaign
//!   finishes.
//! * `runtime_upgrade.campaign.item_outcome` (counter): one per leased item
//!   the reconciler finished. Tags: `kind` and `outcome` (`done`, `skipped`,
//!   `failed`, `retry`, `released` when the item returned to pending without
//!   spending an attempt — a mid-transition subject, a driver-stopped or
//!   `suspended` sandbox waiting out a temporary policy hold, or a `suspended`
//!   sandbox sealed or re-pinned on the source during a memory drop — or
//!   `lease_lost` when the worker no longer held the lease, before the upgrade
//!   started or before its outcome was recorded: it lapsed, another holder took
//!   it, or its campaign was aborted). An item whose outcome write failed isn't
//!   counted.
//! * `runtime_upgrade.campaign.redriven` (counter): items found in flight past
//!   their lease. Tags: `kind` and `result` (`requeued`, or `failed` when the
//!   item had used up its attempts).
//! * `runtime_upgrade.campaign.held` (counter): one per tick a leasing
//!   campaign leased nothing because its driver held it. Tags: `kind` and
//!   `reason`, the driver's hold reason, a short `snake_case` label.
//! * `runtime_upgrade.campaign.auto_paused` (counter): campaigns that paused
//!   themselves at their failure budget. Tag: `kind`. Page on any.

use pplx_space_metrics_emit::{counter, gauge, metric_vocabulary, tag};

metric_vocabulary!(
    /// How a leased campaign item ended.
    CampaignItemOutcome {
        Done => "done",
        Skipped => "skipped",
        Failed => "failed",
        Retry => "retry",
        /// Returned to pending without spending an attempt: the subject was
        /// mid-transition, a driver-stopped or `suspended` sandbox waited out a
        /// temporary policy hold, or a `suspended` sandbox was sealed or
        /// re-pinned on the source during a memory drop.
        Released => "released",
        /// The worker no longer held the lease, before the upgrade started or
        /// before its outcome was recorded: it lapsed, another holder took
        /// it, or its campaign was aborted.
        LeaseLost => "lease_lost",
    }
);

metric_vocabulary!(
    /// What a re-drive did with an item found in flight past its lease.
    RedriveResult {
        Requeued => "requeued",
        Failed => "failed",
    }
);

/// Report how many unfinished campaigns of `kind` are in `phase`.
pub fn campaigns(kind: &'static str, phase: &'static str, count: u64) {
    gauge(
        "runtime_upgrade.campaigns",
        count as f64,
        &[tag("kind", kind), tag("phase", phase)],
    );
}

/// Report how many of one campaign's items are in `phase`.
pub fn campaign_items(campaign_id: &str, kind: &'static str, phase: &'static str, count: u64) {
    gauge(
        "runtime_upgrade.campaign.items",
        count as f64,
        &[
            tag("campaign_id", campaign_id),
            tag("kind", kind),
            tag("phase", phase),
        ],
    );
}

/// Report the stranded subjects a campaign's last full pass found.
pub fn campaign_stranded(campaign_id: &str, count: u64) {
    gauge(
        "runtime_upgrade.campaign.stranded",
        count as f64,
        &[tag("campaign_id", campaign_id)],
    );
}

/// Count one leased item the reconciler finished.
pub fn campaign_item_outcome(kind: &'static str, outcome: CampaignItemOutcome) {
    counter(
        "runtime_upgrade.campaign.item_outcome",
        1,
        &[tag("kind", kind), tag("outcome", outcome.as_str())],
    );
}

/// Count items re-driven past their lease.
pub fn campaign_redriven(kind: &'static str, result: RedriveResult, count: u64) {
    if count == 0 {
        return;
    }
    counter(
        "runtime_upgrade.campaign.redriven",
        count,
        &[tag("kind", kind), tag("result", result.as_str())],
    );
}

/// Count one tick a leasing campaign was held by the policy.
pub fn campaign_held(kind: &'static str, reason: &'static str) {
    counter(
        "runtime_upgrade.campaign.held",
        1,
        &[tag("kind", kind), tag("reason", reason)],
    );
}

/// Count a campaign that paused itself at its failure budget.
pub fn campaign_auto_paused(kind: &'static str) {
    counter(
        "runtime_upgrade.campaign.auto_paused",
        1,
        &[tag("kind", kind)],
    );
}
