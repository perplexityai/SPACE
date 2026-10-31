//! Runtime upgrades: decided when a sandbox resumes or starts, and driven by
//! the cluster controller's upgrade campaigns.
//!
//! * `runtime_upgrade.activation.decision` (counter): one per resume or start
//!   the decision ran for. Tags: `activation` (`start` or `restore`), `plan`
//!   (`as_recorded`, `restore_on_default`, `cold_boot_upgrade`, `stranded`, or
//!   `unattributed` when no recorded execution names an environment the
//!   catalog knows), and `cause` (`blocked`, `unlisted`, `deprecated`, or
//!   `none` when the environment still executes).
//! * `runtime_upgrade.campaigns` (gauge): unfinished campaigns, every tick of
//!   the `runtime_upgrade` reconciler. Tags: `kind` (`templates` or
//!   `sandboxes`) and `phase` (`canary`, `rolling` or `paused`); every
//!   combination is emitted, zero included, so a campaign that changes phase
//!   leaves no stale series.
//! * `runtime_upgrade.campaign.items` (gauge): one campaign's items by phase,
//!   every tick. Tags: `campaign_id`, `kind`, and `phase` (`pending`,
//!   `in_flight`, `done`, `failed` or `skipped`), zero included.
//! * `runtime_upgrade.campaign.stranded` (gauge): sandboxes a campaign's last
//!   full pass found on its source environment that it can't move, because
//!   no certified upgrade edge reaches the target. Tag: `campaign_id`.
//! * `runtime_upgrade.campaign.item_outcome` (counter): one per leased item
//!   the reconciler finished. Tags: `kind` and `outcome` (`done`, `skipped`,
//!   `failed`, `retry`, `released` when the sandbox was mid-transition and
//!   the lease didn't count as an attempt, or `lease_lost` when the lease
//!   lapsed before the outcome was recorded).
//! * `runtime_upgrade.campaign.redriven` (counter): items found in flight past
//!   their lease. Tags: `kind` and `result` (`requeued`, or `failed` when the
//!   item had used up its attempts).
//! * `runtime_upgrade.campaign.held` (counter): one per tick a leasing
//!   campaign leased nothing because the policy doesn't call for its move.
//!   Tags: `kind` and `reason` (`target_not_default`, `source_allowed`).
//! * `runtime_upgrade.campaign.auto_paused` (counter): campaigns that paused
//!   themselves at their failure budget. Tag: `kind`. Page on any.

use crate::emit::{counter, gauge, tag};

metric_vocabulary!(
    /// Why a leasing campaign leased nothing this tick.
    CampaignHold {
        /// The campaign's target isn't the cluster default; the node-side
        /// upgrade only boots under the default.
        TargetNotDefault => "target_not_default",
        /// The campaign's source is `allowed` again, so nothing calls for
        /// the move.
        SourceAllowed => "source_allowed",
    }
);

metric_vocabulary!(
    /// How a leased campaign item ended.
    CampaignItemOutcome {
        Done => "done",
        Skipped => "skipped",
        Failed => "failed",
        Retry => "retry",
        /// Returned to pending without spending an attempt: the sandbox was
        /// mid-transition.
        Released => "released",
        /// The lease lapsed or changed hands before the outcome was
        /// recorded.
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

/// Count one resume- or start-time runtime upgrade decision.
pub fn activation_decision(activation: &'static str, plan: &'static str, cause: &'static str) {
    counter(
        "runtime_upgrade.activation.decision",
        1,
        &[
            tag("activation", activation),
            tag("plan", plan),
            tag("cause", cause),
        ],
    );
}

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
pub fn campaign_held(kind: &'static str, reason: CampaignHold) {
    counter(
        "runtime_upgrade.campaign.held",
        1,
        &[tag("kind", kind), tag("reason", reason.as_str())],
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
