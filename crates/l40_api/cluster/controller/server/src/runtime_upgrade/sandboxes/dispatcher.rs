//! What the sandbox drivers ask of the substrate, and how they read a
//! sandbox's lifecycle into a step.

use std::future::Future;

use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_runtime_upgrade_campaign_store::CampaignId;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};

/// How the node-side upgrade of a stopped sandbox ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoppedUpgrade {
    /// Booted under the target and stopped again.
    Upgraded,
    /// Already on the target; nothing was booted.
    AlreadyOnTarget,
    /// The upgrade's own decision moved nothing, for the reason given: the
    /// policy or catalog changed since the item was found, for example.
    NotMoved(String),
}

/// How a guarded stop ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardedStop {
    /// The sandbox was started or paused on the source and is stopped now, with
    /// the `sandboxes.lifecycle_changed_at` that transition stamped.
    Stopped { stopped_at: jiff::Timestamp },
    /// The sandbox was no longer started or paused on the source, for the
    /// reason given, and was left as it was.
    Declined(String),
}

/// How starting a stopped sandbox onto a campaign's target ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetStart {
    /// Cold-booted under the target and started now.
    Started,
    /// Started on the target now without moving environments (for example,
    /// already upgraded onto the target while stopped and booted now, or
    /// already started on the target from a retried call).
    AlreadyOnTarget,
    /// Still stopped in the fenced `stopped_at` period without booting, for
    /// the reason given: the policy or catalog changed since the driver checked
    /// it so starting now wouldn't cold-boot onto the target, for example. Stop
    /// ownership stays intact so the item is released and cold-booted once the
    /// hold clears.
    HeldStopped(String),
    /// Left the fenced `stopped_at` state (for example, the user started and
    /// stopped it again since the driver checked it) or booted onto an
    /// environment other than the target, for the reason given.
    NotOnTarget(String),
}

/// Durable progress a `sandbox_force_restarts` campaign has already committed
/// in the substrate for one `(campaign_id, sandbox_id)` subject.
///
/// Persisted alongside [`SandboxUpgradeDispatcher::stop`] and
/// [`SandboxUpgradeDispatcher::start`] rather than in controller memory alone,
/// so a retry after a dropped or timed-out call, a lapsed lease, or a
/// controller restart sees what earlier attempts committed without ever booting
/// a sandbox a user stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForceRestartProgress {
    /// [`SandboxUpgradeDispatcher::stop`] committed a guarded stop stamping
    /// this `sandboxes.lifecycle_changed_at`, and [`SandboxUpgradeDispatcher::start`]
    /// hasn't moved or disowned it yet.
    Stopped { stopped_at: jiff::Timestamp },
    /// [`SandboxUpgradeDispatcher::start`] booted the sandbox onto the
    /// campaign's target (or confirmed an earlier attempt's boot onto the
    /// target), so a redriven lease records `Done` even if `finish_item`
    /// failed or the sandbox was later stopped, suspended, or deleted.
    StartedOnTarget,
}

/// A dispatch that didn't finish.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DispatchError {
    /// Worth another lease: the sandbox is busy, the node is unavailable, or
    /// a deadline of the dispatcher's own passed. The sandbox is left as it
    /// was. The worker's item deadline isn't one of these: past it, the item
    /// fails.
    #[error("{0}")]
    Retryable(String),
    /// The upgrade ran and failed; counts against the failure budget. The
    /// sandbox is left stopped on its old environment.
    #[error("{0}")]
    Failed(String),
}

/// What the sandbox drivers ask of the substrate for one sandbox. Every call
/// is idempotent and safe to retry after any failure, including one that
/// left the caller unsure whether it ran.
///
/// The loop drops a call's future when the item's lease is lost or its
/// deadline passes ([`super::super::CampaignDriver::upgrade`]), so the
/// effects of a dropped call must be fenced where they land.
pub trait SandboxUpgradeDispatcher: Send + Sync + 'static {
    /// Upgrade a stopped sandbox to `target`, the campaign's, leaving it
    /// stopped. The upgrade decides afresh where it acts, as a start would,
    /// from the sandbox's recorded environment and the policy then. A move to
    /// anything but `target` (the default changed since the lease) is
    /// [`StoppedUpgrade::NotMoved`], so it isn't recorded as this campaign's.
    /// Must keep `sandboxes.lifecycle` `'stopped'` and
    /// `sandboxes.lifecycle_changed_at` unchanged, updating only the recorded
    /// execution, so a coexisting `sandbox_force_restarts` campaign's
    /// `stopped_at` fence stays valid. The driver checks that the sandbox is
    /// still on the campaign's source or target just before the call.
    fn upgrade_stopped(
        &self,
        sandbox_id: SandboxId,
        target: EnvironmentReference,
    ) -> impl Future<Output = Result<StoppedUpgrade, DispatchError>> + Send;

    /// Stop a started or paused sandbox on `source`, so it can take the
    /// stopped or cold-boot start path. The stop compares the sandbox's
    /// lifecycle and environment where it decides, as a guarded transition
    /// does, and declines ([`GuardedStop::Declined`]) unless it's still
    /// started or paused on `source`: a sandbox restarted onto another
    /// environment since the driver read it keeps running.
    ///
    /// When `campaign_id` is `Some` (a `sandbox_force_restarts` stop), records
    /// `(campaign_id, sandbox_id)`'s stop provenance atomically with claiming
    /// or committing the stop transition, so if the call times out or its
    /// future is dropped after the stop was claimed,
    /// [`Self::force_restart_progress`] reports [`ForceRestartProgress::Stopped`]
    /// with the `sandboxes.lifecycle_changed_at` that stop stamped once the
    /// sandbox settles `stopped`.
    fn stop(
        &self,
        campaign_id: Option<CampaignId>,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
    ) -> impl Future<Output = Result<GuardedStop, DispatchError>> + Send;

    /// Start a stopped sandbox for `campaign_id` from the stopped lifecycle
    /// period stamped at `stopped_at` (`sandboxes.lifecycle_changed_at`),
    /// leaving it started on `target`, the campaign's. The start compares
    /// `stopped` and `stopped_at` where it claims the transition, as a guarded
    /// transition does, and declines ([`TargetStart::NotOnTarget`]) if the
    /// sandbox left that stopped state in the meantime (for example, the user
    /// started and stopped it again since the driver checked it).
    ///
    /// Decides afresh from the sandbox's recorded environment and the policy
    /// then: if the policy or catalog changed before the transition is claimed
    /// so starting now wouldn't land on `target` while the sandbox is still
    /// stopped at `stopped_at`, leaves it stopped with its
    /// [`ForceRestartProgress::Stopped`] intact and returns
    /// [`TargetStart::HeldStopped`]; cold-booting onto `target` records
    /// [`ForceRestartProgress::StartedOnTarget`] and returns
    /// [`TargetStart::Started`]; booting a fenced `stopped_at` sandbox whose
    /// recorded execution is already `target` (for example, after a coexisting
    /// stopped-sandbox upgrade) records [`ForceRestartProgress::StartedOnTarget`]
    /// and returns [`TargetStart::AlreadyOnTarget`]; and leaving the fenced
    /// state or booting onto another environment clears progress and returns
    /// [`TargetStart::NotOnTarget`].
    fn start(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
        stopped_at: jiff::Timestamp,
        target: EnvironmentReference,
    ) -> impl Future<Output = Result<TargetStart, DispatchError>> + Send;

    /// Durable force-restart progress `campaign_id` has already committed for
    /// `sandbox_id`, if any.
    fn force_restart_progress(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
    ) -> impl Future<Output = Result<Option<ForceRestartProgress>, DispatchError>> + Send;

    /// Clear any force-restart progress recorded for `(campaign_id, sandbox_id)`
    /// once the driver skips the item.
    fn clear_force_restart_progress(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;

    /// Drop any cached force-restart progress for campaigns no longer in
    /// `unfinished`.
    fn retain_force_restart_campaigns(&self, _unfinished: &[CampaignId]) {}
}

/// Whether `lifecycle` is an active transition rather than a settled state.
pub(super) const fn is_mid_transition(lifecycle: SandboxLifecycle) -> bool {
    matches!(
        lifecycle,
        SandboxLifecycle::Creating
            | SandboxLifecycle::Starting
            | SandboxLifecycle::Restoring
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Suspending
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Checkpointing
    )
}

/// What to do with a sandbox in a lifecycle.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Step {
    /// Upgrade it as it is: stopped.
    Upgrade,
    /// Stop it, then upgrade it.
    StopThenUpgrade,
    /// Leave the item for a later lease, without spending an attempt.
    Release(String),
    /// Not this campaign's to move.
    Skip(String),
}

/// The step for a sandbox in `lifecycle` (the stored text), for a campaign
/// that does or doesn't take running sandboxes.
pub(super) fn step(lifecycle: &str, include_running: bool) -> Step {
    let Ok(parsed) = lifecycle.parse::<SandboxLifecycle>() else {
        return Step::Skip(format!(
            "the sandbox's lifecycle {lifecycle:?} isn't upgradable"
        ));
    };
    if is_mid_transition(parsed) {
        return Step::Release(format!(
            "the sandbox is {lifecycle}; retrying once it settles"
        ));
    }
    match parsed {
        SandboxLifecycle::Stopped => Step::Upgrade,
        SandboxLifecycle::Started | SandboxLifecycle::Paused if include_running => {
            Step::StopThenUpgrade
        }
        SandboxLifecycle::Started | SandboxLifecycle::Paused => Step::Skip(format!(
            "the sandbox is {lifecycle}, and this campaign moves only stopped sandboxes"
        )),
        _ => Step::Skip(format!(
            "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
        )),
    }
}
