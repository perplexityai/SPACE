//! What the sandbox driver asks of the substrate, and how it reads a
//! sandbox's lifecycle into a step.

use std::future::Future;

use pplx_space_execution_spec::EnvironmentReference;
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
    /// The sandbox was started or paused on the source and is stopped now.
    Stopped,
    /// The sandbox was no longer started or paused on the source, for the
    /// reason given, and was left as it was.
    Declined(String),
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
    /// The call ran and failed; counts against the failure budget. A failed
    /// upgrade leaves the sandbox stopped on its old environment; a failed
    /// stop leaves it as the substrate's stop did.
    #[error("{0}")]
    Failed(String),
}

/// What the sandbox driver asks of the substrate for one sandbox. Every call
/// is idempotent and safe to retry after any failure, including one that
/// left the caller unsure whether it ran.
///
/// The loop drops a call's future when the item's lease is lost or its
/// deadline passes ([`super::super::CampaignDriver::upgrade`]), so the
/// effects of a dropped call must be fenced where they land.
pub trait SandboxUpgradeDispatcher: Send + Sync + 'static {
    /// Upgrade a stopped sandbox on `source` to `target`, the campaign's,
    /// leaving it stopped. The upgrade decides afresh where it acts, as a
    /// start would, from the sandbox's recorded environment and the policy
    /// then, and compares the sandbox's lifecycle and recorded environment
    /// where it decides, as a guarded transition does: a sandbox no longer
    /// stopped (a user started it since the driver read it) is
    /// [`StoppedUpgrade::NotMoved`] and is left running, a sandbox on
    /// `target` already is
    /// [`StoppedUpgrade::AlreadyOnTarget`] (an earlier lease's upgrade that
    /// published it but didn't finish its backup or retire is finished), and
    /// one on anything but `source` or `target` (started and stopped again
    /// under another environment since the driver read it) is
    /// [`StoppedUpgrade::NotMoved`]. So is a move to anything but `target`
    /// (the default changed since the lease), so it isn't recorded as this
    /// campaign's. Just before the call the driver checks only that the
    /// sandbox is stopped on `source` or `target`; whether it still is, and
    /// whether the policy still moves it to `target`, is this call's
    /// decision.
    fn upgrade_stopped(
        &self,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> impl Future<Output = Result<StoppedUpgrade, DispatchError>> + Send;

    /// Stop a started or paused sandbox on `source`, so it can take the
    /// stopped path to `target`. A stop loses the sandbox's processes and
    /// memory, and only a block of the source consents to that. So the stop
    /// decides both where it commits, as a guarded transition does, and
    /// declines ([`GuardedStop::Declined`]) unless the sandbox is still
    /// started or paused on `source` and the policy still blocks `source`
    /// with a certified upgrade edge to `target`: a sandbox restarted onto
    /// another environment, or whose source was unblocked, since the driver
    /// read it keeps running.
    fn stop(
        &self,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> impl Future<Output = Result<GuardedStop, DispatchError>> + Send;
}

/// What to do with a sandbox in a lifecycle.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Step {
    /// Upgrade it as it is: stopped.
    Upgrade,
    /// Stop it, then upgrade it.
    StopThenUpgrade,
    /// Mid-transition; decided by where it is (see `SandboxDriver::upgrade`).
    Settling(String),
    /// Not this campaign's to move.
    Skip(String),
}

/// The step for a sandbox in `lifecycle` (the stored text), for a campaign
/// that does or doesn't take running sandboxes.
pub(super) fn step(lifecycle: &str, include_running: bool) -> Step {
    // A deleted row is kept as `deleted` for a while; it isn't a lifecycle.
    if lifecycle == "deleted" {
        return Step::Skip("the sandbox was deleted".to_owned());
    }
    let Ok(parsed) = lifecycle.parse::<SandboxLifecycle>() else {
        return Step::Skip(format!(
            "the sandbox's lifecycle {lifecycle:?} isn't upgradable"
        ));
    };
    match parsed {
        SandboxLifecycle::Stopped => Step::Upgrade,
        SandboxLifecycle::Started | SandboxLifecycle::Paused if include_running => {
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
        | SandboxLifecycle::Checkpointing => Step::Settling(format!("the sandbox is {lifecycle}")),
        SandboxLifecycle::Created
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Quarantined
        | SandboxLifecycle::Failed => Step::Skip(format!(
            "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
        )),
    }
}
