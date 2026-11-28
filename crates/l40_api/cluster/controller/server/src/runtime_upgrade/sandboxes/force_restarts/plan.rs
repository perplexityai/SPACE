//! Pure planning, lifecycle-step, and candidate-page helpers for
//! [`super::SandboxForceRestartDriver`].

use pplx_space_api::controllers::substrate::runtime_upgrade::{
    CatalogEnvironment, RuntimePlan, UpgradeActivation, decide,
};
use pplx_space_node_model::SubstrateKind;
use pplx_space_runtime_environment_policy_store::{
    RuntimeEnvironmentPolicy, RuntimeEnvironmentState,
};
use pplx_space_runtime_upgrade_campaign_store::{self as store, Campaign};
use pplx_space_sandbox_model::SandboxLifecycle;
use sqlx::PgPool;
use uuid::Uuid;

use super::super::dispatcher::{InitiatedStop, MID_TRANSITION_LIFECYCLES, StopFence};
use super::super::{Inputs, MAX_ATTEMPTS, SubjectPage};
use super::SETTLED_SCANNED_LIFECYCLES;

/// What a leasing `sandbox_force_restarts` campaign does under the current
/// policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime_upgrade::sandboxes) enum ForceRestartPlan {
    /// Nothing calls for the move; the reason is the hold metric's tag.
    Hold(&'static str),
    /// Find the source's running and paused sandboxes. `moves` says whether
    /// a start cold-boots them onto the target or leaves them stranded.
    Scan { moves: bool },
}

pub(in crate::runtime_upgrade::sandboxes) fn force_restart_plan(
    policy: &RuntimeEnvironmentPolicy,
    from_catalog: Option<&CatalogEnvironment>,
    memory_retired: bool,
    campaign: &Campaign,
) -> ForceRestartPlan {
    if policy.default_environment != Some(campaign.to) {
        return ForceRestartPlan::Hold("target_not_default");
    }
    if policy.environments.get(&campaign.from) == Some(&RuntimeEnvironmentState::Allowed) {
        return ForceRestartPlan::Hold("source_allowed");
    }
    if !memory_retired {
        return ForceRestartPlan::Hold("memory_not_retired");
    }
    let moves = matches!(
        decide(
            policy,
            UpgradeActivation::Start,
            campaign.from,
            from_catalog
        ),
        RuntimePlan::ColdBootUpgrade(_)
    );
    ForceRestartPlan::Scan { moves }
}

/// What to do with a leased sandbox in `lifecycle`.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::runtime_upgrade::sandboxes) enum ForceRestartStep {
    /// Guarded stop on the source, then start onto the target.
    StopThenStart,
    /// Settle a prior attempt's [`RecordedStop::Initiated`] against a `'stopped'`
    /// sandbox row via [`SandboxUpgradeDispatcher::stop`], then start onto the
    /// target if the node confirms it admitted the campaign's stop.
    SettleInitiatedStop(InitiatedStop),
    /// Start a sandbox this campaign already stopped under `0`.
    StartStopped(StopFence),
    /// Leave the item for a later lease, without spending an attempt.
    Release(String),
    /// Not this campaign's to move.
    Skip(String),
}

pub(in crate::runtime_upgrade::sandboxes) fn force_restart_step(
    lifecycle: &str,
    owned_stop: Option<StopFence>,
    initiated_stop: Option<InitiatedStop>,
) -> ForceRestartStep {
    if lifecycle == "deleted" {
        return ForceRestartStep::Skip("the sandbox was deleted".to_owned());
    }
    let Ok(parsed) = lifecycle.parse::<SandboxLifecycle>() else {
        return ForceRestartStep::Skip(format!(
            "the sandbox's lifecycle {lifecycle:?} isn't upgradable"
        ));
    };
    match parsed {
        SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Suspending
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Checkpointing => ForceRestartStep::Release(format!(
            "the sandbox is {lifecycle}; retrying once it settles"
        )),
        SandboxLifecycle::Started | SandboxLifecycle::Paused => ForceRestartStep::StopThenStart,
        SandboxLifecycle::Stopped => match (owned_stop, initiated_stop) {
            (Some(stop), _) => ForceRestartStep::StartStopped(stop),
            (None, Some(initiated)) => ForceRestartStep::SettleInitiatedStop(initiated),
            (None, None) => ForceRestartStep::Skip(format!(
                "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
            )),
        },
        SandboxLifecycle::Created
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Quarantined
        | SandboxLifecycle::Failed => ForceRestartStep::Skip(format!(
            "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
        )),
    }
}

pub(super) async fn page_force_restart_candidates(
    inputs: &Inputs,
    pool: &PgPool,
    campaign: &Campaign,
    managed_by: SubstrateKind,
    after: Option<Uuid>,
    limit: u32,
) -> Result<SubjectPage, String> {
    let page = store::force_restart_candidates(
        pool,
        campaign.id,
        managed_by,
        SETTLED_SCANNED_LIFECYCLES,
        MID_TRANSITION_LIFECYCLES,
        after,
        limit,
        MAX_ATTEMPTS,
    )
    .await
    .map_err(|error| format!("reading candidates failed: {error}"))?;
    let on_source = page
        .iter()
        .filter(|candidate| match &candidate.execution {
            Some(execution) => inputs.on_source(execution, campaign),
            // An unpublished, status-only, or projection-pruned active or
            // mid-transition sandbox may already run or settle onto
            // `campaign.from`; recording it pending lets `upgrade` release it
            // without spending attempts until node publication and any
            // transition settle.
            None => true,
        })
        .map(|candidate| candidate.sandbox_id)
        .collect();
    Ok(SubjectPage {
        on_source,
        // Nothing is retired: a catalog or `legacy_unbound_allowed` toggle that
        // attributes an unbound sandbox elsewhere now may attribute it to the
        // source again, and a retired item is never found again.
        elsewhere: Vec::new(),
        last: page.last().map(|candidate| candidate.sandbox_id),
        examined: page.len(),
    })
}
