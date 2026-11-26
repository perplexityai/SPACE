//! The `sandbox_memory_drops` campaign driver: dropping the memory of a source
//! environment's `suspended` sandboxes offline, re-pinning each row to a
//! projected `disk` artifact while keeping it `suspended`, with no placement
//! and no node call.
//!
//! * **Plan.** Held while the campaign's target isn't the cluster default
//!   (`target_not_default`), while its source is `allowed` (`source_allowed`),
//!   or while its source's memory isn't retired (`memory_not_retired`,
//!   including when cleared mid-campaign while the source is `deprecated`).
//!   Once those clear, the start-time decision
//!   ([`pplx_space_api::controllers::substrate::runtime_upgrade::decide`])
//!   says whether the catalog certifies a cold-boot upgrade edge to the target
//!   or leaves the source's sandboxes stranded. The scan fingerprint covers the
//!   policy generation, the catalog, `legacy_unbound_allowed`, and the source's
//!   memory-retirement generation (`Inputs::read_retired_memory_plan`), matching
//!   how unbound `E0` and legacy checkpoints are attributed.
//! * **Subjects.** `suspended` sandboxes of the substrate whose current local
//!   artifact pin resolves to a `ready`, unorphaned `full` checkpoint
//!   attributed to the source — including checkpoints explicitly bound to the
//!   source, unbound `E0` checkpoints matching the catalog, and pre-metadata
//!   checkpoints when `legacy_unbound_allowed == Some(false)`
//!   ([`pplx_space_sandbox_store::suspended_environment_pins::candidates_page`]
//!   filtered via [`Inputs::attributed_checkpoint`]). Non-matching unbound rows
//!   are omitted rather than retired in `elsewhere` so a later catalog or
//!   `legacy_unbound_allowed` change that attributes them to `campaign.from`
//!   can still reopen a `skipped` item. Because items act in Postgres and
//!   object storage without a node, leasing uses the cluster-cap-only path
//!   (`!paces_per_node`), so a sandbox whose last publishing node is gone
//!   doesn't hold the campaign open, while keeping `SubjectKind::Sandbox` so
//!   one worker owns a sandbox at a time across all sandbox campaign kinds.
//! * **Upgrade.** Stateless in controller memory: checks
//!   [`SandboxMemoryDropDispatcher::memory_drop_completed`] first so a drop an
//!   earlier attempt already committed for `(campaign.id, sandbox_id)` finishes
//!   `done` even if `finish_item` failed or the sandbox later woke, deleted, or
//!   suspended a new `full` checkpoint on the source. Otherwise re-reads the
//!   sandbox's lifecycle, pinned `LocalArtifactId`, checkpoint execution
//!   columns, catalog, policy, and memory retirement fresh from Postgres via
//!   [`Self::inspect_pin`]. A sandbox mid-transition (`restoring`,
//!   `suspending`, etc.) is released back to `pending` without spending an
//!   attempt; an item already re-pinned to a `ready`, unorphaned `disk`
//!   projection attributed to the source finishes `done` without calling the
//!   dispatcher; one no longer pinned to the source is skipped; one still
//!   `full`-pinned on the source under a policy that temporarily stopped
//!   dropping its memory (`Hold` or `!moves`) is released back to `pending`
//!   without spending an attempt so re-retiring or certifying an edge resumes
//!   it without waiting for another full scan pass. Otherwise
//!   [`SandboxMemoryDropDispatcher::drop_memory`] projects that exact
//!   `expected_artifact_id` for `(campaign.id, campaign.from, campaign.to)` and
//!   re-pins the row under the sandbox's seal: `Dropped` and `AlreadyDropped`
//!   finish `done`; `Sealed` and `Held` release the item without spending an
//!   attempt; `Moved` checks durable per-campaign completion and [`Self::inspect_pin`]
//!   so a row mid-transition or rotated onto a new `full` checkpoint on the
//!   source is released while one that moved off is skipped (`done` if a
//!   concurrent drop committed its `disk` pin); `Ineligible` skips the item
//!   (restore-time memory drop cold-boots any that later wake onto the target,
//!   while `sandbox_population` keeps counting any that stay `suspended` on the
//!   source); `DispatchError::Retryable` checks
//!   [`SandboxMemoryDropDispatcher::memory_drop_completed`] and
//!   [`Self::inspect_pin`] so a committed drop whose reply was lost finishes
//!   `done` without ever dropping a second `full` checkpoint for the same
//!   subject before falling back to `Retry`; and `DispatchError::Failed` fails
//!   the item.

use std::sync::Arc;

use async_trait::async_trait;
use pplx_space_node_model::SubstrateKind;
use pplx_space_runtime_upgrade_campaign_store::{
    Campaign, CampaignId, CampaignKind, ItemError, ItemOutcome, LeasedItem,
};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_store::suspended_environment_pins::{
    self, PinnedCheckpointExecution, SandboxPinState,
};
use pplx_space_snapshot_model::{ArtifactKind, LocalArtifactId};
use sqlx::PgPool;
use uuid::Uuid;

use super::dispatcher::{
    DispatchError, SandboxMemoryDrop, SandboxMemoryDropDispatcher, is_mid_transition,
};
use super::{
    CampaignDriver, Inputs, MemoryRetirementSource, NO_LONGER_ON_SOURCE, Plan, PlanChange,
    RetiredMemoryPlan, ScanPlan, SubjectPage, SubjectScan, error_outcome, retired_memory_plan,
};

pub(super) const POLICY_STOPPED_DROPPING_MEMORY: &str =
    "the policy no longer drops this campaign's sandbox memory";

fn is_lifecycle_mid_transition(lifecycle: &str) -> bool {
    lifecycle
        .parse::<SandboxLifecycle>()
        .is_ok_and(is_mid_transition)
}

enum PinInspection {
    FullOnSource {
        artifact_id: LocalArtifactId,
        execution: PinnedCheckpointExecution,
    },
    AlreadyDiskOnSource,
    MidTransition(String),
    OffSource,
    ReadError(String),
}

/// Drives `sandbox_memory_drops` campaigns of one substrate's `suspended`
/// sandboxes pinned to the campaign's source environment.
pub struct SandboxMemoryDropDriver<D, M> {
    pool: PgPool,
    /// The substrate's `sandboxes.managed_by`: only its sandboxes are found.
    managed_by: SubstrateKind,
    dispatcher: Arc<D>,
    memory_retirement: Arc<M>,
}

impl<D: SandboxMemoryDropDispatcher, M: MemoryRetirementSource> SandboxMemoryDropDriver<D, M> {
    /// A driver for the `suspended` sandboxes `managed_by` owns, dropping
    /// their source-attributed `full` checkpoints to `disk` projections through
    /// `dispatcher` while `memory_retirement` reports the campaign's source
    /// memory retired.
    pub fn new(
        pool: PgPool,
        managed_by: SubstrateKind,
        dispatcher: D,
        memory_retirement: M,
    ) -> Self {
        Self {
            pool,
            managed_by,
            dispatcher: Arc::new(dispatcher),
            memory_retirement: Arc::new(memory_retirement),
        }
    }

    async fn read_plan(&self, campaign: &Campaign) -> Result<(Inputs, ScanPlan), PlanChange> {
        Inputs::read_retired_memory_plan(&self.pool, &*self.memory_retirement, campaign).await
    }

    async fn inspect_pin(&self, campaign: &Campaign, sandbox_id: SandboxId) -> PinInspection {
        let pin =
            match suspended_environment_pins::pin_state(&self.pool, self.managed_by, sandbox_id)
                .await
            {
                Ok(pin) => pin,
                Err(error) => {
                    return PinInspection::ReadError(format!(
                        "reading the sandbox's environment pin failed: {error}"
                    ));
                }
            };
        match pin {
            SandboxPinState::Absent => PinInspection::OffSource,
            SandboxPinState::NotPinned { lifecycle } => {
                if is_lifecycle_mid_transition(&lifecycle) {
                    PinInspection::MidTransition(lifecycle)
                } else {
                    PinInspection::OffSource
                }
            }
            SandboxPinState::SuspendedPinned {
                artifact_id,
                kind,
                execution,
            } => {
                let inputs = match Inputs::read(&self.pool).await {
                    Ok(inputs) => inputs,
                    Err(error) => return PinInspection::ReadError(error),
                };
                if inputs.attributed_checkpoint(&execution) != Some(campaign.from) {
                    return PinInspection::OffSource;
                }
                match kind {
                    ArtifactKind::Disk => PinInspection::AlreadyDiskOnSource,
                    ArtifactKind::Full => PinInspection::FullOnSource {
                        artifact_id,
                        execution,
                    },
                }
            }
        }
    }

    async fn check_completed_or_salvage_disk(
        &self,
        campaign: &Campaign,
        sandbox_id: SandboxId,
        context: Option<&str>,
    ) -> Option<ItemOutcome> {
        let format_error = |error: String| match context {
            Some(prefix) => ItemError::new(format!("{prefix}; {error}")),
            None => ItemError::new(error),
        };
        match self
            .dispatcher
            .memory_drop_completed(campaign.id, sandbox_id)
            .await
        {
            Ok(true) => Some(ItemOutcome::Done),
            Ok(false) => None,
            Err(DispatchError::Retryable(error)) => {
                if matches!(
                    self.inspect_pin(campaign, sandbox_id).await,
                    PinInspection::AlreadyDiskOnSource
                ) {
                    Some(ItemOutcome::Done)
                } else {
                    Some(ItemOutcome::Retry(format_error(error)))
                }
            }
            Err(DispatchError::Failed(error)) => Some(ItemOutcome::Failed(format_error(error))),
        }
    }

    async fn skip_unless_completed(
        &self,
        campaign: &Campaign,
        sandbox_id: SandboxId,
        reason: &str,
        context: Option<&str>,
    ) -> ItemOutcome {
        // Recheck durable per-campaign completion before recording `Skipped`: a
        // concurrent worker whose lease lapsed may have committed the memory
        // drop and had the sandbox wake or delete after this attempt's initial
        // `memory_drop_completed == false` read and before `inspect_pin` saw
        // `OffSource`.
        self.check_completed_or_salvage_disk(campaign, sandbox_id, context)
            .await
            .unwrap_or_else(|| ItemOutcome::Skipped(ItemError::new(reason)))
    }
}

#[async_trait]
impl<D: SandboxMemoryDropDispatcher, M: MemoryRetirementSource> CampaignDriver
    for SandboxMemoryDropDriver<D, M>
{
    fn kind(&self) -> CampaignKind {
        CampaignKind::SandboxMemoryDrops
    }

    fn retain_campaigns(&self, unfinished: &[Campaign]) {
        let ids: Vec<CampaignId> = unfinished.iter().map(|campaign| campaign.id).collect();
        self.dispatcher.retain_memory_drop_campaigns(&ids);
    }

    fn salvages_expired_final_leases(&self) -> bool {
        true
    }

    async fn completed_before_redrive(
        &self,
        campaign: &Campaign,
        subject_id: Uuid,
    ) -> Result<bool, String> {
        let sandbox_id = SandboxId::from_uuid(subject_id);
        match self
            .dispatcher
            .memory_drop_completed(campaign.id, sandbox_id)
            .await
        {
            Ok(true) => Ok(true),
            Ok(false) => match self.inspect_pin(campaign, sandbox_id).await {
                PinInspection::AlreadyDiskOnSource => Ok(true),
                PinInspection::ReadError(error) => Err(error),
                PinInspection::FullOnSource { .. }
                | PinInspection::MidTransition(_)
                | PinInspection::OffSource => {
                    match self
                        .dispatcher
                        .memory_drop_completed(campaign.id, sandbox_id)
                        .await
                    {
                        Ok(completed) => Ok(completed),
                        Err(DispatchError::Retryable(error)) => Err(error),
                        Err(DispatchError::Failed(_)) => Ok(false),
                    }
                }
            },
            Err(DispatchError::Retryable(error)) => {
                match self.inspect_pin(campaign, sandbox_id).await {
                    PinInspection::AlreadyDiskOnSource => Ok(true),
                    PinInspection::ReadError(pin_error) => Err(format!("{error}; {pin_error}")),
                    PinInspection::FullOnSource { .. }
                    | PinInspection::MidTransition(_)
                    | PinInspection::OffSource => Err(error),
                }
            }
            Err(DispatchError::Failed(_)) => Ok(false),
        }
    }

    async fn plan(&self, campaign: &Campaign) -> Result<Plan, String> {
        match self.read_plan(campaign).await {
            Ok((_, plan)) => Ok(Plan::Scan(plan)),
            Err(PlanChange::Hold(reason)) => Ok(Plan::Hold(reason)),
            Err(PlanChange::ReadError(error)) => Err(error),
        }
    }

    async fn find_subjects(
        &self,
        campaign: &Campaign,
        plan: ScanPlan,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<SubjectScan, String> {
        let inputs = match self.read_plan(campaign).await {
            Ok((inputs, now)) if now == plan => inputs,
            Ok(_) | Err(PlanChange::Hold(_)) => return Ok(SubjectScan::PlanChanged),
            Err(PlanChange::ReadError(error)) => return Err(error),
        };
        let page = suspended_environment_pins::candidates_page(
            &self.pool,
            self.managed_by,
            campaign.from,
            after.map(SandboxId::from_uuid),
            limit,
        )
        .await
        .map_err(|error| format!("reading candidates failed: {error}"))?;
        // Re-read the plan after `candidates_page` returns so a policy, catalog,
        // or memory-retirement shift while the query was running returns
        // `PlanChanged` instead of letting a short page end the pass and
        // complete the campaign under stale attribution inputs.
        match self.read_plan(campaign).await {
            Ok((_, now)) if now == plan => {}
            Ok(_) | Err(PlanChange::Hold(_)) => return Ok(SubjectScan::PlanChanged),
            Err(PlanChange::ReadError(error)) => return Err(error),
        }
        // Non-matching candidates stay out of `elsewhere`: the SQL prefilter
        // also yields unbound checkpoints whose `attribute` result is `None` or
        // another environment until catalog or `legacy_unbound_allowed` changes,
        // so retiring `skipped` rows into `RETIRED_ATTEMPTS` would keep them
        // from reopening when a later policy flip attributes them to `from`.
        // `ItemPhase::Done` stays terminal per campaign in
        // `store::insert_pending` (matching `templates`, `sandboxes`, and
        // `sandbox_force_restarts`): each campaign drops a subject's memory at
        // most once, and a subject that later acquires a new `from` checkpoint
        // (for instance across a mid-campaign `source_allowed` hold, like one
        // reaching `from` behind the scan cursor) is left to a later campaign
        // rather than reopening `Done`.
        let on_source = page
            .iter()
            .filter(|candidate| {
                inputs.attributed_checkpoint(&candidate.execution) == Some(campaign.from)
            })
            .map(|candidate| candidate.sandbox_id.as_uuid())
            .collect();
        Ok(SubjectScan::Page(SubjectPage {
            on_source,
            elsewhere: Vec::new(),
            last: page.last().map(|c| c.sandbox_id.as_uuid()),
            examined: page.len(),
        }))
    }

    async fn upgrade(&self, campaign: &Campaign, item: &LeasedItem) -> ItemOutcome {
        let sandbox_id = SandboxId::from_uuid(item.subject_id);
        if let Some(outcome) = self
            .check_completed_or_salvage_disk(campaign, sandbox_id, None)
            .await
        {
            return outcome;
        }

        let (artifact_id, execution) = match self.inspect_pin(campaign, sandbox_id).await {
            PinInspection::FullOnSource {
                artifact_id,
                execution,
            } => (artifact_id, execution),
            PinInspection::AlreadyDiskOnSource => return ItemOutcome::Done,
            PinInspection::MidTransition(lifecycle) => {
                return ItemOutcome::Release(ItemError::new(format!(
                    "the sandbox is {lifecycle}; retrying once it settles"
                )));
            }
            PinInspection::OffSource => {
                return self
                    .skip_unless_completed(campaign, sandbox_id, NO_LONGER_ON_SOURCE, None)
                    .await;
            }
            PinInspection::ReadError(error) => {
                return ItemOutcome::Retry(ItemError::new(error));
            }
        };

        // Dropping memory is irreversible, so re-read memory retirement and
        // then fresh policy/catalog inputs right before the call rather than
        // trusting lease-time or pre-retirement state. Releasing (instead of
        // skipping) leaves the still-eligible row `pending` without spending an
        // attempt so re-retiring or certifying an upgrade edge resumes it
        // immediately.
        let retirement = match self
            .memory_retirement
            .memory_retirement(campaign.from)
            .await
        {
            Ok(retirement) => retirement,
            Err(error) => {
                return ItemOutcome::Retry(ItemError::new(format!(
                    "reading memory retirement failed: {error}"
                )));
            }
        };
        let inputs = match Inputs::read(&self.pool).await {
            Ok(inputs) => inputs,
            Err(error) => return ItemOutcome::Retry(ItemError::new(error)),
        };
        if inputs.attributed_checkpoint(&execution) != Some(campaign.from) {
            return self
                .skip_unless_completed(campaign, sandbox_id, NO_LONGER_ON_SOURCE, None)
                .await;
        }
        match retired_memory_plan(
            &inputs.policy,
            inputs.catalog_entry(campaign.from),
            retirement.retired,
            campaign,
        ) {
            RetiredMemoryPlan::Scan { moves: true } => {}
            RetiredMemoryPlan::Scan { moves: false } | RetiredMemoryPlan::Hold(_) => {
                return ItemOutcome::Release(ItemError::new(POLICY_STOPPED_DROPPING_MEMORY));
            }
        }

        match self
            .dispatcher
            .drop_memory(
                campaign.id,
                sandbox_id,
                artifact_id,
                campaign.from,
                campaign.to,
            )
            .await
        {
            Ok(SandboxMemoryDrop::Dropped | SandboxMemoryDrop::AlreadyDropped) => ItemOutcome::Done,
            Ok(SandboxMemoryDrop::Sealed(reason) | SandboxMemoryDrop::Held(reason)) => {
                ItemOutcome::Release(ItemError::new(reason))
            }
            Ok(SandboxMemoryDrop::Moved(reason)) => {
                if let Some(outcome) = self
                    .check_completed_or_salvage_disk(campaign, sandbox_id, Some(&reason))
                    .await
                {
                    return outcome;
                }
                match self.inspect_pin(campaign, sandbox_id).await {
                    PinInspection::AlreadyDiskOnSource => ItemOutcome::Done,
                    PinInspection::FullOnSource { .. } | PinInspection::MidTransition(_) => {
                        ItemOutcome::Release(ItemError::new(reason))
                    }
                    PinInspection::OffSource => {
                        self.skip_unless_completed(campaign, sandbox_id, &reason, Some(&reason))
                            .await
                    }
                    PinInspection::ReadError(error) => {
                        ItemOutcome::Retry(ItemError::new(format!("{reason}; {error}")))
                    }
                }
            }
            Ok(SandboxMemoryDrop::Ineligible(reason)) => {
                self.skip_unless_completed(campaign, sandbox_id, &reason, Some(&reason))
                    .await
            }
            Err(DispatchError::Retryable(message)) => {
                // Check durable per-campaign completion first, then re-inspect the
                // current pin before recording `Retry`: if `drop_memory` already
                // committed the `disk` re-pin (even if the sandbox then woke,
                // deleted, or suspended a new `full` checkpoint on `campaign.from`),
                // finish `Done` without ever dropping a second checkpoint for this
                // subject; and if a concurrent transition moved an uncommitted
                // subject off `campaign.from` or into a transient lifecycle, record
                // that state rather than burning an attempt or failing on
                // `MAX_ATTEMPTS`.
                if let Some(outcome) = self
                    .check_completed_or_salvage_disk(campaign, sandbox_id, Some(&message))
                    .await
                {
                    return outcome;
                }
                match self.inspect_pin(campaign, sandbox_id).await {
                    PinInspection::AlreadyDiskOnSource => ItemOutcome::Done,
                    PinInspection::MidTransition(lifecycle) => {
                        ItemOutcome::Release(ItemError::new(format!(
                            "the sandbox is {lifecycle}; retrying once it settles"
                        )))
                    }
                    PinInspection::OffSource => {
                        self.skip_unless_completed(
                            campaign,
                            sandbox_id,
                            NO_LONGER_ON_SOURCE,
                            Some(&message),
                        )
                        .await
                    }
                    PinInspection::FullOnSource { .. } => {
                        ItemOutcome::Retry(ItemError::new(message))
                    }
                    PinInspection::ReadError(error) => {
                        ItemOutcome::Retry(ItemError::new(format!("{message}; {error}")))
                    }
                }
            }
            Err(error) => error_outcome(error),
        }
    }
}
