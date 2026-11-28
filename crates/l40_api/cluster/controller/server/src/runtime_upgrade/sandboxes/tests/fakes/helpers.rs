//! Shared lease, clock, execution, authorization, and recorded-stop helpers
//! for sandbox runtime-upgrade dispatcher tests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_api::controllers::substrate::runtime_upgrade::{
    RuntimePlan, UpgradeActivation, decide,
};
use pplx_space_runtime_upgrade_campaign_store::{
    CampaignId, CampaignPhase, ItemPhase, LeasedItem, SubjectKind,
};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::{Executor, PgPool, Postgres};

use super::super::*;
use super::FakeDispatcher;

pub(super) async fn has_active_item_lease<'a>(
    executor: impl Executor<'a, Database = Postgres>,
    item: &LeasedItem,
) -> bool {
    let leasing = [
        CampaignPhase::Canary.as_str(),
        CampaignPhase::Rolling.as_str(),
    ];
    let active_lease: Option<i32> = sqlx::query_scalar(sql!(
        "SELECT 1
         FROM runtime_upgrade_items AS i
         JOIN runtime_upgrade_campaigns AS c ON c.id = i.campaign_id
         WHERE i.campaign_id = $1
           AND i.subject_kind = $2
           AND i.subject_id = $3
           AND i.phase = $4
           AND i.lease_token = $5
           AND i.lease_expires_at > clock_timestamp()
           AND c.phase = ANY($6)"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .bind(&leasing[..])
    .fetch_optional(executor)
    .await
    .expect("checking active item lease");
    active_lease.is_some()
}

pub(super) async fn pause_and_hang_past_deadline(advance: Duration) {
    struct ResumeClockOnDrop;
    impl Drop for ResumeClockOnDrop {
        fn drop(&mut self) {
            tokio::time::resume();
        }
    }
    let _resume_clock = ResumeClockOnDrop;
    tokio::time::pause();
    tokio::time::advance(advance).await;
    std::future::pending::<()>().await;
}

pub(in crate::runtime_upgrade::sandboxes::tests) async fn active_k8s_stopped_execution(
    pool: &PgPool,
    sandbox: Uuid,
) -> Option<StoppedK8sExecution> {
    let row: Option<(i64, String)> = sqlx::query_as(sql!(
        "SELECT generation, BTRIM(pod_uid)
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active AND NULLIF(BTRIM(pod_uid), '') IS NOT NULL"
    ))
    .bind(sandbox)
    .fetch_optional(pool)
    .await
    .expect("reading active k8s execution before fake stop");
    row.and_then(|(generation, pod_uid)| {
        pplx_space_k8s_ids::PodUid::parse(pod_uid)
            .ok()
            .map(|pod_uid| StoppedK8sExecution {
                generation,
                pod_uid,
            })
    })
}

pub(super) fn stop_claim_refusal(
    inputs: &Inputs,
    record_stop: bool,
    source: EnvironmentReference,
    target: EnvironmentReference,
    memory_retired: bool,
) -> Option<GuardedStop> {
    if record_stop {
        let cold_boot_to_target = inputs.policy.default_environment == Some(target)
            && memory_retired
            && matches!(
                decide(
                    &inputs.policy,
                    UpgradeActivation::Start,
                    source,
                    inputs.catalog_entry(source),
                ),
                RuntimePlan::ColdBootUpgrade(ref mv) if mv.to == target
            );
        if !cold_boot_to_target {
            return Some(GuardedStop::Held(
                crate::runtime_upgrade::sandboxes::force_restarts::POLICY_STOPPED_FORCE_RESTARTING
                    .to_owned(),
            ));
        }
        return None;
    }
    if inputs.policy.default_environment != Some(target) {
        return Some(GuardedStop::Held(POLICY_STOPPED_MOVING_RUNNING.to_owned()));
    }
    match decide(
        &inputs.policy,
        UpgradeActivation::Start,
        source,
        inputs.catalog_entry(source),
    ) {
        RuntimePlan::ColdBootUpgrade(mv)
            if mv.to == target && mv.authorization(UpgradeActivation::Restore).memory_loss =>
        {
            None
        }
        RuntimePlan::ColdBootUpgrade(_) => Some(GuardedStop::Declined(
            POLICY_STOPPED_MOVING_RUNNING.to_owned(),
        )),
        RuntimePlan::Stranded(_) | RuntimePlan::AsRecorded | RuntimePlan::RestoreOnDefault(_) => {
            Some(GuardedStop::Held(POLICY_STOPPED_MOVING_RUNNING.to_owned()))
        }
    }
}

pub(super) fn start_claim_refusal(
    inputs: &Inputs,
    subject: &store::SubjectSandbox,
    source: EnvironmentReference,
    target: EnvironmentReference,
    memory_retired: bool,
) -> Option<TargetStart> {
    let effective = subject.effective_execution();
    let attributed = effective.and_then(|exec| inputs.attributed(exec));
    if attributed == Some(source) || effective.is_none() {
        let cold_boot_to_target = inputs.policy.default_environment == Some(target)
            && memory_retired
            && matches!(
                decide(
                    &inputs.policy,
                    UpgradeActivation::Start,
                    source,
                    inputs.catalog_entry(source),
                ),
                RuntimePlan::ColdBootUpgrade(ref mv) if mv.to == target
            );
        if !cold_boot_to_target {
            return Some(TargetStart::HeldStopped(
                crate::runtime_upgrade::sandboxes::force_restarts::POLICY_STOPPED_FORCE_RESTARTING
                    .to_owned(),
            ));
        }
        None
    } else if attributed == Some(target) {
        if inputs.policy.default_environment != Some(target)
            || inputs.policy.environments.get(&source) == Some(&RuntimeEnvironmentState::Allowed)
            || !memory_retired
        {
            return Some(TargetStart::HeldStopped(
                crate::runtime_upgrade::sandboxes::force_restarts::POLICY_STOPPED_FORCE_RESTARTING
                    .to_owned(),
            ));
        }
        None
    } else if effective.is_some_and(Inputs::is_unbound) {
        Some(TargetStart::HeldStopped(
            crate::runtime_upgrade::sandboxes::force_restarts::POLICY_STOPPED_FORCE_RESTARTING
                .to_owned(),
        ))
    } else {
        Some(TargetStart::Declined(
            "the sandbox moved to neither source nor target".to_owned(),
        ))
    }
}

impl FakeDispatcher {
    pub(super) fn memory_is_retired(&self, source: EnvironmentReference) -> bool {
        self.memory_retirement
            .as_ref()
            .is_none_or(|retirement| retirement.is_currently_retired(source))
    }

    pub(super) async fn item_phase_allows_stop_write(
        &self,
        campaign_id: CampaignId,
        subject_kind: SubjectKind,
        sandbox: Uuid,
    ) -> bool {
        let Some(pool) = &self.persist_pool else {
            return true;
        };
        let phase: Option<String> = sqlx::query_scalar(sql!(
            "SELECT phase
             FROM runtime_upgrade_items
             WHERE campaign_id = $1
               AND subject_kind = $2
               AND subject_id = $3"
        ))
        .bind(campaign_id)
        .bind(subject_kind.as_str())
        .bind(sandbox)
        .fetch_optional(pool)
        .await
        .expect("reading item phase before recording fake stop progress");
        !matches!(phase.as_deref(), Some("skipped" | "done") | None)
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) async fn record_item_initiated_stop(
        &self,
        item: Option<&LeasedItem>,
        sandbox: Uuid,
        initiated: InitiatedStop,
    ) -> InitiatedStop {
        let Some(leased) = item else {
            return initiated;
        };
        if !self
            .item_phase_allows_stop_write(
                leased.campaign_id,
                leased.subject_kind,
                leased.subject_id,
            )
            .await
        {
            return initiated;
        }
        let mut state = self.state();
        let key = (leased.campaign_id, sandbox);
        if let Some(RecordedStop::Initiated(existing)) = state.item_stopped_at.get(&key) {
            return existing.clone();
        }
        state
            .item_stopped_at
            .insert(key, RecordedStop::Initiated(initiated.clone()));
        initiated
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) async fn clear_initiated_stop(
        &self,
        item: Option<&LeasedItem>,
        sandbox: Uuid,
        expected_initiated: Option<&InitiatedStop>,
    ) {
        let Some(leased) = item else {
            return;
        };
        if let Some(pool) = &self.persist_pool
            && !has_active_item_lease(pool, leased).await
        {
            return;
        }
        let mut state = self.state();
        let key = (leased.campaign_id, sandbox);
        let matches_expected = match (state.item_stopped_at.get(&key), expected_initiated) {
            (Some(RecordedStop::Initiated(current)), Some(expected)) => current == expected,
            (Some(RecordedStop::Initiated(_)), None) => true,
            _ => false,
        };
        if matches_expected {
            state.item_stopped_at.remove(&key);
            state.node_admitted_campaign_stops.remove(&key);
        }
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) async fn record_item_stopped_at(
        &self,
        item: Option<&LeasedItem>,
        sandbox: Uuid,
        expected_initiated: Option<&InitiatedStop>,
        stop: StopFence,
    ) {
        let Some(leased) = item else {
            return;
        };
        if !self
            .item_phase_allows_stop_write(
                leased.campaign_id,
                leased.subject_kind,
                leased.subject_id,
            )
            .await
        {
            return;
        }
        let mut state = self.state();
        let key = (leased.campaign_id, sandbox);
        if let Some(expected) = expected_initiated {
            if !matches!(
                state.item_stopped_at.get(&key),
                Some(RecordedStop::Initiated(current)) if current == expected
            ) {
                return;
            }
        } else if !matches!(
            state.item_stopped_at.get(&key),
            None | Some(RecordedStop::Initiated(_))
        ) {
            return;
        }
        state
            .item_stopped_at
            .insert(key, RecordedStop::Stopped(stop));
    }

    pub(super) fn record_item_starting_on_target_sync(
        &self,
        campaign_id: CampaignId,
        sandbox: Uuid,
        claim: StartingClaim,
    ) {
        let mut state = self.state();
        if state
            .item_stopped_at
            .get(&(campaign_id, sandbox))
            .and_then(RecordedStop::as_fence)
            == Some(&claim.stop)
        {
            state.item_stopped_at.insert(
                (campaign_id, sandbox),
                RecordedStop::StartingOnTarget(claim),
            );
        }
    }

    pub(super) async fn record_item_starting_on_target(
        &self,
        campaign_id: CampaignId,
        subject_kind: SubjectKind,
        sandbox: Uuid,
        claim: StartingClaim,
    ) {
        let key = (campaign_id, sandbox);
        if let Some(pool) = &self.persist_pool {
            let mut tx = pool
                .begin()
                .await
                .expect("opening transaction to record fake item starting on target");
            let phase: Option<String> = sqlx::query_scalar(sql!(
                "SELECT phase
                 FROM runtime_upgrade_items
                 WHERE campaign_id = $1
                   AND subject_kind = $2
                   AND subject_id = $3
                 FOR UPDATE"
            ))
            .bind(campaign_id)
            .bind(subject_kind.as_str())
            .bind(sandbox)
            .fetch_optional(&mut *tx)
            .await
            .expect("locking item row before marking fake item starting on target");
            if matches!(phase.as_deref(), None | Some("done" | "skipped")) {
                return;
            }
            let matches_fence = self
                .state()
                .item_stopped_at
                .get(&key)
                .and_then(RecordedStop::as_fence)
                == Some(&claim.stop);
            if !matches_fence {
                return;
            }
            let marked =
                store::mark_starting_on_target(&mut *tx, campaign_id, subject_kind, sandbox)
                    .await
                    .expect("marking fake item as starting on target");
            if !marked {
                return;
            }
            tx.commit()
                .await
                .expect("committing fake item starting on target");
            self.record_item_starting_on_target_sync(campaign_id, sandbox, claim);
            return;
        }
        self.record_item_starting_on_target_sync(campaign_id, sandbox, claim);
    }

    pub(super) fn revert_item_starting_to_stopped_sync(
        &self,
        campaign_id: CampaignId,
        sandbox: Uuid,
        claim: &StartingClaim,
        rolled_back_stopped_at: jiff::Timestamp,
    ) {
        let mut state = self.state();
        let key = (campaign_id, sandbox);
        if state.item_stopped_at.get(&key) == Some(&RecordedStop::StartingOnTarget(claim.clone())) {
            state.item_stopped_at.insert(
                key,
                RecordedStop::Stopped(StopFence {
                    initiated_at: claim.stop.initiated_at,
                    dispatched_at: claim.stop.dispatched_at,
                    stopped_at: claim.stop.stopped_at,
                    settled_stopped_at: rolled_back_stopped_at,
                    k8s_stopped_execution: claim.stop.k8s_stopped_execution.clone(),
                }),
            );
            state.node_dispatched_campaign_starts.remove(&key);
        }
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) async fn record_item_started_on_target(
        &self,
        campaign_id: CampaignId,
        subject_kind: SubjectKind,
        sandbox: Uuid,
        stop: StopFence,
    ) {
        let key = (campaign_id, sandbox);
        if let Some(pool) = &self.persist_pool {
            let mut tx = pool
                .begin()
                .await
                .expect("opening transaction to record fake item started on target");
            let phase: Option<String> = sqlx::query_scalar(sql!(
                "SELECT phase
                 FROM runtime_upgrade_items
                 WHERE campaign_id = $1
                   AND subject_kind = $2
                   AND subject_id = $3
                 FOR UPDATE"
            ))
            .bind(campaign_id)
            .bind(subject_kind.as_str())
            .bind(sandbox)
            .fetch_optional(&mut *tx)
            .await
            .expect("locking item row before marking fake item started on target");
            if matches!(phase.as_deref(), None | Some("done" | "skipped")) {
                return;
            }
            let matches_starting_fence = matches!(
                self.state().item_stopped_at.get(&key),
                Some(RecordedStop::StartingOnTarget(claim)) if claim.stop == stop
            );
            if !matches_starting_fence {
                return;
            }
            let marked =
                store::mark_started_on_target(&mut *tx, campaign_id, subject_kind, sandbox)
                    .await
                    .expect("marking fake item as started on target");
            if !marked {
                return;
            }
            tx.commit()
                .await
                .expect("committing fake item started on target");
            {
                let mut state = self.state();
                if matches!(
                    state.item_stopped_at.get(&key),
                    Some(RecordedStop::StartingOnTarget(claim)) if claim.stop == stop
                ) {
                    state
                        .item_stopped_at
                        .insert(key, RecordedStop::StartedOnTarget(stop));
                    state.node_dispatched_campaign_starts.remove(&key);
                }
            }
            return;
        }
        let mut state = self.state();
        if matches!(
            state.item_stopped_at.get(&key),
            Some(RecordedStop::StartingOnTarget(claim)) if claim.stop == stop
        ) {
            state
                .item_stopped_at
                .insert(key, RecordedStop::StartedOnTarget(stop));
            state.node_dispatched_campaign_starts.remove(&key);
        }
    }

    /// Simulate a timed-out or deadline-cancelled `dispatcher.start` finishing on
    /// the node side via `internal-api` node-status publish, updating only the
    /// sandbox row and published execution metadata in Postgres (without any
    /// controller-side post-RPC write to `item_stopped_at`).
    pub(in crate::runtime_upgrade::sandboxes::tests) async fn settle_lingering_start(
        &self,
        pool: &PgPool,
        sandbox: Uuid,
        target: EnvironmentReference,
    ) -> TestResult {
        settle_started_lifecycle(pool, sandbox).await?;
        set_bound(pool, sandbox, target).await?;
        Ok(())
    }
}

pub(super) async fn settle_started_lifecycle(pool: &PgPool, sandbox: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = $2,
             current_local_artifact_id = CASE
                 WHEN current_local_artifact_kind = 'disk' THEN NULL
                 ELSE current_local_artifact_id
             END,
             current_local_artifact_kind = CASE
                 WHEN current_local_artifact_kind = 'disk' THEN NULL
                 ELSE current_local_artifact_kind
             END,
             current_local_artifact_origin = CASE
                 WHEN current_local_artifact_kind = 'disk' THEN NULL
                 ELSE current_local_artifact_origin
             END
         WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(SandboxLifecycle::Started.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) async fn check_stop_external_fences(
    pool: &PgPool,
    sandbox: Uuid,
    initiated_at: time::OffsetDateTime,
    dispatched_at: time::OffsetDateTime,
) -> Result<(), (bool, GuardedStop)> {
    match store::external_stop_since(pool, sandbox, initiated_at, dispatched_at)
        .await
        .expect("checking external stop operations during fake stop")
    {
        store::ExternalOperation::None => {}
        store::ExternalOperation::InFlight => {
            return Err((
                false,
                GuardedStop::Held(
                    crate::runtime_upgrade::sandboxes::force_restarts::EXTERNAL_OPERATION_IN_FLIGHT
                        .to_owned(),
                ),
            ));
        }
        store::ExternalOperation::Succeeded => {
            return Err((
                true,
                GuardedStop::Declined(
                    crate::runtime_upgrade::sandboxes::force_restarts::EXTERNAL_STOP_SUPERSEDED
                        .to_owned(),
                ),
            ));
        }
    }
    match store::external_activation_since(pool, sandbox, dispatched_at)
        .await
        .expect("checking external activation operations during fake stop")
    {
        store::ExternalOperation::None => Ok(()),
        store::ExternalOperation::InFlight => Err((
            false,
            GuardedStop::Held(
                crate::runtime_upgrade::sandboxes::force_restarts::EXTERNAL_OPERATION_IN_FLIGHT
                    .to_owned(),
            ),
        )),
        store::ExternalOperation::Succeeded => Err((
            true,
            GuardedStop::Declined(
                "an external activation superseded this campaign's stop".to_owned(),
            ),
        )),
    }
}

pub(super) async fn verify_converged_initiated_stop_execution(
    pool: &PgPool,
    sandbox: Uuid,
    initiated: &InitiatedStop,
    subject: &store::SubjectSandbox,
    superseded_reason: &str,
) -> Result<(), (bool, GuardedStop)> {
    if let Some(exec) = &initiated.k8s_stopped_execution {
        let floor = if subject.pinned_stop_disk {
            to_offset_date_time(initiated.dispatched_at).min(subject.lifecycle_changed_at)
        } else {
            subject.lifecycle_changed_at
        };
        match store::stopped_execution_ownership(
            pool,
            sandbox,
            floor,
            subject.lifecycle_changed_at,
            exec.generation,
            &exec.pod_uid,
        )
        .await
        .expect("checking stopped execution ownership for converged initiated stop")
        {
            store::StoppedExecutionOwnership::Owned => Ok(()),
            store::StoppedExecutionOwnership::InFlight => Err((
                false,
                GuardedStop::Held(
                    crate::runtime_upgrade::sandboxes::force_restarts::EXTERNAL_OPERATION_IN_FLIGHT
                        .to_owned(),
                ),
            )),
            store::StoppedExecutionOwnership::Superseded => {
                Err((true, GuardedStop::Declined(superseded_reason.to_owned())))
            }
        }
    } else if subject.pinned_stop_disk {
        Err((
            true,
            GuardedStop::Declined("the sandbox pinned a stop disk".to_owned()),
        ))
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct FakeMemoryRetirementState {
    retired: HashSet<EnvironmentReference>,
    last_observed: HashMap<EnvironmentReference, bool>,
    generation: u64,
    script: VecDeque<Result<bool, String>>,
}

#[derive(Clone, Default)]
pub(in crate::runtime_upgrade::sandboxes::tests) struct FakeMemoryRetirement {
    state: Arc<Mutex<FakeMemoryRetirementState>>,
}

impl FakeMemoryRetirement {
    pub(in crate::runtime_upgrade::sandboxes::tests) fn retired(
        environment: EnvironmentReference,
    ) -> Self {
        let source = Self::default();
        source.retire(environment);
        source
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) fn is_currently_retired(
        &self,
        environment: EnvironmentReference,
    ) -> bool {
        self.state.lock().unwrap().retired.contains(&environment)
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) fn retire(
        &self,
        environment: EnvironmentReference,
    ) {
        let mut state = self.state.lock().unwrap();
        if state.retired.insert(environment) && state.last_observed.get(&environment) != Some(&true)
        {
            state.generation += 1;
            state.last_observed.insert(environment, true);
        }
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) fn unretire(
        &self,
        environment: EnvironmentReference,
    ) {
        let mut state = self.state.lock().unwrap();
        if state.retired.remove(&environment)
            && state.last_observed.get(&environment) != Some(&false)
        {
            state.generation += 1;
            state.last_observed.insert(environment, false);
        }
    }

    pub(in crate::runtime_upgrade::sandboxes::tests) fn script_reads(
        &self,
        reads: impl IntoIterator<Item = Result<bool, String>>,
    ) {
        self.state.lock().unwrap().script.extend(reads);
    }
}

impl MemoryRetirementSource for FakeMemoryRetirement {
    async fn memory_retirement(
        &self,
        environment: EnvironmentReference,
    ) -> Result<MemoryRetirement, String> {
        let mut state = self.state.lock().unwrap();
        let baseline = state.retired.contains(&environment);
        let retired = match state.script.pop_front() {
            Some(scripted) => scripted?,
            None => baseline,
        };
        let previous = state
            .last_observed
            .insert(environment, retired)
            .unwrap_or(baseline);
        if retired != previous {
            state.generation += 1;
        }
        let generation = state.generation;
        Ok(MemoryRetirement {
            retired,
            generation,
        })
    }
}
