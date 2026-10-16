use super::activation_support::auto_lifecycle_request;
use super::cleanup_support::{StateWriteRejected, state_write_result};
use super::grpc_lifecycle::SuspendRequestContext;
use super::network_runtime::*;
use super::*;
use pplx_space_common_core::types::SandboxRouteIdentityInvariantError;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};

const ROUTE_VERIFICATION_BUNDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const AUTO_LIFECYCLE_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const AUTO_LIFECYCLE_RECONCILE_CONCURRENCY: usize = 8;
/// How far a gated row's scan deadline is pushed out (see
/// `run_due_auto_lifecycle`). Without it the row is re-picked on every 5 s
/// tick — spawn, wake lock, due-stage recompute — for as long as it stays
/// transient, and a retained failed-restore row kept for retry has no
/// reclaim of its own to bound that. Sixty seconds is the same order as the
/// transitions that make a row transient (a restore or suspend is seconds
/// to a couple of minutes) and one node-ctl sync, so a row that settles
/// waits at most a minute for its stage to run again; it is far below the
/// smallest policy stage cadence (stages are minutes to hours), so no stage
/// is meaningfully late, and it is the same grace class as the
/// `AUTO_SUSPEND_RETRY_BASE_SECS` retry ladder's first rungs.
const AUTO_LIFECYCLE_GATED_REPICK_GRACE_SECS: u64 = 60;

#[derive(Debug, thiserror::Error)]
enum NetworkPolicyReconcileError {
    #[error(transparent)]
    Refresh(#[from] NetworkPolicyRefreshError),
    #[error(transparent)]
    Operation(#[from] Status),
    #[error("network policy reconcile task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

impl NetworkPolicyReconcileError {
    /// An abandoned refresh observed that its sandbox can no longer host a
    /// live net-gateway. A later policy change can enqueue fresh work if the
    /// sandbox becomes eligible again, but this queue row is complete.
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Refresh(error) if error.is_abandoned())
    }
}

async fn reconcile_policy_and_route_security<I, P, R, F>(
    state: SandboxState,
    identity: &SpaceApiRouteIdentity,
    route_identity_push: I,
    policy_reconcile: P,
    route_security_push: R,
) -> Result<ApplyDecisionKind, NetworkPolicyReconcileError>
where
    I: std::future::Future<Output = Result<NetGatewayRouteIdentityPushOutcome, Status>>,
    P: std::future::Future<Output = Result<ApplyDecisionKind, NetworkPolicyReconcileError>>,
    R: FnOnce(Option<u32>) -> F,
    F: std::future::Future<Output = Result<(), Status>>,
{
    if matches!(state, SandboxState::Starting | SandboxState::Restoring) {
        use IncompleteSandboxRouteIdentityError::*;
        use SandboxRouteIdentityInvariantError::*;

        match identity.clone().try_into_complete() {
            Ok(_) => {}
            Err(InvalidProfile(error @ UnexpectedPodUid)) => {
                // A contradictory profile is not pending runtime publication.
                // Keep policy convergence independent of identity validation.
                policy_reconcile.await?;
                return Err(Status::failed_precondition(format!(
                    "runtime route identity has an invalid substrate profile: {error}"
                ))
                .into());
            }
            Err(
                MissingSandboxId
                | MissingRouteGeneration
                | MissingCurrentNodeId
                | MissingSubstrateKind
                | MissingExecutionGeneration
                | MissingActivationAttemptId
                | InvalidProfile(MissingPodUid),
            ) => {
                // Gateway registration enables policy pulls before activation
                // publishes the runtime identity, including a Kubernetes Pod UID.
                // Keep policy hydration prompt; runtime publication enqueues
                // another attempt for identity and verification keys. Settled
                // states retain their normal checks for partial identities.
                return policy_reconcile.await;
            }
        }
    }
    // The identity result is held until policy reconciliation has run: policy
    // convergence does not depend on route identity, and failing out early
    // would leave the gateway on a stale policy until the retry.
    let identity_result = route_identity_push.await;
    let decision = policy_reconcile.await?;
    let identity_profile_version = identity_result?.identity_profile_version();
    route_security_push(identity_profile_version).await?;
    Ok(decision)
}

async fn run_network_policy_queue<F, Fut>(
    queue: super::network_policy::NetworkPolicyReconcileQueue,
    shutdown_token: CancellationToken,
    reconcile: F,
) where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<(), NetworkPolicyReconcileError>> + Send + 'static,
{
    use tokio_util::task::JoinMap;

    let mut jobs = JoinMap::<super::network_policy::NetworkPolicyReconcileAttempt, _>::new();
    let mut tick = tokio::time::interval(NETWORK_POLICY_RECONCILE_BATCH_DEBOUNCE);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if jobs.is_empty() {
            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                _ = queue.wait_for_work_or_retry_deadline() => {}
            }
        }
        queue.promote_due_retries(tokio::time::Instant::now());
        let available = NETWORK_POLICY_RECONCILE_CONCURRENCY - jobs.len();
        let pending = {
            let in_flight = jobs
                .keys()
                .map(|attempt| attempt.sandbox_id.as_str())
                .collect();
            queue.take_pending(&in_flight, available)
        };
        for attempt in pending {
            let work = reconcile(attempt.sandbox_id.clone());
            jobs.spawn(attempt, work);
        }
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => break,
            Some((attempt, result)) = jobs.join_next() => {
                let sandbox_id = &attempt.sandbox_id;
                let result = result.unwrap_or_else(|error| {
                    tracing::warn!(%sandbox_id, %error, "network policy reconcile task failed");
                    Err(error.into())
                });
                let failure = result.err().map(|error| error.to_string());
                queue.finish_attempt(attempt, failure.as_deref(), NETWORK_POLICY_RECONCILE_RETRY_DELAY);
            }
            _ = tick.tick() => {}
        }
    }
    tracing::info!("network policy reconciler shutting down");
}

impl SandboxManagerService {
    pub(super) async fn handle_auto_lifecycle_error(
        &self,
        storage_id: &str,
        sandbox_id: SandboxId,
        error: &Status,
    ) {
        if pplx_space_sbx_mgr_client::is_restart_retry_on_successor(error) {
            tracing::debug!(
                %sandbox_id,
                "auto lifecycle transition deferred to the successor"
            );
            return;
        }
        if error
            .metadata()
            .contains_key(super::cleanup::AUTO_LIFECYCLE_DELETE_BLOCKED_METADATA)
        {
            tracing::debug!(
                %sandbox_id,
                %error,
                "auto-lifecycle delete remains blocked by durable authority evidence"
            );
            return;
        }
        if error
            .metadata()
            .contains_key(super::cleanup::AUTO_LIFECYCLE_DELETE_CAS_LOST_METADATA)
        {
            tracing::debug!(
                %sandbox_id,
                %error,
                "auto-lifecycle delete will be re-evaluated after a concurrent policy change"
            );
            return;
        }
        let _writer = match self.state_writer_lease("AutoLifecycleError") {
            Ok(writer) => writer,
            Err(error) => {
                tracing::warn!(%sandbox_id, %error, "deferring lifecycle error persistence");
                return;
            }
        };
        if error
            .metadata()
            .contains_key(super::cleanup::AUTO_LIFECYCLE_DELETE_RECEIPT_CONFLICT_METADATA)
        {
            tracing::warn!(
                %sandbox_id,
                %error,
                "auto-lifecycle delete is waiting for conflicting receipt evidence to drain"
            );
            let Ok(db) = self.db_conn() else {
                return;
            };
            match crate::db::auto_lifecycle_delete_receipts::record_receipt_conflict_retry(
                &db, storage_id,
            ) {
                Ok(true) => emit_sandbox_event(
                    &sandbox_id.to_string(),
                    SandboxEventType::LifecycleTransitionCasLost,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .trigger(SandboxEventTrigger::AutoLifecycle)
                        .operation("auto_lifecycle_delete")
                        .reason("receipt_conflict")
                        .logged_by_caller(),
                ),
                Ok(false) => {}
                Err(persist_error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        %persist_error,
                        "failed to defer auto-lifecycle delete receipt conflict"
                    );
                }
            }
            return;
        }
        tracing::warn!(
            %sandbox_id,
            %error,
            "auto lifecycle transition failed"
        );
        // Read-modify-write under the same per-sandbox wake lock the
        // transitions use, keyed on the storage spelling (not the
        // canonicalized id) so legacy uppercase/braced ids share the mutex
        // with their transition/activity path: activity landing after the
        // failed transition released its lock must not be overwritten by this
        // stale failure — worst case, slow-retry-parked by a stale tenth one.
        let wake_lock = self.auto_lifecycle.wake_lock(storage_id);
        let _wake_guard = wake_lock.lock_owned().await;
        if let Ok(sandbox) = self.lookup_auto_lifecycle_sandbox(storage_id) {
            let mut runtime_state = sandbox.auto_lifecycle_state.clone();
            let due = due_stage(&sandbox.auto_lifecycle, &runtime_state, now_secs());
            let stage_context = due
                .as_ref()
                .map(|(stage_index, condition_set, action, due_at)| {
                format!(
                    "condition_set={condition_set} stage_index={stage_index} action={action} due_at={due_at}"
                )
            })
            .unwrap_or_else(|| "condition_set=<unknown>".to_string());
            tracing::warn!(
                %sandbox_id,
                stage = %stage_context,
                %error,
                "auto lifecycle transition failed"
            );
            let error_detail = format!("{stage_context} error={error}");
            let parked = if error.metadata().contains_key(
                super::grpc_lifecycle::AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA,
            ) {
                mark_error_and_disarm_conditions(
                    &mut runtime_state,
                    &sandbox.auto_lifecycle,
                    error_detail,
                );
                false
            } else if due
                .as_ref()
                .is_some_and(|(_, _, action, _)| *action == AutoLifecycleIdleAction::Suspend)
            {
                crate::auto_lifecycle::mark_suspend_error(
                    &mut runtime_state,
                    &sandbox.auto_lifecycle,
                    now_secs(),
                    error_detail,
                )
            } else {
                mark_error(&mut runtime_state, &sandbox.auto_lifecycle, error_detail);
                false
            };
            let persisted = {
                let Ok(db) = self.db_conn() else {
                    return;
                };
                crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &runtime_state)
            };
            match persisted {
                // Signal only once the slow-retry park actually persisted: a
                // failed write leaves the fast loop live and must not claim
                // the sandbox was parked.
                Ok(_) if parked => {
                    tracing::warn!(
                        %sandbox_id,
                        "auto-suspend retry budget exhausted; slow-retrying ~every 6h \
                         (sandbox left running, self-heals when the cause clears)"
                    );
                    pplx_space_metrics::sandbox::worker::auto_lifecycle_retries_exhausted(
                        AutoLifecycleIdleAction::Suspend,
                    );
                }
                Ok(_) => {}
                Err(persist_error) => tracing::warn!(
                    %sandbox_id,
                    %persist_error,
                    "failed to persist auto-lifecycle error state"
                ),
            }
        }
    }

    fn lookup_auto_lifecycle_sandbox(&self, id: &str) -> Result<crate::db::SandboxRow, Status> {
        let db = self
            .db
            .lock()
            .map_err(|_| Status::internal("lifecycle database mutex poisoned"))?;
        crate::db::get_sandbox(&db, id)
            .map_err(|error| Status::internal(format!("read auto lifecycle sandbox: {error}")))?
            .ok_or_else(|| {
                Status::not_found("sandbox disappeared before auto lifecycle transition")
            })
    }

    pub(super) async fn run_due_auto_lifecycle(&self, storage_id: &str) -> Result<(), Status> {
        let arrived_at = std::time::Instant::now();
        let sandbox = self.lookup_auto_lifecycle_sandbox(storage_id)?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let wake_guard = wake_lock.lock_owned().await;

        // Same arbitration as the Suspend RPC: a wake that completed while
        // this transition waited on the wake lock invalidates the due-ness
        // decision the scheduler made before queuing. The post-lock re-read
        // below usually catches this through the refreshed activity epoch,
        // but the wake's touch and this read race through separate DB writes;
        // the wake-completion record is the authoritative ordering. Skip and
        // let the next scheduler tick re-evaluate against settled state.
        if super::grpc_lifecycle::suspend_wake_arbitration_enabled()
            && super::grpc_lifecycle::suspend_superseded_by_wake(
                arrived_at,
                self.auto_lifecycle.last_wake_completed_at(&sandbox.id),
            )
        {
            tracing::info!(
                sandbox_id = %sandbox.id,
                "skipping due auto-lifecycle transition: a wake completed while \
                 waiting on the wake lock"
            );
            return Ok(());
        }

        let sandbox = self.lookup_auto_lifecycle_sandbox(&sandbox.id)?;
        if sandbox.auto_lifecycle.is_disabled() {
            return Ok(());
        }

        let now = now_secs();
        let mut runtime_state = sandbox.auto_lifecycle_state.clone();
        // This check shares the wake lock with admission. Release publishes its
        // count and timestamp synchronously, so zero never hides completion.
        if let Some(protection) = self.ingress_protection(&sandbox)? {
            let (count, released, _) = protection.observation();
            if count > 0 {
                pplx_space_metrics::sandbox::worker::ingress_lifecycle_deferred();
                self.defer_gated_auto_lifecycle_row(&sandbox.id, now)?;
                return Ok(());
            }
            if let Some(released) = released {
                crate::auto_lifecycle::apply_observed_access_touch(
                    &mut runtime_state,
                    &sandbox.auto_lifecycle,
                    released,
                );
                crate::db::update_auto_lifecycle_state(
                    &*self.db_conn()?,
                    &sandbox.id,
                    &runtime_state,
                )
                .map_err(|error| Status::internal(error.to_string()))?;
                protection.consumed(released);
            }
        }
        recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);

        let Some((stage_index, condition_set, action, due_at)) =
            due_stage(&sandbox.auto_lifecycle, &runtime_state, now)
        else {
            // Nothing due after the re-read: persist the recomputed runtime
            // state so a stale `next_auto_event_at` stops re-selecting the
            // row — but not on a mid-transition row, where the write would
            // bump `updated_at`. There only the scan deadline is
            // pushed out by a short grace so the row is not re-picked every
            // tick; it is reconsidered once settled.
            if !crate::db::auto_lifecycle_state_is_settled(sandbox.state) {
                self.defer_gated_auto_lifecycle_row(&sandbox.id, now)?;
                return Ok(());
            }
            let db = self.db_conn()?;
            crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &runtime_state).map_err(
                |e| Status::internal(format!("db auto_lifecycle_state update failed: {e}")),
            )?;
            return Ok(());
        };
        // A stage whose action can neither act on this state nor count it
        // as satisfied — a Suspend due on a mid-transition row, or on a
        // failed cross-node restore's retained `restoring` row — is left
        // exactly as found: recording it as fired would advance the policy
        // past a transition that never happened, and any write here bumps
        // `updated_at`, the quiescence clock the cluster-deleted eviction of
        // that retained row is waiting on. Only the scan deadline
        // is pushed out by a short grace (the runtime state keeps the true
        // one), so the row is neither re-picked every tick nor advanced; it
        // is reconsidered once it settles. `Delete` is not gated here: the
        // state machine force-deletes from transient states.
        if !crate::db::auto_lifecycle_action_can_act(action, sandbox.state) {
            tracing::debug!(
                sandbox_id = %sandbox.id,
                state = %sandbox.state,
                %action,
                "skipping due auto-lifecycle transition: the action cannot act on this \
                 state; nothing recorded"
            );
            self.defer_gated_auto_lifecycle_row(&sandbox.id, now)?;
            return Ok(());
        }
        // Join the writer set only after wake arbitration and due-policy
        // revalidation; keep the supervisor pinned through the transition.
        let _state_writer = self.state_writer_lease("AutoLifecycle")?;
        pplx_space_metrics::sandbox::worker::auto_lifecycle_transition_lateness(
            now.saturating_sub(due_at),
            action,
        );

        tracing::info!(
            sandbox_id = %sandbox.id,
            condition_set = %condition_set,
            stage_index,
            action = %action,
            due_at,
            "running auto lifecycle transition"
        );
        let transition = match action {
            AutoLifecycleIdleAction::None => AutoLifecycleTransition::Noop,
            AutoLifecycleIdleAction::Pause => {
                if sandbox.state == SandboxState::Started {
                    self.pause(auto_lifecycle_request(PauseRequest {
                        sandbox_id: sandbox.id.clone(),
                    }))
                    .await?;
                }
                AutoLifecycleTransition::Pause
            }
            AutoLifecycleIdleAction::Suspend => {
                if matches!(sandbox.state, SandboxState::Started | SandboxState::Paused) {
                    let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
                    let artifact_guard = artifact_lock.lock_owned().await;
                    let context = SuspendRequestContext::new(
                        sandbox.id.clone(),
                        true,
                        0,
                        true,
                        Some(SandboxEventTrigger::AutoLifecycle),
                    );
                    let (_, publication) = self
                        .prepare_suspend_snapshot_publication_after_locks(
                            &context,
                            &wake_guard,
                            &artifact_guard,
                        )
                        .await?;
                    self.complete_suspend_snapshot_publication(publication)
                        .await?;
                    drop(artifact_guard);
                }
                AutoLifecycleTransition::Suspend
            }
            AutoLifecycleIdleAction::Stop => {
                match sandbox.state {
                    SandboxState::Started | SandboxState::Paused => {
                        self.stop(auto_lifecycle_request(StopRequest {
                            sandbox_id: sandbox.id.clone(),
                            template_filter: None,
                            blocking: true,
                            immediate_kill: false,
                        }))
                        .await?;
                    }
                    SandboxState::Suspended => {
                        let stopped = self.stop_suspended_for_auto_lifecycle(&sandbox.id).await?;
                        if !stopped {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
                AutoLifecycleTransition::Stop
            }
            AutoLifecycleIdleAction::Delete => {
                self.delete_with_auto_lifecycle_expectation(
                    auto_lifecycle_request(DeleteRequest {
                        sandbox_id: sandbox.id.clone(),
                        template_filter: None,
                        blocking: true,
                    }),
                    Some(
                        crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation {
                            policy_fingerprint: pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                                &sandbox.auto_lifecycle,
                            ),
                            authority: sandbox.auto_lifecycle_authority,
                        },
                    ),
                )
                .await?;
                AutoLifecycleTransition::Delete
            }
        };

        if transition == AutoLifecycleTransition::Delete {
            return Ok(());
        }

        let sandbox = self.lookup_auto_lifecycle_sandbox(&sandbox.id)?;
        let mut runtime_state = sandbox.auto_lifecycle_state.clone();
        mark_transition(
            &mut runtime_state,
            &sandbox.auto_lifecycle,
            &condition_set,
            transition,
            now_secs(),
            stage_index,
        );
        let db = self.db_conn()?;
        crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &runtime_state)
            .map_err(|e| Status::internal(format!("db auto_lifecycle_state update failed: {e}")))?;
        drop(wake_guard);
        Ok(())
    }

    /// Auto-lifecycle Stop of a `suspended` row writes the node-local SQLite
    /// `sandboxes.state` directly: no VM runs, so no RPC is involved. It holds
    /// only the wake lock, which Stop and Delete admission and eviction do not
    /// take, so the row can leave `suspended` between the caller's re-read and
    /// this write. The write then refuses and this returns `false`: the caller
    /// skips the transition without recording it, and the next scan
    /// re-evaluates the row in its new state.
    async fn stop_suspended_for_auto_lifecycle(&self, sandbox_id: &str) -> Result<bool, Status> {
        const CONTEXT: &str = "reconcile suspended sandbox to stopped";
        let edge = &crate::db::edges::AUTO_LIFECYCLE_STOP_SUSPENDED;
        #[cfg(test)]
        {
            let barrier = self
                .auto_lifecycle
                .suspended_stop_barrier
                .lock()
                .unwrap()
                .clone();
            if let Some(barrier) = barrier {
                barrier.wait().await;
                barrier.wait().await;
            }
        }
        let outcome = crate::db::update_state_from(&*self.db_conn()?, sandbox_id, edge)
            .map_err(|error| Status::internal(format!("{CONTEXT}: {error}")))?;
        match state_write_result(outcome) {
            Ok(_) => Ok(true),
            Err(
                StateWriteRejected::FromState { found } | StateWriteRejected::Guard { found, .. },
            ) if !edge.from().contains(found) => {
                tracing::info!(
                    sandbox_id,
                    state = %found,
                    "skipping due auto-lifecycle Stop: the sandbox left suspended before its \
                     state write; nothing recorded"
                );
                Ok(false)
            }
            // A guard refusal of a row still `suspended`, or a missing row.
            Err(rejected) => Err(rejected.into_status(CONTEXT)),
        }
    }

    /// Push a gated row's scan deadline out by
    /// `AUTO_LIFECYCLE_GATED_REPICK_GRACE_SECS` without touching `updated_at`
    /// or the runtime state (see `crate::db::defer_auto_lifecycle_deadline`).
    fn defer_gated_auto_lifecycle_row(&self, sandbox_id: &str, now: u64) -> Result<(), Status> {
        let db = self.db_conn()?;
        crate::db::defer_auto_lifecycle_deadline(
            &db,
            sandbox_id,
            now.saturating_add(AUTO_LIFECYCLE_GATED_REPICK_GRACE_SECS),
        )
        .map_err(|e| Status::internal(format!("db auto_lifecycle deadline deferral failed: {e}")))
    }

    pub fn start_auto_lifecycle_scheduler(
        &self,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        task_tracker.spawn(self.clone().run_auto_lifecycle_scheduler(shutdown_token));
    }

    async fn run_auto_lifecycle_scheduler(self, shutdown_token: CancellationToken) {
        let _guard = self.critical_task_guard("auto lifecycle scheduler", &shutdown_token);
        let mut interval = tokio::time::interval(AUTO_LIFECYCLE_RECONCILE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut tasks = tokio_util::task::JoinMap::new();
        let mut cursor = crate::db::AutoLifecycleScan::default();
        loop {
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => break,
                Some((sandbox_id, result)) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(error) = result {
                        tracing::error!(%sandbox_id, %error, "auto lifecycle transition task failed; durable policy will be reconsidered");
                    }
                    continue;
                }
                _ = interval.tick() => {}
            }
            if let Ok(epochs) = self.route_stream_epochs.lock() {
                let mut count = 0;
                let mut oldest = 0.0_f64;
                for epoch in epochs.values() {
                    let (active, _, age) = epoch.ingress.observation();
                    count += active;
                    oldest = oldest.max(age.map_or(0.0, |age| age.as_secs_f64()));
                }
                pplx_space_metrics::sandbox::worker::ingress_protection(count, oldest);
            }
            let available = AUTO_LIFECYCLE_RECONCILE_CONCURRENCY.saturating_sub(tasks.len());
            pplx_space_metrics::sandbox::worker::auto_lifecycle_scheduler_saturated(available == 0);
            if available == 0 {
                continue;
            }
            let active = tasks.keys().copied().collect::<Vec<_>>();
            let service = self.clone();
            let scan = tokio::task::spawn_blocking(move || {
                let db = service.db_conn()?;
                let selected = crate::db::due_auto_lifecycle_batch(
                    &db,
                    now_secs(),
                    &active,
                    available,
                    &mut cursor,
                )
                .map_err(|error| Status::internal(format!("auto lifecycle discovery: {error}")))?;
                Ok::<_, Status>((cursor, selected))
            })
            .await;
            let (next_cursor, selected) = match scan {
                Ok(Ok(scan)) => scan,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "failed to list due auto lifecycle sandboxes");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "auto lifecycle scheduler scan task failed");
                    continue;
                }
            };
            if shutdown_token.is_cancelled() {
                break;
            }
            cursor = next_cursor;
            for due in selected {
                let service = self.clone();
                // Each due transition is a root of its own: the scheduler, not
                // a request, caused it, so it links to nothing.
                let span =
                    super::drain::detached_trace_span("AutoLifecycle", &due.sandbox_id.to_string());
                tasks.spawn(
                    due.sandbox_id,
                    async move {
                        if let Err(error) = service.run_due_auto_lifecycle(&due.storage_id).await {
                            service
                                .handle_auto_lifecycle_error(
                                    &due.storage_id,
                                    due.sandbox_id,
                                    &error,
                                )
                                .await;
                        }
                    }
                    .instrument(span),
                );
            }
        }
        tracing::info!(
            in_flight = tasks.len(),
            "auto lifecycle scheduler stopping admission and draining transitions"
        );
        while let Some((sandbox_id, result)) = tasks.join_next().await {
            if let Err(error) = result {
                tracing::error!(%sandbox_id, %error, "auto lifecycle transition task failed during shutdown");
            }
        }
        tracing::info!("auto lifecycle scheduler shut down");
    }

    pub(super) fn enqueue_network_policy_reconcile(&self, sandbox_id: impl Into<String>) {
        self.network_policy_reconcile_queue.enqueue(sandbox_id);
    }

    async fn enqueue_network_policy_batch(
        &self,
        cursor: &mut crate::db::inventory::SandboxInventoryScan,
    ) -> Result<usize, Status> {
        let mut scan = std::mem::take(cursor);
        let (scan, count) = self
            .blocking_io(move |service| {
                let rows = scan
                    .next_batch(&*service.db_conn()?)
                    .map_err(|error| Status::internal(format!("policy inventory: {error}")))?;
                let mut count = 0;
                for sandbox in rows {
                    if !Self::sandbox_is_pool_member(&sandbox)
                        && Self::sandbox_has_live_net_gateway(&sandbox)
                    {
                        service.enqueue_network_policy_reconcile(sandbox.id);
                        count += 1;
                    }
                }
                Ok((scan, count))
            })
            .await?;
        *cursor = scan;
        Ok(count)
    }

    /// Startup and key rotation visit the complete finite sweep, yielding between
    /// bounded pages. Ordinary periodic repair advances just one page per tick.
    pub(super) async fn enqueue_live_network_policy_reconciles(&self) -> usize {
        let mut cursor = crate::db::inventory::SandboxInventoryScan::default();
        let mut count = 0;
        loop {
            if self.shutdown_token.is_cancelled() {
                break;
            }
            match self.enqueue_network_policy_batch(&mut cursor).await {
                Ok(queued) => count += queued,
                Err(error) => {
                    tracing::warn!(%error, "policy inventory failed; periodic sweep will retry");
                    break;
                }
            }
            if cursor.sweep_complete() {
                break;
            }
        }
        count
    }

    pub fn start_network_policy_reconciler(
        &self,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        if self
            .network_policy_reconciler_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::warn!("network policy reconciler already started");
            return;
        }

        self.spawn_periodic_reconcile_sweep(shutdown_token.clone(), task_tracker);

        let service = self.clone();
        let guard = self.critical_task_guard("network policy reconciler", &shutdown_token);
        task_tracker.spawn(async move {
            let _guard = guard;
            let startup_count = service.enqueue_live_network_policy_reconciles().await;
            tracing::info!(
                queued_sandboxes = startup_count,
                "network policy reconciler started"
            );

            let queue = service.network_policy_reconcile_queue.clone();
            run_network_policy_queue(queue, shutdown_token, move |sandbox_id| {
                let service = service.clone();
                async move { service.reconcile_queued_network_policy(&sandbox_id).await }
            })
            .await;
        });
    }

    pub fn start_route_verification_bundle_refresher(
        &self,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        let service = self.clone();
        task_tracker.spawn(async move {
            let mut ticker = tokio::time::interval(ROUTE_VERIFICATION_BUNDLE_REFRESH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Startup refresh runs synchronously after acquiring the service
            // mutex, before production sockets are bound.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("route verification bundle refresher shutting down");
                        break;
                    }
                    _ = ticker.tick() => {}
                }
                let refresh_service = service.clone();
                let updated = match tokio::task::spawn_blocking(move || {
                    refresh_service.refresh_route_ed25519_verification_bundle_now()
                })
                .await
                {
                    Ok(updated) => updated,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "route verification bundle refresh task failed"
                        );
                        false
                    }
                };
                if updated {
                    let queued = service.enqueue_live_network_policy_reconciles().await;
                    tracing::info!(
                        queued_sandboxes = queued,
                        "queued net-gateway key distribution after route verification bundle refresh"
                    );
                }
            }
        });
    }

    /// Periodic backstop: re-enqueue all live sandboxes on a timer so config
    /// that has no event trigger of its own — notably the SPACE-API route
    /// identity the reconcile pushes to each gateway — converges on
    /// already-running gateways even when the network policy is unchanged.
    fn spawn_periodic_reconcile_sweep(
        &self,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        let interval = NETWORK_POLICY_RECONCILE_SWEEP_INTERVAL;
        let service = self.clone();
        task_tracker.spawn(async move {
            let mut cursor = crate::db::inventory::SandboxInventoryScan::default();
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires immediately; startup already enqueued the
            // live set, so consume it and sweep only on later ticks.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = ticker.tick() => {
                        let queued = match service.enqueue_network_policy_batch(&mut cursor).await {
                            Ok(queued) => queued,
                            Err(error) => {
                                tracing::warn!(%error, "periodic policy inventory failed; will retry");
                                continue;
                            }
                        };
                        if queued > 0 {
                            tracing::debug!(
                                queued_sandboxes = queued,
                                "periodic network policy reconcile sweep enqueued live sandboxes"
                            );
                        }
                    }
                }
            }
            tracing::info!("periodic network policy reconcile sweep shutting down");
        });
    }

    pub fn start_node_state_dirty_syncer(
        &self,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        let guard = self.critical_task_guard("node dirty outbox", &shutdown_token);
        crate::node_state_sync::start_dirty_outbox_worker(
            Arc::clone(&self.db),
            shutdown_token,
            task_tracker,
            guard,
        );
    }

    async fn reconcile_queued_network_policy(
        &self,
        sandbox_id: &str,
    ) -> Result<(), NetworkPolicyReconcileError> {
        let started_at = Instant::now();
        let result = match self.reconcile_network_policy_for_sandbox(sandbox_id).await {
            Ok(ApplyDecisionKind::Conflict) => {
                Err(Status::aborted("network policy reconcile returned conflict").into())
            }
            Ok(decision) => {
                tracing::debug!(sandbox_id, ?decision, "network policy reconciled");
                Ok(())
            }
            Err(error) if error.is_terminal() => {
                tracing::info!(sandbox_id, %error, "network policy reconcile abandoned");
                Ok(())
            }
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            tracing::warn!(sandbox_id, %error, "network policy reconcile failed");
        } else {
            let db = self.db_conn()?;
            if let Err(error) = crate::db::delete_network_policy_reconcile_rows(&db, [sandbox_id]) {
                tracing::warn!(sandbox_id, %error, "failed to delete completed network policy reconcile row");
            }
        }

        let outcome = if result.is_ok() { "ok" } else { "error" };
        pplx_space_metrics::reconcile::run_duration(
            "network-policy",
            "local",
            outcome,
            "queue",
            started_at,
        );
        pplx_space_metrics::reconcile::run_items("network-policy", "local", outcome, 1);
        result
    }

    async fn reconcile_network_policy_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<ApplyDecisionKind, NetworkPolicyReconcileError> {
        let sandbox = match self.lookup_sandbox(sandbox_id) {
            Ok(sandbox) => sandbox,
            Err(error) if error.code() == tonic::Code::NotFound => {
                return Ok(ApplyDecisionKind::Skip);
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(decision) = network_policy_reconcile_entry_decision(sandbox.state) {
            return Ok(decision);
        }
        if !Self::sandbox_has_live_net_gateway(&sandbox) {
            return Ok(ApplyDecisionKind::Skip);
        }
        if Self::sandbox_is_pool_member(&sandbox) {
            set_network_no_identity(&sandbox.id, "reconcile")
                .await
                .inspect_err(|_| {
                    pplx_space_metrics::reconcile::apply_failed(FACET_NETWORK_POLICY, "gateway");
                })?;
            pplx_space_metrics::reconcile::apply_applied(FACET_NETWORK_POLICY, "pool");
            return Ok(ApplyDecisionKind::Apply);
        }
        let route_verification_bundle = self.route_ed25519_verification_bundle.load_full();
        let route_security_sandbox_id = sandbox.id.clone();
        let route_security_push = move |identity_profile_version: Option<u32>| async move {
            if let Some(route_verification_bundle) = route_verification_bundle.as_deref() {
                push_route_verification_keys(
                    &route_security_sandbox_id,
                    identity_profile_version,
                    route_verification_bundle,
                )
                .await?;
            }
            Ok(())
        };
        reconcile_policy_and_route_security(
            sandbox.state,
            &sandbox.space_api_route_identity,
            push_route_identity(&sandbox.id, &sandbox.space_api_route_identity),
            self.reconcile_effective_network_policy_for_sandbox(&sandbox),
            route_security_push,
        )
        .await
    }

    async fn reconcile_effective_network_policy_for_sandbox(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<ApplyDecisionKind, NetworkPolicyReconcileError> {
        let effective_policy = self.effective_network_policy(sandbox).await?;
        let effective_policy_ref = effective_network_policy_ref(sandbox, &effective_policy)?;
        let applied_source = Self::network_policy_applied_source(effective_policy.inherited);
        let markers_match = sandbox.applied_network_policy_generation
            == effective_policy.generation
            && sandbox.applied_network_policy_source == applied_source;
        let decision = if markers_match {
            match fetch_network_policy(&sandbox.id).await {
                Ok(current_policy)
                    if current_policy.policy == effective_policy.policy
                        && pplx_space_net_common::effective_network_policy_refs_match(
                            &current_policy.installed_policy_ref,
                            &effective_policy_ref.to_string(),
                        ) =>
                {
                    pplx_space_metrics::reconcile::apply_skipped(
                        FACET_NETWORK_POLICY,
                        "idempotent",
                    );
                    return Ok(ApplyDecisionKind::Skip);
                }
                Ok(_) => ApplyDecisionKind::Repair,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        %error,
                        "failed to read net-gateway policy before repair; applying desired policy"
                    );
                    ApplyDecisionKind::Repair
                }
            }
        } else {
            ApplyDecisionKind::Apply
        };
        refresh_network_policy(
            &self.db,
            &sandbox.id,
            Some(&effective_policy_ref),
            "reconcile",
        )
        .await
        .inspect_err(|_| {
            pplx_space_metrics::reconcile::apply_failed(FACET_NETWORK_POLICY, "gateway");
        })?;
        self.mark_network_policy_applied(
            &sandbox.id,
            effective_policy.inherited,
            effective_policy.generation,
        )?;
        pplx_space_metrics::reconcile::apply_applied(FACET_NETWORK_POLICY, decision.as_str());
        Ok(decision)
    }

    /// Resolve the requested boot mode with fallthrough: pool → base_snapshot → cold.
    /// `snapshot_dir` is the resolved snapshot dir from template-manager (empty = no snapshot).
    pub(super) fn resolve_boot_mode(
        &self,
        requested: BootMode,
        template: &str,
        snapshot_dir: Option<&str>,
    ) -> Result<BootMode, Status> {
        if requested == BootMode::Pool {
            let has_pool = {
                let db = self.db_conn()?;
                crate::db::find_pool_sandbox(&db, template, None, None)
                    .map_err(|error| Status::internal(format!("resolve pool creation: {error}")))?
                    .is_some()
            };
            if has_pool {
                return Ok(BootMode::Pool);
            }
            // Fallthrough to base_snapshot check
        }
        if requested == BootMode::Pool || requested == BootMode::BaseSnapshot {
            let has_snapshot = snapshot_dir.is_some_and(|snapshot_dir| {
                std::path::Path::new(&format!("{snapshot_dir}/vmstate.base")).exists()
                    && std::path::Path::new(&format!("{snapshot_dir}/snapshot.base")).exists()
            });
            if has_snapshot {
                return Ok(BootMode::BaseSnapshot);
            }
        }
        Ok(BootMode::Cold)
    }
}

/// Entry gate for the per-sandbox network-policy reconcile: `None` to
/// proceed to the gateway pushes, or the decision to return without touching
/// the gateway.
///
/// A gateway process can outlive its sandbox's serving life — a failed
/// teardown leaves the sidecar running in `cleanup_failed`, and a suspend
/// leak leaves it running in `suspended` — with `netgtw_pid` still set, so
/// the gateway-liveness check must not be the only admission to the push
/// path. States outside the launch-through-teardown window
/// ([`SandboxState::can_authorize_net_gateway`]) are skipped before any
/// control RPC: pushes at such gateways can never converge (the policy
/// refresh abandons for the same states), and each sweep otherwise emits
/// warn logs and error client spans against them forever.
fn network_policy_reconcile_entry_decision(state: SandboxState) -> Option<ApplyDecisionKind> {
    (!state.can_authorize_net_gateway()).then_some(ApplyDecisionKind::Skip)
}

#[cfg(test)]
mod tests;
