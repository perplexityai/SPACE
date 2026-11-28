use pplx_space_sandbox_model::SandboxLifecycle;

use super::super::pod_ops::is_transient_kube_read_error;
use super::super::store::{
    FailedRecoveryPodAbsenceOutcome, PlacementRetryDisposition, PodTrackingCleanupCause,
};
use super::super::support::*;
use super::super::*;
use super::PodProxyWaitTarget;

/// Maximum concurrent uncached readiness fallback `GET`s per API replica.
pub(super) const READINESS_FALLBACK_MAX_IN_FLIGHT: usize = 4;

/// Minimum spacing between readiness fallback `GET` starts per API replica
/// (`10` starts/s), bounding control-plane load during reflector outages.
pub(super) const READINESS_FALLBACK_MIN_SPACING: Duration = Duration::from_millis(100);

/// Grace window during which a `Succeeded` pod on a `suspending` sandbox may
/// still publish its durable snapshot before readiness treats it as orphaned.
const SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE: Duration = Duration::from_secs(180);

/// Replica-scoped concurrency and pacing gate for readiness fallback pod reads.
///
/// Cold creates normally learn their pod's IP from the shared sandbox-pod
/// reflector or the target node's state report with zero direct `GET`s. When a
/// fallback `GET` is needed (the reflector watch is failing, a cached pod
/// vanished before settling, or the reflector delivered nothing during the
/// quiet-reflector window), every waiting request on the replica shares this
/// gate so concurrent creates can't stampede `kube-apiserver`.
pub(in crate::controllers::substrate::kubernetes) struct ReadinessFallbackGate {
    semaphore: Arc<tokio::sync::Semaphore>,
    next_allowed_at: tokio::sync::Mutex<Option<Instant>>,
}

impl Default for ReadinessFallbackGate {
    fn default() -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(
                READINESS_FALLBACK_MAX_IN_FLIGHT,
            )),
            next_allowed_at: tokio::sync::Mutex::new(None),
        }
    }
}

pub(super) struct FallbackPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FallbackAcquireError {
    /// The readiness phase deadline expired while waiting for a slot.
    PhaseElapsed,
    /// `readiness_fallback_wait` expired before `deadline`, indicating the
    /// replica's fallback queue is saturated.
    WaitBudgetSaturated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FallbackCancelMode {
    /// Never cancel a queued or in-flight fallback read early (used for
    /// terminal-pod verification reads, where `pod_store` already holds a
    /// terminal phase).
    Never,
    /// Cancel a queued or in-flight fallback read once `target` settles into a
    /// non-terminal status in `pod_store` (used for one-shot absence and
    /// quiet-reflector reads so a pod that settles via the reflector resolves
    /// from cache instead of waiting on a slow `GET` or saturating the gate).
    TargetSettledNonTerminal,
    /// Cancel a queued wait if the reflector becomes healthy or `target`
    /// settles into a non-terminal status in `pod_store`, and cancel an
    /// in-flight `GET` once `target` settles non-terminally in `pod_store`
    /// (used during pre-IP degraded polling).
    ReflectorHealthyOrTargetSettled,
    /// Cancel a queued wait once the reflector becomes healthy again, and only
    /// shortcut to `pod_store` once the reflector is healthy (used during
    /// post-IP proxy re-resolution when `pod_store` may hold a stale
    /// pre-outage entry, or when the current cached entry was already
    /// evaluated).
    ReflectorHealthy,
}

impl FallbackCancelMode {
    fn allows_settled_cache_shortcut(self, reflector_healthy: bool) -> bool {
        match self {
            Self::Never => false,
            Self::TargetSettledNonTerminal | Self::ReflectorHealthyOrTargetSettled => true,
            Self::ReflectorHealthy => reflector_healthy,
        }
    }
}

impl ReadinessFallbackGate {
    pub(super) async fn acquire(
        &self,
        deadline: Instant,
        wait_budget: Duration,
    ) -> Result<FallbackPermit, FallbackAcquireError> {
        let now = Instant::now();
        if now >= deadline {
            return Err(FallbackAcquireError::PhaseElapsed);
        }
        let budget_deadline = now + wait_budget;
        let wait_limit = deadline.min(budget_deadline);
        let classify_timeout = || {
            if Instant::now() >= deadline {
                FallbackAcquireError::PhaseElapsed
            } else {
                FallbackAcquireError::WaitBudgetSaturated
            }
        };

        let permit = timeout_at(wait_limit, Arc::clone(&self.semaphore).acquire_owned())
            .await
            .map_err(|_| classify_timeout())?
            .expect("readiness fallback semaphore is never closed");

        // Hold `next_allowed_at` across the pacing sleep and advance the next
        // slot only after waking, so a waiter cancelled mid-sleep neither
        // consumes a rate-limit slot nor holds an HTTP concurrency slot idle
        // past its own cancellation.
        timeout_at(wait_limit, async {
            let mut next_allowed = self.next_allowed_at.lock().await;
            let now = Instant::now();
            let start_at = next_allowed.map_or(now, |allowed| allowed.max(now));
            if start_at > now {
                tokio::time::sleep_until(start_at).await;
            }
            *next_allowed = Some(Instant::now() + READINESS_FALLBACK_MIN_SPACING);
        })
        .await
        .map_err(|_| classify_timeout())?;

        Ok(FallbackPermit { _permit: permit })
    }
}

fn succeeded_pod_within_publication_grace(pod: &SandboxPodView, now: jiff::Timestamp) -> bool {
    if terminal_pod_phase(pod) != Some(TerminalPodPhase::Succeeded) {
        return false;
    }
    let Some(finished_at) = pod
        .container_statuses()
        .all()
        .filter_map(|state| state.terminated.as_ref()?.finished_at)
        .max()
    else {
        return false;
    };
    let Ok(grace_secs) = i64::try_from(SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE.as_secs()) else {
        return false;
    };
    let Ok(deadline) = finished_at.checked_add(jiff::SignedDuration::from_secs(grace_secs)) else {
        return false;
    };
    Duration::try_from(deadline.duration_since(now)).is_ok()
}

fn row_allows_pod_for_readiness(
    row: &super::super::store::ActiveExecutionStatusRow,
    target: &PodProxyWaitTarget<'_>,
    pod: &SandboxPodView,
) -> bool {
    if is_suspended_lifecycle(&row.lifecycle)
        || row.generation != target.generation
        || !target.names(pod)
    {
        return false;
    }
    // Passive `resolve_pod_ip` callers leave `creating` and `recovering`
    // executions untouched, whereas active readiness waits (`create`,
    // `create_join`, and wake / started-recovery wake) drive those executions
    // to proxy reachability. Once an in-flight row has recorded its pod's
    // immutable UID, require the observed pod to match that UID so a lagging
    // reflector view of a replaced predecessor can never clobber the active
    // recovery or create metadata or fail a wake with a stale `Unschedulable`.
    if execution_pod_lookup_in_flight(row.state) {
        if let Some(recorded_uid) = row.pod_uid.as_ref() {
            return pod.uid() == Some(recorded_uid.as_str());
        }
        // `create` / `create_join` pass `target.pod_uid = Some(...)`
        // (`target.names(pod)` already matched it above), whereas `wake`
        // passes `target.pod_uid = None` and must wait for the materializing
        // owner to record the new pod's UID unless a stale restore create has
        // already aged out.
        if target.pod_uid.is_none()
            && !stale_restore_execution_create(&row.lifecycle, row.state, row.create_age_expired)
        {
            return false;
        }
    }
    true
}

impl KubernetesSubstrate {
    pub(super) fn pod_reflector_healthy(&self) -> bool {
        self.pod_reflector_ready.is_ready() && !self.pod_client_index.is_watch_failing()
    }

    /// Validate a cached pod for `target`: when `target.pod_uid` is `Some`,
    /// `target.names(pod)` already pins the exact Pod UID in memory; when
    /// `target.pod_uid` is `None` (`wake`), verify `pod` against the active
    /// execution row before acting on cached scheduling status (`Unschedulable`),
    /// persisting node placement, or counting the pod as observed in `pod_store`.
    pub(super) async fn cached_pod_matches_readiness_target(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        pod: &SandboxPodView,
    ) -> Result<bool, SubstrateError> {
        if !target.names(pod) {
            return Ok(false);
        }
        if target.pod_uid.is_some() {
            return Ok(true);
        }
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(false);
        };
        Ok(row_allows_pod_for_readiness(&row, target, pod))
    }

    pub(super) async fn cached_target_settled_non_terminal(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
    ) -> Result<bool, SubstrateError> {
        let Some(pod) = self.lookup_sandbox_pod(target.pod_name) else {
            return Ok(false);
        };
        if !pod_status_is_settled(&pod) || terminal_pod_phase(&pod).is_some() {
            return Ok(false);
        }
        self.cached_pod_matches_readiness_target(sandbox_id, target, &pod)
            .await
    }

    /// Resolve a settled non-terminal cached pod for `target` without issuing
    /// any apiserver `GET`, including active `creating` and `recovering`
    /// readiness waits that `resolve_pod_ip_for_readiness` keeps gated for
    /// passive callers.
    pub(super) async fn resolve_settled_cached_pod_for_readiness(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        pod: &SandboxPodView,
        deadline: Instant,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        if !target.names(pod) {
            return Ok(None);
        }
        if target.pod_uid.is_some() {
            if let Some(message) = pod.unschedulable_message() {
                return Err(SubstrateError::Unschedulable {
                    message: format!("sandbox pod is unschedulable: {message}"),
                });
            }
            if let Some(resolved) = self
                .resolve_pod_ip_for_readiness(sandbox_id, target, deadline)
                .await?
            {
                return Ok(Some(resolved));
            }
        }
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        if !row_allows_pod_for_readiness(&row, target, pod) {
            return Ok(None);
        }
        if let Some(message) = pod.unschedulable_message() {
            return Err(SubstrateError::Unschedulable {
                message: format!("sandbox pod is unschedulable: {message}"),
            });
        }
        if !execution_pod_lookup_in_flight(row.state) {
            return self
                .resolve_pod_ip_for_readiness(sandbox_id, target, deadline)
                .await;
        }
        let Some(ip) = assigned_pod_ip(pod)? else {
            return Ok(None);
        };

        let current_node_name = assigned_pod_node_name(pod).map(str::to_owned);
        if row.k8s_node_name.as_deref() != current_node_name.as_deref() {
            let _ = self
                .remember_pod_node_placement(
                    sandbox_id,
                    pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?;
        }

        let pod_uid = pod.uid().map(str::to_owned);
        let ip_string = ip.to_string();
        let pod_client = pod.client_id();
        if row.state == ActiveExecutionState::Creating
            || row.pod_uid.as_ref().map(PodUid::as_str) != pod_uid.as_deref()
            || row.pod_ip.as_deref() != Some(ip_string.as_str())
        {
            match self
                .store
                .persist_pod_metadata(
                    sandbox_id,
                    row.generation,
                    pod_uid.as_deref(),
                    Some(&ip_string),
                    pod_client.as_ref(),
                )
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        "pod metadata write no-oped during reflected readiness: rechecking execution status"
                    );
                    if self
                        .store
                        .active_execution_status(sandbox_id)
                        .await?
                        .is_none_or(|current| !row_allows_pod_for_readiness(&current, target, pod))
                    {
                        return Ok(None);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        error = %err,
                        "failed to persist Kubernetes pod metadata during reflected readiness"
                    );
                }
            }
        } else if self.k8s.activation_client_attribution.is_enabled()
            && let Some(pod_uid) = pod_uid.as_deref()
            && crate::controllers::substrate::kubernetes::store::attribution_update(
                &row.lifecycle,
                Some(pod_uid),
                Some(pod_uid),
                pod_client.as_ref(),
                row.activating_client_id.as_ref(),
            )
            .is_some()
            && let Err(err) = self
                .store
                .repair_runtime_attribution(
                    sandbox_id,
                    row.generation,
                    pod_uid,
                    pod_client.as_ref(),
                )
                .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = row.generation,
                error = %err,
                "failed to repair the sandbox's activating client during reflected readiness"
            );
        }

        Ok(Some((ip, PodIpSource::Published)))
    }

    /// Resolve a cached unsettled pod against the active execution row in
    /// Postgres without issuing any apiserver `GET`. Returns `Some` when the
    /// execution row already holds a node-reported IP for the cached pod's UID,
    /// and repairs `pod_uid` on an in-flight row whose initial post-create
    /// metadata write transiently failed so node-reported confirmation can
    /// match it.
    pub(super) async fn resolve_unsettled_cached_pod_for_readiness(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        pod: &SandboxPodView,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        if !row_allows_pod_for_readiness(&row, target, pod) {
            return Ok(None);
        }
        let mut recorded_uid = row.pod_uid;
        if execution_pod_lookup_in_flight(row.state)
            && recorded_uid.is_none()
            && let Some(expected_uid) = target.pod_uid
        {
            let pod_client = pod.client_id();
            match self
                .store
                .persist_pod_metadata(
                    sandbox_id,
                    row.generation,
                    Some(expected_uid),
                    None,
                    pod_client.as_ref(),
                )
                .await
            {
                Ok(Some(_)) => recorded_uid = PodUid::parse(expected_uid).ok(),
                Ok(None) => {}
                Err(err) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    generation = row.generation,
                    error = %err,
                    "failed to repair Kubernetes pod UID during unsettled readiness check"
                ),
            }
        }
        current_pod_ip_with_source(pod, recorded_uid.as_ref(), row.pod_ip.as_deref())
    }

    async fn should_cancel_queued_fallback(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        cancel_mode: FallbackCancelMode,
    ) -> Result<bool, SubstrateError> {
        match cancel_mode {
            FallbackCancelMode::Never => Ok(false),
            FallbackCancelMode::TargetSettledNonTerminal => {
                self.cached_target_settled_non_terminal(sandbox_id, target)
                    .await
            }
            FallbackCancelMode::ReflectorHealthy => Ok(self.pod_reflector_healthy()),
            FallbackCancelMode::ReflectorHealthyOrTargetSettled => {
                if self.pod_reflector_healthy() {
                    Ok(true)
                } else {
                    self.cached_target_settled_non_terminal(sandbox_id, target)
                        .await
                }
            }
        }
    }

    /// Execute at most one gated uncached `GET /pods/{name}` when the shared
    /// reflector is unhealthy, a previously cached pod vanished from
    /// `pod_store`, or the quiet-reflector window elapsed with zero deliveries.
    ///
    /// Depending on `cancel_mode`, a reflector update while waiting in
    /// [`ReadinessFallbackGate`] drops the queued wait before any `GET` starts,
    /// and a reflector update while the `GET` is in flight forwards node
    /// placement to `observed` or resolves settled state immediately without
    /// blocking on a slow apiserver read. Apiserver throttling (`429`), `5xx` /
    /// transport errors, individual `GET` timeouts before `deadline`, and
    /// gate-wait saturation fail fast with [`SubstrateError::Unavailable`]
    /// (`503`).
    pub(super) async fn fallback_read_pod_for_readiness(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        observed: &tokio::sync::watch::Sender<Option<SandboxPodView>>,
        deadline: Instant,
        cancel_mode: FallbackCancelMode,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        let mut recovery_events =
            (cancel_mode != FallbackCancelMode::Never).then(|| self.pod_events.subscribe());

        let permit = if let Some(recovery_events) = recovery_events.as_mut() {
            let acquire = self
                .readiness_fallback_gate
                .acquire(deadline, self.budgets.readiness_fallback_wait);
            pin_mut!(acquire);
            loop {
                if self
                    .should_cancel_queued_fallback(sandbox_id, target, cancel_mode)
                    .await?
                {
                    return Ok(None);
                }
                tokio::select! {
                    biased;
                    acquired = &mut acquire => break acquired,
                    recv = recovery_events.recv() => {
                        if matches!(recv, Err(tokio::sync::broadcast::error::RecvError::Closed)) {
                            break acquire.await;
                        }
                    }
                }
            }
        } else {
            self.readiness_fallback_gate
                .acquire(deadline, self.budgets.readiness_fallback_wait)
                .await
        };

        let permit = match permit {
            Ok(permit) => permit,
            Err(FallbackAcquireError::PhaseElapsed) => return Ok(None),
            Err(FallbackAcquireError::WaitBudgetSaturated) => {
                if self
                    .should_cancel_queued_fallback(sandbox_id, target, cancel_mode)
                    .await?
                {
                    return Ok(None);
                }
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "Kubernetes API readiness fallback budget saturated while waiting to query sandbox pod {}",
                        target.pod_name
                    ),
                });
            }
        };

        // Re-check `pod_store` after acquiring the gate slot: when pre-IP or
        // one-shot waits see `target` settled in `pod_store` (or the reflector
        // is now healthy during post-IP re-resolution), drop the permit with
        // zero GETs.
        if cancel_mode.allows_settled_cache_shortcut(self.pod_reflector_healthy())
            && let Some(cached) = self.lookup_sandbox_pod(target.pod_name)
            && pod_status_is_settled(&cached)
            && terminal_pod_phase(&cached).is_none()
            && self
                .cached_pod_matches_readiness_target(sandbox_id, target, &cached)
                .await?
        {
            drop(permit);
            return self
                .resolve_settled_cached_pod_for_readiness(sandbox_id, target, &cached, deadline)
                .await;
        }

        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        if is_suspended_lifecycle(&row.lifecycle) || row.generation != target.generation {
            return Ok(None);
        }
        if execution_pod_lookup_in_flight(row.state) {
            if let (Some(expected_uid), Some(recorded_uid)) = (target.pod_uid, row.pod_uid.as_ref())
                && recorded_uid.as_str() != expected_uid
            {
                return Ok(None);
            }
            if target.pod_uid.is_none()
                && row.pod_uid.is_none()
                && !stale_restore_execution_create(
                    &row.lifecycle,
                    row.state,
                    row.create_age_expired,
                )
            {
                return Ok(None);
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return Ok(None);
        }
        let get_limit = deadline.min(now + self.budgets.readiness_pod_get);
        let fetched = if let Some(mut recovery_events) = recovery_events {
            let get_fut = timeout_at(get_limit, self.read_sandbox_pod_uncached(&row.pod_name));
            pin_mut!(get_fut);
            let mut forwarded_node: Option<String> = None;
            loop {
                if cancel_mode.allows_settled_cache_shortcut(self.pod_reflector_healthy())
                    && let Some(cached) = self.lookup_sandbox_pod(&row.pod_name)
                    && row_allows_pod_for_readiness(&row, target, &cached)
                    && terminal_pod_phase(&cached).is_none()
                {
                    if pod_status_is_settled(&cached) {
                        drop(permit);
                        return self
                            .resolve_settled_cached_pod_for_readiness(
                                sandbox_id, target, &cached, deadline,
                            )
                            .await;
                    }
                    if let Some(node_name) = assigned_pod_node_name(&cached)
                        && forwarded_node.as_deref() != Some(node_name)
                    {
                        if row.k8s_node_name.as_deref() != Some(node_name) {
                            let _ = self
                                .remember_pod_node_placement(
                                    sandbox_id,
                                    &cached,
                                    PlacementRetryDisposition::RetryDuplicatesExecution,
                                )
                                .await?;
                        }
                        forwarded_node = Some(node_name.to_owned());
                        observed.send_replace(Some(SandboxPodView::clone(&cached)));
                        if let Some(resolved) = self
                            .resolve_unsettled_cached_pod_for_readiness(sandbox_id, target, &cached)
                            .await?
                        {
                            drop(permit);
                            return Ok(Some(resolved));
                        }
                    }
                }
                tokio::select! {
                    biased;
                    res = &mut get_fut => break res,
                    recv = recovery_events.recv() => {
                        if matches!(recv, Err(tokio::sync::broadcast::error::RecvError::Closed)) {
                            break get_fut.await;
                        }
                    }
                }
            }
        } else {
            timeout_at(get_limit, self.read_sandbox_pod_uncached(&row.pod_name)).await
        };
        // Release the concurrency slot as soon as the single HTTP GET completes
        // so downstream store writes never hold a fallback slot.
        drop(permit);

        let pod = match fetched {
            Ok(Ok(Some(pod))) => pod,
            Ok(Ok(None)) => {
                if execution_pod_lookup_in_flight(row.state)
                    && !stale_restore_execution_create(
                        &row.lifecycle,
                        row.state,
                        row.create_age_expired,
                    )
                {
                    return Ok(None);
                }
                self.cleanup_absent_pod_from_fallback_read(sandbox_id, &row)
                    .await?;
                return Ok(None);
            }
            Ok(Err(err)) if is_transient_kube_read_error(&err) => {
                if cancel_mode.allows_settled_cache_shortcut(self.pod_reflector_healthy())
                    && let Some(cached) = self.lookup_sandbox_pod(&row.pod_name)
                    && row_allows_pod_for_readiness(&row, target, &cached)
                    && pod_status_is_settled(&cached)
                    && terminal_pod_phase(&cached).is_none()
                {
                    return self
                        .resolve_settled_cached_pod_for_readiness(
                            sandbox_id, target, &cached, deadline,
                        )
                        .await;
                }
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "Kubernetes API readiness fallback pod read failed for pod {}: {err}",
                        row.pod_name
                    ),
                });
            }
            Ok(Err(err)) => {
                return Err(SubstrateError::Failed {
                    message: format!("failed to query K8s pod {}: {err}", row.pod_name),
                });
            }
            Err(_) => {
                if cancel_mode.allows_settled_cache_shortcut(self.pod_reflector_healthy())
                    && let Some(cached) = self.lookup_sandbox_pod(&row.pod_name)
                    && row_allows_pod_for_readiness(&row, target, &cached)
                    && pod_status_is_settled(&cached)
                    && terminal_pod_phase(&cached).is_none()
                {
                    return self
                        .resolve_settled_cached_pod_for_readiness(
                            sandbox_id, target, &cached, deadline,
                        )
                        .await;
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "Kubernetes API readiness fallback pod read timed out for pod {}",
                        row.pod_name
                    ),
                });
            }
        };

        if !row_allows_pod_for_readiness(&row, target, &pod) {
            return Ok(None);
        }

        if let Some(phase) = terminal_pod_phase(&pod) {
            if row.lifecycle == SandboxLifecycle::Suspending.as_str()
                && succeeded_pod_within_publication_grace(&pod, jiff::Timestamp::now())
            {
                return Err(SubstrateError::suspend_in_flight(sandbox_id));
            }
            tracing::info!(
                sandbox_id = %sandbox_id,
                pod_name = %row.pod_name,
                %phase,
                "tracked Kubernetes sandbox pod is terminal on readiness fallback read; deleting it and clearing cached pod IP"
            );
            self.delete_pod_and_forget_tracking(
                sandbox_id,
                &row.pod_name,
                row.generation,
                PodTrackingCleanupCause::MissingPodObservation,
            )
            .await?;
            return Ok(None);
        }

        if let Some(message) = pod.unschedulable_message() {
            return Err(SubstrateError::Unschedulable {
                message: format!("sandbox pod is unschedulable: {message}"),
            });
        }

        let current_node_name = assigned_pod_node_name(&pod).map(str::to_owned);
        if row.k8s_node_name.as_deref() != current_node_name.as_deref() {
            let _ = self
                .remember_pod_node_placement(
                    sandbox_id,
                    &pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?;
        }
        observed.send_replace(Some(SandboxPodView::clone(&pod)));

        let pod_uid = pod.uid().map(str::to_owned);
        let resolved =
            current_pod_ip_with_source(&pod, row.pod_uid.as_ref(), row.pod_ip.as_deref())?;
        let pod_ip_string = resolved.map(|(ip, _)| ip.to_string());
        let pod_client = pod.client_id();
        if row.state == ActiveExecutionState::Creating
            || row.pod_uid.as_ref().map(PodUid::as_str) != pod_uid.as_deref()
            || row.pod_ip.as_deref() != pod_ip_string.as_deref()
        {
            match self
                .store
                .persist_pod_metadata(
                    sandbox_id,
                    row.generation,
                    pod_uid.as_deref(),
                    pod_ip_string.as_deref(),
                    pod_client.as_ref(),
                )
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        "pod metadata write no-oped after readiness fallback read: the execution was retired or replaced mid-flight"
                    );
                    if self
                        .store
                        .active_execution_status(sandbox_id)
                        .await?
                        .is_none_or(|current| {
                            is_suspended_lifecycle(&current.lifecycle)
                                || current.generation != target.generation
                        })
                    {
                        return Ok(None);
                    }
                }
                Err(err) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    generation = row.generation,
                    error = %err,
                    "failed to persist Kubernetes pod metadata after readiness fallback read"
                ),
            }
        } else if self.k8s.activation_client_attribution.is_enabled()
            && let Some(pod_uid) = pod_uid.as_deref()
            && crate::controllers::substrate::kubernetes::store::attribution_update(
                &row.lifecycle,
                Some(pod_uid),
                Some(pod_uid),
                pod_client.as_ref(),
                row.activating_client_id.as_ref(),
            )
            .is_some()
            && let Err(err) = self
                .store
                .repair_runtime_attribution(
                    sandbox_id,
                    row.generation,
                    pod_uid,
                    pod_client.as_ref(),
                )
                .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = row.generation,
                error = %err,
                "failed to repair the sandbox's activating client after readiness fallback read"
            );
        }

        Ok(resolved.filter(|_| {
            target
                .pod_uid
                .is_none_or(|uid| pod_uid.as_deref() == Some(uid))
        }))
    }

    async fn cleanup_absent_pod_from_fallback_read(
        &self,
        sandbox_id: SandboxId,
        row: &super::super::store::ActiveExecutionStatusRow,
    ) -> Result<(), SubstrateError> {
        if row.state == ActiveExecutionState::RecoveryFailed {
            match self
                .store
                .converge_failed_recovery_after_pod_absence_if_generation(
                    sandbox_id,
                    row.generation,
                    row.pod_uid.as_ref().map(PodUid::as_str),
                    row.pod_ip.as_deref(),
                    row.k8s_node_name.as_deref(),
                )
                .await?
            {
                FailedRecoveryPodAbsenceOutcome::NotMatched => {}
                FailedRecoveryPodAbsenceOutcome::ClaimPreserved => tracing::debug!(
                    sandbox_id = %sandbox_id,
                    generation = row.generation,
                    pod_name = %row.pod_name,
                    "failed recovery pod is absent; clearing cached route and preserving execution claim"
                ),
                FailedRecoveryPodAbsenceOutcome::ConvergedSuspended => tracing::info!(
                    sandbox_id = %sandbox_id,
                    generation = row.generation,
                    pod_name = %row.pod_name,
                    "failed recovery pod is absent; converged current durable snapshot to suspended"
                ),
            }
            return Ok(());
        }
        if row.lifecycle == SandboxLifecycle::Suspending.as_str()
            && let Some(pod_uid) = row.pod_uid.as_ref()
            && self
                .store
                .finalize_missing_suspending_pod_with_current_snapshot(
                    sandbox_id,
                    row.generation,
                    pod_uid.as_str(),
                )
                .await?
        {
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation = row.generation,
                pod_name = %row.pod_name,
                "finalized suspended sandbox after its pod disappeared with a current durable snapshot"
            );
            return Ok(());
        }
        tracing::warn!(
            sandbox_id = %sandbox_id,
            pod_name = %row.pod_name,
            cached_pod_ip = ?row.pod_ip,
            "tracked Kubernetes sandbox pod is authoritatively absent; clearing cached pod IP"
        );
        self.store
            .forget_pod_tracking_if_generation(
                sandbox_id,
                row.generation,
                PodTrackingCleanupCause::MissingPodObservation,
            )
            .await?;
        Ok(())
    }
}
