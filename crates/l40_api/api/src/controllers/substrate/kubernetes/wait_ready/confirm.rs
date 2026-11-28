use super::target::{
    PodProxyWaitTarget, RuntimeAnswer, node_sandboxes, runtime_answer, serves_target,
};
use super::*;
use pplx_space_accounts::ClientId;

pub(super) fn retired_during_proxy_wait_error(pod_name: &str) -> SubstrateError {
    SubstrateError::Failed {
        message: format!(
            "sandbox pod {pod_name} exited or disappeared before its proxy became reachable"
        ),
    }
}

pub(super) fn superseded_during_proxy_wait_error(pod_name: &str) -> SubstrateError {
    SubstrateError::Unavailable {
        message: format!("sandbox pod {pod_name} was superseded before its proxy became reachable"),
    }
}

pub(super) enum TargetExecutionState {
    Active(Box<super::super::store::ActiveExecutionStatusRow>),
    Retired,
    Superseded,
}

pub(super) enum LossConfirmationDisposition {
    ConfirmedCurrent,
    Replaced((PodIp, PodIpSource), Option<PodUid>),
    Lost,
    DeadlineElapsed,
}

pub(super) async fn recv_relevant_pod_update(
    rx: &mut tokio::sync::broadcast::Receiver<String>,
    pod_name: &str,
) -> Result<(), tokio::sync::broadcast::error::RecvError> {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(updated) if !updated.is_empty() && updated != pod_name => continue,
            Ok(_) => return Ok(()),
            Err(RecvError::Lagged(skipped)) => return Err(RecvError::Lagged(skipped)),
            Err(RecvError::Closed) => return Err(RecvError::Closed),
        }
    }
}

impl KubernetesSubstrate {
    pub(in crate::controllers::substrate::kubernetes) async fn worker_client_for_pod_ip(
        &self,
        pod_ip: PodIp,
    ) -> Result<RuntimeClient, ControllerError> {
        let endpoint = Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)
            .map_err(ControllerError::from)?;
        connect_worker(endpoint).await
    }

    /// [`Self::worker_client_for_pod_ip`] on the channel kept for `pod`'s
    /// instance while its connection is open; see
    /// [`super::super::pod_worker_channels`]. A failed dial fails the same way.
    pub(in crate::controllers::substrate::kubernetes) async fn kept_worker_client(
        &self,
        pod: &ResolvedPodInstance,
    ) -> Result<RuntimeClient, ControllerError> {
        let port = self.k8s.sandbox_proxy_port;
        let key = super::super::pod_worker_channels::PodWorkerKey {
            addr: pod.ip.socket_addr(port),
            pod_uid: pod.pod_uid.clone(),
            generation: pod.generation,
        };
        let pod_ip = pod.ip;
        self.pod_worker_channels
            .client(key, async move {
                let endpoint =
                    Self::endpoint_for_pod_ip(pod_ip, port).map_err(ControllerError::from)?;
                crate::controllers::substrate::connect_worker_watched(endpoint).await
            })
            .await
    }

    /// Whether the execution row still owns `pod_ip` for `target`, recording a
    /// node-reported address its node has just confirmed. A row that a suspend,
    /// retire, or newer pod has moved past does not take it. When the initial
    /// post-create metadata write failed transiently (`state = 'creating'`
    /// with `pod_uid IS NULL`), repairs `pod_uid` first so the confirmed
    /// address can transition the row to `running`.
    pub(super) async fn record_confirmed(
        &self,
        sandbox_id: SandboxId,
        (pod_ip, source): (PodIp, PodIpSource),
        target: &PodProxyWaitTarget<'_>,
        observed_client: Option<Option<&ClientId>>,
        deadline: Instant,
    ) -> Result<bool, SubstrateError> {
        match (source, target.pod_uid) {
            (PodIpSource::Published, target_uid) => {
                let retry_started = Instant::now();
                let row = loop {
                    match self.store.active_execution_status(sandbox_id).await {
                        Ok(row) => break row,
                        Err(err) => {
                            let now = Instant::now();
                            if now >= deadline {
                                return Ok(false);
                            }
                            tracing::warn!(
                                sandbox_id = %sandbox_id,
                                pod_name = target.pod_name,
                                generation = target.generation,
                                error = %err,
                                "transient active_execution_status error during Published IP confirmation; retrying within readiness deadline"
                            );
                            let sleep_for = start_wait_poll_interval(retry_started)
                                .min(deadline.saturating_duration_since(now));
                            tokio::time::sleep(sleep_for).await;
                        }
                    }
                };
                let Some(row) = row else {
                    return Ok(false);
                };
                if row.generation != target.generation || is_suspended_lifecycle(&row.lifecycle) {
                    return Ok(false);
                }
                match (target_uid, row.pod_uid.as_ref().map(PodUid::as_str)) {
                    (Some(expected_uid), Some(recorded_uid)) => {
                        if recorded_uid != expected_uid {
                            return Ok(false);
                        }
                        match row.pod_ip.as_deref() {
                            Some(ip) => Ok(PodIp::parse(ip).ok() == Some(pod_ip)),
                            None => {
                                self.store
                                    .record_confirmed_pod_ip(
                                        sandbox_id,
                                        target.generation,
                                        expected_uid,
                                        pod_ip,
                                    )
                                    .await
                            }
                        }
                    }
                    (Some(expected_uid), None) if row.state == ActiveExecutionState::Creating => {
                        if !self
                            .repair_creating_pod_metadata(
                                sandbox_id,
                                target,
                                expected_uid,
                                observed_client,
                                deadline,
                            )
                            .await?
                        {
                            return Ok(false);
                        }
                        self.remember_repaired_pod_placement(
                            sandbox_id,
                            target.generation,
                            target.pod_name,
                            expected_uid,
                        )
                        .await?;
                        self.store
                            .record_confirmed_pod_ip(
                                sandbox_id,
                                target.generation,
                                expected_uid,
                                pod_ip,
                            )
                            .await
                    }
                    (Some(_), None) => Ok(false),
                    (None, _) => Ok(!execution_pod_lookup_in_flight(row.state)
                        && row
                            .pod_ip
                            .as_deref()
                            .is_none_or(|ip| PodIp::parse(ip).ok() == Some(pod_ip))),
                }
            }
            (PodIpSource::NodeReported, Some(pod_uid)) => {
                if self
                    .store
                    .record_confirmed_pod_ip(sandbox_id, target.generation, pod_uid, pod_ip)
                    .await?
                {
                    return Ok(true);
                }
                if !self
                    .repair_creating_pod_metadata(
                        sandbox_id,
                        target,
                        pod_uid,
                        observed_client,
                        deadline,
                    )
                    .await?
                {
                    return Ok(false);
                }
                self.remember_repaired_pod_placement(
                    sandbox_id,
                    target.generation,
                    target.pod_name,
                    pod_uid,
                )
                .await?;
                self.store
                    .record_confirmed_pod_ip(sandbox_id, target.generation, pod_uid, pod_ip)
                    .await
            }
            (PodIpSource::NodeReported, None) => Ok(false),
        }
    }

    async fn remember_repaired_pod_placement(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<(), SubstrateError> {
        if let Some(pod) = self
            .lookup_sandbox_pod(pod_name)
            .filter(|pod| pod.uid() == Some(pod_uid))
        {
            let _ = self
                .remember_pod_node_placement_fenced(
                    sandbox_id,
                    generation,
                    Some(pod_uid),
                    &pod,
                    PlacementRetryDisposition::RetryDuplicatesExecution,
                )
                .await?;
        }
        Ok(())
    }

    pub(super) async fn repair_creating_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        pod_uid: &str,
        observed_client: Option<Option<&ClientId>>,
        deadline: Instant,
    ) -> Result<bool, SubstrateError> {
        let Some(expected_updated_at) = target.creating_repair_epoch else {
            return Ok(false);
        };
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(false);
        };
        if row.generation != target.generation
            || row.state != ActiveExecutionState::Creating
            || row.pod_uid.is_some()
            || row.pod_materialized_at_set
            || is_suspended_lifecycle(&row.lifecycle)
            || row.updated_at != expected_updated_at
        {
            return Ok(false);
        }
        let pod_client = if let Some(client) = observed_client {
            client.cloned()
        } else if let Some(pod) = self
            .lookup_sandbox_pod(target.pod_name)
            .filter(|pod| pod.uid() == Some(pod_uid))
        {
            pod.client_id()
        } else {
            // Never treat a reflector cache miss as an unlabeled pod, which
            // would omit or reset `sandboxes.activating_client_id`.
            let super::pod_ops::PodGet::Observed(Some(pod)) = self
                .get_pod_for_resolution(
                    target.pod_name,
                    Some(deadline),
                    "failed to query Kubernetes pod for metadata repair",
                )
                .await?
            else {
                return Ok(false);
            };
            if pod.uid() != Some(pod_uid) {
                return Ok(false);
            }
            pod.client_id()
        };
        self.store
            .repair_creating_pod_uid(
                sandbox_id,
                target.generation,
                pod_uid,
                expected_updated_at,
                pod_client.as_ref(),
            )
            .await
    }

    /// The pod IP the pod's node reports for it, once `observed` places the
    /// pod. The node manager reads it from the pod's network namespace when
    /// it allocates the sandbox's network, so the node knows it before
    /// kubelet's status sync publishes the same address. Only an address
    /// reported for this execution generation and the observed pod's UID is
    /// taken, as a candidate the readiness probe must confirm before anything
    /// records it. Only a wait that knows its pod's UID can confirm one.
    pub(super) async fn node_reported_pod_ip(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_pod_uid: Option<&str>,
        mut observed: tokio::sync::watch::Receiver<Option<SandboxPodView>>,
    ) -> Option<PodIp> {
        let expected_pod_uid = expected_pod_uid?;
        let pod = observed
            .wait_for(|pod| {
                pod.as_ref().is_some_and(|pod| {
                    pod.uid() == Some(expected_pod_uid) && assigned_pod_node_name(pod).is_some()
                })
            })
            .await
            .ok()?
            .clone()?;
        let node_name = assigned_pod_node_name(&pod)?;
        let pod_uid = pod.uid()?;
        let target = NodeApiTarget {
            managed_by: pplx_space_node_model::NodeManagedBy::Kubernetes,
            role: SpaceNodeRole::Worker,
            direct_endpoint: None,
            substrate_node_id: Some(node_name),
        };
        let started_at = Instant::now();
        let mut worker = None;
        loop {
            if worker.is_none()
                && let Ok(endpoint) = self.try_resolve_node_api_endpoint(&target)
            {
                worker = Self::worker_client_at(&endpoint, POD_WORKER_CONNECT_TIMEOUT).await;
            }
            if let Some(client) = worker.as_mut() {
                let state = timeout(
                    POD_WORKER_CONNECT_TIMEOUT,
                    client.sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
                        sandbox_ids: vec![sandbox_id.to_string()],
                    }),
                )
                .await;
                match state {
                    Ok(Ok(response)) => {
                        if let Some(pod_ip) = super::target::node_reported_address(
                            response.into_inner().sandboxes,
                            sandbox_id,
                            generation,
                            pod_uid,
                        ) {
                            return Some(pod_ip);
                        }
                    }
                    Ok(Err(_)) | Err(_) => worker = None,
                }
            }
            tokio::time::sleep(start_wait_poll_interval(started_at)).await;
        }
    }

    pub(super) async fn active_execution_for_target(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
    ) -> Result<TargetExecutionState, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(TargetExecutionState::Retired);
        };
        if row.generation != target.generation || is_suspended_lifecycle(&row.lifecycle) {
            return Ok(TargetExecutionState::Retired);
        }
        if let (Some(target_uid), Some(recorded_uid)) =
            (target.pod_uid, row.pod_uid.as_ref().map(PodUid::as_str))
            && target_uid != recorded_uid
        {
            return Ok(TargetExecutionState::Superseded);
        }
        if target.pod_uid.is_some()
            && row.pod_uid.is_none()
            && (row.pod_materialized_at_set
                || matches!(
                    row.state,
                    ActiveExecutionState::Creating | ActiveExecutionState::Recovering
                ) && !(row.state == ActiveExecutionState::Creating
                    && target.creating_repair_epoch == Some(row.updated_at)))
        {
            return Ok(TargetExecutionState::Superseded);
        }
        Ok(TargetExecutionState::Active(Box::new(row)))
    }

    pub(super) async fn check_mismatched_cached_pod(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        raw_view: Option<&Arc<SandboxPodView>>,
        now: Instant,
        started_at: Instant,
        checked_mismatched: &mut Option<(Arc<SandboxPodView>, Instant)>,
    ) -> Result<(), SubstrateError> {
        let Some(mismatched) = raw_view.filter(|pod| !target.names(pod)) else {
            *checked_mismatched = None;
            return Ok(());
        };
        if checked_mismatched
            .as_ref()
            .is_some_and(|(seen, recheck_at)| Arc::ptr_eq(seen, mismatched) && now < *recheck_at)
        {
            return Ok(());
        }
        match self.active_execution_for_target(sandbox_id, target).await? {
            TargetExecutionState::Retired => Err(retired_during_proxy_wait_error(target.pod_name)),
            TargetExecutionState::Superseded if target.abort_on_uid_change => {
                Err(superseded_during_proxy_wait_error(target.pod_name))
            }
            TargetExecutionState::Active(_) | TargetExecutionState::Superseded => {
                let recheck_at = Instant::now() + start_wait_poll_interval(started_at);
                *checked_mismatched = Some((Arc::clone(mismatched), recheck_at));
                Ok(())
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct PostIpEndpointSnapshot<'a> {
    pub sandbox_id: SandboxId,
    pub pod_name: &'a str,
    pub current_endpoint: (PodIp, PodIpSource),
    pub current_uid: &'a Option<PodUid>,
    pub deadline: Instant,
}

impl<'a> PostIpEndpointSnapshot<'a> {
    pub(super) const fn new(
        sandbox_id: SandboxId,
        pod_name: &'a str,
        current_endpoint: (PodIp, PodIpSource),
        current_uid: &'a Option<PodUid>,
        deadline: Instant,
    ) -> Self {
        Self {
            sandbox_id,
            pod_name,
            current_endpoint,
            current_uid,
            deadline,
        }
    }
}

pub(super) struct PostIpReflectorWatchState<'a, 'b> {
    pub seen_in_pod_store: &'b mut bool,
    pub handled_post_ip_view: &'b mut Option<Arc<SandboxPodView>>,
    pub pending_post_ip_fallback: &'b mut Option<super::fallback::InFlightFallback<'a>>,
    pub fallback: &'b mut super::fallback::ReadinessFallback,
}

impl KubernetesSubstrate {
    pub(super) async fn resolve_post_ip_loss_confirmation(
        &self,
        in_flight: super::fallback::InFlightFallback<'_>,
        fallback: &mut super::fallback::ReadinessFallback,
        snapshot: PostIpEndpointSnapshot<'_>,
    ) -> Result<LossConfirmationDisposition, SubstrateError> {
        let PostIpEndpointSnapshot {
            sandbox_id,
            pod_name,
            current_endpoint,
            current_uid,
            deadline,
        } = snapshot;
        #[cfg(test)]
        self.readiness_test_hooks
            .awaiting_post_ip_loss_confirmation
            .notify_waiters();
        let loss_outcome = match timeout_at(
            deadline,
            in_flight.resolve(fallback, self, sandbox_id, pod_name),
        )
        .await
        {
            Ok(res) => res?,
            Err(_) => return Ok(LossConfirmationDisposition::DeadlineElapsed),
        };
        let Some(outcome) = loss_outcome else {
            return Ok(LossConfirmationDisposition::Lost);
        };
        if outcome.pod_uid_superseded {
            return Err(superseded_during_proxy_wait_error(pod_name));
        }
        if outcome.terminal_or_absent_retired {
            return Err(retired_during_proxy_wait_error(pod_name));
        }
        Ok(match outcome.resolved {
            Some(current)
                if current != current_endpoint || &outcome.resolved_uid != current_uid =>
            {
                LossConfirmationDisposition::Replaced(current, outcome.resolved_uid)
            }
            Some(_) => LossConfirmationDisposition::ConfirmedCurrent,
            None => LossConfirmationDisposition::Lost,
        })
    }

    pub(super) async fn recheck_reflector_loss_after_confirm<'a>(
        &'a self,
        target: &PodProxyWaitTarget<'_>,
        probe_target: &PodProxyWaitTarget<'_>,
        snapshot: PostIpEndpointSnapshot<'_>,
        state: PostIpReflectorWatchState<'a, '_>,
    ) -> Result<LossConfirmationDisposition, SubstrateError> {
        let PostIpEndpointSnapshot {
            sandbox_id,
            pod_name: _,
            current_endpoint,
            current_uid,
            deadline,
        } = snapshot;
        let PostIpReflectorWatchState {
            seen_in_pod_store,
            handled_post_ip_view,
            pending_post_ip_fallback,
            fallback,
        } = state;
        let raw_after_confirm = self.lookup_sandbox_pod(target.pod_name);
        let matching_after_confirm = raw_after_confirm
            .as_ref()
            .filter(|pod| probe_target.names(pod));
        let terminal_after_confirm =
            matching_after_confirm.is_some_and(|pod| terminal_pod_phase(pod).is_some());
        let mismatched_after_confirm =
            raw_after_confirm.is_some() && matching_after_confirm.is_none();
        let vanished_after_confirm = if matching_after_confirm.is_some() {
            *seen_in_pod_store = true;
            false
        } else {
            std::mem::replace(seen_in_pod_store, false)
        };
        if !(terminal_after_confirm || mismatched_after_confirm || vanished_after_confirm) {
            if let Some(pod) = matching_after_confirm
                && !handled_post_ip_view
                    .as_ref()
                    .is_some_and(|seen| Arc::ptr_eq(seen, pod))
                && self.pod_reflector_watch_healthy_at(Instant::now())
                && let Some(ip) = super::assigned_pod_ip(pod)?
                && (ip, PodIpSource::Published) != current_endpoint
            {
                *handled_post_ip_view = Some(Arc::clone(pod));
                let outcome = match timeout_at(
                    deadline,
                    self.resolve_pod_ip_for_readiness(sandbox_id, target, deadline),
                )
                .await
                {
                    Ok(res) => res?,
                    Err(_) => return Ok(LossConfirmationDisposition::DeadlineElapsed),
                };
                if outcome.used_uncached_get {
                    fallback.record_completed_at(Instant::now());
                }
                if outcome.pod_uid_superseded {
                    return Err(superseded_during_proxy_wait_error(target.pod_name));
                }
                if outcome.terminal_or_absent_retired {
                    return Err(retired_during_proxy_wait_error(target.pod_name));
                }
                return Ok(match outcome.resolved {
                    Some(current)
                        if current != current_endpoint || &outcome.resolved_uid != current_uid =>
                    {
                        LossConfirmationDisposition::Replaced(current, outcome.resolved_uid)
                    }
                    Some(_) => LossConfirmationDisposition::ConfirmedCurrent,
                    None => LossConfirmationDisposition::Lost,
                });
            }
            return Ok(LossConfirmationDisposition::ConfirmedCurrent);
        }
        if let Some(pod) = matching_after_confirm.filter(|_| terminal_after_confirm) {
            *handled_post_ip_view = Some(Arc::clone(pod));
        }
        *pending_post_ip_fallback = None;
        let in_flight = super::fallback::InFlightFallback::start(
            self.resolve_pod_ip_for_readiness_uncached(sandbox_id, target, deadline),
            true,
        );
        self.resolve_post_ip_loss_confirmation(in_flight, fallback, snapshot)
            .await
    }

    /// Whether the node behind `pod_ip` answers that it serves `target`'s
    /// execution at that address, as [`serves_target`] compares it for
    /// `source`. The address forwards to its node's node-api, so a stale or
    /// reused address reaches a node that does not, or that reports another
    /// address for the execution's current network allocation.
    pub(in crate::controllers::substrate::kubernetes) async fn probe_pod_proxy(
        &self,
        sandbox_id: SandboxId,
        pod_ip: PodIp,
        source: PodIpSource,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
    ) -> bool {
        let connect_timeout = START_WAIT_CONNECT_TIMEOUT.min(remaining_ready_window(deadline));
        let Some(mut client) =
            Self::pod_worker_client(pod_ip, self.k8s.sandbox_proxy_port, connect_timeout).await
        else {
            return false;
        };
        if Instant::now() >= deadline {
            return false;
        }
        let rpc_timeout = START_WAIT_CONNECT_TIMEOUT.min(remaining_ready_window(deadline));
        node_sandboxes(&mut client, sandbox_id, rpc_timeout)
            .await
            .is_some_and(|sandboxes| serves_target(sandboxes, sandbox_id, target, pod_ip, source))
    }

    /// `pod_ip` when it is safe to act on as `target`'s runtime address: a
    /// kubelet-published address as it is, a node-reported one only once the
    /// node behind it affirms the execution (see [`Self::runtime_at`]).
    pub(in crate::controllers::substrate::kubernetes) async fn verified_runtime_pod_ip(
        &self,
        sandbox_id: SandboxId,
        (pod_ip, source): (PodIp, PodIpSource),
        target: &PodProxyWaitTarget<'_>,
    ) -> Result<Option<PodIp>, SubstrateError> {
        if source == PodIpSource::Published {
            return Ok(Some(pod_ip));
        }
        let mut client = Self::pod_worker_client(
            pod_ip,
            self.k8s.sandbox_proxy_port,
            START_WAIT_CONNECT_TIMEOUT,
        )
        .await
        .ok_or_else(|| SubstrateError::Failed {
            message: format!(
                "no node answered at the node-reported address {pod_ip} of sandbox {sandbox_id}"
            ),
        })?;
        Self::runtime_at(&mut client, sandbox_id, pod_ip, target).await
    }

    /// `pod_ip` when the worker behind `client` reports `target`'s execution,
    /// `None` when it reports the sandbox elsewhere or not at all (see
    /// [`runtime_answer`]). A worker that does not answer, or does not report
    /// enough to tell, is an error, so a teardown keeps the execution tracked
    /// for retry.
    pub(in crate::controllers::substrate::kubernetes) async fn runtime_at(
        client: &mut crate::controllers::substrate::RuntimeClient,
        sandbox_id: SandboxId,
        pod_ip: PodIp,
        target: &PodProxyWaitTarget<'_>,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let answer = node_sandboxes(client, sandbox_id, START_WAIT_CONNECT_TIMEOUT)
            .await
            .map(|sandboxes| runtime_answer(sandboxes, sandbox_id, target, pod_ip));
        match answer {
            Some(RuntimeAnswer::Serves) => Ok(Some(pod_ip)),
            Some(RuntimeAnswer::Elsewhere) => Ok(None),
            Some(RuntimeAnswer::Unconfirmed) | None => Err(SubstrateError::Failed {
                message: format!(
                    "the worker at {pod_ip} did not confirm sandbox {sandbox_id}'s runtime"
                ),
            }),
        }
    }

    async fn execution_has_usable_pod_ip(
        &self,
        sandbox_id: SandboxId,
        execution: &super::super::store::ActiveExecutionStatusRow,
        match_recorded_pod_uid: bool,
    ) -> Result<bool, SubstrateError> {
        if execution.pod_ip.is_none() || execution.state == ActiveExecutionState::Recovering {
            return Ok(false);
        }
        if match_recorded_pod_uid {
            if is_suspended_lifecycle(&execution.lifecycle) {
                return Err(retired_during_proxy_wait_error(&execution.pod_name));
            }
            return Ok(true);
        }
        let cached_pod = self.lookup_sandbox_pod(&execution.pod_name);
        let pod_is_restore_in_progress = cached_pod.as_ref().is_some_and(|pod| {
            pod.has_annotation(SandboxPodKey::SuspendRestoreStorageUri)
                || pod.has_annotation(SandboxPodKey::SuspendRestoreSourceNode)
        });
        if is_suspended_lifecycle(&execution.lifecycle) && !pod_is_restore_in_progress {
            if !execution.activation_attempt_live {
                return Err(retired_during_proxy_wait_error(&execution.pod_name));
            }
            return Ok(false);
        }
        if execution.lifecycle == pplx_space_sandbox_model::SandboxLifecycle::Stopped.as_str()
            && self
                .store
                .execution_predates_current_stop(sandbox_id, execution.generation)
                .await?
        {
            return Ok(false);
        }
        if let Some(pod) = cached_pod {
            let stale_cached = pod.is_deleting()
                || terminal_pod_phase(&pod).is_some()
                || pod.unschedulable_message().is_some()
                || (execution.pod_uid.is_some()
                    && pod.uid() != execution.pod_uid.as_ref().map(PodUid::as_str));
            if stale_cached {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn ensure_active_execution_pod_proxy_reachable(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        // True when the caller owns pod materialization (`ensure_pod_exists`
        // already persisted the target pod's UID); false for a joiner whose
        // owner may still be replacing a predecessor pod or generation.
        match_recorded_pod_uid: bool,
        // Forwarded to `wait_for_pod_proxy_reachable`; see its doc.
        phase_deadline: Instant,
    ) -> Result<(), SubstrateError> {
        let started_at = Instant::now();
        let mut execution = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "sandbox has no active Kubernetes execution".to_string(),
            })?;

        if self
            .execution_has_usable_pod_ip(sandbox_id, &execution, match_recorded_pod_uid)
            .await?
        {
            return Ok(());
        }

        loop {
            if is_suspended_lifecycle(&execution.lifecycle) {
                return Err(retired_during_proxy_wait_error(&execution.pod_name));
            }
            let mut target = PodProxyWaitTarget::new(
                &execution.pod_name,
                match_recorded_pod_uid
                    .then_some(execution.pod_uid.as_ref().map(PodUid::as_str))
                    .flatten(),
                execution.generation,
            );
            if match_recorded_pod_uid {
                target = target.abort_on_uid_change();
            }
            let wait = self
                .wait_for_pod_proxy_reachable(metric_identity, sandbox_id, target, phase_deadline)
                .instrument(tracing::info_span!(
                    "substrate.k8s.wait_wake_pod_proxy_reachable",
                    sandbox_id = %sandbox_id,
                    pod_name = %execution.pod_name,
                    namespace = %self.k8s.sandbox_namespace,
                ));
            if match_recorded_pod_uid {
                wait.await?;
                return Ok(());
            }
            let current_generation = execution.generation;
            let current_pod_name = execution.pod_name.clone();
            let follow_owner = async {
                loop {
                    let delay = start_wait_poll_interval(started_at);
                    let wake_at = (Instant::now() + delay).min(phase_deadline);
                    sleep_until(wake_at).await;
                    if Instant::now() >= phase_deadline {
                        std::future::pending::<()>().await;
                    }
                    let next = match self.store.active_execution_status(sandbox_id).await {
                        Ok(Some(next)) => next,
                        Ok(None) => return Err(retired_during_proxy_wait_error(&current_pod_name)),
                        Err(err) => {
                            #[cfg(test)]
                            self.readiness_test_hooks
                                .follow_owner_status_error
                                .notify_waiters();
                            tracing::debug!(
                                sandbox_id = %sandbox_id,
                                generation = current_generation,
                                error = %err,
                                "transient active_execution_status error while following activation owner; continuing proxy wait"
                            );
                            continue;
                        }
                    };
                    if is_suspended_lifecycle(&next.lifecycle) {
                        return Err(retired_during_proxy_wait_error(&current_pod_name));
                    }
                    if next.generation > current_generation {
                        return Ok(next);
                    }
                }
            };
            tokio::select! {
                res = wait => match res {
                    Ok(_) => return Ok(()),
                    Err(err) => {
                        if Instant::now() < phase_deadline
                            && let Ok(Ok(Some(next))) = timeout_at(
                                phase_deadline,
                                self.store.active_execution_status(sandbox_id),
                            )
                            .await
                            && !is_suspended_lifecycle(&next.lifecycle)
                            && next.generation > current_generation
                        {
                            execution = next;
                            continue;
                        }
                        return Err(err);
                    }
                },
                followed = follow_owner => {
                    execution = followed?;
                }
            }
        }
    }
}
