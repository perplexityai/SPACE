mod connected_command_ready;
mod readiness_fallback;
#[cfg(test)]
mod tests;

pub(super) use connected_command_ready::CommandReadyPod;
#[cfg(test)]
pub(super) use connected_command_ready::ConnectedCommandReadiness;
use readiness_fallback::FallbackCancelMode;
pub(super) use readiness_fallback::ReadinessFallbackGate;
#[cfg(test)]
use readiness_fallback::{
    FallbackAcquireError, READINESS_FALLBACK_MAX_IN_FLIGHT, READINESS_FALLBACK_MIN_SPACING,
};

use super::store::PlacementRetryDisposition;
use super::support::*;
use super::*;
use k8s_openapi::api::core::v1::Event;
use pplx_space_node_mgr_client::MEMORY_ADMISSION_REFUSAL_PREFIX;

/// The kubelet event reason for a pod sandbox the runtime failed to start.
const FAILED_CREATE_POD_SANDBOX_REASON: &str = "FailedCreatePodSandBox";

/// The marker `annotateStatus` in `shim/space.go` writes for node-manager's
/// `InvalidArgument`: the request fails identically on every node. sbx-mgr's
/// `failure_status` decides which node-manager codes reach the shim intact.
const INVALID_ARGUMENT_MARKER: &str = "[space-err=InvalidArgument]";
const RESOURCE_EXHAUSTED_MARKER: &str = "[space-err=ResourceExhausted]";

/// How long a create waiting for its pod's IP waits to watch that pod's
/// events again after a watch ended or failed. A watch from `"0"` replays the
/// events already recorded, so a refusal recorded in between is not missed.
const POD_SANDBOX_REJECTION_REWATCH_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PodIpLearned {
    Cached,
    Reflected,
    Node,
    Fallback,
}

impl PodIpLearned {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cached => "cached",
            Self::Reflected => "reflector",
            Self::Node => "node",
            Self::Fallback => "fallback",
        }
    }
}

/// Only explicit node-manager verdicts end the wait; other failures remain
/// eligible for kubelet's retry of the same pod.
fn pod_sandbox_rejection_error(
    event_message: &str,
    fail_fast_on_memory_refusal: bool,
) -> Option<SubstrateError> {
    if let Some((_, reason)) = event_message.split_once(INVALID_ARGUMENT_MARKER) {
        return Some(SubstrateError::InvalidRequest {
            message: format!(
                "the node refused to start the sandbox: {}",
                reason.trim_start_matches(':').trim()
            ),
        });
    }
    if !fail_fast_on_memory_refusal {
        return None;
    }
    let (_, reason) = event_message.split_once(RESOURCE_EXHAUSTED_MARKER)?;
    // ResourceExhausted also covers transient queue/slot pressure. The
    // allocation path's explicit prefix distinguishes memory admission.
    let (_, reason) = reason.split_once(MEMORY_ADMISSION_REFUSAL_PREFIX)?;
    Some(SubstrateError::Unavailable {
        message: format!("{MEMORY_ADMISSION_REFUSAL_PREFIX} {}", reason.trim()),
    })
}

/// Why a watch of a pod's sandbox refusals ended without one.
struct RejectionWatchFailure {
    /// The apiserver's HTTP status, when it answered with one.
    code: Option<u16>,
    message: String,
}

impl RejectionWatchFailure {
    fn from_kube(error: kube::Error) -> Self {
        let code = match &error {
            kube::Error::Api(response) => Some(response.code),
            _ => None,
        };
        Self {
            code,
            message: error.to_string(),
        }
    }

    /// A client error that another watch would get again: anything in 4xx
    /// but a request timeout, an expired resource version or throttling.
    fn is_permanent(&self) -> bool {
        self.code
            .is_some_and(|code| (400..500).contains(&code) && !matches!(code, 408 | 410 | 429))
    }
}

/// Watch `params`' events from `"0"` until one carries a classified refusal,
/// or the watch ends (`None`).
async fn watch_pod_sandbox_rejection(
    events: &Api<Event>,
    params: &WatchParams,
    fail_fast_on_memory_refusal: bool,
) -> Result<Option<SubstrateError>, RejectionWatchFailure> {
    let stream = events
        .watch(params, "0")
        .await
        .map_err(RejectionWatchFailure::from_kube)?;
    pin_mut!(stream);
    while let Some(event) = stream
        .try_next()
        .await
        .map_err(RejectionWatchFailure::from_kube)?
    {
        match event {
            WatchEvent::Added(event) | WatchEvent::Modified(event) => {
                if let Some(error) = event.message.as_deref().and_then(|message| {
                    pod_sandbox_rejection_error(message, fail_fast_on_memory_refusal)
                }) {
                    return Ok(Some(error));
                }
            }
            WatchEvent::Deleted(_) | WatchEvent::Bookmark(_) => {}
            WatchEvent::Error(status) => {
                return Err(RejectionWatchFailure {
                    code: Some(status.code),
                    message: status.message.clone(),
                });
            }
        }
    }
    Ok(None)
}

impl KubernetesSubstrate {
    /// Observe `target`'s pod IP through the shared sandbox-pod reflector
    /// (`pod_store` + `pod_events`), forwarding scheduled views to `observed`
    /// once node placement is persisted so [`Self::node_reported_pod_ip`] can
    /// race kubelet status publication.
    ///
    /// Opens zero per-pod watches and issues zero apiserver `GET`s while the
    /// reflector is healthy, except for two single-shot safety checks:
    /// - One-shot A: `target` was previously present in `pod_store` and later
    ///   disappeared before settling (for example, deleted or filtered out on a
    ///   terminal transition).
    /// - One-shot B: no pod named `target.pod_name` has appeared in `pod_store`
    ///   after `readiness_quiet_reflector` and zero readable reflector updates
    ///   have landed since the wait began.
    async fn reflected_pod_ip(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        observed: &tokio::sync::watch::Sender<Option<SandboxPodView>>,
        deadline: Instant,
    ) -> Result<Option<((PodIp, PodIpSource), PodIpLearned)>, SubstrateError> {
        let mut events = self.pod_events.subscribe();
        let wait_started_at = Instant::now();
        let epoch_at_start = self.pod_events.delivery_epoch();
        let mut initial_check = true;
        let mut force_recheck = true;
        let mut seen_in_store = false;
        let mut absence_one_shot_used = false;
        let mut quiet_one_shot_used = false;
        let mut placed_on: Option<String> = None;
        #[derive(PartialEq, Eq)]
        struct CachedPodSignature {
            uid: Option<String>,
            node_name: Option<String>,
            ip: Option<PodIp>,
            settled: bool,
            terminal: Option<TerminalPodPhase>,
        }

        let mut last_signature: Option<CachedPodSignature> = None;
        let mut next_degraded_poll_at: Option<Instant> = None;

        loop {
            if Instant::now() >= deadline {
                return Ok(None);
            }

            let was_initial = std::mem::replace(&mut initial_check, false);
            let cached_any = self.lookup_sandbox_pod(target.pod_name);
            let cached = match cached_any.as_ref() {
                Some(pod)
                    if self
                        .cached_pod_matches_readiness_target(sandbox_id, target, pod)
                        .await? =>
                {
                    Some(SandboxPodView::clone(pod))
                }
                _ => None,
            };

            if let Some(pod) = cached.as_ref() {
                seen_in_store = true;
                if let Some(message) = pod.unschedulable_message() {
                    return Err(SubstrateError::Unschedulable {
                        message: format!("sandbox pod is unschedulable: {message}"),
                    });
                }

                let settled = pod_status_is_settled(pod);
                let terminal = terminal_pod_phase(pod);
                let signature = CachedPodSignature {
                    uid: pod.uid().map(str::to_owned),
                    node_name: assigned_pod_node_name(pod).map(str::to_owned),
                    ip: assigned_pod_ip(pod)?,
                    settled,
                    terminal,
                };
                let changed = last_signature.as_ref() != Some(&signature);
                if changed || force_recheck {
                    last_signature = Some(signature);
                    force_recheck = false;

                    if let Some(node_name) = assigned_pod_node_name(pod)
                        && placed_on.as_deref() != Some(node_name)
                    {
                        let _ = self
                            .remember_pod_node_placement(
                                sandbox_id,
                                pod,
                                PlacementRetryDisposition::RetryDuplicatesExecution,
                            )
                            .await?;
                        placed_on = Some(node_name.to_owned());
                    }
                    observed.send_replace(Some(SandboxPodView::clone(pod)));

                    let learned = if was_initial {
                        PodIpLearned::Cached
                    } else {
                        PodIpLearned::Reflected
                    };
                    if terminal.is_some() {
                        if let Some(resolved) = self
                            .fallback_read_pod_for_readiness(
                                sandbox_id,
                                target,
                                observed,
                                deadline,
                                FallbackCancelMode::Never,
                            )
                            .await?
                        {
                            return Ok(Some((resolved, PodIpLearned::Fallback)));
                        }
                    } else if settled {
                        if let Some(resolved) = self
                            .resolve_settled_cached_pod_for_readiness(
                                sandbox_id, target, pod, deadline,
                            )
                            .await?
                        {
                            return Ok(Some((resolved, learned)));
                        }
                    } else if let Some(resolved) = self
                        .resolve_unsettled_cached_pod_for_readiness(sandbox_id, target, pod)
                        .await?
                    {
                        return Ok(Some((resolved, learned)));
                    }
                }
            } else {
                last_signature = None;
                force_recheck = false;
            }

            if !self.pod_reflector_healthy() {
                if next_degraded_poll_at.is_none_or(|at| Instant::now() >= at) {
                    let already_settled = cached.as_ref().is_some_and(|pod| {
                        pod_status_is_settled(pod) && terminal_pod_phase(pod).is_none()
                    });
                    let cancel_mode = if already_settled {
                        FallbackCancelMode::ReflectorHealthy
                    } else {
                        FallbackCancelMode::ReflectorHealthyOrTargetSettled
                    };
                    if let Some(resolved) = self
                        .fallback_read_pod_for_readiness(
                            sandbox_id,
                            target,
                            observed,
                            deadline,
                            cancel_mode,
                        )
                        .await?
                    {
                        return Ok(Some((resolved, PodIpLearned::Fallback)));
                    }
                    // If the reflector recovered or settled `target` into a new
                    // non-terminal status in `pod_store` while queued in the
                    // fallback gate, re-evaluate `pod_store` immediately
                    // instead of arming the degraded poll backoff.
                    if self.pod_reflector_healthy()
                        || (!already_settled
                            && self
                                .cached_target_settled_non_terminal(sandbox_id, target)
                                .await?)
                    {
                        next_degraded_poll_at = None;
                        force_recheck = true;
                        continue;
                    }
                    next_degraded_poll_at =
                        Some(Instant::now() + self.budgets.readiness_quiet_reflector);
                }
            } else {
                next_degraded_poll_at = None;
                if seen_in_store && cached.is_none() && !absence_one_shot_used {
                    absence_one_shot_used = true;
                    if let Some(resolved) = self
                        .fallback_read_pod_for_readiness(
                            sandbox_id,
                            target,
                            observed,
                            deadline,
                            FallbackCancelMode::TargetSettledNonTerminal,
                        )
                        .await?
                    {
                        return Ok(Some((resolved, PodIpLearned::Fallback)));
                    }
                    if self
                        .cached_target_settled_non_terminal(sandbox_id, target)
                        .await?
                    {
                        force_recheck = true;
                        continue;
                    }
                } else if !seen_in_store
                    && cached_any.is_none()
                    && !quiet_one_shot_used
                    && self.pod_events.delivery_epoch() == epoch_at_start
                    && Instant::now() >= wait_started_at + self.budgets.readiness_quiet_reflector
                {
                    quiet_one_shot_used = true;
                    if let Some(resolved) = self
                        .fallback_read_pod_for_readiness(
                            sandbox_id,
                            target,
                            observed,
                            deadline,
                            FallbackCancelMode::TargetSettledNonTerminal,
                        )
                        .await?
                    {
                        return Ok(Some((resolved, PodIpLearned::Fallback)));
                    }
                    if self
                        .cached_target_settled_non_terminal(sandbox_id, target)
                        .await?
                    {
                        force_recheck = true;
                        continue;
                    }
                }
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let mut wake_at = deadline;
            if !self.pod_reflector_healthy() {
                if let Some(poll_at) = next_degraded_poll_at {
                    wake_at = wake_at.min(poll_at);
                }
            } else if !seen_in_store
                && cached_any.is_none()
                && !quiet_one_shot_used
                && self.pod_events.delivery_epoch() == epoch_at_start
            {
                wake_at = wake_at.min(wait_started_at + self.budgets.readiness_quiet_reflector);
            }
            if target.pod_uid.is_none() && cached_any.is_some() {
                wake_at = wake_at.min(now + START_WAIT_SLOW_POLL_INTERVAL);
            }

            match timeout_at(wake_at, events.recv()).await {
                Ok(Ok(name)) => {
                    if name.is_empty() || name == target.pod_name {
                        force_recheck = true;
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped))) => {
                    tracing::debug!(
                        sandbox_id = %sandbox_id,
                        pod_name = %target.pod_name,
                        skipped,
                        "pod readiness consumer lagged on PodEvents; re-reading reflector store"
                    );
                    force_recheck = true;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                    tokio::time::sleep_until(wake_at).await;
                    force_recheck = true;
                }
                Err(_) => {
                    force_recheck = true;
                }
            }
        }
    }

    /// Re-resolve `target`'s address between failed proxy probes without
    /// falling through to uncached apiserver `GET`s while the reflector is
    /// healthy and `pod_store` is still unsettled (such as after
    /// [`Self::node_reported_pod_ip`] wins ahead of kubelet status sync). When
    /// the reflector is unhealthy, pace gated fallback `GET`s to once per
    /// `readiness_quiet_reflector` window even if `pod_store` holds a stale
    /// unsettled or pre-outage snapshot.
    async fn reresolve_pod_ip_during_proxy_wait(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        next_degraded_reresolve_at: &mut Option<Instant>,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        if !self.pod_reflector_healthy()
            && next_degraded_reresolve_at.is_none_or(|at| Instant::now() >= at)
        {
            let (observed, _) = tokio::sync::watch::channel(None);
            if let Some(resolved) = self
                .fallback_read_pod_for_readiness(
                    sandbox_id,
                    target,
                    &observed,
                    deadline,
                    FallbackCancelMode::ReflectorHealthy,
                )
                .await?
            {
                *next_degraded_reresolve_at =
                    Some(Instant::now() + self.budgets.readiness_quiet_reflector);
                return Ok(Some(resolved));
            }
            if !self.pod_reflector_healthy() {
                *next_degraded_reresolve_at =
                    Some(Instant::now() + self.budgets.readiness_quiet_reflector);
                return Ok(None);
            }
        }
        if self.pod_reflector_healthy() {
            *next_degraded_reresolve_at = None;
        }
        if let Some(pod) = self.lookup_sandbox_pod(target.pod_name)
            && self
                .cached_pod_matches_readiness_target(sandbox_id, target, &pod)
                .await?
        {
            if terminal_pod_phase(&pod).is_some() {
                let (observed, _) = tokio::sync::watch::channel(None);
                return self
                    .fallback_read_pod_for_readiness(
                        sandbox_id,
                        target,
                        &observed,
                        deadline,
                        FallbackCancelMode::Never,
                    )
                    .await;
            }
            if pod_status_is_settled(&pod) {
                return self
                    .resolve_settled_cached_pod_for_readiness(sandbox_id, target, &pod, deadline)
                    .await;
            }
            return self
                .resolve_unsettled_cached_pod_for_readiness(sandbox_id, target, &pod)
                .await;
        }
        Ok(None)
    }

    /// The pod IP the pod's node reports for it, once `observed` places the
    /// pod. The node manager reads it from the pod's network namespace when
    /// it allocates the sandbox's network, so the node knows it before
    /// kubelet's status sync publishes the same address. Only an address
    /// reported for this execution generation and the observed pod's UID is
    /// taken, as a candidate the readiness probe must confirm before anything
    /// records it. Only a wait that knows its pod's UID can confirm one.
    async fn node_reported_pod_ip(
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
                        if let Some(pod_ip) = node_reported_address(
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

    async fn wait_for_pod_ip_assignment(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        phase: &'static str,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        let PodProxyWaitTarget {
            pod_name,
            pod_uid,
            generation,
        } = *target;
        let started_at = Instant::now();
        if started_at >= deadline {
            return Ok(None);
        }

        // The pod's node usually reports the address before kubelet publishes
        // `status.podIP`; whichever answers first binds the execution.
        let (observed, observed_by_node) = tokio::sync::watch::channel(None);
        // Both waits are large futures; keep them off the caller's stack.
        let reflected = Box::pin(
            self.reflected_pod_ip(sandbox_id, target, &observed, deadline)
                .instrument(tracing::info_span!(
                    "substrate.k8s.reflected_pod_ip",
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    namespace = %self.k8s.sandbox_namespace,
                    phase = phase,
                )),
        );
        let node_reported = Box::pin(
            self.node_reported_pod_ip(sandbox_id, generation, pod_uid, observed_by_node)
                .instrument(tracing::info_span!(
                    "substrate.k8s.node_pod_ip",
                    sandbox_id = %sandbox_id,
                )),
        );
        let raced = async {
            tokio::select! {
                reflected = reflected => reflected,
                Some(pod_ip) = node_reported => {
                    Ok(Some(((pod_ip, PodIpSource::NodeReported), PodIpLearned::Node)))
                }
            }
        };
        match timeout_at(deadline, raced).await {
            Ok(Ok(Some((resolved, learned)))) => {
                record_pod_ip_ready(metric_identity, sandbox_id, started_at, learned.as_str());
                Ok(Some(resolved))
            }
            Ok(Ok(None)) | Err(_) => Ok(None),
            Ok(Err(err)) => Err(err),
        }
    }

    pub(super) async fn wait_for_pod_proxy_reachable(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        target: PodProxyWaitTarget<'_>,
        phase: &'static str,
        // Phase deadline of the enclosing budget-bounded wait (see
        // `ready_phase_deadline`). The configured data-plane timeout is
        // clamped to it so this wait's phase-tagged timeout fires before
        // the outer budget swallows it into a generic phase-less one.
        phase_deadline: Instant,
    ) -> Result<(PodIp, ExecutionBinding), SubstrateError> {
        let started_at = Instant::now();
        let mut connect_attempts = 0u64;
        let deadline =
            (Instant::now() + self.k8s.create_data_plane_ready_timeout).min(phase_deadline);
        let sandbox_id_string = sandbox_id.to_string();

        let pod_ip_assignment = self
            .wait_for_pod_ip_assignment(metric_identity, sandbox_id, &target, deadline, phase)
            .instrument(tracing::info_span!(
                "substrate.k8s.wait_pod_ip",
                sandbox_id = %sandbox_id,
                pod_name = %target.pod_name,
                namespace = %self.k8s.sandbox_namespace,
                phase = phase,
            ));
        // A pod whose sandbox the node refuses never gets an IP: kubelet
        // retries it until the wait's deadline. Only a wait that knows its
        // pod's UID can tell that pod's refusal from an earlier pod's.
        let pod_ip = match target.pod_uid {
            Some(pod_uid) => tokio::select! {
                pod_ip = pod_ip_assignment => pod_ip?,
                rejection = self.pod_sandbox_rejection(pod_uid) => return Err(rejection),
            },
            None => pod_ip_assignment.await?,
        };
        let Some((mut pod_ip, mut source)) = pod_ip else {
            record_pod_proxy_wait_timeout(metric_identity, sandbox_id, started_at);
            // Pod never reached scheduled / IP-assigned in the window. No
            // node-mgr probe ran on this path, so the message names only
            // the phase that stalled; the create path's defer-cleanup
            // decision keys off the `Timeout` variant itself, not a
            // node-mgr label.
            let phase = WaitReadyTimeoutPhase::PodNeverReady;
            return Err(SubstrateError::Timeout {
                message: format!(
                    "timed out waiting for sandbox pod to become ready (phase: {phase})"
                ),
                phase: Some(phase),
            });
        };

        let mut next_degraded_reresolve_at: Option<Instant> = None;

        loop {
            connect_attempts += 1;
            let connect_started_at = Instant::now();
            let serves = self
                .probe_pod_proxy(sandbox_id, pod_ip, source, &target, deadline)
                .instrument(tracing::info_span!(
                    "substrate.k8s.connect_pod_proxy",
                    sandbox_id = %sandbox_id,
                    pod_ip = %pod_ip,
                    port = self.k8s.sandbox_proxy_port,
                    attempt = connect_attempts,
                ))
                .await
                && timeout_at(
                    deadline,
                    self.record_confirmed(sandbox_id, pod_ip, source, &target),
                )
                .await
                .unwrap_or(Ok(false))?;
            if serves {
                record_pod_proxy_wait_ready(
                    metric_identity,
                    sandbox_id,
                    started_at,
                    connect_started_at,
                );
                return Ok((
                    pod_ip,
                    ExecutionBinding {
                        execution_object_id: target
                            .pod_uid
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| sandbox_id_string.clone()),
                        runtime_endpoint: Self::endpoint_for_pod_ip(
                            pod_ip,
                            self.k8s.sandbox_proxy_port,
                        )?,
                        execution_generation: execution_generation_to_u64(target.generation)?,
                    },
                ));
            }

            if Instant::now() >= deadline {
                record_pod_proxy_wait_timeout(metric_identity, sandbox_id, started_at);
                // Pod IP was assigned but no probe through it reached a node
                // serving this execution. Name the phase that stalled and let
                // the `Timeout` variant drive the defer-cleanup decision.
                let phase = WaitReadyTimeoutPhase::ProxyUnreachable;
                return Err(SubstrateError::Timeout {
                    message: format!(
                        "timed out waiting for sandbox pod proxy to become reachable \
                         (phase: {phase})"
                    ),
                    phase: Some(phase),
                });
            }

            // An address that does not serve the target is re-resolved from the
            // reflector store (or via the fallback gate when the reflector is
            // unhealthy). The recorded address for the target's UID is kept
            // until kubelet publishes another, so after a runtime sandbox is
            // recreated under the same UID the wait binds its new address at
            // kubelet's pace, as without a node report.
            if let Some(current) = self
                .reresolve_pod_ip_during_proxy_wait(
                    sandbox_id,
                    &target,
                    deadline,
                    &mut next_degraded_reresolve_at,
                )
                .await?
            {
                (pod_ip, source) = current;
            }
            sleep(start_wait_poll_interval(started_at)).await;
        }
    }

    /// Whether the node behind `pod_ip` answers that it serves `target`'s
    /// execution at that address, as [`serves_target`] compares it for
    /// `source`. The address forwards to its node's node-api, so a stale or
    /// reused address reaches a node that does not, or that reports another
    /// address for the execution's current network allocation.
    pub(super) async fn probe_pod_proxy(
        &self,
        sandbox_id: SandboxId,
        pod_ip: PodIp,
        source: PodIpSource,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
    ) -> bool {
        let timeout = START_WAIT_CONNECT_TIMEOUT.min(remaining_ready_window(deadline));
        let Some(mut client) =
            Self::pod_worker_client(pod_ip, self.k8s.sandbox_proxy_port, timeout).await
        else {
            return false;
        };
        node_sandboxes(&mut client, sandbox_id, timeout)
            .await
            .is_some_and(|sandboxes| serves_target(sandboxes, sandbox_id, target, pod_ip, source))
    }

    /// `pod_ip` when it is safe to act on as `target`'s runtime address: a
    /// kubelet-published address as it is, a node-reported one only once the
    /// node behind it affirms the execution (see [`Self::runtime_at`]).
    pub(super) async fn verified_runtime_pod_ip(
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
    pub(super) async fn runtime_at(
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

    /// Whether the execution row records `pod_ip` for `target`, recording a
    /// node-reported address its node has just confirmed. A row a suspend,
    /// retire or newer pod has moved on does not take it.
    async fn record_confirmed(
        &self,
        sandbox_id: SandboxId,
        pod_ip: PodIp,
        source: PodIpSource,
        target: &PodProxyWaitTarget<'_>,
    ) -> Result<bool, SubstrateError> {
        match (source, target.pod_uid) {
            (PodIpSource::Published, _) => Ok(true),
            (PodIpSource::NodeReported, Some(pod_uid)) => {
                self.store
                    .record_confirmed_pod_ip(sandbox_id, target.generation, pod_uid, pod_ip)
                    .await
            }
            (PodIpSource::NodeReported, None) => Ok(false),
        }
    }

    /// Resolve once the node refuses `pod_uid`'s pod sandbox as invalid or,
    /// when enabled, refuses memory admission; otherwise never. A memory
    /// refusal fails this placement but lets the caller retry the create.
    ///
    /// A watch the apiserver refuses outright, with a client error other than
    /// a timeout, throttling or an expired version (such as a missing `watch`
    /// grant), leaves the wait to its deadline. Any other failure, and a watch
    /// that ends, is watched again after
    /// [`POD_SANDBOX_REJECTION_REWATCH_DELAY`].
    async fn pod_sandbox_rejection(&self, pod_uid: &str) -> SubstrateError {
        let events: Api<Event> =
            Api::namespaced(self.k8s.client.clone(), &self.k8s.sandbox_namespace);
        // A watch from "0" is served from the apiserver watch cache, replays
        // the refusals already recorded as `Added` and pushes later ones. A
        // LIST would read the namespace's Events from etcd on every poll,
        // since field selectors on Events are not indexed. It is bounded to one
        // pod's UID and one reason, and runs only while a create waits for
        // that pod's IP.
        let params = WatchParams::default()
            .fields(&format!(
                "involvedObject.uid={pod_uid},reason={FAILED_CREATE_POD_SANDBOX_REASON}"
            ))
            .timeout(POD_IP_WATCH_TIMEOUT_SECS);
        let fail_fast_on_memory_refusal =
            pplx_space_env_vars::env_flag_var("SPACE_CREATE_FAIL_FAST_ON_NODE_REJECTION", false);
        let mut warned = false;
        loop {
            match watch_pod_sandbox_rejection(&events, &params, fail_fast_on_memory_refusal).await {
                Ok(Some(error)) => return error,
                Ok(None) => {}
                Err(failure) if failure.is_permanent() => {
                    tracing::warn!(
                        pod_uid,
                        error = %failure.message,
                        "sandbox pod events cannot be watched; waiting for the pod IP without them"
                    );
                    return std::future::pending().await;
                }
                Err(failure) => {
                    if !warned {
                        warned = true;
                        tracing::warn!(
                            pod_uid,
                            error = %failure.message,
                            "failed to watch sandbox pod events; watching them again"
                        );
                    }
                }
            }
            sleep(POD_SANDBOX_REJECTION_REWATCH_DELAY).await;
        }
    }

    pub(super) async fn ensure_active_execution_pod_proxy_reachable(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        // Forwarded to `wait_for_pod_proxy_reachable`; see its doc.
        phase_deadline: Instant,
    ) -> Result<(), SubstrateError> {
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "sandbox has no active Kubernetes execution".to_string(),
            })?;

        if execution.pod_ip.is_some() {
            return Ok(());
        }

        self.wait_for_pod_proxy_reachable(
            metric_identity,
            sandbox_id,
            PodProxyWaitTarget {
                pod_name: &execution.pod_name,
                pod_uid: None,
                generation: execution.generation,
            },
            "wake",
            phase_deadline,
        )
        .instrument(tracing::info_span!(
            "substrate.k8s.wait_wake_pod_proxy_reachable",
            sandbox_id = %sandbox_id,
            pod_name = %execution.pod_name,
            namespace = %self.k8s.sandbox_namespace,
        ))
        .await?;
        Ok(())
    }
}

/// Identity of the execution pod a proxy-reachability wait targets. Groups
/// the execution-row fields so `wait_for_pod_proxy_reachable`'s signature
/// stays under clippy's `too_many_arguments` ceiling as knobs are added.
#[derive(Clone, Copy)]
pub(super) struct PodProxyWaitTarget<'a> {
    pub pod_name: &'a str,
    /// Becomes the binding's `execution_object_id` when present; the wake
    /// path passes `None` (the binding it produces is discarded).
    pub pod_uid: Option<&'a str>,
    pub generation: i64,
}

/// Whether `sandboxes` show `target`'s execution served at `pod_ip`. For a
/// kubelet-published address, a field the node does not report (an older
/// node, or a sandbox manager that has restarted since the allocation) is not
/// compared. A node-reported address is confirmed only by a node that reports
/// the target's generation, Pod UID and that address.
fn serves_target(
    sandboxes: Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>,
    sandbox_id: SandboxId,
    target: &PodProxyWaitTarget<'_>,
    pod_ip: PodIp,
    source: PodIpSource,
) -> bool {
    let strict = source == PodIpSource::NodeReported;
    let matches = |compared: Option<bool>| compared.unwrap_or(!strict);
    let sandbox_id = sandbox_id.to_string();
    sandboxes.iter().any(|info| {
        info.sandbox_id == sandbox_id
            && matches(
                info.execution_generation
                    .map(|generation| generation == target.generation),
            )
            && match (target.pod_uid, info.pod_uid.as_deref()) {
                (Some(expected), Some(reported)) => expected == reported,
                _ => !strict,
            }
            && matches(
                info.external_address
                    .as_deref()
                    .map(|reported| PodIp::parse(reported).ok() == Some(pod_ip)),
            )
    })
}

/// `sandbox_id`'s entries as the worker behind `client` reports them, or
/// `None` when it does not answer within `wait`.
async fn node_sandboxes(
    client: &mut crate::controllers::substrate::RuntimeClient,
    sandbox_id: SandboxId,
    wait: Duration,
) -> Option<Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>> {
    let state = timeout(
        wait,
        client.sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
            sandbox_ids: vec![sandbox_id.to_string()],
        }),
    )
    .await;
    match state {
        Ok(Ok(response)) => Some(response.into_inner().sandboxes),
        _ => None,
    }
}

/// What a worker's report says about `target`'s runtime there.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RuntimeAnswer {
    /// The worker reports the target's generation and Pod UID, and `pod_ip`
    /// whenever it reports an address.
    Serves,
    /// The worker does not report the sandbox, or reports a different
    /// generation or Pod UID.
    Elsewhere,
    /// The worker reports the sandbox without its generation or Pod UID (an
    /// older node, or a sandbox manager that has restarted), or reports the
    /// target's execution at another address.
    Unconfirmed,
}

/// [`RuntimeAnswer`] for `sandboxes`. Unlike readiness ([`serves_target`]),
/// acting on a runtime that is there does not need its address reported: the
/// identity fields name the execution.
pub(super) fn runtime_answer(
    sandboxes: Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>,
    sandbox_id: SandboxId,
    target: &PodProxyWaitTarget<'_>,
    pod_ip: PodIp,
) -> RuntimeAnswer {
    let sandbox_id = sandbox_id.to_string();
    let Some(info) = sandboxes.iter().find(|info| info.sandbox_id == sandbox_id) else {
        return RuntimeAnswer::Elsewhere;
    };
    let identity_differs = info
        .execution_generation
        .is_some_and(|generation| generation != target.generation)
        || info
            .pod_uid
            .as_deref()
            .zip(target.pod_uid)
            .is_some_and(|(reported, expected)| reported != expected);
    let address_differs = info
        .external_address
        .as_deref()
        .is_some_and(|reported| PodIp::parse(reported).ok() != Some(pod_ip));
    let identified =
        info.execution_generation.is_some() && info.pod_uid.is_some() && target.pod_uid.is_some();
    if identity_differs {
        RuntimeAnswer::Elsewhere
    } else if identified && !address_differs {
        RuntimeAnswer::Serves
    } else {
        RuntimeAnswer::Unconfirmed
    }
}

impl PodProxyWaitTarget<'_> {
    /// Whether `pod` is the pod this wait binds: with a target UID, only the
    /// pod that has it.
    pub(super) fn names(&self, pod: &SandboxPodView) -> bool {
        self.pod_uid.is_none_or(|uid| pod.uid() == Some(uid))
    }
}

/// The address `sandboxes` report for `sandbox_id` while its pod is `pod_uid`.
fn node_reported_address(
    sandboxes: Vec<pplx_space_sbx_mgr_client::proto::SandboxInfo>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
) -> Option<PodIp> {
    let sandbox_id = sandbox_id.to_string();
    sandboxes
        .into_iter()
        .find(|info| {
            info.sandbox_id == sandbox_id
                && info.execution_generation == Some(generation)
                && info.pod_uid.as_deref() == Some(pod_uid)
        })
        .and_then(|info| PodIp::parse(info.external_address.as_deref()?).ok())
}
