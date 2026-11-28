mod confirm;
mod connected_command_ready;
mod fallback;
pub(in crate::controllers::substrate::kubernetes) mod target;

use self::confirm::{
    LossConfirmationDisposition, TargetExecutionState, recv_relevant_pod_update,
    retired_during_proxy_wait_error, superseded_during_proxy_wait_error,
};
#[cfg(test)]
pub(super) use self::connected_command_ready::ConnectedCommandReadiness;
pub(in crate::controllers::substrate::kubernetes) use self::fallback::ReadinessFallback;
use self::fallback::{InFlightFallback, poll_in_flight_fallback};
pub(super) use self::target::PodProxyWaitTarget;
#[cfg(test)]
use self::target::{RuntimeAnswer, node_reported_address, runtime_answer, serves_target};
use super::store::PlacementRetryDisposition;
use super::support::*;
use super::*;
use pplx_space_accounts::ClientId;
use pplx_space_api_route::wait_ready_lifecycle_request;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::sleep_until;

async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// How a readiness wait learned its pod's address, as the
/// `network_address_assigned` step's `result` names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PodIpLearned {
    /// The reflector already showed it when the wait began.
    Cached,
    /// A reflector update during the wait.
    Reflector,
    /// A backstop read from the apiserver.
    Backstop,
    /// The pod's node, before Kubernetes published it.
    Node,
}

impl PodIpLearned {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cached => "cached",
            Self::Reflector => "reflector",
            Self::Backstop => "backstop",
            Self::Node => "node",
        }
    }
}

type ExecutionRowStamp = (Option<PodUid>, ActiveExecutionState, time::OffsetDateTime);

fn execution_row_stamp(row: &super::store::ActiveExecutionStatusRow) -> ExecutionRowStamp {
    (row.pod_uid.clone(), row.state, row.updated_at)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AssignedPodIp {
    ip: PodIp,
    source: PodIpSource,
    pod_uid: Option<PodUid>,
}

#[derive(Clone, Copy)]
pub(super) enum CommandReadyPod<'a> {
    PreResolved(PodIp),
    /// Preserve the pre-route legacy behavior: resolve the active execution on
    /// each retry without claiming that guest readiness proves route identity.
    ResolveLegacy,
    SelectedRoute {
        route_identity: &'a pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        pod_ip: Option<PodIp>,
    },
}

impl KubernetesSubstrate {
    /// The kept worker client a runtime or access path hands out for
    /// `sandbox_id`'s resolved `pod`. A Native data-plane request (see
    /// [`crate::controllers::substrate::native_access`]) gets
    /// [`Self::native_kept_worker_client`]; every other caller gets
    /// [`Self::kept_worker_client`], unchanged.
    pub(super) async fn access_kept_worker_client(
        &self,
        sandbox_id: SandboxId,
        pod: &ResolvedPodInstance,
    ) -> Result<RuntimeClient, ControllerError> {
        if crate::controllers::substrate::native_access::active() {
            return self.native_kept_worker_client(sandbox_id, pod).await;
        }
        self.kept_worker_client(pod).await
    }

    /// [`Self::kept_worker_client`] for a Native data-plane request, which
    /// fails over from a pod that does not answer instead of waiting out the
    /// full connect budget on its address.
    ///
    /// A miss first dials once within [`POD_WORKER_FIRST_DIAL_TIMEOUT`]. If
    /// that fails, the sandbox's pod is resolved again: the same instance is
    /// dialed with the full connect budget, as every other caller dials it; a
    /// replacement is dialed at its own address; and no pod returns the first
    /// dial's error, so the caller's recovery path takes over at once. The
    /// client also reports how it used its connection if the request holds it
    /// long; see [`super::super::KeptCallReport`].
    pub(super) async fn native_kept_worker_client(
        &self,
        sandbox_id: SandboxId,
        pod: &ResolvedPodInstance,
    ) -> Result<RuntimeClient, ControllerError> {
        let first = self
            .kept_worker_client_reported(pod, Some(POD_WORKER_FIRST_DIAL_TIMEOUT))
            .await;
        let Err(first_error) = first else {
            return first;
        };
        let current = match self.resolve_pod_instance(sandbox_id).await {
            Ok(Some(current)) => current,
            Ok(None) => {
                pplx_space_grpc_metrics::client_channel_cache("worker-node-api", "pod_gone");
                return Err(first_error);
            }
            Err(_) => return Err(first_error),
        };
        if current.sandbox_restoring {
            // A replacement mid-restore isn't dialed for a user request (see
            // `join_restore_before_dial`); the caller's recovery joins it.
            return Err(first_error);
        }
        if !current.same_instance(pod) {
            pplx_space_grpc_metrics::client_channel_cache("worker-node-api", "pod_moved");
        }
        self.kept_worker_client_reported(&current, None).await
    }

    /// The kept client for `pod`, its miss dialed with the default policy
    /// or, given `first_dial`, once within it; reported as
    /// [`super::super::KeptCallReport`] describes.
    async fn kept_worker_client_reported(
        &self,
        pod: &ResolvedPodInstance,
        first_dial: Option<Duration>,
    ) -> Result<RuntimeClient, ControllerError> {
        let port = self.k8s.sandbox_proxy_port;
        let key = super::pod_worker_channels::PodWorkerKey {
            addr: pod.ip.socket_addr(port),
            pod_uid: pod.pod_uid.clone(),
            generation: pod.generation,
        };
        let pod_ip = pod.ip;
        let (client, handout) = self
            .pod_worker_channels
            .client_with_handout(key, async move {
                let endpoint =
                    Self::endpoint_for_pod_ip(pod_ip, port).map_err(ControllerError::from)?;
                let Some(bound) = first_dial else {
                    return crate::controllers::substrate::connect_worker_watched(endpoint).await;
                };
                // Bounding the attempt from outside, rather than through the
                // endpoint's connect timeout, leaves the kept channel's own
                // redials at the bound every other caller's channel has.
                tokio::time::timeout(
                    bound,
                    crate::controllers::substrate::connect_worker_watched_with(
                        endpoint,
                        pplx_space_common_grpc::GrpcRetryPolicy::none(),
                    ),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(ControllerError::service_unavailable(format!(
                        "node-api not reachable: no connection within {bound:?}"
                    )))
                })
            })
            .await?;
        Ok(client.with_report(handout.kept, handout.connection_age, handout.idle))
    }

    pub(super) fn should_allow_access_recovery(
        sandbox: &Sandbox,
        activity: ExecutionActivityKind,
    ) -> Result<(), ControllerError> {
        if is_dormant_lifecycle(&sandbox.lifecycle)
            && !activity.allows_implicit_wake(&sandbox.auto_lifecycle)
        {
            return Err(
                match sandbox
                    .lifecycle
                    .parse::<pplx_space_sandbox_model::SandboxLifecycle>()
                {
                    Ok(lifecycle) => ControllerError::sandbox_not_running(
                        crate::controllers::SandboxNotRunning {
                            sandbox_id: sandbox.id,
                            lifecycle,
                        },
                        "sandbox is not running",
                    ),
                    Err(_) => ControllerError::new(
                        axum::http::StatusCode::CONFLICT,
                        "sandbox is not running",
                    ),
                },
            );
        }

        if !is_wake_recovery_lifecycle(&sandbox.lifecycle) {
            return Err(ControllerError::service_unavailable(format!(
                "sandbox cannot be recovered while lifecycle is {}",
                sandbox.lifecycle
            )));
        }

        Ok(())
    }

    /// The address of the pod `target` names, from the shared sandbox-pod
    /// reflector, sending each view of the pod to `observed` once its
    /// placement is recorded. A pod other than the one `target` names is
    /// ignored.
    ///
    /// The wait opens no watch and polls nothing of its own: it reads the
    /// reflector's store and wakes on [`pod_reflector::PodEvents`]. Once the
    /// store shows the pod settled (an IP, a terminal phase or
    /// unschedulable), the address is resolved and validated as every
    /// readiness resolution is. While the shared pod reflector watch is
    /// healthy, an unsettled (`Pending`) pod stays on the event-driven wait
    /// with zero apiserver `GET`s; `fallback` paces degraded-mode apiserver
    /// reads only when the reflector hasn't finished initial sync, its watch
    /// has faulted, or it has been silent past `readiness_quiet_reflector`.
    async fn reflected_pod_ip(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        fallback: &mut ReadinessFallback,
        observed: &tokio::sync::watch::Sender<Option<SandboxPodView>>,
    ) -> Result<Option<(AssignedPodIp, PodIpLearned)>, SubstrateError> {
        // Subscribed before the first store read, so no update between that
        // read and the wait is missed.
        let mut updates = self.pod_events.subscribe();
        let started_at = Instant::now();
        let mut learned = PodIpLearned::Cached;
        let mut seen_in_pod_store = false;
        let mut last_view: Option<Arc<SandboxPodView>> = None;
        let mut rejected_view: Option<Arc<SandboxPodView>> = None;
        let mut checked_mismatched: Option<(Arc<SandboxPodView>, Instant)> = None;
        let mut placed_on: Option<String> = None;
        let mut recheck_at: Option<Instant> = None;
        let mut joiner_missing_pod_recheck_at: Option<Instant> = None;
        let mut pending_fallback: Option<InFlightFallback<'_>> = None;
        loop {
            #[cfg(test)]
            self.readiness_test_hooks
                .ip_wait_iterations
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let now = Instant::now();
            let raw_view = self.lookup_sandbox_pod(target.pod_name);
            self.check_mismatched_cached_pod(
                sandbox_id,
                target,
                raw_view.as_ref(),
                now,
                started_at,
                &mut checked_mismatched,
            )
            .await?;
            let view = raw_view.filter(|pod| target.names(pod));
            if view.is_some() {
                seen_in_pod_store = true;
                joiner_missing_pod_recheck_at = None;
            }
            let view_rejected = view.as_ref().is_some_and(|pod| {
                rejected_view
                    .as_ref()
                    .is_some_and(|rej| Arc::ptr_eq(rej, pod))
            });
            let view_usable = !view_rejected && view.is_some();
            if !view_usable {
                recheck_at = None;
            }
            let mut settled = view_usable && view.as_deref().is_some_and(pod_status_is_settled);
            // The store replaces a pod's view on every update, so an
            // unchanged `Arc` is a view this wait has already handled.
            let fresh = view.as_ref().is_some_and(|pod| {
                last_view
                    .as_ref()
                    .is_none_or(|seen| !Arc::ptr_eq(seen, pod))
            });
            let recheck = view_usable && recheck_at.is_some_and(|at| now >= at);
            if let Some(pod) = view.as_ref().filter(|_| fresh || recheck) {
                last_view = Some(Arc::clone(pod));
                recheck_at = None;
                let check = self
                    .inspect_active_execution_for_pod(sandbox_id, target, pod)
                    .await?;
                if check.pod_uid_superseded {
                    return Err(superseded_during_proxy_wait_error(target.pod_name));
                }
                if check.execution_abandoned {
                    return Err(retired_during_proxy_wait_error(target.pod_name));
                }
                let mut matches_execution = check.matches_generation;
                if !matches_execution {
                    if target.pod_uid.is_none() || check.owner_replacing_pod {
                        recheck_at = Some(now + start_wait_poll_interval(started_at));
                    } else {
                        rejected_view = Some(Arc::clone(pod));
                    }
                    settled = false;
                } else if !settled {
                    if let Some((ip, source)) = check.recorded_ip {
                        return Ok(Some((
                            AssignedPodIp {
                                ip,
                                source,
                                pod_uid: check.recorded_uid,
                            },
                            learned,
                        )));
                    }
                    if target.pod_uid.is_none() {
                        // A UID-unpinned joiner cannot run `node_reported_pod_ip`
                        // directly; recheck Postgres at the start-wait cadence
                        // so a `NodeReported` address confirmed by the owner is
                        // picked up even while kubelet lags `status.podIP`.
                        recheck_at = Some(now + start_wait_poll_interval(started_at));
                    }
                } else {
                    let mut pre_resolved = None;
                    if terminal_pod_phase(pod).is_some() {
                        if let Some(in_flight) = pending_fallback.take() {
                            #[cfg(test)]
                            self.readiness_test_hooks
                                .awaiting_in_flight_terminal_fallback
                                .notify_waiters();
                            if let Some(outcome) = in_flight
                                .resolve(fallback, self, sandbox_id, target.pod_name)
                                .await?
                            {
                                if outcome.pod_uid_superseded
                                    || outcome.terminal_or_absent_retired
                                    || outcome.resolved.is_some()
                                {
                                    pre_resolved = Some(outcome);
                                } else if let Some(scheduled) = outcome.scheduled_pod {
                                    observed.send_replace(Some(scheduled));
                                }
                            }
                        }
                    } else if self.pod_reflector_watch_healthy_at(Instant::now())
                        && pending_fallback
                            .as_ref()
                            .is_none_or(|in_flight| !in_flight.confirms_loss())
                    {
                        pending_fallback = None;
                    }
                    let outcome = match pre_resolved {
                        Some(outcome) => outcome,
                        None => {
                            let outcome = self
                                .resolve_pod_ip_for_readiness(sandbox_id, target, deadline)
                                .await?;
                            if outcome.used_uncached_get {
                                fallback.record_completed_at(Instant::now());
                            }
                            outcome
                        }
                    };
                    if outcome.pod_uid_superseded {
                        return Err(superseded_during_proxy_wait_error(target.pod_name));
                    }
                    if outcome.terminal_or_absent_retired {
                        return Err(retired_during_proxy_wait_error(target.pod_name));
                    }
                    if let Some(scheduled) = outcome.scheduled_pod {
                        observed.send_replace(Some(scheduled));
                    }
                    if let Some((ip, source)) = outcome.resolved {
                        let source_kind = if outcome.used_uncached_get {
                            PodIpLearned::Backstop
                        } else {
                            learned
                        };
                        return Ok(Some((
                            AssignedPodIp {
                                ip,
                                source,
                                pod_uid: outcome.resolved_uid,
                            },
                            source_kind,
                        )));
                    }
                    matches_execution = outcome.cache_matches_execution;
                    if matches_execution {
                        // The execution row does not describe the settled pod
                        // yet, as for a create joining one whose owner has not
                        // recorded its pod. Ask again at the start-wait cadence:
                        // an authoritative settled pod resolves from the store
                        // and Postgres alone, while an unverified `creating`
                        // view also leaves degraded fallback unblocked.
                        recheck_at = Some(now + start_wait_poll_interval(started_at));
                        if outcome.execution_in_flight && target.pod_uid.is_none() {
                            settled = false;
                        }
                    } else if outcome.execution_in_flight && target.pod_uid.is_none() {
                        recheck_at = Some(now + start_wait_poll_interval(started_at));
                        settled = false;
                    } else if !outcome.deadline_elapsed {
                        rejected_view = Some(Arc::clone(pod));
                        settled = false;
                    } else {
                        settled = false;
                    }
                }
                if fresh
                    && matches_execution
                    && (check.placement_fence_uid.is_some()
                        || target.creating_repair_epoch.is_some())
                    && terminal_pod_phase(pod).is_none()
                {
                    if let (Some(node_name), Some(fence_uid)) = (
                        assigned_pod_node_name(pod),
                        check.placement_fence_uid.as_ref(),
                    ) && placed_on.as_deref() != Some(node_name)
                    {
                        let placed = self
                            .remember_pod_node_placement_fenced(
                                sandbox_id,
                                target.generation,
                                fence_uid.as_ref().map(PodUid::as_str),
                                pod,
                                PlacementRetryDisposition::RetryDuplicatesExecution,
                            )
                            .await?;
                        if !placed.matched_execution {
                            last_view = None;
                            recheck_at = Some(Instant::now());
                            continue;
                        }
                        placed_on = Some(node_name.to_owned());
                    }
                    observed.send_replace(Some(SandboxPodView::clone(pod)));
                }
            }
            if view.is_none()
                && target.pod_uid.is_none()
                && pending_fallback.is_none()
                && now < deadline
                && joiner_missing_pod_recheck_at.is_none_or(|at| now >= at)
            {
                let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
                    return Err(retired_during_proxy_wait_error(target.pod_name));
                };
                if row.generation != target.generation || is_suspended_lifecycle(&row.lifecycle) {
                    return Err(retired_during_proxy_wait_error(target.pod_name));
                }
                if !row.activation_attempt_live
                    && !row.pod_materialized_at_set
                    && stale_restore_execution_create(
                        &row.lifecycle,
                        row.state,
                        row.create_age_expired,
                    )
                {
                    pending_fallback = Some(InFlightFallback::start(
                        self.resolve_pod_ip_for_readiness_uncached(sandbox_id, target, deadline),
                        true,
                    ));
                }
                joiner_missing_pod_recheck_at =
                    Some(Instant::now() + self.budgets.readiness_quiet_reflector);
            }
            let vanished_from_store = view.is_none()
                && target.pod_uid.is_some()
                && pending_fallback.is_none()
                && std::mem::replace(&mut seen_in_pod_store, false);
            let now = Instant::now();
            if pending_fallback.is_none()
                && !settled
                && now < deadline
                && (vanished_from_store || fallback.due(now, self))
            {
                pending_fallback = Some(InFlightFallback::start(
                    self.resolve_pod_ip_for_readiness_uncached(sandbox_id, target, deadline),
                    vanished_from_store,
                ));
            }
            learned = PodIpLearned::Reflector;
            // Fallback reads are only eligible when the store doesn't show the
            // pod settled and no fallback read is already in flight; a settled
            // pod is asked again at `recheck_at`.
            let next_read =
                (!settled && pending_fallback.is_none()).then(|| fallback.next_read(self));
            let joiner_recheck = pending_fallback
                .is_none()
                .then_some(joiner_missing_pod_recheck_at)
                .flatten();
            let mismatched_recheck = pending_fallback
                .is_none()
                .then(|| checked_mismatched.as_ref().map(|(_, at)| *at))
                .flatten();
            let wake_at = [next_read, recheck_at, joiner_recheck, mismatched_recheck]
                .into_iter()
                .flatten()
                .fold(deadline, Instant::min);
            if Instant::now() >= deadline {
                return Ok(None);
            }
            #[cfg(test)]
            if recheck_at.is_some() || mismatched_recheck.is_some() {
                self.readiness_test_hooks.recheck_armed.notify_waiters();
            }
            let reflector_wakeup = async {
                if !self.pod_reflector_ready.is_ready() {
                    // An unready reflector's store is not yet readable; its
                    // first list's updates still arrive on `updates`.
                    tokio::select! {
                        ready = self.pod_reflector_ready.wait_until_ready() => {
                            if ready.is_err() {
                                sleep_until(wake_at).await;
                            }
                        }
                        () = sleep_until(wake_at) => {}
                    }
                    return;
                }
                loop {
                    match timeout_at(wake_at, updates.recv()).await {
                        Ok(Ok(pod_name)) if !pod_name.is_empty() && pod_name != target.pod_name => {
                            continue;
                        }
                        // An update for the pod, a reflector watch-fault or
                        // re-list sentinel (`""`), or an update this wait may
                        // have missed.
                        Ok(Ok(_) | Err(RecvError::Lagged(_))) => return,
                        // The reflector has stopped: only the backstop reads
                        // remain.
                        Ok(Err(RecvError::Closed)) => {
                            sleep_until(wake_at).await;
                            return;
                        }
                        Err(_) => return,
                    }
                }
            };
            tokio::select! {
                (outcome, confirms_loss) = poll_in_flight_fallback(&mut pending_fallback), if pending_fallback.is_some() => {
                    if let Some(outcome) = InFlightFallback::finish(
                        outcome,
                        confirms_loss,
                        fallback,
                        self,
                        sandbox_id,
                        target.pod_name,
                    )? {
                        if outcome.pod_uid_superseded {
                            return Err(superseded_during_proxy_wait_error(target.pod_name));
                        }
                        if outcome.terminal_or_absent_retired {
                            return Err(retired_during_proxy_wait_error(target.pod_name));
                        }
                        if let Some(scheduled) = outcome.scheduled_pod {
                            observed.send_replace(Some(scheduled));
                        }
                        if let Some((ip, source)) = outcome.resolved {
                            return Ok(Some((
                                AssignedPodIp {
                                    ip,
                                    source,
                                    pod_uid: outcome.resolved_uid,
                                },
                                PodIpLearned::Backstop,
                            )));
                        }
                    }
                }
                () = reflector_wakeup => {}
            }
        }
    }

    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) async fn wait_for_pod_ip_assignment(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        fallback: &mut ReadinessFallback,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        Ok(self
            .wait_for_pod_ip_assignment_with_client(
                metric_identity,
                sandbox_id,
                target,
                deadline,
                fallback,
            )
            .await?
            .map(|(assigned, _, _)| (assigned.ip, assigned.source)))
    }

    async fn wait_for_pod_ip_assignment_with_client(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        fallback: &mut ReadinessFallback,
    ) -> Result<Option<(AssignedPodIp, Option<Option<ClientId>>, PodIpLearned)>, SubstrateError>
    {
        let PodProxyWaitTarget {
            pod_name,
            pod_uid,
            generation,
            creating_repair_epoch: _,
            abort_on_uid_change: _,
        } = *target;
        let started_at = Instant::now();
        // The pod's node usually reports the address before Kubernetes does;
        // whichever answers first binds the execution.
        let (observed, observed_by_node) = tokio::sync::watch::channel(None);
        // Both waits are large futures; keep them off the caller's stack.
        let reflected = Box::pin(
            self.reflected_pod_ip(sandbox_id, target, deadline, fallback, &observed)
                .instrument(tracing::info_span!(
                    "substrate.k8s.reflected_pod_ip",
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    namespace = %self.k8s.sandbox_namespace,
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
                    Ok(Some((
                        AssignedPodIp {
                            ip: pod_ip,
                            source: PodIpSource::NodeReported,
                            pod_uid: pod_uid.and_then(|uid| PodUid::parse(uid).ok()),
                        },
                        PodIpLearned::Node,
                    )))
                }
            }
        };
        match timeout_at(deadline, raced).await {
            Ok(Ok(Some((assigned, learned)))) => {
                let observed_client = observed
                    .borrow()
                    .as_ref()
                    .filter(|pod| target.names(pod))
                    .map(SandboxPodView::client_id);
                record_pod_ip_ready(metric_identity, sandbox_id, started_at, learned.as_str());
                Ok(Some((assigned, observed_client, learned)))
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
        // Phase deadline of the enclosing budget-bounded wait (see
        // `ready_phase_deadline`). The configured data-plane timeout is
        // clamped to it so this wait's phase-tagged timeout fires before
        // the outer budget swallows it into a generic phase-less one.
        phase_deadline: Instant,
    ) -> Result<(PodIp, ExecutionBinding), SubstrateError> {
        let started_at = Instant::now();
        #[cfg(test)]
        let _proxy_wait_hook = self.readiness_test_hooks.enter_proxy_wait(started_at);
        let mut connect_attempts = 0u64;
        let deadline =
            (Instant::now() + self.k8s.create_data_plane_ready_timeout).min(phase_deadline);
        let sandbox_id_string = sandbox_id.to_string();
        #[cfg(test)]
        let mut fallback = match *self
            .readiness_test_hooks
            .fallback_jitter
            .lock()
            .expect("fallback_jitter hook")
        {
            Some(jitter) => ReadinessFallback::with_jitter(
                Instant::now(),
                self.budgets.readiness_quiet_reflector,
                jitter,
            ),
            None => ReadinessFallback::new(&self.budgets),
        };
        #[cfg(not(test))]
        let mut fallback = ReadinessFallback::new(&self.budgets);

        let pod_ip_assignment = self
            .wait_for_pod_ip_assignment_with_client(
                metric_identity,
                sandbox_id,
                &target,
                deadline,
                &mut fallback,
            )
            .instrument(tracing::info_span!(
                "substrate.k8s.wait_pod_ip",
                sandbox_id = %sandbox_id,
                pod_name = %target.pod_name,
                namespace = %self.k8s.sandbox_namespace,
            ));
        // A pod whose sandbox the node refuses never gets an IP: kubelet
        // retries it until the wait's deadline. Only a wait that knows its
        // pod's UID can tell that pod's refusal from an earlier pod's.
        let pod_ip = match target.pod_uid {
            Some(pod_uid) => {
                let refusal = self.pod_sandbox_rejections.register(pod_uid);
                let fail_fast_on_memory_refusal = pplx_space_env_vars::env_flag_var(
                    "SPACE_CREATE_FAIL_FAST_ON_NODE_REJECTION",
                    false,
                );
                tokio::select! {
                    pod_ip = pod_ip_assignment => pod_ip?,
                    rejection = refusal.refused(fail_fast_on_memory_refusal) => {
                        return Err(rejection);
                    }
                }
            }
            None => pod_ip_assignment.await?,
        };
        let Some((
            AssignedPodIp {
                ip: mut pod_ip,
                mut source,
                pod_uid: mut resolved_uid,
            },
            observed_client,
            learned,
        )) = pod_ip
        else {
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
        #[cfg(test)]
        self.readiness_test_hooks.run_after_ip_assigned();

        let mut seen_in_pod_store =
            matches!(learned, PodIpLearned::Cached | PodIpLearned::Reflector)
                || self
                    .lookup_sandbox_pod(target.pod_name)
                    .is_some_and(|pod| target.names(&pod));
        let mut pod_rx = self.pod_events.subscribe();
        let mut pod_rx_open = true;
        let mut handled_post_ip_view: Option<Arc<SandboxPodView>> = (source
            == PodIpSource::Published)
            .then(|| {
                self.lookup_sandbox_pod(target.pod_name).filter(|pod| {
                    target.names(pod)
                        && pod.uid() == resolved_uid.as_ref().map(PodUid::as_str)
                        && terminal_pod_phase(pod).is_none()
                        && assigned_pod_ip(pod).ok().flatten() == Some(pod_ip)
                })
            })
            .flatten();
        let mut rejected_post_ip_view: Option<(Arc<SandboxPodView>, ExecutionRowStamp)> = None;
        let mut last_row_stamp: Option<ExecutionRowStamp> = None;
        let mut row_change_pending = false;
        let mut pending_post_ip_fallback: Option<InFlightFallback<'_>> = None;
        loop {
            connect_attempts += 1;
            let connect_started_at = Instant::now();
            let mut replaced_during_probe = None;
            let probe_ok = {
                let probe_target = target
                    .with_pod_uid(target.pod_uid.or(resolved_uid.as_ref().map(PodUid::as_str)));
                let mut probe = std::pin::pin!(
                    self.probe_pod_proxy(sandbox_id, pod_ip, source, &probe_target, deadline)
                        .instrument(tracing::info_span!(
                            "substrate.k8s.connect_pod_proxy",
                            sandbox_id = %sandbox_id,
                            pod_ip = %pod_ip,
                            port = self.k8s.sandbox_proxy_port,
                            attempt = connect_attempts,
                        ))
                );
                loop {
                    let now = Instant::now();
                    let cached_for_probe = self
                        .lookup_sandbox_pod(target.pod_name)
                        .filter(|pod| target.names(pod));
                    let terminal_in_store = cached_for_probe.as_ref().is_some_and(|pod| {
                        terminal_pod_phase(pod).is_some()
                            && !handled_post_ip_view
                                .as_ref()
                                .is_some_and(|seen| Arc::ptr_eq(seen, pod))
                            && !rejected_post_ip_view.as_ref().is_some_and(|(rej, stamp)| {
                                Arc::ptr_eq(rej, pod) && last_row_stamp.as_ref() == Some(stamp)
                            })
                    });
                    let vanished_from_store = if cached_for_probe.is_some() {
                        seen_in_pod_store = true;
                        false
                    } else if pending_post_ip_fallback.is_none() {
                        std::mem::replace(&mut seen_in_pod_store, false)
                    } else {
                        false
                    };
                    let confirms_loss = vanished_from_store || terminal_in_store;
                    if pending_post_ip_fallback.is_none()
                        && now < deadline
                        && (confirms_loss || fallback.due(now, self))
                    {
                        if let Some(pod) = cached_for_probe.filter(|_| terminal_in_store) {
                            handled_post_ip_view = Some(pod);
                        }
                        pending_post_ip_fallback = Some(InFlightFallback::start(
                            self.resolve_pod_ip_for_readiness_uncached(
                                sandbox_id, &target, deadline,
                            ),
                            confirms_loss,
                        ));
                    }
                    let arm_fallback_at = (pending_post_ip_fallback.is_none() && now < deadline)
                        .then(|| fallback.next_read(self).min(deadline));
                    tokio::select! {
                        ok = &mut probe => break Some(ok),
                        () = sleep_until_optional(arm_fallback_at), if arm_fallback_at.is_some() => {}
                        recv = recv_relevant_pod_update(&mut pod_rx, target.pod_name), if pod_rx_open && pending_post_ip_fallback.is_none() => {
                            if matches!(recv, Err(RecvError::Closed)) {
                                pod_rx_open = false;
                            }
                        }
                        (outcome, confirms_loss) = poll_in_flight_fallback(&mut pending_post_ip_fallback), if pending_post_ip_fallback.is_some() => {
                            if let Some(outcome) = InFlightFallback::finish(
                                outcome,
                                confirms_loss,
                                &mut fallback,
                                self,
                                sandbox_id,
                                target.pod_name,
                            )? {
                                if outcome.pod_uid_superseded {
                                    return Err(superseded_during_proxy_wait_error(
                                        target.pod_name,
                                    ));
                                }
                                if outcome.terminal_or_absent_retired {
                                    return Err(retired_during_proxy_wait_error(
                                        target.pod_name,
                                    ));
                                }
                                if let Some(current) = outcome.resolved
                                    && (current != (pod_ip, source)
                                        || outcome.resolved_uid != resolved_uid)
                                {
                                    replaced_during_probe =
                                        Some((current, outcome.resolved_uid));
                                    break None;
                                }
                            }
                        }
                    }
                }
            };
            if let Some(((next_ip, next_source), next_uid)) = replaced_during_probe {
                (pod_ip, source, resolved_uid) = (next_ip, next_source, next_uid);
                if Instant::now() < deadline {
                    continue;
                }
            }
            let proxy_unreachable_timeout = || {
                record_pod_proxy_wait_timeout(metric_identity, sandbox_id, started_at);
                // Pod IP was assigned but no probe through it reached a node
                // serving this execution. Name the phase that stalled and let
                // the `Timeout` variant drive the defer-cleanup decision.
                let phase = WaitReadyTimeoutPhase::ProxyUnreachable;
                SubstrateError::Timeout {
                    message: format!(
                        "timed out waiting for sandbox pod proxy to become reachable \
                         (phase: {phase})"
                    ),
                    phase: Some(phase),
                }
            };
            let mut probe_confirmed = probe_ok.unwrap_or(false);
            if probe_confirmed
                && pending_post_ip_fallback
                    .as_ref()
                    .is_some_and(InFlightFallback::confirms_loss)
                && let Some(in_flight) = pending_post_ip_fallback.take()
            {
                let snapshot = confirm::PostIpEndpointSnapshot::new(
                    sandbox_id,
                    target.pod_name,
                    (pod_ip, source),
                    &resolved_uid,
                    deadline,
                );
                match self
                    .resolve_post_ip_loss_confirmation(in_flight, &mut fallback, snapshot)
                    .await?
                {
                    LossConfirmationDisposition::ConfirmedCurrent => {}
                    LossConfirmationDisposition::Replaced(current, next_uid) => {
                        (pod_ip, source, resolved_uid) = (current.0, current.1, next_uid);
                        if Instant::now() < deadline {
                            continue;
                        }
                        return Err(proxy_unreachable_timeout());
                    }
                    LossConfirmationDisposition::Lost => probe_confirmed = false,
                    LossConfirmationDisposition::DeadlineElapsed => {
                        return Err(proxy_unreachable_timeout());
                    }
                }
            }
            let probe_target =
                target.with_pod_uid(target.pod_uid.or(resolved_uid.as_ref().map(PodUid::as_str)));
            let mut serves = probe_confirmed
                && timeout_at(
                    deadline,
                    self.record_confirmed(
                        sandbox_id,
                        (pod_ip, source),
                        &probe_target,
                        observed_client.as_ref().map(Option::as_ref),
                        deadline,
                    ),
                )
                .await
                .unwrap_or(Ok(false))?;
            if serves {
                let snapshot = confirm::PostIpEndpointSnapshot::new(
                    sandbox_id,
                    target.pod_name,
                    (pod_ip, source),
                    &resolved_uid,
                    deadline,
                );
                let state = confirm::PostIpReflectorWatchState {
                    seen_in_pod_store: &mut seen_in_pod_store,
                    handled_post_ip_view: &mut handled_post_ip_view,
                    pending_post_ip_fallback: &mut pending_post_ip_fallback,
                    fallback: &mut fallback,
                };
                match self
                    .recheck_reflector_loss_after_confirm(&target, &probe_target, snapshot, state)
                    .await?
                {
                    LossConfirmationDisposition::ConfirmedCurrent => {}
                    LossConfirmationDisposition::Replaced(current, next_uid) => {
                        (pod_ip, source, resolved_uid) = (current.0, current.1, next_uid);
                        if Instant::now() < deadline {
                            continue;
                        }
                        return Err(proxy_unreachable_timeout());
                    }
                    LossConfirmationDisposition::Lost => serves = false,
                    LossConfirmationDisposition::DeadlineElapsed => {
                        return Err(proxy_unreachable_timeout());
                    }
                }
            }
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
                        execution_object_id: probe_target
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
                return Err(proxy_unreachable_timeout());
            }

            let row_stamp = match timeout_at(
                deadline,
                self.active_execution_for_target(sandbox_id, &target),
            )
            .await
            {
                Ok(Ok(TargetExecutionState::Active(row))) => execution_row_stamp(&row),
                Ok(Ok(TargetExecutionState::Retired)) => {
                    return Err(retired_during_proxy_wait_error(target.pod_name));
                }
                Ok(Ok(TargetExecutionState::Superseded)) => {
                    return Err(superseded_during_proxy_wait_error(target.pod_name));
                }
                Ok(Err(err)) => return Err(err),
                Err(_) => return Err(proxy_unreachable_timeout()),
            };
            if last_row_stamp.as_ref() != Some(&row_stamp) {
                row_change_pending = true;
                last_row_stamp = Some(row_stamp.clone());
            }

            // An address that does not serve the target is re-resolved. The
            // recorded address for the target's UID is kept until kubelet
            // publishes another, so after a runtime sandbox is recreated under
            // the same UID the wait binds its new address at kubelet's pace,
            // as without a node report. While the reflector is healthy, an
            // unsettled pod (unless `row_stamp` changed since the last
            // scheduled pod inspection) or an already-handled `Arc` skips
            // redundant re-resolution; degraded-mode fallback reads stay paced
            // and run concurrently with proxy probes.
            let cached = self
                .lookup_sandbox_pod(target.pod_name)
                .filter(|pod| target.names(pod));
            if cached.is_some() {
                seen_in_pod_store = true;
            } else if pending_post_ip_fallback.is_none()
                && std::mem::replace(&mut seen_in_pod_store, false)
                && Instant::now() < deadline
            {
                pending_post_ip_fallback = Some(InFlightFallback::start(
                    self.resolve_pod_ip_for_readiness_uncached(sandbox_id, &target, deadline),
                    true,
                ));
            }
            let candidate_view = cached.as_ref().is_some_and(|pod| {
                !rejected_post_ip_view
                    .as_ref()
                    .is_some_and(|(rej, stamp)| Arc::ptr_eq(rej, pod) && stamp == &row_stamp)
                    && !handled_post_ip_view
                        .as_ref()
                        .is_some_and(|seen| Arc::ptr_eq(seen, pod))
                    && (pod_status_is_settled(pod)
                        || (row_change_pending && assigned_pod_node_name(pod).is_some()))
            });
            let mut resolved_new_endpoint = false;
            if let Some(pod) = cached.as_ref().filter(|_| candidate_view) {
                row_change_pending = false;
                let check = match timeout_at(
                    deadline,
                    self.inspect_active_execution_for_pod(sandbox_id, &target, pod),
                )
                .await
                {
                    Ok(res) => res?,
                    Err(_) => return Err(proxy_unreachable_timeout()),
                };
                if check.pod_uid_superseded {
                    return Err(superseded_during_proxy_wait_error(target.pod_name));
                }
                if check.execution_abandoned {
                    return Err(retired_during_proxy_wait_error(target.pod_name));
                }
                if !check.matches_generation {
                    rejected_post_ip_view = Some((Arc::clone(pod), row_stamp.clone()));
                } else if !pod_status_is_settled(pod) {
                    if let Some(current) = check.recorded_ip {
                        handled_post_ip_view = Some(Arc::clone(pod));
                        resolved_new_endpoint |=
                            current != (pod_ip, source) || check.recorded_uid != resolved_uid;
                        (pod_ip, source, resolved_uid) = (current.0, current.1, check.recorded_uid);
                    } else {
                        rejected_post_ip_view = Some((Arc::clone(pod), row_stamp));
                    }
                } else {
                    let mut pre_resolved = None;
                    if terminal_pod_phase(pod).is_some() {
                        if let Some(in_flight) = pending_post_ip_fallback.take() {
                            let resolved_in_flight = match timeout_at(
                                deadline,
                                in_flight.resolve(&mut fallback, self, sandbox_id, target.pod_name),
                            )
                            .await
                            {
                                Ok(res) => res?,
                                Err(_) => return Err(proxy_unreachable_timeout()),
                            };
                            if let Some(outcome) = resolved_in_flight
                                && (outcome.pod_uid_superseded
                                    || outcome.terminal_or_absent_retired
                                    || outcome.resolved.is_some())
                            {
                                pre_resolved = Some(outcome);
                            }
                        }
                    } else if self.pod_reflector_watch_healthy_at(Instant::now()) {
                        pending_post_ip_fallback = None;
                    }
                    let outcome = match pre_resolved {
                        Some(outcome) => outcome,
                        None => {
                            let outcome = match timeout_at(
                                deadline,
                                self.resolve_pod_ip_for_readiness(sandbox_id, &target, deadline),
                            )
                            .await
                            {
                                Ok(res) => res?,
                                Err(_) => return Err(proxy_unreachable_timeout()),
                            };
                            if outcome.used_uncached_get {
                                fallback.record_completed_at(Instant::now());
                            }
                            outcome
                        }
                    };
                    if outcome.pod_uid_superseded {
                        return Err(superseded_during_proxy_wait_error(target.pod_name));
                    }
                    if outcome.terminal_or_absent_retired {
                        return Err(retired_during_proxy_wait_error(target.pod_name));
                    }
                    if outcome.resolved.is_some()
                        || (outcome.cache_matches_execution
                            && terminal_pod_phase(pod).is_some()
                            && !outcome.execution_in_flight)
                    {
                        handled_post_ip_view = cached.clone();
                    } else if (!outcome.cache_matches_execution || outcome.execution_in_flight)
                        && !outcome.deadline_elapsed
                    {
                        rejected_post_ip_view = Some((Arc::clone(pod), row_stamp));
                    }
                    if let Some(current) = outcome.resolved {
                        resolved_new_endpoint |=
                            current != (pod_ip, source) || outcome.resolved_uid != resolved_uid;
                        (pod_ip, source, resolved_uid) =
                            (current.0, current.1, outcome.resolved_uid);
                    }
                }
            }
            if resolved_new_endpoint {
                pending_post_ip_fallback = None;
                continue;
            }
            let now = Instant::now();
            if pending_post_ip_fallback.is_none() && now < deadline && fallback.due(now, self) {
                pending_post_ip_fallback = Some(InFlightFallback::start(
                    self.resolve_pod_ip_for_readiness_uncached(sandbox_id, &target, deadline),
                    false,
                ));
            }
            let poll_sleep =
                sleep_until((now + start_wait_poll_interval(started_at)).min(deadline));
            tokio::select! {
                (outcome, confirms_loss) = poll_in_flight_fallback(&mut pending_post_ip_fallback), if pending_post_ip_fallback.is_some() => {
                    if let Some(outcome) = InFlightFallback::finish(
                        outcome,
                        confirms_loss,
                        &mut fallback,
                        self,
                        sandbox_id,
                        target.pod_name,
                    )? {
                        if outcome.pod_uid_superseded {
                            return Err(superseded_during_proxy_wait_error(target.pod_name));
                        }
                        if outcome.terminal_or_absent_retired {
                            return Err(retired_during_proxy_wait_error(target.pod_name));
                        }
                        if let Some(current) = outcome.resolved {
                            (pod_ip, source, resolved_uid) =
                                (current.0, current.1, outcome.resolved_uid);
                        }
                    }
                }
                () = poll_sleep => {}
            }
            if Instant::now() >= deadline {
                return Err(proxy_unreachable_timeout());
            }
        }
    }

    pub(super) async fn poll_until_command_ready(
        &self,
        sandbox_id: SandboxId,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        ready_timeout: Duration,
        pod: CommandReadyPod<'_>,
        // Sandbox template, threaded through only so the
        // `sandbox.api.create.command_ready_timeout` counter can carry a
        // template tag when the create deadline fires. Ignored for non-create
        // operations.
        template: Option<&str>,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let started_at = Instant::now();
        let deadline = started_at + ready_timeout;
        let mut poll_backoff = CommandReadyPollBackoff::new(started_at, deadline);
        let mut node_api_connected_at = None;
        // Tracks whether a pod IP ever resolved, so a deadline that fires
        // with no IP is attributed to `pod_never_ready` rather than to a
        // proxy/guest phase the wait never reached.
        let mut resolved_ip_seen = false;
        // Tracks whether any guest-ready probe returned `Code::Unavailable`.
        // Cascade-amplifier signal: a deadline that fires after a probe
        // genuinely returned gRPC `Unavailable` is the node-mgr-brownout
        // symptom, and the timeout message names node-mgr so callers can
        // classify the failure and route cleanup to the reconciler instead
        // of spawning more cleanup load. TCP connect failures and per-probe
        // deadline misses do NOT set this — they have other dominant causes
        // (e.g. a wedged sbx-mgr) and naming node-mgr for them sends the
        // on-call chasing the wrong service.
        let mut saw_unavailable_probe = false;
        let (
            selected_route_identity,
            mut selected_route_pod_ip,
            pre_resolved_pod_ip,
            resolve_legacy,
        ) = match pod {
            CommandReadyPod::PreResolved(pod_ip) => (None, None, Some(pod_ip), false),
            CommandReadyPod::ResolveLegacy => (None, None, None, true),
            CommandReadyPod::SelectedRoute {
                route_identity,
                pod_ip,
            } => (Some(route_identity), pod_ip, None, false),
        };
        let mut connection: Option<(PodIp, RuntimeClient)> = None;
        let mut selected_pod_resolution_error = None;

        loop {
            if let Some(route_identity) = selected_route_identity
                && selected_route_pod_ip.is_none()
            {
                match self.selected_wake_pod_ip(sandbox_id, route_identity).await {
                    Ok(pod_ip) => {
                        selected_route_pod_ip = pod_ip;
                        selected_pod_resolution_error = None;
                    }
                    Err(SubstrateError::Timeout { message, .. }) => {
                        // A live route remains selected; retry transient
                        // apiserver resolution at the slow cadence through the
                        // caller's readiness deadline. Identity/UID changes
                        // stay terminal and never enter this arm.
                        selected_pod_resolution_error = Some(message);
                    }
                    Err(error) => return Err(error),
                }
            }
            let resolved_pod_ip = if resolve_legacy {
                self.resolve_pod_ip(sandbox_id).await?
            } else {
                selected_route_pod_ip.or(pre_resolved_pod_ip)
            };
            if let Some(pod_ip) = resolved_pod_ip {
                resolved_ip_seen = true;
                if connection
                    .as_ref()
                    .is_none_or(|(connected_ip, _)| *connected_ip != pod_ip)
                {
                    let connect_started_at = Instant::now();
                    let client = Self::pod_worker_client(
                        pod_ip,
                        self.k8s.sandbox_proxy_port,
                        START_WAIT_CONNECT_TIMEOUT.min(remaining_ready_window(deadline)),
                    )
                    .await;
                    if client.is_some() {
                        record_api_wait_ready_latency(
                            sandbox_id,
                            metric_identity,
                            "node_api_connect",
                            "success",
                            connect_started_at,
                            None,
                        );
                    }
                    connection = client.map(|client| (pod_ip, client));
                    if connection.is_none() && selected_route_identity.is_some() {
                        // Before retrying a failed authenticated connection,
                        // revalidate the live Pod UID/IP and selected route.
                        selected_route_pod_ip = None;
                    }
                }
                // A failed TCP connect to the sandbox pod proxy is NOT a
                // node-mgr signal: the proxy can be down because the shim's
                // LaunchSandbox is hung on a wedged sbx-mgr just as easily
                // as on a stuck node-mgr. The deadline classification below
                // reports it as `proxy_unreachable` (via
                // `node_api_connected_at` staying `None`) without naming a
                // service, and the create path's defer-cleanup decision
                // keys off the `Timeout` variant itself.
                let mut reset_connection = false;
                if let Some((_, client)) = connection.as_mut() {
                    if node_api_connected_at.is_none() {
                        node_api_connected_at = Some(Instant::now());
                        record_api_wait_ready_latency(
                            sandbox_id,
                            metric_identity,
                            "node_api_first_connect",
                            "success",
                            started_at,
                            None,
                        );
                        record_api_wait_ready_latency(
                            sandbox_id,
                            metric_identity,
                            "placement",
                            "success",
                            started_at,
                            None,
                        );
                    }

                    let probe_started_at = Instant::now();
                    // No probe outlives the wait's deadline, so a short wait,
                    // such as a Native create's, ends when it says.
                    let probe_timeout =
                        START_WAIT_COMMAND_PROBE_TIMEOUT.min(remaining_ready_window(deadline));
                    let probe = client.spaced_wait_ready(wait_ready_lifecycle_request(
                        SpacedWaitReadyRequest {
                            sandbox_id: sandbox_id.to_string(),
                            timeout: Some(duration_to_proto(probe_timeout)),
                        },
                    ));
                    match timeout(probe_timeout, probe).await {
                        Ok(Ok(_)) => {
                            record_api_wait_ready_latency(
                                sandbox_id,
                                metric_identity,
                                "command_probe",
                                "success",
                                probe_started_at,
                                None,
                            );
                            record_api_wait_ready_latency(
                                sandbox_id,
                                metric_identity,
                                "total",
                                "success",
                                started_at,
                                None,
                            );
                            if let Some(node_api_connected_at) = node_api_connected_at {
                                record_api_wait_ready_latency(
                                    sandbox_id,
                                    metric_identity,
                                    "command_ready_after_placement",
                                    "success",
                                    node_api_connected_at,
                                    Some("placement"),
                                );
                            }
                            if let (Some(route_identity), Some(selected_pod_ip)) =
                                (selected_route_identity, selected_route_pod_ip)
                            {
                                return self
                                    .selected_wake_binding(
                                        sandbox_id,
                                        route_identity,
                                        selected_pod_ip,
                                    )
                                    .await;
                            }
                            let execution = self
                                .store
                                .active_execution_status(sandbox_id)
                                .await?
                                .ok_or_else(|| SubstrateError::Unavailable {
                                message: "sandbox has no active Kubernetes execution".to_string(),
                            })?;
                            return Ok(ExecutionBinding {
                                execution_object_id: execution.pod_name,
                                runtime_endpoint: Self::endpoint_for_pod_ip(
                                    pod_ip,
                                    self.k8s.sandbox_proxy_port,
                                )?,
                                execution_generation: execution_generation_to_u64(
                                    execution.generation,
                                )?,
                            });
                        }
                        Ok(Err(err))
                            if err.code() == Code::FailedPrecondition
                                && (err.message().contains("not started")
                                    || err.message().contains("sandbox is starting")
                                    || err.message().contains("wait for start to complete")) => {}
                        Ok(Err(err)) if err.code() == Code::Unavailable => {
                            saw_unavailable_probe = true;
                            reset_connection = true;
                        }
                        Ok(Err(err)) => {
                            record_api_wait_ready_error(
                                sandbox_id,
                                metric_identity,
                                "command_probe",
                                "grpc_error",
                                grpc_code_tag(err.code()),
                            );
                            return Err(SubstrateError::Failed {
                                message: format!("sandbox failed before start completed: {err}"),
                            });
                        }
                        Err(_) => {
                            // Per-probe deadline elapsed: connected to the
                            // proxy but `spaced_wait_ready` didn't respond
                            // in time. Not a node-mgr signal — the probe
                            // hung rather than returning `Unavailable` — so
                            // the eventual deadline reports `guest_not_ready`
                            // without naming a service.
                            reset_connection = true;
                        }
                    }
                }
                if reset_connection {
                    // Tonic channels reconnect internally. Once a probe fails,
                    // retaining the channel could silently reconnect to a
                    // reassigned Pod IP without rechecking the immutable UID.
                    // Rebuild legacy/pre-resolved transports too, matching the
                    // pre-cache behavior instead of burning the full deadline
                    // on one half-open channel.
                    connection = None;
                    if selected_route_identity.is_some() {
                        selected_route_pod_ip = None;
                    }
                }
            }

            if Instant::now() >= deadline {
                if metric_identity.op == "create" {
                    crate::metrics::sandbox::api::create_command_ready_timeout(
                        sandbox_id, template,
                    );
                }
                let phase = if !resolved_ip_seen {
                    WaitReadyTimeoutPhase::PodNeverReady
                } else if node_api_connected_at.is_none() {
                    WaitReadyTimeoutPhase::ProxyUnreachable
                } else {
                    WaitReadyTimeoutPhase::GuestNotReady
                };
                let mut error = command_ready_timeout_error(phase, saw_unavailable_probe);
                if let (SubstrateError::Timeout { message, .. }, Some(resolution_error)) =
                    (&mut error, selected_pod_resolution_error.as_deref())
                {
                    message.push_str(&format!("; last pod resolution error: {resolution_error}"));
                }
                return Err(error);
            }

            let delay = poll_backoff.next_delay();
            // Recompute after connection/probe handling: either path may have
            // invalidated a previously selected IP in this same iteration.
            // The slow clamp must apply immediately or a brownout produces an
            // uncached Pod GET and several store reads every 250 ms.
            let waiting_for_selected_pod_ip =
                selected_route_identity.is_some() && selected_route_pod_ip.is_none();
            let delay = if waiting_for_selected_pod_ip {
                delay
                    .max(START_WAIT_SLOW_POLL_INTERVAL)
                    .min(deadline.saturating_duration_since(Instant::now()))
            } else {
                delay
            };
            sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod confirm_tests;
#[cfg(test)]
mod fallback_race_tests;
#[cfg(test)]
mod tests;
