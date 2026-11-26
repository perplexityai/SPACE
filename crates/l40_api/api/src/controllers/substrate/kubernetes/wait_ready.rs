use super::store::PlacementRetryDisposition;
use super::support::*;
use super::*;
use pplx_space_api_route::wait_ready_lifecycle_request;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::sleep_until;

/// The subtractive jitter on [`SubstrateBudgets::readiness_backstop`], as a
/// fraction of it, so the backstop reads of creates that started together
/// spread out.
const READINESS_BACKSTOP_JITTER: f64 = 0.2;

/// When a readiness wait reads its pod from the apiserver while the shared
/// reflector does not show the pod settled: about every
/// [`SubstrateBudgets::readiness_backstop`], jittered, and once more just
/// before the deadline, early enough for that read to finish within it.
/// While the reflector has delivered nothing at all for
/// [`SubstrateBudgets::readiness_quiet_reflector`] (a stalled watch), also
/// at most once per that interval. Reflector lag costs a wait at most one
/// backstop interval, a stalled watch about one quiet interval past its
/// recovery, and the apiserver at most one read per waiting create per
/// backstop interval while the reflector is delivering.
pub(super) struct ReadinessBackstop {
    next: Instant,
    last: Instant,
    interval: Duration,
    quiet: Duration,
    last_read: Instant,
    exhausted: bool,
}

impl ReadinessBackstop {
    fn new(budgets: &SubstrateBudgets, deadline: Instant) -> Self {
        let now = Instant::now();
        let last = deadline
            .checked_sub(budgets.readiness_pod_get + budgets.reflector_wait)
            .unwrap_or(now)
            .max(now);
        let interval = budgets.readiness_backstop;
        Self {
            next: (now + jittered_backstop(interval)).min(last),
            last,
            interval,
            quiet: budgets.readiness_quiet_reflector,
            last_read: now,
            exhausted: false,
        }
    }

    /// When a read may next be due, given when the reflector last delivered
    /// anything, or `None` once the last read has run.
    fn next_read(&self, last_delivery: Instant) -> Option<Instant> {
        let quiet_read = last_delivery.max(self.last_read) + self.quiet;
        (!self.exhausted).then_some(self.next.min(quiet_read))
    }

    fn due(&self, now: Instant, last_delivery: Instant) -> bool {
        let reflector_quiet = now.saturating_duration_since(last_delivery) >= self.quiet
            && now.saturating_duration_since(self.last_read) >= self.quiet;
        !self.exhausted && (now >= self.next || (now < self.last && reflector_quiet))
    }

    /// Count a read, starting the next interval; the one at or after the last
    /// read's time is the last.
    fn read(&mut self, now: Instant) {
        self.last_read = now;
        if now >= self.last {
            self.exhausted = true;
            return;
        }
        self.next = (now + jittered_backstop(self.interval)).min(self.last);
    }
}

fn jittered_backstop(interval: Duration) -> Duration {
    interval.mul_f64(1.0 - rand::random::<f64>() * READINESS_BACKSTOP_JITTER)
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
    pub(super) async fn worker_client_for_pod_ip(
        &self,
        pod_ip: PodIp,
    ) -> Result<RuntimeClient, ControllerError> {
        let endpoint = Self::endpoint_for_pod_ip(pod_ip, self.k8s.sandbox_proxy_port)
            .map_err(ControllerError::from)?;
        connect_worker(endpoint).await
    }

    /// [`Self::worker_client_for_pod_ip`] on the channel kept for `pod`'s
    /// instance while its connection is open; see
    /// [`super::pod_worker_channels`]. A failed dial fails the same way.
    pub(super) async fn kept_worker_client(
        &self,
        pod: &ResolvedPodInstance,
    ) -> Result<RuntimeClient, ControllerError> {
        let port = self.k8s.sandbox_proxy_port;
        let key = super::pod_worker_channels::PodWorkerKey {
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
            && !activity.allows_implicit_wake(&sandbox.auto_lifecycle.0)
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
    /// readiness resolution is. While it does not, `backstop` paces the
    /// apiserver reads that cover reflector lag and a stalled watch. A
    /// reflector that has not finished its first list is waited for, within
    /// the same reads.
    async fn reflected_pod_ip(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        backstop: &mut ReadinessBackstop,
        observed: &tokio::sync::watch::Sender<Option<SandboxPodView>>,
    ) -> Result<Option<((PodIp, PodIpSource), PodIpLearned)>, SubstrateError> {
        // Subscribed before the first store read, so no update between that
        // read and the wait is missed.
        let mut updates = self.pod_events.subscribe();
        let started_at = Instant::now();
        let mut learned = PodIpLearned::Cached;
        let mut last_view: Option<Arc<SandboxPodView>> = None;
        let mut placed_on: Option<String> = None;
        let mut recheck_at: Option<Instant> = None;
        loop {
            let now = Instant::now();
            let view = self
                .lookup_sandbox_pod(target.pod_name)
                .filter(|pod| target.names(pod));
            let settled = view.as_deref().is_some_and(pod_status_is_settled);
            // The store replaces a pod's view on every update, so an
            // unchanged `Arc` is a view this wait has already handled.
            let fresh = view.as_ref().is_some_and(|pod| {
                last_view
                    .as_ref()
                    .is_none_or(|seen| !Arc::ptr_eq(seen, pod))
            });
            let recheck = settled && recheck_at.is_some_and(|at| now >= at);
            if let Some(pod) = view.filter(|_| fresh || recheck) {
                last_view = Some(Arc::clone(&pod));
                recheck_at = None;
                if settled {
                    if let Some(resolved) = self
                        .resolve_pod_ip_for_readiness(sandbox_id, target, deadline)
                        .await?
                    {
                        return Ok(Some((resolved, learned)));
                    }
                    // The execution row does not describe the settled pod
                    // yet, as for a create joining one whose owner has not
                    // recorded its pod. Ask again at the start-wait cadence:
                    // a settled pod resolves from the store and Postgres
                    // alone. A terminal pod is verified once per view.
                    if terminal_pod_phase(&pod).is_none() {
                        recheck_at = Some(now + start_wait_poll_interval(started_at));
                    }
                }
                if fresh {
                    if let Some(node_name) = assigned_pod_node_name(&pod)
                        && placed_on.as_deref() != Some(node_name)
                    {
                        self.remember_pod_node_placement(
                            sandbox_id,
                            &pod,
                            PlacementRetryDisposition::RetryDuplicatesExecution,
                        )
                        .await?;
                        placed_on = Some(node_name.to_owned());
                    }
                    observed.send_replace(Some(SandboxPodView::clone(&pod)));
                }
            }
            if !settled && backstop.due(Instant::now(), self.pod_events.last_delivery()) {
                backstop.read(Instant::now());
                if let Some(resolved) = self
                    .resolve_pod_ip_for_readiness(sandbox_id, target, deadline)
                    .await?
                {
                    return Ok(Some((resolved, PodIpLearned::Backstop)));
                }
            }
            learned = PodIpLearned::Reflector;
            // Reads are only for a pod the store does not show settled; a
            // settled one is asked again at `recheck_at`.
            let next_read = if settled {
                None
            } else {
                backstop.next_read(self.pod_events.last_delivery())
            };
            let wake_at = [next_read, recheck_at]
                .into_iter()
                .flatten()
                .fold(deadline, Instant::min);
            if Instant::now() >= deadline {
                return Ok(None);
            }
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
                continue;
            }
            loop {
                match timeout_at(wake_at, updates.recv()).await {
                    Ok(Ok(pod_name)) if pod_name != target.pod_name => continue,
                    // An update for the pod, or one this wait may have missed.
                    Ok(Ok(_) | Err(RecvError::Lagged(_))) => break,
                    // The reflector has stopped: only the backstop reads
                    // remain.
                    Ok(Err(RecvError::Closed)) => {
                        sleep_until(wake_at).await;
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
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
        backstop: &mut ReadinessBackstop,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        let PodProxyWaitTarget {
            pod_name,
            pod_uid,
            generation,
        } = *target;
        let started_at = Instant::now();
        // The pod's node usually reports the address before Kubernetes does;
        // whichever answers first binds the execution.
        let (observed, observed_by_node) = tokio::sync::watch::channel(None);
        // Both waits are large futures; keep them off the caller's stack.
        let reflected = Box::pin(
            self.reflected_pod_ip(sandbox_id, target, deadline, backstop, &observed)
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
        let mut backstop = ReadinessBackstop::new(&self.budgets, deadline);

        let pod_ip_assignment = self
            .wait_for_pod_ip_assignment(
                metric_identity,
                sandbox_id,
                &target,
                deadline,
                &mut backstop,
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

            // An address that does not serve the target is re-resolved. The
            // recorded address for the target's UID is kept until kubelet
            // publishes another, so after a runtime sandbox is recreated under
            // the same UID the wait binds its new address at kubelet's pace,
            // as without a node report. Until the reflector shows the pod
            // settled, only the backstop reads it from the apiserver.
            let settled = self
                .lookup_sandbox_pod(target.pod_name)
                .is_some_and(|pod| target.names(&pod) && pod_status_is_settled(&pod));
            if settled || backstop.due(Instant::now(), self.pod_events.last_delivery()) {
                if !settled {
                    backstop.read(Instant::now());
                }
                if let Some(current) = self
                    .resolve_pod_ip_for_readiness(sandbox_id, &target, deadline)
                    .await?
                {
                    (pod_ip, source) = current;
                }
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

    pub(super) async fn poll_connected_client_until_command_ready(
        &self,
        client: &mut NodeApiClient,
        sandbox_id: SandboxId,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        ready_timeout: Duration,
        binding: ExecutionBinding,
        template: Option<&str>,
    ) -> Result<ExecutionBinding, SubstrateError> {
        let probes = match Self::probe_connected_client_until_command_ready(
            client,
            sandbox_id,
            metric_identity,
            ready_timeout,
        )
        .await?
        {
            ConnectedCommandReadiness::Ready => return Ok(binding),
            ConnectedCommandReadiness::TimedOut(probes) => probes,
        };
        if metric_identity.op == "create" {
            crate::metrics::sandbox::api::create_command_ready_timeout(sandbox_id, template);
        }
        // The caller reached the node for the adoption's setup and Wake, so
        // the timeout names the guest's phase. A probe that could not reach
        // the node also ends in `DeadlineExceeded`; the warning carries what
        // the probes returned. Keep it out of the message: a transport
        // signature there would make the timeout reclaimable and spend
        // another readiness budget on the same pod.
        tracing::warn!(
            sandbox_id = %sandbox_id,
            deadline_exceeded_probes = probes.deadline_exceeded,
            last_probe_code = ?probes.last_status.as_ref().map(tonic::Status::code),
            last_probe_message = probes.last_status.as_ref().map(tonic::Status::message),
            "pooled sandbox command readiness wait timed out"
        );
        Err(command_ready_timeout_error(
            WaitReadyTimeoutPhase::GuestNotReady,
            probes.saw_unavailable,
        ))
    }

    /// Probes the connected node until the guest reports command readiness
    /// or `ready_timeout` passes. Starts no probe once the deadline has
    /// passed: one with no budget left would end before it reached the node.
    pub(super) async fn probe_connected_client_until_command_ready(
        client: &mut NodeApiClient,
        sandbox_id: SandboxId,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        ready_timeout: Duration,
    ) -> Result<ConnectedCommandReadiness, SubstrateError> {
        let started_at = Instant::now();
        let deadline = started_at + ready_timeout;
        let mut poll_backoff = CommandReadyPollBackoff::new(started_at, deadline);
        let node_api_connected_at = Instant::now();
        let mut probes = SwallowedCommandProbes::default();

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

        loop {
            let probe_started_at = Instant::now();
            if probe_started_at >= deadline {
                return Ok(ConnectedCommandReadiness::TimedOut(probes));
            }

            let probe_timeout = (START_WAIT_COMMAND_PROBE_TIMEOUT
                + START_WAIT_COMMAND_PROBE_CLIENT_MARGIN)
                .min(deadline.saturating_duration_since(probe_started_at));
            let mut worker_client = RetryingWorkerApiClient::new(client);
            let probe = worker_client.spaced_wait_ready_with_metadata(
                wait_ready_lifecycle_request(SpacedWaitReadyRequest {
                    sandbox_id: sandbox_id.to_string(),
                    timeout: Some(duration_to_proto(START_WAIT_COMMAND_PROBE_TIMEOUT)),
                }),
                probe_timeout,
            );
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
                    record_api_wait_ready_latency(
                        sandbox_id,
                        metric_identity,
                        "command_ready_after_placement",
                        "success",
                        node_api_connected_at,
                        Some("placement"),
                    );
                    return Ok(ConnectedCommandReadiness::Ready);
                }
                Ok(Err(err))
                    if err.code() == Code::FailedPrecondition
                        && (err.message().contains("not started")
                            || err.message().contains("sandbox is starting")
                            || err.message().contains("wait for start to complete")) =>
                {
                    probes.last_status = Some(err);
                }
                Ok(Err(err)) if err.code() == Code::Unavailable => {
                    probes.saw_unavailable = true;
                    probes.last_status = Some(err);
                }
                // Not ready yet, like the outer timeout below: the readiness
                // deadline, not one probe, decides when the guest has failed
                // to start. Three sources end a probe this way: the client's
                // per-probe deadline, which ends a probe for a guest that is
                // not ready because the node keeps waiting for it; the
                // retrying client running out of its budget while the node
                // refuses connections; and the node's own expired wait.
                Ok(Err(err)) if err.code() == Code::DeadlineExceeded => {
                    probes.deadline_exceeded += 1;
                    probes.last_status = Some(err);
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
                // The same bound as the client's deadline, which usually
                // fires first.
                Err(_) => probes.deadline_exceeded += 1,
            }

            sleep(poll_backoff.next_delay()).await;
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

/// How [`KubernetesSubstrate::probe_connected_client_until_command_ready`]
/// ended.
pub(super) enum ConnectedCommandReadiness {
    Ready,
    /// The readiness deadline passed first.
    TimedOut(SwallowedCommandProbes),
}

/// What the readiness probes the poll swallowed returned, for the warning at
/// the deadline. A node that stops answering and a guest that never reports
/// both end every probe at its deadline, which the timeout's phase cannot
/// tell apart.
#[derive(Debug, Default)]
pub(super) struct SwallowedCommandProbes {
    /// See `poll_until_command_ready`: set only by a probe that genuinely
    /// returned gRPC `Unavailable`, so the timeout message names node-mgr
    /// only when a probe actually implicated it.
    pub(super) saw_unavailable: bool,
    /// Probes that ended in `DeadlineExceeded`, counting those the outer
    /// timeout ended without a status.
    pub(super) deadline_exceeded: usize,
    /// The latest status a probe returned.
    pub(super) last_status: Option<tonic::Status>,
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

#[cfg(test)]
mod node_reported_address_tests {
    use super::*;
    use pplx_space_sbx_mgr_client::proto::SandboxInfo;

    fn info(sandbox_id: SandboxId, generation: i64, pod_uid: &str, pod_ip: &str) -> SandboxInfo {
        SandboxInfo {
            sandbox_id: sandbox_id.to_string(),
            execution_generation: Some(generation),
            pod_uid: Some(pod_uid.to_owned()),
            external_address: Some(pod_ip.to_owned()),
            ..Default::default()
        }
    }

    #[test]
    fn only_the_observed_executions_address_is_taken() {
        let sandbox_id = SandboxId::new();
        let other_sandbox = SandboxId::new();
        let reported = vec![
            info(other_sandbox, 1, "pod-a", "10.0.0.1"),
            info(sandbox_id, 1, "pod-old", "10.0.0.2"),
            info(sandbox_id, 1, "pod-a", "10.0.0.3"),
        ];
        let taken = |reported, generation, pod_uid| {
            node_reported_address(reported, sandbox_id, generation, pod_uid)
        };

        assert_eq!(
            taken(reported.clone(), 1, "pod-a"),
            Some(PodIp::parse("10.0.0.3").unwrap())
        );
        assert_eq!(taken(reported.clone(), 1, "pod-new"), None);
        assert_eq!(taken(reported, 2, "pod-a"), None);
        assert_eq!(
            taken(vec![info(sandbox_id, 1, "pod-a", "not-an-ip")], 1, "pod-a"),
            None
        );
    }
    /// The readiness probe accepts only a node serving the target execution
    /// at the probed address. For a kubelet-published address, fields an older
    /// node omits are not compared; a node-reported one needs them all.
    #[test]
    fn a_probe_accepts_only_the_target_served_at_its_address() {
        let sandbox_id = SandboxId::new();
        let probed = PodIp::parse("10.0.0.3").unwrap();
        let target = PodProxyWaitTarget {
            pod_name: "pod",
            pod_uid: Some("pod-a"),
            generation: 1,
        };
        let degraded = SandboxInfo {
            sandbox_id: sandbox_id.to_string(),
            ..Default::default()
        };
        for source in [PodIpSource::Published, PodIpSource::NodeReported] {
            let serves =
                |info: SandboxInfo| serves_target(vec![info], sandbox_id, &target, probed, source);
            assert!(serves(info(sandbox_id, 1, "pod-a", "10.0.0.3")));
            // A recreated runtime sandbox's node reports its new allocation.
            assert!(!serves(info(sandbox_id, 1, "pod-a", "10.0.0.4")));
            assert!(!serves(info(sandbox_id, 1, "pod-b", "10.0.0.3")));
            assert!(!serves(info(sandbox_id, 2, "pod-a", "10.0.0.3")));
            assert!(!serves(info(SandboxId::new(), 1, "pod-a", "10.0.0.3")));
            assert!(!serves_target(
                Vec::new(),
                sandbox_id,
                &target,
                probed,
                source
            ));
            assert_eq!(serves(degraded.clone()), source == PodIpSource::Published);
        }
    }

    /// A teardown acts on a runtime its worker names by identity, skips it
    /// only when the worker reports another execution, and does not read
    /// missing fields or a moved address as either.
    #[test]
    fn a_runtime_answer_skips_only_a_worker_that_reports_another_runtime() {
        let sandbox_id = SandboxId::new();
        let probed = PodIp::parse("10.0.0.3").unwrap();
        let target = PodProxyWaitTarget {
            pod_name: "pod",
            pod_uid: Some("pod-a"),
            generation: 1,
        };
        let answer = |info: SandboxInfo| runtime_answer(vec![info], sandbox_id, &target, probed);
        assert_eq!(
            answer(info(sandbox_id, 1, "pod-a", "10.0.0.3")),
            RuntimeAnswer::Serves
        );
        // A restarted sandbox manager no longer reports the address.
        let restarted = SandboxInfo {
            external_address: None,
            ..info(sandbox_id, 1, "pod-a", "10.0.0.3")
        };
        assert_eq!(answer(restarted), RuntimeAnswer::Serves);
        for elsewhere in [
            info(sandbox_id, 1, "pod-b", "10.0.0.3"),
            info(sandbox_id, 2, "pod-a", "10.0.0.3"),
            info(SandboxId::new(), 1, "pod-a", "10.0.0.3"),
        ] {
            assert_eq!(answer(elsewhere), RuntimeAnswer::Elsewhere);
        }
        // The target's execution at another address is still a runtime to
        // clean up, so the teardown waits for an address it can confirm.
        let moved = info(sandbox_id, 1, "pod-a", "10.0.0.4");
        let unidentified_moved = SandboxInfo {
            pod_uid: None,
            ..info(sandbox_id, 1, "pod-a", "10.0.0.4")
        };
        let degraded = SandboxInfo {
            sandbox_id: sandbox_id.to_string(),
            ..Default::default()
        };
        for unconfirmed in [moved, unidentified_moved, degraded] {
            assert_eq!(answer(unconfirmed), RuntimeAnswer::Unconfirmed);
        }
    }
}
