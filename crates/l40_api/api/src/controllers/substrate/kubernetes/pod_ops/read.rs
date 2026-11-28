//! Pod read paths: cached and uncached sandbox pod reads, pod IP resolution,
//! and remembering the node a resolved pod landed on.

use super::*;
use crate::controllers::substrate::PooledExecutionClaim;
use crate::controllers::substrate::kubernetes::wait_ready::PodProxyWaitTarget;

const SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE: Duration = Duration::from_secs(180);

enum PodGet {
    Observed(Option<Arc<SandboxPodView>>),
    DeadlineElapsed,
}

/// How [`KubernetesSubstrate::lookup_cached_pod`] decides whether a cached
/// [`SandboxPodView`] is authoritative without an apiserver `GET`.
#[derive(Clone, Copy)]
enum CacheTrustPolicy {
    /// Post-watch readiness fallback polling (`poll_until_pod_ip_assigned`):
    /// only settled pods (`Running`+IP, `Unschedulable`, `Succeeded`, `Failed`)
    /// short-circuit from cache. After the per-pod watch ends or errors, an
    /// unsettled `Pending` entry in `pod_store` must fall through to an
    /// uncached `GET` once `reflector_wait` elapses rather than suppressing
    /// fallback polling.
    SettledOnly,
    /// Resolution, initial readiness check, and status reads: settled pods
    /// (`Running`+IP, `Unschedulable`, `Succeeded`, `Failed`) are accepted even
    /// across a watch fault because `resolve_pod_instance_inner` always
    /// re-verifies terminal phases with an uncached `GET` before retiring an
    /// execution and `Running` pods are verified when dialed; pre-binding
    /// `Pending` pods without an IP are trusted only while the reflector watch
    /// is healthy.
    SettledOrHealthyWatch,
    /// Warm-pool claim adoption (`read_pooled_claim_pod`): cached pods are
    /// trusted only while the reflector watch is healthy, because claim
    /// adoption hands a cached `Running`+IP pod directly to a caller without a
    /// follow-up uncached `GET` and must not adopt a pod that died during a
    /// watch disconnect.
    HealthyWatchOnly,
}

#[derive(Debug, Clone)]
pub(in crate::controllers::substrate::kubernetes) enum PooledClaimPodObservation {
    /// Authoritative cached or uncached view of the claimed pod (`None` when an
    /// uncached apiserver `GET` confirmed `404`).
    Observed(Option<Arc<SandboxPodView>>),
    /// A healthy reflector still holds a pre-`Running` snapshot of the claimed
    /// pod's UID after `reflector_wait`; the caller preserves the warm slot
    /// rather than retiring it or issuing an uncached `GET`.
    ReflectorLagging,
}

fn pooled_claim_pod_reflector_lagging(claim: &PooledExecutionClaim, pod: &SandboxPodView) -> bool {
    let Some(expected_uid) = claim.pod_uid.as_ref().map(PodUid::as_str) else {
        return false;
    };
    // Warm-pool slots only reach `ready` after `mark_slot_ready` observed the
    // pod `Running` with `claim.pod_ip` under `expected_uid`, and a `Running`
    // pod never transitions back to `Pending` under the same UID. Any
    // non-deleting, non-terminal cached snapshot of `expected_uid` that isn't
    // usable yet (including an earlier `Pending` + `Unschedulable` scheduling
    // cycle before capacity freed up) is therefore pre-`Running` reflector lag.
    !pod.is_deleting()
        && matches!(pod.phase(), None | Some("Pending"))
        && pod.uid() == Some(expected_uid)
        && pod.pod_ip().is_none_or(|ip| ip == claim.pod_ip.as_str())
        && !pooled_claim_pod_is_usable(claim, Some(pod))
}

fn pooled_claim_pod_cache_is_authoritative(
    claim: &PooledExecutionClaim,
    pod: &SandboxPodView,
) -> bool {
    let Some(expected_uid) = claim.pod_uid.as_ref().map(PodUid::as_str) else {
        return false;
    };
    pod.uid() == Some(expected_uid) && !pooled_claim_pod_reflector_lagging(claim, pod)
}

pub(super) fn terminal_cleanup_phase_after_uncached_check(
    cached_phase: TerminalPodPhase,
    authoritative_pod: Option<&SandboxPodView>,
) -> Option<TerminalPodPhase> {
    match authoritative_pod {
        Some(pod) => terminal_pod_phase(pod),
        None => Some(cached_phase),
    }
}

pub(super) fn succeeded_pod_publication_grace_remaining(
    pod: &SandboxPodView,
    now: jiff::Timestamp,
) -> Option<Duration> {
    if terminal_pod_phase(pod) != Some(TerminalPodPhase::Succeeded) {
        return None;
    }
    let finished_at = pod
        .container_statuses()
        .all()
        .filter_map(|state| state.terminated.as_ref()?.finished_at)
        .max()?;
    let grace = jiff::SignedDuration::from_secs(
        i64::try_from(SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE.as_secs()).ok()?,
    );
    let deadline = finished_at.checked_add(grace).ok()?;
    let remaining = deadline.duration_since(now);
    Some(
        Duration::try_from(remaining)
            .ok()?
            .min(SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE),
    )
}

impl KubernetesSubstrate {
    /// The cached view of `pod_name`, shared with the store rather than
    /// copied: the `Arc` is a refcount bump, not a deep clone.
    pub fn lookup_sandbox_pod(&self, pod_name: &str) -> Option<Arc<SandboxPodView>> {
        let key = ObjectRef::<SandboxPodView>::new(pod_name).within(&self.k8s.sandbox_namespace);
        self.pod_store.get(&key)
    }

    /// Read a sandbox pod directly from the apiserver, bypassing the
    /// reflector cache. Use on paths that must verify cached terminal state
    /// before deleting a carrier pod.
    pub async fn read_sandbox_pod_uncached(
        &self,
        pod_name: &str,
    ) -> Result<Option<Arc<SandboxPodView>>, KubeError> {
        fetch_sandbox_pod_uncached(&self.k8s, pod_name).await
    }

    /// Like [`Self::read_sandbox_pod_uncached`], but in `namespace` rather than
    /// the configured sandbox namespace. For callers proving a pod absent from
    /// an execution row's recorded `pod_namespace`: reading a pod tracked in
    /// another namespace from the configured one would 404 and read as absent.
    pub async fn read_sandbox_pod_uncached_in_namespace(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Option<Arc<SandboxPodView>>, KubeError> {
        fetch_sandbox_pod_uncached_in(&self.k8s, namespace, pod_name).await
    }

    /// Reads a sandbox pod with cache-first, event-wait, kube-apiserver-
    /// fallback semantics.
    ///
    /// 1. **Authoritative cache hit** (settled pod, or `Pending` while the
    ///    sandbox-pod reflector has completed its initial sync and its watch
    ///    isn't failing or mid-re-list) → return cached immediately with no
    ///    wait and no apiserver `GET`.
    /// 2. **Cache miss on a healthy reflector** → wait up to
    ///    `SubstrateBudgets::reflector_wait` on the shared reflector event
    ///    stream, aborting early if the watch faults, and return cached as soon
    ///    as the entry becomes authoritative.
    /// 3. **Cache miss after the event budget** or **degraded/unready
    ///    reflector** → fall through to a single direct `GET /pods/{name}`.
    ///
    /// A quiet watch stays healthy however long it delivers no events
    /// (`client_live_pods.rs`): kube's watcher reconnects on idle timeout, and
    /// `watch_failing` flips only on a watch error, stream end, or active
    /// re-list (`Init`..`InitDone`).
    pub async fn read_sandbox_pod(
        &self,
        pod_name: &str,
    ) -> Result<Option<Arc<SandboxPodView>>, KubeError> {
        if let Some(pod) = self
            .read_settled_sandbox_pod(pod_name, self.budgets.reflector_wait)
            .await
        {
            return Ok(Some(pod));
        }
        fetch_sandbox_pod_uncached(&self.k8s, pod_name).await
    }

    fn healthy_pod_watch_generation(&self) -> Option<u64> {
        self.pod_client_index
            .healthy_watch_generation(self.pod_reflector_ready.is_ready())
    }

    fn pod_reflector_watch_healthy(&self) -> bool {
        self.healthy_pod_watch_generation().is_some()
    }

    /// Look up a cached pod under `policy`, validating the reflector watch
    /// generation before and after reading `pod_store` (`InitDone` swaps the
    /// store before advancing the healthy generation). If a re-list completed
    /// between the two samples, retries once against the newly installed store
    /// so the immediate healthy-cache path stays zero-wait.
    fn lookup_cached_pod(
        &self,
        pod_name: &str,
        expected_uid: Option<&str>,
        policy: CacheTrustPolicy,
    ) -> Option<Arc<SandboxPodView>> {
        for _ in 0..2 {
            let gen_before = self.healthy_pod_watch_generation();
            if matches!(policy, CacheTrustPolicy::HealthyWatchOnly) && gen_before.is_none() {
                return None;
            }
            let pod = self.lookup_sandbox_pod(pod_name);
            let gen_after = self.healthy_pod_watch_generation();
            if let Some(pod) = pod {
                if expected_uid.is_some_and(|expected| pod.uid() != Some(expected)) {
                    if gen_before == gen_after || gen_after.is_none() {
                        return None;
                    }
                    continue;
                }
                match policy {
                    CacheTrustPolicy::SettledOnly => {
                        if pod_status_is_settled(&pod) {
                            return Some(pod);
                        }
                        if gen_before == gen_after || gen_after.is_none() {
                            return None;
                        }
                    }
                    CacheTrustPolicy::SettledOrHealthyWatch => {
                        if pod_status_is_settled(&pod) {
                            return Some(pod);
                        }
                        if gen_before.is_some() && gen_before == gen_after {
                            return Some(pod);
                        }
                    }
                    CacheTrustPolicy::HealthyWatchOnly => {
                        if gen_before.is_some() && gen_before == gen_after {
                            return Some(pod);
                        }
                    }
                }
            } else if gen_before == gen_after || gen_after.is_none() {
                return None;
            }
        }
        None
    }

    /// Re-evaluates `probe` on every reflector wakeup (including `Lagged` and
    /// nameless watch-state signals) until it yields, the watch turns
    /// unhealthy, or `wait` elapses; probes once more before giving up.
    async fn await_cached<T>(
        &self,
        wait: Duration,
        mut probe: impl FnMut() -> Option<T>,
    ) -> Option<T> {
        let mut rx = self.pod_events.subscribe();
        let deadline = Instant::now() + wait;
        loop {
            if let Some(found) = probe() {
                return Some(found);
            }
            if !self.pod_reflector_watch_healthy() || Instant::now() >= deadline {
                break;
            }
            match timeout_at(deadline, rx.recv()).await {
                Ok(Ok(_)) | Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                    continue;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) | Err(_) => break,
            }
        }
        probe()
    }

    async fn read_settled_sandbox_pod(
        &self,
        pod_name: &str,
        reflector_wait: Duration,
    ) -> Option<Arc<SandboxPodView>> {
        self.read_cached_sandbox_pod_for_uid(
            pod_name,
            None,
            reflector_wait,
            CacheTrustPolicy::SettledOrHealthyWatch,
        )
        .await
    }

    async fn read_cached_sandbox_pod_for_uid(
        &self,
        pod_name: &str,
        expected_uid: Option<&str>,
        reflector_wait: Duration,
        policy: CacheTrustPolicy,
    ) -> Option<Arc<SandboxPodView>> {
        if let Some(pod) = self.lookup_cached_pod(pod_name, expected_uid, policy) {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("cache_settled");
            return Some(pod);
        }
        if !self.pod_reflector_watch_healthy() || reflector_wait.is_zero() {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
            return None;
        }
        if let Some(pod) = self
            .await_cached(reflector_wait, || {
                self.lookup_cached_pod(pod_name, expected_uid, policy)
            })
            .await
        {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
            return Some(pod);
        }
        crate::metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
        None
    }

    /// Observe a claimed warm-pool pod with reflector catch-up and a bounded
    /// degraded-reflector `GET` fallback.
    ///
    /// Uses [`CacheTrustPolicy::HealthyWatchOnly`] so a pod that died during a
    /// watch disconnect is verified via `GET` rather than adopted from stale
    /// cache. When a healthy reflector still holds a pre-`Running` snapshot of
    /// the claimed pod's UID after `budgets.reflector_wait`, returns
    /// [`PooledClaimPodObservation::ReflectorLagging`] without issuing an
    /// apiserver `GET` so callers release or retry the slot rather than
    /// retiring healthy warm capacity on reflector lag.
    pub(in crate::controllers::substrate::kubernetes) async fn read_pooled_claim_pod(
        &self,
        claim: &PooledExecutionClaim,
        error_context: &str,
    ) -> Result<PooledClaimPodObservation, SubstrateError> {
        let authoritative_hit = || {
            self.lookup_cached_pod(&claim.pod_name, None, CacheTrustPolicy::HealthyWatchOnly)
                .filter(|pod| pooled_claim_pod_cache_is_authoritative(claim, pod))
        };
        if let Some(pod) = authoritative_hit() {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("cache_settled");
            return Ok(PooledClaimPodObservation::Observed(Some(pod)));
        }
        if self.pod_reflector_watch_healthy()
            && !self.budgets.reflector_wait.is_zero()
            && let Some(pod) = self
                .await_cached(self.budgets.reflector_wait, authoritative_hit)
                .await
        {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
            return Ok(PooledClaimPodObservation::Observed(Some(pod)));
        }
        if let Some(pod) =
            self.lookup_cached_pod(&claim.pod_name, None, CacheTrustPolicy::HealthyWatchOnly)
        {
            if pooled_claim_pod_cache_is_authoritative(claim, &pod) {
                crate::metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
                return Ok(PooledClaimPodObservation::Observed(Some(pod)));
            }
            if pooled_claim_pod_reflector_lagging(claim, &pod) {
                crate::metrics::sandbox::api::read_sandbox_pod_outcome("reflector_lagging");
                return Ok(PooledClaimPodObservation::ReflectorLagging);
            }
        }

        crate::metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
        let pod = self
            .fetch_sandbox_pod_bounded(&claim.pod_name, error_context)
            .await?;
        Ok(PooledClaimPodObservation::Observed(pod))
    }

    async fn fetch_sandbox_pod_bounded(
        &self,
        pod_name: &str,
        error_context: &str,
    ) -> Result<Option<Arc<SandboxPodView>>, SubstrateError> {
        let observed = timeout(
            self.budgets.readiness_pod_get,
            fetch_sandbox_pod_uncached(&self.k8s, pod_name),
        )
        .await
        .map_err(|_| {
            tracing::warn!(
                pod_name,
                error_context,
                timeout_ms = self.budgets.readiness_pod_get.as_millis(),
                "bounded Kubernetes pod GET timed out; failing fast"
            );
            kube_api_unavailable(
                error_context,
                format_args!(
                    "pod read timed out after {:?}",
                    self.budgets.readiness_pod_get
                ),
            )
        })?;
        Self::classify_fail_fast_pod_get(observed, error_context)
    }

    async fn fetch_sandbox_pod_until_deadline(
        &self,
        pod_name: &str,
        outer_deadline: Instant,
        error_context: &str,
    ) -> Result<PodGet, SubstrateError> {
        let now = Instant::now();
        if now >= outer_deadline {
            return Ok(PodGet::DeadlineElapsed);
        }
        let get_deadline = now + self.budgets.readiness_pod_get;
        let effective_deadline = outer_deadline.min(get_deadline);
        let observed = match timeout_at(
            effective_deadline,
            fetch_sandbox_pod_uncached(&self.k8s, pod_name),
        )
        .await
        {
            Ok(observed) => observed,
            Err(_) if get_deadline < outer_deadline => {
                tracing::warn!(
                    pod_name,
                    error_context,
                    timeout_ms = self.budgets.readiness_pod_get.as_millis(),
                    "bounded Kubernetes pod GET timed out; failing fast"
                );
                return Err(kube_api_unavailable(
                    error_context,
                    format_args!(
                        "pod read timed out after {:?}",
                        self.budgets.readiness_pod_get
                    ),
                ));
            }
            Err(_) => {
                tracing::debug!(pod_name, "readiness pod GET hit phase deadline");
                return Ok(PodGet::DeadlineElapsed);
            }
        };
        Self::classify_fail_fast_pod_get(observed, error_context).map(PodGet::Observed)
    }

    fn classify_fail_fast_pod_get(
        observed: Result<Option<Arc<SandboxPodView>>, KubeError>,
        error_context: &str,
    ) -> Result<Option<Arc<SandboxPodView>>, SubstrateError> {
        observed.map_err(|err| {
            if is_kube_api_throttling_error(&err) {
                kube_api_unavailable(error_context, format_args!("throttling: {err}"))
            } else if is_transient_kube_read_error(&err) {
                kube_api_unavailable(error_context, format_args!("transient failure: {err}"))
            } else {
                SubstrateError::Failed {
                    message: format!("{error_context}: {err}"),
                }
            }
        })
    }

    /// Bound direct pod GETs against `readiness_deadline` and
    /// `budgets.readiness_pod_get`.
    ///
    /// When `readiness_deadline` is `Some(deadline)`:
    /// - Expiry of `deadline` itself returns `Ok(PodGet::DeadlineElapsed)` (an
    ///   unknown observation, never an authoritative 404) so the readiness
    ///   caller reports its phase-deadline `504` timeout.
    /// - Expiry of `readiness_pod_get` *before* `deadline`, apiserver `429`
    ///   throttling, or transient apiserver read errors (`408`/`5xx` API status
    ///   errors and HTTP transport failures) fail fast with retryable
    ///   `SubstrateError::Unavailable` (`503`) rather than stacking overlapping
    ///   `60s` client `GET`s.
    async fn get_pod_for_resolution(
        &self,
        pod_name: &str,
        readiness_deadline: Option<Instant>,
        error_context: &str,
    ) -> Result<PodGet, SubstrateError> {
        if let Some(deadline) = readiness_deadline {
            return self
                .fetch_sandbox_pod_until_deadline(pod_name, deadline, error_context)
                .await;
        }

        fetch_sandbox_pod_uncached(&self.k8s, pod_name)
            .await
            .map(PodGet::Observed)
            .map_err(|err| kube_error_for_substrate(error_context, err))
    }

    /// `retry` states what the calling operation's retry would do; see
    /// [`PlacementRetryDisposition`]. It is a parameter because this helper
    /// serves both `create` (whose retry mints a new sandbox) and
    /// `start`/`wake`/`access` (whose retry converges).
    ///
    /// Two call sites pass a constant that is not their operation's:
    /// `resolve_pod_ip` and `wait_ready`'s `watch_until_pod_ip_assigned` are
    /// each reached from create readiness *and* from wake/access/delete, and
    /// both pass `RetryDuplicatesExecution`. That is the safe direction — it
    /// costs a converging caller a 500 where a 503 would have done, rather
    /// than risking a duplicate sandbox — but it does mean contention on those
    /// two paths is not retryable even for operations that converge. Relaxing
    /// either one requires threading the disposition down through it, not
    /// flipping the constant.
    pub(in crate::controllers::substrate::kubernetes) async fn remember_pod_node_placement(
        &self,
        sandbox_id: SandboxId,
        pod: &SandboxPodView,
        retry: PlacementRetryDisposition,
    ) -> Result<Option<NodeId>, SubstrateError> {
        let Some(node_name) = assigned_pod_node_name(pod) else {
            return Ok(None);
        };
        // Watcher-backed cache is the warm path. Fall back to a direct API
        // get when the reflector hasn't populated this Node yet — startup
        // races and slow API-server lists must not break placement that the
        // scheduler has already committed.
        let node = match self.k8s.caches.node_by_name(node_name) {
            Some(node) => node,
            None => {
                let nodes: Api<Node> = Api::all(self.k8s.client.clone());
                let fetched = nodes.get(node_name).await.map_err(|err| match err {
                    kube::Error::Api(resp) if resp.code == 404 => SubstrateError::Unavailable {
                        message: format!(
                            "Kubernetes node {node_name} no longer exists; cannot preserve node-local sandbox placement"
                        ),
                    },
                    other => SubstrateError::Failed {
                        message: format!("failed to query Kubernetes node {node_name}: {other}"),
                    },
                })?;
                std::sync::Arc::new(fetched)
            }
        };
        let node_uid = node
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| SubstrateError::Failed {
                message: format!("Kubernetes node {node_name} has no metadata.uid"),
            })?;
        let role = SpaceNodeRole::Worker;
        let node_id = space_k8s_node_id(node_name, role.as_str());
        let instance_id = k8s_node_instance_id(&node);
        let address = k8s_node_internal_ip(&node).ok_or_else(|| SubstrateError::Unavailable {
            message: format!(
                "Kubernetes node {node_name} has no InternalIP; cannot authenticate node-local reconciliation"
            ),
        })?;

        self.store
            .persist_pod_node_placement_after_pod_exists(
                PodNodePlacement {
                    sandbox_id,
                    node_name,
                    node_uid,
                    node_id,
                    role,
                    address: Some(address.as_str()),
                    instance_id: instance_id.as_ref(),
                },
                retry,
            )
            .await?;
        Ok(Some(node_id))
    }

    /// Read-only counterpart to the repairing resolver. Missing, terminal, or
    /// replaced pods belong to recovery; a reachability probe cannot delete
    /// them or change execution tracking while its request can be cancelled.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_reachable_pod_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        // A restoring pod is not reachable for a request: see
        // `join_restore_before_dial`.
        if is_suspended_lifecycle(&row.lifecycle)
            || is_restoring_lifecycle(&row.lifecycle)
            || execution_pod_lookup_in_flight(row.state)
            || row.pod_uid.is_none()
            || row.pod_ip.is_none()
        {
            return Ok(None);
        }
        // The row's IP for the cached pod it names is current before kubelet
        // publishes it; only a pod the row does not describe needs a settled read.
        let cached = self
            .lookup_sandbox_pod(&row.pod_name)
            .filter(|pod| row.pod_uid.as_ref().map(PodUid::as_str) == pod.uid());
        let pod = match cached {
            Some(pod) => pod,
            None => match self
                .read_cached_sandbox_pod_for_uid(
                    &row.pod_name,
                    row.pod_uid.as_ref().map(PodUid::as_str),
                    self.budgets.reflector_wait,
                    CacheTrustPolicy::SettledOrHealthyWatch,
                )
                .await
            {
                Some(pod) => pod,
                None => match self
                    .fetch_sandbox_pod_bounded(
                        &row.pod_name,
                        "failed to read reachable sandbox pod",
                    )
                    .await?
                {
                    Some(pod) => pod,
                    None => return Ok(None),
                },
            },
        };
        if pod.is_deleting()
            || terminal_pod_phase(&pod).is_some()
            || pod.unschedulable_message().is_some()
            || row.pod_uid.as_ref().map(PodUid::as_str) != pod.uid()
        {
            return Ok(None);
        }
        let address =
            current_pod_ip_with_source(&pod, row.pod_uid.as_ref(), row.pod_ip.as_deref())?;
        // Let the owned path publish changed routing evidence before returning
        // a client; otherwise ingress would repeatedly resolve stale metadata.
        if row.pod_ip != address.map(|(ip, _)| ip.to_string())
            || row.k8s_node_name.as_deref() != assigned_pod_node_name(&pod)
        {
            return Ok(None);
        }
        let Some(address) = address else {
            return Ok(None);
        };
        // An address kubelet has not published is reachable only while the
        // worker behind it reports this execution.
        let target = PodProxyWaitTarget {
            pod_name: row.pod_name.as_str(),
            pod_uid: row.pod_uid.as_ref().map(PodUid::as_str),
            generation: row.generation,
        };
        Ok(self
            .verified_runtime_pod_ip(sandbox_id, address, &target)
            .await
            .ok()
            .flatten())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodIp>, SubstrateError> {
        Ok(self
            .resolve_pod_instance(sandbox_id)
            .await?
            .map(|pod| pod.ip))
    }

    /// Bound direct pod GETs without putting the store mutations in the
    /// resolver under the shorter GET timeout. An expired `deadline` returns
    /// `Ok(None)` so readiness reports its phase timeout, while a
    /// `readiness_pod_get` timeout before `deadline` or a transient apiserver
    /// read failure (`429`/`408`/`5xx`/transport) fails fast with
    /// [`SubstrateError::Unavailable`]. Only an address for the execution and
    /// pod `target` names is returned, with where it comes from.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_ip_for_readiness(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        self.resolve_pod_ip_for_readiness_with_policy(
            sandbox_id,
            target,
            deadline,
            CacheTrustPolicy::SettledOrHealthyWatch,
        )
        .await
    }

    /// Like [`Self::resolve_pod_ip_for_readiness`], for post-watch fallback
    /// polling (`poll_until_pod_ip_assigned`): uses [`CacheTrustPolicy::SettledOnly`]
    /// so an unsettled `Pending` entry in `pod_store` does not suppress fallback
    /// `GET`s after the per-pod watch ends or errors.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_ip_for_readiness_poll(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        self.resolve_pod_ip_for_readiness_with_policy(
            sandbox_id,
            target,
            deadline,
            CacheTrustPolicy::SettledOnly,
        )
        .await
    }

    async fn resolve_pod_ip_for_readiness_with_policy(
        &self,
        sandbox_id: SandboxId,
        target: &PodProxyWaitTarget<'_>,
        deadline: Instant,
        policy: CacheTrustPolicy,
    ) -> Result<Option<(PodIp, PodIpSource)>, SubstrateError> {
        Ok(self
            .resolve_pod_instance_inner(sandbox_id, Some(deadline), policy)
            .await?
            .filter(|pod| {
                pod.generation == target.generation
                    && target
                        .pod_uid
                        .is_none_or(|uid| pod.pod_uid.as_ref().map(PodUid::as_str) == Some(uid))
            })
            .map(|pod| (pod.ip, pod.source)))
    }

    /// [`Self::resolve_pod_ip`], with the identity of the pod the address
    /// belongs to.
    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_instance(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<ResolvedPodInstance>, SubstrateError> {
        self.resolve_pod_instance_inner(sandbox_id, None, CacheTrustPolicy::SettledOrHealthyWatch)
            .await
    }

    async fn resolve_pod_instance_inner(
        &self,
        sandbox_id: SandboxId,
        readiness_deadline: Option<Instant>,
        policy: CacheTrustPolicy,
    ) -> Result<Option<ResolvedPodInstance>, SubstrateError> {
        for allow_reresolve in [true, false] {
            let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
                return Ok(None);
            };
            if is_suspended_lifecycle(&row.lifecycle) {
                return Ok(None);
            }
            let observed_pod_uid = row.pod_uid.as_ref().map(PodUid::as_str);

            let (pod, verified_uncached) = if execution_pod_lookup_in_flight(row.state) {
                if !stale_restore_execution_create(
                    &row.lifecycle,
                    row.state,
                    row.create_age_expired,
                ) {
                    return Ok(None);
                }
                (
                    self.get_pod_for_resolution(
                        &row.pod_name,
                        readiness_deadline,
                        "failed to verify stale creating restore pod",
                    )
                    .await?,
                    true,
                )
            } else {
                let reflector_wait =
                    readiness_deadline.map_or(self.budgets.reflector_wait, |deadline| {
                        self.budgets
                            .reflector_wait
                            .min(deadline.saturating_duration_since(Instant::now()))
                    });
                if let Some(pod) = self
                    .read_cached_sandbox_pod_for_uid(
                        &row.pod_name,
                        observed_pod_uid,
                        reflector_wait,
                        policy,
                    )
                    .await
                {
                    (PodGet::Observed(Some(pod)), false)
                } else {
                    (
                        self.get_pod_for_resolution(
                            &row.pod_name,
                            readiness_deadline,
                            "failed to query K8s pod",
                        )
                        .await?,
                        true,
                    )
                }
            };

            let PodGet::Observed(pod) = pod else {
                return Ok(None);
            };
            let (pod, verified_uncached) = match pod {
                Some(pod) => (pod, verified_uncached),
                None => {
                    // A cache miss alone must never authorize execution retirement,
                    // because a finalizer-blocked or unlisted pod can be absent
                    // from the cache while still present on the apiserver. When the
                    // `None` above already came from an uncached `GET`
                    // (`verified_uncached`), reuse that 404 observation rather than
                    // issuing a second back-to-back apiserver `GET`.
                    let authoritative = if verified_uncached {
                        None
                    } else {
                        let PodGet::Observed(pod) = self
                            .get_pod_for_resolution(
                                &row.pod_name,
                                readiness_deadline,
                                "failed to verify missing Kubernetes sandbox pod",
                            )
                            .await?
                        else {
                            return Ok(None);
                        };
                        pod
                    };
                    match authoritative {
                        Some(pod) => (pod, true),
                        None => {
                            if row.state == ActiveExecutionState::RecoveryFailed {
                                match self
                                    .store
                                    .converge_failed_recovery_after_pod_absence_if_generation(
                                        sandbox_id,
                                        row.generation,
                                        observed_pod_uid,
                                        row.pod_ip.as_deref(),
                                        row.k8s_node_name.as_deref(),
                                    )
                                    .await?
                                {
                                    FailedRecoveryPodAbsenceOutcome::NotMatched
                                        if allow_reresolve =>
                                    {
                                        continue;
                                    }
                                    FailedRecoveryPodAbsenceOutcome::NotMatched => {}
                                    FailedRecoveryPodAbsenceOutcome::ClaimPreserved => {
                                        tracing::debug!(
                                            sandbox_id = %sandbox_id,
                                            generation = row.generation,
                                            pod_name = %row.pod_name,
                                            "failed recovery pod is absent; clearing cached route and preserving execution claim"
                                        );
                                    }
                                    FailedRecoveryPodAbsenceOutcome::ConvergedSuspended => {
                                        tracing::info!(
                                            sandbox_id = %sandbox_id,
                                            generation = row.generation,
                                            pod_name = %row.pod_name,
                                            "failed recovery pod is absent; converged current durable snapshot to suspended"
                                        );
                                    }
                                }
                                return Ok(None);
                            }
                            if row.lifecycle == SandboxLifecycle::Suspending.as_str()
                                && let Some(pod_uid) = observed_pod_uid
                                && self
                                    .store
                                    .finalize_missing_suspending_pod_with_current_snapshot(
                                        sandbox_id,
                                        row.generation,
                                        pod_uid,
                                    )
                                    .await?
                            {
                                tracing::info!(
                                    sandbox_id = %sandbox_id,
                                    generation = row.generation,
                                    pod_name = %row.pod_name,
                                    "finalized suspended sandbox after its pod disappeared with a current durable snapshot"
                                );
                                return Ok(None);
                            }
                            match self
                                .store
                                .forget_missing_pod_tracking_if_generation_and_pod_uid(
                                    sandbox_id,
                                    row.generation,
                                    observed_pod_uid,
                                )
                                .await?
                            {
                                MissingPodTrackingCleanupOutcome::Forgotten => {
                                    tracing::warn!(
                                        sandbox_id = %sandbox_id,
                                        pod_name = %row.pod_name,
                                        cached_pod_ip = ?row.pod_ip,
                                        "tracked Kubernetes sandbox pod is authoritatively absent; clearing cached pod IP"
                                    );
                                    return Ok(None);
                                }
                                MissingPodTrackingCleanupOutcome::DeferredToSuspend
                                | MissingPodTrackingCleanupOutcome::SandboxAbsent => {
                                    return Ok(None);
                                }
                                MissingPodTrackingCleanupOutcome::NotMatched => {
                                    if allow_reresolve {
                                        tracing::debug!(
                                            sandbox_id = %sandbox_id,
                                            generation = row.generation,
                                            pod_name = %row.pod_name,
                                            observed_pod_uid = ?observed_pod_uid,
                                            "missing-pod cleanup skipped after concurrent execution update; re-resolving"
                                        );
                                        continue;
                                    }
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
            };

            let pod = if let Some(cached_phase) = terminal_pod_phase(&pod) {
                let authoritative_pod = if verified_uncached {
                    Some(pod)
                } else {
                    let PodGet::Observed(authoritative_pod) = self
                        .get_pod_for_resolution(
                            &row.pod_name,
                            readiness_deadline,
                            "failed to verify terminal Kubernetes sandbox pod",
                        )
                        .await?
                    else {
                        return Ok(None);
                    };
                    authoritative_pod
                };
                if let Some(phase) = terminal_cleanup_phase_after_uncached_check(
                    cached_phase,
                    authoritative_pod.as_deref(),
                ) {
                    let publication_grace_remaining = authoritative_pod
                        .as_ref()
                        .filter(|_| phase == TerminalPodPhase::Succeeded)
                        .and_then(|pod| {
                            succeeded_pod_publication_grace_remaining(pod, jiff::Timestamp::now())
                        });
                    if row.lifecycle == SandboxLifecycle::Suspending.as_str()
                        && publication_grace_remaining.is_some()
                    {
                        tracing::info!(
                            sandbox_id = %sandbox_id,
                            pod_name = %row.pod_name,
                            generation = row.generation,
                            pod_uid = ?row.pod_uid,
                            lifecycle = %row.lifecycle,
                            grace_remaining_ms = publication_grace_remaining
                                .map(|remaining| remaining.as_millis()),
                            "deferring successful terminal pod cleanup while suspend publication may still arrive"
                        );
                        return Err(SubstrateError::suspend_in_flight(sandbox_id));
                    }
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        pod_name = %row.pod_name,
                        cached_phase = %cached_phase,
                        phase = %phase,
                        cached_pod_ip = ?row.pod_ip,
                        "tracked Kubernetes sandbox pod is terminal after uncached verification; deleting it and clearing cached pod IP"
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
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name = %row.pod_name,
                    cached_phase = %cached_phase,
                    cached_pod_ip = ?row.pod_ip,
                    "cached Kubernetes sandbox pod was terminal but uncached pod is still active; keeping tracking"
                );
                authoritative_pod
                    .expect("non-terminal verification result requires an existing pod")
            } else {
                pod
            };

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

            let pod_uid = pod.uid().map(str::to_owned);
            let resolved =
                current_pod_ip_with_source(&pod, row.pod_uid.as_ref(), row.pod_ip.as_deref())?;
            let pod_ip = resolved.map(|(ip, _)| ip);
            let pod_ip_string = pod_ip.map(|ip| ip.to_string());
            let pod_client = pod.client_id();
            if observed_pod_uid != pod_uid.as_deref()
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
                    Ok(None) => tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        "pod metadata write no-oped after pod lookup: the execution was \
                         retired or replaced mid-flight"
                    ),
                    Err(err) => tracing::warn!(
                        sandbox_id = %sandbox_id,
                        generation = row.generation,
                        error = %err,
                        "failed to persist Kubernetes pod metadata after pod lookup"
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
            {
                // The pod is the one the row tracks, and only the record lags its
                // label: repair the record alone, leaving the row's state as it is.
                if let Err(err) = self
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
                        "failed to repair the sandbox's activating client after pod lookup"
                    );
                }
            }

            return Ok(resolved.map(|(ip, source)| ResolvedPodInstance {
                ip,
                source,
                pod_uid: pod_uid.as_deref().and_then(|uid| PodUid::parse(uid).ok()),
                generation: row.generation,
                sandbox_restoring: is_restoring_lifecycle(&row.lifecycle),
            }));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
