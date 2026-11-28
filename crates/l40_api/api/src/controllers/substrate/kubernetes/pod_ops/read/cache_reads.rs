//! Reflector-backed and direct apiserver pod read helpers used by execution
//! resolution, warm-pool claims, and readiness fallback reads.

use super::*;
use crate::controllers::substrate::PooledExecutionClaim;

pub(in crate::controllers::substrate::kubernetes) enum PodGet {
    Observed(Option<Arc<SandboxPodView>>),
    DeadlineElapsed,
}

pub(in crate::controllers::substrate::kubernetes) enum PooledClaimPodObservation {
    Observed(Option<Arc<SandboxPodView>>),
    /// The shared pod reflector is healthy, holds the claimed pod in a
    /// non-deleting `Pending` (or unset-phase) state with no conflicting UID or
    /// IP — including a pre-bind `Unschedulable` snapshot from before the slot
    /// was promoted to `ready` — and hasn't processed the `Pending` -> `Running`
    /// transition within `reflector_wait`.
    ReflectorLagging,
}

fn pooled_claim_pod_observation_settled(
    claim: &PooledExecutionClaim,
    pod: &SandboxPodView,
) -> bool {
    if pooled_claim_pod_is_usable(claim, Some(pod))
        || pod.is_deleting()
        || terminal_pod_phase(pod).is_some()
        || !matches!(pod.phase(), None | Some("Pending"))
    {
        return true;
    }
    if pod.pod_ip().is_some_and(|ip| ip != claim.pod_ip.as_str()) {
        return true;
    }
    match (claim.pod_uid.as_ref().map(|uid| uid.as_str()), pod.uid()) {
        (Some(expected_uid), Some(actual_uid)) => expected_uid != actual_uid,
        (None, _) => true,
        (Some(_), None) => false,
    }
}

#[derive(Debug)]
pub(in crate::controllers::substrate::kubernetes) enum ScheduledCreatePodRead {
    Matched(Arc<SandboxPodView>),
    NotFound,
    SupersededUid,
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

    /// True while the shared sandbox-pod reflector has completed initial sync,
    /// its watch stream hasn't faulted, and a watch delivery arrived within
    /// [`SubstrateBudgets::readiness_quiet_reflector`].
    pub(in crate::controllers::substrate::kubernetes) fn pod_reflector_watch_healthy_at(
        &self,
        now: Instant,
    ) -> bool {
        self.pod_reflector_ready.is_ready()
            && !self.pod_client_index.is_watch_failing()
            && self.pod_events.last_delivery().is_some_and(|last| {
                now.saturating_duration_since(last) < self.budgets.readiness_quiet_reflector
            })
    }

    pub(super) fn cached_pod_is_authoritative(
        &self,
        pod: &SandboxPodView,
        expected_uid: Option<&str>,
        require_settled: bool,
        now: Instant,
    ) -> bool {
        let settled = pod_status_is_settled(pod);
        let uid_acceptable = match (expected_uid, pod.uid()) {
            (Some(expected), Some(actual)) => expected == actual,
            // Synthetic unit-test fixtures (`sandbox_pod_on_node`) omit `uid` on
            // settled pods; unsettled cache entries for a UID-pinned execution
            // must carry the expected UID before a healthy reflector trusts them.
            (Some(_), None) => settled,
            (None, _) => true,
        };
        uid_acceptable
            && (settled || (!require_settled && self.pod_reflector_watch_healthy_at(now)))
    }

    /// Reads a warm-pool claim's pod, waiting up to `reflector_wait` on
    /// `pod_events` when this replica's reflector still holds a pre-`Running`
    /// snapshot of a slot the cluster controller just promoted to `ready`.
    ///
    /// Preserves both warm-pool capacity and the healthy-reflector no-`GET`
    /// rule: if `reflector_wait` elapses while the shared pod reflector is
    /// healthy and the cached pod is still pre-`Running`, returns
    /// [`PooledClaimPodObservation::ReflectorLagging`] without issuing an
    /// apiserver `GET` so callers release/retry rather than invalidating the
    /// healthy slot.
    pub(in crate::controllers::substrate::kubernetes) async fn read_pooled_claim_pod(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledClaimPodObservation, KubeError> {
        let pod_name = claim.pod_name.as_str();
        if let Some(pod) = self.lookup_sandbox_pod(pod_name)
            && pooled_claim_pod_observation_settled(claim, &pod)
        {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("cache_settled");
            return Ok(PooledClaimPodObservation::Observed(Some(pod)));
        }

        let mut rx = self.pod_events.subscribe();
        let deadline = Instant::now() + self.budgets.reflector_wait;
        loop {
            if let Some(pod) = self.lookup_sandbox_pod(pod_name)
                && pooled_claim_pod_observation_settled(claim, &pod)
            {
                crate::metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
                return Ok(PooledClaimPodObservation::Observed(Some(pod)));
            }
            if !self.pod_reflector_watch_healthy_at(Instant::now()) {
                break;
            }
            match timeout_at(deadline, rx.recv()).await {
                Ok(Ok(ref name)) if !name.is_empty() && name != pod_name => continue,
                Ok(Ok(_)) | Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) | Err(_) => break,
            }
        }

        if let Some(pod) = self.lookup_sandbox_pod(pod_name) {
            if pooled_claim_pod_observation_settled(claim, &pod) {
                crate::metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
                return Ok(PooledClaimPodObservation::Observed(Some(pod)));
            }
            if self.pod_reflector_watch_healthy_at(Instant::now()) {
                crate::metrics::sandbox::api::read_sandbox_pod_outcome("reflector_lagging");
                return Ok(PooledClaimPodObservation::ReflectorLagging);
            }
        }
        crate::metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
        fetch_sandbox_pod_uncached(&self.k8s, pod_name)
            .await
            .map(PooledClaimPodObservation::Observed)
    }

    /// Reads a sandbox pod, preferring the shared reflector cache over a direct
    /// apiserver `GET`.
    ///
    /// 1. **Cache hit + authoritative** (settled status, or any status —
    ///    including `Pending` — while the shared pod reflector watch is
    ///    healthy) → return cached with zero apiserver `GET`s.
    /// 2. **Cache miss while the reflector is healthy** → wait up to
    ///    `SubstrateBudgets::reflector_wait` on `pod_events` for the watch
    ///    event to populate the cache.
    /// 3. **Degraded reflector or still absent after `reflector_wait`** → fall
    ///    through immediately to a single direct `GET /pods/{name}`.
    pub async fn read_sandbox_pod(
        &self,
        pod_name: &str,
    ) -> Result<Option<Arc<SandboxPodView>>, KubeError> {
        if let Some(pod) = self
            .read_cached_sandbox_pod(pod_name, self.budgets.reflector_wait)
            .await
        {
            return Ok(Some(pod));
        }
        fetch_sandbox_pod_uncached(&self.k8s, pod_name).await
    }

    pub(super) async fn read_cached_sandbox_pod(
        &self,
        pod_name: &str,
        reflector_wait: Duration,
    ) -> Option<Arc<SandboxPodView>> {
        self.read_cached_sandbox_pod_for_uid(pod_name, None, false, reflector_wait)
            .await
    }

    pub(super) async fn read_cached_sandbox_pod_for_uid(
        &self,
        pod_name: &str,
        expected_uid: Option<&str>,
        require_settled: bool,
        reflector_wait: Duration,
    ) -> Option<Arc<SandboxPodView>> {
        self.read_cached_sandbox_pod_matching(pod_name, reflector_wait, |pod, now| {
            self.cached_pod_is_authoritative(pod, expected_uid, require_settled, now)
        })
        .await
    }

    pub(super) async fn read_cached_sandbox_pod_matching(
        &self,
        pod_name: &str,
        reflector_wait: Duration,
        mut matches: impl FnMut(&SandboxPodView, Instant) -> bool,
    ) -> Option<Arc<SandboxPodView>> {
        if let Some(pod) = self.lookup_sandbox_pod(pod_name)
            && matches(&pod, Instant::now())
        {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("cache_settled");
            return Some(pod);
        }
        // Wait up to `reflector_wait` for a matching reflector update to make
        // the cache entry usable (for instance after a same-name pod
        // replacement whose predecessor `Delete` arrives just before the
        // replacement `Apply`, or before scheduler binding reaches the cache).
        // The reflector store is updated by the same task that drives the
        // broadcast, so re-check once more on exit even if the channel lagged
        // or timed out.
        let mut rx = self.pod_events.subscribe();
        let deadline = Instant::now() + reflector_wait;
        loop {
            let now = Instant::now();
            if let Some(pod) = self.lookup_sandbox_pod(pod_name)
                && matches(&pod, now)
            {
                crate::metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
                return Some(pod);
            }
            if !self.pod_reflector_watch_healthy_at(now) {
                break;
            }
            match timeout_at(deadline, rx.recv()).await {
                Ok(Ok(ref name)) if !name.is_empty() && name != pod_name => continue,
                Ok(Ok(_)) | Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) | Err(_) => break,
            }
        }
        if let Some(pod) = self.lookup_sandbox_pod(pod_name)
            && matches(&pod, Instant::now())
        {
            crate::metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
            return Some(pod);
        }
        crate::metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
        None
    }

    pub(in crate::controllers::substrate::kubernetes) async fn read_scheduled_create_pod(
        &self,
        pod_name: &str,
        expected_uid: Option<&str>,
        error_context: &str,
    ) -> Result<ScheduledCreatePodRead, SubstrateError> {
        let uid_matches = |pod: &SandboxPodView| {
            expected_uid.is_none_or(|uid| pod.uid().is_none_or(|actual| actual == uid))
        };
        if let Some(pod) = self
            .read_cached_sandbox_pod_matching(pod_name, self.budgets.reflector_wait, |pod, _| {
                assigned_pod_node_name(pod).is_some()
                    && expected_uid.is_none_or(|uid| pod.uid() == Some(uid))
            })
            .await
        {
            return Ok(ScheduledCreatePodRead::Matched(pod));
        }
        let deadline = Instant::now() + self.budgets.readiness_pod_get + Duration::from_millis(1);
        match self
            .get_pod_for_resolution(pod_name, Some(deadline), error_context)
            .await?
        {
            PodGet::Observed(Some(pod)) if !uid_matches(&pod) => {
                Ok(ScheduledCreatePodRead::SupersededUid)
            }
            PodGet::Observed(Some(pod)) => Ok(ScheduledCreatePodRead::Matched(pod)),
            PodGet::Observed(None) => Ok(ScheduledCreatePodRead::NotFound),
            PodGet::DeadlineElapsed => Err(SubstrateError::Unavailable {
                message: format!(
                    "{error_context}: Kubernetes API pod read timed out after {:?}",
                    self.budgets.readiness_pod_get
                ),
            }),
        }
    }

    /// Reads a pod from `kube-apiserver` for execution resolution.
    ///
    /// When `readiness_deadline` is `Some(deadline)`, the read is bounded by
    /// `readiness_pod_get` (clamped to `deadline`). A phase-deadline expiry
    /// returns [`PodGet::DeadlineElapsed`] (never an authoritative `404`),
    /// whereas a `readiness_pod_get` timeout before `deadline` or a `429` /
    /// `5xx` / transient read failure surfaces as
    /// [`SubstrateError::Unavailable`] (`503`) so readiness fails fast with
    /// retryable backpressure instead of burning the outer create budget.
    pub(in crate::controllers::substrate::kubernetes) async fn get_pod_for_resolution(
        &self,
        pod_name: &str,
        readiness_deadline: Option<Instant>,
        error_context: &str,
    ) -> Result<PodGet, SubstrateError> {
        let get = fetch_sandbox_pod_uncached(&self.k8s, pod_name);
        if let Some(deadline) = readiness_deadline {
            let now = Instant::now();
            if now >= deadline {
                return Ok(PodGet::DeadlineElapsed);
            }
            let pod_get_budget = self.budgets.readiness_pod_get;
            let get_deadline = now + pod_get_budget;
            match timeout_at(deadline.min(get_deadline), get).await {
                Ok(Ok(pod)) => Ok(PodGet::Observed(pod)),
                Ok(Err(err)) if is_kube_api_throttling_error(&err) => {
                    Err(SubstrateError::Unavailable {
                        message: format!(
                            "{error_context}: Kubernetes API throttled pod read: {err}"
                        ),
                    })
                }
                Ok(Err(err)) if is_transient_kube_read_error(&err) => {
                    Err(SubstrateError::Unavailable {
                        message: format!("{error_context}: Kubernetes API pod read failed: {err}"),
                    })
                }
                Ok(Err(err)) => Err(kube_error_for_substrate(error_context, err)),
                Err(_) if get_deadline < deadline => Err(SubstrateError::Unavailable {
                    message: format!(
                        "{error_context}: Kubernetes API pod read timed out after {pod_get_budget:?}"
                    ),
                }),
                Err(_) => {
                    tracing::debug!(pod_name, "readiness phase deadline ended in-flight pod GET");
                    Ok(PodGet::DeadlineElapsed)
                }
            }
        } else {
            get.await
                .map(PodGet::Observed)
                .map_err(|err| kube_error_for_substrate(error_context, err))
        }
    }
}
