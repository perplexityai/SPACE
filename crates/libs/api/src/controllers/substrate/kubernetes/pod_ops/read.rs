//! Pod read paths: cached and uncached sandbox pod reads, pod IP resolution,
//! and remembering the node a resolved pod landed on.

use super::*;

const SUCCEEDED_POD_SUSPEND_PUBLICATION_GRACE: Duration = Duration::from_secs(180);

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

    /// Reads a sandbox pod with cache-first, event-wait, kube-apiserver-
    /// fallback semantics.
    ///
    /// 1. **Cache hit + settled** → return cached.
    /// 2. **Cache miss or unsettled** → wait briefly (`REFLECTOR_WAIT_BUDGET`)
    ///    for a reflector event referencing this pod. On event, re-check the
    ///    cache; if now settled, return cached.
    /// 3. **Event budget elapsed** or **event arrived but still unsettled** →
    ///    fall through to a direct `GET /pods/{name}` (the pre-A4 behavior).
    ///
    /// "Settled" means the cached entry has reached a status that callers
    /// branch on: assigned IP, terminal phase, or scheduler-marked
    /// unschedulable.
    ///
    /// The reflector watch is independent from the per-pod watches used by
    /// `wait_for_pod_proxy_reachable`, so it can briefly lag a Modified event
    /// that the per-pod watch already observed. The bounded event wait closes
    /// most of that window without putting load on the kube apiserver under
    /// burst; the GET fallback preserves correctness when the reflector
    /// genuinely lags or the pod is unknown to the cache.
    ///
    /// Steady-state behavior is unchanged: once a pod is running with an IP
    /// (or terminal/unschedulable), the cache is authoritative and no GET
    /// happens. Extra GETs are bounded to the brief window between pod
    /// creation/scheduling and the reflector observing the bind event.
    pub async fn read_sandbox_pod(
        &self,
        pod_name: &str,
    ) -> Result<Option<Arc<SandboxPodView>>, KubeError> {
        if let Some(pod) = self.lookup_sandbox_pod(pod_name)
            && pod_status_is_settled(&pod)
        {
            pplx_space_metrics::sandbox::api::read_sandbox_pod_outcome("cache_settled");
            return Ok(Some(pod));
        }
        // Wait for either a matching reflector event or the budget to elapse.
        // The boolean is informational — re-check the cache regardless. The
        // reflector store is updated by the same task that drives the event
        // broadcast, so even on a `Lagged`-then-timeout path the store may
        // already have the assignment we were waiting for.
        let _ = self
            .pod_events
            .wait_for(pod_name, REFLECTOR_WAIT_BUDGET)
            .await;
        if let Some(pod) = self.lookup_sandbox_pod(pod_name)
            && pod_status_is_settled(&pod)
        {
            pplx_space_metrics::sandbox::api::read_sandbox_pod_outcome("event_settled");
            return Ok(Some(pod));
        }
        pplx_space_metrics::sandbox::api::read_sandbox_pod_outcome("get_fallback");
        fetch_sandbox_pod_uncached(&self.k8s, pod_name).await
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
        if is_suspended_lifecycle(&row.lifecycle) || execution_pod_lookup_in_flight(row.state) {
            return Ok(None);
        }
        let Some(pod) = self
            .read_sandbox_pod(&row.pod_name)
            .await
            .map_err(|error| {
                kube_error_for_substrate("failed to read reachable sandbox pod", error)
            })?
        else {
            return Ok(None);
        };
        if pod.is_deleting()
            || terminal_pod_phase(&pod).is_some()
            || pod.unschedulable_message().is_some()
            || row.pod_uid.is_none()
            || row.pod_uid.as_ref().map(PodUid::as_str) != pod.uid()
        {
            return Ok(None);
        }
        let pod_ip = assigned_pod_ip(&pod)?;
        // Let the owned path publish changed routing evidence before returning
        // a client; otherwise ingress would repeatedly resolve stale metadata.
        if row.pod_ip != pod_ip.map(|ip| ip.to_string())
            || row.k8s_node_name.as_deref() != assigned_pod_node_name(&pod)
        {
            return Ok(None);
        }
        Ok(pod_ip)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn resolve_pod_ip(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodIp>, SubstrateError> {
        let Some(row) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(None);
        };
        if is_suspended_lifecycle(&row.lifecycle) {
            return Ok(None);
        }

        let pod = if execution_pod_lookup_in_flight(row.state) {
            if !stale_restore_execution_create(&row.lifecycle, row.state, row.create_age_expired) {
                return Ok(None);
            }
            fetch_sandbox_pod_uncached(&self.k8s, &row.pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate("failed to verify stale creating restore pod", err)
                })?
        } else {
            self.read_sandbox_pod(&row.pod_name)
                .await
                .map_err(|err| kube_error_for_substrate("failed to query K8s pod", err))?
        };

        let pod = match pod {
            Some(pod) => pod,
            None => {
                // The filtered reflector store excludes terminal pods. During
                // suspend that creates a cache miss before a finalizer-blocked
                // Kubernetes object is actually absent, so a cache miss alone
                // must never authorize execution retirement.
                match fetch_sandbox_pod_uncached(&self.k8s, &row.pod_name)
                    .await
                    .map_err(|err| {
                        kube_error_for_substrate(
                            "failed to verify missing Kubernetes sandbox pod",
                            err,
                        )
                    })? {
                    Some(pod) => pod,
                    None => {
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
                                FailedRecoveryPodAbsenceOutcome::ConvergedSuspended => {
                                    tracing::info!(
                                        sandbox_id = %sandbox_id,
                                        generation = row.generation,
                                        pod_name = %row.pod_name,
                                        "failed recovery pod is absent; converged current durable snapshot to suspended"
                                    )
                                }
                            }
                            return Ok(None);
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
                            return Ok(None);
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
                        return Ok(None);
                    }
                }
            }
        };

        let pod = if let Some(cached_phase) = terminal_pod_phase(&pod) {
            let authoritative_pod = fetch_sandbox_pod_uncached(&self.k8s, &row.pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate(
                        "failed to verify terminal Kubernetes sandbox pod",
                        err,
                    )
                })?;
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
            authoritative_pod.expect("non-terminal verification result requires an existing pod")
        } else {
            pod
        };

        if let Some(message) = pod.unschedulable_message() {
            return Err(SubstrateError::Unavailable {
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
        let pod_ip = assigned_pod_ip(&pod)?;
        let pod_ip_string = pod_ip.map(|ip| ip.to_string());
        if row.pod_uid.as_ref().map(PodUid::as_str) != pod_uid.as_deref()
            || row.pod_ip.as_deref() != pod_ip_string.as_deref()
        {
            match self
                .store
                .persist_pod_metadata(
                    sandbox_id,
                    row.generation,
                    pod_uid.as_deref(),
                    pod_ip_string.as_deref(),
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
        }

        Ok(pod_ip)
    }
}
