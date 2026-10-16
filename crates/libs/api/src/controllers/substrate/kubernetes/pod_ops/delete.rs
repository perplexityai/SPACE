//! Pod delete paths: throttling-aware deletes, UID-preconditioned blocking
//! deletes, lingering prior-generation pods, and delete-then-forget tracking.

use super::*;

pub(super) const K8S_THROTTLE_DELETE_RETRY_ATTEMPTS: usize = 3;

const K8S_THROTTLE_DELETE_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

const K8S_THROTTLE_DELETE_RETRY_MAX_JITTER: Duration = Duration::from_millis(75);

pub(super) async fn retry_kube_delete_throttling<T, Fut, F>(
    operation: &'static str,
    mut call: F,
) -> Result<T, KubeError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, KubeError>>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        match call().await {
            Ok(value) => return Ok(value),
            Err(err)
                if is_kube_api_throttling_error(&err)
                    && attempt < K8S_THROTTLE_DELETE_RETRY_ATTEMPTS =>
            {
                let jitter_ms = rand::rng().random_range(
                    0..=K8S_THROTTLE_DELETE_RETRY_MAX_JITTER
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                );
                let delay = K8S_THROTTLE_DELETE_RETRY_BASE_DELAY
                    * u32::try_from(attempt).unwrap_or(u32::MAX)
                    + Duration::from_millis(jitter_ms);
                tracing::warn!(
                    operation,
                    attempt,
                    max_attempts = K8S_THROTTLE_DELETE_RETRY_ATTEMPTS,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "retrying throttled Kubernetes pod delete"
                );
                sleep(delay).await;
            }
            Err(err) => return Err(err),
        }
    }
}

pub(super) async fn delete_sandbox_pod_with_throttle_retry(
    k8s: &KubernetesState,
    pod_name: &str,
    operation: &'static str,
) -> Result<(), KubeError> {
    retry_kube_delete_throttling(operation, || delete_sandbox_pod(k8s, pod_name)).await
}

/// Names of this sandbox's live pods from generations **strictly older** than
/// `claim_generation` — the pods a restore create must sweep before POSTing
/// its own. Matched by the sandbox-id + execution-generation annotations
/// stamped at create time. The strict generation bound is the safety
/// property: if this request's claim was superseded between its ownership
/// check and this sweep, the usurper's pod carries a generation >= ours and
/// is never selected — a name-only exclusion would delete it. Pods without a
/// parseable generation annotation are conservatively skipped.
pub(super) fn lingering_sandbox_pod_names<'a>(
    pods: impl IntoIterator<Item = &'a SandboxPodView>,
    sandbox_id: SandboxId,
    claim_generation: i64,
) -> Vec<String> {
    let sandbox_id = sandbox_id.to_string();
    pods.into_iter()
        .filter(|pod| {
            pod.annotation(SandboxPodKey::SandboxId) == Some(sandbox_id.as_str())
                && pod
                    .annotation(SandboxPodKey::ExecutionGeneration)
                    .and_then(|value| value.parse::<i64>().ok())
                    .is_some_and(|generation| generation < claim_generation)
        })
        .map(SandboxPodView::name)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

impl KubernetesSubstrate {
    /// Delete a sandbox pod and block until its deletion is observed, bounded
    /// by `k8s_blocking_delete_timeout`. Used on suspend so the control plane
    /// does not publish `suspended` while kubelet/containerd teardown of the
    /// old execution pod can still run later.
    ///
    /// In the healthy path this rides the shared sandbox-pod reflector: after
    /// an accepted `DELETE` it waits on the reflector delete signal
    /// (`PodEvents`) and then confirms absence with one authoritative GET. The
    /// GET is required because the filtered reflector store also drops terminal
    /// pods that can still exist behind a finalizer. If the object is still
    /// present, one final GET at the deadline decides the bounded outcome.
    pub(in crate::controllers::substrate::kubernetes) async fn k8s_blocking_delete_sandbox_pod_if_uid(
        &self,
        pod_name: &str,
        expected_uid: &PodUid,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(
            pod_name,
            "suspend",
            Some((expected_uid.as_str(), None)),
        )
        .await
    }

    /// Delete a terminal pod that is only being kept as stale projection
    /// carrier state, then wait until absence is observed.
    pub async fn delete_sandbox_pod_for_projection_cleanup(
        &self,
        pod_name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(pod_name, "projection_cleanup", None)
            .await
    }

    /// Blocking, absence-confirming delete of a lingering strictly-older-
    /// generation pod before a restore create. Name-scoped (no UID/RV
    /// precondition): selection is already generation-fenced
    /// ([`lingering_sandbox_pod_names`]) so the name targets exactly one prior
    /// generation, and — unlike the same-generation re-drive teardown — a
    /// bound/running older-generation pod is precisely the orphan hazard we
    /// must remove, so a precondition that stepped aside on a just-bound pod
    /// would defeat the fence. Confirms observed absence (delete signal +
    /// authoritative GET at the deadline) so the caller can make it a create
    /// prerequisite.
    pub(super) async fn delete_lingering_prior_generation_pod(
        &self,
        pod_name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(
            pod_name,
            "lingering_prior_generation",
            None,
        )
        .await
    }

    /// UID + resourceVersion preconditioned blocking delete for the cross-node
    /// re-drive: only deletes the pod if it is still in the exact state the
    /// caller inspected. The UID fences a same-name successor created by a
    /// concurrent re-drive; the resourceVersion fences a concurrent scheduler
    /// bind of the *same* pod (bind keeps the UID but bumps the RV). A
    /// precondition rejection surfaces as
    /// [`K8sBlockingDeleteOutcome::PreconditionFailed`].
    pub(super) async fn delete_sandbox_pod_for_redrive(
        &self,
        pod_name: &str,
        expected_uid: &str,
        expected_resource_version: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.k8s_blocking_delete_sandbox_pod_for_operation(
            pod_name,
            "redrive",
            Some((expected_uid, Some(expected_resource_version))),
        )
        .await
    }

    pub(in crate::controllers::substrate::kubernetes) async fn k8s_blocking_delete_sandbox_pod_for_operation(
        &self,
        pod_name: &str,
        metric_operation: &'static str,
        preconditions: Option<(&str, Option<&str>)>,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        let pods: Api<Pod> = Api::namespaced(self.k8s.client.clone(), &self.k8s.sandbox_namespace);
        let metric_started_at = std::time::Instant::now();
        let timeout_budget = self.k8s.k8s_blocking_delete_timeout;
        let deadline = Instant::now() + timeout_budget;
        let timeout_secs = timeout_budget.as_secs();
        let mut waited = false;

        // Capture whether the reflector already had this pod cached *before* the
        // delete. The pod is a running execution, so normally yes; seeding the
        // wait with this lets it confirm immediately if the reflector removes
        // the pod between the accepted DELETE and the wait starting (fast-delete
        // race) instead of treating the now-absent store as an ambiguous miss.
        let present_before_delete = self.lookup_sandbox_pod(pod_name).is_some();

        let delete_params = match preconditions {
            Some((uid, resource_version)) => DeleteParams {
                preconditions: Some(kube::api::Preconditions {
                    uid: Some(uid.to_string()),
                    resource_version: resource_version.map(str::to_string),
                }),
                ..Default::default()
            },
            None => DeleteParams::default(),
        };
        let result = async {
            match timeout_at(deadline, pods.delete(pod_name, &delete_params)).await {
                Ok(Ok(_)) => {}
                Ok(Err(err)) if is_not_found_error(&err) => {
                    return Ok(K8sBlockingDeleteOutcome::NotFound);
                }
                // A precondition rejection (409 Conflict) means the pod at this
                // name is no longer in the state the caller inspected —
                // replaced (UID) or mutated, e.g. bound (resourceVersion).
                Ok(Err(kube::Error::Api(response)))
                    if preconditions.is_some() && response.code == 409 =>
                {
                    return Ok(K8sBlockingDeleteOutcome::PreconditionFailed);
                }
                Ok(Err(err)) => {
                    return Err(K8sBlockingDeleteError::Delete {
                        pod_name: pod_name.to_string(),
                        source: Box::new(err),
                    });
                }
                Err(_) => {
                    return Err(K8sBlockingDeleteError::DeleteTimeout {
                        pod_name: pod_name.to_string(),
                        timeout_secs,
                    });
                }
            }

            // Healthy path: observe pod absence through the shared reflector,
            // driven by the delete signal — no apiserver GET while the watch
            // keeps up. The wait runs out the full budget in the unconfirmed
            // case (it confirms early on the observed present->absent
            // transition), so a pod still `Terminating` mid-budget but deleted
            // before the deadline is still confirmed and a cold cache doesn't
            // collapse to an immediate timeout.
            waited = true;
            let reflector_absence = pod_reflector::wait_for_pod_absence(
                &self.pod_store,
                &self.k8s.sandbox_namespace,
                &self.pod_events,
                pod_name,
                deadline,
                present_before_delete,
            )
            .await;

            if reflector_absence == pod_reflector::ReflectorAbsence::Confirmed {
                match timeout(
                    K8S_BLOCKING_DELETE_FALLBACK_GET_TIMEOUT,
                    fetch_sandbox_pod_uncached(&self.k8s, pod_name),
                )
                .await
                {
                    Ok(Ok(None)) => return Ok(K8sBlockingDeleteOutcome::Deleted),
                    Ok(Ok(Some(_))) | Ok(Err(_)) | Err(_) => {
                        // A terminal pod disappears from the filtered reflector
                        // before the Kubernetes object is necessarily deleted.
                        // Preserve the caller's remaining budget and let the
                        // authoritative deadline GET make the final decision.
                        tokio::time::sleep_until(deadline).await;
                    }
                }
            }

            // Single authoritative GET at the deadline: covers reflector lag /
            // watch failure so we don't publish `suspended` on a stale cache.
            // It carries its own short budget, so the operation can run up to
            // `K8S_BLOCKING_DELETE_FALLBACK_GET_TIMEOUT` past the deadline — the
            // intended "GET at the deadline" fallback. A timeout keeps the
            // execution active and makes the operation retryable, so this
            // bounded overrun is preferable to retiring an unconfirmed pod.
            match timeout(
                K8S_BLOCKING_DELETE_FALLBACK_GET_TIMEOUT,
                fetch_sandbox_pod_uncached(&self.k8s, pod_name),
            )
            .await
            {
                Ok(Ok(None)) => Ok(K8sBlockingDeleteOutcome::Deleted),
                Ok(Ok(Some(_))) => Ok(K8sBlockingDeleteOutcome::TimedOut),
                Ok(Err(err)) => {
                    tracing::warn!(
                        pod_name,
                        error = %err,
                        "failed to observe Kubernetes pod deletion after accepted delete"
                    );
                    Ok(K8sBlockingDeleteOutcome::ObserveError)
                }
                Err(_) => Ok(K8sBlockingDeleteOutcome::TimedOut),
            }
        }
        .await;

        let result_tag = result
            .as_ref()
            .map(|outcome| outcome.as_str())
            .unwrap_or_else(K8sBlockingDeleteError::result_tag);
        pplx_space_metrics::sandbox::api::k8s_blocking_delete_latency(
            "pod",
            metric_operation,
            result_tag,
            waited,
            metric_started_at,
        );

        result
    }

    pub(in crate::controllers::substrate::kubernetes) async fn delete_pod_and_forget_tracking(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        generation: i64,
        cause: PodTrackingCleanupCause,
    ) -> Result<(), SubstrateError> {
        match self
            .delete_sandbox_pod_for_projection_cleanup(pod_name)
            .await
        {
            Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {}
            Ok(K8sBlockingDeleteOutcome::TimedOut) => {
                return Err(SubstrateError::Timeout {
                    message: format!(
                        "timed out waiting for terminal Kubernetes pod {pod_name} deletion; \
                         keeping execution tracking"
                    ),
                    phase: None,
                });
            }
            Ok(
                K8sBlockingDeleteOutcome::ObserveError
                | K8sBlockingDeleteOutcome::PreconditionFailed,
            ) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "could not confirm terminal Kubernetes pod {pod_name} deletion; \
                         keeping execution tracking"
                    ),
                });
            }
            Err(err) => {
                return Err(SubstrateError::Failed {
                    message: format!("failed to delete terminal Kubernetes pod {pod_name}: {err}"),
                });
            }
        }
        self.store
            .forget_pod_tracking_if_generation(sandbox_id, generation, cause)
            .await?;
        Ok(())
    }
}
