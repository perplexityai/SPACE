use super::*;

/// Why is `CancelGuard` running its cleanup path? Lets `Drop` choose
/// between spawning a real pod-cleanup and deferring to the reconciler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::controllers::substrate::kubernetes) enum CancelCause {
    /// Caller dropped the future (e.g. HTTP client disconnected and axum
    /// dropped the handler). Pod was created but the request can no longer
    /// reach a normal completion path — must clean up.
    CallerDropped,
    /// Observed node-manager unreachable (`Code::Unavailable` / transport
    /// failure) while creating. Cleanup would just feed the saturated
    /// node-manager — defer to the reconciler GC instead. Cleanup of both
    /// the K8s pod and the `pod_tracking` DB row is the orphaned-sandbox
    /// reconciler's `OrphanKind::Normal` path
    /// (`crates/l40_api/cluster/controller/server/src/orphaned_sandboxes.rs`), which
    /// fires for `lifecycle='creating'` rows whose pod is missing past
    /// `CREATE_AGE_THRESHOLD_SECS`.
    NodeMgrUnreachable,
}

/// Guard that gives back what a `create_execution` call claimed when the call
/// does not reach a normal completion path — a failure exit, or the future
/// being cancelled (dropped), e.g. when the HTTP client disconnects and axum
/// drops the handler. Its scope follows what the call can prove:
///
/// - claim armed, no pod create sent: no pod can exist, so it releases the
///   claimed execution alone;
/// - pod create in flight: the apiserver may have accepted a pod this call
///   never learns, so it releases nothing and leaves the claim, which still
///   names that pod, to reconciliation;
/// - armed with the created pod: it retires the claim and deletes that pod by
///   UID.
pub(in crate::controllers::substrate::kubernetes) struct CancelGuard {
    sandbox_id: SandboxId,
    pod_name: String,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    pod_uid: Option<PodUid>,
    k8s: Option<KubernetesState>,
    store: Option<K8sStore>,
    /// Set once the pod create is sent and cleared once its pod arms the
    /// guard; while set, nothing is released.
    pod_create_in_flight: bool,
    cancel_tracker: Arc<CancelRateTracker>,
    /// The substrate's tracker: graceful shutdown waits for the background
    /// release or cleanup a dropped create leaves running.
    task_tracker: TaskTracker,
    cause: CancelCause,
}

impl CancelGuard {
    pub(in crate::controllers::substrate::kubernetes) fn new(
        sandbox_id: SandboxId,
        pod_name: String,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
        cancel_tracker: Arc<CancelRateTracker>,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            sandbox_id,
            pod_name,
            generation,
            activation_attempt_id,
            pod_uid: None,
            k8s: None,
            store: None,
            pod_create_in_flight: false,
            cancel_tracker,
            task_tracker,
            cause: CancelCause::CallerDropped,
        }
    }

    /// Arm the guard for the execution claim alone. Called right after the
    /// claim, so that every exit before the pod exists gives the claim back
    /// without touching Kubernetes.
    pub(in crate::controllers::substrate::kubernetes) fn arm_claim(&mut self, store: K8sStore) {
        self.store = Some(store);
    }

    /// Called just before the pod create is sent. From here until its
    /// response arrives, a pod this call cannot see may exist, and only the
    /// claim's execution row still names it — so the guard stops releasing.
    pub(in crate::controllers::substrate::kubernetes) fn begin_pod_create(&mut self) {
        self.pod_create_in_flight = true;
    }

    /// Widen the guard to the pod. Called after the pod is created, so that
    /// cancellation from here on also deletes the pod this call created.
    pub(in crate::controllers::substrate::kubernetes) fn arm(
        &mut self,
        k8s: KubernetesState,
        store: K8sStore,
        pod_uid: Option<PodUid>,
    ) {
        self.k8s = Some(k8s);
        self.store = Some(store);
        self.pod_uid = pod_uid;
        self.pod_create_in_flight = false;
    }

    /// Give back the claim on a failure exit before the pod create is sent.
    /// Awaited, so the caller's error is returned only after the claim is
    /// released. The caller deletes its fresh sandbox row on that error; if the
    /// delete fails or the request is cancelled, the row survives, and a later
    /// data-plane access, start or wake must find it unclaimed rather than
    /// join an activation that will never produce a pod. The guard keeps
    /// ownership until the release has finished: if this await is cancelled,
    /// its transaction rolls back and `Drop` runs the same fenced release in
    /// the background instead.
    ///
    /// Only valid before `begin_pod_create()` and `arm()`: past them the
    /// early return below drops the guard into the full `Drop` path.
    pub(in crate::controllers::substrate::kubernetes) async fn release_claim(mut self) {
        debug_assert!(
            self.k8s.is_none() && !self.pod_create_in_flight,
            "release_claim is only valid before the pod create is sent"
        );
        if self.k8s.is_some() || self.pod_create_in_flight {
            return;
        }
        if let Some(store) = self.store.clone() {
            release_claimed_execution(
                store,
                self.sandbox_id,
                self.generation,
                self.activation_attempt_id,
            )
            .await;
        }
        self.store = None;
    }

    /// Annotate the guard with the cause of the impending drop so `Drop`
    /// can choose between spawning a real cleanup and deferring to the
    /// reconciler. Caller is expected *not* to defuse after this — the
    /// `Drop` impl owns the decision.
    pub(in crate::controllers::substrate::kubernetes) fn mark_cause(&mut self, cause: CancelCause) {
        self.cause = cause;
    }

    pub(in crate::controllers::substrate::kubernetes) fn defuse(mut self) {
        self.k8s = None;
        self.store = None;
        self.pod_create_in_flight = false;
    }

    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) fn is_unarmed(&self) -> bool {
        self.k8s.is_none() && self.store.is_none()
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        let Some(k8s) = self.k8s.take() else {
            let Some(store) = self.store.take() else {
                return;
            };
            if self.pod_create_in_flight {
                // The apiserver may have accepted the pod. Retiring the claim
                // would drop the only record naming it, so leave the claim to
                // the reconcilers that age out a `creating` sandbox.
                tracing::warn!(
                    sandbox_id = %self.sandbox_id,
                    pod_name = %self.pod_name,
                    "create_execution cancelled while its pod create was in flight — leaving \
                     the claimed execution to reconciliation"
                );
                return;
            }
            // No pod create was sent, so there is nothing to delete and
            // nothing that reaches node-mgr: release the claim alone, and leave
            // the cancel-storm signal, which counts pod cleanups, untouched.
            tracing::warn!(
                sandbox_id = %self.sandbox_id,
                pod_name = %self.pod_name,
                "create_execution cancelled before its pod create was sent — releasing the \
                 claimed execution"
            );
            self.task_tracker.spawn(
                release_claimed_execution(
                    store,
                    self.sandbox_id,
                    self.generation,
                    self.activation_attempt_id,
                )
                .instrument(tracing::info_span!(
                    "substrate.k8s.cancel_release_claim",
                    sandbox_id = %self.sandbox_id,
                    pod_name = %self.pod_name,
                )),
            );
            return;
        };
        let store = self.store.take();
        let sandbox_id = self.sandbox_id;
        let pod_name = self.pod_name.clone();
        let generation = self.generation;
        let activation_attempt_id = self.activation_attempt_id;
        let pod_uid = self.pod_uid.take();

        // Always record the cancel — both branches contribute to the
        // backpressure signal that gates new creates upstream.
        self.cancel_tracker.record_cancel();

        if matches!(self.cause, CancelCause::NodeMgrUnreachable) {
            // Node-mgr unreachable is exactly when *not* to spawn cleanup:
            // it would just feed the saturated node-manager via the
            // sbx-mgr cleanup path. Reconciler will GC the pod.
            tracing::warn!(
                sandbox_id = %sandbox_id,
                pod_name = %pod_name,
                "create_execution cancelled (node-mgr unreachable) — deferring pod cleanup to reconciler"
            );
            crate::metrics::sandbox::api::create_cancel_cleanup_deferred();
            return;
        }

        tracing::error!(
            sandbox_id = %sandbox_id,
            pod_name = %pod_name,
            "create_execution cancelled — spawning background pod cleanup"
        );

        let cleanup_span = tracing::info_span!(
            "substrate.k8s.cancel_cleanup",
            sandbox_id = %sandbox_id,
            pod_name = %pod_name,
        );

        self.task_tracker.spawn(
            async move {
                let Some(store) = store else {
                    return;
                };
                let Some(pod_uid) = pod_uid else {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        "cancel cleanup: pod has no UID; leaving it for reconciliation"
                    );
                    return;
                };
                let retired = match store
                    .forget_pod_tracking_if_generation_and_attempt(
                        sandbox_id,
                        generation,
                        activation_attempt_id,
                    )
                    .await
                {
                    Ok(retired) => retired,
                    Err(err) => {
                        tracing::error!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            generation,
                            error = %err,
                            "cancel cleanup: failed to retire pod tracking"
                        );
                        return;
                    }
                };
                // The fenced retire matches nothing when another writer has
                // already retired this execution — for example the control
                // plane applying a node's activation-rollback receipt, which
                // retires the row as `replaced` without deleting the pod. The
                // pod is still this attempt's (its UID was recorded above), and
                // a retired generation never runs again, so delete it here;
                // otherwise kubelet retries its sandbox create against the
                // retired execution indefinitely. When the row is still active
                // (a newer attempt owns it), leave the pod alone.
                if !retired {
                    let already_retired = match store
                        .execution_generation_retired(sandbox_id, generation, &pod_name)
                        .await
                    {
                        Ok(already_retired) => already_retired,
                        Err(err) => {
                            tracing::error!(
                                sandbox_id = %sandbox_id,
                                pod_name = %pod_name,
                                generation,
                                error = %err,
                                "cancel cleanup: failed to read execution retirement; \
                                 leaving the pod for reconciliation"
                            );
                            return;
                        }
                    };
                    if !already_retired {
                        tracing::info!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            generation,
                            "cancel cleanup: execution is owned by a newer activation; \
                             leaving the pod"
                        );
                        return;
                    }
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        generation,
                        "cancel cleanup: execution was already retired; deleting its pod"
                    );
                }
                match delete_sandbox_pod_if_uid(&k8s, &pod_name, &pod_uid).await {
                    Ok(()) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            "cancel cleanup: sandbox pod delete issued successfully"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            sandbox_id = %sandbox_id,
                            pod_name = %pod_name,
                            error = %err,
                            "cancel cleanup: failed to delete sandbox pod — pod may be orphaned"
                        );
                    }
                }
            }
            .instrument(cleanup_span),
        );
    }
}

/// Retire the execution a create claimed, fenced on its generation and
/// activation attempt, when the create gives up before sending its pod
/// create. This is the release the pod-create failure arm of
/// `create_execution` runs, from the same database state. Fenced and
/// idempotent: a repeat, or a release racing a newer execution, changes
/// nothing.
async fn release_claimed_execution(
    store: K8sStore,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) {
    if let Err(error) = store
        .forget_pod_tracking_if_generation_and_attempt(
            sandbox_id,
            generation,
            activation_attempt_id,
        )
        .await
    {
        tracing::warn!(
            sandbox_id = %sandbox_id,
            generation,
            error = %error,
            "failed to release the Kubernetes execution claimed by a create that ended before \
             its pod; leaving it to sandbox cleanup"
        );
    }
}
