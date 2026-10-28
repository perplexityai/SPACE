//! Cross-node redrive of an unschedulable restore: scheduling-outcome waits,
//! the redrive claim and metric guards, and precondition rereads.

use super::*;

/// Result of a cross-node re-drive attempt on an `Unschedulable` source-pinned
/// restore pod.
pub(in crate::controllers::substrate::kubernetes) enum RedriveOutcome {
    /// The pinned pod was confirmed unschedulable, torn down, and a source-less
    /// successor was created and persisted under the same generation.
    Redriven(Arc<SandboxPodView>),
    /// No successor from this attempt: the authoritative read found the pod
    /// bound after all (the caller's `Unschedulable` verdict was stale) or
    /// already replaced by a concurrent re-drive's successor (required pin
    /// absent), or a concurrent re-drive's successor POST won (this
    /// attempt's POST hit 409). The live pod drives the restore.
    /// `cross_node` distinguishes a retained cross-node successor / fallback
    /// bind from the original pinned pod binding on the source — the caller's
    /// placement metric must reflect what's running.
    PodRetained { cross_node: bool },
    /// Materialization ownership wasn't confirmed mid-flight: the claim reset
    /// or the successor's persist matched no row this attempt owns. Any
    /// successor this attempt created was discarded.
    OwnershipLost,
}

/// The `outcome` tag a finished re-drive attempt reports on
/// `sandbox.restore.redrive`.
fn redrive_metric_outcome(
    result: &Result<RedriveOutcome, SubstrateError>,
) -> pplx_space_metrics::sandbox::restore::RedriveOutcome {
    use pplx_space_metrics::sandbox::restore::RedriveOutcome as Metric;
    match result {
        Ok(RedriveOutcome::Redriven(_)) => Metric::Redriven,
        Ok(RedriveOutcome::PodRetained { .. }) => Metric::Retained,
        Ok(RedriveOutcome::OwnershipLost) => Metric::OwnershipLost,
        Err(_) => Metric::Failed,
    }
}

/// Emits exactly one `sandbox.restore.redrive` sample per re-drive attempt:
/// the recorded outcome when the attempt finishes, `dropped` when its future
/// is dropped (or unwinds) first.
struct RedriveMetricGuard {
    path: pplx_space_metrics::sandbox::restore::RedrivePath,
    outcome: Option<pplx_space_metrics::sandbox::restore::RedriveOutcome>,
}

impl Drop for RedriveMetricGuard {
    fn drop(&mut self) {
        pplx_space_metrics::sandbox::restore::cross_node_redrive(
            self.path,
            self.outcome
                .unwrap_or(pplx_space_metrics::sandbox::restore::RedriveOutcome::Dropped),
        );
    }
}

/// Classification of a pod re-read after its UID+resourceVersion preconditioned
/// re-drive delete was rejected with 409. A conflict only proves the RV moved;
/// this decides what actually changed so the re-drive can never retain a pod
/// that's gone (→ no successor) or bounce a still-impossible pod to the caller.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PreconditionReread {
    /// No pod at the name. A conflict does not imply a successor was created —
    /// a concurrent cleanup may have removed the pod. The re-drive must fall
    /// through to its successor POST (whose 409 handling resolves a concurrent
    /// creator), NOT retain a deleted pod.
    Absent,
    /// A live pod survives and drives the restore: bound (`cross_node` set by
    /// landing node vs the caller's source) or unpinned (a concurrent re-drive
    /// already installed a cross-node successor). Retain it.
    Retain { cross_node: bool },
    /// The same required-pinned pod is still unschedulable/undecided; only a
    /// status write bumped the RV. No concurrent winner is driving a successor,
    /// so retry the preconditioned delete with this fresh identity.
    RetryDelete {
        uid: String,
        resource_version: String,
    },
    /// Still required-pinned but the apiserver exposed no UID/RV to precondition
    /// on (not expected from a live GET). Defer rather than POST past a pod that
    /// may still exist.
    Indeterminate,
}

pub(super) fn classify_precondition_reread(
    now_live: Option<&SandboxPodView>,
    caller_source_node: Option<&str>,
) -> PreconditionReread {
    let Some(pod) = now_live else {
        return PreconditionReread::Absent;
    };
    match scheduling_outcome_of(pod) {
        // Bound: the pin landed after all. A shortlist pod keeps its required
        // pin whether it bound on the source or a fallback, so classify by the
        // landing node, not pin-presence.
        Some(SchedulingOutcome::Scheduled { node_name }) => PreconditionReread::Retain {
            cross_node: Some(node_name.as_str()) != caller_source_node,
        },
        // Not bound, pin gone: a concurrent re-drive's cross-node successor runs.
        _ if !pod.has_required_source_pin() => PreconditionReread::Retain { cross_node: true },
        // Not bound, still pinned: status-only RV bump. Retry with fresh identity.
        _ => match pod
            .uid()
            .map(str::to_owned)
            .zip(pod.resource_version().map(str::to_owned))
        {
            Some((uid, resource_version)) => PreconditionReread::RetryDelete {
                uid,
                resource_version,
            },
            None => PreconditionReread::Indeterminate,
        },
    }
}

/// Cancellation guard for the re-drive's post-delete window. The pinned pod
/// is already gone; if the request future is dropped (client disconnect,
/// shutdown) before the successor is persisted, a still-`creating` claim
/// would have nothing behind it and every retry would take the
/// create-in-flight early return until the 300s stale-create age expires.
/// Unless disarmed on normal completion, `Drop` spawns a best-effort recovery:
///
/// 1. Retire the claim. The retirement CASes on `creating + pod_uid IS NULL +
///    pod_ip IS NULL`, so a row that was materialized before the drop (the
///    persist won the race with the cancellation) is never touched.
/// 2. **Only if the retirement actually fired**, delete the same-name pod: the
///    cancellation may have hit after the successor POST succeeded, and
///    retiring the claim without removing that pod would leave a schedulable
///    pod with no active execution behind it. A no-op retirement means the
///    row owns whatever pod exists — leave it alone.
///
/// Both steps are additionally fenced on the activation-attempt identity and,
/// when present, the restore lease. A dropped leader can outlive its lease and
/// execution generations are reused by retries; either fence changing makes
/// the delayed cleanup a no-op before it can retire the successor's claim.
struct RedriveClaimGuard {
    handles: Option<(K8sStore, KubernetesState)>,
    span: tracing::Span,
    sandbox_id: SandboxId,
    pod_name: String,
    generation: i64,
    restore_lease_token: Option<Uuid>,
    activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
}

impl RedriveClaimGuard {
    /// Always armed: the re-drive constructs this only after resetting the
    /// execution row to `creating`, so the retirement CAS in `Drop` matches
    /// regardless of the (possibly stale `Created`) in-memory `claim.row.state`
    /// on the retry-recovery path. Arming off that stale state would leave a
    /// cancellation after the successor POST unable to clean the orphan up.
    fn armed(
        store: &K8sStore,
        k8s: &KubernetesState,
        sandbox_id: SandboxId,
        claim: &SandboxExecutionClaim,
        restore_lease_token: Option<Uuid>,
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    ) -> Self {
        Self {
            handles: Some((store.clone(), k8s.clone())),
            span: tracing::Span::current(),
            sandbox_id,
            pod_name: claim.row.pod_name.clone(),
            generation: claim.row.generation,
            restore_lease_token,
            activation_attempt_id,
        }
    }

    fn disarm(&mut self) {
        self.handles = None;
    }
}

impl Drop for RedriveClaimGuard {
    fn drop(&mut self) {
        let Some((store, k8s)) = self.handles.take() else {
            return;
        };
        let sandbox_id = self.sandbox_id;
        let generation = self.generation;
        let restore_lease_token = self.restore_lease_token;
        let activation_attempt_id = self.activation_attempt_id;
        let pod_name = std::mem::take(&mut self.pod_name);
        tracing::warn!(
            sandbox_id = %sandbox_id,
            generation,
            "re-drive dropped mid-flight; retiring creating claim in background"
        );
        let span = tracing::info_span!(parent: &self.span, "substrate.wake.redrive_cleanup", sandbox_id = %sandbox_id, generation);
        tokio::spawn(
            async move {
                let retired = match store
                    .abandon_unmaterialized_create_claim_by_generation(
                        sandbox_id,
                        generation,
                        restore_lease_token,
                        activation_attempt_id,
                    )
                    .await
                {
                    Ok(retired) => retired,
                    Err(err) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            generation,
                            error = %err,
                            "failed to retire creating claim after cancelled re-drive"
                        );
                        return;
                    }
                };
                if !retired {
                    // The persist won: the row owns the pod. Nothing to clean.
                    return;
                }
                // The claim was still unmaterialized — any same-name pod is this
                // generation's successor POSTed just before the drop; without an
                // active execution it must not run.
                if let Err(err) = delete_sandbox_pod_with_throttle_retry(
                    &k8s,
                    &pod_name,
                    "redrive_cancel_cleanup",
                )
                .await
                    && !is_not_found_error(&err)
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        pod_name = %pod_name,
                        generation,
                        error = %err,
                        "failed to delete orphaned successor after cancelled re-drive"
                    );
                }
            }
            .instrument(span),
        );
    }
}

impl KubernetesSubstrate {
    /// Watch a freshly-created source-pinned restore pod for the scheduler's
    /// first placement decision, bounded by `budget`. Returns as soon as the
    /// pod is bound (`spec.nodeName` set) or marked `Unschedulable`, riding the
    /// shared reflector's event signal (no apiserver polling). A required
    /// single-node pin resolves in one scheduling cycle, so the common cases
    /// return well under `budget`. On budget exhaustion it does one
    /// authoritative apiserver GET; a pod that is *still undecided* (or gone)
    /// returns `None`. Undecided is deliberately NOT coerced to
    /// `Unschedulable`: only an explicit `PodScheduled=False/Unschedulable`
    /// verdict justifies tearing down the pin. A scheduler that is slow or
    /// wedged delays unpinned pods just the same, so re-driving cross-node on
    /// "no decision yet" would burn the locality this feature exists to keep
    /// while gaining nothing — the caller instead proceeds to the readiness
    /// wait, and retries / the transient-lifecycle sweeper bound a pod that
    /// never gets a decision.
    pub(super) async fn await_scheduling_outcome(
        &self,
        pod_name: &str,
        budget: Duration,
    ) -> Result<Option<SchedulingOutcome>, SubstrateError> {
        let deadline = Instant::now() + budget;
        // Against a lagging scheduler this loop re-checks about every
        // `REFLECTOR_WAIT_BUDGET`, plus once per event for this pod; each check
        // is a store lookup plus an `Arc` bump, and the classifier reads only
        // the view's node name and unschedulable verdict.
        loop {
            if let Some(pod) = self.lookup_sandbox_pod(pod_name)
                && let Some(outcome) = scheduling_outcome_of(&pod)
            {
                return Ok(Some(outcome));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let _ = self
                .pod_events
                .wait_for(pod_name, remaining.min(REFLECTOR_WAIT_BUDGET))
                .await;
        }
        let pod = fetch_sandbox_pod_uncached(&self.k8s, pod_name)
            .await
            .map_err(|err| {
                kube_error_for_substrate("failed to read pod while awaiting scheduling", err)
            })?;
        Ok(pod.as_deref().and_then(scheduling_outcome_of))
    }

    /// [`Self::redrive_unschedulable_restore_cross_node`], recording the
    /// attempt's outcome on `sandbox.restore.redrive`. Both call sites go
    /// through here so the counter sees every attempt, including the ones
    /// that return before any placement sample is emitted.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn redrive_unschedulable_restore_cross_node_recorded(
        &self,
        path: pplx_space_metrics::sandbox::restore::RedrivePath,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        claim_lifecycle: SandboxLifecycle,
        caller_source_node: Option<&str>,
        suspend_restore_storage_uri: Option<&str>,
        restore_lease_token: Option<Uuid>,
    ) -> Result<RedriveOutcome, SubstrateError> {
        let mut metric = RedriveMetricGuard {
            path,
            outcome: None,
        };
        let result = self
            .redrive_unschedulable_restore_cross_node(
                sandbox,
                claim,
                claim_lifecycle,
                caller_source_node,
                suspend_restore_storage_uri,
                restore_lease_token,
            )
            .await;
        metric.outcome = Some(redrive_metric_outcome(&result));
        result
    }

    /// Tear down a source-pinned restore pod that came back `Unschedulable` and
    /// re-create it cross-node under the same generation. Shared by the
    /// fresh-create watch and the retry path: an interrupted create can leave a
    /// pinned pod that a retry would otherwise reuse forever, so the retry
    /// re-drives it here instead.
    ///
    /// Two safety properties are enforced here rather than at the call sites:
    ///
    /// * **The destructive delete never trusts the reflector.** Both callers
    ///   observed `Unschedulable` (possibly from cache); an authoritative GET
    ///   re-checks the live pod first, and a pod that has since bound is left
    ///   alone (`PodRetained`). Reflector staleness is unbounded, so a
    ///   cached verdict alone could tear down a pod whose VM is already
    ///   restoring on the source. After the GET only the benign ms-scale bind
    ///   race remains: a pod bound in that window has no IP/VM yet, and the
    ///   blocking delete removes it before the successor is created.
    /// * **Source provenance survives the re-drive.** The successor keeps the
    ///   wedged pod's `suspend-restore-source-node` annotation (the node whose
    ///   local bytes are trusted) while rendering **no** scheduling affinity.
    ///   Dropping it entirely would poison a later retry: `suspend_restore_plan`
    ///   prefers the live pod's annotations, and without them it falls back to
    ///   `restore_source_node_name`, which reads placement — by then the
    ///   *destination* node — so sbx-mgr there could trust stale local bytes of
    ///   an older restore instead of the selected snapshot URI. When the pod is
    ///   already gone (a concurrent re-drive deleted it first, but this request
    ///   wins the successor POST), `caller_source_node` — the caller's plan
    ///   source, derived while the pinned pod still existed — is the fallback
    ///   provenance, so that interleaving cannot strip the annotation either.
    ///
    /// The blocking delete confirms absence before the same-name re-POST so it
    /// can't 409.
    async fn redrive_unschedulable_restore_cross_node(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        claim_lifecycle: SandboxLifecycle,
        caller_source_node: Option<&str>,
        suspend_restore_storage_uri: Option<&str>,
        restore_lease_token: Option<Uuid>,
    ) -> Result<RedriveOutcome, SubstrateError> {
        let pod_name = &claim.row.pod_name;
        let activation_attempt_id = claim.row.activation_attempt_id;
        let live = fetch_sandbox_pod_uncached(&self.k8s, pod_name)
            .await
            .map_err(|err| {
                kube_error_for_substrate("failed to re-verify pod before cross-node re-drive", err)
            })?;
        // Tear down only the pinned predecessor the caller believed it saw:
        // a bound pod (stale caller verdict) or a pod without the required pin
        // (a concurrent re-drive already replaced it with a same-name
        // successor) is left alone.
        let live_identity = match live.as_ref() {
            Some(pod)
                if matches!(
                    scheduling_outcome_of(pod),
                    Some(SchedulingOutcome::Scheduled { .. })
                ) || !pod.has_required_source_pin() =>
            {
                tracing::info!(
                    sandbox_id = %sandbox.id,
                    pod_name = %pod_name,
                    generation = claim.row.generation,
                    "live pod bound or already re-driven before teardown; leaving it in place"
                );
                // Bound: classify by the landing node — a shortlist pod keeps
                // its required pin whether it bound on the source or a
                // fallback, so pin-presence alone cannot distinguish them.
                // Unbound with the pin absent: a concurrent re-drive's
                // cross-node successor is running instead.
                let cross_node = match scheduling_outcome_of(pod) {
                    Some(SchedulingOutcome::Scheduled { node_name }) => {
                        Some(node_name.as_str()) != caller_source_node
                    }
                    _ => !pod.has_required_source_pin(),
                };
                return Ok(RedriveOutcome::PodRetained { cross_node });
            }
            Some(pod) => pod
                .uid()
                .map(str::to_owned)
                .zip(pod.resource_version().map(str::to_owned)),
            None => None,
        };
        // The wedged pod's annotation is the authoritative source-node
        // provenance; the DB derivation is placement-based and unreliable once
        // pods start moving. When the pod is already gone (concurrent delete),
        // fall back to the caller's plan source — derived while the pinned pod
        // still carried the annotation — so a successor this request creates
        // never ships without provenance.
        let source_node_annotation: Option<String> = live
            .as_ref()
            .and_then(|pod| pod.annotation(SandboxPodKey::SuspendRestoreSourceNode))
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| caller_source_node.map(str::to_string));
        // UID + resourceVersion preconditioned delete: it can only remove the
        // exact pod *state* inspected above. A same-name successor created in
        // the GET->DELETE window has a different UID; a concurrent scheduler
        // bind of the SAME pod keeps the UID but bumps the resourceVersion —
        // both are rejected with a 409 precondition failure instead of tearing
        // down a pod that may already be starting its VM. When the GET found no
        // pod there is nothing of ours to tear down and **no delete runs at
        // all**; if a successor appears before our POST, the 409 step-aside
        // below retains it.
        if let Some(mut delete_identity) = live_identity {
            let mut precondition_retries = 0usize;
            loop {
                // Clone the identity for this attempt so the RetryDelete arm can
                // reassign `delete_identity` without aliasing a borrow held
                // across the `.await`. Bounded, so the clone cost is negligible.
                let (uid, resource_version) = delete_identity.clone();
                match self
                    .delete_sandbox_pod_for_redrive(pod_name, &uid, &resource_version)
                    .await
                {
                    Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {
                        break;
                    }
                    Ok(K8sBlockingDeleteOutcome::PreconditionFailed) => {
                        // The pod changed under us: replaced, bound, or a
                        // status-only RV bump. Re-read once and classify; never
                        // tear down blind on a conflict.
                        let now_live = fetch_sandbox_pod_uncached(&self.k8s, pod_name)
                            .await
                            .map_err(|err| {
                                kube_error_for_substrate(
                                    "failed to re-classify pod after delete precondition \
                                         rejection",
                                    err,
                                )
                            })?;
                        match classify_precondition_reread(now_live.as_deref(), caller_source_node)
                        {
                            // Absent: a conflict does not prove a successor
                            // exists. Fall through to the successor POST (its
                            // own 409 handling steps aside for a real creator)
                            // rather than retain a deleted pod.
                            PreconditionReread::Absent => {
                                tracing::info!(
                                    sandbox_id = %sandbox.id,
                                    pod_name = %pod_name,
                                    generation = claim.row.generation,
                                    "pinned pod gone after preconditioned delete rejection; \
                                     re-creating cross-node successor"
                                );
                                break;
                            }
                            // A live pod (bound, or an unpinned concurrent
                            // successor) drives the restore.
                            PreconditionReread::Retain { cross_node } => {
                                tracing::info!(
                                    sandbox_id = %sandbox.id,
                                    pod_name = %pod_name,
                                    generation = claim.row.generation,
                                    cross_node,
                                    "pod bound or already re-driven under preconditioned delete; \
                                     leaving it in place"
                                );
                                return Ok(RedriveOutcome::PodRetained { cross_node });
                            }
                            // Same required-pinned pod, only the RV moved (a
                            // status write). Retry the preconditioned delete
                            // with the fresh identity — do NOT bounce the caller
                            // back onto the same impossible pod.
                            PreconditionReread::RetryDelete {
                                uid,
                                resource_version,
                            } => {
                                precondition_retries += 1;
                                if precondition_retries >= REDRIVE_PRECONDITION_RETRY_ATTEMPTS {
                                    tracing::warn!(
                                        sandbox_id = %sandbox.id,
                                        pod_name = %pod_name,
                                        generation = claim.row.generation,
                                        precondition_retries,
                                        "deferring cross-node re-drive: unschedulable source-pinned \
                                         pod kept changing resourceVersion under preconditioned \
                                         delete; retry"
                                    );
                                    return Err(SubstrateError::Unavailable {
                                        message: format!(
                                            "deferring restore: unschedulable source-pinned pod \
                                             {pod_name} for sandbox {} kept changing \
                                             resourceVersion; retry",
                                            sandbox.id
                                        ),
                                    });
                                }
                                delete_identity = (uid, resource_version);
                                continue;
                            }
                            PreconditionReread::Indeterminate => {
                                return Err(SubstrateError::Unavailable {
                                    message: format!(
                                        "deferring restore: unschedulable source-pinned pod \
                                         {pod_name} for sandbox {} exposed no identity to \
                                         precondition on; retry",
                                        sandbox.id
                                    ),
                                });
                            }
                        }
                    }
                    outcome => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %pod_name,
                            generation = claim.row.generation,
                            ?outcome,
                            "deferring in-request cross-node re-drive: unschedulable source-pinned \
                             pod not confirmed gone; retry"
                        );
                        return Err(SubstrateError::Unavailable {
                            message: format!(
                                "deferring restore: unschedulable source-pinned pod {pod_name} for \
                                 sandbox {} not confirmed gone; retry",
                                sandbox.id
                            ),
                        });
                    }
                }
            }
        }

        // The pinned pod's UID was persisted before the scheduler ruled it
        // Unschedulable, so the row is `created` (pod_ip NULL) even though that
        // pod is now deleted. Reset it to `creating` before building the
        // successor: the cleanup below (guard + failure handler) retires only a
        // `creating` claim, so this reset is what makes a drop or failure
        // mid-re-drive tear the successor down instead of stranding it. It is a
        // hard prerequisite, not best-effort — proceed only on a confirmed
        // reset:
        //   * `Err`: the row is still `created`; POSTing a successor now would
        //     reopen the orphan gap, so fail retryably before creating one.
        //   * `Ok(false)`: a concurrent winner already materialized the row, or
        //     this attempt lost the restore-lease fence — step aside.
        match self
            .store
            .reset_active_execution_to_creating_if_generation(
                sandbox.id,
                claim.row.generation,
                restore_lease_token,
                activation_attempt_id,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!(
                    sandbox_id = %sandbox.id,
                    pod_name = %pod_name,
                    generation = claim.row.generation,
                    "cross-node re-drive stepping aside: create-claim reset matched no \
                     unmaterialized row (materialized by a concurrent winner or lease lost)"
                );
                return Ok(RedriveOutcome::OwnershipLost);
            }
            Err(err) => return Err(err),
        }

        // Everything past the confirmed delete + reset runs under one failure
        // handler: the pinned pod is gone and the row is `creating`, so an error
        // below (context rebuild, POST, persist) would otherwise leave a
        // `creating` claim with no pod behind it — and every subsequent ensure
        // call would take the create-in-flight early return until the stale-
        // create age (300s) expires. On failure the unmaterialized claim is
        // retired so an immediate retry re-creates instead of stalling. The
        // guard covers the path the failure handler cannot: the request future
        // being *dropped* (client disconnect) mid-block, which skips every
        // handler below. Always armed — the reset just guaranteed `creating`,
        // so the retirement CAS matches even when `claim.row.state` is a stale
        // `Created` from the retry-recovery path.
        let mut claim_guard = RedriveClaimGuard::armed(
            &self.store,
            &self.k8s,
            sandbox.id,
            claim,
            restore_lease_token,
            activation_attempt_id,
        );
        let redrive_result: Result<RedriveOutcome, SubstrateError> = async {
            // Build the successor context directly: keep the portable S3
            // snapshot and the source-node annotation (provenance for later
            // retries and for sbx-mgr's local-bytes trust check), but render no
            // *source-node* affinity — no pin, no shortlist, no preference — so
            // the successor schedules anywhere the boundary allows. It does
            // still carry the node-group boundary once that gate is on, which
            // is the point: "anywhere" means anywhere in the candidate set, not
            // anywhere in the fleet.
            // `parse_pod_create_context` can't express "source as annotation
            // only" — it always pairs a known source with a placement pin.
            let cross_node_context = PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_storage_uri,
                suspend_restore_source_node: source_node_annotation.as_deref(),
                direct_node_name: None,
                preferred_node_name: None,
            });
            let auto_lifecycle = self
                .store
                .auto_lifecycle_policy_after_claim(sandbox.id)
                .await?;
            let auto_lifecycle_json =
                serde_json::to_string(&auto_lifecycle).map_err(|err| SubstrateError::Failed {
                    message: format!("failed to serialize auto lifecycle policy: {err}"),
                })?;
            let network_policy = self.effective_network_policy_for_sandbox(sandbox).await?;
            let network_policy_json =
                serde_json::to_string(&network_policy).map_err(|err| SubstrateError::Failed {
                    message: format!("failed to serialize network policy: {err}"),
                })?;
            ensure_network_policy_annotation_size(&network_policy_json)?;
            let sandbox_owner = self.store.sandbox_owner_for_user(sandbox.user_id);
            // Inside the guard's block on purpose: a resolution failure here
            // retires the reset claim through `RedriveClaimGuard` rather than
            // leaving a `creating` row with no pod behind it.
            let node_group_placement = self
                .resolve_node_group_placement(sandbox, &cross_node_context)
                .await?;

            let cross_node_pod = match create_sandbox_pod(
                &self.k8s,
                self.sandbox_pod_request(
                    sandbox,
                    &claim.row,
                    SandboxPodRequestInputs {
                        sandbox_owner: &sandbox_owner,
                        auto_lifecycle_json: &auto_lifecycle_json,
                        network_policy_json: &network_policy_json,
                        node_group_placement: node_group_placement.placement(),
                    },
                    cross_node_context,
                ),
            )
            .await
            {
                Ok(pod) => pod,
                // A concurrent re-drive won the POST race — its successor owns
                // the name. Step aside rather than fail: the failure handler
                // below would retire the shared still-unmaterialized claim,
                // making the winner's persist no-op and the winner delete its
                // own freshly-created pod.
                Err(kube::Error::Api(response)) if response.code == 409 => {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        "concurrent re-drive already created the successor; leaving it in place"
                    );
                    return Ok(RedriveOutcome::PodRetained { cross_node: true });
                }
                Err(err) => {
                    return Err(SubstrateError::Failed {
                        message: format!("failed to re-create restore pod cross-node: {err}"),
                    });
                }
            };

            match self
                .store
                .persist_materialized_pod_metadata(
                    sandbox.id,
                    claim.row.generation,
                    claim_lifecycle,
                    cross_node_pod.uid(),
                    None,
                )
                .await
            {
                Ok(Some(_)) => Ok(RedriveOutcome::Redriven(Arc::new(cross_node_pod))),
                Ok(None) => {
                    // Materialization ownership was revoked between the two
                    // creates; drop the cross-node pod we just made and let the
                    // winner drive.
                    self.delete_pod_and_forget_tracking(
                        sandbox.id,
                        pod_name,
                        claim.row.generation,
                        PodTrackingCleanupCause::ExplicitTeardown,
                    )
                    .await?;
                    Ok(RedriveOutcome::OwnershipLost)
                }
                Err(error) => {
                    // Mirror the fresh-create path: never leave an untracked
                    // pod behind a failed persist.
                    if let Err(delete_error) = delete_sandbox_pod_with_throttle_retry(
                        &self.k8s,
                        pod_name,
                        "delete_pod_after_redrive_persistence_failure",
                    )
                    .await
                        && !is_not_found_error(&delete_error)
                    {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %pod_name,
                            generation = claim.row.generation,
                            error = %delete_error,
                            "failed to delete cross-node restore pod after persistence failure"
                        );
                    }
                    Err(error)
                }
            }
        }
        .await;
        // Reached only when the block ran to completion; a drop mid-block
        // leaves the guard armed and its `Drop` retires the claim.
        claim_guard.disarm();

        match redrive_result {
            Ok(outcome) => {
                if matches!(outcome, RedriveOutcome::Redriven(_)) {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        "source-pinned restore was Unschedulable; re-drove cross-node in-request"
                    );
                }
                Ok(outcome)
            }
            Err(err) => {
                // The pinned pod is deleted, any successor was torn down inside
                // the block, and the reset left the row `creating` — retire it
                // (best-effort) so the next retry re-creates immediately instead
                // of stalling on the create-in-flight early return. The
                // retirement CASes on `creating + pod_uid IS NULL + pod_ip IS
                // NULL`, so a row a concurrent attempt already materialized is
                // never touched.
                if let Err(abandon_error) = self
                    .store
                    .abandon_unmaterialized_create_claim(sandbox.id, claim)
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        generation = claim.row.generation,
                        error = %abandon_error,
                        "failed to retire creating claim after cross-node re-drive failure"
                    );
                }
                Err(err)
            }
        }
    }
}
