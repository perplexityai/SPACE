//! Pod create path: `ensure_pod_exists` and its create-claim and stale-pod
//! helpers.

use super::*;

/// Derive the per-restore placement decision a [`PodCreateContext`] renders,
/// for the `sandbox.restore.placement` metric. `None` for non-restore creates
/// (on-demand / pooled), which carry no placement decision. `hard_pin` is the
/// `restore_source_node_hard_pin` config flag, which distinguishes a required
/// `matchFields` pin from the legacy preferred term for the same inputs.
pub(super) fn restore_placement_sample(
    context: &PodCreateContext<'_>,
    hard_pin: bool,
) -> Option<pplx_space_metrics::sandbox::restore::PlacementSample> {
    use pplx_space_metrics::sandbox::restore::{PlacementSample, RestorePlacement, SourcePin};
    match context {
        PodCreateContext::OnDemand => None,
        // Evacuation deliberately drops the source pin: always cross-node.
        PodCreateContext::Restore(RestoreContext::Evacuation { .. }) => Some(PlacementSample {
            placement: RestorePlacement::CrossNode,
            local_snapshot_present: false,
            pin: SourcePin::None,
        }),
        PodCreateContext::Restore(RestoreContext::UserRestore {
            direct_node_name,
            preferred_node_name,
            suspend_restore_source_node,
            ..
        }) => {
            let (placement, pin) = if direct_node_name.is_some() {
                (RestorePlacement::SameNode, SourcePin::NodeName)
            } else if preferred_node_name.is_some() {
                let pin = if hard_pin {
                    SourcePin::RequiredAffinity
                } else {
                    SourcePin::PreferredAffinity
                };
                (RestorePlacement::SameNode, pin)
            } else {
                (RestorePlacement::CrossNode, SourcePin::None)
            };
            Some(PlacementSample {
                placement,
                local_snapshot_present: suspend_restore_source_node.is_some(),
                pin,
            })
        }
    }
}

impl KubernetesSubstrate {
    pub(in crate::controllers::substrate::kubernetes) async fn require_observed_create_claim_in_flight(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
    ) -> Result<(), SubstrateError> {
        let sandbox_id = sandbox.id;
        if self
            .store
            .active_execution_is_generation(sandbox_id, claim.row.generation)
            .await?
        {
            return Ok(());
        }

        tracing::warn!(
            sandbox_id = %sandbox.id,
            generation = claim.row.generation,
            pod_name = %claim.row.pod_name,
            "observed Kubernetes pod create claim was replaced before pod creation"
        );
        Err(SubstrateError::Timeout {
            message: format!(
                "observed Kubernetes pod create claim for sandbox {} generation {} was replaced \
                 before pod creation; retry the sandbox operation to converge",
                sandbox.id, claim.row.generation
            ),
            phase: None,
        })
    }

    async fn stale_pod_reason_for_execution(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        pod: &SandboxPodView,
        suspend_restore_storage_uri: Option<&str>,
    ) -> Result<Option<StalePodReason>, SubstrateError> {
        // Pod phase and restore annotations do not identify a STOP episode.
        // A Running successor may have booted successfully before its owner
        // lost the lease or response; retain it so START can finish publication.
        if sandbox.lifecycle.parse::<SandboxLifecycle>() == Ok(SandboxLifecycle::Stopped)
            && self
                .store
                .execution_predates_current_stop(sandbox.id, claim.row.generation)
                .await?
        {
            return Ok(Some(StalePodReason::SandboxStopped));
        }
        Ok(stale_existing_pod_reason(
            sandbox,
            pod,
            suspend_restore_storage_uri,
        ))
    }

    async fn cleanup_stale_existing_pod_after_create_error(
        &self,
        sandbox: &Sandbox,
        claim: &SandboxExecutionClaim,
        existing: &SandboxPodView,
        suspend_restore_storage_uri: Option<&str>,
    ) -> Result<bool, SubstrateError> {
        let Some(reason) = self
            .stale_pod_reason_for_execution(sandbox, claim, existing, suspend_restore_storage_uri)
            .await?
        else {
            return Ok(false);
        };
        tracing::info!(
            sandbox_id = %sandbox.id,
            pod_name = %claim.row.pod_name,
            generation = claim.row.generation,
            reason = %reason,
            "pod create recovery found stale Kubernetes sandbox pod; replacing it"
        );
        if let Err(err) = delete_sandbox_pod_with_throttle_retry(
            &self.k8s,
            &claim.row.pod_name,
            "delete_stale_sandbox_pod_after_create_error",
        )
        .await
            && !is_not_found_error(&err)
        {
            return Err(kube_error_for_substrate(
                "failed to delete stale sandbox pod",
                err,
            ));
        }
        let _ = self
            .store
            .cleanup_replaced_pod_tracking_if_generation(sandbox.id, claim.row.generation)
            .await?;
        Ok(true)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn ensure_pod_exists(
        &self,
        sandbox: &Sandbox,
        evacuation_placement: Option<Option<&EvacuationPlacement>>,
        suspend_restore_source_node: Option<&str>,
        suspend_restore_storage_uri: Option<&str>,
        ownership: PodMaterializationOwnership,
    ) -> Result<(), SubstrateError> {
        let PodMaterializationOwnership {
            started_recovery,
            restore_lease_token,
            activation_attempt,
        } = ownership;
        let started_recovery_owner_generation =
            started_recovery.map(StartedRecoveryOwnership::generation);
        let sandbox_id = sandbox.id;
        let requested_lifecycle = sandbox
            .lifecycle
            .parse::<SandboxLifecycle>()
            .map_err(|err| SubstrateError::Failed {
                message: format!("failed to parse sandbox pod lifecycle: {err}"),
            })?;
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        let materialization_lifecycle =
            sandbox
                .lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse refreshed sandbox pod lifecycle: {err}"),
                })?;
        if materialization_lifecycle == SandboxLifecycle::Suspending {
            return Err(SubstrateError::suspend_in_flight(sandbox_id));
        }
        let lifecycle_is_materializable = matches!(
            materialization_lifecycle,
            SandboxLifecycle::Created
                | SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Started
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Suspended
                | SandboxLifecycle::Checkpointing
        ) || (requested_lifecycle == SandboxLifecycle::Stopped
            && materialization_lifecycle == SandboxLifecycle::Stopped);
        let lifecycle_refresh_is_valid = requested_lifecycle == materialization_lifecycle
            || (requested_lifecycle == SandboxLifecycle::Started
                && materialization_lifecycle == SandboxLifecycle::Restoring);
        if !lifecycle_is_materializable || !lifecycle_refresh_is_valid {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox cannot materialize an execution after lifecycle changed from \
                     {requested_lifecycle} to {materialization_lifecycle}"
                ),
            });
        }

        let claim_lifecycle = if started_recovery_owner_generation.is_some() {
            SandboxLifecycle::Started
        } else {
            materialization_lifecycle
        };
        let mut claim = self
            .store
            .ensure_active_execution_claim_for_lifecycle(
                sandbox_id,
                claim_lifecycle,
                restore_lease_token,
                activation_attempt
                    .as_ref()
                    .map(ActivationAttemptAssignment::request),
            )
            .await?;
        if let Some(activation_attempt) = activation_attempt.as_ref()
            && let Some(assigned_id) = claim.row.activation_attempt_id
        {
            activation_attempt.record_assigned(assigned_id, claim.row.generation);
        }
        let owns_started_recovery = started_recovery_owner_generation
            .is_some_and(|generation| claim.row.generation >= generation);
        let started_recovery_created_execution = started_recovery.is_some_and(|ownership| {
            ownership.created_execution() && ownership.generation() == claim.row.generation
        });
        if claim.row.state == ActiveExecutionState::Recovering && !owns_started_recovery {
            return Ok(());
        }
        let selected_plan = match if suspend_restore_source_node.is_none()
            && suspend_restore_storage_uri.is_none()
        {
            match normalize_lifecycle(&sandbox.lifecycle).as_str() {
                "suspended" | "restoring" => self
                    .suspend_restore_plan(&sandbox, evacuation_placement.is_some())
                    .await
                    .map(Some),
                "stopped" => self.stopped_restore_plan(&sandbox).await.map(Some),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        } {
            Ok(plan) => plan,
            Err(err) => {
                if claim.created && execution_pod_create_in_flight(claim.row.state) {
                    let _ = self
                        .store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                        .await?;
                }
                return Err(err);
            }
        };
        let started_recovery_restore_context = if owns_started_recovery
            && suspend_restore_source_node.is_none()
            && suspend_restore_storage_uri.is_none()
        {
            let generation = ExecutionGeneration::new(claim.row.generation).map_err(|err| {
                SubstrateError::Failed {
                    message: format!(
                        "invalid started recovery execution generation {}: {err}",
                        claim.row.generation
                    ),
                }
            })?;
            Some(
                self.store
                    .started_recovery_restore_context(sandbox_id, generation)
                    .await?,
            )
        } else {
            None
        };
        let mut started_recovery_restore_source = None;
        let (started_recovery_remote_storage_uri, started_recovery_has_restore_context) =
            match started_recovery_restore_context.as_ref() {
                Some(StartedRecoveryRestoreContext::Ready(
                    StartedRecoveryRestoreSource::Remote { storage_uri },
                )) => (Some(storage_uri.clone()), true),
                Some(StartedRecoveryRestoreContext::Ready(
                    StartedRecoveryRestoreSource::Local { source_node_name },
                )) => {
                    if started_recovery_created_execution {
                        started_recovery_restore_source =
                            Some(StartedRecoveryRestoreSource::Local {
                                source_node_name: source_node_name.clone(),
                            });
                    }
                    (None, true)
                }
                Some(StartedRecoveryRestoreContext::Pending) => (None, true),
                Some(StartedRecoveryRestoreContext::Unavailable) | None => (None, false),
            };
        let started_recovery_waits_for_restore_source =
            matches!(
                started_recovery_restore_context,
                Some(StartedRecoveryRestoreContext::Pending)
            ) || (matches!(
                started_recovery_restore_context,
                Some(StartedRecoveryRestoreContext::Unavailable)
            ) && self.store.prior_state_exists(sandbox_id).await?);
        let suspend_restore_source_node = suspend_restore_source_node.or(selected_plan
            .as_ref()
            .and_then(|plan| plan.source_node_name.as_deref()));
        let suspend_restore_storage_uri = suspend_restore_storage_uri
            .or(selected_plan
                .as_ref()
                .and_then(|plan| plan.storage_uri.as_deref()))
            .or_else(|| {
                started_recovery_remote_storage_uri
                    .as_ref()
                    .map(|storage_uri| storage_uri.as_wire())
            });

        // Materialize the evacuation toleration up front so it lives long
        // enough for `SandboxPodRequest::Restore`'s borrowed slots. The
        // outer `Option` flags "this is the evacuation path"; the inner
        // `Option` is whether a target cohort is active (a two-MNG
        // rollout in flight). With no rollout, evacuation still happens —
        // we just drop the source-node pin and emit no extra placement.
        let evacuation_toleration_slot: Option<[PodToleration; 1]> =
            evacuation_placement.and_then(|placement| placement.map(|p| [p.to_toleration()]));
        let evacuation_context = evacuation_placement.map(|placement| EvacuationContext {
            extra_tolerations: evacuation_toleration_slot
                .as_ref()
                .map(|slot| slot.as_slice())
                .unwrap_or(&[]),
            extra_required_affinity: placement.map(EvacuationPlacement::to_required_affinity),
        });

        let mut pod_name = claim.row.pod_name.clone();
        // Interrupted source-pinned restore recovery. A prior attempt can create
        // this generation's pinned pod and persist it — moving the row to
        // `created` (pod_uid set, pod_ip NULL) or leaving it `creating` — then be
        // interrupted (crash / cancel / watch error) before its in-request watch
        // re-drives. The fresh-create watch only runs on the request that
        // creates the pod, so without this a retry would reuse the stuck pod and
        // strand the restore on an unavailable source across retries.
        //
        // Detection derives entirely from evidence on the live pod — the
        // required pin in its spec — never from the current
        // `restore_source_node_hard_pin` config: a flag rollback must not
        // strand pods a previous deployment pinned. Gated on a portable
        // snapshot so the successor has a source to restore from;
        // `has_required_source_pin` is false once a prior retry already
        // re-drove cross-node, so this converges without looping.
        if !claim.created && suspend_restore_storage_uri.is_some() {
            let existing_pod = match self.lookup_sandbox_pod(&pod_name) {
                Some(existing) => Some(existing),
                // Cache miss on a *persisted* claim (`created`: pod_uid set,
                // pod_ip NULL): the pod exists on the apiserver, but a retry can
                // land on a replica whose reflector hasn't observed it yet.
                // Skipping recovery here would send the retry down the
                // POST-conflict path, which reuses the pinned pod without ever
                // running the scheduling watch (`created` is false there). One
                // authoritative GET on this rare shape closes that. A `creating`
                // claim stays cache-only: its POST may never have happened, so
                // the GET would usually 404, and the create-in-flight early
                // return below is the designed answer while the cache warms.
                None if !execution_pod_create_in_flight(claim.row.state) => {
                    fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                        .await
                        .map_err(|err| {
                            kube_error_for_substrate(
                                "failed to verify source-pinned restore pod",
                                err,
                            )
                        })?
                }
                None => None,
            };
            // An *unpinned* pod matching this execution under a still-`creating`
            // claim is a successor an interrupted re-drive created but never
            // persisted. Hydrate it now — without this the create-in-flight
            // early return below would leave the row unmaterialized until the
            // stale-create age expires. An interrupted *direct-node*
            // (`spec.nodeName`, local-only) pod takes this path too, and that
            // is deliberate: hydrating flips the row to `created`, which is
            // exactly what unlocks the cache-hit stale-pod classification
            // (`LocalOnlyRestoreNowPortable` -> replace) on the next attempt;
            // skipping it would strand that pod behind the create-in-flight
            // return for the full stale-create age instead.
            if let Some(existing) = existing_pod
                .as_ref()
                .filter(|existing| !existing.has_required_source_pin())
                .filter(|existing| {
                    execution_pod_create_in_flight(claim.row.state)
                        && pod_matches_execution_generation(
                            existing,
                            sandbox.id,
                            claim.row.generation,
                        )
                })
            {
                self.remember_pod_node_placement(
                    sandbox.id,
                    existing,
                    PlacementRetryDisposition::RetryConverges,
                )
                .await?;
                let pod_ip = assigned_pod_ip(existing)?.map(|ip| ip.to_string());
                if self
                    .store
                    .persist_pod_metadata(
                        sandbox.id,
                        claim.row.generation,
                        existing.uid(),
                        pod_ip.as_deref(),
                    )
                    .await?
                    .is_none()
                {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        generation = claim.row.generation,
                        "successor hydration no-oped: the claimed execution was retired \
                         or replaced mid-flight"
                    );
                }
                return Ok(());
            }
            let pin_state = existing_pod.as_ref().and_then(|existing| {
                existing
                    .has_required_source_pin()
                    .then(|| scheduling_outcome_of(existing))
            });
            if let Some(cached_outcome) = pin_state {
                // Trust a settled cached decision; otherwise re-observe with the
                // bounded watch (which ends in an authoritative apiserver GET) —
                // the original request may have crashed before the Unschedulable
                // condition ever reached this replica's reflector, and skipping
                // recovery on a lagging cache would burn a full readiness
                // timeout per retry until it catches up. A pod that is
                // genuinely still undecided falls through unharmed: only an
                // explicit Unschedulable verdict re-drives.
                let outcome = match cached_outcome {
                    Some(outcome) => Some(outcome),
                    None => {
                        Box::pin(self.await_scheduling_outcome(&pod_name, SCHEDULING_WATCH_BUDGET))
                            .await?
                    }
                };
                if outcome == Some(SchedulingOutcome::Unschedulable) {
                    // Boxed so this already-huge async fn's stack frame embeds
                    // only a pointer, not the re-drive sub-future. The re-drive
                    // itself re-verifies the pod with an authoritative GET
                    // before any teardown.
                    match Box::pin(self.redrive_unschedulable_restore_cross_node_recorded(
                        pplx_space_metrics::sandbox::restore::RedrivePath::Retry,
                        &sandbox,
                        &claim,
                        claim_lifecycle,
                        suspend_restore_source_node,
                        suspend_restore_storage_uri,
                        restore_lease_token,
                    ))
                    .await?
                    {
                        RedriveOutcome::Redriven(cross_node_pod) => {
                            self.remember_pod_node_placement(
                                sandbox.id,
                                &cross_node_pod,
                                PlacementRetryDisposition::RetryConverges,
                            )
                            .await?;
                            // The original may not have emitted a landing
                            // for this restore (it was interrupted, or its
                            // watch ended undecided and recorded
                            // `placement:unknown`, which share queries
                            // exclude), so this retry's successful fallback
                            // could otherwise be invisible to the
                            // cross-host-share signal. The successor's source
                            // annotation says whether the source still held
                            // trusted local bytes.
                            let local_snapshot_present = cross_node_pod
                                .annotation(SandboxPodKey::SuspendRestoreSourceNode)
                                .is_some_and(|value| !value.trim().is_empty());
                            pplx_space_metrics::sandbox::restore::placement_directed(
                                pplx_space_metrics::sandbox::restore::PlacementSample {
                                    placement: pplx_space_metrics::sandbox::restore::RestorePlacement::CrossNode,
                                    local_snapshot_present,
                                    pin: pplx_space_metrics::sandbox::restore::SourcePin::None,
                                },
                            );
                            return Ok(());
                        }
                        RedriveOutcome::OwnershipLost => {
                            return Ok(());
                        }
                        // The pin bound after all (our verdict was stale) or a
                        // concurrent re-drive already replaced it — fall through
                        // to the normal existing-pod handling. No metric here:
                        // whichever request created the live pod emits for it.
                        RedriveOutcome::PodRetained { .. } => {}
                    }
                }
                // Scheduled: the pin landed after all — fall through to the
                // normal existing-pod handling below.
            }
        }
        if !claim.created && execution_pod_create_in_flight(claim.row.state) {
            if !stale_restore_execution_create(
                &sandbox.lifecycle,
                claim.row.state,
                claim.row.create_age_expired,
            ) {
                let pod_create_context = self
                    .pod_create_context_for_claim(
                        &sandbox,
                        &claim,
                        evacuation_context,
                        suspend_restore_source_node,
                        suspend_restore_storage_uri,
                    )
                    .await?;
                if !claim.row.create_age_expired
                    || !matches!(pod_create_context, PodCreateContext::OnDemand)
                {
                    self.require_observed_create_claim_in_flight(&sandbox, &claim)
                        .await?;
                    return Ok(());
                }
            }

            let existing = fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate("failed to verify stale creating pod", err)
                })?;
            let replacement_reason = if let Some(existing) = existing.as_ref() {
                if let Some(reason) = self
                    .stale_pod_reason_for_execution(
                        &sandbox,
                        &claim,
                        existing,
                        suspend_restore_storage_uri,
                    )
                    .await?
                {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        reason = %reason,
                        "replacing stale creating Kubernetes pod"
                    );
                    if let Err(err) = delete_sandbox_pod_with_throttle_retry(
                        &self.k8s,
                        &pod_name,
                        "delete_stale_create_pod",
                    )
                    .await
                        && !is_not_found_error(&err)
                    {
                        return Err(kube_error_for_substrate(
                            "failed to delete stale create pod",
                            err,
                        ));
                    }
                    reason.to_string()
                } else {
                    self.remember_pod_node_placement(
                        sandbox.id,
                        existing,
                        PlacementRetryDisposition::RetryConverges,
                    )
                    .await?;
                    let pod_ip = assigned_pod_ip(existing)?.map(|ip| ip.to_string());
                    if self
                        .store
                        .persist_pod_metadata(
                            sandbox.id,
                            claim.row.generation,
                            existing.uid(),
                            pod_ip.as_deref(),
                        )
                        .await?
                        .is_none()
                    {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            "pod metadata write no-oped: the claimed execution was retired \
                             or replaced mid-flight"
                        );
                    }
                    return Ok(());
                }
            } else {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    pod_name = %pod_name,
                    generation = claim.row.generation,
                    "stale creating Kubernetes execution has no live pod; replacing generation"
                );
                "missing".to_string()
            };

            let replaced_generation = claim.row.generation;
            let replaced_pod_name = pod_name.clone();
            let Some(replacement_claim) = self
                .store
                .replace_active_execution_claim_if_generation(
                    sandbox_id,
                    replaced_generation,
                    "replaced",
                    activation_attempt
                        .as_ref()
                        .map(ActivationAttemptAssignment::request),
                )
                .await?
            else {
                return Ok(());
            };
            SandboxEvent::new(
                sandbox_id,
                SandboxEventType::ExecutionReplaced,
                SandboxEventSource::Substrate,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .operation("ensure_pod_exists")
                    .reason(replacement_reason)
                    .lifecycle_state(sandbox.lifecycle.as_str())
                    .execution_generation(replaced_generation.to_string())
                    .pod_name(replaced_pod_name),
            );
            claim = replacement_claim;
            if let Some(activation_attempt) = activation_attempt.as_ref()
                && let Some(assigned_id) = claim.row.activation_attempt_id
            {
                activation_attempt.record_assigned(assigned_id, claim.row.generation);
            }
            pod_name = claim.row.pod_name.clone();
        }

        let started_recovery_requires_handoff = owns_started_recovery
            && (started_recovery_waits_for_restore_source
                || (!started_recovery_created_execution
                    && (started_recovery_has_restore_context
                        || suspend_restore_source_node.is_some()
                        || suspend_restore_storage_uri.is_some())));

        // Cheap cache check first. Recovery with prior state confirms cache
        // misses because a missing tracked pod requires an atomic restore-source
        // handoff before creating its successor. Other cache misses proceed to
        // POST, where the 409-recovery branch confirms conflicting pods.
        if self.lookup_sandbox_pod(&pod_name).is_some() || started_recovery_requires_handoff {
            // The reflector cache is eventually consistent, so an evicted or
            // force-deleted pod could still appear alive, while a recently
            // created pod could be absent. Confirm with an authoritative GET.
            let existing = fetch_sandbox_pod_uncached(&self.k8s, &pod_name)
                .await
                .map_err(|err| {
                    kube_error_for_substrate("failed to verify cached sandbox pod", err)
                })?;
            if let Some(existing) = existing {
                if let Some(reason) = self
                    .stale_pod_reason_for_execution(
                        &sandbox,
                        &claim,
                        &existing,
                        suspend_restore_storage_uri,
                    )
                    .await?
                {
                    if started_recovery_requires_handoff
                        && let Some(pod_uid) =
                            existing.uid().and_then(|uid| PodUid::parse(uid).ok())
                    {
                        let generation =
                            ExecutionGeneration::new(claim.row.generation).map_err(|error| {
                                SubstrateError::Failed {
                                    message: format!(
                                        "invalid started recovery execution generation: {error}"
                                    ),
                                }
                            })?;
                        if self
                            .store
                            .local_suspend_backup_pending(sandbox_id, generation, &pod_uid)
                            .await?
                        {
                            return Err(SubstrateError::suspend_in_flight(sandbox_id));
                        }
                    }
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %pod_name,
                        generation = claim.row.generation,
                        reason = %reason,
                        "replacing stale Kubernetes sandbox execution pod"
                    );
                    if let Err(err) = delete_sandbox_pod_with_throttle_retry(
                        &self.k8s,
                        &pod_name,
                        "delete_stale_sandbox_pod",
                    )
                    .await
                        && !is_not_found_error(&err)
                    {
                        return Err(kube_error_for_substrate(
                            "failed to delete stale sandbox pod",
                            err,
                        ));
                    }
                    if started_recovery_requires_handoff {
                        if !self
                            .replace_started_recovery_pod(
                                &sandbox,
                                &mut claim,
                                &mut pod_name,
                                &mut started_recovery_restore_source,
                                StartedRecoveryPodReplacement {
                                    reason: reason.to_string(),
                                    restore_context: started_recovery_restore_context.clone(),
                                    explicit_source_node: suspend_restore_source_node,
                                    explicit_storage_uri: suspend_restore_storage_uri,
                                    activation_assignment: activation_attempt.as_ref(),
                                },
                            )
                            .await?
                        {
                            return Ok(());
                        }
                    } else {
                        let replaced_generation = claim.row.generation;
                        let replaced_pod_name = pod_name.clone();
                        let Some(replacement_claim) = self
                            .store
                            .replace_active_execution_claim_if_generation(
                                sandbox_id,
                                replaced_generation,
                                "replaced",
                                activation_attempt
                                    .as_ref()
                                    .map(ActivationAttemptAssignment::request),
                            )
                            .await?
                        else {
                            return Ok(());
                        };
                        let replacement_event_details = started_recovery_replacement_event_details(
                            SandboxEventDetails::default()
                                .operation("ensure_pod_exists")
                                .reason(reason.to_string())
                                .lifecycle_state(sandbox.lifecycle.as_str())
                                .execution_generation(replaced_generation.to_string())
                                .pod_name(replaced_pod_name),
                            suspend_restore_source_node,
                            suspend_restore_storage_uri,
                            None,
                        );
                        SandboxEvent::new(
                            sandbox_id,
                            SandboxEventType::ExecutionReplaced,
                            SandboxEventSource::Substrate,
                        )
                        .emit_with(replacement_event_details);
                        claim = replacement_claim;
                        if let Some(activation_attempt) = activation_attempt.as_ref()
                            && let Some(assigned_id) = claim.row.activation_attempt_id
                        {
                            activation_attempt.record_assigned(assigned_id, claim.row.generation);
                        }
                    }
                } else {
                    let _ = self
                        .remember_pod_node_placement(
                            sandbox.id,
                            &existing,
                            PlacementRetryDisposition::RetryConverges,
                        )
                        .await?;
                    let pod_ip = assigned_pod_ip(&existing)?.map(|ip| ip.to_string());
                    if self
                        .store
                        .persist_pod_metadata(
                            sandbox.id,
                            claim.row.generation,
                            existing.uid(),
                            pod_ip.as_deref(),
                        )
                        .await?
                        .is_none()
                    {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            generation = claim.row.generation,
                            "pod metadata write no-oped: the claimed execution was retired \
                             or replaced mid-flight"
                        );
                    }
                    return Ok(());
                }
            } else if started_recovery_requires_handoff
                && !self
                    .replace_started_recovery_pod(
                        &sandbox,
                        &mut claim,
                        &mut pod_name,
                        &mut started_recovery_restore_source,
                        StartedRecoveryPodReplacement {
                            reason: "missing".to_string(),
                            restore_context: started_recovery_restore_context.clone(),
                            explicit_source_node: suspend_restore_source_node,
                            explicit_storage_uri: suspend_restore_storage_uri,
                            activation_assignment: activation_attempt.as_ref(),
                        },
                    )
                    .await?
            {
                return Ok(());
            }
        }

        let (suspend_restore_source_node, suspend_restore_storage_uri) =
            if suspend_restore_source_node.is_some() || suspend_restore_storage_uri.is_some() {
                (suspend_restore_source_node, suspend_restore_storage_uri)
            } else {
                match started_recovery_restore_source.as_ref() {
                    Some(StartedRecoveryRestoreSource::Local { source_node_name }) => {
                        (Some(source_node_name.as_str()), None)
                    }
                    Some(StartedRecoveryRestoreSource::Remote { storage_uri }) => {
                        (None, Some(storage_uri.as_wire()))
                    }
                    None => (None, None),
                }
            };

        if !claim.created && execution_pod_create_in_flight(claim.row.state) {
            self.require_observed_create_claim_in_flight(&sandbox, &claim)
                .await?;
            return Ok(());
        }
        if !self
            .store
            .active_execution_is_generation(sandbox_id, claim.row.generation)
            .await?
        {
            tracing::debug!(
                sandbox_id = %sandbox.id,
                pod_name = %claim.row.pod_name,
                generation = claim.row.generation,
                "skipping Kubernetes sandbox pod create for replaced execution generation"
            );
            return Ok(());
        }

        let pod_create_context = self
            .pod_create_context_for_claim(
                &sandbox,
                &claim,
                evacuation_context,
                suspend_restore_source_node,
                suspend_restore_storage_uri,
            )
            .await?;
        // Snapshot the placement decision before the context is consumed by
        // `sandbox_pod_request`; emitted once below on a confirmed fresh create,
        // after any in-request cross-node re-drive so it reflects the landing site.
        let placement_sample = restore_placement_sample(
            &pod_create_context,
            self.k8s.sandbox_pod_config.restore_source_node_hard_pin,
        );
        // Resolved before the prior-generation sweep and before any pod is
        // POSTed, so an unplaceable sandbox fails without leaving a pod behind —
        // and, for a local-only restore, before its source-node pin is
        // rendered. `pod_create_context` is consumed by `sandbox_pod_request`
        // further down, so this reads it while it is still in hand.
        //
        // The execution claim is already outstanding at this point — it is
        // bound upstream — which is exactly why this goes through the
        // claim-retiring wrapper rather than resolving directly.
        let node_group_placement = self
            .resolve_node_group_placement_for_claim(&sandbox, &claim, &pod_create_context)
            .await?;

        let auto_lifecycle = match self
            .store
            .auto_lifecycle_policy_after_claim(sandbox.id)
            .await
        {
            Ok(policy) => policy,
            Err(error) => {
                if claim.created && execution_pod_create_in_flight(claim.row.state) {
                    self.store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                        .await?;
                }
                return Err(error);
            }
        };
        let auto_lifecycle_json =
            serde_json::to_string(&auto_lifecycle).map_err(|err| SubstrateError::Failed {
                message: format!("failed to serialize auto lifecycle policy: {err}"),
            })?;
        let network_policy = self.effective_network_policy_for_sandbox(&sandbox).await?;
        let network_policy_json =
            serde_json::to_string(&network_policy).map_err(|err| SubstrateError::Failed {
                message: format!("failed to serialize network policy: {err}"),
            })?;
        ensure_network_policy_annotation_size(&network_policy_json)?;
        let sandbox_owner = self.store.sandbox_owner_for_user(sandbox.user_id);

        // Sweep of lingering older-generation pods before a restore create — a
        // create prerequisite, not best-effort. An interrupted re-drive can
        // strand a source-pinned pod that no request ever deleted (the
        // transient-lifecycle sweeper reverts the sandbox row but never touches
        // pods); if the source later frees capacity, kube-scheduler binds that
        // stale pod and the shim starts an orphan VM for a sandbox that may
        // already be `suspended`. So each such pod must be confirmed **gone**
        // before the successor is materialized. Only pods from generations
        // **strictly older** than this claim are selected (never by name
        // exclusion alone): a concurrent request that superseded this claim has
        // a >= generation and is never touched. Deleting a strictly-older-
        // generation pod is always safe — it must not run.
        //
        // Fail-closed: an unconfirmed delete (timeout / observe-error / delete
        // error) retires this freshly-created claim and returns retryable, so
        // no successor is POSTed while an older pod might still be running, and
        // the retry re-runs the sweep instead of no-oping on the create-in-
        // flight early return. A cold pod-reflector cache cannot enumerate
        // prior-generation pods, so it takes the same retryable deferral rather
        // than materializing a successor unswept: `/readyz` gates external
        // traffic on the reflector, but that indirect invariant does not cover
        // direct/internal callers or a readiness-routing mistake, so the create
        // itself refuses until the cache is warm.
        if suspend_restore_source_node.is_some() || suspend_restore_storage_uri.is_some() {
            if !self.pod_reflector_ready.is_ready() {
                if claim.created && execution_pod_create_in_flight(claim.row.state) {
                    let _ = self
                        .store
                        .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                        .await?;
                }
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "deferring restore: pod reflector cache not ready to verify \
                         lingering prior-generation pods for sandbox {}; retry",
                        sandbox.id
                    ),
                });
            }
            let lingering = lingering_sandbox_pod_names(
                self.pod_store.state().iter().map(|pod| pod.as_ref()),
                sandbox.id,
                claim.row.generation,
            );
            for stale_pod in lingering {
                match self.delete_lingering_prior_generation_pod(&stale_pod).await {
                    Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {
                        tracing::info!(
                            sandbox_id = %sandbox.id,
                            stale_pod = %stale_pod,
                            successor_pod = %pod_name,
                            "confirmed lingering prior-generation restore pod gone before create"
                        );
                    }
                    outcome => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            stale_pod = %stale_pod,
                            ?outcome,
                            "deferring restore: lingering prior-generation pod not confirmed \
                             gone; retry"
                        );
                        if claim.created && execution_pod_create_in_flight(claim.row.state) {
                            let _ = self
                                .store
                                .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                                .await?;
                        }
                        return Err(SubstrateError::Unavailable {
                            message: format!(
                                "deferring restore: lingering prior-generation pod {stale_pod} \
                                 for sandbox {} not confirmed gone; retry",
                                sandbox.id
                            ),
                        });
                    }
                }
            }
        }

        if !self
            .store
            .claim_active_execution_pod_materialization(
                sandbox.id,
                claim.row.generation,
                claim_lifecycle,
            )
            .await?
        {
            tracing::debug!(
                sandbox_id = %sandbox.id,
                pod_name = %claim.row.pod_name,
                generation = claim.row.generation,
                "skipping Kubernetes sandbox pod create for replaced execution generation"
            );
            return Ok(());
        }

        let pod_result = create_sandbox_pod(
            &self.k8s,
            self.sandbox_pod_request(
                &sandbox,
                &claim.row,
                SandboxPodRequestInputs {
                    sandbox_owner: &sandbox_owner,
                    auto_lifecycle_json: &auto_lifecycle_json,
                    network_policy_json: &network_policy_json,
                    node_group_placement: node_group_placement.placement(),
                },
                pod_create_context,
            ),
        )
        .await;
        let (pod, created) = match pod_result {
            Ok(pod) => (Arc::new(pod), true),
            Err(err) if matches!(&err, kube::Error::Api(resp) if resp.code == 409) => {
                let execution = &claim.row;
                let pod_name = execution.pod_name.clone();
                // Reflector may not have observed the conflicting pod yet —
                // fall back to a direct kube apiserver GET so this rare
                // recovery path always sees authoritative state.
                let existing = match fetch_sandbox_pod_uncached(&self.k8s, &pod_name).await {
                    Ok(Some(existing)) => existing,
                    Ok(None) => {
                        return Err(SubstrateError::Failed {
                            message: format!(
                                "sandbox pod create conflicted but pod {pod_name} could not be loaded"
                            ),
                        });
                    }
                    Err(query_error) => {
                        return Err(SubstrateError::restore_in_flight(format!(
                            "sandbox pod create conflicted, but Kubernetes could not confirm pod \
                             {pod_name}: {query_error}"
                        )));
                    }
                };
                if self
                    .cleanup_stale_existing_pod_after_create_error(
                        &sandbox,
                        &claim,
                        &existing,
                        suspend_restore_storage_uri,
                    )
                    .await?
                {
                    return Ok(());
                }
                (existing, false)
            }
            Err(err) => {
                let create_error = format!("failed to create sandbox pod: {err}");
                match fetch_sandbox_pod_uncached(&self.k8s, &claim.row.pod_name).await {
                    Ok(Some(existing)) => {
                        if self
                            .cleanup_stale_existing_pod_after_create_error(
                                &sandbox,
                                &claim,
                                &existing,
                                suspend_restore_storage_uri,
                            )
                            .await?
                        {
                            return Ok(());
                        }
                        (existing, false)
                    }
                    Ok(None) => {
                        let _ = self
                            .store
                            .abandon_created_unmaterialized_create_claim(sandbox.id, &claim)
                            .await?;
                        return Err(SubstrateError::Failed {
                            message: create_error,
                        });
                    }
                    Err(query_error) => {
                        let message = format!(
                            "{create_error}; could not verify whether Kubernetes committed it: \
                             {query_error}"
                        );
                        // A failed create followed by a failed authoritative
                        // read is not a terminal failure: the apiserver may
                        // have committed the Pod before returning the error.
                        // Preserve started-recovery ownership so its failure
                        // publisher cannot roll the sandbox back while that
                        // Pod may be live.
                        return Err(SubstrateError::restore_in_flight(message));
                    }
                }
            }
        };

        let pod_ip = assigned_pod_ip(&pod)?.map(|ip| ip.to_string());
        match self
            .store
            .persist_materialized_pod_metadata(
                sandbox.id,
                claim.row.generation,
                claim_lifecycle,
                pod.uid(),
                pod_ip.as_deref(),
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                if created {
                    tracing::info!(
                        sandbox_id = %sandbox.id,
                        pod_name = %claim.row.pod_name,
                        generation = claim.row.generation,
                        "deleting Kubernetes sandbox pod created after materialization ownership was revoked"
                    );
                    self.delete_pod_and_forget_tracking(
                        sandbox.id,
                        &claim.row.pod_name,
                        claim.row.generation,
                        PodTrackingCleanupCause::ExplicitTeardown,
                    )
                    .await?;
                }
                return Ok(());
            }
            Err(error) => {
                if !created {
                    return Err(SubstrateError::restore_in_flight(format!(
                        "{error}; existing Kubernetes pod {} may still be live because its \
                         materialization metadata was not persisted",
                        claim.row.pod_name
                    )));
                }
                let Some(pod_uid) = pod.uid() else {
                    return Err(SubstrateError::restore_in_flight(format!(
                        "{error}; created Kubernetes pod {} had no UID, so its absence could not \
                         be confirmed",
                        claim.row.pod_name
                    )));
                };
                let pod_uid = PodUid::parse(pod_uid).map_err(|uid_error| {
                    SubstrateError::restore_in_flight(format!(
                        "{error}; created Kubernetes pod {} had an invalid UID, so its absence \
                         could not be confirmed: {uid_error}",
                        claim.row.pod_name
                    ))
                })?;
                match self
                    .k8s_blocking_delete_sandbox_pod_for_operation(
                        &claim.row.pod_name,
                        "materialization_persistence_failure",
                        Some((pod_uid.as_str(), None)),
                    )
                    .await
                {
                    Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => {
                        return Err(error);
                    }
                    Ok(delete_outcome) => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %claim.row.pod_name,
                            generation = claim.row.generation,
                            delete_outcome = delete_outcome.as_str(),
                            "Kubernetes sandbox pod absence remained unconfirmed after materialization persistence failed"
                        );
                    }
                    Err(delete_error) => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            pod_name = %claim.row.pod_name,
                            generation = claim.row.generation,
                            error = %delete_error,
                            "failed to confirm Kubernetes sandbox pod deletion after materialization persistence failed"
                        );
                    }
                }
                return Err(SubstrateError::restore_in_flight(format!(
                    "{error}; Kubernetes pod {} deletion was not confirmed",
                    claim.row.pod_name
                )));
            }
        }

        // Only `Ok(Some(_))` reaches here — materialization is committed and we
        // own the generation. `Ok(None)` / `Err` returned above.
        use pplx_space_metrics::sandbox::restore::{RestorePlacement, SourcePin};
        let mut placement_pod = pod;
        let mut placement_sample = placement_sample;

        // In-request source-pinned restore fallback.
        //
        // The required source-node pin (behind `restore_source_node_hard_pin`)
        // can leave the pod `Unschedulable` when the source is full, gone, or
        // disk-pressured. Observe the scheduler's first decision synchronously
        // and, if the source cannot take it, delete the pod and re-drive
        // cross-node in this same request.
        //
        // This needs no durable marker and no reconciler: an `Unschedulable`
        // pod is inert — never bound, no `nodeName`, no IP, no VM, no network
        // slot — so deleting it can never orphan a running workload, and a
        // crash before the re-drive commits simply replays pin -> watch ->
        // re-drive on the next attempt (the pin is re-derived from the same
        // inputs). Because the decision is re-made per attempt, a client retry
        // never races a half-torn-down pin the way an async reconciler would.
        //
        // Gate on the placement sample the classifier already produced: a
        // `RequiredAffinity` pin is exactly the source-pinned-portable case,
        // so the watch trigger, the rendered pod spec, and the emitted metric
        // all derive from `restore_placement_sample` and cannot drift apart.
        let rendered_required_source_pin = matches!(
            placement_sample.map(|sample| sample.pin),
            Some(SourcePin::RequiredAffinity)
        );
        if created && rendered_required_source_pin {
            match Box::pin(
                self.await_scheduling_outcome(&claim.row.pod_name, SCHEDULING_WATCH_BUDGET),
            )
            .await?
            {
                // Every listed node (bare pin: the source; shortlist: source +
                // fallbacks) failed filtering — tear down and re-drive
                // cross-node. Boxed so this already-large async fn's stack
                // frame embeds only a pointer; the re-drive re-verifies the pod
                // with an authoritative GET before any teardown.
                Some(SchedulingOutcome::Unschedulable) => {
                    match Box::pin(self.redrive_unschedulable_restore_cross_node_recorded(
                        pplx_space_metrics::sandbox::restore::RedrivePath::FreshCreate,
                        &sandbox,
                        &claim,
                        claim_lifecycle,
                        suspend_restore_source_node,
                        suspend_restore_storage_uri,
                        restore_lease_token,
                    ))
                    .await?
                    {
                        RedriveOutcome::Redriven(cross_node_pod) => {
                            // Preserve `local_snapshot_present` (the source
                            // still held local bytes) so the metric
                            // distinguishes "cross-node because the source
                            // rejected it" from "cross-node because no source
                            // was known".
                            if let Some(sample) = placement_sample.as_mut() {
                                sample.placement = RestorePlacement::CrossNode;
                                sample.pin = SourcePin::None;
                            }
                            placement_pod = cross_node_pod;
                        }
                        // Nothing torn down. A retained cross-node successor
                        // (installed by a concurrent re-drive) must still be
                        // recorded as cross-node — this request created the
                        // pinned pod, so it is the one that emits below. A
                        // retained bound-on-source pod keeps the same-node
                        // sample.
                        RedriveOutcome::PodRetained { cross_node } => {
                            if cross_node && let Some(sample) = placement_sample.as_mut() {
                                sample.placement = RestorePlacement::CrossNode;
                                sample.pin = SourcePin::None;
                            }
                        }
                        // Materialization ownership was lost mid-flight; the
                        // winner drives.
                        RedriveOutcome::OwnershipLost => {
                            return Ok(());
                        }
                    }
                }
                // Bound: record the actual landing. Under the shortlist the
                // scheduler may place the pod on a fallback in the same pass
                // (source infeasible but a listed node fit) — that is a
                // cross-node landing even though the pin/preference targeted
                // the source. `pin` stays `RequiredAffinity`: cross_node +
                // required_affinity is exactly the "shortlist fallback"
                // signature.
                Some(SchedulingOutcome::Scheduled { node_name }) => {
                    if Some(node_name.as_str()) != suspend_restore_source_node
                        && let Some(sample) = placement_sample.as_mut()
                    {
                        sample.placement = RestorePlacement::CrossNode;
                    }
                }
                // Undecided after the budget: proceed to the readiness wait —
                // only an explicit Unschedulable verdict justifies teardown.
                // But the landing node is unknown, and the pod may still bind
                // to a shortlist fallback. Recording the rendered `same_node`
                // sample would mis-count exactly the cross-node placements this
                // metric measures; the request can't block on a later bind
                // (latency budget), so emit an explicit `unknown` placement the
                // rollout share query excludes. `pin` stays `RequiredAffinity`.
                None => {
                    if let Some(sample) = placement_sample.as_mut() {
                        sample.placement = RestorePlacement::Unknown;
                    }
                }
            }
        }

        // Record the placement decision exactly once — only for a freshly
        // created restore pod (the 409 "found existing" arm sets
        // `created = false`) whose materialization ownership was just confirmed,
        // and after any in-request cross-node re-drive so the metric reflects
        // the actual landing site.
        if created && let Some(sample) = placement_sample {
            pplx_space_metrics::sandbox::restore::placement_directed(sample);
        }

        let _ = self
            .remember_pod_node_placement(
                sandbox.id,
                &placement_pod,
                PlacementRetryDisposition::RetryConverges,
            )
            .await?;
        Ok(())
    }
}
