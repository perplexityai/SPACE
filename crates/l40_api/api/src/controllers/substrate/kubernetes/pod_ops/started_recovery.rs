//! Started-sandbox recovery pod replacement and handoff.

use super::*;

pub(super) fn started_recovery_replacement_event_details(
    details: SandboxEventDetails,
    restore_source_node: Option<&str>,
    restore_storage_uri: Option<&str>,
    handoff_source: Option<&StartedRecoveryRestoreSource>,
) -> SandboxEventDetails {
    if let Some(source_node) = restore_source_node {
        return details
            .source_node_name(source_node)
            .restore_provenance(SandboxRestoreProvenance::LocalOnly);
    }
    if restore_storage_uri.is_some() {
        return details
            .bool_field(SandboxEventBoolField::RestoreStorageUriPresent, true)
            .restore_provenance(SandboxRestoreProvenance::RemoteMatchesLocal);
    }
    match handoff_source {
        Some(StartedRecoveryRestoreSource::Local { source_node_name }) => details
            .source_node_name(source_node_name)
            .restore_provenance(SandboxRestoreProvenance::LocalOnly),
        Some(StartedRecoveryRestoreSource::Remote { .. }) => details
            .bool_field(SandboxEventBoolField::RestoreStorageUriPresent, true)
            .restore_provenance(SandboxRestoreProvenance::RemoteMatchesLocal),
        None => details,
    }
}

impl KubernetesSubstrate {
    pub(in crate::controllers::substrate::kubernetes) fn started_recovery_source_node_available(
        &self,
        source_node_name: &str,
    ) -> bool {
        if !self.k8s.caches.nodes_ready() || !self.pod_reflector_ready.is_ready() {
            return false;
        }
        let config = &self.k8s.sandbox_pod_config;
        let selector = config.base_node_selector();
        self.k8s.caches.nodes.state().iter().any(|node| {
            node.metadata.name.as_deref() == Some(source_node_name)
                && adapter::node_schedulable(node)
                && selector.matches(node)
                && (!config.ssd_headroom_tier_affinity || adapter::node_not_placement_blocked(node))
                && adapter::node_network_slot_available(node, &self.pod_network_slots)
        })
    }

    async fn handoff_started_recovery_execution(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        restore_context: Option<StartedRecoveryRestoreContext>,
    ) -> Result<Option<(SandboxExecutionClaim, StartedRecoveryRestoreSource)>, SubstrateError> {
        let restore_context = match restore_context {
            Some(restore_context) => restore_context,
            None => {
                self.store
                    .started_recovery_restore_context(sandbox_id, generation)
                    .await?
            }
        };
        let allowed_local_source = match restore_context {
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                source_node_name,
            }) if self.started_recovery_source_node_available(&source_node_name) => {
                Some(source_node_name)
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                ..
            })
            | StartedRecoveryRestoreContext::Pending => {
                return Err(SubstrateError::restore_source_pending(sandbox_id));
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Remote {
                ..
            }) => None,
            StartedRecoveryRestoreContext::Unavailable => {
                return Err(self
                    .started_recovery_unavailable_error(sandbox_id, generation)
                    .await);
            }
        };

        match self
            .store
            .replace_started_recovery_execution(
                sandbox_id,
                generation,
                allowed_local_source.as_deref(),
            )
            .await?
        {
            StartedRecoveryReplacement::Replaced { claim, source } => Ok(Some((*claim, source))),
            StartedRecoveryReplacement::Pending => {
                Err(SubstrateError::restore_source_pending(sandbox_id))
            }
            StartedRecoveryReplacement::Unavailable => {
                Err(SubstrateError::RestoreContextUnavailable {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation} lost its verified restore \
                         source before execution handoff; leaving the execution unchanged"
                    ),
                })
            }
            StartedRecoveryReplacement::OwnershipLost => Ok(None),
        }
    }

    /// Whether the API already sees `source_node` as lost: its row is deleted,
    /// or the warm Node cache doesn't list it, or lists it not Ready or
    /// cordoned. A cold cache answers `false`.
    fn started_recovery_source_node_looks_lost(&self, source_node: &StaleRecoverySourceNode) -> bool {
        match source_node {
            StaleRecoverySourceNode::RowDeleted => true,
            StaleRecoverySourceNode::Unknown => false,
            StaleRecoverySourceNode::Named(name) => {
                if !self.k8s.caches.nodes_ready() {
                    return false;
                }
                self.k8s
                    .caches
                    .nodes
                    .state()
                    .iter()
                    .find(|node| node.metadata.name.as_deref() == Some(name.as_str()))
                    .is_none_or(|node| !adapter::node_schedulable(node))
            }
        }
    }

    /// The error for a recovery with no verified restore source.
    ///
    /// - A newer upload that may still land: `RestoreUploadSettling`, a
    ///   retryable 503 that still completes the attempt as failed.
    /// - Only a stale durable suspend: `RestoreAwaitingSourceNodeLoss`, also
    ///   retryable, but only when the retry can resolve, that is
    ///   [`stale_source_retry_can_resolve`] holds. Otherwise the node is live
    ///   (the pod-loss-on-a-live-node case the stale guard exists for) or
    ///   nothing will roll the sandbox back, and it keeps the
    ///   `RestoreContextUnavailable` answer.
    /// - No durable copy: `RestoreContextUnavailable`.
    async fn started_recovery_unavailable_error(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> SubstrateError {
        match self
            .store
            .started_recovery_unavailable_cause(sandbox_id)
            .await
        {
            Ok(StartedRecoveryUnavailableCause::SourcePublishing) => {
                SubstrateError::RestoreUploadSettling {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation}: a newer snapshot upload \
                         may still land; retry"
                    ),
                }
            }
            Ok(StartedRecoveryUnavailableCause::StaleSourceAwaitingNodeLoss { source_node })
                if stale_source_retry_can_resolve(
                    self.k8s.lost_started_give_up_enabled,
                    self.started_recovery_source_node_looks_lost(&source_node),
                ) =>
            {
                SubstrateError::RestoreAwaitingSourceNodeLoss {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation}: the only durable restore \
                         source predates the sandbox's last activity; waiting until the source \
                         node is confirmed gone before rolling back to it"
                    ),
                }
            }
            Ok(
                StartedRecoveryUnavailableCause::StaleSourceAwaitingNodeLoss { .. }
                | StartedRecoveryUnavailableCause::NoDurableSource,
            ) => {
                SubstrateError::RestoreContextUnavailable {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation} has no verified local or \
                         remote restore source; leaving the execution unchanged"
                    ),
                }
            }
            // The classification is advisory. Failing to read it must not turn
            // a possibly recoverable sandbox into a terminal answer.
            Err(error) => error,
        }
    }

    pub(super) async fn replace_started_recovery_pod(
        &self,
        sandbox: &Sandbox,
        claim: &mut SandboxExecutionClaim,
        pod_name: &mut String,
        restore_source: &mut Option<StartedRecoveryRestoreSource>,
        replacement: StartedRecoveryPodReplacement<'_>,
    ) -> Result<bool, SubstrateError> {
        let StartedRecoveryPodReplacement {
            reason,
            restore_context,
            explicit_source_node,
            explicit_storage_uri,
            activation_assignment,
        } = replacement;
        let sandbox_id = sandbox.id;
        let replaced_generation = claim.row.generation;
        let replaced_pod_name = pod_name.clone();
        let generation = ExecutionGeneration::new(replaced_generation).map_err(|err| {
            SubstrateError::Failed {
                message: format!(
                    "invalid started recovery execution generation {replaced_generation}: {err}"
                ),
            }
        })?;
        let Some((replacement_claim, source)) = self
            .handoff_started_recovery_execution(sandbox_id, generation, restore_context)
            .await?
        else {
            return Ok(false);
        };
        if let Some(assignment) = activation_assignment
            && let Some(id) = replacement_claim.row.activation_attempt_id
        {
            assignment.record_assigned(id, replacement_claim.row.generation);
        }
        let replacement_event_details = started_recovery_replacement_event_details(
            SandboxEventDetails::default()
                .operation("ensure_pod_exists")
                .reason(reason)
                .lifecycle_state(sandbox.lifecycle.as_str())
                .execution_generation(replaced_generation.to_string())
                .pod_name(replaced_pod_name),
            explicit_source_node,
            explicit_storage_uri,
            Some(&source),
        );
        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::ExecutionReplaced,
            SandboxEventSource::Substrate,
        )
        .emit_with(replacement_event_details);
        *claim = replacement_claim;
        *pod_name = claim.row.pod_name.clone();
        *restore_source = Some(source);
        Ok(true)
    }
}

/// Whether telling the client to wait for a node-loss rollback is honest: the
/// lost-started give-up must run, and the API must already see the source
/// node as lost. A live source node is never rolled back, and with the
/// give-up off nothing rolls anything back, so either way the retry could
/// never resolve.
fn stale_source_retry_can_resolve(give_up_enabled: bool, source_node_looks_lost: bool) -> bool {
    give_up_enabled && source_node_looks_lost
}

#[cfg(test)]
mod tests {
    use super::stale_source_retry_can_resolve;

    #[test]
    fn stale_source_waits_only_for_a_lost_node_with_the_give_up_on() {
        assert!(stale_source_retry_can_resolve(true, true));
        assert!(!stale_source_retry_can_resolve(true, false), "live node");
        assert!(!stale_source_retry_can_resolve(false, true), "give-up off");
        assert!(!stale_source_retry_can_resolve(false, false));
    }
}
