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

    pub(in crate::controllers::substrate::kubernetes) async fn handoff_started_recovery_execution(
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
                    .started_recovery_unavailable_error(
                        sandbox_id,
                        SubstrateError::RestoreContextUnavailable {
                            message: format!(
                                "sandbox {sandbox_id} generation {generation} has no verified \
                                 local or remote restore source; leaving the execution unchanged"
                            ),
                        },
                    )
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
            // An upload can start between the pre-check and the handoff
            // transaction; answer the same state the same way.
            StartedRecoveryReplacement::Unavailable => Err(self
                .started_recovery_unavailable_error(
                    sandbox_id,
                    SubstrateError::RestoreContextUnavailable {
                        message: format!(
                            "sandbox {sandbox_id} generation {generation} lost its verified \
                             restore source before execution handoff; leaving the execution \
                             unchanged"
                        ),
                    },
                )
                .await),
            StartedRecoveryReplacement::OwnershipLost => Ok(None),
        }
    }

    /// The error for a started recovery with no restore source: the
    /// retryable `RestoreUploadSettling` while a snapshot upload may still
    /// land ([`K8sStore::started_recovery_source_publishing`]), otherwise
    /// `unavailable` unchanged. A failed read keeps `unavailable`, the answer
    /// before this check existed.
    async fn started_recovery_unavailable_error(
        &self,
        sandbox_id: SandboxId,
        unavailable: SubstrateError,
    ) -> SubstrateError {
        match self
            .store
            .started_recovery_source_publishing(sandbox_id)
            .await
        {
            Ok(true) => SubstrateError::RestoreUploadSettling {
                message: format!(
                    "sandbox {sandbox_id}: a snapshot upload is in flight and may become the \
                     restore source; retry"
                ),
            },
            Ok(false) => unavailable,
            Err(error) => {
                tracing::warn!(
                    %sandbox_id,
                    error = %error,
                    "started recovery: in-flight upload check failed; answering unavailable"
                );
                unavailable
            }
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
