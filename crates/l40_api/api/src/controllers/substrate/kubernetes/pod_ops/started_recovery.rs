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

/// Why the node holding a sandbox's local state can't take a pod pinned to
/// it now ([`KubernetesSubstrate::local_source_node_refusal`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::controllers::substrate::kubernetes) enum LocalSourceNodeRefusal {
    /// The Node or pod reflector hasn't synced, so nothing can be said.
    CachesCold,
    /// The Node reflector has no node by that name.
    Unregistered,
    /// The node is outside the sandbox node selector, so no sandbox pod
    /// schedules there.
    OutsideSelector,
    /// Not Ready, cordoned, or placement-blocked while the SSD headroom tier
    /// affinity is on.
    CantTakeWork,
    /// No network slot is free.
    NoNetworkSlot,
}

impl LocalSourceNodeRefusal {
    /// Whether the refusal holds until the node or the configuration
    /// changes, rather than clearing on its own: a retry can't succeed.
    pub(in crate::controllers::substrate::kubernetes) fn is_lasting(self) -> bool {
        match self {
            Self::Unregistered | Self::OutsideSelector => true,
            Self::CachesCold | Self::CantTakeWork | Self::NoNetworkSlot => false,
        }
    }

    pub(in crate::controllers::substrate::kubernetes) fn describe(self) -> &'static str {
        match self {
            Self::CachesCold => "can't be checked until the node and pod caches sync",
            Self::Unregistered => "isn't a registered node",
            Self::OutsideSelector => "is outside the sandbox node selector",
            Self::CantTakeWork => "isn't Ready, is cordoned or is placement-blocked",
            Self::NoNetworkSlot => "has no free network slot",
        }
    }
}

impl KubernetesSubstrate {
    /// Whether `source_node_name`, the node holding a sandbox's local state,
    /// can take a pod pinned to it now: the Node reflector has it Ready,
    /// schedulable, inside the sandbox node selector, not placement-blocked
    /// and with a free network slot. `false` while either cache is cold.
    /// Started recovery reads it.
    pub(in crate::controllers::substrate::kubernetes) fn local_source_node_available(
        &self,
        source_node_name: &str,
    ) -> bool {
        self.local_source_node_refusal(source_node_name, 0)
            .is_none()
    }

    /// Why `source_node_name` can't take a pod pinned to it now, or `None`
    /// when it can ([`Self::local_source_node_available`]). `held` is the
    /// network slots a pod the caller will reuse there already holds, which
    /// it doesn't need again.
    pub(in crate::controllers::substrate::kubernetes) fn local_source_node_refusal(
        &self,
        source_node_name: &str,
        held: u64,
    ) -> Option<LocalSourceNodeRefusal> {
        if !self.k8s.caches.nodes_ready() || !self.pod_reflector_ready.is_ready() {
            return Some(LocalSourceNodeRefusal::CachesCold);
        }
        let config = &self.k8s.sandbox_pod_config;
        let nodes = self.k8s.caches.nodes.state();
        let Some(node) = nodes
            .iter()
            .find(|node| node.metadata.name.as_deref() == Some(source_node_name))
        else {
            return Some(LocalSourceNodeRefusal::Unregistered);
        };
        if !config.base_node_selector().matches(node) {
            return Some(LocalSourceNodeRefusal::OutsideSelector);
        }
        if !adapter::node_schedulable(node)
            || (config.ssd_headroom_tier_affinity && !adapter::node_not_placement_blocked(node))
        {
            return Some(LocalSourceNodeRefusal::CantTakeWork);
        }
        if !adapter::node_network_slot_available(node, &self.pod_network_slots, held) {
            return Some(LocalSourceNodeRefusal::NoNetworkSlot);
        }
        None
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
            }) if self.local_source_node_available(&source_node_name) => Some(source_node_name),
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
                return Err(SubstrateError::RestoreContextUnavailable {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation} has no verified local or \
                         remote restore source; leaving the execution unchanged"
                    ),
                });
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
            assignment.record_assigned(
                id,
                replacement_claim.row.generation,
                &replacement_claim.row.pod_name,
            );
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
