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
            // A newer activity clock or upload can land between the
            // pre-check and the handoff transaction; classify the answer the
            // same way so the same state can't get a different status.
            StartedRecoveryReplacement::Unavailable => Err(self
                .started_recovery_unavailable_error(sandbox_id, generation)
                .await),
            StartedRecoveryReplacement::OwnershipLost => Ok(None),
        }
    }

    /// Whether the API already sees `source_node` as lost: its row is deleted,
    /// or the warm Node cache doesn't list it, or lists it not Ready or
    /// cordoned. A cold cache answers `false`.
    fn started_recovery_source_node_looks_lost(
        &self,
        source_node: &StaleRecoverySourceNode,
    ) -> bool {
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
    ///   retryable 503 that still completes the attempt as failed. That
    ///   includes a same-node upload past the settle window while the API
    ///   doesn't yet see that node as lost; once it does, the upload is
    ///   treated as absent, as the cluster controller's loop treats it.
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
        let cause = match self
            .store
            .started_recovery_unavailable_cause(sandbox_id)
            .await
        {
            Ok(cause) => cause,
            // The classification is advisory. Failing to read it must not turn
            // a possibly recoverable sandbox into a terminal answer.
            Err(error) => return error,
        };
        match unavailable_answer(&cause, self.k8s.lost_started_give_up_enabled, |node| {
            self.started_recovery_source_node_looks_lost(node)
        }) {
            UnavailableAnswer::UploadSettling => upload_settling(sandbox_id, generation),
            UnavailableAnswer::AwaitingSourceNodeLoss => {
                SubstrateError::RestoreAwaitingSourceNodeLoss {
                    message: format!(
                        "sandbox {sandbox_id} generation {generation}: the only durable restore \
                         source predates the sandbox's last activity; waiting until the source \
                         node is confirmed gone before rolling back to it"
                    ),
                }
            }
            UnavailableAnswer::ContextUnavailable => SubstrateError::RestoreContextUnavailable {
                message: format!(
                    "sandbox {sandbox_id} generation {generation} has no verified local or \
                     remote restore source; leaving the execution unchanged"
                ),
            },
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

/// What started recovery answers for a classified unavailable restore source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnavailableAnswer {
    /// `RestoreUploadSettling`: retryable 503, the attempt completes failed.
    UploadSettling,
    /// `RestoreAwaitingSourceNodeLoss`: retryable 503.
    AwaitingSourceNodeLoss,
    /// `RestoreContextUnavailable`: 412.
    ContextUnavailable,
}

/// The decision behind `started_recovery_unavailable_error`, apart from the
/// Node cache so it can be tested. `looks_lost` is
/// `started_recovery_source_node_looks_lost`.
fn unavailable_answer(
    cause: &StartedRecoveryUnavailableCause,
    give_up_enabled: bool,
    looks_lost: impl Fn(&StaleRecoverySourceNode) -> bool,
) -> UnavailableAnswer {
    use StartedRecoveryUnavailableCause as Cause;
    match cause {
        Cause::SourcePublishing => UnavailableAnswer::UploadSettling,
        Cause::SameNodeUploadPastSettle { source_node, .. } if !looks_lost(source_node) => {
            UnavailableAnswer::UploadSettling
        }
        Cause::StaleSourceAwaitingNodeLoss { source_node }
        | Cause::SameNodeUploadPastSettle {
            source_node,
            stale_rollback_source: true,
        } if stale_source_retry_can_resolve(give_up_enabled, looks_lost(source_node)) => {
            UnavailableAnswer::AwaitingSourceNodeLoss
        }
        Cause::StaleSourceAwaitingNodeLoss { .. }
        | Cause::SameNodeUploadPastSettle { .. }
        | Cause::NoDurableSource => UnavailableAnswer::ContextUnavailable,
    }
}

fn upload_settling(sandbox_id: SandboxId, generation: ExecutionGeneration) -> SubstrateError {
    SubstrateError::RestoreUploadSettling {
        message: format!(
            "sandbox {sandbox_id} generation {generation}: a newer snapshot upload may still \
             land; retry"
        ),
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
    use super::{
        StaleRecoverySourceNode, StartedRecoveryUnavailableCause, UnavailableAnswer,
        stale_source_retry_can_resolve, unavailable_answer,
    };

    fn node() -> StaleRecoverySourceNode {
        StaleRecoverySourceNode::Named("source-node".to_string())
    }

    #[test]
    fn quiet_same_node_upload_on_a_lost_node_waits_for_the_rollback() {
        // Arrange: the node died mid-upload; the loop will fail the quiet
        // upload and roll the row back to the marker.
        let cause = StartedRecoveryUnavailableCause::SameNodeUploadPastSettle {
            source_node: node(),
            stale_rollback_source: true,
        };

        // Act / Assert.
        assert_eq!(
            unavailable_answer(&cause, true, |_| true),
            UnavailableAnswer::AwaitingSourceNodeLoss
        );
        assert_eq!(
            unavailable_answer(&cause, true, |_| false),
            UnavailableAnswer::UploadSettling,
            "a live node can still finalize the upload"
        );
        assert_eq!(
            unavailable_answer(&cause, false, |_| true),
            UnavailableAnswer::ContextUnavailable,
            "nothing rolls back with the give-up off"
        );
    }

    #[test]
    fn quiet_same_node_upload_without_a_rollback_source_is_unavailable_once_lost() {
        let cause = StartedRecoveryUnavailableCause::SameNodeUploadPastSettle {
            source_node: node(),
            stale_rollback_source: false,
        };

        assert_eq!(
            unavailable_answer(&cause, true, |_| true),
            UnavailableAnswer::ContextUnavailable
        );
        assert_eq!(
            unavailable_answer(&cause, true, |_| false),
            UnavailableAnswer::UploadSettling
        );
    }

    #[test]
    fn stale_source_and_other_causes_map_as_before() {
        let stale = StartedRecoveryUnavailableCause::StaleSourceAwaitingNodeLoss {
            source_node: node(),
        };
        assert_eq!(
            unavailable_answer(&stale, true, |_| true),
            UnavailableAnswer::AwaitingSourceNodeLoss
        );
        assert_eq!(
            unavailable_answer(&stale, true, |_| false),
            UnavailableAnswer::ContextUnavailable
        );
        assert_eq!(
            unavailable_answer(&StartedRecoveryUnavailableCause::SourcePublishing, false, |_| false),
            UnavailableAnswer::UploadSettling
        );
        assert_eq!(
            unavailable_answer(&StartedRecoveryUnavailableCause::NoDurableSource, true, |_| true),
            UnavailableAnswer::ContextUnavailable
        );
    }

    #[test]
    fn stale_source_waits_only_for_a_lost_node_with_the_give_up_on() {
        assert!(stale_source_retry_can_resolve(true, true));
        assert!(!stale_source_retry_can_resolve(true, false), "live node");
        assert!(!stale_source_retry_can_resolve(false, true), "give-up off");
        assert!(!stale_source_retry_can_resolve(false, false));
    }
}
