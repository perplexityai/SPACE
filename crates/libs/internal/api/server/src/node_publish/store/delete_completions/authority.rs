use std::{ops::ControlFlow, str::FromStr};

use super::{
    AutoLifecycleDeleteCompletionReason, AutoLifecycleIdleAction, AutoLifecyclePolicy,
    AutoLifecyclePolicyFingerprint, CompletionAuthorityRow, CompletionEvidenceRow, NodeId,
    ParsedCompletion, ReceiptOutcome, SandboxLifecycle, StoredSandboxLifecycle,
};

/// Policy authority from the locked sandbox row, with placement still to check.
#[derive(Debug)]
pub(super) struct DeleteAuthority {
    from_lifecycle: SandboxLifecycle,
    active_node_id: Option<NodeId>,
    ownership_evidence_expired: bool,
}

impl DeleteAuthority {
    pub(super) fn prior_lifecycle(&self) -> SandboxLifecycle {
        self.from_lifecycle
    }

    /// Non-quarantined rows use active placement or durable dormant ownership.
    /// The expiry bit is evaluated by PostgreSQL while acquiring the row lock.
    pub(super) fn ownership_outcome(&self, origin_node_id: NodeId) -> Option<ReceiptOutcome> {
        match self.active_node_id {
            Some(node_id) if node_id == origin_node_id => {}
            Some(_) => {
                return Some(ReceiptOutcome::AcknowledgedNoop(
                    AutoLifecycleDeleteCompletionReason::DifferentActiveNode,
                ));
            }
            None => match self.from_lifecycle {
                // Dormant rows clear their active placement by construction, so
                // the locked latest execution row is the durable ownership
                // evidence.
                SandboxLifecycle::Stopped | SandboxLifecycle::Suspended => {}
                // A live-looking row with no active node is mid-transition.
                // Defer until placement is republished, but only for as long as
                // a transition could plausibly still be running.
                SandboxLifecycle::Started | SandboxLifecycle::Paused => {
                    if self.ownership_evidence_expired {
                        return Some(ReceiptOutcome::AcknowledgedNoop(
                            AutoLifecycleDeleteCompletionReason::OwnershipEvidenceExpired,
                        ));
                    }
                    return Some(ReceiptOutcome::Deferred(
                        AutoLifecycleDeleteCompletionReason::AwaitingOwnershipEvidence,
                    ));
                }
                // Never placed or already failed out of placement: there is no
                // ownership left to prove, and no retry would produce one.
                SandboxLifecycle::Created | SandboxLifecycle::Failed => {
                    return Some(ReceiptOutcome::AcknowledgedNoop(
                        AutoLifecycleDeleteCompletionReason::OwnershipAbsent,
                    ));
                }
                // Transient states returned before this check and quarantine
                // took the other branch. Fail closed rather than widen the
                // delete-authorizing set if that ever stops holding.
                SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Suspending
                | SandboxLifecycle::Stopping
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Checkpointing
                | SandboxLifecycle::Quarantined => {
                    return Some(ReceiptOutcome::AcknowledgedNoop(
                        AutoLifecycleDeleteCompletionReason::OwnershipAbsent,
                    ));
                }
            },
        }
        None
    }
}

/// Compare the locked latest execution before examining lifecycle or policy.
pub(super) fn execution_outcome(
    evidence: &CompletionEvidenceRow,
    completion: &ParsedCompletion,
) -> Option<ReceiptOutcome> {
    let latest_generation = evidence.latest_execution_generation;
    if latest_generation > completion.execution_generation {
        return Some(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::StaleGeneration,
        ));
    }
    if latest_generation < completion.execution_generation {
        return Some(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::AheadOfLineage,
        ));
    }
    let Some(route_activation_id) = evidence.route_activation_id else {
        // Receipts are emitted only after the route-assignment write has
        // populated this identity. A missing value is legacy or partial
        // central evidence and cannot be relaxed to generation-only ABA
        // fencing.
        return Some(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::MissingRouteIdentity,
        ));
    };
    if route_activation_id != completion.route_activation_id {
        return Some(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::RouteIncarnationMismatch,
        ));
    }
    if !evidence.publisher_owns_execution {
        return Some(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::DifferentExecutionNode,
        ));
    }
    None
}

/// Check the locked row only after the execution-lineage fences have passed.
/// Preserve rejection precedence; an ineligible claim is a receipt outcome.
pub(super) fn decide(
    row: CompletionAuthorityRow,
    completion: &ParsedCompletion,
) -> ControlFlow<ReceiptOutcome, DeleteAuthority> {
    let stored_lifecycle = match StoredSandboxLifecycle::from_str(&row.lifecycle) {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %completion.sandbox_id,
                receipt_id = %completion.receipt_id,
                lifecycle = %row.lifecycle,
                %error,
                "acknowledging auto-lifecycle delete completion for unknown central lifecycle"
            );
            return ControlFlow::Break(ReceiptOutcome::AcknowledgedNoop(
                AutoLifecycleDeleteCompletionReason::UnknownLifecycle,
            ));
        }
    };
    let from_lifecycle = match stored_lifecycle {
        StoredSandboxLifecycle::Deleted => {
            return ControlFlow::Break(ReceiptOutcome::AcknowledgedNoop(
                AutoLifecycleDeleteCompletionReason::DeletedTombstone,
            ));
        }
        StoredSandboxLifecycle::CleanupRequired => {
            return ControlFlow::Break(ReceiptOutcome::AcknowledgedNoop(
                AutoLifecycleDeleteCompletionReason::CleanupRequired,
            ));
        }
        StoredSandboxLifecycle::Live(lifecycle) => lifecycle,
    };
    let current_policy =
        match serde_json::from_value::<AutoLifecyclePolicy>(row.auto_lifecycle_json.0) {
            Ok(policy) => policy,
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %completion.sandbox_id,
                    receipt_id = %completion.receipt_id,
                    %error,
                    "deferring auto-lifecycle delete completion with unreadable central policy"
                );
                return ControlFlow::Break(ReceiptOutcome::Deferred(
                    AutoLifecycleDeleteCompletionReason::PolicyUnreadable,
                ));
            }
        };
    let current_policy_fingerprint = AutoLifecyclePolicyFingerprint::for_policy(&current_policy);
    if current_policy_fingerprint != completion.policy_fingerprint {
        return ControlFlow::Break(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::PolicyMismatch,
        ));
    }
    // The fingerprint already commits to the delete action. Retain this
    // explicit authorization check as defense against a nonconforming
    // publisher or a future fingerprint projection bug.
    if !current_policy.contains_action(AutoLifecycleIdleAction::Delete) {
        return ControlFlow::Break(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::PolicyDoesNotAuthorizeDelete,
        ));
    }
    if from_lifecycle.is_transient() {
        return ControlFlow::Break(ReceiptOutcome::Deferred(
            AutoLifecycleDeleteCompletionReason::TransientLifecycle,
        ));
    }
    ControlFlow::Continue(DeleteAuthority {
        from_lifecycle,
        active_node_id: row.active_node_id,
        ownership_evidence_expired: row.ownership_evidence_expired,
    })
}
