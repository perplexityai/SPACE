use std::{collections::BTreeSet, str::FromStr};

use pplx_space_common_core::types::{
    AutoLifecycleDeleteReceiptId, ExecutionGeneration, ExecutionGenerationParseError,
    SandboxActivationAttemptId, SandboxId, SandboxIdParseError,
};
use pplx_space_common_svc::types::{
    AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE, AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT,
    AutoLifecyclePolicyFingerprint, AutoLifecyclePolicyFingerprintParseError,
};
use pplx_space_metrics::sandbox::publication::{
    AutoLifecycleDeleteCompletionDisposition as Disposition,
    AutoLifecycleDeleteCompletionReason as Reason,
};

use crate::node_publish::diagnostics::DeleteCompletionDiagnostic;
use crate::proto::AutoLifecycleDeleteCompletion;

/// A node-authored receipt, not a decision to delete the current database row.
#[derive(Debug)]
pub(crate) struct DeleteCompletionClaim {
    pub(crate) receipt_id: AutoLifecycleDeleteReceiptId,
    pub(crate) sandbox_id: SandboxId,
    pub(crate) execution_generation: ExecutionGeneration,
    pub(crate) route_activation_id: SandboxActivationAttemptId,
    pub(crate) policy_fingerprint: AutoLifecyclePolicyFingerprint,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompletionValidationError {
    #[error("invalid auto-lifecycle delete receipt_id: {0}")]
    ReceiptId(#[source] uuid::Error),
    #[error("invalid auto-lifecycle delete sandbox_id: {0}")]
    SandboxId(#[source] SandboxIdParseError),
    #[error("invalid auto-lifecycle delete execution_generation: {0}")]
    ExecutionGeneration(#[source] ExecutionGenerationParseError),
    #[error("invalid auto-lifecycle delete route_activation_id: {0}")]
    RouteActivationId(#[source] uuid::Error),
    #[error("invalid auto-lifecycle delete policy fingerprint: {0}")]
    PolicyFingerprint(#[source] AutoLifecyclePolicyFingerprintParseError),
}

impl CompletionValidationError {
    /// A newer fingerprint must be deferred, never acknowledged: the sender
    /// would otherwise prune evidence that this build merely cannot interpret.
    fn is_forward_incompatible(&self) -> bool {
        matches!(
            self,
            Self::PolicyFingerprint(
                AutoLifecyclePolicyFingerprintParseError::UnsupportedVersion(_)
            )
        )
    }
}

impl TryFrom<&AutoLifecycleDeleteCompletion> for DeleteCompletionClaim {
    type Error = CompletionValidationError;

    fn try_from(value: &AutoLifecycleDeleteCompletion) -> Result<Self, Self::Error> {
        let receipt_id = AutoLifecycleDeleteReceiptId::from_str(&value.receipt_id)
            .map_err(CompletionValidationError::ReceiptId)?;
        let sandbox_id =
            SandboxId::from_str(&value.sandbox_id).map_err(CompletionValidationError::SandboxId)?;
        let execution_generation = ExecutionGeneration::try_from(value.execution_generation)
            .map_err(CompletionValidationError::ExecutionGeneration)?;
        let route_activation_id = SandboxActivationAttemptId::from_str(&value.route_activation_id)
            .map_err(CompletionValidationError::RouteActivationId)?;
        let policy_fingerprint =
            AutoLifecyclePolicyFingerprint::from_str(&value.auto_lifecycle_policy_fingerprint)
                .map_err(CompletionValidationError::PolicyFingerprint)?;
        Ok(Self {
            receipt_id,
            sandbox_id,
            execution_generation,
            route_activation_id,
            policy_fingerprint,
        })
    }
}

/// Selection is pure; ownership, lifecycle, and execution fencing are not.
#[derive(Debug, Default)]
pub(crate) struct ValidatedDeleteCompletions {
    pub(crate) selected: Vec<DeleteCompletionClaim>,
    pub(crate) acknowledged_receipt_ids: Vec<AutoLifecycleDeleteReceiptId>,
    pub(crate) deferred_by_batch_limit: usize,
    pub(crate) deferred_by_request_limit: usize,
    pub(crate) completion_metrics: Vec<(Disposition, Reason)>,
    pub(crate) diagnostics: Vec<DeleteCompletionDiagnostic>,
}

impl ValidatedDeleteCompletions {
    pub(crate) fn from_proto(completions: &[AutoLifecycleDeleteCompletion]) -> Self {
        let mut validated = Self::default();
        // Bound parsing and acknowledgement growth independently of the work
        // budget. Entries beyond this prefix are not inspected.
        let completions = if completions.len() > AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT {
            let (accepted, over_limit) =
                completions.split_at(AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT);
            validated.deferred_by_request_limit = over_limit.len();
            validated
                .diagnostics
                .push(DeleteCompletionDiagnostic::Oversized {
                    received: completions.len(),
                    dropped: over_limit.len(),
                });
            accepted
        } else {
            completions
        };
        let parsed = validated.parse(completions);
        validated.selected = validated.select(parsed);
        validated
    }

    fn parse(
        &mut self,
        completions: &[AutoLifecycleDeleteCompletion],
    ) -> Vec<DeleteCompletionClaim> {
        completions
            .iter()
            .filter_map(|completion| {
                let error = match DeleteCompletionClaim::try_from(completion) {
                    Ok(completion) => return Some(completion),
                    Err(error) => error,
                };
                let receipt_id =
                    AutoLifecycleDeleteReceiptId::from_str(&completion.receipt_id).ok();
                let (disposition, reason) = match (receipt_id, error.is_forward_incompatible()) {
                    (None, _) => (Disposition::Unacknowledgeable, Reason::MalformedReceipt),
                    (Some(_), true) => {
                        (Disposition::Deferred, Reason::UnsupportedFingerprintVersion)
                    }
                    (Some(receipt_id), false) => {
                        self.acknowledged_receipt_ids.push(receipt_id);
                        (Disposition::AcknowledgedNoop, Reason::MalformedReceipt)
                    }
                };
                self.completion_metrics.push((disposition, reason));
                self.diagnostics
                    .push(DeleteCompletionDiagnostic::Malformed {
                        receipt_id: completion.receipt_id.clone(),
                        error,
                        disposition,
                    });
                None
            })
            .collect()
    }

    fn select(&mut self, parsed: Vec<DeleteCompletionClaim>) -> Vec<DeleteCompletionClaim> {
        let mut selected_sandboxes = BTreeSet::new();
        let mut selected = Vec::with_capacity(
            parsed
                .len()
                .min(AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE),
        );
        // Select in publisher order to preserve its retry rotation. A receipt
        // turned away by the budget does not make later receipts duplicates.
        for completion in parsed {
            if selected_sandboxes.contains(&completion.sandbox_id) {
                self.completion_metrics
                    .push((Disposition::Deferred, Reason::DuplicateSandboxReceipt));
                self.diagnostics
                    .push(DeleteCompletionDiagnostic::Duplicate {
                        sandbox_id: completion.sandbox_id,
                        receipt_id: completion.receipt_id,
                    });
            } else if selected.len() < AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE {
                selected_sandboxes.insert(completion.sandbox_id);
                selected.push(completion);
            } else {
                self.deferred_by_batch_limit = self.deferred_by_batch_limit.saturating_add(1);
            }
        }
        // Selection leaves one receipt per sandbox, so this is a total lock order.
        selected.sort_unstable_by_key(|completion| completion.sandbox_id);
        selected
    }
}

#[cfg(test)]
mod tests;
