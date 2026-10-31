use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use pplx_space_common_core::{
    sql,
    types::{
        AutoLifecycleDeleteReceiptId, AutoLifecycleIdleAction, AutoLifecyclePolicy,
        ExecutionGeneration, ExecutionGenerationParseError, NodeId, SandboxActivationAttemptId,
        SandboxId, SandboxIdParseError, SandboxLifecycle, SandboxQuarantineReasonCode,
        SandboxQuarantineSource,
    },
};
use pplx_space_common_svc::sql::contention::{self, is_lock_not_available};
use pplx_space_common_svc::types::{
    AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE, AUTO_LIFECYCLE_DELETE_COMPLETION_LOCK_TIMEOUT,
    AUTO_LIFECYCLE_DELETE_COMPLETION_OWNERSHIP_GRACE,
    AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT, AutoLifecyclePolicyFingerprint,
    AutoLifecyclePolicyFingerprintParseError,
};
use pplx_space_metrics::sandbox::publication::{
    AutoLifecycleDeleteCompletionDisposition, AutoLifecycleDeleteCompletionReason,
};
use pplx_space_placement::StoredSandboxLifecycle;
use pplx_space_sandbox_store::delete::delete_with_dependents;
use sqlx::{Acquire as _, FromRow, Postgres, Transaction, types::Json};
use tonic::Status;

use crate::proto::AutoLifecycleDeleteCompletion;

#[derive(Debug)]
struct ParsedCompletion {
    receipt_id: AutoLifecycleDeleteReceiptId,
    sandbox_id: SandboxId,
    execution_generation: ExecutionGeneration,
    route_activation_id: SandboxActivationAttemptId,
    policy_fingerprint: AutoLifecyclePolicyFingerprint,
}

#[derive(Debug, thiserror::Error)]
enum ParsedCompletionError {
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

impl ParsedCompletionError {
    /// True when the receipt is well-formed for a newer receiver than this one.
    ///
    /// Such a receipt must never be acknowledged: acknowledgement is the
    /// publisher's signal to prune, so acknowledging here would discard a
    /// deletion this build merely cannot interpret. Deferring keeps it on the
    /// node until an upgraded receiver accepts it.
    fn is_forward_incompatible(&self) -> bool {
        matches!(
            self,
            Self::PolicyFingerprint(
                AutoLifecyclePolicyFingerprintParseError::UnsupportedVersion(_)
            )
        )
    }
}

impl TryFrom<&AutoLifecycleDeleteCompletion> for ParsedCompletion {
    type Error = ParsedCompletionError;

    fn try_from(value: &AutoLifecycleDeleteCompletion) -> Result<Self, Self::Error> {
        let receipt_id = AutoLifecycleDeleteReceiptId::from_str(&value.receipt_id)
            .map_err(ParsedCompletionError::ReceiptId)?;
        let sandbox_id =
            SandboxId::from_str(&value.sandbox_id).map_err(ParsedCompletionError::SandboxId)?;
        let execution_generation = ExecutionGeneration::try_from(value.execution_generation)
            .map_err(ParsedCompletionError::ExecutionGeneration)?;
        let route_activation_id = SandboxActivationAttemptId::from_str(&value.route_activation_id)
            .map_err(ParsedCompletionError::RouteActivationId)?;
        let policy_fingerprint =
            AutoLifecyclePolicyFingerprint::from_str(&value.auto_lifecycle_policy_fingerprint)
                .map_err(ParsedCompletionError::PolicyFingerprint)?;
        Ok(Self {
            receipt_id,
            sandbox_id,
            execution_generation,
            route_activation_id,
            policy_fingerprint,
        })
    }
}

#[derive(Debug, FromRow)]
struct CompletionAuthorityRow {
    lifecycle: String,
    auto_lifecycle_json: Json<serde_json::Value>,
    active_node_id: Option<NodeId>,
    ownership_evidence_expired: bool,
}

#[derive(Debug, FromRow)]
struct CompletionEvidenceRow {
    latest_execution_generation: ExecutionGeneration,
    route_activation_id: Option<SandboxActivationAttemptId>,
    publisher_owns_execution: bool,
    creating_snapshot_exists: bool,
}

/// What one receipt's evaluation decided, before the caller keeps or discards
/// the receipt's savepoint.
#[derive(Debug)]
enum ReceiptOutcome {
    /// The central row was deleted inside the receipt savepoint.
    Applied { from_lifecycle: SandboxLifecycle },
    /// Permanently settled: the publisher may prune the receipt.
    AcknowledgedNoop(AutoLifecycleDeleteCompletionReason),
    /// Transient: the publisher retries the receipt on a later publication.
    Deferred(AutoLifecycleDeleteCompletionReason),
}

#[derive(Debug)]
pub(super) struct DeletedSandbox {
    pub(super) sandbox_id: SandboxId,
    pub(super) from_lifecycle: SandboxLifecycle,
    pub(super) receipt_id: AutoLifecycleDeleteReceiptId,
    pub(super) execution_generation: ExecutionGeneration,
    pub(super) route_activation_id: SandboxActivationAttemptId,
}

#[derive(Debug, Default)]
pub(super) struct AutoLifecycleDeleteConsumption {
    pub(super) acknowledged_receipt_ids: Vec<AutoLifecycleDeleteReceiptId>,
    pub(super) deleted_sandboxes: Vec<DeletedSandbox>,
    pub(super) deferred_by_batch_limit: usize,
    pub(super) deferred_by_request_limit: usize,
    completion_metrics: Vec<(
        AutoLifecycleDeleteCompletionDisposition,
        AutoLifecycleDeleteCompletionReason,
    )>,
}

impl AutoLifecycleDeleteConsumption {
    pub(super) fn emit_metrics(&self) {
        let mut aggregated = BTreeMap::new();
        for &(disposition, reason) in &self.completion_metrics {
            let count = aggregated.entry((disposition, reason)).or_insert(0_u64);
            *count = count.saturating_add(1);
        }
        for ((disposition, reason), count) in aggregated {
            pplx_space_metrics::sandbox::publication::auto_lifecycle_delete_completion(
                disposition,
                reason,
                count,
            );
        }
        for (deferred, reason) in [
            (
                self.deferred_by_batch_limit,
                AutoLifecycleDeleteCompletionReason::BatchLimit,
            ),
            (
                self.deferred_by_request_limit,
                AutoLifecycleDeleteCompletionReason::RequestLimit,
            ),
        ] {
            if deferred > 0 {
                pplx_space_metrics::sandbox::publication::auto_lifecycle_delete_completion(
                    AutoLifecycleDeleteCompletionDisposition::Deferred,
                    reason,
                    u64::try_from(deferred).unwrap_or(u64::MAX),
                );
            }
        }
    }
}

fn acknowledge_noop(
    consumption: &mut AutoLifecycleDeleteConsumption,
    completion: &ParsedCompletion,
    origin_node_id: NodeId,
    reason: AutoLifecycleDeleteCompletionReason,
) {
    consumption
        .acknowledged_receipt_ids
        .push(completion.receipt_id);
    consumption.completion_metrics.push((
        AutoLifecycleDeleteCompletionDisposition::AcknowledgedNoop,
        reason,
    ));
    if reason == AutoLifecycleDeleteCompletionReason::SandboxAbsent {
        tracing::debug!(
            sandbox_id = %completion.sandbox_id,
            receipt_id = %completion.receipt_id,
            %origin_node_id,
            %reason,
            "acknowledging idempotent auto-lifecycle delete completion retry"
        );
    } else {
        tracing::warn!(
            sandbox_id = %completion.sandbox_id,
            receipt_id = %completion.receipt_id,
            %origin_node_id,
            %reason,
            "acknowledging terminal auto-lifecycle delete anomaly without deleting"
        );
    }
}

fn defer_completion(
    consumption: &mut AutoLifecycleDeleteConsumption,
    completion: &ParsedCompletion,
    reason: AutoLifecycleDeleteCompletionReason,
) {
    consumption
        .completion_metrics
        .push((AutoLifecycleDeleteCompletionDisposition::Deferred, reason));
    tracing::debug!(
        sandbox_id = %completion.sandbox_id,
        receipt_id = %completion.receipt_id,
        %reason,
        "deferring auto-lifecycle delete completion"
    );
}

fn is_retryable_finalize_error(error: &sqlx::Error) -> bool {
    // The shared set, not a local list: its sibling
    // `is_retryable_finalize_sandbox_db_delete_error` asks the same question,
    // and two spellings of one question is how one of them drifts.
    //
    // `40001` is rare at READ COMMITTED but not impossible — a row moved to
    // another partition raises it, as does a `MERGE` re-check — and it is
    // retryable wherever it appears.
    contention::is_lock_contention(error)
}

/// Parse the request's receipts, dropping and reporting the ones this build
/// cannot act on.
fn parse_completions(
    consumption: &mut AutoLifecycleDeleteConsumption,
    completions: &[AutoLifecycleDeleteCompletion],
) -> Vec<ParsedCompletion> {
    completions
        .iter()
        .filter_map(|completion| {
            let error = match ParsedCompletion::try_from(completion) {
                Ok(completion) => return Some(completion),
                Err(error) => error,
            };
            let receipt_id = AutoLifecycleDeleteReceiptId::from_str(&completion.receipt_id).ok();
            let (disposition, reason) = match (receipt_id, error.is_forward_incompatible()) {
                (None, _) => (
                    AutoLifecycleDeleteCompletionDisposition::Unacknowledgeable,
                    AutoLifecycleDeleteCompletionReason::MalformedReceipt,
                ),
                (Some(_), true) => (
                    AutoLifecycleDeleteCompletionDisposition::Deferred,
                    AutoLifecycleDeleteCompletionReason::UnsupportedFingerprintVersion,
                ),
                (Some(receipt_id), false) => {
                    consumption.acknowledged_receipt_ids.push(receipt_id);
                    (
                        AutoLifecycleDeleteCompletionDisposition::AcknowledgedNoop,
                        AutoLifecycleDeleteCompletionReason::MalformedReceipt,
                    )
                }
            };
            consumption.completion_metrics.push((disposition, reason));
            tracing::warn!(
                receipt_id = %completion.receipt_id,
                %error,
                %disposition,
                "ignoring auto-lifecycle delete completion this receiver cannot act on"
            );
            None
        })
        .collect()
}

/// Choose the bounded work set.
///
/// The durable publisher rotates retries before constructing the request, so
/// take receipts in publisher order and sort only the selected window, which
/// keeps lock acquisition deterministic without discarding that fairness.
fn select_completions(
    consumption: &mut AutoLifecycleDeleteConsumption,
    parsed_completions: Vec<ParsedCompletion>,
) -> Vec<ParsedCompletion> {
    let mut selected_sandboxes = BTreeSet::new();
    let mut selected = Vec::with_capacity(
        parsed_completions
            .len()
            .min(AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE),
    );
    for completion in parsed_completions {
        // Only a receipt for an already-selected sandbox is a duplicate. A
        // receipt the budget turned away leaves its sandbox unselected, so a
        // second receipt for it is reported against the budget that displaced
        // it rather than against the receipt that never ran.
        if selected_sandboxes.contains(&completion.sandbox_id) {
            defer_completion(
                consumption,
                &completion,
                AutoLifecycleDeleteCompletionReason::DuplicateSandboxReceipt,
            );
        } else if selected.len() < AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE {
            selected_sandboxes.insert(completion.sandbox_id);
            selected.push(completion);
        } else {
            consumption.deferred_by_batch_limit =
                consumption.deferred_by_batch_limit.saturating_add(1);
        }
    }
    // One receipt per sandbox survives selection, so the sandbox id alone is a
    // total order over the window.
    selected.sort_unstable_by_key(|completion| completion.sandbox_id);
    selected
}

pub(super) async fn consume_auto_lifecycle_delete_completions(
    tx: &mut Transaction<'_, Postgres>,
    origin_node_id: NodeId,
    completions: &[AutoLifecycleDeleteCompletion],
) -> Result<AutoLifecycleDeleteConsumption, Status> {
    let mut consumption = AutoLifecycleDeleteConsumption::default();
    if completions.is_empty() {
        return Ok(consumption);
    }
    // Bound the work the repeated field itself can create. Parsing, metric
    // accumulation, and the acknowledgement list all scale with the field, not
    // with the batch budget below.
    let completions = if completions.len() > AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT {
        let (accepted, over_limit) =
            completions.split_at(AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT);
        consumption.deferred_by_request_limit = over_limit.len();
        tracing::warn!(
            node_id = %origin_node_id,
            received = completions.len(),
            limit = AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT,
            dropped = over_limit.len(),
            "truncating oversized auto-lifecycle delete completion batch"
        );
        accepted
    } else {
        completions
    };

    let parsed_completions = parse_completions(&mut consumption, completions);
    let selected = select_completions(&mut consumption, parsed_completions);
    if selected.is_empty() {
        return Ok(consumption);
    }

    // Every evidence read below uses NOWAIT, but the canonical finalize path
    // issues plain UPDATE/DELETE statements while this transaction already
    // holds the batch's sandbox row locks. Bound them for the remainder of the
    // publication transaction; receipt consumption is its last work, and the
    // expiry raises 55P03, which the contention paths already classify.
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(format!(
            "{}ms",
            AUTO_LIFECYCLE_DELETE_COMPLETION_LOCK_TIMEOUT.as_millis()
        ))
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            Status::internal(format!(
                "bound auto-lifecycle delete completion lock waits: {error}"
            ))
        })?;

    for completion in selected {
        // One savepoint per receipt. A NOWAIT failure or a finalize deadlock
        // aborts the current Postgres (sub)transaction, so the attempt has to
        // be isolated for one contended sandbox to become a deferred receipt
        // instead of rolling back the node publication. Keep it to exactly one:
        // a savepoint that takes a row lock assigns a subtransaction id, and a
        // backend that exceeds its 64-entry subtransaction cache pushes every
        // other backend onto pg_subtrans SLRU lookups.
        let mut receipt_tx = tx.begin().await.map_err(|error| {
            Status::internal(format!(
                "begin auto-lifecycle delete receipt scope: {error}"
            ))
        })?;
        // Keeping the savepoint disposition and the bookkeeping in one match is
        // what makes a new outcome variant a compile error rather than a silent
        // rollback.
        match apply_receipt(&mut receipt_tx, origin_node_id, &completion).await? {
            ReceiptOutcome::Applied { from_lifecycle } => {
                // Releasing the savepoint promotes the delete and its row locks
                // to the publication transaction.
                receipt_tx.commit().await.map_err(|error| {
                    Status::internal(format!(
                        "finish auto-lifecycle delete receipt scope: {error}"
                    ))
                })?;
                consumption
                    .acknowledged_receipt_ids
                    .push(completion.receipt_id);
                consumption.deleted_sandboxes.push(DeletedSandbox {
                    sandbox_id: completion.sandbox_id,
                    from_lifecycle,
                    receipt_id: completion.receipt_id,
                    execution_generation: completion.execution_generation,
                    route_activation_id: completion.route_activation_id,
                });
                consumption.completion_metrics.push((
                    AutoLifecycleDeleteCompletionDisposition::Applied,
                    AutoLifecycleDeleteCompletionReason::Applied,
                ));
            }
            ReceiptOutcome::AcknowledgedNoop(reason) => {
                discard_receipt_scope(receipt_tx).await?;
                acknowledge_noop(&mut consumption, &completion, origin_node_id, reason);
            }
            ReceiptOutcome::Deferred(reason) => {
                discard_receipt_scope(receipt_tx).await?;
                defer_completion(&mut consumption, &completion, reason);
            }
        }
    }

    Ok(consumption)
}

/// Roll back a receipt's savepoint.
///
/// No receipt path other than [`ReceiptOutcome::Applied`] writes, so this only
/// releases evidence locks the publication no longer needs.
async fn discard_receipt_scope(receipt_tx: Transaction<'_, Postgres>) -> Result<(), Status> {
    receipt_tx.rollback().await.map_err(|error| {
        Status::internal(format!(
            "discard auto-lifecycle delete receipt scope: {error}"
        ))
    })
}

/// Evaluate one receipt's fencing chain and, when every link matches, delete the
/// central row.
///
/// Runs entirely inside the caller's per-receipt savepoint: on any outcome other
/// than [`ReceiptOutcome::Applied`] the caller discards it, and a returned
/// `Err` is a failure of the whole publication rather than of one receipt.
async fn apply_receipt(
    tx: &mut Transaction<'_, Postgres>,
    origin_node_id: NodeId,
    completion: &ParsedCompletion,
) -> Result<ReceiptOutcome, Status> {
    let row = sqlx::query_as::<_, CompletionAuthorityRow>(sql!(
        "SELECT s.lifecycle,
                s.auto_lifecycle_json,
                s.active_node_id,
                COALESCE(s.placement_changed_at, s.created_at)
                    <= NOW() - make_interval(secs => $2) AS ownership_evidence_expired
           FROM sandboxes s
          WHERE s.id = $1
          FOR UPDATE OF s NOWAIT"
    ))
    .bind(completion.sandbox_id)
    .bind(AUTO_LIFECYCLE_DELETE_COMPLETION_OWNERSHIP_GRACE.as_secs_f64())
    .fetch_optional(&mut **tx)
    .await;
    let row = match row {
        Ok(row) => row,
        Err(error) if is_lock_not_available(&error) => {
            return Ok(ReceiptOutcome::Deferred(
                AutoLifecycleDeleteCompletionReason::LockContended,
            ));
        }
        Err(error) => {
            return Err(Status::internal(format!(
                "inspect auto-lifecycle delete completion authority: {error}"
            )));
        }
    };

    let Some(row) = row else {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::SandboxAbsent,
        ));
    };

    // The parent-row lock above serializes this evidence read against
    // execution and snapshot inserts through their sandboxes foreign key.
    // Keep this as a second statement: PostgreSQL need not acquire a row
    // lock before evaluating subqueries in the locking SELECT itself.
    let evidence = sqlx::query_as::<_, CompletionEvidenceRow>(sql!(
        "SELECT execution.generation AS latest_execution_generation,
                execution.route_activation_id,
                EXISTS (
                    SELECT 1
                      FROM node_k8s
                     WHERE node_k8s.node_id = $2
                       AND node_k8s.k8s_node_name = execution.k8s_node_name
                ) AS publisher_owns_execution,
                EXISTS (
                    SELECT 1
                      FROM snapshots snapshot
                     WHERE snapshot.sandbox_id = $1
                       AND snapshot.state = 'creating'
                ) AS creating_snapshot_exists
           FROM sandbox_k8s_executions execution
          WHERE execution.sandbox_id = $1
          ORDER BY execution.generation DESC
          LIMIT 1
          FOR UPDATE OF execution NOWAIT"
    ))
    .bind(completion.sandbox_id)
    .bind(origin_node_id)
    .fetch_optional(&mut **tx)
    .await;
    let evidence = match evidence {
        Ok(evidence) => evidence,
        Err(error) if is_lock_not_available(&error) => {
            return Ok(ReceiptOutcome::Deferred(
                AutoLifecycleDeleteCompletionReason::LockContended,
            ));
        }
        Err(error) => {
            return Err(Status::internal(format!(
                "inspect auto-lifecycle delete completion evidence: {error}"
            )));
        }
    };

    let Some(evidence) = evidence else {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::MissingLineage,
        ));
    };
    let latest_generation = evidence.latest_execution_generation;
    if latest_generation > completion.execution_generation {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::StaleGeneration,
        ));
    }
    if latest_generation < completion.execution_generation {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::AheadOfLineage,
        ));
    }
    let Some(route_activation_id) = evidence.route_activation_id else {
        // Receipts are emitted only after the route-assignment write has
        // populated this identity. A missing value is legacy or partial
        // central evidence and cannot be relaxed to generation-only ABA
        // fencing.
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::MissingRouteIdentity,
        ));
    };
    if route_activation_id != completion.route_activation_id {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::RouteIncarnationMismatch,
        ));
    }
    if !evidence.publisher_owns_execution {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::DifferentExecutionNode,
        ));
    }

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
            return Ok(ReceiptOutcome::AcknowledgedNoop(
                AutoLifecycleDeleteCompletionReason::UnknownLifecycle,
            ));
        }
    };
    let from_lifecycle = match stored_lifecycle {
        StoredSandboxLifecycle::Deleted => {
            return Ok(ReceiptOutcome::AcknowledgedNoop(
                AutoLifecycleDeleteCompletionReason::DeletedTombstone,
            ));
        }
        StoredSandboxLifecycle::CleanupRequired => {
            return Ok(ReceiptOutcome::AcknowledgedNoop(
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
                return Ok(ReceiptOutcome::Deferred(
                    AutoLifecycleDeleteCompletionReason::PolicyUnreadable,
                ));
            }
        };
    let current_policy_fingerprint = AutoLifecyclePolicyFingerprint::for_policy(&current_policy);
    if current_policy_fingerprint != completion.policy_fingerprint {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::PolicyMismatch,
        ));
    }
    // The fingerprint already commits to the delete action. Retain this
    // explicit authorization check as defense against a nonconforming
    // publisher or a future fingerprint projection bug.
    if !current_policy.contains_action(AutoLifecycleIdleAction::Delete) {
        return Ok(ReceiptOutcome::AcknowledgedNoop(
            AutoLifecycleDeleteCompletionReason::PolicyDoesNotAuthorizeDelete,
        ));
    }
    if from_lifecycle.is_transient() {
        return Ok(ReceiptOutcome::Deferred(
            AutoLifecycleDeleteCompletionReason::TransientLifecycle,
        ));
    }

    if from_lifecycle == SandboxLifecycle::Quarantined {
        let matching_orphan_quarantine = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT COALESCE(last_observed_node_id = $2, FALSE)
                    AND source = $3
                    AND reason_code = ANY($4::TEXT[])
               FROM sandbox_quarantine_state
              WHERE sandbox_id = $1
              FOR UPDATE NOWAIT"
        ))
        .bind(completion.sandbox_id)
        .bind(origin_node_id)
        .bind(SandboxQuarantineSource::ClusterController)
        .bind(vec![
            SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact,
            SandboxQuarantineReasonCode::OrphanedExecutionLegacyDeletingState,
        ])
        .fetch_optional(&mut **tx)
        .await;
        let matching_orphan_quarantine = match matching_orphan_quarantine {
            Ok(matching) => matching.unwrap_or(false),
            Err(error) if is_lock_not_available(&error) => {
                return Ok(ReceiptOutcome::Deferred(
                    AutoLifecycleDeleteCompletionReason::LockContended,
                ));
            }
            Err(error) => {
                return Err(Status::internal(format!(
                    "inspect auto-lifecycle delete quarantine ownership: {error}"
                )));
            }
        };
        if !matching_orphan_quarantine {
            return Ok(ReceiptOutcome::AcknowledgedNoop(
                AutoLifecycleDeleteCompletionReason::QuarantineHold,
            ));
        }
    } else {
        match row.active_node_id {
            Some(node_id) if node_id == origin_node_id => {}
            Some(_) => {
                return Ok(ReceiptOutcome::AcknowledgedNoop(
                    AutoLifecycleDeleteCompletionReason::DifferentActiveNode,
                ));
            }
            None => match from_lifecycle {
                // Dormant rows clear their active placement by construction, so
                // the locked latest execution row is the durable ownership
                // evidence.
                SandboxLifecycle::Stopped | SandboxLifecycle::Suspended => {}
                // A live-looking row with no active node is mid-transition.
                // Defer until placement is republished, but only for as long as
                // a transition could plausibly still be running.
                SandboxLifecycle::Started | SandboxLifecycle::Paused => {
                    if row.ownership_evidence_expired {
                        return Ok(ReceiptOutcome::AcknowledgedNoop(
                            AutoLifecycleDeleteCompletionReason::OwnershipEvidenceExpired,
                        ));
                    }
                    return Ok(ReceiptOutcome::Deferred(
                        AutoLifecycleDeleteCompletionReason::AwaitingOwnershipEvidence,
                    ));
                }
                // Never placed or already failed out of placement: there is no
                // ownership left to prove, and no retry would produce one.
                SandboxLifecycle::Created | SandboxLifecycle::Failed => {
                    return Ok(ReceiptOutcome::AcknowledgedNoop(
                        AutoLifecycleDeleteCompletionReason::OwnershipAbsent,
                    ));
                }
                // Transient states returned above and quarantine took the other
                // branch. Fail closed rather than widen the delete-authorizing
                // set if that ever stops holding.
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
                    return Ok(ReceiptOutcome::AcknowledgedNoop(
                        AutoLifecycleDeleteCompletionReason::OwnershipAbsent,
                    ));
                }
            },
        }
    }
    if evidence.creating_snapshot_exists {
        return Ok(ReceiptOutcome::Deferred(
            AutoLifecycleDeleteCompletionReason::CreatingSnapshot,
        ));
    }

    match delete_with_dependents(tx, completion.sandbox_id).await {
        Ok(()) => Ok(ReceiptOutcome::Applied { from_lifecycle }),
        Err(error) if is_retryable_finalize_error(&error) => Ok(ReceiptOutcome::Deferred(
            AutoLifecycleDeleteCompletionReason::FinalizeContended,
        )),
        Err(error) => Err(Status::internal(format!(
            "finalize auto-lifecycle delete completion: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests;
