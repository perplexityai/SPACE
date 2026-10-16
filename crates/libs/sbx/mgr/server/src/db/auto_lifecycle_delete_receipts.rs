use std::str::FromStr;

use crate::db::{Connection, Error, OptionalExtension};
use pplx_space_common_core::types::{
    AutoLifecycleDeleteReceiptId, ExecutionGeneration, NodeManagedBy, NodeRole, PolicyAuthority,
    SandboxActivationAttemptId, SandboxId,
};
use pplx_space_common_svc::{sql::sqlite::SqliteU64, types::AutoLifecyclePolicyFingerprint};
use rusqlite::{params, types::Type};

use crate::auto_lifecycle_delete_receipts::{
    AutoLifecycleDeleteAdmission, AutoLifecycleDeleteBlockReason, AutoLifecycleDeleteBlockedStats,
    AutoLifecycleDeleteOrdinaryReason, AutoLifecycleDeleteOutboxState,
    AutoLifecycleDeleteOutboxStats, AutoLifecycleDeleteReceipt, RouteEvidence,
    RouteIdentityProvenance,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RouteIdentityAdoptionMetadata {
    pub(crate) current_provenance: Option<RouteIdentityProvenance>,
    retained_evidence: Option<RouteEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SupersededPendingReceipt {
    pub(crate) receipt_id: AutoLifecycleDeleteReceiptId,
    pub(crate) execution_generation: ExecutionGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "receipt supersession must be emitted after the transaction commits"]
pub(crate) struct PendingReceiptSupersession {
    pub(crate) receipt: SupersededPendingReceipt,
    pub(crate) superseding_generation: ExecutionGeneration,
}

impl RouteIdentityAdoptionMetadata {
    pub(crate) fn is_current(
        self,
        identity: &pplx_space_common_core::types::SpaceApiRouteIdentity,
        provenance: RouteIdentityProvenance,
    ) -> bool {
        if self.current_provenance != Some(provenance) {
            return false;
        }
        match provenance {
            RouteIdentityProvenance::Unfenced => true,
            RouteIdentityProvenance::ClusterFenced => {
                RouteEvidence::from_identity(identity, provenance)
                    .is_some_and(|evidence| self.retained_evidence == Some(evidence))
            }
        }
    }
}

const RECEIPT_BATCH_SIZE: usize =
    pplx_space_common_svc::types::AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE;
const AUTO_LIFECYCLE_DELETE_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_secs(15 * 60);

#[derive(Debug, thiserror::Error)]
#[error("{message}", message = Self::DIAGNOSTIC)]
pub(crate) struct PendingReceiptOwnsDeletion;

impl PendingReceiptOwnsDeletion {
    pub(crate) const DIAGNOSTIC: &'static str =
        "pending auto-lifecycle delete receipt owns sandbox deletion";
}

#[derive(Debug, thiserror::Error)]
#[error("pending auto-lifecycle delete receipt owns the sandbox execution")]
pub(crate) struct PendingReceiptOwnsExecution;

#[derive(Debug, thiserror::Error)]
#[error("sandbox lifecycle or cleanup intent owns the sandbox execution")]
pub(crate) struct LifecycleOwnsExecution;

pub(crate) fn execution_write_miss_error(
    conn: &Connection,
    sandbox_id: SandboxId,
    incoming_generation: Option<ExecutionGeneration>,
) -> Error {
    match conn.query_row(
        "SELECT
             EXISTS (SELECT 1 FROM sandboxes WHERE id = ?1),
             EXISTS (
                 SELECT 1
                   FROM auto_lifecycle_delete_outbox
                  WHERE sandbox_id = ?1
                    AND state = 'pending'
                    AND (?2 IS NULL OR execution_generation >= ?2)
             )",
        params![
            sandbox_id.to_string(),
            incoming_generation.map(ExecutionGeneration::get)
        ],
        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
    ) {
        Ok((false, _)) => Error::no_rows(),
        Ok((true, true)) => Error::to_sql_conversion(PendingReceiptOwnsExecution),
        Ok((true, false)) => Error::to_sql_conversion(LifecycleOwnsExecution),
        Err(error) => error,
    }
}

pub(crate) fn is_pending_receipt_execution_fence(error: &Error) -> bool {
    matches!(
        error.raw(),
        rusqlite::Error::ToSqlConversionFailure(source)
            if source.downcast_ref::<PendingReceiptOwnsExecution>().is_some()
    )
}

pub(crate) fn is_lifecycle_execution_fence(error: &Error) -> bool {
    matches!(
        error.raw(),
        rusqlite::Error::ToSqlConversionFailure(source)
            if source.downcast_ref::<LifecycleOwnsExecution>().is_some()
    )
}

pub(crate) fn map_pending_receipt_delete_error(error: Error) -> Error {
    match error.raw() {
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message == PendingReceiptOwnsDeletion::DIAGNOSTIC =>
        {
            Error::to_sql_conversion(PendingReceiptOwnsDeletion)
        }
        _ => error,
    }
}

#[derive(Debug, thiserror::Error)]
#[error("auto-lifecycle delete receipt conflicts with existing evidence for sandbox {sandbox_id}")]
pub(crate) struct ReceiptEvidenceConflict {
    sandbox_id: SandboxId,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InsertPendingReceiptError {
    #[error(transparent)]
    Database(#[from] Error),
    #[error(transparent)]
    Conflict(#[from] ReceiptEvidenceConflict),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompletePendingReceiptError {
    #[error(transparent)]
    Database(#[from] Error),
    #[error(transparent)]
    Conflict(#[from] ReceiptEvidenceConflict),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AutoLifecycleDeleteEvidenceError {
    #[error(transparent)]
    Database(#[from] Error),
    #[error("auto-lifecycle policy changed before delete admission")]
    PolicyChanged,
    #[error("auto-lifecycle authority changed from {expected} to {actual} before delete admission")]
    AuthorityChanged {
        expected: PolicyAuthority,
        actual: PolicyAuthority,
    },
}

#[derive(Clone, Copy)]
struct PersistedRouteEvidence<'a> {
    generation: i64,
    attempt: &'a str,
    substrate: &'a str,
    provenance: &'a str,
}

pub(crate) fn classify_and_record_admission(
    conn: &Connection,
    sandbox_id: SandboxId,
    node_role: Option<NodeRole>,
    node_managed_by: Option<NodeManagedBy>,
    expectation: crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation,
) -> Result<AutoLifecycleDeleteAdmission, AutoLifecycleDeleteEvidenceError> {
    let values = conn
        .query_row(
            "SELECT auto_lifecycle_authority,
                    auto_lifecycle_json,
                    space_api_route_execution_generation,
                    space_api_route_activation_attempt_id,
                    space_api_route_substrate_kind,
                    space_api_route_identity_provenance,
                    last_observed_execution_generation,
                    last_observed_route_activation_id,
                    last_observed_route_substrate_kind,
                    last_observed_route_identity_provenance,
                    auto_lifecycle_delete_block_reason,
                    state = 'suspended',
                    space_api_route_pod_uid IS NOT NULL,
                    last_observed_route_generation_substrate_kind
               FROM sandboxes
              WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, bool>(11)?,
                    row.get::<_, bool>(12)?,
                    row.get::<_, Option<String>>(13)?,
                ))
            },
        )
        .optional()?;
    let Some((
        authority,
        policy_json,
        current_generation,
        current_attempt,
        current_substrate,
        current_provenance,
        retained_generation,
        retained_attempt,
        retained_substrate,
        retained_provenance,
        prior_block_reason,
        is_suspended,
        has_pod_uid,
        retained_generation_substrate,
    )) = values
    else {
        return Err(AutoLifecycleDeleteEvidenceError::Database(Error::no_rows()));
    };

    let policy: pplx_space_common_core::types::AutoLifecyclePolicy =
        serde_json::from_str(&policy_json)
            .map_err(|error| crate::db::Error::decode(1, Type::Text, error))?;
    if AutoLifecyclePolicyFingerprint::for_policy(&policy) != expectation.policy_fingerprint {
        return Err(AutoLifecycleDeleteEvidenceError::PolicyChanged);
    }
    let authority: PolicyAuthority = parse_text(&authority, 0).map_err(crate::db::Error::from)?;
    if authority != expectation.authority {
        return Err(AutoLifecycleDeleteEvidenceError::AuthorityChanged {
            expected: expectation.authority,
            actual: authority,
        });
    }
    if authority == PolicyAuthority::Local {
        clear_block(conn, sandbox_id)?;
        return Ok(AutoLifecycleDeleteAdmission::Ordinary(
            AutoLifecycleDeleteOrdinaryReason::LocalAuthority,
        ));
    }
    // Deployment-owned substrate can classify a route-less Direct row, but
    // never overrides contrary or unknown persisted execution evidence.
    let mut substrate = node_managed_by;
    for raw in [
        current_substrate.as_deref(),
        retained_substrate.as_deref(),
        retained_generation_substrate.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        let parsed = match raw.parse::<NodeManagedBy>() {
            Ok(parsed) => parsed,
            Err(_) => {
                return record_block(
                    conn,
                    sandbox_id,
                    prior_block_reason.as_deref(),
                    AutoLifecycleDeleteBlockReason::UnknownSubstrate,
                );
            }
        };
        if substrate.is_some_and(|prior| prior != parsed) {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity,
            );
        }
        substrate = Some(parsed);
    }
    if substrate == Some(NodeManagedBy::Space) {
        if pending_receipt_for_sandbox(conn, sandbox_id)?.is_some()
            || has_pod_uid
            || [
                current_provenance.as_deref(),
                retained_provenance.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|value| value != "unfenced")
        {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity,
            );
        }
        clear_block(conn, sandbox_id)?;
        return Ok(AutoLifecycleDeleteAdmission::Ordinary(
            AutoLifecycleDeleteOrdinaryReason::DirectUnsupported,
        ));
    }

    let current_present = current_generation.is_some()
        || current_attempt.is_some()
        || current_substrate.is_some()
        || current_provenance.is_some();
    let retained_evidence = match (
        retained_generation,
        retained_attempt.as_deref(),
        retained_substrate.as_deref(),
        retained_provenance.as_deref(),
    ) {
        (Some(generation), Some(attempt), Some(substrate), Some(provenance)) => {
            Some(PersistedRouteEvidence {
                generation,
                attempt,
                substrate,
                provenance,
            })
        }
        _ => None,
    };
    if retained_evidence.is_none()
        && (retained_generation.is_some()
            || retained_attempt.is_some()
            || retained_substrate.is_some()
            || retained_provenance.is_some())
        && current_present
    {
        return record_block(
            conn,
            sandbox_id,
            prior_block_reason.as_deref(),
            AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity,
        );
    }
    // Successful suspend retains only the current execution generation (and
    // pod UID, which is not part of the receipt) while the trigger captures
    // the complete route tuple. Accept exactly that settled lifecycle shape;
    // every other partial current identity remains contradictory evidence.
    let current_is_compatible_dormant_subset = is_suspended
        && current_generation.is_some()
        && current_attempt.is_none()
        && current_substrate.is_none()
        && current_provenance.is_none()
        && retained_evidence
            .as_ref()
            .is_some_and(|evidence| current_generation == Some(evidence.generation));
    let evidence = if current_present {
        match (
            current_generation,
            current_attempt.as_deref(),
            current_substrate.as_deref(),
            current_provenance.as_deref(),
        ) {
            (Some(generation), Some(attempt), Some(substrate), Some(provenance)) => {
                Some(PersistedRouteEvidence {
                    generation,
                    attempt,
                    substrate,
                    provenance,
                })
            }
            _ if current_is_compatible_dormant_subset => retained_evidence,
            (_, _, None, _) => {
                return record_block(
                    conn,
                    sandbox_id,
                    prior_block_reason.as_deref(),
                    AutoLifecycleDeleteBlockReason::MissingSubstrate,
                );
            }
            (_, _, _, None) => {
                return record_block(
                    conn,
                    sandbox_id,
                    prior_block_reason.as_deref(),
                    AutoLifecycleDeleteBlockReason::UnknownProvenance,
                );
            }
            _ => {
                return record_block(
                    conn,
                    sandbox_id,
                    prior_block_reason.as_deref(),
                    AutoLifecycleDeleteBlockReason::PartialCurrentRoute,
                );
            }
        }
    } else {
        match retained_evidence {
            Some(evidence) => Some(evidence),
            None if retained_generation.is_none()
                && retained_attempt.is_none()
                && retained_substrate.is_none()
                && retained_provenance.is_none() =>
            {
                None
            }
            None if retained_substrate.is_none() => {
                return record_block(
                    conn,
                    sandbox_id,
                    prior_block_reason.as_deref(),
                    AutoLifecycleDeleteBlockReason::MissingSubstrate,
                );
            }
            None if retained_provenance.is_none() => {
                return record_block(
                    conn,
                    sandbox_id,
                    prior_block_reason.as_deref(),
                    AutoLifecycleDeleteBlockReason::UnknownProvenance,
                );
            }
            None => {
                return record_block(
                    conn,
                    sandbox_id,
                    prior_block_reason.as_deref(),
                    AutoLifecycleDeleteBlockReason::MissingRouteIdentity,
                );
            }
        }
    };
    let matching_current_and_retained = if current_present
        && !current_is_compatible_dormant_subset
        && let (Some(current), Some(retained)) = (evidence, retained_evidence)
    {
        if current.generation != retained.generation
            || current.substrate != retained.substrate
            || current.provenance != retained.provenance
        {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity,
            );
        }
        Some((current, retained))
    } else {
        None
    };
    let Some(evidence) = evidence else {
        return record_block(
            conn,
            sandbox_id,
            prior_block_reason.as_deref(),
            AutoLifecycleDeleteBlockReason::MissingRouteIdentity,
        );
    };
    // Direct rows were admitted before requiring route evidence. Receipt
    // publication additionally requires a Worker on the Kubernetes substrate.
    match node_role {
        None => {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::NodeRoleUnavailable,
            );
        }
        Some(NodeRole::Worker) => {}
        Some(NodeRole::Builder | NodeRole::Cluster) => {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::NodeRoleNotWorker,
            );
        }
    }
    let provenance = match evidence.provenance.parse::<RouteIdentityProvenance>() {
        Ok(provenance) => provenance,
        Err(_) => {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::UnknownProvenance,
            );
        }
    };
    if provenance != RouteIdentityProvenance::ClusterFenced {
        return record_block(
            conn,
            sandbox_id,
            prior_block_reason.as_deref(),
            AutoLifecycleDeleteBlockReason::UnfencedRoute,
        );
    }
    let (execution_generation, activation_attempt_id) = match (
        ExecutionGeneration::new(evidence.generation),
        evidence.attempt.parse::<SandboxActivationAttemptId>(),
    ) {
        (Ok(generation), Ok(attempt)) => (generation, attempt),
        _ => {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity,
            );
        }
    };
    if let Some((_, retained)) = matching_current_and_retained {
        // The central receipt consumer fences the exact activation attempt.
        // A local ordering choice cannot prove which of two same-generation
        // attempts central still owns, so conflicting evidence must retain the
        // sandbox rather than mint a receipt central will reject as stale.
        if retained.attempt.parse::<SandboxActivationAttemptId>().ok()
            != Some(activation_attempt_id)
        {
            return record_block(
                conn,
                sandbox_id,
                prior_block_reason.as_deref(),
                AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity,
            );
        }
    }

    clear_block(conn, sandbox_id)?;
    Ok(AutoLifecycleDeleteAdmission::Receipt(RouteEvidence {
        execution_generation,
        activation_attempt_id,
        substrate: NodeManagedBy::Kubernetes,
        provenance,
    }))
}

pub(crate) fn record_retained_route_evidence(
    conn: &Connection,
    sandbox_id: SandboxId,
    evidence: RouteEvidence,
) -> crate::db::Result<bool> {
    // Activation attempts are UUIDv7. Their canonical text order therefore
    // prevents a delayed same-generation write from replacing a later dispatch.
    let updated = conn.execute(
        "UPDATE sandboxes
            SET last_observed_execution_generation = ?1,
                last_observed_route_activation_id = ?2,
                last_observed_route_substrate_kind = ?3,
                last_observed_route_identity_provenance = ?4
          WHERE id = ?5
            AND (
                last_observed_execution_generation IS NULL
                OR ?1 > last_observed_execution_generation
                OR (
                    ?1 = last_observed_execution_generation
                    AND (
                        last_observed_route_activation_id IS NULL
                        OR (
                            ?2 = last_observed_route_activation_id
                            AND (
                                ?4 = 'cluster_fenced'
                                OR COALESCE(last_observed_route_identity_provenance, 'unfenced')
                                    != 'cluster_fenced'
                            )
                        )
                        OR (
                            ?4 = 'cluster_fenced'
                            AND ?2 > last_observed_route_activation_id
                        )
                    )
                )
            )",
        params![
            evidence.execution_generation.get(),
            evidence.activation_attempt_id.to_string(),
            evidence.substrate.to_string(),
            evidence.provenance.as_str(),
            sandbox_id.to_string(),
        ],
    )? == 1;
    if updated && evidence.provenance == RouteIdentityProvenance::ClusterFenced {
        clear_route_evidence_block(conn, sandbox_id)?;
    }
    Ok(updated)
}

pub(crate) fn route_identity_adoption_metadata(
    conn: &Connection,
    sandbox_id: SandboxId,
) -> crate::db::Result<RouteIdentityAdoptionMetadata> {
    conn.query_row(
        "SELECT space_api_route_identity_provenance,
                last_observed_execution_generation,
                last_observed_route_activation_id,
                last_observed_route_substrate_kind,
                last_observed_route_identity_provenance
           FROM sandboxes
          WHERE id = ?1",
        [sandbox_id.to_string()],
        |row| {
            let current_provenance = row
                .get::<_, Option<String>>(0)?
                .map(|value| parse_text::<RouteIdentityProvenance>(&value, 0))
                .transpose()?;
            let generation = row.get::<_, Option<i64>>(1)?;
            let attempt = row.get::<_, Option<String>>(2)?;
            let substrate = row.get::<_, Option<String>>(3)?;
            let provenance = row.get::<_, Option<String>>(4)?;
            let retained_evidence = match (generation, attempt, substrate, provenance) {
                (Some(generation), Some(attempt), Some(substrate), Some(provenance)) => {
                    Some(RouteEvidence {
                        execution_generation: ExecutionGeneration::new(generation).map_err(
                            |error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    1,
                                    Type::Integer,
                                    Box::new(error),
                                )
                            },
                        )?,
                        activation_attempt_id: parse_text(&attempt, 2)?,
                        substrate: parse_text(&substrate, 3)?,
                        provenance: parse_text(&provenance, 4)?,
                    })
                }
                _ => None,
            };
            Ok(RouteIdentityAdoptionMetadata {
                current_provenance,
                retained_evidence,
            })
        },
    )
}

fn clear_route_evidence_block(conn: &Connection, sandbox_id: SandboxId) -> crate::db::Result<()> {
    let reason = conn
        .query_row(
            "SELECT auto_lifecycle_delete_block_reason
               FROM sandboxes
              WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    let Some(reason) = reason else {
        return Ok(());
    };
    let reason = parse_text::<AutoLifecycleDeleteBlockReason>(&reason, 0)?;
    if reason.is_route_evidence() {
        clear_block(conn, sandbox_id)?;
    }
    Ok(())
}

pub(super) fn discard_pending_receipt_superseded_by_generation(
    conn: &Connection,
    sandbox_id: SandboxId,
    current_generation: ExecutionGeneration,
) -> crate::db::Result<Option<SupersededPendingReceipt>> {
    conn.query_row(
        "DELETE FROM auto_lifecycle_delete_outbox
          WHERE sandbox_id = ?1
            AND state = 'pending'
            AND execution_generation < ?2
         RETURNING receipt_id, execution_generation",
        params![sandbox_id.to_string(), current_generation.get()],
        |row| {
            let receipt_id = row.get::<_, String>(0)?;
            let execution_generation = row.get::<_, i64>(1)?;
            Ok(SupersededPendingReceipt {
                receipt_id: parse_text(&receipt_id, 0)?,
                execution_generation: ExecutionGeneration::new(execution_generation).map_err(
                    |error| {
                        rusqlite::Error::FromSqlConversionFailure(1, Type::Integer, Box::new(error))
                    },
                )?,
            })
        },
    )
    .optional()
}

#[must_use = "receipt supersession must be emitted after the transaction commits"]
pub(crate) fn discard_pending_receipt_for_execution_advance(
    conn: &Connection,
    sandbox_id: SandboxId,
    superseding_generation: Option<ExecutionGeneration>,
) -> crate::db::Result<Option<PendingReceiptSupersession>> {
    let Some(superseding_generation) = superseding_generation else {
        return Ok(None);
    };
    Ok(
        discard_pending_receipt_superseded_by_generation(conn, sandbox_id, superseding_generation)?
            .map(|receipt| PendingReceiptSupersession {
                receipt,
                superseding_generation,
            }),
    )
}

/// Returns whether any pending delete receipt still describes every observed
/// execution incarnation for this row.
///
/// This protects recovery of receipts admitted by an older mixed-version
/// writer: local cleanup must not dispatch after either the current route or
/// its retained high-water mark has advanced to a different generation or
/// same-generation activation attempt.
pub(crate) fn pending_receipt_matches_observed_execution(
    conn: &Connection,
    sandbox_id: SandboxId,
) -> crate::db::Result<bool> {
    conn.query_row(
        "SELECT NOT EXISTS (
             SELECT 1
               FROM auto_lifecycle_delete_outbox AS receipt
               JOIN sandboxes AS sandbox ON sandbox.id = receipt.sandbox_id
              WHERE receipt.sandbox_id = ?1
                AND receipt.state = 'pending'
                AND (
                    COALESCE(
                        sandbox.space_api_route_execution_generation
                            > receipt.execution_generation,
                        FALSE
                    )
                    OR COALESCE(
                        sandbox.last_observed_execution_generation
                            > receipt.execution_generation,
                        FALSE
                    )
                    OR (
                        sandbox.space_api_route_execution_generation
                            = receipt.execution_generation
                        AND sandbox.space_api_route_activation_attempt_id IS NOT NULL
                        AND sandbox.space_api_route_activation_attempt_id
                            != receipt.route_activation_id
                    )
                    OR (
                        sandbox.last_observed_execution_generation
                            = receipt.execution_generation
                        AND sandbox.last_observed_route_activation_id IS NOT NULL
                        AND sandbox.last_observed_route_activation_id
                            != receipt.route_activation_id
                    )
                )
         )",
        [sandbox_id.to_string()],
        |row| row.get(0),
    )
}

fn record_block(
    conn: &Connection,
    sandbox_id: SandboxId,
    previous_reason: Option<&str>,
    reason: AutoLifecycleDeleteBlockReason,
) -> Result<AutoLifecycleDeleteAdmission, AutoLifecycleDeleteEvidenceError> {
    let changed = previous_reason != Some(reason.as_str());
    let (policy_json, runtime_state_json): (String, String) = conn.query_row(
        "SELECT auto_lifecycle_json, auto_lifecycle_state_json
           FROM sandboxes
          WHERE id = ?1",
        [sandbox_id.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let policy = serde_json::from_str(&policy_json)
        .map_err(|error| crate::db::Error::decode(0, Type::Text, error))?;
    let mut runtime_state: pplx_space_common_core::types::AutoLifecycleRuntimeState =
        serde_json::from_str(&runtime_state_json)
            .map_err(|error| crate::db::Error::decode(1, Type::Text, error))?;
    let retry_delay = i64::try_from(AUTO_LIFECYCLE_DELETE_RETRY_DELAY.as_secs())
        .map_err(crate::db::Error::to_sql_conversion)?;
    let retry_at = crate::db::now_epoch_i64()?.saturating_add(retry_delay);
    crate::auto_lifecycle::mark_error(
        &mut runtime_state,
        &policy,
        format!("auto-lifecycle delete blocked: {reason}"),
    );
    runtime_state.next_auto_event_at =
        Some(u64::try_from(retry_at).map_err(crate::db::Error::to_sql_conversion)?);
    let runtime_state_json =
        serde_json::to_string(&runtime_state).map_err(crate::db::Error::to_sql_conversion)?;
    conn.execute(
        "UPDATE sandboxes
            SET auto_lifecycle_delete_block_reason = ?1,
                auto_lifecycle_delete_blocked_at = CASE
                    WHEN auto_lifecycle_delete_block_reason = ?1
                    THEN auto_lifecycle_delete_blocked_at
                    ELSE ?2
                END,
                auto_lifecycle_state_json = ?3,
                next_auto_event_at = ?4
          WHERE id = ?5",
        params![
            reason.as_str(),
            crate::db::now()?,
            runtime_state_json,
            retry_at,
            sandbox_id.to_string()
        ],
    )?;
    Ok(AutoLifecycleDeleteAdmission::Blocked { reason, changed })
}

pub(crate) fn record_receipt_conflict_retry(
    conn: &Connection,
    storage_id: &str,
) -> crate::db::Result<bool> {
    let transaction = crate::db::Transaction::immediate(conn)?;
    clear_block_by_storage_id(&transaction, storage_id)?;
    let (policy_json, runtime_state_json): (String, String) = transaction.query_row(
        "SELECT auto_lifecycle_json, auto_lifecycle_state_json
           FROM sandboxes
          WHERE id = ?1",
        [storage_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let policy = serde_json::from_str(&policy_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
    })?;
    let mut runtime_state: pplx_space_common_core::types::AutoLifecycleRuntimeState =
        serde_json::from_str(&runtime_state_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
        })?;
    const CONFLICT_ERROR: &str = "auto-lifecycle delete blocked by conflicting receipt evidence";
    let changed = runtime_state.last_auto_error.as_deref() != Some(CONFLICT_ERROR);
    crate::auto_lifecycle::mark_error(&mut runtime_state, &policy, CONFLICT_ERROR);
    let retry_delay = i64::try_from(AUTO_LIFECYCLE_DELETE_RETRY_DELAY.as_secs())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let retry_at = crate::db::now_epoch_i64()?.saturating_add(retry_delay);
    runtime_state.next_auto_event_at = Some(
        u64::try_from(retry_at)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
    );
    let runtime_state_json = serde_json::to_string(&runtime_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    transaction.execute(
        "UPDATE sandboxes
            SET auto_lifecycle_state_json = ?1,
                next_auto_event_at = ?2
          WHERE id = ?3",
        params![runtime_state_json, retry_at, storage_id],
    )?;
    transaction.commit()?;
    Ok(changed)
}

fn clear_block(conn: &Connection, sandbox_id: SandboxId) -> crate::db::Result<()> {
    clear_block_by_storage_id(conn, &sandbox_id.to_string())
}

fn clear_block_by_storage_id(conn: &Connection, storage_id: &str) -> crate::db::Result<()> {
    let values = conn
        .query_row(
            "SELECT auto_lifecycle_json, auto_lifecycle_state_json
               FROM sandboxes
              WHERE id = ?1
                AND auto_lifecycle_delete_block_reason IS NOT NULL",
            [storage_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((policy_json, runtime_state_json)) = values else {
        return Ok(());
    };
    let policy = serde_json::from_str(&policy_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
    })?;
    let mut runtime_state = serde_json::from_str(&runtime_state_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
    })?;
    crate::auto_lifecycle::clear_error(&mut runtime_state, &policy);
    let runtime_state_json = serde_json::to_string(&runtime_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
            SET auto_lifecycle_delete_block_reason = NULL,
                auto_lifecycle_delete_blocked_at = NULL,
                auto_lifecycle_state_json = ?1,
                next_auto_event_at = ?2
          WHERE id = ?3",
        params![
            runtime_state_json,
            runtime_state.next_auto_event_at.map(SqliteU64),
            storage_id
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_pending_receipt(
    conn: &Connection,
    receipt: AutoLifecycleDeleteReceipt,
) -> Result<AutoLifecycleDeleteReceipt, InsertPendingReceiptError> {
    let inserted = conn.execute(
        "INSERT INTO auto_lifecycle_delete_outbox (
             receipt_id, sandbox_id, execution_generation, route_activation_id,
             auto_lifecycle_policy_fingerprint, state, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)
         ON CONFLICT (sandbox_id) DO NOTHING",
        params![
            receipt.receipt_id.to_string(),
            receipt.sandbox_id.to_string(),
            receipt.execution_generation.get(),
            receipt.route_activation_id.to_string(),
            receipt.policy_fingerprint.to_string(),
            crate::db::now()?,
        ],
    )?;
    if inserted == 1 {
        return Ok(receipt);
    }

    let (existing, existing_state) = receipt_record_for_sandbox(conn, receipt.sandbox_id)?
        .ok_or_else(crate::db::Error::no_rows)?;
    if existing_state == AutoLifecycleDeleteOutboxState::Pending
        && existing.execution_generation == receipt.execution_generation
        && existing.route_activation_id == receipt.route_activation_id
        && existing.policy_fingerprint == receipt.policy_fingerprint
    {
        return Ok(existing);
    }
    // The production caller holds the per-sandbox cleanup fence. A retry may
    // refresh the policy evaluated for the same execution attempt. A different
    // attempt is a different dispatch and must not inherit this delete claim.
    let pending_replacement = existing_state == AutoLifecycleDeleteOutboxState::Pending
        && existing.execution_generation == receipt.execution_generation
        && existing.route_activation_id == receipt.route_activation_id
        && existing.policy_fingerprint != receipt.policy_fingerprint;
    if pending_replacement {
        let updated = conn.execute(
            "UPDATE auto_lifecycle_delete_outbox
                SET receipt_id = ?1,
                    auto_lifecycle_policy_fingerprint = ?2
              WHERE sandbox_id = ?3
                AND receipt_id = ?4
                AND state = 'pending'
                AND execution_generation = ?5
                AND route_activation_id = ?6
                AND auto_lifecycle_policy_fingerprint = ?7",
            params![
                receipt.receipt_id.to_string(),
                receipt.policy_fingerprint.to_string(),
                receipt.sandbox_id.to_string(),
                existing.receipt_id.to_string(),
                existing.execution_generation.get(),
                existing.route_activation_id.to_string(),
                existing.policy_fingerprint.to_string(),
            ],
        )?;
        if updated == 1 {
            return Ok(receipt);
        }
    }
    Err(ReceiptEvidenceConflict {
        sandbox_id: receipt.sandbox_id,
    }
    .into())
}

pub(crate) fn complete_pending_receipt(
    conn: &Connection,
    sandbox_id: SandboxId,
    receipt_id: AutoLifecycleDeleteReceiptId,
) -> Result<(), CompletePendingReceiptError> {
    if conn.execute(
        "UPDATE auto_lifecycle_delete_outbox
            SET state = 'completed', completed_at = ?1
          WHERE receipt_id = ?2
            AND sandbox_id = ?3
            AND state = 'pending'",
        params![
            crate::db::now()?,
            receipt_id.to_string(),
            sandbox_id.to_string()
        ],
    )? == 1
    {
        return Ok(());
    }
    Err(ReceiptEvidenceConflict { sandbox_id }.into())
}

pub(crate) fn list_completed_receipts(
    conn: &Connection,
) -> crate::db::Result<Vec<AutoLifecycleDeleteReceipt>> {
    let batch_size = i64::try_from(RECEIPT_BATCH_SIZE)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let transaction = crate::db::Transaction::immediate(conn)?;
    let rows = {
        // Interleave fresh and retried receipts by rank within each class.
        // Fresh completions cannot starve deferred evidence, and a repeatedly
        // deferred cohort cannot consume the whole bounded publication batch.
        let mut statement = transaction.prepare(
            "SELECT receipt_id, sandbox_id, execution_generation, route_activation_id,
                    auto_lifecycle_policy_fingerprint
               FROM (
                    SELECT receipt_id, sandbox_id, execution_generation, route_activation_id,
                           auto_lifecycle_policy_fingerprint, attempt_count,
                           ROW_NUMBER() OVER (
                               PARTITION BY (attempt_count = 0)
                               ORDER BY COALESCE(last_attempted_at, completed_at),
                                        completed_at,
                                        receipt_id
                           ) AS class_rank
                      FROM auto_lifecycle_delete_outbox
                     WHERE state = 'completed'
               ) ranked
              ORDER BY class_rank, (attempt_count != 0), receipt_id
              LIMIT ?1",
        )?;
        statement
            .query_map([batch_size], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };

    let receipts = rows
        .iter()
        .map(
            |(receipt_id, sandbox_id, generation, activation_id, fingerprint)| {
                Ok(AutoLifecycleDeleteReceipt {
                    receipt_id: parse_text(receipt_id, 0)?,
                    sandbox_id: parse_text(sandbox_id, 1)?,
                    execution_generation: ExecutionGeneration::new(*generation).map_err(
                        |error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                Type::Integer,
                                Box::new(error),
                            )
                        },
                    )?,
                    route_activation_id: parse_text(activation_id, 3)?,
                    policy_fingerprint: parse_text(fingerprint, 4)?,
                })
            },
        )
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let attempted_at = crate::db::now()?;
    for (receipt_id, ..) in &rows {
        transaction.execute(
            "UPDATE auto_lifecycle_delete_outbox
                SET last_attempted_at = ?1,
                    attempt_count = attempt_count + 1
              WHERE receipt_id = ?2 AND state = 'completed'",
            params![attempted_at, receipt_id],
        )?;
    }
    transaction.commit()?;
    Ok(receipts)
}

pub(crate) fn acknowledge_completed_receipts(
    conn: &Connection,
    receipt_ids: &[AutoLifecycleDeleteReceiptId],
) -> crate::db::Result<Vec<AutoLifecycleDeleteReceiptId>> {
    let transaction = crate::db::Transaction::immediate(conn)?;
    let mut acknowledged = Vec::new();
    for receipt_id in receipt_ids {
        if transaction.execute(
            "DELETE FROM auto_lifecycle_delete_outbox
              WHERE receipt_id = ?1 AND state = 'completed'",
            [receipt_id.to_string()],
        )? == 1
        {
            acknowledged.push(*receipt_id);
        }
    }
    transaction.commit()?;
    Ok(acknowledged)
}

pub(crate) fn outbox_stats(conn: &Connection) -> crate::db::Result<AutoLifecycleDeleteOutboxStats> {
    let now = crate::db::now_epoch_i64()?;
    let (pending, completed, attempts, oldest_pending, oldest_completed) = conn.query_row(
        "SELECT
             COALESCE(SUM(state = 'pending'), 0),
             COALESCE(SUM(state = 'completed'), 0),
             COALESCE(MAX(CASE WHEN state = 'completed' THEN attempt_count END), 0),
             MIN(CASE WHEN state = 'pending' THEN CAST(created_at AS INTEGER) END),
             MIN(CASE WHEN state = 'completed' THEN CAST(completed_at AS INTEGER) END)
           FROM auto_lifecycle_delete_outbox",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        },
    )?;
    let value = |value: i64, column| {
        u64::try_from(value).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
        })
    };
    let age = |timestamp: Option<i64>, column| {
        timestamp
            .map(|timestamp| value(now.saturating_sub(timestamp), column))
            .transpose()
            .map(Option::unwrap_or_default)
    };
    Ok(AutoLifecycleDeleteOutboxStats {
        pending_count: value(pending, 0)?,
        completed_count: value(completed, 1)?,
        max_attempt_count: value(attempts, 2)?,
        oldest_pending_age_secs: age(oldest_pending, 3)?,
        oldest_completed_age_secs: age(oldest_completed, 4)?,
    })
}

pub(crate) fn blocked_stats(
    conn: &Connection,
) -> crate::db::Result<Vec<AutoLifecycleDeleteBlockedStats>> {
    let now = crate::db::now_epoch_i64()?;
    let mut statement = conn.prepare(
        "SELECT auto_lifecycle_delete_block_reason,
                COUNT(*),
                MIN(CAST(auto_lifecycle_delete_blocked_at AS INTEGER))
           FROM sandboxes
          WHERE auto_lifecycle_delete_block_reason IS NOT NULL
          GROUP BY auto_lifecycle_delete_block_reason",
    )?;
    statement
        .query_map([], |row| {
            let reason =
                parse_text::<AutoLifecycleDeleteBlockReason>(&row.get::<_, String>(0)?, 0)?;
            let count = u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Integer, Box::new(error))
            })?;
            let oldest = row.get::<_, Option<i64>>(2)?.unwrap_or(now);
            let oldest_age_secs = u64::try_from(now.saturating_sub(oldest)).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(2, Type::Integer, Box::new(error))
            })?;
            Ok(AutoLifecycleDeleteBlockedStats {
                reason,
                count,
                oldest_age_secs,
            })
        })?
        .collect::<rusqlite::Result<_>>()
        .map_err(crate::db::Error::from)
}

pub(crate) fn pending_receipt_for_sandbox(
    conn: &Connection,
    sandbox_id: SandboxId,
) -> crate::db::Result<Option<AutoLifecycleDeleteReceipt>> {
    Ok(
        receipt_record_for_sandbox(conn, sandbox_id)?.and_then(|(receipt, state)| {
            (state == AutoLifecycleDeleteOutboxState::Pending).then_some(receipt)
        }),
    )
}

fn receipt_record_for_sandbox(
    conn: &Connection,
    sandbox_id: SandboxId,
) -> crate::db::Result<Option<(AutoLifecycleDeleteReceipt, AutoLifecycleDeleteOutboxState)>> {
    conn.query_row(
        "SELECT receipt_id, execution_generation, route_activation_id,
                auto_lifecycle_policy_fingerprint, state
           FROM auto_lifecycle_delete_outbox
          WHERE sandbox_id = ?1",
        [sandbox_id.to_string()],
        |row| {
            Ok((
                AutoLifecycleDeleteReceipt {
                    receipt_id: parse_text(&row.get::<_, String>(0)?, 0)?,
                    sandbox_id,
                    execution_generation: ExecutionGeneration::new(row.get(1)?).map_err(
                        |error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                Type::Integer,
                                Box::new(error),
                            )
                        },
                    )?,
                    route_activation_id: parse_text(&row.get::<_, String>(2)?, 2)?,
                    policy_fingerprint: parse_text(&row.get::<_, String>(3)?, 3)?,
                },
                parse_text(&row.get::<_, String>(4)?, 4)?,
            ))
        },
    )
    .optional()
}

fn parse_text<T>(value: &str, column: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

#[cfg(test)]
mod tests;
