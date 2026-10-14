//! Startup recovery of sandboxes interrupted mid-transition.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupRecoveryOutcome {
    State(SandboxState),
    CleanupRequired {
        keep_filesystem: bool,
        completion: CleanupCompletion,
    },
    Deferred,
}

impl StartupRecoveryOutcome {
    pub fn changed(self) -> bool {
        !matches!(self, Self::Deferred)
    }

    pub fn requires_quarantine(self) -> bool {
        matches!(self, Self::State(SandboxState::Quarantined))
            || matches!(
                self,
                Self::CleanupRequired {
                    completion: CleanupCompletion::Quarantine,
                    ..
                }
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupRecoveryRecord {
    pub id: String,
    pub previous_state: SandboxState,
    pub outcome: StartupRecoveryOutcome,
}

/// The settlement determines both durable state and event semantics. Healthy
/// outcomes cannot carry quarantine reasons or errors.
pub(crate) enum StartupResidentSettlement<'a> {
    Started,
    Paused,
    Quarantined {
        reason_code: SandboxQuarantineReasonCode,
        message: &'a str,
    },
}

/// Commit lifecycle and quarantine evidence together. A changed row or durable
/// cleanup owner supersedes the entire observation.
pub(crate) fn settle_startup_resident(
    conn: &Connection,
    observed: &SandboxRow,
    settlement: StartupResidentSettlement<'_>,
) -> Result<bool> {
    if !matches!(
        observed.state,
        SandboxState::Started
            | SandboxState::Paused
            | SandboxState::Resuming
            | SandboxState::Checkpointing
    ) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let (target, reason, error) = match &settlement {
        StartupResidentSettlement::Started => (
            SandboxState::Started,
            "startup_recovery_runtime_state_observed",
            None,
        ),
        StartupResidentSettlement::Paused => (
            SandboxState::Paused,
            "startup_recovery_runtime_state_observed",
            None,
        ),
        StartupResidentSettlement::Quarantined {
            reason_code,
            message,
        } => (
            SandboxState::Quarantined,
            reason_code.as_str(),
            Some(*message),
        ),
    };
    let tx = immediate_transaction(conn)?;
    if !startup_resident_witness_current(&tx, observed)? {
        return Ok(false);
    }
    let timestamp = now()?;
    // A receipt acknowledges its current target, not historical success.
    // Preserve a matching healthy acknowledgement across recovery, but retire
    // every receipt on quarantine in the same transaction as its evidence.
    tx.execute(
        "UPDATE sandboxes SET state = ?1, failure_reason = ?2,
             transition_receipt_id = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_id
                 ELSE NULL
             END,
             transition_receipt_kind = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_kind
                 ELSE NULL
             END,
             transition_receipt_target_state = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                 ELSE NULL
             END,
             transition_receipt_execution_generation = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                 ELSE NULL
             END,
             transition_receipt_activation_attempt_id = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                 ELSE NULL
             END,
             transition_receipt_reason_code = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                 ELSE NULL
             END,
             state_changed_at = ?3, updated_at = ?3 WHERE id = ?4",
        params![target.as_str(), error, timestamp, observed.id],
    )?;
    if let StartupResidentSettlement::Quarantined {
        reason_code,
        message,
    } = settlement
    {
        crate::route_activation_claims::close_claim(&tx, &observed.id)?;
        insert_new_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id: &observed.id,
                entered_from_state: Some(observed.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code,
                reason_message: Some(message),
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &timestamp,
            },
        )?;
    }
    tx.commit()?;
    match settlement {
        StartupResidentSettlement::Quarantined { reason_code, .. } => {
            emit_sandbox_quarantined_event(
                &observed.id,
                Some(observed.state.as_str()),
                reason_code,
                "startup_resident_recovery",
                error,
            );
        }
        StartupResidentSettlement::Started | StartupResidentSettlement::Paused => {
            emit_sandbox_state_event(
                &observed.id,
                SandboxEventType::LifecycleConverged,
                Some(observed.state.as_str()),
                reason,
                "startup_resident_recovery",
                target,
                error,
            );
        }
    }
    Ok(true)
}

pub fn cleanup_intent_for_startup_recovery(row: &SandboxRow) -> Option<(bool, CleanupCompletion)> {
    if let (Some(keep_filesystem), Some(completion)) =
        (row.cleanup_keep_filesystem, row.cleanup_completion)
        && completion.owns_row_intent(row)
    {
        return Some((keep_filesystem, completion));
    }

    match row.state {
        // Transient cleanup states represent an unfinished lifecycle operation.
        // Finish them even if runtime fields were already cleared when the
        // target terminal state is self-contained. Suspending is stricter:
        // recovery may only mark it suspended once a full suspend artifact is
        // already pinned; otherwise service-level startup recovery must either
        // finish a complete staged publish or quarantine the row.
        SandboxState::Stopping => Some((true, CleanupCompletion::MarkStopped)),
        SandboxState::Suspending if sandbox_has_full_suspend_artifact_pin(row) => {
            Some((true, CleanupCompletion::MarkSuspended))
        }
        SandboxState::Suspending => None,
        SandboxState::Deleting => Some((false, CleanupCompletion::DeleteRow)),
        SandboxState::CleanupRequired => Some((true, CleanupCompletion::Quarantine)),
        SandboxState::Starting if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::MarkStopped))
        }
        SandboxState::Restoring if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::MarkSuspended))
        }
        SandboxState::CleanupFailed if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::Quarantine))
        }
        _ => None,
    }
}

/// On startup: reconcile interrupted DB states to a safe local outcome.
/// Also check Started sandboxes for dead PIDs.
pub fn recover_interrupted_startup_states(
    conn: &Connection,
) -> std::result::Result<Vec<StartupRecoveryRecord>, RecoverStartupStatesError> {
    let rows = list_sandboxes_for_recovery(conn)?;
    let mut recovered = Vec::new();

    for row in rows {
        if row.state.is_transient() {
            let episode = match cleanup_episode(conn, &row.id) {
                Ok(episode) => episode,
                Err(error) if is_row_decode_error(&error) => {
                    tracing::warn!(sandbox_id = %row.id, error = %describe_error(&error), "retaining malformed startup cleanup ownership; operator repair required");
                    recovered.push(StartupRecoveryRecord {
                        id: row.id,
                        previous_state: row.state,
                        outcome: StartupRecoveryOutcome::Deferred,
                    });
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(episode) = episode.filter(|episode| {
                episode.requested_at.is_none() && episode.activation_state == row.state
            }) {
                // Hand off the exact episode before startup can admit work.
                // Generic lifecycle rewriting loses its receipt; merely
                // deferring leaves no periodic owner if later recovery fails.
                let reason = match row.state {
                    SandboxState::Creating => SandboxQuarantineReasonCode::CreateInterrupted,
                    SandboxState::Starting => {
                        SandboxQuarantineReasonCode::ColdActivationInterrupted
                    }
                    SandboxState::Restoring => {
                        SandboxQuarantineReasonCode::RestoreActivationInterrupted
                    }
                    state => {
                        return Err(RecoverStartupStatesError::UnsupportedActivationState(state));
                    }
                };
                let requested = request_cleanup_episode(
                    conn,
                    &row.id,
                    episode.id,
                    reason,
                    "startup recovery resumed interrupted activation cleanup",
                )?;
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: match requested {
                        CleanupEpisodeRequestOutcome::Requested
                        | CleanupEpisodeRequestOutcome::AlreadyRequested => {
                            StartupRecoveryOutcome::CleanupRequired {
                                keep_filesystem: episode.keep_filesystem,
                                completion: episode.completion,
                            }
                        }
                        CleanupEpisodeRequestOutcome::Superseded => {
                            StartupRecoveryOutcome::Deferred
                        }
                    },
                });
                continue;
            }
            if row.state == SandboxState::Creating {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
                continue;
            }

            if matches!(
                row.state,
                SandboxState::Pausing | SandboxState::Resuming | SandboxState::Checkpointing
            ) || (row.state == SandboxState::Suspending
                && !sandbox_has_full_suspend_artifact_pin(&row))
                || sandbox_is_retryable_interrupted_restore(&row)
            {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
            } else if let Some((keep_filesystem, completion)) =
                cleanup_intent_for_startup_recovery(&row)
            {
                mark_cleanup_required(
                    conn,
                    &row.id,
                    keep_filesystem,
                    completion,
                    &format!(
                        "startup recovery: sandbox was left in transient state '{}'",
                        row.state
                    ),
                )?;
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::CleanupRequired {
                        keep_filesystem,
                        completion,
                    },
                });
            } else if row.state == SandboxState::Restoring {
                let outcome =
                    update_state_from(conn, &row.id, &edges::INTERRUPTED_RESTORE_RECOVERY)?;
                if !matches!(outcome, StateWriteOutcome::Applied { .. }) {
                    tracing::warn!(
                        sandbox_id = %row.id,
                        ?outcome,
                        "startup recovery left an interrupted restore in place"
                    );
                    continue;
                }
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::State(SandboxState::Suspended),
                });
            } else {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
            }
            continue;
        }

        if row.state == SandboxState::Started
            && !row
                .pid
                .is_some_and(|pid| std::path::Path::new(&format!("/proc/{pid}")).exists())
        {
            // The state-only Firecracker probe cannot reconstruct trusted
            // process credentials. Preserve all resources until the missing
            // runtime identity can be explained; an older artifact is not
            // evidence of the customer's current state.
            if settle_startup_resident(
                conn,
                &row,
                StartupResidentSettlement::Quarantined {
                    reason_code: SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
                    message: "startup recovery: started sandbox has no live registered VM pid",
                },
            )? {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::State(SandboxState::Quarantined),
                });
            }
        }
    }

    Ok(recovered)
}

#[derive(Debug, thiserror::Error)]
pub enum RecoverStartupStatesError {
    #[error("startup state recovery database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("failed to hand interrupted activation to cleanup: {0}")]
    CleanupRequest(#[from] RequestCleanupEpisodeError),
    #[error("armed cleanup episode has unsupported activation state {0}")]
    UnsupportedActivationState(SandboxState),
    #[error("startup recovery write {edge} was refused: sandbox is {found}, {}", refusal_text(.refusal))]
    StateWriteRefused {
        edge: &'static str,
        found: SandboxState,
        refusal: StateWriteRefusal,
    },
    #[error("startup recovery write {edge} found no sandbox row")]
    StateWriteMissing { edge: &'static str },
}

fn refusal_text(refusal: &StateWriteRefusal) -> &'static str {
    match refusal {
        StateWriteRefusal::FromState => "outside the edge's source states",
        StateWriteRefusal::Guard(StateWriteGuard::Quarantined) => "blocked by quarantine",
        StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned) => "owned by cleanup",
    }
}

/// [`update_state_from`] for a startup recovery writer that requires its
/// write. Startup recovery runs before sbx-manager binds its sockets and
/// before it starts its background loops. Service construction already
/// spawned the snapshot scheduler and watchdog workers, but they act only on
/// sandboxes registered with them, and runtime-registration recovery
/// registers sandboxes only after these writes. So nothing else can move the
/// node-local SQLite `sandboxes.state` row after recovery read it: any outcome
/// but `Applied` fails recovery.
pub fn require_startup_state_write(
    conn: &Connection,
    id: &str,
    edge: &'static Edge,
) -> std::result::Result<(), RecoverStartupStatesError> {
    match update_state_from(conn, id, edge)? {
        StateWriteOutcome::Applied { .. } => Ok(()),
        StateWriteOutcome::Refused { found, refusal } => {
            Err(RecoverStartupStatesError::StateWriteRefused {
                edge: edge.name(),
                found,
                refusal,
            })
        }
        StateWriteOutcome::Missing => {
            Err(RecoverStartupStatesError::StateWriteMissing { edge: edge.name() })
        }
    }
}
