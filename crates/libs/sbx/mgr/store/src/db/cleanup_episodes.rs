//! `sandbox_cleanup_episodes` and the sandbox cleanup intent they complete.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuarantineRecoveryFence {
    pub local_artifact_id: LocalArtifactId,
    pub execution_generation: ExecutionGeneration,
}

pub const SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE: &str =
    "suspend_publish_failed_no_artifact_runtime_gone";

#[derive(Debug, thiserror::Error)]
pub enum CompleteCleanupEpisodeError {
    #[error(transparent)]
    InvalidReceipt(#[from] SandboxTransitionReceiptInvariantError),
    #[error("row-deleting cleanup cannot complete as a resident state")]
    DeleteHasNoTargetState,
    #[error(transparent)]
    Db(#[from] Error),
    #[error("cleanup episode cannot restore suspended without a registered full suspend artifact")]
    MissingSuspendArtifact,
    #[error("cleanup episode sandbox row disappeared")]
    MissingSandboxRow,
    #[error("quarantine recovery history insert affected {changed} rows instead of one")]
    RecoveryHistoryInvariant { changed: usize },
    #[error("quarantine recovery state retirement affected {changed} rows instead of one")]
    RecoveryStateRetirementInvariant { changed: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum InsertSandboxWithCleanupEpisodeError {
    #[error(transparent)]
    Db(#[from] Error),
    #[error(
        "sandbox state {sandbox_state} does not match cleanup activation state {activation_state}"
    )]
    ActivationStateMismatch {
        sandbox_state: SandboxState,
        activation_state: SandboxState,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RequestCleanupEpisodeError {
    #[error(transparent)]
    Db(#[from] Error),
    #[error("cleanup episode phase update affected {changed} rows instead of one")]
    PhaseUpdateInvariant { changed: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CleanupEpisodeSpec {
    pub id: SandboxCleanupEpisodeId,
    pub completion: CleanupCompletion,
    pub keep_filesystem: bool,
    pub entered_from_state: Option<SandboxState>,
    pub activation_state: SandboxState,
    pub execution_generation: Option<ExecutionGeneration>,
    /// Route-installation ownership, not the independently supplied operation.
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PreparedCloneRoot {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupEpisode {
    /// Receipt owner; route authorization continues to use activation_attempt_id.
    pub operation_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub prepared_clone_root: Option<PreparedCloneRoot>,
    pub id: SandboxCleanupEpisodeId,
    pub completion: CleanupCompletion,
    pub keep_filesystem: bool,
    pub entered_from_state: Option<SandboxState>,
    pub activation_state: SandboxState,
    pub requested_at: Option<u64>,
    pub quarantine_reason_code: Option<SandboxQuarantineReasonCode>,
    pub failure_message: Option<String>,
    pub last_cleanup_error: Option<String>,
    pub execution_generation: Option<ExecutionGeneration>,
    /// Legacy physical column: retained route-installation ownership evidence.
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub attempt_count: u32,
    pub quarantine_recovery: Option<QuarantineRecoveryFence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupEpisodeRequestOutcome {
    Requested,
    AlreadyRequested,
    Superseded,
}

impl CleanupEpisodeRequestOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AlreadyRequested => "already_requested",
            Self::Superseded => "superseded",
        }
    }
}

impl fmt::Display for CleanupEpisodeRequestOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(super) fn insert_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    episode: CleanupEpisodeSpec,
) -> Result<()> {
    conn.execute(
        "INSERT INTO sandbox_cleanup_episodes (
             sandbox_id, cleanup_id, completion, keep_filesystem, phase,
             entered_from_state, activation_state,
             execution_generation, activation_attempt_id, created_at
         ) VALUES (?1, ?2, ?3, ?4, 'armed', ?5, ?6, ?7, ?8, ?9)",
        params![
            sandbox_id,
            episode.id.to_string(),
            episode.completion.as_str(),
            episode.keep_filesystem,
            episode.entered_from_state.map(|state| state.as_str()),
            episode.activation_state.as_str(),
            episode.execution_generation.map(ExecutionGeneration::get),
            episode.activation_attempt_id.map(|id| id.to_string()),
            now_epoch_i64()?,
        ],
    )?;
    conn.execute(
        "UPDATE sandboxes
            SET cleanup_completion = ?1,
                cleanup_keep_filesystem = ?2
          WHERE id = ?3",
        params![
            episode.completion.as_str(),
            episode.keep_filesystem,
            sandbox_id
        ],
    )?;
    Ok(())
}

pub fn insert_sandbox_with_cleanup_episode_and_route_evidence(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    episode: CleanupEpisodeSpec,
    route_evidence: Option<crate::auto_lifecycle_delete_receipts::RouteEvidence>,
    open_route_activation: Option<&route_activation_claims::OpenRouteActivationClaim>,
    execution: Option<&ExecutionMetadata>,
) -> std::result::Result<(), InsertSandboxWithCleanupEpisodeError> {
    if params.state != episode.activation_state {
        return Err(
            InsertSandboxWithCleanupEpisodeError::ActivationStateMismatch {
                sandbox_state: params.state,
                activation_state: episode.activation_state,
            },
        );
    }
    let sandbox_id = params.id.to_string();
    let tx = immediate_transaction(conn)?;
    insert_sandbox(&tx, params)?;
    if let Some(execution) = execution {
        upsert_sandbox_execution_metadata_in_transaction(&tx, &sandbox_id, execution)?;
    }
    if let Some(route_evidence) = route_evidence {
        auto_lifecycle_delete_receipts::record_retained_route_evidence(
            &tx,
            sandbox_id.parse().map_err(Error::to_sql_conversion)?,
            route_evidence,
        )?;
    }
    if let Some(open_route_activation) = open_route_activation {
        route_activation_claims::insert_create_claim(&tx, &sandbox_id, open_route_activation)?;
    }
    insert_cleanup_episode(&tx, &sandbox_id, episode)?;
    tx.commit()?;
    Ok(())
}

/// Charge before connecting or dispatching so crashes cannot reset the budget.
pub fn charge_prepared_clone_disposal_attempt(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    Ok(conn.execute(
        "UPDATE sandbox_cleanup_episodes SET attempt_count = attempt_count + 1
         WHERE sandbox_id = ?1 AND cleanup_id = ?2 AND completion = 'delete_pool_clone'
           AND phase = 'requested' AND attempt_count < 8
           AND EXISTS (SELECT 1 FROM sandboxes WHERE id = ?1 AND state IN ('cleanup_required', 'cleanup_failed')
               AND cleanup_completion = 'delete_pool_clone')",
        params![id, cleanup_id.to_string()],
    )? == 1)
}

/// An explicit delete retries a held disposal with its original identity.
/// It never grants ownership of the replacement now occupying the pathname.
pub fn rearm_prepared_clone_disposal(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes SET state = 'cleanup_required',
            cleanup_completion = 'delete_pool_clone', cleanup_keep_filesystem = 0,
            state_changed_at = ?1, updated_at = ?1
         WHERE id = ?2 AND state = 'quarantined' AND EXISTS (
            SELECT 1 FROM sandbox_cleanup_episodes WHERE sandbox_id = ?2
             AND cleanup_id = ?3 AND completion = 'delete_pool_clone')",
        params![now()?, id, cleanup_id.to_string()],
    )? == 1;
    if changed {
        tx.execute("UPDATE sandbox_cleanup_episodes SET attempt_count = 0, last_cleanup_error = NULL,
                phase = 'requested', requested_at = ?3, quarantine_reason_code = 'create_interrupted'
             WHERE sandbox_id = ?1 AND cleanup_id = ?2", params![id, cleanup_id.to_string(), now_epoch_i64()?])?;
    }
    tx.commit()?;
    Ok(changed)
}

/// Bind disposal authority before any clone RPC can mutate the prepared tree.
/// A missing binding after a crash permits only absence, never pathname deletion.
pub fn bind_prepared_clone_root(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    root: PreparedCloneRoot,
) -> Result<bool> {
    let root = serde_json::to_string(&root)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    Ok(conn.execute(
        "UPDATE sandbox_cleanup_episodes SET prepared_clone_root = ?1
         WHERE sandbox_id = ?2 AND cleanup_id = ?3 AND phase = 'armed'
           AND completion = 'delete_pool_clone' AND prepared_clone_root IS NULL",
        params![root, id, cleanup_id.to_string()],
    )? == 1)
}

/// Presence is authoritative even when a newer binary wrote an intent this
/// reader cannot decode. Startup must not promote or recreate its resources.
pub fn has_cleanup_episode(conn: &Connection, id: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1)",
        [id],
        |row| row.get(0),
    )
}

pub fn cleanup_episode(conn: &Connection, id: &str) -> Result<Option<CleanupEpisode>> {
    conn.query_row(
        "SELECT cleanup_id, completion, keep_filesystem, entered_from_state,
                activation_state, requested_at, quarantine_reason_code,
                failure_message, last_cleanup_error, attempt_count,
                recovery_local_artifact_id, recovery_execution_generation,
                execution_generation, activation_attempt_id, prepared_clone_root,
                operation_activation_attempt_id
           FROM sandbox_cleanup_episodes
          WHERE sandbox_id = ?1",
        params![id],
        |row| {
            let cleanup_id = row.get::<_, String>(0)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })?;
            let completion = row.get::<_, String>(1)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })?;
            let entered_from_state = row
                .get::<_, Option<String>>(3)?
                .map(|state| {
                    state.parse::<SandboxState>().map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("unknown cleanup entered-from state '{state}'"),
                            )),
                        )
                    })
                })
                .transpose()?;
            let activation_state_raw = row.get::<_, String>(4)?;
            let activation_state = activation_state_raw.parse::<SandboxState>().map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown cleanup activation state '{activation_state_raw}'"),
                    )),
                )
            })?;
            let quarantine_reason_code = row
                .get::<_, Option<String>>(6)?
                .map(|reason| {
                    reason.parse().map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(6, Type::Text, Box::new(error))
                    })
                })
                .transpose()?;
            let attempt_count = row.get::<_, i64>(9)?;
            let quarantine_recovery = match (
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<i64>>(11)?,
            ) {
                (None, None) => None,
                (Some(local_artifact_id), Some(execution_generation)) => {
                    Some(QuarantineRecoveryFence {
                        local_artifact_id: local_artifact_id.parse().map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                10,
                                Type::Text,
                                Box::new(error),
                            )
                        })?,
                        execution_generation: ExecutionGeneration::new(execution_generation)
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    11,
                                    Type::Integer,
                                    Box::new(error),
                                )
                            })?,
                    })
                }
                _ => {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        10,
                        Type::Null,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "quarantine recovery fence is incomplete",
                        )),
                    ));
                }
            };
            Ok(CleanupEpisode {
                operation_activation_attempt_id: row
                    .get::<_, Option<String>>(15)?
                    .map(|value| value.parse())
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(15, Type::Text, Box::new(error))
                    })?,
                prepared_clone_root: row
                    .get::<_, Option<String>>(14)?
                    .map(|json| {
                        serde_json::from_str(&json).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                14,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?,
                id: cleanup_id,
                completion,
                keep_filesystem: row.get(2)?,
                entered_from_state,
                activation_state,
                requested_at: row
                    .get::<_, Option<i64>>(5)?
                    .and_then(|value| u64::try_from(value).ok()),
                quarantine_reason_code,
                failure_message: row.get(7)?,
                last_cleanup_error: row.get(8)?,
                execution_generation: row
                    .get::<_, Option<i64>>(12)?
                    .map(ExecutionGeneration::new)
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            Type::Integer,
                            Box::new(error),
                        )
                    })?,
                activation_attempt_id: row
                    .get::<_, Option<String>>(13)?
                    .map(|value| value.parse())
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(13, Type::Text, Box::new(error))
                    })?,
                attempt_count: u32::try_from(attempt_count).unwrap_or(u32::MAX),
                quarantine_recovery,
            })
        },
    )
    .optional()
}

/// Retire an armed episode only after another state writer has conclusively
/// moved the row out of the activation state it owned.
pub fn retire_stale_armed_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    observed_state: SandboxState,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(false);
    };
    if episode.id != cleanup_id
        || episode.completion == CleanupCompletion::DeletePoolClone
        || episode.requested_at.is_some()
        || episode.activation_state == observed_state
    {
        tx.commit()?;
        return Ok(false);
    }
    let deleted = tx.execute(
        "DELETE FROM sandbox_cleanup_episodes
          WHERE sandbox_id = ?1 AND cleanup_id = ?2 AND phase = 'armed'",
        params![sandbox_id, cleanup_id.to_string()],
    )?;
    if deleted == 1 {
        tx.execute(
            "UPDATE sandboxes
                SET cleanup_completion = NULL,
                    cleanup_keep_filesystem = NULL
              WHERE id = ?1
                AND state = ?2
                AND cleanup_completion = ?3
                AND cleanup_keep_filesystem = ?4",
            params![
                sandbox_id,
                observed_state.as_str(),
                episode.completion.as_str(),
                episode.keep_filesystem,
            ],
        )?;
    }
    tx.commit()?;
    Ok(deleted == 1)
}

pub fn request_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    reason_code: SandboxQuarantineReasonCode,
    failure_message: &str,
) -> std::result::Result<CleanupEpisodeRequestOutcome, RequestCleanupEpisodeError> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(CleanupEpisodeRequestOutcome::Superseded);
    };
    if episode.id != cleanup_id {
        tx.commit()?;
        return Ok(CleanupEpisodeRequestOutcome::Superseded);
    }
    if episode.requested_at.is_some() {
        let current = cleanup_episode_current(&tx, sandbox_id, cleanup_id)?;
        tx.commit()?;
        return Ok(if current {
            CleanupEpisodeRequestOutcome::AlreadyRequested
        } else {
            CleanupEpisodeRequestOutcome::Superseded
        });
    }
    let previous_state = get_sandbox(&tx, sandbox_id)?.map(|row| row.state);
    let requested_at = now_epoch_i64()?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = 'cleanup_required',
                failure_reason = ?1,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                state_changed_at = CASE
                    WHEN state != 'cleanup_required' THEN ?2
                    ELSE state_changed_at
                END,
                updated_at = ?2
          WHERE id = ?3
            AND state NOT IN ('deleting', 'quarantined')
            AND cleanup_completion = ?4
            AND cleanup_keep_filesystem = ?5",
        params![
            failure_message,
            requested_at.to_string(),
            sandbox_id,
            episode.completion.as_str(),
            episode.keep_filesystem,
        ],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(CleanupEpisodeRequestOutcome::Superseded);
    }
    let episode_changed = tx.execute(
        "UPDATE sandbox_cleanup_episodes
            SET phase = 'requested',
                requested_at = ?1,
                quarantine_reason_code = ?2,
                failure_message = ?3,
                last_cleanup_error = NULL
          WHERE sandbox_id = ?4
            AND cleanup_id = ?5
            AND phase = 'armed'",
        params![
            requested_at,
            reason_code.as_str(),
            failure_message,
            sandbox_id,
            cleanup_id.to_string(),
        ],
    )?;
    if episode_changed != 1 {
        return Err(RequestCleanupEpisodeError::PhaseUpdateInvariant {
            changed: episode_changed,
        });
    }
    tx.commit()?;
    emit_sandbox_state_event(
        sandbox_id,
        SandboxEventType::CleanupRequested,
        previous_state.as_ref().map(SandboxState::as_str),
        reason_code.as_str(),
        "request_cleanup_episode",
        SandboxState::CleanupRequired,
        Some(failure_message),
    );
    Ok(CleanupEpisodeRequestOutcome::Requested)
}

pub fn record_cleanup_episode_failure(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    failure_message: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandbox_cleanup_episodes
            SET last_cleanup_error = ?1,
                attempt_count = CASE WHEN completion = 'delete_pool_clone' THEN attempt_count ELSE attempt_count + 1 END
          WHERE sandbox_id = ?2
            AND cleanup_id = ?3
            AND phase = 'requested'",
        params![failure_message, sandbox_id, cleanup_id.to_string()],
    )?;
    let previous_state = if changed == 1 {
        current_state_label(&tx, sandbox_id)?.and_then(|state| state.parse::<SandboxState>().ok())
    } else {
        None
    };
    let mut state_changed = 0;
    if changed == 1 {
        state_changed = tx.execute(
            "UPDATE sandboxes
                SET state = 'cleanup_failed',
                    state_changed_at = CASE
                        WHEN state != 'cleanup_failed' THEN ?1
                        ELSE state_changed_at
                    END,
                    updated_at = ?1
              WHERE id = ?2
                AND state IN ('cleanup_required', 'cleanup_failed')
                AND cleanup_completion = (
                    SELECT completion FROM sandbox_cleanup_episodes
                     WHERE sandbox_id = ?2 AND cleanup_id = ?3
                )",
            params![now()?, sandbox_id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    if state_changed == 1 && previous_state == Some(SandboxState::CleanupRequired) {
        emit_sandbox_state_event(
            sandbox_id,
            SandboxEventType::CleanupFailed,
            Some(SandboxState::CleanupRequired.as_str()),
            "activation_cleanup_failed",
            "record_cleanup_episode_failure",
            SandboxState::CleanupFailed,
            Some(failure_message),
        );
    }
    Ok(changed == 1)
}

pub fn cleanup_episode_current(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1
               FROM sandbox_cleanup_episodes episode
               JOIN sandboxes sandbox ON sandbox.id = episode.sandbox_id
              WHERE episode.sandbox_id = ?1
                AND episode.cleanup_id = ?2
                AND episode.phase = 'requested'
                AND sandbox.state IN ('cleanup_required', 'cleanup_failed')
                AND sandbox.cleanup_completion = episode.completion
                AND sandbox.cleanup_keep_filesystem = episode.keep_filesystem
         )",
        params![sandbox_id, cleanup_id.to_string()],
        |row| row.get(0),
    )
}

#[derive(Clone, Debug)]
struct QuarantineRecoveryCompletion {
    quarantine_id: String,
    reason_code: SandboxQuarantineReasonCode,
    receipt_id: SandboxCleanupEpisodeId,
    execution_generation: ExecutionGeneration,
}

enum QuarantineRecoveryMatch {
    NotApplicable,
    Superseded,
    Current(QuarantineRecoveryCompletion),
}

fn validate_cleanup_completion_artifact(
    tx: &Transaction<'_>,
    sandbox_id: &str,
    episode: &CleanupEpisode,
) -> std::result::Result<(), CompleteCleanupEpisodeError> {
    if episode.completion != CleanupCompletion::MarkSuspended {
        return Ok(());
    }

    let row = get_sandbox(tx, sandbox_id)?.ok_or(CompleteCleanupEpisodeError::MissingSandboxRow)?;
    if !sandbox_has_registered_full_suspend_artifact(tx, &row)? {
        return Err(CompleteCleanupEpisodeError::MissingSuspendArtifact);
    }
    Ok(())
}

fn match_quarantine_recovery(
    tx: &Transaction<'_>,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    episode: &CleanupEpisode,
) -> Result<QuarantineRecoveryMatch> {
    if episode.entered_from_state != Some(SandboxState::Quarantined) {
        return Ok(QuarantineRecoveryMatch::NotApplicable);
    }
    let Some(reason_code) = episode
        .quarantine_reason_code
        .filter(|reason| reason.automatic_recovery_policy().is_some())
    else {
        return Ok(QuarantineRecoveryMatch::Superseded);
    };
    let Some(recovery_fence) = episode.quarantine_recovery else {
        return Ok(QuarantineRecoveryMatch::Superseded);
    };

    let recovery = tx
        .query_row(
            "SELECT qs.quarantine_id, s.space_api_route_execution_generation
               FROM sandbox_quarantine_state qs
               JOIN sandboxes s ON s.id = qs.sandbox_id
              WHERE qs.sandbox_id = ?1
                AND qs.source = ?2
                AND qs.reason_code = ?3
                AND s.state IN ('cleanup_required', 'cleanup_failed')
                AND s.current_local_artifact_id = ?4
                AND s.current_local_artifact_kind = 'full'
                AND s.current_local_artifact_origin = 'suspend'
                AND s.space_api_route_execution_generation = ?5",
            params![
                sandbox_id,
                SandboxQuarantineSource::SbxManager.as_str(),
                reason_code.as_str(),
                recovery_fence.local_artifact_id.to_string(),
                recovery_fence.execution_generation.get(),
            ],
            |row| {
                let execution_generation = ExecutionGeneration::new(row.get::<_, i64>(1)?)
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(1, Type::Integer, Box::new(error))
                    })?;
                Ok(QuarantineRecoveryCompletion {
                    quarantine_id: row.get(0)?,
                    reason_code,
                    receipt_id: cleanup_id,
                    execution_generation,
                })
            },
        )
        .optional()?;

    Ok(recovery.map_or(
        QuarantineRecoveryMatch::Superseded,
        QuarantineRecoveryMatch::Current,
    ))
}

fn transition_receipt_for_completion(
    cleanup_id: SandboxCleanupEpisodeId,
    episode: &CleanupEpisode,
    recovery: Option<&QuarantineRecoveryCompletion>,
) -> std::result::Result<
    Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    SandboxTransitionReceiptInvariantError,
> {
    if let Some(recovery) = recovery {
        return Ok(Some(SandboxTransitionReceipt::quarantine_recovery(
            recovery.receipt_id,
            recovery.execution_generation,
            recovery.reason_code,
            // Prefer the independent operation owner for lifecycle receipts.
            episode
                .operation_activation_attempt_id
                .or(episode.activation_attempt_id),
        )));
    }

    episode
        .completion
        .transition_target()
        .zip(episode.execution_generation)
        .zip(episode.quarantine_reason_code)
        // Old episodes have no operation field; preserve their legacy receipt.
        .zip(
            episode
                .operation_activation_attempt_id
                .or(episode.activation_attempt_id),
        )
        .map(
            |(((target_state, execution_generation), reason_code), activation_attempt_id)| {
                SandboxTransitionReceipt::try_new(
                    cleanup_id,
                    SandboxTransitionReceiptKind::ActivationRollback,
                    target_state,
                    execution_generation,
                    Some(reason_code),
                    Some(activation_attempt_id),
                )
            },
        )
        .transpose()
}

fn apply_cleanup_state(
    tx: &Transaction<'_>,
    sandbox_id: &str,
    target_state: SandboxState,
    receipt: Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    failure_reason: Option<&str>,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
) -> Result<bool> {
    if let Some((local_artifact_id, local_path)) = published_suspend_artifact {
        upsert_suspend_local_artifact(tx, sandbox_id, local_artifact_id, local_path)?;
    }
    let published_artifact_id = published_suspend_artifact.map(|(id, _)| id.to_string());
    // An ordinary STOP retains only generation/pod identity as teardown proof;
    // routing credentials and live runtime fields are always cleared. Activation
    // rollback instead uses its explicit receipt and must clear the identity.
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = ?1,
                uid = NULL,
                slot = NULL,
                guest_cid = NULL,
                pid = NULL,
                spaced_host_pid = NULL,
                netgtw_pid = NULL,
                vsock_uds_path = NULL,
                api_socket_path = NULL,
                space_api_route_generation = NULL,
                space_api_route_node_id = NULL,
                space_api_route_execution_generation = CASE
                    WHEN (?1 = 'stopped' AND ?4 IS NULL) OR (?1 = 'suspended'
                         AND (
                             ?11 IS NOT NULL
                             OR (
                                 current_local_artifact_id IS NOT NULL
                                 AND current_local_artifact_kind = 'full'
                                 AND current_local_artifact_origin = 'suspend'
                             )
                         ))
                    THEN space_api_route_execution_generation
                    ELSE NULL
                END,
                space_api_route_pod_uid = CASE
                    WHEN (?1 = 'stopped' AND ?4 IS NULL) OR (?1 = 'suspended'
                         AND (
                             ?11 IS NOT NULL
                             OR (
                                 current_local_artifact_id IS NOT NULL
                                 AND current_local_artifact_kind = 'full'
                                 AND current_local_artifact_origin = 'suspend'
                             )
                         ))
                    THEN space_api_route_pod_uid
                    ELSE NULL
                END,
                space_api_route_substrate_kind = NULL,
                space_api_route_activation_attempt_id = NULL,
                space_api_route_identity_provenance = NULL,
                spaced_endpoint_proof_json = NULL,
                guest_uptime_secs = NULL,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                transition_receipt_id = CASE
                    WHEN ?4 IS NOT NULL THEN ?4
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_id
                    ELSE NULL
                END,
                transition_receipt_kind = CASE
                    WHEN ?4 IS NOT NULL THEN ?5
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_kind
                    ELSE NULL
                END,
                transition_receipt_target_state = CASE
                    WHEN ?4 IS NOT NULL THEN ?6
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                    ELSE NULL
                END,
                transition_receipt_execution_generation = CASE
                    WHEN ?4 IS NOT NULL THEN ?7
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                    ELSE NULL
                END,
                transition_receipt_reason_code = CASE
                    WHEN ?4 IS NOT NULL THEN ?8
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                    ELSE NULL
                END,
                transition_receipt_activation_attempt_id = CASE
                    WHEN ?4 IS NOT NULL THEN ?9
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                    ELSE NULL
                END,
                failure_reason = ?10,
                current_local_artifact_id = COALESCE(?11, current_local_artifact_id),
                current_local_artifact_kind = CASE
                    WHEN ?11 IS NULL THEN current_local_artifact_kind ELSE 'full'
                END,
                current_local_artifact_origin = CASE
                    WHEN ?11 IS NULL THEN current_local_artifact_origin ELSE 'suspend'
                END,
                suspend_predecessor_artifact_id = NULL,
                state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
                updated_at = ?2
          WHERE id = ?3",
        params![
            target_state.as_str(),
            now()?,
            sandbox_id,
            receipt.map(|receipt| receipt.id().to_string()),
            receipt.map(|receipt| receipt.kind().as_str()),
            receipt.map(|receipt| receipt.target_state().as_str()),
            receipt.map(|receipt| receipt.execution_generation().get()),
            receipt.and_then(|receipt| receipt.reason_code().map(|reason| reason.as_str())),
            receipt.and_then(|receipt| receipt.activation_attempt_id().map(|id| id.to_string())),
            failure_reason,
            published_artifact_id,
        ],
    )?;
    Ok(changed == 1)
}

fn complete_state_cleanup(
    tx: &Transaction<'_>,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    episode: &CleanupEpisode,
    recovery: Option<&QuarantineRecoveryCompletion>,
) -> std::result::Result<bool, CompleteCleanupEpisodeError> {
    let quarantine_completion = episode.completion == CleanupCompletion::Quarantine;
    let target_state = episode
        .completion
        .target_state()
        .ok_or(CompleteCleanupEpisodeError::DeleteHasNoTargetState)?;
    let receipt = transition_receipt_for_completion(cleanup_id, episode, recovery)?;
    let retained_failure_reason = if recovery.is_some() {
        None
    } else {
        episode.failure_message.as_deref()
    };
    let previous_state = current_state_label(tx, sandbox_id)?;
    let applied = apply_cleanup_state(
        tx,
        sandbox_id,
        target_state,
        receipt,
        retained_failure_reason,
        None,
    )?;
    if applied && quarantine_completion {
        insert_operator_quarantine_episode(
            tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: previous_state.as_deref(),
                source: SandboxQuarantineSource::SbxManager,
                reason_code: episode
                    .quarantine_reason_code
                    .unwrap_or(SandboxQuarantineReasonCode::RuntimeProbeInconsistent),
                reason_message: retained_failure_reason,
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &now()?,
            },
        )?;
    }
    Ok(applied)
}

fn retire_quarantine_recovery(
    tx: &Transaction<'_>,
    sandbox_id: &str,
    recovery: &QuarantineRecoveryCompletion,
) -> std::result::Result<(), CompleteCleanupEpisodeError> {
    let inserted = tx.execute(
        "INSERT INTO sandbox_quarantine_history (
             id, quarantine_id, sandbox_id, occurred_at, source, action,
             reason_code, message, next_action, evidence_json
         )
         SELECT ?1, quarantine_id, sandbox_id, ?2, ?3, ?4,
                reason_code, 'canonical suspend artifact validated after runtime cleanup',
                NULL, evidence_json
           FROM sandbox_quarantine_state
          WHERE sandbox_id = ?5
            AND quarantine_id = ?6
            AND reason_code = ?7",
        params![
            recovery.receipt_id.to_string(),
            now()?,
            SandboxQuarantineSource::SbxManager.as_str(),
            SandboxQuarantineHistoryAction::RecoveredToSuspended.as_str(),
            sandbox_id,
            recovery.quarantine_id,
            recovery.reason_code.as_str(),
        ],
    )?;
    if inserted != 1 {
        return Err(CompleteCleanupEpisodeError::RecoveryHistoryInvariant { changed: inserted });
    }

    let removed = tx.execute(
        "DELETE FROM sandbox_quarantine_state
          WHERE sandbox_id = ?1 AND quarantine_id = ?2",
        params![sandbox_id, recovery.quarantine_id],
    )?;
    if removed != 1 {
        return Err(
            CompleteCleanupEpisodeError::RecoveryStateRetirementInvariant { changed: removed },
        );
    }
    Ok(())
}

/// Apply an activation rollback and retire its fence in one transaction.
///
/// Runtime cleanup happens before this call. The episode id prevents an old
/// worker from completing a newer activation, while the transaction prevents
/// a crash from leaving a healthy row blocked by a requested episode.
pub fn complete_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<Option<()>, CompleteCleanupEpisodeError> {
    complete_cleanup_episode_for_job(conn, sandbox_id, cleanup_id, None)
}

pub fn complete_cleanup_episode_for_job(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    job_id: Option<&JobId>,
) -> std::result::Result<Option<()>, CompleteCleanupEpisodeError> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(None);
    };
    if episode.id != cleanup_id
        || episode.requested_at.is_none()
        || !cleanup_episode_current(&tx, sandbox_id, cleanup_id)?
    {
        tx.commit()?;
        return Ok(None);
    }

    let previous_state = current_state_label(&tx, sandbox_id)?;
    validate_cleanup_completion_artifact(&tx, sandbox_id, &episode)?;
    let recovery = match match_quarantine_recovery(&tx, sandbox_id, cleanup_id, &episode)? {
        QuarantineRecoveryMatch::NotApplicable => None,
        QuarantineRecoveryMatch::Superseded => {
            tx.commit()?;
            return Ok(None);
        }
        QuarantineRecoveryMatch::Current(recovery) => Some(recovery),
    };

    if episode.completion.deletes_row() {
        let execution_generation = episode
            .execution_generation
            .or(runtime_execution_generation(&tx, sandbox_id)?);
        record_execution_delete_receipt(&tx, sandbox_id, execution_generation)?;
        delete_sandbox_child_rows(&tx, sandbox_id)?;
        tx.execute("DELETE FROM sandboxes WHERE id = ?1", params![sandbox_id])
            .map_err(auto_lifecycle_delete_receipts::map_pending_receipt_delete_error)?;
    } else {
        if !complete_state_cleanup(&tx, sandbox_id, cleanup_id, &episode, recovery.as_ref())? {
            tx.commit()?;
            return Ok(None);
        }
        if let Some(recovery) = &recovery {
            retire_quarantine_recovery(&tx, sandbox_id, recovery)?;
        }
        // Created, Stopped, and Suspended are activation rollback states, not
        // completed lifecycle teardown. Keep the exact attempt's continuation
        // claim so a transiently failed RPC can retry after runtime cleanup.
        if episode.completion == CleanupCompletion::Quarantine {
            route_activation_claims::close_claim(&tx, sandbox_id)?;
        }
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![sandbox_id, cleanup_id.to_string()],
        )?;
    }

    cleanup_jobs::finish_in_transaction(&tx, job_id, &cleanup_jobs::CleanupOutcome::Applied)?;
    tx.commit()?;
    if episode.completion == CleanupCompletion::Quarantine {
        emit_sandbox_quarantined_event(
            sandbox_id,
            previous_state.as_deref(),
            episode
                .quarantine_reason_code
                .unwrap_or(SandboxQuarantineReasonCode::RuntimeProbeInconsistent),
            "complete_cleanup_episode",
            // Retain historical failure evidence in storage, but do not
            // republish potentially guest-controlled text in a new event.
            None,
        );
    }
    if let Some(recovery) = recovery {
        emit_sandbox_state_event(
            sandbox_id,
            SandboxEventType::QuarantineRecovered,
            Some(SandboxState::Quarantined.as_str()),
            recovery.reason_code.as_str(),
            "complete_cleanup_episode",
            SandboxState::Suspended,
            None,
        );
    }
    Ok(Some(()))
}

/// Why ordinary cleanup may not erase a sandbox row and its filesystem.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxEraseSafety {
    Safe,
    SuspendPublicationPending,
    RetainedFilesystemOnlyCopy,
}

/// Classify whether erasing this record would drop unpublished suspend state.
///
/// The live and retry-owned states still have a suspend completion that can
/// publish. A terminal missing-artifact failure cannot publish, but its
/// retained rootfs may be the only copy of the sandbox's latest work. The
/// failure reason is the durable witness for that distinct safety condition.
pub fn sandbox_erase_safety(
    state: SandboxState,
    cleanup_completion: Option<CleanupCompletion>,
    failure_reason: Option<&str>,
) -> SandboxEraseSafety {
    if state == SandboxState::Suspending
        || (matches!(
            state,
            SandboxState::CleanupRequired | SandboxState::CleanupFailed
        ) && cleanup_completion == Some(CleanupCompletion::MarkSuspended))
    {
        SandboxEraseSafety::SuspendPublicationPending
    } else if matches!(state, SandboxState::Failed | SandboxState::Quarantined)
        && failure_reason == Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    {
        SandboxEraseSafety::RetainedFilesystemOnlyCopy
    } else {
        SandboxEraseSafety::Safe
    }
}

/// Complete cleanup owned by lifecycle intent stored directly on the sandbox
/// row. Routine stop, delete, kill, suspend, and startup recovery use this
/// path; activation compensation uses [`complete_cleanup_episode`] instead.
#[derive(Debug, thiserror::Error)]
pub enum CompleteRowIntentCleanupError {
    #[error("row-deleting cleanup cannot complete as a resident state")]
    DeleteHasNoTargetState,
    #[error(transparent)]
    Database(#[from] Error),
    #[error(transparent)]
    ReceiptCompletion(#[from] auto_lifecycle_delete_receipts::CompletePendingReceiptError),
    #[error(
        "expected auto-lifecycle delete receipt is missing or changed for sandbox {sandbox_id}"
    )]
    ExpectedReceiptMissingOrChanged { sandbox_id: SandboxId },
    #[error("pending receipt outlived its delete intent for sandbox {sandbox_id}")]
    PendingReceiptLostDeleteIntent { sandbox_id: SandboxId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompleteRowIntentCleanupDisposition {
    Applied,
    Quarantined,
    Superseded,
}

fn receipt_superseding_execution_generation(
    conn: &Connection,
    row: &SandboxRow,
    expected_generation: ExecutionGeneration,
) -> Result<Option<ExecutionGeneration>> {
    let retained_generation = conn
        .query_row(
            "SELECT last_observed_execution_generation
               FROM sandboxes
              WHERE id = ?1",
            [row.id.as_str()],
            |sql_row| sql_row.get::<_, Option<i64>>(0),
        )?
        .map(ExecutionGeneration::new)
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(0, Type::Integer, Box::new(error))
        })?;

    // The current route generation identifies the execution attached to the
    // row. The retained generation is its monotonic route high-water mark.
    // Either one strictly advancing proves that the expected receipt no
    // longer owns this execution; NULL or lower evidence does not.
    Ok(row
        .space_api_route_identity
        .execution_generation
        .into_iter()
        .chain(retained_generation)
        .filter(|generation| *generation > expected_generation)
        .max())
}

fn emit_auto_lifecycle_delete_receipt_completion_superseded(
    sandbox_id: SandboxId,
    expected_receipt: crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt,
    superseding_generation: ExecutionGeneration,
) {
    emit_sandbox_event(
        &sandbox_id.to_string(),
        SandboxEventType::DeleteSuperseded,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .operation("delete")
            .trigger(SandboxEventTrigger::System)
            .result("superseded")
            .receipt_id(expected_receipt.receipt_id.to_string())
            .execution_generation(expected_receipt.execution_generation.to_string())
            .reason(format!(
                "delete receipt completion superseded by execution generation {superseding_generation}"
            )),
    );
}

#[cfg(any(test, feature = "test-support"))]
pub fn complete_row_intent_cleanup(
    conn: &Connection,
    sandbox_id: &str,
    completion: CleanupCompletion,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
) -> std::result::Result<(), CompleteRowIntentCleanupError> {
    complete_row_intent_cleanup_with_receipt(
        conn,
        sandbox_id,
        completion,
        published_suspend_artifact,
        None,
    )
    .map(|_| ())
}

#[cfg(any(test, feature = "test-support"))]
pub fn complete_row_intent_cleanup_with_receipt(
    conn: &Connection,
    sandbox_id: &str,
    completion: CleanupCompletion,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
    expected_receipt: Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt>,
) -> std::result::Result<CompleteRowIntentCleanupDisposition, CompleteRowIntentCleanupError> {
    complete_row_intent_cleanup_for_job(
        conn,
        sandbox_id,
        completion,
        published_suspend_artifact,
        expected_receipt,
        None,
    )
}

pub fn complete_row_intent_cleanup_for_job(
    conn: &Connection,
    sandbox_id: &str,
    completion: CleanupCompletion,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
    expected_receipt: Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt>,
    job_id: Option<&JobId>,
) -> std::result::Result<CompleteRowIntentCleanupDisposition, CompleteRowIntentCleanupError> {
    if published_suspend_artifact.is_some() && completion != CleanupCompletion::MarkSuspended {
        return Err(Error::invalid_query().into());
    }
    let tx = immediate_transaction(conn)?;
    let parsed_delete_sandbox_id = (completion.deletes_row())
        .then(|| sandbox_id.parse().map_err(Error::to_sql_conversion))
        .transpose()?;
    let pending_delete_receipt = parsed_delete_sandbox_id
        .map(|sandbox_id| {
            auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(&tx, sandbox_id)
        })
        .transpose()?
        .flatten();
    let Some(row) = get_sandbox(&tx, sandbox_id)? else {
        if let Some(sandbox_id) = parsed_delete_sandbox_id
            && pending_delete_receipt.is_some()
        {
            return Err(
                CompleteRowIntentCleanupError::PendingReceiptLostDeleteIntent { sandbox_id },
            );
        }
        tx.commit()?;
        return Ok(if job_id.is_some() {
            CompleteRowIntentCleanupDisposition::Superseded
        } else {
            CompleteRowIntentCleanupDisposition::Applied
        });
    };
    if let (Some(parsed_sandbox_id), Some(expected_receipt)) =
        (parsed_delete_sandbox_id, expected_receipt)
        && pending_delete_receipt != Some(expected_receipt)
    {
        if let Some(superseding_generation) = receipt_superseding_execution_generation(
            &tx,
            &row,
            expected_receipt.execution_generation,
        )? {
            tx.commit()?;
            emit_auto_lifecycle_delete_receipt_completion_superseded(
                parsed_sandbox_id,
                expected_receipt,
                superseding_generation,
            );
            return Ok(CompleteRowIntentCleanupDisposition::Superseded);
        }
        return Err(
            CompleteRowIntentCleanupError::ExpectedReceiptMissingOrChanged {
                sandbox_id: parsed_sandbox_id,
            },
        );
    }
    if cleanup_episode(&tx, sandbox_id)?.is_some() || !completion.owns_row_intent(&row) {
        if let Some(sandbox_id) = parsed_delete_sandbox_id
            && pending_delete_receipt.is_some()
        {
            return Err(
                CompleteRowIntentCleanupError::PendingReceiptLostDeleteIntent { sandbox_id },
            );
        }
        tx.commit()?;
        return Ok(if job_id.is_some() {
            CompleteRowIntentCleanupDisposition::Superseded
        } else {
            CompleteRowIntentCleanupDisposition::Applied
        });
    }
    if let Some(parsed_sandbox_id) = parsed_delete_sandbox_id {
        if let Some(receipt) = pending_delete_receipt {
            // A caller-supplied expected receipt was matched by the hoisted
            // ownership check. Startup and operator recovery instead derive
            // ownership from this durable receipt plus the DeleteRow intent.
            auto_lifecycle_delete_receipts::complete_pending_receipt(
                &tx,
                parsed_sandbox_id,
                receipt.receipt_id,
            )?;
        }
        record_execution_delete_receipt(
            &tx,
            sandbox_id,
            row.space_api_route_identity.execution_generation,
        )?;
        delete_sandbox_child_rows(&tx, sandbox_id)?;
        tx.execute("DELETE FROM sandboxes WHERE id = ?1", params![sandbox_id])
            .map_err(auto_lifecycle_delete_receipts::map_pending_receipt_delete_error)?;
        cleanup_jobs::finish_in_transaction(&tx, job_id, &cleanup_jobs::CleanupOutcome::Applied)?;
        tx.commit()?;
        return Ok(CompleteRowIntentCleanupDisposition::Applied);
    }

    let missing_suspend_artifact = completion == CleanupCompletion::MarkSuspended
        && published_suspend_artifact.is_none()
        && !sandbox_has_registered_full_suspend_artifact(&tx, &row)?;
    let orphan_cleanup = completion == CleanupCompletion::Quarantine
        && row.template == ORPHANED_SANDBOX_TEMPLATE
        && row.user == ORPHANED_SANDBOX_OWNER;
    let quarantine_completion = completion == CleanupCompletion::Quarantine;
    let quarantine_reason = if orphan_cleanup {
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing
    } else if missing_suspend_artifact
        || row.failure_reason.as_deref() == Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    {
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed
    } else {
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent
    };
    let target_state = if missing_suspend_artifact {
        SandboxState::Quarantined
    } else {
        completion
            .target_state()
            .ok_or(CompleteRowIntentCleanupError::DeleteHasNoTargetState)?
    };
    let failure_reason = if missing_suspend_artifact {
        Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    } else if quarantine_completion {
        row.failure_reason.as_deref()
    } else {
        None
    };
    if !apply_cleanup_state(
        &tx,
        sandbox_id,
        target_state,
        None,
        failure_reason,
        published_suspend_artifact,
    )? {
        return Err(Error::no_rows().into());
    }
    if missing_suspend_artifact {
        let timestamp = now()?;
        insert_new_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: Some(row.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code: SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
                reason_message: failure_reason,
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &timestamp,
            },
        )?;
    }
    if quarantine_completion {
        insert_operator_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: Some(row.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code: quarantine_reason,
                reason_message: failure_reason,
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &now()?,
            },
        )?;
    }
    // `apply_cleanup_state` clears the live identity and fires the retained
    // identity trigger. Close the claim only after that observation, but
    // before this transaction commits.
    route_activation_claims::close_claim(&tx, sandbox_id)?;
    let outcome = if missing_suspend_artifact {
        cleanup_jobs::CleanupOutcome::Quarantined(
            SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE.to_owned(),
        )
    } else {
        cleanup_jobs::CleanupOutcome::Applied
    };
    cleanup_jobs::finish_in_transaction(&tx, job_id, &outcome)?;
    tx.commit()?;

    if missing_suspend_artifact || quarantine_completion {
        emit_sandbox_quarantined_event(
            sandbox_id,
            Some(row.state.as_str()),
            quarantine_reason,
            "complete_row_intent_cleanup",
            None,
        );
    }
    Ok(if missing_suspend_artifact {
        CompleteRowIntentCleanupDisposition::Quarantined
    } else {
        CompleteRowIntentCleanupDisposition::Applied
    })
}

pub fn quarantine_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    unsafe_evidence: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    quarantine_cleanup_episode_for_job(conn, sandbox_id, cleanup_id, unsafe_evidence, None)
}

pub fn quarantine_cleanup_episode_for_job(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    unsafe_evidence: &str,
    job_id: Option<&JobId>,
) -> std::result::Result<bool, MarkQuarantinedError> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(false);
    };
    if episode.id != cleanup_id {
        tx.commit()?;
        return Ok(false);
    }
    let Some(reason_code) = episode.quarantine_reason_code else {
        tx.commit()?;
        return Ok(false);
    };
    let Some(row) = get_sandbox(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(false);
    };
    if !matches!(
        row.state,
        SandboxState::CleanupRequired | SandboxState::CleanupFailed
    ) {
        tx.commit()?;
        return Ok(false);
    }
    let reason = format!(
        "activation compensation cannot safely reach {}; unsafe_evidence={unsafe_evidence}",
        episode.completion,
    );
    row.state.transition(SandboxState::Quarantined)?;
    let timestamp = now()?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = 'quarantined',
                failure_reason = ?1,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                state_changed_at = CASE
                    WHEN state != 'quarantined' THEN ?2
                    ELSE state_changed_at
                END,
                updated_at = ?2
          WHERE id = ?3
            AND state = ?4
            AND cleanup_completion = ?5
            AND cleanup_keep_filesystem = ?6",
        params![
            reason,
            timestamp,
            sandbox_id,
            row.state.as_str(),
            episode.completion.as_str(),
            episode.keep_filesystem,
        ],
    )?;
    if changed == 1 {
        route_activation_claims::close_claim(&tx, sandbox_id)?;
        insert_new_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: Some(row.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code,
                reason_message: Some(&reason),
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &timestamp,
            },
        )?;
        if episode.completion != CleanupCompletion::DeletePoolClone {
            tx.execute(
                "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
                params![sandbox_id, cleanup_id.to_string()],
            )?;
        }
        cleanup_jobs::finish_in_transaction(
            &tx,
            job_id,
            &cleanup_jobs::CleanupOutcome::Quarantined(reason.clone()),
        )?;
    }
    tx.commit()?;
    if changed == 1 {
        emit_sandbox_quarantined_event(
            sandbox_id,
            Some(row.state.as_str()),
            reason_code,
            "quarantine_cleanup_episode",
            Some(&reason),
        );
    }
    Ok(changed == 1)
}

pub fn mark_cleanup_required(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<()> {
    let reason = reason.trim();
    let reason = (!reason.is_empty()).then_some(reason);
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, SandboxState::CleanupRequired) {
        tracing::warn!(
            sandbox_id = %id,
            "ignored mark_cleanup_required because sandbox is quarantined"
        );
        return Ok(());
    }
    conn.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_required',
             failure_reason = ?1,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = CASE
                 WHEN state != 'cleanup_required' THEN ?4
                 ELSE state_changed_at
             END,
             updated_at = ?4
         WHERE id = ?5
           AND state != 'quarantined'",
        params![reason, completion.as_str(), keep_filesystem, now()?, id],
    )?;
    Ok(())
}

/// Persist a cleanup requirement only while no cleanup intent is already on
/// the row. Returns whether the write landed; `false` means another
/// producer's persisted intent (e.g. a pending user delete) is present and
/// must not be overwritten by a stale lifecycle observer.
pub fn mark_cleanup_required_if_no_intent(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<bool> {
    mark_cleanup_required_if_no_intent_inner(conn, id, keep_filesystem, completion, reason, None)
}

pub fn mark_cleanup_required_if_no_intent_in_state(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<bool> {
    mark_cleanup_required_if_no_intent_inner(
        conn,
        id,
        keep_filesystem,
        completion,
        reason,
        Some(expected_state),
    )
}

fn mark_cleanup_required_if_no_intent_inner(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
    expected_state: Option<SandboxState>,
) -> Result<bool> {
    let reason = reason.trim();
    let reason = (!reason.is_empty()).then_some(reason);
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, SandboxState::CleanupRequired) {
        tracing::warn!(
            sandbox_id = %id,
            "ignored mark_cleanup_required_if_no_intent because sandbox is quarantined"
        );
        return Ok(false);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_required',
             failure_reason = ?1,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = CASE
                 WHEN state != 'cleanup_required' THEN ?4
                 ELSE state_changed_at
             END,
             updated_at = ?4
         WHERE id = ?5
           AND state != 'quarantined'
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL
           AND (?6 IS NULL OR state = ?6)",
        params![
            reason,
            completion.as_str(),
            keep_filesystem,
            now()?,
            id,
            expected_state.map(|state| state.as_str()),
        ],
    )?;
    if changed == 1 {
        emit_sandbox_state_event(
            id,
            SandboxEventType::CleanupRequested,
            previous_state.as_deref(),
            reason.unwrap_or("cleanup_required"),
            "mark_cleanup_required_if_no_intent",
            SandboxState::CleanupRequired,
            None,
        );
    }
    Ok(changed == 1)
}

pub fn set_cleanup_intent(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET cleanup_completion = ?1,
             cleanup_keep_filesystem = ?2,
             updated_at = ?3
         WHERE id = ?4
           AND NOT EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes WHERE sandbox_id = ?4
           )",
        params![completion.as_str(), keep_filesystem, now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn mark_cleanup_failed(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<bool> {
    let reason = reason.trim();
    let reason = (!reason.is_empty()).then_some(reason);
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, SandboxState::CleanupFailed) {
        tracing::warn!(
            sandbox_id = %id,
            "ignored mark_cleanup_failed because sandbox is quarantined"
        );
        return Ok(false);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_failed',
             failure_reason = ?1,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             state_changed_at = CASE
                 WHEN state != 'cleanup_failed' THEN ?4
                 ELSE state_changed_at
             END,
             updated_at = ?4
         WHERE id = ?5
           AND state != 'quarantined'",
        params![reason, completion.as_str(), keep_filesystem, now()?, id],
    )?;
    if changed != 0 && previous_state_value != Some(SandboxState::CleanupFailed) {
        emit_sandbox_state_event(
            id,
            SandboxEventType::CleanupFailed,
            previous_state.as_deref(),
            "cleanup_failed",
            "mark_cleanup_failed",
            SandboxState::CleanupFailed,
            reason,
        );
    }
    Ok(changed != 0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn clear_cleanup_intent(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes
         SET cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             updated_at = ?1
         WHERE id = ?2",
        params![now()?, id],
    )?;
    Ok(())
}

/// Identity and authorization fields needed to target a cleanup. Optional
/// launch and snapshot metadata must not make stop/delete unavailable.
pub struct CleanupTarget {
    pub uuid8: SandboxRuntimeLabel,
    pub template: String,
    pub user: SandboxOwner,
}

pub fn cleanup_target(conn: &Connection, id: &str) -> Result<Option<CleanupTarget>> {
    conn.query_row(
        sql!("SELECT uuid8, template, user FROM sandboxes WHERE id = ?1"),
        [id],
        |row| {
            Ok(CleanupTarget {
                uuid8: SandboxRuntimeLabel::parse(row.get::<_, String>(0)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
                })?,
                template: row.get(1)?,
                user: SandboxOwner::parse(row.get::<_, String>(2)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(error))
                })?,
            })
        },
    )
    .optional()
}

pub fn cleanup_runtime_label(conn: &Connection, id: &str) -> Result<Option<SandboxRuntimeLabel>> {
    conn.query_row(
        sql!("SELECT uuid8 FROM sandboxes WHERE id = ?1"),
        [id],
        |row| {
            SandboxRuntimeLabel::parse(row.get::<_, String>(0)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        },
    )
    .optional()
}

pub fn cleanup_intent_current(
    conn: &Connection,
    id: &str,
    completion: CleanupCompletion,
) -> Result<bool> {
    let intent = conn
        .query_row(
            sql!("SELECT state, cleanup_completion FROM sandboxes WHERE id = ?1"),
            [id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((state, intent)) = intent else {
        return Ok(false);
    };
    let Ok(state) = state.parse::<SandboxState>() else {
        return Ok(false);
    };
    let intent = intent
        .map(|value| {
            value.parse::<CleanupCompletion>().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })
        })
        .transpose()?;
    Ok(completion.owns_intent(state, intent))
}

/// Sandboxes with durable cleanup evidence that a live reconciliation pass may
/// safely re-drive after acquiring the per-sandbox cleanup fence.
#[cfg(any(test, feature = "test-support"))]
pub fn list_requested_cleanup_recovery_candidate_ids(conn: &Connection) -> Result<Vec<SandboxId>> {
    let mut statement = conn.prepare(
        "SELECT sandbox.id
           FROM sandboxes sandbox
           LEFT JOIN sandbox_cleanup_episodes episode
             ON episode.sandbox_id = sandbox.id
          WHERE (episode.phase = 'requested' AND NOT (episode.completion = 'delete_pool_clone' AND sandbox.state = 'quarantined'))
             OR (
                  episode.sandbox_id IS NULL
                  AND (
                       (
                         sandbox.cleanup_completion IS NOT NULL
                         AND sandbox.cleanup_keep_filesystem IS NOT NULL
                       )
                       OR (
                         sandbox.cleanup_completion IS NULL
                         AND sandbox.cleanup_keep_filesystem IS NULL
                         AND (
                              sandbox.state IN ('stopping', 'deleting', 'cleanup_required')
                              OR (
                                   sandbox.state = 'cleanup_failed'
                                   AND (
                                        sandbox.uid IS NOT NULL
                                        OR sandbox.slot IS NOT NULL
                                        OR sandbox.guest_cid IS NOT NULL
                                        OR sandbox.pid IS NOT NULL
                                        OR sandbox.spaced_host_pid IS NOT NULL
                                        OR sandbox.netgtw_pid IS NOT NULL
                                        OR sandbox.vsock_uds_path IS NOT NULL
                                        OR sandbox.api_socket_path IS NOT NULL
                                   )
                              )
                         )
                       )
                  )
             )
          ORDER BY sandbox.created_at",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut sandbox_ids = Vec::new();
    for sandbox_id in rows {
        let sandbox_id = sandbox_id?;
        match SandboxId::from_str(&sandbox_id) {
            Ok(sandbox_id) => sandbox_ids.push(sandbox_id),
            Err(error) => tracing::warn!(
                %sandbox_id,
                %error,
                "skipping invalid sandbox id during requested cleanup recovery"
            ),
        }
    }
    Ok(sandbox_ids)
}

/// Discover unscheduled obligations without decoding the entire local inventory.
/// Limit the inventory range first, so a sparse backlog cannot turn one page
/// into an inventory-sized scan. The cursor advances even when none qualify.
pub struct CleanupRecoveryPage {
    pub ids: Vec<String>,
    pub next_cursor: Option<i64>,
}

pub fn cleanup_recovery_page(
    conn: &Connection,
    after: Option<i64>,
    limit: usize,
) -> Result<CleanupRecoveryPage> {
    let sql_limit = i64::try_from(limit)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    // The rowid is a scan position, never lifecycle authority.
    let Some(first) = after.map_or(Some(i64::MIN), |rowid| rowid.checked_add(1)) else {
        return Ok(CleanupRecoveryPage {
            ids: Vec::new(),
            next_cursor: None,
        });
    };
    let tx = Transaction::deferred(conn)?;
    let mut inventory = tx.prepare(sql!(
        "SELECT rowid FROM sandboxes WHERE rowid >= ?1 ORDER BY rowid LIMIT ?2"
    ))?;
    let range: Vec<i64> = inventory
        .query_map(params![first, sql_limit], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let Some(&last) = range.last() else {
        return Ok(CleanupRecoveryPage {
            ids: Vec::new(),
            next_cursor: None,
        });
    };
    let mut statement = tx.prepare(
        sql!("SELECT sandbox.id
           FROM sandboxes sandbox
           LEFT JOIN sandbox_cleanup_episodes episode
             ON episode.sandbox_id = sandbox.id
          WHERE sandbox.rowid >= ?1 AND sandbox.rowid <= ?2 AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.kind = 'sandbox_cleanup' AND jobs.subject_id = sandbox.id AND jobs.status IN ('pending', 'running')) AND ((episode.phase = 'requested' AND NOT (episode.completion = 'delete_pool_clone' AND sandbox.state = 'quarantined'))
             OR (
                  episode.sandbox_id IS NULL
                  AND (
                       (
                         sandbox.cleanup_completion IS NOT NULL
                         AND sandbox.cleanup_keep_filesystem IS NOT NULL
                       )
                       OR (
                         sandbox.cleanup_completion IS NULL
                         AND sandbox.cleanup_keep_filesystem IS NULL
                         AND (
                              sandbox.state IN ('stopping', 'deleting', 'cleanup_required')
                              OR (
                                   sandbox.state = 'cleanup_failed'
                                   AND (
                                        sandbox.uid IS NOT NULL
                                        OR sandbox.slot IS NOT NULL
                                        OR sandbox.guest_cid IS NOT NULL
                                        OR sandbox.pid IS NOT NULL
                                        OR sandbox.spaced_host_pid IS NOT NULL
                                        OR sandbox.netgtw_pid IS NOT NULL
                                        OR sandbox.vsock_uds_path IS NOT NULL
                                        OR sandbox.api_socket_path IS NOT NULL
                                   )
                              )
                         )
                       )
                  )
             )
          ) ORDER BY sandbox.rowid"),
    )?;
    let ids = statement
        .query_map(params![first, last], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    let next_cursor = (range.len() == limit).then_some(last);
    drop(statement);
    drop(inventory);
    tx.commit()?;
    Ok(CleanupRecoveryPage { ids, next_cursor })
}
