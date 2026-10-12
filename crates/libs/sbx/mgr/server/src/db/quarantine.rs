//! `sandbox_quarantine_state` episodes: entering quarantine and publishing its reason.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishedQuarantineReasonCode {
    Known(SandboxQuarantineReasonCode),
    ForwardCompatible(String),
}

impl std::fmt::Display for PublishedQuarantineReasonCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PublishedQuarantineReasonCode {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Known(reason_code) => reason_code.as_str(),
            Self::ForwardCompatible(reason_code) => reason_code,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxStatePublicationMetadata {
    Quarantined {
        reason_code: PublishedQuarantineReasonCode,
    },
    TransitionReceipt(SandboxTransitionReceipt<PublishedQuarantineReasonCode>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawSandboxStatePublicationMetadata {
    pub quarantine_reason_code: Option<String>,
    pub transition_receipt_id: Option<String>,
    pub transition_receipt_kind: Option<String>,
    pub transition_receipt_target_state: Option<String>,
    pub transition_receipt_execution_generation: Option<i64>,
    pub transition_receipt_reason_code: Option<String>,
    pub transition_receipt_activation_attempt_id: Option<String>,
}

pub type RawSandboxStatePublicationMetadataRead =
    std::result::Result<RawSandboxStatePublicationMetadata, rusqlite::Error>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxStatePublicationMetadataField {
    QuarantineReasonCode,
    TransitionReceiptId,
    TransitionReceiptKind,
    TransitionReceiptTargetState,
    TransitionReceiptExecutionGeneration,
    TransitionReceiptReasonCode,
    TransitionReceiptActivationAttemptId,
}

impl SandboxStatePublicationMetadataField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuarantineReasonCode => "quarantine_reason_code",
            Self::TransitionReceiptId => "transition_receipt_id",
            Self::TransitionReceiptKind => "transition_receipt_kind",
            Self::TransitionReceiptTargetState => "transition_receipt_target_state",
            Self::TransitionReceiptExecutionGeneration => "transition_receipt_execution_generation",
            Self::TransitionReceiptReasonCode => "transition_receipt_reason_code",
            Self::TransitionReceiptActivationAttemptId => {
                "transition_receipt_activation_attempt_id"
            }
        }
    }
}

impl fmt::Display for SandboxStatePublicationMetadataField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeSandboxStatePublicationMetadataError {
    #[error("missing {field}")]
    Missing {
        field: SandboxStatePublicationMetadataField,
    },
    #[error("invalid {field}: {message}")]
    Invalid {
        field: SandboxStatePublicationMetadataField,
        message: String,
    },
}

impl DecodeSandboxStatePublicationMetadataError {
    pub const fn field(&self) -> SandboxStatePublicationMetadataField {
        match self {
            Self::Missing { field } | Self::Invalid { field, .. } => *field,
        }
    }
}

impl RawSandboxStatePublicationMetadata {
    pub fn decode(
        self,
        state: SandboxState,
    ) -> std::result::Result<
        SandboxStatePublicationMetadata,
        DecodeSandboxStatePublicationMetadataError,
    > {
        if state == SandboxState::Quarantined {
            let reason = required_trimmed(
                self.quarantine_reason_code,
                SandboxStatePublicationMetadataField::QuarantineReasonCode,
            )?;
            return Ok(SandboxStatePublicationMetadata::Quarantined {
                reason_code: parse_published_quarantine_reason(reason),
            });
        }

        let receipt_id = required_trimmed(
            self.transition_receipt_id,
            SandboxStatePublicationMetadataField::TransitionReceiptId,
        )?
        .parse()
        .map_err(|error: uuid::Error| {
            DecodeSandboxStatePublicationMetadataError::Invalid {
                field: SandboxStatePublicationMetadataField::TransitionReceiptId,
                message: error.to_string(),
            }
        })?;
        let kind = required_trimmed(
            self.transition_receipt_kind,
            SandboxStatePublicationMetadataField::TransitionReceiptKind,
        )?
        .parse()
        .map_err(
            |error: pplx_space_common_core::types::SandboxTransitionReceiptKindParseError| {
                DecodeSandboxStatePublicationMetadataError::Invalid {
                    field: SandboxStatePublicationMetadataField::TransitionReceiptKind,
                    message: error.to_string(),
                }
            },
        )?;
        let target_state: SandboxTransitionTargetState = required_trimmed(
            self.transition_receipt_target_state,
            SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
        )?
        .parse()
        .map_err(
            |error: pplx_space_common_core::types::SandboxTransitionTargetStateParseError| {
                DecodeSandboxStatePublicationMetadataError::Invalid {
                    field: SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
                    message: error.to_string(),
                }
            },
        )?;
        if target_state.as_str() != state.as_str() {
            return Err(DecodeSandboxStatePublicationMetadataError::Invalid {
                field: SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
                message: format!("target {target_state} does not match row state {state}"),
            });
        }
        let execution_generation = self
            .transition_receipt_execution_generation
            .ok_or(DecodeSandboxStatePublicationMetadataError::Missing {
                field: SandboxStatePublicationMetadataField::TransitionReceiptExecutionGeneration,
            })
            .and_then(|value| {
                ExecutionGeneration::new(value).map_err(|error| {
                    DecodeSandboxStatePublicationMetadataError::Invalid {
                        field: SandboxStatePublicationMetadataField::TransitionReceiptExecutionGeneration,
                        message: error.to_string(),
                    }
                })
            })?;
        let reason_code = self
            .transition_receipt_reason_code
            .map(|reason| {
                required_trimmed(
                    Some(reason),
                    SandboxStatePublicationMetadataField::TransitionReceiptReasonCode,
                )
                .map(parse_published_quarantine_reason)
            })
            .transpose()?;
        let activation_attempt_id = self
            .transition_receipt_activation_attempt_id
            .map(|value| {
                let value = value.trim();
                value.parse().map_err(|error: uuid::Error| {
                    DecodeSandboxStatePublicationMetadataError::Invalid {
                        field:
                            SandboxStatePublicationMetadataField::TransitionReceiptActivationAttemptId,
                        message: error.to_string(),
                    }
                })
            })
            .transpose()?;
        let receipt = SandboxTransitionReceipt::try_new(
            receipt_id,
            kind,
            target_state,
            execution_generation,
            reason_code,
            activation_attempt_id,
        )
        .map_err(
            |error| DecodeSandboxStatePublicationMetadataError::Invalid {
                field: match error {
                    SandboxTransitionReceiptInvariantError::MissingActivationAttempt => {
                        SandboxStatePublicationMetadataField::TransitionReceiptActivationAttemptId
                    }
                    SandboxTransitionReceiptInvariantError::MissingReasonCode
                    | SandboxTransitionReceiptInvariantError::UnexpectedReasonCode => {
                        SandboxStatePublicationMetadataField::TransitionReceiptReasonCode
                    }
                    SandboxTransitionReceiptInvariantError::InvalidActivationCompletionTarget {
                        ..
                    }
                    | SandboxTransitionReceiptInvariantError::InvalidActivationRollbackTarget {
                        ..
                    }
                    | SandboxTransitionReceiptInvariantError::InvalidQuarantineRecoveryTarget {
                        ..
                    } => SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
                },
                message: error.to_string(),
            },
        )?;
        Ok(SandboxStatePublicationMetadata::TransitionReceipt(receipt))
    }
}

fn required_trimmed(
    value: Option<String>,
    field: SandboxStatePublicationMetadataField,
) -> std::result::Result<String, DecodeSandboxStatePublicationMetadataError> {
    let value = value.ok_or(DecodeSandboxStatePublicationMetadataError::Missing { field })?;
    let value = value.trim();
    if value.is_empty() {
        return Err(DecodeSandboxStatePublicationMetadataError::Invalid {
            field,
            message: "value is blank".to_string(),
        });
    }
    Ok(value.to_string())
}

fn parse_published_quarantine_reason(value: String) -> PublishedQuarantineReasonCode {
    value.parse().map_or_else(
        |_| PublishedQuarantineReasonCode::ForwardCompatible(value),
        PublishedQuarantineReasonCode::Known,
    )
}

#[derive(Debug, thiserror::Error)]
pub enum MarkQuarantinedError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("unknown current sandbox state '{state}'")]
    UnknownCurrentState { state: String },
    #[error("illegal transition into quarantined: {0}")]
    IllegalTransition(#[from] StateTransitionError),
    #[error("quarantined sandbox insert must use quarantined state, got {state}")]
    NonQuarantinedInsert { state: SandboxState },
}

pub(super) struct QuarantineEpisode<'a> {
    pub(super) sandbox_id: &'a str,
    pub(super) entered_from_state: Option<&'a str>,
    pub(super) source: SandboxQuarantineSource,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) reason_message: Option<&'a str>,
    pub(super) next_action: SandboxQuarantineNextAction,
    pub(super) timestamp: &'a str,
}

fn insert_quarantine_history(
    conn: &Connection,
    episode: &QuarantineEpisode<'_>,
    quarantine_id: &str,
    action: SandboxQuarantineHistoryAction,
) -> Result<()> {
    conn.execute(
        "INSERT INTO sandbox_quarantine_history (
             id, quarantine_id, sandbox_id, occurred_at, source, action,
             reason_code, message, next_action, evidence_json
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, '{}')",
        params![
            uuid::Uuid::now_v7().to_string(),
            quarantine_id,
            episode.sandbox_id,
            episode.timestamp,
            episode.source.as_str(),
            action.as_str(),
            episode.reason_code.as_str(),
            episode.reason_message,
            episode.next_action.as_str()
        ],
    )?;
    Ok(())
}

/// Cleanup can discard resident memory while retaining newer filesystem data.
/// The orphan reason alone allows recovery from older snapshots, so these
/// episodes need an explicit owner outside automatic legacy recovery. Older
/// binaries already exclude unknown owners from their recovery scans.
pub(super) fn insert_operator_quarantine_episode(
    conn: &rusqlite::Transaction<'_>,
    episode: &QuarantineEpisode<'_>,
) -> Result<()> {
    insert_new_quarantine_episode(conn, episode)?;
    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_owner = ?1
         WHERE sandbox_id = ?2
           AND (next_action_owner IS NULL OR next_action_owner = ?3)",
        params![
            QuarantineRecoveryOwner::OperatorAttention.as_str(),
            episode.sandbox_id,
            QuarantineRecoveryOwner::LegacyRecovery.as_str()
        ],
    )?;
    Ok(())
}

pub(super) fn insert_new_quarantine_episode(
    conn: &Connection,
    episode: &QuarantineEpisode<'_>,
) -> Result<()> {
    teardown_jobs::fail_active(
        conn,
        episode.sandbox_id,
        TeardownSuspendFailure::Quarantined,
    )?;
    let quarantine_id = uuid::Uuid::now_v7().to_string();
    // A sandbox re-entering quarantine from a non-quarantined state replaces its
    // episode. Two things must survive that: a lease someone is still acting
    // under, and anything owned by a release other than this one.
    //
    // Clearing a live lease would make the episode look due and unclaimed while
    // its operation is still running, so the next tick would start a second one.
    // Clearing a foreign owner's state would reach into a newer release's
    // bookkeeping, which this release cannot reason about — the reconfirmation
    // path refuses that too.
    //
    // Every arm is NULL-guarded on purpose. `owner = ?3` against a NULL owner
    // yields SQL NULL rather than false, which propagates through AND, fails
    // `row.get::<bool>`, and aborts the whole transaction — leaving a sandbox
    // that must be quarantined un-quarantined.
    let current_epoch = now_epoch_i64()?;
    let (claim_dead, legacy_owned): (bool, bool) = conn
        .query_row(
            "SELECT claim_expires_at,
                    (next_action_owner IS NULL OR next_action_owner = ?2)
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![
                episode.sandbox_id,
                QuarantineRecoveryOwner::LegacyRecovery.as_str(),
            ],
            |row| {
                let expiry: rusqlite::types::Value = row.get(0)?;
                let claim_dead = matches!(expiry, rusqlite::types::Value::Null)
                    || timestamps::EpochSeconds::decode(&expiry)
                        .is_ok_and(|value| value.get() <= current_epoch);
                Ok((claim_dead, row.get(1)?))
            },
        )
        .optional()?
        // With no prior row there is no conflict, so the fragment these flags
        // pick is never executed and the values cannot be observed. This states
        // the intent — nothing to preserve — rather than implying a live branch.
        .unwrap_or((true, true));
    // Three cases, because "preserve" means different things to different owners.
    //
    // A newer release's owner keeps its retry and lease columns: clearing those
    // would reach into a mechanism this release cannot reason about. Its identity
    // columns are a different matter — the `ON CONFLICT` below rewrites everything
    // outside `retry_state_reset` on every branch, foreign-owned included:
    // `quarantine_id`, `source`, `reason_code`, `entered_at`,
    // `entered_from_state`, `reason_message`, `evidence_json`, and `next_action`.
    // A new episode has to be recorded and this row is where it lives, so
    // "never touched" would overstate it.
    //
    // Our own live lease — owner, token, expiry — is kept so a running operation
    // still blocks the episode from being handed to a second one. The token can no
    // longer satisfy any fence, because `ON CONFLICT` rewrites `quarantine_id` and
    // every fence pins it; what it still does is hold `claim_is_live` true until
    // the lease lapses, which is what keeps two operations off one sandbox while
    // the first is mid-transfer.
    //
    // The pacing — attempt count, retry time, last error — is reset whenever the
    // row is ours, because a new `quarantine_id` means the sandbox left quarantine
    // and came back. A genuinely new episode should not wait out the previous
    // episode's backoff or report its error. That differs from a reconfirmation of
    // the same episode, where the pacing is inherited on purpose.
    let retry_state_reset = match (legacy_owned, claim_dead) {
        (false, _) => {
            "next_action_after = next_action_after,
             next_action_owner = next_action_owner,
             attempt_count = attempt_count,
             claim_token = claim_token,
             claim_expires_at = claim_expires_at,
             last_error = last_error"
        }
        (true, false) => {
            "next_action_after = NULL,
             next_action_owner = next_action_owner,
             attempt_count = 0,
             claim_token = claim_token,
             claim_expires_at = claim_expires_at,
             last_error = NULL"
        }
        (true, true) => {
            "next_action_after = NULL,
             next_action_owner = NULL,
             attempt_count = 0,
             claim_token = NULL,
             claim_expires_at = NULL,
             last_error = NULL"
        }
    };
    conn.execute(
        &format!(
            "INSERT INTO sandbox_quarantine_state (
                 sandbox_id, quarantine_id, entered_at, updated_at,
                 entered_from_state, source, reason_code, reason_message,
                 evidence_json, next_action
             )
             VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, ?7, '{{}}', ?8)
             ON CONFLICT(sandbox_id) DO UPDATE SET
                 quarantine_id = excluded.quarantine_id,
                 entered_at = excluded.entered_at,
                 updated_at = excluded.updated_at,
                 entered_from_state = excluded.entered_from_state,
                 source = excluded.source,
                 reason_code = excluded.reason_code,
                 reason_message = excluded.reason_message,
                 evidence_json = excluded.evidence_json,
                 next_action = excluded.next_action,
                 {retry_state_reset}"
        ),
        params![
            episode.sandbox_id,
            quarantine_id,
            episode.timestamp,
            episode.entered_from_state,
            episode.source.as_str(),
            episode.reason_code.as_str(),
            episode.reason_message,
            episode.next_action.as_str()
        ],
    )?;
    insert_quarantine_history(
        conn,
        episode,
        &quarantine_id,
        SandboxQuarantineHistoryAction::EnteredQuarantine,
    )
}

pub(crate) fn insert_quarantined_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<(), MarkQuarantinedError> {
    if params.state != SandboxState::Quarantined {
        return Err(MarkQuarantinedError::NonQuarantinedInsert {
            state: params.state,
        });
    }
    let sandbox_id = params.id.to_string();
    let reason_message = reason_message.trim();
    let reason_message = (!reason_message.is_empty()).then_some(reason_message);
    let timestamp = now()?;
    insert_sandbox(conn, params)?;
    conn.execute(
        "UPDATE sandboxes SET failure_reason = ?1 WHERE id = ?2",
        params![reason_message, sandbox_id],
    )?;
    insert_new_quarantine_episode(
        conn,
        &QuarantineEpisode {
            sandbox_id: &sandbox_id,
            entered_from_state: None,
            source,
            reason_code,
            reason_message,
            next_action,
            timestamp: &timestamp,
        },
    )?;
    Ok(())
}

pub(crate) fn emit_insert_quarantined_sandbox_event(
    sandbox_id: &str,
    reason_code: SandboxQuarantineReasonCode,
) {
    emit_sandbox_quarantined_event(
        sandbox_id,
        None,
        reason_code,
        "insert_quarantined_sandbox",
        None,
    );
}

#[cfg(test)]
pub fn mark_quarantined(
    conn: &Connection,
    id: &str,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<(), MarkQuarantinedError> {
    mark_quarantined_inner(
        conn,
        id,
        None,
        source,
        reason_code,
        reason_message,
        next_action,
    )?;
    Ok(())
}

pub fn current_quarantine_reason_wire_value(conn: &Connection, id: &str) -> Result<Option<String>> {
    let raw = conn
        .query_row(
            "SELECT reason_code FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            params![id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    raw.map(|raw| {
        let value = raw.trim();
        if value.is_empty() {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "quarantine reason code is blank",
                )),
            ));
        }
        Ok(value.to_string())
    })
    .transpose()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PauseFailedRecoveryTarget {
    Started,
    Paused,
}

impl PauseFailedRecoveryTarget {
    pub(crate) const fn sandbox_state(self) -> SandboxState {
        match self {
            Self::Started => SandboxState::Started,
            Self::Paused => SandboxState::Paused,
        }
    }

    pub(super) const fn history_action(self) -> SandboxQuarantineHistoryAction {
        match self {
            Self::Started => SandboxQuarantineHistoryAction::RecoveredToStarted,
            Self::Paused => SandboxQuarantineHistoryAction::RecoveredToPaused,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PauseFailedRecoveryError {
    #[error("pause-failed recovery database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("pause-failed sandbox state CAS affected {changed} rows for {sandbox_id}")]
    SandboxStateInvariant {
        sandbox_id: SandboxId,
        changed: usize,
    },
    #[error("pause-failed recovery history CAS affected {changed} rows for {sandbox_id}")]
    HistoryInvariant {
        sandbox_id: SandboxId,
        changed: usize,
    },
    #[error("pause-failed quarantine retirement CAS affected {changed} rows for {sandbox_id}")]
    RetirementInvariant {
        sandbox_id: SandboxId,
        changed: usize,
    },
}

/// Recover an sbx-manager pause failure after the caller establishes
/// definitive runtime state.
pub(crate) fn recover_pause_failed_quarantine(
    conn: &Connection,
    id: &SandboxId,
    target: PauseFailedRecoveryTarget,
) -> std::result::Result<bool, PauseFailedRecoveryError> {
    let sandbox_id = id.to_string();
    let source = SandboxQuarantineSource::SbxManager.as_str();
    let reason = SandboxQuarantineReasonCode::PauseFailed.as_str();
    let tx = immediate_transaction(conn)?;
    let Some(quarantine_id): Option<String> = tx
        .query_row(
            "SELECT qs.quarantine_id
               FROM sandboxes s
               JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
              WHERE s.id = ?1
                AND s.state = 'quarantined'
                AND qs.source = ?2
                AND qs.reason_code = ?3",
            params![&sandbox_id, source, reason],
            |row| row.get(0),
        )
        .optional()?
    else {
        tx.commit()?;
        return Ok(false);
    };
    let target_state = target.sandbox_state();
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = ?1,
                failure_reason = NULL,
                state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
                updated_at = ?2
          WHERE id = ?3
            AND state = 'quarantined'
            AND EXISTS (
                SELECT 1
                  FROM sandbox_quarantine_state
                 WHERE sandbox_id = ?3
                   AND quarantine_id = ?4
                   AND source = ?5
                   AND reason_code = ?6
            )",
        params![
            target_state.as_str(),
            now()?,
            &sandbox_id,
            &quarantine_id,
            source,
            reason
        ],
    )?;
    if changed != 1 {
        return Err(PauseFailedRecoveryError::SandboxStateInvariant {
            sandbox_id: *id,
            changed,
        });
    }
    let recorded = tx.execute(
        "INSERT INTO sandbox_quarantine_history (
             id, quarantine_id, sandbox_id, occurred_at, source, action,
             reason_code, message, next_action, evidence_json
         )
         SELECT ?1, quarantine_id, sandbox_id, ?2, source, ?3,
                reason_code, 'runtime state became definitive after pause failure',
                NULL, evidence_json
           FROM sandbox_quarantine_state
          WHERE sandbox_id = ?4
            AND quarantine_id = ?5
            AND source = ?6
            AND reason_code = ?7",
        params![
            uuid::Uuid::now_v7().to_string(),
            now()?,
            target.history_action().as_str(),
            &sandbox_id,
            &quarantine_id,
            source,
            reason
        ],
    )?;
    if recorded != 1 {
        return Err(PauseFailedRecoveryError::HistoryInvariant {
            sandbox_id: *id,
            changed: recorded,
        });
    }
    let cleared = tx.execute(
        "DELETE FROM sandbox_quarantine_state
          WHERE sandbox_id = ?1
            AND quarantine_id = ?2
            AND source = ?3
            AND reason_code = ?4",
        params![&sandbox_id, &quarantine_id, source, reason],
    )?;
    if cleared != 1 {
        return Err(PauseFailedRecoveryError::RetirementInvariant {
            sandbox_id: *id,
            changed: cleared,
        });
    }
    tx.commit()?;
    emit_sandbox_state_event(
        &sandbox_id,
        SandboxEventType::QuarantineRecovered,
        Some(SandboxState::Quarantined.as_str()),
        reason,
        "recover_pause_failed_quarantine",
        target_state,
        None,
    );
    Ok(true)
}

/// Load optional quarantine or transition-receipt metadata for inventory
/// publication. Wire degradation for malformed rows happens in the State RPC,
/// so this storage helper exposes the row vocabulary without deciding whether
/// another sandbox may still be published.
pub fn sandbox_state_publication_metadata(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<BTreeMap<String, RawSandboxStatePublicationMetadataRead>> {
    let mut statement = conn.prepare_cached(
        "SELECT s.id, s.state, qs.reason_code,
                s.transition_receipt_id,
                s.transition_receipt_kind,
                s.transition_receipt_target_state,
                s.transition_receipt_execution_generation,
                s.transition_receipt_reason_code,
                s.transition_receipt_activation_attempt_id
           FROM sandboxes s
           LEFT JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
          WHERE s.id = ?1
            AND (s.state = 'quarantined'
                 OR s.transition_receipt_id IS NOT NULL)",
    )?;
    let mut publications = BTreeMap::new();
    for sandbox_id in sandbox_ids {
        let publication = statement
            .query_row(params![sandbox_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    RawSandboxStatePublicationMetadata {
                        quarantine_reason_code: row.get(2)?,
                        transition_receipt_id: row.get(3)?,
                        transition_receipt_kind: row.get(4)?,
                        transition_receipt_target_state: row.get(5)?,
                        transition_receipt_execution_generation: row.get(6)?,
                        transition_receipt_reason_code: row.get(7)?,
                        transition_receipt_activation_attempt_id: row.get(8)?,
                    },
                ))
            })
            .optional();
        match publication {
            Ok(Some((id, publication))) => {
                publications.insert(id, Ok(publication));
            }
            Ok(None) => {}
            Err(error) => {
                publications.insert(sandbox_id.clone(), Err(error));
            }
        }
    }
    Ok(publications)
}

pub(crate) fn mark_quarantined_from_state(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_inner(
        conn,
        id,
        Some(expected_state),
        source,
        reason_code,
        reason_message,
        next_action,
    )
}

fn mark_quarantined_inner(
    conn: &Connection,
    id: &str,
    expected_state: Option<SandboxState>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<bool, MarkQuarantinedError> {
    let reason_message = reason_message.trim();
    let reason_message = (!reason_message.is_empty()).then_some(reason_message);
    let previous_state = current_state_label(conn, id)?;
    if expected_state.is_some_and(|expected| {
        previous_state
            .as_deref()
            .and_then(|state| state.parse::<SandboxState>().ok())
            != Some(expected)
    }) {
        return Ok(false);
    }
    if let Some(previous_state_label) = previous_state.as_deref() {
        let previous_state_value = previous_state_label.parse::<SandboxState>().map_err(|_| {
            MarkQuarantinedError::UnknownCurrentState {
                state: previous_state_label.to_string(),
            }
        })?;
        previous_state_value.transition(SandboxState::Quarantined)?;
    }
    let starts_new_episode = previous_state.as_deref() != Some(SandboxState::Quarantined.as_str());
    let ts = now()?;

    let tx = conn.unchecked_transaction()?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'quarantined',
             failure_reason = ?1,
             suspend_predecessor_artifact_id = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             transition_receipt_id = NULL,
             transition_receipt_kind = NULL,
             transition_receipt_target_state = NULL,
             transition_receipt_execution_generation = NULL,
             transition_receipt_activation_attempt_id = NULL,
             transition_receipt_reason_code = NULL,
             state_changed_at = CASE WHEN state != 'quarantined' THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3 AND (?4 IS NULL OR state = ?4)",
        params![reason_message, ts, id, previous_state.as_deref()],
    )?;
    if changed == 0 {
        tracing::warn!(
            sandbox_id = %id,
            expected_state = previous_state.as_deref().unwrap_or("<missing>"),
            source = %source,
            reason_code = %reason_code,
            "ignored mark_quarantined because sandbox state changed before the CAS write"
        );
    }
    if changed != 0 {
        crate::route_activation_claims::close_claim(&tx, id)?;
        let episode = QuarantineEpisode {
            sandbox_id: id,
            entered_from_state: previous_state.as_deref(),
            source,
            reason_code,
            reason_message,
            next_action,
            timestamp: &ts,
        };
        if starts_new_episode {
            insert_new_quarantine_episode(&tx, &episode)?;
        } else {
            // A reconfirmation that changes source or reason invalidates the
            // retry state legacy recovery accumulated for the old reason — but
            // only once nothing is acting on it. Clearing a live lease would
            // make the episode look due and unclaimed while its claimed
            // operation is still running, so the next tick would start a second
            // one. A newer release's owner keeps its retry and lease columns, which
            // is the whole of its claim. This branch still rewrites `source`,
            // `reason_code`, `reason_message` and `evidence_json` on a
            // foreign-owned row — identity columns a future owner's own fence
            // would pin — because the new classification has to be recorded and
            // this row is where it lives.
            //
            // Nor does a later tick finish the job: `legacy_owned` is `owner IS
            // NULL OR owner = 'legacy_recovery'`, and a foreign owner string does
            // not become NULL when its lease lapses. The retry state stays until
            // that owner clears it — by design, not by deferral.
            // Three facts, not one: whether the classification actually changed,
            // whether legacy recovery still owns the row, and whether a lease is
            // live. The lease decides only whether the *lease* fields may be
            // cleared; the retry bookkeeping is invalidated by the change itself.
            let current_epoch = now_epoch_i64()?;
            let (classification_changed, legacy_owned, claim_live): (bool, bool, bool) = tx
                .query_row(
                    "SELECT (source != ?2 OR reason_code != ?3),
                        (next_action_owner IS NULL OR next_action_owner = ?4),
                        claim_expires_at
                   FROM sandbox_quarantine_state
                  WHERE sandbox_id = ?1",
                    params![
                        id,
                        source.as_str(),
                        reason_code.as_str(),
                        QuarantineRecoveryOwner::LegacyRecovery.as_str(),
                    ],
                    |row| {
                        let expiry: rusqlite::types::Value = row.get(2)?;
                        // Unknown expiry is not evidence that a previous writer
                        // stopped. Keep its lease until explicitly repaired.
                        let claim_live = !matches!(expiry, rusqlite::types::Value::Null)
                            && timestamps::EpochSeconds::decode(&expiry)
                                .map_or(true, |value| value.get() > current_epoch);
                        Ok((row.get(0)?, row.get(1)?, claim_live))
                    },
                )?;
            let reset_legacy_recovery_state = classification_changed && legacy_owned && !claim_live;
            // Under a live lease the lease itself is kept and the attempt count with
            // it, but the retry *time* is cleared. Three separate reasons, because
            // an earlier revision got each of them wrong in turn:
            //
            // The lease stays because clearing it would make the episode look due
            // and unclaimed while its operation is still doing physical work, so
            // the next tick would start a second one on the same sandbox.
            //
            // The attempt count stays because a sandbox whose classification keeps
            // changing would otherwise reset its curve on every reconfirmation and
            // land attempts one tick apart indefinitely — the cadence this exists
            // to bound. It is not kept because `ExactClaimFence` pins it: this
            // statement rewrites `source` and `reason_code`, which the fence pins
            // too, so the running operation's writes already miss regardless.
            // `quarantine_recovery_reason_change_preserves_a_live_claim` asserts
            // exactly that.
            //
            // The retry time is cleared because it was computed as claim expiry
            // plus a backoff, for a classification that no longer applies. Left in
            // place it strands the episode for the lease *and* that backoff — up to
            // half an hour — when the operation holding the lease can no longer
            // commit anything. Clearing it bounds the wait by the lease alone, and
            // the next attempt then recomputes the backoff from the preserved count.
            if reset_legacy_recovery_state {
                tx.execute(
                    "UPDATE sandbox_quarantine_state
                     SET updated_at = ?2,
                         next_action_after = NULL,
                         next_action_owner = NULL,
                         attempt_count = 0,
                         claim_token = NULL,
                         claim_expires_at = NULL,
                         last_error = NULL,
                         source = ?3,
                         reason_code = ?4,
                         reason_message = ?5,
                         evidence_json = '{}'
                     WHERE sandbox_id = ?1",
                    params![
                        id,
                        ts,
                        source.as_str(),
                        reason_code.as_str(),
                        reason_message
                    ],
                )?;
            } else if classification_changed && legacy_owned {
                tx.execute(
                    "UPDATE sandbox_quarantine_state
                     SET updated_at = ?2,
                         next_action_after = NULL,
                         last_error = NULL,
                         source = ?3,
                         reason_code = ?4,
                         reason_message = ?5,
                         evidence_json = '{}'
                     WHERE sandbox_id = ?1",
                    params![
                        id,
                        ts,
                        source.as_str(),
                        reason_code.as_str(),
                        reason_message
                    ],
                )?;
            } else {
                tx.execute(
                    "UPDATE sandbox_quarantine_state
                     SET updated_at = ?2,
                         source = ?3,
                         reason_code = ?4,
                         reason_message = ?5,
                         evidence_json = '{}'
                     WHERE sandbox_id = ?1",
                    params![
                        id,
                        ts,
                        source.as_str(),
                        reason_code.as_str(),
                        reason_message
                    ],
                )?;
            }
            let current_quarantine_id: String = tx.query_row(
                "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                params![id],
                |row| row.get(0),
            )?;
            insert_quarantine_history(
                &tx,
                &episode,
                &current_quarantine_id,
                SandboxQuarantineHistoryAction::ReconfirmedQuarantine,
            )?;
        }
    }
    tx.commit()?;

    if changed != 0 && previous_state.as_deref() != Some(SandboxState::Quarantined.as_str()) {
        emit_sandbox_quarantined_event(
            id,
            previous_state.as_deref(),
            reason_code,
            "mark_quarantined",
            None,
        );
    }
    Ok(changed != 0)
}
