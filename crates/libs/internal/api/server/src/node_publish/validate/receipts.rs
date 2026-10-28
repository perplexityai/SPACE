use pplx_space_common_core::types::{
    ExecutionGeneration, SandboxQuarantineReasonCode, SandboxTransitionReceipt,
    SandboxTransitionReceiptInvariantError, SandboxTransitionReceiptKind,
    SandboxTransitionTargetState,
};
use pplx_space_metrics::sandbox::publication::StatePublicationField as Field;

use crate::optional_trimmed;
use crate::proto::PublishedSandboxState;

#[derive(Debug, thiserror::Error)]
#[error("invalid {field}: {message}")]
pub(crate) struct PublishedTransitionReceiptError {
    pub(crate) field: Field,
    message: String,
}

#[derive(Debug)]
pub(crate) struct RollbackReasonFallback {
    pub(crate) raw_reason: String,
    pub(crate) error: String,
    pub(crate) reason: SandboxQuarantineReasonCode,
}

#[derive(Debug)]
pub(crate) struct ReceiptValidation {
    pub(crate) receipt: Result<
        Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
        PublishedTransitionReceiptError,
    >,
    pub(crate) fallback: Option<RollbackReasonFallback>,
}

pub(crate) fn parse(state: &PublishedSandboxState) -> ReceiptValidation {
    let mut fallback = None;
    let receipt = parse_receipt(state, &mut fallback);
    // Keep a discovered fallback even if a later field rejects the receipt.
    // The caller owns diagnostics; parsing has no metrics or logging side effects.
    ReceiptValidation { receipt, fallback }
}

fn parse_receipt(
    state: &PublishedSandboxState,
    fallback: &mut Option<RollbackReasonFallback>,
) -> Result<
    Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    PublishedTransitionReceiptError,
> {
    let Some(receipt) = state.transition_receipt.as_ref() else {
        return Ok(None);
    };
    let required = |value: Option<&str>, field| {
        optional_trimmed(value).ok_or_else(|| PublishedTransitionReceiptError {
            field,
            message: "required receipt field is missing or blank".to_string(),
        })
    };
    let id = required(Some(&receipt.id), Field::TransitionReceiptId)?
        .parse()
        .map_err(|error: uuid::Error| PublishedTransitionReceiptError {
            field: Field::TransitionReceiptId,
            message: error.to_string(),
        })?;
    let kind = required(Some(&receipt.kind), Field::TransitionReceiptKind)?
        .parse()
        .map_err(
            |error: pplx_space_common_core::types::SandboxTransitionReceiptKindParseError| {
                PublishedTransitionReceiptError {
                    field: Field::TransitionReceiptKind,
                    message: error.to_string(),
                }
            },
        )?;
    let target_state: SandboxTransitionTargetState = required(
        Some(&receipt.target_state),
        Field::TransitionReceiptTargetState,
    )?
    .parse()
    .map_err(
        |error: pplx_space_common_core::types::SandboxTransitionTargetStateParseError| {
            PublishedTransitionReceiptError {
                field: Field::TransitionReceiptTargetState,
                message: error.to_string(),
            }
        },
    )?;
    let execution_generation =
        ExecutionGeneration::new(receipt.execution_generation).map_err(|error| {
            PublishedTransitionReceiptError {
                field: Field::TransitionReceiptExecutionGeneration,
                message: error.to_string(),
            }
        })?;
    let reason_code = parse_reason(kind, target_state, &receipt.reason_code, fallback)?;
    let status = optional_trimmed(state.status.as_deref()).unwrap_or_default();
    if !status.eq_ignore_ascii_case(target_state.as_str()) {
        return Err(PublishedTransitionReceiptError {
            field: Field::TransitionReceiptTargetState,
            message: format!("receipt target {target_state} disagrees with status {status:?}"),
        });
    }
    if kind == SandboxTransitionReceiptKind::QuarantineRecovery
        && reason_code
            .as_ref()
            .is_some_and(|reason| reason.automatic_recovery_policy().is_none())
    {
        return Err(PublishedTransitionReceiptError {
            field: Field::TransitionReceiptKind,
            message: "quarantine recovery requires a reviewed pre-resume reason".to_string(),
        });
    }
    let activation_attempt_id = receipt
        .activation_attempt_id
        .as_deref()
        .map(|value| {
            value
                .trim()
                .parse()
                .map_err(|error: uuid::Error| PublishedTransitionReceiptError {
                    field: Field::TransitionReceiptActivationAttemptId,
                    message: error.to_string(),
                })
        })
        .transpose()?;
    let receipt = SandboxTransitionReceipt::try_new(
        id,
        kind,
        target_state,
        execution_generation,
        reason_code,
        activation_attempt_id,
    )
    .map_err(|error| PublishedTransitionReceiptError {
        field: match error {
            SandboxTransitionReceiptInvariantError::MissingActivationAttempt => {
                Field::TransitionReceiptActivationAttemptId
            }
            SandboxTransitionReceiptInvariantError::MissingReasonCode
            | SandboxTransitionReceiptInvariantError::UnexpectedReasonCode => {
                Field::TransitionReceiptReasonCode
            }
            SandboxTransitionReceiptInvariantError::InvalidActivationCompletionTarget {
                ..
            }
            | SandboxTransitionReceiptInvariantError::InvalidActivationRollbackTarget { .. }
            | SandboxTransitionReceiptInvariantError::InvalidQuarantineRecoveryTarget { .. } => {
                Field::TransitionReceiptTargetState
            }
        },
        message: error.to_string(),
    })?;
    Ok(Some(receipt))
}

fn parse_reason(
    kind: SandboxTransitionReceiptKind,
    target_state: SandboxTransitionTargetState,
    reason: &str,
    diagnostic: &mut Option<RollbackReasonFallback>,
) -> Result<Option<SandboxQuarantineReasonCode>, PublishedTransitionReceiptError> {
    if kind == SandboxTransitionReceiptKind::ActivationCompleted {
        return Ok(None);
    }
    optional_trimmed(Some(reason))
        .map(
            |reason_value| match reason_value.parse::<SandboxQuarantineReasonCode>() {
                Ok(reason_code) => Ok(reason_code),
                Err(error) if kind == SandboxTransitionReceiptKind::ActivationRollback => {
                    let fallback = match target_state {
                        SandboxTransitionTargetState::Created => {
                            SandboxQuarantineReasonCode::CreateInterrupted
                        }
                        SandboxTransitionTargetState::Stopped => {
                            SandboxQuarantineReasonCode::ColdActivationInterrupted
                        }
                        SandboxTransitionTargetState::Suspended => {
                            SandboxQuarantineReasonCode::RestoreActivationInterrupted
                        }
                        SandboxTransitionTargetState::Started
                        | SandboxTransitionTargetState::Paused => {
                            return Err(PublishedTransitionReceiptError {
                                field: Field::TransitionReceiptTargetState,
                                message: format!(
                                    "activation rollback cannot target {target_state}"
                                ),
                            });
                        }
                    };
                    *diagnostic = Some(RollbackReasonFallback {
                        raw_reason: reason_value,
                        error: error.to_string(),
                        reason: fallback,
                    });
                    Ok(fallback)
                }
                Err(error) => Err(PublishedTransitionReceiptError {
                    field: Field::TransitionReceiptReasonCode,
                    message: error.to_string(),
                }),
            },
        )
        .transpose()
}

#[cfg(test)]
mod tests;
