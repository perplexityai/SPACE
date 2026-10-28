use super::*;
use pplx_space_sbx_mgr_client::proto::TransitionReceipt;

fn receipt() -> TransitionReceipt {
    TransitionReceipt {
        id: "01990000-0000-7000-8000-000000000001".to_string(),
        kind: "activation_completed".to_string(),
        target_state: "started".to_string(),
        execution_generation: 1,
        reason_code: String::new(),
        activation_attempt_id: Some("01990000-0000-7000-8000-000000000002".to_string()),
    }
}

fn state(receipt: TransitionReceipt) -> PublishedSandboxState {
    PublishedSandboxState {
        status: Some(receipt.target_state.clone()),
        transition_receipt: Some(receipt),
        ..PublishedSandboxState::default()
    }
}

#[test]
fn malformed_receipts_are_rejected_at_the_original_field() {
    type Corruption = fn(&mut TransitionReceipt);
    let cases: &[(Field, Corruption)] = &[
        (Field::TransitionReceiptId, |receipt| receipt.id.clear()),
        (Field::TransitionReceiptId, |receipt| {
            receipt.id = "invalid".to_string()
        }),
        (Field::TransitionReceiptKind, |receipt| receipt.kind.clear()),
        (Field::TransitionReceiptKind, |receipt| {
            receipt.kind = "future".to_string()
        }),
        (Field::TransitionReceiptTargetState, |receipt| {
            receipt.target_state.clear()
        }),
        (Field::TransitionReceiptTargetState, |receipt| {
            receipt.target_state = "future".to_string()
        }),
        (Field::TransitionReceiptExecutionGeneration, |receipt| {
            receipt.execution_generation = 0
        }),
        (Field::TransitionReceiptActivationAttemptId, |receipt| {
            receipt.activation_attempt_id = None
        }),
        (Field::TransitionReceiptActivationAttemptId, |receipt| {
            receipt.activation_attempt_id = Some("invalid".to_string())
        }),
        (Field::TransitionReceiptTargetState, |receipt| {
            receipt.target_state = "stopped".to_string()
        }),
        (Field::TransitionReceiptReasonCode, |receipt| {
            receipt.kind = "activation_rollback".to_string();
            receipt.target_state = "stopped".to_string();
        }),
        (Field::TransitionReceiptReasonCode, |receipt| {
            receipt.kind = "quarantine_recovery".to_string();
            receipt.reason_code = "future".to_string();
        }),
        (Field::TransitionReceiptKind, |receipt| {
            receipt.kind = "quarantine_recovery".to_string();
            receipt.target_state = "suspended".to_string();
            receipt.reason_code = "pause_failed".to_string();
        }),
        (Field::TransitionReceiptTargetState, |receipt| {
            receipt.kind = "quarantine_recovery".to_string();
            receipt.reason_code = "restore_prepare_failed".to_string();
        }),
        (Field::TransitionReceiptTargetState, |receipt| {
            receipt.kind = "activation_rollback".to_string();
            receipt.reason_code = "cold_activation_interrupted".to_string();
        }),
    ];
    for (field, corrupt) in cases {
        let mut receipt = receipt();
        corrupt(&mut receipt);
        let error = parse(&state(receipt)).receipt.unwrap_err();
        assert_eq!(error.field.to_string(), field.to_string());
        assert!(!error.message.is_empty());
    }
    let mut mismatched = state(receipt());
    mismatched.status = Some(" stopped ".to_string());
    let error = parse(&mismatched).receipt.unwrap_err();
    assert_eq!(
        error.field.to_string(),
        Field::TransitionReceiptTargetState.to_string()
    );
    assert_eq!(
        error.message,
        "receipt target started disagrees with status \"stopped\""
    );
}

#[test]
fn future_rollback_reasons_use_only_the_reviewed_target_specific_fallbacks() {
    let cases = [
        (
            SandboxTransitionTargetState::Created,
            Some(SandboxQuarantineReasonCode::CreateInterrupted),
        ),
        (
            SandboxTransitionTargetState::Stopped,
            Some(SandboxQuarantineReasonCode::ColdActivationInterrupted),
        ),
        (
            SandboxTransitionTargetState::Suspended,
            Some(SandboxQuarantineReasonCode::RestoreActivationInterrupted),
        ),
        (SandboxTransitionTargetState::Started, None),
        (SandboxTransitionTargetState::Paused, None),
    ];
    for (target, expected) in cases {
        let receipt = TransitionReceipt {
            kind: "activation_rollback".to_string(),
            target_state: target.to_string(),
            reason_code: "future_reason".to_string(),
            ..receipt()
        };
        let mut state = state(receipt);
        state.status = Some(format!(" {} ", target.as_str().to_ascii_uppercase()));
        let parsed = parse(&state);
        match expected {
            Some(reason) => {
                let receipt = parsed.receipt.unwrap().unwrap();
                assert_eq!(receipt.reason_code(), Some(&reason));
                assert_eq!(receipt.target_state(), target);
            }
            None => {
                let error = parsed.receipt.unwrap_err();
                assert_eq!(
                    error.field.to_string(),
                    Field::TransitionReceiptTargetState.to_string()
                );
                assert_eq!(
                    error.message,
                    format!("activation rollback cannot target {target}")
                );
            }
        }
    }
}

#[test]
fn absent_receipts_and_activation_success_keep_legacy_reason_handling() {
    assert!(
        parse(&PublishedSandboxState::default())
            .receipt
            .unwrap()
            .is_none()
    );
    let receipt = TransitionReceipt {
        reason_code: "future_reason".to_string(),
        ..receipt()
    };
    let parsed = parse(&state(receipt)).receipt.unwrap().unwrap();
    assert_eq!(
        parsed.kind(),
        SandboxTransitionReceiptKind::ActivationCompleted
    );
    assert!(parsed.reason_code().is_none());
}
