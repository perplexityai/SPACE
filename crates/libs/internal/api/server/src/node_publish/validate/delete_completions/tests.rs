use super::*;
use pplx_space_common_core::types::AutoLifecyclePolicy;

#[test]
fn receipt_rejection_precedence_preserves_acknowledgement_and_forward_compatibility() {
    let valid = AutoLifecycleDeleteCompletion {
        receipt_id: uuid::Uuid::from_u128(1).to_string(),
        sandbox_id: uuid::Uuid::from_u128(2).to_string(),
        execution_generation: 1,
        route_activation_id: uuid::Uuid::from_u128(3).to_string(),
        auto_lifecycle_policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(
            &AutoLifecyclePolicy {
                wake: Default::default(),
                condition_sets: Default::default(),
                stages: Vec::new(),
            },
        )
        .to_string(),
    };
    let mut invalid = AutoLifecycleDeleteCompletion {
        receipt_id: "invalid".to_owned(),
        sandbox_id: "invalid".to_owned(),
        execution_generation: -1,
        route_activation_id: "invalid".to_owned(),
        auto_lifecycle_policy_fingerprint: "invalid".to_owned(),
    };
    for step in 0..5 {
        let error = DeleteCompletionClaim::try_from(&invalid).unwrap_err();
        let rejected_field = match error {
            CompletionValidationError::ReceiptId(_) => 0,
            CompletionValidationError::SandboxId(_) => 1,
            CompletionValidationError::ExecutionGeneration(_) => 2,
            CompletionValidationError::RouteActivationId(_) => 3,
            CompletionValidationError::PolicyFingerprint(_) => 4,
        };
        assert_eq!(rejected_field, step);
        let prepared = ValidatedDeleteCompletions::from_proto(&[invalid.clone()]);
        assert!(prepared.selected.is_empty());
        if step == 0 {
            assert!(prepared.acknowledged_receipt_ids.is_empty());
        } else {
            assert_eq!(
                prepared.acknowledged_receipt_ids,
                [valid
                    .receipt_id
                    .parse::<AutoLifecycleDeleteReceiptId>()
                    .unwrap()]
            );
        }
        match step {
            0 => invalid.receipt_id.clone_from(&valid.receipt_id),
            1 => invalid.sandbox_id.clone_from(&valid.sandbox_id),
            2 => invalid.execution_generation = valid.execution_generation,
            3 => invalid
                .route_activation_id
                .clone_from(&valid.route_activation_id),
            4 => invalid
                .auto_lifecycle_policy_fingerprint
                .clone_from(&valid.auto_lifecycle_policy_fingerprint),
            _ => unreachable!(),
        }
    }
    DeleteCompletionClaim::try_from(&invalid).unwrap();
    invalid.auto_lifecycle_policy_fingerprint = valid
        .auto_lifecycle_policy_fingerprint
        .replace("v1:", "v2:");
    let prepared = ValidatedDeleteCompletions::from_proto(&[invalid]);
    assert!(prepared.selected.is_empty());
    assert!(prepared.acknowledged_receipt_ids.is_empty());
    let prepared = ValidatedDeleteCompletions::from_proto(&[valid]);
    assert_eq!(prepared.selected.len(), 1);
    assert!(prepared.acknowledged_receipt_ids.is_empty());
}
