use super::*;
use crate::proto::operations::RecoveryHandle;

#[test]
fn mutation_sequences_require_the_version_that_enforces_them() {
    for (version, sequence, expected) in [
        (1, None, None),
        (1, Some(1), Some(tonic::Code::InvalidArgument)),
        (2, None, Some(tonic::Code::InvalidArgument)),
        (2, Some(0), Some(tonic::Code::InvalidArgument)),
        (2, Some(1), None),
        (3, Some(1), Some(tonic::Code::FailedPrecondition)),
    ] {
        let handle = RecoveryHandle {
            contract_version: version,
            sequence,
            expected_instance_id: ProcessRecoveryToken::new().to_string(),
            expected_activation_id: ProcessRecoveryToken::new().to_string(),
            process_id: "process".into(),
            process_incarnation: ProcessRecoveryToken::new().to_string(),
        };
        let parsed = validate_handle("", Some(&handle));
        assert_eq!(parsed.as_ref().err().map(Status::code), expected);
        if let Ok(Some(parsed)) = parsed {
            assert_eq!(
                matches!(parsed.contract, ControlContract::Sequenced(_)),
                version == 2
            );
        }
    }
}
