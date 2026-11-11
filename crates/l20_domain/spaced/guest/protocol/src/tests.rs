use super::*;

#[test]
fn process_stdin_debug_output_is_redacted() {
    let request = proto::operations::SendInputRequest {
        process_id: "process-id".to_string(),
        data: b"sensitive process input".to_vec(),
    };

    let debug = format!("{request:?}");

    assert!(debug.contains("data: \"<redacted>\""));
    assert!(!debug.contains("sensitive process input"));
}

#[test]
fn process_stdin_zeroizes_on_drop() {
    fn assert_zeroize_on_drop<T: zeroize::ZeroizeOnDrop>() {}

    assert_zeroize_on_drop::<proto::operations::SendInputRequest>();
}

#[test]
fn configuration_owner_is_reserved_and_only_same_execution_can_take_over() {
    use update::HoldOwner;
    let execution = uuid::Uuid::now_v7();
    let config = HoldOwner::try_from(&HoldOwner::configuration(execution)).unwrap();
    assert!(HoldOwner::validate_operation(execution, execution).is_err());
    let operation = HoldOwner {
        operation: uuid::Uuid::now_v7(),
        execution,
    };
    HoldOwner::validate_operation(operation.operation, execution).unwrap();
    assert!(operation.may_replace(&config));
    assert!(operation.may_replace(&operation));
    assert!(!config.may_replace(&operation));
    let other = HoldOwner {
        operation: uuid::Uuid::now_v7(),
        execution: uuid::Uuid::now_v7(),
    };
    assert!(!other.may_replace(&config));
    assert!(!other.may_replace(&operation));
}
