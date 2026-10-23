use super::{
    IdempotencyKey, MAX_IDEMPOTENCY_KEY_BYTES, MAX_PROCESS_COMMAND_ARGS, MAX_PROCESS_COMMAND_BYTES,
    ProcessCommand, ProcessParseError, ProcessSignal,
};

#[test]
fn process_command_bounds_retained_argument_metadata() {
    ProcessCommand::new(vec![String::new(); MAX_PROCESS_COMMAND_ARGS + 1]).unwrap_err();
    ProcessCommand::new(vec!["x".repeat(MAX_PROCESS_COMMAND_BYTES)]).unwrap_err();
    ProcessCommand::new(vec!["x".repeat(MAX_PROCESS_COMMAND_BYTES - 1)]).unwrap();
}

#[test]
fn process_signal_rejects_values_outside_unix_range() {
    ProcessSignal::try_from_i32(1).unwrap();
    ProcessSignal::try_from_i32(64).unwrap();
    ProcessSignal::try_from_i32(0).unwrap_err();
    ProcessSignal::try_from_i32(-1).unwrap_err();
    ProcessSignal::try_from_i32(65).unwrap_err();
    ProcessSignal::try_from_i32(i32::MAX).unwrap_err();
}

#[test]
fn idempotency_key_uses_endpoint_owned_length_cap() {
    let key = IdempotencyKey::parse("request-1").unwrap();
    assert_eq!(
        key.clone().ensure_len_at_most(8),
        Err(ProcessParseError("idempotency key is too long"))
    );
    assert_eq!(key.clone().ensure_len_at_most(9), Ok(key));
    IdempotencyKey::parse("x".repeat(4097)).unwrap();
    IdempotencyKey::parse("x".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1)).unwrap_err();
}
