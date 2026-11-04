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
