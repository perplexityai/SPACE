use super::*;
use pplx_space_spaced_guest_protocol::recovery::RecoveryReason;

#[tokio::test]
async fn interrupted_input_reports_the_written_prefix_without_resending() {
    let (writer, mut reader) = tokio::io::duplex(4);
    let input = InputDescriptor::new(Some(writer));
    let shutdown = CancellationToken::new();
    let mut writing = Box::pin(write_descriptor(&input, b"abcdefgh", &shutdown));
    assert!(futures_util::poll!(&mut writing).is_pending());
    shutdown.cancel();
    let error = writing.await.result().unwrap_err();
    assert_eq!(
        RecoveryReason::from_status(&error),
        Some(RecoveryReason::InputOutcomeUncertain)
    );
    assert_eq!(
        error.metadata().get("space-process-input-written").unwrap(),
        "4"
    );
    input.release();
    let mut actual = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, b"abcd");
}
