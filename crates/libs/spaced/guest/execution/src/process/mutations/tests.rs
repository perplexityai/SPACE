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

struct ScriptedWriter {
    steps: std::collections::VecDeque<Result<usize, std::io::ErrorKind>>,
    received: Vec<u8>,
}
impl tokio::io::AsyncWrite for ScriptedWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let count = self
            .steps
            .pop_front()
            .unwrap()
            .map_err(std::io::Error::from)?;
        self.received.extend_from_slice(&bytes[..count]);
        std::task::Poll::Ready(Ok(count))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn sequenced_input_retries_interrupts_and_distinguishes_known_write_failures() {
    use std::io::ErrorKind::{BrokenPipe, Interrupted};
    for (steps, bytes, uncertain) in [
        (vec![Err(Interrupted), Ok(3)], b"abc".as_slice(), false),
        (vec![Err(BrokenPipe)], b"".as_slice(), false),
        (vec![Ok(1), Err(BrokenPipe)], b"a".as_slice(), true),
    ] {
        let input = InputDescriptor::new(Some(ScriptedWriter {
            steps: steps.into(),
            received: vec![],
        }));
        let result = write_descriptor(&input, b"abc", &CancellationToken::new())
            .await
            .result();
        if bytes == b"abc" {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                RecoveryReason::from_status(&error),
                uncertain.then_some(RecoveryReason::InputOutcomeUncertain)
            );
            if !uncertain {
                assert_eq!(
                    error.code(),
                    map_io_error(std::io::Error::from(BrokenPipe)).code()
                );
            }
        }
        assert_eq!(input.lock().await.as_ref().unwrap().received, bytes);
    }
}
