use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test(start_paused = true)]
async fn only_successful_input_can_finish_the_downstream_body() {
    for fail in [false, true] {
        let saw_eof = Arc::new(AtomicBool::new(false));
        let observed = saw_eof.clone();
        // The failure arrives once the downstream is reading its body.
        let (reading, downstream_reading) = tokio::sync::oneshot::channel::<()>();
        let failure = futures_util::stream::once(async move {
            downstream_reading.await.expect("downstream reading");
            Err(Status::aborted("input failed"))
        })
        .filter(move |_| std::future::ready(fail));
        let input = Box::pin(
            futures_util::stream::iter([Ok(WriteRequest { message: None })]).chain(failure),
        );
        let result = relay(input, move |mut body| async move {
            // A transport reads the body on a task of its own, which
            // outlives the call when the call is dropped.
            let (done, finished) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                body.next().await.expect("first frame");
                reading.send(()).expect("input waiting");
                while body.next().await.is_some() {}
                observed.store(true, Ordering::SeqCst);
                let _ = done.send(());
            });
            finished.await.expect("reader finished");
            Ok(())
        })
        .await;
        assert_eq!(result.is_err(), fail);
        // The paused clock advances only once every task is idle.
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        assert_eq!(saw_eof.load(Ordering::SeqCst), !fail);
    }
}

#[tokio::test]
async fn cancellation_drops_input_and_downstream() {
    let (input_dropped, input_observed) = tokio::sync::oneshot::channel::<()>();
    let (call_dropped, call_observed) = tokio::sync::oneshot::channel::<()>();
    let input = futures_util::stream::pending().map(move |message| {
        let _retain = &input_dropped;
        message
    });
    let mut future = Box::pin(relay(input, move |_body| async move {
        let _retain = call_dropped;
        std::future::pending::<Result<(), Status>>().await
    }));
    assert!(futures_util::poll!(&mut future).is_pending());
    drop(future);
    assert!(input_observed.await.is_err());
    assert!(call_observed.await.is_err());
}

#[tokio::test]
async fn large_messages_are_rechunked_without_reading_ahead_of_a_stalled_consumer() {
    use pplx_space_spaced_common::proto::operations::write_request::Message;
    use std::sync::atomic::AtomicUsize;

    let polled = Arc::new(AtomicUsize::new(0));
    let observed = polled.clone();
    let input = futures_util::stream::repeat_with(move || {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(WriteRequest {
            message: Some(Message::Data(vec![42; 4 * 1024 * 1024 - 16])),
        })
    });
    let mut stalled = Box::pin(relay(input, |body| async move {
        let _body = body;
        std::future::pending::<Result<(), Status>>().await
    }));
    assert!(futures_util::poll!(&mut stalled).is_pending());
    assert_eq!(
        polled.load(Ordering::SeqCst),
        1,
        "backpressure must stop within the first large message"
    );
    drop(stalled);

    let payload = vec![42; 4 * 1024 * 1024 - 16];
    let expected = payload.clone();
    relay(
        futures_util::stream::iter([Ok(WriteRequest {
            message: Some(Message::Data(payload)),
        })]),
        move |mut body| async move {
            let mut received = Vec::new();
            while let Some(message) = body.next().await {
                let Some(Message::Data(data)) = message.message else {
                    panic!("expected data")
                };
                assert!(
                    data.len() <= 64 * 1024,
                    "forwarded chunk exceeds the byte bound"
                );
                received.extend(data);
            }
            assert_eq!(received, expected);
            Ok(())
        },
    )
    .await
    .expect("upload completes");
}

#[tokio::test(start_paused = true)]
async fn downstream_input_closure_preserves_its_delayed_terminal_result() {
    for terminal in [Ok(7), Err(Status::invalid_argument("unknown user"))] {
        let expected = terminal.clone();
        let result = relay(
            futures_util::stream::repeat_with(|| Ok(WriteRequest { message: None })),
            move |body| async move {
                drop(body);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                terminal
            },
        )
        .await;
        assert_eq!(
            result.map_err(|error| error.into_status().code()),
            expected.map_err(|error| error.code())
        );
    }
}

#[tokio::test]
async fn identical_status_codes_preserve_input_versus_downstream_failure() {
    for input_failure in [true, false] {
        let input = futures_util::stream::once(async move {
            if input_failure {
                Err(Status::aborted("cancelled"))
            } else {
                std::future::pending::<Result<WriteRequest, Status>>().await
            }
        })
        .boxed();
        let result = relay(input, move |_body| async move {
            if input_failure {
                std::future::pending::<Result<(), Status>>().await
            } else {
                Err(Status::aborted("cancelled"))
            }
        })
        .await
        .expect_err("upload error");
        match &result {
            RelayError::Input(_) => assert!(input_failure),
            RelayError::Downstream(_) => assert!(!input_failure),
        }
        assert_eq!(result.into_status().code(), tonic::Code::Aborted);
    }
}
