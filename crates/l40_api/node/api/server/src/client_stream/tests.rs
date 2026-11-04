use super::*;
use std::time::Duration;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

struct Tracked<S> {
    inner: S,
    dropped: Arc<AtomicBool>,
}
impl<S> Drop for Tracked<S> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl<S: Stream + Unpin> Stream for Tracked<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

#[tokio::test(start_paused = true)]
async fn forwarding_releases_input_on_upstream_failure_idle_and_cancellation() {
    for outcome in ["upstream", "idle", "backpressure", "cancel"] {
        let dropped = Arc::new(AtomicBool::new(false));
        let messages: Vec<Result<u8, Status>> = if outcome == "backpressure" {
            vec![Ok(2); 16]
        } else {
            Vec::new()
        };
        let input = Tracked {
            inner: tokio_stream::iter(messages).chain(futures_util::stream::pending()),
            dropped: dropped.clone(),
        };
        let upstream_dropped = Arc::new(AtomicBool::new(false));
        let upstream_guard = Tracked {
            inner: (),
            dropped: upstream_dropped.clone(),
        };
        let mut request = Box::pin(forward(
            input,
            Some(1),
            "Upload",
            |_| Ok(()),
            |mut rx| async move {
                let _upstream_guard = upstream_guard;
                assert_eq!(rx.next().await, Some(1));
                if outcome == "upstream" {
                    return Err::<(), _>(Status::permission_denied("refused"));
                }
                std::future::pending().await
            },
        ));
        if outcome == "cancel" {
            assert!(futures_util::poll!(&mut request).is_pending());
            drop(request);
        } else {
            let error = request.await.unwrap_err();
            assert_eq!(
                error.code(),
                if matches!(outcome, "idle" | "backpressure") {
                    tonic::Code::DeadlineExceeded
                } else {
                    tonic::Code::PermissionDenied
                }
            );
        }
        assert!(
            dropped.load(Ordering::SeqCst),
            "{outcome} must drop inbound body"
        );
        assert!(
            upstream_dropped.load(Ordering::SeqCst),
            "{outcome} must drop upstream future"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn forwarding_preserves_messages_and_never_ends_the_body_of_a_failed_input() {
    for (last, failure) in [
        (Ok(3_u8), None),
        (Ok(9), Some(tonic::Code::InvalidArgument)),
        (
            Err(Status::cancelled("caller dropped")),
            Some(tonic::Code::Cancelled),
        ),
    ] {
        // The last frame arrives once the upstream is reading its body.
        let (reading, upstream_reading) = tokio::sync::oneshot::channel::<()>();
        let input = Box::pin(
            tokio_stream::iter([Ok(2)]).chain(futures_util::stream::once(async move {
                upstream_reading.await.expect("upstream reading");
                last
            })),
        );
        let ended = Arc::new(AtomicBool::new(false));
        let ended_by_reader = ended.clone();
        let result = forward(
            input,
            Some(1),
            "Upload",
            |value| {
                if *value > 3 {
                    Err(Status::invalid_argument("changed target"))
                } else {
                    Ok(())
                }
            },
            |mut body| async move {
                // A transport reads the body on a task of its own, which
                // outlives the call when the call is dropped.
                let (done, finished) = tokio::sync::oneshot::channel();
                tokio::spawn(async move {
                    let mut messages = vec![
                        body.next().await.expect("first"),
                        body.next().await.expect("second"),
                    ];
                    reading.send(()).expect("input waiting");
                    while let Some(message) = body.next().await {
                        messages.push(message);
                    }
                    ended_by_reader.store(true, Ordering::SeqCst);
                    let _ = done.send(messages);
                });
                Ok(finished.await.expect("reader finished"))
            },
        )
        .await;
        match failure {
            None => assert_eq!(result.unwrap(), [1, 2, 3]),
            Some(code) => {
                assert_eq!(result.unwrap_err().code(), code);
                // The paused clock advances only once every task is idle.
                tokio::time::sleep(Duration::from_secs(60)).await;
                // A transport sends an ended body as a complete upload.
                assert!(
                    !ended.load(Ordering::SeqCst),
                    "{code:?} ended the upstream body"
                );
            }
        }
    }
}
