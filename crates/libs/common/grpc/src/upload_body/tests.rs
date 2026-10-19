use super::*;
use std::future::poll_fn;
use std::time::Duration;

async fn next<T>(body: &mut UploadBody<T>) -> Option<T> {
    poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut *body), cx)).await
}

#[tokio::test(start_paused = true)]
async fn only_finish_ends_the_body() {
    for finished in [true, false] {
        let (tx, mut body) = upload_channel(4);
        tx.send(1).await.unwrap();
        let other = tx.clone();
        other.send(2).await.unwrap();
        drop(other);
        if finished {
            tx.finish().await.unwrap();
        } else {
            drop(tx);
        }
        assert_eq!(next(&mut body).await, Some(1));
        assert_eq!(next(&mut body).await, Some(2));
        let end = tokio::time::timeout(Duration::from_secs(60), next(&mut body)).await;
        if finished {
            assert_eq!(end.expect("finished body ends"), None);
            assert_eq!(next(&mut body).await, None);
        } else {
            end.expect_err("an unfinished body never ends");
        }
    }
}
