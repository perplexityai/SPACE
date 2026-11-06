use std::time::Duration;

use futures_util::stream;

use super::*;

const BUDGET: Duration = Duration::from_secs(30);

#[tokio::test]
async fn a_read_refused_before_its_first_chunk_is_answered_with_the_refusal() {
    let refused = stream::iter([
        Err(tonic::Status::failed_precondition("Is a directory")),
        Ok(b"never".to_vec()),
    ]);

    let error = start_read(refused, BUDGET)
        .await
        .err()
        .expect("the first message's failure is the answer");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "Is a directory");
}

#[tokio::test]
async fn a_started_read_yields_every_message_in_order() {
    let chunks = stream::iter([
        Ok(b"first".to_vec()),
        Ok(b"second".to_vec()),
        Err(tonic::Status::internal("later failure")),
    ]);

    let read: Vec<_> = start_read(chunks, BUDGET)
        .await
        .expect("a read whose first chunk arrives starts")
        .collect()
        .await;

    assert_eq!(read.len(), 3);
    assert_eq!(read[0].as_deref().unwrap(), b"first");
    assert_eq!(read[1].as_deref().unwrap(), b"second");
    assert_eq!(read[2].as_ref().unwrap_err().code(), tonic::Code::Internal);
}

#[tokio::test]
async fn an_empty_read_starts_and_ends() {
    let empty = stream::iter(Vec::<Result<Vec<u8>, tonic::Status>>::new());

    let read: Vec<_> = start_read(empty, BUDGET)
        .await
        .expect("an empty file is read")
        .collect()
        .await;

    assert!(read.is_empty());
}

// A read that sends nothing, as an old guest's read of a FIFO without a
// writer does, ends once the budget passes rather than holding the response.
#[tokio::test(start_paused = true)]
async fn a_read_that_sends_nothing_ends_after_its_budget() {
    let silent = stream::pending::<Result<Vec<u8>, tonic::Status>>();

    let started = tokio::time::Instant::now();
    let error = tokio::time::timeout(BUDGET * 2, start_read(silent, BUDGET))
        .await
        .expect("the read is bounded by its budget")
        .err()
        .expect("a read that sends nothing fails");

    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert_eq!(started.elapsed(), BUDGET);
}
