use super::*;
use futures_util::StreamExt;

fn record() -> Arc<Record> {
    Arc::new(Record {
        id: ProcessId::default(),
        request: StartRequest::default(),
        state: Mutex::new(State {
            info: Some(ProcessInfo {
                pid: 42,
                ..Default::default()
            }),
            primary: Some(0),
            ..Default::default()
        }),
        notify: Arc::default(),
        consumed: Condvar::new(),
        output_limit: OUTPUT_CHUNK_BYTES + std::mem::size_of::<process_event::Event>(),
        job: Mutex::default(),
        stdin: Mutex::default(),
        input: Arc::new(Semaphore::new(1)),
        readers: Arc::new(Semaphore::new(4)),
    })
}
fn chunk(byte: u8) -> process_event::Event {
    process_event::Event::Stdout(ProcessOutput {
        data: vec![byte; OUTPUT_CHUNK_BYTES],
    })
}

#[tokio::test]
async fn attached_output_is_backpressured_and_detach_releases_the_producer() {
    let record = record();
    let mut stream = record.clone().stream(Some(0), false, true).await.unwrap();
    record.output(chunk(1));
    let producer_record = record.clone();
    let producer = std::thread::spawn(move || {
        producer_record.output(chunk(2));
        producer_record.output(chunk(3));
    });
    // Started is separate from output. Consuming output frees its retention slot.
    stream.next().await.unwrap().unwrap();
    let event = stream.next().await.unwrap().unwrap();
    assert!(matches!(event.event, Some(process_event::Event::Stdout(ref o)) if o.data[0] == 1));
    drop(stream);
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || producer.join()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let state = record.state.lock().unwrap();
    assert!(state.bytes <= record.output_limit);
    assert_eq!(state.first, 2);
}

#[test]
fn signal_validation_distinguishes_malformed_and_unsupported_requests() {
    for (signal, expected) in [
        (9, None),
        (15, Some(tonic::Code::FailedPrecondition)),
        (0, Some(tonic::Code::InvalidArgument)),
        (-1, Some(tonic::Code::InvalidArgument)),
        (65, Some(tonic::Code::InvalidArgument)),
    ] {
        assert_eq!(
            require_supported_signal(signal).err().map(|e| e.code()),
            expected
        );
    }
}

#[test]
fn start_requires_replay_identity_and_bounded_nul_free_tag_before_admission() {
    let mut request = StartRequest::default();
    assert_eq!(
        start_key(&request).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    request.idempotency_key = Some("synthetic-start".into());
    for (tag, valid) in [
        (None, true),
        (Some("x".repeat(4096)), true),
        (Some("x".repeat(4097)), false),
        (Some("tag\0suffix".into()), false),
    ] {
        request.tag = tag;
        match start_key(&request) {
            Ok(_) => assert!(valid),
            Err(error) => {
                assert!(!valid);
                assert_eq!(error.code(), tonic::Code::InvalidArgument);
            }
        }
    }
}

#[test]
fn oversized_output_fails_without_eviction_or_spinning() {
    let mut record = record();
    Arc::get_mut(&mut record).unwrap().output_limit = 1;
    record.output(chunk(1));
    let state = record.state.lock().unwrap();
    assert_eq!(
        state.error.as_ref().unwrap().code(),
        tonic::Code::ResourceExhausted
    );
    assert!(state.output.is_empty());
    assert_eq!(state.first, 0);
}

#[tokio::test]
async fn retained_cursor_reattachment_reclaims_backpressure_without_observer_takeover() {
    for retried_start in [false, true] {
        let record = record();
        let first = record.clone().stream(Some(0), false, true).await.unwrap();
        drop(first);
        record.output(chunk(7));
        let mut replacement = record
            .clone()
            .stream(Some(0), false, retried_start)
            .await
            .unwrap();
        assert_eq!(
            record.state.lock().unwrap().primary,
            Some(0),
            "reattachment must protect its retained cursor before returning"
        );
        let observer = record.clone().stream(None, false, false).await.unwrap();
        drop(observer);
        assert_eq!(
            record.state.lock().unwrap().primary,
            Some(0),
            "an observer cannot release the primary reader"
        );
        replacement.next().await.unwrap().unwrap();
        replacement.next().await.unwrap().unwrap();
        assert_eq!(record.state.lock().unwrap().primary, Some(1));
        drop(replacement);
        assert!(
            record
                .clone()
                .stream(Some(999), false, retried_start)
                .await
                .is_err()
        );
        assert_eq!(
            record.state.lock().unwrap().primary,
            None,
            "invalid cursors cannot block the producer"
        );
        let final_reader = record.clone().stream(Some(1), false, false).await.unwrap();
        assert_eq!(record.state.lock().unwrap().primary, Some(1));
        drop(final_reader);
    }
}
