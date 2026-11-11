use super::*;

#[tokio::test]
async fn source_completion_requires_exact_length_successful_trailers_and_no_extra_data() {
    let data = || BtrfsSendChunk {
        data: b"payload".to_vec(),
        completed_bytes: None,
    };
    let done = |n| BtrfsSendChunk {
        data: Vec::new(),
        completed_bytes: Some(n),
    };
    // EOF, a lying producer, or a late RPC failure must never publish a
    // successfully compressed prefix as a complete snapshot.
    for (frames, valid) in [
        (vec![Ok(data()), Ok(done(7))], true),
        (vec![Ok(done(0))], true),
        (vec![Ok(data())], false),
        (vec![Ok(data()), Ok(done(6))], false),
        (vec![Ok(data()), Ok(done(8))], false),
        (vec![Ok(data()), Ok(done(7)), Ok(data())], false),
        (
            vec![
                Ok(data()),
                Ok(done(7)),
                Err(tonic::Status::internal("send failed")),
            ],
            false,
        ),
        (
            vec![Ok(BtrfsSendChunk {
                data: vec![1; 256 * 1024 + 1],
                completed_bytes: None,
            })],
            false,
        ),
        (
            vec![Ok(BtrfsSendChunk {
                data: vec![1],
                completed_bytes: Some(1),
            })],
            false,
        ),
        (
            vec![Ok(BtrfsSendChunk {
                data: Vec::new(),
                completed_bytes: None,
            })],
            false,
        ),
    ] {
        let input = RemoteReader {
            stream: Box::pin(tokio_stream::iter(frames)),
            buffer: std::io::Cursor::new(Vec::new()),
            runtime: tokio::runtime::Handle::current(),
            cancellation: CancellationToken::new(),
            consumed: 0,
            finished: false,
        };
        let result = tokio::task::spawn_blocking(move || {
            let mut input = input;
            let mut decoded = Vec::new();
            input.read_to_end(&mut decoded).map(|_| decoded)
        })
        .await
        .unwrap();
        assert_eq!(result.is_ok(), valid, "{result:?}");
        if let Ok(bytes) = result {
            assert!(bytes.is_empty() || bytes == b"payload");
        }
    }
}

#[tokio::test]
async fn cancellation_interrupts_a_source_wait_without_waiting_for_the_peer() {
    let cancellation = CancellationToken::new();
    let input = RemoteReader {
        stream: Box::pin(tokio_stream::pending()),
        buffer: std::io::Cursor::new(Vec::new()),
        runtime: tokio::runtime::Handle::current(),
        cancellation: cancellation.clone(),
        consumed: 0,
        finished: false,
    };
    let task = tokio::task::spawn_blocking(move || {
        let mut input = input;
        input.read(&mut [0; 1])
    });
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
}
