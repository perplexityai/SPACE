use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;

use super::*;

#[tokio::test]
async fn fifo_read_rejects_without_a_writer_and_returns_admission() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("fifo");
    nix::unistd::mkfifo(
        &path,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .expect("fifo");
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        read_stream(
            GuestPath::parse(path.display().to_string()).expect("path"),
            0,
            None,
            slots.clone().try_acquire_owned().expect("admission"),
        ),
    )
    .await;
    // Unblock the old implementation before reporting the regression, so a
    // failing assertion cannot leave runtime shutdown parked in the kernel.
    if result.is_err() {
        let _writer = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path)
            .expect("release blocked reader");
    }
    assert!(
        matches!(result, Ok(Err(_))),
        "FIFO must fail without waiting for a writer"
    );
    assert_eq!(slots.available_permits(), 1);
    let regular = directory.path().join("regular");
    tokio::fs::write(&regular, b"data").await.expect("file");
    let mut stream = read_stream(
        GuestPath::parse(regular.display().to_string()).expect("path"),
        0,
        None,
        slots
            .clone()
            .try_acquire_owned()
            .expect("recovered admission"),
    )
    .await
    .expect("normal read");
    use futures_util::StreamExt;
    assert_eq!(
        &stream.next().await.expect("chunk").expect("read").data[..],
        b"data"
    );
    assert!(stream.next().await.is_none());
    assert_eq!(slots.available_permits(), 1);
}

// A read whose reader has stopped taking chunks still ends when `ended`
// completes, and releases what it holds without waiting for the reader; the
// reader then gets the chunks already queued, followed by the failure.
#[tokio::test]
async fn a_stalled_read_releases_its_guard_when_it_ends() {
    use futures_util::StreamExt;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("file");
    let data: Vec<u8> = (0..=250_u8).cycle().take(1 << 20).collect();
    std::fs::write(&path, &data).expect("file");
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let (end, ended) = tokio::sync::oneshot::channel::<()>();
    let mut stream = stream_file(
        std::fs::File::open(&path).expect("open"),
        None,
        slots.clone().try_acquire_owned().expect("admission"),
        async move {
            let _ = ended.await;
            Status::unavailable("worker ended")
        },
    );
    // Take nothing, so the queue fills and the read waits for room in it.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(slots.available_permits(), 0, "the read holds its guard");
    end.send(()).expect("end the read");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while slots.available_permits() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the read releases its guard while its reader is stalled");

    let mut read = Vec::new();
    let mut failure = None;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => read.extend_from_slice(&chunk.data),
            Err(status) => failure = Some(status),
        }
    }
    let failure = failure.expect("the stream ends with the failure");
    assert_eq!(failure.code(), tonic::Code::Unavailable);
    assert!(read.len() < data.len(), "the read stopped before the end");
    assert!(data.starts_with(&read), "the queued chunks arrive in order");
}
