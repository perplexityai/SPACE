use super::*;

#[tokio::test]
async fn uploads_of_any_size_are_written_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let frame = pplx_space_spaced_common::filesystem_limits::WRITE_CHUNK_BYTES;
    for size in [
        0,
        1,
        DIRECT_IO_ALIGNMENT - 1,
        DIRECT_IO_ALIGNMENT,
        DIRECT_IO_ALIGNMENT + 1,
        SMALL_UPLOAD_BYTES,
        SMALL_UPLOAD_BYTES + 1,
        UPLOAD_WRITE_BYTES,
        UPLOAD_WRITE_BYTES + 1,
        2 * UPLOAD_WRITE_BYTES + UPLOAD_WRITE_BYTES / 2 + 3,
    ] {
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let path = directory.path().join(format!("upload-{size}"));
        let file = tokio::fs::File::create(&path).await.unwrap();
        let mut writer = UploadWriter::new(file, false, Arc::new(UploadBufferPool::new()))
            .await
            .unwrap();
        for chunk in data.chunks(frame) {
            writer.write(chunk).await.unwrap();
        }
        writer.finish().await.unwrap().sync_all().unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            data,
            "upload of {size} bytes"
        );
    }
}

#[tokio::test]
async fn appended_uploads_keep_the_existing_content() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("log");
    std::fs::write(&path, b"existing,").unwrap();
    let file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .unwrap();
    let mut writer = UploadWriter::new(file, true, Arc::new(UploadBufferPool::new()))
        .await
        .unwrap();
    assert!(matches!(writer.mode, UploadMode::Streaming));
    writer.write(b"frame,").await.unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"existing,frame,",
        "an appended frame is visible before the upload finishes"
    );
    let tail = vec![b'x'; UPLOAD_WRITE_BYTES + 17];
    writer.write(&tail).await.unwrap();
    writer.finish().await.unwrap();
    let mut expected = b"existing,frame,".to_vec();
    expected.extend_from_slice(&tail);
    assert_eq!(std::fs::read(&path).unwrap(), expected);
}

#[test]
fn a_service_holds_a_bounded_number_of_upload_buffers_and_reuses_them() {
    let pool = UploadBufferPool::new();
    let mut first = pool.take().expect("a first buffer");
    let address = first.address();
    assert_eq!(
        address % UPLOAD_WRITE_BYTES,
        0,
        "a huge page needs an aligned buffer"
    );
    let mut others: Vec<_> = (1..UPLOAD_BUFFERS_PER_SERVICE)
        .map(|_| pool.take().expect("a buffer within the bound"))
        .collect();
    assert!(pool.take().is_none(), "no buffer beyond the bound exists");
    first.fill(b"previous upload");
    pool.give(first);
    let reused = pool.take().expect("the returned buffer");
    assert_eq!(
        reused.address(),
        address,
        "a finished upload's buffer is reused"
    );
    assert_eq!(reused.filled, 0);
    others.clear();
    assert_eq!(pool.held(), 1, "dropped buffers free their places");
    pool.give(reused);
    assert_eq!((pool.idle(), pool.held()), (1, 1));
}

async fn write_in_frames(writer: &mut UploadWriter, data: &[u8]) {
    for chunk in data.chunks(pplx_space_spaced_common::filesystem_limits::WRITE_CHUNK_BYTES) {
        writer.write(chunk).await.unwrap();
    }
}

// Most uploads are small; they hold no buffer while their frames arrive, and
// borrow one only for their final write where it can go direct.
#[tokio::test]
async fn a_small_upload_holds_no_upload_buffer_while_it_arrives() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("small");
    let pool = Arc::new(UploadBufferPool::new());
    let file = tokio::fs::File::create(&path).await.unwrap();
    let direct = set_direct_io(&std::fs::File::open(&path).unwrap(), true).is_ok();
    let mut writer = UploadWriter::new(file, false, Arc::clone(&pool))
        .await
        .unwrap();
    let data: Vec<u8> = (0..SMALL_UPLOAD_BYTES).map(|i| (i % 251) as u8).collect();
    write_in_frames(&mut writer, &data).await;
    assert!(matches!(writer.mode, UploadMode::Small(_)));
    assert_eq!(pool.held(), 0);
    writer.finish().await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), data);
    let borrowed = usize::from(direct);
    assert_eq!(
        (pool.idle(), pool.held()),
        (borrowed, borrowed),
        "a direct final write returns its buffer; without O_DIRECT none is taken"
    );
}

// A small upload that is not a whole number of blocks goes direct up to its
// last whole block and writes the rest buffered.
#[tokio::test]
async fn a_small_upload_with_a_partial_block_is_written_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("partial");
    let file = tokio::fs::File::create(&path).await.unwrap();
    let mut writer = UploadWriter::new(file, false, Arc::new(UploadBufferPool::new()))
        .await
        .unwrap();
    let data: Vec<u8> = (0..3 * DIRECT_IO_ALIGNMENT + 17)
        .map(|i| (i % 251) as u8)
        .collect();
    write_in_frames(&mut writer, &data).await;
    writer.finish().await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), data);
}

// An upload that outgrows the small path while every buffer is in use is
// written as its frames arrive, and still exactly.
#[tokio::test]
async fn an_upload_that_finds_no_buffer_free_streams_its_frames() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("streamed");
    let pool = Arc::new(UploadBufferPool::new());
    let _in_use: Vec<_> = (0..UPLOAD_BUFFERS_PER_SERVICE)
        .map(|_| pool.take().unwrap())
        .collect();
    let file = tokio::fs::File::create(&path).await.unwrap();
    let mut writer = UploadWriter::new(file, false, Arc::clone(&pool))
        .await
        .unwrap();
    let data: Vec<u8> = (0..3 * SMALL_UPLOAD_BYTES + 5)
        .map(|i| (i % 251) as u8)
        .collect();
    write_in_frames(&mut writer, &data).await;
    assert!(matches!(writer.mode, UploadMode::Streaming));
    writer.finish().await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), data);
}

#[cfg(target_os = "linux")]
fn is_direct(writer: &UploadWriter) -> bool {
    matches!(writer.mode, UploadMode::Collecting { direct: true, .. })
}

// A FIFO accepts O_DIRECT but turns it into packet mode, which can drop data
// a reader does not take in one read, so uploads into one stay buffered.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn uploads_into_a_fifo_are_not_written_direct() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pipe");
    nix::unistd::mkfifo(&path, nix::sys::stat::Mode::S_IRWXU).unwrap();
    let file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await
        .unwrap();
    let mut writer = UploadWriter::new(file, false, Arc::new(UploadBufferPool::new()))
        .await
        .unwrap();
    // Past the small path, but short of a full buffer, so nothing reaches
    // the FIFO yet.
    writer
        .write(&vec![1_u8; SMALL_UPLOAD_BYTES + 1])
        .await
        .unwrap();
    assert!(matches!(writer.mode, UploadMode::Collecting { .. }));
    assert!(!is_direct(&writer));
}

// Where the filesystem takes O_DIRECT, a whole upload is written direct; the
// final partial block goes buffered without turning the earlier writes back.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn uploads_stay_direct_where_the_filesystem_allows_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("upload");
    let probe = std::fs::File::create(&path).unwrap();
    if set_direct_io(&probe, true).is_err() {
        // tmpfs, for example, has no O_DIRECT.
        return;
    }
    drop(probe);
    let file = tokio::fs::File::create(&path).await.unwrap();
    let mut writer = UploadWriter::new(file, false, Arc::new(UploadBufferPool::new()))
        .await
        .unwrap();
    let data: Vec<u8> = (0..2 * UPLOAD_WRITE_BYTES)
        .map(|i| (i % 251) as u8)
        .collect();
    writer.write(&data).await.unwrap();
    assert!(is_direct(&writer), "full buffers are written direct");
    writer.write(b"tail").await.unwrap();
    writer.finish().await.unwrap();
    let mut expected = data;
    expected.extend_from_slice(b"tail");
    assert_eq!(std::fs::read(&path).unwrap(), expected);
}
