use super::*;

#[tokio::test]
async fn snapshot_delta_reserves_expansion_even_when_other_files_arrive_first() {
    for delta_first in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut receiver = SparseImport {
            limits: ImportLimits {
                file_bytes: 8,
                total_bytes: 12,
            },
            ..Default::default()
        };
        let files = if delta_first {
            [("rootfs.ext4.diff", 1), ("memory", 4)]
        } else {
            [("memory", 4), ("rootfs.ext4.diff", 1)]
        };
        let (name, size) = files[0];
        receiver
            .write(name, &temp.path().join(name), size, &[], true)
            .await
            .unwrap();
        let (name, size) = files[1];
        let error = receiver
            .write(name, &temp.path().join(name), size, &[], true)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(!temp.path().join(name).exists());
    }
}

#[tokio::test]
async fn import_limits_reject_data_sparse_extents_and_aggregate_growth_before_writing() {
    for (first_size, offset, data) in [
        (0, 0, vec![1; 9]), // one oversized data chunk
        (0, 9, vec![]),     // sparse EOF, no received bytes
        (0, 8, vec![1]),    // small chunk at a large offset
        (8, 5, vec![]),     // multiple individually valid files exceed total
        (8, 0, vec![1; 5]),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut receiver = SparseImport {
            limits: ImportLimits {
                file_bytes: 8,
                total_bytes: 12,
            },
            ..Default::default()
        };
        if first_size != 0 {
            receiver
                .write("first", &temp.path().join("first"), first_size, &[], true)
                .await
                .unwrap();
        }
        let rejected = temp.path().join("rejected");
        let error = receiver
            .write("rejected", &rejected, offset, &data, data.is_empty())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(!rejected.exists(), "rejected chunk must not create a file");
    }

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("boundary");
    let mut receiver = SparseImport {
        limits: ImportLimits {
            file_bytes: 8,
            total_bytes: 12,
        },
        ..Default::default()
    };
    receiver
        .write("boundary", &path, 0, b"1234", false)
        .await
        .unwrap();
    assert_eq!(
        receiver
            .write("boundary", &path, 9, &[], true)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), 4);
    receiver
        .write("boundary", &path, 8, &[], true)
        .await
        .unwrap();
    receiver
        .write("second", &temp.path().join("second"), 4, &[], true)
        .await
        .unwrap();
    assert_eq!(receiver.finish().unwrap(), 4);
    assert_eq!(tokio::fs::metadata(path).await.unwrap().len(), 8);

    // Rootfs entitlement is independent of the transport's memory/file budget.
    for name in ["rootfs.ext4", "snapshot/rootfs.ext4"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rootfs.ext4");
        let mut receiver = SparseImport {
            limits: ImportLimits {
                file_bytes: u64::MAX,
                total_bytes: u64::MAX,
            },
            ..Default::default()
        };
        assert_eq!(
            receiver
                .write(name, &path, 128 * 1024 * 1024 * 1024 + 1, &[], true)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert!(!path.exists());
    }
}

#[tokio::test]
async fn stalled_export_releases_producer_and_delivers_terminal_error_after_buffer() {
    use tokio_stream::StreamExt;
    let (sender, mut stream) = export_channel::<u8>();
    tokio::task::spawn_blocking(move || {
        for byte in 0..4 {
            sender.blocking_send(Ok(byte)).unwrap();
        }
        let result = sender.send_with_timeout(Ok(4), std::time::Duration::ZERO);
        assert_eq!(
            result.as_ref().unwrap_err().code(),
            tonic::Code::DeadlineExceeded
        );
        sender.finish(result);
    })
    .await
    .unwrap();
    for byte in 0..4 {
        assert_eq!(stream.next().await.unwrap().unwrap(), byte);
    }
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    assert!(stream.next().await.is_none());
}
#[tokio::test(start_paused = true)]
async fn active_import_cannot_refresh_its_absolute_deadline() {
    let deadline =
        ImportDeadline(tokio::time::Instant::now() + std::time::Duration::from_secs(100));
    let mut stream = tokio_stream::iter([Ok(1), Ok(2), Ok(3)]);
    assert_eq!(next_message(&mut stream, deadline).await.unwrap(), Some(1));
    tokio::time::advance(std::time::Duration::from_secs(50)).await;
    assert_eq!(next_message(&mut stream, deadline).await.unwrap(), Some(2));
    tokio::time::advance(std::time::Duration::from_secs(50)).await;
    assert_eq!(
        next_message(&mut stream, deadline)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(
        deadline
            .wait(std::future::ready(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
}
