use super::*;
#[derive(Default, Debug)]
struct Bytes(Vec<u8>);
impl DownloadWriter for Bytes {
    fn written(&self) -> u64 {
        self.0.len() as u64
    }
    fn append(&mut self, bytes: &[u8], _: &Path) -> Result<(), StagingError> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

#[tokio::test]
async fn transfers_enforce_the_byte_limit_idle_deadline_and_cancellation() {
    let cancel = CancellationToken::new();
    let path = PathBuf::from("synthetic-download");
    for (chunks, limit, succeeds) in [
        (vec![b"ab".as_slice(), b"cd".as_slice()], 4, true),
        (vec![b"ab".as_slice(), b"cde".as_slice()], 4, false),
    ] {
        let stream = futures_util::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(chunk))),
        );
        let result = receive(
            Bytes::default(),
            stream,
            path.clone(),
            limit,
            Duration::from_secs(1),
            &cancel,
        )
        .await;
        if succeeds {
            assert_eq!(result.unwrap().0, b"abcd");
        } else {
            assert!(matches!(result, Err(TransferError::TooLarge(4))));
        }
    }
    let pending = || futures_util::stream::pending::<Result<bytes::Bytes, std::io::Error>>();
    assert!(matches!(
        receive(
            Bytes::default(),
            pending(),
            path.clone(),
            4,
            Duration::from_millis(1),
            &cancel
        )
        .await,
        Err(TransferError::Stalled(_))
    ));
    cancel.cancel();
    assert!(matches!(
        receive(
            Bytes::default(),
            pending(),
            path,
            4,
            Duration::from_secs(60),
            &cancel
        )
        .await,
        Err(TransferError::Cancelled)
    ));
}
