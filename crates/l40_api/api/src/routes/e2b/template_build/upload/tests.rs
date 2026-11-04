use super::*;

use std::convert::Infallible;
use std::io::{self, Write as _};
use std::sync::Arc;

use axum::body::Bytes;
use flate2::{Compression, write::GzEncoder};
use pplx_space_build_mgr_client::proto::UploadBlobPhase;
use rstest::rstest;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Duration, timeout};
use tokio_stream::{iter, wrappers::ReceiverStream};

const TEST_ORG: OrgId = OrgId::from_uuid(uuid::Uuid::from_u128(1));
const TEST_PATH: &str = "/v1/internal/e2b-files/code-runtime/abc123";
const TEST_SECRET: &[u8] = b"test-file-upload-signing-secret";

async fn collect_upload(body: Body, max_bytes: u64) -> (u64, Vec<u8>) {
    let (sender, mut receiver) = mpsc::channel(2);
    let forward = stream_upload_body(body, sender, max_bytes);
    let collect = async move {
        let mut bytes = Vec::new();
        let mut last_phase = None;
        while let Some(chunk) = receiver.recv().await {
            last_phase = UploadBlobPhase::try_from(chunk.phase).ok();
            bytes.extend(chunk.data);
        }
        assert_eq!(last_phase, Some(UploadBlobPhase::Commit));
        bytes
    };
    let (size_bytes, bytes) = tokio::join!(forward, collect);
    (size_bytes.unwrap(), bytes)
}

#[tokio::test]
async fn upload_body_is_forwarded_before_request_finishes() {
    let (body_sender, body_receiver) = mpsc::channel(2);
    body_sender
        .send(Ok::<_, Infallible>(Bytes::from_static(b"first")))
        .await
        .unwrap();
    let body = Body::from_stream(ReceiverStream::new(body_receiver));
    let (upload_sender, mut upload_receiver) = mpsc::channel(2);

    let forward = tokio::spawn(stream_upload_body(body, upload_sender, 1024));
    let marker = timeout(Duration::from_millis(100), upload_receiver.recv())
        .await
        .expect("upload start marker should arrive")
        .unwrap();
    assert!(marker.data.is_empty());
    assert_eq!(
        UploadBlobPhase::try_from(marker.phase).unwrap(),
        UploadBlobPhase::Data
    );
    let first = timeout(Duration::from_millis(100), upload_receiver.recv())
        .await
        .expect("first upload chunk should arrive while the request remains open")
        .unwrap();
    body_sender
        .send(Ok(Bytes::from_static(b"second")))
        .await
        .unwrap();
    drop(body_sender);
    let mut bytes = first.data;
    let collect = async {
        while let Some(chunk) = upload_receiver.recv().await {
            bytes.extend(chunk.data);
        }
        bytes
    };
    let (size_bytes, bytes) = tokio::join!(forward, collect);
    let size_bytes = size_bytes.unwrap().unwrap();

    assert_eq!(bytes, b"firstsecond");
    assert_eq!(size_bytes, 11);
}

#[tokio::test]
async fn gzip_upload_is_decompressed_while_forwarding() {
    let expected = b"plain tar bytes";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(expected).unwrap();
    let compressed = encoder.finish().unwrap();

    let (size_bytes, bytes) = collect_upload(Body::from(compressed), 1024).await;

    assert_eq!(bytes, expected);
    assert_eq!(size_bytes, expected.len() as u64);
}

#[tokio::test]
async fn gzip_magic_may_be_split_across_http_frames() {
    let expected = b"plain tar bytes";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(expected).unwrap();
    let compressed = encoder.finish().unwrap();
    let body = Body::from_stream(iter([
        Ok::<_, Infallible>(Bytes::copy_from_slice(&compressed[..1])),
        Ok(Bytes::copy_from_slice(&compressed[1..])),
    ]));

    let (size_bytes, bytes) = collect_upload(body, 1024).await;

    assert_eq!(bytes, expected);
    assert_eq!(size_bytes, expected.len() as u64);
}

#[tokio::test]
async fn gzip_upload_is_forwarded_before_request_finishes() {
    let expected = vec![b'x'; 2 * BLOB_UPLOAD_CHUNK_BYTES];
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&expected).unwrap();
    let compressed = encoder.finish().unwrap();
    let split = compressed.len() - 8;
    let (body_sender, body_receiver) = mpsc::channel(2);
    body_sender
        .send(Ok::<_, Infallible>(Bytes::copy_from_slice(
            &compressed[..split],
        )))
        .await
        .unwrap();
    let body = Body::from_stream(ReceiverStream::new(body_receiver));
    let (upload_sender, mut upload_receiver) = mpsc::channel(4);

    let forward = tokio::spawn(stream_upload_body(
        body,
        upload_sender,
        expected.len() as u64,
    ));
    let marker = timeout(Duration::from_secs(1), upload_receiver.recv())
        .await
        .expect("upload start marker should arrive")
        .unwrap();
    assert_eq!(
        UploadBlobPhase::try_from(marker.phase).unwrap(),
        UploadBlobPhase::Data
    );
    let first = timeout(Duration::from_secs(1), upload_receiver.recv())
        .await
        .expect("decoded data should arrive while the gzip request remains open")
        .unwrap();
    body_sender
        .send(Ok(Bytes::copy_from_slice(&compressed[split..])))
        .await
        .unwrap();
    drop(body_sender);
    let mut bytes = first.data;
    while let Some(chunk) = upload_receiver.recv().await {
        bytes.extend(chunk.data);
    }
    let size_bytes = forward.await.unwrap().unwrap();

    assert_eq!(bytes, expected);
    assert_eq!(size_bytes, expected.len() as u64);
}

async fn start_stalled_gzip_upload(
    limits: UploadLimits,
) -> (
    mpsc::Sender<Result<Bytes, Infallible>>,
    mpsc::Receiver<pplx_space_build_mgr_client::proto::UploadBlobChunk>,
    tokio::task::JoinHandle<Result<u64, UploadBodyError>>,
) {
    let (body_sender, body_receiver) = mpsc::channel(1);
    body_sender
        .send(Ok(Bytes::copy_from_slice(&GZIP_MAGIC)))
        .await
        .unwrap();
    let body = Body::from_stream(ReceiverStream::new(body_receiver));
    let (upload_sender, upload_receiver) = mpsc::channel(2);
    let task = tokio::spawn(stream_upload_body_with_limits(
        body,
        upload_sender,
        1024,
        limits,
    ));
    (body_sender, upload_receiver, task)
}

async fn wait_for_decoder_admission(slots: &Semaphore) {
    timeout(Duration::from_secs(1), async {
        while slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gzip decoder should acquire its slot");
}

#[tokio::test]
async fn stalled_gzip_upload_over_capacity_is_rejected_and_cancellation_releases_slot() {
    let slots = Arc::new(Semaphore::new(1));
    let limits = UploadLimits {
        gzip_decoder_slots: Arc::clone(&slots),
        idle_timeout: Duration::from_secs(5),
        deadline: Duration::from_secs(5),
    };
    let (admitted_body, admitted_upload, admitted_task) =
        start_stalled_gzip_upload(limits.clone()).await;
    wait_for_decoder_admission(&slots).await;
    let (rejected_body, rejected_upload, rejected_task) = start_stalled_gzip_upload(limits).await;

    let rejected = timeout(Duration::from_secs(1), rejected_task)
        .await
        .expect("over-capacity upload should return promptly")
        .unwrap();

    assert!(matches!(rejected, Err(UploadBodyError::DecoderCapacity)));
    assert_eq!(slots.available_permits(), 0);

    admitted_task.abort();
    assert!(admitted_task.await.unwrap_err().is_cancelled());
    drop(admitted_body);
    drop(admitted_upload);
    drop(rejected_body);
    drop(rejected_upload);
    let permit = timeout(Duration::from_secs(1), Arc::clone(&slots).acquire_owned())
        .await
        .expect("cancelled decoder should release its slot")
        .unwrap();
    drop(permit);
}

#[derive(Debug, Clone, Copy)]
enum StalledUploadTimeout {
    Idle,
    Deadline,
}

#[rstest]
#[case::idle(StalledUploadTimeout::Idle)]
#[case::deadline(StalledUploadTimeout::Deadline)]
#[tokio::test]
async fn stalled_gzip_upload_times_out_and_releases_decoder_slot(
    #[case] upload_timeout: StalledUploadTimeout,
) {
    let slots = Arc::new(Semaphore::new(1));
    let limits = UploadLimits {
        gzip_decoder_slots: Arc::clone(&slots),
        idle_timeout: match upload_timeout {
            StalledUploadTimeout::Idle => Duration::from_millis(250),
            StalledUploadTimeout::Deadline => Duration::from_secs(5),
        },
        deadline: match upload_timeout {
            StalledUploadTimeout::Idle => Duration::from_secs(5),
            StalledUploadTimeout::Deadline => Duration::from_secs(1),
        },
    };
    let (body_sender, upload_receiver, task) = start_stalled_gzip_upload(limits).await;
    wait_for_decoder_admission(&slots).await;

    let result = timeout(Duration::from_secs(2), task)
        .await
        .expect("stalled upload should time out")
        .unwrap();

    assert!(matches!(
        (upload_timeout, result),
        (
            StalledUploadTimeout::Idle,
            Err(UploadBodyError::IdleTimeout { .. })
        ) | (
            StalledUploadTimeout::Deadline,
            Err(UploadBodyError::DeadlineExceeded { .. })
        )
    ));
    drop(body_sender);
    drop(upload_receiver);
    let permit = timeout(Duration::from_secs(1), Arc::clone(&slots).acquire_owned())
        .await
        .expect("timed-out decoder should release its slot")
        .unwrap();
    drop(permit);
}

#[tokio::test]
async fn failed_request_body_does_not_commit_partial_upload() {
    let body = Body::from_stream(iter([
        Ok(Bytes::from_static(b"partial tar bytes")),
        Err(io::Error::other("request body failed")),
    ]));
    let (sender, mut receiver) = mpsc::channel(4);
    let forward = stream_upload_body(body, sender, 1024);
    let collect = async move {
        let mut phases = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            phases.push(UploadBlobPhase::try_from(chunk.phase).unwrap());
        }
        phases
    };

    let (result, phases) = tokio::join!(forward, collect);

    assert!(matches!(result, Err(UploadBodyError::Read(_))));
    assert_eq!(phases.first(), Some(&UploadBlobPhase::Data));
    assert!(!phases.contains(&UploadBlobPhase::Commit));
}

#[test]
fn file_upload_capability_is_path_bound_and_expires() {
    let token = mint_file_upload_capability_at(TEST_ORG, TEST_PATH, 1_600, TEST_SECRET).unwrap();

    assert_eq!(
        verify_file_upload_capability_at(TEST_PATH, &token, 1_000, TEST_SECRET,),
        Some(TEST_ORG)
    );
    assert!(
        verify_file_upload_capability_at(
            "/v1/internal/e2b-files/other/abc123",
            &token,
            1_000,
            TEST_SECRET,
        )
        .is_none()
    );
    assert!(verify_file_upload_capability_at(TEST_PATH, &token, 1_601, TEST_SECRET,).is_none());
}

#[test]
fn file_upload_capability_rejects_tampering_and_excessive_lifetime() {
    let token = mint_file_upload_capability_at(TEST_ORG, TEST_PATH, 1_600, TEST_SECRET).unwrap();
    let other_org = OrgId::from_uuid(uuid::Uuid::from_u128(2));
    let forged_owner = token.replace(&TEST_ORG.to_string(), &other_org.to_string());
    assert!(
        verify_file_upload_capability_at(TEST_PATH, &forged_owner, 1_000, TEST_SECRET).is_none()
    );
    let mut tampered = token.into_bytes();
    let last = tampered.last_mut().unwrap();
    *last = if *last == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(tampered).unwrap();

    assert!(verify_file_upload_capability_at(TEST_PATH, &tampered, 1_000, TEST_SECRET,).is_none());

    let long_lived =
        mint_file_upload_capability_at(TEST_ORG, TEST_PATH, 1_661, TEST_SECRET).unwrap();
    assert!(
        verify_file_upload_capability_at(TEST_PATH, &long_lived, 1_000, TEST_SECRET,).is_none()
    );
}

#[test]
fn upload_query_carries_only_a_verifiable_owner_capability() {
    let query = mint_file_upload_query(TEST_ORG, TEST_PATH).unwrap();
    assert!(
        query_parameter(&query, "upload_token").is_none(),
        "the retired tenantless upload token must not be minted"
    );
    let uri: Uri = format!("{TEST_PATH}?{query}").parse().unwrap();
    assert_eq!(file_upload_capability_org(&uri), Some(TEST_ORG));

    let forged = query.replace(&TEST_ORG.to_string(), &uuid::Uuid::from_u128(2).to_string());
    let forged_uri: Uri = format!("{TEST_PATH}?{forged}").parse().unwrap();
    assert_eq!(file_upload_capability_org(&forged_uri), None);
}
