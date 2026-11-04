use std::time::Duration;

use super::super::tests::{
    MockObjectStore, codec_test_executor_with_store, push_payload, test_creds, test_job,
    test_target, zstd_compress,
};
use super::super::*;
use super::*;

const PULL_DEADLINE: Duration = Duration::from_secs(30);
const PENDING_PROBE: Duration = Duration::from_millis(200);

#[test]
fn only_small_artifact_classes_bypass_decode_admission() {
    let expected = [
        (Some(SnapshotArtifact::Metadata), true),
        (Some(SnapshotArtifact::VmState), true),
        (Some(SnapshotArtifact::Memory), false),
        (Some(SnapshotArtifact::Rootfs), false),
        (Some(SnapshotArtifact::RootfsBtrfsSend), false),
        (Some(SnapshotArtifact::EncryptedSnapshotVolume), false),
        (Some(SnapshotArtifact::EncryptedRootfsCow), false),
        (None, false),
    ];
    for (artifact, bypasses) in expected {
        assert_eq!(
            bypasses_decode_admission(artifact),
            bypasses,
            "{artifact:?}"
        );
    }
}

#[test]
fn output_cap_admits_exactly_the_cap_and_refuses_more() {
    let mut out = Vec::new();
    let mut capped = DecodedOutputCap::new(&mut out, Some(8));
    capped.write_all(b"12345678").expect("writes up to the cap");
    let error = capped.write(b"9").expect_err("the byte past the cap");
    assert!(is_output_cap_exceeded(&error), "{error}");
    assert_eq!(out, b"12345678", "a refused write leaves no prefix");

    let mut out = Vec::new();
    DecodedOutputCap::new(&mut out, None)
        .write_all(&[7; 64])
        .expect("an uncapped writer passes everything");
    assert_eq!(out.len(), 64);
}

#[test]
fn output_cap_overflow_is_distinguishable_from_other_io_errors() {
    assert!(!is_output_cap_exceeded(&std::io::Error::other("disk full")));
    assert!(!is_output_cap_exceeded(&std::io::Error::from(
        std::io::ErrorKind::UnexpectedEof
    )));
}

#[test]
fn output_cap_overflow_survives_the_decoding_writers_final_flush() {
    // Small enough that every decoded byte stays in the writer's buffer
    // until `finish` flushes it into the capped sink.
    let plaintext = vec![b'f'; 1024];
    let mut out = Vec::new();
    let mut writer = MaybeZstdDecodingWriter::with_sink_selector(&mut out, true, |sink, _| {
        Ok(DecodedOutputCap::new(sink, Some(1023)))
    });
    writer
        .write_all(&zstd_compress(&plaintext))
        .expect("decoded bytes stay buffered");
    let error = writer
        .finish()
        .expect_err("the final flush crosses the cap");
    assert!(is_output_cap_exceeded(&error), "{error}");
}

#[test]
fn capped_copy_decode_reports_overflow_separately_from_corruption() {
    let plaintext = vec![b'm'; 4096];
    let frame = zstd_compress(&plaintext);

    let mut out = Vec::new();
    copy_decode_capped(&frame[..], &mut out, Some(4096))
        .unwrap()
        .expect_complete();
    assert_eq!(out, plaintext, "a decode at the cap completes");

    let mut out = Vec::new();
    assert!(matches!(
        copy_decode_capped(&frame[..], &mut out, Some(4095)).unwrap(),
        CappedDecode::OverCap
    ));

    let mut corrupt = ZSTD_MAGIC.to_vec();
    corrupt.extend_from_slice(b"not a zstd frame body");
    assert!(
        copy_decode_capped(&corrupt[..], &mut Vec::new(), Some(4096)).is_err(),
        "corruption must stay an error, not a restartable overflow"
    );
}

/// One pull route through the executor's decode passes.
#[derive(Clone, Copy, Debug)]
enum Route {
    /// Download, then the standalone `decompress_in_place` pass.
    Plain,
    /// Download, then the chained decrypt + decode pass.
    Encrypted,
}

const ROUTES: [Route; 2] = [Route::Plain, Route::Encrypted];

/// Synthetic data-encryption key, built like the executor tests' fixture key.
fn test_dek() -> Vec<u8> {
    (0..32u8).collect()
}

fn test_encryption() -> TransferEncryption {
    TransferEncryption {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".parse().unwrap(),
        provider: KekProvider::InMemory,
        master_key_identifier: "test/key-1".parse().unwrap(),
        wrapped_dek: vec![0; 16],
    }
}

struct PullCase {
    _dir: tempfile::TempDir,
    local_path: PathBuf,
    payload: TransferPayload,
    body: Vec<u8>,
}

/// A compressed `artifact` pull of `plaintext`, encoded the way the push
/// path encodes it: a streaming frame with no pledged content size, so
/// frame-header admission can never prove it small.
fn pull_case(route: Route, artifact: SnapshotArtifact, plaintext: &[u8]) -> PullCase {
    pull_case_with_payload_bytes(route, artifact, zstd_compress(plaintext))
}

/// A pull whose (pre-encryption) object bytes are exactly `frame`.
fn pull_case_with_payload_bytes(
    route: Route,
    artifact: SnapshotArtifact,
    frame: Vec<u8>,
) -> PullCase {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join(format!("pulled-{}", artifact.file_name()));
    let mut payload = push_payload(&local_path);
    payload.snapshot_artifact = Some(artifact);
    payload.raw = false;
    payload.existing_file_policy = PullExistingPolicy::LegacySkipPresent;
    let body = match route {
        Route::Plain => frame,
        Route::Encrypted => {
            let encryption = test_encryption();
            payload.encryption = Some(encryption.clone());
            payload.artifact_id = Some("019e9000-0000-7000-8000-000000000001".to_string());
            payload.entry_role = Some(artifact.file_name().to_string());
            let (_, entry_aad) = derive_artifact_aads(&payload, encryption.organization_id)
                .expect("derive test AAD");
            let mut ciphertext = Vec::new();
            crypto::encrypt_stream_blocking(
                &mut std::io::Cursor::new(frame),
                &mut ciphertext,
                &test_dek(),
                &entry_aad,
            )
            .expect("encrypt test frame");
            ciphertext
        }
    };
    PullCase {
        _dir: dir,
        local_path,
        payload,
        body,
    }
}

/// One codec permit in total, so a test holding it proves whether a decode
/// needs one.
fn single_permit_executor(body: Vec<u8>) -> TransferExecutor {
    codec_test_executor_with_store(1, Arc::new(MockObjectStore::new([], body)))
}

async fn pull(
    executor: &TransferExecutor,
    route: Route,
    payload: &TransferPayload,
) -> Result<i64, JobError> {
    let dek = test_dek();
    let resolved_dek = match route {
        Route::Plain => None,
        Route::Encrypted => Some(dek.as_slice()),
    };
    executor
        .execute_pull(
            &test_job("pull"),
            payload,
            &test_creds(),
            test_target(),
            resolved_dek,
            PullExistingPolicy::LegacySkipPresent,
        )
        .await
}

fn assert_no_scratch(case: &PullCase) {
    let dir = case.local_path.parent().unwrap();
    let residue: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path != &case.local_path)
        .collect();
    assert!(residue.is_empty(), "scratch left behind: {residue:?}");
}

#[tokio::test]
async fn bypassed_class_under_the_cap_decodes_without_a_permit() {
    let plaintext = br#"{"version":1,"sandbox":"synthetic"}"#.repeat(64);
    for route in ROUTES {
        for artifact in [SnapshotArtifact::Metadata, SnapshotArtifact::VmState] {
            let case = pull_case(route, artifact, &plaintext);
            let executor = single_permit_executor(case.body.clone());
            let _held = executor
                .acquire_codec_permit(JobPriority::default())
                .await
                .unwrap();

            let out = tokio::time::timeout(PULL_DEADLINE, pull(&executor, route, &case.payload))
                .await
                .unwrap_or_else(|_| {
                    panic!("{route:?} {artifact:?} pull waited on the held codec permit")
                })
                .unwrap_or_else(|e| panic!("{route:?} {artifact:?} pull failed: {e}"));

            assert_eq!(usize::try_from(out).unwrap(), plaintext.len());
            assert_eq!(std::fs::read(&case.local_path).unwrap(), plaintext);
            assert_eq!(
                executor.codec.shared_available(),
                0,
                "only the test's permit is outstanding"
            );
            assert_no_scratch(&case);
        }
    }
}

#[tokio::test]
async fn bypassed_class_over_the_cap_completes_under_a_permit() {
    // Highly compressible: a few KiB on the wire that inflate past the cap,
    // the shape a corrupt or hostile object would take. The pass sequence
    // (capped, then a permit) is pinned by
    // `admitted_decode_hands_each_pass_the_expected_gate`; this proves the
    // restarted pull still produces the full output once the permit frees.
    let cap = usize::try_from(CLASS_BYPASS_MAX_DECODED_BYTES).unwrap();
    let plaintext = vec![b'v'; cap + 1];
    // A one-shot frame pledges its size, small enough to pass frame-header
    // admission; the overflow already disproved it, so the restart must
    // still take a permit.
    let pledged = zstd::bulk::compress(&plaintext, ZSTD_LEVEL).unwrap();
    let cases = [
        (Route::Plain, zstd_compress(&plaintext)),
        (Route::Encrypted, zstd_compress(&plaintext)),
        (Route::Plain, pledged),
    ];
    for (route, frame) in cases {
        let case = pull_case_with_payload_bytes(route, SnapshotArtifact::VmState, frame);
        let executor = single_permit_executor(case.body.clone());
        let held = executor
            .acquire_codec_permit(JobPriority::default())
            .await
            .unwrap();
        let pull = pull(&executor, route, &case.payload);
        tokio::pin!(pull);
        assert!(
            tokio::time::timeout(PENDING_PROBE, &mut pull)
                .await
                .is_err(),
            "{route:?} restart must wait for a codec permit"
        );

        drop(held);
        let out = tokio::time::timeout(PULL_DEADLINE, pull)
            .await
            .expect("restart completes once the permit frees")
            .unwrap_or_else(|e| panic!("{route:?} over-cap pull must succeed: {e}"));
        assert_eq!(usize::try_from(out).unwrap(), plaintext.len());
        assert_eq!(std::fs::read(&case.local_path).unwrap(), plaintext);
        assert_eq!(
            executor.codec.shared_available(),
            1,
            "the restart's permit is released"
        );
        assert_no_scratch(&case);
    }
}

#[tokio::test]
async fn non_bypassed_class_still_waits_for_a_permit() {
    let plaintext = vec![b'r'; 4096];
    for route in ROUTES {
        for artifact in [SnapshotArtifact::Memory, SnapshotArtifact::Rootfs] {
            let case = pull_case(route, artifact, &plaintext);
            let executor = single_permit_executor(case.body.clone());
            let held = executor
                .acquire_codec_permit(JobPriority::default())
                .await
                .unwrap();

            let pull = pull(&executor, route, &case.payload);
            tokio::pin!(pull);
            assert!(
                tokio::time::timeout(PENDING_PROBE, &mut pull)
                    .await
                    .is_err(),
                "{route:?} {artifact:?} decode must wait for the held permit"
            );

            drop(held);
            let out = tokio::time::timeout(PULL_DEADLINE, pull)
                .await
                .expect("decode completes once the permit frees")
                .unwrap_or_else(|e| panic!("{route:?} {artifact:?} pull failed: {e}"));
            assert_eq!(usize::try_from(out).unwrap(), plaintext.len());
            assert_eq!(std::fs::read(&case.local_path).unwrap(), plaintext);
            assert_no_scratch(&case);
        }
    }
}

/// Deterministic bytes zstd cannot shrink.
fn incompressible(len: usize) -> Vec<u8> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[tokio::test]
async fn bypassed_class_over_the_input_bound_waits_for_a_permit() {
    // The output cap does not bound decrypt or frame-parsing work, so an
    // input larger than the bypass input bound must wait for admission.
    let cap = usize::try_from(CLASS_BYPASS_MAX_INPUT_BYTES).unwrap();
    let data = incompressible(cap + 4096);
    let frame = zstd_compress(&data);
    assert!(frame.len() > cap, "fixture must exceed the input bound");
    let cases = [
        ("plain zstd", Route::Plain, frame.clone()),
        ("encrypted zstd", Route::Encrypted, frame),
        ("encrypted passthrough", Route::Encrypted, data.clone()),
    ];
    for (name, route, payload_bytes) in cases {
        let case = pull_case_with_payload_bytes(route, SnapshotArtifact::VmState, payload_bytes);
        let executor = single_permit_executor(case.body.clone());
        let held = executor
            .acquire_codec_permit(JobPriority::default())
            .await
            .unwrap();
        let pull = pull(&executor, route, &case.payload);
        tokio::pin!(pull);
        assert!(
            tokio::time::timeout(PENDING_PROBE, &mut pull)
                .await
                .is_err(),
            "{name} pull must wait for the held permit"
        );
        drop(held);
        let out = tokio::time::timeout(PULL_DEADLINE, pull)
            .await
            .expect("pull completes once the permit frees")
            .unwrap_or_else(|e| panic!("{name} pull failed: {e}"));

        assert_eq!(usize::try_from(out).unwrap(), data.len());
        assert!(
            std::fs::read(&case.local_path).unwrap() == data,
            "{name} output differs"
        );
        assert_no_scratch(&case);
    }
}

/// The gate one decode pass was handed.
#[derive(Debug, PartialEq)]
enum Seen {
    Capped,
    Permit,
    Unadmitted,
}

impl Seen {
    fn of(gate: &DecodeGate) -> Self {
        match gate {
            DecodeGate::Capped => Self::Capped,
            DecodeGate::Permit(Some(_)) => Self::Permit,
            DecodeGate::Permit(None) => Self::Unadmitted,
        }
    }
}

/// Run [`TransferExecutor::run_admitted_decode`] over `input` with a fake
/// pass that overflows on its first `overflows` calls, and return the gate
/// each pass saw.
async fn gates_seen(
    artifact: SnapshotArtifact,
    wait_phase: Option<TransferPhase>,
    input: &[u8],
    overflows: usize,
) -> Vec<Seen> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    std::fs::write(&path, input).unwrap();
    let mut payload = push_payload(&path);
    payload.snapshot_artifact = Some(artifact);
    let executor = single_permit_executor(Vec::new());
    let seen = std::sync::Mutex::new(Vec::new());
    let (_, result) = executor
        .run_admitted_decode(
            &test_job("pull"),
            &payload,
            MaterializationMode::Disabled,
            wait_phase,
            &path,
            |gate| {
                let mut seen = seen.lock().unwrap();
                seen.push(Seen::of(&gate));
                let outcome = if seen.len() <= overflows {
                    CappedDecode::OverCap
                } else {
                    CappedDecode::Complete(())
                };
                async move { Ok::<_, JobError>(outcome) }
            },
        )
        .await
        .expect("admission succeeds");
    result.expect("pass succeeds");
    assert_eq!(
        executor.codec.shared_available(),
        1,
        "every permit is released"
    );
    seen.into_inner().unwrap()
}

#[tokio::test]
async fn admitted_decode_hands_each_pass_the_expected_gate() {
    let wait = Some(TransferPhase::DecompressWait);
    let small = zstd_compress(&[b's'; 512]);
    let cap = usize::try_from(CLASS_BYPASS_MAX_INPUT_BYTES).unwrap();
    let large = vec![0; cap + 1];
    // Pledges a small content size, which frame-header admission alone
    // would trust.
    let pledged = zstd::bulk::compress(&[b'p'; 512], ZSTD_LEVEL).unwrap();
    let cases = [
        (
            "bypassed class",
            SnapshotArtifact::VmState,
            wait,
            &small[..],
            0,
            vec![Seen::Capped],
        ),
        (
            "bypassed class over the cap",
            SnapshotArtifact::Metadata,
            wait,
            &small[..],
            1,
            vec![Seen::Capped, Seen::Permit],
        ),
        (
            "bypassed class with a pledged frame over the cap",
            SnapshotArtifact::VmState,
            wait,
            &pledged[..],
            1,
            vec![Seen::Capped, Seen::Permit],
        ),
        (
            "bypassed class whose input exceeds the cap",
            SnapshotArtifact::VmState,
            wait,
            &large[..],
            0,
            vec![Seen::Permit],
        ),
        (
            "admitted class",
            SnapshotArtifact::Memory,
            wait,
            &small[..],
            0,
            vec![Seen::Permit],
        ),
        (
            "pass that decodes nothing",
            SnapshotArtifact::VmState,
            None,
            &small[..],
            0,
            vec![Seen::Unadmitted],
        ),
    ];
    for (name, artifact, wait_phase, input, overflows, expected) in cases {
        assert_eq!(
            gates_seen(artifact, wait_phase, input, overflows).await,
            expected,
            "{name}"
        );
    }
}

#[tokio::test]
async fn capped_passes_report_overflow_and_leave_only_their_input() {
    let plaintext = vec![b'c'; usize::try_from(CLASS_BYPASS_MAX_DECODED_BYTES).unwrap() + 1];
    let frame = zstd_compress(&plaintext);
    let only_entry = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    };

    let dir = tempfile::tempdir().unwrap();
    let compressed = dir.path().join("vmstate");
    std::fs::write(&compressed, &frame).unwrap();
    let outcome = decompress_in_place(
        &compressed,
        Some(SnapshotArtifact::VmState),
        MaterializationMode::Disabled,
        DecodeGate::Capped,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, CappedDecode::OverCap), "{outcome:?}");
    assert_eq!(std::fs::read(&compressed).unwrap(), frame, "input intact");
    assert_eq!(only_entry(dir.path()), [compressed], "no decode scratch");

    let case = pull_case_with_payload_bytes(Route::Encrypted, SnapshotArtifact::VmState, frame);
    let ciphertext = case.local_path.with_file_name("ciphertext");
    std::fs::write(&ciphertext, &case.body).unwrap();
    let outcome = stage_decrypted_temp(
        &test_job("pull"),
        &case.payload,
        &test_encryption(),
        &ciphertext,
        DecryptedPullOptions {
            decompress_if_zstd: true,
            materialization_mode: MaterializationMode::Disabled,
        },
        Some(&test_dek()),
        DecodeGate::Capped,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, CappedDecode::OverCap), "{outcome:?}");
    assert_eq!(
        std::fs::read(&ciphertext).unwrap(),
        case.body,
        "input intact"
    );
    assert_eq!(
        only_entry(ciphertext.parent().unwrap()),
        [ciphertext],
        "no plaintext scratch"
    );
}
