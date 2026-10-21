use super::*;
use std::os::unix::fs::FileExt;

struct ShortReader {
    bytes: Vec<u8>,
    pos: usize,
    max_read: usize,
}

impl std::io::Read for ShortReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos == self.bytes.len() {
            return Ok(0);
        }
        let read = self
            .max_read
            .min(buf.len())
            .min(self.bytes.len() - self.pos);
        buf[..read].copy_from_slice(&self.bytes[self.pos..self.pos + read]);
        self.pos += read;
        Ok(read)
    }
}

#[test]
fn read_decoded_rootfs_chunk_coalesces_short_reads() {
    let bytes: Vec<u8> = (0..25).collect();
    let mut reader = ShortReader {
        bytes: bytes.clone(),
        pos: 0,
        max_read: 3,
    };

    let first = read_decoded_rootfs_chunk(&mut reader, vec![0u8; 10])
        .unwrap()
        .unwrap();
    assert_eq!(first.bytes().unwrap(), &bytes[..10]);
    assert_eq!(first.into_buffer().len(), 10);

    let second = read_decoded_rootfs_chunk(&mut reader, vec![0u8; 10])
        .unwrap()
        .unwrap();
    assert_eq!(second.bytes().unwrap(), &bytes[10..20]);

    let third = read_decoded_rootfs_chunk(&mut reader, vec![0u8; 10])
        .unwrap()
        .unwrap();
    assert_eq!(third.bytes().unwrap(), &bytes[20..]);

    let eof = read_decoded_rootfs_chunk(&mut reader, vec![0u8; 10]).unwrap();
    assert!(eof.is_none());
}

/// Fails its `n`th read once with each listed error kind, then reads normally.
struct FaultyReader {
    inner: ShortReader,
    calls: usize,
    faults: Vec<(usize, std::io::ErrorKind)>,
}

impl std::io::Read for FaultyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.calls += 1;
        if let Some(index) = self.faults.iter().position(|(call, _)| *call == self.calls) {
            let (_, kind) = self.faults.remove(index);
            return Err(std::io::Error::new(kind, "injected"));
        }
        self.inner.read(buf)
    }
}

/// The decoder sees every input byte once, in order, and the off-thread
/// digest covers exactly those bytes, however reads split across pooled
/// buffers that are recycled many times over. Interrupted reads retry, a
/// failed read surfaces once without losing bytes already read, and reads
/// past the end keep returning nothing. A buffer lost on any of those paths
/// would leave a later read waiting forever, so each case runs under a
/// timeout.
#[test]
fn hashed_compressed_input_matches_a_whole_file_digest() {
    use std::io::ErrorKind::{Interrupted, Other};
    let bytes: Vec<u8> = (0..10_000u32).map(|i| (i * 31 % 251) as u8).collect();
    for (chunk_size, depth, max_read, faults) in [
        (1, 1, 1, vec![]),
        (7, 2, 3, vec![]),
        (64, 3, 64, vec![]),
        (20_000, 3, 4096, vec![]),
        (7, 1, 3, vec![(2, Interrupted), (5, Other)]),
        (64, 1, 64, vec![(1, Other), (40, Other)]),
    ] {
        let case = format!("chunk {chunk_size}, depth {depth}, faults {faults:?}");
        let expected_errors = faults.iter().filter(|(_, kind)| *kind == Other).count();
        let bytes = bytes.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let reader = FaultyReader {
                inner: ShortReader {
                    bytes: bytes.clone(),
                    pos: 0,
                    max_read,
                },
                calls: 0,
                faults,
            };
            let mut input = HashedCompressedInput::new(reader, chunk_size, depth).unwrap();
            let (mut seen, mut errors) = (Vec::new(), 0);
            let mut scratch = [0u8; 5];
            loop {
                match input.read(&mut scratch) {
                    Ok(0) => break,
                    Ok(read) => seen.extend_from_slice(&scratch[..read]),
                    Err(_) => errors += 1,
                }
            }
            let past_end = (0..depth + 2)
                .map(|_| input.read(&mut scratch).unwrap())
                .sum::<usize>();
            let digest = input.finish_hash().unwrap();
            done_tx
                .send((
                    seen == bytes,
                    errors,
                    past_end,
                    digest == sha256_bytes(&bytes),
                ))
                .unwrap();
        });
        let (bytes_match, errors, past_end, digest_matches) = done_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap_or_else(|_| panic!("{case}: reader stalled"));
        assert!(bytes_match, "{case}: delivered bytes");
        assert_eq!(errors, expected_errors, "{case}: surfaced errors");
        assert_eq!(past_end, 0, "{case}: reads past the end");
        assert!(digest_matches, "{case}: digest");
    }
}

#[rstest::rstest]
#[case::default(None)]
#[case::sha256(Some(HashAlg::Sha256))]
#[case::blake3(Some(HashAlg::Blake3))]
fn rootfs_zstd_round_trips_sparse_file(#[case] algorithm: Option<HashAlg>) {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join(TEMPLATE_ROOTFS_FILE);
    let compressed = temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE);
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len((2 * DECODE_BUFFER_SIZE + 123) as u64)
        .unwrap();
    let data = b"space rootfs zstd round trip";
    std::fs::OpenOptions::new()
        .write(true)
        .open(&rootfs)
        .unwrap()
        .write_at(data, 8 * 1024 * 1024)
        .unwrap();

    let compress_stats = compress_rootfs(&rootfs, &compressed).unwrap();
    assert_eq!(
        compress_stats.rootfs_size,
        (2 * DECODE_BUFFER_SIZE + 123) as u64
    );
    assert!(compress_stats.compressed_size < compress_stats.rootfs_size);
    assert_eq!(
        compress_stats.compressed_hash,
        sha256_bytes(&std::fs::read(&compressed).unwrap())
    );

    let bytes = std::fs::read(&rootfs).unwrap();
    let expected = match algorithm.unwrap_or(HashAlg::Sha256) {
        HashAlg::Sha256 => sha256_bytes(&bytes),
        HashAlg::Blake3 => format!("blake3:{}", blake3::hash(&bytes)),
    };
    std::fs::remove_file(&rootfs).unwrap();
    let decompress_stats = decompress_rootfs(
        &compressed,
        &rootfs,
        algorithm.map(|_| expected.as_str()),
        crate::rootfs_backing::RootfsBackingPolicy::Inherit,
    )
    .unwrap();
    assert_eq!(
        decompress_stats.rootfs_size,
        (2 * DECODE_BUFFER_SIZE + 123) as u64
    );
    assert_eq!(
        decompress_stats.compressed_size,
        compress_stats.compressed_size
    );
    assert_eq!(decompress_stats.rootfs_hash, expected);
    assert_eq!(
        decompress_stats.compressed_hash,
        sha256_bytes(&std::fs::read(&compressed).unwrap())
    );

    let restored = std::fs::OpenOptions::new()
        .read(true)
        .open(&rootfs)
        .unwrap();
    let mut read_back = vec![0u8; data.len()];
    restored.read_at(&mut read_back, 8 * 1024 * 1024).unwrap();
    assert_eq!(read_back, data);
}

/// Recovery adopts whatever file sits at the rootfs path without rehashing
/// it, so bytes that fail verification must never be renamed into place.
#[test]
fn decompress_rootfs_refuses_to_publish_a_digest_mismatch() {
    for algorithm in [HashAlg::Sha256, HashAlg::Blake3] {
        let temp = tempfile::tempdir().unwrap();
        let rootfs = temp.path().join(TEMPLATE_ROOTFS_FILE);
        let compressed = temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE);
        std::fs::write(&rootfs, b"bytes the release never hashed").unwrap();
        compress_rootfs(&rootfs, &compressed).unwrap();
        let bytes = std::fs::read(&rootfs).unwrap();
        let actual_rootfs_hash = match algorithm {
            HashAlg::Sha256 => sha256_bytes(&bytes),
            HashAlg::Blake3 => format!("blake3:{}", blake3::hash(&bytes)),
        };
        std::fs::remove_file(&rootfs).unwrap();
        let expected = format!("{algorithm}:{}", "1".repeat(64));

        let err = decompress_rootfs(
            &compressed,
            &rootfs,
            Some(&expected),
            crate::rootfs_backing::RootfsBackingPolicy::Inherit,
        )
        .unwrap_err();

        assert!(
            matches!(
                &err,
                RootfsZstdError::ExpandedDigestMismatch { expected: e, actual }
                    if *e == expected && *actual == actual_rootfs_hash
            ),
            "{err}"
        );
        assert!(!rootfs.exists(), "mismatching bytes must not be published");
        assert_eq!(
            std::fs::read_dir(temp.path()).unwrap().count(),
            1,
            "only the compressed input may remain; no temp output left behind"
        );

        let stats = decompress_rootfs(
            &compressed,
            &rootfs,
            Some(&actual_rootfs_hash),
            crate::rootfs_backing::RootfsBackingPolicy::Inherit,
        )
        .unwrap();
        assert_eq!(stats.rootfs_hash, actual_rootfs_hash);
        assert!(rootfs.exists());
    }
}

#[test]
fn hash_rootfs_file_matches_decompressed_hash() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join(TEMPLATE_ROOTFS_FILE);
    let compressed = temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE);
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len(4 * 1024 * 1024)
        .unwrap();
    let data = b"space hash_rootfs_file matches decompress";
    std::fs::OpenOptions::new()
        .write(true)
        .open(&rootfs)
        .unwrap()
        .write_at(data, 1024 * 1024)
        .unwrap();

    compress_rootfs(&rootfs, &compressed).unwrap();
    let (legacy_hash, legacy_size) =
        hash_rootfs_file(&rootfs, pplx_space_common_core::types::HashAlg::Sha256).unwrap();
    assert_eq!(legacy_size, 4 * 1024 * 1024);
    assert_eq!(legacy_hash, sha256_bytes(&std::fs::read(&rootfs).unwrap()));

    std::fs::remove_file(&rootfs).unwrap();
    let decompress_stats = decompress_rootfs(
        &compressed,
        &rootfs,
        None,
        crate::rootfs_backing::RootfsBackingPolicy::Inherit,
    )
    .unwrap();
    assert_eq!(decompress_stats.rootfs_hash, legacy_hash);
    assert_eq!(decompress_stats.rootfs_size, legacy_size);
}

#[test]
fn decompress_rootfs_rejects_truncated_zstd() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join(TEMPLATE_ROOTFS_FILE);
    let compressed = temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE);
    std::fs::write(&rootfs, vec![42u8; 1024 * 1024]).unwrap();
    compress_rootfs(&rootfs, &compressed).unwrap();
    let compressed_size = std::fs::metadata(&compressed).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&compressed)
        .unwrap()
        .set_len(compressed_size / 2)
        .unwrap();
    std::fs::remove_file(&rootfs).unwrap();

    let err = decompress_rootfs(
        &compressed,
        &rootfs,
        None,
        crate::rootfs_backing::RootfsBackingPolicy::Inherit,
    )
    .unwrap_err();
    assert!(
        matches!(err, RootfsZstdError::Zstd { .. }),
        "truncated zstd must fail in the decoder, got {err:?}"
    );
    assert!(
        !rootfs.exists(),
        "truncated input must not be renamed into place"
    );
}

#[test]
fn materialization_result_prefers_writer_error() {
    let hash_error = RootfsZstdError::Pipeline {
        operation: "send hashed rootfs chunk to sparse writer",
    };
    let writer_error = RootfsZstdError::Io {
        operation: "write sparse rootfs temp",
        path: "/tmp/rootfs.ext4.tmp".to_string(),
        source: std::io::Error::from(std::io::ErrorKind::WriteZero),
    };

    let err = materialization_result(Err(hash_error), Err(writer_error)).unwrap_err();
    match err {
        RootfsZstdError::Io {
            operation, source, ..
        } => {
            assert_eq!(operation, "write sparse rootfs temp");
            assert_eq!(source.kind(), std::io::ErrorKind::WriteZero);
        }
        other => panic!("expected writer I/O error, got {other:?}"),
    }
}

#[test]
fn encoder_workers_cap_host_parallelism() {
    assert_eq!(encoder_workers(1), 1);
    assert_eq!(encoder_workers(16), 16);
    // A metal host without a pod CPU limit exposes its full core count; the
    // encoder must not claim it.
    assert_eq!(encoder_workers(128), MAX_ENCODER_WORKERS);
    // Zero can only come from a failed parallelism probe; stay single-threaded.
    assert_eq!(encoder_workers(0), 1);
}

// Public SHA-256 and BLAKE3 test vectors for "abc" also check that raw-file
// verification and the streaming decoder implement the same digest contract.
#[test]
fn verifies_both_algorithms_and_rejects_unknown_before_creating_output() {
    let temp = tempfile::tempdir().unwrap();
    let compressed = temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE);
    std::fs::write(&compressed, zstd::encode_all(&b"abc"[..], 1).unwrap()).unwrap();
    let rootfs = temp.path().join(TEMPLATE_ROOTFS_FILE);
    for expected in [
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
    ] {
        let stats = decompress_rootfs(
            &compressed,
            &rootfs,
            Some(expected),
            crate::rootfs_backing::RootfsBackingPolicy::Inherit,
        )
        .unwrap();
        assert_eq!(stats.rootfs_hash, expected);
        assert_eq!(std::fs::read(&rootfs).unwrap(), b"abc");
        assert_eq!(
            hash_rootfs_file(&rootfs, expected.parse::<Digest>().unwrap().algorithm()).unwrap(),
            (expected.to_string(), 3)
        );
        std::fs::remove_file(&rootfs).unwrap();
    }
    for expected in ["md5:abc", "blake3:abc"] {
        assert!(matches!(
            decompress_rootfs(
                &compressed,
                &rootfs,
                Some(expected),
                crate::rootfs_backing::RootfsBackingPolicy::Inherit
            ),
            Err(RootfsZstdError::InvalidDigest(_))
        ));
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    }
}
