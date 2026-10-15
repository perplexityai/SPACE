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

#[test]
fn rootfs_zstd_round_trips_sparse_file() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join(TEMPLATE_ROOTFS_FILE);
    let compressed = temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE);
    std::fs::File::create(&rootfs)
        .unwrap()
        .set_len(16 * 1024 * 1024)
        .unwrap();
    let data = b"space rootfs zstd round trip";
    std::fs::OpenOptions::new()
        .write(true)
        .open(&rootfs)
        .unwrap()
        .write_at(data, 8 * 1024 * 1024)
        .unwrap();

    let compress_stats = compress_rootfs(&rootfs, &compressed).unwrap();
    assert_eq!(compress_stats.rootfs_size, 16 * 1024 * 1024);
    assert!(compress_stats.compressed_size < compress_stats.rootfs_size);
    assert_eq!(
        compress_stats.compressed_hash,
        sha256_bytes(&std::fs::read(&compressed).unwrap())
    );

    std::fs::remove_file(&rootfs).unwrap();
    let decompress_stats = decompress_rootfs(
        &compressed,
        &rootfs,
        None,
        crate::rootfs_backing::RootfsBackingPolicy::Inherit,
    )
    .unwrap();
    assert_eq!(decompress_stats.rootfs_size, 16 * 1024 * 1024);
    assert_eq!(
        decompress_stats.compressed_size,
        compress_stats.compressed_size
    );
    assert_eq!(
        decompress_stats.rootfs_hash,
        sha256_bytes(&std::fs::read(&rootfs).unwrap())
    );
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
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join(TEMPLATE_ROOTFS_FILE);
    let compressed = temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE);
    std::fs::write(&rootfs, b"bytes the release never hashed").unwrap();
    compress_rootfs(&rootfs, &compressed).unwrap();
    let actual_rootfs_hash = sha256_bytes(&std::fs::read(&rootfs).unwrap());
    std::fs::remove_file(&rootfs).unwrap();
    let expected = format!("sha256:{}", "1".repeat(64));

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
    let (legacy_hash, legacy_size) = hash_rootfs_file(&rootfs).unwrap();
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
