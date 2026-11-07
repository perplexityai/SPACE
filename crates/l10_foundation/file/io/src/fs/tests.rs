use super::*;

fn key() -> Vec<u8> {
    // Fixed key bytes for deterministic test failures; never
    // generated like this in production.
    (0..KEY_LEN as u8).collect()
}

fn aad() -> Vec<u8> {
    b"space-test/v1".to_vec()
}

fn write_tmp(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, bytes).unwrap();
    p
}

fn read_all(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap()
}

#[test]
fn interrupted_stream_reads_preserve_plaintext() {
    struct InterruptedReader<R> {
        inner: R,
        interrupt: bool,
    }
    impl<R: Read> Read for InterruptedReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.interrupt = !self.interrupt;
            if self.interrupt {
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            let length = buf.len().min(8192);
            self.inner.read(&mut buf[..length])
        }
    }

    let multiple_chunks = vec![0xAB; CHUNK_SIZE * 2 + 7];
    for plaintext in [b"".as_slice(), b"example payload", &multiple_chunks] {
        let mut source = InterruptedReader {
            inner: plaintext,
            interrupt: false,
        };
        let mut encrypted = Vec::new();
        encrypt_stream_blocking(&mut source, &mut encrypted, &key(), &aad()).unwrap();
        let mut source = InterruptedReader {
            inner: encrypted.as_slice(),
            interrupt: false,
        };
        let mut recovered = Vec::new();
        decrypt_stream_blocking(&mut source, &mut recovered, &key(), &aad()).unwrap();
        assert_eq!(recovered, plaintext);
    }
}

#[test]
fn round_trips_small_input() {
    let dir = tempfile::tempdir().unwrap();
    let pt = b"hello, BYOK";
    let src = write_tmp(dir.path(), "src", pt);
    let enc = dir.path().join("src.enc");
    let dec = dir.path().join("src.dec");

    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();
    // Header + last chunk + tag must add up to at least
    // HEADER_LEN + TAG_LEN bytes, and ciphertext must differ
    // from plaintext.
    let enc_bytes = read_all(&enc);
    assert!(enc_bytes.len() >= HEADER_LEN + TAG_LEN);
    assert_ne!(&enc_bytes[HEADER_LEN..HEADER_LEN + pt.len()], pt);

    decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap();
    assert_eq!(read_all(&dec), pt);
}

#[test]
fn round_trips_empty_input() {
    // An empty plaintext file produces a header + one zero-length
    // last chunk + 16-byte tag. Decrypting it must produce a
    // zero-length file, not an error.
    let dir = tempfile::tempdir().unwrap();
    let src = write_tmp(dir.path(), "empty", b"");
    let enc = dir.path().join("empty.enc");
    let dec = dir.path().join("empty.dec");

    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();
    assert_eq!(read_all(&enc).len(), HEADER_LEN + TAG_LEN);
    decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap();
    assert!(read_all(&dec).is_empty());
}

#[test]
fn round_trips_multi_chunk_input() {
    // Force the double-buffered loop through several iterations:
    // a payload of CHUNK_SIZE * 2 + 7 bytes covers two full
    // non-final chunks and a short last chunk, which exercises
    // every branch of the encrypt/decrypt loops.
    let dir = tempfile::tempdir().unwrap();
    let pt: Vec<u8> = (0..(CHUNK_SIZE * 2 + 7) as u32).map(|i| i as u8).collect();
    let src = write_tmp(dir.path(), "big", &pt);
    let enc = dir.path().join("big.enc");
    let dec = dir.path().join("big.dec");

    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();
    decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap();
    assert_eq!(read_all(&dec), pt);
}

#[test]
fn round_trips_at_chunk_boundary() {
    // The double-buffered loop has a subtle hazard when the
    // plaintext is an exact multiple of CHUNK_SIZE: the last
    // plaintext chunk is full-size and the next read returns 0,
    // which must still be flushed via encrypt_last (not skipped).
    let dir = tempfile::tempdir().unwrap();
    let pt: Vec<u8> = vec![0xAB; CHUNK_SIZE];
    let src = write_tmp(dir.path(), "boundary", &pt);
    let enc = dir.path().join("boundary.enc");
    let dec = dir.path().join("boundary.dec");

    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();
    decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap();
    assert_eq!(read_all(&dec), pt);
}

#[test]
fn rejects_wrong_key() {
    let dir = tempfile::tempdir().unwrap();
    let src = write_tmp(dir.path(), "src", b"some plaintext");
    let enc = dir.path().join("src.enc");
    let dec = dir.path().join("src.dec");
    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();

    let mut bad = key();
    bad[0] ^= 0xFF;
    let err = decrypt_file_blocking(&enc, &dec, &bad, &aad()).unwrap_err();
    assert!(matches!(err, CryptoError::AuthFailed));
}

#[test]
fn rejects_wrong_aad() {
    let dir = tempfile::tempdir().unwrap();
    let src = write_tmp(dir.path(), "src", b"some plaintext");
    let enc = dir.path().join("src.enc");
    let dec = dir.path().join("src.dec");
    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();

    let err = decrypt_file_blocking(&enc, &dec, &key(), b"different-aad").unwrap_err();
    assert!(matches!(err, CryptoError::AuthFailed));
}

#[test]
fn rejects_tampered_ciphertext() {
    let dir = tempfile::tempdir().unwrap();
    let pt: Vec<u8> = (0..(CHUNK_SIZE + 100) as u32).map(|i| i as u8).collect();
    let src = write_tmp(dir.path(), "src", &pt);
    let enc = dir.path().join("src.enc");
    let dec = dir.path().join("src.dec");
    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();

    // Flip a byte deep inside the first ciphertext chunk so the
    // tampering hits the AEAD tag check, not the header.
    let mut bytes = read_all(&enc);
    bytes[HEADER_LEN + 1024] ^= 0x01;
    std::fs::write(&enc, &bytes).unwrap();

    let err = decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap_err();
    assert!(matches!(err, CryptoError::AuthFailed));
}

#[test]
fn rejects_truncated_after_header() {
    let dir = tempfile::tempdir().unwrap();
    let src = write_tmp(dir.path(), "src", b"plaintext");
    let enc = dir.path().join("src.enc");
    let dec = dir.path().join("src.dec");
    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();

    // Keep just the header; cut all ciphertext + tag.
    let bytes = read_all(&enc);
    std::fs::write(&enc, &bytes[..HEADER_LEN]).unwrap();
    let err = decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap_err();
    assert!(matches!(err, CryptoError::Truncated));
}

#[test]
fn rejects_bad_magic() {
    let dir = tempfile::tempdir().unwrap();
    // Hand-craft a file whose first 8 bytes are not the magic.
    let mut bytes = vec![0u8; HEADER_LEN + TAG_LEN];
    bytes[..8].copy_from_slice(b"NOTMAGIC");
    let enc = dir.path().join("forged.enc");
    let dec = dir.path().join("forged.dec");
    std::fs::write(&enc, &bytes).unwrap();
    let err = decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap_err();
    assert!(matches!(err, CryptoError::BadHeader));
}

#[test]
fn rejects_unsupported_version() {
    let dir = tempfile::tempdir().unwrap();
    let src = write_tmp(dir.path(), "src", b"plaintext");
    let enc = dir.path().join("src.enc");
    let dec = dir.path().join("src.dec");
    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();

    // Bump the version byte to a value this build doesn't know.
    let mut bytes = read_all(&enc);
    bytes[FILE_MAGIC.len()] = 0xFE;
    std::fs::write(&enc, &bytes).unwrap();
    let err = decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap_err();
    assert!(matches!(err, CryptoError::UnsupportedVersion(0xFE)));
}

#[test]
fn rejects_chunk_reorder_via_be32() {
    // Move a non-last ciphertext chunk to a different position;
    // the BE32 counter is bound into each chunk's nonce so this
    // must fail authentication.
    let dir = tempfile::tempdir().unwrap();
    let pt: Vec<u8> = (0..(CHUNK_SIZE * 3) as u32).map(|i| i as u8).collect();
    let src = write_tmp(dir.path(), "src", &pt);
    let enc = dir.path().join("src.enc");
    let dec = dir.path().join("src.dec");
    encrypt_file_blocking(&src, &enc, &key(), &aad()).unwrap();

    // Layout: [HEADER_LEN][CT0][CT1][CT2_LAST]
    // CT0/CT1 are CT_CHUNK_SIZE bytes each; swap them.
    let mut bytes = read_all(&enc);
    let c0 = HEADER_LEN;
    let c1 = HEADER_LEN + CT_CHUNK_SIZE;
    let c2 = HEADER_LEN + 2 * CT_CHUNK_SIZE;
    let mut tmp = vec![0u8; CT_CHUNK_SIZE];
    tmp.copy_from_slice(&bytes[c0..c1]);
    let snd = bytes[c1..c2].to_vec();
    bytes[c0..c1].copy_from_slice(&snd);
    bytes[c1..c2].copy_from_slice(&tmp);
    std::fs::write(&enc, &bytes).unwrap();

    let err = decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap_err();
    assert!(matches!(err, CryptoError::AuthFailed));
}

#[test]
fn rejects_short_key() {
    let dir = tempfile::tempdir().unwrap();
    let src = write_tmp(dir.path(), "src", b"plaintext");
    let enc = dir.path().join("src.enc");
    let err = encrypt_file_blocking(&src, &enc, &[0u8; 16], &aad()).unwrap_err();
    assert!(matches!(err, CryptoError::InvalidKeyLength(16)));
}

#[test]
fn encrypted_sibling_appends_suffix() {
    assert_eq!(
        encrypted_sibling(Path::new("/space/cache/vmstate")),
        Path::new("/space/cache/vmstate.enc.tmp"),
    );
    assert_eq!(
        encrypted_sibling(Path::new("/space/cache/memory.zst.tmp")),
        Path::new("/space/cache/memory.zst.tmp.enc.tmp"),
    );
}

#[test]
fn decrypted_sibling_appends_distinct_suffix() {
    // Distinct from .dec.tmp (used by decompress_in_place) so the
    // crypto pass and the zstd-decompress pass cannot ever share a
    // temp path even on the same base name.
    assert_eq!(
        decrypted_sibling(Path::new("/space/cache/memory.partial")),
        Path::new("/space/cache/memory.partial.plaintext.tmp"),
    );
}

#[test]
fn zstd_then_encrypt_stream_round_trips_via_file_decrypt() {
    // The chained push pipeline reads plaintext through
    // `zstd::stream::read::Encoder` and into `encrypt_stream_blocking`,
    // so the produced ciphertext must decrypt cleanly via the file-
    // shape `decrypt_file_blocking` followed by a standalone zstd
    // decompress. Verifies (a) the wire format is wire-identical to
    // what `encrypt_file_blocking` produces and (b) the AEAD's
    // multi-chunk path works when the plaintext arrives from a
    // streaming `Read` rather than a `File`.
    let dir = tempfile::tempdir().unwrap();
    let plaintext: Vec<u8> = (0u8..=u8::MAX) // 3 MiB → multiple AEAD chunks
        .cycle()
        .take(3 * 1024 * 1024)
        .collect();
    let src = write_tmp(dir.path(), "src", &plaintext);
    let enc = dir.path().join("src.zst.enc");

    // Source → BufReader → zstd::stream::read::Encoder → AEAD → file.
    let src_file = std::fs::File::open(&src).unwrap();
    let buf = std::io::BufReader::with_capacity(64 * 1024, src_file);
    let mut zstd_reader = zstd::stream::read::Encoder::new(buf, 3).unwrap();
    let mut enc_file = std::fs::File::create(&enc).unwrap();
    encrypt_stream_blocking(&mut zstd_reader, &mut enc_file, &key(), &aad()).unwrap();
    drop(enc_file);

    // File-shape decrypt of the streaming output proves the wire
    // format is interchangeable across the two encrypt entry points.
    let dec = dir.path().join("src.zst.dec");
    decrypt_file_blocking(&enc, &dec, &key(), &aad()).unwrap();

    // The decrypted bytes are zstd-compressed plaintext; decompress
    // and assert the original bytes came through untouched.
    let zst_bytes = read_all(&dec);
    let recovered = zstd::decode_all(&zst_bytes[..]).unwrap();
    assert_eq!(recovered, plaintext);
}
