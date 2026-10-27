use super::*;

#[test]
fn published_chunks_reconstruct_source_across_sizes_and_deduplicate() {
    let scratch = tempfile::tempdir().unwrap();
    let source = scratch.path().join("source.ext4");
    let repeated = vec![0x5a_u8; 512 << 10];
    let mut original = repeated.clone();
    original.resize(1 << 20, 0);
    original.extend_from_slice(&repeated);
    original.extend_from_slice(b"independent trailing bytes");
    std::fs::write(&source, &original).unwrap();
    let expected = Digest::blake3(*blake3::hash(&original).as_bytes());
    for size in [
        ChunkSize::Kib512,
        ChunkSize::Mib1,
        ChunkSize::Mib4,
        ChunkSize::Mib16,
        ChunkSize::Mib64,
    ] {
        let objects = scratch.path().join(size.as_str());
        let publication = publish_chunks(&source, &objects, size, expected).unwrap();
        let index =
            AuthenticatedIndex::from_bytes(&publication.bytes, publication.digest, expected)
                .unwrap();
        let mut restored = Vec::new();
        for leaf in index.leaves() {
            match leaf {
                Leaf::Zero { length } => restored.resize(restored.len() + *length as usize, 0),
                Leaf::Zstd { length, digest } => restored.extend_from_slice(
                    &decode_chunk(
                        File::open(objects.join(object_name(*digest))).unwrap(),
                        *digest,
                        *length,
                    )
                    .unwrap(),
                ),
            }
        }
        assert_eq!(restored, original, "{size}");
        if size == ChunkSize::Kib512 {
            assert_eq!(publication.new_objects, 2);
            assert!(matches!(index.leaves()[1], Leaf::Zero { .. }));
        }
        let repeated_publication = publish_chunks(&source, &objects, size, expected).unwrap();
        assert_eq!(repeated_publication.new_objects, 0);
        assert_eq!(repeated_publication.bytes, publication.bytes);
    }
}

#[test]
fn index_authentication_and_layout_reject_substitution() {
    let expected = Digest::blake3(*blake3::hash(b"expected image").as_bytes());
    let bytes = serde_json::to_vec(&Index {
        version: INDEX_VERSION,
        rootfs_digest: expected,
        logical_bytes: 7,
        chunk_size: ChunkSize::Kib512,
        leaves: vec![Leaf::Zero { length: 7 }],
    })
    .unwrap();
    let trusted = digest_bytes(&bytes);
    let mut changed = bytes.clone();
    changed.push(b' ');
    assert!(matches!(
        AuthenticatedIndex::from_bytes(&changed, trusted, expected),
        Err(ReadIndexError::Authentication)
    ));
    assert!(matches!(
        AuthenticatedIndex::from_bytes(&bytes, trusted, Digest::sha256([0; 32])),
        Err(ReadIndexError::Rootfs)
    ));
    let original: Index = serde_json::from_slice(&bytes).unwrap();
    let mut wrong_eof = original.clone();
    wrong_eof.logical_bytes = 8;
    let mut missing = original.clone();
    missing.leaves.clear();
    let mut wrong_length = original;
    wrong_length.leaves[0] = Leaf::Zero { length: 6 };
    for malformed in [wrong_eof, missing, wrong_length] {
        let bytes = serde_json::to_vec(&malformed).unwrap();
        assert!(matches!(
            AuthenticatedIndex::from_bytes(&bytes, digest_bytes(&bytes), expected),
            Err(ReadIndexError::Layout(_))
        ));
    }
}

#[test]
fn decoder_rejects_wrong_content_and_trailing_frames() {
    let original = b"verified chunk bytes";
    let expected = digest_bytes(original);
    let altered = zstd::bulk::compress(b"modified chunk bytes", 3).unwrap();
    assert!(matches!(
        decode_chunk(altered.as_slice(), expected, original.len() as u64),
        Err(DecodeChunkError::Integrity)
    ));
    let frame = zstd::bulk::compress(original, 3).unwrap();
    assert_eq!(
        decode_chunk(frame.as_slice(), expected, original.len() as u64).unwrap(),
        original
    );
    for declared in [original.len() - 1, original.len() + 1] {
        decode_chunk(frame.as_slice(), expected, declared as u64).unwrap_err();
    }
    let doubled = [frame.as_slice(), frame.as_slice()].concat();
    assert!(matches!(
        decode_chunk(doubled.as_slice(), expected, original.len() as u64),
        Err(DecodeChunkError::Frame)
    ));
    decode_chunk(&frame[..frame.len() - 1], expected, original.len() as u64).unwrap_err();
}
